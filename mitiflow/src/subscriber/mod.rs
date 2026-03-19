//! Event subscriber with gap detection, recovery, and deduplication.

pub mod gap_detector;

#[cfg(feature = "store")]
pub mod checkpoint;

use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};
use zenoh::Session;
use zenoh::sample::{Sample, SampleKind};

use crate::attachment::{EventMeta, decode_metadata};
use crate::config::{EventBusConfig, HeartbeatMode, RecoveryMode};
use crate::error::{Error, Result};
use crate::event::{Event, RawEvent};
use crate::publisher::HeartbeatBeacon;
use crate::types::{EventId, PublisherId};
use gap_detector::{GapDetector, MissInfo, SampleResult, SequenceTracker};

/// Message routed to a specific processing shard.
enum ShardMsg {
    Sample {
        meta: crate::attachment::EventMeta,
        key: String,
        payload: Vec<u8>,
    },
    Heartbeat(HeartbeatBeacon),
}

/// Route a publisher ID to the owning shard index.
#[inline]
fn shard_for(pub_id: &PublisherId, num_shards: usize) -> usize {
    let bytes = pub_id.to_bytes();
    let n = u128::from_le_bytes(bytes);
    (n % num_shards as u128) as usize
}

/// Internal recovery configuration extracted from [`EventBusConfig`].
struct RecoveryConfig {
    key_prefix: String,
    store_key_prefix: Option<String>,
    recovery_delay: Duration,
    max_recovery_attempts: u32,
}

impl RecoveryConfig {
    fn from_bus_config(config: &EventBusConfig) -> Self {
        #[cfg(feature = "store")]
        let store_key_prefix = Some(config.resolved_store_key_prefix());
        #[cfg(not(feature = "store"))]
        let store_key_prefix = None;

        Self {
            key_prefix: config.key_prefix.clone(),
            store_key_prefix,
            recovery_delay: config.recovery_delay,
            max_recovery_attempts: config.max_recovery_attempts,
        }
    }
}

/// Tracks which sequences in a gap have been recovered.
struct RecoveryTracker {
    expected: Range<u64>,
    recovered: HashSet<u64>,
}

impl RecoveryTracker {
    fn new(missed: Range<u64>) -> Self {
        let cap = (missed.end - missed.start) as usize;
        Self {
            expected: missed,
            recovered: HashSet::with_capacity(cap),
        }
    }

    fn record(&mut self, seq: u64) {
        if self.expected.contains(&seq) {
            self.recovered.insert(seq);
        }
    }

    fn is_complete(&self) -> bool {
        self.recovered.len() as u64 == (self.expected.end - self.expected.start)
    }

    fn remaining(&self) -> Vec<u64> {
        self.expected
            .clone()
            .filter(|s| !self.recovered.contains(s))
            .collect()
    }
}

/// Try to decode a data sample into metadata + payload, filtering out
/// non-Put samples, internal keys, and samples without valid attachments.
/// Returns `None` for samples that should be skipped.
fn decode_sample(sample: &Sample) -> Option<(EventMeta, String, Vec<u8>)> {
    if sample.kind() != SampleKind::Put {
        return None;
    }
    let key = sample.key_expr().as_str();
    if key.contains("/_") {
        return None;
    }
    let attachment = match sample.attachment() {
        Some(a) => a,
        None => {
            trace!("sample without attachment on {key}, skipping");
            return None;
        }
    };
    let meta = match decode_metadata(attachment) {
        Ok(m) => m,
        Err(e) => {
            warn!("failed to decode attachment on {key}: {e}");
            return None;
        }
    };
    let payload = sample.payload().to_bytes().to_vec();
    Some((meta, key.to_string(), payload))
}

/// Parse a heartbeat beacon from a Zenoh sample payload.
fn decode_heartbeat(sample: &Sample) -> Option<HeartbeatBeacon> {
    let bytes = sample.payload().to_bytes();
    match serde_json::from_slice(&bytes) {
        Ok(b) => Some(b),
        Err(e) => {
            trace!("invalid heartbeat: {e}");
            None
        }
    }
}

/// Process a gap detection result: deliver the current event and spawn
/// recovery for any detected gap.
fn handle_sample_result(
    result: SampleResult,
    meta: &EventMeta,
    key: &str,
    payload: &[u8],
    tx: &flume::Sender<RawEvent>,
    session: &Session,
    recovery_config: &Arc<RecoveryConfig>,
) {
    match result {
        SampleResult::Deliver => {
            deliver_event(tx, meta.pub_id, meta.seq, key, payload, meta.event_id, meta.timestamp);
        }
        SampleResult::Duplicate => {
            trace!(seq = meta.seq, pub_id = %meta.pub_id, "duplicate, dropping");
        }
        SampleResult::Gap(miss) => {
            deliver_event(tx, meta.pub_id, meta.seq, key, payload, meta.event_id, meta.timestamp);
            spawn_recovery(session.clone(), Arc::clone(recovery_config), miss, tx.clone());
        }
    }
}

/// Spawn heartbeat-triggered gap recovery for all detected misses.
fn handle_heartbeat_gaps(
    misses: Vec<MissInfo>,
    session: &Session,
    recovery_config: &Arc<RecoveryConfig>,
    tx: &flume::Sender<RawEvent>,
) {
    for miss in misses {
        spawn_recovery(session.clone(), Arc::clone(recovery_config), miss, tx.clone());
    }
}

/// Spawn a background task to recover missed events using tiered recovery.
fn spawn_recovery(
    session: Session,
    recovery_config: Arc<RecoveryConfig>,
    miss: MissInfo,
    tx: flume::Sender<RawEvent>,
) {
    tokio::spawn(async move {
        if let Err(e) = recover_gap(&session, &recovery_config, &miss, &tx).await {
            warn!("gap recovery failed: {e}");
        }
    });
}

/// Periodically evict stale publisher entries from the gap detector.
fn maybe_evict(gd: &mut GapDetector, sample_count: u64, publisher_ttl: Option<std::time::Duration>) {
    if let Some(ttl) = publisher_ttl {
        if sample_count % 10_000 == 0 {
            let evicted = gd.evict_older_than(ttl);
            if evicted > 0 {
                debug!(shard_evicted = evicted, "evicted stale publisher entries");
            }
        }
    }
}

/// Subscribes to events with gap detection, automatic recovery, and dedup.
///
/// Background tasks handle heartbeat listening and gap recovery.
/// Events are delivered in-order through an internal channel.
pub struct EventSubscriber {
    /// Channel receiver for ordered event delivery.
    event_rx: flume::Receiver<RawEvent>,
    /// Configuration snapshot.
    config: EventBusConfig,
    /// Token to cancel background tasks.
    cancel: CancellationToken,
    /// Background task handles.
    _tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl EventSubscriber {
    /// Create a new subscriber, spawning background tasks for event processing,
    /// heartbeat listening, and gap recovery.
    pub async fn new(session: &Session, config: EventBusConfig) -> Result<Self> {
        let key_prefix = &config.key_prefix;

        // Declare Zenoh subscriber on the data key expression.
        let subscriber = session
            .declare_subscriber(format!("{key_prefix}/**"))
            .await?;

        let cancel = CancellationToken::new();
        let (event_tx, event_rx) = flume::unbounded::<RawEvent>();
        let mut tasks = Vec::new();

        let num_shards = config.num_processing_shards;
        let publisher_ttl = config.publisher_ttl;
        let recovery_enabled = matches!(
            config.recovery_mode,
            RecoveryMode::Heartbeat | RecoveryMode::Both
        );

        if num_shards == 1 {
            // Fast path: collapse dispatcher + shard worker into a single task,
            // eliminating one flume channel hop and one Tokio context switch per message.
            let cancel_clone = cancel.clone();
            let tx = event_tx.clone();
            let sess = session.clone();
            let rc = Arc::new(RecoveryConfig::from_bus_config(&config));
            let hb_sub = if config.heartbeat != HeartbeatMode::Disabled {
                Some(
                    session
                        .declare_subscriber(format!("{key_prefix}/_heartbeat/*"))
                        .await?,
                )
            } else {
                None
            };
            let has_hb = hb_sub.is_some();

            let handle = tokio::spawn(async move {
                let mut gd = GapDetector::new();
                let mut sample_count = 0u64;

                loop {
                    tokio::select! {
                        _ = cancel_clone.cancelled() => break,
                        sample_result = subscriber.recv_async() => {
                            match sample_result {
                                Ok(sample) => {
                                    if let Some((meta, key, payload)) = decode_sample(&sample) {
                                        let partition = crate::attachment::extract_partition(&key);
                                        let result = gd.on_sample(&meta.pub_id, partition, meta.seq);
                                        handle_sample_result(result, &meta, &key, &payload, &tx, &sess, &rc);
                                        sample_count += 1;
                                        maybe_evict(&mut gd, sample_count, publisher_ttl);
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                        hb_result = async { hb_sub.as_ref().unwrap().recv_async().await }, if has_hb => {
                            match hb_result {
                                Ok(sample) => {
                                    if let Some(beacon) = decode_heartbeat(&sample) {
                                        if recovery_enabled {
                                            let misses = gd.on_heartbeat(&beacon.pub_id, &beacon.partition_seqs);
                                            handle_heartbeat_gaps(misses, &sess, &rc, &tx);
                                        }
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                    }
                }
                debug!("combined dispatcher/shard task stopped");
            });
            tasks.push(handle);
        } else {
            // Multi-shard path: one GapDetector per shard, connected via channels.
            let mut shard_txs: Vec<flume::Sender<ShardMsg>> = Vec::with_capacity(num_shards);
            let mut shard_rxs: Vec<flume::Receiver<ShardMsg>> = Vec::with_capacity(num_shards);
            for _ in 0..num_shards {
                let (tx, rx) = flume::unbounded();
                shard_txs.push(tx);
                shard_rxs.push(rx);
            }

            // -- Spawn shard worker tasks --
            // Each shard owns its GapDetector exclusively — no locks needed.
            for shard_rx in shard_rxs {
                let tx = event_tx.clone();
                let sess = session.clone();
                let rc = Arc::new(RecoveryConfig::from_bus_config(&config));
                let cancel_clone = cancel.clone();

                let handle = tokio::spawn(async move {
                    let mut gd = GapDetector::new();
                    let mut sample_count = 0u64;

                    loop {
                        tokio::select! {
                            _ = cancel_clone.cancelled() => break,
                            msg = shard_rx.recv_async() => {
                                match msg {
                                    Ok(ShardMsg::Sample { meta, key, payload }) => {
                                        let partition = crate::attachment::extract_partition(&key);
                                        let result = gd.on_sample(&meta.pub_id, partition, meta.seq);
                                        handle_sample_result(result, &meta, &key, &payload, &tx, &sess, &rc);
                                        sample_count += 1;
                                        maybe_evict(&mut gd, sample_count, publisher_ttl);
                                    }
                                    Ok(ShardMsg::Heartbeat(beacon)) => {
                                        if recovery_enabled {
                                            let misses = gd.on_heartbeat(&beacon.pub_id, &beacon.partition_seqs);
                                            handle_heartbeat_gaps(misses, &sess, &rc, &tx);
                                        }
                                    }
                                    Err(_) => break,
                                }
                            }
                        }
                    }
                    debug!("shard worker stopped");
                });
                tasks.push(handle);
            }

            // -- Dispatcher task: Zenoh samples → shard channels --
            {
                let dispatchers_tx = shard_txs.clone();
                let cancel_clone = cancel.clone();

                let handle = tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            _ = cancel_clone.cancelled() => break,
                            sample_result = subscriber.recv_async() => {
                                match sample_result {
                                    Ok(sample) => {
                                        if let Some((meta, key, payload)) = decode_sample(&sample) {
                                            let shard_idx = shard_for(&meta.pub_id, num_shards);
                                            let _ = dispatchers_tx[shard_idx].send(ShardMsg::Sample {
                                                meta,
                                                key,
                                                payload,
                                            });
                                        }
                                    }
                                    Err(_) => break,
                                }
                            }
                        }
                    }
                    debug!("sample dispatcher stopped");
                });
                tasks.push(handle);
            }

            // -- Heartbeat dispatcher task (only when heartbeats are enabled) --
            if config.heartbeat != HeartbeatMode::Disabled {
                let hb_subscriber = session
                    .declare_subscriber(format!("{key_prefix}/_heartbeat/*"))
                    .await?;
                let hb_shard_txs = shard_txs.clone();
                let hb_cancel = cancel.clone();

                let handle = tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            _ = hb_cancel.cancelled() => break,
                            sample_result = hb_subscriber.recv_async() => {
                                match sample_result {
                                    Ok(sample) => {
                                        if let Some(beacon) = decode_heartbeat(&sample) {
                                            let shard_idx = shard_for(&beacon.pub_id, num_shards);
                                            let _ = hb_shard_txs[shard_idx].send(ShardMsg::Heartbeat(beacon));
                                        }
                                    }
                                    Err(_) => break,
                                }
                            }
                        }
                    }
                    debug!("heartbeat dispatcher stopped");
                });
                tasks.push(handle);
            }
        }

        Ok(Self {
            event_rx,
            config,
            cancel,
            _tasks: tasks,
        })
    }

    /// Receive the next event, deserializing the payload into type `T`
    /// using the configured codec.
    pub async fn recv<T: Serialize + DeserializeOwned>(&self) -> Result<Event<T>> {
        let raw = self
            .event_rx
            .recv_async()
            .await
            .map_err(|_| Error::ChannelClosed)?;
        raw.deserialize_with(self.config.codec)
    }

    /// Receive the next event as raw bytes (no payload deserialization).
    pub async fn recv_raw(&self) -> Result<RawEvent> {
        self.event_rx
            .recv_async()
            .await
            .map_err(|_| Error::ChannelClosed)
    }

    /// Configuration snapshot.
    pub fn config(&self) -> &EventBusConfig {
        &self.config
    }

    /// Gracefully shut down the subscriber: cancel all background tasks and
    /// await their completion. Consumes `self` so `Drop` does not run.
    pub async fn shutdown(mut self) {
        self.cancel.cancel();
        let tasks = std::mem::take(&mut self._tasks);
        for handle in tasks {
            let _ = handle.await;
        }
    }
}

impl Drop for EventSubscriber {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Construct and send a `RawEvent` through the delivery channel.
/// All metadata comes from the attachment — no payload decode required.
fn deliver_event(
    tx: &flume::Sender<RawEvent>,
    pub_id: PublisherId,
    seq: u64,
    key: &str,
    payload: &[u8],
    event_id: EventId,
    timestamp: chrono::DateTime<chrono::Utc>,
) {
    let raw = RawEvent {
        id: event_id,
        seq,
        publisher_id: pub_id,
        key_expr: key.to_string(),
        payload: payload.to_vec(),
        timestamp,
    };
    if tx.send(raw).is_err() {
        trace!("event channel closed, dropping event seq={seq}");
    }
}

/// Build the store recovery query selector.
///
/// Partition is encoded as a path segment (`{store_prefix}/{partition}`) so
/// Zenoh can route the query to the correct partition-specific queryable.
fn store_recovery_selector(
    store_prefix: &str,
    miss: &MissInfo,
) -> String {
    format!(
        "{}/{}?publisher_id={}&after_seq={}&before_seq={}",
        store_prefix,
        miss.partition,
        miss.source,
        miss.missed.start.saturating_sub(1),
        miss.missed.end,
    )
}

/// Query a Zenoh queryable and deliver any matching events, tracking recovery.
async fn query_and_deliver(
    session: &Session,
    selector: &str,
    tx: &flume::Sender<RawEvent>,
    tracker: &mut RecoveryTracker,
) {
    let replies = match session.get(selector).await {
        Ok(r) => r,
        Err(e) => {
            warn!("recovery query failed: {e}");
            return;
        }
    };

    while let Ok(reply) = replies.recv_async().await {
        match reply.result() {
            Ok(sample) => {
                // Decode metadata first — defer payload copy until we confirm
                // the seq is within range and not already recovered.
                match sample.attachment().and_then(|a| decode_metadata(a).ok()) {
                    Some(meta) if tracker.expected.contains(&meta.seq) => {
                        if tracker.recovered.contains(&meta.seq) {
                            trace!(seq = meta.seq, "already recovered, skipping");
                            continue;
                        }
                        let key = sample.key_expr().as_str();
                        let payload_bytes = sample.payload().to_bytes().to_vec();
                        deliver_event(
                            tx,
                            meta.pub_id,
                            meta.seq,
                            key,
                            &payload_bytes,
                            meta.event_id,
                            meta.timestamp,
                        );
                        tracker.record(meta.seq);
                    }
                    Some(_) => {
                        // seq outside the gap range — ignore
                    }
                    None => {
                        warn!("recovery reply without valid attachment, skipping");
                    }
                }
            }
            Err(err) => {
                warn!(
                    "recovery reply error: {}",
                    err.payload().try_to_string().unwrap_or_default()
                );
            }
        }
    }
}

/// Attempt to recover missed events using tiered sources.
///
/// Order: EventStore → Publisher Cache → Retry Store (with backoff).
/// Recovered events are delivered through `tx` as they arrive.
async fn recover_gap(
    session: &Session,
    config: &RecoveryConfig,
    miss: &MissInfo,
    tx: &flume::Sender<RawEvent>,
) -> Result<()> {
    let mut tracker = RecoveryTracker::new(miss.missed.clone());

    debug!(
        publisher = %miss.source,
        missed = ?miss.missed,
        "recovering gap via tiered recovery"
    );

    // ── Step 1: Query EventStore (if available) ──
    if let Some(store_prefix) = &config.store_key_prefix {
        tokio::time::sleep(config.recovery_delay).await;

        let selector = store_recovery_selector(store_prefix, miss);
        query_and_deliver(session, &selector, tx, &mut tracker).await;
        if tracker.is_complete() {
            debug!(publisher = %miss.source, "gap recovery complete (store)");
            return Ok(());
        }
    }

    // ── Step 2: Query Publisher Cache ──
    let cache_key = format!(
        "{}/_cache/{}?after_seq={}",
        config.key_prefix, miss.source, miss.missed.start
    );
    query_and_deliver(session, &cache_key, tx, &mut tracker).await;
    if tracker.is_complete() {
        debug!(publisher = %miss.source, "gap recovery complete (cache)");
        return Ok(());
    }

    // ── Step 3: Retry Store with exponential backoff ──
    if let Some(store_prefix) = &config.store_key_prefix {
        for attempt in 1..config.max_recovery_attempts {
            let backoff = config.recovery_delay * 2u32.pow(attempt);
            tokio::time::sleep(backoff).await;

            let selector = store_recovery_selector(store_prefix, miss);
            query_and_deliver(session, &selector, tx, &mut tracker).await;
            if tracker.is_complete() {
                debug!(publisher = %miss.source, attempt, "gap recovery complete (store retry)");
                return Ok(());
            }
        }
    }

    // ── Step 4: Irrecoverable ──
    let remaining = tracker.remaining();
    warn!(
        publisher = %miss.source,
        partition = miss.partition,
        remaining = ?remaining,
        "irrecoverable gap — {} events lost",
        remaining.len()
    );
    Err(Error::GapRecoveryFailed {
        publisher_id: miss.source,
        missed: miss.missed.clone(),
    })
}
