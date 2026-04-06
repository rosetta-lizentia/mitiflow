//! Event publisher with sequencing, caching, and heartbeat.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use serde::Serialize;
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};
use zenoh::Session;
use zenoh::bytes::ZBytes;
use zenoh::handlers::FifoChannelHandler;
use zenoh::pubsub::Publisher;
use zenoh::qos::CongestionControl;
use zenoh::sample::Sample;

use crate::attachment::{NO_URGENCY, encode_metadata};
use crate::config::{EventBusConfig, HeartbeatMode};
#[cfg(feature = "store")]
use crate::error::Error;
use crate::error::Result;
use crate::event::Event;
use crate::types::PublisherId;

/// A sample cached in the publisher's recovery buffer.
///
/// Uses [`ZBytes`] for zero-copy payload sharing: the same ref-counted buffer
/// is shared between the cache and the Zenoh `put()` call. Cloning is an
/// atomic increment — no memcpy.
#[derive(Clone)]
pub struct CachedSample {
    pub seq: u64,
    pub key_expr: String,
    /// Encoded payload as Zenoh's native ref-counted bytes (zero-copy clone).
    pub payload: ZBytes,
    /// Pre-encoded metadata attachment (zero-copy clone on cache reply).
    pub attachment: ZBytes,
}

/// Heartbeat beacon published periodically so subscribers can detect stale connections.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HeartbeatBeacon {
    pub pub_id: PublisherId,
    /// Per-partition sequence counters (each value is the highest assigned seq).
    pub partition_seqs: HashMap<u32, u64>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Publishes events with monotonic sequencing, an in-memory recovery cache,
/// and periodic heartbeat beacons.
///
/// Background tasks are spawned on construction and cancelled on drop.
pub struct EventPublisher {
    /// Zenoh publisher for the data key expression.
    #[allow(dead_code)]
    publisher: Publisher<'static>,
    /// Zenoh session (cloned Arc) used for declaring queryables and putting heartbeats.
    session: Session,
    /// Bounded in-memory cache for recovery queries.
    cache: Arc<RwLock<VecDeque<CachedSample>>>,
    /// Per-partition monotonic sequence counters.
    partition_seqs: Arc<scc::HashMap<u32, u64>>,
    /// Round-robin counter for default partition assignment.
    round_robin: AtomicU64,
    /// Unique identity for this publisher.
    publisher_id: PublisherId,
    /// Configuration snapshot.
    config: EventBusConfig,
    /// Token to cancel background tasks on drop.
    cancel: CancellationToken,
    /// Handles to spawned background tasks (for join on graceful shutdown).
    _tasks: Vec<tokio::task::JoinHandle<()>>,
    /// Broadcast sender for watermark updates.
    /// Each `publish_durable()` call subscribes to receive all watermarks.
    #[cfg(feature = "store")]
    watermark_tx: Option<tokio::sync::broadcast::Sender<crate::store::CommitWatermark>>,
}

/// Publish heartbeat beacons at a fixed interval until cancelled.
async fn run_heartbeat_task(
    session: Session,
    key: String,
    pub_id: PublisherId,
    partition_seqs: Arc<scc::HashMap<u32, u64>>,
    cancel: CancellationToken,
    mode: HeartbeatMode,
) {
    let interval_dur = match &mode {
        HeartbeatMode::Periodic(d) | HeartbeatMode::Sporadic(d) => *d,
        HeartbeatMode::Disabled => unreachable!(),
    };
    let mut interval = tokio::time::interval(interval_dur);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = interval.tick() => {
                let mut seqs: HashMap<u32, u64> = HashMap::new();
                partition_seqs.iter_sync(|p, s| { seqs.insert(*p, s.saturating_sub(1)); true });
                let beacon = HeartbeatBeacon {
                    pub_id,
                    partition_seqs: seqs,
                    timestamp: chrono::Utc::now(),
                };
                if let Ok(bytes) = serde_json::to_vec(&beacon)
                    && let Err(e) = session.put(&key, bytes)
                        .congestion_control(CongestionControl::Drop)
                        .await
                    {
                        warn!("heartbeat publish failed: {e}");
                    }
            }
        }
    }
    debug!("heartbeat task stopped for {pub_id}");
}

/// Serve cache recovery queries until cancelled.
async fn run_cache_queryable_task(
    cache: Arc<RwLock<VecDeque<CachedSample>>>,
    queryable: zenoh::query::Queryable<FifoChannelHandler<zenoh::query::Query>>,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            query_result = queryable.recv_async() => {
                match query_result {
                    Ok(query) => {
                        let after_seq: u64 = query
                            .parameters()
                            .get("after_seq")
                            .and_then(|v| v.parse().ok())
                            .unwrap_or(0);

                        let before_seq: u64 = query
                            .parameters()
                            .get("before_seq")
                            .and_then(|v| v.parse().ok())
                            .unwrap_or(u64::MAX);

                        // Snapshot under std::sync::RwLock — non-blocking, fast.
                        // Collect matching samples first, then reply outside the lock.
                        let snapshot: Vec<CachedSample> = {
                            let cache_read = cache.read().unwrap_or_else(|e| e.into_inner());
                            cache_read.iter()
                                .filter(|s| s.seq >= after_seq && s.seq < before_seq)
                                .cloned()
                                .collect()
                        };

                        for sample in &snapshot {
                            if let Err(e) = query
                                .reply(&sample.key_expr, sample.payload.clone())
                                .attachment(sample.attachment.clone())
                                .await
                            {
                                trace!("cache query reply failed: {e}");
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    }
    debug!("cache queryable task stopped");
}

/// Serve a registered schema to peers via an ephemeral queryable until cancelled.
///
/// This is spawned only when the publisher registered a new schema
/// (`RegisterOrValidate` mode, first publisher). Peers (or other
/// publishers/subscribers) can `session.get("{prefix}/_schema")` and
/// this task will reply with the schema bytes.
async fn run_schema_queryable_task(
    queryable: zenoh::query::Queryable<FifoChannelHandler<zenoh::query::Query>>,
    schema_bytes: Vec<u8>,
    cancel: CancellationToken,
) {
    let key = queryable.key_expr().to_string();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            query_result = queryable.recv_async() => {
                match query_result {
                    Ok(query) => {
                        if let Err(e) = query.reply(&key, schema_bytes.clone()).await {
                            trace!("schema query reply failed: {e}");
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    }
    debug!("schema queryable task stopped");
}

/// Forward watermark updates from Zenoh to a broadcast channel until cancelled.
#[cfg(feature = "store")]
async fn run_watermark_listener_task(
    subscriber: zenoh::pubsub::Subscriber<FifoChannelHandler<Sample>>,
    tx: tokio::sync::broadcast::Sender<crate::store::CommitWatermark>,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            sample_result = subscriber.recv_async() => {
                match sample_result {
                    Ok(sample) => {
                        let bytes = sample.payload().to_bytes();
                        if let Ok(wm) = serde_json::from_slice::<crate::store::CommitWatermark>(&bytes) {
                            let _ = tx.send(wm);
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    }
    debug!("watermark listener task stopped");
}

impl EventPublisher {
    /// Create a new publisher, spawning heartbeat and cache-queryable tasks.
    pub async fn new(session: &Session, mut config: EventBusConfig) -> Result<Self> {
        // Resolve schema mode (validate / auto-config / register) before
        // any Zenoh declarations so mismatches fail fast.
        let registered_schema = crate::schema::resolve_schema(session, &mut config).await?;

        let publisher_id = PublisherId::new();
        let key_prefix = &config.key_prefix;

        let publisher = session
            .declare_publisher(format!("{key_prefix}/**"))
            .congestion_control(config.congestion_control)
            .await?;

        let _liveliness = session
            .liveliness()
            .declare_token(format!("{key_prefix}/_publishers/{publisher_id}"))
            .await?;

        let cache: Arc<RwLock<VecDeque<CachedSample>>> =
            Arc::new(RwLock::new(VecDeque::with_capacity(config.cache_size)));
        let partition_seqs: Arc<scc::HashMap<u32, u64>> = Arc::new(scc::HashMap::new());
        let cancel = CancellationToken::new();
        let mut tasks = Vec::new();

        if config.heartbeat != HeartbeatMode::Disabled {
            tasks.push(tokio::spawn(run_heartbeat_task(
                session.clone(),
                format!("{key_prefix}/_heartbeat/{publisher_id}"),
                publisher_id,
                Arc::clone(&partition_seqs),
                cancel.clone(),
                config.heartbeat.clone(),
            )));
        }

        {
            let queryable = session
                .declare_queryable(format!("{key_prefix}/_cache/{publisher_id}"))
                .await?;
            tasks.push(tokio::spawn(run_cache_queryable_task(
                Arc::clone(&cache),
                queryable,
                cancel.clone(),
            )));
        }

        // If this publisher registered a new schema (RegisterOrValidate, first
        // publisher), serve it via an ephemeral queryable so peers can fetch it
        // before storage agents persist the schema.
        if let Some(schema) = registered_schema {
            let schema_key = crate::schema::TopicSchema::schema_key(key_prefix);
            let schema_queryable = session.declare_queryable(&schema_key).await?;
            let schema_bytes = schema.to_bytes()?;
            tasks.push(tokio::spawn(run_schema_queryable_task(
                schema_queryable,
                schema_bytes,
                cancel.clone(),
            )));
        }

        #[cfg(feature = "store")]
        let watermark_tx = {
            let wm_subscriber = session
                .declare_subscriber(config.resolved_watermark_key())
                .await?;
            let (tx, _) = tokio::sync::broadcast::channel(64);
            tasks.push(tokio::spawn(run_watermark_listener_task(
                wm_subscriber,
                tx.clone(),
                cancel.clone(),
            )));
            Some(tx)
        };

        Ok(Self {
            publisher,
            session: session.clone(),
            cache,
            partition_seqs,
            round_robin: AtomicU64::new(0),
            publisher_id,
            config,
            cancel,
            _tasks: tasks,
            #[cfg(feature = "store")]
            watermark_tx,
        })
    }

    /// The unique ID of this publisher instance.
    pub fn publisher_id(&self) -> &PublisherId {
        &self.publisher_id
    }

    /// Publish raw bytes directly, bypassing codec encoding.
    ///
    /// Use this when the payload is already serialized (e.g. a benchmark
    /// buffer, a pre-encoded protobuf blob, or any `Vec<u8>` you control).
    /// All sequencing, gap detection, and recovery guarantees still apply —
    /// the attachment is written as normal.
    ///
    /// On the subscriber side, use [`EventSubscriber::recv_raw`] to receive
    /// the bytes without a deserialization step.
    pub async fn publish_bytes(&self, bytes: Vec<u8>) -> Result<u64> {
        let partition = self.assign_partition();
        let seq = self.next_seq_for(partition);
        let key = format!("{}/p/{}/{}", self.config.key_prefix, partition, seq);
        let event_id = crate::types::EventId::new();
        let timestamp = chrono::Utc::now();
        self.put_payload(&key, bytes, seq, event_id, timestamp, NO_URGENCY)
            .await?;
        Ok(seq)
    }

    /// Publish raw bytes to an explicit key expression, bypassing codec encoding.
    ///
    /// The partition is extracted from the key expression (e.g., `prefix/p/3/data`).
    /// If the key has no `/p/` segment, partition 0 is used.
    pub async fn publish_bytes_to(&self, key: &str, bytes: Vec<u8>) -> Result<u64> {
        let partition = crate::attachment::extract_partition(key);
        let seq = self.next_seq_for(partition);
        let event_id = crate::types::EventId::new();
        let timestamp = chrono::Utc::now();
        self.put_payload(key, bytes, seq, event_id, timestamp, NO_URGENCY)
            .await?;
        Ok(seq)
    }

    /// Publish raw bytes and wait for watermark confirmation from the Event Store.
    ///
    /// Raw-bytes variant of [`publish_durable`] — skips codec encoding.
    #[cfg(feature = "store")]
    pub async fn publish_bytes_durable(&self, bytes: Vec<u8>) -> Result<u64> {
        let urgency_ms = self.urgency_ms();
        let partition = self.assign_partition();
        let seq = self.next_seq_for(partition);
        let key = format!("{}/p/{}/{}", self.config.key_prefix, partition, seq);
        let event_id = crate::types::EventId::new();
        let timestamp = chrono::Utc::now();
        self.put_payload(&key, bytes, seq, event_id, timestamp, urgency_ms)
            .await?;
        self.wait_for_watermark(partition, seq).await
    }

    /// Publish raw bytes to an explicit key expression and wait for watermark
    /// confirmation from the Event Store.
    ///
    /// The partition is extracted from the key expression (e.g., `prefix/p/3/data`).
    /// If the key has no `/p/` segment, partition 0 is used.
    #[cfg(feature = "store")]
    pub async fn publish_bytes_durable_to(&self, key: &str, bytes: Vec<u8>) -> Result<u64> {
        let urgency_ms = self.urgency_ms();
        let partition = crate::attachment::extract_partition(key);
        let seq = self.next_seq_for(partition);
        let event_id = crate::types::EventId::new();
        let timestamp = chrono::Utc::now();
        self.put_payload(key, bytes, seq, event_id, timestamp, urgency_ms)
            .await?;
        self.wait_for_watermark(partition, seq).await
    }

    /// Publish a keyed event. Partition is determined by `hash(key) % num_partitions`.
    ///
    /// The key is embedded in the Zenoh key expression as
    /// `{prefix}/p/{partition}/k/{key}/{seq}`, enabling Zenoh-native key
    /// filtering without any wire overhead.
    pub async fn publish_keyed<T: Serialize>(&self, key: &str, event: &Event<T>) -> Result<u64> {
        crate::attachment::validate_key(key)?;
        let partition = crate::partition::hash_ring::partition_for(key, self.config.num_partitions);
        let seq = self.next_seq_for(partition);
        let key_expr = format!(
            "{}/p/{}/k/{}/{}",
            self.config.key_prefix, partition, key, seq
        );
        self.publish_inner(&key_expr, event, seq, NO_URGENCY)
            .await?;
        Ok(seq)
    }

    /// Publish pre-serialised bytes with a key.
    ///
    /// Raw-bytes variant of [`publish_keyed`] — skips codec encoding.
    pub async fn publish_bytes_keyed(&self, key: &str, bytes: Vec<u8>) -> Result<u64> {
        crate::attachment::validate_key(key)?;
        let partition = crate::partition::hash_ring::partition_for(key, self.config.num_partitions);
        let seq = self.next_seq_for(partition);
        let key_expr = format!(
            "{}/p/{}/k/{}/{}",
            self.config.key_prefix, partition, key, seq
        );
        let event_id = crate::types::EventId::new();
        let timestamp = chrono::Utc::now();
        self.put_payload(&key_expr, bytes, seq, event_id, timestamp, NO_URGENCY)
            .await?;
        Ok(seq)
    }

    /// Publish a keyed event and wait for watermark confirmation.
    ///
    /// Durable variant of [`publish_keyed`].
    #[cfg(feature = "store")]
    pub async fn publish_keyed_durable<T: Serialize>(
        &self,
        key: &str,
        event: &Event<T>,
    ) -> Result<u64> {
        crate::attachment::validate_key(key)?;
        let urgency_ms = self.urgency_ms();
        let partition = crate::partition::hash_ring::partition_for(key, self.config.num_partitions);
        let seq = self.next_seq_for(partition);
        let key_expr = format!(
            "{}/p/{}/k/{}/{}",
            self.config.key_prefix, partition, key, seq
        );
        self.publish_inner(&key_expr, event, seq, urgency_ms)
            .await?;
        self.wait_for_watermark(partition, seq).await
    }

    /// Publish pre-serialised bytes with a key and wait for watermark confirmation.
    ///
    /// Raw-bytes durable variant of [`publish_keyed`].
    #[cfg(feature = "store")]
    pub async fn publish_bytes_keyed_durable(&self, key: &str, bytes: Vec<u8>) -> Result<u64> {
        crate::attachment::validate_key(key)?;
        let urgency_ms = self.urgency_ms();
        let partition = crate::partition::hash_ring::partition_for(key, self.config.num_partitions);
        let seq = self.next_seq_for(partition);
        let key_expr = format!(
            "{}/p/{}/k/{}/{}",
            self.config.key_prefix, partition, key, seq
        );
        let event_id = crate::types::EventId::new();
        let timestamp = chrono::Utc::now();
        self.put_payload(&key_expr, bytes, seq, event_id, timestamp, urgency_ms)
            .await?;
        self.wait_for_watermark(partition, seq).await
    }

    /// Publish an event on the configured key prefix (fast path).
    ///
    /// Assigns a monotonic sequence number, attaches metadata, inserts into
    /// the recovery cache, and publishes via Zenoh. Returns the assigned
    /// sequence number.
    pub async fn publish<T: Serialize>(&self, event: &Event<T>) -> Result<u64> {
        let partition = self.assign_partition();
        let seq = self.next_seq_for(partition);
        let key = format!("{}/p/{}/{}", self.config.key_prefix, partition, seq);
        self.publish_inner(&key, event, seq, NO_URGENCY).await?;
        Ok(seq)
    }

    /// Publish an event to an explicit key expression (e.g., a partition key).
    ///
    /// The partition is extracted from the key expression (e.g., `prefix/p/3/data`).
    /// If the key has no `/p/` segment, partition 0 is used.
    pub async fn publish_to<T: Serialize>(&self, key: &str, event: &Event<T>) -> Result<u64> {
        let partition = crate::attachment::extract_partition(key);
        let seq = self.next_seq_for(partition);
        self.publish_inner(key, event, seq, NO_URGENCY).await?;
        Ok(seq)
    }

    /// Publish an event and wait for watermark confirmation from the Event Store.
    ///
    /// This is the durable publish path: after putting the event onto Zenoh,
    /// the method blocks until the store's [`CommitWatermark`] confirms the
    /// sequence as durably stored, or until the configured `durable_timeout`
    /// expires (returning [`Error::DurabilityTimeout`]).
    ///
    /// Requires an [`EventStore`] to be running and publishing watermarks.
    ///
    /// [`CommitWatermark`]: crate::store::CommitWatermark
    /// [`EventStore`]: crate::store::EventStore
    #[cfg(feature = "store")]
    pub async fn publish_durable<T: Serialize>(&self, event: &Event<T>) -> Result<u64> {
        let urgency_ms = self.urgency_ms();
        let partition = self.assign_partition();
        let seq = self.next_seq_for(partition);
        let key = format!("{}/p/{}/{}", self.config.key_prefix, partition, seq);
        self.publish_inner(&key, event, seq, urgency_ms).await?;
        self.wait_for_watermark(partition, seq).await
    }

    /// Wait until the Event Store's watermark covers `seq` for this publisher on the given partition.
    #[cfg(feature = "store")]
    async fn wait_for_watermark(&self, partition: u32, seq: u64) -> Result<u64> {
        let watermark_tx = self
            .watermark_tx
            .as_ref()
            .ok_or_else(|| Error::InvalidConfig("watermark subscriber not initialized".into()))?;
        // Subscribe before waiting — each caller gets its own receiver.
        let mut rx = watermark_tx.subscribe();
        let timeout_dur = self.config.durable_timeout;
        let my_id = self.publisher_id;

        let result = tokio::time::timeout(timeout_dur, async {
            loop {
                match rx.recv().await {
                    Ok(wm) => {
                        if wm.partition == partition && wm.is_durable(&my_id, seq) {
                            return Ok(seq);
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        return Err(Error::ChannelClosed);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        // Missed some watermarks; continue waiting for the next one.
                        continue;
                    }
                }
            }
        })
        .await;

        match result {
            Ok(inner) => inner,
            Err(_) => Err(Error::DurabilityTimeout { seq }),
        }
    }

    /// Internal publish: serialize payload only, attach full metadata, cache, and send.
    async fn publish_inner<T: Serialize>(
        &self,
        key: &str,
        event: &Event<T>,
        seq: u64,
        urgency_ms: u16,
    ) -> Result<()> {
        // Encode only the user payload — metadata travels in the attachment.
        let payload = self.config.codec.encode(&event.payload)?;
        self.put_payload(key, payload, seq, event.id, event.timestamp, urgency_ms)
            .await
    }

    /// Shared inner: attach metadata, cache, and put to Zenoh.
    /// Called by both the typed `publish_inner` and the raw `publish_bytes` paths.
    async fn put_payload(
        &self,
        key: &str,
        payload: Vec<u8>,
        seq: u64,
        event_id: crate::types::EventId,
        timestamp: chrono::DateTime<chrono::Utc>,
        urgency_ms: u16,
    ) -> Result<()> {
        let attachment =
            encode_metadata(&self.publisher_id, seq, &event_id, &timestamp, urgency_ms);
        let zbytes_payload = ZBytes::from(payload); // takes ownership, no copy

        // Insert into bounded cache (evict oldest if full).
        // Skip entirely when cache_size == 0 to avoid lock acquisition.
        // ZBytes::clone() is an atomic ref-count increment — no memcpy.
        if self.config.cache_size > 0 {
            let mut cache = self.cache.write().unwrap_or_else(|e| e.into_inner());
            if cache.len() >= self.config.cache_size {
                cache.pop_front();
            }
            cache.push_back(CachedSample {
                seq,
                key_expr: key.to_string(),
                payload: zbytes_payload.clone(),
                attachment: attachment.clone(),
            });
        }

        self.session
            .put(key, zbytes_payload)
            .attachment(attachment)
            .congestion_control(self.config.congestion_control)
            .await?;

        trace!(seq, publisher_id = %self.publisher_id, key, "published event");
        Ok(())
    }

    /// Compute urgency_ms from the configured `durable_urgency` duration.
    /// Clamped to `NO_URGENCY - 1` (65 534 ms) because `NO_URGENCY` (0xFFFF)
    /// is the sentinel for "no urgency".
    #[cfg(feature = "store")]
    fn urgency_ms(&self) -> u16 {
        let ms = self.config.durable_urgency.as_millis();
        ms.min((NO_URGENCY - 1) as u128) as u16
    }

    /// Access the Zenoh session (for advanced use cases like durable publish).
    pub fn session(&self) -> &Session {
        &self.session
    }

    /// Current sequence number for partition 0 (next to be assigned).
    pub fn current_seq(&self) -> u64 {
        self.partition_seqs.read_sync(&0, |_, v| *v).unwrap_or(0)
    }

    /// Current sequence number for a specific partition (next to be assigned).
    pub fn current_seq_for(&self, partition: u32) -> u64 {
        self.partition_seqs
            .read_sync(&partition, |_, v| *v)
            .unwrap_or(0)
    }

    /// Assign the next partition using round-robin across `num_partitions`.
    fn assign_partition(&self) -> u32 {
        let n = self.config.num_partitions.max(1);
        (self.round_robin.fetch_add(1, Ordering::Relaxed) % n as u64) as u32
    }

    /// Allocate the next sequence number for the given partition.
    fn next_seq_for(&self, partition: u32) -> u64 {
        match self.partition_seqs.entry_sync(partition) {
            scc::hash_map::Entry::Occupied(mut o) => {
                let seq = *o.get();
                *o.get_mut() += 1;
                seq
            }
            scc::hash_map::Entry::Vacant(v) => {
                v.insert_entry(1);
                0
            }
        }
    }

    /// Configuration snapshot.
    pub fn config(&self) -> &EventBusConfig {
        &self.config
    }

    /// Gracefully shut down the publisher: cancel all background tasks and
    /// await their completion. Consumes `self` so `Drop` does not run.
    pub async fn shutdown(mut self) {
        self.cancel.cancel();
        let tasks = std::mem::take(&mut self._tasks);
        for handle in tasks {
            let _ = handle.await;
        }
    }
}

impl Drop for EventPublisher {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
