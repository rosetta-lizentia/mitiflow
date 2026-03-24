//! Gap recovery: tiered store → cache → backoff retry.
//!
//! Extracted from the subscriber module to keep `mod.rs` focused on
//! the public API and orchestration.

use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, trace, warn};
use zenoh::Session;

use crate::attachment::decode_metadata;
use crate::config::EventBusConfig;
use crate::error::{Error, Result};
use crate::event::RawEvent;
use crate::types::{EventId, PublisherId};

use super::gap_detector::MissInfo;

// ---------------------------------------------------------------------------
// RecoveryConfig
// ---------------------------------------------------------------------------

/// Internal recovery configuration extracted from [`EventBusConfig`].
pub(crate) struct RecoveryConfig {
    pub key_prefix: String,
    pub store_key_prefix: Option<String>,
    pub recovery_delay: Duration,
    pub max_recovery_attempts: u32,
}

impl RecoveryConfig {
    pub fn from_bus_config(config: &EventBusConfig) -> Self {
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

// ---------------------------------------------------------------------------
// RecoveryTracker
// ---------------------------------------------------------------------------

/// Tracks which sequences in a gap have been recovered.
pub(crate) struct RecoveryTracker {
    pub expected: Range<u64>,
    pub recovered: HashSet<u64>,
}

impl RecoveryTracker {
    pub fn new(missed: Range<u64>) -> Self {
        let cap = (missed.end - missed.start) as usize;
        Self {
            expected: missed,
            recovered: HashSet::with_capacity(cap),
        }
    }

    pub fn record(&mut self, seq: u64) {
        if self.expected.contains(&seq) {
            self.recovered.insert(seq);
        }
    }

    pub fn is_complete(&self) -> bool {
        self.recovered.len() as u64 == (self.expected.end - self.expected.start)
    }

    pub fn remaining(&self) -> Vec<u64> {
        self.expected
            .clone()
            .filter(|s| !self.recovered.contains(s))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Event delivery helper
// ---------------------------------------------------------------------------

/// Construct and send a `RawEvent` through the delivery channel.
///
/// Uses `try_send` to avoid blocking the calling async task when the
/// channel is full.  A blocking `send()` would stall the pipeline,
/// prevent it from draining the Zenoh subscriber, and eventually
/// overflow the Zenoh transport buffer — killing the publisher's
/// connection.  Dropped events are recovered via gap detection or
/// store-based catch-up.
pub(crate) fn deliver_event(
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
    match tx.try_send(raw) {
        Ok(()) => {}
        Err(flume::TrySendError::Full(_)) => {
            trace!(seq, "event channel full, dropping event (will be recovered)");
        }
        Err(flume::TrySendError::Disconnected(_)) => {
            trace!(seq, "event channel closed, dropping event");
        }
    }
}

// ---------------------------------------------------------------------------
// Recovery functions
// ---------------------------------------------------------------------------

/// Spawn a background task to recover missed events using tiered recovery.
pub(crate) fn spawn_recovery(
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

/// Build the store recovery query selector.
fn store_recovery_selector(store_prefix: &str, miss: &MissInfo) -> String {
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
    let replies = match session
        .get(selector)
        .accept_replies(zenoh::query::ReplyKeyExpr::Any)
        .consolidation(zenoh::query::ConsolidationMode::None)
        .timeout(Duration::from_secs(5))
        .await
    {
        Ok(r) => r,
        Err(e) => {
            warn!("recovery query failed: {e}");
            return;
        }
    };

    while let Ok(reply) = replies.recv_async().await {
        match reply.result() {
            Ok(sample) => {
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
        "{}/_cache/{}?after_seq={}&before_seq={}",
        config.key_prefix, miss.source, miss.missed.start, miss.missed.end,
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
