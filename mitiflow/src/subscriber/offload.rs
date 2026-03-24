//! Slow-consumer offload: automatic pub/sub → store-based query demotion.
//!
//! When a subscriber falls behind the live stream, it transparently switches
//! from real-time pub/sub to batched store queries, then resumes live streaming
//! once caught up.  During the catch-up phase, Zenoh subscribers are **dropped**
//! (not just paused via a flag) so that transport resources are released and
//! the publisher is never back-pressured.
//!
//! The [`OffloadEvent`] channel provides observability into state transitions
//! without changing the `recv()` API.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tracing::{debug, info, trace, warn};
use zenoh::Session;

use crate::attachment::{decode_metadata, extract_partition};
use crate::config::OffloadConfig;
use crate::error::{Error, Result};
use crate::event::RawEvent;
use crate::types::PublisherId;

use super::forwarder::ForwarderControl;
use super::gap_detector::SequenceTracker;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Consumer state in the offload state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerState {
    /// Normal operation — events arrive via Zenoh pub/sub.
    Live,
    /// Transitional — draining buffered events before switching to store queries.
    Draining,
    /// Reading events from the Event Store in batches.
    CatchingUp,
}

/// Observable lifecycle events emitted during offload transitions.
///
/// Subscribe via [`EventSubscriber::offload_events()`] to monitor the state
/// machine without changing application logic.
#[derive(Debug, Clone)]
pub enum OffloadEvent {
    /// Lag detection triggered — the consumer is falling behind.
    LagDetected {
        channel_fullness: f64,
        max_seq_lag: u64,
    },
    /// The consumer is draining buffered events before switching to store queries.
    Draining { buffered: usize },
    /// The consumer is reading events from the store.
    CatchingUp { behind_by: u64 },
    /// The consumer has caught up with the store and is resuming live pub/sub.
    CaughtUp {
        elapsed: Duration,
        events_replayed: u64,
    },
    /// The consumer has resumed live pub/sub after catching up.
    Resumed,
    /// The offload failed — the consumer reverts to live pub/sub (may have gaps).
    OffloadFailed { error: String },
}

// ---------------------------------------------------------------------------
// LagDetector — pure logic, no Zenoh dependency
// ---------------------------------------------------------------------------

/// Composite lag detector combining channel fullness and heartbeat-reported
/// sequence lag, with debounce to prevent flapping on transient bursts.
pub struct LagDetector {
    /// Current number of pending items in the delivery channel.
    channel_len: usize,
    /// Channel capacity (used to compute fullness ratio).
    channel_capacity: usize,
    /// Per-(publisher, partition) sequence lag reported by heartbeats.
    heartbeat_lag: HashMap<(PublisherId, u32), u64>,
    /// Configuration thresholds.
    config: OffloadConfig,
    /// Debounce: first instant the lag exceeded the threshold (if any).
    sustained_since: Option<Instant>,
}

impl LagDetector {
    /// Create a new lag detector with the given channel capacity and config.
    pub fn new(channel_capacity: usize, config: OffloadConfig) -> Self {
        Self {
            channel_len: 0,
            channel_capacity,
            heartbeat_lag: HashMap::new(),
            config,
            sustained_since: None,
        }
    }

    /// Update the current channel length (called periodically by background task).
    pub fn update_channel_len(&mut self, len: usize) {
        self.channel_len = len;
    }

    /// Update the heartbeat-reported sequence lag for a (publisher, partition).
    pub fn update_heartbeat_lag(&mut self, pub_id: PublisherId, partition: u32, lag: u64) {
        self.heartbeat_lag.insert((pub_id, partition), lag);
    }

    /// Current channel fullness ratio (0.0–1.0).
    pub fn channel_fullness(&self) -> f64 {
        if self.channel_capacity == 0 {
            return 0.0;
        }
        self.channel_len as f64 / self.channel_capacity as f64
    }

    /// Maximum heartbeat-reported sequence lag across all tracked publishers.
    pub fn max_seq_lag(&self) -> u64 {
        self.heartbeat_lag.values().copied().max().unwrap_or(0)
    }

    /// Check whether the consumer is lagging, with debounce.
    ///
    /// Returns `true` only if the lag has been sustained for at least
    /// `debounce_window`.
    pub fn is_lagging(&mut self) -> bool {
        if !self.config.enabled {
            return false;
        }

        let channel_ratio = self.channel_fullness();
        let max_lag = self.max_seq_lag();

        let over_threshold = channel_ratio > self.config.channel_fullness_threshold
            || max_lag > self.config.seq_lag_threshold;

        match (over_threshold, self.sustained_since) {
            (true, None) => {
                self.sustained_since = Some(Instant::now());
                false
            }
            (true, Some(since)) => since.elapsed() > self.config.debounce_window,
            (false, _) => {
                self.sustained_since = None;
                false
            }
        }
    }

    /// Clear the debounce state (used after an offload cycle completes).
    pub fn reset(&mut self) {
        self.sustained_since = None;
        self.heartbeat_lag.clear();
    }
}

// ---------------------------------------------------------------------------
// CatchUpReader — batched store queries
// ---------------------------------------------------------------------------

/// Reads events from the Event Store in batches, advancing a per-(publisher,
/// partition) cursor map. Adapts batch size based on query latency.
pub struct CatchUpReader {
    session: Session,
    store_key_prefix: String,
    /// Switchover cursor: per-(publisher, partition) → last delivered seq.
    cursors: HashMap<(PublisherId, u32), u64>,
    /// Current batch size (adaptive).
    batch_size: usize,
    config: OffloadConfig,
    /// Partitions to query (empty = query all via wildcard).
    partitions: Vec<u32>,
}

impl CatchUpReader {
    /// Create a new reader with the given switchover cursors.
    pub fn new(
        session: Session,
        store_key_prefix: String,
        cursors: HashMap<(PublisherId, u32), u64>,
        config: OffloadConfig,
        partitions: Vec<u32>,
    ) -> Self {
        let batch_size = config.catch_up_batch_size;
        Self {
            session,
            store_key_prefix,
            cursors,
            batch_size,
            config,
            partitions,
        }
    }

    /// Query the store for the next batch of events after the current cursors.
    ///
    /// Issues one query per `(publisher_id, partition)` cursor, passing the
    /// `publisher_id` filter so the store can do an efficient range scan
    /// on `[pub_id][seq]` instead of a full keyspace scan.
    ///
    /// Returns an empty vec when fully caught up.
    pub async fn next_batch(&mut self) -> Result<Vec<RawEvent>> {
        let mut all_events = Vec::new();

        if self.cursors.is_empty() {
            // Nothing tracked — try a single query per explicit partition
            // to bootstrap (e.g. if cursors haven't been populated yet).
            let partitions: Vec<u32> = if self.partitions.is_empty() {
                return Ok(all_events);
            } else {
                self.partitions.clone()
            };
            for &partition in &partitions {
                self.query_partition(partition, None, 0, &mut all_events)
                    .await;
            }
        } else {
            // Query per-(publisher_id, partition) for efficient range scans.
            let cursor_snapshot: Vec<((PublisherId, u32), u64)> =
                self.cursors.iter().map(|(k, v)| (*k, *v)).collect();
            for ((pub_id, partition), after_seq) in &cursor_snapshot {
                self.query_partition(
                    *partition,
                    Some(*pub_id),
                    *after_seq,
                    &mut all_events,
                )
                .await;
            }
        }

        // Sort by seq for deterministic delivery order.
        all_events.sort_by_key(|e| e.seq);

        Ok(all_events)
    }

    /// Issue a single store query for one (partition, optional publisher_id).
    async fn query_partition(
        &mut self,
        partition: u32,
        pub_id: Option<PublisherId>,
        after_seq: u64,
        out: &mut Vec<RawEvent>,
    ) {
        let selector = if let Some(pid) = pub_id {
            format!(
                "{}/{}?publisher_id={}&after_seq={}&limit={}",
                self.store_key_prefix, partition, pid, after_seq, self.batch_size,
            )
        } else {
            format!(
                "{}/{}?after_seq={}&limit={}",
                self.store_key_prefix, partition, after_seq, self.batch_size,
            )
        };

        let replies = match self
            .session
            .get(&selector)
            .accept_replies(zenoh::query::ReplyKeyExpr::Any)
            .consolidation(zenoh::query::ConsolidationMode::None)
            .timeout(Duration::from_secs(15))
            .await
        {
            Ok(r) => r,
            Err(e) => {
                warn!(partition, "catch-up query failed: {e}");
                return;
            }
        };

        while let Ok(reply) = replies.recv_async().await {
            match reply.result() {
                Ok(sample) => {
                    let meta = match sample
                        .attachment()
                        .and_then(|a| decode_metadata(a).ok())
                    {
                        Some(m) => m,
                        None => continue,
                    };

                    let key = (meta.pub_id, extract_partition(sample.key_expr().as_str()));

                    // Dedup: skip events at or below the cursor.
                    if let Some(&cursor_seq) = self.cursors.get(&key)
                        && meta.seq <= cursor_seq
                    {
                        continue;
                    }

                    let raw = RawEvent {
                        id: meta.event_id,
                        seq: meta.seq,
                        publisher_id: meta.pub_id,
                        key_expr: sample.key_expr().as_str().to_string(),
                        payload: sample.payload().to_bytes().to_vec(),
                        timestamp: meta.timestamp,
                    };
                    out.push(raw);

                    // Advance cursor.
                    let entry = self.cursors.entry(key).or_insert(0);
                    *entry = (*entry).max(meta.seq);
                }
                Err(err) => {
                    trace!(
                        "catch-up reply error: {}",
                        err.payload().try_to_string().unwrap_or_default()
                    );
                }
            }
        }
    }

    /// Adjust batch size based on how long the previous query took.
    pub fn adjust_batch_size(&mut self, batch_duration: Duration) {
        adjust_batch_size_adaptive(
            &mut self.batch_size,
            batch_duration,
            &self.config,
        );
    }

    /// Check whether the consumer is close enough to the watermark to
    /// re-subscribe to live pub/sub.
    pub fn is_caught_up(&self, watermark_seqs: &HashMap<(PublisherId, u32), u64>) -> bool {
        is_caught_up_check(&self.cursors, watermark_seqs, self.config.re_subscribe_threshold)
    }

    /// Current batch size (for testing / metrics).
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// Current cursor map (for dedup during re-subscribe overlap).
    pub fn cursors(&self) -> &HashMap<(PublisherId, u32), u64> {
        &self.cursors
    }
}

// ---------------------------------------------------------------------------
// Pure helpers (testable without a Session)
// ---------------------------------------------------------------------------

/// Adaptive batch sizing: double when fast, halve when slow.
fn adjust_batch_size_adaptive(
    batch_size: &mut usize,
    batch_duration: Duration,
    config: &OffloadConfig,
) {
    if batch_duration < config.target_batch_duration / 2 {
        *batch_size = (*batch_size * 2).min(config.max_batch_size);
    } else if batch_duration > config.target_batch_duration * 2 {
        *batch_size = (*batch_size / 2).max(config.min_batch_size);
    }
}

/// Check whether cursors are close enough to watermark seqs to be "caught up".
fn is_caught_up_check(
    cursors: &HashMap<(PublisherId, u32), u64>,
    watermark_seqs: &HashMap<(PublisherId, u32), u64>,
    threshold: u64,
) -> bool {
    for ((pub_id, partition), &wm_seq) in watermark_seqs {
        let cursor_seq = cursors.get(&(*pub_id, *partition)).copied().unwrap_or(0);
        if wm_seq > cursor_seq + threshold {
            return false;
        }
    }
    true
}

// ---------------------------------------------------------------------------
// OffloadManager — three-state machine driving the offload lifecycle
// ---------------------------------------------------------------------------

/// Manages the LIVE → DRAINING → CATCHING_UP → LIVE lifecycle.
///
/// During catch-up, forwarder tasks **drop** their Zenoh subscribers via 
/// [`ForwarderHandle::pause()`], releasing transport resources so the
/// publisher is never back-pressured.  When nearly caught up, forwarders
/// are resumed (re-declaring subscribers) with an overlap window where
/// `GapDetector` dedup-filters events already replayed from the store.
pub(crate) struct OffloadManager {
    session: Session,
    config: OffloadConfig,
    store_key_prefix: String,
    state: ConsumerState,
    lag_detector: LagDetector,
    offload_event_tx: flume::Sender<OffloadEvent>,
    event_tx: flume::Sender<RawEvent>,
    /// Partitions subscribed to (empty = wildcard).
    partitions: Vec<u32>,
    /// Control handle to pause / resume forwarder tasks.
    forwarder_control: ForwarderControl,
}

impl OffloadManager {
    /// Create a new offload manager with access to the forwarder handle.
    pub fn new(
        session: Session,
        config: OffloadConfig,
        store_key_prefix: String,
        event_tx: flume::Sender<RawEvent>,
        offload_event_tx: flume::Sender<OffloadEvent>,
        partitions: Vec<u32>,
        channel_capacity: usize,
        forwarder_control: ForwarderControl,
    ) -> Self {
        let lag_detector = LagDetector::new(channel_capacity, config.clone());
        Self {
            session,
            config,
            store_key_prefix,
            state: ConsumerState::Live,
            lag_detector,
            offload_event_tx,
            event_tx,
            partitions,
            forwarder_control,
        }
    }

    /// Current state of the offload state machine.
    #[cfg(test)]
    pub fn state(&self) -> ConsumerState {
        self.state
    }

    fn emit(&self, event: OffloadEvent) {
        let _ = self.offload_event_tx.try_send(event);
    }

    /// Update lag detector with current channel length and heartbeat info.
    pub fn update_lag(&mut self, channel_len: usize, heartbeat_lag: Option<(PublisherId, u32, u64)>) {
        self.lag_detector.update_channel_len(channel_len);
        if let Some((pub_id, partition, lag)) = heartbeat_lag {
            self.lag_detector.update_heartbeat_lag(pub_id, partition, lag);
        }
    }

    /// Check if lag is detected and transition from LIVE → DRAINING.
    /// Returns true if a transition occurred.
    pub fn check_and_trigger(&mut self) -> bool {
        if self.state != ConsumerState::Live {
            return false;
        }
        if self.lag_detector.is_lagging() {
            let fullness = self.lag_detector.channel_fullness();
            let max_lag = self.lag_detector.max_seq_lag();
            info!(
                channel_fullness = format!("{:.1}%", fullness * 100.0),
                max_seq_lag = max_lag,
                "slow consumer detected — transitioning to DRAINING"
            );
            self.state = ConsumerState::Draining;
            self.emit(OffloadEvent::LagDetected {
                channel_fullness: fullness,
                max_seq_lag: max_lag,
            });
            true
        } else {
            false
        }
    }

    /// Execute the DRAINING → CATCHING_UP → LIVE cycle.
    ///
    /// 1. Drains the sample channel into the event channel (DRAINING)
    /// 2. Pauses forwarders — Zenoh subscribers are dropped
    /// 3. Snapshots cursors and queries the store in batches (CATCHING_UP)
    /// 4. Resumes forwarders (re-declares subscribers) with overlap window
    /// 5. GapDetector dedup handles overlap → LIVE
    pub async fn run_offload_cycle(
        &mut self,
        gap_detector: &mut super::gap_detector::GapDetector,
        sample_rx: &flume::Receiver<(crate::attachment::EventMeta, String, Vec<u8>)>,
    ) -> Result<()> {
        let result = self.run_offload_cycle_inner(gap_detector, sample_rx).await;

        // Always resume forwarders on exit, even on error.
        self.forwarder_control.resume();
        result
    }

    /// Inner implementation of the offload cycle.
    async fn run_offload_cycle_inner(
        &mut self,
        gap_detector: &mut super::gap_detector::GapDetector,
        sample_rx: &flume::Receiver<(crate::attachment::EventMeta, String, Vec<u8>)>,
    ) -> Result<()> {
        let cycle_start = Instant::now();
        let mut events_replayed: u64 = 0;

        // ── DRAINING ──
        let buffered = sample_rx.len();
        self.emit(OffloadEvent::Draining { buffered });
        debug!(buffered, "draining buffered samples");

        // Pause forwarders FIRST — drop Zenoh subscribers immediately so
        // the publisher is never back-pressured while we catch up.
        self.forwarder_control.pause();

        // Wait for in-flight samples from the transport layer to arrive
        // before we snapshot cursors.
        tokio::time::sleep(self.config.drain_quiet_period).await;

        // Drain the sample channel.  We only advance gap detector cursors —
        // no delivery to event_tx here.  The events are in the store and
        // will be replayed during the CATCHING_UP phase if needed.
        Self::drain_sample_cursors(sample_rx, gap_detector);

        // ── Snapshot cursors ──
        let cursors = gap_detector.snapshot_cursors();
        debug!(cursor_count = cursors.len(), "switchover cursors captured");

        // ── CATCHING_UP ──
        self.state = ConsumerState::CatchingUp;
        let behind_by = cursors.values().sum::<u64>();
        self.emit(OffloadEvent::CatchingUp { behind_by });

        let mut reader = CatchUpReader::new(
            self.session.clone(),
            self.store_key_prefix.clone(),
            cursors,
            self.config.clone(),
            self.partitions.clone(),
        );

        // Query in batches until caught up AND the event channel has
        // drained enough to absorb the burst from re-subscribing.
        // This loop runs until:
        //   (a) store returns empty AND event_tx is below resume threshold, OR
        //   (b) event_tx receiver is dropped (subscriber shutting down).
        // No fixed retry limit — a permanently slow consumer stays in
        // offload mode indefinitely (correct behavior).
        let resume_at = self.lag_detector.channel_capacity / 4;
        loop {
            let batch_start = Instant::now();
            let batch = match reader.next_batch().await {
                Ok(b) => b,
                Err(e) => {
                    warn!("catch-up batch failed: {e}");
                    self.state = ConsumerState::Live;
                    self.lag_detector.reset();
                    self.emit(OffloadEvent::OffloadFailed {
                        error: e.to_string(),
                    });
                    return Err(Error::OffloadFailed(e.to_string()));
                }
            };
            let batch_duration = batch_start.elapsed();
            reader.adjust_batch_size(batch_duration);

            if batch.is_empty() {
                // Store exhausted at current cursor.  Only resume when
                // the event channel has drained enough so re-subscribing
                // won't immediately re-trigger offload.
                if self.event_tx.len() <= resume_at {
                    debug!("catch-up complete and channel drained — resuming");
                    break;
                }
                // Wait for the consumer to drain, then re-query (the
                // store may have new events by then).
                debug!(
                    channel_len = self.event_tx.len(),
                    threshold = resume_at,
                    "store exhausted but channel still full — waiting"
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }

            events_replayed += batch.len() as u64;
            for raw in batch {
                // Advance the gap detector so events replayed from the store
                // are not re-delivered when the live stream resumes.
                let partition = extract_partition(&raw.key_expr);
                gap_detector.on_sample(&raw.publisher_id, partition, raw.seq);
                if self.event_tx.send_async(raw).await.is_err() {
                    // Receiver dropped — subscriber is shutting down.
                    debug!("event channel closed during catch-up");
                    return Ok(());
                }
            }
        }

        // ── Re-subscribe (overlap window) ──
        // Resume forwarders: they re-declare Zenoh subscribers.
        // Events with seq <= cursor are automatically dedup-filtered by
        // the gap detector (it returns SampleResult::Duplicate).
        self.forwarder_control.resume();

        self.state = ConsumerState::Live;
        let elapsed = cycle_start.elapsed();
        self.emit(OffloadEvent::CaughtUp {
            elapsed,
            events_replayed,
        });
        self.emit(OffloadEvent::Resumed);
        self.lag_detector.reset();

        info!(
            elapsed_ms = elapsed.as_millis(),
            events_replayed, "offload cycle complete — resumed live pub/sub"
        );

        Ok(())
    }

    /// Drain all pending samples from `sample_rx`, advancing gap detector
    /// cursors only.  Does NOT deliver to `event_tx` — the events will be
    /// replayed from the store during the catch-up phase, avoiding a
    /// blocking send on a full channel.
    fn drain_sample_cursors(
        sample_rx: &flume::Receiver<(crate::attachment::EventMeta, String, Vec<u8>)>,
        gap_detector: &mut super::gap_detector::GapDetector,
    ) {
        let mut drained = 0u64;
        while let Ok((meta, key, _payload)) = sample_rx.try_recv() {
            let partition = extract_partition(&key);
            gap_detector.on_sample(&meta.pub_id, partition, meta.seq);
            drained += 1;
        }
        debug!(drained, "drained samples (cursors advanced)");
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> OffloadConfig {
        OffloadConfig::default()
    }

    #[test]
    fn offload_config_defaults() {
        let c = OffloadConfig::default();
        assert!(c.enabled);
        assert!((c.channel_fullness_threshold - 0.8).abs() < f64::EPSILON);
        assert_eq!(c.seq_lag_threshold, 10_000);
        assert_eq!(c.debounce_window, Duration::from_secs(2));
        assert_eq!(c.catch_up_batch_size, 500);
        assert_eq!(c.min_batch_size, 1_000);
        assert_eq!(c.max_batch_size, 100_000);
        assert_eq!(c.target_batch_duration, Duration::from_millis(100));
        assert_eq!(c.re_subscribe_threshold, 1_000);
        assert_eq!(c.drain_quiet_period, Duration::from_millis(50));
    }

    #[test]
    fn lag_detector_below_threshold_not_lagging() {
        let mut det = LagDetector::new(1024, default_config());
        det.update_channel_len(100); // ~10% full
        assert!(!det.is_lagging());
    }

    #[test]
    fn lag_detector_above_threshold_not_sustained() {
        let mut det = LagDetector::new(1024, default_config());
        det.update_channel_len(900); // ~88% full > 80%
        // First check starts debounce but doesn't trigger.
        assert!(!det.is_lagging());
        // Immediate second check — not enough time has passed.
        assert!(!det.is_lagging());
    }

    #[test]
    fn lag_detector_sustained_triggers() {
        let config = OffloadConfig {
            debounce_window: Duration::from_millis(0), // instant trigger for test
            ..default_config()
        };
        let mut det = LagDetector::new(1024, config);
        det.update_channel_len(900); // ~88% > 80%
        assert!(!det.is_lagging()); // first check starts debounce
        assert!(det.is_lagging()); // second check: 0ms debounce exceeded
    }

    #[test]
    fn lag_detector_heartbeat_seq_lag_triggers() {
        let config = OffloadConfig {
            debounce_window: Duration::from_millis(0),
            ..default_config()
        };
        let mut det = LagDetector::new(1024, config);
        let pub_id = PublisherId::new();
        det.update_heartbeat_lag(pub_id, 0, 20_000); // 20k > 10k threshold
        assert!(!det.is_lagging()); // starts debounce
        assert!(det.is_lagging()); // 0ms debounce exceeded
    }

    #[test]
    fn lag_detector_channel_fullness_triggers() {
        let config = OffloadConfig {
            debounce_window: Duration::from_millis(0),
            ..default_config()
        };
        let mut det = LagDetector::new(1024, config);
        det.update_channel_len(1000); // ~97.6% > 80%
        assert!(!det.is_lagging());
        assert!(det.is_lagging());
    }

    #[test]
    fn lag_detector_disabled_never_triggers() {
        let config = OffloadConfig {
            enabled: false,
            debounce_window: Duration::from_millis(0),
            ..default_config()
        };
        let mut det = LagDetector::new(1024, config);
        det.update_channel_len(1024); // 100% full
        let pub_id = PublisherId::new();
        det.update_heartbeat_lag(pub_id, 0, 999_999);
        assert!(!det.is_lagging());
        assert!(!det.is_lagging());
    }

    #[test]
    fn lag_detector_reset_clears_debounce() {
        let config = OffloadConfig {
            debounce_window: Duration::from_millis(0),
            ..default_config()
        };
        let mut det = LagDetector::new(1024, config);
        det.update_channel_len(1000);
        assert!(!det.is_lagging()); // starts debounce
        det.reset();
        // After reset, debounce restarts from scratch.
        assert!(!det.is_lagging());
    }

    #[test]
    fn lag_detector_transient_clears_debounce() {
        let config = OffloadConfig {
            debounce_window: Duration::from_secs(10), // long debounce
            ..default_config()
        };
        let mut det = LagDetector::new(1024, config);
        det.update_channel_len(1000); // over threshold
        assert!(!det.is_lagging()); // starts debounce

        // Channel clears — transient burst over.
        det.update_channel_len(100); // below threshold
        assert!(!det.is_lagging()); // resets debounce

        // Above threshold again — debounce restart, not instant trigger.
        det.update_channel_len(1000);
        assert!(!det.is_lagging()); // restarts debounce
    }

    #[test]
    fn is_caught_up_within_threshold() {
        let pub_id = PublisherId::new();
        let mut cursors = HashMap::new();
        cursors.insert((pub_id, 0), 950);

        let mut watermarks = HashMap::new();
        watermarks.insert((pub_id, 0), 1000);

        // 1000 - 950 = 50 < 100 threshold → caught up.
        assert!(is_caught_up_check(&cursors, &watermarks, 100));
    }

    #[test]
    fn not_caught_up_beyond_threshold() {
        let pub_id = PublisherId::new();
        let mut cursors = HashMap::new();
        cursors.insert((pub_id, 0), 500);

        let mut watermarks = HashMap::new();
        watermarks.insert((pub_id, 0), 1000);

        // 1000 - 500 = 500 > 100 threshold → not caught up.
        assert!(!is_caught_up_check(&cursors, &watermarks, 100));
    }

    #[test]
    fn adaptive_batch_increases_on_fast_query() {
        let config = OffloadConfig {
            target_batch_duration: Duration::from_millis(100),
            min_batch_size: 1_000,
            max_batch_size: 100_000,
            ..default_config()
        };
        let mut batch_size = 10_000;

        // Fast query: 20ms < 50ms (target/2) → double.
        adjust_batch_size_adaptive(&mut batch_size, Duration::from_millis(20), &config);
        assert_eq!(batch_size, 20_000);
    }

    #[test]
    fn adaptive_batch_decreases_on_slow_query() {
        let config = OffloadConfig {
            target_batch_duration: Duration::from_millis(100),
            min_batch_size: 1_000,
            max_batch_size: 100_000,
            ..default_config()
        };
        let mut batch_size = 10_000;

        // Slow query: 250ms > 200ms (target*2) → halve.
        adjust_batch_size_adaptive(&mut batch_size, Duration::from_millis(250), &config);
        assert_eq!(batch_size, 5_000);
    }

    #[test]
    fn snapshot_cursors_returns_correct_map() {
        use super::super::gap_detector::{GapDetector, SequenceTracker};

        let mut gd = GapDetector::new();
        let pub1 = PublisherId::new();
        let pub2 = PublisherId::new();

        gd.on_sample(&pub1, 0, 0);
        gd.on_sample(&pub1, 0, 1);
        gd.on_sample(&pub1, 0, 2);
        gd.on_sample(&pub2, 1, 0);
        gd.on_sample(&pub2, 1, 1);

        let cursors = gd.snapshot_cursors();
        assert_eq!(cursors.len(), 2);
        assert_eq!(cursors[&(pub1, 0)], 2);
        assert_eq!(cursors[&(pub2, 1)], 1);
    }
}
