#[cfg(feature = "store")]
use std::path::PathBuf;
use std::time::Duration;

use zenoh::qos::CongestionControl;

use crate::codec::CodecFormat;
use crate::error::{Error, Result};

/// How the publisher sends heartbeat beacons.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeartbeatMode {
    /// Send heartbeats at a fixed interval regardless of activity.
    Periodic(Duration),
    /// Send heartbeats only when there has been no publish activity for the given duration.
    Sporadic(Duration),
    /// No heartbeats.
    Disabled,
}

/// How the subscriber recovers missed events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryMode {
    /// Recover only when a heartbeat reveals the subscriber is behind.
    Heartbeat,
    /// Periodically query the publisher cache / event store.
    PeriodicQuery(Duration),
    /// Use both heartbeat-triggered and periodic recovery.
    Both,
}

/// Offset commit mode for consumer groups.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitMode {
    /// Application calls commit() explicitly after processing.
    Manual,
    /// Offsets are committed automatically at interval.
    Auto { interval: Duration },
}

/// What to do when no committed offset exists for a consumer group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OffsetReset {
    /// Start from the earliest available event (replay everything).
    Earliest,
    /// Start from the latest event (skip history).
    Latest,
}

/// Configuration for a consumer group.
#[derive(Debug, Clone)]
pub struct ConsumerGroupConfig {
    /// Consumer group identifier.
    pub group_id: String,
    /// This member's unique ID (default: random UUID).
    pub member_id: String,
    /// Offset commit mode.
    pub commit_mode: CommitMode,
    /// What to do when no committed offset exists.
    pub offset_reset: OffsetReset,
}

/// Configuration for the mitiflow event bus.
///
/// Use [`EventBusConfig::builder`] to construct with defaults and override
/// only the fields you need.
#[derive(Debug, Clone)]
pub struct EventBusConfig {
    // -- Core --
    /// Key expression prefix for this event bus (e.g., `"myapp/events"`).
    pub key_prefix: String,

    // -- Codec --
    /// Codec used for encoding/decoding event payloads. Defaults to JSON.
    pub codec: CodecFormat,

    // -- Publisher --
    /// Maximum number of samples kept in the publisher's in-memory recovery cache.
    pub cache_size: usize,
    /// Heartbeat mode for stale-connection detection.
    pub heartbeat: HeartbeatMode,
    /// Zenoh congestion control policy. `Block` prevents silent drops.
    pub congestion_control: CongestionControl,

    // -- Subscriber --
    /// How the subscriber recovers from detected gaps.
    pub recovery_mode: RecoveryMode,
    /// Whether to fetch history from the event store on subscribe.
    pub history_on_subscribe: bool,
    /// Number of independent processing shards (each owns a GapDetector, no locks).
    /// Sharding by publisher ID provides parallelism across publishers. Defaults to 1.
    pub num_processing_shards: usize,
    /// Evict tracking state for publishers silent longer than this duration.
    /// `None` disables eviction (the default).
    pub publisher_ttl: Option<Duration>,
    /// Delay before querying the store for gap recovery, giving the EventStore
    /// time to persist in-flight events. Default: 50ms.
    pub recovery_delay: Duration,
    /// Maximum number of store recovery attempts (with exponential backoff)
    /// before declaring a gap irrecoverable. Default: 3.
    pub max_recovery_attempts: u32,

    // -- Store (feature = "store") --
    /// Key prefix for the event store queryable. Defaults to `"{key_prefix}/_store"`.
    #[cfg(feature = "store")]
    pub store_key_prefix: Option<String>,
    /// Filesystem path for the fjall storage directory.
    #[cfg(feature = "store")]
    pub store_path: Option<PathBuf>,
    /// Key expression for the watermark stream. Defaults to `"{key_prefix}/_watermark"`.
    #[cfg(feature = "store")]
    pub watermark_key: Option<String>,
    /// How often the event store publishes a `CommitWatermark`.
    #[cfg(feature = "store")]
    pub watermark_interval: Duration,
    /// How long `publish_durable()` waits for watermark confirmation.
    #[cfg(feature = "store")]
    pub durable_timeout: Duration,
    /// Latency budget for durable publishes. The Event Store will try to
    /// broadcast a watermark within this duration of receiving an urgent event.
    /// `Duration::ZERO` means broadcast immediately. Defaults to
    /// `watermark_interval` (no acceleration).
    #[cfg(feature = "store")]
    pub durable_urgency: Duration,
    /// Number of dedicated OS threads that process storage backend operations.
    /// Using dedicated threads avoids blocking the tokio runtime with
    /// synchronous I/O (e.g. fjall, redb). Defaults to `2`.
    #[cfg(feature = "store")]
    pub store_workers: usize,

    // -- Partition (feature = "partition") --
    /// Number of partitions for the hash ring.
    #[cfg(feature = "partition")]
    pub num_partitions: u32,
    /// This worker's identity for partition assignment.
    #[cfg(feature = "partition")]
    pub worker_id: Option<String>,
    /// Liveliness key prefix for worker presence. Defaults to `"{key_prefix}/_workers"`.
    #[cfg(feature = "partition")]
    pub worker_liveliness_prefix: Option<String>,
}

impl EventBusConfig {
    /// Start building a configuration with the given key expression prefix.
    pub fn builder(key_prefix: impl Into<String>) -> EventBusConfigBuilder {
        EventBusConfigBuilder::new(key_prefix)
    }

    /// Resolve an optional override key, falling back to `"{key_prefix}/{suffix}"`.
    fn resolve_key(&self, specific: &Option<String>, suffix: &str) -> String {
        specific
            .clone()
            .unwrap_or_else(|| format!("{}/{suffix}", self.key_prefix))
    }

    /// Resolve the store key prefix, falling back to `"{key_prefix}/_store"`.
    #[cfg(feature = "store")]
    pub fn resolved_store_key_prefix(&self) -> String {
        self.resolve_key(&self.store_key_prefix, "_store")
    }

    /// Resolve the watermark key, falling back to `"{key_prefix}/_watermark"`.
    #[cfg(feature = "store")]
    pub fn resolved_watermark_key(&self) -> String {
        self.resolve_key(&self.watermark_key, "_watermark")
    }

    /// Resolve the worker liveliness prefix, falling back to `"{key_prefix}/_workers"`.
    #[cfg(feature = "partition")]
    pub fn resolved_worker_liveliness_prefix(&self) -> String {
        self.resolve_key(&self.worker_liveliness_prefix, "_workers")
    }
}

/// Builder for [`EventBusConfig`] with sensible defaults.
pub struct EventBusConfigBuilder {
    key_prefix: String,
    codec: CodecFormat,
    cache_size: usize,
    heartbeat: HeartbeatMode,
    congestion_control: CongestionControl,
    recovery_mode: RecoveryMode,
    history_on_subscribe: bool,
    num_processing_shards: usize,
    publisher_ttl: Option<Duration>,
    recovery_delay: Duration,
    max_recovery_attempts: u32,
    #[cfg(feature = "store")]
    store_key_prefix: Option<String>,
    #[cfg(feature = "store")]
    store_path: Option<PathBuf>,
    #[cfg(feature = "store")]
    watermark_key: Option<String>,
    #[cfg(feature = "store")]
    watermark_interval: Duration,
    #[cfg(feature = "store")]
    durable_timeout: Duration,
    #[cfg(feature = "store")]
    durable_urgency: Duration,
    #[cfg(feature = "store")]
    store_workers: usize,
    #[cfg(feature = "partition")]
    num_partitions: u32,
    #[cfg(feature = "partition")]
    worker_id: Option<String>,
    #[cfg(feature = "partition")]
    worker_liveliness_prefix: Option<String>,
}

impl EventBusConfigBuilder {
    fn new(key_prefix: impl Into<String>) -> Self {
        Self {
            key_prefix: key_prefix.into(),
            codec: CodecFormat::default(),
            cache_size: 256,
            heartbeat: HeartbeatMode::Sporadic(Duration::from_secs(1)),
            congestion_control: CongestionControl::Block,
            recovery_mode: RecoveryMode::Both,
            history_on_subscribe: true,
            num_processing_shards: 1,
            publisher_ttl: None,
            recovery_delay: Duration::from_millis(50),
            max_recovery_attempts: 3,
            #[cfg(feature = "store")]
            store_key_prefix: None,
            #[cfg(feature = "store")]
            store_path: None,
            #[cfg(feature = "store")]
            watermark_key: None,
            #[cfg(feature = "store")]
            watermark_interval: Duration::from_millis(100),
            #[cfg(feature = "store")]
            durable_timeout: Duration::from_secs(5),
            #[cfg(feature = "store")]
            durable_urgency: Duration::from_millis(100),
            #[cfg(feature = "store")]
            store_workers: 2,
            #[cfg(feature = "partition")]
            num_partitions: 64,
            #[cfg(feature = "partition")]
            worker_id: None,
            #[cfg(feature = "partition")]
            worker_liveliness_prefix: None,
        }
    }

    pub fn cache_size(mut self, size: usize) -> Self {
        self.cache_size = size;
        self
    }

    /// Set a custom codec for event payload encoding/decoding.
    ///
    /// Defaults to [`CodecFormat::Json`] if not set.
    pub fn codec(mut self, codec: CodecFormat) -> Self {
        self.codec = codec;
        self
    }

    pub fn heartbeat(mut self, mode: HeartbeatMode) -> Self {
        self.heartbeat = mode;
        self
    }

    pub fn congestion_control(mut self, cc: CongestionControl) -> Self {
        self.congestion_control = cc;
        self
    }

    pub fn recovery_mode(mut self, mode: RecoveryMode) -> Self {
        self.recovery_mode = mode;
        self
    }

    pub fn history_on_subscribe(mut self, enable: bool) -> Self {
        self.history_on_subscribe = enable;
        self
    }

    /// Set the number of parallel event-processing shards.
    ///
    /// Each shard owns its own [`GapDetector`] and processes a disjoint subset
    /// of publishers (routed by `pub_id % N`). Higher values increase CPU
    /// parallelism at the cost of slightly more memory. Defaults to `1`.
    pub fn num_processing_shards(mut self, n: usize) -> Self {
        self.num_processing_shards = n.max(1);
        self
    }

    /// Evict tracking state for publishers that have been silent longer than `ttl`.
    ///
    /// Prevents unbounded growth of the per-publisher `HashMap`. Defaults to `None`
    /// (no eviction).
    pub fn publisher_ttl(mut self, ttl: Duration) -> Self {
        self.publisher_ttl = Some(ttl);
        self
    }

    /// Delay before querying the store for gap recovery. Default: 50ms.
    pub fn recovery_delay(mut self, delay: Duration) -> Self {
        self.recovery_delay = delay;
        self
    }

    /// Maximum store recovery attempts with exponential backoff. Default: 3.
    pub fn max_recovery_attempts(mut self, attempts: u32) -> Self {
        self.max_recovery_attempts = attempts;
        self
    }

    #[cfg(feature = "store")]
    pub fn store_key_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.store_key_prefix = Some(prefix.into());
        self
    }

    #[cfg(feature = "store")]
    pub fn store_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.store_path = Some(path.into());
        self
    }

    #[cfg(feature = "store")]
    pub fn watermark_key(mut self, key: impl Into<String>) -> Self {
        self.watermark_key = Some(key.into());
        self
    }

    #[cfg(feature = "store")]
    pub fn watermark_interval(mut self, interval: Duration) -> Self {
        self.watermark_interval = interval;
        self
    }

    #[cfg(feature = "store")]
    pub fn durable_timeout(mut self, timeout: Duration) -> Self {
        self.durable_timeout = timeout;
        self
    }

    /// Set the latency budget for durable publishes.
    ///
    /// The Event Store will try to broadcast a watermark within this duration
    /// of receiving an urgent event, rather than waiting for the next periodic
    /// tick. Clamped to 65 535 ms (≈ 65 s) on the wire.
    ///
    /// Defaults to `watermark_interval`.
    #[cfg(feature = "store")]
    pub fn durable_urgency(mut self, urgency: Duration) -> Self {
        self.durable_urgency = urgency;
        self
    }

    /// Set the number of dedicated OS threads for storage backend operations.
    ///
    /// Multiple threads consume from the same MPMC channel, providing
    /// horizontal throughput scaling for backends like fjall or redb that
    /// perform synchronous I/O. Defaults to `2`.
    #[cfg(feature = "store")]
    pub fn store_workers(mut self, n: usize) -> Self {
        self.store_workers = n.max(1);
        self
    }

    #[cfg(feature = "partition")]
    pub fn num_partitions(mut self, n: u32) -> Self {
        self.num_partitions = n;
        self
    }

    #[cfg(feature = "partition")]
    pub fn worker_id(mut self, id: impl Into<String>) -> Self {
        self.worker_id = Some(id.into());
        self
    }

    #[cfg(feature = "partition")]
    pub fn worker_liveliness_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.worker_liveliness_prefix = Some(prefix.into());
        self
    }

    /// Build the configuration, validating all fields.
    pub fn build(self) -> Result<EventBusConfig> {
        if self.key_prefix.is_empty() {
            return Err(Error::InvalidConfig(
                "key_prefix must not be empty".to_string(),
            ));
        }
        // if self.cache_size == 0 {
        //     return Err(Error::InvalidConfig(
        //         "cache_size must be greater than 0".to_string(),
        //     ));
        // }

        Ok(EventBusConfig {
            key_prefix: self.key_prefix,
            codec: self.codec,
            cache_size: self.cache_size,
            heartbeat: self.heartbeat,
            congestion_control: self.congestion_control,
            recovery_mode: self.recovery_mode,
            history_on_subscribe: self.history_on_subscribe,
            num_processing_shards: self.num_processing_shards,
            publisher_ttl: self.publisher_ttl,
            recovery_delay: self.recovery_delay,
            max_recovery_attempts: self.max_recovery_attempts,
            #[cfg(feature = "store")]
            store_key_prefix: self.store_key_prefix,
            #[cfg(feature = "store")]
            store_path: self.store_path,
            #[cfg(feature = "store")]
            watermark_key: self.watermark_key,
            #[cfg(feature = "store")]
            watermark_interval: self.watermark_interval,
            #[cfg(feature = "store")]
            durable_timeout: self.durable_timeout,
            #[cfg(feature = "store")]
            durable_urgency: self.durable_urgency,
            #[cfg(feature = "store")]
            store_workers: self.store_workers,
            #[cfg(feature = "partition")]
            num_partitions: self.num_partitions,
            #[cfg(feature = "partition")]
            worker_id: self.worker_id,
            #[cfg(feature = "partition")]
            worker_liveliness_prefix: self.worker_liveliness_prefix,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_defaults() {
        let config = EventBusConfig::builder("test/events").build().unwrap();
        assert_eq!(config.key_prefix, "test/events");
        assert_eq!(config.cache_size, 256);
        assert_eq!(
            config.heartbeat,
            HeartbeatMode::Sporadic(Duration::from_secs(1))
        );
        assert_eq!(config.congestion_control, CongestionControl::Block);
        assert_eq!(config.recovery_mode, RecoveryMode::Both);
        assert!(config.history_on_subscribe);
    }

    #[test]
    fn builder_overrides() {
        let config = EventBusConfig::builder("app/data")
            .cache_size(500)
            .heartbeat(HeartbeatMode::Periodic(Duration::from_millis(500)))
            .congestion_control(CongestionControl::Drop)
            .recovery_mode(RecoveryMode::Heartbeat)
            .history_on_subscribe(false)
            .build()
            .unwrap();

        assert_eq!(config.cache_size, 500);
        assert_eq!(
            config.heartbeat,
            HeartbeatMode::Periodic(Duration::from_millis(500))
        );
        assert_eq!(config.congestion_control, CongestionControl::Drop);
        assert_eq!(config.recovery_mode, RecoveryMode::Heartbeat);
        assert!(!config.history_on_subscribe);
    }

    #[test]
    fn empty_prefix_rejected() {
        let result = EventBusConfig::builder("").build();
        assert!(result.is_err());
    }

    #[test]
    fn zero_cache_allowed() {
        // cache_size=0 disables the publisher history cache (valid for benchmarks).
        let result = EventBusConfig::builder("x").cache_size(0).build();
        assert!(result.is_ok());
    }
}
