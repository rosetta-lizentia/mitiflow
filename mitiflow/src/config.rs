#[cfg(feature = "store")]
use std::path::PathBuf;
use std::time::Duration;

use zenoh::qos::CongestionControl;

use crate::codec::CodecFormat;
use crate::error::{Error, Result};
use crate::schema::TopicSchemaMode;

/// Configuration for automatic slow-consumer offload.
///
/// When a subscriber falls behind the live stream, it can transparently switch
/// from real-time pub/sub to store-based query replay, then resume live
/// streaming once caught up. This struct controls the detection thresholds,
/// batch sizing, and transition parameters.
///
/// Requires the `store` feature — without an Event Store there is nothing to
/// read during catch-up.
#[cfg(feature = "store")]
#[derive(Debug, Clone)]
pub struct OffloadConfig {
    /// Enable automatic offload (default: `true`).
    pub enabled: bool,

    /// Channel fullness ratio (0.0–1.0) that contributes to lag detection.
    /// Default: `0.8` (80% of channel capacity).
    pub channel_fullness_threshold: f64,

    /// Maximum per-publisher sequence lag (via heartbeat) before contributing
    /// to lag detection. Default: `10_000`.
    pub seq_lag_threshold: u64,

    /// Lag must be sustained for this duration before offloading.
    /// Default: `2s`.
    pub debounce_window: Duration,

    /// Number of events per store query batch during catch-up.
    /// Default: `10_000`.
    pub catch_up_batch_size: usize,

    /// Minimum batch size for adaptive sizing. Default: `1_000`.
    pub min_batch_size: usize,

    /// Maximum batch size for adaptive sizing. Default: `100_000`.
    pub max_batch_size: usize,

    /// Target duration for a single batch query (adaptive sizer reference).
    /// Default: `100ms`.
    pub target_batch_duration: Duration,

    /// How close to the watermark the consumer must be to re-subscribe.
    /// Expressed as max sequence delta. Default: `1_000`.
    pub re_subscribe_threshold: u64,

    /// Quiet period after channel drain before recording switchover cursor.
    /// Default: `50ms`.
    pub drain_quiet_period: Duration,
}

#[cfg(feature = "store")]
impl Default for OffloadConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            channel_fullness_threshold: 0.8,
            seq_lag_threshold: 10_000,
            debounce_window: Duration::from_secs(2),
            catch_up_batch_size: 500,
            min_batch_size: 1_000,
            max_batch_size: 100_000,
            target_batch_duration: Duration::from_millis(100),
            re_subscribe_threshold: 1_000,
            drain_quiet_period: Duration::from_millis(50),
        }
    }
}

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

    /// Bounded capacity of the internal event delivery channel. The publisher
    /// and consumer share this backpressure channel. Default: 1024.
    pub event_channel_capacity: usize,

    /// Capacity of the event-ID deduplication set used by key-filtered
    /// subscribers (passthrough mode). Ignored for unfiltered subscribers.
    /// Default: 10,000.
    pub dedup_capacity: usize,

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
    /// Interval between automatic keyed‐compaction runs. `None` disables
    /// automatic compaction (the default). Manual calls to
    /// [`StorageBackend::compact_keyed`] are still allowed.
    #[cfg(feature = "store")]
    pub compaction_interval: Option<Duration>,
    /// How long tombstones (empty‐payload keyed events) are retained after
    /// compaction before garbage collection removes them. `None` means
    /// tombstones are never removed automatically (the default).
    #[cfg(feature = "store")]
    pub tombstone_retention: Option<Duration>,

    // -- Offload (feature = "store") --
    /// Slow-consumer offload configuration. When enabled, the subscriber
    /// transparently switches to store-based catch-up when it falls behind.
    #[cfg(feature = "store")]
    pub offload: OffloadConfig,

    // -- Partition --
    /// Number of partitions for the hash ring.
    pub num_partitions: u32,
    /// This worker's identity for partition assignment.
    pub worker_id: Option<String>,
    /// Liveliness key prefix for worker presence. Defaults to `"{key_prefix}/_workers"`.
    pub worker_liveliness_prefix: Option<String>,

    // -- Schema --
    /// Controls how this publisher/subscriber interacts with the topic schema
    /// registry. Defaults to [`TopicSchemaMode::Disabled`].
    pub schema_mode: TopicSchemaMode,
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
    pub fn resolved_worker_liveliness_prefix(&self) -> String {
        self.resolve_key(&self.worker_liveliness_prefix, "_workers")
    }

    /// Key expression matching a specific key across all partitions.
    ///
    /// Example: `config.key_expr_for_key("order-123")` with prefix `"app/events"`
    /// returns `"app/events/p/*/k/order-123/*"`.
    pub fn key_expr_for_key(&self, key: &str) -> String {
        format!("{}/p/*/k/{}/*", self.key_prefix, key)
    }

    /// Key expression matching a key prefix across all partitions.
    ///
    /// Example: `config.key_expr_for_key_prefix("user/42")` with prefix `"app/events"`
    /// returns `"app/events/p/*/k/user/42/**"`.
    pub fn key_expr_for_key_prefix(&self, key_prefix: &str) -> String {
        format!("{}/p/*/k/{}/**", self.key_prefix, key_prefix)
    }

    /// Build a configuration by fetching a topic's schema from the registry.
    ///
    /// Creates a default config for the given `key_prefix`, then applies the
    /// wire-level fields (codec, num_partitions, key_prefix) from the
    /// registered schema. Local tuning fields use builder defaults.
    ///
    /// Returns `Error::TopicSchemaNotFound` if no storage agent or
    /// orchestrator responds within the timeout.
    pub async fn from_topic(session: &zenoh::Session, key_prefix: &str) -> Result<Self> {
        let schema = crate::schema::fetch_schema(session, key_prefix).await?;
        let mut config = Self::builder(key_prefix).build()?;
        schema.apply_to_config(&mut config);
        Ok(config)
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
    event_channel_capacity: usize,
    dedup_capacity: usize,
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
    #[cfg(feature = "store")]
    compaction_interval: Option<Duration>,
    #[cfg(feature = "store")]
    tombstone_retention: Option<Duration>,
    #[cfg(feature = "store")]
    offload: OffloadConfig,
    num_partitions: u32,
    worker_id: Option<String>,
    worker_liveliness_prefix: Option<String>,
    schema_mode: TopicSchemaMode,
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
            event_channel_capacity: 1024,
            dedup_capacity: 10_000,
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
            #[cfg(feature = "store")]
            compaction_interval: None,
            #[cfg(feature = "store")]
            tombstone_retention: None,
            #[cfg(feature = "store")]
            offload: OffloadConfig::default(),
            num_partitions: 64,
            worker_id: None,
            worker_liveliness_prefix: None,
            schema_mode: TopicSchemaMode::default(),
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

    /// Set the bounded capacity of the internal event delivery channel.
    /// Default: 1024.
    pub fn event_channel_capacity(mut self, capacity: usize) -> Self {
        self.event_channel_capacity = capacity.max(1);
        self
    }

    /// Set the capacity of the event-ID dedup set for key-filtered subscribers.
    /// Default: 10,000.
    pub fn dedup_capacity(mut self, capacity: usize) -> Self {
        self.dedup_capacity = capacity.max(1);
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

    #[cfg(feature = "store")]
    pub fn compaction_interval(mut self, interval: Duration) -> Self {
        self.compaction_interval = Some(interval);
        self
    }

    #[cfg(feature = "store")]
    pub fn tombstone_retention(mut self, retention: Duration) -> Self {
        self.tombstone_retention = Some(retention);
        self
    }

    /// Set the slow-consumer offload configuration.
    #[cfg(feature = "store")]
    pub fn offload(mut self, config: OffloadConfig) -> Self {
        self.offload = config;
        self
    }

    pub fn num_partitions(mut self, n: u32) -> Self {
        self.num_partitions = n;
        self
    }

    pub fn worker_id(mut self, id: impl Into<String>) -> Self {
        self.worker_id = Some(id.into());
        self
    }

    pub fn worker_liveliness_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.worker_liveliness_prefix = Some(prefix.into());
        self
    }

    /// Set the topic schema mode for registry validation/auto-configuration.
    ///
    /// Defaults to [`TopicSchemaMode::Disabled`].
    pub fn schema_mode(mut self, mode: TopicSchemaMode) -> Self {
        self.schema_mode = mode;
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
            event_channel_capacity: self.event_channel_capacity,
            dedup_capacity: self.dedup_capacity,
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
            #[cfg(feature = "store")]
            compaction_interval: self.compaction_interval,
            #[cfg(feature = "store")]
            tombstone_retention: self.tombstone_retention,
            #[cfg(feature = "store")]
            offload: self.offload,
            num_partitions: self.num_partitions,
            worker_id: self.worker_id,
            worker_liveliness_prefix: self.worker_liveliness_prefix,
            schema_mode: self.schema_mode,
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

    #[test]
    fn key_expr_for_key_builds_correct_expr() {
        let config = EventBusConfig::builder("app/events").build().unwrap();
        assert_eq!(
            config.key_expr_for_key("order-123"),
            "app/events/p/*/k/order-123/*"
        );
    }

    #[test]
    fn key_expr_for_key_prefix_builds_correct_expr() {
        let config = EventBusConfig::builder("app/events").build().unwrap();
        assert_eq!(
            config.key_expr_for_key_prefix("user/42"),
            "app/events/p/*/k/user/42/**"
        );
    }
}
