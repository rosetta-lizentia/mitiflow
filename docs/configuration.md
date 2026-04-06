# Configuration Reference

Complete reference for all `EventBusConfig` builder options, consumer group settings, and offload tuning.

> **Quick start:** See [Getting Started](getting_started.md) for usage examples. This page is for tuning and reference.

---

## Table of Contents

1. [EventBusConfig Builder](#eventbusconfig-builder)
2. [Consumer Group Config](#consumer-group-config)
3. [Offload Config](#offload-config)
4. [Codec Selection](#codec-selection)
5. [Common Configurations](#common-configurations)

---

## EventBusConfig Builder

Create via `EventBusConfig::builder("key/prefix")`. All settings have sensible defaults.

### Core

| Method | Default | Description |
|--------|---------|-------------|
| `.key_prefix(str)` | *(required)* | Zenoh key expression prefix (e.g., `"myapp/events"`) |
| `.codec(CodecFormat)` | `Json` | Serialization format: `Json`, `MessagePack`, `Postcard` |
| `.num_partitions(u32)` | `1` | Number of partitions for key-based routing |
| `.worker_id(str)` | `None` | This node's identity for partition assignment |
| `.schema_mode(TopicSchemaMode)` | `Disabled` | Schema validation mode: `Disabled`, `Validate`, `AutoConfig`, `RegisterOrValidate`. See [Topic Schema Registry](18_topic_schema_registry.md) |

### Publisher

| Method | Default | Description |
|--------|---------|-------------|
| `.cache_size(usize)` | `0` | In-memory recovery cache size (events). See [Cache Recovery](09_cache_recovery_design.md) |
| `.heartbeat(HeartbeatMode)` | `Disabled` | `Periodic(Duration)`, `Sporadic(Duration)`, or `Disabled` |
| `.congestion_control(CongestionControl)` | `Block` | Zenoh QoS: `Block` (backpressure) or `Drop` (lossy) |

### Subscriber

| Method | Default | Description |
|--------|---------|-------------|
| `.recovery_mode(RecoveryMode)` | `Heartbeat` | `Heartbeat`, `PeriodicQuery(Duration)`, or `Both` |
| `.history_on_subscribe(bool)` | `false` | Fetch stored history when subscriber connects |
| `.num_processing_shards(usize)` | `1` | Parallel processing shards (one `GapDetector` per shard) |
| `.publisher_ttl(Duration)` | `None` | Evict tracking for silent publishers after this duration |
| `.recovery_delay(Duration)` | `50ms` | Delay before store queries (lets in-flight events persist) |
| `.max_recovery_attempts(u32)` | `3` | Max store recovery retries with exponential backoff |
| `.event_channel_capacity(usize)` | `1024` | Bounded internal event delivery channel capacity |
| `.dedup_capacity(usize)` | `10,000` | Dedup set size for key-filtered subscribers |

### Store (requires `store` feature)

| Method | Default | Description |
|--------|---------|-------------|
| `.store_key_prefix(str)` | `"{prefix}/_store"` | Key expression for store queryable |
| `.store_path(Path)` | `None` | Filesystem path for fjall storage |
| `.watermark_key(str)` | `"{prefix}/_watermark"` | Key expression for watermark stream |
| `.watermark_interval(Duration)` | `1s` | How often the store publishes watermarks |
| `.durable_timeout(Duration)` | `5s` | Max wait for `publish_durable()` confirmation |
| `.durable_urgency(Duration)` | `= watermark_interval` | Target latency for urgent watermark broadcasts |
| `.store_workers(usize)` | `2` | Dedicated OS threads for storage backend I/O |
| `.compaction_interval(Duration)` | `None` | Automatic keyed log compaction interval |
| `.tombstone_retention(Duration)` | `None` | How long tombstones are kept after compaction |

### Example

```rust
let config = EventBusConfig::builder("myapp/events")
    .codec(CodecFormat::MessagePack)
    .cache_size(1000)
    .heartbeat(HeartbeatMode::Periodic(Duration::from_millis(500)))
    .num_partitions(8)
    .event_channel_capacity(4096)
    .watermark_interval(Duration::from_millis(100))
    .durable_timeout(Duration::from_secs(10))
    .build()?;
```

---

## Consumer Group Config

Used with `ConsumerGroupSubscriber::new()`. See [Consumer Group Commits](11_consumer_group_commits.md) for protocol details.

| Field | Type | Description |
|-------|------|-------------|
| `group_id` | `String` | Consumer group identifier |
| `member_id` | `String` | This member's unique ID |
| `commit_mode` | `CommitMode` | `Manual` or `Auto { interval }` |
| `offset_reset` | `OffsetReset` | `Earliest` (replay all) or `Latest` (skip history) |

### CommitMode

- **`Manual`** — call `consumer.commit_sync().await?` after processing each batch.
- **`Auto { interval }`** — offsets committed automatically at the given interval. Simpler but at-least-once delivery on crash.

### OffsetReset

- **`Earliest`** — on first join (no committed offset), replay from the beginning.
- **`Latest`** — on first join, skip to the current position.

---

## Offload Config

Controls automatic slow-consumer switchover from live pub/sub to store-based catch-up. Requires the `store` feature. See [Slow Consumer Offload](17_slow_consumer_offload.md).

| Field | Default | Description |
|-------|---------|-------------|
| `enabled` | `true` | Enable/disable automatic offload |
| `channel_fullness_threshold` | `0.8` | Channel fullness ratio (0.0–1.0) that triggers lag detection |
| `seq_lag_threshold` | `10,000` | Max per-publisher sequence lag before contributing to detection |
| `debounce_window` | `2s` | Lag must be sustained this long before offloading |
| `catch_up_batch_size` | `500` | Events per store query batch |
| `min_batch_size` | `1,000` | Minimum batch size (adaptive sizing) |
| `max_batch_size` | `100,000` | Maximum batch size (adaptive sizing) |
| `target_batch_duration` | `100ms` | Target duration per batch (adaptive sizer reference) |
| `re_subscribe_threshold` | `1,000` | Max sequence delta to watermark before re-subscribing to live |
| `drain_quiet_period` | `50ms` | Quiet period after drain before recording switchover cursor |

---

## Codec Selection

| Codec | Method | Payload size | Speed | Human-readable |
|-------|--------|-------------|-------|----------------|
| `Json` | `serde_json` | Largest | Moderate | Yes |
| `MessagePack` | `rmp-serde` | Compact | Fast | No |
| `Postcard` | `postcard` | Smallest | Fastest | No |

Choose based on your needs:
- **Development/debugging:** `Json` (inspect payloads easily)
- **High throughput:** `Postcard` or `MessagePack`
- **Interop with non-Rust:** `Json` or `MessagePack`

---

## Schema Validation

Controls whether publishers and subscribers validate their configuration against a registered topic schema. See [Topic Schema Registry](18_topic_schema_registry.md) for the full design.

### TopicSchemaMode

| Mode | Description |
|------|-------------|
| `Disabled` | No schema validation (default, backward-compatible) |
| `Validate` | Query `{key_prefix}/_schema`, fail on mismatch or if no schema found |
| `AutoConfig` | Load full config from the schema registry (auto-configuration) |
| `RegisterOrValidate` | Validate if a schema exists; register from local config if absent (dev/test) |

### Auto-Configuration

Skip the builder entirely and load config from the registry:

```rust
let config = EventBusConfig::from_topic(&session, "myapp/events", "orders").await?;
let publisher = EventPublisher::new(&session, config).await?;
```

### Validated Fields

| Field | Mismatch severity |
|-------|-------------------|
| `codec` | Fatal — data corruption |
| `num_partitions` | Fatal — routing divergence |
| `key_format` | Warning (fatal in strict mode) |

Other fields (cache size, heartbeat, recovery mode, offload config) are local tuning and are **not** validated against the schema.

---

## Common Configurations

### High-throughput, at-most-once

```rust
EventBusConfig::builder("fast/stream")
    .codec(CodecFormat::Postcard)
    .cache_size(0)                    // No recovery cache
    .heartbeat(HeartbeatMode::Disabled)
    .congestion_control(CongestionControl::Drop)
    .event_channel_capacity(8192)
    .build()?
```

### Reliable, exactly-once processing

```rust
EventBusConfig::builder("reliable/stream")
    .cache_size(10_000)
    .heartbeat(HeartbeatMode::Periodic(Duration::from_millis(200)))
    .recovery_mode(RecoveryMode::Both)
    .watermark_interval(Duration::from_millis(50))
    .durable_timeout(Duration::from_secs(10))
    .num_processing_shards(4)
    .build()?
```

### Key-ordered with compaction

```rust
EventBusConfig::builder("keyed/entities")
    .num_partitions(16)
    .cache_size(5_000)
    .heartbeat(HeartbeatMode::Periodic(Duration::from_secs(1)))
    .compaction_interval(Duration::from_secs(300))
    .tombstone_retention(Duration::from_secs(86400))
    .build()?
```
