# Crate Architecture

Technical design for the `mitiflow` workspace.

---

## 1. Crate Structure

```
mitiflow/                     # Core library crate
├── src/
│   ├── lib.rs                # Re-exports, feature gate macros
│   ├── config.rs             # EventBusConfig (builder pattern)
│   ├── event.rs              # Event<T>, RawEvent envelopes
│   ├── error.rs              # Error types (thiserror)
│   ├── types.rs              # PublisherId, EventId (UUID v7 newtypes)
│   ├── codec.rs              # CodecFormat: JSON, MessagePack, Postcard
│   ├── attachment.rs         # 50-byte binary metadata for Zenoh attachments
│   ├── dlq.rs                # DeadLetterQueue
│   ├── publisher/mod.rs      # EventPublisher (sequencing + cache + heartbeat)
│   ├── subscriber/
│   │   ├── mod.rs            # EventSubscriber (gap detection + recovery)
│   │   ├── gap_detector.rs   # Per-(publisher, partition) sequence tracking
│   │   ├── consumer_group.rs # ConsumerGroupSubscriber
│   │   └── offload.rs        # Slow consumer offload state machine
│   ├── store/                # feature = "store"
│   │   ├── mod.rs            # EventStore (sidecar), StoreManager
│   │   ├── backend.rs        # StorageBackend trait + FjallBackend
│   │   ├── runner.rs         # Background tasks (subscribe, queryable, watermark, gc, offsets)
│   │   ├── watermark.rs      # CommitWatermark types
│   │   ├── query.rs          # QueryFilters parsing
│   │   ├── lifecycle.rs      # Publisher lifecycle state machine
│   │   ├── manager.rs        # Store manager coordination
│   │   └── offset.rs         # OffsetCommit types
│   └── partition/
│       ├── mod.rs            # PartitionManager
│       └── hash_ring.rs      # Rendezvous (HRW) hashing + rack-aware assignment
├── tests/
│   ├── common/mod.rs         # Shared test helpers
│   ├── reliability.rs        # Ordering + gap recovery
│   ├── recovery.rs           # Cache + store recovery paths
│   ├── store.rs              # Persistence + query + watermark
│   ├── consumer_group_commit.rs
│   ├── keyed_publish.rs
│   ├── offload.rs            # Slow consumer offload
│   ├── dedup.rs, dlq.rs, partition.rs
└── examples/
    ├── basic_pubsub.rs
    ├── keyed_pubsub.rs
    ├── consumer_groups.rs
    ├── event_store.rs
    ├── durable_publish.rs
    ├── dead_letter_queue.rs
    └── slow_consumer_offload.rs
```

Other crates: `mitiflow-agent` (storage daemon), `mitiflow-orchestrator`
(control plane), `mitiflow-emulator` (topology runner), `mitiflow-cli`
(unified CLI), `mitiflow-gateway` (Kafka protocol stub), `mitiflow-bench`
(benchmarks), `mitiflow-ui` (Svelte dashboard).

## 2. Feature Flags

```toml
[features]
default = ["store"]
store = []                           # EventStore infrastructure, offload, consumer groups
fjall-backend = ["store", "dep:fjall"] # Concrete LSM implementation
wal = ["dep:fjall"]                  # Write-ahead log for durable publisher (planned)
full = ["store", "fjall-backend", "wal"]
```

## 3. Core Types

### Event Envelope

```rust
pub struct Event<T: Serialize> {
    pub id: EventId,                    // UUID v7 (time-ordered, globally unique)
    pub timestamp: DateTime<Utc>,
    pub seq: Option<u64>,               // set by publisher on send
    pub payload: T,
    pub key_expr: Option<String>,       // set by subscriber on receive
}

pub struct RawEvent {
    pub id: EventId,
    pub seq: u64,
    pub publisher_id: PublisherId,
    pub key_expr: String,
    pub payload: Vec<u8>,
    pub timestamp: DateTime<Utc>,
}
```

### Attachment (50-byte binary header)

Metadata travels in Zenoh attachments — payload contains only serialized `T`:

```
[seq: u64 (8)] [publisher_id: UUID (16)] [event_id: UUID (16)]
[timestamp_ns: i64 (8)] [urgency_ms: u16 (2)]
```

### Configuration

```rust
EventBusConfig::builder("myapp/events")
    .codec(CodecFormat::Postcard)       // JSON, MsgPack, Postcard (default)
    .cache_size(256)                    // publisher recovery buffer
    .heartbeat(HeartbeatMode::Sporadic(Duration::from_secs(1)))
    .recovery_mode(RecoveryMode::Both)  // Heartbeat + PeriodicQuery
    .recovery_delay(Duration::from_millis(50))
    .max_recovery_attempts(3)
    .num_processing_shards(1)           // parallel gap detector shards
    .event_channel_capacity(1024)       // bounded delivery channel
    .congestion_control(CongestionControl::Block)
    // Store options (feature = "store")
    .store_path(path)
    .watermark_interval(Duration::from_millis(100))
    .durable_timeout(Duration::from_secs(5))
    .compaction_interval(Duration::from_secs(3600))
    // Partition options
    .num_partitions(64)
    .worker_id("worker-1")
    .build()?
```

## 4. Key Abstractions

### EventPublisher

Monotonic per-(partition, publisher) sequencing + in-memory ZBytes cache for recovery.

**Background tasks:** heartbeat beacon, cache queryable, watermark listener (store feature).

| Method | Semantics |
|--------|-----------|
| `publish(&self, event)` | Fast publish, round-robin partition |
| `publish_keyed(&self, key, event)` | Key-based: `hash(key) % num_partitions` → `{prefix}/p/{part}/k/{key}/{seq}` |
| `publish_durable(&self, event)` | Publish + wait for watermark ACK |
| `publish_bytes(&self, bytes)` | Raw bytes, bypasses codec |
| `shutdown(self)` | Graceful: cancel tasks, await handles |

### EventSubscriber

Gap detection + tiered recovery (store → publisher cache → backoff retry).

| Method | Semantics |
|--------|-----------|
| `new(session, config)` | Subscribe to `{prefix}/**` |
| `new_partitioned(session, config, partitions)` | Specific partitions |
| `new_keyed(session, config, key)` | Single key filter: `{prefix}/p/*/k/{key}/*` |
| `new_key_prefix(session, config, prefix)` | Key prefix: `{prefix}/p/*/k/{prefix}/**` |
| `recv<T>(&self)` | Receive + deserialize |
| `recv_raw(&self)` | Receive as RawEvent |
| `offload_events(&self)` | Offload lifecycle channel (store feature) |
| `shutdown(self)` | Graceful shutdown |

### EventStore (Sidecar)

Subscribes to events, persists to pluggable `StorageBackend`, publishes watermarks,
serves queries. Manages offset commits for consumer groups.

### ConsumerGroupSubscriber

Joins via `PartitionManager`, fetches committed offsets, rebalances on membership changes.
Supports `commit_sync()`, `commit_async()`, auto-commit, and generation-based zombie fencing.

### PartitionManager

Rendezvous (HRW) hashing with liveliness-driven rebalancing. Weighted nodes, rack-aware
placement via `NodeDescriptor`. Exposes `partition_for()`, `my_partitions()`, `on_rebalance()`.

## 5. Zenoh Key Expression Patterns

| Pattern | Purpose |
|---------|---------|
| `{prefix}/**` | All events (keyed + unkeyed) |
| `{prefix}/p/{partition}/**` | Specific partition |
| `{prefix}/p/*/k/{key}/*` | Specific key (all partitions) |
| `{prefix}/p/*/k/{key_prefix}/**` | Key prefix (hierarchical) |
| `{prefix}/_publishers/{publisher_id}` | Publisher liveliness |
| `{prefix}/_heartbeat/{publisher_id}` | Heartbeat beacon |
| `{prefix}/_cache/{publisher_id}` | Publisher cache queryable |
| `{prefix}/_store` | Event store queryable |
| `{prefix}/_watermark` | Durability watermark |
| `{prefix}/_workers/{group_id}/{member_id}` | Consumer group member |
| `{prefix}/_agents/{node_id}` | Storage agent liveliness |

**Convention:** `$` in key expressions is reserved for `$*` only. Internal channels use `_` prefix.

## 6. Layer Summary

| Layer | Feature | Status |
|-------|---------|--------|
| **L1: Reliable Bus** | Sequencing, gap detection, auto-recovery, dedup | ✅ |
| **L2: Event Store** | Crash recovery, replay, watermark durability, HLC ordering | ✅ |
| **L3: Partitioning** | HRW hash ring, liveliness rebalancing, consumer groups | ✅ |
| **L4: Key-Based** | Partition affinity, Zenoh-native filtering, compaction | ✅ |
| **L5: DLQ** | Poison message isolation, backoff retries | ✅ |
| **L6: Offload** | Automatic pub/sub → store-query for slow consumers | ✅ |
