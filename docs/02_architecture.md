# Crate Architecture & Implementation Plan

Technical design for the `mitiflow` crate.

---

## 1. Crate Structure

```
mitiflow/
├── Cargo.toml
├── src/
│   ├── lib.rs                # Re-exports, feature gate macros
│   ├── config.rs             # EventBusConfig (builder pattern)
│   ├── event.rs              # Event<T> envelope
│   ├── error.rs              # Error types
│   │
│   ├── publisher/
│   │   ├── mod.rs            # EventPublisher (publisher + seq + cache queryable)
│   │   └── durable.rs        # DurablePublisher (feature = "wal")
│   │
│   ├── subscriber/
│   │   ├── mod.rs            # EventSubscriber (subscriber + gap detection)
│   │   ├── gap_detector.rs   # Per-publisher sequence tracking + miss detection
│   │   └── checkpoint.rs     # Cross-restart sequence checkpoint
│   │
│   ├── store/                # feature = "store"
│   │   ├── mod.rs            # EventStore (sidecar process)
│   │   ├── backend.rs        # StorageBackend trait + fjall impl
│   │   ├── query.rs          # Selector → QueryFilters parsing
│   │   └── watermark.rs      # CommitWatermark type + broadcast logic
│   │
│   ├── partition/            # feature = "partition"
│   │   ├── mod.rs            # PartitionManager
│   │   ├── hash_ring.rs      # Rendezvous (HRW) hashing
│   │   └── rebalance.rs      # Liveliness-driven rebalancing
│   │
│   └── dlq.rs                # DeadLetterQueue
│
├── examples/
│   ├── basic_pubsub.rs       # Minimal publish + subscribe
│   ├── event_store.rs        # Running the Event Store sidecar
│   ├── durable_publish.rs    # Watermark-confirmed publish
│   └── partitioned.rs        # Consumer group with rebalancing
│
└── tests/
    ├── reliability.rs        # Ordering + gap recovery
    ├── store.rs              # Persistence + query
    ├── watermark.rs          # Durable publish ACK flow
    └── partition.rs          # Rebalance on join/leave
```

## 2. Feature Flags

```toml
[features]
default = ["store"]
store = ["dep:fjall"]           # EventStore + fjall backend + watermark
wal = ["dep:fjall"]             # DurablePublisher with local WAL
partition = []                 # PartitionManager + hash ring
full = ["store", "wal", "partition"]
```

## 3. Dependencies

```toml
[dependencies]
zenoh = "1.8"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
tokio = { version = "1", features = ["rt", "time", "sync", "macros"] }
tracing = "0.1"
thiserror = "2.0"
uuid = { version = "1", features = ["v7", "serde"] }
chrono = { version = "0.4", features = ["serde"] }
flume = "0.11"                  # MPMC channels for watermark broadcast

fjall = { version = "2", optional = true }
```

---

## 4. Core Types

### Event Envelope

```rust
/// Generic event wrapper with metadata.
/// UUID v7 gives time-ordered, globally unique IDs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event<T: Serialize> {
    pub id: Uuid,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub seq: Option<u64>,           // app-level seq (distinct from Zenoh's)
    pub payload: T,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_expr: Option<String>,   // filled on receive
}

impl<T: Serialize> Event<T> {
    pub fn new(payload: T) -> Self { /* uuid::Uuid::now_v7(), Utc::now() */ }
}
```

### Configuration

```rust
pub struct EventBusConfig {
    // Core
    pub key_prefix: String,                         // e.g., "myapp/events"

    // Publisher
    pub cache_size: usize,                          // default: 10_000
    pub heartbeat: HeartbeatMode,                   // Periodic(1s) | Sporadic(1s) | None
    pub congestion_control: CongestionControl,      // default: Block

    // Subscriber
    pub recovery_mode: RecoveryMode,                // Heartbeat | PeriodicQuery(5s) | Both
    pub history_on_subscribe: bool,                 // default: true

    // Store (feature = "store")
    pub store_key_prefix: Option<String>,            // default: "{key_prefix}/store"
    pub store_path: Option<PathBuf>,                 // fjall directory path
    pub watermark_key: Option<String>,               // default: "{key_prefix}/$watermark"
    pub watermark_interval: Duration,                // default: 100ms
    pub durable_timeout: Duration,                   // default: 5s

    // Partition (feature = "partition")
    pub num_partitions: Option<u32>,                 // default: 64
    pub worker_id: Option<String>,
    pub worker_liveliness_prefix: Option<String>,    // default: "{key_prefix}/$workers"
}
```

---

## 5. Key Abstractions

### EventPublisher

```rust
pub struct EventPublisher {
    publisher: Publisher,              // Zenoh core publisher
    cache: Arc<RwLock<VecDeque<CachedSample>>>,  // in-memory cache for recovery
    cache_queryable: Queryable,        // serves cache to subscribers on gap
    next_seq: AtomicU64,               // monotonic sequence counter
    publisher_id: Uuid,                // unique publisher identity
    config: EventBusConfig,
    watermark_rx: flume::Receiver<CommitWatermark>,
}

impl EventPublisher {
    pub async fn new(session: &Session, config: EventBusConfig) -> Result<Self>;

    /// Strategy A: fast publish into Zenoh reliability pipeline
    pub async fn publish<T: Serialize>(&self, event: &Event<T>) -> Result<u64>;

    /// Strategy B: publish + block until watermark confirms durability
    pub async fn publish_durable<T: Serialize>(&self, event: &Event<T>) -> Result<()>;

    /// Publish to explicit partition key
    pub async fn publish_to<T: Serialize>(&self, key: &str, event: &Event<T>) -> Result<u64>;
}
```

### EventSubscriber

```rust
pub struct EventSubscriber {
    subscriber: Subscriber,            // Zenoh core subscriber
    gap_detector: GapDetector,         // per-publisher seq tracking
    session: Session,                  // for recovery queries
    checkpoint: Option<SequenceCheckpoint>,
}

impl EventSubscriber {
    pub async fn new(session: &Session, config: EventBusConfig) -> Result<Self>;
    pub async fn recv<T: DeserializeOwned>(&self) -> Result<Event<T>>;
    pub fn stream<T: DeserializeOwned>(&self) -> impl Stream<Item = Result<Event<T>>>;
    pub fn on_miss(&mut self, handler: impl Fn(MissInfo) + Send + 'static);
    pub async fn ack(&self, event_id: &Uuid) -> Result<()>;
}
```

### EventStore (Sidecar)

```rust
pub struct EventStore {
    session: Session,
    backend: Box<dyn StorageBackend>,
    config: EventBusConfig,
}

pub trait StorageBackend: Send + Sync {
    fn store(&self, key: &str, event: &[u8], metadata: EventMetadata) -> Result<()>;
    fn query(&self, filters: QueryFilters) -> Result<Vec<StoredEvent>>;
    fn committed_seq(&self) -> u64;
    fn gap_sequences(&self) -> Vec<u64>;
    fn gc(&self, older_than: chrono::DateTime<chrono::Utc>) -> Result<usize>;
}

impl EventStore {
    pub async fn new(session: &Session, config: EventBusConfig) -> Result<Self>;

    /// Run forever: subscribe + queryable + watermark loops
    pub async fn run(&self) -> Result<()>;

    /// Run with custom query filter
    pub async fn run_with_filter<F>(&self, filter: F) -> Result<()>
    where F: Fn(&StoredEvent, &QueryFilters) -> bool + Send + Sync + 'static;
}
```

### PartitionManager

```rust
pub struct PartitionManager {
    hash_ring: ConsistentHashRing,
    my_id: String,
    my_partitions: Arc<RwLock<Vec<u32>>>,
}

impl PartitionManager {
    pub async fn new(session: &Session, config: EventBusConfig) -> Result<Self>;
    pub fn partition_for(&self, key: &str) -> u32;
    pub fn my_partitions(&self) -> Vec<u32>;
    pub fn on_rebalance(&self, cb: impl Fn(&[u32], &[u32]) + Send + 'static);
    pub fn subscription_key_expr(&self) -> String;
}
```

---

## 6. Implementation Phases

### Phase 1: Core Pub/Sub (MVP)
- `Event<T>` envelope, `EventBusConfig` builder
- `EventPublisher` with sequence numbering, publisher cache queryable, heartbeat
- `EventSubscriber` with gap detection, recovery via `session.get()`
- `GapDetector` for per-publisher sequence tracking
- Error types
- Tests: ordering, gap recovery
- Example: `basic_pubsub.rs`

### Phase 2: Event Store + Watermark
- `StorageBackend` trait with `committed_seq()` / `gap_sequences()`
- `FjallBackend` implementation
- `CommitWatermark` type
- `EventStore`: subscribe + queryable + watermark loops
- `QueryFilters` parsing from Zenoh selectors
- `publish_durable()` on publisher side
- GC support
- Tests: persistence, query, watermark ACK
- Examples: `event_store.rs`, `durable_publish.rs`

### Phase 3: Partitioned Consumer Groups
- `ConsistentHashRing` (rendezvous hashing)
- `PartitionManager` with liveliness-driven membership
- Rebalance logic + dynamic subscription management
- Tests: rebalancing on join/leave
- Example: `partitioned.rs`

### Phase 4: Cross-Restart Dedup + DLQ
- `SequenceCheckpoint` (persisted per-publisher seq tracking)
- `DeadLetterQueue` (retry tracking + DLQ key routing)
- Optional `DurablePublisher` (feature = "wal")
- Tests: dedup after restart, DLQ flow

---

## 7. Layer Summary

| Layer | Feature | Complexity | When to Use |
|-------|---------|------------|-------------|
| **L1: Reliable Bus** | Sequencing, gap detection, auto-retransmission, in-session dedup | Low | Always |
| **L2: Event Store** | Crash recovery, replay, app queries, watermark durability | Medium | When events must survive restarts |
| **L3: Partitioning** | Exclusive processing, load distribution | Medium-High | Multiple consumers on shared stream |
| **L4: Dedup** | Cross-restart exactly-once | Low | If rolling restarts cause duplicates |
| **L5: DLQ** | Poison message isolation | Low | When some events may be unprocessable |
