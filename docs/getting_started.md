# Getting Started

This guide walks you through setting up Mitiflow — from a minimal "hello world" to production-ready patterns. Each section builds on the last; jump to wherever fits your experience level.

**Prerequisites:** Rust 1.93+ (edition 2024), Cargo. No external services required — Mitiflow is brokerless.

> [!TIP]
> All code in this guide is taken from the runnable [examples/](../mitiflow/examples/) directory. You can run any example with:
> ```bash
> cargo run -p mitiflow --features full --example <name>
> ```

---

## Table of Contents

1. [Installation](#1-installation)
2. [Basic Pub/Sub](#2-basic-pubsub)
3. [Key-Based Publishing](#3-key-based-publishing)
4. [Durable Publishing](#4-durable-publishing)
5. [Consumer Groups](#5-consumer-groups)
6. [Event Store Queries](#6-event-store-queries)
7. [Dead Letter Queue](#7-dead-letter-queue)
8. [Slow Consumer Offload](#8-slow-consumer-offload)
9. [Topic Schema Validation](#9-topic-schema-validation)
10. [Running the CLI](#10-running-the-cli)
11. [Next Steps](#11-next-steps)

---

## 1. Installation

Add Mitiflow to your `Cargo.toml`:

```toml
[dependencies]
mitiflow = { version = "0.1", features = ["full"] }
zenoh = "1"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

**Feature flags** — pick what you need:

| Flag | Default | What it enables |
|------|---------|-----------------|
| `store` | Yes | `EventStore` + storage backend trait |
| `fjall-backend` | No | Concrete fjall LSM-tree persistence backend (implies `store`) |
| `wal` | No | Write-ahead log for durable publisher |
| `full` | No | All of the above |

For a quick start, `features = ["full"]` is recommended.

> **See also:** [Architecture](02_architecture.md) for how these crates fit together.

---

## 2. Basic Pub/Sub

> **Full example:** [`examples/basic_pubsub.rs`](../mitiflow/examples/basic_pubsub.rs)

The simplest Mitiflow program: one publisher, one subscriber, no router required.

```rust
use mitiflow::{Event, EventBusConfig, EventPublisher, EventSubscriber};

#[tokio::main]
async fn main() -> mitiflow::Result<()> {
    // Peer-mode Zenoh session — no broker needed.
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let config = EventBusConfig::builder("demo/sensors")
        .cache_size(100)
        .build()?;

    // Create subscriber first so it's ready when events arrive.
    let subscriber = EventSubscriber::new(&session, config.clone()).await?;
    let publisher = EventPublisher::new(&session, config).await?;

    // Publish
    let event = Event::new(serde_json::json!({"temp": 22.5}));
    publisher.publish(&event).await?;

    // Receive
    let received: Event<serde_json::Value> = subscriber.recv().await?;
    println!("Got: {:?}", received.payload);

    drop(publisher);
    drop(subscriber);
    session.close().await.unwrap();
    Ok(())
}
```

**Key concepts:**

- **`EventBusConfig`** — builder pattern, `"demo/sensors"` is the Zenoh key expression prefix. All publishers and subscribers on the same prefix share an event stream.
- **`Event<T>`** — generic envelope: your serializable payload `T`, plus an auto-generated `EventId` (UUID v7) and optional sequence number.
- **`cache_size`** — how many recent events the publisher keeps in-memory for gap recovery. See [Cache Recovery Design](09_cache_recovery_design.md).
- **Ordering:** always `drop(publisher)` and `drop(subscriber)` before `session.close()`. Background tasks need a clean shutdown path.

> **See also:** [Sequencing & Replay](04_sequencing_and_replay.md) for how sequence numbers and gap detection work.

---

## 3. Key-Based Publishing

> **Full example:** [`examples/keyed_pubsub.rs`](../mitiflow/examples/keyed_pubsub.rs)

Events with the same key are always routed to the same partition, enabling per-key ordering and key-filtered subscriptions.

```rust
let config = EventBusConfig::builder("demo/orders")
    .num_partitions(8)
    .build()?;

let publisher = EventPublisher::new(&session, config.clone()).await?;

// Publish with a key — automatically hashed to a partition.
publisher.publish_keyed("order-123", &event).await?;
publisher.publish_keyed("order-456", &event).await?;

// Subscribe to only "order-123" events.
let filtered = EventSubscriber::new_keyed(&session, config.clone(), "order-123").await?;

// Subscribe to a key prefix (e.g., all user/42/* events).
let prefix_sub = EventSubscriber::new_key_prefix(&session, config, "user/42").await?;
```

**How it works:** The publisher hashes the key to pick a partition, then publishes to `{prefix}/p/{partition}/k/{key}/{seq}`. Key-filtered subscribers use Zenoh's native key expression matching — no server-side filtering needed.

> **See also:** [Key-Based Publishing](15_key_based_publishing.md) for the full 4-phase design (partitioning, store key index, log compaction, subscriber access).

---

## 4. Durable Publishing

> **Full example:** [`examples/durable_publish.rs`](../mitiflow/examples/durable_publish.rs)

`publish_durable()` blocks until an Event Store confirms persistence via a watermark.

```rust
use mitiflow::{EventStore, FjallBackend};

let config = EventBusConfig::builder("demo/durable")
    .durable_timeout(Duration::from_secs(5))
    .watermark_interval(Duration::from_millis(50))
    .build()?;

// Start an in-process event store.
let store_dir = tempfile::tempdir()?;
let backend = FjallBackend::open(store_dir.path(), 0)?;
let mut store = EventStore::new(&session, backend, config.clone());
store.run().await?;

let publisher = EventPublisher::new(&session, config).await?;

// Blocks until the store confirms persistence.
publisher.publish_durable(&event).await?;
```

**What happens under the hood:**
1. Publisher sends the event with urgency flag in the attachment header.
2. Event Store receives and persists it to fjall LSM.
3. Event Store publishes a watermark on `{prefix}/_watermark`.
4. Publisher waits for watermark ≥ this event's sequence, or times out.

> **See also:** [Durability](03_durability.md) for the watermark protocol and strategy matrix, [Replication](05_replication.md) for multi-store quorum writes.

---

## 5. Consumer Groups

> **Full example:** [`examples/consumer_groups.rs`](../mitiflow/examples/consumer_groups.rs)

Kafka-style consumer groups with partition assignment, offset commits, and rebalancing.

```rust
use mitiflow::{
    ConsumerGroupSubscriber, ConsumerGroupConfig,
    CommitMode, OffsetReset,
};

let config = EventBusConfig::builder("demo/consumer_group")
    .num_partitions(4)
    .build()?;

// Manual commit mode — you control when offsets are persisted.
let group_config = ConsumerGroupConfig {
    group_id: "my-group".into(),
    member_id: "member-1".into(),
    commit_mode: CommitMode::Manual,
    offset_reset: OffsetReset::Earliest,
};

let consumer = ConsumerGroupSubscriber::new(&session, config, group_config).await?;

let event: Event<MyPayload> = consumer.recv().await?;
// Process event...
consumer.commit_sync().await?;
```

**Auto-commit mode** — offsets committed automatically on an interval:

```rust
let group_config = ConsumerGroupConfig {
    commit_mode: CommitMode::Auto {
        interval: Duration::from_secs(5),
    },
    ..group_config
};
```

**Partition assignment** uses rendezvous (HRW) hashing via `PartitionManager`. When members join or leave, partitions are automatically rebalanced. No external coordinator required. See the full example for a rebalancing demo.

> **See also:** [Consumer Group Commits](11_consumer_group_commits.md) for offset protocol, zombie fencing, and generation management.

---

## 6. Event Store Queries

> **Full example:** [`examples/event_store.rs`](../mitiflow/examples/event_store.rs)

The Event Store provides replay, time-range queries, key-scoped queries, compaction, and garbage collection.

```rust
use mitiflow::store::QueryFilters;

// Query all stored events.
let all = store.query(&QueryFilters::default()).await?;

// Query events for a specific key.
let key_events = store.query(&QueryFilters {
    key: Some("order-123".into()),
    ..Default::default()
}).await?;

// Log compaction — retain only the latest event per key.
store.compact().await?;

// Garbage collection — remove events older than a threshold.
store.gc(Duration::from_secs(3600)).await?;

// Publisher watermarks — see how far each publisher has persisted.
let watermarks = store.publisher_watermarks();
```

> **See also:** [Architecture](02_architecture.md) for how the store fits in the system, [Distributed Storage](13_distributed_storage.md) for multi-node storage agent deployment.

---

## 7. Dead Letter Queue

> **Full example:** [`examples/dead_letter_queue.rs`](../mitiflow/examples/dead_letter_queue.rs)

Events that fail processing are retried with configurable backoff, then routed to a DLQ if retries are exhausted.

```rust
use mitiflow::{DeadLetterQueue, DlqConfig, BackoffStrategy, RetryOutcome};

let dlq_config = DlqConfig {
    max_retries: 3,
    backoff: BackoffStrategy::Exponential {
        initial: Duration::from_millis(100),
        multiplier: 2.0,
        max: Duration::from_secs(10),
    },
};

let dlq = DeadLetterQueue::new(dlq_config);

// In your processing loop:
match process(&event) {
    Ok(_) => { /* success */ }
    Err(e) => {
        match dlq.submit(event, e).await {
            RetryOutcome::Retry(delay) => { /* will retry after delay */ }
            RetryOutcome::DeadLettered => { /* moved to DLQ */ }
        }
    }
}
```

---

## 8. Slow Consumer Offload

> **Full example:** [`examples/slow_consumer_offload.rs`](../mitiflow/examples/slow_consumer_offload.rs)

When a subscriber can't keep up with the live stream, Mitiflow automatically switches to store-based catch-up, then re-subscribes to live when caught up.

```rust
use mitiflow::OffloadConfig;

let config = EventBusConfig::builder("demo/offload")
    .offload(OffloadConfig {
        enabled: true,
        seq_lag_threshold: 1_000,
        debounce_window: Duration::from_secs(2),
        re_subscribe_threshold: 100,
        ..Default::default()
    })
    .build()?;
```

The subscriber monitors its lag via heartbeats and channel fullness. When both exceed thresholds for the debounce window, it transparently switches to querying the Event Store in batches.

> **See also:** [Slow Consumer Offload](17_slow_consumer_offload.md) for detection logic, debouncing, and adaptive batch sizing.

---

## 9. Topic Schema Validation

Mitiflow includes a distributed topic schema registry that ensures publishers and subscribers agree on wire-level configuration (codec, partition count, key format) before exchanging events.

### Validate against a registered schema

```rust
use mitiflow::TopicSchemaMode;

let config = EventBusConfig::builder("myapp/events/orders")
    .codec(CodecFormat::Postcard)
    .num_partitions(16)
    .schema_mode(TopicSchemaMode::Validate)
    .build()?;

// Fails with TopicSchemaMismatch if local config doesn't match the registry.
let subscriber = EventSubscriber::new(&session, config).await?;
```

### Auto-configure from the registry

```rust
// Load everything from the schema registry — no manual config needed.
let config = EventBusConfig::from_topic(&session, "myapp/events", "orders").await?;
let publisher = EventPublisher::new(&session, config).await?;
```

### Dev mode: register or validate

```rust
let config = EventBusConfig::builder("myapp/events/orders")
    .codec(CodecFormat::Postcard)
    .num_partitions(16)
    .schema_mode(TopicSchemaMode::RegisterOrValidate)
    .build()?;

// First publisher registers the schema; subsequent participants validate against it.
let publisher = EventPublisher::new(&session, config).await?;
```

**Schema modes:**

| Mode | Behavior |
|------|----------|
| `Disabled` | No validation (default, backward-compatible) |
| `Validate` | Fail on mismatch with registered schema |
| `AutoConfig` | Load full config from registry |
| `RegisterOrValidate` | Register if absent, validate if present |

Schemas are persisted by storage agents and/or the orchestrator. The CLI can also register and inspect schemas directly:

```bash
mitiflow ctl schema inspect --key-prefix myapp/events
mitiflow ctl schema register --key-prefix myapp/events --topic orders \
    --codec postcard --partitions 16 --key-format keyed
mitiflow ctl schema validate --key-prefix myapp/events \
    --codec postcard --partitions 16
```

> **See also:** [Topic Schema Registry](18_topic_schema_registry.md) for the full design, deployment modes, and schema evolution rules. [Configuration Reference](configuration.md#schema-validation) for `TopicSchemaMode` details.

---

## 10. Running the CLI

Install the unified CLI:

```bash
cargo install --features full --path mitiflow-cli/
```

### Dev mode (all-in-one)

```bash
# Starts orchestrator + storage agent + Zenoh in one process.
mitiflow dev --topics "my-topic:8:1"
#                       name:partitions:replication_factor
```

### Production (separate processes)

```bash
# 1. Start the orchestrator (optional — provides HTTP admin API)
mitiflow orchestrator --config orchestrator.yaml

# 2. Start storage agent(s)
mitiflow storage --config agent.yaml

# 3. Admin operations
mitiflow ctl topics list
mitiflow ctl topics get my-topic
mitiflow ctl cluster status
mitiflow ctl diagnose --timeout 10
```

> **See also:** [Deployment Guide](deployment.md) for container setup, environment variables, and production configuration.

---

## 11. Next Steps

| What you want to do | Where to go |
|---------------------|-------------|
| Understand the architecture | [Architecture](02_architecture.md) |
| Deploy with containers | [Deployment Guide](deployment.md) |
| Tune performance | [Configuration Reference](configuration.md) |
| Browse design decisions | [Documentation Index](index.md) |
| Run benchmarks | [`mitiflow-bench/`](../mitiflow-bench/) |
| Explore the full API | `cargo doc --no-deps --features full --open` |
