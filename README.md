# Mitiflow

Brokerless event streaming built on [Zenoh](https://zenoh.io). Kafka-class
reliability — sequencing, gap detection, durable storage, consumer groups —
with microsecond-latency pub/sub. No brokers, no ZooKeeper, no external
coordinator.

## Features

- **Brokerless** — Zenoh peer-to-peer mesh; no central broker to manage or scale
- **Microsecond latency** — events flow directly between publishers and subscribers via Zenoh
- **Gap detection & recovery** — per-publisher sequence tracking with tiered recovery (store → publisher cache → backoff)
- **Durable publishing** — watermark-confirmed writes with configurable timeout
- **Key-based publishing** — `publish_keyed()` with automatic partition routing and Zenoh-native key filtering
- **Consumer groups** — offset commits with zombie fencing, auto-commit, and rebalancing via rendezvous hashing
- **Event store** — fjall LSM backend with replay ordering (HLC), key index, log compaction, and GC
- **Slow consumer offload** — automatic switchover to store-based catch-up when a subscriber falls behind
- **Dead letter queue** — configurable retry with exponential or fixed backoff
- **Multiple codecs** — JSON, MessagePack, Postcard
- **Zero-copy routing** — 50-byte metadata in Zenoh attachments; payload never deserialized for routing

## Architecture

Mitiflow is **truly brokerless** — there is no central process that routes
messages. Events flow directly from publishers to subscribers over Zenoh's
peer-to-peer mesh. The components that *do* exist serve different roles than a
traditional message broker:

- **Event store** — a **sidecar storage layer**, not a message router. It
  persists events for durability, replay, and gap recovery. Publishers and
  subscribers communicate directly; the store is only consulted when a
  subscriber needs to recover missed events or replay history.
- **Orchestrator** — an **optional operations tool**. It provides config CRUD,
  lag monitoring, and an HTTP admin API. The system is fully functional without
  it — it makes things easier to operate, but never makes them possible.
- **Storage agent** — manages partition-to-node assignment and recovery using
  decentralized Zenoh primitives (liveliness, rendezvous hashing). It does not
  participate in the message path.

```
┌─────────────┐    Zenoh pub/sub    ┌──────────────────────────┐
│  Publisher  │ ──────────────────► │       Subscriber         │
│  (keyed /   │    attachments:     │  ┌─────────────────────┐ │
│   durable)  │    seq, pub_id,     │  │  gap detect,        │ │
│             │    event_id, ts,    │  │  recovery, offload  │ │
│             │    urgency          │  ├─────────────────────┤ │
└──────┬──────┘                     │  │  PartitionMgr       │ │
       │                            │  │  (HRW hash,         │ │
       │ Zenoh pub                  │  │   liveliness)       │ │
       │                            │  └─────────────────────┘ │
       │                            └────────────┬─────────────┘
       │                                         │
       │                               store query / recovery
       ▼                                         ▼
┌──────────────────────────────────────────────────┐
│              EventStore (fjall LSM)              │
│          sidecar — durability & recovery         │
└──────────────────────────────────────────────────┘
```

| Crate | Purpose |
|-------|---------|
| `mitiflow` | Core library — publisher, subscriber, event store, partitions, DLQ |
| `mitiflow-agent` | Storage agent — distributed partition management (not on the message path) |
| `mitiflow-orchestrator` | Optional control plane — config CRUD, lag monitoring, HTTP API (not required for operation) |
| `mitiflow-cli` | Unified CLI binary (`mitiflow agent`, `orchestrator`, `ctl`, `dev`) |
| `mitiflow-emulator` | YAML-driven topology runner and chaos testbed |
| `mitiflow-bench` | Comparative benchmarks (Kafka, NATS, Redis, Redpanda) |

## Quick Start

### As a library

Add to your `Cargo.toml`:

```toml
[dependencies]
mitiflow = { version = "0.1", features = ["full"] }
```

### Basic pub/sub

```rust
use mitiflow::{Event, EventBusConfig, EventPublisher, EventSubscriber};

#[tokio::main]
async fn main() -> mitiflow::Result<()> {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let config = EventBusConfig::builder("demo/sensors")
        .cache_size(100)
        .build()?;

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

### Keyed publishing with partition affinity

```rust
// Events with the same key always go to the same partition
publisher.publish_keyed("user-123", &event).await?;

// Subscribe to a specific key
let sub = EventSubscriber::new_keyed(&session, config, "user-123").await?;

// Subscribe to a key prefix
let sub = EventSubscriber::new_key_prefix(&session, config, "user-").await?;
```

### Durable publishing (store-confirmed)

```rust
let config = EventBusConfig::builder("demo/durable")
    .durable_timeout(Duration::from_secs(5))
    .build()?;
let publisher = EventPublisher::new(&session, config).await?;

// Blocks until the event store confirms persistence
publisher.publish_durable(&event).await?;
```

### Consumer groups

```rust
use mitiflow::{ConsumerGroupSubscriber, ConsumerGroupConfig, CommitMode, OffsetReset};

let group_config = ConsumerGroupConfig {
    group_id: "my-group".into(),
    member_id: "member-1".into(),
    commit_mode: CommitMode::Auto { interval: Duration::from_secs(5) },
    offset_reset: OffsetReset::Earliest,
};

let consumer = ConsumerGroupSubscriber::new(&session, config, group_config).await?;
let event: Event<MyPayload> = consumer.recv().await?;
// Offsets are committed automatically every 5 seconds
```

### Running the platform

```bash
# Install the CLI
cargo install --path mitiflow-cli/

# Dev mode: orchestrator + agent in one process
mitiflow dev --topics "my-topic:8:1"

# Production: separate agent and orchestrator
mitiflow orchestrator --config orchestrator.yaml
mitiflow agent --config agent.yaml

# Admin
mitiflow ctl topics list
mitiflow ctl cluster status
mitiflow ctl diagnose
```

## Feature Flags

| Flag | Default | Description |
|------|---------|-------------|
| `store` | Yes | EventStore + storage backend trait |
| `fjall-backend` | No | Concrete fjall LSM-tree backend |
| `wal` | No | Write-ahead log for durable publisher |
| `full` | No | All of the above |

## Examples

Run any example with:

```bash
cargo run -p mitiflow --features full --example <name>
```

| Example | Description |
|---------|-------------|
| `basic_pubsub` | Pub/sub fundamentals with gap detection |
| `keyed_pubsub` | Key-filtered subscriptions and partition routing |
| `consumer_groups` | Consumer group with partition rebalancing |
| `durable_publish` | Watermark-confirmed persistent writes |
| `event_store` | Store queries, replay, and GC |
| `dead_letter_queue` | DLQ with retry and backoff |
| `slow_consumer_offload` | Automatic store-based catch-up |
| `test_queryable` | Publisher cache recovery via Zenoh queryable |

## Building & Testing

```bash
cargo build -p mitiflow --features full     # Build core with all features
cargo nextest run --features full            # Run tests (preferred)
cargo test --features full                   # Alternative
cargo bench -p mitiflow                      # Criterion micro-benchmarks
cargo doc --no-deps --features full --open   # API documentation
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for development conventions.

## Documentation

Design documents are in [`docs/`](docs/):

| Document | Topic |
|----------|-------|
| [Architecture](docs/02_architecture.md) | System design, crate structure, metadata protocol |
| [Durability](docs/03_durability.md) | Write-ahead log, watermarks, store confirmation |
| [Sequencing & Replay](docs/04_sequencing_and_replay.md) | Sequence model, HLC replay, brokerless constraints |
| [Consumer Groups](docs/11_consumer_group_commits.md) | Offset commits, zombie fencing, auto-commit |
| [Distributed Storage](docs/13_distributed_storage.md) | StorageAgent, reconciler, recovery |
| [Key-Based Publishing](docs/15_key_based_publishing.md) | Key expressions, partition affinity, compaction |
| [Slow Consumer Offload](docs/17_slow_consumer_offload.md) | Offload detection, store catch-up, re-subscribe |

## Roadmap

See [docs/implementation_plan.md](docs/implementation_plan.md) for detailed status and [docs/ROADMAP.md](docs/ROADMAP.md) for planned features.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
