# mitiflow: Production-Grade Event Streaming for Zenoh

**A Rust crate that brings Kafka-class reliability to Zenoh's microsecond-latency pub/sub.**

---

## Motivation

Modern distributed systems need event streaming with sequencing, durability, consumer groups, and replay. Existing solutions (Kafka, RabbitMQ, NATS JetStream, Pulsar) are broker-dependent and introduce millisecond-scale latency.

[Zenoh](https://zenoh.io) is a zero-overhead pub/sub protocol that achieves **~16µs latency** and **51 Gbps throughput** in peer-to-peer mode — 5-15x faster than Kafka. However, Zenoh lacks built-in consumer groups, durable persistence, and exactly-once semantics.

**`mitiflow`** bridges this gap by layering production-grade event streaming patterns on top of Zenoh's stable core APIs (`put`, `subscribe`, `queryable`, `get`, `liveliness`).

> **Status (2026-03):** All core features implemented. See [implementation_plan.md](implementation_plan.md) for status and [ROADMAP.md](ROADMAP.md) for planned features.

## What You Get

| Feature | How |
|---------|-----|
| **Reliable pub/sub** | Sequence numbers, gap detection, publisher cache, heartbeat |
| **Durable persistence** | Distributed Event Store (sidecar: subscriber + queryable + fjall LSM backend) |
| **Confirmed durability** | Watermark stream — Event Store broadcasts commit progress; publishers block until covered |
| **App-specific queries** | Zenoh Queryable with custom filters (time range, seq range, HLC replay) |
| **Consumer groups** | Partitioned subscription via rendezvous hashing + liveliness-driven rebalancing; store-managed offset commits with generation fencing |
| **Key-based publishing** | Application keys embedded in Zenoh key expressions — automatic partition affinity, Zenoh-native filtering, log compaction |
| **Slow consumer offload** | Automatic pub/sub → store-query demotion when consumers fall behind |
| **Exactly-once** | In-session dedup built-in; cross-restart dedup via persisted sequence checkpoints |
| **Dead letter queue** | Poison message isolation via key-space routing |

## Differentiating Innovation: Watermark Stream

The key innovation is how durability confirmation works **without** per-event ACK queries or producer-side WAL:

```
Producers → put(events/**) → Zenoh → Event Store subscribes → persists → every 100ms:
                                                                  publishes watermark:
                                                                  { publishers: {
                                                                      "pub-A": { committed_seq: 1042, gaps: [] },
                                                                      "pub-B": { committed_seq: 507, gaps: [499] }
                                                                  }}

All publishers subscribe to watermark stream:
  → "my pub_id=pub-A, seq 1040, committed 1042, not in gaps → DURABLE ✓"
```

One batch broadcast serves all publishers. Cost is O(1) regardless of publisher count.

With replicated stores, each replica publishes its own watermark. Publishers wait
for a **quorum** of replicas to confirm — no Raft, no leader election, just Zenoh
pub/sub fan-out as the replication transport.

## Architecture Overview

```
┌──────────┐  put()   ┌─────────────┐  subscribe  ┌──────────────────┐
│ Producer │ ────────→ │    Zenoh    │ ──────────→ │   Event Store    │
│ (fast)   │          │   Network   │             │  (sidecar)       │
│ or       │          │             │             │                  │
│ (durable)│←─ wm ──  │             │ ←── wm ──  │  sub → persist   │
└──────────┘          │             │             │  queryable       │
                      │             │             │  watermark pub   │
                      │             │  subscribe  │                  │
                      │             │ ──────────→ │  ┌────────────┐  │
                      │             │             │  │ fjall / PG │  │
                      └─────────────┘             │  └────────────┘  │
                            │                     └──────────────────┘
                   subscribe│                              ▲
                            ▼                    session.get() replay
                      ┌────────────┐             ┌──────────────────┐
                      │ Consumer 0 │             │ Consumer (crash  │
                      │ (Adv.Sub)  │             │  recovery)       │
                      │ p0, p3, p6 │             └──────────────────┘
                      └────────────┘
```

## Supporting Documents

| Document | Contents |
|----------|----------|
| [01_zenoh_capabilities.md](01_zenoh_capabilities.md) | Zenoh stable API foundation + what mitiflow builds on top |
| [02_architecture.md](02_architecture.md) | Crate design, core types, feature flags |
| [03_durability.md](03_durability.md) | Durability strategies, watermark protocol, quorum confirmation |
| [04_sequencing_and_replay.md](04_sequencing_and_replay.md) | Sequence ordering, HLC replay, brokerless constraints |
| [05_replication.md](05_replication.md) | Pub/sub-based storage replication without Raft |
| [06_comparison.md](06_comparison.md) | Detailed comparison with Kafka, RabbitMQ, NATS, Pulsar, Redis Streams |
| [09_cache_recovery_design.md](09_cache_recovery_design.md) | Tiered recovery: ZBytes publisher cache + EventStore fallback |
| [10_graceful_termination.md](10_graceful_termination.md) | Explicit `shutdown(self)` for publisher, subscriber, and store |
| [11_consumer_group_commits.md](11_consumer_group_commits.md) | Consumer group offset commits, generation fencing |

| [13_distributed_storage.md](13_distributed_storage.md) | Two-tier distributed storage: StorageAgent + Orchestrator |

| [15_key_based_publishing.md](15_key_based_publishing.md) | Key-based publishing: partition affinity, key filtering, compaction |
| [16_dx_and_multi_topic.md](16_dx_and_multi_topic.md) | Multi-topic agent, unified CLI, developer experience |
| [17_slow_consumer_offload.md](17_slow_consumer_offload.md) | Automatic pub/sub → store-query demotion for slow consumers |
| [ROADMAP.md](ROADMAP.md) | Kafka gateway, key-scoped subscribing, and other planned features |

## Quick Start

```rust
use mitiflow::{EventBusConfig, EventPublisher, EventSubscriber, Event};

// Configuration
let config = EventBusConfig::builder("myapp/events")
    .cache_size(256)
    .build()?;

// Publisher
let publisher = EventPublisher::new(&session, config.clone()).await?;

// Fast publish (fire-and-forget into Zenoh reliability pipeline)
let seq = publisher.publish(&Event::new(payload)).await?;

// Key-based publish (automatic partition affinity)
let seq = publisher.publish_keyed("order-123", &Event::new(payload)).await?;

// Durable publish (blocks until watermark confirms persistence)
let seq = publisher.publish_durable(&Event::new(critical_payload)).await?;

// Subscriber (in another process)
let subscriber = EventSubscriber::new(&session, config).await?;
while let Ok(event) = subscriber.recv::<MyPayload>().await {
    process(event).await?;
}

// Consumer group (partitioned, with offset commits)
let group_config = ConsumerGroupConfig {
    group_id: "my-group".into(),
    member_id: None,
    commit_mode: CommitMode::Auto { interval: Duration::from_secs(5) },
    offset_reset: OffsetReset::Earliest,
};
let consumer = ConsumerGroupSubscriber::new(&session, config, group_config).await?;
while let Ok(event) = consumer.recv::<MyPayload>().await {
    process(event).await?;
    // Offsets auto-committed at configured interval
}
```

## Crate Structure

| Crate | Purpose | Status |
|-------|---------|--------|
| `mitiflow` | Core library — publisher, subscriber, event store, partitions, DLQ, offload | ✅ Complete |
| `mitiflow-agent` | Storage member daemon — per-topic workers, rebalancing, membership, recovery | ✅ Complete |
| `mitiflow-orchestrator` | Control plane — config CRUD, lag monitoring, HTTP API, cluster view | ✅ Core done |
| `mitiflow-emulator` | YAML-driven topology runner, chaos testing, process/container backends | ✅ Complete |
| `mitiflow-cli` | Unified CLI — agent, orchestrator, ctl subcommands | ✅ Complete |
| `mitiflow-gateway` | Kafka protocol gateway | 🔧 Stub |
| `mitiflow-bench` | Comparative benchmarks vs Kafka, NATS, Redis, Redpanda | ✅ Complete |
| `mitiflow-ui` | Web dashboard (Svelte 5) | 🔧 Early |

## License

TBD (Apache-2.0 recommended for Zenoh ecosystem compatibility)
