# mitiflow: Production-Grade Event Streaming for Zenoh

**A reusable Rust crate that brings Kafka-class reliability to Zenoh's microsecond-latency pub/sub.**

---

## Motivation

Modern distributed systems need event streaming with sequencing, durability, consumer groups, and replay. Existing solutions (Kafka, RabbitMQ, NATS JetStream, Pulsar) are broker-dependent and introduce millisecond-scale latency.

[Zenoh](https://zenoh.io) is a zero-overhead pub/sub protocol that achieves **~16µs latency** and **51 Gbps throughput** in peer-to-peer mode — 5-15x faster than Kafka. However, Zenoh lacks built-in consumer groups, durable persistence, and exactly-once semantics.

**`mitiflow`** bridges this gap by layering production-grade event streaming patterns on top of Zenoh's stable core APIs (`put`, `subscribe`, `queryable`, `get`, `liveliness`).

## What You Get

| Feature | How |
|---------|-----|
| **Reliable pub/sub** | Sequence numbers, gap detection, publisher cache, heartbeat (built on stable Zenoh APIs) |
| **Durable persistence** | Distributed Event Store (sidecar: subscriber + queryable + storage backend) |
| **Confirmed durability** | Watermark stream — Event Store broadcasts commit progress; publishers block until covered |
| **App-specific queries** | Zenoh Queryable with custom filters (time range, seq range, payload fields) |
| **Consumer groups** | Partitioned subscription via rendezvous hashing + liveliness-driven rebalancing |
| **Exactly-once** | In-session dedup built-in; cross-restart dedup via persisted sequence checkpoints |
| **Dead letter queue** | Poison message isolation via key-space routing |

## Differentiating Innovation: Watermark Stream

The key innovation is how durability confirmation works **without** per-event ACK queries or producer-side WAL:

```
Producers → put(events/**) → Zenoh → Event Store subscribes → persists → every 100ms:
                                                                  publishes watermark:
                                                                  { committed_seq: 1042, gaps: [1038] }

All publishers subscribe to watermark stream:
  → "my seq 1040, committed 1042, not in gaps → DURABLE ✓"
```

One batch broadcast serves all publishers. Cost is O(1) regardless of publisher count.

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
| [02_architecture.md](02_architecture.md) | Crate design, core types, feature flags, implementation phases |
| [03_durability.md](03_durability.md) | Durability strategies, watermark stream protocol design |
| [04_comparison.md](04_comparison.md) | Detailed comparison with Kafka, RabbitMQ, NATS, Pulsar, Redis Streams |
| [05_kafka_compatibility.md](05_kafka_compatibility.md) | Kafka wire protocol gateway design, API mapping, implementation plan |

## Quick Start (Envisioned API)

```rust
use mitiflow::{EventBusConfig, EventPublisher, EventSubscriber, Event};

// Publisher
let config = EventBusConfig::builder("myapp/events").build();
let publisher = EventPublisher::new(&session, config).await?;

// Fast publish (fire-and-forget into Zenoh reliability pipeline)
publisher.publish(&Event::new(payload)).await?;

// Durable publish (blocks until watermark confirms persistence)
publisher.publish_durable(&Event::new(critical_payload)).await?;

// Subscriber (in another process)
let subscriber = EventSubscriber::new(&session, config).await?;
while let Ok(event) = subscriber.recv::<MyPayload>().await {
    process(event).await?;
}
```

## Implementation Phases

1. **Core pub/sub** — `EventPublisher`, `EventSubscriber`, `Event<T>` envelope
2. **Event Store + Watermark** — sidecar persistence, queryable, watermark stream, `publish_durable()`
3. **Partitioned consumer groups** — rendezvous hashing, liveliness-driven rebalancing
4. **Cross-restart dedup + DLQ** — sequence checkpoints, dead letter routing

## License

TBD (Apache-2.0 recommended for Zenoh ecosystem compatibility)
