# Comparison with Event Streaming Alternatives

How `mitiflow` compares to established event streaming and message queue systems.

---

## 1. Systems Compared

| System | Architecture | Primary Use Case |
|--------|-------------|-----------------|
| **Zenoh + mitiflow** | Peer-to-peer + optional router | Low-latency event streaming with optional durability |
| **Apache Kafka** | Broker cluster + ZooKeeper/KRaft | Durable event streaming, log-based processing |
| **RabbitMQ** | Broker (AMQP) | Traditional message queuing, task distribution |
| **NATS JetStream** | Server cluster | Lightweight messaging with persistence |
| **Apache Pulsar** | Broker + BookKeeper (2-layer) | Multi-tenant event streaming, geo-replication |
| **Redis Streams** | In-memory server + AOF/RDB | Fast streaming with optional persistence |

---

## 2. Performance Comparison

Data sourced from published benchmarks ([zenoh.io 2023](https://zenoh.io/blog/2023-03-21-zenoh-vs-mqtt-kafka-dds/), [StreamNative 2023](https://streamnative.io/blog/benchmarking-pulsar-nats-jetstream-rabbitmq), [Confluent 2020](https://www.confluent.io/blog/kafka-fastest-messaging-system/)).

### Latency

| System | Typical Latency | Best Case | Notes |
|--------|----------------|-----------|-------|
| **Zenoh (peer mode)** | **10-16 µs** | 7 µs (Bahamut) | No broker hop |
| **Zenoh (brokered)** | 21-41 µs | — | Through router |
| **Kafka** | 5-50 ms | 27 µs (single node, no replication) | Batching + replication overhead |
| **RabbitMQ** | 5-20 ms | 1 ms (low throughput) | Degrades under load |
| **NATS JetStream** | 1-5 ms | Sub-ms (in-memory) | Persistence adds latency |
| **Pulsar** | 5-10 ms (p99) | 2-5 ms | BookKeeper write latency |
| **Redis Streams** | 0.5-2 ms | Sub-ms | In-memory, AOF adds latency |

> **Zenoh is 100-1000x lower latency than traditional brokers.** This is the fundamental architectural advantage: no broker hop in peer mode, zero-copy data path, no serialization overhead.

### Throughput

| System | Peak Throughput | Notes |
|--------|----------------|-------|
| **Zenoh (peer mode)** | **67 Gbps** / ~5M msgs/s (8B payload) | Single machine, 100GbE |
| **Zenoh (brokered)** | 51 Gbps | Multi-machine, 100GbE |
| **Kafka** | ~5 Gbps / 500K-1M msgs/s | With batching, partitioned |
| **RabbitMQ** | ~48K msgs/s | Single-threaded queues |
| **NATS JetStream** | 200K-400K msgs/s | With persistence |
| **Pulsar** | ~2.6M msgs/s | Multi-partition |
| **Redis Streams** | 100K-500K msgs/s | In-memory |

---

## 3. Feature Comparison

### Delivery Guarantees

| Feature | Zenoh + mitiflow | Kafka | RabbitMQ | NATS JS | Pulsar | Redis Streams |
|---------|-------------------|-------|----------|---------|--------|---------------|
| **At-most-once** | ✅ Best-effort channel | ✅ acks=0 | ✅ No ack | ✅ | ✅ | ✅ |
| **At-least-once** | ✅ Reliable channel | ✅ acks=1/all | ✅ Publisher confirm | ✅ AckAll | ✅ | ✅ XACK |
| **In-session dedup** | ✅ mitiflow L1 (seq tracking) | ❌ Consumer-side | ❌ | ✅ AckPolicy | ❌ | ❌ |
| **Exactly-once** | ⚠️ L4 checkpoint | ✅ Idempotent producer | ❌ | ✅ Double-ack | ✅ Transaction | ❌ |
| **Confirmed durable** | ✅ Watermark stream | ✅ acks=all + ISR | ✅ Publisher confirm | ✅ | ✅ | ⚠️ fsync config |

### Durability & Persistence

| Feature | Zenoh + mitiflow | Kafka | RabbitMQ | NATS JS | Pulsar | Redis Streams |
|---------|-------------------|-------|----------|---------|--------|---------------|
| **Built-in durable log** | ❌ (Event Store sidecar) | ✅ Topic log | ⚠️ Lazy queues | ✅ File store | ✅ BookKeeper | ⚠️ AOF/RDB |
| **Configurable retention** | ✅ (Store GC policy) | ✅ Size/time | ✅ TTL/length | ✅ | ✅ | ✅ MAXLEN |
| **Replay from offset** | ✅ Queryable API | ✅ Consumer offset | ❌ (re-queue) | ✅ Deliver policies | ✅ Cursor | ✅ XRANGE |
| **App-specific queries** | ✅ Custom filters | ⚠️ ksqlDB/Streams | ❌ | ❌ | ❌ | ❌ |
| **Survives broker crash** | ✅ (fjall WAL) | ✅ Replicated | ✅ Mirrored queues | ✅ R>1 | ✅ Replicated | ⚠️ AOF config |

### Consumer Groups & Partitioning

| Feature | Zenoh + mitiflow | Kafka | RabbitMQ | NATS JS | Pulsar | Redis Streams |
|---------|-------------------|-------|----------|---------|--------|---------------|
| **Consumer groups** | ⚠️ L3 (app-level) | ✅ Native | ⚠️ Competing consumers | ✅ Queue groups | ✅ Subscriptions | ✅ XREADGROUP |
| **Partitioning** | ⚠️ L3 (hash ring) | ✅ Native (topic partitions) | ❌ | ❌ | ✅ Native | ❌ |
| **Auto-rebalance** | ✅ Liveliness-driven | ✅ Coordinator | ❌ | ⚠️ Limited | ✅ | ❌ |
| **Ordering guarantee** | Per-(partition, publisher) | Per-partition | Per-queue | Per-subject | Per-partition | Per-stream |

### Operations & Deployment

| Feature | Zenoh + mitiflow | Kafka | RabbitMQ | NATS JS | Pulsar | Redis Streams |
|---------|-------------------|-------|----------|---------|--------|---------------|
| **Broker required** | ❌ (peer-to-peer capable) | ✅ Cluster | ✅ Server | ✅ Server | ✅ Broker + BK | ✅ Server |
| **Minimum nodes** | 0 (peers only) | 3 (recommended) | 1 (3 for HA) | 1 (3 for HA) | 3+3 (broker+BK) | 1 (3 for HA) |
| **Edge/IoT support** | ✅ zenoh-pico (MCU) | ❌ | ❌ | ⚠️ Leaf nodes | ❌ | ❌ |
| **Key-expression routing** | ✅ Excellent wildcards | ❌ Fixed topics | ✅ Exchange routing | ✅ Subject hierarchy | ✅ Topic hierarchy | ❌ |
| **Multi-tenancy** | ⚠️ Via key prefixes | ⚠️ ACLs | ✅ Vhosts | ✅ Accounts | ✅ Native | ❌ |
| **Geo-replication** | ⚠️ Via router mesh | ✅ MirrorMaker | ⚠️ Shovel/Federation | ✅ Leaf nodes | ✅ Native | ❌ |
| **Managed cloud offering** | ❌ (Zettascale consulting) | ✅ Confluent, AWS MSK | ✅ CloudAMQP | ✅ Synadia Cloud | ✅ StreamNative | ✅ AWS ElastiCache |

### Rust SDK Quality

| System | Crate | Status |
|--------|-------|--------|
| **Zenoh** | `zenoh` 1.8 | ✅ First-class, maintained by Zenoh team |
| **Kafka** | `rdkafka` (librdkafka wrapper) | ⚠️ C FFI, heavy dependency |
| **RabbitMQ** | `lapin` | ✅ Pure Rust, async |
| **NATS** | `async-nats` | ✅ Pure Rust, well maintained |
| **Pulsar** | `pulsar` | ⚠️ Community maintained |
| **Redis** | `redis` | ✅ Well maintained |

---

## 4. Architecture Comparison

### Traditional Broker Model (Kafka, RabbitMQ, Pulsar, NATS, Redis)

```
Producer → Broker (persistence + routing + delivery) → Consumer
             ↑                                           │
             └────────── ACK ────────────────────────────┘
```

- All messages flow through the broker
- Broker owns persistence and delivery
- Consumer groups are first-class
- Latency = network to broker + broker processing + network to consumer

### Zenoh + mitiflow Model

```
Producer ← peer protocol → Consumer      (direct, µs latency)
    │                           │
    │ put(events/**)            │ subscribe(events/**)
    │                           │
    └───→ Event Store ←─────────┘ (sidecar, subscribes like any consumer)
           │
           ├─ persist (fjall)
           ├─ queryable (replay)
           └─ watermark (durability confirmation)
```

- Messages flow peer-to-peer (or via router for wide-area)
- Persistence is a **subscriber-side concern**, not a transport-level feature
- Consumer groups are application-level
- Latency = peer-to-peer (~µs) or via router (~tens of µs)

---

## 5. Scaling Path: Why This Can Match Kafka

A key architectural advantage: **Zenoh decouples transport from storage.** Kafka bundles them (the broker is both router and log), which means scaling Kafka = scaling brokers. With mitiflow, the transport layer (Zenoh) and the storage layer (Event Store) scale independently.

### 5.1 Partitioned Event Store

Each Event Store instance subscribes to a partition subset. Zenoh's key-expression routing handles the partitioning natively — no coordination between stores:

```
EventStore-0: subscribes to "myapp/events/p{0..3}/**"  → watermark on "myapp/wm/0"
EventStore-1: subscribes to "myapp/events/p{4..7}/**"  → watermark on "myapp/wm/1"
EventStore-2: subscribes to "myapp/events/p{8..11}/**" → watermark on "myapp/wm/2"
   ...
EventStore-N: subscribes to "myapp/events/p{...}/**"   → watermark on "myapp/wm/N"
```

- Each store has its own DB file, its own watermark
- Publishers subscribe to their partition's watermark only
- Stores are fully independent — no consensus needed between them
- Adding capacity = deploy more store instances with new partition ranges

This is **simpler** than Kafka's approach where partition reassignment requires broker coordination through the controller.

### 5.2 Replication via Pub/Sub Fan-Out

Multiple Event Store replicas subscribe to the same key expression and receive
all events simultaneously via Zenoh's native fan-out. Publishers wait for a
quorum of replica watermarks — no Raft, no leader election.

See [05_replication.md](05_replication.md) for the full design, including
recovery protocol, consistency guarantees, and failure modes.

| Aspect | Kafka ISR | mitiflow Pub/Sub Replication |
|--------|-----------|--------------------------|
| **How replicas get data** | Followers fetch from leader | All replicas subscribe independently |
| **Replica lag** | Fetch lag (milliseconds) | **Near-zero** (simultaneous delivery) |
| **Consensus** | KRaft / ISR protocol | Not needed (per-publisher sequences) |
| **Infrastructure** | ZooKeeper or KRaft cluster | Already part of Zenoh |
| **Network cost** | Leader sends N copies | Zenoh multicast / routing handles it |

### 5.3 Log Compaction

Purely a `StorageBackend` concern — keep only the latest value per key:

```rust
pub trait StorageBackend: Send + Sync {
    // ...existing methods...
    
    /// Compact: retain only the latest event per key
    fn compact(&self) -> Result<CompactionStats>;
}
```

fjall is an LSM-tree key-value store — compaction is handled natively by its LSM compaction strategy. A background task triggers manual compaction on threshold or relies on fjall's built-in compaction.

### 5.4 Tiered Storage

Old events can be archived to cold storage via a `StorageBackend` implementation or a GC pipeline:

- Local SSD (fjall) for hot data
- S3/GCS for cold data (via a separate archive process)
- The Event Store's Queryable can transparently query both tiers

### 5.5 What About Exactly-Once Transactions?

The one area where Kafka has a genuine architectural advantage: **cross-partition atomic writes** (Kafka's transactional producer). This requires a distributed transaction coordinator — a fundamentally hard problem.

For mitiflow, single-partition exactly-once is achievable via watermark + sequence dedup. Cross-partition atomic writes would need a 2PC coordinator, which is a separate component — possible but out of scope for the initial crate.

### 5.6 Scaling Summary

| Scaling Concern | Status | Mechanism |
|----------------|--------|-----------|
| Storage capacity | ✅ | Partitioned Event Stores, each with own DB |
| Write throughput | ✅ | Parallel writes across partition stores |
| Read throughput | ✅ | Replicas can serve reads |
| Fault tolerance | ✅ | Replicas via Zenoh fan-out + quorum watermark |
| Log compaction | ✅ | StorageBackend trait method |
| Tiered storage | ✅ | Hot (fjall) → cold (S3) archive pipeline |
| Geo-replication | ✅ | Zenoh router mesh + remote Event Stores |
| Cross-partition txn | ❌ | Would need a 2PC coordinator (out of scope) |

> **Nothing in the architecture blocks Kafka-level scalability.** The scaling path is straightforward: partition the Event Store, replicate via Zenoh's native fan-out, and swap storage backends as needed. Zenoh's transport-storage separation makes this more modular than Kafka's monolithic broker design.

---

## 6. When to Choose What

### Choose Zenoh + mitiflow when:

- **Latency is critical** — µs-scale (real-time systems, robotics, gaming, HFT)
- **Edge/IoT** — constrained devices, peer-to-peer without infrastructure
- **Flexible topologies** — mesh, peer-to-peer, brokered, or hybrid
- **Rust-first** — native, zero-copy, no C FFI bindings
- **Custom query patterns** — query events by app-specific fields, not just offset
- **Transport-storage decoupling** — want to choose/swap storage independently

### Choose Kafka when:

- **Ecosystem is key** — need Kafka Connect, ksqlDB, Schema Registry, mature tooling
- **Multi-language** — need JVM, Python, Go, .NET clients with identical semantics
- **Managed service** — want Confluent Cloud or AWS MSK
- **Exactly-once cross-partition** — need transactional producer/consumer

### Choose NATS JetStream when:

- **Simplicity** — broker-level reliability with minimal config
- **Cloud-native** — Kubernetes-friendly, lightweight

### Choose RabbitMQ when:

- **Complex routing** — fanout, topic, direct, headers exchanges
- **Task queuing** — long-running job distribution with retries

### Choose Pulsar when:

- **Multi-tenancy** — built-in namespace + tenant isolation
- **Geo-replication** — native cross-datacenter replication

---

## 7. Key Trade-offs Summary

| Aspect | Zenoh + mitiflow | Traditional Brokers |
|--------|-------------------|---------------------|
| **Latency** | ✅ 10-16 µs (peer) | ❌ 1-50 ms |
| **Throughput** | ✅ 67 Gbps (peer) | ⚠️ 5 Gbps (Kafka) |
| **Scalability** | ✅ Partition + replicate Event Store | ✅ Native partitions + ISR |
| **Replication cost** | ✅ Free (Zenoh fan-out, quorum watermark) | ⚠️ Followers fetch from leader |
| **Durability** | ✅ Event Store sidecar | ✅ Native |
| **Consumer groups** | ⚠️ L3 (app-level) | ✅ Native |
| **Ecosystem maturity** | ❌ Young (Rust-only) | ✅ Battle-tested, multi-language |
| **Managed offerings** | ❌ Self-hosted only | ✅ Multiple cloud providers |
| **Edge deployment** | ✅ Peer-to-peer, no infra | ❌ Requires server |
| **Broker dependency** | ✅ None | ❌ Always required |
| **Custom queryable storage** | ✅ Full control | ⚠️ Limited |

> **Bottom line:** Zenoh + `mitiflow` achieves Kafka-class reliability and scalability (partitioned + replicated Event Store with quorum-confirmed durability) at **100-1000x lower latency** and **without mandatory broker infrastructure**. The architectural gaps are ecosystem (tooling, connectors, multi-language SDKs) — not scalability or durability. For Rust-based systems where latency matters, this is the strictly better foundation.
