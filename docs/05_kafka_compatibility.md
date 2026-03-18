# Kafka-Compatible API Layer

How mitiflow can expose a Kafka wire protocol-compatible interface, enabling existing Kafka clients and tooling to work against Zenoh + Event Store.

---

## 1. Why Kafka Compatibility?

Kafka's wire protocol is the de facto standard for event streaming. Supporting it means:

- **Zero migration cost** — existing Kafka producers/consumers work by changing the bootstrap URL
- **Ecosystem access** — Kafka Connect, ksqlDB, Schema Registry, and 200+ connectors
- **Multi-language support** — any language with a Kafka client (JVM, Python, Go, .NET, Rust) works immediately
- **Hedging** — users can start with mitiflow and fall back to Kafka without code changes

This is the same approach taken by **Redpanda** (C++, full protocol), **WarpStream** (Go, object storage), and **AutoMQ** (Java, cloud-native) — all are Kafka protocol-compatible but with different storage engines underneath.

---

## 2. Architecture: Kafka Protocol Gateway

mitiflow doesn't replace its native Zenoh API. Instead, a **Kafka Protocol Gateway** sits in front and translates:

```
Kafka Client (any language)
    │
    │ Kafka wire protocol (TCP binary)
    │
    ▼
┌─────────────────────────────────────────────┐
│         mitiflow-gateway                    │
│                                             │
│  ┌────────────────┐   ┌──────────────────┐  │
│  │ Kafka Protocol │   │ Topic → Key Expr │  │
│  │ Decoder/Encoder│   │ Mapper           │  │
│  └──────┬─────────┘   └──────┬───────────┘  │
│         │                    │              │
│  ┌──────▼────────────────────▼───────────┐  │
│  │          Translation Layer            │  │
│  │                                       │  │
│  │  Produce  → EventPublisher.publish()  │  │
│  │  Fetch    → EventSubscriber.recv()    │  │
│  │  Commit   → Checkpoint.ack()          │  │
│  │  Metadata → PartitionManager          │  │
│  └──────┬────────────────────────────────┘  │
│         │                                   │
│         │ native mitiflow API               │
└─────────┼───────────────────────────────────┘
          │
          ▼
    Zenoh Network + Event Store(s)
```

The gateway is a **separate binary** (`mitiflow-gateway`), not part of the core crate. It depends on `mitiflow` and speaks Kafka protocol on one side, Zenoh on the other.

---

## 3. Kafka Protocol Mapping

### 3.1 Concept Mapping

| Kafka Concept | mitiflow Equivalent |
|---------------|-------------------|
| **Cluster** | Zenoh session (peer network) |
| **Broker** | mitiflow-gateway instance |
| **Topic** | Key expression prefix (e.g., `app/events/orders`) |
| **Partition** | Key expression suffix `p{N}` (e.g., `app/events/orders/p0`) |
| **Producer** | `EventPublisher` |
| **Consumer** | `EventSubscriber` |
| **Consumer Group** | `PartitionManager` (L3) |
| **Offset** | Event Store sequence number |
| **ISR / Replication** | Zenoh fan-out to Event Store replicas |
| **Committed Offset** | `CommitWatermark.committed_seq` |
| **Log Compaction** | `StorageBackend::compact()` |

### 3.2 API Key Coverage (Priority Order)

The Kafka protocol defines ~60 API keys. Not all are needed for compatibility. Here's the phased approach:

#### Phase 1: Core Produce/Consume (MVP)

| API Key | Kafka API | mitiflow Mapping |
|---------|-----------|-----------------|
| 0 | **Produce** | `EventPublisher::publish()` or `publish_durable()` (if `acks=all`) |
| 1 | **Fetch** | `EventSubscriber::recv()` (live) or Event Store query (historical) |
| 3 | **Metadata** | `PartitionManager::my_partitions()` + gateway topology |
| 8 | **OffsetCommit** | `SequenceCheckpoint::ack()` |
| 9 | **OffsetFetch** | `SequenceCheckpoint::last_checkpoint()` |
| 2 | **ListOffsets** | Event Store query (earliest/latest seq per partition) |

#### Phase 2: Consumer Groups

| API Key | Kafka API | mitiflow Mapping |
|---------|-----------|-----------------|
| 10 | **FindCoordinator** | Gateway returns self (gateway is the coordinator) |
| 11 | **JoinGroup** | `PartitionManager` registers worker via liveliness |
| 14 | **SyncGroup** | `PartitionManager::my_partitions()` after rebalance |
| 13 | **LeaveGroup** | Drop liveliness token |
| 12 | **Heartbeat** | Zenoh liveliness keepalive |

#### Phase 3: Admin & Metadata

| API Key | Kafka API | mitiflow Mapping |
|---------|-----------|-----------------|
| 19 | **CreateTopics** | Register key expression prefix + create Event Store partition set |
| 20 | **DeleteTopics** | Unregister + GC Event Store data |
| 32 | **DescribeConfigs** | Return `EventBusConfig` values as Kafka config format |
| 42 | **DeleteRecords** | `StorageBackend::gc()` |

#### Out of Scope (Initially)

| API Key | Kafka API | Why |
|---------|-----------|-----|
| 22 | **InitProducerId** | Exactly-once transactions (needs 2PC coordinator) |
| 24 | **AddPartitionsToTxn** | Transactional produce |
| 25 | **AddOffsetsToTxn** | Transactional consume |
| 37 | **CreatePartitions** | Dynamic partition count changes |

---

## 4. Produce Path (Detail)

```
Kafka Client                  Gateway                       mitiflow
    │                            │                              │
    │ ProduceRequest             │                              │
    │ {topic, partition,         │                              │
    │  records[], acks}          │                              │
    │───────────────────────────>│                              │
    │                            │ 1. Map topic+partition       │
    │                            │    → key_expr                │
    │                            │                              │
    │                            │ 2. For each record:          │
    │                            │    Event::new(record.value)  │
    │                            │                              │
    │                            │    if acks=0:                │
    │                            │      publisher.publish()  ──>│ put() to Zenoh
    │                            │                              │
    │                            │    if acks=1:                │
    │                            │      publisher.publish()  ──>│ put() + Block
    │                            │                              │
    │                            │    if acks=all:              │
    │                            │      publisher               │
    │                            │        .publish_durable() ──>│ put() + wait
    │                            │                              │ for watermark
    │                            │                              │
    │ ProduceResponse            │                              │
    │ {partition, offset,        │                              │
    │  error_code}               │                              │
    │<───────────────────────────│                              │
```

### acks Mapping

| Kafka `acks` | mitiflow Behavior | Durability |
|-------------|-------------------|------------|
| `acks=0` | Fire-and-forget, don't wait | At-most-once |
| `acks=1` | `CongestionControl::Block`, wait for Zenoh acceptance | At-least-once (in-network) |
| `acks=all` | `publish_durable()` — wait for watermark | Confirmed durable |

This mapping is natural: `acks=all` in Kafka means "wait until all ISR replicas have the data." In mitiflow, `publish_durable()` means "wait until Event Store confirmed persistence via watermark." Same semantics, different mechanism.

---

## 5. Fetch Path (Detail)

```
Kafka Client                  Gateway                       mitiflow
    │                            │                              │
    │ FetchRequest               │                              │
    │ {topic, partition,         │                              │
    │  fetch_offset, max_bytes}  │                              │
    │───────────────────────────>│                              │
    │                            │ 1. If fetch_offset is        │
    │                            │    behind live stream:       │
    │                            │    → query Event Store    ──>│ session.get()
    │                            │      (historical replay)     │
    │                            │                              │
    │                            │ 2. If fetch_offset is live:  │
    │                            │    → subscriber.recv()    ──>│ EventSubscriber
    │                            │      (real-time stream)      │
    │                            │                              │
    │ FetchResponse              │                              │
    │ {partition, records[],     │                              │
    │  high_watermark}           │                              │
    │<───────────────────────────│                              │
    │                            │  high_watermark =            │
    │                            │    CommitWatermark            │
    │                            │      .committed_seq          │
```

**High Watermark mapping:** Kafka's `high_watermark` in FetchResponse = mitiflow's `CommitWatermark.committed_seq`. This is a direct semantic match — both mean "the highest offset confirmed durable."

---

## 6. Consumer Group Path

Kafka's consumer group protocol (JoinGroup → SyncGroup → Heartbeat → LeaveGroup) maps to mitiflow's liveliness-driven partition manager:

| Kafka Protocol Step | mitiflow Mechanism |
|--------------------|--------------------|
| FindCoordinator | Gateway returns itself as coordinator |
| JoinGroup | Consumer registers via Zenoh liveliness token |
| SyncGroup | `PartitionManager` computes assignment, returns partition set |
| Heartbeat | Zenoh liveliness keepalive (automatic) |
| LeaveGroup | Drop liveliness token → triggers rebalance |
| Rebalance | PartitionManager detects membership change via liveliness watcher |

**Key difference:** Kafka's rebalance is a "stop the world" protocol — all consumers in the group pause during reassignment. mitiflow's liveliness-driven rebalance can be cooperative — consumers only pause for their affected partitions.

---

## 7. Implementation Plan

### Dependencies

```toml
[dependencies]
mitiflow = { version = "0.1", features = ["full"] }
kafka-protocol = "0.12"          # Kafka wire protocol codegen
tokio = { version = "1", features = ["net", "rt", "macros"] }
bytes = "1"
tracing = "0.1"
```

The `kafka-protocol` crate provides auto-generated Rust types for all Kafka request/response messages from the official Kafka protocol JSON specs. This saves implementing the binary encoding manually.

### Module Structure

```
mitiflow-gateway/
├── Cargo.toml
├── src/
│   ├── main.rs              # TCP listener, config, gateway startup
│   ├── connection.rs        # Per-client connection handler
│   ├── protocol/
│   │   ├── mod.rs           # Request dispatcher (API key → handler)
│   │   ├── produce.rs       # ProduceRequest → EventPublisher
│   │   ├── fetch.rs         # FetchRequest → EventSubscriber / Store query
│   │   ├── metadata.rs      # MetadataRequest → topology info
│   │   ├── offset.rs        # OffsetCommit/Fetch → SequenceCheckpoint
│   │   ├── group.rs         # Consumer group protocol → PartitionManager
│   │   └── admin.rs         # CreateTopics, DescribeConfigs
│   ├── mapper.rs            # Topic ↔ key expression mapping
│   └── state.rs             # Shared gateway state (sessions, stores, topics)
```

### Implementation Phases

**Phase 1: Read/Write Path (MVP)**
- TCP listener accepting Kafka protocol connections
- Produce → `EventPublisher::publish()` / `publish_durable()`
- Fetch → `EventSubscriber::recv()` + Event Store replay
- Metadata → static topic/partition list
- Smoke test: `rdkafka` Rust client produces and consumes via gateway

**Phase 2: Consumer Groups**
- FindCoordinator, JoinGroup, SyncGroup, Heartbeat, LeaveGroup
- OffsetCommit/Fetch via SequenceCheckpoint
- Test: multi-consumer group rebalancing via `kafka-console-consumer`

**Phase 3: Admin + Polish**
- CreateTopics, DeleteTopics, DescribeConfigs
- ListOffsets (earliest/latest from Event Store)
- Test: Kafka Connect connector running through gateway

---

## 8. What Kafka Clients Will See

From the perspective of `kafka-console-producer` or any `librdkafka`-based client:

```bash
# Produce
kafka-console-producer \
  --bootstrap-server mitiflow-gateway:9092 \
  --topic orders

# Consume
kafka-console-consumer \
  --bootstrap-server mitiflow-gateway:9092 \
  --topic orders \
  --group my-consumer-group \
  --from-beginning

# Admin
kafka-topics --bootstrap-server mitiflow-gateway:9092 \
  --create --topic orders --partitions 16 --replication-factor 3
```

Under the hood:
- `--topic orders` → key expression `app/events/orders/p{0..15}`
- `--replication-factor 3` → 3 Event Store replicas subscribing per partition
- `--from-beginning` → Event Store query from seq 0
- Consumer group → liveliness-driven PartitionManager

---

## 9. Limitations & Honest Gaps

| Feature | Status | Notes |
|---------|--------|-------|
| Basic Produce/Consume | ✅ Mappable | Natural mapping to publish/subscribe |
| Consumer Groups | ✅ Mappable | Liveliness-driven, cooperative rebalance |
| acks=all durability | ✅ Mappable | Watermark stream = ISR semantics |
| Exactly-once (EOS) | ❌ Not supported | Needs 2PC coordinator (out of scope initially) |
| Kafka Transactions | ❌ Not supported | Cross-partition atomic writes |
| Schema Registry | ⚠️ Separate service | Could proxy to Confluent Schema Registry |
| Log Compaction | ✅ Backend concern | `StorageBackend::compact()` |
| ACL / Auth | ⚠️ Separate | Gateway can implement SASL/PLAIN, delegate to auth service |
| Kafka Streams API | ❌ Not applicable | Kafka Streams is a client library, not a protocol feature |

> The Kafka wire protocol has ~60 API keys, but real-world clients typically use only 10-15 for produce/consume/group workloads. Phase 1 (6 API keys) covers the vast majority of use cases.

---

## 10. Prior Art

| Project | Language | Storage | Kafka Compatibility |
|---------|----------|---------|-------------------|
| **Redpanda** | C++ | Local disk (Raft replicated) | Full protocol, drop-in replacement |
| **WarpStream** | Go | Object storage (S3) | Full protocol, stateless agents |
| **AutoMQ** | Java | Object storage | Kafka fork, native protocol |
| **mitiflow-gateway** | Rust | Zenoh Event Store (pluggable) | Phased: core produce/consume → groups → admin |

The main advantage over these alternatives: **mitiflow's storage layer is fully decoupled from the protocol layer.** You can swap fjall for Postgres, S3, or any custom `StorageBackend` without changing the gateway. And the native Zenoh API remains available for µs-latency paths that bypass the Kafka gateway entirely.
