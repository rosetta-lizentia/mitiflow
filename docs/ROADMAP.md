# Roadmap — Future Features

Features designed but not yet implemented. Each section includes rationale
and a summary of the design; full design details are preserved in the linked
archived documents or inline below.

---

## 1. Kafka Protocol Gateway

**Priority:** Medium — only needed when Kafka ecosystem access is a hard requirement.

**Crate:** `mitiflow-gateway` (stub exists, prints "not yet implemented")

A Kafka wire protocol translation layer enabling existing Kafka clients
(`rdkafka`, `kafka-console-*`, Kafka Connect) to work against mitiflow's
Zenoh + EventStore backend. The gateway is architecturally a **broker** — it
serializes writes per partition to assign Kafka-compatible offsets, which
re-introduces the coordination that native mitiflow avoids.

### Concept mapping

| Kafka | mitiflow |
|-------|----------|
| Topic | Key expression prefix |
| Partition | `p/{N}` suffix |
| Offset | Gateway-assigned monotonic counter (or HLC-to-integer encoding) |
| Consumer Group | `PartitionManager` + store-managed offset commits |
| ISR | Zenoh pub/sub fan-out to store replicas |
| acks=all | `publish_durable()` (watermark confirmation) |

### Implementation phases

| Phase | Scope | API keys |
|-------|-------|----------|
| 5a: Core produce/consume | Produce, Fetch, Metadata, OffsetCommit/Fetch, ListOffsets | 6 |
| 5b: Consumer groups | FindCoordinator, JoinGroup, SyncGroup, Heartbeat, LeaveGroup | 5 |
| 5c: Admin | CreateTopics, DeleteTopics, DescribeConfigs, DeleteRecords | 4 |

### Key tradeoff

Native mitiflow clients bypass the gateway entirely and get µs-latency
brokerless pub/sub. Kafka clients get familiar offset semantics at the cost
of a centralized write path. The gateway exists for interop, not as the
primary API.

### Not supported (initially)

- Exactly-once / transactions (needs 2PC coordinator)
- Kafka Streams API (client library, not a protocol feature)
- Schema Registry (separate service, can proxy to Confluent)

**Full design:** archived in `docs/archive/07_kafka_compatibility.md` (removed during docs cleanup; see git history for the original)

---

## 2. Key-Scoped Subscribing — Mode 2 (Store-Mediated)

**Priority:** Medium — needed for complete key-filtered event sourcing.

**Ref:** [15_key_based_publishing.md](15_key_based_publishing.md)

Mode 1 (live passthrough via `new_keyed()` / `new_key_prefix()`) exists
today — microsecond latency, event_id dedup, no gap detection. Mode 2 adds
**pull-based store queries** for consumers that need completeness and ordering.

### Design: Two subscriber modes

| Property | Mode 1: Live Passthrough | Mode 2: Pull-Based (KeyedConsumer) |
|----------|-------------------------|-----------------------------------|
| Latency | Microsecond | poll_interval (~100ms) |
| Ordering | Per-publisher FIFO | Deterministic HLC order |
| Completeness | Best-effort | Guaranteed (store has all events) |
| Store dependency | None | Required |
| Cursor | None (stateless) | HLC timestamp (cross-replica portable) |

### KeyedConsumer API (planned)

```rust
let mut consumer = KeyedConsumer::builder(&session, config)
    .key("order-123")           // or .key_prefix("user/42")
    .poll_interval(Duration::from_millis(100))
    .build().await?;

loop {
    let events = consumer.poll().await?;
    for event in &events { process(event); }
    consumer.commit("my-group").await?;
}
```

### Implementation phases

1. **Store key-scoped replay** — add `key`/`key_prefix` to `ReplayFilters`, implement `query_replay_keyed()` in FjallBackend
2. **KeyedConsumer** — poll-based consumer with HLC cursor, builder pattern
3. **Consumer group key offsets** — `KeyedOffsetCommit` type, HLC-based offset storage

### Why pull instead of hybrid

For ordered delivery, a hybrid (live + catch-up) approach has effective latency
equal to the catch-up interval anyway. By separating Mode 1 (fast, unordered)
and Mode 2 (poll, ordered), the tradeoff is explicit.

---

## 3. Quorum Durability

**Priority:** Low — single-store durability covers most deployments.

**Ref:** [05_replication.md](05_replication.md), [03_durability.md](03_durability.md)

Currently `publish_durable()` waits for a single store's watermark. Quorum
durability would wait for N/2+1 replicas.

### Planned items

- `QuorumTracker` — collects watermarks from N replicas, computes quorum watermark
- `publish_durable()` upgrade — wait for quorum instead of single-store watermark
- Configurable `DurabilityLevel`: `Single`, `Quorum`, `All`

---

## 4. Orchestrator HA

**Priority:** Low — single orchestrator handles most topologies.

Leader election via Zenoh liveliness + lowest UUID. Multiple replicas subscribe
to the same control-plane key expressions; leader handles write ops, all
replicas serve reads.

---

## 5. Other Deferred Items

| Item | Notes |
|------|-------|
| OpenTelemetry integration | `tracing-opentelemetry` + `opentelemetry-prometheus` metrics export |
| `mitiflow dev` subcommand | Co-locate orchestrator + agent + Zenoh router in one process for local dev |
| Auto-drain on node failure | Automatic override generation when a node goes offline |
| Rebalance advisor | Load-aware override generation |
| Topic data deletion | Orchestrator triggers on-disk cleanup on agents |
| Emulator phases 4-5 | Docker/container backend, advanced chaos with netem |
| GUI E2E tests | Playwright browser tests for the Svelte dashboard |
