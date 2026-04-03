# Sequencing & Deterministic Replay

**Status:** Implemented — per-(partition, publisher) sequencing + HLC replay index.

How mitiflow assigns sequence numbers and achieves deterministic,
replica-independent replay ordering.

---

## Part 1: Sequence Design — Per-(Partition, Publisher)

### The Problem

A publisher sending to multiple partitions produces **non-contiguous**
sequences within each partition if it uses a single global counter. The gap
detector sees phantom gaps that never fill, breaking the watermark protocol.

### Three Approaches Considered

| Property | A: Per-Partition (Kafka) | B: Per-Publisher | C: Per-(Partition, Publisher) |
|----------|------------------------|-----------------|------------------------------|
| Sequence assignment | Store-assigned | Publisher-assigned | Publisher-assigned |
| Contiguous within partition | Yes (by definition) | **No** (broken) | Yes (per publisher stream) |
| Ordering scope | Total per partition | Total per publisher | Per (partition, publisher) |
| Store coordination | Required (serialize writes) | None | None |
| Watermark complexity | 1 counter/partition | 1 counter/publisher | 1 counter/(partition, publisher) |
| Kafka compatibility | Native | Incompatible | Incompatible (gateway adds mapping) |
| Multi-partition publishers | Works naturally | Breaks gap tracking | Works naturally |

### Decision: Approach C

**Approach C (per-partition, per-publisher)** is the right choice because it
preserves mitiflow's core principle:

> Brokerless, zero-coordination event streaming.

Approach A requires a serialization point at the store — a broker in disguise.
Approach B breaks gap detection when publishers write to multiple partitions.
Approach C provides:

1. **Contiguous sequences** — gap tracking works correctly per (partition, publisher).
2. **Zero coordination** — publishers assign their own sequences, stores persist independently.
3. **Direct durability feedback** — publishers know their seq at publish time and confirm via broadcast watermark.
4. **Natural per-entity ordering** — events for the same key hash to the same partition; with one publisher per entity type, ordering is total for that entity.

### The Brokerless Constraint

Total partition order (every system that offers it — Kafka, NATS JetStream,
Pulsar — has a broker/leader per partition) fundamentally requires a
serialization point. Approach C is the only distributed model that achieves
contiguous sequences **without any broker**. The cost is that consumers see
per-publisher streams rather than a unified log — which is acceptable for the
vast majority of event-driven architectures where per-entity ordering
(not total partition order) is what matters.

### When Total Partition Order Matters

- **changelog/CDC** — conflict resolution needs total order across publishers
- **compacted topics** — "latest per key" needs total order to define "latest"

For most other use cases (event sourcing, telemetry, task queues, IoT, microservice choreography), per-entity ordering from a single publisher suffices.

---

## Part 2: HLC-Based Replay Ordering

### The Replay Problem

Live consumers see events in network delivery order. When replaying from an
`EventStore`, the primary index returns events grouped by publisher — a
completely different interleaving. With replicated stores, each replica may
have a different arrival order. Consumers failing over between replicas would
see inconsistent orderings.

### Requirements

1. **Same-replica determinism** — replaying the same range always returns the same order
2. **Cross-replica determinism** — replaying from any replica returns the same order
3. **Portable consumer cursor** — a position value meaningful across replicas

Store-assigned offsets fail requirement 2 (they're replica-local). The solution
is to order by a value **intrinsic to the event**.

### Solution: HLC Timestamps

Zenoh assigns a Hybrid Logical Clock (HLC) timestamp at publish time. Every
replica receiving the same event sees the **same HLC** — it's intrinsic, not
receiver-dependent.

**Replay sort key:** `(hlc_timestamp, publisher_id, seq)` — globally unique,
deterministic, and replica-independent.

**HLC properties:**
- Monotonic per node (even with NTP adjustments)
- Approximately physical time across nodes (bounded by NTP accuracy, ~1-10ms)
- No coordination needed (each Zenoh session maintains its own HLC)

### Storage Design: Dual Index

The store maintains two keyspaces in fjall:

| Keyspace | Key | Purpose |
|----------|-----|---------|
| `events` (primary) | `[publisher_id:16][seq:8 BE]` | Watermark lookups, dedup |
| `replay` (HLC index) | `[hlc_physical:8 BE][hlc_logical:4 BE][publisher_id:16][seq:8 BE]` | Deterministic ordered replay |
| `keys` (optional) | `[key_hash:8][hlc...][publisher_id:16]` | Key-scoped queries, compaction |

Both indexes are written atomically on each store. The replay index adds ~60
bytes/event overhead. Consumer cursors are HLC timestamps, portable across any
replica.

### Ordering Guarantees

| Guarantee | Provided? |
|-----------|-----------|
| Deterministic per-replica replay | Yes |
| Cross-replica deterministic replay | Yes |
| Consumer cursor portability | Yes |
| Per-publisher strict ordering | Yes |
| Approximately-physical-time ordering | Yes (within NTP accuracy) |
| True causal ordering across publishers | No (physical-time, not vector clock) |
| Identical to live ordering | No (live depends on network timing) |

---

## Part 3: Publisher Lifecycle Management

### The Problem

Publishers are ephemeral — they crash, scale down, redeploy. But the state they
leave behind (gap tracker entries, watermark entries, subscriber gap detector
state) persists. Over time this accumulates and bloats the watermark broadcast.

### Multi-Signal Liveness Detection

Zenoh liveliness tokens detect network reachability, not process liveness. A
network partition revokes the token even though the publisher is alive and will
reconnect. **Liveliness alone is unsafe for lifecycle transitions.**

Solution: use liveliness as a **fast hint** but require **inactivity
confirmation** (no events or heartbeats) before state transitions.

### State Machine

```
              liveliness restored OR event/heartbeat
          ┌─────────────────────────────────────┐
          ▼                                     │
[new] → ACTIVE ─── liveliness revoked ──→ SUSPECTED
          ▲                                     │
          │        event/heartbeat              │
          ├─────────────────────────────────────┘
          │        no activity for              │
          │        inactivity_timeout (5m)      ▼
          │                               DRAINING
          │        event arrives (late)         │
          └─────────────────────────────────────┘
                                                │ drain_grace (2m)
                                                │ no events
                                                ▼
                                           ARCHIVED → GC
```

| State | Behavior |
|-------|----------|
| ACTIVE | Full gap tracking, included in watermark |
| SUSPECTED | Liveliness revoked but timeout not elapsed; full gap tracking continues |
| DRAINING | Inactivity timeout elapsed; grace period for final in-flight events |
| ARCHIVED | Removed from watermark; gap state dropped; events remain in storage |

### Watermark Compaction

Only ACTIVE, SUSPECTED, and DRAINING publishers appear in `CommitWatermark`.
An `epoch` counter increments on each publisher state transition so consumers
can detect changes.

### Publisher Identity

**UUID v7** (time-ordered, always unique) for every publisher instance —
simplest correct solution. The lifecycle protocol handles accumulation.
For true exactly-once across restarts, publishers can query the store for
their last committed seq and resume (requires stable identity + store query).

---

## Part 4: Comparison

### Ordering vs Architecture

| System | Ordering Scope | Requires Broker? | Distributed? |
|--------|---------------|-------------------|--------------|
| Kafka | Per-partition total | Yes (partition leader) | Yes |
| NATS JetStream | Per-stream total | Yes (RAFT leader) | Yes |
| Pulsar | Per-partition total | Yes (broker + BK) | Yes |
| Redis Streams | Per-stream total | Yes (single writer) | Single-node |
| **mitiflow** | **Per-(partition, publisher)** | **No** | **Yes** |

### Replay Ordering

| System | Replay Order | Cross-Replica Deterministic? | Consumer Cursor |
|--------|-------------|------------------------------|-----------------|
| Kafka | Partition offset | Yes (ISR replicates in order) | Integer offset |
| NATS JetStream | Stream sequence | Yes (RAFT replicates in order) | Integer sequence |
| **mitiflow** | **HLC timestamp** | **Yes (same HLC on all replicas)** | **HLC timestamp** |

Kafka/NATS achieve deterministic cross-replica replay because their leader
serializes writes. mitiflow achieves it without a leader by using
publisher-assigned HLC timestamps.
