# TODO

Tracks the gap between the design documents and the current implementation.

---

## Critical: Sequence Model Migration

**Status:** Not started
**Ref:** [04_ordering.md](04_ordering.md)

The publisher currently uses a **single global `AtomicU64`** (`next_seq`) for
all events regardless of target partition. This breaks watermark tracking when a
publisher writes to multiple partitions — sequences become non-contiguous within
each partition, causing phantom gaps.

### What needs to change

1. **`EventPublisher`** — replace `next_seq: Arc<AtomicU64>` with a
   `partition_seqs: Arc<RwLock<HashMap<u32, AtomicU64>>>` that maintains one
   counter per partition the publisher writes to.

2. **`publish_to(key, event)`** — hash the key to a partition, look up (or
   create) the counter for that partition, increment it. Return `(partition, seq)`
   instead of a bare `seq`.

3. **`publish(event)` / `publish_bytes(event)`** — these currently write to
   `{key_prefix}/{seq}`. They need a default partition (e.g., partition 0) or
   the caller must specify a key for partitioning.

4. **`HeartbeatBeacon`** — currently carries a single `current_seq`. Needs to
   carry per-partition seq info: `HashMap<u32, u64>` or only beacon per
   partition the publisher has written to.

5. **`wait_for_watermark(seq)`** — must become
   `wait_for_watermark(partition, seq)` to look up the right publisher watermark
   entry in the `CommitWatermark`.

6. **`CachedSample`** — add `partition: u32` field so recovery queries can be
   scoped to the correct partition.

7. **Attachment encoding** — currently encodes `(publisher_id, seq)`. Must add
   `partition` to the attachment so the store knows which partition counter the
   seq belongs to.

8. **Store gap tracker** — `PublisherSeqState` in `FjallBackend` is keyed by
   `PublisherId`. Must be re-keyed to `(PublisherId, partition)` or the store
   must track gaps per (publisher, partition).

### Why it works today

Each benchmark worker creates its own `EventPublisher` and writes to a single
fixed partition. So sequences happen to be contiguous per (publisher, partition).
Any real workload with key-based routing will break.

### Migration strategy

The storage key format `(partition:4BE, publisher_id:16, seq:8BE)` already
encodes the partition — no storage migration needed. Only the publisher-side
counter and watermark lookup need changes.

---

## Scalability Improvements

**Ref:** [improvement_note.md](improvement_note.md)

- [ ] **Shard event processing** — the store's single `tokio::spawn` loop
      serializes all publishers' samples. Shard by `pub_id % N` into N worker
      tasks, each owning its own `GapDetector` slice.

- [ ] **Eliminate per-sample write lock** — `GapDetector` is behind
      `Arc<RwLock>` shared between the main task and the heartbeat task. Since
      the main task is single-threaded, send heartbeat beacons into a channel
      and process them inline as `&mut GapDetector`.

- [x] **Dead publisher eviction** — superseded by publisher lifecycle protocol
      (ACTIVE → SUSPECTED → DRAINING → ARCHIVED → GC).
      See [08_replay_ordering.md](08_replay_ordering.md) § Publisher Lifecycle.

- [ ] **Avoid full payload deser for metadata** — `extract_event_meta`
      deserializes the entire payload to get `id` + `timestamp`. Since these
      already travel in the Zenoh attachment, stop deserializing the payload in
      the store's hot path.

---

## Kafka Gateway

**Status:** Stub only (`main.rs` prints "not yet implemented")
**Ref:** [07_kafka_compatibility.md](07_kafka_compatibility.md), [implementation_plan.md](implementation_plan.md) Phase 5

### Priority assessment

The gateway is architecturally a broker — it serializes writes per partition to
assign Kafka-compatible offsets. This re-introduces the coordination that
mitiflow's native API avoids (see [04_ordering.md](04_ordering.md) § "The
Brokerless Constraint"). Worth building only when Kafka ecosystem access is a
hard requirement for users.

### Phases (from implementation plan)

- [ ] **Phase 5a:** Core produce/consume (6 API keys). MVP: Produce, Fetch,
      Metadata, OffsetCommit, OffsetFetch, ListOffsets.
- [ ] **Phase 5b:** Consumer groups (5 API keys). JoinGroup, SyncGroup,
      Heartbeat, LeaveGroup, FindCoordinator.
- [ ] **Phase 5c:** Admin + polish. CreateTopics, DeleteTopics, DescribeConfigs,
      DeleteRecords.

### Open questions

- Is gateway HA worth investing in? Leader election per partition replicates
  Kafka's coordination plane.
- Should the gateway embed its own `EventStore`, or rely on external stores?
- Can the gateway be stateless by delegating offset assignment to the store
  (Approach A from [04_ordering.md](04_ordering.md))? This pushes the broker
  role to the store instead of the gateway — same coordination cost, different
  location.

---

## Replication

**Status:** Design only
**Ref:** [05_replication.md](05_replication.md), [03_durability.md](03_durability.md) § Quorum Watermarks

- [ ] **Multi-store deployment** — run multiple `EventStore` instances
      subscribing to the same key expressions. Zenoh pub/sub fan-out handles
      data distribution.
- [ ] **Quorum watermark tracker** — `QuorumTracker` that collects watermarks
      from N replicas and computes a quorum watermark (majority agreement).
- [ ] **Publisher quorum confirmation** — `publish_durable()` waits for quorum
      watermark instead of single-store watermark.
- [ ] **Recovery protocol** — a lagging replica queries peers for missing
      events via `session.get()`.
- [ ] **Durability levels** — configurable: `Single` (any 1 store),
      `Quorum` (majority), `All` (every replica).

---

## Deterministic Replay Ordering

**Status:** Implemented
**Ref:** [08_replay_ordering.md](08_replay_ordering.md)

- [x] **HLC timestamp in EventMetadata** — store the Zenoh HLC timestamp
      alongside each event for replica-independent ordering.
- [x] **Replay index in FjallBackend** — secondary keyspace `replay` keyed by
      `(hlc_physical, hlc_logical, publisher_id, seq)` for deterministic
      ordered replay across replicas.
- [x] **`query_replay()` on StorageBackend** — scan replay index with HLC
      range filters, returning events in deterministic HLC order.
- [x] **Publisher lifecycle state machine** — ACTIVE → SUSPECTED → DRAINING →
      ARCHIVED → GC. Multi-signal liveness detection (liveliness token +
      inactivity timeout) to avoid false eviction on network partition.
- [x] **Watermark epoch** — `CommitWatermark` includes epoch counter;
      only ACTIVE/SUSPECTED/DRAINING publishers included.

---

## Testing Gaps

Tests listed in [implementation_plan.md](implementation_plan.md) § 3 that don't
exist yet:

- [ ] `tests/watermark.rs` — watermark broadcast, durable publish timeout, gap
      clearing. (Currently tested inline in `tests/store.rs` but not the full
      suite from the plan.)
- [ ] `tests/gateway.rs` — Kafka protocol round-trip. Blocked on gateway
      implementation.
- [ ] `e2e_*` integration tests — multi-process pub/sub, store crash recovery,
      live rebalance. Currently only single-process integration tests exist.
- [ ] Criterion benchmarks (`mitiflow-bench/benches/`) — the plan specifies
      throughput, latency, store, and watermark benchmark suites. Current
      benchmarks use a custom harness (`bench_pubsub`, `bench_durable`), not
      criterion.

---

## Documentation

- [ ] Update [00_proposal.md](00_proposal.md) § watermark example once
      per-partition sequences are implemented (currently shows per-publisher
      which is correct for now).
- [ ] Add `ARCHITECTURE.md` at repo root as a quick-start pointer to the docs.
- [x] Add [08_replay_ordering.md](08_replay_ordering.md) — deterministic replay
      ordering via HLC and publisher lifecycle management.
