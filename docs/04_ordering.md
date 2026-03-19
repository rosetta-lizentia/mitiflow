# Sequence & Ordering Design Discussion

## The Problem

mitiflow currently assigns sequence numbers **per-publisher** (one global `AtomicU64`
per `EventPublisher`). A publisher's sequence counter increments regardless of which
partition or key the event targets.

Consider a publisher sending to 4 partitions round-robin:

```
Publisher P1 (single counter):
  seq=0 → partition 2
  seq=1 → partition 0
  seq=2 → partition 2
  seq=3 → partition 1
  seq=4 → partition 3
  seq=5 → partition 0
```

What the **store** sees per partition:

| Partition | Sequences from P1 |
|-----------|-------------------|
| 0         | 1, 5              |
| 1         | 3                 |
| 2         | 0, 2              |
| 3         | 4                 |

The sequences within each partition are **non-contiguous**. The gap tracker for
partition 0 sees seq 1 arrive, then seq 5 — it records 0, 2, 3, 4 as gaps.
Those gaps never fill because those sequences landed in other partitions.
The `committed_seq` for P1 on partition 0 is stuck at 1 forever.

This breaks the watermark protocol. The publisher calls
`wait_for_watermark(seq=5)` but the per-publisher watermark on partition 0
reports `committed_seq=1, gaps=[0,2,3,4]` — so `is_durable(P1, 5)` returns
false: seq 5 > committed_seq 1.

In the current benchmark this hasn't surfaced because:
- Each bench worker creates its own `EventPublisher`
- Each publisher writes to a single fixed partition (partition 0)
- So sequences _happen to be_ contiguous per (publisher, partition)

But this is fragile — any real workload with key-based partitioning will break.

---

## Three Approaches

### Approach A: Per-Partition Sequence (Kafka model)

The **store** assigns a monotonic offset to each event within a partition,
regardless of which publisher sent it.

```
Partition 0 store:
  offset=0 ← from P1
  offset=1 ← from P2
  offset=2 ← from P1
  offset=3 ← from P3
```

**How it works:**
- Publishers do NOT assign sequence numbers. They send events with a key, the
  partition is determined by hash, and the store assigns the offset on write.
- The watermark becomes `{partition → committed_offset}` — a single number per
  partition, no publisher dimension.
- Durability confirmation: publisher sends event, store assigns offset, watermark
  eventually covers that offset.

**Ordering guarantee:** Total order within a partition across all producers.

**Pros:**
- Simplest mental model — matches Kafka exactly.
- Contiguous by definition (store assigns sequentially).
- Watermark is trivial: one counter per partition.
- Consumer replay is straightforward: "give me everything after offset N in
  partition P."
- Kafka gateway maps directly.

**Cons:**
- **Requires coordination at the store.** The store must serialize writes within
  a partition to assign offsets atomically. With a single `EventStore` per
  partition this is fine (mutex or atomic), but horizontal scaling of stores for
  the same partition requires consensus (similar to Kafka's partition leader).
- **Durability feedback is indirect.** Publisher sends an event but doesn't know
  its offset until the store publishes watermark. To confirm "my specific event
  is durable," the publisher needs either:
  - (a) A per-event ACK from the store returning the assigned offset, or
  - (b) An event ID in the watermark so the publisher can match.
  - (c) The publisher doesn't care about which offset — it waits for "my event_id
    is stored" rather than "seq N is stored."

  Option (c) changes the durability protocol from sequence-based to ID-based.
  Option (a) is effectively a point-to-point ACK per event, losing the broadcast
  advantage of the watermark pattern.

- **Per-publisher ordering is lost.** If publisher P1 sends events A, B, C to the
  same partition, they will be stored in _arrival_ order at the store, which may
  differ from _publish_ order under concurrent access. However, within a single
  Zenoh session, `put()` calls to the same key expression preserve FIFO order, so
  single-publisher ordering is maintained in practice — just not enforced by the
  sequence protocol.

**Use cases:**
- Event sourcing / log compaction (need total partition order).
- Kafka-compatible gateway (offset semantics).
- Any consumer that replays "from offset N" without caring who produced.

---

### Approach B: Per-Publisher Sequence (current model)

Each publisher maintains its own `AtomicU64`. The store keys events by
`(partition, publisher_id, seq)` and tracks watermarks per publisher.

**Ordering guarantee:** Strict order per publisher (across all partitions, for
events from that single publisher).

**Pros:**
- No coordination needed — each publisher independently assigns sequences.
- Publisher can confirm durability without store-assigned offsets.
- Natural fit for actor-model / microservice architectures where each service
  instance is a publisher and you want "all events from service X in order."

**Cons:**
- **Sequences are non-contiguous per partition** (the core problem described
  above). If a publisher writes to multiple partitions, the gap tracker for any
  single partition sees phantom gaps.
- **Consumer replay is awkward.** "Give me events from partition 0 after
  sequence N" is ambiguous — sequence N from _which_ publisher? Consumers must
  track position per (partition, publisher) pair.
- **New publisher discovery.** The store/subscriber must learn about new
  publishers dynamically. If a publisher appears and starts at seq 0, the store
  handles it. But subscribers waiting for "all publishers caught up" need to know
  the set of publishers.
- **Kafka incompatible.** Kafka has one offset space per partition, not per
  producer. The gateway would need to maintain a mapping layer.

**Use cases:**
- Tracing / audit: "show me every event from publisher X in order."
- Exactly-once per publisher (dedup by publisher_id + seq).
- Systems where each publisher is a distinct data source and consumers subscribe
  to specific publishers.

---

### Approach C: Per-(Partition, Publisher) Sequence

Each publisher maintains a **separate counter per partition**. The publisher
hashes the event key to a partition, then increments that partition's counter.

```
Publisher P1:
  partition_seqs: {0: AtomicU64, 1: AtomicU64, 2: AtomicU64, ...}

  event with key "order-123" → partition 2 → seq = partition_seqs[2].fetch_add(1)
  event with key "order-456" → partition 0 → seq = partition_seqs[0].fetch_add(1)
  event with key "order-789" → partition 2 → seq = partition_seqs[2].fetch_add(1)
```

What the store sees:

| Partition | Publisher | Sequences |
|-----------|-----------|-----------|
| 0         | P1        | 0, 1, 2, ... (contiguous) |
| 2         | P1        | 0, 1, 2, ... (contiguous) |

**Ordering guarantee:** Strict order per (publisher, partition) pair.

**Pros:**
- **Sequences are contiguous within each (partition, publisher) pair.** The gap
  tracker works correctly — no phantom gaps.
- **No coordination between publishers.** Each publisher independently manages
  its own counters.
- **Durability protocol works.** Watermark tracks `committed_seq` per
  (partition, publisher) pair. `wait_for_watermark(partition, seq)` is
  unambiguous.
- **Consumer replay is clear.** "Give me events from partition 0, publisher P1,
  after seq N" — each stream is independently replayable.
- **Dedup is natural.** `(partition, publisher_id, seq)` is a unique key — same
  as the current storage key format.

**Cons:**
- **Cross-partition ordering for a single publisher is lost.** If publisher P1
  sends event A to partition 0 and event B to partition 1, there is no sequence
  relationship between A and B. If the application needs "all events from P1 in
  global order," it must use timestamps or a separate mechanism.
- **More state in the publisher.** Instead of one `AtomicU64`, each publisher
  maintains one counter per partition it writes to. For high partition counts
  this is a `HashMap<u32, AtomicU64>` — negligible memory, but more complex.
- **Watermark grows.** The watermark includes `(publisher, partition) →
  committed_seq` instead of `publisher → committed_seq`. With P publishers and
  K partitions, the watermark is up to P×K entries (though in practice each
  publisher writes to few partitions).
- **Still not Kafka-compatible.** Kafka has one offset per partition, not per
  (publisher, partition). The gateway still needs a mapping layer.

**Use cases:**
- The natural choice when publishers write to multiple partitions and you need
  contiguous sequences for gap detection and watermark tracking — which is
  exactly mitiflow's architecture.
- Per-key ordering: since all events with the same key go to the same partition,
  and each publisher's sequences within that partition are contiguous, you get
  "events for key X from publisher P are strictly ordered."

---

## Comparison Matrix

| Property | A: Per-Partition | B: Per-Publisher | C: Per-(Partition, Publisher) |
|----------|-----------------|-----------------|------------------------------|
| Sequence assignment | Store-assigned | Publisher-assigned | Publisher-assigned |
| Contiguous within partition | Yes (by definition) | No (broken) | Yes (per publisher stream) |
| Ordering scope | Total per partition | Total per publisher | Per (partition, publisher) |
| Store coordination | Required (serialize writes) | None | None |
| Watermark complexity | 1 counter/partition | 1 counter/publisher | 1 counter/(partition, publisher) |
| Durability feedback | Indirect (store assigns offset) | Direct (publisher knows seq) | Direct (publisher knows seq) |
| Consumer replay | Simple (one offset/partition) | Complex (offset/publisher) | Moderate (offset/publisher/partition) |
| Kafka compatibility | Native | Incompatible | Incompatible (but closer) |
| Dedup key | (partition, offset) | (publisher, seq) | (partition, publisher, seq) |
| Publisher complexity | Minimal (no seq logic) | Minimal (one counter) | Moderate (counter per partition) |
| Multi-partition publishers | Works naturally | Breaks gap tracking | Works naturally |

---

## Which Ordering Guarantee Should We Target?

### Per-Partition Ordering (Approach A)

**"All events in partition P are in a single total order."**

This is **Kafka's model** and the most useful for event-driven architectures:

- **Event sourcing:** Aggregate events for entity X (routed to partition P by
  entity ID) are totally ordered. Any consumer replaying partition P sees the
  same history.
- **Stream processing:** Windowed aggregations, joins, and exactly-once
  processing all rely on a deterministic partition order.
- **Consumer groups:** Each consumer owns a subset of partitions and processes
  them sequentially — the total partition order determines what "sequential"
  means.

The trade-off is that the store must serialize writes. In mitiflow, each
`EventStore` instance handles specific partitions, so this serialization is
local (not distributed). A single `AtomicU64` per partition in the store is
sufficient.

### Per-Publisher Ordering (Approach B, current)

**"All events from publisher P are in a single total order."**

Use cases where this matters:
- **Audit trail per service:** "show me every action taken by service instance X
  in causal order." But even here, timestamps plus event IDs usually suffice.
- **Dedup per publisher:** if the same publisher retransmits, the sequence
  identifies duplicates. But `event_id` (UUID) already provides dedup without
  needing sequence ordering.

In practice, **consumers rarely care which publisher produced an event.** They
care about the _entity_ or _topic_ the event relates to. Per-publisher ordering
is a producer-side concern that doesn't align with consumer-side needs.

### Per-(Partition, Publisher) Ordering (Approach C)

**"Events from publisher P within partition K are strictly ordered."**

This is a middle ground: it fixes the contiguity problem (Approach B's fatal
flaw) without requiring store-side coordination (Approach A's cost). It
preserves the publisher-assigned sequence model, which means:
- The durability protocol (watermark + `wait_for_watermark`) works without
  modification to the feedback loop — publishers still know their own seq.
- No serialization bottleneck at the store.

The downside: consumers see N independent streams per partition (one per
publisher), not one total order. For consumers that want "all events in
partition 0 in order," they must merge N streams. This is doable (merge by
timestamp) but lossy (timestamps from different publishers aren't
causally ordered).

---

## The Brokerless Constraint

mitiflow's ultimate design goal:

> **No coordination, no broker, no single point of serialization.**

Zenoh's peer-to-peer architecture is the foundation — publishers and subscribers
communicate directly (or through stateless routers). The `EventStore` is a
**sidecar** that subscribes to events like any other subscriber, not a
centralized broker that mediates writes.

This constraint fundamentally shapes which approaches are viable.

### Can Approach A Work Without a Broker?

Approach A requires the store to assign a **total order** (monotonic offset)
within each partition. This means:

1. **All writes to partition P must flow through a single sequencer.** Two
   concurrent publishers cannot independently assign offsets — they'd produce
   duplicates or gaps. The store must serialize writes.

2. **"Serialize at the store" is still coordination.** Even though mitiflow's
   `EventStore` is per-partition (not a global broker), the store becomes a
   **partition leader** — the single authority that assigns offsets. This is
   exactly Kafka's model: each partition has one leader broker that serializes
   appends.

3. **The store is on the write path.** With store-assigned offsets, the
   publisher cannot confirm its own sequence number. It must either:
   - Wait for the store to echo back the assigned offset (request-reply = broker
     pattern), or
   - Wait for the watermark to cover its event_id (adds latency, requires the
     store to track event_id → offset mappings).

   Either way, the store is no longer a passive subscriber — it becomes an
   **active participant in the write protocol**. This is the definition of a
   broker.

4. **Horizontal scaling of stores per partition requires consensus.** If two
   `EventStore` replicas handle the same partition (for HA), they need
   distributed consensus to agree on offset assignment. This is what Kafka's ISR
   protocol and Raft/KRaft solve — and it's heavy coordination.

**Verdict:** Approach A is fundamentally incompatible with a brokerless
architecture. The moment you need a total order across multiple producers, you
need a serialization point, which is a broker by another name.

### Why Approach C Preserves Brokerlessness

With per-(partition, publisher) sequences:

- **No coordination between publishers.** Each publisher independently assigns
  its own sequences for each partition. Two publishers writing to partition 0
  produce independent streams: `(P1, 0), (P1, 1), ...` and `(P2, 0), (P2, 1), ...`
- **No coordination between stores.** Multiple `EventStore` replicas can
  independently receive and persist events. Each tracks gaps per (partition,
  publisher) — no agreement on a global offset needed.
- **The store stays passive.** The store subscribes, persists, and publishes
  watermarks. It never assigns sequences. Publishers know their seq at publish
  time and can confirm durability via the broadcast watermark.
- **Durability feedback is broadcast.** The watermark stream is a single `put()`
  per interval that all publishers subscribe to. No per-event ACK, no
  request-reply, no store on the write path.

The trade-off is weaker ordering: consumers see N independent streams per
partition instead of one total order. But this is an inherent property of
brokerless systems — total order requires a sequencer, and a sequencer is a
broker.

---

## How Other Systems Handle This

### Kafka: Per-Partition Total Order (Brokered)

- **Model:** Each partition has a leader broker. All writes go through the
  leader, which assigns a monotonic offset.
- **Ordering:** Total order per partition. Consumers see one linear log.
- **Coordination:** Heavy — leader election (KRaft/ZooKeeper), ISR protocol,
  producer ID + epoch for idempotency.
- **Trade-off:** Strong ordering at the cost of broker dependency and latency.
  Minimum 3-node cluster recommended.

Kafka's model is the gold standard for "one log per partition" semantics. But
it's inherently brokered — the partition leader is a single point of
serialization.

### NATS JetStream: Per-Stream Total Order (Server-Assigned)

- **Model:** Each stream has a leader server in the RAFT group. The leader
  assigns a monotonic sequence to each message.
- **Ordering:** Total order per stream (analogous to Kafka partition, but
  streams can have subjects = multiple "topics" in one ordered log).
- **Coordination:** RAFT consensus for leader election and replication.
- **Trade-off:** Simple mental model (one sequence per stream), but the server
  is a broker. NATS calls it "server" but it's functionally identical to
  Kafka's partition leader.

### Pulsar: Per-Partition Total Order (Split Storage)

- **Model:** Brokers handle routing; BookKeeper handles persistence. Each
  partition has a broker-assigned sequence (entry ID in a ledger).
- **Ordering:** Total order per partition.
- **Coordination:** Broker-level partition ownership + BookKeeper quorum writes.
  Even heavier than Kafka (two layers of coordination).
- **Trade-off:** Decoupled compute/storage, but more operational complexity.

### Redis Streams: Per-Stream Auto-ID (Server-Assigned)

- **Model:** Single-threaded server assigns `<timestamp>-<seq>` IDs to each
  XADD command. Total order within a stream.
- **Ordering:** Total order per stream.
- **Coordination:** None in single-node; Sentinel/Cluster for HA, but no
  cross-shard stream ordering.
- **Trade-off:** Simple, fast (in-memory), but single-writer bottleneck per
  stream.

### Chronicle Queue: Per-Publisher Appender (Brokerless, JVM)

- **Model:** Each writer thread owns an "appender" that writes to a shared
  memory-mapped file. The queue file itself provides total order because appends
  are serialized at the OS page-cache level.
- **Ordering:** Total order within a queue (single machine only).
- **Coordination:** No broker, but relies on single-machine shared memory for
  serialization. Does not work across network nodes.
- **Trade-off:** Ultra-low latency (~1µs), but single-machine — not distributed.

### Zenoh (raw): No Built-In Ordering

- **Model:** Pure pub/sub. No sequence numbers, no persistence, no ordering
  guarantees beyond "messages on the same key expression from the same session
  are FIFO."
- **Ordering:** Per-session FIFO on same key expression. No cross-session
  ordering.
- **Trade-off:** Maximum performance, zero coordination, but applications must
  build their own ordering, dedup, and durability — which is exactly what
  mitiflow does.

### Summary: Ordering vs. Architecture

| System | Ordering Scope | Requires Broker/Leader? | Distributed? |
|--------|---------------|------------------------|--------------|
| Kafka | Per-partition total | Yes (partition leader) | Yes |
| NATS JetStream | Per-stream total | Yes (RAFT leader) | Yes |
| Pulsar | Per-partition total | Yes (broker + BK) | Yes |
| Redis Streams | Per-stream total | Yes (single writer) | Single-node |
| Chronicle Queue | Per-queue total | No (shared memory) | Single-node |
| Zenoh (raw) | Per-session FIFO | No | Yes |
| **mitiflow (C)** | **Per-(partition, pub)** | **No** | **Yes** |

**The pattern is clear:** total partition order always requires a serialization
point. Every distributed system that offers total partition order has a broker
or leader for that partition. mitiflow with Approach C is the only distributed
system in this list that achieves contiguous sequences **without any broker**.

The cost is that consumers see per-publisher streams rather than a unified log.
But this is not a deficiency — it's the necessary trade-off for brokerless
operation.

---

## When Does Per-Partition Total Order Actually Matter?

It's worth examining which use cases truly _require_ total partition order vs.
which merely _assume_ it because Kafka established the pattern.

### Requires Total Order

- **Changelog/CDC:** A consumer replaying "all changes to entity X" needs a
  single ordered history. If two services update the same entity concurrently,
  the partition order determines which write "wins." Without total order, the
  consumer must resolve conflicts itself (CRDT, last-writer-wins, etc.).
- **Transaction log:** Financial ledger entries must be strictly ordered so
  balance computations are deterministic. But in practice, a single entity's
  transactions come from one service, making per-publisher order sufficient.
- **Compacted topics (Kafka):** Log compaction keeps "latest value per key."
  This needs total order to define "latest." With per-publisher streams,
  compaction must consider publisher identity — manageable but more complex.

### Doesn't Require Total Order (Per-Publisher Suffices)

- **Event sourcing (DDD):** Each aggregate has one owning service. Events for
  aggregate X come from one publisher. Per-publisher order within the partition
  is sufficient.
- **Telemetry / metrics:** Ordering is by timestamp, not offset. Consumers
  window by time, not sequence position. Gaps and reordering are tolerated.
- **Task distribution / work queues:** Consumers process items independently.
  Order doesn't matter — only completeness (no lost messages).
- **IoT sensor streams:** Each device is a publisher. Per-device ordering is
  the natural model. Cross-device order is meaningless.
- **Microservice choreography:** Service A publishes "order created," service B
  publishes "payment received." These are independent streams. Consumers
  correlate by entity ID and timestamp, not partition offset.

### The Key Insight

Most applications that use Kafka don't actually need total partition order.
They need **per-entity ordering** — and since Kafka routes entities to
partitions by key hash, and typically one service owns each entity type, the
total order is a side effect of single-publisher-per-entity, not something
consumers rely on.

mitiflow with Approach C provides per-entity ordering when each entity type has
one publisher — which covers the vast majority of event-driven architectures.

---

## Recommendation

**Approach C (per-partition-publisher sequence)** is the right choice for
mitiflow because it aligns with the project's core principle:

> Brokerless, zero-coordination event streaming.

Approach A (per-partition total order) is theoretically cleaner for consumers but
fundamentally requires a serialization point — a broker in disguise. Adopting it
would undermine mitiflow's reason to exist as an alternative to Kafka/NATS/Pulsar.

Approach C provides:

1. **Correct contiguous sequences.** Gap tracking works because each (partition,
   publisher) stream is independently contiguous.
2. **Zero coordination.** Publishers assign their own sequences. Stores persist
   independently. No leader election, no consensus, no serialization bottleneck.
3. **Direct durability feedback.** Publishers know their seq at publish time and
   confirm via broadcast watermark. No store on the write path.
4. **Natural per-entity ordering.** Events for the same entity key hash to the
   same partition. With one publisher per entity type (the common case), ordering
   within the partition is total for that entity.
5. **Kafka gateway is still feasible.** The gateway maintains a per-partition
   offset counter (a thin mapping layer), presenting a unified log view to Kafka
   clients while mitiflow internally uses per-publisher streams.

For the uncommon case where total partition order is genuinely needed (multi-
publisher writes to the same entity), mitiflow can offer an optional **store-
assigned offset index** as an add-on feature — a secondary index that the store
maintains locally, without changing the core brokerless protocol.

---

## Migration Path

The current implementation can evolve incrementally:

1. **Immediate fix: Approach C.** Change the publisher to maintain per-partition
   counters. This fixes the contiguity bug with minimal API change. The store
   key format `(partition, publisher_id, seq)` already supports this — only the
   publisher and watermark need updates.

2. **Optional future: store-assigned offset index.** For Kafka gateway or
   consumers that want a single replay offset, the store can assign a local
   monotonic offset on persist and expose it via query. This is a read-path
   convenience, not a write-path requirement — the core protocol remains
   brokerless.

3. **Kafka gateway mapping.** The gateway translates between Kafka's
   `(partition, offset)` and mitiflow's `(partition, publisher_id, seq)`. It
   maintains a local offset→event mapping, presenting Kafka-compatible semantics
   to clients while preserving mitiflow's coordination-free internals.
