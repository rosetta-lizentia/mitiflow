# Replay API Design

**Status:** Proposed — Rust API for deterministic event replay from the EventStore.

---

## Problem Statement

The `StorageBackend` trait exposes `query_replay(&ReplayFilters) -> Vec<StoredEvent>`,
but there is **no high-level Rust API** for application code to replay events.
Today, consumers must manually construct `ReplayFilters`, own a `FjallBackend`
directly, implement pagination loops, manage HLC cursors, and coordinate offset
commits — all from scratch.

This design proposes an `EventReplayer` type that provides a Kafka-like consumer
experience for historical replay: builder configuration, typed deserialization,
automatic cursor advancement, and optional offset management.

---

## Design Goals

1. **Remote-first** — works over Zenoh queries, no local backend required.
2. **Builder pattern** — consistent with `EventBusConfig`, `KeyedConsumer`.
3. **Typed deserialization** — `recv::<T>()` returns `Event<T>`, not raw bytes.
4. **Deterministic ordering** — always HLC-sorted, cross-replica portable.
5. **Cursor-based pagination** — `poll()` advances an `HlcTimestamp` cursor automatically.
6. **Offset management** — optional commit to consumer group offsets.
7. **Multiple scopes** — all events, partition, key, key prefix, publisher.
8. **Streaming** — `Stream` adapter for `while let Some(event)` patterns.

---

## Options

### Option A: Remote-Only `EventReplayer`

New type that queries the store's Zenoh queryable (`{prefix}/_store`) using
selector-encoded `ReplayFilters`. Owns its cursor, handles pagination internally.

- **Pros:** Clean separation, works from any Zenoh-connected process.
- **Cons:** No fast path for co-located consumers. Requires store queryable running.
- **Migration cost:** Low.
- **Fits when:** Most consumers are remote.

### Option B: Local-Only `ReplayReader`

Thin wrapper around `Arc<dyn StorageBackend>` with typed `poll()` / `recv()` /
`stream()`. Only usable when co-located with the store.

- **Pros:** Lowest possible latency (in-process, no Zenoh round-trip).
- **Cons:** Doesn't work for remote consumers or the CLI.
- **Migration cost:** Low.
- **Fits when:** Consumer runs inside the storage agent.

### Option C: Dual-Mode `EventReplayer` (Recommended)

Single type that abstracts over the data source: remote (Zenoh queries) by
default, with an opt-in local fast path (direct backend access). Same API
regardless of mode.

- **Pros:** Best of both worlds. Single type to learn.
- **Cons:** Two internal code paths to test.
- **Migration cost:** Medium.
- **Fits when:** Both remote and co-located consumer patterns exist — which is
  mitiflow's core value proposition.

---

## Comparison

| Dimension | A: Remote | B: Local | C: Dual |
|-----------|----------|---------|--------|
| Remote access | ✅ | ❌ | ✅ |
| Direct backend speed | ❌ | ✅ | ✅ (opt-in) |
| API surface | 1 type | 1 type | 1 type + 1 trait |
| CLI compatible | ✅ | ❌ | ✅ |
| Latency (remote) | ~1 RTT/poll | N/A | ~1 RTT/poll |
| Latency (local) | N/A | ~µs | ~µs |

### Recommendation

**Option C** — mitiflow's core value is brokerless operation (consumers anywhere
on the network), but the storage agent itself needs replay for consumer group
catch-up and compaction. A dual-mode type serves both without API fragmentation.

---

## Proposed API Surface

Three configuration enums control **what**, **where**, and **how long** to replay:

| Enum | Purpose | Variants |
|------|---------|----------|
| `ReplayScope` | **What** to replay | `All`, `Partition(n)`, `Key(k)`, `KeyPrefix(p)`, `Publisher(id)` |
| `ReplayPosition` | **Where** to start | `Earliest`, `AfterHlc(ts)`, `AfterTime(t)`, `CommittedOffset { group_id }` |
| `ReplayEnd` | **When** to stop | `Bounded { limit }`, `AtHlc(ts)`, `AtTime(t)`, `Tailing { poll_interval }`, `ThenLive` |

The builder wires these together:

```
EventReplayer::builder(session, config)
    .partition(3)                                    // scope
    .start(ReplayPosition::CommittedOffset { group_id: "orders".into() })
    .end(ReplayEnd::Tailing { poll_interval: Duration::from_millis(100) })
    .local_backend(backend)                          // opt-in fast path (Arc<dyn StorageBackend>)
    .build().await?
```

The core consumption methods:

| Method | Returns | Behavior |
|--------|---------|----------|
| `poll()` | `Vec<StoredEvent>` | Batch fetch from store or live (post-transition) |
| `recv::<T>()` | `Result<Event<T>>` | Single event, blocks in tailing / `ThenLive` mode. Returns `Error::EndOfReplay` for bounded modes, consistent with `EventSubscriber::recv()` |
| `recv_raw()` | `Result<RawEvent>` | Same, without deserialization |
| `stream::<T>()` | `impl Stream<Item = Result<Event<T>>>` | Async iterator adapter |
| `into_live()` | `LiveSubscriber` | Explicit handoff (overlap dedup, same gap-free guarantee). Returns a wrapper around `EventSubscriber` that seeds the `GapDetector` with replay-derived checkpoints |
| `seek(hlc)` | — | Jump cursor to arbitrary position |
| `commit(group_id)` | `Result<()>` | Persist HLC cursor as consumer group replay offset |
| `seek_to_committed(group_id)` | `Result<bool>` | Jump to last committed HLC offset |
| `shutdown()` | `Result<()>` | Clean shutdown — cancels background tasks (live subscriber, overlap buffer) |

### Unbounded Replay: Query-then-Subscribe

The most common replay pattern is **unbounded**: query all historical events
from the store, then seamlessly continue into a live subscription with no gap.
The `ReplayEnd::ThenLive` variant automates this — `recv()` / `stream()` never
return `None`; they internally switch from store queries to live subscription.

**The gap problem with naïve handoff:**

A simple "drain store, then subscribe" approach has a race condition:

```
                   naïve approach — CAN LOSE EVENTS
                   ─────────────────────────────────

  EventStore                    Zenoh pub/sub
  ┌──────────────────┐          ┌──────────────────┐
  │ seq 1..1000      │          │ seq 1001..       │
  └───────┬──────────┘          └───────┬──────────┘
          │                             │
     poll() → empty               subscriber declared
          │                             │
          │    ◄── GAP: seq 1001-1005 ──►  published and stored
          │        already in the store     but poll() missed them
          │        AND already passed       because they arrived
          │        through Zenoh before     between the last poll
          │        subscriber existed       and subscribe declaration
```

Events published between the last store poll and subscriber declaration can be
lost — they're already past the Zenoh subscriber (never received) and the
replayer stopped polling (never queried).

**Solution: Overlap-based seamless transition.**

The `EventReplayer` in `ThenLive` mode uses the same proven pattern as the slow
consumer offload's CATCHING_UP → LIVE transition: **subscribe to live first,
keep querying the store, dedup during overlap**.

```
                   seamless approach — NO GAP
                   ──────────────────────────

  EventStore                    Zenoh pub/sub
  ┌──────────────────┐          ┌──────────────────┐
  │ seq 1..1000      │          │ seq 1001..       │
  └───────┬──────────┘          └───────┬──────────┘
          │                             │
     poll() seq 1..1000          ┌──────┘
     poll() seq 1001..1005 ──────┤ subscriber declared HERE
          │                      │  (before store is exhausted)
          │                      │
          │              overlap window:
          │              live events buffered internally
          │              seq ≤ cursor → dedup (already delivered from store)
          │              seq > cursor → deliver
          │                      │
     poll() → empty ────────────┤ store caught up
          │                      │
          │                flush overlap buffer
          │                      │
          └──── fully live ──────┘
```

**How it works (internally):**

1. **Early subscription.** When the replayer detects it is nearing the end of
   stored data (poll returns zero events, i.e., store exhausted for the
   current cursor), it declares the Zenoh subscriber in the background. Live
   events begin arriving into a **bounded** internal holding buffer (capacity
   configurable via `.overlap_buffer_capacity(n)`, default: `event_channel_capacity`).
   If the buffer fills, back-pressure is applied to the live subscriber.
   The trigger condition is configurable via `.subscribe_early_threshold(n)`:
   when set, the subscriber is declared when poll returns fewer than `n`
   events (useful for high-throughput topics where zero-result detection
   would be too late).

2. **Continue draining the store.** The replayer keeps polling the store.
   Events from the store are delivered to the application normally via
   `recv()` / `poll()`.

3. **Dedup overlap.** Events arriving from the live subscriber are held in the
   buffer. When the store is fully drained, the replayer flushes the overlap
   buffer: events with `(publisher_id, seq)` already delivered by the store
   path are silently dropped. Only genuinely new events are delivered.
   (`StoredEvent` does not carry an explicit `partition` field, but
   `(publisher_id, seq)` is globally unique since a publisher writes to
   exactly one partition per event.)

4. **Go fully live.** Once the store is exhausted and the overlap buffer is
   flushed, all subsequent events come from the live Zenoh subscriber. The
   `EventReplayer` internally becomes equivalent to an `EventSubscriber`.

**Why this is safe — two-layer loss prevention:**

The overlap dedup is the primary guarantee, but it has a narrow edge case: an
event published during Zenoh's subscription propagation delay could miss both
paths (subscriber declaration hasn't reached the publisher yet, and the event
isn't stored yet). A second safety layer covers this:

| Layer | Mechanism | What it catches |
|-------|-----------|-----------------|
| **1. Overlap dedup** (primary) | Subscribe before store exhausted, dedup by `(publisher_id, seq)` | 99.9%+ of transition events — any event seen by either path |
| **2. Gap detection** (safety net) | `GapDetector` tracks per-(publisher, partition) sequences; gaps trigger recovery from publisher cache (Zenoh queryable) → store fallback | Rare edge case: event missed by both paths due to subscription propagation latency |

Layer 2 is the same `GapDetector` used during normal live consumption. After
the overlap transition completes, the subscriber's gap detector has been seeded
with per-publisher sequence checkpoints from the replay cursor. Any sequence
gap — including events lost during the transition window — triggers the normal
recovery path: query the publisher's cache queryable first, then fall back to
the store. This is identical to how a regular subscriber recovers from network
drops or publisher restarts.

**Net result:** The combination of overlap dedup + gap detection provides
**at-least-once delivery** across the transition, with dedup ensuring no
duplicates from the overlap. This matches the guarantee of a normal
`EventSubscriber` — no weaker, no stronger.

**API behavior:**

From the application's perspective, nothing changes — `recv::<T>()` and
`stream::<T>()` return events continuously. The transition from store to live
is completely invisible. The returned type remains `EventReplayer` throughout;
internally it holds both the `ReplaySource` and the live subscriber, and
forwards from whichever has the next event.

| `ReplayEnd` variant | Behavior |
|---|---|
| `ThenLive` | Seamless unbounded replay: store queries with early live subscription, overlap dedup, automatic transition. `recv()` / `stream()` never return `None`. |

The `into_live()` explicit method is still available for advanced users who
want manual control over the transition timing, but it uses the same overlap
internally and documents the same gap-free guarantee.

**Key constraint:** `ThenLive` and `into_live()` require the same `EventBusConfig`
(key prefix, codec, partitions) for both the store queries and the live
subscription. The replayer validates this at build time.

### Interaction with Slow Consumer Offload

The `LiveSubscriber` returned by `into_live()` wraps an `EventSubscriber` with
replay-derived `GapDetector` state. It fully supports the slow consumer offload
mechanism ([17_slow_consumer_offload.md](17_slow_consumer_offload.md)).
This works because both features share the same abstraction: **per-(publisher,
partition) sequence checkpoints**.

**Why they compose naturally:**

| Concern | Replay (into_live) | Offload (CATCHING_UP → LIVE) |
|---------|-------------------|------------------------------|
| Cursor format | HLC timestamp | Per-(pub, partition) seq map |
| Gap detector state | Pre-seeded from replay cursor | Pre-seeded from offload cursor |
| Dedup mechanism | Seq comparison vs checkpoint | Seq comparison vs checkpoint |
| Store queries | `query_replay()` via Zenoh | `session.get()` via Zenoh |
| Overlap handling | Subscribe first, dedup overlap | Subscribe first, dedup overlap |
| Loss recovery | GapDetector → publisher cache → store | GapDetector → publisher cache → store |

Both features use the same two-layer approach: overlap dedup for the common
case, `GapDetector` recovery for edge cases. The `into_live()` handoff seeds
the `GapDetector` with per-publisher sequence checkpoints derived from the
replayer's HLC cursor (by looking up the seq at each HLC position). This is
the same state the offload manager would produce after completing its own
CATCHING_UP phase. If the consumer later falls behind and offload triggers,
the offload manager snapshots these same checkpoints as its switchover cursor
— there is no conflict.

**One implementation detail:** The replayer's primary cursor is an `HlcTimestamp`,
but the gap detector and offload manager use per-(publisher, partition) `u64`
sequence numbers. The `into_live()` conversion must resolve this by extracting
the `(publisher_id, seq)` pairs from the last batch of `StoredEvent`s returned
by `poll()`. This is straightforward because every `StoredEvent` carries full
`EventMetadata` including `publisher_id` and `seq`.

---

## Internal Architecture

### Data Source Trait

An internal `ReplaySource` trait abstracts the transport:

| Implementation | When | Transport |
|---------------|------|-----------|
| `ZenohReplaySource` | Default | `session.get("{prefix}/_store/**?after_hlc=...")` |
| `LocalReplaySource` | `.local_backend()` set | `backend.query_replay(filters)` in-process |

### Poll Loop

```
poll()
  → build ReplayFilters from (cursor, scope, batch_size)
  → source.query_replay(filters)
  → sort by (hlc, publisher_id, seq)
  → advance cursor past last event
  → check end condition (bounded / at-time / tailing)
  → return results
```

### Tailing Mode

`ReplayEnd::Tailing` makes `recv()` block indefinitely — after catching up to
the store's current state, it sleeps for `poll_interval` then retries. This
enables change-data-capture (CDC) patterns where the replayer acts as a
persistent follower of the event log.

### Consumer Group Integration

`ReplayPosition::CommittedOffset` queries `{prefix}/_replay_offsets/{partition}/{group_id}`
for a persisted `HlcTimestamp` cursor and seeks to that position. This uses a
**separate offset format** from `OffsetCommit` (which stores per-publisher `u64`
sequence maps): the replayer stores HLC cursors directly, avoiding the need for
a `(publisher_id, seq) → HlcTimestamp` reverse lookup that does not exist in the
current storage index. The `commit()` method persists the replayer's current HLC
cursor under this key. This supports the catch-up-after-rebalance pattern used
by `ConsumerGroupSubscriber`.

---

## Interaction with Existing Types

| Existing Type | Relationship |
|--------------|-------------|
| `StorageBackend::query_replay()` | Called by `LocalReplaySource`; `ZenohReplaySource` hits the equivalent queryable |
| `ReplayFilters` | Internal detail — requires extension with `publisher_id: Option<PublisherId>` for `ReplayScope::Publisher`. `Partition(n)` is expressed via the Zenoh key path (`{store_prefix}/{partition}`), not a filter field. `ReplayScope::Key` / `KeyPrefix` map directly to existing `ReplayFilters` fields |
| `KeyedConsumer` | Overlaps in key-scoped tailing; `EventReplayer` generalizes it. Future: `KeyedConsumer` delegates to `EventReplayer` internally |
| `ConsumerGroupSubscriber` | Phase 5: replaces ad-hoc offset fetch + manual replay with `EventReplayer` |
| `OffloadManager` | Compatible — `into_live()` subscriber works with slow consumer offload (see above). Both share per-(pub, partition) seq checkpoints |
| `HlcTimestamp` | Cursor type — unchanged |

---

## Error Handling

The `EventReplayer` introduces new failure modes that require extending `mitiflow::Error`:

| Error Variant | When |
|--------------|------|
| `Error::StoreUnreachable` | Zenoh query to `{prefix}/_store` times out or returns no responders |
| `Error::EndOfReplay` | `recv()` called after a bounded replay is exhausted |
| `Error::DeserializationFailed` | `recv::<T>()` payload cannot be decoded with the configured codec |
| `Error::OverlapBufferFull` | (Internal) Live overlap buffer is at capacity and back-pressure fails |
| `Error::OffsetNotFound` | `seek_to_committed(group_id)` finds no stored offset for the given group |

Errors from the underlying `StorageBackend` or Zenoh session propagate unchanged.

---

## Cancellation and Shutdown

`EventReplayer` owns background resources in `ThenLive` / tailing modes:

- A Zenoh subscriber (declared during overlap transition)
- An overlap buffer task
- A poll-loop task (in tailing mode)

`shutdown()` uses a `CancellationToken` (consistent with `EventSubscriber`) to
cancel all background tasks and drain the overlap buffer. Dropping the
`EventReplayer` also triggers shutdown via `Drop`.

---

## Events Without HLC Timestamps

`EventMetadata.hlc_timestamp` is `Option<HlcTimestamp>`. Events with `None` are
**not indexed in the replay index** (`FjallBackend` skips them at store time:
`if let Some(hlc) = &metadata.hlc_timestamp`). These events are invisible to
`EventReplayer` — they cannot be replayed.

This is an intentional limitation: HLC timestamps are required for deterministic
cross-replica ordering. Events without HLC (e.g., from misconfigured publishers
or pre-HLC data) can still be queried via `QueryFilters` (seq-ordered) but not
through the replay API.

---

## GC and Retention During Replay

If the store runs garbage collection while a replay is in progress, events the
replayer has not yet reached could be deleted. Two mitigations:

1. **Minimum retention window.** The store should guarantee a minimum retention
   period (e.g., configurable `min_replay_retention`) during which events are
   not GC'd. Active replayers should be factored into the retention decision.

2. **Stale cursor detection.** If `poll()` detects that its cursor points to a
   region that has been GC'd (the store returns an `after_hlc` gap), it returns
   `Error::CursorStale` so the application can decide whether to restart from
   `Earliest` or abort.

---

## Schema Registry Interaction

`EventSubscriber::init()` calls `resolve_schema()` to ensure the schema is
registered before consuming. `EventReplayer::build()` should perform the same
resolution when the builder is configured with a typed codec that uses the
schema registry. This ensures that `recv::<T>()` can deserialize payloads
correctly even for events produced by older schema versions.

---

## Implementation Phases

| Phase | Scope | Key Deliverable |
|-------|-------|----------------|
| 0 | Store prerequisites | Extend `ReplayFilters` with `publisher_id: Option<PublisherId>`. Update queryable routing in `runner.rs` to route to `ReplayFilters` when `after_hlc` is present (currently only routes when `key=` or `key_prefix=` is present). Update `FjallBackend::query_replay()` to filter by publisher. |
| 1 | Remote replay (MVP) | `EventReplayer` + `ZenohReplaySource` + `poll()` / `recv()` / `stream()` + error types |
| 2 | Offset management | `commit()`, `seek_to_committed()` using HLC-based replay offsets (separate from seq-based `OffsetCommit`) |
| 3 | Local fast path | `LocalReplaySource`, `.local_backend()` builder method |
| 4 | Query-then-subscribe | `into_live()` — replayer → `LiveSubscriber` handoff, `ReplayEnd::ThenLive`, gap-free transition, `shutdown()` |
| 5 | Consumer group integration | Refactor `ConsumerGroupSubscriber` to use `EventReplayer` for catch-up |
| 6 | KeyedConsumer consolidation | (Optional) Delegate `KeyedConsumer` to `EventReplayer`, remove duplicated cursor code |

---

## Open Questions

1. **Tailing `poll()` semantics:** Should `poll()` return empty vec immediately
   (caller retries) or sleep internally? **Proposed:** `poll()` returns
   immediately; `recv()` handles sleep-and-retry internally.

2. **Multi-partition fan-in:** Should `ReplayScope::All` query all partitions
   in parallel and merge-sort by HLC? Each `EventStore` declares its queryable
   at `{store_prefix}/{partition}`, so `ReplayScope::All` requires N separate
   Zenoh queries (one per partition). **Proposed:** Start with sequential
   per-partition queries + client-side merge; cross-partition store index is
   an optimization for later. The `ReplaySource` trait's `query_replay()`
   must accept partition as a parameter to support this fan-out.

3. **Stream back-pressure:** Bounded buffer with back-pressure (consistent with
   `EventSubscriber`'s `event_channel_capacity`), or unbounded? **Proposed:**
   Bounded, configurable via builder.
