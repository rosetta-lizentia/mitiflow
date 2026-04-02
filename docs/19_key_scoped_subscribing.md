# Key-Scoped Subscribing

**Status:** Design document — not yet implemented.

How mitiflow supports subscribing to a subset of application keys (exact key
or key prefix) for both live streaming and historical replay, what ordering
and completeness guarantees change, and how the design interacts with
replicated stores.

**Related docs:**
[04_ordering.md](04_ordering.md),
[05_replication.md](05_replication.md),
[08_replay_ordering.md](08_replay_ordering.md),
[15_key_based_publishing.md](15_key_based_publishing.md),
[11_consumer_group_commits.md](11_consumer_group_commits.md)

---

## Part 1: The Problem

### What Exists Today

mitiflow publishes keyed events with the key expression layout:

```
{prefix}/p/{partition}/k/{key}/{seq}
```

Subscribers can filter by key at the Zenoh routing layer:

```rust
// Exact key across all partitions
let sub = EventSubscriber::new_keyed(&session, config, "order-123").await?;
//   subscribes to: myapp/events/p/*/k/order-123/*

// Key prefix (hierarchical)
let sub = EventSubscriber::new_key_prefix(&session, config, "user/42").await?;
//   subscribes to: myapp/events/p/*/k/user/42/**
```

Zenoh delivers only matching events — zero-copy, server-side filtering in the
routing mesh. No application-level filtering needed. This is a capability Kafka
cannot offer (Kafka consumers read entire partitions).

### What Breaks

The subscriber's gap detector tracks sequences per `(publisher_id, partition)`.
When a key-filtered subscriber receives a **sparse subset** of a publisher's
sequence space, it sees phantom gaps:

```
Publisher P1 writes to partition 0:
  seq=0  key=order-100
  seq=1  key=order-200
  seq=2  key=order-100    ← key subscriber for "order-100" sees this
  seq=3  key=order-300
  seq=4  key=order-100    ← and this

Key subscriber for "order-100" sees: seq=0, seq=2, seq=4
Gap detector reports: gap [1..2) after seq=0, gap [3..4) after seq=2
Recovery fires for seqs 1 and 3 — but those aren't "order-100" events!
```

The gap detector triggers unnecessary recovery for events the subscriber
deliberately filtered out. Worse, if recovery succeeds, the recovered events
are for different keys — they're either delivered (wrong) or filtered and
discarded (wasted work).

The same problem affects watermark tracking: the subscriber cannot meaningfully
track `committed_seq` per (partition, publisher) because it never sees most
sequences.

**For replay**, the store's `query_replay()` returns all events in HLC order
for a partition. A key-filtered consumer must either:
- Receive all events and filter client-side (wasteful).
- Use `query_by_key()` which bypasses HLC ordering and has no cursor support.

Neither is satisfactory.

### Requirements

1. Key-filtered live subscribers must not trigger false gap detection.
2. Key-filtered consumers that need completeness must have a path to receive
   all matching events, not just "most of them."
3. Historical replay must support key-scoped queries in deterministic HLC
   order.
4. The design must work with replicated stores (see
   [05_replication.md](05_replication.md)) — consumer cursors must be
   portable across replicas.
5. No changes to the publisher or wire protocol (50-byte attachment is fixed).

---

## Part 2: Why Key-Scoped Sequences Don't Work

An intuitive approach: assign a parallel sequence counter per (partition, key)
at the publisher. Each keyed publish carries both the existing
`(partition, publisher)` sequence and a new `key_seq`.

This fails at scale:

| Problem | Impact |
|---------|--------|
| **State explosion** | 100K distinct keys × P publishers = 100K counters per publisher. Unbounded for entity-ID keys (order IDs, user IDs). |
| **Attachment overhead** | Every event pays 8 extra bytes for `key_seq`, even if no one subscribes by key. Wire protocol break. |
| **Watermark explosion** | Per-key watermarks grow proportionally with key cardinality. Store must track and broadcast per-key progress. |
| **Key prefix mismatch** | A subscriber on `user/*` still sees sparse `key_seq` values across different sub-keys (`user/1`, `user/2`, ...). Per-key sequences don't help prefix subscribers. |
| **Protocol coupling** | Two parallel sequence spaces (per-publisher and per-key) must stay synchronised. Complexity compounds with replication. |

Per-key sequences optimise for a case (low-cardinality, stable key sets) that
doesn't match mitiflow's target workloads (per-entity keys, high cardinality).

---

## Part 3: Why Store Relay Doesn't Work

Another intuitive approach: the store re-publishes key-matching events on a
relay topic with store-assigned contiguous sequences. The key consumer
subscribes to the relay topic and gets full gap detection.

This breaks replication:

Each replica independently assigns `relay_seq`. Replica R0 assigns
`relay_seq=42` to event X; R1 assigns `relay_seq=42` to event Y. A consumer
failing over between replicas sees an inconsistent sequence — the exact same
problem that led mitiflow to reject per-partition store-assigned offsets
(Approach A in [04_ordering.md](04_ordering.md)).

| Problem | Impact |
|---------|--------|
| **Replica-local sequence** | `relay_seq` is not portable across replicas. Failover breaks consumer position. |
| **Store on write path** | Events reach key consumers only after store write + re-publish. Adds milliseconds to live latency. Defeats microsecond pub/sub. |
| **Double bandwidth** | Every event transmitted twice: original topic + relay topic. |
| **Registration required** | Store must know which key prefixes to relay. Unbounded for high-cardinality keys. |
| **Reintroduces a broker** | The store becomes a leader that mediates delivery — antithetical to mitiflow's brokerless philosophy. |

To fix the relay_seq portability problem, replicas would need consensus
(distributed sequencer). This is Kafka's architecture, not mitiflow's.

---

## Part 4: Design — Two Subscriber Modes

The fundamental tension: **per-(partition, publisher) contiguous sequencing
assumes the consumer sees every event.** A key-filtered consumer by definition
does not. Rather than papering over this with hybrid merge schemes (which
introduce their own latency/ordering tradeoffs), we offer two distinct modes
with explicit guarantees.

### Mode 1: Live Key Subscriber (Passthrough)

**For consumers that prioritise latency over completeness.**

The subscriber receives keyed events via Zenoh's native key expression
filtering with microsecond latency. Gap detection is **disabled** (sequences
are inherently sparse). Dedup uses `event_id` (UUID v7) instead of
`(publisher_id, seq)`.

```
Zenoh sample (key-filtered) → event_id dedup (LRU) → deliver
```

- **Latency:** Microsecond (unchanged from unfiltered path).
- **Ordering:** Per-publisher FIFO (Zenoh's per-session guarantee). No
  cross-publisher ordering beyond network arrival time.
- **Completeness:** Best-effort — Zenoh reliability handles most cases, but
  transport-level drops (slow consumer, buffer overflow) are not recovered.
- **Store dependency:** None.
- **Replication:** N/A (no store interaction).
- **Use cases:** UI updates, real-time dashboards, notifications,
  reactive triggers — anything where "most events, fast" beats "all events,
  ordered."

#### Implementation

When `EventSubscriber::new_keyed()` or `new_key_prefix()` is called, the
pipeline switches to passthrough mode:

```rust
// In pipeline processing:
if key_filtered {
    // Skip GapDetector::on_sample() entirely.
    // Use EventIdDedup instead of sequence-based dedup.
    if dedup.is_new(meta.event_id) {
        deliver(event);
    }
} else {
    // Existing path: GapDetector → recovery → deliver
    let result = gap_detector.on_sample(&meta.pub_id, partition, meta.seq);
    handle_sample_result(result, ...);
}
```

The `EventIdDedup` is a bounded LRU set of `EventId` values:

```rust
pub struct EventIdDedup {
    seen: lru::LruCache<EventId, ()>,
}

impl EventIdDedup {
    pub fn new(capacity: usize) -> Self { ... }

    /// Returns true if this event_id has not been seen before.
    pub fn is_new(&mut self, id: EventId) -> bool {
        self.seen.put(id, ()).is_none()
    }
}
```

The LRU size is configurable via `dedup_capacity` (default 10,000). Since
`EventId` is UUID v7 (time-ordered), eviction of old entries is naturally
correct — a re-delivered event whose ID was evicted from the LRU is old enough
that re-delivery is safe (or was already processed by the application).

### Mode 2: Key Consumer (Pull-Based, Store-Mediated)

**For consumers that need completeness and ordering.**

The consumer **polls** the store's queryable in HLC order, filtered by key.
This is explicitly a pull model — the consumer controls the polling rate and
queries any available replica.

```
loop {
    events = store.query_replay(key_prefix, after_hlc=cursor, limit)
    for event in events:
        process(event)
        cursor = event.hlc
    sleep(poll_interval)
}
```

- **Latency:** `poll_interval` (configurable, default 100ms — matches
  watermark broadcast interval).
- **Ordering:** Deterministic HLC order (same as full replay index from
  [08_replay_ordering.md](08_replay_ordering.md)). Cross-publisher ordering
  is approximately-physical-time.
- **Completeness:** Guaranteed — the store has all events, and the key-scoped
  query returns all matching events.
- **Store dependency:** Required.
- **Replication:** HLC cursor is cross-replica portable. Consumer can fail over
  to any replica mid-stream. See Part 6.
- **Use cases:** Event sourcing projections, aggregation pipelines, CDC
  consumers, audit logs — anything where every matching event must be
  processed in deterministic order.

#### Why Pull Instead of Live + Catch-Up?

A hybrid approach (live delivery + periodic catch-up from store) seems
appealing: microsecond latency in the common case, store fills gaps. But for
a consumer that needs **ordered** delivery:

```
Hybrid ordered path:
  Live event arrives (µs) → must buffer it
  Wait for catch-up query (poll_interval) → confirms no missed events before this HLC
  Only then deliver buffered events in HLC order
  Effective latency: poll_interval (seconds)
```

The effective latency equals the catch-up interval. To get millisecond latency,
you poll every millisecond — generating the same store load as simply polling
for all events. The hybrid doesn't occupy a unique position: it's either
passthrough (Mode 1, no ordering guarantee) or slower-than-polling (ordered
but with more complexity).

**By separating Mode 1 and Mode 2, we make the tradeoff explicit** rather than
hiding it behind a hybrid that promises both but delivers neither cleanly.

---

## Part 5: Store-Side Support

### New Query Filters

`ReplayFilters` gains key-scoped filtering:

```rust
#[derive(Debug, Clone, Default)]
pub struct ReplayFilters {
    /// Only return events with HLC timestamp strictly after this value.
    pub after_hlc: Option<HlcTimestamp>,
    /// Only return events with HLC timestamp strictly before this value.
    pub before_hlc: Option<HlcTimestamp>,
    /// Maximum number of results.
    pub limit: Option<usize>,
    /// Only return events with this exact application key.
    pub key: Option<String>,
    /// Only return events whose application key starts with this prefix.
    pub key_prefix: Option<String>,
}
```

### Query Selector Parameters

The store's queryable parses `key` and `key_prefix` from Zenoh selector
parameters alongside existing `after_seq`, `limit`, etc.:

```
// Exact key replay:
{store_prefix}/{partition}?key=order-123&after_hlc=1711843200000000000:0&limit=500

// Key prefix replay:
{store_prefix}/{partition}?key_prefix=user/42&after_hlc=1711843200000000000:0&limit=500
```

HLC timestamp is serialised as `{physical_ns}:{logical}` in selector params.

### FjallBackend Implementation

When `key` or `key_prefix` is set in `ReplayFilters`, the query strategy
changes:

```rust
fn query_replay(&self, filters: &ReplayFilters) -> Result<Vec<StoredEvent>> {
    if filters.key.is_some() || filters.key_prefix.is_some() {
        // Key-scoped replay: scan the `keys` keyspace filtered by key,
        // collect matching event pointers, look up each in `events`,
        // sort by HLC order, apply after_hlc/before_hlc/limit filters.
        self.query_replay_keyed(filters)
    } else {
        // Existing path: scan the `replay` keyspace directly.
        self.query_replay_full(filters)
    }
}

fn query_replay_keyed(&self, filters: &ReplayFilters) -> Result<Vec<StoredEvent>> {
    // 1. Scan `keys` keyspace for entries matching key/key_prefix.
    //    keys keyspace layout: [key_hash:8][hlc_physical:8 BE][hlc_logical:4 BE][publisher_id:16]
    //                  value:  [publisher_id:16][seq:8 BE]
    //
    // 2. For each match, look up the full event from `events` keyspace.
    //
    // 3. Filter by after_hlc / before_hlc from the key index sort order.
    //
    // 4. Apply limit.
    //
    // The `keys` keyspace is already sorted by (key_hash, hlc, publisher_id),
    // so a prefix scan on key_hash gives HLC-ordered results per key.
    // For key_prefix, we scan all key_hash values and post-filter by
    // metadata.key.starts_with(prefix).
}
```

**Performance note:** Exact-key queries use a prefix scan on `key_hash` — O(k)
where k is the number of events for that key. Key-prefix queries currently
require a full `keys` keyspace scan with post-filtering. A future optimisation
could add a prefix-aware index (trie or additional keyspace bucketed by prefix
segments).

### Queryable Task Changes

The existing `run_queryable_task` in `EventStore` serves replay queries. It
must be extended to recognise `key` and `key_prefix` parameters and route to
`query_replay()` instead of `query()`:

```rust
// In run_queryable_task:
let params = query.parameters().to_string();

if params.contains("key=") || params.contains("key_prefix=") {
    // Key-scoped replay query → use ReplayFilters + query_replay()
    let filters = ReplayFilters::from_selector(&params)?;
    let events = handle.query_replay(filters).await?;
    // ... reply with events ...
} else {
    // Existing path → QueryFilters + query()
    let filters = QueryFilters::from_selector(&params)?;
    let events = handle.query(filters).await?;
    // ... reply with events ...
}
```

---

## Part 6: KeyedConsumer API

### Construction

```rust
use mitiflow::subscriber::KeyedConsumer;

let consumer = KeyedConsumer::builder(&session, config)
    .key("order-123")               // exact key
    // OR: .key_prefix("user/42")   // hierarchical prefix
    .poll_interval(Duration::from_millis(100))
    .build()
    .await?;
```

### Polling

```rust
loop {
    let events: Vec<StoredEvent> = consumer.poll().await?;
    for event in &events {
        process(event);
    }
    // consumer.cursor is automatically advanced.
}
```

`poll()` queries the store via Zenoh `session.get()`:

```rust
impl KeyedConsumer {
    pub async fn poll(&mut self) -> Result<Vec<StoredEvent>> {
        let selector = format!(
            "{}/{}?{}&after_hlc={}:{}&limit={}",
            self.store_prefix,
            self.partition,
            self.key_filter.to_selector_param(),
            self.cursor.physical_ns,
            self.cursor.logical,
            self.batch_size,
        );

        let replies = self.session.get(&selector)
            .accept_replies(ReplyKeyExpr::Any)
            .consolidation(ConsolidationMode::None)
            .timeout(self.query_timeout)
            .await?;

        let mut events = Vec::new();
        while let Ok(reply) = replies.recv_async().await {
            if let Ok(sample) = reply.result() {
                // Parse event from sample...
                events.push(stored_event);
            }
        }

        // Sort by HLC (replies may arrive out of order).
        events.sort_by_key(|e| (
            e.metadata.hlc_timestamp.unwrap_or_default(),
            e.metadata.publisher_id,
            e.metadata.seq,
        ));

        // Advance cursor past the last returned event.
        if let Some(last) = events.last() {
            if let Some(hlc) = &last.metadata.hlc_timestamp {
                self.cursor = *hlc;
            }
        }

        Ok(events)
    }
}
```

### Consumer Position

The consumer's position is a single `HlcTimestamp`:

```rust
pub struct KeyedConsumer {
    session: Session,
    config: EventBusConfig,
    key_filter: KeyFilter,
    cursor: HlcTimestamp,       // last delivered HLC
    partition: u32,             // or ALL_PARTITIONS sentinel
    poll_interval: Duration,
    batch_size: usize,
    query_timeout: Duration,
    store_prefix: String,
}

pub enum KeyFilter {
    Exact(String),
    Prefix(String),
}
```

The HLC cursor is:
- **Monotonically advancing** within a single consumer's lifetime.
- **Cross-replica portable** — the same HLC produces the same results on any
  replica (by design of the HLC replay index, see
  [08_replay_ordering.md](08_replay_ordering.md)).
- **Serialisable** as two integers (`physical_ns: u64`, `logical: u32`).

---

## Part 7: Consumer Group Integration

Key consumers in a consumer group need coordinated offsets. The offset format
differs from per-(publisher, seq) offsets used by unfiltered consumers.

### HLC-Based Offset Commits

```rust
/// Offset commit for key-scoped consumers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyedOffsetCommit {
    pub group_id: String,
    pub member_id: String,
    pub key_filter: KeyFilter,
    pub partition: u32,
    pub last_hlc: HlcTimestamp,
    pub generation: u64,
    pub timestamp: DateTime<Utc>,
}
```

### Offset Storage

HLC offsets are stored in a separate keyspace or with a distinguishing prefix
in the existing `offsets` keyspace:

```
Key:   _offsets/{partition}/{group_id}/key/{key_filter_hash}
Value: KeyedOffsetCommit (JSON)
```

The `key_filter_hash` is a hash of the `KeyFilter` value (exact key or prefix
string), ensuring each key filter gets its own offset entry within a group.

### Commit and Fetch

```rust
impl KeyedConsumer {
    /// Commit the current cursor as the group's offset for this key filter.
    pub async fn commit(&self, group_id: &str) -> Result<()> {
        let commit = KeyedOffsetCommit {
            group_id: group_id.to_string(),
            member_id: self.member_id.clone(),
            key_filter: self.key_filter.clone(),
            partition: self.partition,
            last_hlc: self.cursor,
            generation: self.generation,
            timestamp: Utc::now(),
        };
        // Publish to _offsets topic for store persistence.
        let key = format!(
            "{}/_offsets/{}/{}/key/{}",
            self.config.key_prefix,
            self.partition,
            group_id,
            self.key_filter.hash(),
        );
        self.session.put(&key, serde_json::to_vec(&commit)?).await?;
        Ok(())
    }

    /// Fetch the last committed cursor for this group + key filter.
    pub async fn fetch_offset(&self, group_id: &str) -> Result<Option<HlcTimestamp>> {
        // Query the store's offset queryable.
        // Returns the last committed HLC for this (group, key_filter, partition).
    }
}
```

### Rebalance

When a consumer group member loses a partition (rebalance), it commits its
current HLC cursor. When a new member gains that partition, it fetches the
committed HLC cursor and resumes polling from that point. Because the cursor is
an HLC timestamp intrinsic to the events, this works regardless of which store
replica serves the query.

---

## Part 8: Replication Compatibility

### Mode 1 (Live Passthrough)

No store interaction, so replication is irrelevant. Works identically with 0, 1,
or N replicas.

### Mode 2 (Pull-Based)

The HLC cursor is the key enabler for replication compatibility:

| Property | How it works |
|----------|-------------|
| **Same-replica determinism** | HLC index returns the same events in the same order on every query. |
| **Cross-replica determinism** | The HLC timestamp is intrinsic to the event (assigned by the publisher's Zenoh session at `put()` time). Every replica that receives the same event stores the same HLC. The keys keyspace indexes by HLC within each key. |
| **Consumer cursor portability** | The consumer tracks `last_hlc: HlcTimestamp`. This value is meaningful on any replica because it references the event's HLC, not a store-local offset. |
| **Failover** | Consumer was polling R0 at cursor HLC=X. R0 crashes. Consumer retries with R1, same cursor HLC=X. R1 has the same events (assuming quorum replication) in the same HLC order. Seamless. |
| **Quorum watermark** | Unchanged. The publisher waits for quorum replicas to confirm `(partition, publisher_id, seq)`. Key consumers don't participate in the watermark protocol — they consume from the store after events are already durable. |

### Contrast with Store-Assigned Sequences

This is why Mode 2 uses pull (query) instead of push (relay):

```
Store Relay (rejected):
  R0: relay_seq=42 → event A     R1: relay_seq=42 → event B
  Consumer at relay_seq=41 fails over from R0 to R1 → sees event B instead of A
  BROKEN: replica-local sequence

Pull with HLC cursor:
  R0: query(after_hlc=X) → [A, B, C]    R1: query(after_hlc=X) → [A, B, C]
  Consumer at cursor=X fails over from R0 to R1 → gets same events
  WORKS: HLC is intrinsic to the event
```

### Offset Commit Replication

Key-scoped offsets replicate via the same pub/sub fan-out mechanism as
per-(publisher, seq) offsets (see [11_consumer_group_commits.md](11_consumer_group_commits.md)
and [05_replication.md](05_replication.md)). The consumer publishes the
`KeyedOffsetCommit` to `_offsets/{partition}/{group_id}/key/{hash}`, and all
store replicas subscribing to `_offsets/**` receive and persist it independently.
No additional protocol needed.

---

## Part 9: Ordering Guarantees

### Mode 1 (Live Passthrough)

| Guarantee | Status | Notes |
|-----------|--------|-------|
| Per-publisher, per-key ordering | ✅ Yes | Zenoh FIFO per session. Same publisher's events arrive in publish order. |
| Cross-publisher ordering | ❌ No | Network arrival order. No HLC or sequence ordering. |
| No duplicates | ✅ Yes | event_id LRU dedup. |
| No missed events | ❌ Best-effort | Zenoh reliability handles most cases. Transport-level drops not recovered. |
| Deterministic replay | ❌ N/A | This is a live-only mode. |

### Mode 2 (Pull-Based)

| Guarantee | Status | Notes |
|-----------|--------|-------|
| Per-publisher, per-key ordering | ✅ Yes | HLC is monotonic per publisher. |
| Cross-publisher ordering | ✅ Approximate | HLC-based (~NTP accuracy, < 1ms same DC). |
| No duplicates | ✅ Yes | HLC cursor ensures each event delivered exactly once. |
| No missed events | ✅ Guaranteed | Store has all events; query returns complete results. |
| Deterministic replay | ✅ Yes | HLC index, same as full replay. |
| Cross-replica consistency | ✅ Yes | HLC cursor is portable. |

### What Is Explicitly Relaxed (Both Modes)

Compared to an unfiltered subscriber with full gap detection:

| Unfiltered subscriber guarantee | Key subscriber equivalent |
|--------------------------------|--------------------------|
| Contiguous per-(partition, publisher) sequences | Not applicable — key subscriber sees sparse sequences by design. |
| Sequence-based gap detection + recovery | Mode 1: none. Mode 2: HLC cursor + store completeness. |
| Real-time watermark tracking | Not applicable — key subscribers don't participate in the watermark protocol. Durability was already confirmed before Mode 2 reads from the store. |

---

## Part 10: End-to-End Examples

### Example 1: Real-Time Order Status Dashboard

A UI consumer shows live status updates for a specific order:

```rust
let config = EventBusConfig::builder("myapp/events")
    .num_partitions(8)
    .dedup_capacity(5_000)
    .build()?;

// Subscribe to all events for this order — live, low latency.
let sub = EventSubscriber::new_keyed(&session, config, "order-123").await?;

loop {
    let event: Event<OrderStatus> = sub.recv().await?;
    update_dashboard(event.payload);
}
```

Mode 1 (passthrough). Microsecond latency. If an event is dropped at the
transport level, the dashboard misses one update — acceptable for a UI that
shows the latest state.

### Example 2: Per-User Event Sourcing Projection

A service builds a materialised view of a user's activity:

```rust
let config = EventBusConfig::builder("myapp/events")
    .num_partitions(8)
    .build()?;

let mut consumer = KeyedConsumer::builder(&session, config)
    .key_prefix("user/42")
    .poll_interval(Duration::from_millis(100))
    .build()
    .await?;

// Resume from last committed offset.
if let Some(hlc) = consumer.fetch_offset("user-projection-group").await? {
    consumer.seek(hlc);
}

loop {
    let events = consumer.poll().await?;
    for event in &events {
        apply_to_projection(event);
    }
    consumer.commit("user-projection-group").await?;
}
```

Mode 2 (pull-based). 100ms poll interval. Complete, ordered delivery. If the
store replica it's querying crashes, it retries on another replica with the same
HLC cursor — no data loss, no re-processing.

### Example 3: Consumer Group with Key-Scoped Processing

Multiple workers process different key prefixes, coordinated as a group:

```rust
// Worker 0: handles users 0-99
let mut consumer = KeyedConsumer::builder(&session, config.clone())
    .key_prefix("user/0")       // simplified; real routing uses partition assignment
    .poll_interval(Duration::from_millis(200))
    .build()
    .await?;

// On startup, fetch committed offset for this key filter
if let Some(hlc) = consumer.fetch_offset("user-workers").await? {
    consumer.seek(hlc);
}

loop {
    let events = consumer.poll().await?;
    for event in &events {
        process(event);
    }
    consumer.commit("user-workers").await?;
}
```

### Example 4: Replay All Events for a Key From the Beginning

```rust
// Start with HLC cursor at zero — replay from the beginning.
let mut consumer = KeyedConsumer::builder(&session, config)
    .key("order-123")
    .poll_interval(Duration::from_millis(50))  // fast drain
    .build()
    .await?;

loop {
    let events = consumer.poll().await?;
    if events.is_empty() {
        break; // caught up
    }
    for event in &events {
        replay_event(event);
    }
}
```

---

## Part 11: Implementation Plan

### Phase 1: Pipeline Passthrough Mode

**Changes:** `EventSubscriber`, pipeline module.

1. Add `EventIdDedup` component (bounded LRU set of EventId).
2. Add `key_filtered: bool` flag to the pipeline worker state.
3. When `new_keyed()` or `new_key_prefix()` is called, set `key_filtered = true`.
4. In the pipeline processing loop, skip `GapDetector::on_sample()` when
   `key_filtered` is true. Use `EventIdDedup::is_new()` instead.
5. Disable heartbeat-triggered gap recovery for key-filtered subscribers
   (heartbeats advertise per-publisher sequences, which are meaningless for
   key-filtered consumers).

**Tests:**
- Key-filtered subscriber does not trigger recovery for sparse sequences.
- Event-ID dedup correctly filters duplicate deliveries.
- Passthrough subscriber works with unkeyed events (delivers all without gap detection).

### Phase 2: Store Key-Scoped Replay

**Changes:** `ReplayFilters`, `FjallBackend`, `EventStore` queryable.

1. Add `key: Option<String>` and `key_prefix: Option<String>` to `ReplayFilters`.
2. Add `ReplayFilters::from_selector()` parser (similar to `QueryFilters::from_selector()`).
3. Implement `query_replay_keyed()` in `FjallBackend`: scan `keys` keyspace
   with key filter, look up events, sort by HLC, apply limit.
4. Extend `run_queryable_task()` to route key-scoped queries to `query_replay()`.

**Tests:**
- Store returns only matching events for exact key.
- Store returns only matching events for key prefix.
- Results are in HLC order.
- `after_hlc` cursor correctly excludes already-seen events.
- Empty result when no matching keys exist.

### Phase 3: KeyedConsumer

**Changes:** New `subscriber::keyed_consumer` module.

1. Implement `KeyedConsumer` with builder pattern.
2. `poll()` method: query store, sort by HLC, advance cursor.
3. `seek()` method: set cursor to a specific HLC.
4. `commit()` / `fetch_offset()`: HLC-based offset management.

**Tests:**
- Poll returns events in HLC order for exact key.
- Poll returns events in HLC order for key prefix.
- Cursor advances correctly — no duplicate delivery across polls.
- `commit()` + `fetch_offset()` round-trip preserves HLC cursor.
- Empty poll returns empty vec (not error).

### Phase 4: Consumer Group Key Offsets

**Changes:** Store offset handling, `FjallBackend`.

1. Add `KeyedOffsetCommit` type.
2. Extend store's offset subscriber to persist key-scoped offsets.
3. Extend store's offset queryable to serve key-scoped offset fetches.
4. Wire into `KeyedConsumer::commit()` / `fetch_offset()`.

**Tests:**
- Key offset commit and fetch via store queryable.
- Generation fencing works for key offsets (same as seq offsets).
- Multiple key filters in the same group have independent offsets.

---

## Part 12: Open Questions

### 1. Key Prefix Index Performance

For **exact key** queries, the `keys` keyspace supports efficient prefix scan on
`key_hash`. For **key prefix** queries, we currently scan and post-filter. If
key-prefix consumers become a hot path, options include:

- **Trie-based secondary index** in a new keyspace.
- **Bucketed prefix keyspace** where keys are also indexed by prefix segments.
- **Client-side optimisation:** query exact keys that the consumer knows about,
  rather than open-ended prefix scan.

For now, post-filtering is acceptable for moderate key cardinalities (< 100K
events per prefix per poll).

### 2. Poll Interval vs. Latency

The pull model's latency floor is `poll_interval`. For consumers that need
sub-100ms latency AND completeness:

- **Option:** Long-polling on the store's queryable. The consumer issues a
  query with a "wait" parameter; the store holds the query open until new
  matching events arrive (or timeout). Eliminates polling overhead and reduces
  latency to store write time (~ms).
- **Complexity:** Requires the store to maintain pending query state per
  consumer. Not implemented in Phase 1.

### 3. Cross-Partition Key Queries

The current design queries a single partition or all partitions. For consumers
that want "all events for key X regardless of partition":

- Keys are deterministically routed to partitions via
  `partition_for(key, num_partitions)`. An exact-key consumer can compute which
  partition to query.
- A key-prefix consumer may need to query multiple partitions (different keys
  under the prefix may hash to different partitions). The `KeyedConsumer`
  should support querying all partitions or a computed subset.

### 4. Compacted Key Queries

For consumers that want only the **latest value per key** (log compaction view):

- The store already has `query_latest_by_keys()` and `compact_keyed()`.
- `KeyedConsumer` could expose a `poll_compacted()` method that uses
  `query_latest_by_keys()` instead of `query_replay()`.
- This is a natural extension but out of scope for the initial implementation.
