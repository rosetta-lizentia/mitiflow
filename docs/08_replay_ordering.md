# Deterministic Replay & Publisher Lifecycle

How mitiflow achieves deterministic, replica-independent replay ordering and
manages ephemeral publisher state over time.

---

## Part 1: The Replay Ordering Problem

### What Works Today

**Live consumption** has "natural" interleaving — events arrive in whatever
order the network delivers them. Two consumers connected at the same time see
the same Zenoh delivery order (within the same session's FIFO guarantee).

**Per-(partition, publisher) ordering** is strict: within a single publisher's
stream for a single partition, events are contiguously numbered and totally
ordered. Gap detection and watermark tracking work correctly.

### What Breaks on Replay

When a consumer replays historical events from an `EventStore`, it reads from
fjall's LSM storage. The current key layout is:

```
[publisher_id: 16 bytes][seq: 8 bytes BE]
```

fjall's iterator returns events in **key order**, which means:

```
All of P1's events (seq 0, 1, 2, ..., N)
Then all of P2's events (seq 0, 1, 2, ..., M)
Then all of P3's events (seq 0, 1, 2, ..., K)
```

This is **completely different** from the interleaving that live consumers saw.
A consumer that processes events in live order `[P1:0, P2:0, P1:1, P3:0, P2:1]`
would see `[P1:0, P1:1, P2:0, P2:1, P3:0]` on replay. For stateful consumers
(aggregations, joins, windowing), this can produce different results.

### Replicated Stores Make It Worse

With multiple `EventStore` replicas (see [05_replication.md](05_replication.md)),
each replica receives events in its own arrival order based on network timing.
Even if we added an "arrival order" index, replaying from replica R0 vs R1
would yield different orderings.

```
Live arrival at R0: P1:0, P2:0, P1:1, P3:0, P2:1
Live arrival at R1: P2:0, P1:0, P3:0, P1:1, P2:1
                    ↑ different interleaving, same events
```

A consumer failing over from R0 to R1 mid-replay would see an inconsistent
ordering — some events appear reordered or duplicated relative to what it
already processed.

### Requirements for Deterministic Replay

1. **Same-replica determinism:** Replaying the same partition range from the
   same store always returns events in the same order.
2. **Cross-replica determinism:** Replaying the same partition range from
   _any_ replica returns events in the same order.
3. **Consistent consumer cursor:** A consumer's position can be expressed as
   a single value that is meaningful across replicas.

Requirement 1 is easy (any fixed index order works). Requirements 2 and 3
rule out store-assigned offsets (which are replica-local) and demand an
ordering key that is **intrinsic to the event**, not to the store instance.

---

## Part 2: HLC-Based Replay Ordering

### The Insight

Zenoh assigns a **Hybrid Logical Clock (HLC)** timestamp to every published
sample. This timestamp is set at the publisher's Zenoh session at `put()` time
and travels with the sample to all subscribers. Every replica that receives the
same sample sees the **same HLC timestamp** — it's an intrinsic property of the
event, not the receiver.

This makes HLC timestamps the natural replay ordering key:

```
Sort key: (hlc_timestamp, publisher_id, seq)
```

- **hlc_timestamp** provides approximately-physical-time ordering across all
  publishers
- **publisher_id** breaks ties between publishers with identical HLC values
- **seq** breaks ties within the same publisher (guaranteed unique + ordered)

This triple is globally unique, deterministic, and replica-independent.

### HLC Background

A Hybrid Logical Clock combines:

```rust
struct HlcTimestamp {
    physical: u64,   // wall clock (nanoseconds since epoch)
    logical: u32,    // counter for events at same physical time
    // Zenoh's HLC also embeds a node ID for global uniqueness
}
```

**Ordering rules:**
1. Compare `physical` first (coarser time)
2. Then `logical` (causal tiebreaker within same physical instant)
3. Then `publisher_id` (arbitrary but deterministic tiebreaker)

**Properties:**
- **Monotonic per node:** Even if the wall clock regresses (NTP adjustment),
  the HLC advances the logical counter. A publisher's events are always in
  HLC-increasing order.
- **Approximately physical:** Across nodes, HLC tracks wall-clock time within
  NTP accuracy (~ms). Events from different publishers are ordered by
  approximately-real time.
- **No coordination:** Each Zenoh session maintains its own HLC independently.
  No consensus needed.

### Why Not Raw Timestamps?

Raw `chrono::Utc::now()` has two problems:

1. **Clock skew:** Publisher P1 on machine A and publisher P2 on machine B
   have different clocks. P1's clock 50ms ahead means P1's events appear
   "before" causally-later events from P2.

2. **Non-monotonic:** NTP adjustments can make the wall clock jump backward.
   Two consecutive `Utc::now()` calls can return decreasing values.

HLC fixes problem 2 entirely (always monotonic per node) and bounds problem 1
to NTP accuracy. Zenoh's HLC additionally propagates: when a node receives a
message with a higher HLC than its own clock, it advances its clock. This
provides a weak form of causal ordering across communicating nodes.

### Why Not Store-Assigned Offsets?

Store-assigned offsets (Approach A from [04_ordering.md](04_ordering.md)) give
deterministic replay within a single store but fail requirement 2:

| Property | Store Offset | HLC |
|----------|-------------|-----|
| Same-replica determinism | Yes | Yes |
| Cross-replica determinism | **No** — each replica assigns its own offsets | **Yes** — same HLC travels with the event |
| Failover consumer position | Must translate offsets between replicas | Same HLC position works on any replica |
| Write coordination | AtomicU64 per partition (serialization point) | None (publisher-assigned) |
| Brokerless compatible | Partially (store becomes sequencer) | Fully (no store involvement) |
| Kafka cursor compatibility | Yes (offset integer) | No (HLC is not an integer offset) |

---

## Part 3: Storage Design

### Dual Index Layout

The store maintains two indexes in fjall:

**Primary index** (existing) — for watermark lookups and dedup:
```
Keyspace: events
Key:   [publisher_id: 16 bytes][seq: 8 bytes BE]
Value: [meta_len: 4 LE][metadata JSON][payload bytes]
```

**HLC replay index** — for deterministic ordered replay:
```
Keyspace: replay
Key:   [hlc_physical: 8 bytes BE][hlc_logical: 4 bytes BE][publisher_id: 16 bytes][seq: 8 bytes BE]
Value: (empty — or a pointer back to the events keyspace key)
```

The replay index key is 36 bytes. Since it only stores the ordering
relationship, the value can be empty (the actual event data lives in the
`events` keyspace). A replay query scans the `replay` keyspace in key order
and looks up each event from `events`.

**Key index** (for key-based publishing, see
[15_key_based_publishing.md](15_key_based_publishing.md)):
```
Keyspace: keys
Key:   [key_hash: 8 bytes][hlc_physical: 8 bytes BE][hlc_logical: 4 bytes BE][publisher_id: 16 bytes]
Value: [publisher_id: 16 bytes][seq: 8 bytes BE]  (pointer to events keyspace)
```

The `keys` index enables key-scoped queries and log compaction. It is populated
only when events carry an application key (keyed publish path).

**Alternative: inline value.** Store the full event value in the replay
keyspace too. Doubles storage but avoids the second lookup per event during
replay. Worth it if replay is the dominant read pattern.

### Storing the HLC Timestamp

The Zenoh `Sample` exposes its timestamp via `sample.timestamp()`, which
returns an `Option<&Timestamp>`. The `Timestamp` contains an NTP64 value
(physical time) and a unique ID.

On the store path:

```rust
// In the store's subscribe task, when processing a sample:
let hlc_timestamp = sample.timestamp().cloned();

// Pass to backend alongside existing metadata:
EventMetadata {
    seq,
    publisher_id,
    event_id,
    timestamp,        // existing chrono::DateTime<Utc>
    key_expr,
    hlc: hlc_timestamp, // new field
}
```

The `EventMetadata` gains an `hlc` field. For events recovered from publisher
cache or peer replicas, the HLC timestamp is preserved in the cached/stored
metadata — it's the publisher's original HLC, not the recovery timestamp.

### Write Path

On each `store()` call, the backend writes to both indexes atomically:

```rust
fn store(&self, key: &str, event: &[u8], metadata: EventMetadata) -> Result<()> {
    let event_key = encode_event_key(&metadata.publisher_id, metadata.seq);
    let event_value = encode_event_value(&metadata, event);

    let mut batch = self.db.batch();

    // Primary index (existing)
    batch.insert(&self.events, event_key, &event_value);
    batch.insert(&self.keys, key.as_bytes(), event_key);

    // HLC replay index (new)
    if let Some(hlc) = &metadata.hlc {
        let replay_key = encode_replay_key(hlc, &metadata.publisher_id, metadata.seq);
        // Value: just the primary key, for lookup
        batch.insert(&self.replay, replay_key, event_key);
    }

    // ... gap tracking (unchanged) ...

    batch.commit()?;
    Ok(())
}
```

**Cost:** One additional key-value pair per event (~36 byte key + 24 byte
value = ~60 bytes overhead per event). For 1M events, that's ~60MB of
additional index — negligible compared to the event payloads.

### Query Path

Replay queries scan the `replay` keyspace instead of `events`:

```rust
fn query_replay(
    &self,
    after_hlc: Option<HlcTimestamp>,
    before_hlc: Option<HlcTimestamp>,
    limit: Option<usize>,
) -> Result<Vec<StoredEvent>> {
    let mut results = Vec::new();
    let iter = match &after_hlc {
        Some(hlc) => {
            let start_key = encode_replay_key(hlc, &PublisherId::nil(), 0);
            self.replay.range(start_key..)
        }
        None => self.replay.iter(),
    };

    for guard in iter {
        let kv = guard.into_inner()?;
        let (hlc, pub_id, seq) = decode_replay_key(&kv.0)?;

        if let Some(ref before) = before_hlc {
            if hlc >= *before { break; } // past the range
        }

        // Look up full event from primary index
        let event_key = kv.1;
        let event_value = self.events.get(&event_key)?;
        // ... decode and push to results ...

        if let Some(lim) = limit {
            if results.len() >= lim { break; }
        }
    }
    Ok(results)
}
```

**Consumer cursor:** The consumer tracks its position as an HLC timestamp.
On resume: "give me events from partition 0 after HLC X" — this works on any
replica because the HLC values are intrinsic to the events.

### Handling Missing HLC

If `sample.timestamp()` returns `None` (Zenoh configured without HLC, or
events recovered from a source that didn't preserve it):

- Fall back to `metadata.timestamp` (chrono wall clock) converted to a
  synthetic HLC-like value: `(timestamp_nanos, 0, publisher_id, seq)`.
- This is less accurate (no logical counter, subject to clock skew) but still
  deterministic given the publisher_id + seq tiebreakers.
- Log a warning — HLC should be enabled for deterministic replay.

---

## Part 4: Ordering Guarantees and Limitations

### What HLC Replay Provides

| Guarantee | Status | How |
|-----------|--------|-----|
| **Deterministic per-replica replay** | Yes | HLC index is the same regardless of arrival order |
| **Cross-replica deterministic replay** | Yes | Same HLC travels with each event to all replicas |
| **Consumer cursor portability** | Yes | HLC position is meaningful on any replica |
| **Per-publisher strict ordering** | Yes | HLC is monotonic per publisher; seq reinforces |
| **Approximately-physical-time ordering** | Yes | Events appear in ~real-time order (within NTP accuracy) |

### What HLC Replay Does NOT Provide

| Guarantee | Why Not | Impact |
|-----------|---------|--------|
| **True causal ordering across publishers** | HLC is physical-time-based, not a vector clock. Two independent publishers on skewed clocks can appear misordered | Low — events from different publishers to the same partition are usually for different keys (different entities). Same-key events from one publisher ARE causally ordered. |
| **Identical replay as live ordering** | Live ordering depends on network timing; HLC ordering depends on publisher clocks. These differ by ~ms | Low — consumers should not depend on the exact interleaving of independent publisher streams. Design for per-publisher ordering, get approximate cross-publisher ordering as bonus. |
| **Kafka-compatible integer offset** | HLC is a 12+ byte timestamp, not a simple u64 | See Kafka Gateway section below. |
| **Zero overhead** | Additional ~60 bytes per event for the replay index | Negligible for typical workloads |

### Clock Skew Impact Analysis

With NTP-synchronized clocks (~1-10ms accuracy):

- **Same datacenter:** Clock skew typically < 1ms. Events from different
  publishers are ordered within 1ms accuracy — indistinguishable from "correct"
  for most applications.
- **Cross-datacenter:** Clock skew can be 10-50ms. Events from publishers in
  different regions may appear misordered by up to 50ms. For cross-region
  event streaming, this is usually acceptable — network latency itself is
  10-100ms.
- **Same machine:** Clock is identical. Multiple publishers on the same host
  have perfect relative ordering.

**Bottom line:** HLC ordering is "good enough" for every use case that doesn't
require true causal consistency across independent publishers. The only systems
that provide stronger guarantees use either a centralized sequencer (Kafka,
NATS) or CRDTs + vector clocks (significantly more complex).

---

## Part 5: Kafka Gateway Integration

The Kafka protocol expects integer offsets per partition. Two approaches for
the gateway to bridge HLC → offset:

### Option A: Gateway-Assigned Offset

The Kafka gateway maintains its own `AtomicU64` per partition. As it reads from
the mitiflow store (via the HLC replay index), it assigns contiguous Kafka
offsets:

```
HLC replay order:  (hlc=100, P1, 0), (hlc=102, P2, 0), (hlc=105, P1, 1)
Gateway assigns:   kafka_offset=0,    kafka_offset=1,    kafka_offset=2
```

The gateway persists a mapping `kafka_offset → hlc_timestamp` so it can
translate Kafka fetch requests (`FetchOffset: 1`) back to HLC positions.

**Pros:** Clean Kafka semantics. Offset is contiguous.
**Cons:** Gateway is stateful. Multiple gateway instances for the same
partition need coordination (or partitioned gateway ownership).

### Option B: HLC-to-Integer Encoding

Encode the HLC timestamp as a 64-bit integer: `(physical_ms << 16) | logical`.
This preserves ordering and is directly usable as a Kafka-like offset.

```
HLC (physical=1679000000000ms, logical=3) → offset = 1679000000000 << 16 | 3
```

**Pros:** Stateless gateway, no mapping table.
**Cons:** Offsets are non-contiguous (gaps between physical timestamps).
Kafka clients that do `offset + 1` to fetch the next message will skip.
However, Kafka's `ListOffsets` API supports `EARLIEST` / `LATEST` / `BY_TIMESTAMP`,
so well-behaved clients don't manually increment offsets.

---

## Part 6: Publisher Lifecycle Management

### The Problem

Publishers are ephemeral — a microservice instance starts, publishes events,
and eventually dies (crash, scale-down, deployment). But the state they leave
behind is not ephemeral:

1. **Store gap tracker:** `PublisherSeqState` in `FjallBackend` maintains
   `highest_seen`, `committed_seq`, and `gaps` per publisher. A dead
   publisher's entry persists forever in `publisher_states: HashMap<PublisherId, ...>`.

2. **Watermark payload:** `CommitWatermark` includes a `publishers` map. Every
   dead publisher appears in every watermark broadcast, indefinitely. With 1000
   publishers over time and 100ms watermark interval, this is 1000 entries ×
   10/s = non-trivial serialization and network cost.

3. **Subscriber gap detector:** `GapDetector` tracks per-publisher state. Dead
   publishers' entries consume memory in every subscriber forever.

4. **HLC replay index:** Dead publishers' events remain in the replay index
   (this is correct — they're historical data). But the gap state and watermark
   entries for dead publishers serve no purpose after their events are fully
   committed.

5. **Sequence number reuse:** If a publisher crashes and restarts with a new
   `PublisherId` (UUID v4/v7), everything works — it's a new identity. But if
   publisher identity is derived from a stable name (e.g., pod hostname in
   Kubernetes), the restarted publisher might reuse the same ID and start from
   seq 0, colliding with previously stored events.

### Growth Model

| Timeframe | Publishers seen | Watermark entries | Memory (gap state) |
|-----------|---------------|-------------------|--------------------|
| Day 1 | 10 | 10 | ~1KB |
| Month 1 (daily deploys) | 300 | 300 | ~30KB |
| Year 1 (CI/CD + autoscale) | 10,000+ | 10,000+ | ~1MB+ |
| Year 1 (watermark JSON) | — | ~10KB per broadcast | ~100KB/s network |

The gap state memory is manageable, but the watermark broadcast grows linearly
with every publisher that has ever existed. This is the real scalability
concern.

### The Liveness Detection Problem

The obvious approach is Zenoh **liveliness tokens** — each publisher declares a
token, and its revocation signals death. But liveliness tracks **network
reachability**, not **process liveness**. A network partition between the
publisher and the store revokes the token even though the publisher is still
alive, still publishing (possibly to other reachable parts of the network), and
will reconnect when the partition heals.

```
Publisher P1 ──── [network partition] ──── EventStore
   │ still alive                              │ liveliness revoked!
   │ still publishing                         │ transitions P1 → DRAINING
   │ buffering events                         │ ... grace period expires ...
   │                                          │ transitions P1 → ARCHIVED
   │ [partition heals]                        │ gap state GONE
   │ events arrive ──────────────────────────→│ P1 treated as new publisher?
```

If the store archived P1 during the partition, it lost the gap state. When P1's
events arrive after reconnection, the store sees sequences that don't match any
tracked publisher (or worse, re-creates P1's state from scratch, treating seq N
as the first event and recording phantom gaps for 0..N-1).

**Liveliness alone is not safe for lifecycle transitions.**

### Solution: Multi-Signal Liveness Detection

Use liveliness as a **fast hint** but require **inactivity confirmation** before
any state transition. The ground truth for "publisher is alive" is: **the store
is still receiving events or heartbeats from it.**

#### Signal Sources

| Signal | Detects | Latency | False Positive on Partition? |
|--------|---------|---------|----------------------------|
| Liveliness token | Network reachability | ~ms (Zenoh detection) | **Yes** — revoked when unreachable |
| Event arrival | Publisher is sending | 0 (inline) | No — if events arrive, publisher is reachable |
| Heartbeat beacon | Publisher is alive (even if idle) | Heartbeat interval (1s default) | **Yes** — heartbeats also stop during partition |
| Inactivity timeout | Publisher stopped for real | Configurable (e.g., 5 min) | No — only fires after sustained silence |

**The key rule:** A publisher is considered dead only when **all signals are
absent for the inactivity timeout.** Liveliness revocation alone starts a
suspicion timer, but any arriving event or heartbeat from that publisher
**cancels the timer** and keeps the publisher ACTIVE.

```rust
// Store-side per-publisher tracking
struct PublisherLiveness {
    state: PublisherState,           // ACTIVE, SUSPECTED, DRAINING, ARCHIVED
    last_activity: Instant,          // updated on every event/heartbeat from this publisher
    liveliness_live: bool,           // current liveliness token status
    suspicion_deadline: Option<Instant>, // when SUSPECTED transitions to DRAINING
}
```

#### State Machine

```
                        liveliness restored
                        OR event/heartbeat arrives
                   ┌──────────────────────────────┐
                   │                              │
                   ▼                              │
  [new event] → ACTIVE ──── liveliness revoked ──→ SUSPECTED
                   ▲                                   │
                   │        event/heartbeat arrives     │
                   ├───────────────────────────────────┘
                   │                                   │
                   │        no activity for             │
                   │        inactivity_timeout          ▼
                   │                              DRAINING
                   │        event arrives               │
                   │        (late, in-flight)           │
                   └───────────────────────────────────┘
                                                       │
                                                       │ drain_grace_period
                                                       │ expires AND no
                                                       │ events arrived
                                                       ▼
                                                  ARCHIVED → (GC)
```

**State definitions:**

```
ACTIVE:     Publisher is live. Liveliness token present, or events/heartbeats
            arriving. Full gap tracking, included in watermark.

SUSPECTED:  Liveliness token revoked, but inactivity timeout has not yet
            elapsed. The publisher might be behind a temporary network
            partition. Full gap tracking continues. Included in watermark.
            If any event or heartbeat arrives → back to ACTIVE.
            If liveliness token reappears → back to ACTIVE.

DRAINING:   Inactivity timeout elapsed while SUSPECTED. Publisher is
            considered dead. Continue gap tracking for a shorter grace
            period (drain_grace_period, e.g., 2 min) to catch any final
            in-flight events. Still included in watermark.
            If events arrive → back to ACTIVE (publisher was partitioned
            longer than expected but recovered).

ARCHIVED:   Drain grace period expired with no new events. Final
            committed_seq frozen. Removed from watermark broadcast.
            Gap state dropped from memory. Events remain in storage.
            If events arrive from this publisher_id → re-create state
            from storage (the publisher somehow recovered after a very
            long partition). Log a warning.

GC:         Events older than retention policy are garbage collected
            along with any archived metadata.
```

**Timing defaults:**

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `inactivity_timeout` | 5 min | How long to wait after liveliness loss before declaring DRAINING |
| `drain_grace_period` | 2 min | How long DRAINING lasts before ARCHIVED |
| `heartbeat_interval` | 1s | How often publisher sends heartbeat (already exists) |

The 5-minute inactivity timeout is deliberately long — most network partitions
heal within seconds to minutes. Only a truly dead publisher (process crash +
no restart) stays silent for 5 minutes.

#### Why Not Liveliness Alone?

| Scenario | Liveliness Only | Multi-Signal |
|----------|----------------|--------------|
| Publisher crashes | Token revoked → correct death detection | Token revoked + no events → correct death detection |
| Brief network partition (30s) | Token revoked → **false death**, state evicted | Token revoked → SUSPECTED → events resume → back to ACTIVE |
| Long network partition (10 min) | Token revoked → state evicted, events lost on recovery | SUSPECTED → DRAINING → ARCHIVED. On recovery, state re-created from storage |
| Publisher idle (not publishing) | Token present → ACTIVE (correct) | Token present → ACTIVE. If token lost and no heartbeats → SUSPECTED (heartbeats prevent false positive) |
| Slow publisher (rare events) | Token present → ACTIVE | Token present → ACTIVE. On partition: SUSPECTED, but next event arrives → ACTIVE |

#### Implementation Sketch

```rust
// Store-side: called on every watermark tick (100ms)
fn update_publisher_liveness(&mut self) {
    let now = Instant::now();
    for (pub_id, liveness) in &mut self.publisher_liveness {
        match liveness.state {
            PublisherState::Active => {
                if !liveness.liveliness_live
                    && now.duration_since(liveness.last_activity) > self.inactivity_timeout
                {
                    liveness.state = PublisherState::Draining;
                    liveness.drain_deadline = Some(now + self.drain_grace_period);
                }
            }
            PublisherState::Suspected => {
                if liveness.liveliness_live {
                    // Liveliness restored
                    liveness.state = PublisherState::Active;
                    liveness.suspicion_deadline = None;
                } else if now >= liveness.suspicion_deadline.unwrap() {
                    // Inactivity timeout elapsed while suspected
                    liveness.state = PublisherState::Draining;
                    liveness.drain_deadline = Some(now + self.drain_grace_period);
                }
            }
            PublisherState::Draining => {
                if let Some(deadline) = liveness.drain_deadline {
                    if now >= deadline {
                        liveness.state = PublisherState::Archived;
                    }
                }
            }
            PublisherState::Archived => {} // handled by GC
        }
    }
}

// Called when a liveliness token is revoked
fn on_liveliness_revoked(&mut self, pub_id: PublisherId) {
    if let Some(liveness) = self.publisher_liveness.get_mut(&pub_id) {
        liveness.liveliness_live = false;
        if liveness.state == PublisherState::Active {
            liveness.state = PublisherState::Suspected;
            liveness.suspicion_deadline =
                Some(Instant::now() + self.inactivity_timeout);
        }
    }
}

// Called when an event or heartbeat arrives from a publisher
fn on_publisher_activity(&mut self, pub_id: PublisherId) {
    let liveness = self.publisher_liveness
        .entry(pub_id)
        .or_insert_with(|| PublisherLiveness::new_active());
    liveness.last_activity = Instant::now();

    // Any activity cancels suspicion or draining
    match liveness.state {
        PublisherState::Suspected | PublisherState::Draining => {
            liveness.state = PublisherState::Active;
            liveness.suspicion_deadline = None;
            liveness.drain_deadline = None;
        }
        _ => {}
    }
}
```

### Watermark Compaction

Only `ACTIVE`, `SUSPECTED`, and `DRAINING` publishers appear in the
`CommitWatermark`:

```rust
pub struct CommitWatermark {
    pub partition: u32,
    /// Only currently active / suspected / draining publishers.
    pub publishers: HashMap<PublisherId, PublisherWatermark>,
    /// Epoch counter — increments on each publisher state transition.
    /// Consumers can detect they missed a publisher transition.
    pub epoch: u64,
    pub timestamp: DateTime<Utc>,
}
```

When a publisher transitions to `ARCHIVED`, it's removed from the watermark.
The `epoch` counter increments so consumers know the publisher set changed.

A consumer that needs the final state of an archived publisher can query the
store directly:

```
session.get("{store_prefix}?publisher_id={pub_id}&meta=watermark")
```

### Stable Publisher Identity

For publishers with stable identity (Kubernetes pod name, service instance ID),
the restarted publisher must not collide with its previous incarnation:

**Option A: Incarnation counter.** Publisher identity is `(stable_name, incarnation)`.
Each restart increments the incarnation. The store sees these as distinct
publishers.

```rust
PublisherId = hash(stable_name + incarnation)
// or
PublisherId = Uuid::new_v7()  // always unique, even with same name
```

**Option B: Resume from checkpoint.** The publisher queries the store for its
last committed seq and resumes from there. This requires the publisher to
persist its identity (or derive it deterministically) and the store to expose
the last-known seq:

```rust
// Publisher startup
let my_id = PublisherId::from_name("order-service-pod-3");
let last_seq = session.get(
    format!("{store_prefix}?publisher_id={my_id}&meta=committed_seq")
).await?;
self.next_seq = AtomicU64::new(last_seq + 1);
```

**Option C: Always use UUID v7.** Every publisher instance gets a globally
unique `Uuid::now_v7()`. No collision possible. The downside is that the store
accumulates one entry per lifetime, which is exactly what the lifecycle protocol
above handles.

**Recommendation: Option C (UUID v7) + lifecycle protocol.** This is the
simplest correct solution. The lifecycle protocol handles accumulation. Option B
is only needed for true exactly-once semantics across restarts, which requires
additional infrastructure (see [03_durability.md](03_durability.md) Strategy D:
Producer WAL).

### Subscriber-Side Cleanup

Subscribers also accumulate per-publisher gap detector state. The same
multi-signal approach applies:

- **Liveliness revocation:** Mark publisher as SUSPECTED in the gap detector.
  Continue tracking gaps normally.
- **Inactivity timeout (no events/heartbeats for 5 min):** Drop the publisher's
  gap detector state.
- **Event arrives from dropped publisher:** Re-create gap detector state for
  that publisher. The first event becomes the new baseline (no phantom gaps for
  sequences that arrived before the cleanup).

```rust
// In EventSubscriber
fn on_publisher_activity(&mut self, publisher_id: PublisherId, seq: u64) {
    if !self.gap_detector.has_publisher(&publisher_id) {
        // Publisher was cleaned up but recovered (partition healed).
        // Re-initialize with current seq as baseline.
        self.gap_detector.init_publisher(publisher_id, seq);
        tracing::warn!(
            %publisher_id, seq,
            "re-initialized gap detector for recovered publisher"
        );
    }
}
```

After the inactivity timeout, the subscriber drops the publisher's tracking
state. Late-arriving events from a truly dead publisher are still delivered
(they just skip gap detection — no new events will follow anyway).

---

## Part 7: Interaction Between Replay Ordering and Publisher Lifecycle

### Replay After Publisher Death

When an `ARCHIVED` publisher's events are replayed, the HLC replay index
provides correct ordering. The publisher's events are interleaved with other
publishers' events by HLC timestamp, regardless of whether the publisher is
still alive.

No special handling needed — the replay index is immutable once written. The
publisher lifecycle protocol only affects the _live_ state tracking (gaps,
watermarks), not the stored events or replay index.

### Consumer Checkpoint Across Publisher Transitions

A consumer checkpoints its position as an HLC timestamp. When it resumes:

1. It scans the replay index from its checkpoint HLC forward.
2. New publishers that started after the checkpoint appear naturally in the
   HLC-ordered stream.
3. Dead publishers' events (from before the checkpoint) don't appear — they're
   before the checkpoint HLC.
4. Dead publishers' events (from after the checkpoint, in-flight at death time)
   appear normally in the replay stream.

The consumer doesn't need to know which publishers are alive or dead. The HLC
replay index abstracts this away.

### Epoch-Based Consumer Group Coordination

When the publisher set changes (publisher joins or dies), the watermark `epoch`
increments. Consumer groups can use this as a trigger to re-evaluate partition
assignments or flush in-progress aggregations:

```
epoch 5: publishers = {P1, P2, P3}
P2 dies → epoch 6: publishers = {P1, P3}
P4 joins → epoch 7: publishers = {P1, P3, P4}
```

A consumer processing a windowed aggregation can use epoch boundaries as
"barrier" points — ensuring all events from the old publisher set are processed
before incorporating events from the new set.

---

## Part 8: Comparison with Other Systems

### How Others Handle Publisher Lifecycle

| System | Publisher Identity | On Publisher Death | State Cleanup |
|--------|-------------------|-------------------|---------------|
| Kafka | `producer.id` + epoch (assigned by broker) | Broker detects via session timeout, fences old epoch | Transactional state GC'd after `transactional.id.expiration.ms` (7 days default) |
| NATS JetStream | No publisher identity | Messages are anonymous; server owns sequence | N/A — no per-publisher state |
| Pulsar | `producer_name` (assigned by client or auto) | Broker detects via session close | Producer state GC'd with topic retention |
| Redis Streams | No publisher identity | Messages are anonymous; server assigns IDs | N/A |
| **mitiflow** | **UUID v7 (self-assigned)** | **Multi-signal: liveliness hint + inactivity timeout** | **Lifecycle: ACTIVE → SUSPECTED → DRAINING → ARCHIVED → GC** |

### How Others Handle Replay Ordering

| System | Replay Order | Deterministic Cross-Replica? | Consumer Cursor |
|--------|-------------|------------------------------|-----------------|
| Kafka | Partition offset (total order) | Yes (ISR replicates in order) | Integer offset |
| NATS JetStream | Stream sequence (total order) | Yes (RAFT replicates in order) | Integer sequence |
| Pulsar | Ledger + entry ID | Yes (BookKeeper quorum) | `(ledger_id, entry_id)` |
| Redis Streams | `timestamp-seq` auto-ID | N/A (single node) | String ID |
| **mitiflow** | **HLC timestamp order** | **Yes (same HLC on all replicas)** | **HLC timestamp** |

Note: Kafka, NATS, and Pulsar achieve deterministic cross-replica replay
because their leader serializes writes and replicas copy the leader's order.
mitiflow achieves it without a leader by using publisher-assigned HLC
timestamps as the ordering key.

---

## Summary

| Concern | Solution | Trade-off |
|---------|----------|-----------|
| **Deterministic replay** | HLC-ordered secondary index in fjall | ~60 bytes/event storage overhead |
| **Cross-replica consistency** | Zenoh HLC travels with the event | Ordering accuracy bounded by NTP (~ms) |
| **Consumer cursor portability** | HLC timestamp as position | Not a simple integer (Kafka gateway adds mapping) |
| **Publisher state accumulation** | Multi-signal lifecycle (ACTIVE → SUSPECTED → DRAINING → ARCHIVED → GC) | ~7 min total retention per dead publisher (5 min suspicion + 2 min drain) |
| **Watermark bloat** | Only ACTIVE + SUSPECTED + DRAINING publishers in watermark; epoch counter | Consumers must handle epoch transitions |
| **Network partition resilience** | Liveliness is a hint only; event/heartbeat arrival overrides suspicion | Slower death detection than liveliness-only (~5 min vs ~instant) |
| **Publisher identity collision** | UUID v7 (always unique) | Store accumulates one entry per publisher lifetime |
| **Publisher restart with resume** | Query store for last committed seq (optional) | Requires stable identity + store query round-trip |
