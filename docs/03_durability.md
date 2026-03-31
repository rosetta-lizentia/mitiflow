# Durability & Watermark Protocol

**Status:** Implemented — EventStore with FjallBackend, watermark broadcast, `publish_durable()`, GC.

How mitiflow ensures events are never lost, without requiring every producer to
maintain local storage.

---

## The Problem

Zenoh's reliability pipeline is in-memory. If the producer process crashes after
`put()` returns but before any subscriber has durably stored the event, the event
is lost.

```
Producer                     Zenoh Transport              Event Store
   │                              │                           │
   │ 1. put(payload, Block)       │                           │
   │ ─────────────────────────────>                           │
   │                              │ 2. reliable delivery      │
   │                              │ ──────────────────────────>
   │                              │                           │ 3. persist (fsync)
   │                              │                           │
   │          DURABILITY GAP: steps 1→3 are not atomic        │
```

---

## Strategy Matrix

| Strategy | Guarantee | Latency | Producer State | Complexity |
|----------|-----------|---------|---------------|------------|
| **A. Accept the gap** | At-least-once, best-effort durable | Lowest (~µs) | None | None |
| **B. Watermark stream** ⭐ | Confirmed durable | +batch interval (~100ms) | None | Low |
| **C. Inbox pattern** | Durable before broadcast | +1 write | None | Medium |
| **D. Producer WAL** | Crash-proof durable | +1 fsync (~ms) | fjall dir | Medium |

---

## Strategy A: Accept the Gap

Use `CongestionControl::Block` + publisher cache + Event Store recovery.

The only loss scenario: **producer crash while Event Store is simultaneously
unreachable**. The window is microseconds wide.

**Use for:** UI updates, trace steps, status broadcasts, telemetry — any event
where rare loss is acceptable.

---

## Strategy B: Watermark Stream ⭐

The primary innovation. The Event Store publishes batched commit progress;
publishers subscribe and block until covered.

### Sequence Model

Sequences are assigned **per (partition, publisher)** — each publisher maintains
an independent monotonic counter for each partition it writes to. This ensures
contiguous sequences within each stream without any coordination between
publishers. See [04_ordering.md](04_ordering.md) for the design rationale.

### Protocol

```
Time ──────────────────────────────────────────────────────>

Publisher P1       Event Store
  │                     │
  │ put(seq=100)        │
  │────────────────────>│ persists (p0, P1, 100)
  │                     │
  │ put(seq=101)        │
  │────────────────────>│ persists (p0, P1, 101)
  │                     │
  │                     │ [100ms tick]
  │     watermark       │ put(watermark, {
  │<────────────────────│   publishers: {
  │                     │     P1: { committed_seq: 101, gaps: [] },
  │                     │     P2: { committed_seq: 42,  gaps: [38] }
  │                     │   }
  │                     │ })
  │                     │
  │ "my pub_id=P1,      │
  │  seq 100 ≤ 101      │
  │  && 100 ∉ gaps"     │
  │ → DURABLE ✓         │
```

### Watermark Types

```rust
/// Per-publisher durability progress.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublisherWatermark {
    /// Highest contiguous sequence durably stored for this publisher.
    pub committed_seq: u64,
    /// Sequence numbers below committed_seq that are still missing (gaps).
    pub gaps: Vec<u64>,
}

/// Commit watermark broadcast by the Event Store.
///
/// Published periodically on `{key_prefix}/_watermark`.
/// Only includes publishers in ACTIVE, SUSPECTED, or DRAINING lifecycle state.
/// See [08_replay_ordering.md](08_replay_ordering.md) § Publisher Lifecycle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitWatermark {
    /// Per-publisher durability progress (only active/suspected/draining).
    pub publishers: HashMap<PublisherId, PublisherWatermark>,
    /// Epoch counter — increments on publisher state transitions.
    pub epoch: u64,
    /// Event Store wall-clock time.
    pub timestamp: DateTime<Utc>,
}

impl CommitWatermark {
    /// Check whether a specific sequence from a publisher is confirmed durable.
    pub fn is_durable(&self, publisher_id: &PublisherId, seq: u64) -> bool {
        self.publishers
            .get(publisher_id)
            .is_some_and(|pw| seq <= pw.committed_seq && !pw.gaps.contains(&seq))
    }
}
```

### Event Store Side

The store tracks per-publisher gap state incrementally. On each `store()` call,
it removes the seq from gaps (if present), extends the publisher's
`highest_seen`, and advances `committed_seq` past any filled positions.

Gaps are tracked using a `BTreeSet<u64>` per publisher — a sparse bitset that is
O(1) memory when empty and O(log g) for insert/remove/contains where g is the
gap count. In stable networks, gaps are rare, so the set stays near-empty.

```rust
impl StorageBackend for FjallBackend {
    fn publisher_watermarks(&self) -> HashMap<PublisherId, PublisherWatermark> {
        // Returns current per-publisher committed_seq + gaps
        // from incrementally-maintained in-memory state.
    }
}
```

The watermark task broadcasts every `watermark_interval` (default 100ms), or
earlier if an event carries an urgency hint (`durable_urgency(0ms)` = immediate):

```rust
async fn run_watermark_task(...) {
    loop {
        // Wait for periodic tick or urgency deadline
        let watermark = CommitWatermark {
            publishers: backend.publisher_watermarks(),
            timestamp: Utc::now(),
        };
        session.put(&watermark_key, serde_json::to_vec(&watermark)?).await?;
    }
}
```

### Publisher Side

```rust
impl EventPublisher {
    pub async fn publish_durable<T: Serialize>(&self, event: &Event<T>) -> Result<u64> {
        let seq = self.publish(event).await?;

        tokio::time::timeout(
            self.config.durable_timeout,
            self.wait_for_watermark(seq),
        ).await.map_err(|_| Error::DurabilityTimeout { seq })??;

        Ok(seq)
    }

    async fn wait_for_watermark(&self, target: u64) -> Result<()> {
        let my_id = &self.publisher_id;
        loop {
            let wm = self.watermark_rx.recv_async().await?;
            if wm.is_durable(my_id, target) {
                return Ok(());
            }
        }
    }
}
```

### Why Watermark > Per-Event ACK

| Aspect | Watermark Stream | Per-Event Query ACK |
|--------|-----------------|---------------------|
| Network cost | 1 broadcast / batch interval | 1 query + 1 reply per event |
| Publisher latency | Batch interval (configurable) | 1 full RTT per event |
| Event Store load | 1 `put()` per batch | 1 queryable handler per event |
| Scales with publishers | O(1) | O(N × event_rate) |

### Gap Handling

If the Event Store's subscriber detects a missed sequence (via mitiflow's gap
detector), the gap appears in the publisher's `gaps` field. The publisher sees
its sequence in the gap list and continues waiting. Once the Event Store
recovers the gap (via `session.get()` to the publisher's cache queryable), the
next watermark clears it.

If the gap is **irrecoverable** (publisher cache evicted), the Event Store's
miss handler can:
1. Log the gap via tracing
2. Keep it in `gaps[]` permanently (publisher eventually times out with `DurabilityTimeout`)
3. Optionally alert via a separate Zenoh topic

---

## Quorum Watermarks (Replicated Stores)

When multiple Event Store replicas subscribe to the same key expression (see
[05_replication.md](05_replication.md)), each replica independently publishes its
own watermark. The publisher waits for a **quorum** of replicas to confirm
durability before considering an event durable.

### Durability Levels

| Level | Behavior | Equivalent |
|-------|----------|------------|
| `Durability::None` | Fire-and-forget, don't wait for any watermark | Kafka `acks=0` |
| `Durability::One` | Wait for any 1 replica's watermark | Kafka `acks=1` |
| `Durability::Quorum` | Wait for majority of replicas | Kafka `acks=all` + `min.insync.replicas` |
| `Durability::All` | Wait for all known replicas | Strongest, highest latency |

Each replica publishes its watermark on `{key_prefix}/_watermark/{replica_id}`.
The publisher subscribes to `{key_prefix}/_watermark/*` and tracks the latest
watermark from each replica:

```rust
struct QuorumTracker {
    replicas: HashMap<ReplicaId, CommitWatermark>,
    quorum: usize,
}

impl QuorumTracker {
    fn is_durable(&self, publisher_id: &PublisherId, seq: u64) -> bool {
        let confirmed = self.replicas.values()
            .filter(|wm| wm.is_durable(publisher_id, seq))
            .count();
        confirmed >= self.quorum
    }
}
```

No Raft consensus, no leader election — just counting independent confirmations.
See [05_replication.md](05_replication.md) for the full replication design,
failure modes, and recovery protocol.

### Consumer Group Offset Commits

Consumer group offset commits follow the same durability model. Consumers
publish offset commits via `put()` to `_offsets/{partition}/{group_id}`, and
the EventStore persists them in a dedicated `offsets` keyspace. A `commit_sync()`
uses a queryable round-trip to confirm the store has persisted the commit,
analogous to how `publish_durable()` waits for watermark confirmation. With
replicated stores, offset commits are received by all replicas via the same
pub/sub fan-out. See [11_consumer_group_commits.md](11_consumer_group_commits.md)
for the full design.

---

## Strategy C: Inbox Pattern

The producer writes to an inbox key expression that the Event Store subscribes
to and persists **before** re-publishing to the main event stream.

```
Producer → put("myapp/inbox/{event_id}", payload) → Event Store persists first
                                                   → Event Store re-publishes to "myapp/events/..."
```

The Event Store becomes the authoritative publisher. Strongest guarantee, but
changes the publish flow:
- Events go through store before consumers see them
- Adds write latency
- Event Store is on the critical path for all publishes

**Use for:** financial transactions, audit trails — when zero loss is
non-negotiable and the extra hop is acceptable.

---

## Strategy D: Producer WAL (Optional Feature)

For truly crash-proof publishing, embed a local WAL in the producer:

```rust
pub struct DurablePublisher {
    inner: EventPublisher,
    wal: fjall::Keyspace,
}

impl DurablePublisher {
    pub async fn publish<T: Serialize>(&self, event: &Event<T>) -> Result<()> {
        let seq = self.wal.append(&event)?;   // fsync
        self.inner.publish(event).await?;      // to Zenoh
        Ok(())
    }

    /// On startup, re-publish any events that weren't confirmed
    pub async fn recover(&self) -> Result<usize> {
        let unpublished = self.wal.replay_from(self.last_confirmed_seq()?)?;
        for event in unpublished {
            self.inner.publish(&event).await?;
        }
        Ok(unpublished.len())
    }
}
```

Enabled via `feature = "wal"`.

**Use for:** regulatory requirements where "event in WAL = legally committed."

---

## Recommended Defaults

| Event Criticality | Strategy | Method |
|-------------------|----------|--------|
| Telemetry, UI updates | A (Accept gap) | `publisher.publish(&event)` |
| Business events | B (Watermark) | `publisher.publish_durable(&event)` |
| Business + HA | B + Quorum | `publisher.publish_durable_quorum(&event)` |
| Financial / audit | C or D | Inbox or WAL |

---

## Tuning Knobs

| Parameter | Default | Effect |
|-----------|---------|--------|
| `watermark_interval` | 100ms | Lower = faster confirmation, more network traffic |
| `durable_timeout` | 5s | How long `publish_durable()` waits before error |
| `durable_urgency` | None | 0ms = immediate watermark on this event's arrival |
| `cache_size` | 10,000 | Larger = longer recovery window, more memory |
| `heartbeat` | Sporadic(1s) | Faster = quicker gap detection, more traffic |
| `quorum` | 1 (single store) | Set to N/2+1 for replicated deployments |
