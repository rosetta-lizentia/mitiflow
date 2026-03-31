# Pub/Sub-Based Storage Replication

**Status:** Design document — multi-store deployment works; quorum watermark not yet implemented.

How mitiflow achieves replicated durability without Raft, leveraging Zenoh's
pub/sub as the replication transport.

---

## The Insight

Traditional replicated logs (Kafka ISR, NATS JetStream RAFT, Pulsar BookKeeper)
use a **leader** that serializes writes and replicates to followers via a
dedicated protocol. The leader is a coordination point — it decides ordering,
manages follower state, and handles failover.

mitiflow's `EventStore` is already a Zenoh subscriber. Multiple stores
subscribing to the same key expression **all receive the same events** — Zenoh's
pub/sub provides fan-out natively. This means:

> **Replication is just multiple subscribers.**

No leader election. No replication protocol. No log shipping. Each replica
independently subscribes, independently persists, and independently publishes
its own watermark. The publisher waits for a **quorum** of watermarks before
confirming durability.

```
                        Zenoh Network (fan-out)
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
        ┌──────────┐   ┌──────────┐   ┌──────────┐
        │ Store R0 │   │ Store R1 │   │ Store R2 │
        │ sub+persist  │ sub+persist  │ sub+persist
        │ watermark │   │ watermark │   │ watermark │
        └──────────┘   └──────────┘   └──────────┘
              │               │               │
              ▼               ▼               ▼
        wm on           wm on           wm on
        $wm/r0          $wm/r1          $wm/r2
              │               │               │
              └───────────────┼───────────────┘
                              ▼
                     Publisher subscribes
                     to $wm/** and waits
                     for quorum (2 of 3)
```

---

## Protocol Design

### Replica Identity

Each `EventStore` replica has a unique **replica ID** (e.g., `ReplicaId(Uuid)`).
The replica ID is included in the watermark key expression so publishers can
distinguish watermarks from different replicas:

```
Watermark key: {key_prefix}/_watermark/{replica_id}
```

A publisher subscribes to `{key_prefix}/_watermark/*` and receives watermarks
from all replicas.

### Quorum Watermark at the Publisher

The publisher maintains a map of the latest watermark from each replica. On each
watermark update, it checks whether a **quorum** (configurable, default =
majority) of replicas have confirmed the target sequence:

```rust
struct QuorumTracker {
    /// Latest watermark from each known replica.
    replicas: HashMap<ReplicaId, CommitWatermark>,
    /// How many replicas must confirm for durability.
    quorum: usize,
}

impl QuorumTracker {
    fn update(&mut self, replica_id: ReplicaId, wm: CommitWatermark) {
        self.replicas.insert(replica_id, wm);
    }

    fn is_durable(&self, publisher_id: &PublisherId, seq: u64) -> bool {
        let confirmed = self.replicas.values()
            .filter(|wm| wm.is_durable(publisher_id, seq))
            .count();
        confirmed >= self.quorum
    }
}
```

### Durability Levels

The quorum requirement is configurable per publish call, analogous to Kafka's
`acks` setting:

| Level | Behavior | Equivalent |
|-------|----------|------------|
| `Durability::None` | Fire-and-forget, don't wait for any watermark | Kafka `acks=0` |
| `Durability::One` | Wait for any 1 replica's watermark | Kafka `acks=1` |
| `Durability::Quorum` | Wait for majority of replicas | Kafka `acks=all` (with `min.insync.replicas`) |
| `Durability::All` | Wait for all known replicas | Strongest, highest latency |

```rust
pub async fn publish_durable(
    &self,
    event: &Event<T>,
    durability: Durability,
) -> Result<u64>
```

### Replica Discovery

Publishers need to know how many replicas exist to compute quorum. Two options:

**Option 1: Liveliness tokens (dynamic).** Each replica declares a liveliness
token on `{key_prefix}/_store/{replica_id}`. Publishers subscribe to
`{key_prefix}/_store/*` liveliness and dynamically discover/forget replicas.
Quorum adjusts as replicas join/leave.

```rust
// Replica side
session.liveliness().declare_token(
    format!("{key_prefix}/_store/{replica_id}")
).await?;

// Publisher side
let replicas = session.liveliness()
    .get(format!("{key_prefix}/_store/*"))
    .await?;
let quorum = replicas.len() / 2 + 1;
```

**Option 2: Static configuration.** The publisher is configured with
`replica_count: usize`. Simpler, but doesn't adapt to failures.

Liveliness tokens are the natural choice — mitiflow already uses them for
consumer group membership in `PartitionManager`.

---

## Why This Works Without Raft

Raft (and Paxos, ISR, etc.) solve two problems:

1. **Leader election:** Who serializes writes?
2. **Log replication:** How do followers get the same data as the leader?

mitiflow doesn't need either:

### No Leader Election Needed

With per-(partition, publisher) sequences (see [04_ordering.md](04_ordering.md)), **there is no total order to agree on**. Each publisher independently
assigns its own sequences. The store doesn't assign offsets — it just persists
what arrives. There is no "which write comes first?" question that requires a
leader to answer.

Two replicas receiving the same event from publisher P1 with seq=42 will both
store it under the same key `(partition, P1, 42)`. There's no conflict, no
ordering ambiguity, no need for arbitration.

### No Log Replication Needed

Zenoh's pub/sub already ensures all subscribers receive the same messages
(subject to Zenoh's reliability guarantees). Each replica subscribes to the same
key expression and independently persists. This is the replication mechanism.

Compare:

| | Kafka ISR | NATS RAFT | mitiflow pub/sub |
|---|---|---|---|
| Replication transport | Leader → follower TCP | Leader → follower RAFT log | Zenoh fan-out (native) |
| Who sends to replicas? | Partition leader | RAFT leader | Publishers directly |
| Ordering agreement | Leader serializes | RAFT consensus | Not needed (per-publisher seq) |
| Replica catches up via | Fetch from leader | RAFT snapshot + log | Query peer replica |
| Leader election on failure | ZK/KRaft vote | RAFT vote | Not needed |

### What About Consistency?

Without a leader, can replicas diverge? Possible scenarios:

**Scenario 1: Network partition.** Replica R0 receives events A, B, C. Replica
R1 only receives A, C (missed B due to temporary network issue).

- R1's gap tracker detects the missing seq for B.
- R1's watermark reports `committed_seq` stuck below B's seq, with B in gaps.
- Publisher sees R1 hasn't confirmed B, so quorum isn't met for B until R1
  recovers (or if R0 alone satisfies quorum=1).
- B is not "lost" — it's in R0 and in the publisher's cache. R1 can recover
  (see Recovery section).

**Scenario 2: Replica crash.** R2 crashes and restarts. It has events up to
some point in its fjall database.

- On restart, R2 subscribes again and receives new events.
- For the gap between its last persisted event and the current stream position,
  it runs recovery (see below).
- Its watermark will be behind until recovery completes. Publishers waiting on
  quorum simply exclude R2's stale watermark.

**Scenario 3: Slow replica.** R1 is slow (disk I/O contention). It's behind by
a few seconds.

- R1's watermark reflects its actual persisted position. Publishers don't count
  R1 in quorum until it catches up. With quorum=2 of 3, this is fine — R0 and
  R2 provide durability.

In all cases, the system remains correct:
- **Safety:** A publisher only considers an event durable when `quorum` replicas
  have confirmed it. A stale/behind replica is simply not counted.
- **Liveness:** As long as `quorum` replicas are healthy and reachable,
  publishers make progress.

---

## Replica Recovery

When a replica falls behind (crash, network issue, slow), it needs to fill gaps.
Several mechanisms work together:

### 1. Publisher Cache Recovery

mitiflow publishers already maintain an in-memory cache of recent events (served
via Zenoh queryable). A behind replica can query the publisher:

```
R1: session.get("{key_prefix}/{seq}?after_seq=1000&limit=500")
    → publisher P1's cache returns events 1001..1500
```

This works for recent gaps (within the publisher cache window, default 10,000
events). No additional infrastructure needed.

### 2. Peer Replica Query

For gaps beyond the publisher cache window, a behind replica queries a peer
replica. Each `EventStore` already exposes a `queryable` for replay. A
recovering replica can use it:

```
R1 (behind): session.get("{store_key_prefix_of_R0}?after_seq=5000&before_seq=6000")
    → R0 returns stored events 5001..5999
R1 persists each returned event.
```

The key insight: **the queryable API is already built.** Recovery is just a
normal store query directed at a peer.

### 3. Recovery Protocol

On startup (or when a gap is detected in the watermark), a replica runs:

```
Recovery protocol for replica R_behind:

1. Compare own watermark with a peer's watermark.
   For each (partition, publisher_id):
     own_committed = own publisher_watermarks[(part, pub)].committed_seq
     peer_committed = peer publisher_watermarks[(part, pub)].committed_seq
     if peer_committed > own_committed:
       need_recovery[(part, pub)] = (own_committed, peer_committed)

2. For each (part, pub) in need_recovery:
   a. Try publisher cache first:
      session.get("{key_prefix}/**?after_seq={own}&pub_id={pub}")
   b. If publisher cache doesn't cover the full range, query peer replica:
      session.get("{peer_store_prefix}?after_seq={own}&pub_id={pub}")
   c. Persist each recovered event via backend.store()

3. After recovery, normal watermark broadcasting resumes and
   reflects the updated state.
```

### 4. Comparing Watermarks

To know what it's missing, a behind replica needs another replica's watermark.
It can get this by either:

- **Subscribing to peer watermarks.** Each replica already publishes on
  `{key_prefix}/_watermark/{replica_id}`. The recovering replica subscribes
  to `{key_prefix}/_watermark/*` and picks the most advanced peer.
- **Direct query.** A replica exposes a queryable at
  `{key_prefix}/_store/{replica_id}/watermark` that returns its current
  watermark on demand.

The subscribing approach requires no new API — it reuses the existing watermark
stream.

### Consistency During Recovery

During recovery, events are being persisted from two sources: the live Zenoh
subscription (new events) and the recovery query (historical events). This is
safe because:

- The store key `(partition, publisher_id, seq)` is deterministic. Persisting
  the same event twice is an idempotent overwrite — the same key maps to the
  same value.
- The gap tracker in `FjallBackend` handles out-of-order insertion: if seq 5000
  arrives from recovery while seq 5100 already arrived from the live stream,
  seq 5000 fills the gap and `committed_seq` advances past it.

---

## Consistency Guarantees

### What This Provides

| Guarantee | Status | How |
|-----------|--------|-----|
| **Durability** | Quorum-confirmed | Publisher waits for N/2+1 watermarks |
| **No data loss** (quorum replicas alive) | Yes | Quorum ensures at least one surviving replica has every confirmed event |
| **Per-(partition, publisher) ordering** | Yes | Each replica sees the same per-publisher sequences |
| **Consistency across replicas** | Eventual | Replicas converge via pub/sub + recovery; no instantaneous consistency |
| **Read-your-writes** | Per-replica | A query to the replica that confirmed the watermark sees the event |

### What This Does NOT Provide

| Guarantee | Why Not | Mitigation |
|-----------|---------|------------|
| **Total partition order** | No leader to serialize (by design) | Per-(partition, publisher) order suffices for most use cases |
| **Linearizable reads** | Different replicas may be at different positions | Query the most-advanced replica, or use quorum reads |
| **Instant consistency** | Replicas converge asynchronously | Watermark tells you exactly what each replica has |
| **Exactly-once across replicas** | A recovered event might be processed twice by consumers | Consumer-side dedup via `(publisher_id, seq)` or `event_id` |

### Comparison: Consistency Models

| System | Replication | Write Consistency | Read Consistency |
|--------|------------|-------------------|------------------|
| Kafka | ISR (leader → follower) | Leader ack when ISR caught up | Read from leader (or follower with lag) |
| NATS JetStream | RAFT | Quorum ack | Read from leader |
| Pulsar | BookKeeper quorum | Quorum ack | Read from broker (any) |
| **mitiflow** | **Pub/sub fan-out** | **Quorum watermark** | **Per-replica (eventual)** |

mitiflow's consistency model is closest to **Dynamo-style eventual consistency**
(replicas independently process writes, use quorum for durability) but applied
to an ordered event log rather than a key-value store.

---

## Failure Modes

### Replica Crashes and Restarts

1. Replica restarts, opens fjall database (events on disk are intact).
2. `FjallBackend::open()` rebuilds per-publisher gap state from stored events.
3. Replica subscribes to the live event stream — new events flow in.
4. Compares own watermark with peers, runs recovery for any gaps.
5. Within seconds, watermark catches up to peers.
6. Publishers start counting this replica in quorum again.

**Data risk:** Zero, if quorum was met before the crash. Events confirmed by
quorum exist on at least N/2+1 replicas. One crashing doesn't lose data.

### Network Partition (Split Brain)

Replica R2 can't reach publishers or peers, but R0 and R1 can.

- R2 stops receiving events. Its watermark freezes.
- Publishers still reach R0 and R1. With quorum=2 of 3, durability continues.
- When partition heals, R2 compares watermarks with R0/R1 and recovers the gap.

**Data risk:** Zero. R2 is simply excluded from quorum while partitioned.

### All Replicas Crash

If all replicas crash simultaneously:

- Events in publisher caches but not yet watermark-confirmed are at risk.
- Events already confirmed by quorum are on disk in fjall (WAL + memtable
  flush). On restart, each replica recovers from its local database.
- Unconfirmed events: publishers received `DurabilityTimeout` (no watermark
  within 5s). The application's retry logic re-publishes.

**Data risk:** Possible loss of in-flight events that hadn't been confirmed.
Same as Kafka when all ISR replicas crash before fsync.

### Slow Replica (Lag)

A replica with high disk latency falls behind:

- Its watermark is behind. Publishers don't count it in quorum — no impact on
  confirmed durability.
- If the lag exceeds the publisher cache window, recovery must come from a peer
  replica.
- The slow replica is still useful for read queries (can serve stale but
  consistent data).

**Mitigation:** Monitor replica lag (watermark timestamp delta). Alert if a
replica is consistently behind — it may need disk tuning or replacement.

---

## Advantages Over Raft-Based Replication

| Property | Raft/ISR | Pub/Sub Replication |
|----------|---------|---------------------|
| Leader election | Required (complex, slow on failure) | Not needed |
| Replication protocol | Dedicated log shipping | Zenoh fan-out (already exists) |
| Write path | Publisher → Leader → Followers | Publisher → Zenoh → All replicas |
| Failure recovery | New leader election + log catchup | Peer query (existing queryable API) |
| Code complexity | RAFT state machine or ISR protocol | Quorum tracker + recovery query |
| Latency on leader failure | Election timeout (seconds) | No impact (no leader) |
| Network hops for replication | 2 (pub→leader, leader→follower) | 1 (pub→all via Zenoh) |

The main advantage: **no new protocol to implement.** Replication uses Zenoh's
existing fan-out. Recovery uses the existing queryable API. The only new
component is the `QuorumTracker` in the publisher, which is ~50 lines of code.

### Offset Commit Replication

Consumer group offset commits (see [11_consumer_group_commits.md](11_consumer_group_commits.md))
replicate via the same pub/sub fan-out. Consumers publish offsets to
`_offsets/{partition}/{group_id}`, and all EventStore replicas subscribing to
that keyspace receive the commit. Each replica independently persists the offset
in its `offsets` keyspace. This means offset durability comes for free with
store replication — no additional protocol needed.

---

## Implementation Sketch

### Changes to `EventStore`

```rust
pub struct EventStore {
    session: Session,
    backend: Arc<dyn StorageBackend>,
    config: EventBusConfig,
    replica_id: ReplicaId,           // NEW: unique identity
    cancel: CancellationToken,
    _tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl EventStore {
    pub async fn run(&mut self) -> Result<()> {
        // ... existing tasks ...

        // NEW: declare liveliness token for replica discovery
        let _token = self.session.liveliness()
            .declare_token(format!(
                "{}/_store/{}",
                self.config.key_prefix, self.replica_id
            ))
            .await?;

        // Watermark key includes replica_id
        let watermark_key = format!(
            "{}/_watermark/{}",
            self.config.key_prefix, self.replica_id
        );

        // NEW: spawn recovery task on startup
        self._tasks.push(tokio::spawn(run_recovery_task(
            self.session.clone(),
            Arc::clone(&self.backend),
            self.replica_id,
            self.config.clone(),
            self.cancel.clone(),
        )));

        // ... rest unchanged ...
    }
}
```

### Changes to `EventPublisher`

```rust
pub struct EventPublisher {
    // ... existing fields ...
    quorum_tracker: Arc<RwLock<QuorumTracker>>,  // NEW
}

impl EventPublisher {
    async fn wait_for_watermark(&self, seq: u64) -> Result<()> {
        loop {
            let wm_sample = self.watermark_rx.recv_async().await?;
            // Parse replica_id from key expression suffix
            let replica_id = extract_replica_id(wm_sample.key_expr());
            let wm: CommitWatermark = serde_json::from_slice(...)?;

            let mut tracker = self.quorum_tracker.write().await;
            tracker.update(replica_id, wm);

            if tracker.is_durable(&self.publisher_id, seq) {
                return Ok(());
            }
        }
    }
}
```

### Recovery Task

```rust
async fn run_recovery_task(
    session: Session,
    backend: Arc<dyn StorageBackend>,
    own_id: ReplicaId,
    config: EventBusConfig,
    cancel: CancellationToken,
) {
    // 1. Subscribe to peer watermarks
    let peer_wm_sub = session
        .declare_subscriber(format!("{}/_watermark/*", config.key_prefix))
        .await
        .unwrap();

    // 2. Collect peer watermarks, find the most advanced peer
    let own_wmarks = backend.publisher_watermarks();
    let mut best_peer: Option<(ReplicaId, HashMap<_, _>)> = None;

    // Wait briefly to collect peer watermarks
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 3. For each (publisher) where peer is ahead, query peer store
    if let Some((peer_id, peer_wmarks)) = best_peer {
        for (pub_id, peer_pw) in &peer_wmarks {
            let own_pw = own_wmarks.get(pub_id);
            let own_committed = own_pw.map_or(0, |pw| pw.committed_seq);

            if peer_pw.committed_seq > own_committed {
                // Query peer's store for missing events
                let peer_store_key = format!(
                    "{}/_store/{}",
                    config.key_prefix, peer_id
                );
                let replies = session
                    .get(format!("{peer_store_key}?after_seq={own_committed}"))
                    .await
                    .unwrap();

                while let Ok(reply) = replies.recv_async().await {
                    // Persist each recovered event
                    // backend.store(...) is idempotent on (partition, pub_id, seq)
                }
            }
        }
    }
}
```

---

## Open Questions

### 1. Quorum Size and Publisher Configuration

Should `quorum` be a property of the publisher, the topic, or the individual
publish call? Per-call gives maximum flexibility but most API complexity. A
topic-level default with per-call override is a reasonable middle ground.

### 2. Stale Replica Eviction

If a replica's liveliness token disappears (crash/network), publishers
immediately stop counting it in quorum. But what if a replica is alive but
permanently lagging? Should there be a "max lag" threshold after which a replica
is excluded, to avoid reducing effective quorum?

### 3. Read Affinity

When a consumer queries the store for replay, which replica should it query?
Options:
- **Any replica** (simplest, but may get stale data).
- **Most advanced** (query the replica with highest `committed_seq`).
- **Local** (same machine/rack, for latency).

### 4. Garbage Collection Coordination

When a replica runs GC (deleting events older than X), it should not delete
events that a behind peer still needs for recovery. Options:
- **GC watermark:** Before deleting, check that all known replicas have the
  event confirmed. Only GC events below the minimum `committed_seq` across all
  live replicas.
- **Time-based with safety margin:** GC events older than `retention - recovery_window`.
  If retention is 1 hour and recovery window is 10 minutes, GC at 50 minutes.

### 5. Consistency of Recovery Queries

When a recovering replica queries a peer, the peer's store is still receiving
new events. The query result is a point-in-time snapshot. This is fine because:
- Recovery is idempotent (re-inserting the same event is a no-op).
- The live subscription fills in events arriving after the query snapshot.
- Small gaps between the query snapshot and the live stream are handled by the
  existing gap tracker.

But if the gap is very large, recovery may need multiple rounds of queries.
A "streaming recovery" mode (subscribe to peer's watermark + query in parallel)
would handle this efficiently.
