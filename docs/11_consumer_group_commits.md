# 11 — Consumer Group Commits & Orchestration

How mitiflow supports Kafka-style consumer group offset commits, with offsets
managed as part of the EventStore for simplicity.

---

## Part 1: The Problem

### Current State

mitiflow has the building blocks for consumer groups but lacks coordinated
offset management:

| Component | Status | What It Does |
|-----------|--------|-------------|
| `PartitionManager` | ✅ Exists | HRW-based partition assignment + liveliness-driven rebalancing |
| `on_rebalance(gained, lost)` | ✅ Exists | Callback when partitions are reassigned |
| `SequenceCheckpoint` | ✅ Exists | Per-(publisher, partition) checkpoint stored locally in fjall |
| `GapDetector::with_checkpoints()` | ✅ Exists | Pre-seeds tracker from restored checkpoints |
| Consumer group offset commit | ✅ Implemented | `commit_sync()` / `commit_async()` on `ConsumerGroupSubscriber` |
| Generation fencing | ✅ Implemented | `PartitionManager::current_generation()` + fenced `commit_offsets()` on `FjallBackend` |
| Rebalance offset handoff | ✅ Implemented | `fetch_offsets()` via store queryable on `_offsets/{partition}/**` |
| Store/config automation | ✅ Implemented | Multi-topic agent (`mitiflow-storage`) + orchestrator topic provisioning |

### The Gap

When consumer C0 owns partition 3 and crashes, `PartitionManager` reassigns
partition 3 to consumer C1 via liveliness-driven rebalancing. But **C1 has no
idea where C0 left off.** C0's `SequenceCheckpoint` is on C0's local disk — C1
cannot access it.

C1 has two bad options:
1. **Replay from beginning** — correct but wasteful, duplicates processing.
2. **Start from latest** — loses events produced while C0 was alive but
   uncommitted.

Kafka solves this with `__consumer_offsets` — a shared, durable offset store
that any group member can read and write. We need an equivalent.

### What "Commit" Means in mitiflow

mitiflow's sequence model is per-(partition, publisher). A consumer tracking
partition 0 must track its position per publisher:

```
Consumer C0, partition 0:
  publisher P1 → last processed seq: 142
  publisher P2 → last processed seq: 87
  publisher P3 → last processed seq: 301
```

A "commit" persists this map so that on reassignment, the new owner can resume
each publisher's stream from the committed position.

---

## Part 2: Offset Cursor Model

**Decision: Per-(Publisher, Partition) Sequence Cursor (Model A).**

```rust
/// Committed position for a consumer group member on a partition.
struct PartitionCursor {
    /// Per-publisher last committed sequence on this partition.
    publishers: HashMap<PublisherId, u64>,
}

/// Full commit for a consumer group.
/// Key: (group_id, partition) → PartitionCursor
```

**Resume behavior:** On gaining partition P, consumer loads `PartitionCursor`
for P. For each known publisher, it seeds `GapDetector` with the committed
seq. Events with seq ≤ committed are treated as duplicates.

**Why this model:**
- Exact resume — no reprocessing of committed events, no missed events.
- Matches the existing `SequenceCheckpoint` data model (same data shape).
- Works with `GapDetector::with_checkpoints()` directly.
- Provides exactly-once semantics without requiring idempotent consumers.
- Cursor size is bounded by active publishers (dead publishers are cleaned up
  via the publisher lifecycle — see [04_sequencing_and_replay.md](04_sequencing_and_replay.md)
  § Publisher Lifecycle).

**Trade-offs accepted:**
- Cursor grows with the number of publishers that have ever written to the
  partition. Dead publishers accumulate unless garbage-collected.
- On commit, must serialize the full publisher map.
- Not compatible with HLC replay queries (which use a timestamp cursor).
  An HLC cursor can be added later as an optimization for the Kafka gateway.

---

## Part 3: Offset Storage — Hybrid Sidecar (Approach D)

**Decision: Offset service co-located with EventStore as a separate module.**

Extend the EventStore with a lightweight offset service that uses its own
keyspace and API surface, but runs within the same process. This keeps
deployment simple (no new services) while maintaining clean module boundaries. 
The wire protocol is designed so the offset service could later be extracted
into a standalone process without changing how consumers interact with it.

```
EventStore process (per partition):
  ├── Event ingestion (subscribe → persist)
  ├── Event queryable (replay queries)
  ├── Watermark publisher
  └── Offset service (NEW)
       ├── Subscribe to _offsets/{partition}/**
       ├── Queryable for _offsets/{partition}/**
       └── fjall keyspace: offsets
```

**Why this approach:**
- **No new processes.** Deploys with the existing EventStore.
- **Offsets are replicated for free** — if the EventStore is replicated (see
  [05_replication.md](05_replication.md)), offset commits are received by all
  replicas via pub/sub fan-out.
- **Wire protocol is extraction-ready.** Consumers don't know whether they're
  talking to a co-located offset module or a standalone orchestrator. If we
  later build an orchestrator service, the consumer code doesn't change.
- **Same failure domain.** If the EventStore for partition P is down, consumers
  can't recover events either — so offset unavailability is not an additional
  failure mode.
- **Transactional potential.** Offsets and events live in the same fjall
  database. Future: atomic "consume + commit" for exactly-once.

**Trade-offs accepted:**
- Cross-partition offset queries require querying every partition's store
  (acceptable for rebalancing, awkward for admin tools — addressed by the
  orchestrator in Phase 2).
- Offset reads on the rebalance path add ~1-10ms latency (store roundtrip).

---

## Part 4: Phased Implementation Plan

### Phase 1: Store-Managed Offsets (this document)

Offset storage co-located in the EventStore. Delivers consumer group commits
with zero new services. ~400 lines of code leveraging existing patterns from
`SequenceCheckpoint` and `FjallBackend`.

### Phase 2: Orchestrator for Configuration & Lifecycle

A lightweight orchestrator for control-plane concerns that the store can't
address:
- Cross-partition group lag monitoring
- Topic/partition configuration management
- EventStore provisioning and health checks
- Consumer group session management with generation fencing

The orchestrator reads offset state from the stores (via Zenoh queries) — it
doesn't replace offset storage, it augments it.

---

## Part 5: Detailed Design — Store-Managed Offsets (Phase 1)

### 5.1 Offset Commit Wire Protocol

Consumers publish offset commits via Zenoh `put()`:

```
Key expression:  {key_prefix}/_offsets/{partition}/{group_id}
Payload:         OffsetCommit (serialized)
```

```rust
/// An offset commit from a consumer group member.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetCommit {
    /// Consumer group identifier.
    pub group_id: String,
    /// Member that is committing (for audit/debugging).
    pub member_id: String,
    /// Partition this commit applies to.
    pub partition: u32,
    /// Per-publisher committed sequence numbers.
    pub offsets: HashMap<PublisherId, u64>,
    /// Generation ID — for fencing stale commits.
    pub generation: u64,
    /// Commit timestamp.
    pub timestamp: DateTime<Utc>,
}
```

### 5.2 Offset Fetch Protocol

Consumers fetch offsets via Zenoh `session.get()`:

```
Selector:  {key_prefix}/_offsets/{partition}/{group_id}
Response:  OffsetCommit (latest committed)
```

The store's queryable for `_offsets/**` looks up the latest commit from its
`offsets` keyspace and replies.

### 5.3 EventStore Offset Keyspace

```rust
/// Offset storage in the EventStore's fjall backend.
///
/// Keyspace: "offsets"
/// Key:   [group_id_hash: 8][publisher_id: 16]
/// Value: [seq: 8 BE][generation: 8 BE][timestamp_ms: 8 BE]
///
/// group_id_hash is xxhash64 of the group name — fixed width for efficient
/// prefix scans. The full group_id is stored in a separate "offset_groups"
/// keyspace for reverse lookup.
```

Operations:

```rust
impl FjallBackend {
    /// Persist an offset commit. Only accepts if generation >= stored generation.
    pub fn commit_offsets(&self, commit: &OffsetCommit) -> Result<()> {
        for (pub_id, seq) in &commit.offsets {
            let key = offset_key(&commit.group_id, pub_id);
            
            // Fencing: reject commits from older generations
            if let Some(existing) = self.get_offset(&commit.group_id, pub_id)? {
                if commit.generation < existing.generation {
                    return Err(Error::StaleFencedCommit {
                        group: commit.group_id.clone(),
                        commit_gen: commit.generation,
                        stored_gen: existing.generation,
                    });
                }
            }
            
            let value = encode_offset_value(*seq, commit.generation, commit.timestamp);
            self.offsets.insert(key, value)?;
        }
        Ok(())
    }

    /// Fetch all committed offsets for a group on this partition.
    pub fn fetch_offsets(&self, group_id: &str) -> Result<HashMap<PublisherId, u64>> {
        let prefix = group_hash(group_id).to_be_bytes();
        let mut result = HashMap::new();
        for entry in self.offsets.prefix(prefix) {
            let kv = entry.into_inner()?;
            let pub_id = decode_publisher_id(&kv.0[8..24])?;
            let seq = u64::from_be_bytes(kv.1[..8].try_into()?);
            result.insert(pub_id, seq);
        }
        Ok(result)
    }
}
```

### 5.4 Consumer-Side Integration

```rust
impl EventSubscriber {
    /// Commit current offsets to the EventStore.
    ///
    /// Publishes the current per-publisher sequence positions for all
    /// assigned partitions. Call after processing a batch of events.
    pub async fn commit(&self) -> Result<()> {
        for partition in &self.assigned_partitions {
            let offsets = self.gap_detector.current_positions(*partition);
            let commit = OffsetCommit {
                group_id: self.group_id.clone(),
                member_id: self.member_id.clone(),
                partition: *partition,
                offsets,
                generation: self.generation,
                timestamp: Utc::now(),
            };
            let key = format!("{}/_offsets/{}/{}", 
                self.config.key_prefix, partition, self.group_id);
            self.session.put(&key, serde_json::to_vec(&commit)?).await?;
        }
        Ok(())
    }
    
    /// Fetch committed offsets from the store and seed the gap detector.
    /// Called internally when partition is gained during rebalance.
    async fn load_offsets(&mut self, partition: u32) -> Result<()> {
        let selector = format!("{}/_offsets/{}/{}", 
            self.config.key_prefix, partition, self.group_id);
        let replies = self.session.get(&selector).await?;
        while let Ok(reply) = replies.recv_async().await {
            if let Ok(sample) = reply.result() {
                let commit: OffsetCommit = serde_json::from_slice(
                    &sample.payload().to_bytes()
                )?;
                for (pub_id, seq) in commit.offsets {
                    self.gap_detector.seed_checkpoint(pub_id, partition, seq);
                }
            }
        }
        Ok(())
    }
}
```

### 5.5 Rebalance Integration

The `on_rebalance` callback becomes the coordination point:

```rust
// Inside PartitionManager rebalance callback:
async fn on_rebalance(gained: &[u32], lost: &[u32], subscriber: &mut EventSubscriber) {
    // 1. Commit offsets for partitions being released
    for partition in lost {
        subscriber.commit_partition(*partition).await
            .unwrap_or_else(|e| warn!("failed to commit on rebalance: {e}"));
    }
    
    // 2. Unsubscribe from lost partitions
    subscriber.release_partitions(lost).await;
    
    // 3. Fetch offsets for newly gained partitions
    for partition in gained {
        subscriber.load_offsets(*partition).await
            .unwrap_or_else(|e| warn!("failed to load offsets: {e}"));
    }
    
    // 4. Subscribe to gained partitions
    subscriber.acquire_partitions(gained).await;
}
```

### 5.6 Generation Fencing

#### The Zombie Consumer Problem

Generations exist to prevent **zombie consumers** — consumers that the group
considers dead but are still alive (e.g., due to a long GC pause or network
partition) — from corrupting offset state.

Consider this timeline:

```
Time ─────────────────────────────────────────────────────────────>

C0 owns partition 3, processing events, committing offsets
  │
  │   t1: C0 enters long GC pause (or network partition)
  │       PartitionManager detects C0 missing via liveliness timeout
  │       Rebalance: partition 3 reassigned to C1
  │
  │   t2: C1 gains partition 3
  │       C1 loads offsets from store: {P1: 142}
  │       C1 starts processing from seq 143
  │       C1 commits: {P1: 200}
  │
  │   t3: C0 recovers from GC pause. C0 still thinks it owns partition 3.
  │       C0 commits its stale offsets: {P1: 155}
  │       ← THIS OVERWRITES C1's PROGRESS (200 → 155)
  │
  │   t4: C1 crashes and restarts.
  │       Loads offsets from store: {P1: 155}     ← C0's stale commit!
  │       Reprocesses events 156–200 that C1 already handled.
  │       ← DUPLICATE PROCESSING
```

Without fencing, the zombie C0's commit at t3 rolls back the offset from 200
to 155. When C1 (or any consumer) next loads offsets, it reprocesses 45 events.

#### How Generation Fencing Prevents This

A **generation** is a monotonically increasing counter that advances on every
rebalance. Each offset commit carries the generation in which it was made. The
store **rejects commits from older generations**.

```
Time ─────────────────────────────────────────────────────────────>

  t0: Initial assignment. Generation = 1.
      C0 owns partition 3.

  t1: Rebalance triggered (C0 timed out). Generation increments to 2.
      C1 gains partition 3 in generation 2.
      C1 loads offsets, starts processing, commits with generation=2.

  t3: Zombie C0 tries to commit with generation=1.
      Store sees: stored generation (2) > commit generation (1).
      Store REJECTS the commit → StaleFencedCommit error.
      C0's stale offset is never written. C1's progress is safe.
```

The fencing rule is simple: **the store only accepts commits where
`commit.generation >= stored_generation`** for that group + partition.

#### Where Does the Generation Come From?

Each `PartitionManager` tracks its own generation counter, incrementing on
every rebalance event it observes:

```rust
struct PartitionManager {
    // ... existing fields ...
    generation: AtomicU64,  // increments on every rebalance
}

impl PartitionManager {
    fn on_membership_change(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
    }
    
    pub fn current_generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }
}
```

This is a **local** counter — two consumers may have different generation
values. But that's fine for fencing because:

1. **Partition ownership is exclusive.** At any point, only one consumer owns
   a given partition. Rebalance produces a clean handoff.

2. **The rebalance that transfers ownership always increments the generation.**
   When C0 loses partition 3 and C1 gains it, C1's `PartitionManager` saw the
   same rebalance event and has a generation ≥ C0's.

3. **The store doesn't compare generations across consumers.** It only checks:
   "is this commit's generation ≥ what's already stored?" Since C1 commits with
   a higher generation than C0, C0's later zombie commit has a lower generation
   and gets rejected.

**Edge case — simultaneous rebalance:** If C0 and C1 both see the rebalance
and both increment to generation=2, C0 could still commit with generation=2.
This is safe because by the time C0 tries to commit, C1 has already committed
with generation=2 (updating the stored generation). C0's commit with the same
generation=2 is accepted (>= check), but this only happens during the brief
rebalance window and the offset values would be close to each other anyway.

For stronger isolation, Phase 2 (orchestrator) can assign a globally unique
generation per group via centralized coordination — similar to Kafka's
`group.generation.id` from the `JoinGroup` response.

### 5.7 Auto-Commit

For consumers that want Kafka-like auto-commit:

```rust
pub struct AutoCommitConfig {
    /// Commit interval (default: 5s, matching Kafka's default).
    pub interval: Duration,
    /// Whether auto-commit is enabled.
    pub enabled: bool,
}
```

A background task periodically calls `subscriber.commit()`:

```rust
async fn auto_commit_task(
    subscriber: Arc<EventSubscriber>,
    interval: Duration,
    cancel: CancellationToken,
) {
    let mut ticker = tokio::time::interval(interval);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                // Final commit on shutdown
                let _ = subscriber.commit().await;
                break;
            }
            _ = ticker.tick() => {
                if let Err(e) = subscriber.commit().await {
                    warn!("auto-commit failed: {e}");
                }
            }
        }
    }
}
```

### 5.8 Commit Durability & Confirmation

**Decision:** Synchronous commit via queryable ACK for `commit_sync()`;
fire-and-forget put for `commit_async()`.

- `commit_sync()`: Consumer sends the commit as a `session.get()` (query).
  The store receives the query, persists the offset, and replies with
  confirmation. One RTT per commit — matches Kafka's `commitSync()`.
- `commit_async()`: Consumer sends via `session.put()`. Returns immediately.
  If the store is temporarily unreachable, the commit is lost. Acceptable for
  auto-commit since occasional lost commits just cause a few events to be
  reprocessed on failover.

```rust
/// Synchronous commit: sends offset as a query, waits for store ACK.
pub async fn commit_sync(&self) -> Result<()> { /* session.get() */ }

/// Async commit: fire-and-forget put, no durability guarantee.
pub async fn commit_async(&self) -> Result<()> { /* session.put() */ }
```

---

## Part 6: Detailed Design — Orchestrator (Phase 2)

The orchestrator is a **control-plane service** — it does not sit in the
event data path. Events flow directly from publishers to subscribers via Zenoh.
The orchestrator manages metadata and automation.

```
Data plane (hot path):
  Publisher → Zenoh → EventStore (subscribe + persist)
                  └→ Consumer (subscribe + process)

Control plane (orchestrator):
  - Group lag monitoring  (reads watermarks + offsets from stores)
  - Config management     (topic/partition CRUD)
  - Store lifecycle       (provision, health-check, scale stores)
  - Admin API             (HTTP/gRPC for dashboards, CLI)
```

### 6.1 Architecture

```
┌──────────────────────────────────────────────┐
│              mitiflow-orchestrator            │
│                                              │
│  ┌─────────────┐   ┌──────────────────────┐  │
│  │ Config Store │   │  Group Metadata      │  │
│  │  (fjall)     │   │  - group sessions    │  │
│  │  - topics    │   │  - generation IDs    │  │
│  │  - partitions│   │  - member registry   │  │
│  │  - retention │   │  - lag cache         │  │
│  └─────────────┘   └──────────────────────┘  │
│                                              │
│  ┌─────────────┐   ┌──────────────────────┐  │
│  │ Store Mgr   │   │  Admin API           │  │
│  │  - provision │   │  - HTTP REST         │  │
│  │  - health    │   │  - Zenoh queryable   │  │
│  │  - scale     │   │  (for CLI tools)     │  │
│  └─────────────┘   └──────────────────────┘  │
│                                              │
│  Zenoh subscriber:                           │
│    _watermark/*   → aggregate lag            │
│    _offsets/**    → cache group state         │
│    _store/*       → track store liveliness   │
│    _workers/*     → track consumer members    │
└──────────────────────────────────────────────┘
```

### 6.2 Configuration Management

The orchestrator stores topic and partition configuration persistently, and
distributes it to EventStore instances and consumers via Zenoh.

```rust
/// Topic configuration managed by the orchestrator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConfig {
    pub name: String,
    pub num_partitions: u32,
    pub replication_factor: u32,
    pub retention: RetentionPolicy,
    pub compaction: CompactionPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicy {
    /// Max age of events before GC.
    pub max_age: Option<Duration>,
    /// Max total size per partition.
    pub max_bytes: Option<u64>,
    /// Max number of events per partition.
    pub max_events: Option<u64>,
}
```

**Configuration distribution:**

```
Orchestrator:
  1. Stores TopicConfig in fjall
  2. Publishes to _config/{topic_name} via Zenoh put()
  3. EventStore instances subscribe to _config/** and reconfigure

EventStore (on config update):
  - Adjusts GC params (retention)
  - Triggers compaction if policy changes
  - Reports current config via queryable
```

### 6.3 Store Lifecycle Automation

The orchestrator automates EventStore deployment and management:

```
1. Topic creation:
   - User calls: orchestrator.create_topic("orders", partitions=8, rf=3)
   - Orchestrator writes config
   - Orchestrator provisions 8 × 3 = 24 EventStore instances
     (or signals existing store hosts to create new partitions)

2. Health monitoring:
   - Orchestrator subscribes to _store/* liveliness
   - If a store goes offline, log alert
   - Optionally: trigger replacement provisioning

3. Scaling:
   - User calls: orchestrator.add_partitions("orders", 4)  // 8 → 12
   - Orchestrator updates config, provisions 4 new stores
   - Publishes new partition map to _config/orders
   - PartitionManager instances pick up the change and rebalance
```

### 6.4 Consumer Group Session Management

For advanced consumer group semantics (Kafka-like JoinGroup/SyncGroup), the
orchestrator can act as the group coordinator:

```
Consumer join flow:
  1. Consumer sends JoinGroup request to orchestrator (via Zenoh queryable)
  2. Orchestrator collects JoinGroup from all members (with timeout)
  3. Orchestrator computes partition assignment (range or round-robin)
  4. Orchestrator sends SyncGroup response with assignments
  5. Consumers start consuming from assigned partitions
  6. Consumers send periodic heartbeats to orchestrator
  7. Orchestrator detects dead consumers via heartbeat timeout → triggers rebalance
```

Keep the existing liveliness-based `PartitionManager` as the **default**
consumer group mechanism — it works without the orchestrator. The
orchestrator-managed sessions are an **optional upgrade** for users who need
Kafka-compatible group semantics or centralized monitoring.

### 6.5 Lag Monitoring

The orchestrator computes consumer lag by comparing watermarks with committed
offsets:

```
For each (group, partition, publisher):
  lag = watermark.committed_seq - committed_offset.seq

Published as:
  _lag/{group_id}/{partition} → { publishers: { P1: 42, P2: 0, ... }, total: 42 }
```

This enables dashboards and alerting without consumers computing their own lag.

---

## Part 7: Consumer Group API

### 7.1 Configuration

```rust
pub struct ConsumerGroupConfig {
    /// Consumer group identifier.
    pub group_id: String,
    /// This member's unique ID (default: random UUID).
    pub member_id: Option<String>,
    /// Offset commit mode.
    pub commit_mode: CommitMode,
    /// What to do when no committed offset exists.
    pub offset_reset: OffsetReset,
}

pub enum CommitMode {
    /// Application calls commit() explicitly after processing.
    Manual,
    /// Offsets are committed automatically at interval.
    Auto { interval: Duration },
}

pub enum OffsetReset {
    /// Start from the earliest available event (replay everything).
    Earliest,
    /// Start from the latest event (skip history).
    Latest,
    /// Fail if no committed offset exists.
    Error,
}
```

### 7.2 Public API

```rust
impl EventSubscriber {
    /// Create a consumer group subscriber.
    ///
    /// Joins the group identified by `group_config.group_id`, discovers other
    /// members via liveliness, computes partition assignment via HRW hashing,
    /// fetches committed offsets from the EventStore, and begins consuming.
    pub async fn new_consumer_group(
        session: &Session,
        config: EventBusConfig,
        group_config: ConsumerGroupConfig,
    ) -> Result<Self>;

    /// Synchronously commit current offsets. Blocks until the store confirms.
    pub async fn commit_sync(&self) -> Result<()>;

    /// Asynchronously commit current offsets. Returns immediately.
    pub async fn commit_async(&self) -> Result<()>;

    /// Commit offsets for a specific partition only.
    pub async fn commit_partition_sync(&self, partition: u32) -> Result<()>;

    /// Get the current uncommitted positions (for monitoring).
    pub fn positions(&self) -> HashMap<u32, HashMap<PublisherId, u64>>;

    /// Get the last committed offsets (for monitoring).
    pub async fn committed(&self) -> Result<HashMap<u32, HashMap<PublisherId, u64>>>;
}
```

### 7.3 Usage Example

```rust
use mitiflow::*;

let session = zenoh::open(zenoh::Config::default()).await?;
let config = EventBusConfig::builder("orders/events")
    .num_partitions(16)
    .build()?;

let group = ConsumerGroupConfig {
    group_id: "order-processor".into(),
    member_id: None,  // auto-generated
    commit_mode: CommitMode::Manual,
    offset_reset: OffsetReset::Earliest,
};

let subscriber = EventSubscriber::new_consumer_group(&session, config, group).await?;

// Process events
while let Ok(event) = subscriber.recv::<OrderEvent>().await {
    process_order(&event).await?;
    
    // Commit after processing each event (or batch)
    subscriber.commit_sync().await?;
}

// On shutdown: final commit + cleanup
subscriber.shutdown().await;
```

---

## Part 8: Orchestrator API & Deployment

### 8.1 Admin API (Zenoh Queryable + HTTP)

```rust
// Via Zenoh queryable (for CLI / programmatic access):
session.get("mitiflow/_admin/topics").await?;           // list topics
session.get("mitiflow/_admin/topics/orders").await?;    // describe topic
session.get("mitiflow/_admin/groups").await?;           // list consumer groups
session.get("mitiflow/_admin/groups/order-processor/lag").await?;  // get lag

// Via HTTP REST (for dashboards / external tools):
// GET /api/v1/topics
// GET /api/v1/topics/orders
// GET /api/v1/groups
// GET /api/v1/groups/order-processor/lag
// POST /api/v1/topics  { name: "orders", partitions: 16, rf: 3 }
```

### 8.2 Deployment Topology

```
Minimal (single-node dev):
  1 process: mitiflow-orchestrator
    - Embeds EventStore instances for all partitions
    - Runs offset service, config store, admin API
    - Single binary, zero dependencies beyond Zenoh

Production (distributed):
  N nodes: mitiflow-store (1 per partition × replication_factor)
    - Each runs EventStore + offset sidecar for assigned partitions
    - Registers via liveliness, autonomous operation
  
  1-3 nodes: mitiflow-orchestrator (replicated for HA)
    - Config management, lag monitoring, store lifecycle
    - Reads offset state from stores (doesn't store offsets itself)
    - Orchestrator replicas coordinate via Zenoh pub/sub
```

### 8.3 Orchestrator HA

The orchestrator itself can be replicated using the same patterns as
EventStore replication (see [05_replication.md](05_replication.md)):
- Multiple instances subscribe to the same control-plane key expressions.
- Each independently maintains its config store.
- Leader election (if needed for write operations) uses Zenoh liveliness +
  deterministic ordering (lowest UUID wins).
- Read operations (lag queries, config reads) can be served by any replica.

---

## Part 9: Summary & Implementation Priority

| Phase | Scope | Effort | Value |
|-------|-------|--------|-------|
| **Phase 1** | Store-managed offsets + consumer commit API | ~400 LOC | Enables consumer groups with at-least-once semantics |
| **Phase 2** | Orchestrator — config + monitoring | ~800 LOC | Central visibility, topic CRUD, lag monitoring |
| **Phase 3** | Orchestrator — store lifecycle automation | ~500 LOC | Automated EventStore provisioning |
| **Phase 4** | Orchestrator — full group session management | ~600 LOC | Kafka-compatible JoinGroup/SyncGroup protocol |

### Changes Required for Phase 1

| File | Change |
|------|--------|
| `store/backend.rs` | Add `offsets` keyspace, `commit_offsets()`, `fetch_offsets()` |
| `store/runner.rs` | Subscribe to `_offsets/**`, declare offset queryable |
| `subscriber/mod.rs` | Add `commit_sync()`, `commit_async()`, `load_offsets()`, `new_consumer_group()` |
| `config.rs` | Add `ConsumerGroupConfig`, `CommitMode`, `OffsetReset` |
| `error.rs` | Add `StaleFencedCommit` variant |
| `lib.rs` | Re-export new types |
| `examples/consumer_groups.rs` | Update to demonstrate commit API |
| `tests/` | Add `consumer_group_commit.rs` integration test |

### New Crate for Phase 2+

```
mitiflow-orchestrator/
├── Cargo.toml
└── src/
    ├── main.rs         # Binary entry point
    ├── config.rs       # Orchestrator config + topic config management
    ├── lag.rs          # Lag monitoring (reads watermarks + offsets)
    ├── admin.rs        # HTTP + Zenoh queryable admin API
    └── lifecycle.rs    # Store provisioning and health checks
```

---

## Part 10: E2E Test Plan

Systematic test scenarios for consumer group commits, rebalance, and fencing.
All tests use `#[tokio::test(flavor = "multi_thread", worker_threads = 2)]`
with unique key prefixes, short timeouts, and `temp_dir()` for storage.

### Category 1: Basic Offset Commit & Fetch

| Test | What it verifies |
|------|-----------------|
| `commit_and_fetch_round_trip` | Single consumer commits, new consumer fetches and resumes correctly |
| `commit_multiple_publishers` | Offsets for multiple publishers on same partition round-trip correctly |
| `commit_across_partitions` | Consumer owning multiple partitions commits and recovers all |
| `commit_sync_vs_async` | `commit_sync()` provides immediate confirmation; `commit_async()` propagates after delay |
| `offset_reset_earliest_vs_latest` | `OffsetReset::Earliest` replays from beginning; `Latest` skips history |

### Category 2: Rebalance & Offset Handoff

| Test | What it verifies |
|------|-----------------|
| `rebalance_transfers_offsets` | Partition moves from C0→C1; C1 resumes from C0's last commit (no dupes) |
| `rebalance_commit_before_release` | Leaving consumer commits final offsets for lost partitions before release |
| `rapid_rebalance_three_consumers` | Third consumer joins mid-rebalance; cascading rebalance settles correctly |
| `consumer_leaves_gracefully` | Clean shutdown commits final offsets; remaining member absorbs partitions |
| `all_consumers_restart` | All crash simultaneously; new instances recover from stored offsets |

### Category 3: Generation Fencing

| Test | What it verifies |
|------|-----------------|
| `zombie_commit_rejected` | Stale consumer's commit (old generation) is rejected by store |
| `generation_increments_on_every_rebalance` | Generation counter increments on each membership change |
| `same_generation_commit_accepted` | Equal-generation commits pass the `>=` fencing check |
| `zombie_detects_fencing_and_stops` | Fenced consumer receives `StaleFencedCommit` and stops processing |

**Test implementation:** `mitiflow/tests/consumer_group_commit.rs` — 8 tests
covering commit round-trip, multi-publisher, generation fencing, auto-commit,
per-publisher independence, independent groups, sync vs async, and store crash
recovery.
