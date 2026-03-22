# Distributed Storage Management

Two-tier architecture for managing EventStore instances across a cluster of
storage nodes. Tier 1 (StorageAgent) handles the heavy lifting — partition
assignment, failure recovery, peer coordination — entirely via decentralized
Zenoh primitives. Tier 2 (Orchestrator extensions) provides an optional control
plane for strategic operations, observability dashboards, and manual overrides.

**Design principle:** The system must be fully functional without any central
coordinator. The orchestrator only makes things easier to operate — it never
makes things possible. Every feature the orchestrator provides is built on top
of what the decentralized tier already does.

**Related docs:**
[02_architecture.md](02_architecture.md),
[03_durability.md](03_durability.md),
[05_replication.md](05_replication.md),
[10_graceful_termination.md](10_graceful_termination.md),
[11_consumer_group_commits.md](11_consumer_group_commits.md)

### Implementation Status

**Tier 1 (StorageAgent): Done.** The `mitiflow-agent` crate implements the
full decentralized storage management layer — weighted HRW assignment,
liveliness-based membership, reconciler with `(partition, replica)` tracking,
recovery from peer stores, rack-aware placement, override subscription, and
health/status reporting. 47 tests pass (7 agent, 9 reconciler, 5 recovery,
6 membership, 13 E2E scenarios, 3 smoke, 4 reporters).

**Tier 2 (Orchestrator extensions): Not started.** ClusterView, OverrideManager,
drain operations, and admin API extensions are designed but not implemented.

### Key Files

| Area | Path |
|------|------|
| Agent daemon | `mitiflow-agent/src/agent.rs` |
| Agent binary entry | `mitiflow-agent/src/main.rs` |
| Agent config | `mitiflow-agent/src/config.rs` |
| Membership tracker | `mitiflow-agent/src/membership.rs` |
| Reconciler | `mitiflow-agent/src/reconciler.rs` |
| Recovery manager | `mitiflow-agent/src/recovery.rs` |
| Health reporter | `mitiflow-agent/src/health.rs` |
| Status reporter | `mitiflow-agent/src/status.rs` |
| Types (NodeHealth, OverrideTable, etc.) | `mitiflow-agent/src/types.rs` |
| Weighted HRW + rack-aware | `mitiflow/src/partition/hash_ring.rs` |
| StorageBackend Arc blanket impl | `mitiflow/src/store/backend.rs` |
| E2E test scenarios | `mitiflow-agent/tests/e2e/scenarios.rs` |
| E2E test helpers | `mitiflow-agent/tests/e2e/helpers.rs` |

---

## 1. System Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Tier 2: Orchestrator (optional)                   │
│  ┌─────────────┐ ┌──────────┐ ┌────────────┐ ┌───────────────────┐ │
│  │ ClusterView  │ │ AdminAPI │ │ Override   │ │   Dashboard       │ │
│  │ (aggregated) │ │ (REST +  │ │ Manager    │ │   (lag, health,   │ │
│  │              │ │  Zenoh)  │ │            │ │    assignments)   │ │
│  └──────┬───────┘ └─────┬────┘ └─────┬──────┘ └────────┬──────────┘ │
│         │ observe       │ query      │ publish          │ read       │
└─────────┼───────────────┼────────────┼──────────────────┼───────────┘
          │               │            │                  │
══════════╪═══════════════╪════════════╪══════════════════╪═══════════
          │ Zenoh pub/sub │ queryable  │ _cluster/        │ _cluster/
          │               │            │  overrides       │  status
══════════╪═══════════════╪════════════╪══════════════════╪═══════════
          │               │            │                  │
┌─────────┼───────────────┼────────────┼──────────────────┼───────────┐
│         ▼               ▼            ▼                  ▲           │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │                 Tier 1: StorageAgent (per node)              │    │
│  │  ┌──────────────┐ ┌────────────┐ ┌───────────────────────┐  │    │
│  │  │ Membership   │ │ Assignment │ │ RecoveryManager       │  │    │
│  │  │ (liveliness) │ │ (HRW hash) │ │ (peer query on gain)  │  │    │
│  │  └──────┬───────┘ └──────┬─────┘ └───────────┬───────────┘  │    │
│  │         │                │                   │              │    │
│  │         ▼                ▼                   ▼              │    │
│  │  ┌──────────────────────────────────────────────────────┐   │    │
│  │  │              Reconciler                              │   │    │
│  │  │  desired_state(hash + overrides) vs actual_state     │   │    │
│  │  │  → start/stop EventStore instances                   │   │    │
│  │  └──────────────────────────────────────────────────────┘   │    │
│  │         │              │              │                     │    │
│  │    EventStore p0  EventStore p3  EventStore p7              │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                        StorageNode                                  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 2. Tier 1: StorageAgent (Decentralized)

The `StorageAgent` is a per-node daemon that manages `EventStore` instances
locally. It discovers peers, computes its partition assignment, and
reconciles local state — all without any central coordinator.

### 2.1 Core Components

| Component | Responsibility |
|-----------|---------------|
| `StorageAgent` | Top-level daemon. Owns the Zenoh session, spawns all sub-components |
| `MembershipTracker` | Watches peer liveliness tokens, maintains live node set |
| `AssignmentComputer` | Deterministic HRW assignment from `(topic, partition, replica)` → node |
| `Reconciler` | Diffs desired vs actual local stores; starts/stops `EventStore` instances |
| `RecoveryManager` | On partition gain, queries peers for missing events |
| `HealthReporter` | Publishes local node health metrics |
| `StatusReporter` | Publishes per-node partition assignment status |

### 2.2 Membership & Discovery

Each storage node declares a Zenoh liveliness token on startup. All nodes
watch the same liveliness prefix to maintain a consistent view of the cluster.

**Liveliness key:**
```
{key_prefix}/_agents/{node_id}
```

**Node metadata** published as a retained Zenoh sample:
```
{key_prefix}/_agents/{node_id}/meta
```

```rust
/// Published once on startup, updated on capability changes.
pub struct NodeMetadata {
    pub node_id: NodeId,
    pub labels: HashMap<String, String>,   // e.g., {"rack": "us-east-1a", "tier": "ssd"}
    pub capacity: u32,                     // relative weight for HRW (default: 100)
    pub data_dir: PathBuf,
    pub started_at: DateTime<Utc>,
}
```

**Discovery protocol:**
1. Agent starts → declares liveliness at `{key_prefix}/_agents/{node_id}`
2. Agent publishes `NodeMetadata` to `{key_prefix}/_agents/{node_id}/meta`
3. Agent queries `{key_prefix}/_agents/*` (liveliness get, history=true)
4. Agent subscribes to liveliness changes on `{key_prefix}/_agents/*`
5. On membership change → recompute assignments

**Why `_agents/` instead of reusing `_workers/` or `_store/`?**
- `_workers/` is for consumer group members (subscribers), not storage nodes
- `_store/{partition}` is per-partition liveliness declared by `EventStore`
  instances
- `_agents/{node_id}` is per-node liveliness for the storage management plane
- Keeping them separate avoids conflating consumer membership with storage
  membership

### 2.3 Assignment Computation

Reuses the existing rendezvous hashing from `partition/hash_ring.rs`. Each
node independently computes the full assignment table and determines which
partitions it owns. Since the algorithm is deterministic and all nodes see
the same membership set (via liveliness), they converge to the same
assignment.

**Extended HRW for replicas and weighted nodes:**

```rust
/// Weighted HRW score. Higher weight → higher probability of assignment.
fn weighted_hrw_score(node_id: &str, partition: u32, replica: u32, weight: u32) -> f64 {
    let mut hasher = DefaultHasher::new();
    (node_id, partition, replica).hash(&mut hasher);
    let hash_val = hasher.finish();

    // Normalize to (0, 1] — avoid ln(0)
    let normalized = (hash_val as f64) / (u64::MAX as f64);
    let normalized = normalized.max(f64::MIN_POSITIVE);

    // Weighted score: higher weight → higher score in expectation
    -(weight as f64) / normalized.ln()
}
```

**Assignment resolution with overrides:**
```rust
fn effective_assignment(
    partition: u32,
    replica: u32,
    nodes: &[NodeDescriptor],
    overrides: &OverrideTable,
) -> NodeId {
    if let Some(node) = overrides.get(partition, replica) {
        return node;  // Orchestrator override takes precedence
    }
    assign_replicas(partition, replication_factor, nodes)[replica]
}
```

**Properties of rendezvous hashing:**
- **Minimal disruption:** When a node joins/leaves, only ~1/N partitions move
- **Deterministic:** All nodes compute the same result from the same inputs
- **No coordination:** No consensus protocol needed
- **Weighted:** Heterogeneous nodes get proportional partition counts

### 2.4 Reconciler

The reconciler is the core state machine that keeps local `EventStore`
instances in sync with the computed assignment. It runs on every membership
change and on override updates.

```rust
pub struct Reconciler {
    node_id: NodeId,
    active_stores: HashMap<(u32, u32), ManagedStore>,  // (partition, replica) → store
    base_path: PathBuf,
    session: Session,
}

/// A managed EventStore instance with its lifecycle state.
struct ManagedStore {
    store: EventStore,
    state: StoreState,
    started_at: DateTime<Utc>,
}

pub enum StoreState {
    Starting,     // Opening backend, preparing recovery
    Recovering,   // Querying peers for missing events
    Active,       // Fully operational
    Draining,     // Assigned away, finishing in-flight work
    Stopped,      // Shut down, backend closed
}
```

**Reconciliation loop:**
```
on membership_change OR override_update:
    desired = effective_assignment(all partitions, all replicas, live_nodes, overrides)
    my_desired = desired.filter(|a| a.node_id == self.node_id)
    my_actual = self.active_stores.keys()

    gained = my_desired - my_actual
    lost = my_actual - my_desired

    for (partition, replica) in gained:
        1. Create FjallBackend at {base_path}/{partition}/{replica}
        2. Create EventStore with backend + config
        3. Set state = Starting
        4. Call store.run()  → spawns subscribe/queryable/watermark tasks
        5. Declare liveliness at {key_prefix}/_store/{partition}
        6. Set state = Recovering
        7. Spawn RecoveryManager to fill gaps from peers
        8. On recovery complete → set state = Active

    for (partition, replica) in lost:
        1. Set state = Draining
        2. Wait drain_grace_period (allow in-flight events to land)
        3. Call store.shutdown_gracefully()
        4. Set state = Stopped
        5. Remove from active_stores
```

**Drain grace period:** Configurable (default 30s). During drain, the store
continues receiving and storing events but publishers are expected to be
switching to the new owner. This prevents data loss during transitions.

### 2.5 Recovery Manager

When a node gains a partition (because another node left or rebalanced),
the new `EventStore` starts receiving live events immediately but may be
missing historical data. The `RecoveryManager` fills this gap.

**Recovery protocol:**
```
RecoveryManager::recover(partition, replica):
    1. Start EventStore → begins subscribing to live events immediately
    2. Identify peers that own other replicas of this partition
       → query liveliness at {key_prefix}/_store/{partition}
    3. Fetch local watermark state (if any prior data exists)
    4. For each publisher with gaps or low watermark:
       a. Query peer store: session.get(
            "{peer_store_prefix}/{partition}?after_seq={local_committed}&pub_id={pub}"
          )
       b. Store recovered events in local backend (idempotent)
    5. If no peers available, query publisher caches:
       → session.get("{key_prefix}/_cache/{publisher_id}?after_seq={local_committed}")
    6. Mark recovery complete when local watermark ≥ peer watermark
```

**Recovery is idempotent:** The `FjallBackend.store()` operation is a
key-value put keyed by `(publisher_id, seq)`. Storing the same event twice
is a no-op overwrite. This makes recovery safe to retry and safe to run
concurrently with live ingestion.

**Recovery priority:** Peer replicas (full data) → publisher cache
(recent data) → accept gap (data not recoverable). The subscriber's existing
gap detection + recovery machinery handles the consumer-side equivalent.

### 2.6 Health Reporting

Each node periodically publishes health metrics so peers and the
orchestrator (if present) can observe cluster state.

**Health key:**
```
{key_prefix}/_cluster/health/{node_id}
```

```rust
pub struct NodeHealth {
    pub node_id: NodeId,
    pub partitions_active: u32,
    pub partitions_recovering: u32,
    pub partitions_draining: u32,
    pub disk_used_bytes: u64,
    pub disk_total_bytes: u64,
    pub events_stored: u64,
    pub uptime_secs: u64,
    pub timestamp: DateTime<Utc>,
}
```

Published every `health_interval` (default 10s). Uses Zenoh `put()` with
`CongestionControl::Drop` — health is best-effort, never blocks the data
path.

### 2.7 Status Reporting

Each node publishes its current partition assignment status so any observer
(peer agent, orchestrator, CLI tool) can build a cluster-wide view by
subscribing to all status reports.

**Status key:**
```
{key_prefix}/_cluster/status/{node_id}
```

```rust
pub struct NodeStatus {
    pub node_id: NodeId,
    pub assignments: Vec<PartitionStatus>,
    pub epoch: u64,          // membership epoch that produced this assignment
    pub timestamp: DateTime<Utc>,
}

pub struct PartitionStatus {
    pub partition: u32,
    pub replica: u32,
    pub state: StoreState,   // Starting | Recovering | Active | Draining | Stopped
    pub role: ReplicaRole,   // Primary | Follower
    pub watermark_seq: HashMap<PublisherId, u64>,  // local durability progress
}
```

Published on every reconciliation (membership change, override update)
and periodically (every 30s) as a heartbeat.

### 2.8 Configuration

```rust
pub struct StorageAgentConfig {
    /// Unique node identifier. Default: hostname or random UUID.
    pub node_id: String,

    /// Base directory for all partition data.
    /// Partitions stored at {data_dir}/{partition}/{replica}/
    pub data_dir: PathBuf,

    /// Node capacity weight for HRW assignment (default: 100).
    /// Higher weight → more partitions assigned to this node.
    pub capacity: u32,

    /// Labels for rack-aware placement (optional).
    /// e.g., {"rack": "us-east-1a", "tier": "ssd"}
    pub labels: HashMap<String, String>,

    /// Grace period before stopping a draining store (default: 30s).
    pub drain_grace_period: Duration,

    /// How often to publish health metrics (default: 10s).
    pub health_interval: Duration,

    /// EventBusConfig for the managed topic.
    pub bus_config: EventBusConfig,
}
```

### 2.9 Startup Sequence

```
StorageAgent::run():
  1. Open Zenoh session
  2. Declare liveliness at {key_prefix}/_agents/{node_id}
  3. Publish NodeMetadata to {key_prefix}/_agents/{node_id}/meta
  4. Discover existing agents via liveliness().get()
  5. Subscribe to agent liveliness changes
  6. Subscribe to override updates at {key_prefix}/_cluster/overrides
  7. Compute initial assignment via HRW
  8. Reconcile: start EventStore instances for assigned partitions
  9. Start health reporter (periodic publish)
  10. Start status reporter (on-change + periodic publish)
  11. Enter event loop: wait for membership changes or override updates
```

### 2.10 Failure Scenarios

| Scenario | Detection | Response | Data risk |
|----------|-----------|----------|-----------|
| **Node crash** | Liveliness token expires | Surviving nodes recompute; gain orphaned partitions; recover from peers/caches | None if RF ≥ 2 |
| **Node network partition** | Liveliness lost to some peers | Split-brain: both sides may claim same partition. Safe because stores are idempotent and watermarks are per-replica | Duplicate storage, no loss |
| **Slow node** | Health metrics show high latency | No automatic action (Tier 2 orchestrator can drain) | None |
| **Disk full** | Health metrics show disk usage | No automatic action (Tier 2 orchestrator can drain or trigger GC) | Writes may fail locally |
| **Node rejoins after partition** | Liveliness restored | Nodes recompute; returning node may lose partitions that were reassigned; reconciler drains extras | None |

**Split-brain note:** Because stores are idempotent (same `(publisher_id, seq)`
produces the same key), duplicate storage from split-brain is wasteful but not
incorrect. Watermarks from each replica are independently accurate. Publishers
using `Durability::Quorum` will detect that some replicas are unreachable and
may timeout — this is the correct behavior.

---

## 3. Tier 2: Orchestrator Extensions (Optional)

The orchestrator extends the existing `mitiflow-orchestrator` crate with
cluster-awareness. It **observes** the decentralized Tier 1 system and
provides three capabilities that pure decentralization cannot:

1. **Aggregated cluster view** — single place to see all assignments, lag,
   health
2. **Strategic overrides** — manual placement, drain, rack-awareness
   constraints
3. **Operational API** — REST/queryable endpoints for dashboards and tooling

### 3.1 Cluster View (Observer)

The orchestrator subscribes to all `_cluster/status/{node_id}` and
`_cluster/health/{node_id}` streams, aggregating them into a single
cluster-wide view.

```rust
pub struct ClusterView {
    nodes: HashMap<NodeId, NodeInfo>,
    assignments: HashMap<(u32, u32), AssignmentInfo>,  // (partition, replica) → info
    last_updated: DateTime<Utc>,
}

pub struct NodeInfo {
    pub metadata: NodeMetadata,
    pub health: Option<NodeHealth>,
    pub status: Option<NodeStatus>,
    pub online: bool,
    pub last_seen: DateTime<Utc>,
}

pub struct AssignmentInfo {
    pub partition: u32,
    pub replica: u32,
    pub node_id: NodeId,
    pub state: StoreState,
    pub role: ReplicaRole,
    pub source: AssignmentSource,   // Computed | Override
}

pub enum AssignmentSource {
    Computed,   // Determined by HRW hash
    Override,   // Manually placed by orchestrator
}
```

**Key insight:** The orchestrator does not compute assignments. It reads the
status reports that each `StorageAgent` publishes after computing its own
assignment. This means the orchestrator is a **pure observer** by default —
removing it changes nothing about partition placement.

### 3.2 Override Manager

The orchestrator can publish **override entries** that `StorageAgent` nodes
subscribe to. Overrides take precedence over HRW-computed assignments.

**Override key:**
```
{key_prefix}/_cluster/overrides
```

```rust
/// Published by orchestrator, consumed by all StorageAgents.
pub struct OverrideTable {
    pub entries: Vec<OverrideEntry>,
    pub epoch: u64,        // monotonic, prevents stale override application
    pub expires_at: Option<DateTime<Utc>>,  // auto-expire for temporary overrides
}

pub struct OverrideEntry {
    pub partition: u32,
    pub replica: u32,
    pub node_id: NodeId,
    pub reason: String,    // audit trail: "drain node-3", "rack rebalance"
}
```

**Override semantics:**
- Overrides are **additive** — they replace specific `(partition, replica)`
  assignments, not the entire table
- Overrides have an `epoch` — agents ignore overrides with epoch ≤ last seen
- Overrides can **expire** — useful for temporary maintenance drains
- When an override expires or is removed, agents revert to HRW assignment
- Overrides reference node IDs — if the target node is offline, the override
  is ignored and HRW assignment applies (fail-safe)

**Override lifecycle:**
```
Operator: "drain node-3 for maintenance"
  → Orchestrator computes: which partitions does node-3 own?
  → For each: pick alternative node via HRW (skip node-3)
  → Publish OverrideTable with entries moving partitions away from node-3
  → StorageAgents receive override, reconcile:
      node-3: drains assigned partitions
      other nodes: gain partitions, start recovery
  → Operator confirms drain complete via dashboard
  → Operator performs maintenance on node-3
  → Operator: "undrain node-3"
  → Orchestrator removes override entries, publishes new OverrideTable
  → StorageAgents revert to HRW (node-3 regains its partitions)
```

### 3.3 Strategic Operations

Operations that require a global view — things a single node can't decide
on its own.

#### 3.3.1 Drain Node

Move all partitions off a node before maintenance. The orchestrator computes
replacement assignments and publishes overrides.

```
drain_node(node_id):
    1. Fetch current ClusterView
    2. Find all (partition, replica) assigned to node_id
    3. For each: compute best alternative via HRW (excluding node_id)
    4. Respect constraints: no two replicas on same rack
    5. Publish OverrideTable
    6. Monitor status reports until all partitions on node_id reach Stopped
    7. Return: drain complete
```

#### 3.3.2 Rack-Aware Rebalance

Ensure replicas of the same partition land on different failure domains.
Pure HRW doesn't guarantee this — it's probabilistic with rack labels.

```
ensure_rack_diversity():
    1. Fetch ClusterView with all node labels
    2. For each partition: check if replicas span distinct racks
    3. If violation found: compute override moving one replica to a different rack
    4. Publish OverrideTable with corrections
```

#### 3.3.3 Scale-Aware Rebalance

When cluster load is uneven (some nodes have more partitions than capacity
warrants), the orchestrator can publish overrides to redistribute.

```
rebalance():
    1. Fetch ClusterView + health metrics
    2. Compute ideal distribution based on node capacities
    3. Find partitions to move (minimize total moves)
    4. Publish OverrideTable with moves
    5. Monitor until all moves complete
```

#### 3.3.4 Topic Lifecycle

Extends the existing `create_topic()` / `delete_topic()` on the
orchestrator. When a topic is created, the orchestrator can pre-publish
overrides for initial placement. On delete, it publishes overrides
removing all assignments.

### 3.4 Admin API Extensions

Extends the existing `_admin/**` queryable with cluster operations.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `_admin/cluster/nodes` | GET | All nodes + health + assignment count |
| `_admin/cluster/assignments` | GET | Full assignment table (computed + overrides) |
| `_admin/cluster/assignments/{partition}` | GET | Assignment for one partition |
| `_admin/cluster/overrides` | GET | Current override table |
| `_admin/cluster/drain/{node_id}` | PUT | Start draining a node |
| `_admin/cluster/undrain/{node_id}` | PUT | Remove drain overrides |
| `_admin/cluster/rebalance` | PUT | Trigger rack/load rebalance |
| `_admin/cluster/status` | GET | Cluster-wide health summary |

All endpoints use Zenoh queryable. A future HTTP REST gateway can proxy
these for dashboard integration.

### 3.5 Orchestrator HA

Since the orchestrator is optional and only publishes overrides, HA is
straightforward:

- **Multiple orchestrator replicas** subscribe to the same status/health
  streams
- **Leader election** via Zenoh liveliness + lowest UUID (simplest approach)
  - All replicas observe cluster state
  - Only the leader publishes overrides and serves write endpoints
  - Any replica serves read-only queries (cluster view, assignment list)
- **Leader failure** → next-lowest UUID takes over, reads persisted overrides
  from fjall ConfigStore, re-publishes

**Key simplification:** Because the orchestrator doesn't compute assignments
(agents do that themselves), a leader transition has zero impact on the data
path. Worst case: strategic operations (drain, rebalance) are briefly
unavailable.

---

## 4. Zenoh Key Space

Complete key space for the distributed storage management layer.

### 4.1 Tier 1 Keys (StorageAgent)

| Key Pattern | Publisher | Subscriber | Type | Purpose |
|-------------|-----------|------------|------|---------|
| `{prefix}/_agents/{node_id}` | Agent (liveliness) | All agents | Liveliness | Node presence |
| `{prefix}/_agents/{node_id}/meta` | Agent | All agents, Orchestrator | Put | Node metadata (labels, capacity) |
| `{prefix}/_cluster/health/{node_id}` | Agent | Orchestrator, Dashboard | Put (periodic) | Health metrics |
| `{prefix}/_cluster/status/{node_id}` | Agent | Orchestrator, Dashboard | Put (on-change) | Assignment status |
| `{prefix}/_store/{partition}` | EventStore (liveliness) | RecoveryManager, StoreTracker | Liveliness | Per-partition store presence |
| `{prefix}/_watermark` | EventStore | Publishers, LagMonitor | Put (periodic) | Durability watermarks |

### 4.2 Tier 2 Keys (Orchestrator)

| Key Pattern | Publisher | Subscriber | Type | Purpose |
|-------------|-----------|------------|------|---------|
| `{prefix}/_cluster/overrides` | Orchestrator | All agents | Put (on-change) | Assignment overrides |
| `{prefix}/_admin/cluster/**` | Orchestrator (queryable) | Admin CLI, Dashboard | Queryable | Management API |
| `{prefix}/_config/{topic}` | Orchestrator | Agents | Put | Topic configuration |
| `{prefix}/_lag/{group}/{partition}` | Orchestrator | Dashboard | Put (periodic) | Consumer group lag |

### 4.3 Key Separation from Existing Subsystems

| Prefix | Subsystem | Existing? |
|--------|-----------|-----------|
| `_workers/` | Consumer group membership | Yes (PartitionManager) |
| `_store/` | Per-partition store liveliness | Yes (EventStore) |
| `_watermark` | Durability watermarks | Yes (EventStore) |
| `_offsets/` | Consumer group offsets | Yes (EventStore) |
| `_agents/` | Storage node membership | **New** (StorageAgent) |
| `_cluster/` | Cluster management | **New** (health, status, overrides) |
| `_config/` | Topic configuration | Yes (Orchestrator) |
| `_admin/` | Admin API | Yes (Orchestrator) |
| `_lag/` | Consumer group lag | Yes (LagMonitor) |

---

## 5. Interaction Protocols

### 5.1 Node Join

```
Timeline ──────────────────────────────────────────────────>

Agent D (new)                     Agent A, B, C (existing)
    │                                    │
    │ 1. declare liveliness              │
    │    _agents/D                       │
    │───────────────────────────────────>│
    │                                    │ 2. detect new member
    │ 3. publish NodeMetadata            │    via liveliness
    │    _agents/D/meta                  │
    │───────────────────────────────────>│
    │                                    │ 4. read NodeMetadata
    │                                    │    (capacity, labels)
    │                                    │
    │ 4. discover peers via              │ 5. recompute HRW
    │    liveliness().get()              │    assignments
    │                                    │    (with D included)
    │                                    │
    │ 5. recompute HRW assignments       │ 6. reconcile:
    │    (includes self)                 │    - some partitions
    │                                    │      now assigned to D
    │ 6. reconcile:                      │    - drain those
    │    start EventStores for           │      partitions
    │    assigned partitions             │
    │                                    │
    │ 7. RecoveryManager:               │
    │    query peers for missing events  │
    │<──────────────────────────────────│ 8. serve recovery
    │                                    │    queries
    │ 9. publish NodeStatus              │
    │    _cluster/status/D               │
    │                                    │
```

### 5.2 Node Crash

```
Timeline ──────────────────────────────────────────────────>

Agent B (crashes)                Agent A, C (surviving)
    │                                    │
    │ ╳ crash                            │
    │                                    │ 1. liveliness token
    │                                    │    for B expires
    │                                    │    (Zenoh session timeout)
    │                                    │
    │                                    │ 2. recompute HRW
    │                                    │    (without B)
    │                                    │    A gains p1 (was B's)
    │                                    │    C gains p4 (was B's)
    │                                    │
    │                                    │ 3. reconcile: start
    │                                    │    new EventStores
    │                                    │
    │                                    │ 4. RecoveryManager:
    │                                    │    query other replicas
    │                                    │    for p1, p4 history
    │                                    │
    │                                    │ 5. publish updated
    │                                    │    NodeStatus
    │                                    │
```

### 5.3 Orchestrator Drain

```
Timeline ──────────────────────────────────────────────────>

Operator       Orchestrator          Agent A     Agent B (drain target)
    │                │                  │              │
    │ PUT drain/B    │                  │              │
    │───────────────>│                  │              │
    │                │ 1. compute       │              │
    │                │    overrides     │              │
    │                │    (move B's     │              │
    │                │    partitions    │              │
    │                │    to A and C)   │              │
    │                │                  │              │
    │                │ 2. publish       │              │
    │                │    OverrideTable │              │
    │                │─────────────────>│─────────────>│
    │                │                  │              │
    │                │                  │ 3. reconcile:│
    │                │               gain p1,p4    drain p1,p4
    │                │                  │              │
    │                │                  │ 4. recovery  │
    │                │                  │<────────────│ (serve queries)
    │                │                  │              │
    │                │                  │ 5. active    │ 5. shutdown
    │                │                  │              │    stores
    │                │                  │              │
    │                │ 6. monitor       │              │
    │                │    status until  │              │
    │                │    B has 0       │              │
    │                │    active        │              │
    │                │                  │              │
    │ "drain         │                  │              │
    │  complete"     │                  │              │
    │<───────────────│                  │              │
```

---

## 6. Weighted Assignment Algorithm

The existing `hash_ring.rs` uses unweighted rendezvous hashing. For
heterogeneous clusters, we extend it with capacity weights.

### 6.1 Weighted Rendezvous Hashing

Standard rendezvous hashing assigns partition `p` to the node with the
highest `hash(node_id, p)` score. Weighted rendezvous hashing (WRH)
adjusts scores so that a node with weight `w` receives `w/Σw` of total
partitions in expectation.

**Algorithm** (Schindelhauer & Schomaker, 2005):

$$\text{score}(n, p) = \frac{-w_n}{\ln\left(\frac{h(n, p)}{H_{\max}}\right)}$$

Where $h$ is a hash function normalized to $(0, 1]$. Higher weight produces
higher scores on average, shifting more partitions to that node. When weight
is equal for all nodes, this reduces to standard rendezvous hashing.

```rust
/// Weighted rendezvous hash score.
fn weighted_hrw_score(node_id: &str, partition: u32, replica: u32, weight: u32) -> f64 {
    let mut hasher = DefaultHasher::new();
    (node_id, partition, replica).hash(&mut hasher);
    let hash_val = hasher.finish();

    // Normalize to (0, 1] — avoid ln(0)
    let normalized = (hash_val as f64) / (u64::MAX as f64);
    let normalized = normalized.max(f64::MIN_POSITIVE);

    // Weighted score: higher weight → higher score in expectation
    -(weight as f64) / normalized.ln()
}
```

### 6.2 Rack-Aware Constraint (Tier 1 Best-Effort)

Tier 1 supports best-effort rack-awareness: when selecting replicas,
skip nodes that share a rack label with already-selected replicas. This
doesn't require an orchestrator — each agent can enforce it locally since
the algorithm is deterministic.

```rust
pub fn assign_replicas_rack_aware(
    partition: u32,
    replication_factor: u32,
    nodes: &[NodeDescriptor],
) -> Vec<NodeId> {
    let mut scored: Vec<_> = nodes.iter()
        .map(|n| (weighted_hrw_score(&n.id, partition, 0, n.capacity), n))
        .collect();
    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());

    let mut selected = Vec::new();
    let mut used_racks = HashSet::new();

    // First pass: prefer rack diversity
    for (_, node) in &scored {
        if selected.len() >= replication_factor as usize { break; }
        let rack = node.labels.get("rack").cloned().unwrap_or_default();
        if !used_racks.contains(&rack) {
            selected.push(node.id.clone());
            used_racks.insert(rack);
        }
    }

    // Second pass: fill remaining slots ignoring rack constraint
    if selected.len() < replication_factor as usize {
        for (_, node) in &scored {
            if selected.len() >= replication_factor as usize { break; }
            if !selected.contains(&node.id) {
                selected.push(node.id.clone());
            }
        }
    }

    selected
}
```

**Tier 1** uses this best-effort approach (deterministic, no coordination).
**Tier 2** can override specific placements when the heuristic produces
suboptimal results.

---

## 7. Replication Integration

This design subsumes the replication protocol from
[05_replication.md](05_replication.md). The `replica` index in assignment
tuples `(partition, replica)` directly maps to the replication factor.

### 7.1 How Replication Works

```
TopicConfig { partitions: 4, replication_factor: 3 }
Cluster: [node-A, node-B, node-C, node-D]

Assignment (via weighted HRW):
  (p0, r0) → node-A (primary)    (p0, r1) → node-C    (p0, r2) → node-D
  (p1, r0) → node-B (primary)    (p1, r1) → node-A    (p1, r2) → node-C
  (p2, r0) → node-C (primary)    (p2, r1) → node-D    (p2, r2) → node-A
  (p3, r0) → node-D (primary)    (p3, r1) → node-B    (p3, r2) → node-C

Each node runs EventStore instances for its assigned (partition, replica)
pairs. All replicas subscribe to the same Zenoh key: {prefix}/p/{partition}/**
Zenoh fan-out delivers events to all replicas simultaneously.
```

### 7.2 Watermark per Replica

Each `EventStore` publishes its watermark with a replica identifier:

```
{key_prefix}/_watermark/{partition}/{replica}
```

Publishers using `Durability::Quorum` subscribe to all replica watermarks
and confirm durability when a quorum reports the sequence as committed.

### 7.3 Recovery across Replicas

When a new replica starts (due to node join or failure recovery), the
`RecoveryManager` queries existing replicas of the same partition:

```
session.get("{store_key_prefix}/{partition}?after_seq={local_seq}&pub_id={publisher}")
```

The existing `EventStore` queryable (from `run_queryable_task`) already
serves these queries — no new protocol needed.

---

## 8. Multi-Topic Support

A single `StorageAgent` can manage multiple topics. Each topic has its
own partition count, replication factor, and assignment computed
independently.

```rust
pub struct StorageAgent {
    node_id: NodeId,
    session: Session,
    topics: HashMap<String, TopicState>,
    config: StorageAgentConfig,
}

struct TopicState {
    topic_config: TopicConfig,
    reconciler: Reconciler,
    recovery_mgr: RecoveryManager,
}
```

**Topic discovery:**
- Agent subscribes to `{key_prefix}/_config/*` (orchestrator publishes
  topic configs here — this is existing behavior)
- On new topic config: create `TopicState`, compute assignments, reconcile
- On topic deletion: drain all local stores for that topic

**Without orchestrator:** Topics can be configured via a config file or
CLI that publishes to `_config/{topic}` directly. The orchestrator is
just one possible source of topic configs.

---

## 9. Implementation Plan

### Phase 1: StorageAgent Core (Tier 1 MVP) — ✅ Done

Minimal decentralized system for single-topic deployment.

- [x] **Weighted HRW** — `hash_ring.rs` extended with `weighted_hrw_score()`
      and `assign_replicas()`
- [x] **MembershipTracker** — liveliness-based node discovery at
      `_agents/{id}`, metadata queryable, peer watch task
- [x] **Reconciler** — desired vs actual state diff, start/stop EventStore
      per `(partition, replica)` tuple
- [x] **StorageAgent binary** — main daemon wiring all components
- [x] **StorageAgentConfig** — configuration struct with builder pattern
- [x] **HealthReporter** — periodic health publish to
      `_cluster/health/{id}`
- [x] **StatusReporter** — on-change status publish to
      `_cluster/status/{id}`

### Phase 2: Recovery & Multi-Replica (Tier 1 Complete) — ✅ Done

Failure resilience and replication support.

- [x] **RecoveryManager** — peer query on partition gain via
      `session.get()` with `accept_replies(ReplyKeyExpr::Any)`;
      `recover_from_cache()` fallback; wired into Reconciler via
      `with_recovery()`
- [x] **Rack-aware assignment** — `assign_replicas_rack_aware()` with
      two-pass rack diversity selection
- [x] **Multi-replica support** — `(partition, replica)` tuple in reconciler;
      `assign_replicas()` returns replication chain
- [x] **Override support** — agents subscribe to `_cluster/overrides`;
      overrides take precedence over HRW; epoch and expiry support
- [x] **Drain grace period** — configurable drain timeout in reconciler
- [x] **Arc<dyn StorageBackend> sharing** — blanket impl allows EventStore
      and RecoveryManager to share the same backend

### Phase 3: Orchestrator Cluster Extensions (Tier 2) — Not started

Optional operational control.

- [ ] **ClusterView** — subscribe to status/health streams, aggregate
- [ ] **OverrideManager** — publish/manage override table
- [ ] **Drain operation** — compute overrides to evacuate a node
- [ ] **Admin API extensions** — cluster endpoints on
      `_admin/cluster/**`
- [ ] **Orchestrator HA** — liveliness-based leader election

### Phase 4: Multi-Topic & Polish — Not started

- [ ] **Multi-topic support** — topic discovery via `_config/*`
- [ ] **Rebalance operation** — load-aware override generation
- [ ] **CLI tooling** — `mitiflow-ctl` for cluster management
- [ ] **Dashboard integration** — REST proxy for admin queryable

---

## 10. Comparison with Alternatives

| Dimension | This design (2-tier) | Pure centralized | Pure decentralized | Kafka Controller |
|---|---|---|---|---|
| Works without coordinator | Yes (Tier 1 only) | No | Yes | No |
| Operator control | Via overrides (Tier 2) | Full | None | Full |
| Failure recovery | Automatic (HRW) | Orchestrator-driven | Automatic | Controller-driven |
| SPOF | None (Tier 1) | Orchestrator | None | Controller (ZK) |
| Rack-awareness | Best-effort (T1) + Override (T2) | Full | None | Full |
| Weighted nodes | HRW weights | Placement policy | Equal only | Rack-aware only |
| Rebalance disruption | Minimal (~1/N) | Staged | Minimal | Staged |
| Complexity | Medium | Medium | Low | High |
| Operational overhead | Low (T1) / Medium (T1+T2) | Medium | Low | High |
