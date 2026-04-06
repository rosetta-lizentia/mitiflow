# 13. Distributed Storage Management

Two-tier architecture for partition placement and lifecycle across a cluster of `StorageAgent` nodes.

**Implementation status:** Tier 1 complete, Tier 2 not started.

| Area | Key Files |
|------|-----------|
| Storage agent | `mitiflow-storage/src/agent.rs`, `reconciler.rs`, `recovery.rs`, `membership.rs`, `health.rs`, `status.rs` |
| Hash ring / HRW | `mitiflow/src/partition/hash_ring.rs` |
| Topic management | `mitiflow-storage/src/topic_supervisor.rs`, `topic_watcher.rs`, `topic_worker.rs` |
| Orchestrator | `mitiflow-orchestrator/src/orchestrator.rs` |

---

## 1. Design Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    Tier 2: Orchestrator (optional)               │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────────────┐ │
│  │ ClusterView  │  │  Override    │  │  Admin API            │ │
│  │ (observer)   │  │  Manager     │  │  (_admin/cluster/**) │ │
│  └──────┬───────┘  └──────┬───────┘  └───────────────────────┘ │
│         │ subscribes       │ publishes                          │
└─────────┼──────────────────┼────────────────────────────────────┘
          ↓                  ↓
┌─────────────────────────────────────────────────────────────────┐
│                    Tier 1: StorageAgent (per node)               │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────────────┐ │
│  │ Membership   │  │  Reconciler  │  │  Recovery Manager     │ │
│  │ Tracker      │  │              │  │                       │ │
│  └──────┬───────┘  └──────┬───────┘  └───────────┬───────────┘ │
│         │                  │                      │             │
│  ┌──────┴──────────────────┴──────────────────────┴───────────┐ │
│  │              EventStore instances (per partition/replica)   │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

- **Tier 1** runs on every node with zero coordination. Nodes discover each other via Zenoh liveliness, compute partition assignments deterministically using weighted rendezvous hashing (HRW), and reconcile local EventStore instances.
- **Tier 2** is an optional orchestrator that observes the cluster and publishes override entries for operational tasks (drain, rack-aware rebalance). Removing the orchestrator has **no impact** on partition placement.

---

## 2. Tier 1 Components

### 2.1 Core Components

| Component | Responsibility |
|-----------|---------------|
| **MembershipTracker** | Discovers peers via Zenoh liveliness at `_agents/{node_id}`. Maintains a set of `NodeDescriptor` (id, capacity, labels). Fires membership-changed events on join/leave. |
| **Reconciler** | Computes desired state (HRW assignments + overrides) vs actual state (running EventStores). Starts, recovers, or drains stores to converge. |
| **RecoveryManager** | On partition gain, queries peer replicas via `session.get()` to backfill missing events. Falls back to publisher cache queries. |
| **HealthReporter** | Publishes `NodeHealth` (partition counts, disk usage, uptime) every `health_interval` (default 10s) to `_cluster/health/{node_id}`. |
| **StatusReporter** | Publishes `NodeStatus` (per-partition state, epoch, watermarks) on every reconciliation and periodically (30s) to `_cluster/status/{node_id}`. |

### 2.2 Membership & Discovery

Nodes declare Zenoh liveliness tokens at `{prefix}/_agents/{node_id}` and publish metadata (capacity weight, rack labels) to `{prefix}/_agents/{node_id}/meta`. On startup, nodes discover existing peers via `liveliness().get()` and subscribe to join/leave changes.

### 2.3 Assignment Computation

Weighted rendezvous hashing (WRH) extends standard HRW with capacity weights (Schindelhauer & Schomaker, 2005):

$$\text{score}(n, p) = \frac{-w_n}{\ln\left(\frac{h(n, p)}{H_{\max}}\right)}$$

A node with weight $w$ receives $w / \Sigma w$ of total partitions in expectation. When weights are equal, this reduces to standard rendezvous hashing. The algorithm is deterministic — all nodes compute the same assignment independently.

**Override resolution:** If an override exists for `(partition, replica)` and the target node is online, it takes precedence over HRW. If the target is offline, HRW applies (fail-safe).

**Rack-aware placement (best-effort):** A two-pass approach selects replicas preferring rack diversity first, then fills remaining slots ignoring the constraint. No coordination needed since the algorithm is deterministic.

### 2.4 Reconciler

The reconciler maintains per-partition state:

| State | Meaning |
|-------|---------|
| **Starting** | EventStore is being opened |
| **Recovering** | Backfilling from peer replicas |
| **Active** | Fully caught up, serving reads/writes |
| **Draining** | No longer assigned, waiting for grace period before shutdown |
| **Stopped** | EventStore closed |

On each membership change or override update, the reconciler:
1. Computes desired assignments (HRW + overrides)
2. Diffs against running stores
3. Starts new stores (→ Starting → Recovering → Active)
4. Drains removed stores (→ Draining → Stopped after grace period)

### 2.5 Recovery Protocol

When a node gains a new partition, the `RecoveryManager`:
1. Queries peer replicas via `session.get("{store_prefix}/{partition}?after_seq={local_seq}")` with `accept_replies(ReplyKeyExpr::Any)`
2. Inserts recovered events into the local store
3. Falls back to publisher cache queries if no peer replicas are available
4. Transitions partition to Active once caught up

### 2.6 Configuration

Key configuration fields for `StorageAgentConfig`:
- `node_id` — unique node identifier (default: hostname or UUID)
- `data_dir` — base directory; partitions stored at `{data_dir}/{partition}/{replica}/`
- `capacity` — HRW weight (default 100); higher = more partitions
- `labels` — rack-aware placement labels (e.g., `{"rack": "us-east-1a"}`)
- `drain_grace_period` — time before stopping a draining store (default 30s)
- `health_interval` — health publish frequency (default 10s)

### 2.7 Startup Sequence

1. Open Zenoh session
2. Declare liveliness at `_agents/{node_id}`
3. Publish `NodeMetadata` to `_agents/{node_id}/meta`
4. Discover existing agents via `liveliness().get()`
5. Subscribe to agent liveliness changes + override updates
6. Compute initial assignment via HRW
7. Reconcile: start EventStore instances for assigned partitions
8. Start health and status reporters
9. Enter event loop: react to membership changes or override updates

### 2.8 Failure Scenarios

| Scenario | Detection | Response | Data risk |
|----------|-----------|----------|-----------|
| **Node crash** | Liveliness token expires | Surviving nodes recompute; gain orphaned partitions; recover from peers | None if RF ≥ 2 |
| **Network partition** | Liveliness lost to some peers | Split-brain: both sides may claim same partition. Safe because stores are idempotent (same `(pub_id, seq)` → same key) | Duplicate storage, no loss |
| **Slow node** | Health metrics | No automatic action (Tier 2 can drain) | None |
| **Disk full** | Health metrics | No automatic action (Tier 2 can drain/GC) | Writes may fail locally |
| **Node rejoin** | Liveliness restored | Recompute; returning node may lose reassigned partitions; reconciler drains extras | None |

---

## 3. Tier 2: Orchestrator Extensions (Optional)

The orchestrator **observes** the decentralized Tier 1 system and provides three capabilities:

1. **Aggregated cluster view** — single place to see all assignments, lag, health
2. **Strategic overrides** — manual placement, drain, rack-awareness corrections
3. **Operational API** — queryable endpoints for dashboards and tooling

### 3.1 Cluster View

Subscribes to all `_cluster/status/{node_id}` and `_cluster/health/{node_id}` streams, building a `ClusterView` (nodes, assignments, health). The orchestrator does **not** compute assignments — it reads status reports from agents. Removing the orchestrator changes nothing about partition placement.

### 3.2 Override Manager

Publishes `OverrideTable` to `{prefix}/_cluster/overrides`:
- Overrides are **additive** — they replace specific `(partition, replica)` assignments
- Monotonic `epoch` prevents stale application
- Optional `expires_at` for temporary overrides (e.g., maintenance drains)
- If the target node is offline, the override is ignored (fail-safe)

### 3.3 Strategic Operations

| Operation | Purpose |
|-----------|---------|
| **Drain node** | Move all partitions off a node before maintenance via overrides |
| **Undrain node** | Remove drain overrides, revert to HRW assignment |
| **Rack-aware rebalance** | Check replica rack diversity, publish corrective overrides |
| **Scale-aware rebalance** | Redistribute based on node capacities and load |
| **Topic lifecycle** | Pre-publish placement overrides on topic create; remove on delete |

### 3.4 Admin API Extensions

| Endpoint | Description |
|----------|-------------|
| `_admin/cluster/nodes` | All nodes + health + assignment count |
| `_admin/cluster/assignments` | Full assignment table (computed + overrides) |
| `_admin/cluster/overrides` | Current override table |
| `_admin/cluster/drain/{node_id}` | Start draining a node |
| `_admin/cluster/undrain/{node_id}` | Remove drain overrides |
| `_admin/cluster/rebalance` | Trigger rack/load rebalance |
| `_admin/cluster/status` | Cluster-wide health summary |

### 3.5 Orchestrator HA

- Multiple replicas subscribe to the same status/health streams
- Leader election via Zenoh liveliness + lowest UUID
- Only the leader publishes overrides; any replica serves read-only queries
- Leader failure → next-lowest UUID takes over with zero data-path impact

---

## 4. Zenoh Key Space

### Tier 1 Keys (StorageAgent)

| Key Pattern | Type | Purpose |
|-------------|------|---------|
| `{prefix}/_agents/{node_id}` | Liveliness | Node presence |
| `{prefix}/_agents/{node_id}/meta` | Put | Node metadata (capacity, labels) |
| `{prefix}/_cluster/health/{node_id}` | Put (periodic) | Health metrics |
| `{prefix}/_cluster/status/{node_id}` | Put (on-change) | Assignment status |
| `{prefix}/_store/{partition}` | Liveliness | Per-partition store presence |
| `{prefix}/_watermark` | Put (periodic) | Durability watermarks |

### Tier 2 Keys (Orchestrator)

| Key Pattern | Type | Purpose |
|-------------|------|---------|
| `{prefix}/_cluster/overrides` | Put (on-change) | Assignment overrides |
| `{prefix}/_admin/cluster/**` | Queryable | Management API |
| `{prefix}/_config/{topic}` | Put | Topic configuration |
| `{prefix}/_lag/{group}/{partition}` | Put (periodic) | Consumer group lag |

### Key Separation from Existing Subsystems

| Prefix | Subsystem | New? |
|--------|-----------|------|
| `_workers/` | Consumer group membership | Existing |
| `_store/` | Per-partition store liveliness | Existing |
| `_watermark` | Durability watermarks | Existing |
| `_offsets/` | Consumer group offsets | Existing |
| `_agents/` | Storage node membership | **New** |
| `_cluster/` | Cluster management (health, status, overrides) | **New** |

---

## 5. Replication Integration

The `replica` index in assignment tuples `(partition, replica)` maps directly to the replication factor. See [05_replication.md](05_replication.md) for the base design.

- All replicas subscribe to `{prefix}/p/{partition}/**`; Zenoh fan-out delivers events simultaneously
- Each replica publishes its watermark at `_watermark/{partition}/{replica}`
- Publishers using `Durability::Quorum` confirm when a quorum of replicas report the sequence as committed
- Recovery uses the existing EventStore queryable — no new protocol needed

---

## 6. Multi-Topic Support

A single `StorageAgent` manages multiple topics, each with independent partition count, replication factor, and HRW assignment.

- **Topic discovery:** Agent subscribes to `{prefix}/_config/*` (published by orchestrator or CLI)
- **On new topic:** Create topic state, compute assignments, reconcile
- **On topic delete:** Drain all local stores for that topic
- **Without orchestrator:** Topics can be configured via config file or CLI publishing directly to `_config/{topic}`

---

## 7. Implementation Status

### Phase 1: StorageAgent Core (Tier 1 MVP) — Done

Weighted HRW, MembershipTracker, Reconciler, StorageAgent binary, configuration, health and status reporters.

### Phase 2: Recovery & Multi-Replica (Tier 1 Complete) — Done

RecoveryManager (peer query + cache fallback), rack-aware assignment, multi-replica support, override support, drain grace period.

### Phase 3: Orchestrator Cluster Extensions (Tier 2) — Not started

ClusterView, OverrideManager, drain operation, admin API extensions, orchestrator HA.

### Phase 4: Multi-Topic & Polish — Not started

Multi-topic discovery, rebalance operations, CLI tooling, dashboard integration.

---

## 8. Comparison with Alternatives

| Dimension | This design (2-tier) | Pure centralized | Pure decentralized | Kafka Controller |
|---|---|---|---|---|
| Works without coordinator | Yes (Tier 1) | No | Yes | No |
| Operator control | Via overrides (Tier 2) | Full | None | Full |
| Failure recovery | Automatic (HRW) | Orchestrator-driven | Automatic | Controller-driven |
| SPOF | None (Tier 1) | Orchestrator | None | Controller (ZK) |
| Rack-awareness | Best-effort (T1) + Override (T2) | Full | None | Full |
| Weighted nodes | HRW weights | Placement policy | Equal only | Rack-aware only |
| Rebalance disruption | Minimal (~1/N) | Staged | Minimal | Staged |
| Complexity | Medium | Medium | Low | High |
