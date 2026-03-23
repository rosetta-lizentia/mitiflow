# Developer Experience & Multi-Topic Agent Design

Discussion document exploring improvements to Mitiflow's developer experience,
operational simplicity, and the evolution toward a broker-like single-process
deployment model.

---

## 1. Problem Statement

Today, running Mitiflow storage involves:

1. **One agent process per topic** — `StorageAgentConfig` wraps a single
   `EventBusConfig` (single `key_prefix`). To serve N topics you deploy N
   agent processes, each with its own env-var config.
2. **Manual topic setup** — the orchestrator can persist `TopicConfig` and
   publish it, but nothing automatically provisions stores on agents. An
   operator must pre-configure each agent with the right `key_prefix`,
   `num_partitions`, and `replication_factor` before starting it.
3. **No unified control loop** — the orchestrator tracks topics and cluster
   state, but the agent has no way to *discover* new topics at runtime or
   adjust its partition count without a restart.

This creates friction at every scale:

- **Dev laptop:** spin up 3 processes for 3 topics, manage env vars per
  process, remember to restart when config changes.
- **Small production:** Kubernetes Deployment per topic × nodes, separate
  health checks, independent scaling knobs.
- **Multi-tenant:** topic lifecycle is fully manual — create topic in
  orchestrator, then update agent deployments to match.

The goal is: **one agent process per node, serving any number of topics, with
topics created dynamically via the orchestrator.** This matches the operational
model of Kafka, Redpanda, and NATS JetStream — run a cluster of N nodes, then
create/delete topics via an admin API without touching the node deployment.

---

## 2. Multi-Topic Agent

### 2.1 Previous Architecture (Single-Topic)

> **Note:** This architecture has been superseded. The agent now runs
> `TopicSupervisor` + `TopicWatcher` in a multi-topic configuration.
> See the target architecture in §6.

```
┌─── mitiflow-agent process ────────────────────────────┐
│  StorageAgentConfig { bus_config: EventBusConfig }     │
│            │                                          │
│     MembershipTracker  ←  Zenoh liveliness            │
│            │                 (_agents/{node_id})       │
│     Reconciler ─────────→ EventStore × P×RF           │
│            │                 per (partition, replica)  │
│     RecoveryManager                                   │
│     HealthReporter                                    │
│     StatusReporter                                    │
│     OverrideSubscriber                                │
└───────────────────────────────────────────────────────┘
      ▲ all scoped to ONE key_prefix
```

### 2.2 Options for Multi-Topic Support

#### Option A: Topic Supervisor — One Reconciler per Topic

**Approach:** Add a `TopicSupervisor` layer above the current `StorageAgent`
internals. Each managed topic gets its own `Reconciler`, `MembershipTracker`,
`RecoveryManager`, health/status reporters — essentially the current
`StorageAgent` guts wrapped in a per-topic struct.

```
┌─── mitiflow-agent process ──────────────────────────────────┐
│  AgentConfig { node_id, data_dir, capacity, labels }        │
│                                                             │
│  TopicSupervisor                                            │
│    ├── TopicWorker("events", key_prefix="app/events")       │
│    │     ├── MembershipTracker (liveliness: app/events/_agents)
│    │     ├── Reconciler → EventStore × P×RF                 │
│    │     ├── RecoveryManager                                │
│    │     └── StatusReporter                                 │
│    ├── TopicWorker("logs", key_prefix="app/logs")           │
│    │     └── ...                                            │
│    └── TopicWatcher                                         │
│          └── subscribes to orchestrator topic configs        │
│                                                             │
│  Shared: HealthReporter (node-level), OverrideSubscriber    │
└─────────────────────────────────────────────────────────────┘
```

- **Pros:**
  - Minimal code change — each topic is an independent copy of the current
    `StorageAgent` internals.
  - Fault isolation — a panic or bug in one topic's reconciler doesn't affect
    others.
  - Per-topic membership is already correct (each topic has its own liveliness
    prefix, so only nodes serving that topic participate in its HRW ring).
  - Easy to reason about lifecycle: add topic = spawn workers, remove topic =
    shut down workers.
- **Cons:**
  - O(T) liveliness tokens, O(T) override subscribers, O(T) status reporters
    where T = number of topics — overhead scales linearly with topic count.
  - Each `MembershipTracker` declares a liveliness token and watches for peers
    independently — at 100 topics × 10 nodes that's 1000 liveliness tokens on
    the Zenoh network.
  - No resource sharing (e.g., disk I/O budget, memory limits) between topics.
- **Migration cost:** Medium — refactor `StorageAgent` into `TopicWorker` +
  `TopicSupervisor`, keep internal logic untouched.
- **Fits when:** Topic count is moderate (≤50 per node), isolation matters more
  than efficiency.

#### Option B: Shared Membership — Unified Ring, Per-Topic Reconcilers

**Approach:** All topics on a node share a single `MembershipTracker`
(one liveliness token: `_agents/{node_id}`). Each topic still gets its own
`Reconciler`, but the HRW ring is shared. The membership ring reflects *node*
presence, not *per-topic* presence. Topics are differentiated only at the
reconciler level.

```
┌─── mitiflow-agent process ──────────────────────────────────┐
│  AgentConfig { node_id, data_dir, capacity, labels }        │
│                                                             │
│  SharedMembership ← single liveliness token                 │
│    └── node-level ring: [node-1, node-2, node-3]            │
│                                                             │
│  TopicSupervisor                                            │
│    ├── topic "events": Reconciler (prefix="app/events")     │
│    │     → EventStore × P×RF                                │
│    ├── topic "logs":   Reconciler (prefix="app/logs")       │
│    │     → EventStore × P×RF                                │
│    └── TopicWatcher (subscribes to orchestrator configs)     │
│                                                             │
│  HealthReporter (node-level), StatusReporter (aggregated)   │
└─────────────────────────────────────────────────────────────┘
```

- **Pros:**
  - Constant O(1) liveliness overhead per node regardless of topic count.
  - Simpler operational model — Zenoh network sees N liveliness tokens (one per
    node) not N×T.
  - Health/status reporting is unified at the node level.
- **Cons:**
  - **All-or-nothing membership:** If 3 out of 5 nodes serve topic X, the HRW
    ring still includes all 5 nodes, assigning partitions to nodes that don't
    actually host that topic. Requires either: (a) topic subscription metadata
    in `NodeMetadata` so HRW filters, or (b) all nodes serve all topics.
  - More complex membership semantics — need to distinguish "node is alive" from
    "node is serving topic X".
  - A bug in shared membership affects all topics simultaneously.
- **Migration cost:** Medium-High — refactor membership to be topic-unaware,
  add topic filtering to HRW, rework status reporting.
- **Fits when:** Topic count is high, all nodes are homogeneous (every node
  serves every topic), or per-topic membership filtering is acceptable.

#### Option C: Hybrid — Shared Membership + Per-Topic Subscription Tokens

**Approach:** Single node-level liveliness token for presence detection, plus
lightweight per-topic subscription tokens. The HRW ring is computed per-topic
using only nodes that declared interest in that topic.

```
Liveliness tokens:
  _agents/{node_id}                       ← node presence (shared)
  _agents/{node_id}/topics/{topic_name}   ← topic interest (per-topic)

HRW ring for topic "events":
  filter(_agents/*/topics/events) → [node-1, node-3]  (only 2 of 3 nodes)
```

- **Pros:**
  - Best of both: low overhead (1 main token + 1 per topic) and correct
    per-topic rings.
  - Supports heterogeneous clusters — node A serves topics X,Y; node B serves
    topic Z only.
  - Topic addition/removal is just declaring/undeclaring a liveliness token.
- **Cons:**
  - Additional Zenoh liveliness complexity (hierarchical tokens).
  - Slightly more complex membership tracker — must watch both levels.
  - Need to handle race conditions: topic token appears before node metadata
    is queryable.
- **Migration cost:** Medium — extend `MembershipTracker` with topic-level
  tokens, modify `compute_desired_assignment` to accept per-topic node list.
- **Fits when:** Heterogeneous clusters (not all nodes serve all topics) or
  large topic counts where per-topic membership matters.

### 2.3 Comparison

| Dimension                   | A: Per-Topic Workers  | B: Shared Ring    | C: Hybrid Tokens    |
|-----------------------------|-----------------------|-------------------|---------------------|
| Liveliness tokens per node  | O(T)                  | O(1)              | O(T) but lightweight|
| Per-topic HRW correctness   | Yes (natural)         | Needs filtering   | Yes (filtered)      |
| Heterogeneous clusters      | Yes                   | No (all-or-nothing)| Yes                |
| Fault isolation             | Strong                | Weak              | Moderate            |
| Code complexity             | Low (refactor only)   | Medium-High       | Medium              |
| Shared resource management  | None                  | Possible          | Possible            |
| Operational overhead        | Moderate (many tokens)| Low               | Low-Moderate        |
| Migration from current code | Straightforward       | Significant       | Moderate            |

### 2.4 Recommendation

**Option A (Topic Supervisor)** for the initial implementation, with an
incremental path toward **Option C** if token overhead becomes a problem at
scale.

Rationale:
- Option A is the lowest-risk change — the per-topic `TopicWorker` is
  essentially today's `StorageAgent` internals, so all 47 existing tests
  continue to pass with minimal modification.
- The current `MembershipTracker` already works per-prefix, so per-topic
  membership is free.
- For the expected scale (tens of topics, not thousands), O(T) tokens per node
  is acceptable.
- Option C can be adopted later by consolidating per-topic membership trackers
  into a shared one + topic tokens — this is additive, not a rewrite.

---

## 3. Topic Provisioning Protocol

### 3.1 Closed Gap

> **Implemented.** The gap described below is now closed. Agents run a
> `TopicWatcher` that subscribes to `{global_prefix}/_config/**`. The
> orchestrator exposes a `_config/**` Zenoh queryable for late-joiner
> recovery. Placement filtering uses `required_labels` / `excluded_labels`
> on `TopicConfig` (Option C, see §3.4). See `mitiflow-agent/src/topic_watcher.rs`.

The orchestrator can `create_topic(TopicConfig)` which persists config to
its fjall store and publishes to `{key_prefix}/_config/{topic_name}`. But
nothing happens on the agent side — agents don't listen for topic configs and
can't dynamically start serving a new topic.

### 3.2 Options

#### Option A: Pull Model — Agents Subscribe to Config Changes

**Approach:** Agents subscribe to `_config/**` on a well-known global prefix
(or the orchestrator's prefix). When a new `TopicConfig` is published, agents
start a `TopicWorker` for that topic. On delete, they shut it down.

```
Orchestrator                          Agent (TopicWatcher)
     │                                     │
     │── publish TopicConfig ──────────────▶│ (subscribe _config/**)
     │   to _config/{topic_name}           │
     │                                     │── spawn TopicWorker
     │                                     │   for new topic
     │                                     │
     │── delete TopicConfig ───────────────▶│── shutdown TopicWorker
```

- **Pros:**
  - Decentralized — agents react independently, no orchestrator→agent RPC
    needed.
  - Resilient — late-joining agents query existing `_config/**` via
    `session.get()` (Zenoh queryable on orchestrator side) to discover all
    active topics.
  - Consistent with existing patterns (override subscription, liveliness
    already work this way).
- **Cons:**
  - Agents must trust published configs — no access control for who can create
    topics (Zenoh ACL can mitigate).
  - All agents see all topic configs — need a filter mechanism if some nodes
    shouldn't serve certain topics (solved by topic filters in agent config or
    label matching on `TopicConfig`).
- **Migration cost:** Low — add `TopicWatcher` component to agent, extend
  orchestrator queryable to serve historical configs.

#### Option B: Push Model — Orchestrator Directs Agents

**Approach:** Orchestrator publishes topic assignments directly to each agent
via `_agents/{node_id}/assign`. Agent treats these as commands.

- **Pros:**
  - Orchestrator has full control over which agent serves which topic.
  - Supports placement policies (e.g., "topic X only on nodes with SSD label").
- **Cons:**
  - Requires orchestrator to be online for topic creation — breaks
    decentralized-first principle.
  - Orchestrator must track which agents exist and push to each one.
  - More complex failure modes (what if push is lost?).
- **Migration cost:** Medium — new RPC-like protocol between orchestrator and
  agents.

#### Option C: Hybrid — Pull with Orchestrator Hints

**Approach:** Agents subscribe to `_config/**` (pull), but `TopicConfig`
includes optional placement hints (labels, node filters). Agents decide
independently whether to serve a topic based on their own labels matching the
config's placement requirements.

```rust
pub struct TopicConfig {
    pub name: String,
    pub key_prefix: String,
    pub num_partitions: u32,
    pub replication_factor: u32,
    pub retention: RetentionPolicy,
    pub compaction: CompactionPolicy,
    // NEW: placement constraints
    pub required_labels: HashMap<String, String>,  // agent must have these
    pub excluded_labels: HashMap<String, String>,   // agent must NOT have these
}
```

Agent logic:
```rust
fn should_serve_topic(&self, topic: &TopicConfig) -> bool {
    // Check required labels
    for (k, v) in &topic.required_labels {
        if self.labels.get(k) != Some(v) {
            return false;
        }
    }
    // Check excluded labels
    for (k, v) in &topic.excluded_labels {
        if self.labels.get(k) == Some(v) {
            return false;
        }
    }
    true
}
```

- **Pros:**
  - Decentralized (agents decide), but orchestrator influences placement.
  - Works without orchestrator for simpler setups (agents serve all topics).
  - Label-based placement is familiar (Kubernetes affinity/anti-affinity).
- **Cons:**
  - Orchestrator can't force exact placement (it's advisory).
  - Slightly more complex than pure pull.
- **Migration cost:** Low-Medium.

### 3.3 Comparison

| Dimension                   | A: Pull           | B: Push            | C: Hybrid Pull     |
|-----------------------------|--------------------|--------------------|---------------------|
| Decentralized-first         | Yes                | No                 | Yes                 |
| Orchestrator required?      | No (fallback)      | Yes                | No (fallback)       |
| Placement control           | None               | Full               | Advisory (labels)   |
| Late-joiner recovery        | Query _config/**   | Orchestrator resend| Query _config/**    |
| Complexity                  | Low                | Medium             | Low-Medium          |
| Consistent with existing    | Yes (like overrides)| New pattern       | Yes                 |

### 3.4 Recommendation

**Option C (Hybrid Pull)** — it preserves the decentralized-first principle
while giving operators placement control via labels, which is the same
mechanism already used for rack-aware assignment.

Combined protocol:

1. **Orchestrator** creates topic → persists to `ConfigStore` → publishes
   `TopicConfig` to `{global_prefix}/_config/{topic_name}`.
2. **Orchestrator** declares queryable on `{global_prefix}/_config/**` so
   late-joining agents can fetch all current topics.
3. **Agent** on startup: `session.get("{global_prefix}/_config/**")` → discover
   all existing topics → filter by labels → spawn `TopicWorker` for each.
4. **Agent** subscribes to `{global_prefix}/_config/**` → react to create/update/delete.
5. **Agent** on topic delete: gracefully drain and shut down `TopicWorker`.

---

## 4. Orchestrator Improvements

### 4.1 Current State

> **Updated.** Several gaps listed here have been addressed:
> - ✅ HTTP REST API — embedded axum server (`mitiflow-orchestrator/src/http.rs`)
> - ✅ Automated provisioning — via `TopicWatcher` on agents
> - ✅ Proactive action — `AlertManager` monitors cluster health
> - HA remains deferred (single instance)

The orchestrator is functional for config management, lag monitoring, store
lifecycle tracking, and cluster observation. Notable gaps:

- No HTTP REST API (only Zenoh queryable)
- No automated provisioning (create_topic doesn't materialize stores)
- No HA (single instance)
- ClusterView and TopicManager exist but aren't wired to any proactive actions

### 4.2 Proposed Improvements

#### 4.2.1 HTTP Admin API

**Problem:** Zenoh queryable is powerful but requires a Zenoh client to interact
with. DevOps tools, dashboards, and CI/CD pipelines expect HTTP/REST.

**Options:**

| Option | Approach | Pros | Cons |
|--------|----------|------|------|
| A: Embedded Axum | Embed an HTTP server (axum) in the orchestrator binary | Single binary, low latency to internal state | Adds dependency, mixes concerns |
| B: Sidecar gateway | Separate HTTP→Zenoh gateway process | Clean separation, reusable for any Zenoh app | Extra deployment, extra hop |
| C: CLI only | Enhance `mitiflow-ctl` as the primary admin tool | Simplest, already exists | Not suitable for automation/dashboards |

**Recommendation:** **Option A (Embedded Axum)** — axum is lightweight, async
(Tokio-native), and the orchestrator is already a long-running binary. Endpoints
mirror the existing Zenoh queryable:

```
GET  /api/v1/topics                    # list topics
POST /api/v1/topics                    # create topic
GET  /api/v1/topics/{name}             # get topic config
DELETE /api/v1/topics/{name}           # delete topic

GET  /api/v1/cluster/nodes             # online nodes
GET  /api/v1/cluster/assignments       # partition assignments
POST /api/v1/cluster/drain/{node_id}   # drain a node
DELETE /api/v1/cluster/drain/{node_id} # undrain a node

GET  /api/v1/lag/{group_id}            # consumer group lag
```

#### 4.2.2 ProActive Cluster Management

**Problem:** The orchestrator observes but doesn't act. It knows about
unhealthy nodes, unbalanced partitions, and missing replicas, but takes no
remedial action.

**Proposed improvements (incremental):**

1. **Health-based alerts** — when a node's health metrics cross thresholds
   (disk usage > 80%, event backlog > N), publish alerts to
   `_cluster/alerts/{severity}/{node_id}`.
2. **Auto-drain unhealthy nodes** — if a node's liveliness token disappears
   for > configurable timeout, automatically generate drain overrides to
   redistribute its partitions.
3. **Rebalance advisor** — compute partition distribution skew and suggest
   (or auto-apply) overrides to equalize load. Uses node capacity weights.
4. **Under-replicated partition detection** — if `rf` replicas are expected
   but `ClusterView` shows fewer online, alert and optionally trigger
   placement overrides to restore replication.

#### 4.2.3 Topic Lifecycle Automation

When `create_topic()` is called, the orchestrator should:

1. Persist config ✅ (done today)
2. Publish config to `_config/{topic}` ✅ (done today)
3. Create per-topic `ClusterView` ✅ (done today)
4. **NEW:** Wait for agents to pick up the topic and report status
5. **NEW:** Monitor partition coverage — all `P × RF` assignments active
6. **NEW:** Expose topic readiness status via admin API (`ready`, `partial`,
   `pending`)

When `delete_topic()` is called:
1. Publish delete marker (e.g., tombstone `TopicConfig` with `deleted: true`)
2. Wait for agents to drain and report empty
3. Remove from `ConfigStore`
4. Remove per-topic `ClusterView`

#### 4.2.4 Topic Data Deletion API

**Problem:** When a topic is deleted, data files remain on agent nodes.
Operators need a way to reclaim disk space without SSH-ing into each node.

**Decision:** Expose a `DELETE /api/v1/topics/{name}/data` endpoint (and
corresponding Zenoh queryable command) that instructs agents to delete the
on-disk fjall data for a topic.

Protocol:
1. Orchestrator publishes data-delete command to
   `{global_prefix}/_commands/delete_data/{topic_name}`.
2. Agents' `TopicWatcher` receives the command.
3. Agent stops `TopicWorker` for the topic (if still running).
4. Agent deletes `{data_dir}/{topic_name}/` recursively.
5. Agent publishes acknowledgement to
   `{global_prefix}/_commands/ack/delete_data/{topic_name}/{node_id}`.
6. Orchestrator collects acks and reports completion status.

Safety:
- Delete command requires the topic to already be in `deleted` state (cannot
  delete data for an active topic).
- Agent logs the deletion with full path for audit trail.
- Idempotent — re-sending delete for already-cleaned data is a no-op.

---

## 5. Developer Experience Improvements

Beyond multi-topic and orchestrator, several areas would reduce friction for
developers building on Mitiflow.

### 5.1 Unified Agent Configuration

**Current pain:** Agent configuration is entirely env-var driven with no
validation, defaults are scattered across builder methods, and topic-level
config is separate from node-level config.

**Decision:** YAML config file with env-var overrides, consistent with the
emulator's existing YAML-based topology configs (`serde_yaml` + `humantime_serde`).

```yaml
# mitiflow-agent.yaml

node:
  id: auto                         # auto = UUID v7
  data_dir: /var/lib/mitiflow
  capacity: 100
  health_interval: 10s
  drain_grace_period: 30s
  labels:
    rack: us-east-1a
    tier: ssd

cluster:
  global_prefix: mitiflow           # configurable, default "mitiflow"
  auto_discover_topics: true        # subscribe to _config/** and auto-serve

# Optional: static topic list (for running without orchestrator)
topics:
  - name: events
    key_prefix: app/events
    num_partitions: 16
    replication_factor: 2
  - name: logs
    key_prefix: app/logs
    num_partitions: 8
    replication_factor: 1
```

This gives three deployment modes:

| Mode | Config | Requires Orchestrator? |
|------|--------|------------------------|
| **Auto-discover** | `auto_discover_topics = true` | Yes |
| **Static topics** | `[[topics]]` list in config file | No |
| **Hybrid** | Both — static topics + discover new ones | Optional |

### 5.2 Unified Binary with Subcommands

**Problem:** Today the workspace produces separate binaries per crate:
`mitiflow-agent`, `mitiflow-orchestrator`, `mitiflow-ctl`,
`mitiflow-emulator`. Users must discover, build, and manage multiple
binaries.

**Decision:** Consolidate all entry points into a single `mitiflow` binary
with subcommands. This mirrors tools like `kubectl`, `nats-server`, and
`docker` where one binary serves multiple roles.

```bash
# Storage agent (multi-topic)
mitiflow agent --config agent.yaml
mitiflow agent                          # defaults: auto-discover, data in /tmp/mitiflow

# Orchestrator (control plane)
mitiflow orchestrator --config orchestrator.yaml

# CLI tooling (admin commands)
mitiflow ctl topics list
mitiflow ctl cluster nodes
mitiflow ctl cluster drain node-1

# Emulator (topology simulation)
mitiflow emulator --topology topology.yaml

# Dev mode (all-in-one: embedded Zenoh + orchestrator + agent + HTTP API)
mitiflow dev --topics events:4:1,logs:2:1
mitiflow dev --config dev.yaml
```

**Implementation:** Single crate (`mitiflow-cli` or top-level binary) using
`clap` subcommands, depending on the library crates. Each subcommand delegates
to the existing crate logic:

```
mitiflow (binary)
├── agent     → mitiflow-agent::StorageAgent
├── orchestrator → mitiflow-orchestrator::Orchestrator
├── ctl       → mitiflow-orchestrator (client-side queries)
├── emulator  → mitiflow-emulator::Emulator
└── dev       → orchestrator + agent + embedded Zenoh in one process
```

The individual crate binaries (`mitiflow-agent/src/main.rs`, etc.) remain
for direct use but the unified binary becomes the recommended entry point.

Dev mode co-locates orchestrator + agent + embedded Zenoh router in one
process, similar to `redpanda --dev-container`:

```bash
mitiflow dev --topics events:4:1,logs:2:1
# Creates:
#   - Embedded Zenoh router (no external Zenoh needed)
#   - Orchestrator with 2 topics
#   - Agent serving all partitions locally
#   - HTTP admin API on :8080
```

### 5.3 Observability Improvements

| Area | Current | Proposed |
|------|---------|----------|
| Metrics | Custom health reporter (Zenoh pub) | OpenTelemetry metrics (Prometheus exporter via `opentelemetry-prometheus`) |
| Tracing | `tracing` crate (log-only) | `tracing-opentelemetry` for distributed traces through pub→store→sub |
| Admin | Zenoh queryable + CLI | HTTP API + optional Web UI (static SPA served by orchestrator) |
| Debugging | Read logs | `mitiflow-ctl diagnose` — checks connectivity, topic health, partition coverage, consumer lag, store status |

### 5.4 Error Messages and Diagnostics

**Proposal:** Add structured error context using `miette` or enhanced
`thiserror` messages. Example current vs proposed:

```
# Current
Error: fjall error: partition locked

# Proposed
Error: Failed to open EventStore for partition 3, replica 0
  ├─ Path: /var/lib/mitiflow/3/0
  ├─ Cause: fjall error: partition locked
  ╰─ Help: Another process may be using this data directory.
           Check if another mitiflow-agent is running with the same data_dir.
```

---

## 6. Unified Architecture Vision

Combining multi-topic agent + provisioning protocol + orchestrator
improvements, the target architecture looks like:

```
                    ┌──────────────────────────┐
                    │     Orchestrator          │
                    │  ┌───────────────────┐    │
                    │  │ ConfigStore(fjall) │    │ ← Topic CRUD
                    │  │ TopicManager      │    │ ← Per-topic ClusterViews
                    │  │ LagMonitor        │    │ ← Consumer group lag
                    │  │ HTTP API (axum)   │    │ ← REST for ops
                    │  │ AlertManager      │    │ ← Health-based alerts
                    │  └───────────────────┘    │
                    └──────────┬───────────────┘
                               │ Zenoh pub/sub
           ┌───────────────────┼───────────────────┐
           ▼                   ▼                   ▼
  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
  │  Agent Node 1   │ │  Agent Node 2   │ │  Agent Node 3   │
  │  ┌───────────┐  │ │  ┌───────────┐  │ │  ┌───────────┐  │
  │  │TopicSuperv│  │ │  │TopicSuperv│  │ │  │TopicSuperv│  │
  │  │ ┌─events──┐│ │ │  │ ┌─events──┐│ │ │  │ ┌─events──┐│ │
  │  │ │Reconciler││ │ │  │ │Reconciler││ │ │  │ │Reconciler││ │
  │  │ │Store×P/R ││ │ │  │ │Store×P/R ││ │ │  │ │Store×P/R ││ │
  │  │ └─────────┘│ │ │  │ └─────────┘│ │ │  │ └─────────┘│ │
  │  │ ┌─logs────┐│ │ │  │ ┌─logs────┐│ │ │  │            │ │
  │  │ │Reconciler││ │ │  │ │Reconciler││ │ │  │(not serving│ │
  │  │ │Store×P/R ││ │ │  │ │Store×P/R ││ │ │  │ logs topic)│ │
  │  │ └─────────┘│ │ │  │ └─────────┘│ │ │  │            │ │
  │  └───────────┘  │ │  └───────────┘  │ │  └───────────┘  │
  │  HealthReporter │ │  HealthReporter │ │  HealthReporter │
  └─────────────────┘ └─────────────────┘ └─────────────────┘
```

### End-to-End Topic Lifecycle

```
1.  Admin: POST /api/v1/topics { name: "events", partitions: 16, rf: 2 }
2.  Orchestrator: ConfigStore.put_topic() → persists
3.  Orchestrator: publish TopicConfig to "mitiflow/_config/events"
4.  Orchestrator: create ClusterView for topic "events"
5.  Agent 1..N: TopicWatcher receives TopicConfig
6.  Agent 1..N: should_serve_topic() checks labels → yes/no
7.  Agent 1..N: spawn TopicWorker("events")
8.  Agent 1..N: TopicWorker declares liveliness, joins HRW ring
9.  Agent 1..N: Reconciler computes assignment, starts EventStores
10. Agent 1..N: StatusReporter publishes partition assignments
11. Orchestrator: ClusterView aggregates — topic "events" is READY
12. Admin: GET /api/v1/topics/events → { status: "ready", coverage: "16/16" }
```

---

## 7. Implementation Roadmap

Ordered by value and dependency. Phases A–F are complete; remaining items are deferred.

### Phase A: Multi-Topic Agent Foundation ✅

1. Extract `TopicWorker` from `StorageAgent` internals (refactor, no behavior
   change).
2. Add `TopicSupervisor` that manages `HashMap<String, TopicWorker>` with
   `add_topic()` / `remove_topic()`.
3. Update `StorageAgentConfig` to accept `Vec<TopicConfig>` for static
   multi-topic — backward compatible (single topic = vec of 1).
4. Update `main.rs` to support YAML config file with `topics:` list.
5. Update existing tests to use `TopicWorker` directly.
6. Separate fjall instance per topic: data path
   `{data_dir}/{topic_name}/{partition}/{replica}/`.

### Phase B: Topic Provisioning Protocol ✅

1. Add `TopicWatcher` to agent — subscribes to `{global_prefix}/_config/**`.
2. Extend orchestrator `ConfigStore` with Zenoh queryable for `_config/**`
   so agents can discover topics on join.
3. Add placement labels to `TopicConfig` (`required_labels`,
   `excluded_labels`).
4. Implement `should_serve_topic()` label matching on agent side.
5. Wire `TopicWatcher` → `TopicSupervisor` → dynamic `TopicWorker` creation.
6. Add topic data deletion command protocol (§4.2.4). (deferred)
7. E2E tests: TopicWatcher reacts to config events, filters by labels,
   discovers existing topics on startup.

### Phase C: Unified Binary & Config ✅

1. Create `mitiflow-cli` crate with `clap` subcommands.
2. Wire subcommands: `agent`, `orchestrator`, `ctl`.
3. YAML config file support for agent and orchestrator (`serde_yaml` +
   `humantime_serde`, consistent with emulator).
4. Configurable `global_prefix` (default `"mitiflow"`) with env-var
   override `MITIFLOW_GLOBAL_PREFIX`.
5. `mitiflow dev` mode: co-locate orchestrator + agent + embedded Zenoh.
   (deferred)

### Phase D: Orchestrator HTTP API ✅

1. Added `axum` dependency to `mitiflow-orchestrator`.
2. Exposed REST endpoints mirroring existing Zenoh queryable.
3. `DELETE /api/v1/topics/{name}/data` for topic data deletion. (deferred)
4. Topic readiness and partition coverage status. (deferred)
5. Swagger/OpenAPI spec generation. (deferred)

### Phase E: Dev Experience Polish ✅

1. Better error messages with diagnostic context (`miette`) — all
   `mitiflow::Error` and `AgentError` variants annotated.
2. `mitiflow ctl diagnose` health check command — Zenoh connectivity,
   topic discovery, agent liveliness, orchestrator admin queryable.
3. OpenTelemetry metrics export. (deferred)

### Phase F: Proactive Orchestrator (partially done)

1. Under-replicated partition alerts ✅ — `AlertManager` in
   `mitiflow-orchestrator/src/alert_manager.rs`.
2. Node offline detection ✅ — warning → critical escalation after grace period.
3. Auto-drain on node failure. (deferred)
4. Rebalance advisor / auto-rebalance. (deferred)
5. Orchestrator HA (leader election via liveliness). (deferred)

---

## 8. Resolved Decisions

Decisions made during design review:

1. **Global prefix:** Configurable with default `"mitiflow"`. Set via
   `cluster.global_prefix` in YAML config or `MITIFLOW_GLOBAL_PREFIX` env var.

2. **Topic deletion semantics:** Two-step: `delete_topic()` stops serving
   (data remains), then explicit `DELETE /api/v1/topics/{name}/data` triggers
   on-disk cleanup across agents. See §4.2.4.

3. **Config format:** YAML, consistent with the emulator's topology configs.
   Parsed via `serde_yaml` + `humantime_serde`. Env vars override YAML values.

4. **Binary distribution:** Unified `mitiflow` binary with subcommands
   (`agent`, `orchestrator`, `ctl`, `emulator`, `dev`). See §5.2.

5. **Resource isolation:** Separate fjall instances per topic (one DB per
   topic per node). Simpler failure isolation, independent retention/compaction,
   clean deletion. Path: `{data_dir}/{topic_name}/{partition}/{replica}/`.

6. **Orchestrator HA:** Deferred. The system works without orchestrator
   (static topics in YAML config). Dynamic topic creation requires a
   reachable orchestrator, but HA is not a prerequisite for the multi-topic
   agent work.

## 9. Remaining Open Questions

1. **Agent topic limits:** Should agents have a configurable max-topics limit
   to prevent resource exhaustion? Or rely on placement labels to control
   distribution?

2. **Topic update semantics:** Can `num_partitions` be increased for a live
   topic? Partition expansion is straightforward (new partitions, no data
   movement), but shrinking is destructive. Should the API allow only
   expansion?

3. **Cross-topic queries:** Should the orchestrator support querying lag or
   status across all topics in one call? Or always per-topic?

4. **Unified binary build size:** The unified binary pulls in all crate
   dependencies. Should subcommands be feature-gated to allow minimal builds
   (e.g., `--features agent-only`)?
