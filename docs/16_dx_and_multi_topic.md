# Multi-Topic Agent & Developer Experience

**Status:** Implemented (Phases A–F complete, some items deferred).

How mitiflow evolved from one-agent-per-topic to a multi-topic broker-like
deployment model with dynamic topic discovery and a unified CLI.

---

## Architecture

One agent process per node, serving any number of topics dynamically.

```
┌─── mitiflow-storage process ──────────────────────────────────┐
│  AgentConfig { node_id, data_dir, capacity, labels }        │
│                                                             │
│  TopicSupervisor                                            │
│    ├── TopicWorker("events", key_prefix="app/events")       │
│    │     ├── MembershipTracker (liveliness per topic)        │
│    │     ├── Reconciler → EventStore × P×RF                 │
│    │     ├── RecoveryManager                                │
│    │     └── StatusReporter                                 │
│    ├── TopicWorker("logs", key_prefix="app/logs")           │
│    │     └── ...                                            │
│    └── TopicWatcher                                         │
│          └── subscribes to orchestrator _config/**          │
│                                                             │
│  Shared: HealthReporter (node-level)                        │
└─────────────────────────────────────────────────────────────┘
```

**Design choice:** Option A (per-topic workers) — each topic gets its own
`Reconciler`, `MembershipTracker`, `RecoveryManager`. Provides fault isolation
and natural per-topic membership via existing liveliness patterns.

---

## Topic Provisioning Protocol

Agents subscribe to `{global_prefix}/_config/**`. When the orchestrator
publishes a new `TopicConfig`, agents filter by placement labels
(`required_labels` / `excluded_labels`) and spawn `TopicWorker` instances.

Late-joining agents query the orchestrator's `_config/**` queryable on startup.

**Three deployment modes:**

| Mode | Config | Orchestrator required? |
|------|--------|------------------------|
| Auto-discover | `auto_discover_topics: true` | Yes |
| Static topics | `topics:` list in YAML | No |
| Hybrid | Both | Optional |

---

## Unified Binary

Single `mitiflow` binary with `clap` subcommands:

```bash
mitiflow agent --config agent.yaml
mitiflow orchestrator --config orchestrator.yaml
mitiflow ctl topics list
mitiflow ctl cluster drain node-1
mitiflow ctl diagnose
```

Individual crate binaries remain for direct use.

---

## YAML Configuration

Agent config (`serde_yaml` + `humantime_serde`):

```yaml
node:
  id: auto
  data_dir: /var/lib/mitiflow
  capacity: 100
  health_interval: 10s
  labels:
    rack: us-east-1a
    tier: ssd

cluster:
  global_prefix: mitiflow
  auto_discover_topics: true

topics:   # optional static list
  - name: events
    key_prefix: app/events
    num_partitions: 16
    replication_factor: 2
```

---

## Orchestrator HTTP API

Embedded axum server mirroring the Zenoh queryable:

| Method | Endpoint | Purpose |
|--------|----------|---------|
| GET/POST | `/api/v1/topics` | List / create topics |
| GET/PUT/DELETE | `/api/v1/topics/{name}` | Topic CRUD |
| GET | `/api/v1/cluster/nodes` | Online nodes |
| GET | `/api/v1/cluster/status` | Cluster status |
| POST/DELETE | `/api/v1/cluster/nodes/{id}/drain` | Drain / undrain |
| GET/POST/DELETE | `/api/v1/cluster/overrides` | Override management |
| GET | `/api/v1/consumer-groups` | List groups |
| GET | `/api/v1/consumer-groups/{id}` | Group detail |
| POST | `/api/v1/consumer-groups/{id}/reset` | Reset offsets |
| GET | `/api/v1/events` | Event browsing |
| SSE | `/api/v1/stream/cluster` | Live cluster events |
| SSE | `/api/v1/stream/lag` | Live lag reports |
| SSE | `/api/v1/stream/events` | Live event tail |
| GET | `/api/v1/health` | Health check |

---

## Key Decisions

1. **Separate fjall per topic** — `{data_dir}/{topic_name}/{partition}/{replica}/` for failure isolation, independent retention, clean deletion.
2. **Global prefix** — configurable (default `"mitiflow"`), set via YAML or `MITIFLOW_GLOBAL_PREFIX`.
3. **Error diagnostics** — `miette` annotations with `#[diagnostic(code, help)]` on all error variants.
4. **Alert manager** — detects under-replicated partitions and offline nodes.

---

## Deferred Items

- `mitiflow dev` — co-locate orchestrator + agent + Zenoh in one process
- OpenTelemetry integration — distributed traces and Prometheus metrics
- Auto-drain on node failure
- Rebalance advisor / auto-rebalance
- Orchestrator HA — leader election via liveliness
- Topic data deletion command protocol
