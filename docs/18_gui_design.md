# Mitiflow GUI — Design Deep Dive

> **Status:** Active design — Option A selected  
> **Date:** 2026-03-29  
> **Decision:** Embedded SPA served from the orchestrator (axum). Read-write. TUI (Option D) deferred.

---

## Decision Record

| Question | Decision |
|----------|----------|
| UI approach | **Option A: Embedded Web SPA** served from orchestrator's axum server |
| Primary user | **Developers** (debugging event flows, inspecting gaps, browsing stored events) |
| Secondary user | **Platform engineers** (topic config review, partition map, node health) |
| Deferred persona | SRE monitoring (alerting, dashboards, runbooks — adds too much complexity now) |
| Hosting | **Part of the orchestrator** — same binary, same port |
| Auth | **Static bearer token** (optional, disabled by default for local dev) |
| Cluster scope | **Single cluster** — one orchestrator, one UI |
| Write operations | **Read-write** — topic CRUD, node drain/undrain, consumer group offset reset, override management |
| Real-time transport | **SSE** (Server-Sent Events) for server→client push; fallback to polling |

---

## 1. Architecture Overview

```
┌──────────────────────────────────────────────────────────┐
│                   mitiflow orchestrator                   │
│                                                          │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────┐  │
│  │ ConfigStore   │  │ ClusterView  │  │  LagMonitor   │  │
│  │  (fjall DB)   │  │ (node state) │  │ (watermarks)  │  │
│  └──────┬───────┘  └──────┬───────┘  └──────┬────────┘  │
│         │                 │                  │           │
│  ┌──────▼─────────────────▼──────────────────▼────────┐  │
│  │                   axum Router                       │  │
│  │                                                     │  │
│  │  /api/v1/*            REST JSON endpoints           │  │
│  │  /api/v1/stream/*     SSE streams                   │  │
│  │  /ui/*                Static SPA assets             │  │
│  │                       (rust-embed)                  │  │
│  └──────────────────────┬──────────────────────────────┘  │
│                         │                                │
│  ┌──────────────────────▼──────────────────────────────┐  │
│  │  Optional: Bearer token middleware (tower layer)    │  │
│  └─────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
          │ HTTP :8080 (configurable)
          ▼
┌──────────────────────┐
│  Browser (SPA)       │
│  Svelte 5 + Vite     │
│  fetch → /api/v1/*   │
│  EventSource → SSE   │
└──────────────────────┘
```

**Key design choice:** The SPA is served from the **same axum server** that already hosts the REST API. No CORS, no separate process, no proxy. The `rust-embed` crate compiles the built frontend assets into the orchestrator binary at `cargo build` time.

---

## 2. Tech Stack

### Backend (additions to orchestrator)

| Component | Crate / Approach | Notes |
|-----------|-----------------|-------|
| Static file serving | `rust-embed` + axum fallback route | Serves `index.html`, JS, CSS from embedded bytes |
| SSE streaming | `axum::response::sse::Sse` | Native axum SSE support, no extra dependency |
| Auth middleware | `tower_http::validate_request::ValidateRequestHeaderLayer` | Static bearer token from config/env var |
| JSON serialization | `serde_json` (already present) | All API types already derive `Serialize` |

### Frontend

| Layer | Choice | Rationale |
|-------|--------|-----------|
| Framework | **Svelte 5** | Tiny bundles (~15 KB gzip for framework), compiler-based reactivity, excellent DX |
| Build tool | **Vite** | Fast HMR in dev, optimized chunks in prod |
| Routing | **svelte-spa-router** or hash-based | SPA routing without SSR; avoids catch-all server config |
| CSS | **Tailwind CSS 4** | Utility classes, no custom design system needed |
| Charts | **uPlot** | 35 KB, high-perf time-series rendering, good for lag/throughput sparklines |
| Tables | **@tanstack/svelte-table** | Virtual scrolling, sorting, filtering, column resize |
| Icons | **Lucide** (tree-shakeable) | Clean icon set, only bundle what's used |
| HTTP | Native `fetch` + thin typed wrapper | Zero-dep, matches API response types |
| SSE | Native `EventSource` | Browser-native, auto-reconnect |

### Why Svelte over React/Vue?

1. **Bundle size** — Svelte compiles away the framework. A full admin dashboard will produce ~80–150 KB gzip total (vs. ~250 KB+ for React + router + state). Smaller = faster embedding.
2. **No virtual DOM overhead** — Direct DOM updates match the "lots of small real-time updates" pattern (lag ticking, node status changes).
3. **Minimal boilerplate** — Single-file components with `$state` runes. Less code to maintain.
4. **Good enough ecosystem** — TanStack Table, uPlot, and Tailwind all support Svelte.

---

## 3. Page & Component Design

### 3.1 Navigation Structure (Sidebar)

```
📊  Dashboard              ← landing page
📦  Topics
    └─ {topic-name}        ← topic detail (drill-in)
🖥️  Nodes
    └─ {node-id}           ← node detail (drill-in)
👥  Consumer Groups
    └─ {group-id}          ← group detail (drill-in)
🔍  Event Inspector        ← developer primary tool
⚠️  Dead Letter Queue
ℹ️  About / Config
```

### 3.2 Dashboard Page (Landing)

**Purpose:** At-a-glance cluster health for developer orientation — "is my local cluster working?"

```
┌─────────────────────────────────────────────────────┐
│  Mitiflow                           cluster: local  │
├──────────┬──────────┬──────────┬────────────────────┤
│ 🟢 3/3   │ 📦 5     │ 🗂️ 48    │ ⚠️ 2 groups       │
│ nodes    │ topics   │ partns   │  lagging           │
├──────────┴──────────┴──────────┴────────────────────┤
│                                                     │
│  Node Health                                        │
│  ┌──────────┬────────┬──────┬──────┬──────┐        │
│  │ Node     │ Status │ CPU  │ Disk │ Parts│        │
│  ├──────────┼────────┼──────┼──────┼──────┤        │
│  │ agent-01 │ 🟢     │  12% │ 2.1G │   16 │        │
│  │ agent-02 │ 🟢     │   8% │ 1.8G │   16 │        │
│  │ agent-03 │ 🟢     │  15% │ 2.4G │   16 │        │
│  └──────────┴────────┴──────┴──────┴──────┘        │
│                                                     │
│  Lag Summary (live via SSE)                         │
│  ┌──────────────┬───────┬──────────┬──────────┐    │
│  │ Group        │ Topic │ Total    │ Trend    │    │
│  ├──────────────┼───────┼──────────┼──────────┤    │
│  │ order-svc    │ orders│    1,204 │ ▂▃▅▇    │    │
│  │ analytics    │ clicks│        0 │ ▁▁▁▁    │    │
│  └──────────────┴───────┴──────────┴──────────┘    │
│                                                     │
│  Recent Events (live via SSE — last 20)             │
│  10:23:45.123  orders/p/3  seq=4521  pub=a3f2..    │
│  10:23:45.119  orders/p/1  seq=8832  pub=a3f2..    │
│  10:23:45.102  clicks/p/0  seq=102   pub=7bc1..    │
│  ...                                                │
└─────────────────────────────────────────────────────┘
```

**Data sources:**
- Summary cards → `GET /api/v1/cluster/status` + `GET /api/v1/topics` (polled every 5s)
- Node table → `SSE /api/v1/stream/cluster` (push on change)
- Lag table → `SSE /api/v1/stream/lag` (push every lag publish interval)
- Recent events → `SSE /api/v1/stream/events` (live tail, limited to most recent N)

### 3.3 Topic Detail Page

**Purpose:** Developer drills in to understand a specific topic's partition layout and publisher activity.

```
┌─────────────────────────────────────────────────────┐
│  Topic: orders                                      │
├─────────────────────────────────────────────────────┤
│  Config                                             │
│  Key prefix: myapp/orders    Partitions: 16         │
│  Replication: 3              Retention: 7d / 10 GB  │
│  Compaction: enabled (1h)                           │
├─────────────────────────────────────────────────────┤
│  Partition Map                                      │
│  ┌────┬───────────────────────────┬────────┬──────┐│
│  │ ID │ Replicas                  │ State  │Events││
│  ├────┼───────────────────────────┼────────┼──────┤│
│  │  0 │ agent-01(L) agent-02 ·-03│ Active │12.4K ││
│  │  1 │ agent-02(L) agent-03 ·-01│ Active │11.9K ││
│  │  2 │ agent-03(L) agent-01 ·-02│ Active │13.1K ││
│  │ .. │ ...                       │        │      ││
│  └────┴───────────────────────────┴────────┴──────┘│
│                                                     │
│  Active Publishers                                  │
│  ┌──────────────┬─────────────┬──────────┐         │
│  │ Publisher ID  │ Last Seq    │ Last Seen│         │
│  ├──────────────┼─────────────┼──────────┤         │
│  │ a3f2..bc01   │ p0:4521 …   │ 2s ago  │         │
│  │ 7bc1..ef23   │ p0:102  …   │ 5s ago  │         │
│  └──────────────┴─────────────┴──────────┘         │
│                                                     │
│  Consumer Group Lag (for this topic)                │
│  ┌──────────────┬──────┬──────────────────┐        │
│  │ Group        │ Lag  │ Per-Partition     │        │
│  │ order-svc    │ 1204 │ ▁▃▅▇▂▁▁▁▁▁▁▂▁▁▁▁│        │
│  │ analytics    │    0 │ ▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁│        │
│  └──────────────┴──────┴──────────────────┘        │
└─────────────────────────────────────────────────────┘
```

**Data sources:**
- Config → `GET /api/v1/topics/{name}`
- Partition map → `GET /api/v1/topics/{name}/partitions` (new — derived from `ClusterView::assignments()`)
- Publishers → `GET /api/v1/topics/{name}/publishers` (new — derived from watermark data)
- Lag per group → `GET /api/v1/topics/{name}/lag` (new — filtered from `LagMonitor`)

### 3.4 Event Inspector Page (Developer Primary Tool)

This is the **most important page for the developer persona**. It provides a way to browse, search, and inspect individual events stored in the system.

```
┌─────────────────────────────────────────────────────────┐
│  Event Inspector                                        │
├─────────────────────────────────────────────────────────┤
│  Filters:                                               │
│  Topic: [orders     ▼]  Partition: [All ▼]  Key: [    ]│
│  Publisher: [All               ▼]                       │
│  Time range: [2026-03-29 10:00] → [2026-03-29 10:30]   │
│  Seq range:  [4500           ] → [4600              ]   │
│  [Apply]                                                │
├─────────────────────────────────────────────────────────┤
│  Results (87 events)                          Page 1/5  │
│  ┌──────┬────┬──────┬────────────┬──────┬────────────┐ │
│  │ Seq  │ P  │ Key  │ Publisher  │ Time │ Size       │ │
│  ├──────┼────┼──────┼────────────┼──────┼────────────┤ │
│  │ 4521 │  3 │ ORD-…│ a3f2..bc01 │10:23 │ 1.2 KB     │ │
│  │ 4520 │  3 │ ORD-…│ a3f2..bc01 │10:23 │ 842 B      │ │
│  │ 4519 │  3 │      │ a3f2..bc01 │10:23 │ 256 B      │ │
│  └──────┴────┴──────┴────────────┴──────┴────────────┘ │
│                                                         │
│  ▼ Event Detail (click to expand)                       │
│  ┌─────────────────────────────────────────────────┐   │
│  │ Event ID:    f47ac10b-58cc-4372-a567-0e02b2c3d479│  │
│  │ Sequence:    4521                                │   │
│  │ Partition:   3                                   │   │
│  │ Publisher:   a3f2bc01-7d8e-4f9a-b123-456789abcdef│  │
│  │ Timestamp:   2026-03-29T10:23:45.123456Z        │   │
│  │ Key:         ORD-2026-001234                     │   │
│  │ Key Expr:    myapp/orders/p/3/k/ORD-2026-001234 │   │
│  │ HLC:         1711699425123456789 / 0             │   │
│  │ Urgency:     100ms                              │   │
│  │ Size:        1,247 bytes                        │   │
│  ├─────────────────────────────────────────────────┤   │
│  │ Payload (JSON, auto-detected):                  │   │
│  │ {                                               │   │
│  │   "order_id": "ORD-2026-001234",               │   │
│  │   "customer": "alice@example.com",              │   │
│  │   "items": [                                    │   │
│  │     { "sku": "WIDGET-A", "qty": 3 }            │   │
│  │   ],                                            │   │
│  │   "total": 42.50                                │   │
│  │ }                                               │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

**Data source:** `GET /api/v1/events?topic={name}&partition={p}&after_seq=...&before_seq=...&after_time=...&before_time=...&publisher_id=...&key=...&limit=50`

This is a **new endpoint** that must query the EventStore backends for the requested topic's partitions. The orchestrator doesn't own stores today — see Section 5 for how to bridge this.

**Payload rendering:**
- Try JSON parse first → pretty-print with syntax highlighting
- Try MessagePack decode → show as JSON
- Fallback → hex dump + UTF-8 attempt
- Show raw bytes toggle for all modes

### 3.5 Consumer Group Detail Page

```
┌─────────────────────────────────────────────────────┐
│  Consumer Group: order-svc                          │
├─────────────────────────────────────────────────────┤
│  Members: 3    Generation: 12    Total Lag: 1,204   │
├─────────────────────────────────────────────────────┤
│  Members                                            │
│  ┌──────────────┬───────────────────┬───────┐      │
│  │ Member       │ Partitions        │ State │      │
│  ├──────────────┼───────────────────┼───────┤      │
│  │ worker-01    │ 0,1,2,3,4,5       │ 🟢    │      │
│  │ worker-02    │ 6,7,8,9,10        │ 🟢    │      │
│  │ worker-03    │ 11,12,13,14,15    │ 🟢    │      │
│  └──────────────┴───────────────────┴───────┘      │
│                                                     │
│  Per-Partition Lag (live)                            │
│  ┌────┬──────────────────────────────────┬────────┐│
│  │ P  │ Publisher Lag Detail             │ Total  ││
│  ├────┼──────────────────────────────────┼────────┤│
│  │  0 │ a3f2→742  7bc1→0                │    742 ││
│  │  1 │ a3f2→0    7bc1→0                │      0 ││
│  │  2 │ a3f2→310  7bc1→152              │    462 ││
│  │ .. │ ...                              │        ││
│  └────┴──────────────────────────────────┴────────┘│
│                                                     │
│  Lag Trend (last 5 minutes, live)                   │
│  1500 ┤                                             │
│  1000 ┤          ╭─╮                                │
│   500 ┤     ╭────╯ ╰──╮                            │
│     0 ┤─────╯         ╰────                        │
│       └──────────────────────                       │
└─────────────────────────────────────────────────────┘
```

**Data sources:**
- Group metadata → `GET /api/v1/consumer-groups/{id}` (new — assembled from offset commits + liveliness)
- Lag detail → `SSE /api/v1/stream/lag?group={id}` (filtered lag stream)
- Lag trend → Client-side ring buffer of last N lag snapshots from SSE

### 3.6 Node Detail Page

```
┌─────────────────────────────────────────────────────┐
│  Node: agent-01                                     │
├─────────────────────────────────────────────────────┤
│  Status: 🟢 Online     Since: 2026-03-29 08:00:12  │
│  Labels: region=us-east, tier=standard              │
├─────────────────────────────────────────────────────┤
│  Health Metrics (live)                              │
│  CPU: 12%   Disk: 2.1 GB   Events: 142,891         │
│  Store p99: 45 µs   Errors: 0                      │
├─────────────────────────────────────────────────────┤
│  Partition Assignments                              │
│  ┌───────┬────┬─────────┬──────────┬───────────┐   │
│  │ Topic │ P  │ Replica │ State    │ Events    │   │
│  ├───────┼────┼─────────┼──────────┼───────────┤   │
│  │ orders│  0 │ 0 (L)   │ Active   │ 12,481    │   │
│  │ orders│  3 │ 1       │ Active   │ 12,091    │   │
│  │ clicks│  0 │ 0 (L)   │ Active   │  8,312    │   │
│  │ clicks│  5 │ 2       │ Recovering│ 3,102    │   │
│  └───────┴────┴─────────┴──────────┴───────────┘   │
└─────────────────────────────────────────────────────┘
```

### 3.7 DLQ Page

```
┌─────────────────────────────────────────────────────┐
│  Dead Letter Queue                                  │
├─────────────────────────────────────────────────────┤
│  Topic: [orders ▼]   Status: [All ▼]               │
│                                                     │
│  ┌──────────────┬────┬──────────┬─────────┬────────┐│
│  │ Event ID     │ P  │ Attempts │ Last Err│ Time   ││
│  ├──────────────┼────┼──────────┼─────────┼────────┤│
│  │ f47a..c3d4   │  3 │ 3/3      │ timeout │ 10:20  ││
│  │ 8b21..a9e7   │  1 │ 3/3      │ decode  │ 10:18  ││
│  └──────────────┴────┴──────────┴─────────┴────────┘│
│                                                     │
│  ▼ Event Detail (expand)                            │
│  Original payload + error stack trace               │
└─────────────────────────────────────────────────────┘
```

---

## 4. API Design

All endpoints extend the existing axum router. Response format is JSON. Errors use standard HTTP status codes with `{ "error": "message" }` body.

Write operations require confirmation in the UI (modal dialogs) to prevent accidental mutations. All write endpoints are idempotent where possible.

### 4.1 Existing Endpoints (unchanged)

```
GET  /api/v1/health                    → 200 OK
GET  /api/v1/topics                    → TopicConfig[]
GET  /api/v1/topics/{name}             → TopicConfig
GET  /api/v1/cluster/nodes             → Map<String, NodeInfo>
GET  /api/v1/cluster/status            → ClusterStatus
```

### 4.2 New REST Endpoints

#### Topic partitions

```
GET /api/v1/topics/{name}/partitions
```

Response: partition assignments for the given topic, assembled from `ClusterView::assignments()` filtered by topic.

```json
[
  {
    "partition": 0,
    "replicas": [
      { "replica": 0, "node_id": "agent-01", "state": "Active", "source": "Computed" },
      { "replica": 1, "node_id": "agent-02", "state": "Active", "source": "Computed" }
    ],
    "event_count": 12481,
    "publishers": {
      "a3f2bc01-...": { "committed_seq": 4521, "gaps": [] }
    }
  }
]
```

**Implementation:** Orchestrator already has `ClusterView::assignments()` and `TopicManager::get_view(topic)`. The handler iterates assignments, groups by partition, and augments with watermark data from `LagMonitor`.

#### Topic lag

```
GET /api/v1/topics/{name}/lag
```

Response: lag per consumer group for this topic.

```json
[
  {
    "group_id": "order-svc",
    "partitions": [
      { "partition": 0, "publishers": { "a3f2..": 742 }, "total": 742 },
      { "partition": 1, "publishers": { "a3f2..": 0 }, "total": 0 }
    ],
    "total_lag": 1204
  }
]
```

**Implementation:** `LagMonitor::get_group_lag()` already returns `Vec<LagReport>`. The handler queries all known group IDs and filters to the given topic's partition range.

#### Topic publishers

```
GET /api/v1/topics/{name}/publishers
```

Response: active publishers derived from watermark data.

```json
[
  {
    "publisher_id": "a3f2bc01-...",
    "partitions": {
      "0": { "committed_seq": 4521, "gaps": [] },
      "3": { "committed_seq": 1892, "gaps": [1890] }
    },
    "last_seen": "2026-03-29T10:23:45Z"
  }
]
```

#### Consumer groups

```
GET /api/v1/consumer-groups
```

Response: list of all known consumer groups (discovered from offset commits observed by LagMonitor).

```json
[
  {
    "group_id": "order-svc",
    "partition_count": 16,
    "total_lag": 1204,
    "last_commit": "2026-03-29T10:23:40Z"
  }
]
```

**Implementation note:** The orchestrator's `LagMonitor` subscribes to `_offsets/**`. It already tracks offsets per `(group_id, partition, publisher_id)`. A new method `LagMonitor::known_groups() → Vec<String>` returns the distinct group IDs observed.

```
GET /api/v1/consumer-groups/{id}
```

Response: detailed group state including per-partition lag and known members.

```json
{
  "group_id": "order-svc",
  "members": [
    { "member_id": "worker-01", "partitions": [0,1,2,3,4,5] },
    { "member_id": "worker-02", "partitions": [6,7,8,9,10] }
  ],
  "lag": [
    { "partition": 0, "publishers": { "a3f2..": 742 }, "total": 742 },
    { "partition": 1, "publishers": {}, "total": 0 }
  ],
  "total_lag": 1204,
  "last_commit": "2026-03-29T10:23:40Z"
}
```

**Implementation note — member discovery:** Consumer group members are visible via Zenoh liveliness tokens on `{key_prefix}/_workers/{group_id}/*`. The orchestrator would need to subscribe to these tokens (similar to how `ClusterView` watches `_agents/*`). This is a moderate new feature. **Alternative for v1:** omit member list and show only lag data (which is already available). Mark member list as "v1.1".

#### Event browsing (Query-through)

```
GET /api/v1/events?topic={name}&partition={p}&after_seq={n}&before_seq={n}&after_time={iso}&before_time={iso}&publisher_id={uuid}&key={key}&limit={n}
```

Response: events from the store, with metadata and payload.

```json
{
  "events": [
    {
      "seq": 4521,
      "partition": 3,
      "publisher_id": "a3f2bc01-...",
      "event_id": "f47ac10b-...",
      "timestamp": "2026-03-29T10:23:45.123Z",
      "key": "ORD-2026-001234",
      "key_expr": "myapp/orders/p/3/k/ORD-2026-001234",
      "hlc": { "physical_ns": 1711699425123456789, "logical": 0 },
      "payload_size": 1247,
      "payload_base64": "eyJvcmRlcl9pZCI...",
      "payload_text": "{\"order_id\": ...}"
    }
  ],
  "total": 87,
  "has_more": true
}
```

**Implementation — this is the hardest endpoint.** The orchestrator does NOT own the EventStore backends. Stores live on agent nodes. Two approaches:

**Approach A: Zenoh query-through.** The orchestrator issues a `session.get()` to the store's queryable (`{key_prefix}/_store/{partition}?after_seq=...`) and relays results to HTTP. This is how subscribers already recover events — the mechanism exists. The orchestrator just acts as an HTTP→Zenoh proxy.

- Pros: No new storage on orchestrator, uses existing store queryables
- Cons: Latency of Zenoh round-trip added to HTTP; store must be online; results are partition-scoped (multi-partition queries need fan-out)

**Approach B: Orchestrator subscribes to events and keeps a small ring buffer.** Only for "recent events tail" use case on the dashboard. Does NOT replace query-through for the event inspector.

**Recommendation for v1:** Use approach A (Zenoh query-through) for the Event Inspector. Use approach B (ring buffer) only for the dashboard's "recent events" widget.

#### DLQ listing

```
GET /api/v1/dlq?topic={name}&limit={n}
```

**Implementation note:** DLQ events are published to `{dlq_key_prefix}/{event_id}`. The orchestrator would need to subscribe to DLQ topics and maintain a recent buffer. For v1, this can be a small in-memory ring buffer of the last N DLQ events per topic. **Mark as v1.1** if scope needs trimming.

### 4.3 Write Endpoints

These endpoints expose the orchestrator's existing write methods to the UI.
The underlying `Orchestrator` methods already exist and are tested—the HTTP
handlers are thin JSON wrappers with input validation.

#### Topic CRUD

```
POST /api/v1/topics                    (already exists)
DELETE /api/v1/topics/{name}           (already exists)
PUT  /api/v1/topics/{name}             (new — update topic config)
```

`POST` and `DELETE` already exist in the HTTP router but call only `ConfigStore`
directly. They must be upgraded to call `Orchestrator::create_topic()` and
`Orchestrator::delete_topic()` instead, so that Zenoh config distribution and
per-topic `ClusterView` lifecycle are triggered.

**`PUT /api/v1/topics/{name}`** — Update mutable fields (retention, compaction,
labels, replication factor). Immutable fields (name, key_prefix,
num_partitions) return 400 if the caller tries to change them.

Request body (partial update — only supplied fields are changed):

```json
{
  "replication_factor": 3,
  "retention": { "max_age": "14d", "max_bytes": 10737418240 },
  "compaction": { "enabled": true, "interval": "30m" },
  "required_labels": { "region": "us-east" },
  "excluded_labels": {}
}
```

Response: the full updated `TopicConfig`.

**Implementation:** Read existing config, merge fields, call
`Orchestrator::create_topic()` (upsert semantics in `ConfigStore::put_topic`).

#### Node drain / undrain

```
POST /api/v1/cluster/nodes/{id}/drain      (new)
POST /api/v1/cluster/nodes/{id}/undrain    (new)
```

**Drain** computes override entries to evacuate all partitions from the given
node and publishes them via `OverrideManager`. Returns the list of generated
override entries.

Request body (optional):

```json
{ "replication_factor": 3 }
```

Response:

```json
{
  "node_id": "agent-03",
  "overrides": [
    { "partition": 5, "replica": 0, "node_id": "agent-01", "reason": "drain agent-03" }
  ]
}
```

**Undrain** removes drain overrides for the node so HRW re-assigns partitions
naturally. Returns 204 No Content.

**Implementation:** Delegates to `Orchestrator::drain_node()` and
`Orchestrator::undrain_node()` which already exist and are tested.

#### Override management

```
GET    /api/v1/cluster/overrides           (new — read current table)
POST   /api/v1/cluster/overrides           (new — add override entries)
DELETE /api/v1/cluster/overrides           (new — clear all overrides)
```

**`GET`** returns the current `OverrideTable` (entries + epoch + expiry).

**`POST`** adds entries to the override table:

```json
{
  "entries": [
    { "partition": 3, "replica": 0, "node_id": "agent-02", "reason": "manual placement" }
  ],
  "ttl_seconds": 3600
}
```

**`DELETE`** clears all overrides (epoch is incremented).

**Implementation:** Delegates to `OverrideManager::add_entries()`,
`OverrideManager::current()`, and `OverrideManager::clear()`.

#### Consumer group offset reset

```
POST /api/v1/consumer-groups/{id}/reset    (new)
```

Resets committed offsets for a consumer group to a specified position.

Request body:

```json
{
  "topic": "orders",
  "partition": 0,
  "strategy": "earliest" | "latest" | { "to_seq": 4500 } | { "to_time": "2026-03-29T10:00:00Z" }
}
```

Response: 200 with the new offset positions.

**Implementation:** The orchestrator publishes an `OffsetCommit` message to
`{key_prefix}/_offsets/{partition}/{group_id}` with the target sequence.
For `earliest`, seq = 0. For `latest`, seq = current watermark. For `to_seq`,
use the provided value. For `to_time`, query the store for the seq at that
timestamp.

**Safety:** The request must include `topic` and `partition` to prevent
accidental cluster-wide resets. The UI shows a confirmation dialog with
current vs. target offset positions before submitting.

### 4.4 SSE Streaming Endpoints

### 4.4 SSE Streaming Endpoints

SSE endpoints emit newline-delimited JSON events. The client uses `EventSource` (browser-native API). Each event has a `type` field for client-side routing.

#### Cluster stream

```
GET /api/v1/stream/cluster
```

Emits events whenever the ClusterView state changes:

```
event: node_online
data: {"node_id":"agent-01","timestamp":"..."}

event: node_offline
data: {"node_id":"agent-03","timestamp":"..."}

event: node_health
data: {"node_id":"agent-01","partitions_owned":16,"disk_usage_bytes":2147483648,...}

event: partition_state
data: {"partition":5,"node_id":"agent-03","state":"Recovering"}
```

**Implementation:** The orchestrator's `ClusterView` already watches liveliness tokens and subscribes to `_cluster/status/*` and `_cluster/health/*`. A new SSE handler:
1. Takes a snapshot of all current nodes and emits them as initial events
2. Spawns a background task that watches a `tokio::sync::broadcast` channel
3. The `ClusterView` publishes change events to this broadcast channel
4. The SSE handler forwards events to the HTTP response stream

#### Lag stream

```
GET /api/v1/stream/lag?group={group_id}   (optional filter)
```

Emits lag updates as they are computed:

```
event: lag
data: {"group_id":"order-svc","partition":0,"publishers":{"a3f2..":742},"total":742,"timestamp":"..."}
```

**Implementation:** The `LagMonitor` already computes and publishes lag to Zenoh. Add a `broadcast::Sender<LagReport>` to `LagMonitor` that fires alongside the Zenoh put. SSE handler subscribes and optionally filters by `group_id`.

#### Event tail stream

```
GET /api/v1/stream/events?topic={name}&partition={p}  (optional filters)
```

Emits live events as they arrive (recent tail, not historical):

```
event: message
data: {"seq":4521,"partition":3,"publisher_id":"a3f2..","key":"ORD-...","timestamp":"...","payload_size":1247}
```

**Implementation:** The orchestrator subscribes to `{key_prefix}/p/**` (or filtered partition). Events are decoded from the 50-byte attachment header (no payload deserialization needed for the summary). Full payload fetch is lazy — the client requests it via the REST event endpoint if needed.

**Payload handling in SSE:** The stream sends metadata only (no payload) to keep bandwidth low. The client fetches full event detail on click via the REST event endpoint.

---

## 5. Backend Implementation Plan

### 5.1 Changes to `mitiflow-orchestrator/src/http.rs`

```rust
// New in http.rs

/// Extended shared state for HTTP + SSE handlers.
pub struct HttpState {
    pub config_store: Arc<ConfigStore>,
    pub nodes: Option<Arc<RwLock<HashMap<String, NodeInfo>>>>,
    // New fields:
    pub lag_monitor: Option<Arc<LagMonitor>>,
    pub cluster_events_tx: broadcast::Sender<ClusterEvent>,
    pub lag_events_tx: broadcast::Sender<LagReport>,
    pub event_tail_tx: broadcast::Sender<EventSummary>,
    pub session: Option<Session>,       // For Zenoh query-through
    pub topic_manager: Option<Arc<TopicManager>>,
    pub auth_token: Option<String>,     // Static bearer token (None = no auth)
}

/// Router additions
fn build_router(state: HttpState) -> Router {
    let api = Router::new()
        // ... existing routes ...
        // New read REST
        .route("/api/v1/topics/{name}/partitions", get(topic_partitions))
        .route("/api/v1/topics/{name}/lag", get(topic_lag))
        .route("/api/v1/topics/{name}/publishers", get(topic_publishers))
        .route("/api/v1/consumer-groups", get(list_consumer_groups))
        .route("/api/v1/consumer-groups/{id}", get(get_consumer_group))
        .route("/api/v1/events", get(query_events))
        // Write REST
        .route("/api/v1/topics/{name}", put(update_topic))
        .route("/api/v1/cluster/nodes/{id}/drain", post(drain_node))
        .route("/api/v1/cluster/nodes/{id}/undrain", post(undrain_node))
        .route("/api/v1/cluster/overrides", get(get_overrides).post(add_overrides).delete(clear_overrides))
        .route("/api/v1/consumer-groups/{id}/reset", post(reset_consumer_group))
        // SSE
        .route("/api/v1/stream/cluster", get(sse_cluster))
        .route("/api/v1/stream/lag", get(sse_lag))
        .route("/api/v1/stream/events", get(sse_events));

    let app = api
        .fallback(get(serve_ui))  // Catch-all serves SPA index.html
        .with_state(state);

    // Optional auth layer
    // if let Some(token) = &state.auth_token {
    //     app = app.layer(ValidateRequestHeaderLayer::bearer(token));
    // }
    app
}
```

### 5.2 SSE Handler Pattern (axum)

```rust
use axum::response::sse::{Event, Sse};
use futures::stream::Stream;
use tokio_stream::wrappers::BroadcastStream;

async fn sse_lag(
    State(state): State<HttpState>,
    Query(params): Query<LagStreamParams>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.lag_events_tx.subscribe();
    let stream = BroadcastStream::new(rx)
        .filter_map(move |result| {
            match result {
                Ok(report) => {
                    // Optional group filter
                    if let Some(ref group) = params.group {
                        if &report.group_id != group { return None; }
                    }
                    let data = serde_json::to_string(&report).ok()?;
                    Some(Ok(Event::default().event("lag").data(data)))
                }
                Err(_) => None, // Lagged receiver, skip
            }
        });

    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(15))
    )
}
```

### 5.3 Static Asset Serving with `rust-embed`

```rust
use rust_embed::Embed;

#[derive(Embed)]
#[folder = "mitiflow-ui/build/"]  // Path to Vite build output
struct UiAssets;

async fn serve_ui(uri: axum::http::Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');

    // Try exact file first (JS, CSS, images)
    if let Some(file) = UiAssets::get(path) {
        let mime = mime_guess::from_path(path).first_or_octet_stream();
        return (
            [(header::CONTENT_TYPE, mime.as_ref())],
            file.data.into_owned(),
        ).into_response();
    }

    // Fallback to index.html for SPA client-side routing
    match UiAssets::get("index.html") {
        Some(file) => (
            [(header::CONTENT_TYPE, "text/html")],
            file.data.into_owned(),
        ).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}
```

### 5.4 Authentication (Optional Static Token)

```rust
// In orchestrator config:
pub struct OrchestratorConfig {
    // ...existing fields...
    /// Static bearer token for API/UI auth. None = no auth (local dev).
    pub ui_auth_token: Option<String>,
}

// Applied as tower middleware layer:
if let Some(ref token) = config.ui_auth_token {
    Router::new()
        .merge(api_routes)
        .layer(ValidateRequestHeaderLayer::bearer(token))
        .merge(public_routes)  // health endpoint stays unprotected
} else {
    Router::new().merge(api_routes).merge(public_routes)
}
```

The token can be set via:
- CLI flag: `mitiflow orchestrator --ui-token=mysecret`
- Env var: `MITIFLOW_UI_TOKEN=mysecret`

The SPA reads the token from a browser-local config (prompted on first visit, stored in `localStorage`).

### 5.5 Broadcast Channel Wiring (Orchestrator.run)

The orchestrator's `run()` method creates broadcast channels and passes senders to the subsystems:

```
Orchestrator::run()
  ├── broadcast::channel::<ClusterEvent>(256)  → tx to ClusterView, rx to HttpState
  ├── broadcast::channel::<LagReport>(256)     → tx to LagMonitor, rx to HttpState
  ├── broadcast::channel::<EventSummary>(1024) → tx to event subscriber task, rx to HttpState
  ├── LagMonitor::new(..., lag_tx)
  ├── ClusterView::new(..., cluster_tx)
  ├── spawn event tail subscriber (subscribes {key_prefix}/p/**)
  └── start_http(HttpState { ..., lag_tx, cluster_tx, event_tx })
```

---

## 6. Frontend Project Structure

```
mitiflow-ui/
├── package.json
├── vite.config.ts
├── tsconfig.json
├── tailwind.config.ts
├── src/
│   ├── app.html                  # Shell HTML
│   ├── App.svelte                # Root component, router mount
│   ├── lib/
│   │   ├── api.ts                # Typed fetch() wrappers
│   │   ├── sse.ts                # EventSource wrapper with reconnect
│   │   ├── types.ts              # TypeScript interfaces matching API JSON
│   │   └── format.ts             # Formatters (bytes, duration, UUID truncation)
│   ├── stores/
│   │   ├── cluster.svelte.ts     # Reactive cluster state (from SSE)
│   │   ├── lag.svelte.ts         # Reactive lag state (from SSE)
│   │   └── events.svelte.ts      # Recent events ring buffer (from SSE)
│   ├── pages/
│   │   ├── Dashboard.svelte      # Landing page
│   │   ├── Topics.svelte         # Topic list + create button
│   │   ├── TopicDetail.svelte    # Single topic deep view + edit/delete
│   │   ├── Nodes.svelte          # Node list + drain/undrain actions
│   │   ├── NodeDetail.svelte     # Single node detail
│   │   ├── ConsumerGroups.svelte # Group list
│   │   ├── GroupDetail.svelte    # Single group detail + offset reset
│   │   ├── EventInspector.svelte # Event browsing + detail
│   │   ├── Overrides.svelte      # Override table + add/clear
│   │   └── DLQ.svelte            # Dead letter queue
│   └── components/
│       ├── Layout.svelte         # Sidebar + header + content slot
│       ├── Sidebar.svelte        # Navigation
│       ├── StatCard.svelte       # Summary number card
│       ├── DataTable.svelte      # Wrapper around TanStack Table
│       ├── LagSparkline.svelte   # Tiny inline chart for lag trend
│       ├── PartitionGrid.svelte  # Visual partition map (color-coded)
│       ├── EventDetail.svelte    # Expandable event card with payload
│       ├── PayloadViewer.svelte  # JSON/hex/raw payload display
│       ├── StatusBadge.svelte    # Online/offline/recovering indicator
│       ├── TimeAgo.svelte        # Relative time display
│       ├── ConfirmDialog.svelte  # Reusable confirmation modal for writes
│       ├── TopicForm.svelte      # Create/edit topic form
│       └── Toast.svelte          # Error/info notifications
├── static/
│   └── favicon.svg
└── build/                        # Vite output → consumed by rust-embed
```

### 6.1 TypeScript API Types (matching Rust structs)

```typescript
// src/lib/types.ts

export interface TopicConfig {
  name: string;
  key_prefix: string;
  num_partitions: number;
  replication_factor: number;
  retention: RetentionPolicy;
  compaction: CompactionPolicy;
  required_labels: Record<string, string>;
  excluded_labels: Record<string, string>;
}

export interface RetentionPolicy {
  max_age?: string;     // Duration as string (e.g., "7d")
  max_bytes?: number;
  max_events?: number;
}

export interface CompactionPolicy {
  enabled: boolean;
  interval?: string;
}

export interface NodeInfo {
  metadata?: NodeMetadata;
  health?: NodeHealth;
  status?: NodeStatus;
  online: boolean;
  last_seen: string;
}

export interface NodeMetadata {
  node_id: string;
  capacity: number;
  labels: Record<string, string>;
  started_at: string;
}

export interface NodeHealth {
  node_id: string;
  partitions_owned: number;
  events_stored: number;
  disk_usage_bytes: number;
  store_latency_p99_us: number;
  error_count: number;
  timestamp: string;
}

export interface NodeStatus {
  node_id: string;
  partitions: PartitionStatus[];
  timestamp: string;
}

export interface PartitionStatus {
  partition: number;
  replica: number;
  state: "Starting" | "Recovering" | "Active" | "Draining" | "Stopped";
  event_count: number;
  watermark_seq: Record<string, number>;
}

export interface LagReport {
  group_id: string;
  partition: number;
  publishers: Record<string, number>;
  total: number;
  timestamp: string;
}

export interface ClusterStatus {
  total_nodes: number;
  online_nodes: number;
  total_partitions: number;
}

export interface EventSummary {
  seq: number;
  partition: number;
  publisher_id: string;
  event_id: string;
  timestamp: string;
  key?: string;
  key_expr: string;
  payload_size: number;
}

export interface EventDetail extends EventSummary {
  hlc?: { physical_ns: number; logical: number };
  urgency_ms: number;
  payload_base64: string;
  payload_text?: string;  // Pre-decoded if valid UTF-8
}

export interface AssignmentInfo {
  partition: number;
  replica: number;
  node_id: string;
  state: string;
  source: "Computed" | "Override";
}

export interface ConsumerGroupSummary {
  group_id: string;
  partition_count: number;
  total_lag: number;
  last_commit: string;
}

// --- Write operation request types ---

export interface CreateTopicRequest {
  name: string;
  key_prefix: string;
  num_partitions: number;
  replication_factor: number;
  required_labels?: Record<string, string>;
  excluded_labels?: Record<string, string>;
}

export interface UpdateTopicRequest {
  replication_factor?: number;
  retention?: Partial<RetentionPolicy>;
  compaction?: Partial<CompactionPolicy>;
  required_labels?: Record<string, string>;
  excluded_labels?: Record<string, string>;
}

export interface DrainRequest {
  replication_factor?: number;
}

export interface DrainResponse {
  node_id: string;
  overrides: OverrideEntry[];
}

export interface OverrideEntry {
  partition: number;
  replica: number;
  node_id: string;
  reason: string;
}

export interface AddOverridesRequest {
  entries: OverrideEntry[];
  ttl_seconds?: number;
}

export interface OverrideTable {
  entries: OverrideEntry[];
  epoch: number;
  expires_at?: string;
}

export type ResetStrategy =
  | "earliest"
  | "latest"
  | { to_seq: number }
  | { to_time: string };

export interface ResetOffsetsRequest {
  topic: string;
  partition: number;
  strategy: ResetStrategy;
}
```

### 6.2 SSE Client Wrapper

```typescript
// src/lib/sse.ts

export function createSSE<T>(
  url: string,
  eventType: string,
  onMessage: (data: T) => void,
  options?: { token?: string }
): { close: () => void } {
  // EventSource doesn't support custom headers natively.
  // For auth, append token as query param: ?token=...
  // (This is acceptable for a static token over localhost/internal network.)
  const fullUrl = options?.token ? `${url}?token=${options.token}` : url;
  const source = new EventSource(fullUrl);

  source.addEventListener(eventType, (e: MessageEvent) => {
    try {
      const data: T = JSON.parse(e.data);
      onMessage(data);
    } catch { /* ignore parse errors */ }
  });

  source.onerror = () => {
    // EventSource auto-reconnects. Log for debugging.
    console.warn(`SSE connection error for ${url}, reconnecting...`);
  };

  return { close: () => source.close() };
}
```

**Note on auth + SSE:** `EventSource` doesn't support custom headers. Options:
1. Pass token as query parameter: `/api/v1/stream/lag?token=mysecret`
2. Use `fetch()` with headers + manual SSE parsing (more complex)
3. Skip auth for SSE endpoints (they're read-only and on the same origin)

For v1 with static token, option 1 is simplest. The server validates the query param the same way as the `Authorization` header.

---

## 7. Build & Development Pipeline

### 7.1 Development Workflow

```bash
# Terminal 1: Run orchestrator with API
cargo run -p mitiflow-cli -- orchestrator --http-bind 0.0.0.0:8080

# Terminal 2: Run frontend dev server (proxies /api to orchestrator)
cd mitiflow-ui
pnpm dev   # Vite dev server on :5173, proxy /api → :8080
```

Vite config for dev proxy:

```typescript
// mitiflow-ui/vite.config.ts
export default defineConfig({
  server: {
    proxy: {
      '/api': 'http://localhost:8080',
    },
  },
});
```

### 7.2 Production Build

```bash
cd mitiflow-ui && pnpm build     # → mitiflow-ui/build/
cargo build -p mitiflow-cli      # rust-embed picks up build/ directory
```

The `justfile` should automate this:

```just
ui-build:
    cd mitiflow-ui && pnpm install && pnpm build

build-with-ui: ui-build
    cargo build -p mitiflow-cli --features ui

build:
    cargo build -p mitiflow-cli
```

### 7.3 Feature Flag

The UI embedding should be behind a feature flag so builds without node/pnpm still work:

```toml
# mitiflow-orchestrator/Cargo.toml
[features]
default = []
ui = ["dep:rust-embed", "dep:mime_guess"]
```

When `ui` feature is disabled, the `/ui/*` fallback route returns 404 and the binary is smaller. The REST API and SSE endpoints remain available regardless.

---

## 8. Data Flow Summary

```
                       Zenoh Mesh
                 ┌──────────┴──────────┐
                 │                      │
    ┌────────────▼──────┐    ┌─────────▼─────────┐
    │  Agent Nodes       │    │  Publishers/       │
    │  (health, status,  │    │  Subscribers        │
    │   stores, offsets) │    │  (events, offsets,  │
    └────────────┬───────┘    │   heartbeats)       │
                 │            └─────────┬───────────┘
                 │                      │
    ┌────────────▼──────────────────────▼───────────┐
    │             Orchestrator                       │
    │                                                │
    │  ClusterView ──broadcast──► SSE /stream/cluster│
    │  LagMonitor  ──broadcast──► SSE /stream/lag    │
    │  EventTail   ──broadcast──► SSE /stream/events │
    │  ConfigStore ──────────────► REST /topics       │
    │  Session.get ──────────────► REST /events       │
    │                               (query-through)  │
    └────────────────────────┬───────────────────────┘
                             │ HTTP :8080
                             ▼
    ┌────────────────────────────────────────────────┐
    │  Browser SPA                                   │
    │                                                │
    │  EventSource ←── /stream/cluster  → cluster $  │
    │  EventSource ←── /stream/lag      → lag $      │
    │  EventSource ←── /stream/events   → events $   │
    │  fetch()     ←── /api/v1/*        → on-demand  │
    └────────────────────────────────────────────────┘
```

---

## 9. Phasing Plan

### Phase 1 — Foundation (MVP)

- [x] Set up `mitiflow-ui/` project (Svelte 5 + Vite + Tailwind)
- [x] Add `rust-embed` to orchestrator behind `ui` feature flag
- [x] Implement static file serving + SPA fallback route
- [x] Implement SSE infrastructure (broadcast channels in `Orchestrator::run()`)
- [x] Build `SSE /api/v1/stream/cluster` (node join/leave/health)
- [x] Build Dashboard page (summary cards + node table from SSE)
- [x] Build Topics list page (from existing `GET /api/v1/topics`)
- [x] Build Nodes list page (from existing `GET /api/v1/cluster/nodes`)

**Outcome:** Navigable UI showing cluster overview, topics, and nodes.

### Phase 2 — Developer Debugging Core

- [x] Add `GET /api/v1/topics/{name}/partitions` (from ClusterView)
- [x] Add `GET /api/v1/topics/{name}/publishers` (from watermark data)
- [x] Build `SSE /api/v1/stream/lag` (from LagMonitor broadcast)
- [x] Build Topic Detail page (config + partition map + publishers + lag)
- [x] Add `GET /api/v1/events` (Zenoh query-through to store)
- [x] Build Event Inspector page (live tail + REST query mode with topic/key/seq/publisher filters)
- [x] Build PayloadViewer component (JSON / text / hex with auto-detection)
- [x] Build `SSE /api/v1/stream/events` (handler + producer task wired)

**Outcome:** Developers can browse topics, inspect partitions, and search/view individual events with decoded payloads.

### Phase 3 — Consumer Groups & Lag

- [x] Add `LagMonitor::known_groups()` method
- [x] Add `GET /api/v1/consumer-groups` and `GET /api/v1/consumer-groups/{id}`
- [x] Build Consumer Groups list page
- [x] Build Group Detail page (per-partition lag + live SSE; lag trend chart deferred)
- [x] Add lag sparkline on Dashboard and Group Detail pages

**Outcome:** Full visibility into consumption progress and lag across all consumer groups.

### Phase 4 — Write Operations

- [x] Upgrade `POST /api/v1/topics` handler (uses ConfigStore directly; Orchestrator::create_topic() deferred)
- [x] Upgrade `DELETE /api/v1/topics/{name}` handler (uses ConfigStore directly)
- [x] Add `PUT /api/v1/topics/{name}` (partial update with immutable field guard)
- [x] Build Topic Create form (inline in Topics page)
- [x] Build Topic Delete confirmation dialog
- [x] Build Topic Edit form (RF + retention + compaction settings panel)
- [x] Add `POST /api/v1/cluster/nodes/{id}/drain` and `/undrain`
- [x] Build Node Drain/Undrain action buttons with confirmation modal
- [x] Add `GET/POST/DELETE /api/v1/cluster/overrides`
- [x] Build Override Management panel (view + add + clear)
- [x] Add `POST /api/v1/consumer-groups/{id}/reset`
- [x] Build Consumer Group Offset Reset dialog (strategy selector + topic/partition form)

**Outcome:** Full CRUD for topics, node drain/undrain, override management, and consumer group offset resets from the UI.

### Phase 5 — Polish & DLQ

- [x] Static token auth (axum middleware + `MITIFLOW_UI_TOKEN` env var)
- [x] Node Detail page
- [ ] DLQ event buffering in orchestrator
- [x] DLQ page placeholder (backend DLQ subscription not yet implemented)
- [ ] Error handling UX (toast notifications, connection status indicator)
- [ ] Loading states, empty states, responsive layout

### Deferred (Option D — TUI)

- [ ] `mitiflow top` subcommand using ratatui
- [ ] Connects to orchestrator HTTP API (consumes same SSE endpoints)
- [ ] Real-time dashboard: node table, lag bars, event rate
- [ ] Independent of web UI — no shared code except API contract

---

## 10. Open Design Decisions

| # | Question | Options | Leaning |
|---|----------|---------|---------|
| 1 | **Event Inspector: max payload size to render?** | 1 KB / 10 KB / 100 KB | 10 KB default, warn above, truncate at 100 KB |
| 2 | **SSE reconnect backoff?** | Fixed 1s / Exponential 1–30s | Exponential (browser EventSource does this natively) |
| 3 | **Dashboard event tail depth?** | 20 / 50 / 100 events | 50, configurable in UI |
| 4 | **Lag trend history window?** | 1 min / 5 min / 15 min | 5 min (client-side ring buffer) |
| 5 | **Event query-through timeout?** | 1s / 5s / 10s | 5s (Zenoh get timeout to store) |
| 6 | **Consumer group member discovery** | Zenoh liveliness / skip in v1 | Skip in v1, show lag only |
| 7 | **UI routing** | Hash-based (#/topics) / History API | Hash-based (no server config needed) |
| 8 | **Multiple topics in event inspector?** | One at a time / cross-topic | One at a time (simpler query-through) |
| 9 | **Write confirmation UX** | Modal dialog / inline confirm / undo | Modal dialog (explicit, prevents accidents) |
| 10 | **Topic create: require key_prefix?** | Auto-generate from name / require explicit | Auto-generate default `mitiflow/{name}`, allow override |
| 11 | **Offset reset: allow all-partitions?** | Per-partition only / bulk | Per-partition only in v1 (safer) |
