# TODO

Tracks the gap between the design documents and the current implementation.

---

## Consumer Group Commits (Phase 1)

**Status:** Done
**Ref:** [11_consumer_group_commits.md](11_consumer_group_commits.md)

Store-managed offset commits co-located with the EventStore. Enables
Kafka-style consumer groups with at-least-once semantics over Zenoh.

### Implementation checklist

- [x] **`store/backend.rs` — offsets keyspace.** Add `offsets` keyspace to
      `FjallBackend`. Key: `[group_id_hash:8][publisher_id:16]`, Value:
      `[seq:8 BE][generation:8 BE][timestamp_ms:8 BE]`. Implement
      `commit_offsets(&OffsetCommit)` with generation fencing (reject if
      `commit.generation < stored_generation`). Implement
      `fetch_offsets(group_id) → HashMap<PublisherId, u64>` via prefix scan.

- [x] **`store/runner.rs` — offset subscribe + queryable.** Subscribe to
      `{key_prefix}/_offsets/{partition}/**` and persist incoming commits.
      Declare queryable on `{key_prefix}/_offsets/{partition}/**` to serve
      offset fetch requests via `session.get()`.

- [x] **`subscriber/consumer_group.rs` — commit API.** `commit_sync()` (query-based,
      waits for store ACK), `commit_async()` (fire-and-forget put), and
      `load_offsets(partition)` (fetches via `session.get()` and seeds
      `GapDetector`). Integrated into `on_rebalance`: commit lost partitions,
      load offsets for gained partitions.

- [x] **`subscriber/consumer_group.rs` — `ConsumerGroupSubscriber::new()`.** Constructor
      that joins a group via `PartitionManager`, fetches committed offsets, and begins
      consuming from assigned partitions.

- [x] **`config.rs` — consumer group config.** Add `ConsumerGroupConfig`
      (`group_id`, `member_id`, `CommitMode::Manual|Auto`, `OffsetReset`).

- [x] **`partition/mod.rs` — generation counter.** Add `generation: Arc<AtomicU64>`
      to `PartitionManager`. Increment on every membership change. Expose via
      `current_generation()`.

- [x] **Auto-commit task.** Background `tokio::spawn` that calls
      `commit_async()` at configurable interval (default 5s). Final commit on
      `CancellationToken` cancel.

- [x] **`error.rs` — `StaleFencedCommit` variant.** For generation fencing
      rejections.

- [x] **`lib.rs` — re-exports.** `OffsetCommit`, `ConsumerGroupConfig`,
      `CommitMode`, `OffsetReset`.

- [x] **`examples/consumer_groups.rs` — update.** Demonstrate
      `ConsumerGroupSubscriber`, manual commit, and auto-commit modes.
      (Current example only shows `PartitionManager`.)

### Depends on

- Sequence model migration (below) should ideally land first so that offset
  commits use per-partition sequences. But Phase 1 can proceed with the current
  model if each publisher writes to a single partition.

---

## Orchestrator (Phase 2)

**Status:** Core done (optional items deferred)
**Ref:** [11_consumer_group_commits.md](11_consumer_group_commits.md) § Part 6

A control-plane service for cross-partition visibility and lifecycle automation.
Does **not** sit in the event data path.

### Implementation checklist

- [x] **New crate: `mitiflow-orchestrator/`.** Binary entry point with
      `OrchestratorConfig` (env-var driven).
- [x] **Config management.** `TopicConfig` storage in fjall (`ConfigStore`).
      Distribute via `_config/{topic_name}` pub/sub. CRUD: `put_topic`,
      `get_topic`, `list_topics`, `delete_topic`.
- [x] **Lag monitoring.** (`LagMonitor`) subscribes to `_watermark/*` and
      `_offsets/**`, computes per-(group, partition, publisher) lag, publishes
      to `_lag/{group}/{partition}`.
- [x] **Store lifecycle.** (`StoreTracker`) tracks store liveliness via
      `_store/*`. Detects online/offline transitions. Exposes
      `online_stores()`, `offline_stores()`, `is_partition_online()`.
- [x] **Admin API (Zenoh queryable).** `_admin/**` queryable serving
      `topics` (list all) and `topics/{name}` (get specific).
- [x] **Admin API (HTTP REST).** Embedded axum HTTP server in orchestrator
      binary. Routes: `GET/POST /api/v1/topics`,
      `GET/DELETE /api/v1/topics/{name}`, `GET /api/v1/cluster/nodes`,
      `GET /api/v1/cluster/status`, `GET /api/v1/health`. Bind address
      configurable via `http_bind: Option<SocketAddr>` in `OrchestratorConfig`.
- [x] **Automated store provisioning.** `TopicWatcher` on agents subscribes
      to `{global_prefix}/_config/**`. On new `TopicConfig`, spawns
      `TopicWorker`. Agents filter by `required_labels` / `excluded_labels`
      matching. Late-joiner recovery via orchestrator `_config/**` queryable.
- [ ] **Consumer group sessions (optional).** JoinGroup/SyncGroup/Heartbeat
      protocol via Zenoh queryable. Assigns globally unique generation IDs.
      Fallback: existing liveliness-based `PartitionManager` remains default.
- [ ] **Orchestrator HA.** Deferred. Multiple replicas subscribe to same key
      expressions. Leader election via liveliness + lowest UUID for write ops.
      Tracked in Multi-Topic Agent & DX § Phase F.

### Tests

- `mitiflow-orchestrator/tests/orchestrator.rs` — 28 tests covering config
  store CRUD, persistence, lag monitoring, store tracker online/offline,
  ClusterView (6), OverrideManager (6), Drain (2), Admin API cluster
  endpoints (3), and multi-topic ClusterViews (3).
- `mitiflow-orchestrator/tests/http_api.rs` — 15 tests covering the embedded
  axum HTTP API: health endpoint, topics CRUD, cluster nodes/status,
  label creation, default values, and JSON error handling.
- `mitiflow-orchestrator/tests/e2e_orchestrator.rs` — 7 E2E scenarios.

### Depends on

- Consumer Group Commits (Phase 1) — offset storage must exist before the
  orchestrator can aggregate lag. ✅ Done.

---

## Key-Based Publishing

**Status:** Done
**Ref:** [15_key_based_publishing.md](15_key_based_publishing.md)

Kafka-style message keys embedded in Zenoh key expressions. Enables automatic
partition affinity, Zenoh-native key filtering (server-side), key-scoped replay,
and log compaction.

**Key expression layout:** `{prefix}/p/{partition}/k/{key}/{seq}` (keyed)
coexists with `{prefix}/p/{partition}/{seq}` (unkeyed, backward compatible).

### Phase 1 — Core keyed publish

- [x] **Key validation** — reject `*`, `$`, empty keys at publish time.
- [x] **`publish_keyed()` family** — `publish_keyed`, `publish_bytes_keyed`,
      `publish_keyed_durable`, `publish_bytes_keyed_durable` on
      `EventPublisher`. Internal: `hash(key) % num_partitions → partition`,
      construct `{prefix}/p/{partition}/k/{key}/{seq}`.
- [x] **`RawEvent::key()` accessor** — parse key from `key_expr` via `/k/`
      sentinel. Zero allocation (`&str` slice).
- [x] **`extract_partition()` update** — handle both keyed
      (`{prefix}/p/{partition}/k/{key}/{seq}`) and unkeyed layouts.
- [x] **Config helpers** — `key_expr_for_key()`,
      `key_expr_for_key_prefix()` on `EventBusConfig`.
- [x] **Tests** — publish with key, subscribe with key filter, round-trip key
      extraction, partition affinity for same key.

### Phase 2 — Store key index

- [x] **`keys` keyspace** in `FjallBackend`. Key:
      `[key_hash:8][hlc_physical:8 BE][hlc_logical:4 BE][publisher_id:16]`.
      Value: `[publisher_id:16][seq:8 BE]` (pointer to primary index).
- [x] **Write path** — index key on `store()` when key is present in
      `EventMetadata`.
- [x] **`EventMetadata` extension** — add `key: Option<String>` field.
- [x] **`query_by_key()`** — key-scoped queries on `StorageBackend`.

### Phase 3 — Log compaction

- [x] **Background compaction task** — periodic scan of key index, retain
      only highest-HLC entry per key_hash, delete superseded entries from
      all three indexes (primary, replay, keys).
- [x] **Tombstone handling** — null-payload keyed events as delete markers.
      Configurable tombstone retention period.
- [x] **`query_latest_by_keys()`** — compacted view query.
- [x] **Retention policy config** — compaction interval, tombstone GC period.

### Phase 4 — Subscriber convenience

- [x] **`EventSubscriber::new_keyed()`** — subscribe to a specific key.
- [x] **`EventSubscriber::new_key_prefix()`** — subscribe to a key prefix.
- [x] **Example** — `examples/keyed_pubsub.rs`.

---

## Kafka Gateway

**Status:** Stub only (`main.rs` prints "not yet implemented")
**Ref:** [07_kafka_compatibility.md](07_kafka_compatibility.md), [implementation_plan.md](implementation_plan.md) Phase 5

### Priority assessment

The gateway is architecturally a broker — it serializes writes per partition to
assign Kafka-compatible offsets. This re-introduces the coordination that
mitiflow's native API avoids (see [04_ordering.md](04_ordering.md) § "The
Brokerless Constraint"). Worth building only when Kafka ecosystem access is a
hard requirement for users.

### Phases (from implementation plan)

- [ ] **Phase 5a:** Core produce/consume (6 API keys). MVP: Produce, Fetch,
      Metadata, OffsetCommit, OffsetFetch, ListOffsets.
- [ ] **Phase 5b:** Consumer groups (5 API keys). JoinGroup, SyncGroup,
      Heartbeat, LeaveGroup, FindCoordinator.
- [ ] **Phase 5c:** Admin + polish. CreateTopics, DeleteTopics, DescribeConfigs,
      DeleteRecords.

### Open questions

- Is gateway HA worth investing in? Leader election per partition replicates
  Kafka's coordination plane.
- Should the gateway embed its own `EventStore`, or rely on external stores?
- Can the gateway be stateless by delegating offset assignment to the store
  (Approach A from [04_ordering.md](04_ordering.md))? This pushes the broker
  role to the store instead of the gateway — same coordination cost, different
  location.

---

## Distributed Storage Management

**Status:** Tier 1 done, Tier 2 done
**Ref:** [13_distributed_storage.md](13_distributed_storage.md), [05_replication.md](05_replication.md)

Two-tier architecture: decentralized StorageAgent (Tier 1) handles partition
assignment, failure recovery, and peer coordination via Zenoh primitives.
Optional Orchestrator extensions (Tier 2) add strategic overrides, drain
operations, and cluster dashboards.

### Tier 1 — StorageAgent (Phases 1–2)

- [x] **Weighted HRW** — `hash_ring.rs` extended with `weighted_hrw_score()`
      and `assign_replicas()`. Weighted rendezvous hashing assigns partitions
      proportionally to node capacity.
- [x] **MembershipTracker** — liveliness-based node discovery at
      `_agents/{node_id}`. Publishes `NodeMetadata`, queries existing peers
      with `history=true`, watches for membership changes.
- [x] **Reconciler** — desired vs actual state diff via `compute_actions()`.
      Starts/stops `EventStore` instances per `(partition, replica)` tuple.
      State machine: Starting → Recovering → Active → Draining → Stopped.
      Triggers `RecoveryManager` on partition gain when wired via
      `with_recovery()`.
- [x] **StorageAgent binary** — per-node daemon (`mitiflow-agent`) wires
      MembershipTracker, Reconciler, RecoveryManager, HealthReporter,
      StatusReporter. Env-var config (`MITIFLOW_KEY_PREFIX`, `MITIFLOW_DATA_DIR`,
      etc.). Graceful shutdown on Ctrl+C.
- [x] **HealthReporter + StatusReporter** — publish `NodeHealth` to
      `_cluster/health/{node_id}` (periodic, `CongestionControl::Drop`);
      publish `NodeStatus` with partition assignments to
      `_cluster/status/{node_id}` (on-change + 30s heartbeat).
- [x] **RecoveryManager** — `recover(partition, backend, peer_ids)` queries
      `{prefix}/_store/{partition}` via `session.get()` with
      `accept_replies(ReplyKeyExpr::Any)`. `recover_from_cache()` as fallback.
      Idempotent via `(publisher_id, seq)` keyed storage. Wired into
      Reconciler for automatic recovery on partition gain.
- [x] **Rack-aware assignment** — `assign_replicas_rack_aware()` in
      `hash_ring.rs`. Two-pass selection: first pass prefers rack diversity,
      second pass fills remaining slots. Uses `NodeDescriptor.labels`.
- [x] **Multi-replica support** — `HashMap<(u32, u32), ManagedStore>` in
      Reconciler tracks `(partition, replica)` tuples. Assignment via
      `assign_replicas()` returns replication chain. Tests validate RF=2
      scenarios.
- [x] **Override support** — agents subscribe to `_cluster/overrides` and
      prefer overrides over HRW-computed assignments. `OverrideTable` with
      epoch and expiry support.
- [x] **Arc<dyn StorageBackend> blanket impl** — allows sharing backend
      between EventStore (owned) and RecoveryManager (writes during recovery).

### Tier 2 — Orchestrator Extensions (Phase 3)

- [x] **ClusterView** — aggregate status/health streams into cluster-wide view.
      `cluster_view.rs` subscribes to `_cluster/status/*`, `_cluster/health/*`,
      watches `_agents/*` liveliness. Provides `assignments()`, `online_nodes()`,
      `online_count()` APIs.
- [x] **OverrideManager** — publish assignment overrides via
      `_cluster/overrides`. Auto-incrementing epoch, add/remove/clear entries.
- [x] **Drain operation** — compute overrides to evacuate a node for
      maintenance. `drain_node()` / `undrain_node()` in `drain.rs`.
- [x] **Admin API extensions** — cluster endpoints on `_admin/cluster/**`.
      `cluster/nodes`, `cluster/assignments`, `cluster/status` queryable endpoints.
- [ ] **Orchestrator HA** — liveliness-based leader election.

### Tier 1+2 Polish (Phase 4)

- [x] **Multi-topic support** — `TopicManager` creates per-topic `ClusterView`
      instances when `TopicConfig.key_prefix` is set. Lifecycle managed by
      Orchestrator `create_topic()` / `delete_topic()`.
- [x] **Multi-topic agent** — `TopicSupervisor` + `TopicWatcher` +
      `TopicWorker` in `mitiflow-agent`. Dynamic topic discovery via
      `_config/**` subscription. Placement-label filtering
      (`required_labels` / `excluded_labels`). YAML config with
      `auto_discover_topics` flag. See Multi-Topic Agent & DX §§ A–B.
- [ ] **Rebalance operation** — load-aware override generation. Tracked in
      Multi-Topic Agent & DX § Phase F.
- [x] **CLI tooling** — `mitiflow-ctl` binary for cluster management.
      Commands: topics list/get, cluster nodes/assignments/drain/undrain/overrides/status.
- [x] **Unified binary** — `mitiflow-cli` crate with `clap` subcommands:
      `agent`, `orchestrator`, `ctl`. YAML config files (`serde_yaml` +
      `humantime_serde`). `mitiflow ctl diagnose` health-check command.
      `mitiflow ctl topics/cluster` admin commands.

### Tests

- `mitiflow-agent/tests/agent.rs` — 7 tests: single node, 2-node split,
  node leave/rebalance, shutdown drains, overrides, RF=2 multi-replica,
  rack-aware placement.
- `mitiflow-agent/tests/reconciler.rs` — 9 tests: start/stop, noop,
  simultaneous gain/loss, drain grace period, state tracking, multi-replica,
  shutdown, recovery trigger.
- `mitiflow-agent/tests/recovery.rs` — 5 tests: no peers, unreachable peer,
  recover from EventStore, cache fallback, idempotency.
- `mitiflow-agent/tests/membership.rs` — 6 tests: discovery, leave, ignore
  self, metadata propagation, consistent node list.
- `mitiflow-agent/tests/multi_topic.rs` — multi-topic static configuration
  and per-topic data isolation tests.
- `mitiflow-agent/tests/topic_supervisor` — TopicSupervisor add/remove,
  duplicate errors, runtime additions, isolated assignment, separate data dirs.
- `mitiflow-agent/tests/topic_watcher` — TopicWatcher start/stop reaction,
  label filtering, idempotent duplicates, late-joiner discovery.
- `mitiflow-agent/tests/topic_worker` — TopicWorker lifecycle, RF=2,
  two-node split, recompute after node leave, override respects, shutdown drain.
- `mitiflow-agent/tests/yaml_config` — YAML parse (full, minimal,
  humantime durations, auto-discover, static topics), roundtrip, validation.
- `mitiflow-agent/tests/reporters` — health and status reporter unit tests.
- `mitiflow-agent/tests/e2e/scenarios.rs` — 13 E2E scenarios: cluster
  formation, rebalance, crash/rejoin, data survival across graceful leave
  and crash recovery, publish during rebalance, live ingestion.
- `mitiflow-agent/tests/smoke/scenarios.rs` — 3 smoke tests.
- `mitiflow-orchestrator/tests/orchestrator.rs` — 28 unit tests: config store,
  lag monitor, store tracker, ClusterView (6), OverrideManager (6), Drain (2),
  Admin API cluster endpoints (3), Multi-topic ClusterViews (3).
- `mitiflow-orchestrator/tests/http_api.rs` — 15 HTTP API tests.
- `mitiflow-orchestrator/tests/e2e_orchestrator.rs` — 7 E2E scenarios:
  orchestrator joins running cluster, drain moves partitions, override to
  offline node, override TTL expiry, full drain/maintenance/undrain roundtrip,
  orchestrator restart rebuilds view, concurrent override + crash.

---

## Multi-Topic Agent & Developer Experience

**Status:** Done (Phases A–F complete)
**Ref:** [16_dx_and_multi_topic.md](16_dx_and_multi_topic.md)

Evolve the single-topic-per-agent model into a broker-like multi-topic agent
that dynamically discovers topics from the orchestrator. Consolidate all
binaries into a unified `mitiflow` CLI with subcommands.

### Phase A — Multi-Topic Agent Foundation

- [x] **Extract `TopicWorker`** from `StorageAgent` internals (refactor, no
      behavior change). Each `TopicWorker` owns its own `Reconciler`,
      `MembershipTracker`, `RecoveryManager`, `StatusReporter`.
- [x] **`TopicSupervisor`** — manages `HashMap<String, TopicWorker>` with
      `add_topic()` / `remove_topic()`. Shared `HealthReporter` at node level.
- [x] **`StorageAgentConfig` multi-topic** — accepts `Vec<TopicConfig>`via
      `AgentConfig`. Backward compatible with single-topic usage.
- [x] **Separate fjall per topic** — data path
      `{data_dir}/{topic_name}/{partition}/{replica}/`. One fjall instance
      per topic for failure isolation and independent retention.
- [x] **YAML config file** — `serde_yaml` + `humantime_serde`. Fields:
      `node`, `cluster`, `topics`. Env vars override YAML values.
- [x] **Update existing tests** to use `TopicWorker` directly.

### Phase B — Topic Provisioning Protocol

- [x] **`TopicWatcher`** — agent subscribes to `{global_prefix}/_config/**`.
      On new `TopicConfig`, calls `TopicSupervisor::add_topic()`. On delete,
      calls `remove_topic()` which drains and stops the worker.
- [x] **Orchestrator queryable for `_config/**`** — late-joining agents query
      all existing topics via `session.get()` on startup.
- [x] **Placement labels** — `required_labels` / `excluded_labels` on
      `TopicConfig`. Agent's `should_serve_topic()` matches own labels.
- [x] **Configurable global prefix** — default `"mitiflow"`, set via
      `cluster.global_prefix` in YAML or `MITIFLOW_GLOBAL_PREFIX` env var.
- [ ] **Topic data deletion command** — orchestrator publishes to
      `_commands/delete_data/{topic_name}`, agents delete on-disk data and
      ack. Requires topic in `deleted` state. Idempotent. (deferred)
- [x] **E2E tests** — TopicWatcher integration: creates/deletes workers on
      config events, label filtering, idempotent duplicates, startup discovery.

### Phase C — Unified Binary

- [x] **`mitiflow-cli` binary** with `clap` subcommands: `agent`,
      `orchestrator`, `ctl`.
- [x] **`mitiflow agent --config agent.yaml`** — YAML-driven agent startup.
- [x] **`mitiflow orchestrator --config orchestrator.yaml`** — YAML-driven.
- [x] **`mitiflow ctl`** — `topics list/get/delete`, `cluster nodes/status/
      drain/undrain/overrides`, `diagnose`.
- [x] **Individual crate binaries remain** for direct use.
- [ ] **`mitiflow dev`** — co-locate orchestrator + agent + embedded Zenoh
      router in one process. (deferred)

### Phase D — Orchestrator HTTP API

- [x] **Embedded axum** HTTP server in orchestrator binary. Bind address
      configurable via `http_bind: Option<SocketAddr>` in `OrchestratorConfig`.
- [x] **REST endpoints:** `GET/POST /api/v1/topics`,
      `GET/DELETE /api/v1/topics/{name}`, `GET /api/v1/cluster/nodes`,
      `GET /api/v1/cluster/status`, `GET /api/v1/health`.
- [ ] **`DELETE /api/v1/topics/{name}/data`** — trigger on-disk cleanup.
      (deferred, depends on delete command protocol)
- [ ] **Topic readiness status** — `ready`, `partial`, `pending` based on
      partition coverage from `ClusterView`. (deferred)

### Phase E — Dev Experience Polish

- [x] **Error diagnostics** — `miette` annotations with `#[diagnostic(code,
      help)]` on all `mitiflow::Error` and `AgentError` variants.
- [x] **`mitiflow ctl diagnose`** — checks Zenoh connectivity, topic
      discovery, agent liveliness, and orchestrator admin queryable.
- [ ] **OpenTelemetry** — `tracing-opentelemetry` for distributed traces,
      `opentelemetry-prometheus` for metrics export. (deferred)

### Phase F — Proactive Orchestrator

- [x] **Under-replicated partition alerts** — `AlertManager` in
      `mitiflow-orchestrator/src/alert_manager.rs`. Detects when live replicas
      < replication factor (Warning) or == 0 (Critical). Configurable thresholds.
- [x] **Node offline alerts** — detects nodes absent from health stream for
      > grace period (Warning), escalates to Critical after longer absence.
- [ ] **Auto-drain on node failure** — automatic override generation when
      a node goes offline. (deferred)
- [ ] **Rebalance advisor** — load-aware override generation. (deferred)
- [ ] **Orchestrator HA** — leader election via liveliness + lowest UUID.
      (deferred)

### Tests

- `mitiflow-agent/tests/multi_topic.rs` — multi-topic static config,
  per-topic data isolation.
- `mitiflow-agent/tests/topic_supervisor` — add/remove, duplicate errors,
  runtime additions, isolated partition assignment, separate data dirs.
- `mitiflow-agent/tests/topic_watcher` — start/stop reaction, label
  filtering, idempotent duplicates, late-joiner discovery via queryable.
- `mitiflow-agent/tests/topic_worker` — lifecycle, RF=2, two-node split,
  recompute after node leave, override respects, shutdown drain.
- `mitiflow-agent/tests/yaml_config` — full/minimal/humantime/auto-discover/
  static-topics parse, roundtrip serialization, validation errors.
- `mitiflow-agent/tests/reporters` — health and status reporter unit tests.
- `mitiflow-agent/tests/e2e_tests.rs` — agent lifecycle and integration tests.
- `mitiflow-orchestrator/tests/http_api.rs` — 15 tests: health, topics CRUD,
  cluster nodes/status, label creation, defaults, JSON error handling.
- `mitiflow-orchestrator/src/alert_manager.rs` — 8 unit tests: no alerts
  baseline, node offline (warning/critical escalation), under-replicated
  (partial/critical), multiple nodes, replication=1 correct baseline.

---

## Replication

**Status:** Partially implemented (via Distributed Storage Management)
**Ref:** [05_replication.md](05_replication.md), [13_distributed_storage.md](13_distributed_storage.md)

Replication is now part of the StorageAgent design — the `replica` index in
`(partition, replica)` assignment tuples maps directly to the replication
factor. See [13_distributed_storage.md](13_distributed_storage.md) § 7.

- [x] **Multi-store deployment** — run multiple `EventStore` instances
      subscribing to the same key expressions. Zenoh pub/sub fan-out handles
      data distribution. Reconciler manages `(partition, replica)` tuples.
- [ ] **Quorum watermark tracker** — `QuorumTracker` that collects watermarks
      from N replicas and computes a quorum watermark (majority agreement).
- [ ] **Publisher quorum confirmation** — `publish_durable()` waits for quorum
      watermark instead of single-store watermark.
- [x] **Recovery protocol** — `RecoveryManager` queries peers for missing
      events via `session.get()` with `accept_replies(ReplyKeyExpr::Any)`.
      Idempotent via `(publisher_id, seq)` keyed storage.
- [ ] **Durability levels** — configurable: `Single` (any 1 store),
      `Quorum` (majority), `All` (every replica).

---

## Deterministic Replay Ordering

**Status:** Implemented
**Ref:** [08_replay_ordering.md](08_replay_ordering.md)

- [x] **HLC timestamp in EventMetadata** — store the Zenoh HLC timestamp
      alongside each event for replica-independent ordering.
- [x] **Replay index in FjallBackend** — secondary keyspace `replay` keyed by
      `(hlc_physical, hlc_logical, publisher_id, seq)` for deterministic
      ordered replay across replicas.
- [x] **`query_replay()` on StorageBackend** — scan replay index with HLC
      range filters, returning events in deterministic HLC order.
- [x] **Publisher lifecycle state machine** — ACTIVE → SUSPECTED → DRAINING →
      ARCHIVED → GC. Multi-signal liveness detection (liveliness token +
      inactivity timeout) to avoid false eviction on network partition.
- [x] **Watermark epoch** — `CommitWatermark` includes epoch counter;
      only ACTIVE/SUSPECTED/DRAINING publishers included.

---

## Slow Consumer Offload

**Status:** Core implemented (Option C Hybrid)
**Ref:** [17_slow_consumer_offload.md](17_slow_consumer_offload.md)

Automatic pub/sub → store-query demotion for consumers that fall behind the
live stream. The consumer transparently switches from Zenoh pub/sub to batched
Event Store queries when lag is detected, and resumes live delivery when caught
up. Implemented as Option C (Hybrid): transparent offload + observable
`OffloadEvent` channel.

### Phase 1 — Lag Detection & State Machine

- [x] **`subscriber/offload.rs` — `OffloadManager`.** Three-state
      consumer: LIVE → DRAINING → CATCHING_UP → LIVE. Manages offload
      lifecycle and store query cursor inline in the processing task.
- [x] **`subscriber/offload.rs` — `LagDetector`.** Composite lag score
      from channel fullness + heartbeat sequence delta. Debounce window
      prevents flapping on transient bursts.
- [x] **`config.rs` — `OffloadConfig`.** Configuration:
      `channel_fullness_threshold` (0.8), `seq_lag_threshold` (10,000),
      `debounce_window` (2s), `re_subscribe_threshold` (1,000),
      `drain_quiet_period` (50ms). Builder integration with `EventBusConfig`.
      Also added `event_channel_capacity` (default 1024) to `EventBusConfig`.
- [x] **`subscriber/offload.rs` — `OffloadEvent` enum.** Observable lifecycle
      events: `LagDetected`, `Draining`, `CatchingUp`, `CaughtUp`, `Resumed`,
      `OffloadFailed`. Published to unbounded `flume` channel.
- [x] **`subscriber/mod.rs` — `offload_events()`.** Public method returning
      `Option<flume::Receiver<OffloadEvent>>` for optional observability
      (returns `None` when offload is disabled).

### Phase 2 — Store Catch-Up Reader

- [x] **`subscriber/offload.rs` — `CatchUpReader`.** Batched store queries
      via `session.get()` with adaptive sizing. Advances per-(publisher,
      partition) cursors. Default batch: 10,000 events, 5s query timeout.
- [x] **Adaptive batch sizer.** Auto-tunes batch size based on query
      duration (halve on slow, double on fast). Extracted as standalone
      `adjust_batch_size_adaptive()` function for testability.
- [x] **`is_caught_up_check()`.** Standalone function comparing cursors
      against latest known sequences to determine catch-up completion.

### Phase 3 — Switchover Protocol

- [x] **LIVE → DRAINING transition.** Drain bounded flume `sample_rx`
      channel with `try_recv()` + configurable quiet period, record
      switchover cursor via `GapDetector::snapshot_cursors()`.
- [x] **CATCHING_UP → LIVE transition.** Cursor-based dedup: events
      from store with `seq <= cursor` are skipped. GapDetector cursors
      are advanced during catch-up via `on_sample()` to prevent duplicates
      when live stream resumes.
- [x] **Gap detector preservation.** Gap detector state is preserved across
      offload cycles — cursors are advanced during catch-up, not reset.

### Phase 4 — Integration & Tests

- [x] **Feature gate.** Offload compiles only with `store` feature.
      `#[cfg(feature = "store")] pub mod offload;`
- [ ] **Consumer group compatibility.** Offloaded consumer maintains
      liveliness token and continues committing offsets from store queries.
      (Not yet tested with `ConsumerGroupSubscriber`.)
- [x] **Unit tests** — 14 tests in `offload.rs`: LagDetector (debounce,
      reset, seq lag, composite signals), adaptive batch sizing,
      caught-up detection, OffloadEvent variants, ConsumerState transitions.
- [x] **E2E tests** — `tests/offload.rs` with 6 tests: disabled offload,
      fast consumer no-offload, event channel availability, slow consumer
      lifecycle (no duplicates), ordering preservation, debounce flapping
      prevention.
- [x] **Example** — `examples/slow_consumer_offload.rs` demonstrating
      automatic offload with small channel, `OffloadEvent` monitoring.

### Depends on

- Event Store must be running for the topic (offload queries the store).
- Heartbeat mode must not be `Disabled` for heartbeat-based lag detection.

---

## Cleanup: Remove `history_on_subscribe`

**Status:** Not started

`history_on_subscribe` is a config field in `EventBusConfig` that is never
read or acted upon in any subscriber code. It was a placeholder for an earlier
design (store query on subscribe) that was never implemented.

- [ ] **Remove `history_on_subscribe` field** from `EventBusConfig`.
- [ ] **Remove builder method** `history_on_subscribe()` from
      `EventBusConfigBuilder`.
- [ ] **Update tests** — remove all `history_on_subscribe(false)` calls in
      test setup (`tests/store.rs`, `tests/keyed_publish.rs`).
- [ ] **Update `consumer_group.rs`** — remove
      `.history_on_subscribe(config.history_on_subscribe)` in
      `ConsumerGroupSubscriber::new()`.
- [ ] **Update docs** — remove from `02_architecture.md` config table and
      `implementation_plan.md` example.

---

## Testing Gaps

Tests listed in [implementation_plan.md](implementation_plan.md) § 3 that don't
exist yet:

- [ ] `tests/watermark.rs` — watermark broadcast, durable publish timeout, gap
      clearing. (Currently tested inline in `tests/store.rs` but not the full
      suite from the plan.)
- [ ] `tests/gateway.rs` — Kafka protocol round-trip. Blocked on gateway
      implementation.
- [x] `tests/consumer_group_commit.rs` — consumer group offset commit e2e
      tests. 8 tests covering: commit round-trip, multi-publisher, generation
      fencing, auto-commit interval, per-publisher independence, independent
      groups on same topic, sync vs async commit, store crash recovery.
      See [12_consumer_group_e2e_tests.md](12_consumer_group_e2e_tests.md).
- [x] `e2e_*` integration tests — `mitiflow-agent/tests/e2e/` has 13
      scenarios covering cluster formation, rebalance, crash/rejoin, data
      survival, and live ingestion. `mitiflow-agent/tests/smoke/` has 3
      multi-process smoke tests.
- [ ] Criterion benchmarks (`mitiflow-bench/benches/`) — the plan specifies
      throughput, latency, store, and watermark benchmark suites. Current
      benchmarks use a custom harness (`bench_pubsub`, `bench_durable`), not
      criterion.

---

## GUI — Embedded Web UI

**Status:** Phase 1–7 mostly done, Phase 8 partial (CI missing)
**Ref:** [18_gui_design.md](18_gui_design.md)

Embedded SPA (Svelte 5 + Vite) served from the orchestrator's axum server
via `rust-embed`. Read-write. Primary user: developers (debugging). Secondary:
platform engineers. Auth: optional static bearer token. Single cluster.
TUI deferred.

### Phase 1 — Backend Foundation

All backend work is test-first: write the HTTP/SSE test, then implement the
handler. Tests use `axum::test` helpers (`tower::ServiceExt::oneshot`) against
`build_router()` — no real Zenoh needed for unit tests.

- [x] **`http.rs` — `HttpState` extension.** Add `orchestrator: Arc<RwLock<Orchestrator>>`,
      `lag_events_tx: broadcast::Sender<LagReport>`,
      `cluster_events_tx: broadcast::Sender<ClusterEvent>`,
      `event_tail_tx: broadcast::Sender<EventSummary>`,
      `session: Option<Session>`, `topic_manager: Option<Arc<TopicManager>>`,
      `auth_token: Option<String>` fields. Update `start_http()` signature.

      **Tests (update `http_api.rs`):**
      - [x] existing 15 tests continue to pass with extended `HttpState`

- [x] **`http.rs` — static file serving.** Add `rust-embed` dependency behind
      `ui` feature flag. Implement `serve_ui()` fallback handler that serves
      embedded assets or `index.html` for SPA routing.

      **Tests (`http_api.rs`):**
      - [ ] `test_serve_ui_index_html` — GET `/` returns 200 with
            `text/html` content type (when `ui` feature enabled)
      - [ ] `test_serve_ui_fallback` — GET `/topics/orders` (SPA route)
            returns `index.html` content
      - [ ] `test_serve_ui_static_asset` — GET `/assets/app.js` returns
            the correct asset with proper MIME type
      - [ ] `test_serve_ui_404_without_feature` — when `ui` feature
            disabled, GET `/` returns 404

- [x] **`http.rs` — optional auth middleware.** axum `middleware::from_fn`
      bearer token check on all API routes except `/api/v1/health`. Token
      from `MITIFLOW_UI_TOKEN` env var. None = no auth (local dev)

      **Tests (`http_api.rs`):**
      - [ ] `test_auth_disabled_by_default` — no token configured, all
            endpoints accessible without `Authorization` header
      - [ ] `test_auth_rejects_missing_token` — token configured, GET
            `/api/v1/topics` without header returns 401
      - [ ] `test_auth_rejects_wrong_token` — invalid bearer returns 401
      - [ ] `test_auth_accepts_valid_token` — correct bearer returns 200
      - [ ] `test_health_bypasses_auth` — GET `/api/v1/health` returns
            200 even when auth is enabled and no token is provided

- [x] **`orchestrator.rs` — broadcast channels.** Create
      `broadcast::channel` for `ClusterEvent`, `LagReport`, `EventSummary`
      in `Orchestrator::run()`. Pass `Sender` clones to subsystems and
      `HttpState`.

      **Tests (`orchestrator.rs`):**
      - [ ] `test_broadcast_channels_created` — after `run()`, verify
            the broadcast senders are connected by subscribing and
            checking a lag report is received

- [x] **`lag.rs` — `known_groups()` + broadcast.** Add
      `LagMonitor::known_groups() → Vec<String>` returning distinct group IDs
      from observed offset commits. Add optional `broadcast::Sender<LagReport>`
      parameter; fire alongside Zenoh put. Also added `get_publishers()`
      returning `Vec<(PublisherId, Vec<u32>)>` from watermarks.

      **Tests (`orchestrator.rs`):**
      - [ ] `test_lag_monitor_known_groups_empty` — no offsets observed,
            returns empty vec
      - [ ] `test_lag_monitor_known_groups_discovers` — after offset
            commits from two groups, `known_groups()` returns both
      - [ ] `test_lag_monitor_broadcasts` — subscribe to broadcast rx,
            publish watermark + offset, verify `LagReport` received on rx

### Phase 2 — Read API Endpoints

Each endpoint: write test → implement handler → verify test passes.

- [x] **`GET /api/v1/topics/{name}/partitions`** — partition assignments for
      a topic from `ClusterView::assignments()` filtered by topic key prefix.

      **Tests (`http_api.rs`):**
      - [ ] `test_topic_partitions_found` — create topic + seed ClusterView
            with node status containing partition assignments, GET returns
            correct partitions with replicas grouped
      - [ ] `test_topic_partitions_not_found` — unknown topic returns 404

- [x] **`GET /api/v1/topics/{name}/publishers`** — active publishers from
      watermark data in LagMonitor.

      **Tests (`http_api.rs`):**
      - [ ] `test_topic_publishers` — seed watermarks for two publishers,
            GET returns both with per-partition committed_seq
      - [ ] `test_topic_publishers_empty` — no watermarks, returns empty list

- [x] **`GET /api/v1/topics/{name}/lag`** — lag per consumer group for a topic.

      **Tests (`http_api.rs`):**
      - [ ] `test_topic_lag` — seed lag data for two groups, GET returns
            both with per-partition breakdown and totals
      - [ ] `test_topic_lag_no_groups` — no lag data, returns empty list

- [x] **`GET /api/v1/consumer-groups`** — list all known consumer groups.

      **Tests (`http_api.rs`):**
      - [ ] `test_list_consumer_groups` — seed offset commits from two
            groups, GET returns both with total_lag
      - [ ] `test_list_consumer_groups_empty` — no groups, returns `[]`

- [x] **`GET /api/v1/consumer-groups/{id}`** — consumer group detail.

      **Tests (`http_api.rs`):**
      - [ ] `test_consumer_group_detail` — returns lag per partition with
            publisher breakdown
      - [ ] `test_consumer_group_not_found` — unknown group returns 404

- [x] **`GET /api/v1/events`** — event browsing via Zenoh query-through.

      **Tests:** Integration test (requires Zenoh session + EventStore):
      - [ ] `test_event_query_by_seq_range` — publish 10 events, query
            `after_seq=3&before_seq=7`, verify 3 events returned with
            metadata and base64 payload
      - [ ] `test_event_query_by_key` — publish keyed events, query
            `key=ORD-001`, verify only matching events returned
      - [ ] `test_event_query_limit` — publish 100 events, query
            `limit=10`, verify 10 events + `has_more=true`
      - [ ] `test_event_query_missing_topic` — no `topic` param returns 400
      - [ ] `test_event_query_store_offline` — store not running, returns
            504 Gateway Timeout

### Phase 3 — SSE Streaming Endpoints

- [x] **`SSE /api/v1/stream/cluster`** — node join/leave/health changes.

      **Tests (`http_api.rs`):**
      - [ ] `test_sse_cluster_initial_snapshot` — connect SSE, verify
            initial node events emitted for all known nodes
      - [ ] `test_sse_cluster_node_change` — connect SSE, broadcast a
            `ClusterEvent::NodeOnline`, verify event received

- [x] **`SSE /api/v1/stream/lag`** — lag report stream.

      **Tests (`http_api.rs`):**
      - [ ] `test_sse_lag_unfiltered` — connect SSE, broadcast LagReport,
            verify received
      - [ ] `test_sse_lag_filtered_by_group` — connect SSE with
            `?group=order-svc`, broadcast two reports (one matching, one
            not), verify only matching received

- [x] **`SSE /api/v1/stream/events`** — live event tail. (Handler exists;
      producer task that subscribes to `{prefix}/p/**` not yet wired.)

      **Tests (`http_api.rs`):**
      - [ ] `test_sse_events_receives_tail` — connect SSE, broadcast
            `EventSummary`, verify metadata-only event received (no payload)

### Phase 4 — Write API Endpoints

- [x] **`POST /api/v1/topics` — upgrade.** Handler uses `ConfigStore::put_topic()`.
      Design wanted `Orchestrator::create_topic()` for Zenoh distribution +
      ClusterView lifecycle (deferred).

      **Tests (`http_api.rs`):**
      - [ ] `test_create_topic_via_orchestrator` — POST creates topic,
            verify it appears in list AND per-topic ClusterView is started

- [x] **`DELETE /api/v1/topics/{name}` — upgrade.** Uses `ConfigStore`
      directly. Design wanted `Orchestrator::delete_topic()` (deferred).

      **Tests (`http_api.rs`):**
      - [ ] `test_delete_topic_via_orchestrator` — DELETE removes topic,
            verify per-topic ClusterView is stopped

- [x] **`PUT /api/v1/topics/{name}`** — partial update.

      **Tests (`http_api.rs`):**
      - [ ] `test_update_topic_retention` — PUT with `retention` field
            updates only retention, other fields unchanged
      - [ ] `test_update_topic_rejects_immutable` — PUT with `name` or
            `num_partitions` change returns 400
      - [ ] `test_update_topic_not_found` — PUT on nonexistent topic
            returns 404
      - [ ] `test_update_topic_empty_body` — PUT with `{}` returns 200
            with unchanged config

- [x] **`POST /api/v1/cluster/nodes/{id}/drain`** — drain a node.

      **Tests (`http_api.rs`):**
      - [ ] `test_drain_node` — POST with RF=3, verify overrides generated
            and returned in response
      - [ ] `test_drain_unknown_node` — POST for nonexistent node returns
            404 or empty overrides

- [x] **`POST /api/v1/cluster/nodes/{id}/undrain`** — undrain a node.

      **Tests (`http_api.rs`):**
      - [ ] `test_undrain_node` — POST after drain, verify overrides cleared

- [x] **`GET /api/v1/cluster/overrides`** — read current override table.

      **Tests (`http_api.rs`):**
      - [ ] `test_get_overrides_empty` — no overrides, returns empty table
      - [ ] `test_get_overrides_after_drain` — after drain, returns
            entries with epoch > 0

- [x] **`POST /api/v1/cluster/overrides`** — add override entries.

      **Tests (`http_api.rs`):**
      - [ ] `test_add_overrides` — POST entries, GET returns them with
            incremented epoch
      - [ ] `test_add_overrides_with_ttl` — POST with `ttl_seconds`,
            verify `expires_at` set

- [x] **`DELETE /api/v1/cluster/overrides`** — clear all overrides.

      **Tests (`http_api.rs`):**
      - [ ] `test_clear_overrides` — add overrides, DELETE, GET returns
            empty entries with incremented epoch

- [x] **`POST /api/v1/consumer-groups/{id}/reset`** — reset offsets.

      **Tests:** Integration test (requires Zenoh session + EventStore):
      - [ ] `test_reset_offsets_to_earliest` — publish events, commit
            offsets, POST reset `strategy: "earliest"`, verify offset = 0
      - [ ] `test_reset_offsets_to_latest` — POST reset `strategy: "latest"`,
            verify offset equals current watermark
      - [ ] `test_reset_offsets_to_seq` — POST reset `strategy: {"to_seq": 100}`,
            verify offset = 100
      - [ ] `test_reset_offsets_requires_topic_and_partition` — POST
            without `topic` returns 400

### Phase 5 — Frontend Foundation

Frontend tests use Vitest (Svelte component testing) and Playwright (E2E).

- [x] **Project scaffold.** `mitiflow-ui/` with Svelte 5, Vite, Tailwind,
      svelte-spa-router.

      **Tests:**
      - [ ] `vitest` — App.svelte renders without errors
      - [ ] `vitest` — Router mounts correct page for each route hash

- [x] **`lib/api.ts`** — typed fetch wrappers for all endpoints.

      **Tests (`api.test.ts` with msw mock server):**
      - [ ] `test_list_topics` — mock GET `/api/v1/topics`, verify typed
            result
      - [ ] `test_create_topic` — mock POST, verify request body shape
      - [ ] `test_update_topic` — mock PUT, verify partial body
      - [ ] `test_delete_topic` — mock DELETE, verify 204 handling
      - [ ] `test_drain_node` — mock POST, verify response parsing
      - [ ] `test_error_handling` — mock 500, verify error thrown

- [x] **`lib/sse.ts`** — SSE client wrapper with reconnect.

      **Tests (`sse.test.ts`):**
      - [ ] `test_sse_parses_events` — mock EventSource, verify callback
            invoked with parsed data
      - [ ] `test_sse_close` — verify EventSource.close() called

- [x] **Components: Layout, Sidebar, StatCard, StatusBadge, Toast,
      ConfirmDialog, DataTable.** (DataTable not started; rest done.)

      **Tests (Vitest component tests):**
      - [ ] `test_sidebar_navigation` — renders all nav links
      - [ ] `test_stat_card_display` — renders label and value
      - [ ] `test_confirm_dialog` — shows message, calls onConfirm/onCancel
      - [ ] `test_toast_auto_dismiss` — appears and disappears after timeout

### Phase 6 — Frontend Read Pages

- [x] **Dashboard.svelte** — summary cards, node table, lag summary, event
      tail. (REST + SSE: live node table, recent events tail, lag sparklines.)

      **Tests:**
      - [ ] `vitest` — renders stat cards with mocked API data
      - [ ] `vitest` — lag table updates when SSE store changes
      - [ ] `playwright` — full dashboard renders with mock API server

- [x] **Topics.svelte** — topic list table with search/filter.

      **Tests:**
      - [ ] `vitest` — renders topic rows from mocked data
      - [ ] `vitest` — search input filters displayed topics

- [x] **TopicDetail.svelte** — config, partition map, publishers, lag.
      (RF edit + retention/compaction settings panel done.)

      **Tests:**
      - [ ] `vitest` — renders partition map from mocked partition data
      - [ ] `vitest` — shows publisher table from mocked publisher data

- [x] **Nodes.svelte** — node list with health metrics.

      **Tests:**
      - [ ] `vitest` — renders node rows with status badges
      - [ ] `vitest` — offline node shows red badge

- [x] **NodeDetail.svelte** — single node health + partition assignments.

- [x] **ConsumerGroups.svelte** — group list with lag totals.

- [x] **GroupDetail.svelte** — per-partition lag, lag trend chart.
      (Lag table + live SSE + LagSparkline + offset reset dialog done.)

      **Tests:**
      - [ ] `vitest` — renders lag table with per-partition breakdown
      - [ ] `vitest` — lag chart renders sparkline from SSE data

- [x] **EventInspector.svelte** — filter form, result table, detail expand,
      PayloadViewer. (Live/Query mode tabs, topic/key/seq/publisher filters,
      EventDetail expand, PayloadViewer done.)

      **Tests:**
      - [ ] `vitest` — filter form submits correct query params
      - [ ] `vitest` — payload viewer renders JSON with syntax highlighting
      - [ ] `vitest` — payload viewer falls back to hex for binary

- [x] **DLQ.svelte** — dead letter queue placeholder page.
      (Backend DLQ subscription not yet implemented.)

### Phase 7 — Frontend Write Pages

- [x] **TopicForm.svelte** — create form inline in Topics.svelte; edit
      (RF + retention + compaction) in TopicDetail settings panel.

      **Tests:**
      - [ ] `vitest` — create mode: all fields empty, submit calls POST
      - [ ] `vitest` — edit mode: fields pre-filled, submit calls PUT
      - [ ] `vitest` — validation: name required, partitions > 0, RF > 0
      - [ ] `vitest` — immutable fields disabled in edit mode

- [x] **Topics.svelte — create button + TopicForm modal.** (Inline create
      form done.)

      **Tests:**
      - [ ] `vitest` — click "Create Topic" opens form modal
      - [ ] `vitest` — after successful create, topic list refreshes

- [x] **TopicDetail.svelte — edit + delete actions.** (RF edit + delete
      with ConfirmDialog done. Full edit form deferred.)

      **Tests:**
      - [ ] `vitest` — click "Edit" opens TopicForm in edit mode
      - [ ] `vitest` — click "Delete" opens ConfirmDialog
      - [ ] `vitest` — confirm delete calls API and navigates to list

- [x] **Nodes.svelte — drain/undrain action buttons.** (Drain/undrain
      with ConfirmDialog done.)

      **Tests:**
      - [ ] `vitest` — click "Drain" opens ConfirmDialog with node name
      - [ ] `vitest` — confirm drain calls POST and shows override summary
      - [ ] `vitest` — drained node shows "Undrain" button instead

- [x] **Overrides.svelte — override table + add/clear.** (View +
      add + clear with ConfirmDialog done.)

      **Tests:**
      - [ ] `vitest` — renders current overrides from GET
      - [ ] `vitest` — add override form submits correct entries
      - [ ] `vitest` — clear button opens ConfirmDialog, confirm calls DELETE

- [x] **GroupDetail.svelte — offset reset action.** Reset dialog with
      topic/partition/strategy selector. Calls POST reset endpoint.

      **Tests:**
      - [ ] `vitest` — click "Reset Offsets" opens strategy selector
      - [ ] `vitest` — "earliest" strategy sends correct request
      - [ ] `vitest` — ConfirmDialog shows current vs target offset

### Phase 8 — Build Integration

- [x] **`mitiflow-orchestrator/Cargo.toml` — `ui` feature flag.** Add
      `rust-embed` and `mime_guess` as optional deps behind `ui` feature.

- [x] **`justfile` — `ui-build` and `build-with-ui` recipes.**

- [x] **`mitiflow-ui/vite.config.ts` — dev proxy for `/api`.**

- [ ] **CI — build frontend + embed in release binary.**

      **Tests:**
      - [ ] CI job: `cd mitiflow-ui && pnpm install && pnpm build`
            succeeds
      - [ ] CI job: `cargo build -p mitiflow-orchestrator --features ui`
            succeeds
      - [ ] CI job: `cargo test -p mitiflow-orchestrator --features ui`
            passes all tests including UI serving

### E2E Integration Tests

These tests run the full stack: orchestrator + agent + frontend.

- [ ] **`e2e_gui_topic_lifecycle`** — create topic via UI, verify agent
      spawns TopicWorker, delete via UI, verify worker stopped.
- [ ] **`e2e_gui_event_inspector`** — publish events, open Event Inspector,
      verify events visible with correct payload.
- [ ] **`e2e_gui_lag_live`** — start consumer group, publish events without
      consuming, verify lag dashboard shows increasing lag via SSE.
- [ ] **`e2e_gui_drain_undrain`** — drain node via UI, verify partition
      migration, undrain, verify partitions return.

### Depends on

- Orchestrator (Phase 2) — HTTP API, ClusterView, LagMonitor. ✅ Done.
- Distributed Storage Management (Tier 1+2). ✅ Done.
- Multi-Topic Agent (Phases A–F). ✅ Done.

---

## Documentation

- [x] Update [00_proposal.md](00_proposal.md) — status markers, actual API
      examples, crate structure table.
- [ ] Add `ARCHITECTURE.md` at repo root as a quick-start pointer to the docs.
- [x] Add [08_replay_ordering.md](08_replay_ordering.md) — deterministic replay
      ordering via HLC and publisher lifecycle management.
- [x] Add [11_consumer_group_commits.md](11_consumer_group_commits.md) —
      consumer group offset commits, generation fencing, and orchestrator design.
- [x] Add [12_consumer_group_e2e_tests.md](12_consumer_group_e2e_tests.md) —
      systematic e2e test plan for consumer group edge cases.
- [x] Add [15_key_based_publishing.md](15_key_based_publishing.md) —
      key-based publishing design: key expression layout, API, store key
      index, log compaction, Kafka comparison.
- [x] Add [16_dx_and_multi_topic.md](16_dx_and_multi_topic.md) —
      multi-topic agent, unified binary, topic provisioning protocol,
      orchestrator HTTP API, and developer experience improvements.
- [x] Add [17_slow_consumer_offload.md](17_slow_consumer_offload.md) —
      automatic slow consumer offload from pub/sub to store-query replay,
      three-state consumer model, lag detection, and catch-up protocol.
