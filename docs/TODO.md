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

- [ ] **`examples/consumer_groups.rs` — update.** Demonstrate
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

**Status:** Not started
**Ref:** [15_key_based_publishing.md](15_key_based_publishing.md)

Kafka-style message keys embedded in Zenoh key expressions. Enables automatic
partition affinity, Zenoh-native key filtering (server-side), key-scoped replay,
and log compaction.

**Key expression layout:** `{prefix}/p/{partition}/k/{key}/{seq}` (keyed)
coexists with `{prefix}/p/{partition}/{seq}` (unkeyed, backward compatible).

### Phase 1 — Core keyed publish

- [ ] **Key validation** — reject `*`, `$`, empty keys at publish time.
- [ ] **`publish_keyed()` family** — `publish_keyed`, `publish_bytes_keyed`,
      `publish_keyed_durable`, `publish_bytes_keyed_durable` on
      `EventPublisher`. Internal: `hash(key) % num_partitions → partition`,
      construct `{prefix}/p/{partition}/k/{key}/{seq}`.
- [ ] **`RawEvent::key()` accessor** — parse key from `key_expr` via `/k/`
      sentinel. Zero allocation (`&str` slice).
- [ ] **`extract_partition()` update** — handle both keyed
      (`{prefix}/p/{partition}/k/{key}/{seq}`) and unkeyed layouts.
- [ ] **Config helpers** — `key_expr_for_key()`,
      `key_expr_for_key_prefix()` on `EventBusConfig`.
- [ ] **Tests** — publish with key, subscribe with key filter, round-trip key
      extraction, partition affinity for same key.

### Phase 2 — Store key index

- [ ] **`keys` keyspace** in `FjallBackend`. Key:
      `[key_hash:8][hlc_physical:8 BE][hlc_logical:4 BE][publisher_id:16]`.
      Value: `[publisher_id:16][seq:8 BE]` (pointer to primary index).
- [ ] **Write path** — index key on `store()` when key is present in
      `EventMetadata`.
- [ ] **`EventMetadata` extension** — add `key: Option<String>` field.
- [ ] **`query_by_key()`** — key-scoped queries on `StorageBackend`.

### Phase 3 — Log compaction

- [ ] **Background compaction task** — periodic scan of key index, retain
      only highest-HLC entry per key_hash, delete superseded entries from
      all three indexes (primary, replay, keys).
- [ ] **Tombstone handling** — null-payload keyed events as delete markers.
      Configurable tombstone retention period.
- [ ] **`query_latest_by_keys()`** — compacted view query.
- [ ] **Retention policy config** — compaction interval, tombstone GC period.

### Phase 4 — Subscriber convenience

- [ ] **`EventSubscriber::new_keyed()`** — subscribe to a specific key.
- [ ] **`EventSubscriber::new_key_prefix()`** — subscribe to a key prefix.
- [ ] **Example** — `examples/keyed_pubsub.rs`.

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

## Documentation

- [ ] Update [00_proposal.md](00_proposal.md) § watermark example once
      per-partition sequences are implemented (currently shows per-publisher
      which is correct for now).
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
