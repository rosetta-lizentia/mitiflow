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
- [ ] **Admin API (HTTP REST).** Not implemented — only Zenoh queryable exists.
- [ ] **Automated store provisioning.** `create_topic()` → spawn N × RF store
      instances. Currently `create_topic()` persists config and publishes it
      but does not spawn stores.
- [ ] **Consumer group sessions (optional).** JoinGroup/SyncGroup/Heartbeat
      protocol via Zenoh queryable. Assigns globally unique generation IDs.
      Fallback: existing liveliness-based `PartitionManager` remains default.
- [ ] **Orchestrator HA.** Multiple replicas subscribe to same key expressions.
      Leader election via liveliness + lowest UUID for write ops. Any replica
      serves reads.

### Tests

- `mitiflow-orchestrator/tests/orchestrator.rs` — 8 tests covering config
  store CRUD, persistence, lag monitoring, store tracker online/offline,
  orchestrator topic lifecycle, admin queryable, and lag integration.

### Depends on

- Consumer Group Commits (Phase 1) — offset storage must exist before the
  orchestrator can aggregate lag. ✅ Done.

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
- [ ] **Rebalance operation** — load-aware override generation.
- [x] **CLI tooling** — `mitiflow-ctl` binary for cluster management.
      Commands: topics list/get, cluster nodes/assignments/drain/undrain/overrides/status.

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
- `mitiflow-agent/tests/e2e/scenarios.rs` — 13 E2E scenarios: cluster
  formation, rebalance, crash/rejoin, data survival across graceful leave
  and crash recovery, publish during rebalance, live ingestion.
- `mitiflow-agent/tests/smoke/scenarios.rs` — 3 smoke tests.
- `mitiflow-orchestrator/tests/orchestrator.rs` — 28 unit tests: config store,
  lag monitor, store tracker, ClusterView (6), OverrideManager (6), Drain (2),
  Admin API cluster endpoints (3), Multi-topic ClusterViews (3).
- `mitiflow-orchestrator/tests/e2e_orchestrator.rs` — 7 E2E scenarios:
  orchestrator joins running cluster, drain moves partitions, override to
  offline node, override TTL expiry, full drain/maintenance/undrain roundtrip,
  orchestrator restart rebuilds view, concurrent override + crash.

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
