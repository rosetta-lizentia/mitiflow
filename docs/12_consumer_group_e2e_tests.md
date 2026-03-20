# 12 — Consumer Group E2E Test Plan

Systematic emulation tests for consumer group offset commits, generation
fencing, rebalancing, and failure recovery. All tests run in-process using
Zenoh's peer-to-peer mode — no external services required.

**Ref:** [11_consumer_group_commits.md](11_consumer_group_commits.md)

---

## Test Infrastructure

### Shared Helpers

Extend `tests/common/mod.rs` with consumer group utilities:

```rust
/// Create consumer group config with unique group_id per test.
fn group_config(test_name: &str) -> ConsumerGroupConfig {
    ConsumerGroupConfig {
        group_id: format!("test-group-{test_name}"),
        member_id: None,
        commit_mode: CommitMode::Manual,
        offset_reset: OffsetReset::Earliest,
    }
}

/// Create EventBusConfig with fast timeouts for testing.
fn cg_test_config(test_name: &str) -> EventBusConfig {
    EventBusConfig::builder(format!("test/{test_name}"))
        .num_partitions(4)
        .cache_size(256)
        .heartbeat(HeartbeatMode::Periodic(Duration::from_millis(200)))
        .watermark_interval(Duration::from_millis(50))
        .recovery_delay(Duration::from_millis(30))
        .max_recovery_attempts(3)
        .build()
        .unwrap()
}

/// Start an EventStore for a single partition, returns handle for shutdown.
async fn start_store(
    session: &Session,
    config: &EventBusConfig,
    partition: u32,
    dir: &Path,
) -> EventStore {
    let backend = FjallBackend::open(dir, partition).unwrap();
    let mut store = EventStore::new(session, backend, config.clone());
    store.run().await.unwrap();
    store
}

/// Publish N events to a specific partition via key routing.
async fn publish_to_partition(
    publisher: &EventPublisher,
    partition: u32,
    count: u64,
    start_value: u64,
) {
    for i in start_value..(start_value + count) {
        let key = format!("p/{partition}/{i}");
        publisher.publish_to(&key, &Event::new(TestPayload { value: i }))
            .await.unwrap();
    }
}

/// Wait for rebalance to settle by polling partition assignments.
async fn wait_rebalance(pms: &[&PartitionManager], timeout: Duration) {
    let start = Instant::now();
    loop {
        let total: usize = futures::future::join_all(
            pms.iter().map(|pm| pm.my_partitions())
        ).await.iter().map(|p| p.len()).sum();
        if total == pms[0].config().num_partitions as usize {
            break;
        }
        if start.elapsed() > timeout {
            panic!("rebalance did not settle within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
```

### Convention

All tests use:
- `#[tokio::test(flavor = "multi_thread", worker_threads = 2)]` (Zenoh
  requires multi-thread Tokio runtime).
- `temp_dir(test_name)` for fjall storage with auto-cleanup.
- Short timeouts (200ms heartbeat, 50ms watermark) for fast test execution.
- Unique key prefixes per test to avoid cross-test interference within a
  shared Zenoh session.

---

## Category 1: Basic Offset Commit & Fetch

### Test 1.1: `commit_and_fetch_round_trip`

**What:** A single consumer commits offsets, then a new consumer fetches them.

```
Setup:
  - 1 EventStore (partition 0)
  - 1 publisher → publishes 20 events
  - 1 consumer (group "g1") → processes all 20, commits

Verify:
  - commit_sync() returns Ok
  - New consumer joins group "g1", calls load_offsets(0)
  - GapDetector is seeded with {publisher_id: 20}
  - Subsequent events from seq 21+ are delivered
  - Events seq 1-20 are treated as duplicates
```

**Edge case tested:** Basic correctness of the commit → fetch → seed pipeline.

### Test 1.2: `commit_multiple_publishers`

**What:** Consumer commits offsets for multiple publishers on the same partition.

```
Setup:
  - 1 EventStore (partition 0)
  - 3 publishers: P1 publishes 10, P2 publishes 5, P3 publishes 15
  - 1 consumer → processes all, commits

Verify:
  - Fetched offsets contain all 3 publishers: {P1:10, P2:5, P3:15}
  - New consumer resumes correctly per-publisher
  - P1 events 1-10: duplicate. P1 event 11: delivered.
  - P2 events 1-5: duplicate. P2 event 6: delivered.
  - P3 events 1-15: duplicate. P3 event 16: delivered.
```

### Test 1.3: `commit_across_partitions`

**What:** Consumer in a group owns multiple partitions and commits them all.

```
Setup:
  - 4 EventStores (partitions 0-3)
  - 1 publisher → writes to all 4 partitions via key routing
  - 1 consumer (solo group) → owns all 4 partitions, processes, commits

Verify:
  - Each partition's store has its own committed offsets
  - Fetch from each partition returns correct per-publisher sequences
  - After restart, consumer resumes from committed position on all partitions
```

### Test 1.4: `commit_sync_vs_async`

**What:** Verify `commit_sync()` provides confirmation while `commit_async()`
is fire-and-forget.

```
Setup:
  - 1 EventStore (partition 0)
  - 1 publisher → publishes 10 events
  - 1 consumer → processes all 10

Test commit_sync:
  - commit_sync() returns Ok(())
  - Immediately fetch offsets → returns {pub_id: 10}

Test commit_async:
  - Process 10 more events (seq 11-20)
  - commit_async() returns Ok(()) immediately
  - Wait 100ms for async propagation
  - Fetch offsets → returns {pub_id: 20}
```

### Test 1.5: `offset_reset_earliest_vs_latest`

**What:** Verify `OffsetReset` behavior when no committed offset exists.

```
Setup:
  - 1 EventStore (partition 0) with 50 pre-existing events
  - No prior commits for group "g-new"

OffsetReset::Earliest:
  - Consumer joins → receives all 50 events from the beginning
  - (Actually receives via store query replay)

OffsetReset::Latest:
  - Consumer joins → receives nothing from history
  - Only events published after joining are delivered

OffsetReset::Error:
  - Consumer join → returns Err(NoCommittedOffset)
```

---

## Category 2: Rebalance & Offset Handoff

### Test 2.1: `rebalance_transfers_offsets`

**What:** When a partition moves from C0 to C1, C1 resumes from C0's committed
position (no duplicate processing).

```
Timeline:
  t0: Store (partition 0) + publisher + C0 (group "g1") running
      Publisher produces events 1-100
      C0 processes and commits at event 50: {pub: 50}

  t1: C1 joins group "g1"
      Rebalance: C0 commits final offset {pub: 75} for lost partitions
      C1 gains partition 0, fetches offset {pub: 75} from store

  t2: Publisher produces events 101-150
      C1 receives events 76-150 (continues from C0's committed position)

Verify:
  - C1 never receives events 1-75
  - No gap in delivery: 76, 77, 78, ..., 150
  - C0 no longer receives events for partition 0
```

### Test 2.2: `rebalance_commit_before_release`

**What:** On rebalance, the leaving consumer commits offsets for lost partitions
before releasing them.

```
Timeline:
  t0: C0 owns partitions 0-3, has processed up to:
      P0: {pub: 42}, P1: {pub: 31}, P2: {pub: 55}, P3: {pub: 18}
      But only committed P0:{pub:30}, P1:{pub:20}, P2:{pub:40}, P3:{pub:10}

  t1: C1 joins → rebalance
      C0's on_rebalance fires for lost partitions (say P2, P3)
      C0 commits final offsets for P2, P3: {pub:55}, {pub:18}
      Then C0 releases P2, P3

  t2: C1 loads offsets for P2, P3
      Gets: P2:{pub:55}, P3:{pub:18} (the final committed values)

Verify:
  - C1 starts at seq 56 for P2, seq 19 for P3
  - Not at the stale committed values (40, 10)
```

### Test 2.3: `rapid_rebalance_three_consumers`

**What:** A third consumer joins while a rebalance is still settling, causing
a cascading rebalance.

```
Timeline:
  t0: C0 owns all 8 partitions

  t1: C1 joins → rebalance in progress (C0 committing, C1 loading)

  t2: C2 joins before t1 settles → second rebalance triggered
      All three consumers see the membership change
      Assignment stabilizes to ~3/3/2 partitions

Verify:
  - All 8 partitions are assigned exactly once
  - No partition is unowned after settling
  - Committed offsets are not lost during cascading rebalance
  - All consumers can commit and fetch offsets for their final assignments
```

### Test 2.4: `consumer_leaves_gracefully`

**What:** Consumer shuts down cleanly — commits final offsets, other members
absorb its partitions.

```
Timeline:
  t0: C0 owns P0-P3, C1 owns P4-P7
      Both processing events, periodically committing

  t1: C0 calls shutdown()
      C0 commits final offsets for P0-P3
      C0 drops liveliness token
      C1 detects C0 left via liveliness → rebalance
      C1 gains P0-P3, loads C0's final committed offsets

Verify:
  - C1 resumes each of C0's partitions exactly where C0 left off
  - No events are reprocessed
  - No events are lost
```

### Test 2.5: `all_consumers_restart`

**What:** All consumers in a group crash simultaneously. New instances recover
from the store.

```
Timeline:
  t0: C0, C1 each own 4 partitions. Both commit at various points.

  t1: Both C0 and C1 crash (drop sessions)

  t2: C2, C3 start as new members of the same group
      They discover each other, split 8 partitions
      They fetch committed offsets from the stores

Verify:
  - C2/C3 resume from C0/C1's last committed positions (not from beginning)
  - Each partition's offset is the last committed by whichever consumer owned it
```

---

## Category 3: Generation Fencing

### Test 3.1: `zombie_commit_rejected`

**What:** A zombie consumer's stale commit is rejected by the store.

```
Timeline:
  t0: C0 owns partition 0, generation=1. Commits {pub: 50, gen:1}.

  t1: Simulate C0 becoming a zombie:
      - C0's liveliness times out
      - C1 joins, gains partition 0, generation=2
      - C1 loads old offset {pub: 50}
      - C1 processes events 51-100, commits {pub: 100, gen:2}

  t2: Zombie C0 attempts to commit {pub: 60, gen:1}
      Store rejects: StaleFencedCommit { commit_gen:1, stored_gen:2 }

Verify:
  - Store still holds {pub: 100, gen:2} (C1's commit)
  - C0's stale offset is never persisted
  - fetch_offsets returns {pub: 100}, not 60
```

### Test 3.2: `generation_increments_on_every_rebalance`

**What:** The generation counter increments on each membership change.

```
Timeline:
  t0: C0 starts → generation=1 (initial)
  t1: C1 joins  → both see rebalance → generation=2
  t2: C2 joins  → all three see rebalance → generation=3
  t3: C1 leaves → C0, C2 see rebalance → generation=4

Verify:
  - C0.current_generation() == 4 at end
  - C2.current_generation() == 4 (started at gen=3, saw one more rebalance)
  - All commits from each phase carry the correct generation
```

### Test 3.3: `same_generation_commit_accepted`

**What:** Two consumers with the same generation (edge case during rebalance)
can both commit — the `>=` rule allows it.

```
Setup:
  - C0 and C1 both see the same rebalance event simultaneously
  - Both increment to generation=2
  - C0 still owns partition 0 (assignment unchanged for C0)

Action:
  - C0 commits {pub: 50, gen:2} → accepted (no stored gen yet)
  - C1 commits for a different partition → accepted
  - No StaleFencedCommit errors

Verify:
  - The >= check passes for equal generations
  - Both commits are persisted correctly
```

### Test 3.4: `zombie_detects_fencing_and_stops`

**What:** When a zombie consumer receives `StaleFencedCommit`, it should stop
processing (it no longer owns the partition).

```
Timeline:
  t0: C0 owns partition 0, generation=1. Processing events.

  t1: C0's network partitions (liveliness lost)
      C1 joins, gains partition 0, generation=2
      C1 commits with generation=2

  t2: C0's network recovers. C0 tries to commit.
      Store returns StaleFencedCommit.

Verify:
  - C0 receives the error
  - C0 should realize it's been fenced and trigger internal recovery
    (e.g., re-join the group, acquire new partitions)
  - C0 does NOT continue processing events for partition 0
```

---

## Category 4: Store Unavailability & Recovery

### Test 4.1: `commit_during_store_down`

**What:** Consumer tries to commit while the EventStore is temporarily down.

```
Timeline:
  t0: Store + publisher + consumer running. Consumer commits → Ok.

  t1: Store shuts down.

  t2: Consumer processes more events. commit_sync() times out or returns error.
      commit_async() returns Ok (fire-and-forget) but offset is lost.

  t3: Store comes back up.

  t4: Consumer commits again → Ok. Latest position is persisted.

Verify:
  - commit_sync() during store downtime returns Err (timeout or no reply)
  - commit_async() during store downtime returns Ok (but offset not persisted)
  - After store recovery, commits work normally
  - A new consumer joining after store recovery gets the last successful commit
    (from t0), not the attempts during downtime
```

### Test 4.2: `fetch_during_store_down`

**What:** New consumer tries to fetch offsets while store is down.

```
Timeline:
  t0: C0 commits offsets, then leaves.
  t1: Store goes down.
  t2: C1 joins, tries to load_offsets() → times out
  t3: Behavior depends on OffsetReset:
      - Earliest: C1 replays from beginning (when store comes back and can serve queries)
      - Latest: C1 starts from live events, accepting potential gap
      - Error: C1 fails to start

Verify:
  - load_offsets() returns error on timeout
  - OffsetReset fallback behavior activates
  - When store recovers, subsequent load_offsets() succeeds
```

### Test 4.3: `store_crash_and_recovery`

**What:** EventStore crashes after committing some offsets. On restart, offsets
are recovered from fjall.

```
Timeline:
  t0: Consumer commits offsets at {pub: 50}
  t1: Store confirms commit (via commit_sync)
  t2: Store crashes (process kill / drop without graceful shutdown)
  t3: Store restarts with same fjall directory
  t4: New consumer fetches offsets → gets {pub: 50}

Verify:
  - fjall durability: committed offsets survive process crash
  - No data corruption in the offsets keyspace
  - Store queryable works immediately after restart
```

### Test 4.4: `offset_and_event_consistency`

**What:** Verify that if a consumer committed offset N, events 1..N are
actually present in the store (offsets and events are in the same fjall DB).

```
Setup:
  - Publisher sends 100 events to partition 0
  - Consumer processes all 100, commits at {pub: 100}
  - Store is restarted

Verify:
  - fetch_offsets returns {pub: 100}
  - Store query for events 1-100 returns all 100 events
  - No inconsistency between committed offset and available events
```

---

## Category 5: Auto-Commit

### Test 5.1: `auto_commit_interval`

**What:** Auto-commit fires at the configured interval.

```
Setup:
  - Consumer with CommitMode::Auto { interval: 200ms }
  - Publisher sends events continuously

Verify:
  - After 200ms, offsets are committed (fetch from store shows progress)
  - After 400ms, offsets advance further
  - The committed offset lags behind the processed position by at most
    ~interval (200ms worth of events)
```

### Test 5.2: `auto_commit_on_shutdown`

**What:** Auto-commit fires a final commit on graceful shutdown.

```
Timeline:
  t0: Consumer with auto-commit, processing events
  t1: Consumer processes event 100
  t2: Consumer calls shutdown() (before next auto-commit interval)
      The cancellation-triggered final commit fires

Verify:
  - After shutdown, stored offset reflects the position at shutdown time
  - Not the last auto-commit position (which could be stale)
```

### Test 5.3: `auto_commit_survives_transient_failure`

**What:** If one auto-commit fails (store temporarily unreachable), the next
interval retries with updated offsets.

```
Timeline:
  t0: Auto-commit at {pub: 50} → Ok
  t1: Store goes down
  t2: Auto-commit at {pub: 75} → fails silently (warn log)
  t3: Store comes back
  t4: Auto-commit at {pub: 100} → Ok

Verify:
  - Stored offset jumps from 50 to 100 (skipping the failed 75)
  - No crash, no panic, just a warning log
  - Consumer continues processing uninterrupted
```

---

## Category 6: Multi-Publisher & Multi-Partition

### Test 6.1: `offset_per_publisher_independence`

**What:** Committing offsets for one publisher doesn't affect another.

```
Setup:
  - P1 publishes 100 events, P2 publishes 50 events to partition 0
  - Consumer processes 80 from P1 and all 50 from P2
  - Consumer commits

Verify:
  - Stored offsets: {P1: 80, P2: 50}
  - New consumer resumes: P1 from 81, P2 from 51
  - P1 events 81-100 are delivered
  - P2 receives only new events (51+), none from 1-50
```

### Test 6.2: `dead_publisher_offset_persistence`

**What:** Offsets for a dead publisher persist and are fetched correctly.

```
Timeline:
  t0: P1 publishes 30 events. Consumer commits {P1: 30}.
  t1: P1 goes offline (liveliness revoked, lifecycle → ARCHIVED)
  t2: P2 starts publishing to the same partition
  t3: Consumer commits {P1: 30, P2: 15}
  t4: New consumer joins, loads offsets

Verify:
  - Fetched offsets include both {P1: 30, P2: 15}
  - P1's offset is still present even though P1 is dead
  - GapDetector is seeded for both publishers
  - If P1 ever comes back, events from P1 with seq > 30 are delivered
```

### Test 6.3: `partition_specific_commit`

**What:** `commit_partition_sync(partition)` commits only one partition.

```
Setup:
  - Consumer owns partitions 0 and 1
  - Partition 0: processed up to {pub: 100}
  - Partition 1: processed up to {pub: 50}

Action:
  - commit_partition_sync(0) → Ok

Verify:
  - Partition 0 store: offset {pub: 100}
  - Partition 1 store: no offset committed (or previous stale value)
```

---

## Category 7: Concurrent Consumer Groups

### Test 7.1: `independent_groups_same_topic`

**What:** Two different consumer groups consume the same topic independently.

```
Setup:
  - 1 EventStore (partition 0)
  - 1 publisher → 100 events
  - Group "analytics": 1 consumer, commits at {pub: 100}
  - Group "billing": 1 consumer, commits at {pub: 60}

Verify:
  - Groups have independent offset namespaces
  - Fetching offsets for "analytics" returns {pub: 100}
  - Fetching offsets for "billing" returns {pub: 60}
  - Neither group's commit affects the other
```

### Test 7.2: `group_isolation_across_generations`

**What:** Different groups have independent generation counters.

```
Setup:
  - Group "g1": C0, C1 (generation=2 after rebalance)
  - Group "g2": C2 alone (generation=1, no rebalance)

Action:
  - g1 consumer commits with generation=2
  - g2 consumer commits with generation=1

Verify:
  - Both commits succeed (generations are per-group, not global)
  - Fetching g1 offsets returns generation=2
  - Fetching g2 offsets returns generation=1
```

---

## Category 8: Edge Cases & Stress

### Test 8.1: `commit_empty_offsets`

**What:** Consumer commits with an empty offset map (no events processed yet).

```
Action:
  - Consumer joins, subscribes, but no events arrive
  - Consumer calls commit_sync()

Verify:
  - Commit succeeds (empty map is a valid commit)
  - fetch_offsets returns empty HashMap
  - No crash, no error
```

### Test 8.2: `commit_overwrites_previous`

**What:** Successive commits overwrite the stored offset.

```
Timeline:
  t0: commit {pub: 10, gen:1}
  t1: commit {pub: 20, gen:1}
  t2: commit {pub: 30, gen:1}

Verify:
  - fetch_offsets returns {pub: 30} (latest commit wins)
  - The store does not accumulate offset history
```

### Test 8.3: `commit_with_backward_offset`

**What:** Consumer commits an offset lower than the previously committed one
(e.g., after a seek-to-beginning operation).

```
Timeline:
  t0: commit {pub: 100, gen:1}
  t1: Consumer seeks to beginning, reprocesses
  t2: commit {pub: 50, gen:1} (backward offset, same generation)

Verify:
  - Commit is accepted (the store does not validate offset monotonicity,
    only generation monotonicity)
  - fetch_offsets returns {pub: 50}
  - Philosophy: the consumer knows what it's doing when it resets offsets
```

### Test 8.4: `high_frequency_commits`

**What:** Consumer commits after every single event (worst-case commit load).

```
Setup:
  - Publisher sends 500 events
  - Consumer calls commit_sync() after each event

Verify:
  - All 500 commits succeed
  - Final offset is {pub: 500}
  - Store handles high write rate without corruption
  - Total test time is bounded (< 10s, ideally < 5s)
```

### Test 8.5: `large_publisher_map_commit`

**What:** Consumer commits offsets for a large number of publishers.

```
Setup:
  - 50 publishers each write 10 events to partition 0
  - Consumer processes all 500 events
  - Consumer commits: {P1:10, P2:10, ..., P50:10}

Verify:
  - Commit succeeds
  - fetch_offsets returns all 50 publisher entries
  - Each publisher's offset is correct
  - Serialization/deserialization handles large maps
```

### Test 8.6: `concurrent_commits_same_partition`

**What:** Two consumers from different groups commit to the same partition
simultaneously.

```
Setup:
  - Group "g1" consumer and Group "g2" consumer both own partition 0
  - Both process events and commit concurrently (tokio::join!)

Verify:
  - Both commits succeed (different group_id → different keys)
  - No data corruption or key collision
  - Each group's offset is independent
```

### Test 8.7: `store_restart_preserves_all_groups`

**What:** After store restart, offsets for all consumer groups are intact.

```
Setup:
  - 3 groups each commit different offsets to partition 0
  - Store restarts (drop + reopen with same directory)

Verify:
  - All 3 groups' offsets are recoverable
  - No cross-group contamination
```

---

## Category 9: Rebalance Timing & Ordering

### Test 9.1: `rebalance_during_commit`

**What:** A rebalance triggers while a `commit_sync()` is in flight.

```
Timeline:
  t0: C0 starts commit_sync() for partition 0 (query sent to store)
  t1: C1 joins → rebalance triggered
  t2: Store replies to C0's commit (commit succeeds)
  t3: Rebalance fires: C0 commits again for lost partitions, then releases

Verify:
  - The in-flight commit from t0 succeeds
  - The rebalance-triggered commit from t3 also succeeds (may overwrite with same or newer offset)
  - C1 loads the latest committed offset (whichever is higher)
  - No lost events, no corrupted state
```

### Test 9.2: `rebalance_lost_partition_commit_failure`

**What:** During rebalance, the commit for a lost partition fails (store
unreachable). The new owner falls back to a stale offset.

```
Timeline:
  t0: C0 owns partition 0, committed offset {pub: 50}
      C0 has processed up to seq 80

  t1: Store for partition 0 goes offline
  t2: Rebalance: C0 tries to commit {pub: 80} for partition 0 → fails
  t3: C1 gains partition 0, tries load_offsets → fails (store down)
  t4: Store comes back. C1 retries load_offsets → gets {pub: 50} (stale)

Verify:
  - C1 starts from seq 51 (stale offset), reprocesses events 51-80
  - This is at-least-once: duplicates happen, but no events are lost
  - After C1 processes and commits, the offset catches up
```

### Test 9.3: `partition_ownership_exclusive`

**What:** At no point do two consumers in the same group process the same
partition simultaneously (after rebalance settles).

```
Setup:
  - 8 partitions, 3 consumers
  - Run for 2 seconds with publishers continuously sending events

Verify:
  - At any snapshot, the union of all consumers' assigned partitions == {0..7}
  - No partition appears in two consumers' assignments simultaneously
  - Events received by each consumer are only from their assigned partitions
```

---

## Category 10: Offset Keyspace Isolation

### Test 10.1: `offsets_keyspace_does_not_interfere_with_events`

**What:** Adding the `offsets` keyspace to fjall doesn't affect event storage
or queries.

```
Setup:
  - Store with offsets keyspace
  - Publisher writes 100 events
  - Consumer commits offsets for 3 groups

Verify:
  - query(&QueryFilters::default()) returns exactly the 100 events
  - query_replay() returns events in correct HLC order
  - Offset data does not appear in event queries
  - Event data does not appear in offset queries
```

### Test 10.2: `offset_gc_does_not_affect_event_gc`

**What:** GC of events does not remove offsets, and vice versa.

```
Setup:
  - Store with 100 events and committed offsets
  - Run event GC (e.g., retain last 50 events)

Verify:
  - 50 events remain
  - Committed offsets are unaffected
  - A new consumer can still fetch offsets even though some events are GC'd
```

---

## Category 11: Wire Protocol Correctness

### Test 11.1: `offset_commit_serialization_round_trip`

**What:** `OffsetCommit` survives serialization → Zenoh transport →
deserialization without data loss.

```rust
let commit = OffsetCommit {
    group_id: "test-group".into(),
    member_id: "member-0".into(),
    partition: 3,
    offsets: HashMap::from([
        (PublisherId::from_bytes([0u8; 16]), 42),
        (PublisherId::from_bytes([255u8; 16]), u64::MAX),
    ]),
    generation: 7,
    timestamp: Utc::now(),
};

let bytes = serde_json::to_vec(&commit).unwrap();
let decoded: OffsetCommit = serde_json::from_slice(&bytes).unwrap();

assert_eq!(commit.group_id, decoded.group_id);
assert_eq!(commit.offsets, decoded.offsets);
assert_eq!(commit.generation, decoded.generation);
```

### Test 11.2: `offset_key_encoding_determinism`

**What:** The offset key encoding is deterministic — same inputs always
produce the same key bytes.

```rust
let key1 = offset_key("my-group", &pub_id);
let key2 = offset_key("my-group", &pub_id);
assert_eq!(key1, key2);

// Different group → different key
let key3 = offset_key("other-group", &pub_id);
assert_ne!(key1, key3);

// Different publisher → different key
let key4 = offset_key("my-group", &other_pub_id);
assert_ne!(key1, key4);
```

### Test 11.3: `subscriber_key_expression_for_offsets`

**What:** The `_offsets/` key expression is properly namespaced and doesn't
collide with event key expressions.

```rust
let config = EventBusConfig::builder("app/orders")
    .num_partitions(4)
    .build()?;

// Event keys: app/orders/p/{partition}/{seq}
// Offset keys: app/orders/_offsets/{partition}/{group_id}

// Assert no overlap
assert!(!zenoh_keyexpr::intersect("app/orders/p/**", "app/orders/_offsets/**"));
```

---

## Test Execution Strategy

### Unit tests (fast, no Zenoh)

Run without Zenoh session — test pure logic:
- Test 11.1: serialization round-trip
- Test 11.2: key encoding determinism
- `commit_offsets()` / `fetch_offsets()` on bare `FjallBackend`
- Generation fencing logic on bare `FjallBackend`

### Integration tests (medium, in-process Zenoh)

Run with `zenoh::open(Config::default())` in peer mode:
- All Category 1-10 tests
- Use `#[tokio::test(flavor = "multi_thread", worker_threads = 2)]`
- Each test gets its own key prefix and temp dir

### Stress tests (slow, opt-in via feature flag)

Run with `#[cfg(feature = "stress-tests")]`:
- Test 8.4: high-frequency commits (500 sequential commits)
- Test 8.5: large publisher map (50 publishers)
- Extended versions of rebalance tests with longer durations

### Suggested file layout

```
tests/
  consumer_group_commit.rs    # Categories 1, 5, 8 (commit mechanics)
  consumer_group_rebalance.rs # Categories 2, 9 (rebalance + offset handoff)
  consumer_group_fencing.rs   # Category 3 (generation fencing)
  consumer_group_recovery.rs  # Category 4 (store unavailability)
  consumer_group_multi.rs     # Categories 6, 7 (multi-publisher, multi-group)
  consumer_group_store.rs     # Category 10 (keyspace isolation)
  common/
    mod.rs                    # Extended with group_config(), cg_test_config(), etc.
```

Unit tests for serialization and key encoding go in `src/store/backend.rs`
as `#[cfg(test)] mod tests`.
