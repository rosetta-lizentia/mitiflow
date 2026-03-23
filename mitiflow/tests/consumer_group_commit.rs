//! Consumer group offset commit E2E tests.
//!
//! Tests basic offset commit/fetch, generation fencing, auto-commit,
//! multi-publisher independence, and concurrent groups.
//!
//! All tests use Zenoh peer-to-peer mode — no external services required.

mod common;

use std::collections::HashMap;
use std::time::Duration;

use common::TestPayload;
use mitiflow::{
    CommitMode, ConsumerGroupConfig, ConsumerGroupSubscriber, Event, EventBusConfig,
    EventPublisher, EventStore, FjallBackend, HeartbeatMode, OffsetReset,
};

/// Create an EventBusConfig with fast timeouts for testing.
fn cg_test_config(test_name: &str) -> EventBusConfig {
    EventBusConfig::builder(format!("test/{test_name}"))
        .cache_size(1000)
        .heartbeat(HeartbeatMode::Periodic(Duration::from_millis(200)))
        .history_on_subscribe(false)
        .watermark_interval(Duration::from_millis(50))
        .num_partitions(1)
        .build()
        .expect("valid config")
}

/// Create a ConsumerGroupConfig.
fn group_config(test_name: &str) -> ConsumerGroupConfig {
    ConsumerGroupConfig {
        group_id: format!("group-{test_name}"),
        member_id: format!("member-{}", uuid::Uuid::now_v7()),
        commit_mode: CommitMode::Manual,
        offset_reset: OffsetReset::Earliest,
    }
}

fn auto_commit_group_config(test_name: &str, interval: Duration) -> ConsumerGroupConfig {
    ConsumerGroupConfig {
        group_id: format!("group-{test_name}"),
        member_id: format!("member-{}", uuid::Uuid::now_v7()),
        commit_mode: CommitMode::Auto { interval },
        offset_reset: OffsetReset::Earliest,
    }
}

/// Start an EventStore for a single partition.
async fn start_store(
    session: &zenoh::Session,
    config: EventBusConfig,
    dir: &std::path::Path,
    partition: u32,
) -> EventStore {
    let backend = FjallBackend::open(dir.join(partition.to_string()), partition).unwrap();
    let mut store = EventStore::new(session, backend, config);
    store.run().await.unwrap();
    store
}

/// Publish N events with sequential values to partition 0.
async fn publish_to_partition(publisher: &EventPublisher, count: u64, start_value: u64) {
    for i in start_value..(start_value + count) {
        publisher
            .publish(&Event::new(TestPayload { value: i }))
            .await
            .unwrap();
    }
}

// ==========================================================================
// Category 1: Basic Offset Commit & Fetch
// ==========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_and_fetch_round_trip() {
    let test_name = "cg_commit_fetch_rt";
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let config = cg_test_config(test_name);
    let tmp = common::temp_dir(test_name);

    // Start store for partition 0
    let store = start_store(&session, config.clone(), tmp.path(), 0).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create consumer BEFORE publishing so it receives live events
    let gc = group_config(test_name);
    let group_id = gc.group_id.clone();
    let c0 = ConsumerGroupSubscriber::new(&session, config.clone(), gc)
        .await
        .unwrap();

    // Now publish 20 events
    let publisher = EventPublisher::new(&session, config.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    publish_to_partition(&publisher, 20, 0).await;
    for _ in 0..20 {
        let _: Event<TestPayload> = tokio::time::timeout(Duration::from_secs(5), c0.recv())
            .await
            .expect("timed out")
            .expect("recv failed");
    }
    c0.commit_sync().await.unwrap();
    c0.shutdown().await;

    // New consumer with same group_id fetches committed offsets
    let gc2 = ConsumerGroupConfig {
        group_id: group_id.clone(),
        member_id: format!("member-{}", uuid::Uuid::now_v7()),
        commit_mode: CommitMode::Manual,
        offset_reset: OffsetReset::Earliest,
    };
    let c1 = ConsumerGroupSubscriber::new(&session, config.clone(), gc2)
        .await
        .unwrap();
    let offsets = c1.load_offsets(0).await.unwrap();
    assert!(!offsets.is_empty(), "should have committed offsets");

    // The committed offset should be >= 19 (seq for the 20th event)
    for (_, seq) in &offsets {
        assert!(*seq >= 19, "committed offset should be >= 19, got {seq}");
    }

    c1.shutdown().await;
    store.shutdown();
}

// ==========================================================================
// Category 2: Commit Multiple Publishers
// ==========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_multiple_publishers() {
    let test_name = "cg_multi_pub";
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let config = cg_test_config(test_name);
    let tmp = common::temp_dir(test_name);

    let store = start_store(&session, config.clone(), tmp.path(), 0).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create consumer BEFORE publishing
    let gc = group_config(test_name);
    let c0 = ConsumerGroupSubscriber::new(&session, config.clone(), gc)
        .await
        .unwrap();

    // Create 2 publishers, each sends events
    let p1 = EventPublisher::new(&session, config.clone()).await.unwrap();
    let p2 = EventPublisher::new(&session, config.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    publish_to_partition(&p1, 10, 0).await;
    publish_to_partition(&p2, 5, 100).await;
    for _ in 0..15 {
        let _: Event<TestPayload> = tokio::time::timeout(Duration::from_secs(5), c0.recv())
            .await
            .expect("timed out")
            .expect("recv failed");
    }
    c0.commit_sync().await.unwrap();

    // Verify offsets contain both publishers
    let offsets = c0.load_offsets(0).await.unwrap();
    assert!(
        offsets.len() >= 2,
        "should have offsets for both publishers, got {}",
        offsets.len()
    );

    c0.shutdown().await;
    store.shutdown();
}

// ==========================================================================
// Category 3: Generation Fencing — Zombie Commit Rejected
// ==========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zombie_commit_rejected() {
    let test_name = "cg_zombie_fenced";
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let config = cg_test_config(test_name);
    let tmp = common::temp_dir(test_name);

    let store = start_store(&session, config.clone(), tmp.path(), 0).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let publisher = EventPublisher::new(&session, config.clone()).await.unwrap();
    let pub_id = *publisher.publisher_id();
    publish_to_partition(&publisher, 10, 0).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let group_id = format!("group-{test_name}");

    // Simulate: Consumer C0 commits with generation=1
    let commit_gen1 = mitiflow::OffsetCommit {
        group_id: group_id.clone(),
        member_id: "c0".into(),
        partition: 0,
        offsets: HashMap::from([(pub_id, 5)]),
        generation: 1,
        timestamp: chrono::Utc::now(),
    };
    let key = format!("test/{test_name}/_offsets/0/{group_id}");
    let payload = serde_json::to_vec(&commit_gen1).unwrap();
    let replies = session.get(&key).payload(payload).await.unwrap();
    while let Ok(_reply) = replies.recv_async().await {}

    // C1 commits with generation=2 (simulating after rebalance)
    let commit_gen2 = mitiflow::OffsetCommit {
        group_id: group_id.clone(),
        member_id: "c1".into(),
        partition: 0,
        offsets: HashMap::from([(pub_id, 9)]),
        generation: 2,
        timestamp: chrono::Utc::now(),
    };
    let payload2 = serde_json::to_vec(&commit_gen2).unwrap();
    let replies2 = session.get(&key).payload(payload2).await.unwrap();
    while let Ok(_reply) = replies2.recv_async().await {}

    // Zombie C0 tries to commit with generation=1 → should be rejected
    let zombie_commit = mitiflow::OffsetCommit {
        group_id: group_id.clone(),
        member_id: "c0-zombie".into(),
        partition: 0,
        offsets: HashMap::from([(pub_id, 6)]),
        generation: 1,
        timestamp: chrono::Utc::now(),
    };
    let zombie_payload = serde_json::to_vec(&zombie_commit).unwrap();
    let zombie_replies = session.get(&key).payload(zombie_payload).await.unwrap();
    let mut was_rejected = false;
    while let Ok(reply) = zombie_replies.recv_async().await {
        if let Ok(sample) = reply.result() {
            let body = sample.payload().to_bytes();
            let body_str = std::str::from_utf8(&body).unwrap_or("");
            if body_str.starts_with("error:") && body_str.contains("stale fenced commit") {
                was_rejected = true;
            }
        }
    }
    assert!(was_rejected, "zombie commit should have been rejected");

    // Verify stored offset is still C1's value (9), not zombie's (6)
    let fetch_selector =
        format!("test/{test_name}/_offsets/0/{group_id}?fetch=true&group_id={group_id}");
    let fetch_replies = session.get(&fetch_selector).await.unwrap();
    while let Ok(reply) = fetch_replies.recv_async().await {
        if let Ok(sample) = reply.result() {
            let payload = sample.payload().to_bytes();
            if let Ok(commit) = serde_json::from_slice::<mitiflow::OffsetCommit>(&payload) {
                let stored_seq = commit.offsets.get(&pub_id).copied().unwrap_or(0);
                assert_eq!(stored_seq, 9, "stored offset should be C1's value (9)");
            }
        }
    }

    store.shutdown();
}

// ==========================================================================
// Category 4: Auto-Commit
// ==========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auto_commit_interval() {
    let test_name = "cg_auto_commit";
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let config = cg_test_config(test_name);
    let tmp = common::temp_dir(test_name);

    let store = start_store(&session, config.clone(), tmp.path(), 0).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Consumer with auto-commit every 200ms — created BEFORE publishing
    let gc = auto_commit_group_config(test_name, Duration::from_millis(200));
    let c0 = ConsumerGroupSubscriber::new(&session, config.clone(), gc)
        .await
        .unwrap();

    let publisher = EventPublisher::new(&session, config.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    publish_to_partition(&publisher, 10, 0).await;

    // Process events
    for _ in 0..10 {
        let _: Event<TestPayload> = tokio::time::timeout(Duration::from_secs(5), c0.recv())
            .await
            .expect("timed out")
            .expect("recv failed");
    }

    // Wait for auto-commit to fire (>200ms)
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify offsets were committed
    let offsets = c0.load_offsets(0).await.unwrap();
    assert!(
        !offsets.is_empty(),
        "auto-commit should have committed offsets"
    );

    c0.shutdown().await;
    store.shutdown();
}

// ==========================================================================
// Category 5: Offset Per-Publisher Independence
// ==========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offset_per_publisher_independence() {
    let test_name = "cg_pub_indep";
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let config = cg_test_config(test_name);
    let tmp = common::temp_dir(test_name);

    let store = start_store(&session, config.clone(), tmp.path(), 0).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create consumer BEFORE publishing
    let gc = group_config(test_name);
    let c0 = ConsumerGroupSubscriber::new(&session, config.clone(), gc)
        .await
        .unwrap();

    let p1 = EventPublisher::new(&session, config.clone()).await.unwrap();
    let p2 = EventPublisher::new(&session, config.clone()).await.unwrap();
    let p1_id = *p1.publisher_id();
    let p2_id = *p2.publisher_id();

    // P1 sends 10 events, P2 sends 5 events
    tokio::time::sleep(Duration::from_millis(100)).await;
    publish_to_partition(&p1, 10, 0).await;
    publish_to_partition(&p2, 5, 100).await;

    // Receive all 15 events
    for _ in 0..15 {
        let _: Event<TestPayload> = tokio::time::timeout(Duration::from_secs(5), c0.recv())
            .await
            .expect("timed out")
            .expect("recv failed");
    }

    // Commit
    c0.commit_sync().await.unwrap();
    let offsets = c0.load_offsets(0).await.unwrap();

    // Each publisher should have independent offsets
    assert!(offsets.contains_key(&p1_id), "should have offset for P1");
    assert!(offsets.contains_key(&p2_id), "should have offset for P2");
    assert!(
        offsets[&p1_id] >= 9,
        "P1 offset should be >= 9, got {}",
        offsets[&p1_id]
    );
    assert!(
        offsets[&p2_id] >= 4,
        "P2 offset should be >= 4, got {}",
        offsets[&p2_id]
    );

    c0.shutdown().await;
    store.shutdown();
}

// ==========================================================================
// Category 6: Concurrent Consumer Groups
// ==========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn independent_groups_same_topic() {
    let test_name = "cg_independent_groups";
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let config = cg_test_config(test_name);
    let tmp = common::temp_dir(test_name);

    let store = start_store(&session, config.clone(), tmp.path(), 0).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create both consumers BEFORE publishing
    let gc_analytics = ConsumerGroupConfig {
        group_id: format!("analytics-{test_name}"),
        member_id: format!("m-{}", uuid::Uuid::now_v7()),
        commit_mode: CommitMode::Manual,
        offset_reset: OffsetReset::Earliest,
    };
    let c_analytics = ConsumerGroupSubscriber::new(&session, config.clone(), gc_analytics)
        .await
        .unwrap();

    let gc_billing = ConsumerGroupConfig {
        group_id: format!("billing-{test_name}"),
        member_id: format!("m-{}", uuid::Uuid::now_v7()),
        commit_mode: CommitMode::Manual,
        offset_reset: OffsetReset::Earliest,
    };
    let c_billing = ConsumerGroupSubscriber::new(&session, config.clone(), gc_billing)
        .await
        .unwrap();

    let publisher = EventPublisher::new(&session, config.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    publish_to_partition(&publisher, 10, 0).await;

    // Group "analytics" receives and commits
    for _ in 0..10 {
        let _: Event<TestPayload> =
            tokio::time::timeout(Duration::from_secs(5), c_analytics.recv())
                .await
                .expect("timed out")
                .expect("recv failed");
    }
    c_analytics.commit_sync().await.unwrap();

    // Group "billing" receives and commits
    for _ in 0..10 {
        let _: Event<TestPayload> = tokio::time::timeout(Duration::from_secs(5), c_billing.recv())
            .await
            .expect("timed out")
            .expect("recv failed");
    }
    c_billing.commit_sync().await.unwrap();

    // Verify groups have independent offsets
    let analytics_offsets = c_analytics.load_offsets(0).await.unwrap();
    let billing_offsets = c_billing.load_offsets(0).await.unwrap();

    assert!(
        !analytics_offsets.is_empty(),
        "analytics should have offsets"
    );
    assert!(!billing_offsets.is_empty(), "billing should have offsets");

    c_analytics.shutdown().await;
    c_billing.shutdown().await;
    store.shutdown();
}

// ==========================================================================
// Category 7: Commit Sync vs Async
// ==========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_sync_vs_async() {
    let test_name = "cg_sync_vs_async";
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let config = cg_test_config(test_name);
    let tmp = common::temp_dir(test_name);

    let store = start_store(&session, config.clone(), tmp.path(), 0).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create consumer BEFORE publishing
    let gc = group_config(test_name);
    let c0 = ConsumerGroupSubscriber::new(&session, config.clone(), gc)
        .await
        .unwrap();

    let publisher = EventPublisher::new(&session, config.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    publish_to_partition(&publisher, 10, 0).await;

    for _ in 0..10 {
        let _: Event<TestPayload> = tokio::time::timeout(Duration::from_secs(5), c0.recv())
            .await
            .expect("timed out")
            .expect("recv failed");
    }

    // commit_sync should return Ok
    c0.commit_sync().await.unwrap();

    // Immediately fetch offsets → should reflect committed position
    let offsets = c0.load_offsets(0).await.unwrap();
    assert!(
        !offsets.is_empty(),
        "sync commit should persist immediately"
    );

    // Now test commit_async
    publish_to_partition(&publisher, 5, 10).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    for _ in 0..5 {
        let _: Event<TestPayload> = tokio::time::timeout(Duration::from_secs(5), c0.recv())
            .await
            .expect("timed out")
            .expect("recv failed");
    }

    c0.commit_async().await.unwrap();
    // Wait for async commit to be processed
    tokio::time::sleep(Duration::from_millis(300)).await;

    let offsets2 = c0.load_offsets(0).await.unwrap();
    // The offset should have advanced beyond the first commit
    for (_, seq) in &offsets2 {
        assert!(*seq >= 14, "async commit should advance offsets, got {seq}");
    }

    c0.shutdown().await;
    store.shutdown();
}

// ==========================================================================
// Category 8: Store Crash and Recovery
// ==========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn store_crash_and_offset_recovery() {
    let test_name = "cg_store_recovery";
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let config = cg_test_config(test_name);
    let tmp = common::temp_dir(test_name);

    // Phase 1: commit offsets
    {
        let store = start_store(&session, config.clone(), tmp.path(), 0).await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create consumer BEFORE publishing
        let gc = group_config(test_name);
        let c0 = ConsumerGroupSubscriber::new(&session, config.clone(), gc)
            .await
            .unwrap();

        let publisher = EventPublisher::new(&session, config.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        publish_to_partition(&publisher, 10, 0).await;
        for _ in 0..10 {
            let _: Event<TestPayload> = tokio::time::timeout(Duration::from_secs(5), c0.recv())
                .await
                .expect("timed out")
                .expect("recv failed");
        }
        c0.commit_sync().await.unwrap();
        c0.shutdown().await;
        store.shutdown_gracefully().await;
    }

    // Phase 2: restart store and verify offsets survive
    tokio::time::sleep(Duration::from_millis(200)).await;
    {
        let store = start_store(&session, config.clone(), tmp.path(), 0).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let gc = group_config(test_name);
        let c1 = ConsumerGroupSubscriber::new(&session, config.clone(), gc)
            .await
            .unwrap();
        let offsets = c1.load_offsets(0).await.unwrap();
        assert!(!offsets.is_empty(), "offsets should survive store restart");
        for (_, seq) in &offsets {
            assert!(*seq >= 9, "recovered offset should be >= 9, got {seq}");
        }

        c1.shutdown().await;
        store.shutdown();
    }
}
