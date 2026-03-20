//! Orchestrator integration tests.
//!
//! Tests config store CRUD, lag monitoring, store lifecycle tracking,
//! and the admin queryable API.
//!
//! All tests use Zenoh peer-to-peer mode — no external services required.

use std::collections::HashMap;
use std::time::Duration;

use mitiflow::store::watermark::{CommitWatermark, PublisherWatermark};
use mitiflow::store::offset::OffsetCommit;
use mitiflow::types::PublisherId;
use mitiflow_orchestrator::config::{
    CompactionPolicy, ConfigStore, RetentionPolicy, TopicConfig,
};
use mitiflow_orchestrator::lag::LagMonitor;
use mitiflow_orchestrator::lifecycle::StoreTracker;
use mitiflow_orchestrator::orchestrator::{Orchestrator, OrchestratorConfig};

/// Unique key prefix per test to avoid cross-talk.
fn key_prefix(test: &str) -> String {
    format!("test/orch_{test}_{}", uuid::Uuid::now_v7())
}

// ==========================================================================
// ConfigStore CRUD
// ==========================================================================

#[test]
fn config_store_put_get_delete() {
    let dir = tempfile::tempdir().unwrap();
    let store = ConfigStore::open(dir.path()).unwrap();

    // Initially empty
    assert!(store.list_topics().unwrap().is_empty());
    assert!(store.get_topic("orders").unwrap().is_none());

    // Put
    let cfg = TopicConfig {
        name: "orders".into(),
        num_partitions: 4,
        replication_factor: 3,
        retention: RetentionPolicy::default(),
        compaction: CompactionPolicy::default(),
    };
    store.put_topic(&cfg).unwrap();

    // Get
    let got = store.get_topic("orders").unwrap().unwrap();
    assert_eq!(got.name, "orders");
    assert_eq!(got.num_partitions, 4);
    assert_eq!(got.replication_factor, 3);

    // List
    let all = store.list_topics().unwrap();
    assert_eq!(all.len(), 1);

    // Delete
    assert!(store.delete_topic("orders").unwrap());
    assert!(!store.delete_topic("orders").unwrap()); // already gone
    assert!(store.get_topic("orders").unwrap().is_none());
}

#[test]
fn config_store_multiple_topics() {
    let dir = tempfile::tempdir().unwrap();
    let store = ConfigStore::open(dir.path()).unwrap();

    for i in 0..5 {
        store
            .put_topic(&TopicConfig {
                name: format!("topic-{i}"),
                num_partitions: i + 1,
                replication_factor: 1,
                retention: RetentionPolicy::default(),
                compaction: CompactionPolicy::default(),
            })
            .unwrap();
    }

    let all = store.list_topics().unwrap();
    assert_eq!(all.len(), 5);

    // Update one
    store
        .put_topic(&TopicConfig {
            name: "topic-2".into(),
            num_partitions: 99,
            replication_factor: 2,
            retention: RetentionPolicy::default(),
            compaction: CompactionPolicy::default(),
        })
        .unwrap();

    let updated = store.get_topic("topic-2").unwrap().unwrap();
    assert_eq!(updated.num_partitions, 99);
    // Total count unchanged (upsert)
    assert_eq!(store.list_topics().unwrap().len(), 5);
}

#[test]
fn config_store_persistence() {
    let dir = tempfile::tempdir().unwrap();

    // Write in one store instance
    {
        let store = ConfigStore::open(dir.path()).unwrap();
        store
            .put_topic(&TopicConfig {
                name: "durable".into(),
                num_partitions: 8,
                replication_factor: 1,
                retention: RetentionPolicy::default(),
                compaction: CompactionPolicy::default(),
            })
            .unwrap();
    }

    // Re-open and verify data persists
    {
        let store = ConfigStore::open(dir.path()).unwrap();
        let got = store.get_topic("durable").unwrap().unwrap();
        assert_eq!(got.name, "durable");
        assert_eq!(got.num_partitions, 8);
    }
}

// ==========================================================================
// LagMonitor
// ==========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lag_monitor_computes_lag_from_watermarks_and_offsets() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let prefix = key_prefix("lag");

    let monitor = LagMonitor::new(&session, &prefix, Duration::from_millis(100))
        .await
        .unwrap();

    let pub_id = PublisherId::new();
    let partition: u32 = 0;

    // Publish a watermark at seq 10
    let wm = CommitWatermark {
        partition,
        publishers: HashMap::from([(
            pub_id,
            PublisherWatermark {
                committed_seq: 10,
                gaps: vec![],
            },
        )]),
        timestamp: chrono::Utc::now(),
        epoch: 0,
    };
    let wm_key = format!("{prefix}/_watermark/{partition}");
    session
        .put(&wm_key, serde_json::to_vec(&wm).unwrap())
        .await
        .unwrap();

    // Publish an offset commit: group "g1" at seq 3 for publisher 42
    let commit = OffsetCommit {
        group_id: "g1".into(),
        member_id: "m1".into(),
        partition,
        offsets: HashMap::from([(pub_id, 3)]),
        generation: 1,
        timestamp: chrono::Utc::now(),
    };
    let offset_key = format!("{prefix}/_offsets/{partition}/g1");
    session
        .put(&offset_key, serde_json::to_vec(&commit).unwrap())
        .await
        .unwrap();

    // Give the monitor time to ingest
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Query lag
    let reports = monitor.get_group_lag("g1").await;
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].partition, 0);
    assert_eq!(reports[0].total, 7); // 10 - 3
    assert_eq!(*reports[0].publishers.get(&pub_id).unwrap(), 7);

    monitor.shutdown().await;
}

// ==========================================================================
// StoreTracker
// ==========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn store_tracker_online_offline() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let prefix = key_prefix("tracker");

    let tracker = StoreTracker::new(&session, &prefix).await.unwrap();

    // Initially no stores
    assert!(tracker.online_stores().await.is_empty());

    // Simulate a store coming online by declaring a liveliness token
    let token = session
        .liveliness()
        .declare_token(format!("{prefix}/_store/0"))
        .await
        .unwrap();

    // Wait for liveliness subscription to fire
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Should see one online store
    let online = tracker.online_stores().await;
    assert_eq!(online.len(), 1);
    assert_eq!(online[0].partition, 0);
    assert!(online[0].online);
    assert!(tracker.is_partition_online(0).await);

    // Drop the token to simulate store going offline
    drop(token);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Should see the store as offline
    assert!(!tracker.is_partition_online(0).await);
    let offline = tracker.offline_stores().await;
    assert_eq!(offline.len(), 1);
    assert_eq!(offline[0].partition, 0);

    tracker.shutdown().await;
}

// ==========================================================================
// Orchestrator Integration
// ==========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orchestrator_create_list_delete_topic() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let prefix = key_prefix("orch_crud");

    let config = OrchestratorConfig {
        key_prefix: prefix.clone(),
        data_dir: dir.path().to_path_buf(),
        lag_interval: Duration::from_secs(10),
        admin_prefix: None,
    };

    let mut orch = Orchestrator::new(&session, config).unwrap();
    orch.run().await.unwrap();

    // Create topics
    orch.create_topic(TopicConfig {
        name: "events".into(),
        num_partitions: 4,
        replication_factor: 1,
        retention: RetentionPolicy::default(),
        compaction: CompactionPolicy::default(),
    })
    .await
    .unwrap();

    orch.create_topic(TopicConfig {
        name: "logs".into(),
        num_partitions: 2,
        replication_factor: 1,
        retention: RetentionPolicy::default(),
        compaction: CompactionPolicy::default(),
    })
    .await
    .unwrap();

    // List
    let topics = orch.list_topics().unwrap();
    assert_eq!(topics.len(), 2);

    // Get
    let got = orch.get_topic("events").unwrap().unwrap();
    assert_eq!(got.num_partitions, 4);

    // Delete
    orch.delete_topic("events").await.unwrap();
    assert_eq!(orch.list_topics().unwrap().len(), 1);
    assert!(orch.get_topic("events").unwrap().is_none());

    orch.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orchestrator_admin_queryable_topics() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let prefix = key_prefix("orch_admin");
    let admin = format!("{prefix}/_admin");

    let config = OrchestratorConfig {
        key_prefix: prefix.clone(),
        data_dir: dir.path().to_path_buf(),
        lag_interval: Duration::from_secs(10),
        admin_prefix: Some(admin.clone()),
    };

    let mut orch = Orchestrator::new(&session, config).unwrap();
    orch.run().await.unwrap();

    // Create a topic via the orchestrator API
    orch.create_topic(TopicConfig {
        name: "analytics".into(),
        num_partitions: 3,
        replication_factor: 1,
        retention: RetentionPolicy::default(),
        compaction: CompactionPolicy::default(),
    })
    .await
    .unwrap();

    // Give admin queryable time to settle
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Query the admin endpoint for topics list
    let replies = session.get(format!("{admin}/topics")).await.unwrap();
    let mut found = false;
    while let Ok(reply) = replies.recv_async().await {
        if let Ok(sample) = reply.result() {
            let topics: Vec<TopicConfig> =
                serde_json::from_slice(&sample.payload().to_bytes()).unwrap();
            assert_eq!(topics.len(), 1);
            assert_eq!(topics[0].name, "analytics");
            found = true;
        }
    }
    assert!(found, "should have received a reply from admin/topics");

    // Query for specific topic
    let replies = session
        .get(format!("{admin}/topics/analytics"))
        .await
        .unwrap();
    let mut found_specific = false;
    while let Ok(reply) = replies.recv_async().await {
        if let Ok(sample) = reply.result() {
            let topic: TopicConfig =
                serde_json::from_slice(&sample.payload().to_bytes()).unwrap();
            assert_eq!(topic.name, "analytics");
            assert_eq!(topic.num_partitions, 3);
            found_specific = true;
        }
    }
    assert!(found_specific, "should have received a reply for topics/analytics");

    orch.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orchestrator_lag_integration() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let prefix = key_prefix("orch_lag");

    let config = OrchestratorConfig {
        key_prefix: prefix.clone(),
        data_dir: dir.path().to_path_buf(),
        lag_interval: Duration::from_millis(100),
        admin_prefix: None,
    };

    let mut orch = Orchestrator::new(&session, config).unwrap();
    orch.run().await.unwrap();

    let pub_id = PublisherId::new();
    let partition: u32 = 0;

    // Publish watermark
    let wm = CommitWatermark {
        partition,
        publishers: HashMap::from([(
            pub_id,
            PublisherWatermark {
                committed_seq: 50,
                gaps: vec![],
            },
        )]),
        timestamp: chrono::Utc::now(),
        epoch: 0,
    };
    session
        .put(
            format!("{prefix}/_watermark/{partition}"),
            serde_json::to_vec(&wm).unwrap(),
        )
        .await
        .unwrap();

    // Publish offset commit
    let commit = OffsetCommit {
        group_id: "app-group".into(),
        member_id: "m1".into(),
        partition,
        offsets: HashMap::from([(pub_id, 20)]),
        generation: 1,
        timestamp: chrono::Utc::now(),
    };
    session
        .put(
            format!("{prefix}/_offsets/{partition}/app-group"),
            serde_json::to_vec(&commit).unwrap(),
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(400)).await;

    // Check lag via orchestrator
    let reports = orch.get_group_lag("app-group").await;
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].total, 30); // 50 - 20

    orch.shutdown().await;
}
