//! Orchestrator integration tests.
//!
//! Tests config store CRUD, lag monitoring, store lifecycle tracking,
//! and the admin queryable API.
//!
//! All tests use Zenoh peer-to-peer mode — no external services required.

use std::collections::HashMap;
use std::time::Duration;

use mitiflow::store::offset::OffsetCommit;
use mitiflow::store::watermark::{CommitWatermark, PublisherWatermark};
use mitiflow::types::PublisherId;
use mitiflow_orchestrator::cluster_view::ClusterView;
use mitiflow_orchestrator::config::{CompactionPolicy, ConfigStore, RetentionPolicy, TopicConfig};
use mitiflow_orchestrator::lag::LagMonitor;
use mitiflow_orchestrator::lifecycle::StoreTracker;
use mitiflow_orchestrator::orchestrator::{Orchestrator, OrchestratorConfig};
use mitiflow_orchestrator::override_manager::OverrideManager;
use mitiflow_orchestrator::{
    NodeHealth, NodeStatus, OverrideEntry, OverrideTable, PartitionStatus, StoreState,
};

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
        key_prefix: String::new(),
        num_partitions: 4,
        replication_factor: 3,
        retention: RetentionPolicy::default(),
        compaction: CompactionPolicy::default(),
        required_labels: HashMap::new(),
        excluded_labels: HashMap::new(),
        codec: Default::default(),
        key_format: Default::default(),
        schema_version: 0,
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
                key_prefix: String::new(),
                num_partitions: i + 1,
                replication_factor: 1,
                retention: RetentionPolicy::default(),
                compaction: CompactionPolicy::default(),
                required_labels: HashMap::new(),
                excluded_labels: HashMap::new(),
        codec: Default::default(),
        key_format: Default::default(),
        schema_version: 0,
            })
            .unwrap();
    }

    let all = store.list_topics().unwrap();
    assert_eq!(all.len(), 5);

    // Update one
    store
        .put_topic(&TopicConfig {
            name: "topic-2".into(),
            key_prefix: String::new(),
            num_partitions: 99,
            replication_factor: 2,
            retention: RetentionPolicy::default(),
            compaction: CompactionPolicy::default(),
            required_labels: HashMap::new(),
            excluded_labels: HashMap::new(),
        codec: Default::default(),
        key_format: Default::default(),
        schema_version: 0,
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
                key_prefix: String::new(),
                num_partitions: 8,
                replication_factor: 1,
                retention: RetentionPolicy::default(),
                compaction: CompactionPolicy::default(),
                required_labels: HashMap::new(),
                excluded_labels: HashMap::new(),
        codec: Default::default(),
        key_format: Default::default(),
        schema_version: 0,
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
        http_bind: None,
        auth_token: None,
    };

    let mut orch = Orchestrator::new(&session, config).unwrap();
    orch.run().await.unwrap();

    // Create topics
    orch.create_topic(TopicConfig {
        name: "events".into(),
        key_prefix: String::new(),
        num_partitions: 4,
        replication_factor: 1,
        retention: RetentionPolicy::default(),
        compaction: CompactionPolicy::default(),
        required_labels: HashMap::new(),
        excluded_labels: HashMap::new(),
        codec: Default::default(),
        key_format: Default::default(),
        schema_version: 0,
    })
    .await
    .unwrap();

    orch.create_topic(TopicConfig {
        name: "logs".into(),
        key_prefix: String::new(),
        num_partitions: 2,
        replication_factor: 1,
        retention: RetentionPolicy::default(),
        compaction: CompactionPolicy::default(),
        required_labels: HashMap::new(),
        excluded_labels: HashMap::new(),
        codec: Default::default(),
        key_format: Default::default(),
        schema_version: 0,
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
        http_bind: None,
        auth_token: None,
    };

    let mut orch = Orchestrator::new(&session, config).unwrap();
    orch.run().await.unwrap();

    // Create a topic via the orchestrator API
    orch.create_topic(TopicConfig {
        name: "analytics".into(),
        key_prefix: String::new(),
        num_partitions: 3,
        replication_factor: 1,
        retention: RetentionPolicy::default(),
        compaction: CompactionPolicy::default(),
        required_labels: HashMap::new(),
        excluded_labels: HashMap::new(),
        codec: Default::default(),
        key_format: Default::default(),
        schema_version: 0,
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
            let topic: TopicConfig = serde_json::from_slice(&sample.payload().to_bytes()).unwrap();
            assert_eq!(topic.name, "analytics");
            assert_eq!(topic.num_partitions, 3);
            found_specific = true;
        }
    }
    assert!(
        found_specific,
        "should have received a reply for topics/analytics"
    );

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
        http_bind: None,
        auth_token: None,
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

// ==========================================================================
// ClusterView
// ==========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_view_empty_on_start() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let prefix = key_prefix("cv_empty");

    let cv = ClusterView::new(&session, &prefix).await.unwrap();

    assert!(cv.nodes().await.is_empty());
    assert!(cv.assignments().await.is_empty());
    assert!(cv.online_nodes().await.is_empty());
    assert_eq!(cv.online_count().await, 0);

    cv.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_view_picks_up_status_updates() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let prefix = key_prefix("cv_status");

    let cv = ClusterView::new(&session, &prefix).await.unwrap();

    // Publish a status update as if from an agent
    let status = NodeStatus {
        node_id: "node-1".into(),
        partitions: vec![
            PartitionStatus {
                partition: 0,
                replica: 0,
                state: StoreState::Active,
                event_count: 42,
                watermark_seq: HashMap::new(),
            },
            PartitionStatus {
                partition: 3,
                replica: 0,
                state: StoreState::Recovering,
                event_count: 0,
                watermark_seq: HashMap::new(),
            },
        ],
        timestamp: chrono::Utc::now(),
    };

    let status_key = format!("{prefix}/_cluster/status/node-1");
    session
        .put(&status_key, serde_json::to_vec(&status).unwrap())
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;

    let nodes = cv.nodes().await;
    assert!(nodes.contains_key("node-1"));
    let info = &nodes["node-1"];
    assert!(info.status.is_some());
    assert_eq!(info.status.as_ref().unwrap().partitions.len(), 2);

    // Check derived assignments
    let assignments = cv.assignments().await;
    assert_eq!(assignments.len(), 2);
    assert!(assignments.contains_key(&(0, 0)));
    assert!(assignments.contains_key(&(3, 0)));
    assert_eq!(assignments[&(0, 0)].node_id, "node-1");
    assert_eq!(assignments[&(0, 0)].state, StoreState::Active);

    cv.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_view_picks_up_health_updates() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let prefix = key_prefix("cv_health");

    let cv = ClusterView::new(&session, &prefix).await.unwrap();

    let health = NodeHealth {
        node_id: "node-2".into(),
        partitions_owned: 4,
        events_stored: 1000,
        disk_usage_bytes: 1024 * 1024,
        store_latency_p99_us: 50,
        error_count: 0,
        timestamp: chrono::Utc::now(),
    };

    let health_key = format!("{prefix}/_cluster/health/node-2");
    session
        .put(&health_key, serde_json::to_vec(&health).unwrap())
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;

    let nodes = cv.nodes().await;
    assert!(nodes.contains_key("node-2"));
    let info = &nodes["node-2"];
    assert!(info.health.is_some());
    assert_eq!(info.health.as_ref().unwrap().events_stored, 1000);

    cv.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_view_marks_node_offline_on_liveliness_drop() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let prefix = key_prefix("cv_live");

    let cv = ClusterView::new(&session, &prefix).await.unwrap();

    // Declare a liveliness token simulating an agent
    let token = session
        .liveliness()
        .declare_token(format!("{prefix}/_agents/node-5"))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;

    assert!(cv.is_node_online("node-5").await);
    assert_eq!(cv.online_count().await, 1);

    // Drop the token → node goes offline
    drop(token);
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert!(!cv.is_node_online("node-5").await);
    assert_eq!(cv.online_count().await, 0);

    cv.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_view_online_nodes_filtered() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let prefix = key_prefix("cv_filter");

    let cv = ClusterView::new(&session, &prefix).await.unwrap();

    // Two nodes come online
    let _t1 = session
        .liveliness()
        .declare_token(format!("{prefix}/_agents/alive-1"))
        .await
        .unwrap();
    let t2 = session
        .liveliness()
        .declare_token(format!("{prefix}/_agents/alive-2"))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(cv.online_count().await, 2);

    // Drop one
    drop(t2);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let online = cv.online_nodes().await;
    assert_eq!(online.len(), 1);
    assert!(online.contains(&"alive-1".to_string()));

    cv.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_view_deriving_assignments_multi_node() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let prefix = key_prefix("cv_assign");

    let cv = ClusterView::new(&session, &prefix).await.unwrap();

    // Two agents publish their status
    for (node, partitions) in [
        ("agent-a", vec![(0, 0), (1, 0)]),
        ("agent-b", vec![(2, 0), (3, 0)]),
    ] {
        let status = NodeStatus {
            node_id: node.into(),
            partitions: partitions
                .iter()
                .map(|&(p, r)| PartitionStatus {
                    partition: p,
                    replica: r,
                    state: StoreState::Active,
                    event_count: 0,
                    watermark_seq: HashMap::new(),
                })
                .collect(),
            timestamp: chrono::Utc::now(),
        };
        session
            .put(
                format!("{prefix}/_cluster/status/{node}"),
                serde_json::to_vec(&status).unwrap(),
            )
            .await
            .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(300)).await;

    let assignments = cv.assignments().await;
    assert_eq!(assignments.len(), 4);
    assert_eq!(assignments[&(0, 0)].node_id, "agent-a");
    assert_eq!(assignments[&(2, 0)].node_id, "agent-b");

    cv.shutdown().await;
}

// ==========================================================================
// OverrideManager
// ==========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn override_manager_starts_empty() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let prefix = key_prefix("om_empty");

    let om = OverrideManager::new(&session, &prefix);

    let current = om.current().await;
    assert!(current.entries.is_empty());
    assert_eq!(current.epoch, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn override_manager_publishes_and_subscriber_receives() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let prefix = key_prefix("om_pub");
    let overrides_key = format!("{prefix}/_cluster/overrides");

    // Subscribe before publishing
    let sub = session.declare_subscriber(&overrides_key).await.unwrap();

    let om = OverrideManager::new(&session, &prefix);

    om.publish_entries(
        vec![OverrideEntry {
            partition: 0,
            replica: 0,
            node_id: "node-99".into(),
            reason: "test".into(),
        }],
        None,
    )
    .await
    .unwrap();

    // Subscriber should receive the override
    let sample = tokio::time::timeout(Duration::from_secs(2), sub.recv_async())
        .await
        .expect("timeout waiting for override")
        .unwrap();

    let table: OverrideTable = serde_json::from_slice(&sample.payload().to_bytes()).unwrap();
    assert_eq!(table.entries.len(), 1);
    assert_eq!(table.entries[0].node_id, "node-99");
    assert_eq!(table.epoch, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn override_manager_epoch_increments() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let prefix = key_prefix("om_epoch");

    let om = OverrideManager::new(&session, &prefix);

    om.publish_entries(vec![], None).await.unwrap();
    assert_eq!(om.current().await.epoch, 1);

    om.publish_entries(vec![], None).await.unwrap();
    assert_eq!(om.current().await.epoch, 2);

    om.publish_entries(vec![], None).await.unwrap();
    assert_eq!(om.current().await.epoch, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn override_manager_clear_sends_empty_table() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let prefix = key_prefix("om_clear");

    let om = OverrideManager::new(&session, &prefix);

    // Publish entries
    om.publish_entries(
        vec![OverrideEntry {
            partition: 5,
            replica: 0,
            node_id: "n1".into(),
            reason: "test".into(),
        }],
        None,
    )
    .await
    .unwrap();
    assert_eq!(om.current().await.entries.len(), 1);

    // Clear
    om.clear().await.unwrap();
    let current = om.current().await;
    assert!(current.entries.is_empty());
    assert_eq!(current.epoch, 2); // epoch still incremented
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn override_manager_remove_entries_for_node() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let prefix = key_prefix("om_remove");

    let om = OverrideManager::new(&session, &prefix);

    om.publish_entries(
        vec![
            OverrideEntry {
                partition: 0,
                replica: 0,
                node_id: "keep-me".into(),
                reason: "test".into(),
            },
            OverrideEntry {
                partition: 1,
                replica: 0,
                node_id: "remove-me".into(),
                reason: "test".into(),
            },
            OverrideEntry {
                partition: 2,
                replica: 0,
                node_id: "remove-me".into(),
                reason: "test".into(),
            },
        ],
        None,
    )
    .await
    .unwrap();

    om.remove_entries_for_node("remove-me").await.unwrap();

    let current = om.current().await;
    assert_eq!(current.entries.len(), 1);
    assert_eq!(current.entries[0].node_id, "keep-me");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn override_manager_add_entries_merges() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let prefix = key_prefix("om_merge");

    let om = OverrideManager::new(&session, &prefix);

    // Initial entries
    om.publish_entries(
        vec![OverrideEntry {
            partition: 0,
            replica: 0,
            node_id: "old-target".into(),
            reason: "initial".into(),
        }],
        None,
    )
    .await
    .unwrap();

    // Add overlapping + new entries
    om.add_entries(
        vec![
            OverrideEntry {
                partition: 0,
                replica: 0,
                node_id: "new-target".into(),
                reason: "updated".into(),
            },
            OverrideEntry {
                partition: 1,
                replica: 0,
                node_id: "another".into(),
                reason: "new".into(),
            },
        ],
        None,
    )
    .await
    .unwrap();

    let current = om.current().await;
    assert_eq!(current.entries.len(), 2);
    // (0,0) should be updated to new-target
    let entry_0 = current.entries.iter().find(|e| e.partition == 0).unwrap();
    assert_eq!(entry_0.node_id, "new-target");
}

// ==========================================================================
// Drain Operations
// ==========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drain_node_publishes_correct_overrides() {
    use mitiflow_orchestrator::drain;

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let prefix = key_prefix("drain_pub");

    let cv = ClusterView::new(&session, &prefix).await.unwrap();
    let om = OverrideManager::new(&session, &prefix);

    // Simulate 3 online agents
    let _t0 = session
        .liveliness()
        .declare_token(format!("{prefix}/_agents/node-0"))
        .await
        .unwrap();
    let _t1 = session
        .liveliness()
        .declare_token(format!("{prefix}/_agents/node-1"))
        .await
        .unwrap();
    let _t2 = session
        .liveliness()
        .declare_token(format!("{prefix}/_agents/node-2"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Publish status: node-0 owns partitions 0,1; node-1 owns 2,3; node-2 owns 4,5
    for (node, parts) in [
        ("node-0", vec![0, 1]),
        ("node-1", vec![2, 3]),
        ("node-2", vec![4, 5]),
    ] {
        let status = NodeStatus {
            node_id: node.into(),
            partitions: parts
                .iter()
                .map(|&p| PartitionStatus {
                    partition: p,
                    replica: 0,
                    state: StoreState::Active,
                    event_count: 0,
                    watermark_seq: HashMap::new(),
                })
                .collect(),
            timestamp: chrono::Utc::now(),
        };
        session
            .put(
                format!("{prefix}/_cluster/status/{node}"),
                serde_json::to_vec(&status).unwrap(),
            )
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Drain node-0
    let overrides = drain::drain_node("node-0", &cv, &om, 1).await.unwrap();

    assert_eq!(overrides.len(), 2); // partitions 0 and 1
    for o in &overrides {
        assert_ne!(
            o.node_id, "node-0",
            "override should not target the drained node"
        );
        assert!(o.reason.contains("drain node-0"));
    }

    // Override table should reflect the drain
    let current = om.current().await;
    assert_eq!(current.entries.len(), 2);

    cv.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn undrain_node_removes_overrides() {
    use mitiflow_orchestrator::drain;

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let prefix = key_prefix("undrain");

    let cv = ClusterView::new(&session, &prefix).await.unwrap();
    let om = OverrideManager::new(&session, &prefix);

    // Simulate nodes and status
    let _t0 = session
        .liveliness()
        .declare_token(format!("{prefix}/_agents/node-0"))
        .await
        .unwrap();
    let _t1 = session
        .liveliness()
        .declare_token(format!("{prefix}/_agents/node-1"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    let status = NodeStatus {
        node_id: "node-0".into(),
        partitions: vec![PartitionStatus {
            partition: 0,
            replica: 0,
            state: StoreState::Active,
            event_count: 0,
            watermark_seq: HashMap::new(),
        }],
        timestamp: chrono::Utc::now(),
    };
    session
        .put(
            format!("{prefix}/_cluster/status/node-0"),
            serde_json::to_vec(&status).unwrap(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Drain, then undrain
    drain::drain_node("node-0", &cv, &om, 1).await.unwrap();
    assert!(!om.current().await.entries.is_empty());

    drain::undrain_node("node-0", &om).await.unwrap();
    assert!(om.current().await.entries.is_empty());

    cv.shutdown().await;
}

// ==========================================================================
// Admin API — Cluster Endpoints
// ==========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_cluster_nodes_endpoint() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let prefix = key_prefix("admin_cn");
    let admin = format!("{prefix}/_admin");

    let config = OrchestratorConfig {
        key_prefix: prefix.clone(),
        data_dir: dir.path().to_path_buf(),
        lag_interval: Duration::from_secs(10),
        admin_prefix: Some(admin.clone()),
        http_bind: None,
        auth_token: None,
    };

    let mut orch = Orchestrator::new(&session, config).unwrap();
    orch.run().await.unwrap();

    // Simulate agent liveliness + status
    let _token = session
        .liveliness()
        .declare_token(format!("{prefix}/_agents/node-A"))
        .await
        .unwrap();
    let status = NodeStatus {
        node_id: "node-A".into(),
        partitions: vec![PartitionStatus {
            partition: 0,
            replica: 0,
            state: StoreState::Active,
            event_count: 10,
            watermark_seq: HashMap::new(),
        }],
        timestamp: chrono::Utc::now(),
    };
    session
        .put(
            format!("{prefix}/_cluster/status/node-A"),
            serde_json::to_vec(&status).unwrap(),
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Query cluster/nodes
    let replies = session.get(format!("{admin}/cluster/nodes")).await.unwrap();
    let mut found = false;
    while let Ok(reply) = replies.recv_async().await {
        if let Ok(sample) = reply.result() {
            let nodes: HashMap<String, serde_json::Value> =
                serde_json::from_slice(&sample.payload().to_bytes()).unwrap();
            assert!(nodes.contains_key("node-A"), "should contain node-A");
            found = true;
        }
    }
    assert!(found, "should have received a reply from cluster/nodes");

    orch.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_cluster_assignments_endpoint() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let prefix = key_prefix("admin_ca");
    let admin = format!("{prefix}/_admin");

    let config = OrchestratorConfig {
        key_prefix: prefix.clone(),
        data_dir: dir.path().to_path_buf(),
        lag_interval: Duration::from_secs(10),
        admin_prefix: Some(admin.clone()),
        http_bind: None,
        auth_token: None,
    };

    let mut orch = Orchestrator::new(&session, config).unwrap();
    orch.run().await.unwrap();

    // Simulate agent with 2 partitions
    let status = NodeStatus {
        node_id: "node-X".into(),
        partitions: vec![
            PartitionStatus {
                partition: 0,
                replica: 0,
                state: StoreState::Active,
                event_count: 0,
                watermark_seq: HashMap::new(),
            },
            PartitionStatus {
                partition: 1,
                replica: 0,
                state: StoreState::Active,
                event_count: 0,
                watermark_seq: HashMap::new(),
            },
        ],
        timestamp: chrono::Utc::now(),
    };
    session
        .put(
            format!("{prefix}/_cluster/status/node-X"),
            serde_json::to_vec(&status).unwrap(),
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let replies = session
        .get(format!("{admin}/cluster/assignments"))
        .await
        .unwrap();
    let mut found = false;
    while let Ok(reply) = replies.recv_async().await {
        if let Ok(sample) = reply.result() {
            let assignments: Vec<serde_json::Value> =
                serde_json::from_slice(&sample.payload().to_bytes()).unwrap();
            assert_eq!(assignments.len(), 2);
            found = true;
        }
    }
    assert!(found, "should have received reply from cluster/assignments");

    orch.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_cluster_status_summary() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let prefix = key_prefix("admin_cs");
    let admin = format!("{prefix}/_admin");

    let config = OrchestratorConfig {
        key_prefix: prefix.clone(),
        data_dir: dir.path().to_path_buf(),
        lag_interval: Duration::from_secs(10),
        admin_prefix: Some(admin.clone()),
        http_bind: None,
        auth_token: None,
    };

    let mut orch = Orchestrator::new(&session, config).unwrap();
    orch.run().await.unwrap();

    // Simulate 2 online agents
    let _t1 = session
        .liveliness()
        .declare_token(format!("{prefix}/_agents/n1"))
        .await
        .unwrap();
    let _t2 = session
        .liveliness()
        .declare_token(format!("{prefix}/_agents/n2"))
        .await
        .unwrap();

    // Status for n1 with 3 partitions
    let status = NodeStatus {
        node_id: "n1".into(),
        partitions: (0..3)
            .map(|p| PartitionStatus {
                partition: p,
                replica: 0,
                state: StoreState::Active,
                event_count: 0,
                watermark_seq: HashMap::new(),
            })
            .collect(),
        timestamp: chrono::Utc::now(),
    };
    session
        .put(
            format!("{prefix}/_cluster/status/n1"),
            serde_json::to_vec(&status).unwrap(),
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let replies = session
        .get(format!("{admin}/cluster/status"))
        .await
        .unwrap();
    let mut found = false;
    while let Ok(reply) = replies.recv_async().await {
        if let Ok(sample) = reply.result() {
            let summary: serde_json::Value =
                serde_json::from_slice(&sample.payload().to_bytes()).unwrap();
            assert_eq!(summary["online_nodes"], 2);
            assert_eq!(summary["total_partitions"], 3);
            found = true;
        }
    }
    assert!(found, "should have received reply from cluster/status");

    orch.shutdown().await;
}

// ==========================================================================
// Multi-topic ClusterViews
// ==========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_topic_cluster_view_created_on_topic_create() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let prefix = key_prefix("mt_create");

    let config = OrchestratorConfig {
        key_prefix: prefix.clone(),
        data_dir: dir.path().to_path_buf(),
        lag_interval: Duration::from_secs(10),
        admin_prefix: None,
        http_bind: None,
        auth_token: None,
    };

    let mut orch = Orchestrator::new(&session, config).unwrap();
    orch.run().await.unwrap();

    // No per-topic views initially
    let tm = orch.topic_manager().unwrap().read().await;
    assert!(tm.tracked_topics().is_empty());
    drop(tm);

    // Create a topic WITH a dedicated key prefix
    orch.create_topic(TopicConfig {
        name: "sensors".into(),
        key_prefix: format!("{prefix}/sensors"),
        num_partitions: 4,
        replication_factor: 2,
        retention: RetentionPolicy::default(),
        compaction: CompactionPolicy::default(),
        required_labels: HashMap::new(),
        excluded_labels: HashMap::new(),
        codec: Default::default(),
        key_format: Default::default(),
        schema_version: 0,
    })
    .await
    .unwrap();

    let tm = orch.topic_manager().unwrap().read().await;
    assert_eq!(tm.tracked_topics().len(), 1);
    assert!(tm.get_view("sensors").is_some());
    drop(tm);

    // Create a topic WITHOUT a dedicated prefix → no view
    orch.create_topic(TopicConfig {
        name: "logs".into(),
        key_prefix: String::new(),
        num_partitions: 2,
        replication_factor: 1,
        retention: RetentionPolicy::default(),
        compaction: CompactionPolicy::default(),
        required_labels: HashMap::new(),
        excluded_labels: HashMap::new(),
        codec: Default::default(),
        key_format: Default::default(),
        schema_version: 0,
    })
    .await
    .unwrap();

    let tm = orch.topic_manager().unwrap().read().await;
    assert_eq!(
        tm.tracked_topics().len(),
        1,
        "empty prefix should not create a view"
    );
    drop(tm);

    orch.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_topic_cluster_view_removed_on_topic_delete() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let prefix = key_prefix("mt_delete");

    let config = OrchestratorConfig {
        key_prefix: prefix.clone(),
        data_dir: dir.path().to_path_buf(),
        lag_interval: Duration::from_secs(10),
        admin_prefix: None,
        http_bind: None,
        auth_token: None,
    };

    let mut orch = Orchestrator::new(&session, config).unwrap();
    orch.run().await.unwrap();

    // Create topic with prefix
    orch.create_topic(TopicConfig {
        name: "metrics".into(),
        key_prefix: format!("{prefix}/metrics"),
        num_partitions: 4,
        replication_factor: 1,
        retention: RetentionPolicy::default(),
        compaction: CompactionPolicy::default(),
        required_labels: HashMap::new(),
        excluded_labels: HashMap::new(),
        codec: Default::default(),
        key_format: Default::default(),
        schema_version: 0,
    })
    .await
    .unwrap();

    assert!(
        orch.topic_manager()
            .unwrap()
            .read()
            .await
            .get_view("metrics")
            .is_some()
    );

    // Delete topic → view should be removed
    orch.delete_topic("metrics").await.unwrap();
    assert!(
        orch.topic_manager()
            .unwrap()
            .read()
            .await
            .get_view("metrics")
            .is_none()
    );
    assert!(
        orch.topic_manager()
            .unwrap()
            .read()
            .await
            .tracked_topics()
            .is_empty()
    );

    orch.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_topic_views_pick_up_independent_agents() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let prefix = key_prefix("mt_indep");

    let config = OrchestratorConfig {
        key_prefix: prefix.clone(),
        data_dir: dir.path().to_path_buf(),
        lag_interval: Duration::from_secs(10),
        admin_prefix: None,
        http_bind: None,
        auth_token: None,
    };

    let mut orch = Orchestrator::new(&session, config).unwrap();
    orch.run().await.unwrap();

    // Create two topics with different prefixes
    let prefix_a = format!("{prefix}/topicA");
    let prefix_b = format!("{prefix}/topicB");

    orch.create_topic(TopicConfig {
        name: "topicA".into(),
        key_prefix: prefix_a.clone(),
        num_partitions: 2,
        replication_factor: 1,
        retention: RetentionPolicy::default(),
        compaction: CompactionPolicy::default(),
        required_labels: HashMap::new(),
        excluded_labels: HashMap::new(),
        codec: Default::default(),
        key_format: Default::default(),
        schema_version: 0,
    })
    .await
    .unwrap();

    orch.create_topic(TopicConfig {
        name: "topicB".into(),
        key_prefix: prefix_b.clone(),
        num_partitions: 2,
        replication_factor: 1,
        retention: RetentionPolicy::default(),
        compaction: CompactionPolicy::default(),
        required_labels: HashMap::new(),
        excluded_labels: HashMap::new(),
        codec: Default::default(),
        key_format: Default::default(),
        schema_version: 0,
    })
    .await
    .unwrap();

    // Simulate an agent under topicA only
    let _token_a = session
        .liveliness()
        .declare_token(format!("{prefix_a}/_agents/agentA"))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let tm = orch.topic_manager().unwrap().read().await;
    let view_a = tm.get_view("topicA").unwrap();
    assert_eq!(view_a.online_count().await, 1);

    let view_b = tm.get_view("topicB").unwrap();
    assert_eq!(
        view_b.online_count().await,
        0,
        "topicB should have no agents"
    );
    drop(tm);

    orch.shutdown().await;
}

// ==========================================================================
// Config Queryable (_config/**)
// ==========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orchestrator_config_queryable_returns_all_topics() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let prefix = key_prefix("cfg_qbl_all");

    let config = OrchestratorConfig {
        key_prefix: prefix.clone(),
        data_dir: dir.path().to_path_buf(),
        lag_interval: Duration::from_secs(10),
        admin_prefix: None,
        http_bind: None,
        auth_token: None,
    };

    let mut orch = Orchestrator::new(&session, config).unwrap();
    orch.run().await.unwrap();

    // Create two topics.
    orch.create_topic(TopicConfig {
        name: "alpha".into(),
        key_prefix: String::new(),
        num_partitions: 4,
        replication_factor: 1,
        retention: RetentionPolicy::default(),
        compaction: CompactionPolicy::default(),
        required_labels: HashMap::new(),
        excluded_labels: HashMap::new(),
        codec: Default::default(),
        key_format: Default::default(),
        schema_version: 0,
    })
    .await
    .unwrap();

    orch.create_topic(TopicConfig {
        name: "beta".into(),
        key_prefix: String::new(),
        num_partitions: 2,
        replication_factor: 2,
        retention: RetentionPolicy::default(),
        compaction: CompactionPolicy::default(),
        required_labels: HashMap::new(),
        excluded_labels: HashMap::new(),
        codec: Default::default(),
        key_format: Default::default(),
        schema_version: 0,
    })
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Query _config/**
    let config_key = format!("{prefix}/_config/**");
    let replies = session
        .get(&config_key)
        .consolidation(zenoh::query::ConsolidationMode::None)
        .accept_replies(zenoh::query::ReplyKeyExpr::Any)
        .timeout(Duration::from_secs(5))
        .await
        .unwrap();

    let mut topic_names = Vec::new();
    while let Ok(reply) = replies.recv_async().await {
        if let Ok(sample) = reply.result() {
            let payload = sample.payload().to_bytes();
            let cfg: TopicConfig = serde_json::from_slice(&payload).unwrap();
            topic_names.push(cfg.name);
        }
    }
    topic_names.sort();
    assert_eq!(topic_names, vec!["alpha", "beta"]);

    orch.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orchestrator_config_queryable_returns_single_topic() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let prefix = key_prefix("cfg_qbl_single");

    let config = OrchestratorConfig {
        key_prefix: prefix.clone(),
        data_dir: dir.path().to_path_buf(),
        lag_interval: Duration::from_secs(10),
        admin_prefix: None,
        http_bind: None,
        auth_token: None,
    };

    let mut orch = Orchestrator::new(&session, config).unwrap();
    orch.run().await.unwrap();

    orch.create_topic(TopicConfig {
        name: "orders".into(),
        key_prefix: String::new(),
        num_partitions: 8,
        replication_factor: 3,
        retention: RetentionPolicy::default(),
        compaction: CompactionPolicy::default(),
        required_labels: HashMap::new(),
        excluded_labels: HashMap::new(),
        codec: Default::default(),
        key_format: Default::default(),
        schema_version: 0,
    })
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Query specific topic
    let config_key = format!("{prefix}/_config/orders");
    let replies = session
        .get(&config_key)
        .consolidation(zenoh::query::ConsolidationMode::None)
        .accept_replies(zenoh::query::ReplyKeyExpr::Any)
        .timeout(Duration::from_secs(5))
        .await
        .unwrap();

    let mut found = false;
    while let Ok(reply) = replies.recv_async().await {
        if let Ok(sample) = reply.result() {
            let payload = sample.payload().to_bytes();
            let cfg: TopicConfig = serde_json::from_slice(&payload).unwrap();
            assert_eq!(cfg.name, "orders");
            assert_eq!(cfg.num_partitions, 8);
            assert_eq!(cfg.replication_factor, 3);
            found = true;
        }
    }
    assert!(found, "should receive 'orders' topic config");

    orch.shutdown().await;
}

#[test]
fn topic_config_serializes_labels_roundtrip() {
    let mut required = HashMap::new();
    required.insert("tier".into(), "ssd".into());
    let mut excluded = HashMap::new();
    excluded.insert("env".into(), "staging".into());

    let cfg = TopicConfig {
        name: "labeled".into(),
        key_prefix: "test/labeled".into(),
        num_partitions: 4,
        replication_factor: 2,
        retention: RetentionPolicy::default(),
        compaction: CompactionPolicy::default(),
        required_labels: required,
        excluded_labels: excluded,
        codec: Default::default(),
        key_format: Default::default(),
        schema_version: 0,
    };

    let bytes = serde_json::to_vec(&cfg).unwrap();
    let parsed: TopicConfig = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(parsed.required_labels.get("tier").unwrap(), "ssd");
    assert_eq!(parsed.excluded_labels.get("env").unwrap(), "staging");
}
