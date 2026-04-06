//! Tests for TopicSupervisor — multi-topic management.

use std::time::Duration;

use mitiflow_storage::{AgentConfig, AgentConfigBuilder, TopicEntry, TopicSupervisor};

fn make_config(test_name: &str, topics: Vec<(&str, u32, u32)>) -> (tempfile::TempDir, AgentConfig) {
    let tmp = tempfile::tempdir().unwrap();
    let entries: Vec<TopicEntry> = topics
        .into_iter()
        .map(|(name, partitions, rf)| TopicEntry {
            name: name.to_string(),
            key_prefix: format!("test/{test_name}/{name}"),
            num_partitions: partitions,
            replication_factor: rf,
        })
        .collect();

    let config = AgentConfigBuilder::new(tmp.path().to_path_buf())
        .node_id(format!("node-{test_name}"))
        .global_prefix(format!("test/{test_name}"))
        .topics(entries)
        .drain_grace_period(Duration::from_millis(200))
        .health_interval(Duration::from_secs(60))
        .build()
        .unwrap();
    (tmp, config)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervisor_add_single_topic() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let (_tmp, config) = make_config("sup_single", vec![("events", 4, 1)]);

    let mut supervisor = TopicSupervisor::start(&session, &config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert!(supervisor.has_topic("events"));
    assert_eq!(supervisor.topics().len(), 1);

    let worker = supervisor.worker("events").unwrap();
    let assigned = worker.assigned_partitions().await;
    assert_eq!(assigned.len(), 4, "should own all 4 partitions");

    supervisor.shutdown().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervisor_add_multiple_topics() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let (_tmp, config) = make_config("sup_multi", vec![("events", 4, 1), ("logs", 3, 1)]);

    let mut supervisor = TopicSupervisor::start(&session, &config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_eq!(supervisor.topics().len(), 2);
    assert!(supervisor.has_topic("events"));
    assert!(supervisor.has_topic("logs"));

    let events_parts = supervisor
        .worker("events")
        .unwrap()
        .assigned_partitions()
        .await;
    let logs_parts = supervisor
        .worker("logs")
        .unwrap()
        .assigned_partitions()
        .await;
    assert_eq!(events_parts.len(), 4, "events should have 4 partitions");
    assert_eq!(logs_parts.len(), 3, "logs should have 3 partitions");

    let total = supervisor.total_assigned_partitions().await;
    assert_eq!(total, 7, "total should be 7");

    supervisor.shutdown().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervisor_remove_topic() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let (_tmp, config) = make_config("sup_remove", vec![("events", 4, 1), ("logs", 2, 1)]);

    let mut supervisor = TopicSupervisor::start(&session, &config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(supervisor.topics().len(), 2);

    let removed = supervisor.remove_topic("events").await.unwrap();
    assert!(removed, "should return true for existing topic");
    assert!(!supervisor.has_topic("events"));
    assert_eq!(supervisor.topics().len(), 1);

    // Remaining topic still works.
    let logs_parts = supervisor
        .worker("logs")
        .unwrap()
        .assigned_partitions()
        .await;
    assert_eq!(logs_parts.len(), 2);

    supervisor.shutdown().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervisor_remove_nonexistent_is_noop() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let (_tmp, config) = make_config("sup_noop", vec![("events", 2, 1)]);

    let mut supervisor = TopicSupervisor::start(&session, &config).await.unwrap();

    let removed = supervisor.remove_topic("does_not_exist").await.unwrap();
    assert!(!removed, "should return false for nonexistent topic");

    supervisor.shutdown().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervisor_shutdown_stops_all() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let (_tmp, config) = make_config("sup_shutdown", vec![("events", 4, 1), ("logs", 2, 1)]);

    let mut supervisor = TopicSupervisor::start(&session, &config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    supervisor.shutdown().await.unwrap();

    // All topics should be removed after shutdown.
    assert!(supervisor.topics().is_empty());

    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervisor_add_topic_at_runtime() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let (_tmp, config) = make_config("sup_runtime", vec![("events", 2, 1)]);

    let mut supervisor = TopicSupervisor::start(&session, &config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(supervisor.topics().len(), 1);

    // Add a new topic at runtime.
    let new_entry = TopicEntry {
        name: "metrics".to_string(),
        key_prefix: "test/sup_runtime/metrics".to_string(),
        num_partitions: 3,
        replication_factor: 1,
    };
    supervisor.add_topic_from_entry(&new_entry).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(supervisor.topics().len(), 2);
    assert!(supervisor.has_topic("metrics"));

    let metrics_parts = supervisor
        .worker("metrics")
        .unwrap()
        .assigned_partitions()
        .await;
    assert_eq!(metrics_parts.len(), 3);

    supervisor.shutdown().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervisor_duplicate_topic_errors() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let (_tmp, config) = make_config("sup_dup", vec![("events", 2, 1)]);

    let mut supervisor = TopicSupervisor::start(&session, &config).await.unwrap();

    let entry = TopicEntry {
        name: "events".to_string(),
        key_prefix: "test/sup_dup/events".to_string(),
        num_partitions: 2,
        replication_factor: 1,
    };
    let result = supervisor.add_topic_from_entry(&entry).await;
    assert!(result.is_err(), "duplicate topic should error");

    supervisor.shutdown().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervisor_separate_data_dirs_per_topic() {
    let tmp = tempfile::tempdir().unwrap();
    let entries = vec![
        TopicEntry {
            name: "events".to_string(),
            key_prefix: "test/sup_dirs/events".to_string(),
            num_partitions: 2,
            replication_factor: 1,
        },
        TopicEntry {
            name: "logs".to_string(),
            key_prefix: "test/sup_dirs/logs".to_string(),
            num_partitions: 2,
            replication_factor: 1,
        },
    ];

    let config = AgentConfigBuilder::new(tmp.path().to_path_buf())
        .node_id("node-dirs")
        .global_prefix("test/sup_dirs")
        .topics(entries)
        .drain_grace_period(Duration::from_millis(200))
        .health_interval(Duration::from_secs(60))
        .build()
        .unwrap();

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let mut supervisor = TopicSupervisor::start(&session, &config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify that topic data dirs are separate subdirectories.
    let events_dir = tmp.path().join("events");
    let logs_dir = tmp.path().join("logs");
    assert!(events_dir.exists(), "events data dir should exist");
    assert!(logs_dir.exists(), "logs data dir should exist");

    supervisor.shutdown().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervisor_topics_isolated_assignment() {
    // Two topics on same node, each should get independent partition assignment.
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let (_tmp, config) = make_config("sup_isolated", vec![("events", 4, 1), ("logs", 6, 1)]);

    let mut supervisor = TopicSupervisor::start(&session, &config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let events = supervisor
        .worker("events")
        .unwrap()
        .assigned_partitions()
        .await;
    let logs = supervisor
        .worker("logs")
        .unwrap()
        .assigned_partitions()
        .await;

    assert_eq!(events.len(), 4, "events: single node should own all 4");
    assert_eq!(logs.len(), 6, "logs: single node should own all 6");

    // Partition numbers are independent per topic.
    let events_pids: Vec<u32> = events.iter().map(|(p, _)| *p).collect();
    let logs_pids: Vec<u32> = logs.iter().map(|(p, _)| *p).collect();
    for p in 0..4 {
        assert!(events_pids.contains(&p));
    }
    for p in 0..6 {
        assert!(logs_pids.contains(&p));
    }

    supervisor.shutdown().await.unwrap();
    session.close().await.unwrap();
}
