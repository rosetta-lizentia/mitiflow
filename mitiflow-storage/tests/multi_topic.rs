//! Tests for multi-topic StorageAgent API.

use std::time::Duration;

use mitiflow_storage::{AgentConfig, AgentConfigBuilder, StorageAgent, TopicEntry};

fn multi_config(
    test_name: &str,
    node_id: &str,
    topics: Vec<(&str, u32, u32)>,
) -> (tempfile::TempDir, AgentConfig) {
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
        .node_id(node_id)
        .global_prefix(format!("test/{test_name}"))
        .topics(entries)
        .drain_grace_period(Duration::from_millis(200))
        .health_interval(Duration::from_secs(60))
        .build()
        .unwrap();
    (tmp, config)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_multi_topic_starts_all() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let (_tmp, config) = multi_config(
        "amt_start",
        "node-only",
        vec![("events", 4, 1), ("logs", 3, 1)],
    );

    let mut agent = StorageAgent::start_multi(&session, config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let topics = agent.topics().await;
    assert_eq!(topics.len(), 2);

    // Multi-topic assigned_partitions should aggregate.
    let all_parts = agent.assigned_partitions().await;
    assert_eq!(all_parts.len(), 7, "4 + 3 = 7 partitions total");

    agent.shutdown().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_multi_topic_add_at_runtime() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let (_tmp, config) = multi_config("amt_add", "node-only", vec![("events", 2, 1)]);

    let mut agent = StorageAgent::start_multi(&session, config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(agent.topics().await.len(), 1);

    // Add a topic at runtime.
    let entry = TopicEntry {
        name: "metrics".to_string(),
        key_prefix: "test/amt_add/metrics".to_string(),
        num_partitions: 3,
        replication_factor: 1,
    };
    agent.add_topic(entry).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(agent.topics().await.len(), 2);
    let parts = agent.assigned_partitions().await;
    assert_eq!(parts.len(), 5, "2 + 3 = 5");

    agent.shutdown().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_multi_topic_remove_at_runtime() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let (_tmp, config) = multi_config(
        "amt_remove",
        "node-only",
        vec![("events", 4, 1), ("logs", 2, 1)],
    );

    let mut agent = StorageAgent::start_multi(&session, config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(agent.assigned_partitions().await.len(), 6);

    let removed = agent.remove_topic("events").await.unwrap();
    assert!(removed);
    assert_eq!(agent.topics().await.len(), 1);
    assert_eq!(agent.assigned_partitions().await.len(), 2);

    agent.shutdown().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_multi_topic_two_nodes_independent_topics() {
    let s1 = zenoh::open(zenoh::Config::default()).await.unwrap();
    let s2 = zenoh::open(zenoh::Config::default()).await.unwrap();

    let (_t1, c1) = multi_config("amt_indep", "node-a", vec![("events", 4, 1)]);
    let (_t2, c2) = multi_config("amt_indep", "node-b", vec![("logs", 3, 1)]);

    let mut a1 = StorageAgent::start_multi(&s1, c1).await.unwrap();
    let mut a2 = StorageAgent::start_multi(&s2, c2).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Each node manages its own topic independently.
    assert_eq!(a1.topics().await, vec!["events"]);
    assert_eq!(a2.topics().await, vec!["logs"]);
    assert_eq!(a1.assigned_partitions().await.len(), 4);
    assert_eq!(a2.assigned_partitions().await.len(), 3);

    a1.shutdown().await.unwrap();
    a2.shutdown().await.unwrap();
    s1.close().await.unwrap();
    s2.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_multi_topic_same_topic_two_nodes_splits() {
    let s1 = zenoh::open(zenoh::Config::default()).await.unwrap();
    let s2 = zenoh::open(zenoh::Config::default()).await.unwrap();

    let (_t1, c1) = multi_config("amt_shared", "node-a", vec![("events", 8, 1)]);
    let (_t2, c2) = multi_config("amt_shared", "node-b", vec![("events", 8, 1)]);

    let mut a1 = StorageAgent::start_multi(&s1, c1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut a2 = StorageAgent::start_multi(&s2, c2).await.unwrap();

    tokio::time::sleep(Duration::from_millis(1000)).await;
    a1.recompute_and_reconcile().await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let p1 = a1.assigned_partitions().await;
    let p2 = a2.assigned_partitions().await;

    let total = p1.len() + p2.len();
    assert!(
        total >= 8,
        "combined should own >= 8 partitions, got {total}"
    );

    assert!(
        p1.len() >= 2 && p1.len() <= 6,
        "node-a: {}, node-b: {}",
        p1.len(),
        p2.len()
    );

    a1.shutdown().await.unwrap();
    a2.shutdown().await.unwrap();
    s1.close().await.unwrap();
    s2.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_backward_compat_single_topic() {
    use mitiflow::EventBusConfig;
    use mitiflow_storage::StorageAgentConfigBuilder;

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let bus_config = EventBusConfig::builder("test/amt_compat")
        .cache_size(100)
        .build()
        .unwrap();
    let config = StorageAgentConfigBuilder::new(tmp.path().to_path_buf(), bus_config)
        .node_id("node-compat")
        .num_partitions(4)
        .replication_factor(1)
        .drain_grace_period(Duration::from_millis(200))
        .health_interval(Duration::from_secs(60))
        .build()
        .unwrap();

    let mut agent = StorageAgent::start(&session, config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    let assigned = agent.assigned_partitions().await;
    assert_eq!(
        assigned.len(),
        4,
        "backward compat: should own 4 partitions"
    );
    assert_eq!(agent.topics().await.len(), 1);

    agent.shutdown().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_has_topic() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let (_tmp, config) = multi_config("amt_has_topic", "node-only", vec![("events", 2, 1)]);

    let mut agent = StorageAgent::start_multi(&session, config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert!(agent.has_topic("events").await);
    assert!(!agent.has_topic("missing").await);

    agent.shutdown().await.unwrap();
    session.close().await.unwrap();
}
