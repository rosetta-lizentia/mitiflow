//! Tests for TopicWorker — per-topic partition management.

use std::collections::HashMap;
use std::time::Duration;

use mitiflow_agent::{TopicEntry, TopicWorkerConfig};
use mitiflow_agent::topic_worker::TopicWorker;

fn worker_config(
    test_name: &str,
    topic: &str,
    num_partitions: u32,
    rf: u32,
) -> (tempfile::TempDir, TopicWorkerConfig) {
    let tmp = tempfile::tempdir().unwrap();
    let entry = TopicEntry {
        name: topic.to_string(),
        key_prefix: format!("test/{test_name}"),
        num_partitions,
        replication_factor: rf,
    };
    let config = TopicWorkerConfig::from_entry(
        &entry,
        &tmp.path().to_path_buf(),
        100,
        &HashMap::new(),
        Duration::from_millis(200),
    )
    .unwrap();
    (tmp, config)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_worker_starts_and_owns_partitions() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let (_tmp, config) = worker_config("tw_single", "events", 4, 1);

    let worker = TopicWorker::start(&session, "node-only", config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    let assigned = worker.assigned_partitions().await;
    assert_eq!(
        assigned.len(),
        4,
        "single node should own all 4 partitions, got {:?}",
        assigned
    );
    for p in 0..4 {
        assert!(assigned.contains(&(p, 0)), "should own partition {p}");
    }

    assert_eq!(worker.topic_name(), "events");
    assert_eq!(worker.node_id(), "node-only");

    worker.shutdown().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_worker_two_nodes_split() {
    let s1 = zenoh::open(zenoh::Config::default()).await.unwrap();
    let s2 = zenoh::open(zenoh::Config::default()).await.unwrap();

    let (_t1, c1) = worker_config("tw_split", "events", 8, 1);
    let (_t2, c2) = worker_config("tw_split", "events", 8, 1);

    let w1 = TopicWorker::start(&s1, "node-a", c1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let w2 = TopicWorker::start(&s2, "node-b", c2).await.unwrap();

    tokio::time::sleep(Duration::from_millis(1000)).await;
    w1.recompute_and_reconcile().await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let p1 = w1.assigned_partitions().await;
    let p2 = w2.assigned_partitions().await;

    let total = p1.len() + p2.len();
    assert!(
        total >= 8,
        "combined should own >= 8 partitions, got {}",
        total
    );
    assert!(
        p1.len() >= 2 && p1.len() <= 6,
        "node-a should own 2-6 partitions, got {}",
        p1.len()
    );

    // No overlap.
    for pr in &p1 {
        assert!(!p2.contains(pr), "partition {:?} on both nodes", pr);
    }

    w1.shutdown().await.unwrap();
    w2.shutdown().await.unwrap();
    s1.close().await.unwrap();
    s2.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_worker_shutdown_drains() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let (_tmp, config) = worker_config("tw_drain", "events", 4, 1);

    let worker = TopicWorker::start(&session, "node-only", config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(worker.assigned_partitions().await.len(), 4);

    worker.shutdown().await.unwrap();
    assert!(
        worker.assigned_partitions().await.is_empty(),
        "all stores should be stopped after shutdown"
    );

    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_worker_respects_overrides() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let (_t1, c1) = worker_config("tw_override", "events", 4, 1);
    let (_t2, c2) = worker_config("tw_override", "events", 4, 1);

    let w1 = TopicWorker::start(&session, "node-a", c1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let w2 = TopicWorker::start(&session, "node-b", c2).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Publish override pinning partition 0 to node-b.
    let override_table = mitiflow_agent::OverrideTable {
        entries: vec![mitiflow_agent::OverrideEntry {
            partition: 0,
            replica: 0,
            node_id: "node-b".to_string(),
            reason: "test override".to_string(),
        }],
        epoch: 1,
        expires_at: None,
    };
    let override_key = "test/tw_override/_cluster/overrides";
    let bytes = serde_json::to_vec(&override_table).unwrap();
    session.put(override_key, bytes).await.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;
    w1.recompute_and_reconcile().await.unwrap();
    w2.recompute_and_reconcile().await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let p2 = w2.assigned_partitions().await;
    assert!(
        p2.contains(&(0, 0)),
        "node-b should own p0 due to override, got {:?}",
        p2
    );

    w1.shutdown().await.unwrap();
    w2.shutdown().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_worker_recompute_after_node_leave() {
    let s1 = zenoh::open(zenoh::Config::default()).await.unwrap();
    let s2 = zenoh::open(zenoh::Config::default()).await.unwrap();

    let (_t1, c1) = worker_config("tw_leave", "events", 6, 1);
    let (_t2, c2) = worker_config("tw_leave", "events", 6, 1);

    let w1 = TopicWorker::start(&s1, "node-a", c1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let w2 = TopicWorker::start(&s2, "node-b", c2).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Shutdown w2.
    w2.shutdown().await.unwrap();
    s2.close().await.unwrap();

    tokio::time::sleep(Duration::from_millis(1500)).await;
    w1.recompute_and_reconcile().await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let p1 = w1.assigned_partitions().await;
    assert!(
        p1.len() >= 6,
        "remaining node should own all 6 partitions, got {}",
        p1.len()
    );

    w1.shutdown().await.unwrap();
    s1.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_worker_multi_replica_rf2() {
    let s1 = zenoh::open(zenoh::Config::default()).await.unwrap();
    let s2 = zenoh::open(zenoh::Config::default()).await.unwrap();
    let s3 = zenoh::open(zenoh::Config::default()).await.unwrap();

    let (_t1, c1) = worker_config("tw_rf2", "events", 4, 2);
    let (_t2, c2) = worker_config("tw_rf2", "events", 4, 2);
    let (_t3, c3) = worker_config("tw_rf2", "events", 4, 2);

    let w1 = TopicWorker::start(&s1, "node-a", c1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let w2 = TopicWorker::start(&s2, "node-b", c2).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let w3 = TopicWorker::start(&s3, "node-c", c3).await.unwrap();

    tokio::time::sleep(Duration::from_millis(1500)).await;
    w1.recompute_and_reconcile().await.unwrap();
    w2.recompute_and_reconcile().await.unwrap();
    w3.recompute_and_reconcile().await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let p1 = w1.assigned_partitions().await;
    let p2 = w2.assigned_partitions().await;
    let p3 = w3.assigned_partitions().await;

    let total = p1.len() + p2.len() + p3.len();
    assert!(
        total >= 8,
        "RF=2 × 4 partitions = 8 total assignments, got {}",
        total
    );

    // Each partition should have exactly 2 replicas across all nodes.
    for partition in 0..4u32 {
        let mut owners = Vec::new();
        for (parts, name) in [(&p1, "a"), (&p2, "b"), (&p3, "c")] {
            for &(p, r) in parts {
                if p == partition {
                    owners.push((name, r));
                }
            }
        }
        assert_eq!(
            owners.len(),
            2,
            "partition {partition} should have 2 replicas, got {owners:?}"
        );
    }

    w1.shutdown().await.unwrap();
    w2.shutdown().await.unwrap();
    w3.shutdown().await.unwrap();
    s1.close().await.unwrap();
    s2.close().await.unwrap();
    s3.close().await.unwrap();
}
