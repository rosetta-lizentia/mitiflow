//! Integration tests for MembershipTracker.
//!
//! Each test opens independent Zenoh sessions and uses a unique key prefix
//! to avoid cross-test interference.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use mitiflow::EventBusConfig;
use mitiflow_agent::membership::{MembershipEvent, MembershipTracker};
use mitiflow_agent::{StorageAgentConfig, StorageAgentConfigBuilder};
use tokio::sync::Notify;

fn agent_config(test_name: &str, node_id: &str) -> StorageAgentConfig {
    let bus_config = EventBusConfig::builder(format!("test/{test_name}"))
        .build()
        .expect("valid bus config");
    StorageAgentConfigBuilder::new(
        std::env::temp_dir().join(format!("mitiflow_test_{test_name}_{node_id}")),
        bus_config,
    )
    .node_id(node_id)
    .num_partitions(4)
    .replication_factor(1)
    .health_interval(Duration::from_secs(60))
    .build()
    .expect("valid agent config")
}

/// Helper: declare a liveliness token simulating another agent being present.
async fn declare_agent_token(
    session: &zenoh::Session,
    key_prefix: &str,
    node_id: &str,
) -> zenoh::liveliness::LivelinessToken {
    let key = format!("{key_prefix}/_agents/{node_id}");
    session
        .liveliness()
        .declare_token(&key)
        .await
        .expect("declare liveliness token")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tracker_discovers_existing_nodes() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let prefix = "test/membership_discover";

    // Pre-declare 2 peer liveliness tokens before the tracker starts.
    let _token_a = declare_agent_token(&session, prefix, "node-a").await;
    let _token_b = declare_agent_token(&session, prefix, "node-b").await;

    // Small settle delay for Zenoh propagation.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let config = agent_config("membership_discover", "node-self");
    let tracker = MembershipTracker::new(&session, &config).await.unwrap();

    let nodes = tracker.current_nodes().await;
    // Should see self + 2 peers = 3 total.
    assert!(
        nodes.len() >= 3,
        "expected at least 3 nodes (self + 2 peers), got {}",
        nodes.len()
    );
    let ids: Vec<&str> = nodes.iter().map(|n| n.id.as_str()).collect();
    assert!(ids.contains(&"node-self"), "should include self");
    assert!(ids.contains(&"node-a"), "should include node-a");
    assert!(ids.contains(&"node-b"), "should include node-b");

    // peer_nodes should exclude self.
    let peers = tracker.peer_nodes().await;
    let peer_ids: Vec<&str> = peers.iter().map(|n| n.id.as_str()).collect();
    assert!(
        !peer_ids.contains(&"node-self"),
        "peers should not include self"
    );

    tracker.shutdown().await;
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tracker_detects_node_join() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let config = agent_config("membership_join", "node-self");
    let tracker = MembershipTracker::new(&session, &config).await.unwrap();

    // Register callback that signals when a node joins.
    let join_notify = Arc::new(Notify::new());
    let join_signal = Arc::clone(&join_notify);
    let joined_id: Arc<std::sync::Mutex<Option<String>>> = Arc::new(std::sync::Mutex::new(None));
    let joined_capture = Arc::clone(&joined_id);

    tracker
        .on_change(move |_nodes, event| {
            if let MembershipEvent::Joined(id) = event {
                let mut guard = joined_capture.lock().unwrap();
                *guard = Some(id.clone());
                join_signal.notify_one();
            }
        })
        .await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Declare a new peer — this should trigger the callback.
    let _token = declare_agent_token(&session, "test/membership_join", "node-new").await;

    // Wait for callback, with a timeout.
    tokio::time::timeout(Duration::from_secs(5), join_notify.notified())
        .await
        .expect("timed out waiting for join callback");

    let id = joined_id.lock().unwrap();
    assert_eq!(id.as_deref(), Some("node-new"));

    // Verify node appears in the current list.
    let nodes = tracker.current_nodes().await;
    let ids: Vec<&str> = nodes.iter().map(|n| n.id.as_str()).collect();
    assert!(
        ids.contains(&"node-new"),
        "should include newly joined node"
    );

    tracker.shutdown().await;
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tracker_detects_node_leave() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    // Use a separate session for the peer to ensure liveliness events propagate.
    let peer_session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let config = agent_config("membership_leave", "node-self");

    // Pre-declare a peer on the separate session.
    let token = declare_agent_token(&peer_session, "test/membership_leave", "node-temp").await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let tracker = MembershipTracker::new(&session, &config).await.unwrap();

    // Verify peer is initially seen.
    let nodes = tracker.current_nodes().await;
    let ids: Vec<&str> = nodes.iter().map(|n| n.id.as_str()).collect();
    assert!(ids.contains(&"node-temp"), "should initially see peer");

    // Register callback for leave events.
    let leave_notify = Arc::new(Notify::new());
    let leave_signal = Arc::clone(&leave_notify);
    let left_id: Arc<std::sync::Mutex<Option<String>>> = Arc::new(std::sync::Mutex::new(None));
    let left_capture = Arc::clone(&left_id);

    tracker
        .on_change(move |_nodes, event| {
            if let MembershipEvent::Left(id) = event {
                let mut guard = left_capture.lock().unwrap();
                *guard = Some(id.clone());
                leave_signal.notify_one();
            }
        })
        .await;

    // Drop the liveliness token to simulate the peer leaving.
    token.undeclare().await.unwrap();

    // Wait for callback.
    tokio::time::timeout(Duration::from_secs(5), leave_notify.notified())
        .await
        .expect("timed out waiting for leave callback");

    let id = left_id.lock().unwrap();
    assert_eq!(id.as_deref(), Some("node-temp"));

    // Peer should no longer be in the list.
    let nodes = tracker.current_nodes().await;
    let ids: Vec<&str> = nodes.iter().map(|n| n.id.as_str()).collect();
    assert!(!ids.contains(&"node-temp"), "left node should be removed");

    tracker.shutdown().await;
    peer_session.close().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tracker_ignores_self() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let config = agent_config("membership_self", "node-me");
    let tracker = MembershipTracker::new(&session, &config).await.unwrap();

    // peer_nodes() must never include "node-me".
    let peers = tracker.peer_nodes().await;
    let peer_ids: Vec<&str> = peers.iter().map(|n| n.id.as_str()).collect();
    assert!(
        !peer_ids.contains(&"node-me"),
        "peer_nodes should not include self"
    );

    // current_nodes() should include "node-me" exactly once (self).
    let nodes = tracker.current_nodes().await;
    let count = nodes.iter().filter(|n| n.id == "node-me").count();
    assert_eq!(count, 1, "current_nodes should include self exactly once");

    tracker.shutdown().await;
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tracker_provides_consistent_node_list() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    // Use a separate session for peer tokens so liveliness events propagate
    // reliably through the Zenoh routing layer.
    let peer_session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let config = agent_config("membership_consistent", "node-self");
    let tracker = MembershipTracker::new(&session, &config).await.unwrap();

    // Declare several peers on the separate session.
    let mut tokens = Vec::new();
    for i in 0..5 {
        let t = declare_agent_token(
            &peer_session,
            "test/membership_consistent",
            &format!("node-{i}"),
        )
        .await;
        tokens.push(t);
    }

    // Wait until all 5 peers are visible to the tracker (self + 5 = 6).
    for _ in 0..50 {
        let nodes = tracker.current_nodes().await;
        if nodes.len() >= 6 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let nodes = tracker.current_nodes().await;
    assert!(
        nodes.len() >= 6,
        "expected at least 6 nodes (self + 5 peers), got {}",
        nodes.len()
    );

    // Drop half (node-4, node-3, node-2).
    for _ in 0..3 {
        if let Some(t) = tokens.pop() {
            t.undeclare().await.unwrap();
        }
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Snapshot should be consistent: all remaining peers present,
    // no duplicates, and self always present.
    let nodes = tracker.current_nodes().await;
    let ids: Vec<&str> = nodes.iter().map(|n| n.id.as_str()).collect();

    // No duplicate IDs.
    let mut sorted = ids.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(ids.len(), sorted.len(), "no duplicate nodes in snapshot");

    // Self always present.
    assert!(ids.contains(&"node-self"), "self always in snapshot");

    // Remaining tokens (node-0, node-1) should still be present.
    assert!(ids.contains(&"node-0"), "node-0 should still be present");
    assert!(ids.contains(&"node-1"), "node-1 should still be present");

    tracker.shutdown().await;
    peer_session.close().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tracker_metadata_propagation() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    // Start a tracker with capacity and labels.
    let bus_config = EventBusConfig::builder("test/membership_meta")
        .build()
        .expect("valid bus config");
    let mut labels = HashMap::new();
    labels.insert("rack".to_string(), "rack-1".to_string());
    let config = StorageAgentConfigBuilder::new(
        std::env::temp_dir().join("mitiflow_test_membership_meta_a"),
        bus_config.clone(),
    )
    .node_id("agent-a")
    .capacity(200)
    .labels(labels)
    .num_partitions(4)
    .health_interval(Duration::from_secs(60))
    .build()
    .unwrap();

    let tracker_a = MembershipTracker::new(&session, &config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Start a second tracker that should discover agent-a.
    let config_b = StorageAgentConfigBuilder::new(
        std::env::temp_dir().join("mitiflow_test_membership_meta_b"),
        bus_config,
    )
    .node_id("agent-b")
    .num_partitions(4)
    .health_interval(Duration::from_secs(60))
    .build()
    .unwrap();

    let tracker_b = MembershipTracker::new(&session, &config_b).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // tracker_b should see agent-a with capacity=200 and rack label.
    let peers = tracker_b.peer_nodes().await;
    let agent_a = peers.iter().find(|n| n.id == "agent-a");
    assert!(agent_a.is_some(), "tracker_b should discover agent-a");
    let agent_a = agent_a.unwrap();
    assert_eq!(agent_a.capacity, 200, "should propagate capacity");
    assert_eq!(
        agent_a.labels.get("rack").map(|s| s.as_str()),
        Some("rack-1"),
        "should propagate rack label"
    );

    tracker_a.shutdown().await;
    tracker_b.shutdown().await;
    session.close().await.unwrap();
}
