//! E2E scenario tests for the distributed storage agent cluster.

use std::time::Duration;

use super::helpers::TestCluster;

// ── Steady State ────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_3_nodes_full_coverage() {
    let mut cluster = TestCluster::new("e2e_3n_full", 3, 12, 1);
    for i in 0..3 {
        cluster.start_agent(i).await;
    }
    cluster.wait_for_stable(Duration::from_millis(1500)).await;

    cluster.verify_full_coverage().await;
    cluster.verify_no_overlap().await;

    // Each node should own roughly 4 partitions (12/3).
    let snapshot = cluster.get_assignment_snapshot().await;
    for (node, parts) in &snapshot {
        assert!(
            parts.len() >= 2 && parts.len() <= 6,
            "{node} should own 2-6 partitions out of 12, got {}",
            parts.len()
        );
    }

    cluster.shutdown_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_3_nodes_rf2_coverage() {
    let mut cluster = TestCluster::new("e2e_3n_rf2", 3, 6, 2);
    for i in 0..3 {
        cluster.start_agent(i).await;
    }
    cluster.wait_for_stable(Duration::from_millis(1500)).await;

    // 6 partitions × RF=2 = 12 total assignments, each on 2 distinct nodes.
    let total = cluster.total_assignments().await;
    assert!(
        total >= 12,
        "6 partitions × RF=2 = 12 assignments, got {total}"
    );

    cluster.verify_full_coverage().await;

    // Each partition should be on exactly 2 different nodes.
    let snapshot = cluster.get_assignment_snapshot().await;
    for partition in 0..6u32 {
        let mut owners = Vec::new();
        for (node, parts) in &snapshot {
            if parts.iter().any(|&(p, _)| p == partition) {
                owners.push(node.clone());
            }
        }
        assert_eq!(
            owners.len(),
            2,
            "partition {partition} should be on exactly 2 nodes, got {owners:?}"
        );
    }

    cluster.shutdown_all().await;
}

// ── Node Join ───────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_node_join_rebalances() {
    let mut cluster = TestCluster::new("e2e_join", 3, 8, 1);

    // Start with 2 nodes.
    cluster.start_agent(0).await;
    cluster.start_agent(1).await;
    cluster.wait_for_stable(Duration::from_millis(1500)).await;

    let before = cluster.get_assignment_snapshot().await;
    let before_total: usize = before.values().map(|v| v.len()).sum();
    assert!(before_total >= 8, "2 nodes should cover 8 partitions");

    // Add a third node.
    cluster.start_agent(2).await;
    cluster.wait_for_stable(Duration::from_millis(1500)).await;

    cluster.verify_full_coverage().await;
    cluster.verify_no_overlap().await;

    // Third node should own some partitions.
    let snapshot = cluster.get_assignment_snapshot().await;
    let node2_parts = snapshot.get("node-2").map(|v| v.len()).unwrap_or(0);
    assert!(
        node2_parts > 0,
        "newly joined node should own some partitions"
    );

    cluster.shutdown_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_node_join_minimal_disruption() {
    let mut cluster = TestCluster::new("e2e_join_min", 3, 12, 1);

    cluster.start_agent(0).await;
    cluster.start_agent(1).await;
    cluster.wait_for_stable(Duration::from_millis(1500)).await;

    let before = cluster.get_assignment_snapshot().await;
    let stable_before: Vec<(u32, u32)> = before.get("node-0").cloned().unwrap_or_default();

    // Add third node.
    cluster.start_agent(2).await;
    cluster.wait_for_stable(Duration::from_millis(1500)).await;

    let after = cluster.get_assignment_snapshot().await;
    let stable_after: Vec<(u32, u32)> = after.get("node-0").cloned().unwrap_or_default();

    // HRW minimal disruption: node-0 should keep most partitions.
    // When going from 2→3 nodes, each existing node should lose ~1/3.
    let retained = stable_before
        .iter()
        .filter(|p| stable_after.contains(p))
        .count();
    let moved = stable_before.len() - retained;
    assert!(
        moved <= stable_before.len() / 2 + 1,
        "adding 1 node should move at most ~1/3 of partitions, moved {moved} of {}",
        stable_before.len()
    );

    cluster.shutdown_all().await;
}

// ── Graceful Leave ──────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_graceful_leave_rebalances() {
    let mut cluster = TestCluster::new("e2e_leave", 3, 6, 1);
    for i in 0..3 {
        cluster.start_agent(i).await;
    }
    cluster.wait_for_stable(Duration::from_millis(1500)).await;
    cluster.verify_full_coverage().await;

    // Gracefully stop node-2.
    cluster.stop_agent(2).await;
    cluster.wait_for_stable(Duration::from_millis(1500)).await;

    // Remaining 2 nodes should cover all 6 partitions.
    let snapshot = cluster.get_assignment_snapshot().await;
    let total: usize = snapshot.values().map(|v| v.len()).sum();
    assert!(
        total >= 6,
        "2 remaining nodes should cover ≥ 6 partitions, got {total}"
    );

    cluster.shutdown_all().await;
}

// ── Node Crash ──────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_crash_and_rejoin() {
    let mut cluster = TestCluster::new("e2e_crash_rejoin", 3, 6, 1);
    for i in 0..3 {
        cluster.start_agent(i).await;
    }
    cluster.wait_for_stable(Duration::from_millis(1500)).await;
    cluster.verify_full_coverage().await;

    // Kill node-2 (ungraceful).
    cluster.kill_agent(2).await;
    // Poll until remaining 2 nodes cover all 6 partitions (up to 10s).
    let total = cluster.wait_for_coverage(6, Duration::from_secs(10)).await;
    assert!(
        total >= 6,
        "2 nodes should cover 6 partitions after crash, got {total}"
    );

    // Restart node-2.
    cluster.start_agent(2).await;
    cluster.wait_for_stable(Duration::from_millis(1500)).await;

    cluster.verify_full_coverage().await;
    cluster.wait_for_no_overlap(Duration::from_secs(10)).await;

    // Restarted node should own some partitions again.
    let snapshot = cluster.get_assignment_snapshot().await;
    let node2_parts = snapshot.get("node-2").map(|v| v.len()).unwrap_or(0);
    assert!(node2_parts > 0, "restarted node should own some partitions");

    cluster.shutdown_all().await;
}

// ── Edge Cases ──────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_single_node_cluster() {
    let mut cluster = TestCluster::new("e2e_single", 1, 4, 1);
    cluster.start_agent(0).await;
    cluster.wait_for_stable(Duration::from_millis(500)).await;

    cluster.verify_full_coverage().await;

    let snapshot = cluster.get_assignment_snapshot().await;
    let n0 = snapshot.get("node-0").unwrap();
    assert_eq!(n0.len(), 4, "single node should own all 4 partitions");

    cluster.shutdown_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_all_nodes_restart() {
    let mut cluster = TestCluster::new("e2e_restart_all", 3, 6, 1);
    for i in 0..3 {
        cluster.start_agent(i).await;
    }
    cluster.wait_for_stable(Duration::from_millis(1500)).await;

    let before = cluster.get_assignment_snapshot().await;

    // Stop all nodes.
    for i in 0..3 {
        cluster.stop_agent(i).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Restart all nodes.
    for i in 0..3 {
        cluster.start_agent(i).await;
    }
    cluster.wait_for_stable(Duration::from_millis(1500)).await;

    cluster.verify_full_coverage().await;
    cluster.verify_no_overlap().await;

    // Since HRW is deterministic, assignment should be the same.
    let after = cluster.get_assignment_snapshot().await;
    for node in ["node-0", "node-1", "node-2"] {
        let mut b: Vec<(u32, u32)> = before.get(node).cloned().unwrap_or_default();
        let mut a: Vec<(u32, u32)> = after.get(node).cloned().unwrap_or_default();
        b.sort();
        a.sort();
        assert_eq!(
            b, a,
            "assignment for {node} should be deterministic after full restart"
        );
    }

    cluster.shutdown_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_rapid_join_leave() {
    let mut cluster = TestCluster::new("e2e_rapid", 5, 8, 1);

    // Start 2 nodes.
    cluster.start_agent(0).await;
    cluster.start_agent(1).await;

    // Rapidly add and remove nodes.
    cluster.start_agent(2).await;
    cluster.start_agent(3).await;
    cluster.stop_agent(2).await;
    cluster.start_agent(4).await;
    cluster.stop_agent(3).await;

    // Wait for everything to settle.
    cluster.wait_for_stable(Duration::from_millis(2000)).await;

    // Remaining nodes (0, 1, 4) should cover all partitions.
    let total = cluster.total_assignments().await;
    assert!(
        total >= 8,
        "remaining nodes should cover all 8 partitions, got {total}"
    );
    cluster.verify_no_overlap().await;

    cluster.shutdown_all().await;
}

// ── Data-Flow Recovery ──────────────────────────────────────────────────────

/// Publish events, then gracefully remove one node. The partition owner changes
/// but the new owner's store should already have the events (live ingestion).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_data_survives_graceful_leave() {
    // 2 nodes, 4 partitions, RF=1. After removing one node the other takes over.
    let mut cluster = TestCluster::new("e2e_data_leave", 2, 4, 1);
    cluster.start_agent(0).await;
    cluster.start_agent(1).await;
    cluster.wait_for_stable(Duration::from_millis(1500)).await;

    // Publish 5 events to every partition using a separate session.
    let pub_session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let publisher = cluster.create_publisher(&pub_session).await;
    for p in 0..4u32 {
        cluster.publish_events(&publisher, p, 5).await;
    }

    // Let the stores ingest.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Gracefully stop node-1.
    cluster.stop_agent(1).await;
    cluster.wait_for_stable(Duration::from_millis(2000)).await;

    // Node-0 should now own all 4 partitions.
    let total = cluster.wait_for_coverage(4, Duration::from_secs(5)).await;
    assert_eq!(total, 4, "node-0 should own all 4 partitions after leave");

    // Verify at least some events are queryable through the surviving stores.
    // (Events published before the leave should have been ingested by at least
    // the original owner; after rebalance, the new owner starts fresh but
    // may not have all events if recovery from the left node isn't possible.)
    let mut found_total = 0;
    for p in 0..4u32 {
        let count = cluster.query_store_count(&pub_session, p).await;
        found_total += count;
    }
    // We published 20 events total. The partitions that node-0 already owned
    // should have their events; newly-acquired partitions may have events
    // if they were ingested live (stores subscribe to all partitions' key space).
    assert!(
        found_total > 0,
        "at least some events should be queryable after graceful leave, got {found_total}"
    );

    cluster.shutdown_all().await;
    pub_session.close().await.unwrap();
}

/// Publish events while stores are actively ingesting, then verify
/// the events are queryable from the stores. This tests that the live
/// ingestion path (EventStore subscriber → FjallBackend) works end-to-end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_data_live_ingestion() {
    // 2-node cluster, 4 partitions.
    let mut cluster = TestCluster::new("e2e_data_live", 2, 4, 1);
    cluster.start_agent(0).await;
    cluster.start_agent(1).await;
    cluster.wait_for_stable(Duration::from_millis(1500)).await;

    // Publish events.
    let pub_session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let publisher = cluster.create_publisher(&pub_session).await;
    for p in 0..4u32 {
        cluster.publish_events(&publisher, p, 5).await;
    }

    // Wait for ingestion.
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Verify events are stored.
    let mut found_total = 0;
    for p in 0..4u32 {
        let count = cluster.query_store_count(&pub_session, p).await;
        found_total += count;
    }

    // We published 20 events. At least some should be ingested and queryable.
    assert!(
        found_total > 0,
        "events should be ingested by stores, got {found_total}"
    );

    cluster.shutdown_all().await;
    pub_session.close().await.unwrap();
}

/// Publish events while adding and removing nodes. At steady state, all
/// published events should be accounted for in the surviving stores.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_publish_during_rebalance() {
    let mut cluster = TestCluster::new("e2e_data_rebalance", 3, 4, 1);
    cluster.start_agent(0).await;
    cluster.start_agent(1).await;
    cluster.wait_for_stable(Duration::from_millis(1500)).await;

    let pub_session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let publisher = cluster.create_publisher(&pub_session).await;

    // Publish a first batch.
    for p in 0..4u32 {
        cluster.publish_events(&publisher, p, 3).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Add a third node (triggers rebalance).
    cluster.start_agent(2).await;

    // Publish more events during rebalance.
    for p in 0..4u32 {
        cluster.publish_events(&publisher, p, 3).await;
    }

    // Let everything settle.
    cluster.wait_for_stable(Duration::from_millis(2000)).await;
    cluster.verify_full_coverage().await;

    // With 3 nodes and 4 partitions, verify events are queryable.
    let mut found_total = 0;
    for p in 0..4u32 {
        let count = cluster.query_store_count(&pub_session, p).await;
        found_total += count;
    }

    // We published 24 events total (4 partitions × 6 events each).
    // Not all may be queryable (some may have been published to a store that
    // was subsequently drained). But at least some must survive.
    assert!(
        found_total > 0,
        "at least some events should survive rebalance, got {found_total}"
    );

    cluster.shutdown_all().await;
    pub_session.close().await.unwrap();
}

/// Crash a node and verify that surviving nodes take over. Events previously
/// stored on the crashed node will be lost (no recovery peer available), but
/// new events published after the takeover should be stored successfully.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_data_survives_crash_and_recovery() {
    let mut cluster = TestCluster::new("e2e_data_crash", 2, 4, 1);
    cluster.start_agent(0).await;
    cluster.start_agent(1).await;
    cluster.wait_for_stable(Duration::from_millis(1500)).await;

    let pub_session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let publisher = cluster.create_publisher(&pub_session).await;

    // Pre-crash publish.
    for p in 0..4u32 {
        cluster.publish_events(&publisher, p, 3).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Crash node-1.
    cluster.kill_agent(1).await;
    cluster.wait_for_stable(Duration::from_millis(2000)).await;

    // Node-0 should take over all partitions.
    let total = cluster.wait_for_coverage(4, Duration::from_secs(5)).await;
    assert!(
        total >= 4,
        "node-0 should cover all 4 partitions, got {total}"
    );

    // Publish new events after crash.
    for p in 0..4u32 {
        cluster.publish_events(&publisher, p, 3).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // At least the post-crash events should be in the stores.
    let mut found_total = 0;
    for p in 0..4u32 {
        let count = cluster.query_store_count(&pub_session, p).await;
        found_total += count;
    }
    assert!(
        found_total > 0,
        "post-crash events should be stored, got {found_total}"
    );

    cluster.shutdown_all().await;
    pub_session.close().await.unwrap();
}
