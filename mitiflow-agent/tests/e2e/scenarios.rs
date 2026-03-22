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
    let stable_before: Vec<(u32, u32)> = before
        .get("node-0")
        .cloned()
        .unwrap_or_default();

    // Add third node.
    cluster.start_agent(2).await;
    cluster.wait_for_stable(Duration::from_millis(1500)).await;

    let after = cluster.get_assignment_snapshot().await;
    let stable_after: Vec<(u32, u32)> = after
        .get("node-0")
        .cloned()
        .unwrap_or_default();

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
    assert!(total >= 6, "2 nodes should cover 6 partitions after crash, got {total}");

    // Restart node-2.
    cluster.start_agent(2).await;
    cluster.wait_for_stable(Duration::from_millis(1500)).await;

    cluster.verify_full_coverage().await;
    cluster.wait_for_no_overlap(Duration::from_secs(10)).await;

    // Restarted node should own some partitions again.
    let snapshot = cluster.get_assignment_snapshot().await;
    let node2_parts = snapshot.get("node-2").map(|v| v.len()).unwrap_or(0);
    assert!(
        node2_parts > 0,
        "restarted node should own some partitions"
    );

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
        assert_eq!(b, a, "assignment for {node} should be deterministic after full restart");
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
