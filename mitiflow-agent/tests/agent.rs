//! Integration tests for the top-level StorageAgent.
//!
//! These tests start full StorageAgent instances with real Zenoh sessions
//! and verify partition ownership, rebalancing, overrides, and shutdown.

use std::collections::HashMap;
use std::time::Duration;

use mitiflow::EventBusConfig;
use mitiflow_agent::{StorageAgent, StorageAgentConfig, StorageAgentConfigBuilder};

fn agent_config(
    test_name: &str,
    node_id: &str,
    num_partitions: u32,
    rf: u32,
) -> (tempfile::TempDir, StorageAgentConfig) {
    let tmp = tempfile::tempdir().unwrap();
    let bus_config = EventBusConfig::builder(format!("test/{test_name}"))
        .cache_size(100)
        .build()
        .unwrap();
    let config = StorageAgentConfigBuilder::new(tmp.path().to_path_buf(), bus_config)
        .node_id(node_id)
        .num_partitions(num_partitions)
        .replication_factor(rf)
        .drain_grace_period(Duration::from_millis(200))
        .health_interval(Duration::from_secs(60))
        .build()
        .unwrap();
    (tmp, config)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_starts_and_owns_all_partitions_single_node() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let (_tmp, config) = agent_config("agent_single", "node-only", 4, 1);

    let agent = StorageAgent::start(&session, config).await.unwrap();

    // Single node should own all 4 partitions.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let assigned = agent.assigned_partitions().await;
    assert_eq!(
        assigned.len(),
        4,
        "single node should own all 4 partitions, got {:?}",
        assigned
    );

    for p in 0..4 {
        assert!(
            assigned.contains(&(p, 0)),
            "should own partition {p}"
        );
    }

    agent.shutdown().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_two_nodes_split_partitions() {
    let session1 = zenoh::open(zenoh::Config::default()).await.unwrap();
    let session2 = zenoh::open(zenoh::Config::default()).await.unwrap();

    let (_tmp1, config1) = agent_config("agent_split", "node-a", 8, 1);
    let (_tmp2, config2) = agent_config("agent_split", "node-b", 8, 1);

    let agent1 = StorageAgent::start(&session1, config1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let agent2 = StorageAgent::start(&session2, config2).await.unwrap();

    // Wait for rebalance to propagate.
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Force recompute on agent1 since agent2 joining triggers a membership callback.
    agent1.recompute_and_reconcile().await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let a1_parts = agent1.assigned_partitions().await;
    let a2_parts = agent2.assigned_partitions().await;

    // Between them they should own all 8 partitions.
    let total = a1_parts.len() + a2_parts.len();
    assert!(
        total >= 8,
        "combined should own ≥ 8 partitions, got {} (a1={}, a2={})",
        total,
        a1_parts.len(),
        a2_parts.len()
    );

    // Each should own roughly 4 (±2). HRW isn't perfectly balanced.
    assert!(
        a1_parts.len() >= 2 && a1_parts.len() <= 6,
        "node-a should own 2-6 partitions, got {}",
        a1_parts.len()
    );

    // No overlap: no partition should be owned by both.
    for pr in &a1_parts {
        assert!(
            !a2_parts.contains(pr),
            "partition {:?} should not be on both nodes",
            pr
        );
    }

    agent1.shutdown().await.unwrap();
    agent2.shutdown().await.unwrap();
    session1.close().await.unwrap();
    session2.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_node_leave_triggers_rebalance() {
    let session1 = zenoh::open(zenoh::Config::default()).await.unwrap();
    let session2 = zenoh::open(zenoh::Config::default()).await.unwrap();
    let session3 = zenoh::open(zenoh::Config::default()).await.unwrap();

    let (_tmp1, config1) = agent_config("agent_leave", "node-a", 6, 1);
    let (_tmp2, config2) = agent_config("agent_leave", "node-b", 6, 1);
    let (_tmp3, config3) = agent_config("agent_leave", "node-c", 6, 1);

    let agent1 = StorageAgent::start(&session1, config1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let agent2 = StorageAgent::start(&session2, config2).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let agent3 = StorageAgent::start(&session3, config3).await.unwrap();

    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Record initial assignments.
    let _before = agent1.assigned_partitions().await.len()
        + agent2.assigned_partitions().await.len()
        + agent3.assigned_partitions().await.len();

    // Shutdown agent3 (simulates graceful leave).
    agent3.shutdown().await.unwrap();
    session3.close().await.unwrap();

    // Wait for rebalance.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Force recompute on remaining agents.
    agent1.recompute_and_reconcile().await.unwrap();
    agent2.recompute_and_reconcile().await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let a1_parts = agent1.assigned_partitions().await;
    let a2_parts = agent2.assigned_partitions().await;
    let total = a1_parts.len() + a2_parts.len();

    assert!(
        total >= 6,
        "remaining 2 nodes should own ≥ 6 partitions, got {}",
        total
    );

    agent1.shutdown().await.unwrap();
    agent2.shutdown().await.unwrap();
    session1.close().await.unwrap();
    session2.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_shutdown_drains_stores() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let (_tmp, config) = agent_config("agent_drain_shutdown", "node-only", 4, 1);

    let agent = StorageAgent::start(&session, config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(agent.assigned_partitions().await.len(), 4);

    // Shutdown should drain all stores.
    agent.shutdown().await.unwrap();

    // After shutdown, no active stores.
    assert!(
        agent.assigned_partitions().await.is_empty(),
        "all stores should be stopped after shutdown"
    );

    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_respects_overrides() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let (_tmp1, config1) = agent_config("agent_override", "node-a", 4, 1);
    let (_tmp2, config2) = agent_config("agent_override", "node-b", 4, 1);

    let agent1 = StorageAgent::start(&session, config1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let agent2 = StorageAgent::start(&session, config2).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Publish an override table pinning partition 0 to node-b.
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
    let override_key = "test/agent_override/_cluster/overrides";
    let bytes = serde_json::to_vec(&override_table).unwrap();
    session.put(override_key, bytes).await.unwrap();

    // Wait for override to be processed and trigger rebalance.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Re-reconcile both agents with override awareness.
    agent1.recompute_and_reconcile().await.unwrap();
    agent2.recompute_and_reconcile().await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // agent2 (node-b) should own partition 0 due to override.
    let a2_parts = agent2.assigned_partitions().await;
    assert!(
        a2_parts.contains(&(0, 0)),
        "node-b should own p0 due to override, got {:?}",
        a2_parts
    );

    agent1.shutdown().await.unwrap();
    agent2.shutdown().await.unwrap();
    session.close().await.unwrap();
}

// --- Phase H: Multi-Replica & Rack-Awareness ---

fn agent_config_with_labels(
    test_name: &str,
    node_id: &str,
    num_partitions: u32,
    rf: u32,
    labels: HashMap<String, String>,
) -> (tempfile::TempDir, StorageAgentConfig) {
    let tmp = tempfile::tempdir().unwrap();
    let bus_config = EventBusConfig::builder(format!("test/{test_name}"))
        .cache_size(100)
        .build()
        .unwrap();
    let config = StorageAgentConfigBuilder::new(tmp.path().to_path_buf(), bus_config)
        .node_id(node_id)
        .num_partitions(num_partitions)
        .replication_factor(rf)
        .drain_grace_period(Duration::from_millis(200))
        .health_interval(Duration::from_secs(60))
        .labels(labels)
        .build()
        .unwrap();
    (tmp, config)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_multi_replica_rf2() {
    // 3 nodes, RF=2, 4 partitions → each partition stored on 2 nodes.
    let s1 = zenoh::open(zenoh::Config::default()).await.unwrap();
    let s2 = zenoh::open(zenoh::Config::default()).await.unwrap();
    let s3 = zenoh::open(zenoh::Config::default()).await.unwrap();

    let (_t1, c1) = agent_config("agent_rf2", "node-a", 4, 2);
    let (_t2, c2) = agent_config("agent_rf2", "node-b", 4, 2);
    let (_t3, c3) = agent_config("agent_rf2", "node-c", 4, 2);

    let a1 = StorageAgent::start(&s1, c1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let a2 = StorageAgent::start(&s2, c2).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let a3 = StorageAgent::start(&s3, c3).await.unwrap();

    // Wait for rebalance.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    a1.recompute_and_reconcile().await.unwrap();
    a2.recompute_and_reconcile().await.unwrap();
    a3.recompute_and_reconcile().await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let p1 = a1.assigned_partitions().await;
    let p2 = a2.assigned_partitions().await;
    let p3 = a3.assigned_partitions().await;

    // Total: 4 partitions × 2 replicas = 8 assignments across 3 nodes.
    let total = p1.len() + p2.len() + p3.len();
    assert!(
        total >= 8,
        "RF=2 × 4 partitions = 8 total assignments, got {}",
        total
    );

    // Each partition should have exactly 2 replicas (r0 and r1) across all nodes.
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
        // The two replicas should be on different nodes.
        let nodes: Vec<&str> = owners.iter().map(|(n, _)| *n).collect();
        assert_ne!(
            nodes[0], nodes[1],
            "partition {partition} replicas should be on different nodes"
        );
    }

    a1.shutdown().await.unwrap();
    a2.shutdown().await.unwrap();
    a3.shutdown().await.unwrap();
    s1.close().await.unwrap();
    s2.close().await.unwrap();
    s3.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_rack_aware_placement() {
    // 4 nodes across 2 racks, RF=2 → replicas preferably on separate racks.
    let s1 = zenoh::open(zenoh::Config::default()).await.unwrap();
    let s2 = zenoh::open(zenoh::Config::default()).await.unwrap();
    let s3 = zenoh::open(zenoh::Config::default()).await.unwrap();
    let s4 = zenoh::open(zenoh::Config::default()).await.unwrap();

    let mut rack_a = HashMap::new();
    rack_a.insert("rack".to_string(), "rack-A".to_string());
    let mut rack_b = HashMap::new();
    rack_b.insert("rack".to_string(), "rack-B".to_string());

    let (_t1, c1) = agent_config_with_labels("agent_rack", "node-1", 4, 2, rack_a.clone());
    let (_t2, c2) = agent_config_with_labels("agent_rack", "node-2", 4, 2, rack_a);
    let (_t3, c3) = agent_config_with_labels("agent_rack", "node-3", 4, 2, rack_b.clone());
    let (_t4, c4) = agent_config_with_labels("agent_rack", "node-4", 4, 2, rack_b);

    let a1 = StorageAgent::start(&s1, c1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let a2 = StorageAgent::start(&s2, c2).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let a3 = StorageAgent::start(&s3, c3).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let a4 = StorageAgent::start(&s4, c4).await.unwrap();

    tokio::time::sleep(Duration::from_millis(1500)).await;
    a1.recompute_and_reconcile().await.unwrap();
    a2.recompute_and_reconcile().await.unwrap();
    a3.recompute_and_reconcile().await.unwrap();
    a4.recompute_and_reconcile().await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let p1 = a1.assigned_partitions().await;
    let p2 = a2.assigned_partitions().await;
    let p3 = a3.assigned_partitions().await;
    let p4 = a4.assigned_partitions().await;

    // Build a map: for each (partition, replica) → which node owns it.
    let mut assignment_map: HashMap<(u32, u32), &str> = HashMap::new();
    for (parts, name) in [(&p1, "node-1"), (&p2, "node-2"), (&p3, "node-3"), (&p4, "node-4")] {
        for &pr in parts {
            assignment_map.insert(pr, name);
        }
    }

    let node_rack = |name: &str| -> &str {
        match name {
            "node-1" | "node-2" => "rack-A",
            "node-3" | "node-4" => "rack-B",
            _ => "unknown",
        }
    };

    // For each partition, check that replicas are on different racks.
    let mut cross_rack_count = 0;
    for partition in 0..4u32 {
        let r0_owner = assignment_map.get(&(partition, 0));
        let r1_owner = assignment_map.get(&(partition, 1));
        if let (Some(o0), Some(o1)) = (r0_owner, r1_owner) {
            if node_rack(o0) != node_rack(o1) {
                cross_rack_count += 1;
            }
        }
    }

    // With 2 racks and RF=2, rack-aware placement should put replicas
    // on different racks for most/all partitions.
    assert!(
        cross_rack_count >= 3,
        "at least 3 of 4 partitions should have cross-rack replicas, got {}",
        cross_rack_count
    );

    a1.shutdown().await.unwrap();
    a2.shutdown().await.unwrap();
    a3.shutdown().await.unwrap();
    a4.shutdown().await.unwrap();
    s1.close().await.unwrap();
    s2.close().await.unwrap();
    s3.close().await.unwrap();
    s4.close().await.unwrap();
}
