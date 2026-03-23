//! Multi-process smoke tests.
//!
//! These tests spawn actual `mitiflow-agent` binaries as subprocesses.
//! They verify that agents start, discover each other, and respond to
//! process lifecycle events (SIGTERM, SIGKILL).

use std::time::Duration;

use super::helpers::SmokeCluster;

// ── Basic Cluster Startup ───────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn smoke_3_node_cluster_starts() {
    let mut cluster = SmokeCluster::new("smoke_3node", 6, 1);
    cluster.spawn_agent("agent-0");
    cluster.spawn_agent("agent-1");
    cluster.spawn_agent("agent-2");

    // Observer session to check health and liveliness.
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let healthy = cluster
        .wait_healthy(&session, Duration::from_secs(15))
        .await;
    assert!(healthy, "all 3 agents should become healthy within 15s");

    // Verify all 3 agents are live.
    let live = cluster.get_live_agents(&session).await;
    assert_eq!(live.len(), 3, "3 agents should be live, got: {live:?}");

    cluster.kill_all().await;
    session.close().await.unwrap();
}

// ── Graceful Shutdown ───────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn smoke_graceful_shutdown() {
    let mut cluster = SmokeCluster::new("smoke_graceful", 4, 1);
    cluster.spawn_agent("agent-0");

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let healthy = cluster
        .wait_healthy(&session, Duration::from_secs(10))
        .await;
    assert!(healthy, "agent should become healthy");

    // Send SIGTERM.
    cluster.agents[0].terminate();
    let status = cluster.agents[0].wait_exit(Duration::from_secs(10)).await;
    assert!(
        status.is_some(),
        "agent should exit after SIGTERM within 10s"
    );

    // After exit, liveliness should expire.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let live = cluster.get_live_agents(&session).await;
    assert!(
        !live.contains(&"agent-0".to_string()),
        "agent-0 should no longer be live after shutdown"
    );

    session.close().await.unwrap();
}

// ── Process Crash ───────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn smoke_process_crash() {
    let mut cluster = SmokeCluster::new("smoke_crash", 4, 1);
    cluster.spawn_agent("agent-0");
    cluster.spawn_agent("agent-1");

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let healthy = cluster
        .wait_healthy(&session, Duration::from_secs(10))
        .await;
    assert!(healthy, "both agents should become healthy");

    // Kill agent-1 (SIGKILL).
    cluster.agents[1].kill();
    let status = cluster.agents[1].wait_exit(Duration::from_secs(5)).await;
    assert!(status.is_some(), "killed agent should exit");

    // Wait for liveliness to expire.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let live = cluster.get_live_agents(&session).await;
    assert!(
        live.contains(&"agent-0".to_string()),
        "agent-0 should still be live"
    );

    cluster.kill_all().await;
    session.close().await.unwrap();
}
