//! Integration tests for the Reconciler.
//!
//! Each test creates a real Zenoh session and a Reconciler backed by tempdir,
//! then exercises the desired vs actual diff and lifecycle transitions.

use std::time::Duration;

use mitiflow::EventBusConfig;
use mitiflow_agent::reconciler::{ReconcileAction, Reconciler};
use mitiflow_agent::StoreState;

fn bus_config(test_name: &str) -> EventBusConfig {
    EventBusConfig::builder(format!("test/{test_name}"))
        .cache_size(100)
        .build()
        .expect("valid bus config")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconciler_starts_stores_for_assigned_partitions() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let config = bus_config("reconciler_start");

    let reconciler = Reconciler::new(
        "node-1".into(),
        tmp.path().to_path_buf(),
        session.clone(),
        config,
        Duration::from_secs(5),
    );

    // Desired: partitions 0 and 3 (replica 0).
    let desired = vec![(0, 0), (3, 0)];
    let actions = reconciler.reconcile(&desired).await.unwrap();

    // Should have started 2 stores.
    assert_eq!(actions.len(), 2);
    assert!(actions.contains(&ReconcileAction::Start { partition: 0, replica: 0 }));
    assert!(actions.contains(&ReconcileAction::Start { partition: 3, replica: 0 }));

    // Active stores should match.
    let active = reconciler.active_stores().await;
    assert!(active.contains(&(0, 0)));
    assert!(active.contains(&(3, 0)));

    reconciler.shutdown_all().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconciler_stops_stores_for_lost_partitions() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let config = bus_config("reconciler_stop");

    let reconciler = Reconciler::new(
        "node-1".into(),
        tmp.path().to_path_buf(),
        session.clone(),
        config,
        Duration::from_millis(100), // Short grace period for test speed.
    );

    // Start with p0, p3.
    reconciler.reconcile(&[(0, 0), (3, 0)]).await.unwrap();
    assert_eq!(reconciler.active_stores().await.len(), 2);

    // New desired: only p0. p3 should drain.
    let actions = reconciler.reconcile(&[(0, 0)]).await.unwrap();
    assert_eq!(actions.len(), 1);
    assert!(actions.contains(&ReconcileAction::Drain { partition: 3, replica: 0 }));

    // After grace period, the drained store should be removed.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let active = reconciler.active_stores().await;
    assert_eq!(active.len(), 1);
    assert!(active.contains(&(0, 0)));

    reconciler.shutdown_all().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconciler_noop_when_aligned() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let config = bus_config("reconciler_noop");

    let reconciler = Reconciler::new(
        "node-1".into(),
        tmp.path().to_path_buf(),
        session.clone(),
        config,
        Duration::from_secs(5),
    );

    let desired = vec![(0, 0), (1, 0)];
    reconciler.reconcile(&desired).await.unwrap();

    // Reconcile again with the same desired set → no actions.
    let actions = reconciler.reconcile(&desired).await.unwrap();
    assert!(actions.is_empty(), "no actions when already aligned");

    reconciler.shutdown_all().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconciler_handles_simultaneous_gain_and_loss() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let config = bus_config("reconciler_gain_loss");

    let reconciler = Reconciler::new(
        "node-1".into(),
        tmp.path().to_path_buf(),
        session.clone(),
        config,
        Duration::from_millis(100),
    );

    // Start with p0, p3.
    reconciler.reconcile(&[(0, 0), (3, 0)]).await.unwrap();

    // New desired: p0, p5. Should start p5 and drain p3.
    let actions = reconciler.reconcile(&[(0, 0), (5, 0)]).await.unwrap();
    assert!(actions.contains(&ReconcileAction::Start { partition: 5, replica: 0 }));
    assert!(actions.contains(&ReconcileAction::Drain { partition: 3, replica: 0 }));

    // Verify p5 is active.
    let active = reconciler.active_stores().await;
    assert!(active.contains(&(0, 0)));
    assert!(active.contains(&(5, 0)));

    reconciler.shutdown_all().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconciler_drain_grace_period() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let config = bus_config("reconciler_drain");

    let reconciler = Reconciler::new(
        "node-1".into(),
        tmp.path().to_path_buf(),
        session.clone(),
        config,
        Duration::from_millis(500), // 500ms grace period.
    );

    reconciler.reconcile(&[(0, 0)]).await.unwrap();

    // Remove p0 → should enter Draining.
    let actions = reconciler.reconcile(&[]).await.unwrap();
    assert!(actions.contains(&ReconcileAction::Drain { partition: 0, replica: 0 }));

    // Immediately check: partition should be in Draining state.
    let statuses = reconciler.partition_statuses().await;
    let p0 = statuses.iter().find(|s| s.partition == 0).unwrap();
    assert_eq!(p0.state, StoreState::Draining, "should be draining");

    // Before grace period elapses, store still exists.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let statuses = reconciler.partition_statuses().await;
    assert!(!statuses.is_empty(), "store should still exist during drain");

    // After grace period, store should be gone.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let statuses = reconciler.partition_statuses().await;
    assert!(statuses.is_empty(), "store should be removed after drain");

    // No active stores left.
    assert!(reconciler.active_stores().await.is_empty());

    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconciler_tracks_store_state_transitions() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let config = bus_config("reconciler_states");

    let reconciler = Reconciler::new(
        "node-1".into(),
        tmp.path().to_path_buf(),
        session.clone(),
        config,
        Duration::from_secs(5),
    );

    // Start p0.
    reconciler.reconcile(&[(0, 0)]).await.unwrap();

    // Check Status: should be Active (store starts directly as Active).
    let statuses = reconciler.partition_statuses().await;
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].partition, 0);
    assert_eq!(statuses[0].state, StoreState::Active);

    reconciler.shutdown_all().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconciler_multi_replica() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let config = bus_config("reconciler_multi_replica");

    let reconciler = Reconciler::new(
        "node-1".into(),
        tmp.path().to_path_buf(),
        session.clone(),
        config,
        Duration::from_secs(5),
    );

    // This node is assigned p0/r0 and p1/r1 (different partitions, different replicas).
    let desired = vec![(0, 0), (1, 1)];
    let actions = reconciler.reconcile(&desired).await.unwrap();

    assert_eq!(actions.len(), 2);
    let active = reconciler.active_stores().await;
    assert!(active.contains(&(0, 0)));
    assert!(active.contains(&(1, 1)));

    // Each store should use a distinct data path.
    let path_0_0 = tmp.path().join("0/0");
    let path_1_1 = tmp.path().join("1/1");
    assert!(path_0_0.exists(), "p0/r0 data dir should exist");
    assert!(path_1_1.exists(), "p1/r1 data dir should exist");

    reconciler.shutdown_all().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconciler_shutdown_all_stops_everything() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let config = bus_config("reconciler_shutdown");

    let reconciler = Reconciler::new(
        "node-1".into(),
        tmp.path().to_path_buf(),
        session.clone(),
        config,
        Duration::from_secs(5),
    );

    reconciler.reconcile(&[(0, 0), (1, 0), (2, 0)]).await.unwrap();
    assert_eq!(reconciler.active_stores().await.len(), 3);

    reconciler.shutdown_all().await.unwrap();
    assert!(reconciler.active_stores().await.is_empty(), "all stores stopped after shutdown");

    session.close().await.unwrap();
}
