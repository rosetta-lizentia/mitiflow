//! Integration tests for the RecoveryManager.
//!
//! These test the recovery protocol where a node queries peer nodes
//! (via EventStore queryable) for missing events and stores them locally.

use std::sync::Arc;
use std::time::Duration;

use mitiflow::store::backend::StorageBackend;
use mitiflow::store::query::QueryFilters;
use mitiflow::{EventBusConfig, EventPublisher, EventStore, FjallBackend};
use mitiflow_storage::recovery::RecoveryManager;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_from_peer_no_peers() {
    // Recovery with no peers should return 0 events recovered.
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let recovery = RecoveryManager::new(&session, "test/recovery_no_peers");

    let tmp = tempfile::tempdir().unwrap();
    let backend = Arc::new(FjallBackend::open(tmp.path(), 0).unwrap());

    let recovered = recovery
        .recover(0, &(backend as Arc<dyn StorageBackend>), &[])
        .await
        .unwrap();
    assert_eq!(recovered, 0, "no peers = no recovery");

    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_from_peer_unreachable() {
    // Recovery querying a non-existent peer queryable should not error,
    // just return 0 recovered events (graceful fallback).
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let recovery = RecoveryManager::new(&session, "test/recovery_unreachable");

    let tmp = tempfile::tempdir().unwrap();
    let backend = Arc::new(FjallBackend::open(tmp.path(), 0).unwrap());

    let recovered = recovery
        .recover(
            0,
            &(backend as Arc<dyn StorageBackend>),
            &["ghost-node".to_string()],
        )
        .await
        .unwrap();
    // With no queryable listening on the store key, get() will return 0 replies.
    assert_eq!(recovered, 0, "unreachable peer = 0 recovered");

    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_from_peer_with_store() {
    // Set up a real EventStore as a peer, publish events, then recover
    // into a fresh backend and verify the events were stored.
    let prefix = "test/recovery_store_real";
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    // --- Peer: EventStore for partition 0 with some published events ---
    let peer_tmp = tempfile::tempdir().unwrap();
    let peer_backend = FjallBackend::open(peer_tmp.path(), 0).unwrap();
    let bus_config = EventBusConfig::builder(prefix)
        .cache_size(100)
        .build()
        .unwrap();
    let mut peer_store = EventStore::new(&session, peer_backend, bus_config.clone());
    peer_store.run().await.unwrap();

    // Publish 5 events to partition 0.
    let publisher = EventPublisher::new(&session, bus_config.clone())
        .await
        .unwrap();
    let pub_key = format!("{prefix}/p/0/test");
    for i in 0..5u32 {
        publisher
            .publish_bytes_to(&pub_key, format!("event-{i}").into_bytes())
            .await
            .unwrap();
    }
    // Wait for store ingestion.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify peer store has 5 events.
    let peer_events = peer_store.query(&QueryFilters::default()).await.unwrap();
    assert_eq!(peer_events.len(), 5, "peer store should have 5 events");

    // --- Recovery: fresh backend on same session ---
    let local_tmp = tempfile::tempdir().unwrap();
    let local_backend: Arc<dyn StorageBackend> =
        Arc::new(FjallBackend::open(local_tmp.path(), 0).unwrap());

    let recovery = RecoveryManager::new(&session, prefix);
    let recovered = recovery
        .recover(0, &local_backend, &["peer-node".to_string()])
        .await
        .unwrap();

    assert_eq!(recovered, 5, "should recover 5 events from peer store");

    // Verify local backend has the events.
    let local_events = local_backend.query(&QueryFilters::default()).unwrap();
    assert_eq!(local_events.len(), 5, "local backend should have 5 events");

    peer_store.shutdown();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_from_cache_no_queryable() {
    // When no publisher cache queryable exists, cache recovery returns 0.
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let recovery = RecoveryManager::new(&session, "test/recovery_cache_empty");

    let tmp = tempfile::tempdir().unwrap();
    let backend: Arc<dyn StorageBackend> = Arc::new(FjallBackend::open(tmp.path(), 0).unwrap());

    let recovered = recovery.recover_from_cache(0, &backend).await.unwrap();
    assert_eq!(recovered, 0, "no cache queryable = 0 recovered");

    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_idempotent() {
    // Recovering the same events twice should produce the same count in backend.
    // FjallBackend's (publisher_id, seq) keying makes this a no-op overwrite.
    let prefix = "test/recovery_idempotent_data";
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    // --- Peer store with 3 events ---
    let peer_tmp = tempfile::tempdir().unwrap();
    let peer_backend = FjallBackend::open(peer_tmp.path(), 0).unwrap();
    let bus_config = EventBusConfig::builder(prefix)
        .cache_size(100)
        .build()
        .unwrap();
    let mut peer_store = EventStore::new(&session, peer_backend, bus_config.clone());
    peer_store.run().await.unwrap();

    let publisher = EventPublisher::new(&session, bus_config.clone())
        .await
        .unwrap();
    let pub_key = format!("{prefix}/p/0/test");
    for i in 0..3u32 {
        publisher
            .publish_bytes_to(&pub_key, format!("event-{i}").into_bytes())
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // --- First recovery ---
    let local_tmp = tempfile::tempdir().unwrap();
    let local_backend: Arc<dyn StorageBackend> =
        Arc::new(FjallBackend::open(local_tmp.path(), 0).unwrap());

    let recovery = RecoveryManager::new(&session, prefix);
    let r1 = recovery
        .recover(0, &local_backend, &["peer-node".to_string()])
        .await
        .unwrap();
    assert_eq!(r1, 3);

    // --- Second recovery (same events) ---
    let r2 = recovery
        .recover(0, &local_backend, &["peer-node".to_string()])
        .await
        .unwrap();
    // The recovery function should still count 3 events stored (idempotent overwrites).
    assert_eq!(r2, 3);

    // But the backend should still only have 3 distinct events.
    let events = local_backend.query(&QueryFilters::default()).unwrap();
    assert_eq!(events.len(), 3, "idempotent: 3 events, not 6");

    peer_store.shutdown();
    session.close().await.unwrap();
}
