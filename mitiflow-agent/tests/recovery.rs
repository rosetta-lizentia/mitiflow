//! Integration tests for the RecoveryManager.
//!
//! These test the recovery protocol where a node queries peer nodes
//! (via EventStore queryable) for missing events.

use std::time::Duration;

use mitiflow_agent::recovery::RecoveryManager;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_from_peer_no_peers() {
    // Recovery with no peers should return 0 events recovered.
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let recovery = RecoveryManager::new(&session, "test/recovery_no_peers");

    let recovered = recovery.recover(0, &[]).await.unwrap();
    assert_eq!(recovered, 0, "no peers = no recovery");

    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_from_peer_unreachable() {
    // Recovery querying a non-existent peer queryable should not error,
    // just return 0 recovered events (graceful fallback).
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let recovery = RecoveryManager::new(&session, "test/recovery_unreachable");

    let recovered = recovery
        .recover(0, &["ghost-node".to_string()])
        .await
        .unwrap();
    // With no queryable listening on the store key, get() will return 0 replies.
    assert_eq!(recovered, 0, "unreachable peer = 0 recovered");

    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_from_peer_with_store() {
    // Set up a queryable simulating a peer's EventStore queryable, then
    // verify RecoveryManager queries it and counts replies.
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let key_prefix = "test/recovery_store";
    let store_key = format!("{key_prefix}/_store/0");

    // Declare a queryable that will respond with 3 mock events.
    let queryable = session.declare_queryable(&store_key).await.unwrap();
    let query_task = tokio::spawn({
        let store_key = store_key.clone();
        async move {
            // Serve exactly one query, replying with 3 events.
            if let Ok(query) = queryable.recv_async().await {
                for i in 0..3u8 {
                    let _ = query
                        .reply(&store_key, vec![i; 10])
                        .await;
                }
            }
        }
    });

    // Small delay to let queryable register.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let recovery = RecoveryManager::new(&session, key_prefix);
    let recovered = recovery
        .recover(0, &["peer-node".to_string()])
        .await
        .unwrap();

    assert_eq!(recovered, 3, "should recover 3 events from queryable");

    query_task.abort();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_from_cache_no_queryable() {
    // When no publisher cache queryable exists, cache recovery returns 0.
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let recovery = RecoveryManager::new(&session, "test/recovery_cache_empty");

    let recovered = recovery.recover_from_cache(0).await.unwrap();
    assert_eq!(recovered, 0, "no cache queryable = 0 recovered");

    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_idempotent() {
    // Calling recover twice with the same data should not cause errors.
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let recovery = RecoveryManager::new(&session, "test/recovery_idempotent");

    // First call.
    let r1 = recovery.recover(0, &[]).await.unwrap();
    // Second call.
    let r2 = recovery.recover(0, &[]).await.unwrap();
    assert_eq!(r1, r2, "repeated recovery is idempotent");

    session.close().await.unwrap();
}
