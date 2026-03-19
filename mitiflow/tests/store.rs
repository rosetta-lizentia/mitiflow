//! Integration tests for the FjallBackend and EventStore.

mod common;

use std::time::Duration;

use chrono::Utc;
use serde::{Deserialize, Serialize};

use mitiflow::store::FjallBackend;
use mitiflow::store::backend::{EventMetadata, StorageBackend};
use mitiflow::store::query::QueryFilters;
use mitiflow::types::{EventId, PublisherId};
use mitiflow::{Event, EventBusConfig, EventPublisher, EventStore, HeartbeatMode};

use common::TestPayload;

fn temp_dir(name: &str) -> tempfile::TempDir {
    common::temp_dir(name)
}

// ---------------------------------------------------------------------------
// FjallBackend unit-level tests (no Zenoh needed)
// ---------------------------------------------------------------------------

#[test]
fn fjall_store_and_query() {
    let dir = temp_dir("store_query");
    let backend = FjallBackend::open(dir.path(), 0).unwrap();

    let pub_id = PublisherId::new();
    for seq in 0..5u64 {
        let payload = serde_json::to_vec(&TestPayload { value: seq }).unwrap();
        let key = format!("test/events/{seq}");
        let meta = EventMetadata {
            seq,
            publisher_id: pub_id,
            event_id: EventId::new(),
            timestamp: Utc::now(),
            key_expr: key.clone(),
        };
        backend.store(&key, &payload, meta).unwrap();
    }

    // Query all.
    let all = backend.query(&QueryFilters::default()).unwrap();
    assert_eq!(all.len(), 5);

    // Query with after_seq.
    let filtered = backend
        .query(&QueryFilters {
            after_seq: Some(2),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(filtered.len(), 2); // seq 3, 4

    // Query with limit.
    let limited = backend
        .query(&QueryFilters {
            limit: Some(2),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(limited.len(), 2);
}

#[test]
fn fjall_committed_seq_contiguous() {
    let dir = temp_dir("committed_seq");
    let backend = FjallBackend::open(dir.path(), 0).unwrap();

    let pub_id = PublisherId::new();
    // Store seq 0, 1, 2 contiguously.
    for seq in 0..3u64 {
        let payload = b"{}".to_vec();
        let key = format!("test/events/{seq}");
        let meta = EventMetadata {
            seq,
            publisher_id: pub_id,
            event_id: EventId::new(),
            timestamp: Utc::now(),
            key_expr: key.clone(),
        };
        backend.store(&key, &payload, meta).unwrap();
    }

    assert_eq!(backend.publisher_watermarks()[&pub_id].committed_seq, 2);
}

#[test]
fn fjall_gaps_detected() {
    let dir = temp_dir("gaps");
    let backend = FjallBackend::open(dir.path(), 0).unwrap();

    let pub_id = PublisherId::new();
    // Store seq 0, 1, 3 (skip 2).
    for seq in [0, 1, 3] {
        let payload = b"{}".to_vec();
        let key = format!("test/events/{seq}");
        let meta = EventMetadata {
            seq,
            publisher_id: pub_id,
            event_id: EventId::new(),
            timestamp: Utc::now(),
            key_expr: key.clone(),
        };
        backend.store(&key, &payload, meta).unwrap();
    }

    let wms = backend.publisher_watermarks();
    let pw = &wms[&pub_id];
    assert_eq!(pw.gaps, vec![2]);
}

#[test]
fn fjall_gc_removes_old_events() {
    let dir = temp_dir("gc");
    let backend = FjallBackend::open(dir.path(), 0).unwrap();

    let pub_id = PublisherId::new();
    let old_time = Utc::now() - chrono::Duration::hours(2);
    let new_time = Utc::now();

    // Store an old event.
    backend
        .store(
            "test/old",
            b"{}",
            EventMetadata {
                seq: 0,
                publisher_id: pub_id,
                event_id: EventId::new(),
                timestamp: old_time,
                key_expr: "test/old".to_string(),
            },
        )
        .unwrap();

    // Store a new event.
    backend
        .store(
            "test/new",
            b"{}",
            EventMetadata {
                seq: 1,
                publisher_id: pub_id,
                event_id: EventId::new(),
                timestamp: new_time,
                key_expr: "test/new".to_string(),
            },
        )
        .unwrap();

    // GC events older than 1 hour ago.
    let cutoff = Utc::now() - chrono::Duration::hours(1);
    let removed = backend.gc(cutoff).unwrap();
    assert_eq!(removed, 1);

    // Only the new event should remain.
    let remaining = backend.query(&QueryFilters::default()).unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].metadata.seq, 1);
}

#[test]
fn fjall_compact_keeps_latest_per_key() {
    let dir = temp_dir("compact");
    let backend = FjallBackend::open(dir.path(), 0).unwrap();

    let pub_id = PublisherId::new();

    // Store two versions of the same key_expr.
    backend
        .store(
            "test/sensor/1",
            b"{\"v\":1}",
            EventMetadata {
                seq: 0,
                publisher_id: pub_id,
                event_id: EventId::new(),
                timestamp: Utc::now(),
                key_expr: "test/sensor/1".to_string(),
            },
        )
        .unwrap();

    backend
        .store(
            "test/sensor/1",
            b"{\"v\":2}",
            EventMetadata {
                seq: 1,
                publisher_id: pub_id,
                event_id: EventId::new(),
                timestamp: Utc::now(),
                key_expr: "test/sensor/1".to_string(),
            },
        )
        .unwrap();

    // Store a different key.
    backend
        .store(
            "test/sensor/2",
            b"{\"v\":3}",
            EventMetadata {
                seq: 2,
                publisher_id: pub_id,
                event_id: EventId::new(),
                timestamp: Utc::now(),
                key_expr: "test/sensor/2".to_string(),
            },
        )
        .unwrap();

    let stats = backend.compact().unwrap();
    // One removed (old version of sensor/1), two retained (latest sensor/1 + sensor/2).
    assert_eq!(stats.removed, 1);
    assert_eq!(stats.retained, 2);

    // Query should return 2 events.
    let all = backend.query(&QueryFilters::default()).unwrap();
    assert_eq!(all.len(), 2);
}

#[test]
fn fjall_seq_filter_range() {
    let dir = temp_dir("seq_filter");
    let backend = FjallBackend::open(dir.path(), 0).unwrap();

    let pub_id = PublisherId::new();
    for seq in 0..10u64 {
        let payload = serde_json::to_vec(&TestPayload { value: seq }).unwrap();
        let key = format!("test/events/{seq}");
        let meta = EventMetadata {
            seq,
            publisher_id: pub_id,
            event_id: EventId::new(),
            timestamp: Utc::now(),
            key_expr: key.clone(),
        };
        backend.store(&key, &payload, meta).unwrap();
    }

    // Query seq range (3, 7) → seq 4, 5, 6
    let filtered = backend
        .query(&QueryFilters {
            after_seq: Some(3),
            before_seq: Some(7),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(filtered.len(), 3);
    let seqs: Vec<u64> = filtered.iter().map(|e| e.metadata.seq).collect();
    assert_eq!(seqs, vec![4, 5, 6]);
}

// ---------------------------------------------------------------------------
// End-to-end: EventStore with Zenoh
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn event_store_persists_and_publishes_watermark() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let dir = temp_dir("e2e_store");

    let config = EventBusConfig::builder("test/e2e_store")
        .cache_size(100)
        .heartbeat(HeartbeatMode::Disabled)
        .history_on_subscribe(false)
        .watermark_interval(Duration::from_millis(50))
        .build()
        .unwrap();

    let backend = FjallBackend::open(dir.path(), 0).unwrap();
    let mut store = EventStore::new(&session, backend, config.clone());
    store.run().await.unwrap();

    // Create publisher and send events.
    let publisher = EventPublisher::new(&session, config.clone()).await.unwrap();

    // Allow store to initialize its subscriber.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let count = 5u64;
    for i in 0..count {
        publisher
            .publish(&Event::new(TestPayload { value: i }))
            .await
            .unwrap();
    }

    // Wait for the store to persist and publish a watermark.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The store's backend should have committed some events for the publisher.
    let wms = store.backend().publisher_watermarks();
    assert!(
        !wms.is_empty(),
        "store should have tracked at least one publisher"
    );
    // At least one publisher should have committed_seq >= 1.
    let max_committed = wms.values().map(|pw| pw.committed_seq).max().unwrap_or(0);
    assert!(
        max_committed >= 1,
        "store should have committed some events, got {max_committed}"
    );

    let stored = store.backend().query(&QueryFilters::default()).unwrap();
    assert!(
        stored.len() >= 2,
        "store should have persisted events, got {}",
        stored.len()
    );

    // Subscribe to watermark and verify we get one.
    let wm_key = config.resolved_watermark_key();
    let wm_sub = session.declare_subscriber(&wm_key).await.unwrap();

    let wm_sample = tokio::time::timeout(Duration::from_secs(2), wm_sub.recv_async())
        .await
        .expect("timed out waiting for watermark")
        .expect("watermark recv failed");

    let wm: mitiflow::store::CommitWatermark =
        serde_json::from_slice(&wm_sample.payload().to_bytes()).unwrap();
    let max_committed = wm.publishers.values().map(|pw| pw.committed_seq).max().unwrap_or(0);
    assert!(max_committed >= 1, "watermark should cover events");

    store.shutdown();
    drop(publisher);
    session.close().await.unwrap();
}
