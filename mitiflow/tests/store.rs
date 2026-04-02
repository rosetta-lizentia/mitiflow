//! Integration tests for the FjallBackend and EventStore.
#![cfg(feature = "fjall-backend")]

mod common;

use std::time::Duration;

use chrono::Utc;

use mitiflow::store::FjallBackend;
use mitiflow::store::backend::{EventMetadata, HlcTimestamp, StorageBackend};
use mitiflow::store::query::{QueryFilters, ReplayFilters};
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
            key: None,
            hlc_timestamp: None,
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
            key: None,
            hlc_timestamp: None,
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
            key: None,
            hlc_timestamp: None,
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
                key: None,
                hlc_timestamp: None,
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
                key: None,
                hlc_timestamp: None,
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
                key: None,
                hlc_timestamp: None,
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
                key: None,
                hlc_timestamp: None,
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
                key: None,
                hlc_timestamp: None,
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
            key: None,
            hlc_timestamp: None,
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
        .num_partitions(1)
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
    let wms = store.publisher_watermarks().await.unwrap();
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

    let stored = store.query(&QueryFilters::default()).await.unwrap();
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
    let max_committed = wm
        .publishers
        .values()
        .map(|pw| pw.committed_seq)
        .max()
        .unwrap_or(0);
    assert!(max_committed >= 1, "watermark should cover events");

    store.shutdown();
    drop(publisher);
    session.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// HLC Replay Ordering — deterministic ordering across replicated stores
// ---------------------------------------------------------------------------

/// Generate a list of (publisher_id, seq, hlc_timestamp) events.
/// Simulates N publishers each producing `events_per_publisher` events
/// with HLC timestamps that are interleaved across publishers.
fn generate_interleaved_events(
    num_publishers: usize,
    events_per_publisher: u64,
) -> Vec<(PublisherId, u64, HlcTimestamp)> {
    let publishers: Vec<PublisherId> = (0..num_publishers).map(|_| PublisherId::new()).collect();
    let mut events = Vec::new();

    // Each publisher produces events, but their HLC timestamps interleave:
    // pub_0 gets physical_ns 1000, 1003, 1006, ...
    // pub_1 gets physical_ns 1001, 1004, 1007, ...
    // pub_2 gets physical_ns 1002, 1005, 1008, ...
    for seq in 0..events_per_publisher {
        for (pub_idx, pub_id) in publishers.iter().enumerate() {
            let physical_ns = 1_000_000_000 + seq * (num_publishers as u64) + pub_idx as u64;
            let hlc = HlcTimestamp {
                physical_ns,
                logical: 0,
            };
            events.push((*pub_id, seq, hlc));
        }
    }

    events
}

/// Store events into a FjallBackend in the given order and return the
/// replay-ordered sequence of (publisher_id, seq) pairs.
fn store_and_replay(
    backend: &FjallBackend,
    events: &[(PublisherId, u64, HlcTimestamp)],
) -> Vec<(PublisherId, u64)> {
    for (pub_id, seq, hlc) in events {
        let key = format!("test/replay/{}", seq);
        let meta = EventMetadata {
            seq: *seq,
            publisher_id: *pub_id,
            event_id: EventId::new(),
            timestamp: Utc::now(),
            key_expr: key.clone(),
            key: None,
            hlc_timestamp: Some(*hlc),
        };
        backend.store(&key, b"{}", meta).unwrap();
    }

    let replayed = backend.query_replay(&ReplayFilters::default()).unwrap();

    replayed
        .iter()
        .map(|e| (e.metadata.publisher_id, e.metadata.seq))
        .collect()
}

/// Core property: two replicas receiving the SAME events in DIFFERENT arrival
/// orders must produce IDENTICAL replay output.
#[test]
fn replay_order_deterministic_across_replicas() {
    use rand::SeedableRng;
    use rand::seq::SliceRandom;

    let canonical_events = generate_interleaved_events(3, 10);

    // Create several "replicas" with different randomized arrival orders.
    let num_replicas = 5;
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);

    let mut replay_results = Vec::new();

    for replica_id in 0..num_replicas {
        let dir = temp_dir(&format!("replay_det_{replica_id}"));
        let backend = FjallBackend::open(dir.path(), 0).unwrap();

        // Shuffle the events to simulate different arrival orders.
        let mut shuffled = canonical_events.clone();
        shuffled.shuffle(&mut rng);

        let replay = store_and_replay(&backend, &shuffled);
        replay_results.push(replay);
    }

    // All replicas must produce the same replay order.
    let reference = &replay_results[0];
    for (i, replay) in replay_results.iter().enumerate().skip(1) {
        assert_eq!(
            reference, replay,
            "replica {i} replay order differs from replica 0"
        );
    }

    // Verify the order is actually sorted by HLC timestamp.
    let dir = temp_dir("replay_det_verify");
    let backend = FjallBackend::open(dir.path(), 0).unwrap();
    let _ = store_and_replay(&backend, &canonical_events);

    let replayed = backend.query_replay(&ReplayFilters::default()).unwrap();

    let hlc_timestamps: Vec<HlcTimestamp> = replayed
        .iter()
        .map(|e| e.metadata.hlc_timestamp.unwrap())
        .collect();

    for pair in hlc_timestamps.windows(2) {
        assert!(
            pair[0] <= pair[1],
            "replay not in HLC order: {:?} > {:?}",
            pair[0],
            pair[1]
        );
    }
}

/// Replay filters: after_hlc and before_hlc correctly bound the results.
#[test]
fn replay_hlc_range_filter() {
    let dir = temp_dir("replay_filter");
    let backend = FjallBackend::open(dir.path(), 0).unwrap();

    let pub_id = PublisherId::new();
    let base_ns = 1_000_000_000u64;

    for seq in 0..10u64 {
        let hlc = HlcTimestamp {
            physical_ns: base_ns + seq * 100,
            logical: 0,
        };
        let key = format!("test/replay/{seq}");
        let meta = EventMetadata {
            seq,
            publisher_id: pub_id,
            event_id: EventId::new(),
            timestamp: Utc::now(),
            key_expr: key.clone(),
            key: None,
            hlc_timestamp: Some(hlc),
        };
        backend.store(&key, b"{}", meta).unwrap();
    }

    // Filter: after seq 3 HLC (base+300), before seq 7 HLC (base+700).
    let filtered = backend
        .query_replay(&ReplayFilters {
            after_hlc: Some(HlcTimestamp {
                physical_ns: base_ns + 300,
                logical: 0,
            }),
            before_hlc: Some(HlcTimestamp {
                physical_ns: base_ns + 700,
                logical: 0,
            }),
            limit: None,
            key: None,
            key_prefix: None,
        })
        .unwrap();

    // Should get seq 4, 5, 6 (strictly after 300, strictly before 700).
    let seqs: Vec<u64> = filtered.iter().map(|e| e.metadata.seq).collect();
    assert_eq!(seqs, vec![4, 5, 6]);
}

/// Replay with limit.
#[test]
fn replay_limit() {
    let dir = temp_dir("replay_limit");
    let backend = FjallBackend::open(dir.path(), 0).unwrap();

    let pub_id = PublisherId::new();
    for seq in 0..20u64 {
        let hlc = HlcTimestamp {
            physical_ns: 1_000_000_000 + seq,
            logical: 0,
        };
        let key = format!("test/replay/{seq}");
        let meta = EventMetadata {
            seq,
            publisher_id: pub_id,
            event_id: EventId::new(),
            timestamp: Utc::now(),
            key_expr: key.clone(),
            key: None,
            hlc_timestamp: Some(hlc),
        };
        backend.store(&key, b"{}", meta).unwrap();
    }

    let limited = backend
        .query_replay(&ReplayFilters {
            limit: Some(5),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(limited.len(), 5);
    // Should be the first 5 in HLC order.
    for (i, event) in limited.iter().enumerate() {
        assert_eq!(event.metadata.seq, i as u64);
    }
}

/// GC removes replay index entries alongside primary events.
#[test]
fn gc_cleans_replay_index() {
    let dir = temp_dir("gc_replay");
    let backend = FjallBackend::open(dir.path(), 0).unwrap();

    let pub_id = PublisherId::new();
    let old_time = Utc::now() - chrono::Duration::hours(2);
    let new_time = Utc::now();

    // Store an old event with HLC.
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
                key: None,
                hlc_timestamp: Some(HlcTimestamp {
                    physical_ns: 1_000,
                    logical: 0,
                }),
            },
        )
        .unwrap();

    // Store a new event with HLC.
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
                key: None,
                hlc_timestamp: Some(HlcTimestamp {
                    physical_ns: 2_000,
                    logical: 0,
                }),
            },
        )
        .unwrap();

    // Before GC: 2 events in replay index.
    let before = backend.query_replay(&ReplayFilters::default()).unwrap();
    assert_eq!(before.len(), 2);

    // GC removes the old event.
    let cutoff = Utc::now() - chrono::Duration::hours(1);
    let removed = backend.gc(cutoff).unwrap();
    assert_eq!(removed, 1);

    // After GC: only 1 event in replay index.
    let after = backend.query_replay(&ReplayFilters::default()).unwrap();
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].metadata.seq, 1);
}

/// Stress test: 5 replicas, 4 publishers, 50 events each, random order.
/// Verifies replay determinism at scale.
#[test]
fn replay_stress_many_publishers_many_replicas() {
    use rand::SeedableRng;
    use rand::seq::SliceRandom;

    let canonical_events = generate_interleaved_events(4, 50);
    let num_replicas = 5;
    let mut rng = rand::rngs::StdRng::seed_from_u64(1337);

    let mut replay_results = Vec::new();

    for replica_id in 0..num_replicas {
        let dir = temp_dir(&format!("replay_stress_{replica_id}"));
        let backend = FjallBackend::open(dir.path(), 0).unwrap();

        let mut shuffled = canonical_events.clone();
        shuffled.shuffle(&mut rng);

        let replay = store_and_replay(&backend, &shuffled);
        replay_results.push(replay);
    }

    // All replicas must produce the same result.
    let reference = &replay_results[0];
    assert_eq!(
        reference.len(),
        canonical_events.len(),
        "replay should contain all events"
    );

    for (i, replay) in replay_results.iter().enumerate().skip(1) {
        assert_eq!(
            reference, replay,
            "stress: replica {i} replay order differs from replica 0"
        );
    }
}

// ---------------------------------------------------------------------------
// query_by_key tests (Phase 2 — Store Key Index)
// ---------------------------------------------------------------------------

#[test]
fn fjall_store_keyed_event_queryable_by_key() {
    let dir = temp_dir("query_by_key_basic");
    let backend = FjallBackend::open(dir.path(), 0).unwrap();

    let pub_id = PublisherId::new();
    let payload = b"hello".to_vec();
    let meta = EventMetadata {
        seq: 0,
        publisher_id: pub_id,
        event_id: EventId::new(),
        timestamp: Utc::now(),
        key_expr: "test/p/0/k/order-123/0".to_string(),
        key: Some("order-123".to_string()),
        hlc_timestamp: None,
    };
    backend
        .store("test/p/0/k/order-123/0", &payload, meta)
        .unwrap();

    let results = backend.query_by_key("order-123", None).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].metadata.key, Some("order-123".to_string()));
    assert_eq!(results[0].payload, payload);
}

#[test]
fn fjall_store_multiple_keys_query_correct_key() {
    let dir = temp_dir("query_by_key_multi");
    let backend = FjallBackend::open(dir.path(), 0).unwrap();

    let pub_id = PublisherId::new();
    for (i, key) in ["alpha", "beta", "alpha", "gamma", "alpha"]
        .iter()
        .enumerate()
    {
        let seq = i as u64;
        let meta = EventMetadata {
            seq,
            publisher_id: pub_id,
            event_id: EventId::new(),
            timestamp: Utc::now(),
            key_expr: format!("test/p/0/k/{key}/{seq}"),
            key: Some(key.to_string()),
            hlc_timestamp: None,
        };
        backend
            .store(&meta.key_expr.clone(), &[seq as u8], meta)
            .unwrap();
    }

    let alpha = backend.query_by_key("alpha", None).unwrap();
    assert_eq!(alpha.len(), 3, "should find exactly 3 alpha events");
    for e in &alpha {
        assert_eq!(e.metadata.key, Some("alpha".to_string()));
    }

    let beta = backend.query_by_key("beta", None).unwrap();
    assert_eq!(beta.len(), 1);

    let missing = backend.query_by_key("nonexistent", None).unwrap();
    assert!(missing.is_empty());
}

#[test]
fn fjall_query_by_key_with_limit() {
    let dir = temp_dir("query_by_key_limit");
    let backend = FjallBackend::open(dir.path(), 0).unwrap();

    let pub_id = PublisherId::new();
    for seq in 0..10u64 {
        let meta = EventMetadata {
            seq,
            publisher_id: pub_id,
            event_id: EventId::new(),
            timestamp: Utc::now(),
            key_expr: format!("test/p/0/k/mykey/{seq}"),
            key: Some("mykey".to_string()),
            hlc_timestamp: None,
        };
        backend
            .store(&meta.key_expr.clone(), &[seq as u8], meta)
            .unwrap();
    }

    let limited = backend.query_by_key("mykey", Some(3)).unwrap();
    assert_eq!(limited.len(), 3, "limit should cap results");

    let all = backend.query_by_key("mykey", None).unwrap();
    assert_eq!(all.len(), 10);
}

#[test]
fn fjall_query_by_key_unkeyed_events_excluded() {
    let dir = temp_dir("query_by_key_unkeyed");
    let backend = FjallBackend::open(dir.path(), 0).unwrap();

    let pub_id = PublisherId::new();
    // Store an unkeyed event (key: None)
    let meta = EventMetadata {
        seq: 0,
        publisher_id: pub_id,
        event_id: EventId::new(),
        timestamp: Utc::now(),
        key_expr: "test/p/0/0".to_string(),
        key: None,
        hlc_timestamp: None,
    };
    backend.store("test/p/0/0", &[42], meta).unwrap();

    // Store a keyed event
    let meta2 = EventMetadata {
        seq: 1,
        publisher_id: pub_id,
        event_id: EventId::new(),
        timestamp: Utc::now(),
        key_expr: "test/p/0/k/mykey/1".to_string(),
        key: Some("mykey".to_string()),
        hlc_timestamp: None,
    };
    backend.store("test/p/0/k/mykey/1", &[99], meta2).unwrap();

    let results = backend.query_by_key("mykey", None).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].payload, vec![99]);
}

// ---------------------------------------------------------------------------
// compact_keyed + query_latest_by_keys tests (Phase 3 — Log Compaction)
// ---------------------------------------------------------------------------

#[test]
fn fjall_compact_keyed_keeps_latest_per_key() {
    let dir = temp_dir("compact_keyed_latest");
    let backend = FjallBackend::open(dir.path(), 0).unwrap();

    let pub_id = PublisherId::new();
    for seq in 0..3u64 {
        let meta = EventMetadata {
            seq,
            publisher_id: pub_id,
            event_id: EventId::new(),
            timestamp: Utc::now(),
            key_expr: format!("test/p/0/k/mykey/{seq}"),
            key: Some("mykey".to_string()),
            hlc_timestamp: None,
        };
        backend
            .store(&meta.key_expr.clone(), &[seq as u8], meta)
            .unwrap();
    }

    let stats = backend.compact_keyed().unwrap();
    assert_eq!(
        stats.retained, 1,
        "only the latest event should be retained"
    );
    assert_eq!(stats.removed, 2, "older events should be removed");

    let remaining = backend.query_by_key("mykey", None).unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].metadata.seq, 2, "seq 2 is the latest");
    assert_eq!(remaining[0].payload, vec![2]);
}

#[test]
fn fjall_compact_keyed_multiple_keys() {
    let dir = temp_dir("compact_keyed_multi");
    let backend = FjallBackend::open(dir.path(), 0).unwrap();

    let pub_id = PublisherId::new();
    let mut seq = 0u64;
    for key in ["alpha", "beta"] {
        for _ in 0..3 {
            let meta = EventMetadata {
                seq,
                publisher_id: pub_id,
                event_id: EventId::new(),
                timestamp: Utc::now(),
                key_expr: format!("test/p/0/k/{key}/{seq}"),
                key: Some(key.to_string()),
                hlc_timestamp: None,
            };
            backend
                .store(&meta.key_expr.clone(), &[seq as u8], meta)
                .unwrap();
            seq += 1;
        }
    }

    let stats = backend.compact_keyed().unwrap();
    assert_eq!(stats.retained, 2, "one per key retained");
    assert_eq!(stats.removed, 4, "4 older events removed");

    let alpha = backend.query_by_key("alpha", None).unwrap();
    assert_eq!(alpha.len(), 1);
    assert_eq!(alpha[0].metadata.seq, 2);

    let beta = backend.query_by_key("beta", None).unwrap();
    assert_eq!(beta.len(), 1);
    assert_eq!(beta[0].metadata.seq, 5);
}

#[test]
fn fjall_compact_keyed_with_tombstone() {
    let dir = temp_dir("compact_keyed_tombstone");
    let backend = FjallBackend::open(dir.path(), 0).unwrap();

    let pub_id = PublisherId::new();
    // Write a real event for "mykey"
    let meta0 = EventMetadata {
        seq: 0,
        publisher_id: pub_id,
        event_id: EventId::new(),
        timestamp: Utc::now(),
        key_expr: "test/p/0/k/mykey/0".to_string(),
        key: Some("mykey".to_string()),
        hlc_timestamp: None,
    };
    backend.store("test/p/0/k/mykey/0", &[42], meta0).unwrap();

    // Write a tombstone (empty payload) for "mykey"
    let meta1 = EventMetadata {
        seq: 1,
        publisher_id: pub_id,
        event_id: EventId::new(),
        timestamp: Utc::now(),
        key_expr: "test/p/0/k/mykey/1".to_string(),
        key: Some("mykey".to_string()),
        hlc_timestamp: None,
    };
    backend.store("test/p/0/k/mykey/1", &[], meta1).unwrap();

    let stats = backend.compact_keyed().unwrap();
    assert_eq!(stats.retained, 1, "tombstone retained as latest");
    assert_eq!(stats.removed, 1, "real event removed");

    let remaining = backend.query_by_key("mykey", None).unwrap();
    assert_eq!(remaining.len(), 1);
    assert!(
        remaining[0].payload.is_empty(),
        "tombstone payload is empty"
    );
}

#[test]
fn fjall_compact_keyed_unkeyed_events_untouched() {
    let dir = temp_dir("compact_keyed_unkeyed");
    let backend = FjallBackend::open(dir.path(), 0).unwrap();

    let pub_id = PublisherId::new();
    // Store unkeyed events
    for seq in 0..3u64 {
        let meta = EventMetadata {
            seq,
            publisher_id: pub_id,
            event_id: EventId::new(),
            timestamp: Utc::now(),
            key_expr: format!("test/p/0/{seq}"),
            key: None,
            hlc_timestamp: None,
        };
        backend
            .store(&meta.key_expr.clone(), &[seq as u8], meta)
            .unwrap();
    }

    let stats = backend.compact_keyed().unwrap();
    assert_eq!(stats.retained, 0, "no keyed events — nothing retained");
    assert_eq!(stats.removed, 0, "unkeyed events should not be removed");

    let all = backend.query(&QueryFilters::default()).unwrap();
    assert_eq!(all.len(), 3, "all unkeyed events should remain");
}

#[test]
fn fjall_query_latest_by_keys_returns_one_per_key() {
    let dir = temp_dir("query_latest_by_keys");
    let backend = FjallBackend::open(dir.path(), 0).unwrap();

    let pub_id = PublisherId::new();
    let mut seq = 0u64;
    for key in ["order-A", "order-B"] {
        for _ in 0..3 {
            let meta = EventMetadata {
                seq,
                publisher_id: pub_id,
                event_id: EventId::new(),
                timestamp: Utc::now(),
                key_expr: format!("test/p/0/k/{key}/{seq}"),
                key: Some(key.to_string()),
                hlc_timestamp: None,
            };
            backend
                .store(&meta.key_expr.clone(), &[seq as u8], meta)
                .unwrap();
            seq += 1;
        }
    }

    let latest = backend
        .query_latest_by_keys(&["order-A", "order-B", "nonexistent"])
        .unwrap();
    assert_eq!(latest.len(), 2, "one per existing key");

    let a = latest
        .iter()
        .find(|e| e.metadata.key.as_deref() == Some("order-A"))
        .unwrap();
    assert_eq!(a.metadata.seq, 2, "latest seq for order-A");

    let b = latest
        .iter()
        .find(|e| e.metadata.key.as_deref() == Some("order-B"))
        .unwrap();
    assert_eq!(b.metadata.seq, 5, "latest seq for order-B");
}

// ── Key-scoped replay (Phase 2) ─────────────────────────────────────────────

/// query_replay with exact key filter returns only events with that key.
#[test]
fn replay_key_filter_exact() {
    let dir = temp_dir("replay_key_exact");
    let backend = FjallBackend::open(dir.path(), 0).unwrap();

    let pub_id = PublisherId::new();
    let base_ns = 2_000_000_000u64;
    let keys = ["alpha", "beta", "alpha", "gamma", "alpha"];

    for (seq, key_name) in keys.iter().enumerate() {
        let seq = seq as u64;
        let hlc = HlcTimestamp {
            physical_ns: base_ns + seq * 100,
            logical: 0,
        };
        let key_expr = format!("test/replay_key/p/0/k/{key_name}/{seq}");
        let meta = EventMetadata {
            seq,
            publisher_id: pub_id,
            event_id: EventId::new(),
            timestamp: Utc::now(),
            key_expr: key_expr.clone(),
            key: Some(key_name.to_string()),
            hlc_timestamp: Some(hlc),
        };
        backend.store(&key_expr, b"{}", meta).unwrap();
    }

    let results = backend
        .query_replay(&ReplayFilters {
            key: Some("alpha".to_string()),
            ..Default::default()
        })
        .unwrap();

    assert_eq!(results.len(), 3);
    for event in &results {
        assert_eq!(event.metadata.key.as_deref(), Some("alpha"));
    }
    // Verify HLC ordering
    let seqs: Vec<u64> = results.iter().map(|e| e.metadata.seq).collect();
    assert_eq!(seqs, vec![0, 2, 4]);
}

/// query_replay with key_prefix filter returns events whose key starts with the prefix.
#[test]
fn replay_key_filter_prefix() {
    let dir = temp_dir("replay_key_prefix");
    let backend = FjallBackend::open(dir.path(), 0).unwrap();

    let pub_id = PublisherId::new();
    let base_ns = 3_000_000_000u64;
    let keys = ["order-123", "order-456", "user-789", "order-123"];

    for (seq, key_name) in keys.iter().enumerate() {
        let seq = seq as u64;
        let hlc = HlcTimestamp {
            physical_ns: base_ns + seq * 100,
            logical: 0,
        };
        let key_expr = format!("test/replay_prefix/p/0/k/{key_name}/{seq}");
        let meta = EventMetadata {
            seq,
            publisher_id: pub_id,
            event_id: EventId::new(),
            timestamp: Utc::now(),
            key_expr: key_expr.clone(),
            key: Some(key_name.to_string()),
            hlc_timestamp: Some(hlc),
        };
        backend.store(&key_expr, b"{}", meta).unwrap();
    }

    let results = backend
        .query_replay(&ReplayFilters {
            key_prefix: Some("order-".to_string()),
            ..Default::default()
        })
        .unwrap();

    assert_eq!(results.len(), 3);
    for event in &results {
        assert!(event.metadata.key.as_deref().unwrap().starts_with("order-"));
    }
}

/// query_replay with key filter + HLC range returns bounded keyed events.
#[test]
fn replay_key_filter_with_hlc_range() {
    let dir = temp_dir("replay_key_hlc");
    let backend = FjallBackend::open(dir.path(), 0).unwrap();

    let pub_id = PublisherId::new();
    let base_ns = 4_000_000_000u64;
    let keys = ["alpha", "beta", "alpha", "beta", "alpha"];

    for (seq, key_name) in keys.iter().enumerate() {
        let seq = seq as u64;
        let hlc = HlcTimestamp {
            physical_ns: base_ns + seq * 100,
            logical: 0,
        };
        let key_expr = format!("test/replay_key_hlc/p/0/k/{key_name}/{seq}");
        let meta = EventMetadata {
            seq,
            publisher_id: pub_id,
            event_id: EventId::new(),
            timestamp: Utc::now(),
            key_expr: key_expr.clone(),
            key: Some(key_name.to_string()),
            hlc_timestamp: Some(hlc),
        };
        backend.store(&key_expr, b"{}", meta).unwrap();
    }

    // HLC range: after seq 0 (base+0), before seq 4 (base+400).
    // Alpha events in range: seq 2 (base+200) only.
    let results = backend
        .query_replay(&ReplayFilters {
            key: Some("alpha".to_string()),
            after_hlc: Some(HlcTimestamp {
                physical_ns: base_ns,
                logical: 0,
            }),
            before_hlc: Some(HlcTimestamp {
                physical_ns: base_ns + 400,
                logical: 0,
            }),
            limit: None,
            key_prefix: None,
        })
        .unwrap();

    let seqs: Vec<u64> = results.iter().map(|e| e.metadata.seq).collect();
    assert_eq!(seqs, vec![2]);
}

/// query_replay with non-existent key returns empty vec.
#[test]
fn replay_key_filter_no_match() {
    let dir = temp_dir("replay_key_none");
    let backend = FjallBackend::open(dir.path(), 0).unwrap();

    let pub_id = PublisherId::new();
    let hlc = HlcTimestamp {
        physical_ns: 5_000_000_000,
        logical: 0,
    };
    let key_expr = "test/replay_none/p/0/k/exists/0".to_string();
    let meta = EventMetadata {
        seq: 0,
        publisher_id: pub_id,
        event_id: EventId::new(),
        timestamp: Utc::now(),
        key_expr: key_expr.clone(),
        key: Some("exists".to_string()),
        hlc_timestamp: Some(hlc),
    };
    backend.store(&key_expr, b"{}", meta).unwrap();

    let results = backend
        .query_replay(&ReplayFilters {
            key: Some("does-not-exist".to_string()),
            ..Default::default()
        })
        .unwrap();

    assert!(results.is_empty());
}
