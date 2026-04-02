//! Integration tests for key-scoped subscribing (Phase 1: passthrough mode).
//!
//! Tests verify that key-filtered subscribers:
//! - Use event-ID dedup instead of sequence-based gap detection
//! - Do not trigger false gap recovery for sparse sequences
//! - Work correctly with heartbeats enabled (recovery is skipped)
//! - Coexist with unfiltered subscribers

mod common;

use std::time::Duration;

use common::TestPayload;
use mitiflow::{Event, EventBusConfig, EventPublisher, EventSubscriber, HeartbeatMode};

/// Config with heartbeats enabled — passthrough mode should still skip recovery.
fn passthrough_config(test_name: &str) -> EventBusConfig {
    EventBusConfig::builder(format!("test/{test_name}"))
        .cache_size(1000)
        .heartbeat(HeartbeatMode::Periodic(Duration::from_millis(200)))
        .history_on_subscribe(false)
        .num_partitions(1) // single partition so all keys hash to same partition
        .dedup_capacity(1000)
        .build()
        .expect("valid config")
}

/// Key-filtered subscriber receives only matching key events and does NOT
/// trigger false gap recovery from sparse sequences.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn key_filtered_no_false_gaps() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let config = passthrough_config("kf_no_false_gaps");
    let publisher = EventPublisher::new(&session, config.clone()).await.unwrap();

    // Key-filtered subscriber for "alpha" only.
    let sub = EventSubscriber::new_keyed(&session, config, "alpha")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish interleaved keys: alpha, beta, gamma, alpha, beta, alpha
    // With 1 partition, all go to partition 0, so sequences are 0,1,2,3,4,5
    // The "alpha" subscriber sees seqs 0, 3, 5 — a sparse view.
    publisher
        .publish_keyed("alpha", &Event::new(TestPayload { value: 10 }))
        .await
        .unwrap();
    publisher
        .publish_keyed("beta", &Event::new(TestPayload { value: 20 }))
        .await
        .unwrap();
    publisher
        .publish_keyed("gamma", &Event::new(TestPayload { value: 30 }))
        .await
        .unwrap();
    publisher
        .publish_keyed("alpha", &Event::new(TestPayload { value: 40 }))
        .await
        .unwrap();
    publisher
        .publish_keyed("beta", &Event::new(TestPayload { value: 50 }))
        .await
        .unwrap();
    publisher
        .publish_keyed("alpha", &Event::new(TestPayload { value: 60 }))
        .await
        .unwrap();

    // Should receive exactly 3 "alpha" events, no errors from gap recovery.
    let mut received = Vec::new();
    for _ in 0..3 {
        let raw = tokio::time::timeout(Duration::from_secs(5), sub.recv_raw())
            .await
            .expect("timed out waiting for alpha event")
            .expect("recv failed");
        assert_eq!(raw.key(), Some("alpha"), "should only receive alpha events");
        received.push(raw);
    }
    assert_eq!(received.len(), 3);

    // No more events (beta/gamma should not arrive).
    let extra = tokio::time::timeout(Duration::from_millis(500), sub.recv_raw()).await;
    assert!(extra.is_err(), "should not receive non-alpha events");

    drop(sub);
    drop(publisher);
    session.close().await.unwrap();
}

/// Key-prefix filtered subscriber receives matching prefix events.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn key_prefix_filtered_no_false_gaps() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let config = passthrough_config("kf_prefix_no_gaps");
    let publisher = EventPublisher::new(&session, config.clone()).await.unwrap();

    // Subscribe to key prefix "user/1"
    let sub = EventSubscriber::new_key_prefix(&session, config, "user/1")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish events with various user keys.
    publisher
        .publish_keyed("user/1/orders", &Event::new(TestPayload { value: 1 }))
        .await
        .unwrap();
    publisher
        .publish_keyed("user/2/orders", &Event::new(TestPayload { value: 2 }))
        .await
        .unwrap();
    publisher
        .publish_keyed("user/1/profile", &Event::new(TestPayload { value: 3 }))
        .await
        .unwrap();
    publisher
        .publish_keyed("user/3/orders", &Event::new(TestPayload { value: 4 }))
        .await
        .unwrap();

    // Should receive exactly 2 events: user/1/orders and user/1/profile.
    let mut received = Vec::new();
    for _ in 0..2 {
        let raw = tokio::time::timeout(Duration::from_secs(5), sub.recv_raw())
            .await
            .expect("timed out")
            .expect("recv failed");
        let key = raw.key().expect("should have a key");
        assert!(
            key.starts_with("user/1"),
            "expected user/1 prefix, got: {key}"
        );
        received.push(raw);
    }
    assert_eq!(received.len(), 2);

    // No extra events.
    let extra = tokio::time::timeout(Duration::from_millis(500), sub.recv_raw()).await;
    assert!(extra.is_err(), "should not receive non-matching prefix events");

    drop(sub);
    drop(publisher);
    session.close().await.unwrap();
}

/// Key-filtered subscriber with heartbeats enabled does not trigger
/// recovery for sequences it hasn't seen (which belong to other keys).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn key_filtered_heartbeat_no_recovery() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    // Heartbeats enabled at a fast interval to trigger within test window.
    let config = EventBusConfig::builder("test/kf_hb_no_recovery")
        .cache_size(1000)
        .heartbeat(HeartbeatMode::Periodic(Duration::from_millis(100)))
        .history_on_subscribe(false)
        .num_partitions(1)
        .dedup_capacity(1000)
        .build()
        .unwrap();

    let publisher = EventPublisher::new(&session, config.clone()).await.unwrap();
    let sub = EventSubscriber::new_keyed(&session, config, "target")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish: other, other, target, other, target
    // Key-filtered subscriber sees only "target" events (sparse seq).
    for key in &["other-a", "other-b", "target", "other-c", "target"] {
        publisher
            .publish_keyed(key, &Event::new(TestPayload { value: 1 }))
            .await
            .unwrap();
    }

    // Wait for heartbeats to fire (they advertise seqs 0-4 for publisher on partition 0).
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Should receive exactly 2 "target" events — no recovered non-target events.
    let mut count = 0;
    loop {
        match tokio::time::timeout(Duration::from_millis(500), sub.recv_raw()).await {
            Ok(Ok(raw)) => {
                assert_eq!(
                    raw.key(),
                    Some("target"),
                    "key-filtered subscriber should only deliver target events"
                );
                count += 1;
            }
            _ => break,
        }
    }
    assert_eq!(count, 2, "should receive exactly 2 target events");

    drop(sub);
    drop(publisher);
    session.close().await.unwrap();
}

/// Key-filtered subscriber and unfiltered subscriber can coexist:
/// - Unfiltered subscriber gets all events with full gap detection.
/// - Key-filtered subscriber gets only matching events with event-ID dedup.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn key_filtered_and_unfiltered_coexist() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let config = passthrough_config("kf_coexist");
    let publisher = EventPublisher::new(&session, config.clone()).await.unwrap();

    // Unfiltered subscriber (standard gap detection).
    let all_sub = EventSubscriber::new(&session, config.clone()).await.unwrap();
    // Key-filtered subscriber for "target".
    let key_sub = EventSubscriber::new_keyed(&session, config, "target")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish 3 events: 1 target + 2 other.
    publisher
        .publish_keyed("target", &Event::new(TestPayload { value: 1 }))
        .await
        .unwrap();
    publisher
        .publish_keyed("other", &Event::new(TestPayload { value: 2 }))
        .await
        .unwrap();
    publisher
        .publish_keyed("target", &Event::new(TestPayload { value: 3 }))
        .await
        .unwrap();

    // Unfiltered subscriber should get all 3.
    for _ in 0..3 {
        tokio::time::timeout(Duration::from_secs(5), all_sub.recv_raw())
            .await
            .expect("timed out")
            .expect("recv failed");
    }

    // Key-filtered subscriber should get exactly 2 "target" events.
    for _ in 0..2 {
        let raw = tokio::time::timeout(Duration::from_secs(5), key_sub.recv_raw())
            .await
            .expect("timed out")
            .expect("recv failed");
        assert_eq!(raw.key(), Some("target"));
    }

    // No more for key subscriber.
    let extra = tokio::time::timeout(Duration::from_millis(500), key_sub.recv_raw()).await;
    assert!(extra.is_err());

    drop(key_sub);
    drop(all_sub);
    drop(publisher);
    session.close().await.unwrap();
}

// ── Phase 3: KeyedConsumer (pull-based, store-mediated) ─────────────────────

#[cfg(feature = "store")]
mod keyed_consumer_tests {
    use super::common;
    use common::TestPayload;

    use std::time::Duration;

    use mitiflow::store::backend::StorageBackend;
    use mitiflow::{
        CodecFormat, Event, EventBusConfig, EventPublisher, EventStore, FjallBackend,
        HeartbeatMode, KeyedConsumer,
    };

    fn store_config(test_name: &str) -> EventBusConfig {
        EventBusConfig::builder(format!("test/{test_name}"))
            .cache_size(1000)
            .heartbeat(HeartbeatMode::Periodic(Duration::from_millis(200)))
            .history_on_subscribe(false)
            .num_partitions(1)
            .watermark_interval(Duration::from_millis(50))
            .durable_timeout(Duration::from_secs(5))
            .durable_urgency(Duration::from_millis(0))
            .build()
            .expect("valid config")
    }

    /// Helper: open a Zenoh session with HLC timestamping enabled by default.
    async fn open_session_with_hlc() -> zenoh::Session {
        let mut config = zenoh::Config::default();
        config
            .insert_json5("timestamping/enabled", "true")
            .expect("valid config");
        zenoh::open(config).await.unwrap()
    }

    /// Helper: set up a store, publisher, publish keyed events, and wait for persistence.
    async fn setup_store_with_keyed_events(
        test_name: &str,
        keys_and_values: &[(&str, u64)],
    ) -> (
        zenoh::Session,
        EventPublisher,
        EventStore,
        std::sync::Arc<dyn StorageBackend>,
        tempfile::TempDir,
        EventBusConfig,
    ) {
        let session = open_session_with_hlc().await;
        let config = store_config(test_name);
        let tmp = common::temp_dir(test_name);
        let backend: std::sync::Arc<dyn StorageBackend> =
            std::sync::Arc::new(FjallBackend::open(tmp.path(), 0).unwrap());
        let mut store = EventStore::new(&session, backend.clone(), config.clone());
        store.run().await.unwrap();
        let publisher = EventPublisher::new(&session, config.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;

        for (key, value) in keys_and_values {
            publisher
                .publish_keyed(key, &Event::new(TestPayload { value: *value }))
                .await
                .unwrap();
        }

        // Wait for store to persist all events.
        tokio::time::sleep(Duration::from_millis(500)).await;

        (session, publisher, store, backend, tmp, config)
    }

    /// poll() returns events in HLC order for an exact key.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn keyed_consumer_poll_exact_key() {
        let (session, publisher, store, _backend, _tmp, config) =
            setup_store_with_keyed_events(
                "kc_poll_exact",
                &[
                    ("order-A", 1),
                    ("order-B", 2),
                    ("order-A", 3),
                    ("order-B", 4),
                    ("order-A", 5),
                ],
            )
            .await;

        let mut consumer = KeyedConsumer::builder(&session, config)
            .key("order-A")
            .partition(0)
            .batch_size(100)
            .build()
            .unwrap();

        let events = consumer.poll().await.unwrap();
        assert_eq!(events.len(), 3, "should get 3 events for order-A");
        for event in &events {
            assert_eq!(event.metadata.key.as_deref(), Some("order-A"));
        }
        // Values should be in publish order (HLC-ordered).
        let codec = CodecFormat::default();
        let payloads: Vec<u64> = events
            .iter()
            .map(|e| codec.decode::<TestPayload>(&e.payload).unwrap().value)
            .collect();
        assert_eq!(payloads, vec![1, 3, 5]);

        drop(consumer);
        drop(store);
        drop(publisher);
        session.close().await.unwrap();
    }

    /// poll() returns events for a key prefix.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn keyed_consumer_poll_key_prefix() {
        let (session, publisher, store, _backend, _tmp, config) =
            setup_store_with_keyed_events(
                "kc_poll_prefix",
                &[
                    ("order-100", 1),
                    ("user-42", 2),
                    ("order-200", 3),
                    ("user-43", 4),
                    ("order-100", 5),
                ],
            )
            .await;

        let mut consumer = KeyedConsumer::builder(&session, config)
            .key_prefix("order-")
            .partition(0)
            .batch_size(100)
            .build()
            .unwrap();

        let events = consumer.poll().await.unwrap();
        assert_eq!(events.len(), 3, "should get 3 events with order- prefix");
        for event in &events {
            assert!(event.metadata.key.as_deref().unwrap().starts_with("order-"));
        }

        drop(consumer);
        drop(store);
        drop(publisher);
        session.close().await.unwrap();
    }

    /// Cursor advances — second poll returns no duplicates.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn keyed_consumer_cursor_advances() {
        let (session, publisher, store, _backend, _tmp, config) =
            setup_store_with_keyed_events(
                "kc_cursor",
                &[("alpha", 1), ("beta", 2), ("alpha", 3)],
            )
            .await;

        let mut consumer = KeyedConsumer::builder(&session, config.clone())
            .key("alpha")
            .partition(0)
            .batch_size(100)
            .build()
            .unwrap();

        let first = consumer.poll().await.unwrap();
        assert_eq!(first.len(), 2, "first poll: 2 alpha events");

        // Publish one more after cursor has advanced.
        // Use publish_keyed_durable to ensure the event is persisted before polling.
        tokio::time::sleep(Duration::from_millis(50)).await;
        publisher
            .publish_keyed_durable("alpha", &Event::new(TestPayload { value: 4 }))
            .await
            .unwrap();

        let second = consumer.poll().await.unwrap();
        assert_eq!(second.len(), 1, "second poll: 1 new alpha event");
        let val = CodecFormat::default()
            .decode::<TestPayload>(&second[0].payload)
            .unwrap()
            .value;
        assert_eq!(val, 4);

        drop(consumer);
        drop(store);
        drop(publisher);
        session.close().await.unwrap();
    }

    /// poll() returns empty when no matching events exist.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn keyed_consumer_empty_poll() {
        let (session, publisher, store, _backend, _tmp, config) =
            setup_store_with_keyed_events(
                "kc_empty",
                &[("order-A", 1)],
            )
            .await;

        let mut consumer = KeyedConsumer::builder(&session, config)
            .key("non-existent-key")
            .partition(0)
            .batch_size(100)
            .build()
            .unwrap();

        let events = consumer.poll().await.unwrap();
        assert!(events.is_empty(), "no events for non-existent key");

        drop(consumer);
        drop(store);
        drop(publisher);
        session.close().await.unwrap();
    }

    /// seek() repositions the cursor.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn keyed_consumer_seek() {
        let (session, publisher, store, _backend, _tmp, config) =
            setup_store_with_keyed_events(
                "kc_seek",
                &[("alpha", 1), ("alpha", 2), ("alpha", 3)],
            )
            .await;

        let mut consumer = KeyedConsumer::builder(&session, config)
            .key("alpha")
            .partition(0)
            .batch_size(100)
            .build()
            .unwrap();

        // First poll gets all 3.
        let all = consumer.poll().await.unwrap();
        assert_eq!(all.len(), 3);

        // Seek back to the cursor of the first event's HLC.
        let first_hlc = all[0].metadata.hlc_timestamp.unwrap();
        consumer.seek(first_hlc);

        // Should get events after the first one (2nd and 3rd).
        let after_seek = consumer.poll().await.unwrap();
        assert_eq!(after_seek.len(), 2, "after seek: 2 events past first HLC");

        drop(consumer);
        drop(store);
        drop(publisher);
        session.close().await.unwrap();
    }

    /// commit() + fetch_offset() roundtrip: committed HLC is retrievable.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn keyed_consumer_commit_fetch_roundtrip() {
        let (session, publisher, store, _backend, _tmp, config) =
            setup_store_with_keyed_events(
                "kc_commit_rt",
                &[("alpha", 1), ("alpha", 2), ("alpha", 3)],
            )
            .await;

        let mut consumer = KeyedConsumer::builder(&session, config)
            .key("alpha")
            .partition(0)
            .batch_size(100)
            .build()
            .unwrap();

        // Poll to advance cursor.
        let events = consumer.poll().await.unwrap();
        assert_eq!(events.len(), 3);
        let cursor_before = consumer.cursor();
        assert!(cursor_before.physical_ns > 0, "cursor should be non-zero after poll");

        // Commit the current cursor.
        consumer.commit("test-group").await.unwrap();

        // Allow the store to process the commit put.
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Fetch the committed offset — should match cursor.
        let fetched = consumer.fetch_offset("test-group").await.unwrap();
        assert!(fetched.is_some(), "should have a committed offset");
        let hlc = fetched.unwrap();
        assert_eq!(
            hlc.physical_ns, cursor_before.physical_ns,
            "fetched HLC physical_ns should match committed cursor"
        );
        assert_eq!(
            hlc.logical, cursor_before.logical,
            "fetched HLC logical should match committed cursor"
        );

        drop(consumer);
        drop(store);
        drop(publisher);
        session.close().await.unwrap();
    }

    /// Different key filters maintain independent offsets within the same group.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn keyed_consumer_independent_offsets() {
        let (session, publisher, store, _backend, _tmp, config) =
            setup_store_with_keyed_events(
                "kc_indep_off",
                &[
                    ("alpha", 1),
                    ("beta", 2),
                    ("alpha", 3),
                    ("beta", 4),
                ],
            )
            .await;

        let mut alpha_consumer = KeyedConsumer::builder(&session, config.clone())
            .key("alpha")
            .partition(0)
            .batch_size(100)
            .build()
            .unwrap();

        let mut beta_consumer = KeyedConsumer::builder(&session, config)
            .key("beta")
            .partition(0)
            .batch_size(100)
            .build()
            .unwrap();

        // Poll both consumers.
        let alpha_events = alpha_consumer.poll().await.unwrap();
        let beta_events = beta_consumer.poll().await.unwrap();
        assert_eq!(alpha_events.len(), 2);
        assert_eq!(beta_events.len(), 2);

        // Commit only alpha.
        alpha_consumer.commit("shared-group").await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Alpha should have a committed offset; beta should not.
        let alpha_offset = alpha_consumer.fetch_offset("shared-group").await.unwrap();
        let beta_offset = beta_consumer.fetch_offset("shared-group").await.unwrap();
        assert!(alpha_offset.is_some(), "alpha should have committed offset");
        assert!(beta_offset.is_none(), "beta should NOT have committed offset");

        // Now commit beta and verify both exist independently.
        beta_consumer.commit("shared-group").await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;

        let alpha_offset2 = alpha_consumer.fetch_offset("shared-group").await.unwrap();
        let beta_offset2 = beta_consumer.fetch_offset("shared-group").await.unwrap();
        assert!(alpha_offset2.is_some());
        assert!(beta_offset2.is_some());

        // The two offsets should differ (different HLC cursors).
        let alpha_hlc = alpha_offset2.unwrap();
        let beta_hlc = beta_offset2.unwrap();
        assert_ne!(
            alpha_hlc.physical_ns, beta_hlc.physical_ns,
            "different key filters should have different cursor positions"
        );

        drop(alpha_consumer);
        drop(beta_consumer);
        drop(store);
        drop(publisher);
        session.close().await.unwrap();
    }
}
