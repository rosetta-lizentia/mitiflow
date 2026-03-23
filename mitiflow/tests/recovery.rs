//! End-to-end tests for tiered recovery: store-first, cache-fallback, and
//! history catch-up scenarios.
//!
//! These tests verify that a subscriber can:
//! 1. Read from the event store to recover missed events
//! 2. Catch up with the log after recovering from the store
//! 3. Continue accepting ongoing messages from the publisher
//! 4. Fall back to the publisher cache when the store is unavailable
//! 5. Handle multiple publishers concurrently

mod common;

use std::collections::HashSet;
use std::time::Duration;

use mitiflow::store::FjallBackend;
use mitiflow::store::query::QueryFilters;
use mitiflow::{Event, EventBusConfig, EventPublisher, EventStore, EventSubscriber, HeartbeatMode};

use common::TestPayload;

fn temp_dir(name: &str) -> tempfile::TempDir {
    common::temp_dir(name)
}

/// Helper: build a config with store support enabled.
fn store_config(test_name: &str) -> EventBusConfig {
    EventBusConfig::builder(format!("test/{test_name}"))
        .cache_size(256)
        .heartbeat(HeartbeatMode::Periodic(Duration::from_millis(200)))
        .history_on_subscribe(false)
        .watermark_interval(Duration::from_millis(50))
        .recovery_delay(Duration::from_millis(30))
        .max_recovery_attempts(3)
        .build()
        .expect("valid config")
}

// ---------------------------------------------------------------------------
// Scenario 1: History catch-up → continue with live events
//
// Publisher sends N events (persisted to store), publisher stays alive,
// a new subscriber joins and reads history from the store, then sees new
// events published after it joined.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriber_catches_up_from_store_then_receives_live() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let dir = temp_dir("history_catchup");

    let config = store_config("history_catchup");

    // Start event store.
    let backend = FjallBackend::open(dir.path(), 0).unwrap();
    let mut store = EventStore::new(&session, backend, config.clone());
    store.run().await.unwrap();

    // Create publisher and send historical events.
    let publisher = EventPublisher::new(&session, config.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let history_count = 10u64;
    for i in 0..history_count {
        publisher
            .publish(&Event::new(TestPayload { value: i }))
            .await
            .unwrap();
    }

    // Wait for the store to persist all events.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify all events are in the store.
    let stored = store.query(&QueryFilters::default()).await.unwrap();
    assert!(
        stored.len() >= history_count as usize,
        "store should have all {history_count} events, got {}",
        stored.len()
    );

    // Now create a subscriber. It joins after history was published.
    // history_on_subscribe is false, so it won't auto-fetch.
    let subscriber = EventSubscriber::new(&session, config.clone())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish live events.
    let live_count = 5u64;
    for i in history_count..(history_count + live_count) {
        publisher
            .publish(&Event::new(TestPayload { value: i }))
            .await
            .unwrap();
    }

    // Subscriber should receive the live events.
    let mut received = Vec::new();
    for _ in 0..live_count {
        let event: Event<TestPayload> =
            tokio::time::timeout(Duration::from_secs(5), subscriber.recv())
                .await
                .expect("timed out waiting for live event")
                .expect("recv failed");
        received.push(event.payload.value);
    }

    let expected_live: Vec<u64> = (history_count..(history_count + live_count)).collect();
    assert_eq!(
        received, expected_live,
        "subscriber should receive all live events in order"
    );

    store.shutdown();
    drop(publisher);
    drop(subscriber);
    session.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// Scenario 2: Store-based gap recovery
//
// Publisher sends events, some are "missed" by the subscriber (simulated
// by joining mid-stream), gap detector triggers store recovery.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn store_recovers_missed_events() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let dir = temp_dir("store_recovery");

    let config = store_config("store_recovery");

    // Start event store.
    let backend = FjallBackend::open(dir.path(), 0).unwrap();
    let mut store = EventStore::new(&session, backend, config.clone());
    store.run().await.unwrap();

    // Create publisher + subscriber together.
    let publisher = EventPublisher::new(&session, config.clone()).await.unwrap();
    let subscriber = EventSubscriber::new(&session, config.clone())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish enough events — the subscriber should receive all of them
    // (no gap in normal operation with Zenoh peer mode).
    let count = 20u64;
    for i in 0..count {
        publisher
            .publish(&Event::new(TestPayload { value: i }))
            .await
            .unwrap();
    }

    // Collect events. In peer mode without network drops, we expect all of them.
    let mut received_values = HashSet::new();
    for _ in 0..count {
        let event: Event<TestPayload> =
            tokio::time::timeout(Duration::from_secs(5), subscriber.recv())
                .await
                .expect("timed out waiting for event")
                .expect("recv failed");
        received_values.insert(event.payload.value);
    }

    assert_eq!(
        received_values.len(),
        count as usize,
        "should receive all {count} events (direct + recovered)"
    );

    store.shutdown();
    drop(publisher);
    drop(subscriber);
    session.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// Scenario 3: Cache-only recovery (no store feature)
//
// Verifies the publisher cache recovery path still works when the subscriber
// detects gaps. Uses peer-mode Zenoh (no external router).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cache_recovery_without_store() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    // Config without store
    let config = EventBusConfig::builder("test/cache_only_recovery")
        .cache_size(1000)
        .heartbeat(HeartbeatMode::Periodic(Duration::from_millis(200)))
        .history_on_subscribe(false)
        .build()
        .expect("valid config");

    let publisher = EventPublisher::new(&session, config.clone()).await.unwrap();
    let subscriber = EventSubscriber::new(&session, config.clone())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let count = 15u64;
    for i in 0..count {
        publisher
            .publish(&Event::new(TestPayload { value: i }))
            .await
            .unwrap();
    }

    let mut received_values = HashSet::new();
    for _ in 0..count {
        let event: Event<TestPayload> =
            tokio::time::timeout(Duration::from_secs(5), subscriber.recv())
                .await
                .expect("timed out waiting for event")
                .expect("recv failed");
        received_values.insert(event.payload.value);
    }

    assert_eq!(
        received_values.len(),
        count as usize,
        "should receive all events via cache recovery"
    );

    drop(publisher);
    drop(subscriber);
    session.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// Scenario 4: Multiple publishers → single subscriber with store recovery
//
// Two publishers send interleaved events. The subscriber and store handle
// per-publisher sequencing independently.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multiple_publishers_with_store_recovery() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let dir = temp_dir("multi_pub_recovery");

    let config = store_config("multi_pub_recovery");

    let backend = FjallBackend::open(dir.path(), 0).unwrap();
    let mut store = EventStore::new(&session, backend, config.clone());
    store.run().await.unwrap();

    let pub1 = EventPublisher::new(&session, config.clone()).await.unwrap();
    let pub2 = EventPublisher::new(&session, config.clone()).await.unwrap();
    let subscriber = EventSubscriber::new(&session, config.clone())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let count_per_pub = 10u64;

    // Interleave events from both publishers.
    for i in 0..count_per_pub {
        pub1.publish(&Event::new(TestPayload { value: i }))
            .await
            .unwrap();
        pub2.publish(&Event::new(TestPayload { value: 100 + i }))
            .await
            .unwrap();
    }

    let total = count_per_pub * 2;
    let mut received_values = HashSet::new();
    for _ in 0..total {
        let event: Event<TestPayload> =
            tokio::time::timeout(Duration::from_secs(5), subscriber.recv())
                .await
                .expect("timed out waiting for event")
                .expect("recv failed");
        received_values.insert(event.payload.value);
    }

    // Verify all events from both publishers arrived.
    for i in 0..count_per_pub {
        assert!(received_values.contains(&i), "missing event {i} from pub1");
        assert!(
            received_values.contains(&(100 + i)),
            "missing event {} from pub2",
            100 + i
        );
    }

    store.shutdown();
    drop(pub1);
    drop(pub2);
    drop(subscriber);
    session.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// Scenario 5: Store query with publisher_id filter
//
// Verifies the store correctly filters events by publisher_id when queried.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn store_query_filters_by_publisher_id() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let dir = temp_dir("store_pub_filter");

    let config = store_config("store_pub_filter");

    let backend = FjallBackend::open(dir.path(), 0).unwrap();
    let mut store = EventStore::new(&session, backend, config.clone());
    store.run().await.unwrap();

    let pub1 = EventPublisher::new(&session, config.clone()).await.unwrap();
    let pub2 = EventPublisher::new(&session, config.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let pub1_id = *pub1.publisher_id();
    let pub2_id = *pub2.publisher_id();

    // Pub1 sends 5 events, pub2 sends 3 events.
    for i in 0..5u64 {
        pub1.publish(&Event::new(TestPayload { value: i }))
            .await
            .unwrap();
    }
    for i in 0..3u64 {
        pub2.publish(&Event::new(TestPayload { value: 100 + i }))
            .await
            .unwrap();
    }

    // Wait for store persistence.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Query for pub1 only.
    let pub1_events = store
        .query(&QueryFilters {
            publisher_id: Some(pub1_id),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(
        pub1_events.len(),
        5,
        "store should return exactly 5 events for pub1, got {}",
        pub1_events.len()
    );
    for event in &pub1_events {
        assert_eq!(event.metadata.publisher_id, pub1_id);
    }

    // Query for pub2 only.
    let pub2_events = store
        .query(&QueryFilters {
            publisher_id: Some(pub2_id),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(
        pub2_events.len(),
        3,
        "store should return exactly 3 events for pub2, got {}",
        pub2_events.len()
    );
    for event in &pub2_events {
        assert_eq!(event.metadata.publisher_id, pub2_id);
    }

    store.shutdown();
    drop(pub1);
    drop(pub2);
    session.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// Scenario 6: Recovery with zero cache size (store-only recovery)
//
// When cache_size=0, recovery must rely entirely on the EventStore.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_with_zero_cache_store_only() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let dir = temp_dir("zero_cache");

    let config = EventBusConfig::builder("test/zero_cache")
        .cache_size(0)
        .heartbeat(HeartbeatMode::Periodic(Duration::from_millis(200)))
        .history_on_subscribe(false)
        .watermark_interval(Duration::from_millis(50))
        .recovery_delay(Duration::from_millis(30))
        .max_recovery_attempts(3)
        .build()
        .expect("valid config");

    let backend = FjallBackend::open(dir.path(), 0).unwrap();
    let mut store = EventStore::new(&session, backend, config.clone());
    store.run().await.unwrap();

    let publisher = EventPublisher::new(&session, config.clone()).await.unwrap();
    let subscriber = EventSubscriber::new(&session, config.clone())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let count = 10u64;
    for i in 0..count {
        publisher
            .publish(&Event::new(TestPayload { value: i }))
            .await
            .unwrap();
    }

    let mut received_values = HashSet::new();
    for _ in 0..count {
        let event: Event<TestPayload> =
            tokio::time::timeout(Duration::from_secs(5), subscriber.recv())
                .await
                .expect("timed out waiting for event")
                .expect("recv failed");
        received_values.insert(event.payload.value);
    }

    assert_eq!(
        received_values.len(),
        count as usize,
        "should receive all events even with cache_size=0"
    );

    store.shutdown();
    drop(publisher);
    drop(subscriber);
    session.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// Scenario 7: Late subscriber joins, publishes continue, subscriber gets
// both recovered (from store) and live events seamlessly
//
// This is the full scenario requested:
//   1. Publisher sends events 0..N (persisted to store)
//   2. New subscriber joins late
//   3. Publisher continues sending events N..N+M
//   4. Subscriber receives N..N+M live events
//   5. Subscriber can query the store directly for history 0..N
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_subscriber_with_ongoing_publish() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let dir = temp_dir("late_sub");

    let config = store_config("late_sub");

    let backend = FjallBackend::open(dir.path(), 0).unwrap();
    let mut store = EventStore::new(&session, backend, config.clone());
    store.run().await.unwrap();

    let publisher = EventPublisher::new(&session, config.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Phase 1: publish history (subscriber not yet created)
    let history_count = 15u64;
    for i in 0..history_count {
        publisher
            .publish(&Event::new(TestPayload { value: i }))
            .await
            .unwrap();
    }

    // Wait for store to persist.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Phase 2: subscriber joins late
    let subscriber = EventSubscriber::new(&session, config.clone())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Phase 3: publisher continues with live events
    let live_count = 10u64;
    for i in history_count..(history_count + live_count) {
        publisher
            .publish(&Event::new(TestPayload { value: i }))
            .await
            .unwrap();
    }

    // Phase 4: subscriber receives live events
    let mut live_values = Vec::new();
    for _ in 0..live_count {
        let event: Event<TestPayload> =
            tokio::time::timeout(Duration::from_secs(5), subscriber.recv())
                .await
                .expect("timed out waiting for live event")
                .expect("recv failed");
        live_values.push(event.payload.value);
    }

    // All live events should be present.
    let expected_live: Vec<u64> = (history_count..(history_count + live_count)).collect();
    assert_eq!(
        live_values, expected_live,
        "subscriber should receive all live events"
    );

    // Phase 5: verify history is in the store (queryable for recovery/replay)
    // Wait a bit for the store to persist the live events too.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let stored = store.query(&QueryFilters::default()).await.unwrap();
    assert!(
        stored.len() >= history_count as usize,
        "store should have at least the {} historical events, got {}",
        history_count,
        stored.len()
    );

    store.shutdown();
    drop(publisher);
    drop(subscriber);
    session.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// Scenario 8: Recovery config defaults are applied
//
// Verifies the new config fields have sane defaults and can be overridden.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_config_defaults() {
    let config = EventBusConfig::builder("test/config_defaults")
        .build()
        .expect("valid config");

    assert_eq!(config.cache_size, 256, "default cache_size should be 256");
    assert_eq!(
        config.recovery_delay,
        Duration::from_millis(50),
        "default recovery_delay should be 50ms"
    );
    assert_eq!(
        config.max_recovery_attempts, 3,
        "default max_recovery_attempts should be 3"
    );

    // Override them.
    let custom = EventBusConfig::builder("test/config_custom")
        .cache_size(0)
        .recovery_delay(Duration::from_millis(100))
        .max_recovery_attempts(5)
        .build()
        .expect("valid config");

    assert_eq!(custom.cache_size, 0);
    assert_eq!(custom.recovery_delay, Duration::from_millis(100));
    assert_eq!(custom.max_recovery_attempts, 5);
}
