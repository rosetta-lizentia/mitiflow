//! Integration tests for key-based publishing (Phase 1).
//!
//! Tests cover: keyed publish delivery, key expression layout,
//! partition affinity, coexistence with unkeyed events, key-filtered
//! subscriptions, RawEvent::key() round-trip, bytes variant,
//! key validation, and durable keyed publish.

mod common;

use std::time::Duration;

use common::TestPayload;
use mitiflow::{Event, EventBusConfig, EventPublisher, EventSubscriber, HeartbeatMode};

/// Create config with unique prefix and low partition count for deterministic tests.
fn keyed_config(test_name: &str) -> EventBusConfig {
    EventBusConfig::builder(format!("test/{test_name}"))
        .cache_size(1000)
        .heartbeat(HeartbeatMode::Periodic(Duration::from_millis(200)))
        .history_on_subscribe(false)
        .num_partitions(16)
        .build()
        .expect("valid config")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_keyed_delivers_to_subscriber() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let config = keyed_config("keyed_deliver");
    let subscriber = EventSubscriber::new(&session, config.clone())
        .await
        .unwrap();
    let publisher = EventPublisher::new(&session, config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let event = Event::new(TestPayload { value: 42 });
    publisher.publish_keyed("order-123", &event).await.unwrap();

    let received: Event<TestPayload> =
        tokio::time::timeout(Duration::from_secs(5), subscriber.recv())
            .await
            .expect("timed out")
            .expect("recv failed");

    assert_eq!(received.payload.value, 42);
    drop(subscriber);
    drop(publisher);
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_keyed_key_in_key_expr() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let config = keyed_config("keyed_key_expr");
    let subscriber = EventSubscriber::new(&session, config.clone())
        .await
        .unwrap();
    let publisher = EventPublisher::new(&session, config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let event = Event::new(TestPayload { value: 1 });
    publisher.publish_keyed("order-123", &event).await.unwrap();

    let raw = tokio::time::timeout(Duration::from_secs(5), subscriber.recv_raw())
        .await
        .expect("timed out")
        .expect("recv failed");

    assert!(
        raw.key_expr.contains("/k/order-123/"),
        "key_expr should contain /k/order-123/, got: {}",
        raw.key_expr
    );
    drop(subscriber);
    drop(publisher);
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_keyed_partition_affinity() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let config = keyed_config("keyed_affinity");
    let subscriber = EventSubscriber::new(&session, config.clone())
        .await
        .unwrap();
    let publisher = EventPublisher::new(&session, config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish same key 5 times — all should go to same partition
    for i in 0..5u64 {
        publisher
            .publish_keyed("same-key", &Event::new(TestPayload { value: i }))
            .await
            .unwrap();
    }

    let mut partitions = std::collections::HashSet::new();
    for _ in 0..5 {
        let raw = tokio::time::timeout(Duration::from_secs(5), subscriber.recv_raw())
            .await
            .expect("timed out")
            .expect("recv failed");
        let partition = mitiflow::attachment::extract_partition(&raw.key_expr);
        partitions.insert(partition);
    }

    assert_eq!(
        partitions.len(),
        1,
        "same key should always hash to same partition"
    );
    drop(subscriber);
    drop(publisher);
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_keyed_and_unkeyed_coexist() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let config = keyed_config("keyed_coexist");
    let subscriber = EventSubscriber::new(&session, config.clone())
        .await
        .unwrap();
    let publisher = EventPublisher::new(&session, config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish 1 unkeyed + 1 keyed
    publisher
        .publish(&Event::new(TestPayload { value: 1 }))
        .await
        .unwrap();
    publisher
        .publish_keyed("my-key", &Event::new(TestPayload { value: 2 }))
        .await
        .unwrap();

    // Subscriber on `{prefix}/**` should receive both
    let mut received = Vec::new();
    for _ in 0..2 {
        let raw = tokio::time::timeout(Duration::from_secs(5), subscriber.recv_raw())
            .await
            .expect("timed out")
            .expect("recv failed");
        received.push(raw);
    }

    assert_eq!(received.len(), 2);
    // One should have a key, one should not
    let keys: Vec<Option<&str>> = received.iter().map(|r| r.key()).collect();
    assert!(keys.contains(&None), "unkeyed event should have key=None");
    assert!(
        keys.contains(&Some("my-key")),
        "keyed event should have key=Some(\"my-key\")"
    );
    drop(subscriber);
    drop(publisher);
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_keyed_key_filter_subscribe() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    // Use a config WITHOUT heartbeat/recovery for the filtered subscriber,
    // because gap recovery would pull in the "beta" event to fill a sequence
    // gap, bypassing Zenoh key expression filtering.
    let sub_config = EventBusConfig::builder("test/keyed_filter")
        .cache_size(1000)
        .heartbeat(HeartbeatMode::Disabled)
        .history_on_subscribe(false)
        .num_partitions(16)
        .build()
        .expect("valid config");

    let pub_config = keyed_config("keyed_filter");

    // Create a filtered subscriber for key "alpha" only
    let filter_key_expr = sub_config.key_expr_for_key("alpha");
    let filtered_sub =
        EventSubscriber::init_with_key_exprs(&session, sub_config, &[filter_key_expr])
            .await
            .unwrap();

    let publisher = EventPublisher::new(&session, pub_config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish to "alpha" and "beta"
    publisher
        .publish_keyed("alpha", &Event::new(TestPayload { value: 1 }))
        .await
        .unwrap();
    publisher
        .publish_keyed("beta", &Event::new(TestPayload { value: 2 }))
        .await
        .unwrap();
    publisher
        .publish_keyed("alpha", &Event::new(TestPayload { value: 3 }))
        .await
        .unwrap();

    // Filtered subscriber should only receive "alpha" events
    let mut alpha_events = Vec::new();
    for _ in 0..2 {
        let raw = tokio::time::timeout(Duration::from_secs(5), filtered_sub.recv_raw())
            .await
            .expect("timed out")
            .expect("recv failed");
        assert_eq!(raw.key(), Some("alpha"));
        alpha_events.push(raw);
    }
    assert_eq!(alpha_events.len(), 2);

    // "beta" event should not arrive (timeout expected)
    let result = tokio::time::timeout(Duration::from_millis(500), filtered_sub.recv_raw()).await;
    assert!(result.is_err(), "beta event should not be received");

    drop(filtered_sub);
    drop(publisher);
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_keyed_roundtrip_raw_event_key() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let config = keyed_config("keyed_roundtrip");
    let subscriber = EventSubscriber::new(&session, config.clone())
        .await
        .unwrap();
    let publisher = EventPublisher::new(&session, config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    publisher
        .publish_keyed("user/42/orders", &Event::new(TestPayload { value: 7 }))
        .await
        .unwrap();

    let raw = tokio::time::timeout(Duration::from_secs(5), subscriber.recv_raw())
        .await
        .expect("timed out")
        .expect("recv failed");

    assert_eq!(raw.key(), Some("user/42/orders"));
    drop(subscriber);
    drop(publisher);
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_bytes_keyed_works() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let config = keyed_config("bytes_keyed");
    let subscriber = EventSubscriber::new(&session, config.clone())
        .await
        .unwrap();
    let publisher = EventPublisher::new(&session, config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let payload = vec![1u8, 2, 3, 4, 5];
    publisher
        .publish_bytes_keyed("raw-key", payload.clone())
        .await
        .unwrap();

    let raw = tokio::time::timeout(Duration::from_secs(5), subscriber.recv_raw())
        .await
        .expect("timed out")
        .expect("recv failed");

    assert_eq!(raw.key(), Some("raw-key"));
    assert_eq!(raw.payload, payload);
    drop(subscriber);
    drop(publisher);
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_keyed_sequence_increments() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let config = keyed_config("keyed_seq");
    let publisher = EventPublisher::new(&session, config).await.unwrap();

    let seq0 = publisher
        .publish_keyed("seq-key", &Event::new(TestPayload { value: 0 }))
        .await
        .unwrap();
    let seq1 = publisher
        .publish_keyed("seq-key", &Event::new(TestPayload { value: 1 }))
        .await
        .unwrap();
    let seq2 = publisher
        .publish_keyed("seq-key", &Event::new(TestPayload { value: 2 }))
        .await
        .unwrap();

    // Sequences should be monotonically increasing within the same partition
    assert!(seq1 > seq0, "seq1 ({seq1}) > seq0 ({seq0})");
    assert!(seq2 > seq1, "seq2 ({seq2}) > seq1 ({seq1})");
    drop(publisher);
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_keyed_validates_key() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let config = keyed_config("keyed_validate");
    let publisher = EventPublisher::new(&session, config).await.unwrap();

    let event = Event::new(TestPayload { value: 0 });

    // Empty key
    let result = publisher.publish_keyed("", &event).await;
    assert!(result.is_err());

    // Star in key
    let result = publisher.publish_keyed("bad*key", &event).await;
    assert!(result.is_err());

    // Dollar in key
    let result = publisher.publish_keyed("bad$key", &event).await;
    assert!(result.is_err());

    // Valid key should succeed
    let result = publisher.publish_keyed("good-key", &event).await;
    assert!(result.is_ok());

    drop(publisher);
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriber_new_keyed_receives_only_matching_key() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    // Disable heartbeat/recovery to avoid gap recovery pulling in filtered-out events.
    let config = EventBusConfig::builder("test/sub_new_keyed")
        .cache_size(1000)
        .heartbeat(HeartbeatMode::Disabled)
        .history_on_subscribe(false)
        .num_partitions(16)
        .build()
        .expect("valid config");

    let sub = EventSubscriber::new_keyed(&session, config.clone(), "order-A")
        .await
        .unwrap();
    let publisher = EventPublisher::new(&session, config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    publisher
        .publish_keyed("order-A", &Event::new(TestPayload { value: 1 }))
        .await
        .unwrap();
    publisher
        .publish_keyed("order-B", &Event::new(TestPayload { value: 2 }))
        .await
        .unwrap();
    publisher
        .publish_keyed("order-A", &Event::new(TestPayload { value: 3 }))
        .await
        .unwrap();

    // Should receive exactly 2 events (both order-A)
    for _ in 0..2 {
        let raw = tokio::time::timeout(Duration::from_secs(5), sub.recv_raw())
            .await
            .expect("timed out")
            .expect("recv failed");
        assert_eq!(raw.key(), Some("order-A"));
    }

    // order-B should not arrive
    let extra = tokio::time::timeout(Duration::from_millis(500), sub.recv_raw()).await;
    assert!(extra.is_err(), "order-B event should not be received");

    drop(sub);
    drop(publisher);
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriber_new_key_prefix_receives_matching() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let config = EventBusConfig::builder("test/sub_key_prefix")
        .cache_size(1000)
        .heartbeat(HeartbeatMode::Disabled)
        .history_on_subscribe(false)
        .num_partitions(16)
        .build()
        .expect("valid config");

    let sub = EventSubscriber::new_key_prefix(&session, config.clone(), "user/1")
        .await
        .unwrap();
    let publisher = EventPublisher::new(&session, config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    publisher
        .publish_keyed("user/1/orders", &Event::new(TestPayload { value: 10 }))
        .await
        .unwrap();
    publisher
        .publish_keyed("user/2/orders", &Event::new(TestPayload { value: 20 }))
        .await
        .unwrap();
    publisher
        .publish_keyed("user/1/profile", &Event::new(TestPayload { value: 30 }))
        .await
        .unwrap();

    // Should receive user/1/orders and user/1/profile
    let mut received_keys = Vec::new();
    for _ in 0..2 {
        let raw = tokio::time::timeout(Duration::from_secs(5), sub.recv_raw())
            .await
            .expect("timed out")
            .expect("recv failed");
        received_keys.push(raw.key().unwrap().to_string());
    }
    received_keys.sort();
    assert_eq!(received_keys, vec!["user/1/orders", "user/1/profile"]);

    // user/2 should not arrive
    let extra = tokio::time::timeout(Duration::from_millis(500), sub.recv_raw()).await;
    assert!(extra.is_err(), "user/2 event should not be received");

    drop(sub);
    drop(publisher);
    session.close().await.unwrap();
}

#[cfg(feature = "store")]
mod durable_keyed {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publish_keyed_durable_confirmed() {
        let session = zenoh::open(zenoh::Config::default()).await.unwrap();
        let config = EventBusConfig::builder("test/keyed_durable")
            .cache_size(1000)
            .heartbeat(HeartbeatMode::Periodic(Duration::from_millis(200)))
            .history_on_subscribe(false)
            .num_partitions(1)
            .watermark_interval(Duration::from_millis(50))
            .durable_timeout(Duration::from_secs(5))
            .durable_urgency(Duration::from_millis(0))
            .build()
            .unwrap();

        let tmp = common::temp_dir("keyed_durable");
        let backend = mitiflow::FjallBackend::open(tmp.path(), 0).unwrap();
        let mut store = mitiflow::EventStore::new(&session, backend, config.clone());
        store.run().await.unwrap();
        let publisher = EventPublisher::new(&session, config.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;

        let event = Event::new(TestPayload { value: 99 });
        let seq = publisher
            .publish_keyed_durable("durable-key", &event)
            .await
            .unwrap();

        assert_eq!(seq, 0);

        drop(store);
        drop(publisher);
        session.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publish_bytes_keyed_durable_works() {
        let session = zenoh::open(zenoh::Config::default()).await.unwrap();
        let config = EventBusConfig::builder("test/bytes_keyed_durable")
            .cache_size(1000)
            .heartbeat(HeartbeatMode::Periodic(Duration::from_millis(200)))
            .history_on_subscribe(false)
            .num_partitions(1)
            .watermark_interval(Duration::from_millis(50))
            .durable_timeout(Duration::from_secs(5))
            .durable_urgency(Duration::from_millis(0))
            .build()
            .unwrap();

        let tmp = common::temp_dir("bytes_keyed_durable");
        let backend = mitiflow::FjallBackend::open(tmp.path(), 0).unwrap();
        let mut store = mitiflow::EventStore::new(&session, backend, config.clone());
        store.run().await.unwrap();
        let publisher = EventPublisher::new(&session, config.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;

        let payload = vec![10u8, 20, 30];
        let seq = publisher
            .publish_bytes_keyed_durable("bytes-dur-key", payload)
            .await
            .unwrap();

        assert_eq!(seq, 0);

        drop(store);
        drop(publisher);
        session.close().await.unwrap();
    }

    /// End-to-end: publish keyed → store → query_by_key returns it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_query_by_key_e2e() {
        use mitiflow::store::backend::StorageBackend;

        let session = zenoh::open(zenoh::Config::default()).await.unwrap();
        let config = EventBusConfig::builder("test/store_query_by_key")
            .cache_size(1000)
            .heartbeat(HeartbeatMode::Periodic(Duration::from_millis(200)))
            .history_on_subscribe(false)
            .num_partitions(1)
            .watermark_interval(Duration::from_millis(50))
            .durable_timeout(Duration::from_secs(5))
            .durable_urgency(Duration::from_millis(0))
            .build()
            .unwrap();

        let tmp = common::temp_dir("store_query_by_key");
        let backend: std::sync::Arc<dyn StorageBackend> =
            std::sync::Arc::new(mitiflow::FjallBackend::open(tmp.path(), 0).unwrap());
        let mut store = mitiflow::EventStore::new(&session, backend.clone(), config.clone());
        store.run().await.unwrap();
        let publisher = EventPublisher::new(&session, config.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Publish keyed events
        publisher
            .publish_keyed("order-A", &Event::new(TestPayload { value: 1 }))
            .await
            .unwrap();
        publisher
            .publish_keyed("order-B", &Event::new(TestPayload { value: 2 }))
            .await
            .unwrap();
        publisher
            .publish_keyed("order-A", &Event::new(TestPayload { value: 3 }))
            .await
            .unwrap();

        // Give store time to persist
        tokio::time::sleep(Duration::from_millis(500)).await;

        let a_results = backend.query_by_key("order-A", None).unwrap();
        assert_eq!(a_results.len(), 2, "should find 2 events for order-A");
        for e in &a_results {
            assert_eq!(e.metadata.key, Some("order-A".to_string()));
        }

        let b_results = backend.query_by_key("order-B", None).unwrap();
        assert_eq!(b_results.len(), 1, "should find 1 event for order-B");

        drop(store);
        drop(publisher);
        session.close().await.unwrap();
    }
}
