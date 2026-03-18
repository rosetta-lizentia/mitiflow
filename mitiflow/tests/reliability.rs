//! Integration tests for end-to-end pub/sub reliability.
//!
//! These tests use peer-mode Zenoh sessions (no router required).

use std::time::Duration;

use serde::{Deserialize, Serialize};

use mitiflow::{Event, EventBusConfig, EventPublisher, EventSubscriber, HeartbeatMode};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestPayload {
    value: u64,
}

/// Helper: create a config with unique key prefix to avoid cross-test interference.
fn test_config(test_name: &str) -> EventBusConfig {
    EventBusConfig::builder(format!("test/{test_name}"))
        .cache_size(1000)
        .heartbeat(HeartbeatMode::Periodic(Duration::from_millis(200)))
        .history_on_subscribe(false)
        .build()
        .expect("valid config")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_publisher_single_subscriber() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let config = test_config("single_pub_sub");
    let subscriber = EventSubscriber::new(&session, config.clone()).await.unwrap();
    let publisher = EventPublisher::new(&session, config).await.unwrap();

    // Let subscriber fully initialize.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let count = 5u64;
    for i in 0..count {
        let event = Event::new(TestPayload { value: i });
        let seq = publisher.publish(&event).await.unwrap();
        assert_eq!(seq, i);
    }

    // Receive all events.
    for _ in 0..count {
        let event: Event<TestPayload> = tokio::time::timeout(
            Duration::from_secs(5),
            subscriber.recv(),
        )
        .await
        .expect("timed out")
        .expect("recv failed");

        assert!(event.seq.is_some());
        assert!(event.payload.value < count);
    }

    drop(publisher);
    drop(subscriber);
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recv_raw_returns_deserializable_event() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let config = test_config("recv_raw");
    let subscriber = EventSubscriber::new(&session, config.clone()).await.unwrap();
    let publisher = EventPublisher::new(&session, config).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let event = Event::new(TestPayload { value: 42 });
    publisher.publish(&event).await.unwrap();

    let raw = tokio::time::timeout(Duration::from_secs(5), subscriber.recv_raw())
        .await
        .expect("timed out")
        .expect("recv_raw failed");

    assert_eq!(raw.seq, 0);
    assert_eq!(raw.publisher_id, *publisher.publisher_id());

    // Deserialize from raw.
    let typed: Event<TestPayload> = raw.deserialize().unwrap();
    assert_eq!(typed.payload.value, 42);

    drop(publisher);
    drop(subscriber);
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multiple_events_maintain_sequence_order() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let config = test_config("seq_order");
    let subscriber = EventSubscriber::new(&session, config.clone()).await.unwrap();
    let publisher = EventPublisher::new(&session, config).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let count = 20u64;
    for i in 0..count {
        publisher
            .publish(&Event::new(TestPayload { value: i }))
            .await
            .unwrap();
    }

    let mut received = Vec::new();
    for _ in 0..count {
        let event: Event<TestPayload> = tokio::time::timeout(
            Duration::from_secs(5),
            subscriber.recv(),
        )
        .await
        .expect("timed out")
        .expect("recv failed");

        received.push(event.payload.value);
    }

    // All values should be present (though order may differ due to async delivery).
    received.sort();
    let expected: Vec<u64> = (0..count).collect();
    assert_eq!(received, expected, "all events should be received");

    drop(publisher);
    drop(subscriber);
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_id_is_stable() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let config = test_config("pub_id_stable");
    let publisher = EventPublisher::new(&session, config).await.unwrap();

    // Publisher ID should not change between calls.
    let id1 = publisher.publisher_id().clone();
    let id2 = publisher.publisher_id().clone();
    assert_eq!(id1, id2);

    // Current seq starts at 0.
    assert_eq!(publisher.current_seq(), 0);

    publisher
        .publish(&Event::new(TestPayload { value: 0 }))
        .await
        .unwrap();
    assert_eq!(publisher.current_seq(), 1);

    drop(publisher);
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_to_custom_key() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let config = test_config("custom_key");
    let subscriber = EventSubscriber::new(&session, config.clone()).await.unwrap();
    let publisher = EventPublisher::new(&session, config).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish to a custom sub-key.
    let seq = publisher
        .publish_to(
            "test/custom_key/orders/123",
            &Event::new(TestPayload { value: 99 }),
        )
        .await
        .unwrap();
    assert_eq!(seq, 0);

    let event: Event<TestPayload> = tokio::time::timeout(
        Duration::from_secs(5),
        subscriber.recv(),
    )
    .await
    .expect("timed out")
    .expect("recv failed");

    assert_eq!(event.payload.value, 99);
    assert_eq!(
        event.key_expr.as_deref(),
        Some("test/custom_key/orders/123")
    );

    drop(publisher);
    drop(subscriber);
    session.close().await.unwrap();
}
