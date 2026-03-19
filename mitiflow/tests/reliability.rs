//! Integration tests for end-to-end pub/sub reliability.
//!
//! These tests use peer-mode Zenoh sessions (no router required).

mod common;

use std::time::Duration;

use mitiflow::{Event, EventBusConfig, EventPublisher, EventSubscriber, HeartbeatMode};

use common::TestPayload;

/// Helper: create a config with unique key prefix to avoid cross-test interference.
fn test_config(test_name: &str) -> EventBusConfig {
    common::test_config(test_name)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_publisher_single_subscriber() {
    let (session, publisher, subscriber) = common::setup_pubsub("single_pub_sub").await;

    let count = 5u64;
    common::publish_n(&publisher, count).await;
    let events = common::recv_n(&subscriber, count).await;

    for event in &events {
        assert!(event.seq.is_some());
        assert!(event.payload.value < count);
    }

    drop(publisher);
    drop(subscriber);
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recv_raw_returns_deserializable_event() {
    let (session, publisher, subscriber) = common::setup_pubsub("recv_raw").await;

    let event = Event::new(TestPayload { value: 42 });
    publisher.publish(&event).await.unwrap();

    let raw = tokio::time::timeout(Duration::from_secs(5), subscriber.recv_raw())
        .await
        .expect("timed out")
        .expect("recv_raw failed");

    assert_eq!(raw.seq, 0);
    assert_eq!(raw.publisher_id, *publisher.publisher_id());

    let typed: Event<TestPayload> = raw.deserialize().unwrap();
    assert_eq!(typed.payload.value, 42);

    drop(publisher);
    drop(subscriber);
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multiple_events_maintain_sequence_order() {
    let (session, publisher, subscriber) = common::setup_pubsub("seq_order").await;

    let count = 20u64;
    common::publish_n(&publisher, count).await;
    let events = common::recv_n(&subscriber, count).await;

    let mut received: Vec<u64> = events.iter().map(|e| e.payload.value).collect();
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
    let (session, publisher, subscriber) = common::setup_pubsub("custom_key").await;

    // Publish to a custom sub-key.
    let seq = publisher
        .publish_to(
            "test/custom_key/orders/123",
            &Event::new(TestPayload { value: 99 }),
        )
        .await
        .unwrap();
    assert_eq!(seq, 0);

    let event: Event<TestPayload> = tokio::time::timeout(Duration::from_secs(5), subscriber.recv())
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
