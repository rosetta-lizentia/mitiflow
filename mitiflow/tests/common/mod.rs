//! Shared test helpers for mitiflow integration tests.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use mitiflow::{Event, EventBusConfig, EventPublisher, EventSubscriber, HeartbeatMode};

/// Common test payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TestPayload {
    pub value: u64,
}

#[allow(dead_code)]
/// Create an `EventBusConfig` with a unique key prefix for the given test name.
pub fn test_config(test_name: &str) -> EventBusConfig {
    EventBusConfig::builder(format!("test/{test_name}"))
        .cache_size(1000)
        .heartbeat(HeartbeatMode::Periodic(Duration::from_millis(200)))
        .history_on_subscribe(false)
        .build()
        .expect("valid config")
}

#[allow(dead_code)]
/// Open a peer-mode Zenoh session and create a connected publisher + subscriber pair.
///
/// Returns `(session, publisher, subscriber)`. Includes a 100ms settle delay.
pub async fn setup_pubsub(test_name: &str) -> (zenoh::Session, EventPublisher, EventSubscriber) {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let config = test_config(test_name);
    let subscriber = EventSubscriber::new(&session, config.clone())
        .await
        .unwrap();
    let publisher = EventPublisher::new(&session, config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    (session, publisher, subscriber)
}

#[allow(dead_code)]
/// Publish `count` `TestPayload` events with sequential values `0..count`.
pub async fn publish_n(publisher: &EventPublisher, count: u64) {
    for i in 0..count {
        publisher
            .publish(&Event::new(TestPayload { value: i }))
            .await
            .unwrap();
    }
}

#[allow(dead_code)]
/// Receive `count` typed events with a 5-second per-event timeout.
pub async fn recv_n(subscriber: &EventSubscriber, count: u64) -> Vec<Event<TestPayload>> {
    let mut events = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let event: Event<TestPayload> =
            tokio::time::timeout(Duration::from_secs(5), subscriber.recv())
                .await
                .expect("timed out waiting for event")
                .expect("recv failed");
        events.push(event);
    }
    events
}

#[allow(dead_code)]
/// Create a temporary directory with a prefix based on the test name.
pub fn temp_dir(name: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(&format!("mitiflow_test_{name}_"))
        .tempdir()
        .expect("failed to create temp dir")
}
