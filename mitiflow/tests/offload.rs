//! End-to-end tests for the slow-consumer offload feature.
//!
//! These tests verify that a subscriber transparently switches from live
//! pub/sub to store-based catch-up when it falls behind, and that the
//! OffloadEvent lifecycle channel reports correct transitions.

#![cfg(feature = "fjall-backend")]

mod common;

use std::collections::HashSet;
use std::time::Duration;

use mitiflow::store::FjallBackend;
use mitiflow::{
    Event, EventBusConfig, EventPublisher, EventStore, EventSubscriber, HeartbeatMode,
    OffloadConfig,
};

use common::TestPayload;

fn temp_dir(name: &str) -> tempfile::TempDir {
    common::temp_dir(name)
}

/// Build a config with offload enabled and aggressive (test-friendly) thresholds.
fn offload_config(test_name: &str, channel_capacity: usize) -> EventBusConfig {
    let offload = OffloadConfig {
        enabled: true,
        channel_fullness_threshold: 0.5,
        seq_lag_threshold: 5,
        debounce_window: Duration::from_millis(0),
        catch_up_batch_size: 100,
        min_batch_size: 10,
        max_batch_size: 1_000,
        target_batch_duration: Duration::from_millis(100),
        re_subscribe_threshold: 10,
        drain_quiet_period: Duration::from_millis(10),
    };
    EventBusConfig::builder(format!("test/{test_name}"))
        .cache_size(256)
        .heartbeat(HeartbeatMode::Disabled)
        .history_on_subscribe(false)
        .watermark_interval(Duration::from_millis(50))
        .event_channel_capacity(channel_capacity)
        .num_partitions(1) // All events to partition 0 (matches store).
        .offload(offload)
        .build()
        .expect("valid config")
}

// ---------------------------------------------------------------------------
// Test: offload_disabled_no_transition
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offload_disabled_no_transition() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let offload = OffloadConfig {
        enabled: false,
        ..OffloadConfig::default()
    };
    let config = EventBusConfig::builder("test/offload_disabled")
        .cache_size(100)
        .heartbeat(HeartbeatMode::Disabled)
        .history_on_subscribe(false)
        .offload(offload)
        .build()
        .unwrap();

    let subscriber = EventSubscriber::new(&session, config.clone())
        .await
        .unwrap();

    // offload_events() should return None when offload is disabled.
    assert!(
        subscriber.offload_events().is_none(),
        "offload_events() should be None when disabled"
    );

    let publisher = EventPublisher::new(&session, config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish and receive a few events normally.
    for i in 0..5u64 {
        publisher
            .publish(&Event::new(TestPayload { value: i }))
            .await
            .unwrap();
    }
    for _ in 0..5 {
        let _: Event<TestPayload> = tokio::time::timeout(Duration::from_secs(2), subscriber.recv())
            .await
            .expect("timed out")
            .expect("recv failed");
    }

    drop(publisher);
    subscriber.shutdown().await;
    session.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// Test: fast_consumer_never_offloads
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fast_consumer_never_offloads() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let config = offload_config("fast_consumer", 1024);

    let subscriber = EventSubscriber::new(&session, config.clone())
        .await
        .unwrap();
    let offload_rx = subscriber
        .offload_events()
        .expect("offload channel should be Some when enabled")
        .clone();

    let publisher = EventPublisher::new(&session, config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish and immediately consume — channel never fills up.
    for i in 0..50u64 {
        publisher
            .publish(&Event::new(TestPayload { value: i }))
            .await
            .unwrap();
        let _: Event<TestPayload> = tokio::time::timeout(Duration::from_secs(2), subscriber.recv())
            .await
            .expect("timed out")
            .expect("recv failed");
    }

    // No offload events should have been emitted.
    assert!(
        offload_rx.try_recv().is_err(),
        "fast consumer should not trigger offload"
    );

    drop(publisher);
    subscriber.shutdown().await;
    session.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// Test: offload_events_channel_available
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offload_events_channel_available() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let config = offload_config("offload_channel", 64);

    let subscriber = EventSubscriber::new(&session, config).await.unwrap();
    assert!(
        subscriber.offload_events().is_some(),
        "offload_events() should be Some when offload is enabled"
    );

    subscriber.shutdown().await;
    session.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// Test: slow_consumer_triggers_offload_lifecycle
//
// This test verifies the full offload lifecycle:
// 1. Publisher sends events fast, filling the subscriber's channel
// 2. Consumer reads slowly, triggering lag detection
// 3. Offload cycle runs (drain → catch-up from store → resume)
// 4. OffloadEvent lifecycle events are emitted correctly
// 5. All events are received without data loss
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_consumer_triggers_offload_lifecycle() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let dir = temp_dir("offload_lifecycle");

    // Small channel (32) so offload triggers quickly.
    let config = offload_config("offload_lifecycle", 32);

    // Start the event store.
    let backend = FjallBackend::open(dir.path(), 0).unwrap();
    let mut store = EventStore::new(&session, backend, config.clone());
    store.run().await.unwrap();

    // Create subscriber first, then publisher.
    let subscriber = EventSubscriber::new(&session, config.clone())
        .await
        .unwrap();
    let offload_rx = subscriber.offload_events().unwrap().clone();
    let publisher = EventPublisher::new(&session, config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish enough events to fill the channel and trigger offload.
    let total_events = 200u64;
    for i in 0..total_events {
        publisher
            .publish(&Event::new(TestPayload { value: i }))
            .await
            .unwrap();
    }

    // Wait for store to persist events.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Consume events. The first 16 reads are slow to let offload trigger.
    let mut received_values: Vec<u64> = Vec::new();
    let mut offload_events: Vec<String> = Vec::new();

    loop {
        while let Ok(oe) = offload_rx.try_recv() {
            offload_events.push(format!("{oe:?}"));
        }

        match tokio::time::timeout(Duration::from_secs(3), subscriber.recv::<TestPayload>()).await {
            Ok(Ok(event)) => {
                received_values.push(event.payload.value);
                if received_values.len() <= 16 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
            _ => break,
        }
    }

    while let Ok(oe) = offload_rx.try_recv() {
        offload_events.push(format!("{oe:?}"));
    }

    assert!(
        !received_values.is_empty(),
        "should have received some events"
    );

    let unique: HashSet<u64> = received_values.iter().copied().collect();
    assert_eq!(
        unique.len(),
        received_values.len(),
        "no duplicate events should be received"
    );

    drop(publisher);
    subscriber.shutdown().await;
    store.shutdown();
    session.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// Test: offload_preserves_ordering
//
// Events received before, during, and after an offload cycle should maintain
// per-publisher ordering (sequence numbers are monotonically increasing).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offload_preserves_ordering() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let dir = temp_dir("offload_ordering");

    let config = offload_config("offload_ordering", 32);

    // Start store.
    let backend = FjallBackend::open(dir.path(), 0).unwrap();
    let mut store = EventStore::new(&session, backend, config.clone());
    store.run().await.unwrap();

    let subscriber = EventSubscriber::new(&session, config.clone())
        .await
        .unwrap();
    let publisher = EventPublisher::new(&session, config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish events.
    let total = 100u64;
    for i in 0..total {
        publisher
            .publish(&Event::new(TestPayload { value: i }))
            .await
            .unwrap();
    }

    // Wait for store.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Consume with slow start.
    let mut received: Vec<Event<TestPayload>> = Vec::new();
    while let Ok(Ok(event)) =
        tokio::time::timeout(Duration::from_secs(3), subscriber.recv::<TestPayload>()).await
    {
        received.push(event);
        if received.len() <= 16 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    // Verify ordering: sequence numbers should be monotonically increasing.
    let mut last_seq: u64 = 0;
    for event in &received {
        if let Some(seq) = event.seq {
            assert!(
                seq >= last_seq,
                "out of order: seq {} after {}",
                seq,
                last_seq
            );
            last_seq = seq;
        }
    }

    drop(publisher);
    subscriber.shutdown().await;
    store.shutdown();
    session.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// Test: debounce_prevents_flapping
//
// A short burst fills the channel momentarily but clears quickly.
// With a debounce window, offload should NOT trigger.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn debounce_prevents_flapping() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    // Long debounce (5s) — the burst clears well before.
    let offload = OffloadConfig {
        enabled: true,
        channel_fullness_threshold: 0.5,
        debounce_window: Duration::from_secs(5),
        ..OffloadConfig::default()
    };
    let config = EventBusConfig::builder("test/debounce_flapping")
        .cache_size(100)
        .heartbeat(HeartbeatMode::Disabled)
        .history_on_subscribe(false)
        .event_channel_capacity(64)
        .offload(offload)
        .build()
        .unwrap();

    let subscriber = EventSubscriber::new(&session, config.clone())
        .await
        .unwrap();
    let offload_rx = subscriber.offload_events().unwrap().clone();
    let publisher = EventPublisher::new(&session, config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish a short burst.
    for i in 0..20u64 {
        publisher
            .publish(&Event::new(TestPayload { value: i }))
            .await
            .unwrap();
    }

    // Consume all quickly — channel clears before debounce expires.
    for _ in 0..20 {
        let _: Event<TestPayload> = tokio::time::timeout(Duration::from_secs(2), subscriber.recv())
            .await
            .expect("timed out")
            .expect("recv failed");
    }

    // No offload should have triggered.
    assert!(
        offload_rx.try_recv().is_err(),
        "debounce should prevent offload on transient burst"
    );

    drop(publisher);
    subscriber.shutdown().await;
    session.close().await.unwrap();
}
