//! Slow consumer offload example — automatic pub/sub → store-based catch-up.
//!
//! Demonstrates the transparent offload feature:
//!   1. A publisher sends events at high speed.
//!   2. The subscriber reads slowly, causing the event channel to fill up.
//!   3. The offload system detects sustained lag and transitions to DRAINING.
//!   4. Buffered events are drained, then the subscriber catches up from the store.
//!   5. The subscriber resumes live pub/sub transparently.
//!   6. `OffloadEvent` lifecycle events report each transition.
//!
//! Run with: `cargo run -p mitiflow --example slow_consumer_offload --features full`

use std::time::Duration;

use serde::{Deserialize, Serialize};

use mitiflow::{
    Event, EventBusConfig, EventPublisher, EventStore, EventSubscriber, FjallBackend,
    HeartbeatMode, OffloadConfig,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SensorReading {
    sensor_id: u32,
    value: f64,
}

#[tokio::main]
async fn main() -> mitiflow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("mitiflow=debug")
        .init();

    let session = zenoh::open(zenoh::Config::default()).await?;

    // Configure with a small channel and aggressive thresholds for demo.
    let offload = OffloadConfig {
        enabled: true,
        channel_fullness_threshold: 0.5,
        debounce_window: Duration::from_millis(500),
        ..OffloadConfig::default()
    };

    let config = EventBusConfig::builder("demo/offload")
        .cache_size(100)
        .heartbeat(HeartbeatMode::Disabled)
        .watermark_interval(Duration::from_millis(50))
        .event_channel_capacity(64)
        .num_partitions(1)
        .offload(offload)
        .build()?;

    // Start the event store (required for catch-up reads).
    let store_dir = tempfile::tempdir().expect("failed to create temp dir");
    let backend = FjallBackend::open(store_dir.path(), 0)?;
    let mut store = EventStore::new(&session, backend, config.clone());
    store.run().await?;
    println!("EventStore running");

    // Create subscriber and publisher.
    let subscriber = EventSubscriber::new(&session, config.clone()).await?;
    let publisher = EventPublisher::new(&session, config).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Spawn a task to monitor offload lifecycle events.
    let offload_rx = subscriber.offload_events().unwrap().clone();
    tokio::spawn(async move {
        while let Ok(event) = offload_rx.recv_async().await {
            println!("[OFFLOAD] {event:?}");
        }
    });

    // Publish 500 events rapidly.
    println!("\nPublishing 500 events...");
    for i in 0..500u64 {
        publisher
            .publish(&Event::new(SensorReading {
                sensor_id: (i % 10) as u32,
                value: i as f64 * 0.1,
            }))
            .await?;
    }
    println!("All events published\n");

    // Wait for store to persist.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Consume events with artificial slowness to trigger offload.
    println!("Consuming events (slowly at first)...");
    let mut count = 0u64;
    while let Ok(Ok(event)) =
        tokio::time::timeout(Duration::from_secs(5), subscriber.recv::<SensorReading>()).await
    {
        count += 1;
        // Slow consumer for first 32 reads.
        if count <= 32 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        if count.is_multiple_of(100) || count <= 5 {
            println!(
                "  [{count}] sensor={} value={:.1}",
                event.payload.sensor_id, event.payload.value
            );
        }
    }

    println!("\nReceived {count} total events");

    // Cleanup.
    drop(publisher);
    subscriber.shutdown().await;
    store.shutdown();
    session.close().await?;

    Ok(())
}
