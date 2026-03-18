//! Basic pub/sub example demonstrating mitiflow's event streaming.
//!
//! Run with: `cargo run -p mitiflow --example basic_pubsub --no-default-features`
//!
//! This example creates a publisher and subscriber in the same process using
//! peer-mode Zenoh (no router required). The publisher sends 10 events and
//! the subscriber receives them with gap detection and sequencing.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use mitiflow::{Event, EventBusConfig, EventPublisher, EventSubscriber, HeartbeatMode};

/// Application-level payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SensorReading {
    sensor_id: String,
    temperature: f64,
    humidity: f64,
}

#[tokio::main]
async fn main() -> mitiflow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("mitiflow=debug,basic_pubsub=info")
        .init();

    // Open a peer-mode Zenoh session (no router needed).
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    // Build configuration shared by publisher and subscriber.
    let config = EventBusConfig::builder("demo/sensors")
        .cache_size(100)
        .heartbeat(HeartbeatMode::Periodic(Duration::from_millis(500)))
        .build()?;

    // Create the subscriber first so it's ready when events arrive.
    let subscriber = EventSubscriber::new(&session, config.clone()).await?;

    // Create the publisher.
    let publisher = EventPublisher::new(&session, config).await?;
    println!(
        "Publisher started: {}",
        publisher.publisher_id()
    );

    // Allow subscriber to fully initialize.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish 10 sensor readings.
    let num_events = 10u64;
    for i in 0..num_events {
        let reading = SensorReading {
            sensor_id: format!("sensor-{}", i % 3),
            temperature: 20.0 + i as f64 * 0.5,
            humidity: 45.0 + i as f64 * 1.1,
        };
        let event = Event::new(reading);
        let seq = publisher.publish(&event).await?;
        println!("Published event seq={seq}, id={}", event.id);
    }

    // Receive all events.
    println!("\nReceiving events...");
    for _ in 0..num_events {
        let event: Event<SensorReading> = tokio::time::timeout(
            Duration::from_secs(5),
            subscriber.recv(),
        )
        .await
        .expect("timed out waiting for event")?;

        println!(
            "  Received seq={}, sensor={}, temp={:.1}°C, humidity={:.1}%",
            event.seq.unwrap_or(0),
            event.payload.sensor_id,
            event.payload.temperature,
            event.payload.humidity,
        );
    }

    println!("\nAll {num_events} events received successfully!");

    // Drop publisher and subscriber to cancel background tasks.
    drop(publisher);
    drop(subscriber);
    session.close().await.unwrap();

    Ok(())
}
