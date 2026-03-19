//! Durable publish example: events are persisted by an EventStore sidecar
//! and the publisher waits for watermark confirmation before returning.
//!
//! Run with: `cargo run -p mitiflow --example durable_publish`

use std::time::Duration;

use serde::{Deserialize, Serialize};

use mitiflow::{Event, EventBusConfig, EventPublisher, EventStore, FjallBackend, HeartbeatMode};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderEvent {
    order_id: String,
    amount: f64,
}

#[tokio::main]
async fn main() -> mitiflow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("mitiflow=debug,durable_publish=info")
        .init();

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let config = EventBusConfig::builder("demo/orders")
        .cache_size(1000)
        .heartbeat(HeartbeatMode::Periodic(Duration::from_millis(500)))
        .watermark_interval(Duration::from_millis(50))
        .durable_timeout(Duration::from_secs(5))
        .build()?;

    // Start the event store sidecar (in-process for this demo).
    let store_dir = tempfile::tempdir().expect("failed to create temp dir");
    let backend = FjallBackend::open(store_dir.path(), 0)?;
    let mut store = EventStore::new(&session, backend, config.clone());
    store.run().await?;
    println!("EventStore running at {:?}", store_dir.path());

    // Create a publisher.
    let publisher = EventPublisher::new(&session, config).await?;

    // Allow store to initialize.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Publish 5 orders durably — each call blocks until watermark confirms persistence.
    for i in 0..5 {
        let event = Event::new(OrderEvent {
            order_id: format!("ORD-{i:04}"),
            amount: 100.0 + i as f64 * 25.0,
        });

        let seq = publisher.publish_durable(&event).await?;
        println!(
            "Durably published order {} (seq={seq}, id={})",
            event.payload.order_id, event.id
        );
    }

    println!("\nAll orders durably committed!");

    // Query the store to verify.
    let stored = store
        .query(&mitiflow::store::QueryFilters::default()).await?;
    println!("Store contains {} events", stored.len());

    store.shutdown();
    drop(publisher);
    session.close().await.unwrap();

    Ok(())
}
