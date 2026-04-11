//! Key-based pub/sub example demonstrating automatic partition routing
//! and key-filtered subscriptions.
//!
//! Run with: `cargo run -p mitiflow --example keyed_pubsub`
//!
//! This example shows:
//! 1. Publishing events with application keys (`publish_keyed`)
//! 2. Automatic partition routing via key hashing
//! 3. Key-filtered subscriber (`new_keyed`) that receives only matching events
//! 4. Hierarchical keys (`user/42/orders`)
//! 5. Extracting the key from received events via `RawEvent::key()`

use std::time::Duration;

use serde::{Deserialize, Serialize};

use mitiflow::{Event, EventBusConfig, EventPublisher, EventSubscriber, HeartbeatMode};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderEvent {
    item: String,
    quantity: u32,
}

#[tokio::main]
async fn main() -> mitiflow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("mitiflow=debug,keyed_pubsub=info")
        .init();

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let config = EventBusConfig::builder("demo/orders")
        .cache_size(100)
        .heartbeat(HeartbeatMode::Disabled)
        .num_partitions(8)
        .build()?;

    // ── 1. Wildcard subscriber (receives all events) ──
    let all_sub = EventSubscriber::new(&session, config.clone()).await?;

    // ── 2. Key-filtered subscriber (receives only "order-123" events) ──
    let filtered_sub = EventSubscriber::new_keyed(&session, config.clone(), "order-123").await?;

    let publisher = EventPublisher::new(&session, config).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // ── 3. Publish keyed events ──
    println!("Publishing keyed events...\n");

    let receipt = publisher
        .publish_keyed(
            "order-123",
            &Event::new(OrderEvent {
                item: "Widget A".into(),
                quantity: 5,
            }),
        )
        .await?;
    println!("  [order-123] seq={}", receipt.seq);

    let receipt = publisher
        .publish_keyed(
            "order-456",
            &Event::new(OrderEvent {
                item: "Gadget B".into(),
                quantity: 2,
            }),
        )
        .await?;
    println!("  [order-456] seq={}", receipt.seq);

    // Hierarchical key example
    let receipt = publisher
        .publish_keyed(
            "user/42/orders",
            &Event::new(OrderEvent {
                item: "Doohickey C".into(),
                quantity: 1,
            }),
        )
        .await?;
    println!("  [user/42/orders] seq={}", receipt.seq);

    let receipt = publisher
        .publish_keyed(
            "order-123",
            &Event::new(OrderEvent {
                item: "Widget A (restock)".into(),
                quantity: 10,
            }),
        )
        .await?;
    println!("  [order-123] seq={}", receipt.seq);

    // ── 4. Receive from wildcard subscriber ──
    println!("\n--- Wildcard subscriber (all events) ---");
    for _ in 0..4 {
        let raw = tokio::time::timeout(Duration::from_secs(5), all_sub.recv_raw())
            .await
            .expect("timed out")?;
        println!(
            "  key={:?}  seq={}  payload_len={}",
            raw.key(),
            raw.seq,
            raw.payload.len()
        );
    }

    // ── 5. Receive from filtered subscriber ──
    println!("\n--- Filtered subscriber (order-123 only) ---");
    for _ in 0..2 {
        let raw = tokio::time::timeout(Duration::from_secs(5), filtered_sub.recv_raw())
            .await
            .expect("timed out")?;
        println!(
            "  key={:?}  seq={}  payload_len={}",
            raw.key(),
            raw.seq,
            raw.payload.len()
        );
    }

    // order-456 and user/42/orders should NOT appear
    let extra = tokio::time::timeout(Duration::from_millis(300), filtered_sub.recv_raw()).await;
    assert!(
        extra.is_err(),
        "filtered subscriber should not get other keys"
    );
    println!("  (no extra events — filter works!)");

    println!("\nDone!");

    drop(publisher);
    drop(all_sub);
    drop(filtered_sub);
    session.close().await.unwrap();

    Ok(())
}
