//! Event store sidecar example — durable persistence with FjallBackend and replay queries.
//!
//! This example demonstrates:
//!   1. Running an `EventStore` sidecar that persists every event to an LSM-tree.
//!   2. Publishing events with watermark-confirmed durability (`publish_durable`).
//!   3. Querying the store with `after_seq`, sequence range, and `limit` filters.
//!   4. Observing committed watermark progress and gap detection.
//!   5. Compaction and garbage collection.
//!
//! The `store` feature is enabled by default, so no extra flags are needed.
//!
//! Run with: `cargo run -p mitiflow --example event_store`

use std::time::Duration;

use serde::{Deserialize, Serialize};

use mitiflow::{
    store::QueryFilters, Event, EventBusConfig, EventPublisher, EventStore, EventSubscriber,
    FjallBackend, HeartbeatMode,
};

/// A metric data point collected from a host.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MetricPoint {
    host: String,
    metric: String,
    value: f64,
}

fn build_config() -> mitiflow::Result<EventBusConfig> {
    EventBusConfig::builder("demo/metrics")
        .cache_size(500)
        .heartbeat(HeartbeatMode::Periodic(Duration::from_millis(200)))
        .watermark_interval(Duration::from_millis(50))
        .durable_timeout(Duration::from_secs(5))
        .build()
}

/// Phase 1-2: Start EventStore + create publisher/subscriber.
async fn setup_store_and_pubsub(
    session: &zenoh::Session,
    config: &EventBusConfig,
    store_dir: &std::path::Path,
) -> mitiflow::Result<(EventStore, EventPublisher, EventSubscriber)> {
    let backend = FjallBackend::open(store_dir, 0)?;
    let mut store = EventStore::new(session, backend, config.clone());
    store.run().await?;
    println!("EventStore running\n");

    let subscriber = EventSubscriber::new(session, config.clone()).await?;
    let publisher = EventPublisher::new(session, config.clone()).await?;
    println!("Publisher ID: {}", publisher.publisher_id());
    tokio::time::sleep(Duration::from_millis(200)).await;

    Ok((store, publisher, subscriber))
}

/// Phase 3: Publish events with durable confirmation.
async fn publish_durable_events(
    publisher: &EventPublisher,
    num_events: u64,
) -> mitiflow::Result<()> {
    let hosts = ["web-01", "web-02", "db-01", "db-02"];
    let metrics = ["cpu_pct", "mem_mib", "disk_iops", "net_rx_kbps"];

    println!("Publishing {num_events} metric events with durable confirmation...");
    for i in 0..num_events {
        let event = Event::new(MetricPoint {
            host: hosts[(i as usize) % hosts.len()].to_string(),
            metric: metrics[(i as usize) % metrics.len()].to_string(),
            value: 10.0 * i as f64 + (i % 7) as f64 * 3.14,
        });
        let seq = publisher.publish_durable(&event).await?;
        println!(
            "  committed seq={seq:02}: {}:{} = {:.2}",
            event.payload.host, event.payload.metric, event.payload.value
        );
    }
    Ok(())
}

/// Phase 4: Drain the live subscriber stream.
async fn drain_live_stream(
    subscriber: &EventSubscriber,
    num_events: u64,
) -> mitiflow::Result<()> {
    println!("\nLive stream:");
    for i in 0..num_events {
        let event: Event<MetricPoint> =
            tokio::time::timeout(Duration::from_secs(5), subscriber.recv())
                .await
                .expect("timed out waiting for event")?;
        if i < 4 {
            println!(
                "  recv seq={:02}  {}:{} = {:.2}",
                event.seq.unwrap_or(0),
                event.payload.host,
                event.payload.metric,
                event.payload.value
            );
        } else if i == 4 {
            println!("  ... ({} more)", num_events - 4);
        }
    }
    println!("  All {num_events} events received ✓");
    Ok(())
}

/// Phase 5: Query the store with various filters.
fn run_store_queries(store: &EventStore) -> mitiflow::Result<()> {
    println!("\nStore queries:");

    let recent = store.backend().query(&QueryFilters {
        after_seq: Some(14),
        limit: Some(5),
        ..Default::default()
    })?;
    println!(
        "  after_seq=14, limit=5  -> {} events  seqs={:?}",
        recent.len(),
        recent.iter().map(|e| e.metadata.seq).collect::<Vec<_>>()
    );

    let range = store.backend().query(&QueryFilters {
        after_seq: Some(4),
        before_seq: Some(10),
        ..Default::default()
    })?;
    println!(
        "  seq range (4, 10)      -> {} events  seqs={:?}",
        range.len(),
        range.iter().map(|e| e.metadata.seq).collect::<Vec<_>>()
    );

    let all = store.backend().query(&QueryFilters::default())?;
    println!("  all events             -> {} events  ✓", all.len());
    Ok(())
}

/// Phase 6-7: Watermark, compaction, and GC.
fn run_maintenance(store: &EventStore) -> mitiflow::Result<()> {
    let wms = store.backend().publisher_watermarks();
    println!("\nWatermarks ({} publishers):", wms.len());
    for (pub_id, pw) in &wms {
        println!(
            "  {pub_id}: committed_seq={}  gaps={:?}",
            pw.committed_seq, pw.gaps
        );
    }

    let stats = store.backend().compact()?;
    println!(
        "\nCompaction: removed={}, retained={}",
        stats.removed, stats.retained
    );

    let gc_count = store
        .backend()
        .gc(chrono::Utc::now() - chrono::Duration::hours(1))?;
    println!("GC (older than 1h): removed={gc_count}");
    Ok(())
}

#[tokio::main]
async fn main() -> mitiflow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("mitiflow=info,event_store=info")
        .init();

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let store_dir = tempfile::tempdir().expect("failed to create temp dir");
    println!("Store directory: {:?}", store_dir.path());

    let config = build_config()?;
    let num_events = 20u64;

    let (mut store, publisher, subscriber) =
        setup_store_and_pubsub(&session, &config, store_dir.path()).await?;
    publish_durable_events(&publisher, num_events).await?;
    drain_live_stream(&subscriber, num_events).await?;
    run_store_queries(&store)?;
    run_maintenance(&store)?;

    store.shutdown();
    drop(publisher);
    drop(subscriber);
    session.close().await.unwrap();

    println!("\nDone — {num_events} events durably committed, queried, and verified.");
    Ok(())
}
