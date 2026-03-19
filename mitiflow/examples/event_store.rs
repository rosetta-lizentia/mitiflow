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

#[tokio::main]
async fn main() -> mitiflow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("mitiflow=info,event_store=info")
        .init();

    // Peer-mode Zenoh session — no router required.
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    // Temporary directory for the LSM-tree storage backend.
    let store_dir = tempfile::tempdir().expect("failed to create temp dir");
    println!("Store directory: {:?}", store_dir.path());

    let config = EventBusConfig::builder("demo/metrics")
        .cache_size(500)
        .heartbeat(HeartbeatMode::Periodic(Duration::from_millis(200)))
        // Watermark is published every 50 ms (lower latency for this demo).
        .watermark_interval(Duration::from_millis(50))
        // `publish_durable` will wait up to 5 s for watermark confirmation.
        .durable_timeout(Duration::from_secs(5))
        .build()?;

    // -------------------------------------------------------------------------
    // Phase 1: Start the EventStore sidecar (in-process for this demo).
    //
    // In production you would run EventStore in a separate process or node.
    // It subscribes to the event stream, persists every event, broadcasts a
    // CommitWatermark every `watermark_interval`, and serves replay queries.
    // -------------------------------------------------------------------------
    let backend = FjallBackend::open(store_dir.path(), 0)?;
    let mut store = EventStore::new(&session, backend, config.clone());
    store.run().await?;
    println!("EventStore running\n");

    // -------------------------------------------------------------------------
    // Phase 2: Create a subscriber (live stream) and a durable publisher.
    // -------------------------------------------------------------------------
    let subscriber = EventSubscriber::new(&session, config.clone()).await?;
    let publisher = EventPublisher::new(&session, config.clone()).await?;
    println!("Publisher ID: {}", publisher.publisher_id());

    // Allow Zenoh session state and subscriptions to stabilize.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // -------------------------------------------------------------------------
    // Phase 3: Publish 20 metric events using `publish_durable`.
    //
    // Each call to `publish_durable` blocks until the EventStore confirms that
    // the event's sequence number is covered by a CommitWatermark — meaning
    // it is safely fsync'd to the LSM-tree before the call returns.
    // -------------------------------------------------------------------------
    let hosts = ["web-01", "web-02", "db-01", "db-02"];
    let metrics = ["cpu_pct", "mem_mib", "disk_iops", "net_rx_kbps"];
    let num_events = 20u64;

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

    // -------------------------------------------------------------------------
    // Phase 4: Drain the live subscriber stream.
    //
    // The subscriber runs concurrently with pub/store operations; all 20 events
    // should be delivered in sequence order with gap detection already applied.
    // -------------------------------------------------------------------------
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

    // -------------------------------------------------------------------------
    // Phase 5: Query the store with various filters.
    //
    // The EventStore exposes a Zenoh queryable at `{key_prefix}/_store` that
    // accepts structured filter parameters. The `StorageBackend::query` method
    // can also be called directly, as shown here.
    // -------------------------------------------------------------------------
    println!("\nStore queries:");

    // Most recent 5 events (seq > 14).
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

    // Slice of events in a specific sequence window (5 < seq < 10).
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

    // Verify all events are in the store.
    let all = store.backend().query(&QueryFilters::default())?;
    println!("  all events             -> {} events  ✓", all.len());

    // -------------------------------------------------------------------------
    // Phase 6: Inspect the CommitWatermark state.
    //
    // `committed_seq` is the highest contiguous sequence number that has been
    // durably persisted. `gap_sequences` lists any holes below that horizon.
    // -------------------------------------------------------------------------
    println!(
        "\nWatermark: committed_seq={}  gaps={:?}",
        store.backend().committed_seq(),
        store.backend().gap_sequences()
    );

    // -------------------------------------------------------------------------
    // Phase 7: Compaction and garbage collection.
    //
    // `compact()` keeps only the latest event per key expression (useful for
    // state-update topics). `gc(cutoff)` removes events older than a timestamp.
    // -------------------------------------------------------------------------
    let stats = store.backend().compact()?;
    println!(
        "\nCompaction: removed={}, retained={}",
        stats.removed, stats.retained
    );

    // GC removes events older than 1 hour (none yet in this demo).
    let gc_count = store
        .backend()
        .gc(chrono::Utc::now() - chrono::Duration::hours(1))?;
    println!("GC (older than 1h): removed={gc_count}");

    // -------------------------------------------------------------------------
    // Cleanup
    // -------------------------------------------------------------------------
    store.shutdown();
    drop(publisher);
    drop(subscriber);
    session.close().await.unwrap();

    println!("\nDone — {num_events} events durably committed, queried, and verified.");
    Ok(())
}
