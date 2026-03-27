# mitiflow

Production-grade event streaming for [Zenoh](https://zenoh.io).

Layers Kafka-class reliability — sequencing, gap detection, recovery, durability,
consumer groups — on top of Zenoh's microsecond-latency pub/sub using only
stable APIs. No brokers, no external coordinator.

## Features

- **Publisher** — standard, keyed (`publish_keyed`), and durable (`publish_durable`) variants
- **Subscriber** — gap detection, tiered recovery (store → cache → backoff), processing shards
- **Key-based publishing** — automatic partition routing via `{prefix}/p/{partition}/k/{key}/{seq}`
- **Consumer groups** — `ConsumerGroupSubscriber` with offset commits, zombie fencing, auto-commit
- **Event store** — fjall LSM backend with HLC-ordered replay, key index, log compaction, GC
- **Slow consumer offload** — automatic switchover to store-based catch-up
- **Dead letter queue** — configurable backoff (fixed, exponential)
- **Codecs** — JSON, MessagePack, Postcard via `CodecFormat`
- **Zero-copy routing** — 50-byte metadata in Zenoh attachments

## Quick Start

```rust
use mitiflow::{Event, EventBusConfig, EventPublisher, EventSubscriber};

#[tokio::main]
async fn main() -> mitiflow::Result<()> {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let config = EventBusConfig::builder("demo/sensors").build()?;

    let subscriber = EventSubscriber::new(&session, config.clone()).await?;
    let publisher = EventPublisher::new(&session, config).await?;

    let event = Event::new(serde_json::json!({"temp": 22.5}));
    publisher.publish(&event).await?;

    let received: Event<serde_json::Value> = subscriber.recv().await?;
    println!("Got: {:?}", received.payload);

    drop(publisher);
    drop(subscriber);
    session.close().await.unwrap();
    Ok(())
}
```

## Feature Flags

| Flag | Default | Description |
|------|---------|-------------|
| `store` | Yes | EventStore + storage backend trait |
| `fjall-backend` | No | Concrete fjall LSM-tree backend |
| `wal` | No | Write-ahead log for durable publisher |
| `full` | No | All of the above |

## License

Apache-2.0 — see [LICENSE](../LICENSE).
