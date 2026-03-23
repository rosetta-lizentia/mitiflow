//! End-to-end durable pub/sub benchmark — measures publish-with-ACK through
//! to consumer receive latency across transport backends.
//!
//! Unlike `bench_pubsub` (best-effort) or `bench_durable` (producer-only),
//! this benchmark combines durable publishing with consumer-side measurement
//! for true end-to-end durable latency.
//!
//! Usage examples:
//!
//! ```bash
//! # Mitiflow durable e2e (spawns EventStore sidecar automatically)
//! cargo run --release -p mitiflow-bench --bin bench_durable_pubsub -- \
//!   --transport mitiflow --rate 1000 --duration 10
//!
//! # Kafka acks=all e2e
//! cargo run --release -p mitiflow-bench --features kafka --bin bench_durable_pubsub -- \
//!   --transport kafka --rate 1000 --duration 10
//!
//! # Redpanda acks=all e2e
//! cargo run --release -p mitiflow-bench --features kafka --bin bench_durable_pubsub -- \
//!   --transport redpanda --kafka-broker localhost:29092 --rate 1000 --duration 10
//!
//! # NATS JetStream e2e
//! cargo run --release -p mitiflow-bench --features nats --bin bench_durable_pubsub -- \
//!   --transport nats --rate 1000 --duration 10
//!
//! # Redis Streams e2e (XADD is inherently durable)
//! cargo run --release -p mitiflow-bench --features redis --bin bench_durable_pubsub -- \
//!   --transport redis --rate 1000 --duration 10
//!
//! # Fan-out — mitiflow durable, 1 producer 4 consumers
//! cargo run --release -p mitiflow-bench --bin bench_durable_pubsub -- \
//!   --transport mitiflow --rate 1000 --consumers 4 --duration 10
//! ```

use std::time::Duration;

use clap::Parser;
use lightbench::ProducerConsumerBenchmark;
use mitiflow::{EventBusConfig, HeartbeatMode};
#[cfg(feature = "kafka")]
use mitiflow_bench::kafka_topic;
use mitiflow_bench::transport;
use mitiflow_bench::{DurablePubSubCli, DurableTransport, zenoh_config};
#[cfg(feature = "kafka")]
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env().add_directive("lightbench=info".parse().unwrap()),
        )
        .with_ansi(false)
        .init();
    let cli = DurablePubSubCli::parse();
    let topic = cli.topic.clone();
    let payload_size = cli.payload_size;
    let consumers = cli.consumers;

    match cli.transport {
        DurableTransport::Mitiflow => {
            let pub_session = zenoh::open(zenoh_config(cli.zenoh_connect.as_deref()))
                .await
                .expect("failed to open zenoh publisher session");
            let sub_session = zenoh::open(zenoh_config(cli.zenoh_connect.as_deref()))
                .await
                .expect("failed to open zenoh subscriber session");

            let config = EventBusConfig::builder(topic.clone())
                .cache_size(10_000)
                .heartbeat(HeartbeatMode::Disabled)
                .watermark_interval(Duration::from_millis(10))
                .durable_timeout(Duration::from_secs(1))
                .durable_urgency(Duration::from_millis(1))
                .build()
                .expect("failed to build mitiflow config");

            // Spawn EventStore sidecar (same pattern as bench_durable).
            let tmp_dir =
                std::env::temp_dir().join(format!("mitiflow-bench-store-{}", std::process::id()));
            let backend =
                mitiflow::FjallBackend::open(&tmp_dir, 0).expect("failed to open fjall backend");
            let mut store = mitiflow::EventStore::new(&pub_session, backend, config.clone());
            store.run().await.expect("failed to start event store");

            // Give the store time to initialize and emit its first watermark.
            tokio::time::sleep(Duration::from_millis(200)).await;

            let producer = transport::mitiflow::MitiflowDurableProducer {
                session: pub_session.clone(),
                topic: topic.clone(),
                payload_size,
                config: config.clone(),
            };
            let consumer = transport::mitiflow::MitiflowConsumer {
                session: sub_session.clone(),
                topic,
                config,
            };
            run_durable_pubsub(cli.bench, producer, consumer, consumers).await;

            // Graceful shutdown: store first, then sessions.
            store.shutdown_gracefully().await;
            let _ = pub_session.close().await;
            let _ = sub_session.close().await;

            // Cleanup temp dir.
            let _ = std::fs::remove_dir_all(&tmp_dir);
        }

        #[cfg(feature = "kafka")]
        DurableTransport::Kafka | DurableTransport::Redpanda => {
            let broker = cli.kafka_broker.clone();
            let topic = kafka_topic(&topic);
            let group_id = format!("bench-durable-{}", rand::random::<u32>());
            let producer = transport::kafka::KafkaDurableProducer {
                broker: broker.clone(),
                topic: Arc::new(topic.clone()),
                payload_size,
            };
            let consumer = transport::kafka::KafkaConsumer {
                broker,
                topic,
                group_id,
            };
            run_durable_pubsub(cli.bench, producer, consumer, consumers).await;
        }

        #[cfg(feature = "nats")]
        DurableTransport::Nats => {
            let url = cli.nats_url.clone();
            let producer = transport::nats::NatsDurableProducer {
                url: url.clone(),
                topic: topic.clone(),
                payload_size,
            };
            let consumer = transport::nats::NatsJetStreamConsumer { url, topic };
            run_durable_pubsub(cli.bench, producer, consumer, consumers).await;
        }

        #[cfg(feature = "redis")]
        DurableTransport::Redis => {
            let url = cli.redis_url.clone();
            let producer = transport::redis_stream::RedisProducer {
                url: url.clone(),
                stream_key: topic.clone(),
                payload_size,
            };
            let consumer = transport::redis_stream::RedisConsumer {
                url,
                stream_key: topic,
            };
            run_durable_pubsub(cli.bench, producer, consumer, consumers).await;
        }
    }
}

async fn run_durable_pubsub<P, C>(
    config: lightbench::BenchmarkConfig,
    producer: P,
    consumer: C,
    num_consumers: usize,
) where
    P: lightbench::ProducerWork + Send + Sync + Clone + 'static,
    C: lightbench::ConsumerWork + Send + Sync + Clone + 'static,
{
    let mut bench = ProducerConsumerBenchmark::new()
        .producers(config.workers)
        .consumers(num_consumers)
        .duration_secs(config.duration)
        .burst_factor(config.burst_factor)
        .drain_timeout(None)
        .progress(!config.no_progress)
        .rate(0f64)
        .show_ramp_progress(!config.hide_ramp_progress);

    println!(
        "Running durable pub/sub benchmark with config: {:#?}",
        config
    );
    if let Some(rate) = config.rate {
        bench = bench.rate(rate);
    } else if let Some(rate) = config.rate_per_worker {
        bench = bench.rate(rate * config.workers as f64);
    }
    if let Some(ramp) = config.ramp_up {
        bench = bench.ramp_up(std::time::Duration::from_secs(ramp));
        bench = bench.ramp_start_rate(config.ramp_start_rate);
    }
    if let Some(csv_path) = config.csv {
        bench = bench.csv(csv_path);
    }

    let results = bench.producer(producer).consumer(consumer).run().await;
    results.print_summary();
}
