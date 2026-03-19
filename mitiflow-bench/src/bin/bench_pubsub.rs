//! Pub/sub benchmark comparing transport backends.
//!
//! Usage examples:
//!
//! ```bash
//! # Throughput — raw zenoh peer mode (unlimited rate)
//! cargo run --release -p mitiflow-bench --bin bench_pubsub -- \
//!   --transport zenoh --duration 10
//!
//! # Latency at load — mitiflow, 1KB payloads
//! cargo run --release -p mitiflow-bench --bin bench_pubsub -- \
//!   --transport mitiflow --rate 50000 --duration 10 --payload-size 1024
//!
//! # Fan-out — zenoh advanced, 1 producer 4 consumers
//! cargo run --release -p mitiflow-bench --bin bench_pubsub -- \
//!   --transport zenoh-advanced --rate 10000 --consumers 4 --duration 10
//!
//! # Kafka
//! cargo run --release -p mitiflow-bench --features kafka --bin bench_pubsub -- \
//!   --transport kafka --duration 10
//!
//! # Redpanda
//! cargo run --release -p mitiflow-bench --features kafka --bin bench_pubsub -- \
//!   --transport redpanda --kafka-broker localhost:29092 --duration 10
//!
//! # NATS
//! cargo run --release -p mitiflow-bench --features nats --bin bench_pubsub -- \
//!   --transport nats --duration 10
//!
//! # Redis Streams
//! cargo run --release -p mitiflow-bench --features redis --bin bench_pubsub -- \
//!   --transport redis --duration 10
//! ```

use clap::Parser;
use lightbench::ProducerConsumerBenchmark;
use mitiflow::{EventBusConfig, HeartbeatMode};
#[cfg(feature = "kafka")]
use mitiflow_bench::kafka_topic;
use mitiflow_bench::transport;
use mitiflow_bench::{PubSubCli, Transport, zenoh_config};

/// Open a pair of Zenoh sessions for producer and consumer roles.
async fn open_zenoh_sessions(connect: Option<&str>) -> (zenoh::Session, zenoh::Session) {
    let pub_session = zenoh::open(zenoh_config(connect))
        .await
        .expect("failed to open zenoh publisher session");
    let sub_session = zenoh::open(zenoh_config(connect))
        .await
        .expect("failed to open zenoh subscriber session");
    (pub_session, sub_session)
}

#[tokio::main]
async fn main() {
    lightbench::logging::init_default().ok();
    let cli = PubSubCli::parse();
    let topic = cli.topic.clone();
    let payload_size = cli.payload_size;
    let consumers = cli.consumers;

    match cli.transport {
        Transport::Zenoh => {
            let (pub_s, sub_s) = open_zenoh_sessions(cli.zenoh_connect.as_deref()).await;
            let producer = transport::zenoh_raw::ZenohProducer {
                session: pub_s,
                topic: topic.clone(),
                payload_size,
            };
            let consumer = transport::zenoh_raw::ZenohConsumer {
                session: sub_s,
                topic,
            };
            run_pubsub(cli.bench, producer, consumer, consumers).await;
        }

        Transport::ZenohAdvanced => {
            let (pub_s, sub_s) = open_zenoh_sessions(cli.zenoh_connect.as_deref()).await;
            let producer = transport::zenoh_advanced::ZenohAdvancedProducer {
                session: pub_s,
                topic: topic.clone(),
                payload_size,
            };
            let consumer = transport::zenoh_advanced::ZenohAdvancedConsumer {
                session: sub_s,
                topic,
            };
            run_pubsub(cli.bench, producer, consumer, consumers).await;
        }

        Transport::Mitiflow => {
            let (pub_s, sub_s) = open_zenoh_sessions(cli.zenoh_connect.as_deref()).await;
            let config = EventBusConfig::builder(topic.clone())
                .cache_size(0)
                .heartbeat(HeartbeatMode::Disabled)
                .build()
                .expect("failed to build mitiflow config");
            let producer = transport::mitiflow_transport::MitiflowProducer {
                session: pub_s,
                topic: topic.clone(),
                payload_size,
                config: config.clone(),
            };
            let consumer = transport::mitiflow_transport::MitiflowConsumer {
                session: sub_s,
                topic,
                config,
            };
            run_pubsub(cli.bench, producer, consumer, consumers).await;
        }

        #[cfg(feature = "kafka")]
        Transport::Kafka | Transport::Redpanda => {
            let broker = cli.kafka_broker.clone();
            run_kafka(cli.bench, broker, topic, payload_size, consumers).await;
        }

        #[cfg(feature = "nats")]
        Transport::Nats => {
            let url = cli.nats_url.clone();
            let producer = transport::nats::NatsProducer {
                url: url.clone(),
                topic: topic.clone(),
                payload_size,
            };
            let consumer = transport::nats::NatsConsumer { url, topic };
            run_pubsub(cli.bench, producer, consumer, consumers).await;
        }

        #[cfg(feature = "redis")]
        Transport::Redis => {
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
            run_pubsub(cli.bench, producer, consumer, consumers).await;
        }
    }
}

async fn run_pubsub<P, C>(
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
        .rate(0f64) // Start with unlimited rate, override below if specified
        .show_ramp_progress(!config.hide_ramp_progress);

    println!("Running benchmark with config: {:#?}", config);
    if let Some(rate) = config.rate {
        bench = bench.rate(rate);
    } else if let Some(rate) = config.rate_per_worker {
        // Approximate: multiply per-worker rate by number of producers
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

#[cfg(feature = "kafka")]
async fn run_kafka(
    config: lightbench::BenchmarkConfig,
    broker: String,
    topic: String,
    payload_size: usize,
    consumers: usize,
) {
    let topic = kafka_topic(&topic);
    let group_id = format!("bench-{}", rand::random::<u32>());
    let producer = transport::kafka::KafkaProducer {
        broker: broker.clone(),
        topic: std::sync::Arc::new(topic.clone()),
        payload_size,
    };
    let consumer = transport::kafka::KafkaConsumer {
        broker,
        topic,
        group_id,
    };
    run_pubsub(config, producer, consumer, consumers).await;
}
