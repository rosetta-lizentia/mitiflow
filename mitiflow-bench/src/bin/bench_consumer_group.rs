//! Consumer group benchmark — measures partitioned pub/sub with consumer group
//! semantics across transport backends.
//!
//! Each consumer in the group receives a disjoint subset of messages (work
//! distribution), unlike the fan-out benchmarks where every consumer gets
//! every message.
//!
//! Usage examples:
//!
//! ```bash
//! # Mitiflow consumer group — 8 partitions, 2 consumers
//! cargo run --release -p mitiflow-bench --bin bench_consumer_group -- \
//!   --transport mitiflow --partitions 8 --consumers 2 --rate 10000 --duration 10
//!
//! # Kafka consumer group — 4 partitions, 2 consumers
//! cargo run --release -p mitiflow-bench --features kafka --bin bench_consumer_group -- \
//!   --transport kafka --partitions 4 --consumers 2 --rate 10000 --duration 10
//!
//! # Redpanda consumer group
//! cargo run --release -p mitiflow-bench --features kafka --bin bench_consumer_group -- \
//!   --transport redpanda --kafka-broker localhost:29092 --partitions 4 --consumers 2 --duration 10
//!
//! # NATS queue group (best-effort)
//! cargo run --release -p mitiflow-bench --features nats --bin bench_consumer_group -- \
//!   --transport nats --consumers 2 --rate 10000 --duration 10
//!
//! # Redis Streams consumer group (XREADGROUP)
//! cargo run --release -p mitiflow-bench --features redis --bin bench_consumer_group -- \
//!   --transport redis --consumers 2 --rate 10000 --duration 10
//! ```

use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use clap::Parser;
use lightbench::ProducerConsumerBenchmark;
use mitiflow::{EventBusConfig, HeartbeatMode};
#[cfg(feature = "kafka")]
use mitiflow_bench::kafka_topic;
use mitiflow_bench::transport;
use mitiflow_bench::{ConsumerGroupCli, ConsumerGroupTransport, zenoh_config};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env().add_directive("lightbench=info".parse().unwrap()),
        )
        .with_ansi(false)
        .init();
    let cli = ConsumerGroupCli::parse();
    let topic = cli.topic.clone();
    let payload_size = cli.payload_size;
    let consumers = cli.consumers;
    let partitions = cli.partitions;

    match cli.transport {
        ConsumerGroupTransport::Mitiflow => {
            let pub_session = zenoh::open(zenoh_config(cli.zenoh_connect.as_deref()))
                .await
                .expect("failed to open zenoh publisher session");
            let sub_session = zenoh::open(zenoh_config(cli.zenoh_connect.as_deref()))
                .await
                .expect("failed to open zenoh subscriber session");

            let config = EventBusConfig::builder(topic.clone())
                .cache_size(0)
                .heartbeat(HeartbeatMode::Disabled)
                .num_partitions(partitions)
                .build()
                .expect("failed to build mitiflow config");

            let producer = transport::mitiflow::MitiflowPartitionedProducer {
                session: pub_session,
                topic: topic.clone(),
                payload_size,
                num_partitions: partitions,
                config: config.clone(),
            };
            let consumer = transport::mitiflow::MitiflowConsumerGroupConsumer {
                session: sub_session,
                topic,
                num_partitions: partitions,
                config,
                worker_counter: Arc::new(AtomicU32::new(0)),
            };
            run_consumer_group(cli.bench, producer, consumer, consumers).await;
        }

        #[cfg(feature = "kafka")]
        ConsumerGroupTransport::Kafka | ConsumerGroupTransport::Redpanda => {
            let broker = cli.kafka_broker.clone();
            let topic = kafka_topic(&topic);
            let group_id = format!("bench-cg-{}", rand::random::<u32>());
            let producer = transport::kafka::KafkaPartitionedProducer {
                broker: broker.clone(),
                topic: Arc::new(topic.clone()),
                payload_size,
                num_partitions: partitions,
            };
            let consumer = transport::kafka::KafkaConsumerGroupConsumer {
                broker,
                topic,
                group_id,
                num_partitions: partitions,
                consumer_counter: Arc::new(AtomicU32::new(0)),
            };
            run_consumer_group(cli.bench, producer, consumer, consumers).await;
        }

        #[cfg(feature = "nats")]
        ConsumerGroupTransport::Nats => {
            let url = cli.nats_url.clone();
            let group = format!("bench-cg-{}", rand::random::<u32>());
            let producer = transport::nats::NatsProducer {
                url: url.clone(),
                topic: topic.clone(),
                payload_size,
            };
            let consumer = transport::nats::NatsConsumerGroupConsumer { url, topic, group };
            run_consumer_group(cli.bench, producer, consumer, consumers).await;
        }

        #[cfg(feature = "redis")]
        ConsumerGroupTransport::Redis => {
            let url = cli.redis_url.clone();
            let group = format!("bench-cg-{}", rand::random::<u32>());
            let producer = transport::redis_stream::RedisProducer {
                url: url.clone(),
                stream_key: topic.clone(),
                payload_size,
            };
            let consumer = transport::redis_stream::RedisConsumerGroupConsumer {
                url,
                stream_key: topic,
                group_name: group,
                consumer_counter: Arc::new(AtomicU32::new(0)),
            };
            run_consumer_group(cli.bench, producer, consumer, consumers).await;
        }
    }
}

async fn run_consumer_group<P, C>(
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
        .drain_timeout(Some(std::time::Duration::from_secs(3)))
        .progress(!config.no_progress)
        .rate(0f64)
        .show_ramp_progress(!config.hide_ramp_progress);

    println!(
        "Running consumer group benchmark with config: {:#?}",
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
