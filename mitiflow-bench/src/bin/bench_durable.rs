//! Durable publish benchmark — measures confirmed/ACKed publish latency.
//!
//! Usage examples:
//!
//! ```bash
//! # Mitiflow durable publish (needs EventStore sidecar — spawned automatically)
//! cargo run --release -p mitiflow-bench --bin bench_durable -- \
//!   --transport mitiflow --rate 1000 --workers 1 --duration 10
//!
//! # Kafka acks=all
//! cargo run --release -p mitiflow-bench --features kafka --bin bench_durable -- \
//!   --transport kafka --rate 1000 --workers 1 --duration 10
//!
//! # Redpanda acks=all
//! cargo run --release -p mitiflow-bench --features kafka --bin bench_durable -- \
//!   --transport redpanda --kafka-broker localhost:29092 --rate 1000 --duration 10
//!
//! # NATS JetStream (publish with ACK)
//! cargo run --release -p mitiflow-bench --features nats --bin bench_durable -- \
//!   --transport nats --rate 1000 --workers 1 --duration 10
//!
//! # Redis Streams XADD (inherently durable)
//! cargo run --release -p mitiflow-bench --features redis --bin bench_durable -- \
//!   --transport redis --rate 1000 --workers 1 --duration 10
//!
//! # Zenoh fire-and-forget baseline (no durability, measures raw put latency)
//! cargo run --release -p mitiflow-bench --bin bench_durable -- \
//!   --transport zenoh --rate 1000 --workers 1 --duration 10
//! ```

use std::time::Duration;

use clap::Parser;
use lightbench::{Benchmark, BenchmarkWork, WorkResult, now_unix_ns_estimate};
#[cfg(feature = "kafka")]
use mitiflow_bench::kafka_topic;
use mitiflow_bench::transport;
use mitiflow_bench::{DurableCli, Transport, build_payload, zenoh_config};
use zenoh::qos::CongestionControl;

#[tokio::main]
async fn main() {
    lightbench::logging::init_default().ok();
    let cli = DurableCli::parse();
    let topic = cli.topic.clone();
    let payload_size = cli.payload_size;

    match cli.transport {
        Transport::Zenoh | Transport::ZenohAdvanced => {
            let session = zenoh::open(zenoh_config(cli.zenoh_connect.as_deref()))
                .await
                .expect("failed to open zenoh session");
            let work = ZenohPutWork {
                session,
                topic,
                payload_size,
            };
            run_durable(cli.bench, work).await;
        }

        Transport::Mitiflow => {
            let session = zenoh::open(zenoh_config(cli.zenoh_connect.as_deref()))
                .await
                .expect("failed to open zenoh session");

            let config = mitiflow::EventBusConfig::builder(topic.clone())
                .cache_size(10_000)
                .heartbeat(mitiflow::HeartbeatMode::Disabled)
                .watermark_interval(Duration::from_millis(10))
                .durable_timeout(Duration::from_secs(1))
                .durable_urgency(Duration::from_millis(1))
                .build()
                .expect("failed to build mitiflow config");

            let tmp_dir =
                std::env::temp_dir().join(format!("mitiflow-bench-store-{}", std::process::id()));
            let backend =
                mitiflow::FjallBackend::open(&tmp_dir, 0).expect("failed to open fjall backend");
            let mut store = mitiflow::EventStore::new(&session, backend, config.clone());
            store.run().await.expect("failed to start event store");

            // Give the store time to initialize and emit its first watermark.
            tokio::time::sleep(Duration::from_millis(200)).await;

            let work = transport::mitiflow::MitiflowDurableWork {
                session,
                config,
                topic,
                payload_size,
            };
            run_durable(cli.bench, work).await;
        }

        #[cfg(feature = "kafka")]
        Transport::Kafka | Transport::Redpanda => {
            let work = transport::kafka::KafkaDurableWork {
                broker: cli.kafka_broker.clone(),
                topic: std::sync::Arc::new(kafka_topic(&topic)),
                payload_size,
            };
            run_durable(cli.bench, work).await;
        }

        #[cfg(feature = "nats")]
        Transport::Nats => {
            let work = transport::nats::NatsDurableWork {
                url: cli.nats_url.clone(),
                topic,
                payload_size,
            };
            run_durable(cli.bench, work).await;
        }

        #[cfg(feature = "redis")]
        Transport::Redis => {
            let work = transport::redis_stream::RedisDurableWork {
                url: cli.redis_url.clone(),
                stream_key: topic,
                payload_size,
            };
            run_durable(cli.bench, work).await;
        }
    }
}

async fn run_durable<W: BenchmarkWork + Send + Sync + Clone + 'static>(
    config: lightbench::BenchmarkConfig,
    work: W,
) {
    let results = Benchmark::from_config(config).work(work).run().await;
    results.print_summary();
}

/// Simple Zenoh put as a fire-and-forget baseline for durable comparison.
#[derive(Clone)]
struct ZenohPutWork {
    session: zenoh::Session,
    topic: String,
    payload_size: usize,
}

struct ZenohPutState {
    session: zenoh::Session,
    topic: String,
    payload_size: usize,
}

impl BenchmarkWork for ZenohPutWork {
    type State = ZenohPutState;

    async fn init(&self) -> Self::State {
        ZenohPutState {
            session: self.session.clone(),
            topic: self.topic.clone(),
            payload_size: self.payload_size,
        }
    }

    async fn work(&self, state: &mut Self::State) -> WorkResult {
        let start = now_unix_ns_estimate();
        let payload = build_payload(state.payload_size, start);
        match state
            .session
            .put(&state.topic, payload)
            .congestion_control(CongestionControl::Block)
            .await
        {
            Ok(()) => WorkResult::success(now_unix_ns_estimate() - start),
            Err(e) => WorkResult::error(e.to_string()),
        }
    }
}
