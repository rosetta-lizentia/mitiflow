//! Shared benchmark utilities for mitiflow.

pub mod transport;

use clap::{Parser, ValueEnum};
use lightbench::BenchmarkConfig;

/// Transport backends available for benchmarking.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Transport {
    /// Raw Zenoh pub/sub (best-effort, no sequencing).
    Zenoh,
    /// Zenoh-ext AdvancedPublisher/AdvancedSubscriber with cache + recovery.
    ZenohAdvanced,
    /// Mitiflow event streaming (sequencing, gap detection, recovery).
    Mitiflow,
    /// Apache Kafka via librdkafka.
    #[cfg(feature = "kafka")]
    Kafka,
    /// Redpanda (Kafka-compatible) via librdkafka.
    #[cfg(feature = "kafka")]
    Redpanda,
    /// NATS core pub/sub.
    #[cfg(feature = "nats")]
    Nats,
    /// Redis Streams.
    #[cfg(feature = "redis")]
    Redis,
}

/// CLI arguments for pub/sub benchmarks.
#[derive(Parser)]
#[command(name = "mitiflow-bench")]
pub struct PubSubCli {
    #[command(flatten)]
    pub bench: BenchmarkConfig,

    /// Transport backend to benchmark.
    #[arg(long, value_enum)]
    pub transport: Transport,

    /// Payload size in bytes (first 8 bytes are timestamp).
    #[arg(long, default_value = "256")]
    pub payload_size: usize,

    /// Topic / key expression to use.
    #[arg(long, default_value = "bench/test")]
    pub topic: String,

    /// Number of consumers (for fan-out tests).
    #[arg(long, default_value = "1")]
    pub consumers: usize,

    /// Zenoh router endpoint (omit for peer mode).
    #[arg(long)]
    pub zenoh_connect: Option<String>,

    /// Kafka broker address.
    #[cfg(feature = "kafka")]
    #[arg(long, default_value = "localhost:9092")]
    pub kafka_broker: String,

    /// NATS server URL.
    #[cfg(feature = "nats")]
    #[arg(long, default_value = "nats://localhost:4222")]
    pub nats_url: String,

    /// Redis server URL.
    #[cfg(feature = "redis")]
    #[arg(long, default_value = "redis://localhost:6379")]
    pub redis_url: String,
}

/// CLI arguments for durable publish benchmarks.
#[derive(Parser)]
#[command(name = "mitiflow-bench-durable")]
pub struct DurableCli {
    #[command(flatten)]
    pub bench: BenchmarkConfig,

    /// Transport backend to benchmark.
    #[arg(long, value_enum)]
    pub transport: Transport,

    /// Payload size in bytes.
    #[arg(long, default_value = "256")]
    pub payload_size: usize,

    /// Topic / key expression to use.
    #[arg(long, default_value = "bench/test")]
    pub topic: String,

    /// Zenoh router endpoint (omit for peer mode).
    #[arg(long)]
    pub zenoh_connect: Option<String>,

    /// Kafka broker address.
    #[cfg(feature = "kafka")]
    #[arg(long, default_value = "localhost:9092")]
    pub kafka_broker: String,

    /// NATS server URL.
    #[cfg(feature = "nats")]
    #[arg(long, default_value = "nats://localhost:4222")]
    pub nats_url: String,

    /// Redis server URL.
    #[cfg(feature = "redis")]
    #[arg(long, default_value = "redis://localhost:6379")]
    pub redis_url: String,
}

/// Transport backends that support durable (ACKed) publishing.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum DurableTransport {
    /// Mitiflow durable publish (sequencing + EventStore ACK).
    Mitiflow,
    /// Apache Kafka with acks=all.
    #[cfg(feature = "kafka")]
    Kafka,
    /// Redpanda (Kafka-compatible) with acks=all.
    #[cfg(feature = "kafka")]
    Redpanda,
    /// NATS JetStream (publish with ACK).
    #[cfg(feature = "nats")]
    Nats,
    /// Redis Streams (XADD is inherently durable).
    #[cfg(feature = "redis")]
    Redis,
}

/// Transport backends that support consumer groups.
///
/// Excludes raw Zenoh / ZenohAdvanced (no native consumer group concept).
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ConsumerGroupTransport {
    /// Mitiflow with PartitionManager-based consumer groups.
    Mitiflow,
    /// Apache Kafka consumer groups.
    #[cfg(feature = "kafka")]
    Kafka,
    /// Redpanda (Kafka-compatible) consumer groups.
    #[cfg(feature = "kafka")]
    Redpanda,
    /// NATS queue groups (core) or JetStream consumer groups.
    #[cfg(feature = "nats")]
    Nats,
    /// Redis Streams consumer groups (XREADGROUP).
    #[cfg(feature = "redis")]
    Redis,
}

/// Transport backends that support both durable publishing and consumer groups.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum DurableConsumerGroupTransport {
    /// Mitiflow durable publish + PartitionManager consumer groups.
    Mitiflow,
    /// Apache Kafka acks=all + consumer groups.
    #[cfg(feature = "kafka")]
    Kafka,
    /// Redpanda acks=all + consumer groups.
    #[cfg(feature = "kafka")]
    Redpanda,
    /// NATS JetStream publish with ACK + consumer groups.
    #[cfg(feature = "nats")]
    Nats,
    /// Redis Streams (XADD durable + XREADGROUP consumer groups).
    #[cfg(feature = "redis")]
    Redis,
}

/// CLI arguments for durable pub/sub (end-to-end) benchmarks.
#[derive(Parser)]
#[command(name = "mitiflow-bench-durable-pubsub")]
pub struct DurablePubSubCli {
    #[command(flatten)]
    pub bench: BenchmarkConfig,

    /// Durable transport backend to benchmark.
    #[arg(long, value_enum)]
    pub transport: DurableTransport,

    /// Payload size in bytes (first 8 bytes are timestamp).
    #[arg(long, default_value = "256")]
    pub payload_size: usize,

    /// Topic / key expression to use.
    #[arg(long, default_value = "bench/test")]
    pub topic: String,

    /// Number of consumers (for fan-out tests).
    #[arg(long, default_value = "1")]
    pub consumers: usize,

    /// Zenoh router endpoint (omit for peer mode).
    #[arg(long)]
    pub zenoh_connect: Option<String>,

    /// Kafka broker address.
    #[cfg(feature = "kafka")]
    #[arg(long, default_value = "localhost:9092")]
    pub kafka_broker: String,

    /// NATS server URL.
    #[cfg(feature = "nats")]
    #[arg(long, default_value = "nats://localhost:4222")]
    pub nats_url: String,

    /// Redis server URL.
    #[cfg(feature = "redis")]
    #[arg(long, default_value = "redis://localhost:6379")]
    pub redis_url: String,
}

/// CLI arguments for consumer group benchmarks (best-effort).
#[derive(Parser)]
#[command(name = "mitiflow-bench-consumer-group")]
pub struct ConsumerGroupCli {
    #[command(flatten)]
    pub bench: BenchmarkConfig,

    /// Transport backend to benchmark.
    #[arg(long, value_enum)]
    pub transport: ConsumerGroupTransport,

    /// Payload size in bytes (first 8 bytes are timestamp).
    #[arg(long, default_value = "256")]
    pub payload_size: usize,

    /// Topic / key expression to use.
    #[arg(long, default_value = "bench/cg")]
    pub topic: String,

    /// Number of consumers in the consumer group.
    #[arg(long, default_value = "2")]
    pub consumers: usize,

    /// Number of partitions.
    #[arg(long, default_value = "8")]
    pub partitions: u32,

    /// Zenoh router endpoint (omit for peer mode).
    #[arg(long)]
    pub zenoh_connect: Option<String>,

    /// Kafka broker address.
    #[cfg(feature = "kafka")]
    #[arg(long, default_value = "localhost:9092")]
    pub kafka_broker: String,

    /// NATS server URL.
    #[cfg(feature = "nats")]
    #[arg(long, default_value = "nats://localhost:4222")]
    pub nats_url: String,

    /// Redis server URL.
    #[cfg(feature = "redis")]
    #[arg(long, default_value = "redis://localhost:6379")]
    pub redis_url: String,
}

/// CLI arguments for durable consumer group benchmarks.
#[derive(Parser)]
#[command(name = "mitiflow-bench-durable-consumer-group")]
pub struct DurableConsumerGroupCli {
    #[command(flatten)]
    pub bench: BenchmarkConfig,

    /// Durable transport backend to benchmark.
    #[arg(long, value_enum)]
    pub transport: DurableConsumerGroupTransport,

    /// Payload size in bytes (first 8 bytes are timestamp).
    #[arg(long, default_value = "256")]
    pub payload_size: usize,

    /// Topic / key expression to use.
    #[arg(long, default_value = "bench/dcg")]
    pub topic: String,

    /// Number of consumers in the consumer group.
    #[arg(long, default_value = "2")]
    pub consumers: usize,

    /// Number of partitions.
    #[arg(long, default_value = "8")]
    pub partitions: u32,

    /// Zenoh router endpoint (omit for peer mode).
    #[arg(long)]
    pub zenoh_connect: Option<String>,

    /// Kafka broker address.
    #[cfg(feature = "kafka")]
    #[arg(long, default_value = "localhost:9092")]
    pub kafka_broker: String,

    /// NATS server URL.
    #[cfg(feature = "nats")]
    #[arg(long, default_value = "nats://localhost:4222")]
    pub nats_url: String,

    /// Redis server URL.
    #[cfg(feature = "redis")]
    #[arg(long, default_value = "redis://localhost:6379")]
    pub redis_url: String,
}
///
/// First 8 bytes are a little-endian nanosecond timestamp; rest is zeros.
pub fn build_payload(size: usize, timestamp_ns: u64) -> Vec<u8> {
    let mut buf = vec![0u8; size.max(8)];
    buf[..8].copy_from_slice(&timestamp_ns.to_le_bytes());
    buf
}

/// Extract the nanosecond timestamp from the first 8 bytes of a payload.
pub fn extract_timestamp(payload: &[u8]) -> u64 {
    if payload.len() < 8 {
        return 0;
    }
    u64::from_le_bytes(payload[..8].try_into().unwrap())
}

/// Sanitize a topic for Kafka (replace `/` with `.`).
pub fn kafka_topic(topic: &str) -> String {
    topic.replace('/', ".")
}

/// Build a Zenoh config, optionally connecting to a router.
pub fn zenoh_config(connect: Option<&str>) -> zenoh::Config {
    let mut config = zenoh::Config::default();
    if let Some(endpoint) = connect {
        config
            .insert_json5("connect/endpoints", &format!("[\"{endpoint}\"]"))
            .expect("invalid zenoh connect config");
        config
            .insert_json5("scouting/multicast/enabled", "false")
            .expect("invalid zenoh scouting config");
    }
    config
}
