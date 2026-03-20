//! Apache Kafka / Redpanda transport via rdkafka.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use lightbench::{
    BenchmarkWork, ConsumerRecorder, ConsumerWork, ProducerWork, WorkResult, now_unix_ns_estimate,
};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{ClientConfig, Message};
use tokio_stream::StreamExt;

use crate::{build_payload, extract_timestamp};

/// Kafka/Redpanda producer.
#[derive(Clone)]
pub struct KafkaProducer {
    pub broker: String,
    pub topic: Arc<String>,
    pub payload_size: usize,
}

pub struct KafkaProducerState {
    producer: FutureProducer,
    topic: Arc<String>,
    payload_size: usize,
}

impl ProducerWork for KafkaProducer {
    type State = KafkaProducerState;

    async fn init(&self) -> Self::State {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &self.broker)
            .set("message.timeout.ms", "5000")
            .set("linger.ms", "0")
            .create()
            .expect("failed to create kafka producer");
        KafkaProducerState {
            producer,
            topic: Arc::clone(&self.topic),
            payload_size: self.payload_size,
        }
    }

    async fn produce(&self, state: &mut Self::State) -> Result<(), String> {
        let payload = build_payload(state.payload_size, now_unix_ns_estimate());
        let record: FutureRecord<'_, str, Vec<u8>> =
            FutureRecord::to(&state.topic).payload(&payload);
        state
            .producer
            .send(record, Duration::from_secs(5))
            .await
            .map(|_| ())
            .map_err(|(e, _)| e.to_string())
    }
}

/// Kafka/Redpanda consumer.
#[derive(Clone)]
pub struct KafkaConsumer {
    pub broker: String,
    pub topic: String,
    pub group_id: String,
}

impl ConsumerWork for KafkaConsumer {
    type State = Arc<StreamConsumer>;

    async fn init(&self) -> Self::State {
        // Ensure the topic exists before subscribing — Kafka consumers don't auto-create topics.
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", &self.broker)
            .create()
            .expect("failed to create kafka admin client");
        let new_topic = NewTopic::new(&self.topic, 1, TopicReplication::Fixed(1));
        let _ = admin
            .create_topics(&[new_topic], &AdminOptions::new())
            .await; // ignore error — topic may already exist

        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.broker)
            .set("group.id", &self.group_id)
            .set("auto.offset.reset", "latest")
            .set("enable.auto.commit", "true")
            .set("fetch.min.bytes", "1")
            .set("fetch.wait.max.ms", "100")
            .set("topic.metadata.refresh.interval.ms", "500")
            .create()
            .expect("failed to create kafka consumer");
        consumer
            .subscribe(&[&self.topic])
            .expect("failed to subscribe to kafka topic");
        let consumer = Arc::new(consumer);
        // StreamConsumer runs a background event thread that drives the group
        // join protocol. Wait here until partition assignment is complete so that
        // run() starts ready to receive messages produced by the benchmark producers.
        let _ = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            loop {
                match consumer.assignment() {
                    Ok(tpl) if tpl.count() > 0 => break,
                    _ => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
                }
            }
        })
        .await;
        consumer
    }

    async fn run(&self, state: Self::State, recorder: ConsumerRecorder) -> Self::State {
        {
            let mut stream = state.stream();
            loop {
                tokio::select! {
                    item = stream.next() => {
                        match item {
                            Some(Ok(msg)) => {
                                if let Some(payload) = msg.payload() {
                                    let now = now_unix_ns_estimate();
                                    let sent_ts = extract_timestamp(payload);
                                    recorder.record(now.saturating_sub(sent_ts)).await;
                                }
                            }
                            Some(Err(_)) => continue,
                            None => break,
                        }
                    }
                    _ = recorder.stopped() => break,
                }
            }
        }
        state
    }
}

/// Kafka/Redpanda durable producer (ProducerWork — acks=all per item).
#[derive(Clone)]
pub struct KafkaDurableProducer {
    pub broker: String,
    pub topic: Arc<String>,
    pub payload_size: usize,
}

pub struct KafkaDurableProducerState {
    producer: FutureProducer,
    topic: Arc<String>,
    payload_size: usize,
}

impl ProducerWork for KafkaDurableProducer {
    type State = KafkaDurableProducerState;

    async fn init(&self) -> Self::State {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &self.broker)
            .set("message.timeout.ms", "10000")
            .set("acks", "all")
            .set("linger.ms", "0")
            .create()
            .expect("failed to create kafka durable producer");
        KafkaDurableProducerState {
            producer,
            topic: Arc::clone(&self.topic),
            payload_size: self.payload_size,
        }
    }

    async fn produce(&self, state: &mut Self::State) -> Result<(), String> {
        let payload = build_payload(state.payload_size, now_unix_ns_estimate());
        let record: FutureRecord<'_, str, Vec<u8>> =
            FutureRecord::to(&state.topic).payload(&payload);
        state
            .producer
            .send(record, Duration::from_secs(10))
            .await
            .map(|_| ())
            .map_err(|(e, _)| e.to_string())
    }
}

/// Kafka durable publish (acks=all).
#[derive(Clone)]
pub struct KafkaDurableWork {
    pub broker: String,
    pub topic: Arc<String>,
    pub payload_size: usize,
}

pub struct KafkaDurableState {
    producer: FutureProducer,
    topic: Arc<String>,
    payload_size: usize,
}

impl BenchmarkWork for KafkaDurableWork {
    type State = KafkaDurableState;

    async fn init(&self) -> Self::State {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &self.broker)
            .set("message.timeout.ms", "10000")
            .set("acks", "all")
            .set("linger.ms", "0")
            .create()
            .expect("failed to create kafka durable producer");
        KafkaDurableState {
            producer,
            topic: Arc::clone(&self.topic),
            payload_size: self.payload_size,
        }
    }

    async fn work(&self, state: &mut Self::State) -> WorkResult {
        let start = now_unix_ns_estimate();
        let payload = build_payload(state.payload_size, start);
        let record: FutureRecord<'_, str, Vec<u8>> =
            FutureRecord::to(&state.topic).payload(&payload);
        match state.producer.send(record, Duration::from_secs(10)).await {
            Ok(_) => WorkResult::success(now_unix_ns_estimate() - start),
            Err((e, _)) => WorkResult::error(e.to_string()),
        }
    }
}

// ---------------------------------------------------------------------------
// Partitioned producer (creates topic with N partitions, round-robin keys)
// ---------------------------------------------------------------------------

/// Ensure a Kafka topic exists with the desired partition count.
/// If it already exists, this is a no-op (partition count won't change).
async fn ensure_topic(broker: &str, topic: &str, num_partitions: i32) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", broker)
        .create()
        .expect("failed to create kafka admin client");
    let new_topic = NewTopic::new(topic, num_partitions, TopicReplication::Fixed(1));
    let _ = admin
        .create_topics(&[new_topic], &AdminOptions::new())
        .await;
}

/// Kafka/Redpanda partitioned producer — creates topic with N partitions
/// and distributes messages via round-robin keys.
#[derive(Clone)]
pub struct KafkaPartitionedProducer {
    pub broker: String,
    pub topic: Arc<String>,
    pub payload_size: usize,
    pub num_partitions: u32,
}

pub struct KafkaPartitionedProducerState {
    producer: FutureProducer,
    topic: Arc<String>,
    payload_size: usize,
    num_partitions: u32,
    counter: u32,
}

impl ProducerWork for KafkaPartitionedProducer {
    type State = KafkaPartitionedProducerState;

    async fn init(&self) -> Self::State {
        ensure_topic(&self.broker, &self.topic, self.num_partitions as i32).await;
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &self.broker)
            .set("message.timeout.ms", "5000")
            .set("linger.ms", "0")
            .create()
            .expect("failed to create kafka partitioned producer");
        KafkaPartitionedProducerState {
            producer,
            topic: Arc::clone(&self.topic),
            payload_size: self.payload_size,
            num_partitions: self.num_partitions,
            counter: 0,
        }
    }

    async fn produce(&self, state: &mut Self::State) -> Result<(), String> {
        let key = format!("k{}", state.counter % state.num_partitions);
        state.counter = state.counter.wrapping_add(1);
        let payload = build_payload(state.payload_size, now_unix_ns_estimate());
        let record: FutureRecord<'_, String, Vec<u8>> = FutureRecord::to(&state.topic)
            .payload(&payload)
            .key(&key);
        state
            .producer
            .send(record, Duration::from_secs(5))
            .await
            .map(|_| ())
            .map_err(|(e, _)| e.to_string())
    }
}

/// Kafka/Redpanda durable partitioned producer — acks=all + N partitions.
#[derive(Clone)]
pub struct KafkaDurablePartitionedProducer {
    pub broker: String,
    pub topic: Arc<String>,
    pub payload_size: usize,
    pub num_partitions: u32,
}

pub struct KafkaDurablePartitionedProducerState {
    producer: FutureProducer,
    topic: Arc<String>,
    payload_size: usize,
    num_partitions: u32,
    counter: u32,
}

impl ProducerWork for KafkaDurablePartitionedProducer {
    type State = KafkaDurablePartitionedProducerState;

    async fn init(&self) -> Self::State {
        ensure_topic(&self.broker, &self.topic, self.num_partitions as i32).await;
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &self.broker)
            .set("message.timeout.ms", "10000")
            .set("acks", "all")
            .set("linger.ms", "0")
            .create()
            .expect("failed to create kafka durable partitioned producer");
        KafkaDurablePartitionedProducerState {
            producer,
            topic: Arc::clone(&self.topic),
            payload_size: self.payload_size,
            num_partitions: self.num_partitions,
            counter: 0,
        }
    }

    async fn produce(&self, state: &mut Self::State) -> Result<(), String> {
        let key = format!("k{}", state.counter % state.num_partitions);
        state.counter = state.counter.wrapping_add(1);
        let payload = build_payload(state.payload_size, now_unix_ns_estimate());
        let record: FutureRecord<'_, String, Vec<u8>> = FutureRecord::to(&state.topic)
            .payload(&payload)
            .key(&key);
        state
            .producer
            .send(record, Duration::from_secs(10))
            .await
            .map(|_| ())
            .map_err(|(e, _)| e.to_string())
    }
}

// ---------------------------------------------------------------------------
// Consumer group consumer (same group_id, Kafka assigns partitions)
// ---------------------------------------------------------------------------

/// Kafka/Redpanda consumer group consumer — all instances share the same
/// `group_id` so Kafka distributes partitions among them.
#[derive(Clone)]
pub struct KafkaConsumerGroupConsumer {
    pub broker: String,
    pub topic: String,
    pub group_id: String,
    pub num_partitions: u32,
    /// Shared counter for unique consumer IDs within the group.
    pub consumer_counter: Arc<AtomicU32>,
}

impl ConsumerWork for KafkaConsumerGroupConsumer {
    type State = Arc<StreamConsumer>;

    async fn init(&self) -> Self::State {
        ensure_topic(&self.broker, &self.topic, self.num_partitions as i32).await;

        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.broker)
            .set("group.id", &self.group_id)
            .set("auto.offset.reset", "latest")
            .set("enable.auto.commit", "true")
            .set("fetch.min.bytes", "1")
            .set("fetch.wait.max.ms", "100")
            .set("topic.metadata.refresh.interval.ms", "500")
            .set(
                "client.id",
                format!(
                    "bench-cg-{}",
                    self.consumer_counter.fetch_add(1, Ordering::Relaxed)
                ),
            )
            .create()
            .expect("failed to create kafka consumer group consumer");
        consumer
            .subscribe(&[&self.topic])
            .expect("failed to subscribe to kafka topic");
        let consumer = Arc::new(consumer);
        // Wait for partition assignment.
        let _ = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            loop {
                match consumer.assignment() {
                    Ok(tpl) if tpl.count() > 0 => break,
                    _ => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
                }
            }
        })
        .await;
        consumer
    }

    async fn run(&self, state: Self::State, recorder: ConsumerRecorder) -> Self::State {
        {
            let mut stream = state.stream();
            loop {
                tokio::select! {
                    item = stream.next() => {
                        match item {
                            Some(Ok(msg)) => {
                                if let Some(payload) = msg.payload() {
                                    let now = now_unix_ns_estimate();
                                    let sent_ts = extract_timestamp(payload);
                                    recorder.record(now.saturating_sub(sent_ts)).await;
                                }
                            }
                            Some(Err(_)) => continue,
                            None => break,
                        }
                    }
                    _ = recorder.stopped() => break,
                }
            }
        }
        state
    }
}
