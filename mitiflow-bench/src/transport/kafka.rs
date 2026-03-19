//! Apache Kafka / Redpanda transport via rdkafka.

use std::sync::Arc;
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
