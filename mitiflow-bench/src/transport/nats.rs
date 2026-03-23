//! NATS core pub/sub and JetStream transport.

use async_nats::jetstream;
use lightbench::{
    BenchmarkWork, ConsumerRecorder, ConsumerWork, ProducerWork, WorkResult, now_unix_ns_estimate,
};

use crate::{build_payload, extract_timestamp};

/// NATS core producer.
#[derive(Clone)]
pub struct NatsProducer {
    pub url: String,
    pub topic: String,
    pub payload_size: usize,
}

pub struct NatsProducerState {
    client: async_nats::Client,
    topic: String,
    payload_size: usize,
}

impl ProducerWork for NatsProducer {
    type State = NatsProducerState;

    async fn init(&self) -> Self::State {
        let client = async_nats::connect(&self.url)
            .await
            .expect("failed to connect to NATS");
        NatsProducerState {
            client,
            topic: self.topic.clone(),
            payload_size: self.payload_size,
        }
    }

    async fn produce(&self, state: &mut Self::State) -> Result<(), String> {
        let payload = build_payload(state.payload_size, now_unix_ns_estimate());
        state
            .client
            .publish(state.topic.clone(), payload.into())
            .await
            .map_err(|e| e.to_string())
    }
}

/// NATS core consumer.
#[derive(Clone)]
pub struct NatsConsumer {
    pub url: String,
    pub topic: String,
}

impl ConsumerWork for NatsConsumer {
    type State = async_nats::Subscriber;

    async fn init(&self) -> Self::State {
        let client = async_nats::connect(&self.url)
            .await
            .expect("failed to connect to NATS");
        client
            .subscribe(self.topic.clone())
            .await
            .expect("failed to subscribe to NATS topic")
    }

    async fn run(&self, mut state: Self::State, recorder: ConsumerRecorder) -> Self::State {
        use futures::StreamExt;
        loop {
            tokio::select! {
                item = state.next() => {
                    match item {
                        Some(msg) => {
                            let now = now_unix_ns_estimate();
                            let sent_ts = extract_timestamp(&msg.payload);
                            recorder.record(now.saturating_sub(sent_ts)).await;
                        }
                        None => break,
                    }
                }
                _ = recorder.stopped() => break,
            }
        }
        state
    }
}

/// NATS JetStream durable producer (ProducerWork — publish with ACK per item).
#[derive(Clone)]
pub struct NatsDurableProducer {
    pub url: String,
    pub topic: String,
    pub payload_size: usize,
}

pub struct NatsDurableProducerState {
    js: jetstream::Context,
    topic: String,
    payload_size: usize,
}

impl ProducerWork for NatsDurableProducer {
    type State = NatsDurableProducerState;

    async fn init(&self) -> Self::State {
        let client = async_nats::connect(&self.url)
            .await
            .expect("failed to connect to NATS");
        let js = jetstream::new(client);

        // Ensure the stream exists for this subject.
        let stream_name = self.topic.replace(['.', '/'], "_");
        let _ = js
            .get_or_create_stream(jetstream::stream::Config {
                name: stream_name,
                subjects: vec![self.topic.clone()],
                ..Default::default()
            })
            .await
            .expect("failed to create JetStream stream");

        NatsDurableProducerState {
            js,
            topic: self.topic.clone(),
            payload_size: self.payload_size,
        }
    }

    async fn produce(&self, state: &mut Self::State) -> Result<(), String> {
        let payload = build_payload(state.payload_size, now_unix_ns_estimate());
        match state.js.publish(state.topic.clone(), payload.into()).await {
            Ok(ack_future) => match ack_future.await {
                Ok(_) => Ok(()),
                Err(e) => Err(e.to_string()),
            },
            Err(e) => Err(e.to_string()),
        }
    }
}

/// NATS JetStream consumer (ConsumerWork — ordered push consumer on a JetStream stream).
#[derive(Clone)]
pub struct NatsJetStreamConsumer {
    pub url: String,
    pub topic: String,
}

pub struct NatsJetStreamConsumerState {
    messages: futures::stream::Fuse<async_nats::jetstream::consumer::push::Ordered>,
}

impl ConsumerWork for NatsJetStreamConsumer {
    type State = NatsJetStreamConsumerState;

    async fn init(&self) -> Self::State {
        use async_nats::jetstream::consumer::{DeliverPolicy, push::OrderedConfig};

        let client = async_nats::connect(&self.url)
            .await
            .expect("failed to connect to NATS");
        let js = jetstream::new(client);

        let stream_name = self.topic.replace(['.', '/'], "_");
        let stream = js
            .get_or_create_stream(jetstream::stream::Config {
                name: stream_name,
                subjects: vec![self.topic.clone()],
                ..Default::default()
            })
            .await
            .expect("failed to get JetStream stream");

        let consumer = stream
            .create_consumer(OrderedConfig {
                filter_subject: self.topic.clone(),
                deliver_policy: DeliverPolicy::New,
                ..Default::default()
            })
            .await
            .expect("failed to create JetStream ordered consumer");

        let messages = consumer
            .messages()
            .await
            .expect("failed to get JetStream messages");

        use futures::StreamExt;
        NatsJetStreamConsumerState {
            messages: messages.fuse(),
        }
    }

    async fn run(&self, mut state: Self::State, recorder: ConsumerRecorder) -> Self::State {
        use futures::StreamExt;
        loop {
            tokio::select! {
                item = state.messages.next() => {
                    match item {
                        Some(Ok(msg)) => {
                            let now = now_unix_ns_estimate();
                            let sent_ts = extract_timestamp(&msg.payload);
                            recorder.record(now.saturating_sub(sent_ts)).await;
                        }
                        Some(Err(_)) => continue,
                        None => break,
                    }
                }
                _ = recorder.stopped() => break,
            }
        }
        state
    }
}

/// NATS JetStream durable publish (waits for server ACK).
#[derive(Clone)]
pub struct NatsDurableWork {
    pub url: String,
    pub topic: String,
    pub payload_size: usize,
}

pub struct NatsDurableState {
    js: jetstream::Context,
    topic: String,
    payload_size: usize,
}

impl BenchmarkWork for NatsDurableWork {
    type State = NatsDurableState;

    async fn init(&self) -> Self::State {
        let client = async_nats::connect(&self.url)
            .await
            .expect("failed to connect to NATS");
        let js = jetstream::new(client);

        // Ensure the stream exists for this subject.
        let stream_name = self.topic.replace(['.', '/'], "_");
        let _ = js
            .get_or_create_stream(jetstream::stream::Config {
                name: stream_name,
                subjects: vec![self.topic.clone()],
                ..Default::default()
            })
            .await
            .expect("failed to create JetStream stream");

        NatsDurableState {
            js,
            topic: self.topic.clone(),
            payload_size: self.payload_size,
        }
    }

    async fn work(&self, state: &mut Self::State) -> WorkResult {
        let start = now_unix_ns_estimate();
        let payload = build_payload(state.payload_size, start);
        match state.js.publish(state.topic.clone(), payload.into()).await {
            Ok(ack_future) => match ack_future.await {
                Ok(_) => WorkResult::success(now_unix_ns_estimate() - start),
                Err(e) => WorkResult::error(e.to_string()),
            },
            Err(e) => WorkResult::error(e.to_string()),
        }
    }
}

// ---------------------------------------------------------------------------
// Consumer group consumers
// ---------------------------------------------------------------------------

/// NATS core queue group consumer — messages are distributed among members
/// of the same queue group (no persistence, best-effort).
#[derive(Clone)]
pub struct NatsConsumerGroupConsumer {
    pub url: String,
    pub topic: String,
    pub group: String,
}

impl ConsumerWork for NatsConsumerGroupConsumer {
    type State = async_nats::Subscriber;

    async fn init(&self) -> Self::State {
        let client = async_nats::connect(&self.url)
            .await
            .expect("failed to connect to NATS");
        client
            .queue_subscribe(self.topic.clone(), self.group.clone())
            .await
            .expect("failed to queue-subscribe to NATS topic")
    }

    async fn run(&self, mut state: Self::State, recorder: ConsumerRecorder) -> Self::State {
        use futures::StreamExt;
        loop {
            tokio::select! {
                item = state.next() => {
                    match item {
                        Some(msg) => {
                            let now = now_unix_ns_estimate();
                            let sent_ts = extract_timestamp(&msg.payload);
                            recorder.record(now.saturating_sub(sent_ts)).await;
                        }
                        None => break,
                    }
                }
                _ = recorder.stopped() => break,
            }
        }
        state
    }
}

/// NATS JetStream consumer group consumer — uses a durable pull consumer
/// with shared `deliver_group` so messages are distributed among members.
#[derive(Clone)]
pub struct NatsJetStreamConsumerGroupConsumer {
    pub url: String,
    pub topic: String,
    pub group: String,
}

pub struct NatsJetStreamCGState {
    messages: futures::stream::Fuse<async_nats::jetstream::consumer::pull::Stream>,
}

impl ConsumerWork for NatsJetStreamConsumerGroupConsumer {
    type State = NatsJetStreamCGState;

    async fn init(&self) -> Self::State {
        use async_nats::jetstream::consumer::pull;

        let client = async_nats::connect(&self.url)
            .await
            .expect("failed to connect to NATS");
        let js = jetstream::new(client);

        let stream_name = self.topic.replace(['.', '/'], "_");
        let stream = js
            .get_or_create_stream(jetstream::stream::Config {
                name: stream_name,
                subjects: vec![self.topic.clone()],
                ..Default::default()
            })
            .await
            .expect("failed to get JetStream stream");

        let consumer = stream
            .create_consumer(pull::Config {
                durable_name: Some(self.group.clone()),
                deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::New,
                filter_subject: self.topic.clone(),
                ..Default::default()
            })
            .await
            .expect("failed to create JetStream pull consumer");

        let messages = consumer
            .messages()
            .await
            .expect("failed to get JetStream pull messages");

        use futures::StreamExt;
        NatsJetStreamCGState {
            messages: messages.fuse(),
        }
    }

    async fn run(&self, mut state: Self::State, recorder: ConsumerRecorder) -> Self::State {
        use futures::StreamExt;
        loop {
            tokio::select! {
                item = state.messages.next() => {
                    match item {
                        Some(Ok(msg)) => {
                            let now = now_unix_ns_estimate();
                            let sent_ts = extract_timestamp(&msg.payload);
                            recorder.record(now.saturating_sub(sent_ts)).await;
                        }
                        Some(Err(_)) => continue,
                        None => break,
                    }
                }
                _ = recorder.stopped() => break,
            }
        }
        state
    }
}
