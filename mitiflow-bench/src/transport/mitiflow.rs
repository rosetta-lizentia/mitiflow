//! Mitiflow EventPublisher/EventSubscriber transport.

use lightbench::{
    BenchmarkWork, ConsumerRecorder, ConsumerWork, ProducerWork, WorkResult, now_unix_ns_estimate,
};
use mitiflow::{EventBusConfig, EventPublisher, EventSubscriber};
use zenoh::Session;

use crate::{build_payload, extract_timestamp};

/// Mitiflow producer.
#[derive(Clone)]
pub struct MitiflowProducer {
    pub session: Session,
    pub topic: String,
    pub payload_size: usize,
    pub config: EventBusConfig,
}

pub struct MitiflowProducerState {
    publisher: EventPublisher,
    payload_size: usize,
}

impl ProducerWork for MitiflowProducer {
    type State = MitiflowProducerState;

    async fn init(&self) -> Self::State {
        let publisher = EventPublisher::new(&self.session, self.config.clone())
            .await
            .expect("failed to create mitiflow publisher");
        MitiflowProducerState {
            publisher,
            payload_size: self.payload_size,
        }
    }

    async fn produce(&self, state: &mut Self::State) -> Result<(), String> {
        let data = build_payload(state.payload_size, now_unix_ns_estimate());
        state
            .publisher
            .publish_bytes(data)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

/// Mitiflow consumer.
#[derive(Clone)]
pub struct MitiflowConsumer {
    pub session: Session,
    pub topic: String,
    pub config: EventBusConfig,
}

impl ConsumerWork for MitiflowConsumer {
    type State = EventSubscriber;

    async fn init(&self) -> Self::State {
        EventSubscriber::new(&self.session, self.config.clone())
            .await
            .expect("failed to create mitiflow subscriber")
    }

    async fn run(&self, state: Self::State, recorder: ConsumerRecorder) -> Self::State {
        loop {
            tokio::select! {
                result = state.recv_raw() => {
                    match result {
                        Ok(raw) => {
                            let now = now_unix_ns_estimate();
                            let sent_ts = extract_timestamp(&raw.payload);
                            recorder.record(now.saturating_sub(sent_ts)).await;
                        }
                        Err(_) => {
                            tokio::task::yield_now().await;
                        }
                    }
                }
                _ = recorder.stopped() => break,
            }
        }
        state
    }
}

/// Mitiflow durable producer (ProducerWork — calls publish_bytes_durable per item).
#[derive(Clone)]
pub struct MitiflowDurableProducer {
    pub session: Session,
    pub topic: String,
    pub payload_size: usize,
    pub config: EventBusConfig,
}

pub struct MitiflowDurableProducerState {
    publisher: EventPublisher,
    payload_size: usize,
}

impl ProducerWork for MitiflowDurableProducer {
    type State = MitiflowDurableProducerState;

    async fn init(&self) -> Self::State {
        let publisher = EventPublisher::new(&self.session, self.config.clone())
            .await
            .expect("failed to create mitiflow durable publisher");
        MitiflowDurableProducerState {
            publisher,
            payload_size: self.payload_size,
        }
    }

    async fn produce(&self, state: &mut Self::State) -> Result<(), String> {
        let data = build_payload(state.payload_size, now_unix_ns_estimate());
        state
            .publisher
            .publish_bytes_durable(data)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

/// Mitiflow durable publish work (request pattern — publish_durable per iteration).
#[derive(Clone)]
pub struct MitiflowDurableWork {
    pub session: Session,
    pub config: EventBusConfig,
    pub topic: String,
    pub payload_size: usize,
}

pub struct MitiflowDurableState {
    publisher: EventPublisher,
    payload_size: usize,
}

impl BenchmarkWork for MitiflowDurableWork {
    type State = MitiflowDurableState;

    async fn init(&self) -> Self::State {
        let publisher = EventPublisher::new(&self.session, self.config.clone())
            .await
            .expect("failed to create mitiflow publisher");

        MitiflowDurableState {
            publisher,
            payload_size: self.payload_size,
        }
    }

    async fn work(&self, state: &mut Self::State) -> WorkResult {
        let start = now_unix_ns_estimate();
        let data = build_payload(state.payload_size, start);
        match state.publisher.publish_bytes_durable(data).await {
            Ok(_) => WorkResult::success(now_unix_ns_estimate() - start),
            Err(e) => WorkResult::error(e.to_string()),
        }
    }
}


