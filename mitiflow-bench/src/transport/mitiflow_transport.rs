//! Mitiflow EventPublisher/EventSubscriber transport.

use std::sync::Arc;
use std::time::Duration;

use lightbench::{
    BenchmarkWork, ConsumerRecorder, ConsumerWork, ProducerWork, WorkResult, now_unix_ns_estimate,
};
use mitiflow::{Event, EventBusConfig, EventPublisher, EventSubscriber, HeartbeatMode};
use serde::{Deserialize, Serialize};
use zenoh::Session;

use crate::{build_payload, extract_timestamp};

/// Wrapper payload that carries raw bytes through mitiflow's serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchPayload {
    pub data: Vec<u8>,
}

/// Mitiflow producer.
#[derive(Clone)]
pub struct MitiflowProducer {
    pub session: Arc<Session>,
    pub topic: String,
    pub payload_size: usize,
}

pub struct MitiflowProducerState {
    publisher: EventPublisher,
    payload_size: usize,
}

impl ProducerWork for MitiflowProducer {
    type State = MitiflowProducerState;

    async fn init(&self) -> Self::State {
        let config = EventBusConfig::builder(&self.topic)
            .cache_size(10_000)
            .heartbeat(HeartbeatMode::Disabled)
            .build()
            .expect("failed to build mitiflow config");
        let publisher = EventPublisher::new(&self.session, config)
            .await
            .expect("failed to create mitiflow publisher");
        MitiflowProducerState {
            publisher,
            payload_size: self.payload_size,
        }
    }

    async fn produce(&self, state: &mut Self::State) -> Result<(), String> {
        let data = build_payload(state.payload_size, now_unix_ns_estimate());
        let event = Event::new(BenchPayload { data });
        state
            .publisher
            .publish(&event)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

/// Mitiflow consumer.
#[derive(Clone)]
pub struct MitiflowConsumer {
    pub session: Arc<Session>,
    pub topic: String,
}

impl ConsumerWork for MitiflowConsumer {
    type State = EventSubscriber;

    async fn init(&self) -> Self::State {
        let config = EventBusConfig::builder(&self.topic)
            .cache_size(10_000)
            .heartbeat(HeartbeatMode::Disabled)
            .build()
            .expect("failed to build mitiflow config");
        EventSubscriber::new(&self.session, config)
            .await
            .expect("failed to create mitiflow subscriber")
    }

    async fn run(&self, state: Self::State, recorder: ConsumerRecorder) -> Self::State {
        loop {
            tokio::select! {
                result = state.recv::<BenchPayload>() => {
                    match result {
                        Ok(event) => {
                            let now = now_unix_ns_estimate();
                            let sent_ts = extract_timestamp(&event.payload.data);
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

        // Start the EventStore sidecar and keep it alive in the state so its
        // Drop impl (which cancels background tasks) is not triggered early.
        

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
        let event = Event::new(BenchPayload { data });
        match state.publisher.publish_durable(&event).await {
            Ok(_) => WorkResult::success(now_unix_ns_estimate() - start),
            Err(e) => WorkResult::error(e.to_string()),
        }
    }
}


