//! Mitiflow EventPublisher/EventSubscriber transport.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

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

    async fn cleanup(&self, state: Self::State) {
        state.publisher.shutdown().await;
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

    async fn cleanup(&self, state: Self::State) {
        state.shutdown().await;
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

    async fn cleanup(&self, state: Self::State) {
        state.publisher.shutdown().await;
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

    async fn cleanup(&self, state: Self::State) {
        state.publisher.shutdown().await;
    }
}

// ---------------------------------------------------------------------------
// Partitioned producer (round-robin across N partitions)
// ---------------------------------------------------------------------------

/// Mitiflow partitioned producer — distributes messages across N partitions.
#[derive(Clone)]
pub struct MitiflowPartitionedProducer {
    pub session: Session,
    pub topic: String,
    pub payload_size: usize,
    pub num_partitions: u32,
    pub config: EventBusConfig,
}

pub struct MitiflowPartitionedProducerState {
    publisher: EventPublisher,
    topic: String,
    payload_size: usize,
    num_partitions: u32,
    counter: u32,
}

impl ProducerWork for MitiflowPartitionedProducer {
    type State = MitiflowPartitionedProducerState;

    async fn init(&self) -> Self::State {
        let publisher = EventPublisher::new(&self.session, self.config.clone())
            .await
            .expect("failed to create mitiflow partitioned publisher");
        MitiflowPartitionedProducerState {
            publisher,
            topic: self.topic.clone(),
            payload_size: self.payload_size,
            num_partitions: self.num_partitions,
            counter: 0,
        }
    }

    async fn produce(&self, state: &mut Self::State) -> Result<(), String> {
        let partition = state.counter % state.num_partitions;
        state.counter = state.counter.wrapping_add(1);
        let data = build_payload(state.payload_size, now_unix_ns_estimate());
        let key = format!("{}/p/{}/data", state.topic, partition);
        state
            .publisher
            .publish_bytes_to(&key, data)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    async fn cleanup(&self, state: Self::State) {
        state.publisher.shutdown().await;
    }
}

/// Mitiflow durable partitioned producer — distributes durable publishes across N partitions.
#[derive(Clone)]
pub struct MitiflowDurablePartitionedProducer {
    pub session: Session,
    pub topic: String,
    pub payload_size: usize,
    pub num_partitions: u32,
    pub config: EventBusConfig,
}

pub struct MitiflowDurablePartitionedProducerState {
    publisher: EventPublisher,
    topic: String,
    payload_size: usize,
    num_partitions: u32,
    counter: u32,
}

impl ProducerWork for MitiflowDurablePartitionedProducer {
    type State = MitiflowDurablePartitionedProducerState;

    async fn init(&self) -> Self::State {
        let publisher = EventPublisher::new(&self.session, self.config.clone())
            .await
            .expect("failed to create mitiflow durable partitioned publisher");
        MitiflowDurablePartitionedProducerState {
            publisher,
            topic: self.topic.clone(),
            payload_size: self.payload_size,
            num_partitions: self.num_partitions,
            counter: 0,
        }
    }

    async fn produce(&self, state: &mut Self::State) -> Result<(), String> {
        let partition = state.counter % state.num_partitions;
        state.counter = state.counter.wrapping_add(1);
        let data = build_payload(state.payload_size, now_unix_ns_estimate());
        let key = format!("{}/p/{}/data", state.topic, partition);
        state
            .publisher
            .publish_bytes_durable_to(&key, data)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    async fn cleanup(&self, state: Self::State) {
        state.publisher.shutdown().await;
    }
}

// ---------------------------------------------------------------------------
// Consumer group consumer (PartitionManager-assigned partitions)
// ---------------------------------------------------------------------------

/// Mitiflow consumer group consumer — each instance gets unique partitions
/// via PartitionManager (rendezvous hashing + liveliness-driven rebalancing).
#[derive(Clone)]
pub struct MitiflowConsumerGroupConsumer {
    pub session: Session,
    pub topic: String,
    pub num_partitions: u32,
    pub config: EventBusConfig,
    /// Shared counter for assigning unique worker IDs across clones.
    pub worker_counter: Arc<AtomicU32>,
}

pub struct MitiflowConsumerGroupConsumerState {
    subscriber: EventSubscriber,
    _partition_manager: mitiflow::PartitionManager,
}

impl ConsumerWork for MitiflowConsumerGroupConsumer {
    type State = MitiflowConsumerGroupConsumerState;

    async fn init(&self) -> Self::State {
        let worker_idx = self.worker_counter.fetch_add(1, Ordering::Relaxed);
        let worker_id = format!("bench-worker-{}", worker_idx);

        let pm_config = EventBusConfig::builder(self.topic.clone())
            .cache_size(0)
            .heartbeat(mitiflow::HeartbeatMode::Disabled)
            .num_partitions(self.num_partitions)
            .worker_id(&worker_id)
            .build()
            .expect("failed to build PartitionManager config");

        let pm = mitiflow::PartitionManager::new(&self.session, pm_config)
            .await
            .expect("failed to create PartitionManager");

        // Give time for membership discovery and partition assignment.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let my_partitions = pm.my_partitions().await;
        tracing::info!(
            worker = %worker_id,
            partitions = ?my_partitions,
            "consumer group partition assignment"
        );

        let subscriber =
            EventSubscriber::new_partitioned(&self.session, self.config.clone(), &my_partitions)
                .await
                .expect("failed to create partitioned subscriber");

        MitiflowConsumerGroupConsumerState {
            subscriber,
            _partition_manager: pm,
        }
    }

    async fn run(&self, state: Self::State, recorder: ConsumerRecorder) -> Self::State {
        loop {
            tokio::select! {
                result = state.subscriber.recv_raw() => {
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

    async fn cleanup(&self, state: Self::State) {
        state.subscriber.shutdown().await;
    }
}
