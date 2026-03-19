//! Zenoh-ext AdvancedPublisher/AdvancedSubscriber transport.

use std::sync::Arc;

use lightbench::{ConsumerRecorder, ConsumerWork, ProducerWork, now_unix_ns_estimate};
use zenoh::Session;
use zenoh::qos::CongestionControl;
use zenoh_ext::AdvancedPublisherBuilderExt;
use zenoh_ext::AdvancedSubscriberBuilderExt;

use crate::{build_payload, extract_timestamp};

/// Zenoh advanced publisher.
#[derive(Clone)]
pub struct ZenohAdvancedProducer {
    pub session: Arc<Session>,
    pub topic: String,
    pub payload_size: usize,
}

pub struct ZenohAdvancedProducerState {
    session: Arc<Session>,
    topic: String,
    payload_size: usize,
}

impl ProducerWork for ZenohAdvancedProducer {
    type State = ZenohAdvancedProducerState;

    async fn init(&self) -> Self::State {
        // Declare the advanced publisher on init so the cache queryable is set up.
        // We store session+topic for produce() since AdvancedPublisher has lifetime issues.
        let _publisher = self
            .session
            .declare_publisher(&self.topic)
            .congestion_control(CongestionControl::Block)
            .advanced()
            .cache(zenoh_ext::CacheConfig::default().max_samples(10_000))
            .sample_miss_detection(zenoh_ext::MissDetectionConfig::default())
            .await
            .expect("failed to declare zenoh advanced publisher");
        // Leak to keep alive for the duration of the benchmark.
        std::mem::forget(_publisher);
        ZenohAdvancedProducerState {
            session: self.session.clone(),
            topic: self.topic.clone(),
            payload_size: self.payload_size,
        }
    }

    async fn produce(&self, state: &mut Self::State) -> Result<(), String> {
        let payload = build_payload(state.payload_size, now_unix_ns_estimate());
        state
            .session
            .put(&state.topic, payload)
            .congestion_control(CongestionControl::Block)
            .await
            .map_err(|e| e.to_string())
    }
}

/// Zenoh advanced subscriber.
#[derive(Clone)]
pub struct ZenohAdvancedConsumer {
    pub session: Arc<Session>,
    pub topic: String,
}

use zenoh::handlers::FifoChannelHandler;
use zenoh::sample::Sample;

impl ConsumerWork for ZenohAdvancedConsumer {
    type State = zenoh_ext::AdvancedSubscriber<FifoChannelHandler<Sample>>;

    async fn init(&self) -> Self::State {
        self.session
            .declare_subscriber(&self.topic)
            .advanced()
            .history(zenoh_ext::HistoryConfig::default())
            .recovery(zenoh_ext::RecoveryConfig::default())
            .subscriber_detection()
            .await
            .expect("failed to declare zenoh advanced subscriber")
    }

    async fn run(&self, state: Self::State, recorder: ConsumerRecorder) -> Self::State {
        loop {
            tokio::select! {
                result = state.recv_async() => {
                    match result {
                        Ok(sample) => {
                            let now = now_unix_ns_estimate();
                            let bytes: Vec<u8> = sample.payload().to_bytes().to_vec();
                            let sent_ts = extract_timestamp(&bytes);
                            recorder.record(now.saturating_sub(sent_ts)).await;
                        }
                        Err(_) => break,
                    }
                }
                _ = recorder.stopped() => break,
            }
        }
        state
    }
}
