//! Raw Zenoh pub/sub transport (best-effort, no sequencing).

use lightbench::{ConsumerRecorder, ConsumerWork, ProducerWork, now_unix_ns_estimate};
use zenoh::Session;
use zenoh::handlers::FifoChannelHandler;
use zenoh::pubsub::Subscriber;
use zenoh::qos::CongestionControl;
use zenoh::sample::Sample;

use crate::{build_payload, extract_timestamp};

/// Raw Zenoh producer.
#[derive(Clone)]
pub struct ZenohProducer {
    pub session: Session,
    pub topic: String,
    pub payload_size: usize,
}

pub struct ZenohProducerState {
    session: Session,
    topic: String,
    payload_size: usize,
}

impl ProducerWork for ZenohProducer {
    type State = ZenohProducerState;

    async fn init(&self) -> Self::State {
        ZenohProducerState {
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

/// Raw Zenoh consumer.
#[derive(Clone)]
pub struct ZenohConsumer {
    pub session: Session,
    pub topic: String,
}

impl ConsumerWork for ZenohConsumer {
    type State = Subscriber<FifoChannelHandler<Sample>>;

    async fn init(&self) -> Self::State {
        self.session
            .declare_subscriber(&self.topic)
            .await
            .expect("failed to declare zenoh subscriber")
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
