//! Redis Streams transport.

use lightbench::{
    BenchmarkWork, ConsumerRecorder, ConsumerWork, ProducerWork, WorkResult, now_unix_ns_estimate,
};
use crate::{build_payload, extract_timestamp};

/// Redis Streams producer using XADD.
#[derive(Clone)]
pub struct RedisProducer {
    pub url: String,
    pub stream_key: String,
    pub payload_size: usize,
}

pub struct RedisProducerState {
    conn: redis::aio::MultiplexedConnection,
    stream_key: String,
    payload_size: usize,
}

impl ProducerWork for RedisProducer {
    type State = RedisProducerState;

    async fn init(&self) -> Self::State {
        let client = redis::Client::open(self.url.as_str()).expect("failed to open redis client");
        let conn = client
            .get_multiplexed_async_connection()
            .await
            .expect("failed to connect to redis");
        RedisProducerState {
            conn,
            stream_key: self.stream_key.clone(),
            payload_size: self.payload_size,
        }
    }

    async fn produce(&self, state: &mut Self::State) -> Result<(), String> {
        let payload = build_payload(state.payload_size, now_unix_ns_estimate());
        redis::cmd("XADD")
            .arg(&state.stream_key)
            .arg("*")
            .arg("d")
            .arg(payload)
            .query_async::<String>(&mut state.conn)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

/// Redis Streams consumer using XREAD (without consumer groups, for throughput).
#[derive(Clone)]
pub struct RedisConsumer {
    pub url: String,
    pub stream_key: String,
}

impl ConsumerWork for RedisConsumer {
    type State = redis::aio::MultiplexedConnection;

    async fn init(&self) -> Self::State {
        let client = redis::Client::open(self.url.as_str()).expect("failed to open redis client");
        client
            .get_multiplexed_async_connection()
            .await
            .expect("failed to connect to redis")
    }

    async fn run(&self, mut state: Self::State, recorder: ConsumerRecorder) -> Self::State {
        let mut last_id = "$".to_string();
        while recorder.is_running() {
            let result: redis::RedisResult<redis::streams::StreamReadReply> = redis::cmd("XREAD")
                .arg("COUNT")
                .arg(100)
                .arg("BLOCK")
                .arg(100) // 100ms block
                .arg("STREAMS")
                .arg(&self.stream_key)
                .arg(&last_id)
                .query_async(&mut state)
                .await;

            match result {
                Ok(reply) => {
                    for stream_key in &reply.keys {
                        for entry in &stream_key.ids {
                            last_id.clone_from(&entry.id);
                            if let Some(redis::Value::BulkString(data)) = entry.map.get("d") {
                                let now = now_unix_ns_estimate();
                                let sent_ts = extract_timestamp(data);
                                recorder.record(now.saturating_sub(sent_ts)).await;
                            }
                        }
                    }
                }
                Err(_) => continue,
            }
        }
        state
    }
}

/// Redis Streams durable publish (XADD is inherently persisted).
#[derive(Clone)]
pub struct RedisDurableWork {
    pub url: String,
    pub stream_key: String,
    pub payload_size: usize,
}

pub struct RedisDurableState {
    conn: redis::aio::MultiplexedConnection,
    stream_key: String,
    payload_size: usize,
}

impl BenchmarkWork for RedisDurableWork {
    type State = RedisDurableState;

    async fn init(&self) -> Self::State {
        let client = redis::Client::open(self.url.as_str()).expect("failed to open redis client");
        let conn = client
            .get_multiplexed_async_connection()
            .await
            .expect("failed to connect to redis");
        RedisDurableState {
            conn,
            stream_key: self.stream_key.clone(),
            payload_size: self.payload_size,
        }
    }

    async fn work(&self, state: &mut Self::State) -> WorkResult {
        let start = now_unix_ns_estimate();
        let payload = build_payload(state.payload_size, start);
        match redis::cmd("XADD")
            .arg(&state.stream_key)
            .arg("*")
            .arg("d")
            .arg(payload)
            .query_async::<String>(&mut state.conn)
            .await
        {
            Ok(_) => WorkResult::success(now_unix_ns_estimate() - start),
            Err(e) => WorkResult::error(e.to_string()),
        }
    }
}
