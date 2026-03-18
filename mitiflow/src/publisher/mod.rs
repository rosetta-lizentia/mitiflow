//! Event publisher with sequencing, caching, and heartbeat.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde::Serialize;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};
use zenoh::pubsub::Publisher;
use zenoh::qos::CongestionControl;
use zenoh::Session;

use crate::attachment::encode_metadata;
use crate::config::{EventBusConfig, HeartbeatMode};
use crate::error::Result;
#[cfg(feature = "store")]
use crate::error::Error;
use crate::event::Event;
use crate::types::PublisherId;

/// A sample cached in the publisher's recovery buffer.
#[derive(Debug, Clone)]
pub struct CachedSample {
    pub seq: u64,
    pub key_expr: String,
    pub payload: Vec<u8>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Heartbeat beacon published periodically so subscribers can detect stale connections.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HeartbeatBeacon {
    pub pub_id: PublisherId,
    pub current_seq: u64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Publishes events with monotonic sequencing, an in-memory recovery cache,
/// and periodic heartbeat beacons.
///
/// Background tasks are spawned on construction and cancelled on drop.
pub struct EventPublisher {
    /// Zenoh publisher for the data key expression.
    #[allow(dead_code)]
    publisher: Publisher<'static>,
    /// Zenoh session (cloned Arc) used for declaring queryables and putting heartbeats.
    session: Session,
    /// Bounded in-memory cache for recovery queries.
    cache: Arc<RwLock<VecDeque<CachedSample>>>,
    /// Monotonic sequence counter.
    next_seq: Arc<AtomicU64>,
    /// Unique identity for this publisher.
    publisher_id: PublisherId,
    /// Configuration snapshot.
    config: EventBusConfig,
    /// Token to cancel background tasks on drop.
    cancel: CancellationToken,
    /// Handles to spawned background tasks (for join on graceful shutdown).
    _tasks: Vec<tokio::task::JoinHandle<()>>,
    /// Broadcast sender for watermark updates.
    /// Each `publish_durable()` call subscribes to receive all watermarks.
    #[cfg(feature = "store")]
    watermark_tx: Option<tokio::sync::broadcast::Sender<crate::store::CommitWatermark>>,
}

impl EventPublisher {
    /// Create a new publisher, spawning heartbeat and cache-queryable tasks.
    pub async fn new(session: &Session, config: EventBusConfig) -> Result<Self> {
        let publisher_id = PublisherId::new();
        let key_prefix = &config.key_prefix;

        // Declare the data publisher with configured congestion control.
        let publisher = session
            .declare_publisher(format!("{key_prefix}/**"))
            .congestion_control(config.congestion_control)
            .await?;

        // Declare liveliness token so other nodes know we're alive.
        let _liveliness = session
            .liveliness()
            .declare_token(format!("{key_prefix}/_publishers/{publisher_id}"))
            .await?;

        let cache: Arc<RwLock<VecDeque<CachedSample>>> =
            Arc::new(RwLock::new(VecDeque::with_capacity(config.cache_size)));
        let next_seq = Arc::new(AtomicU64::new(0));
        let cancel = CancellationToken::new();
        let mut tasks = Vec::new();

        // -- Heartbeat task --
        if config.heartbeat != HeartbeatMode::Disabled {
            let hb_session = session.clone();
            let hb_key = format!("{key_prefix}/_heartbeat/{publisher_id}");
            let hb_pub_id = publisher_id;
            let hb_seq = Arc::clone(&next_seq);
            let hb_cancel = cancel.clone();
            let hb_mode = config.heartbeat.clone();

            let handle = tokio::spawn(async move {
                let interval_dur = match &hb_mode {
                    HeartbeatMode::Periodic(d) | HeartbeatMode::Sporadic(d) => *d,
                    HeartbeatMode::Disabled => unreachable!(),
                };
                let mut interval = tokio::time::interval(interval_dur);

                loop {
                    tokio::select! {
                        _ = hb_cancel.cancelled() => break,
                        _ = interval.tick() => {
                            let current = hb_seq.load(Ordering::Relaxed).saturating_sub(1);
                            let beacon = HeartbeatBeacon {
                                pub_id: hb_pub_id,
                                current_seq: current,
                                timestamp: chrono::Utc::now(),
                            };
                            if let Ok(bytes) = serde_json::to_vec(&beacon) {
                                if let Err(e) = hb_session.put(&hb_key, bytes)
                                    .congestion_control(CongestionControl::Drop)
                                    .await
                                {
                                    warn!("heartbeat publish failed: {e}");
                                }
                            }
                        }
                    }
                }
                debug!("heartbeat task stopped for {hb_pub_id}");
            });
            tasks.push(handle);
        }

        // -- Cache queryable task --
        {
            let cq_cache = Arc::clone(&cache);
            let cq_cancel = cancel.clone();
            let cache_key = format!("{key_prefix}/_cache/{publisher_id}");

            let queryable = session.declare_queryable(&cache_key).await?;

            let handle = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = cq_cancel.cancelled() => break,
                        query_result = queryable.recv_async() => {
                            match query_result {
                                Ok(query) => {
                                    // Parse ?after_seq=N from selector parameters.
                                    let after_seq: u64 = query
                                        .parameters()
                                        .get("after_seq")
                                        .and_then(|v| v.parse().ok())
                                        .unwrap_or(0);

                                    let cache_read = cq_cache.read().await;
                                    for sample in cache_read.iter() {
                                        if sample.seq >= after_seq {
                                            if let Err(e) = query
                                                .reply(&sample.key_expr, sample.payload.clone())
                                                .attachment(encode_metadata(
                                                    // Re-encode so the recovering subscriber
                                                    // can decode metadata normally.
                                                    &PublisherId::from_bytes([0; 16]), // placeholder: real pub_id is in payload
                                                    sample.seq,
                                                ))
                                                .await
                                            {
                                                trace!("cache query reply failed: {e}");
                                            }
                                        }
                                    }
                                }
                                Err(_) => break, // queryable dropped
                            }
                        }
                    }
                }
                debug!("cache queryable task stopped");
            });
            tasks.push(handle);
        }

        // -- Shared watermark subscriber (for publish_durable) --
        #[cfg(feature = "store")]
        let watermark_tx = {
            let watermark_key = config.resolved_watermark_key();
            let wm_subscriber = session
                .declare_subscriber(&watermark_key)
                .await?;
            // Capacity is generous — slow callers will just miss old watermarks
            // and wait for the next one.
            let (tx, _) = tokio::sync::broadcast::channel(64);
            let wm_tx = tx.clone();
            let wm_cancel = cancel.clone();

            let handle = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = wm_cancel.cancelled() => break,
                        sample_result = wm_subscriber.recv_async() => {
                            match sample_result {
                                Ok(sample) => {
                                    let bytes = sample.payload().to_bytes();
                                    if let Ok(wm) = serde_json::from_slice::<crate::store::CommitWatermark>(&bytes) {
                                        // Ignore send errors — means no active receivers right now.
                                        let _ = wm_tx.send(wm);
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                    }
                }
                debug!("watermark listener task stopped");
            });
            tasks.push(handle);
            Some(tx)
        };

        Ok(Self {
            publisher,
            session: session.clone(),
            cache,
            next_seq,
            publisher_id,
            config,
            cancel,
            _tasks: tasks,
            #[cfg(feature = "store")]
            watermark_tx,
        })
    }

    /// The unique ID of this publisher instance.
    pub fn publisher_id(&self) -> &PublisherId {
        &self.publisher_id
    }

    /// Publish an event on the configured key prefix (fast path).
    ///
    /// Assigns a monotonic sequence number, attaches metadata, inserts into
    /// the recovery cache, and publishes via Zenoh. Returns the assigned
    /// sequence number.
    pub async fn publish<T: Serialize>(&self, event: &Event<T>) -> Result<u64> {
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
        let key = format!("{}/{}", self.config.key_prefix, seq);
        self.publish_inner(&key, event, seq).await?;
        Ok(seq)
    }

    /// Publish an event to an explicit key expression (e.g., a partition key).
    pub async fn publish_to<T: Serialize>(
        &self,
        key: &str,
        event: &Event<T>,
    ) -> Result<u64> {
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
        self.publish_inner(key, event, seq).await?;
        Ok(seq)
    }

    /// Publish an event and wait for watermark confirmation from the Event Store.
    ///
    /// This is the durable publish path: after putting the event onto Zenoh,
    /// the method blocks until the store's [`CommitWatermark`] confirms the
    /// sequence as durably stored, or until the configured `durable_timeout`
    /// expires (returning [`Error::DurabilityTimeout`]).
    ///
    /// Requires an [`EventStore`] to be running and publishing watermarks.
    ///
    /// [`CommitWatermark`]: crate::store::CommitWatermark
    /// [`EventStore`]: crate::store::EventStore
    #[cfg(feature = "store")]
    pub async fn publish_durable<T: Serialize>(&self, event: &Event<T>) -> Result<u64> {
        let seq = self.publish(event).await?;

        let watermark_tx = self.watermark_tx.as_ref().ok_or_else(|| {
            Error::InvalidConfig("watermark subscriber not initialized".into())
        })?;
        // Subscribe before waiting — each caller gets its own receiver.
        let mut rx = watermark_tx.subscribe();
        let timeout_dur = self.config.durable_timeout;

        let result = tokio::time::timeout(timeout_dur, async {
            loop {
                match rx.recv().await {
                    Ok(wm) => {
                        if wm.is_durable(seq) {
                            return Ok(seq);
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        return Err(Error::ChannelClosed);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        // Missed some watermarks; continue waiting for the next one.
                        continue;
                    }
                }
            }
        })
        .await;

        match result {
            Ok(inner) => inner,
            Err(_) => Err(Error::DurabilityTimeout { seq }),
        }
    }

    /// Internal publish: serialize, attach metadata, cache, and send.
    async fn publish_inner<T: Serialize>(
        &self,
        key: &str,
        event: &Event<T>,
        seq: u64,
    ) -> Result<()> {
        let payload = self.config.codec.encode(event)?;
        let attachment = encode_metadata(&self.publisher_id, seq);

        // Insert into bounded cache (evict oldest if full).
        {
            let mut cache = self.cache.write().await;
            if cache.len() >= self.config.cache_size {
                cache.pop_front();
            }
            cache.push_back(CachedSample {
                seq,
                key_expr: key.to_string(),
                payload: payload.clone(),
                timestamp: chrono::Utc::now(),
            });
        }

        self.session
            .put(key, payload)
            .attachment(attachment)
            .congestion_control(self.config.congestion_control)
            .await?;

        trace!(seq, publisher_id = %self.publisher_id, key, "published event");
        Ok(())
    }

    /// Access the Zenoh session (for advanced use cases like durable publish).
    pub fn session(&self) -> &Session {
        &self.session
    }

    /// Current sequence number (next to be assigned).
    pub fn current_seq(&self) -> u64 {
        self.next_seq.load(Ordering::Relaxed)
    }

    /// Configuration snapshot.
    pub fn config(&self) -> &EventBusConfig {
        &self.config
    }
}

impl Drop for EventPublisher {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
