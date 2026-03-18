//! Event subscriber with gap detection, recovery, and deduplication.

pub mod gap_detector;

#[cfg(feature = "store")]
pub mod checkpoint;

use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};
use zenoh::Session;
use zenoh::sample::SampleKind;

use crate::attachment::decode_metadata;
use crate::codec::CodecFormat;
use crate::config::{EventBusConfig, RecoveryMode};
use crate::error::{Error, Result};
use crate::event::{Event, RawEvent};
use crate::publisher::HeartbeatBeacon;
use crate::types::PublisherId;
use gap_detector::{GapDetector, MissInfo, SampleResult, SequenceTracker};

/// Subscribes to events with gap detection, automatic recovery, and dedup.
///
/// Background tasks handle heartbeat listening and gap recovery.
/// Events are delivered in-order through an internal channel.
pub struct EventSubscriber {
    /// Channel receiver for ordered event delivery.
    event_rx: flume::Receiver<RawEvent>,
    /// Gap detector (shared with background task for heartbeat updates).
    _gap_detector: Arc<RwLock<GapDetector>>,
    /// Configuration snapshot.
    config: EventBusConfig,
    /// Token to cancel background tasks.
    cancel: CancellationToken,
    /// Background task handles.
    _tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl EventSubscriber {
    /// Create a new subscriber, spawning background tasks for event processing,
    /// heartbeat listening, and gap recovery.
    pub async fn new(session: &Session, config: EventBusConfig) -> Result<Self> {
        let key_prefix = &config.key_prefix;

        // Declare Zenoh subscriber on the data key expression.
        let subscriber = session
            .declare_subscriber(format!("{key_prefix}/**"))
            .await?;

        let gap_detector = Arc::new(RwLock::new(GapDetector::new()));
        let cancel = CancellationToken::new();
        let (event_tx, event_rx) = flume::unbounded::<RawEvent>();
        let mut tasks = Vec::new();

        // -- Main event processing task --
        {
            let gd = Arc::clone(&gap_detector);
            let tx = event_tx.clone();
            let sess = session.clone();
            let cancel_clone = cancel.clone();
            let prefix = config.key_prefix.clone();
            let codec = config.codec;

            let handle = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = cancel_clone.cancelled() => break,
                        sample_result = subscriber.recv_async() => {
                            match sample_result {
                                Ok(sample) => {
                                    if sample.kind() != SampleKind::Put {
                                        continue;
                                    }

                                    // Skip internal keys (_heartbeat, _cache, _watermark, etc.)
                                    let key = sample.key_expr().as_str();
                                    if key.contains("/_") {
                                        continue;
                                    }

                                    // Decode metadata from attachment.
                                    let attachment = match sample.attachment() {
                                        Some(a) => a,
                                        None => {
                                            trace!("sample without attachment on {key}, skipping");
                                            continue;
                                        }
                                    };
                                    let (pub_id, seq) = match decode_metadata(attachment) {
                                        Ok(m) => m,
                                        Err(e) => {
                                            warn!("failed to decode attachment on {key}: {e}");
                                            continue;
                                        }
                                    };

                                    // Run gap detection.
                                    let result = {
                                        let mut gd = gd.write().await;
                                        gd.on_sample(&pub_id, seq)
                                    };

                                    match result {
                                        SampleResult::Deliver => {
                                            let payload_bytes = sample.payload().to_bytes().to_vec();
                                            deliver_event(&tx, pub_id, seq, key, &payload_bytes, codec);
                                        }
                                        SampleResult::Duplicate => {
                                            trace!(seq, %pub_id, "duplicate, dropping");
                                        }
                                        SampleResult::Gap(miss) => {
                                            // Deliver current sample (it's valid, just out of order).
                                            let payload_bytes = sample.payload().to_bytes().to_vec();
                                            deliver_event(&tx, pub_id, seq, key, &payload_bytes, codec);

                                            // Spawn async recovery for the gap.
                                            let recovery_sess = sess.clone();
                                            let recovery_tx = tx.clone();
                                            let recovery_prefix = prefix.clone();
                                            tokio::spawn(async move {
                                                if let Err(e) = recover_gap(
                                                    &recovery_sess,
                                                    &recovery_prefix,
                                                    &miss,
                                                    &recovery_tx,
                                                    codec,
                                                ).await {
                                                    warn!("gap recovery failed: {e}");
                                                }
                                            });
                                        }
                                    }
                                }
                                Err(_) => break, // subscriber dropped
                            }
                        }
                    }
                }
                debug!("event processing task stopped");
            });
            tasks.push(handle);
        }

        // -- Heartbeat listener task --
        {
            let hb_subscriber = session
                .declare_subscriber(format!("{key_prefix}/_heartbeat/*"))
                .await?;
            let gd = Arc::clone(&gap_detector);
            let hb_cancel = cancel.clone();
            let hb_sess = session.clone();
            let hb_tx = event_tx.clone();
            let hb_prefix = config.key_prefix.clone();
            let hb_codec = config.codec;
            let recovery_enabled = matches!(
                config.recovery_mode,
                RecoveryMode::Heartbeat | RecoveryMode::Both
            );

            let handle = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = hb_cancel.cancelled() => break,
                        sample_result = hb_subscriber.recv_async() => {
                            match sample_result {
                                Ok(sample) => {
                                    if !recovery_enabled {
                                        continue;
                                    }
                                    let bytes = sample.payload().to_bytes();
                                    let beacon: HeartbeatBeacon = match serde_json::from_slice(&bytes) {
                                        Ok(b) => b,
                                        Err(e) => {
                                            trace!("invalid heartbeat: {e}");
                                            continue;
                                        }
                                    };

                                    let misses = {
                                        let mut gd = gd.write().await;
                                        gd.on_heartbeat(&beacon.pub_id, beacon.current_seq)
                                    };

                                    for miss in misses {
                                        let sess = hb_sess.clone();
                                        let tx = hb_tx.clone();
                                        let prefix = hb_prefix.clone();
                                        tokio::spawn(async move {
                                            if let Err(e) = recover_gap(&sess, &prefix, &miss, &tx, hb_codec).await {
                                                warn!("heartbeat-triggered recovery failed: {e}");
                                            }
                                        });
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                    }
                }
                debug!("heartbeat listener task stopped");
            });
            tasks.push(handle);
        }

        Ok(Self {
            event_rx,
            _gap_detector: gap_detector,
            config,
            cancel,
            _tasks: tasks,
        })
    }

    /// Receive the next event, deserializing the payload into type `T`
    /// using the configured codec.
    pub async fn recv<T: Serialize + DeserializeOwned>(&self) -> Result<Event<T>> {
        let raw = self
            .event_rx
            .recv_async()
            .await
            .map_err(|_| Error::ChannelClosed)?;
        raw.deserialize_with(self.config.codec)
    }

    /// Receive the next event as raw bytes (no payload deserialization).
    pub async fn recv_raw(&self) -> Result<RawEvent> {
        self.event_rx
            .recv_async()
            .await
            .map_err(|_| Error::ChannelClosed)
    }

    /// Configuration snapshot.
    pub fn config(&self) -> &EventBusConfig {
        &self.config
    }
}

impl Drop for EventSubscriber {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Construct and send a `RawEvent` through the delivery channel.
fn deliver_event(
    tx: &flume::Sender<RawEvent>,
    pub_id: PublisherId,
    seq: u64,
    key: &str,
    payload: &[u8],
    codec: CodecFormat,
) {
    // Best-effort extraction of event ID and timestamp from the encoded payload.
    // If the payload can't be decoded or doesn't have the expected fields,
    // we still deliver with defaults.
    let (id, timestamp) = extract_event_meta(payload, codec);

    let raw = RawEvent {
        id,
        seq,
        publisher_id: pub_id,
        key_expr: key.to_string(),
        payload: payload.to_vec(),
        timestamp,
    };
    if tx.send(raw).is_err() {
        trace!("event channel closed, dropping event seq={seq}");
    }
}

/// Extract `id` and `timestamp` from the serialized event payload.
fn extract_event_meta(
    payload: &[u8],
    codec: CodecFormat,
) -> (crate::types::EventId, chrono::DateTime<chrono::Utc>) {
    #[derive(serde::Deserialize)]
    struct Envelope {
        id: crate::types::EventId,
        timestamp: chrono::DateTime<chrono::Utc>,
    }
    match codec.decode::<Envelope>(payload) {
        Ok(env) => (env.id, env.timestamp),
        Err(_) => (crate::types::EventId::new(), chrono::Utc::now()),
    }
}

/// Attempt to recover missed events by querying the publisher's cache.
async fn recover_gap(
    session: &Session,
    key_prefix: &str,
    miss: &MissInfo,
    tx: &flume::Sender<RawEvent>,
    codec: CodecFormat,
) -> Result<()> {
    let cache_key = format!(
        "{key_prefix}/_cache/{}?after_seq={}",
        miss.source, miss.missed.start
    );
    debug!(
        publisher = %miss.source,
        missed = ?miss.missed,
        "recovering gap via cache query"
    );

    let replies = session.get(&cache_key).await?;
    let mut recovered = 0u64;

    while let Ok(reply) = replies.recv_async().await {
        match reply.result() {
            Ok(sample) => {
                let key = sample.key_expr().as_str();
                let payload_bytes = sample.payload().to_bytes().to_vec();

                // Decode seq from attachment if present.
                let seq = sample
                    .attachment()
                    .and_then(|a| decode_metadata(a).ok())
                    .map(|(_, s)| s)
                    .unwrap_or(miss.missed.start + recovered);

                deliver_event(tx, miss.source, seq, key, &payload_bytes, codec);
                recovered += 1;
            }
            Err(err) => {
                warn!(
                    "cache query reply error from {}: {}",
                    miss.source,
                    err.payload().try_to_string().unwrap_or_default()
                );
            }
        }
    }

    if recovered == 0 {
        return Err(Error::GapRecoveryFailed {
            publisher_id: miss.source,
            missed: miss.missed.clone(),
        });
    }

    debug!(
        publisher = %miss.source,
        recovered,
        "gap recovery complete"
    );
    Ok(())
}
