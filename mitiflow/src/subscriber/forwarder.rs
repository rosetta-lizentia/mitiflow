//! Zenoh subscriber lifecycle management for event forwarding.
//!
//! Each key expression gets its own forwarder task that decodes Zenoh samples
//! into `(EventMeta, key, payload)` tuples and pushes them into a shared
//! fan-in channel.  During offload the forwarders **drop** their Zenoh
//! subscribers (releasing transport resources) and re-declare them when
//! live streaming resumes.

use std::sync::Arc;

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};
use zenoh::Session;
use zenoh::sample::{Sample, SampleKind};

use crate::attachment::{EventMeta, decode_metadata};
use crate::publisher::HeartbeatBeacon;

/// Decoded sample tuple pushed through the fan-in channel.
pub(crate) type DecodedSample = (EventMeta, String, Vec<u8>);

// ---------------------------------------------------------------------------
// Forwarder state
// ---------------------------------------------------------------------------

/// State signal sent to forwarder tasks via a `watch` channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ForwarderState {
    /// Normal operation — receive from Zenoh and forward.
    Active,
    /// Offload in progress — drop the Zenoh subscriber and wait.
    Paused,
}

/// Cloneable control handle for pausing / resuming forwarder tasks.
///
/// `OffloadManager` holds a clone of this to drive the offload lifecycle
/// without owning the task join handles.
#[derive(Clone)]
pub(crate) struct ForwarderControl {
    state_tx: Arc<watch::Sender<ForwarderState>>,
}

impl ForwarderControl {
    /// Pause all forwarders — they will drop their Zenoh subscribers.
    pub fn pause(&self) {
        let _ = self.state_tx.send(ForwarderState::Paused);
    }

    /// Resume all forwarders — they will re-declare their Zenoh subscribers.
    pub fn resume(&self) {
        let _ = self.state_tx.send(ForwarderState::Active);
    }

    /// Current forwarder state.
    #[allow(dead_code)]
    pub fn state(&self) -> ForwarderState {
        *self.state_tx.borrow()
    }
}

/// Handle returned by [`spawn_forwarders`] to control forwarder lifecycle.
pub(crate) struct ForwarderHandle {
    /// Cloneable control for pause / resume.
    pub control: ForwarderControl,
    /// Task join handles (owned by `EventSubscriber._tasks`).
    pub tasks: Vec<tokio::task::JoinHandle<()>>,
}

// ---------------------------------------------------------------------------
// Sample / heartbeat decoding
// ---------------------------------------------------------------------------

/// Try to decode a data sample into metadata + payload, filtering out
/// non-Put samples, internal keys, and samples without valid attachments.
pub(crate) fn decode_sample(sample: &Sample) -> Option<DecodedSample> {
    if sample.kind() != SampleKind::Put {
        return None;
    }
    let key = sample.key_expr().as_str();
    if key.contains("/_") {
        return None;
    }
    let attachment = match sample.attachment() {
        Some(a) => a,
        None => {
            trace!("sample without attachment on {key}, skipping");
            return None;
        }
    };
    let meta = match decode_metadata(attachment) {
        Ok(m) => m,
        Err(e) => {
            warn!("failed to decode attachment on {key}: {e}");
            return None;
        }
    };
    let payload = sample.payload().to_bytes().to_vec();
    Some((meta, key.to_string(), payload))
}

/// Parse a heartbeat beacon from a Zenoh sample payload.
pub(crate) fn decode_heartbeat(sample: &Sample) -> Option<HeartbeatBeacon> {
    let bytes = sample.payload().to_bytes();
    match serde_json::from_slice(&bytes) {
        Ok(b) => Some(b),
        Err(e) => {
            trace!("invalid heartbeat: {e}");
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Spawning
// ---------------------------------------------------------------------------

/// Spawn one forwarder task per key expression.
///
/// Each task declares a Zenoh subscriber, decodes incoming samples, and
/// pushes them into `sample_tx`. When the `ForwarderHandle` signals
/// [`ForwarderState::Paused`], forwarders drop their Zenoh subscribers
/// (freeing transport resources) and wait until resumed.
pub(crate) async fn spawn_forwarders(
    session: &Session,
    key_exprs: &[String],
    sample_tx: flume::Sender<DecodedSample>,
    cancel: CancellationToken,
) -> crate::error::Result<ForwarderHandle> {
    let (state_tx, _state_rx) = watch::channel(ForwarderState::Active);
    let state_tx = Arc::new(state_tx);
    let mut tasks = Vec::with_capacity(key_exprs.len());

    for ke in key_exprs {
        let session = session.clone();
        let ke = ke.clone();
        let tx = sample_tx.clone();
        let cancel_c = cancel.clone();
        let mut state_rx = state_tx.subscribe();

        tasks.push(tokio::spawn(async move {
            // Declare the initial subscriber.
            let mut sub = match session.declare_subscriber(ke.as_str()).await {
                Ok(s) => s,
                Err(e) => {
                    warn!(key_expr = %ke, "failed to declare subscriber: {e}");
                    return;
                }
            };

            loop {
                if *state_rx.borrow() == ForwarderState::Paused {
                    // ── PAUSED: drop subscriber, wait for resume ──
                    debug!(key_expr = %ke, "forwarder paused — unsubscribing");
                    drop(sub);

                    // Wait until state changes back to Active or cancel.
                    loop {
                        tokio::select! {
                            _ = cancel_c.cancelled() => return,
                            result = state_rx.changed() => {
                                match result {
                                    Ok(()) if *state_rx.borrow() == ForwarderState::Active => break,
                                    Ok(()) => continue, // still paused
                                    Err(_) => return, // sender dropped
                                }
                            }
                        }
                    }

                    // Re-declare subscriber.
                    debug!(key_expr = %ke, "forwarder resumed — re-subscribing");
                    sub = match session.declare_subscriber(ke.as_str()).await {
                        Ok(s) => s,
                        Err(e) => {
                            warn!(key_expr = %ke, "failed to re-declare subscriber: {e}");
                            return;
                        }
                    };
                }

                // ── ACTIVE: receive and forward ──
                tokio::select! {
                    _ = cancel_c.cancelled() => break,
                    result = state_rx.changed() => {
                        match result {
                            Ok(()) => continue, // re-check state at top of loop
                            Err(_) => break,    // sender dropped
                        }
                    }
                    result = sub.recv_async() => {
                        match result {
                            Ok(sample) => {
                                if let Some(decoded) = decode_sample(&sample) {
                                    let _ = tx.send_async(decoded).await;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
        }));
    }

    Ok(ForwarderHandle {
        control: ForwarderControl { state_tx },
        tasks,
    })
}
