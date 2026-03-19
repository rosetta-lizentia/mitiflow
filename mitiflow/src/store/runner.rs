//! EventStore runner — subscribes to events, persists them, publishes watermarks,
//! and serves stored events via queryable.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};
use zenoh::Session;
use zenoh::handlers::FifoChannelHandler;
use zenoh::sample::{Sample, SampleKind};

use crate::attachment::{NO_URGENCY, decode_metadata};
use crate::config::EventBusConfig;
use crate::error::Result;

use super::backend::{EventMetadata, StorageBackend, StoredEvent};
use super::query::QueryFilters;
use super::watermark::CommitWatermark;

/// Sentinel: no urgent deadline pending.
const NO_DEADLINE: i64 = i64::MAX;

/// Persist incoming events from the Zenoh subscriber into the storage backend.
///
/// When an event carries an urgency hint (`urgency_ms < NO_URGENCY`), computes
/// the deadline and atomically updates `earliest_deadline` if this event's
/// deadline is sooner, then wakes the watermark task via `urgency_notify`.
async fn run_subscribe_task(
    subscriber: zenoh::pubsub::Subscriber<FifoChannelHandler<Sample>>,
    backend: Arc<dyn StorageBackend>,
    earliest_deadline: Arc<AtomicI64>,
    urgency_notify: Arc<Notify>,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            sample_result = subscriber.recv_async() => {
                match sample_result {
                    Ok(sample) => {
                        if sample.kind() != SampleKind::Put {
                            continue;
                        }

                        let key = sample.key_expr().as_str();
                        if key.contains("/_") {
                            continue;
                        }

                        let meta = match sample.attachment()
                            .and_then(|a| decode_metadata(a).ok())
                        {
                            Some(m) => m,
                            None => {
                                trace!("sample without valid attachment on {key}, skipping");
                                continue;
                            }
                        };

                        let payload = sample.payload().to_bytes().to_vec();
                        let metadata = EventMetadata {
                            seq: meta.seq,
                            publisher_id: meta.pub_id,
                            event_id: meta.event_id,
                            timestamp: meta.timestamp,
                            key_expr: key.to_string(),
                        };

                        if let Err(e) = backend.store(key, &payload, metadata) {
                            warn!(seq = meta.seq, pub_id = %meta.pub_id, "failed to persist event: {e}");
                        } else {
                            trace!(seq = meta.seq, pub_id = %meta.pub_id, key, "persisted event");
                        }

                        // If the event carries urgency, update the earliest deadline
                        // so the watermark task wakes early.
                        if meta.urgency_ms != NO_URGENCY {
                            let deadline_ns = meta
                                .timestamp
                                .timestamp_nanos_opt()
                                .unwrap_or(i64::MAX)
                                .saturating_add(meta.urgency_ms as i64 * 1_000_000);
                            // Atomic min: only store if our deadline is earlier.
                            let _ = earliest_deadline.fetch_update(
                                Ordering::AcqRel,
                                Ordering::Acquire,
                                |current| {
                                    if deadline_ns < current {
                                        Some(deadline_ns)
                                    } else {
                                        None
                                    }
                                },
                            );
                            urgency_notify.notify_one();
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    }
    debug!("store subscribe task stopped");
}

/// Serve stored events for replay queries.
async fn run_queryable_task(
    queryable: zenoh::query::Queryable<FifoChannelHandler<zenoh::query::Query>>,
    backend: Arc<dyn StorageBackend>,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            query_result = queryable.recv_async() => {
                match query_result {
                    Ok(query) => {
                        let params = query.parameters().to_string();
                        let filters = match QueryFilters::from_selector(&params) {
                            Ok(f) => f,
                            Err(e) => {
                                warn!("invalid query filters: {e}");
                                continue;
                            }
                        };

                        let events: Vec<StoredEvent> = match backend.query(&filters) {
                            Ok(e) => e,
                            Err(e) => {
                                warn!("store query failed: {e}");
                                continue;
                            }
                        };

                        for event in &events {
                            let meta_attachment = crate::attachment::encode_metadata(
                                &event.metadata.publisher_id,
                                event.metadata.seq,
                                &event.metadata.event_id,
                                &event.metadata.timestamp,
                                crate::attachment::NO_URGENCY,
                            );
                            if let Err(e) = query
                                .reply(&event.key, event.payload.clone())
                                .attachment(meta_attachment)
                                .await
                            {
                                trace!("store query reply failed: {e}");
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    }
    debug!("store queryable task stopped");
}

/// Broadcast commit watermarks, adapting to urgency hints.
///
/// Normally fires every `interval_dur`. When the subscribe task stores an
/// event with `urgency_ms < NO_URGENCY`, the watermark is broadcast as soon as
/// the computed deadline arrives (or immediately for `urgency_ms == 0`).
async fn run_watermark_task(
    session: Session,
    backend: Arc<dyn StorageBackend>,
    watermark_key: String,
    interval_dur: Duration,
    earliest_deadline: Arc<AtomicI64>,
    urgency_notify: Arc<Notify>,
    cancel: CancellationToken,
) {
    let mut interval = tokio::time::interval(interval_dur);

    loop {
        // Determine how long to sleep: the shorter of the next periodic tick
        // and the earliest urgency deadline.
        let deadline_ns = earliest_deadline.load(Ordering::Acquire);

        if deadline_ns == NO_DEADLINE {
            // No urgent events — wait for either the periodic tick or a new
            // urgency notification.
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = interval.tick() => {}
                _ = urgency_notify.notified() => {
                    // Re-evaluate the deadline on the next loop iteration.
                    continue;
                }
            }
        } else {
            // Compute tokio Instant from the nanos deadline.
            let now_nanos = chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(0);
            let remaining = Duration::from_nanos(
                (deadline_ns - now_nanos).max(0) as u64,
            );

            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(remaining) => {}
            }

            // Reset the interval so the next periodic tick is a full interval
            // from now, avoiding a burst of ticks after an early wake.
            interval.reset();
        }

        // Clear the deadline — it will be re-set by the next urgent event.
        earliest_deadline.store(NO_DEADLINE, Ordering::Release);

        let watermark = CommitWatermark {
            partition: backend.partition(),
            publishers: backend.publisher_watermarks(),
            timestamp: chrono::Utc::now(),
        };

        if let Ok(bytes) = serde_json::to_vec(&watermark) {
            if let Err(e) = session.put(&watermark_key, bytes).await {
                warn!("watermark publish failed: {e}");
            }
        }
    }
    debug!("store watermark task stopped");
}

/// Periodically run garbage collection on old events.
async fn run_gc_task(
    backend: Arc<dyn StorageBackend>,
    gc_interval: Duration,
    cancel: CancellationToken,
) {
    let mut interval = tokio::time::interval(gc_interval);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = interval.tick() => {
                let cutoff = chrono::Utc::now() - chrono::Duration::hours(1);
                match backend.gc(cutoff) {
                    Ok(removed) if removed > 0 => {
                        debug!(removed, "gc removed old events");
                    }
                    Ok(_) => {}
                    Err(e) => warn!("gc failed: {e}"),
                }
            }
        }
    }
    debug!("store gc task stopped");
}

/// Durable event store sidecar.
///
/// Subscribes to the event stream, persists events via a [`StorageBackend`],
/// publishes periodic [`CommitWatermark`]s, and answers replay queries.
///
/// Spawns 4 background tasks when [`EventStore::run`] is called:
/// 1. **subscribe_task** — persists incoming events
/// 2. **queryable_task** — serves stored events for replay queries
/// 3. **watermark_task** — broadcasts commit watermarks
/// 4. **gc_task** — periodic garbage collection
pub struct EventStore {
    session: Session,
    backend: Arc<dyn StorageBackend>,
    config: EventBusConfig,
    cancel: CancellationToken,
    _tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl EventStore {
    /// Create a new EventStore. Call [`EventStore::run`] to start the background tasks.
    pub fn new(
        session: &Session,
        backend: impl StorageBackend + 'static,
        config: EventBusConfig,
    ) -> Self {
        Self {
            session: session.clone(),
            backend: Arc::new(backend),
            config,
            cancel: CancellationToken::new(),
            _tasks: Vec::new(),
        }
    }

    /// Start all background tasks.
    pub async fn run(&mut self) -> Result<()> {
        let key_prefix = &self.config.key_prefix;
        let store_key_prefix = self.config.resolved_store_key_prefix();
        let watermark_key = self.config.resolved_watermark_key();

        // Shared urgency state between subscribe and watermark tasks.
        let earliest_deadline = Arc::new(AtomicI64::new(NO_DEADLINE));
        let urgency_notify = Arc::new(Notify::new());

        let subscriber = self
            .session
            .declare_subscriber(format!("{key_prefix}/**"))
            .await?;
        self._tasks.push(tokio::spawn(run_subscribe_task(
            subscriber,
            Arc::clone(&self.backend),
            Arc::clone(&earliest_deadline),
            Arc::clone(&urgency_notify),
            self.cancel.clone(),
        )));

        let queryable = self.session.declare_queryable(&store_key_prefix).await?;
        self._tasks.push(tokio::spawn(run_queryable_task(
            queryable,
            Arc::clone(&self.backend),
            self.cancel.clone(),
        )));

        self._tasks.push(tokio::spawn(run_watermark_task(
            self.session.clone(),
            Arc::clone(&self.backend),
            watermark_key,
            self.config.watermark_interval,
            earliest_deadline,
            urgency_notify,
            self.cancel.clone(),
        )));

        self._tasks.push(tokio::spawn(run_gc_task(
            Arc::clone(&self.backend),
            Duration::from_secs(60),
            self.cancel.clone(),
        )));

        Ok(())
    }

    /// Access the underlying storage backend.
    pub fn backend(&self) -> &dyn StorageBackend {
        self.backend.as_ref()
    }

    /// Cancel all background tasks.
    pub fn shutdown(&self) {
        self.cancel.cancel();
    }
}

impl Drop for EventStore {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
