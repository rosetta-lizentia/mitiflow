//! EventStore runner — subscribes to events, persists them, publishes watermarks,
//! and serves stored events via queryable.

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};
use zenoh::Session;
use zenoh::sample::SampleKind;

use crate::attachment::decode_metadata;
use crate::config::EventBusConfig;
use crate::error::Result;

use super::backend::{EventMetadata, StorageBackend, StoredEvent};
use super::query::QueryFilters;
use super::watermark::CommitWatermark;

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

    /// Start all background tasks. Returns `&mut Self` for chaining.
    pub async fn run(&mut self) -> Result<()> {
        let key_prefix = &self.config.key_prefix;
        let store_key_prefix = self.config.resolved_store_key_prefix();
        let watermark_key = self.config.resolved_watermark_key();

        // -- 1. Subscribe task: persist incoming events --
        {
            let subscriber = self
                .session
                .declare_subscriber(format!("{key_prefix}/**"))
                .await?;
            let backend = Arc::clone(&self.backend);
            let cancel = self.cancel.clone();

            let handle = tokio::spawn(async move {
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
                                    // Skip internal keys.
                                    if key.contains("/_") {
                                        continue;
                                    }

                                    // Decode metadata from attachment.
                                    let (pub_id, seq) = match sample.attachment()
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
                                        seq,
                                        publisher_id: pub_id,
                                        timestamp: chrono::Utc::now(),
                                        key_expr: key.to_string(),
                                    };

                                    if let Err(e) = backend.store(key, &payload, metadata) {
                                        warn!(seq, %pub_id, "failed to persist event: {e}");
                                    } else {
                                        trace!(seq, %pub_id, key, "persisted event");
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                    }
                }
                debug!("store subscribe task stopped");
            });
            self._tasks.push(handle);
        }

        // -- 2. Queryable task: serve stored events for replay --
        {
            let queryable = self.session.declare_queryable(&store_key_prefix).await?;
            let backend = Arc::clone(&self.backend);
            let cancel = self.cancel.clone();

            let handle = tokio::spawn(async move {
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
            });
            self._tasks.push(handle);
        }

        // -- 3. Watermark task: broadcast commit progress --
        {
            let session = self.session.clone();
            let backend = Arc::clone(&self.backend);
            let cancel = self.cancel.clone();
            let interval_dur = self.config.watermark_interval;
            let wm_key = watermark_key;

            let handle = tokio::spawn(async move {
                let mut interval = tokio::time::interval(interval_dur);

                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        _ = interval.tick() => {
                            let watermark = CommitWatermark {
                                committed_seq: backend.committed_seq(),
                                gaps: backend.gap_sequences(),
                                timestamp: chrono::Utc::now(),
                            };

                            if let Ok(bytes) = serde_json::to_vec(&watermark) {
                                if let Err(e) = session.put(&wm_key, bytes).await {
                                    warn!("watermark publish failed: {e}");
                                }
                            }
                        }
                    }
                }
                debug!("store watermark task stopped");
            });
            self._tasks.push(handle);
        }

        // -- 4. GC task: periodic garbage collection --
        {
            let backend = Arc::clone(&self.backend);
            let cancel = self.cancel.clone();
            // GC every 60 seconds, removing events older than 1 hour.
            let gc_interval = Duration::from_secs(60);

            let handle = tokio::spawn(async move {
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
            });
            self._tasks.push(handle);
        }

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
