//! EventStore runner — subscribes to events, persists them, publishes watermarks,
//! and serves stored events via queryable.
//!
//! All storage backend operations run on dedicated OS threads (not tokio) to
//! avoid blocking the async runtime. Multiple worker threads consume from a
//! shared MPMC [`flume`] channel, providing horizontal throughput scaling for
//! synchronous backends (fjall, redb, etc.). Fire-and-forget store requests
//! are automatically batched by each worker for improved write throughput.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};
use zenoh::handlers::FifoChannelHandler;
use zenoh::sample::{Sample, SampleKind};
use zenoh::Session;

use crate::attachment::{decode_metadata, NO_URGENCY};
use crate::config::EventBusConfig;
use crate::error::{Error, Result};
use crate::types::PublisherId;

use super::backend::{CompactionStats, EventMetadata, HlcTimestamp, StorageBackend, StoredEvent};
use super::query::QueryFilters;
use super::watermark::{CommitWatermark, PublisherWatermark};

/// Sentinel: no urgent deadline pending.
const NO_DEADLINE: i64 = i64::MAX;

// ---------------------------------------------------------------------------
// Backend message protocol
// ---------------------------------------------------------------------------

/// Fire-and-forget store request. Workers batch these before flushing.
struct StoreRequest {
    key: String,
    payload: Vec<u8>,
    metadata: EventMetadata,
}

/// Request–response operations. Each variant carries a oneshot reply channel.
enum BackendRequest {
    Query {
        filters: QueryFilters,
        reply: flume::Sender<Result<Vec<StoredEvent>>>,
    },
    PublisherWatermarks {
        reply: flume::Sender<HashMap<PublisherId, PublisherWatermark>>,
    },
    Gc {
        older_than: DateTime<Utc>,
        reply: flume::Sender<Result<usize>>,
    },
    Compact {
        reply: flume::Sender<Result<CompactionStats>>,
    },
}

/// Union of all messages the backend thread processes.
enum BackendMsg {
    /// A pre-assembled batch of store requests (from the subscriber drain loop).
    StoreBatch(Vec<StoreRequest>),
    Request(BackendRequest),
    Shutdown,
}

// ---------------------------------------------------------------------------
// BackendHandle — async-friendly proxy to the backend thread pool
// ---------------------------------------------------------------------------

/// Async-friendly handle to a pool of backend worker threads.
///
/// Store operations are fire-and-forget; query / watermark / gc / compact
/// operations send a request and await the reply via a oneshot channel.
#[derive(Clone)]
struct BackendHandle {
    tx: flume::Sender<BackendMsg>,
    /// Cached partition id (immutable, no round-trip needed).
    partition: u32,
}

impl BackendHandle {
    /// Fire-and-forget batch store. Sends the entire batch as one message so
    /// the worker thread can persist it in a single `store_batch()` call.
    fn send_batch(&self, batch: Vec<StoreRequest>) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        self.tx
            .send(BackendMsg::StoreBatch(batch))
            .map_err(|_| Error::StoreError("backend workers shut down".into()))
    }

    async fn query(&self, filters: QueryFilters) -> Result<Vec<StoredEvent>> {
        let (reply_tx, reply_rx) = flume::bounded(1);
        self.tx
            .send(BackendMsg::Request(BackendRequest::Query {
                filters,
                reply: reply_tx,
            }))
            .map_err(|_| Error::StoreError("backend workers shut down".into()))?;
        reply_rx
            .recv_async()
            .await
            .map_err(|_| Error::StoreError("backend worker dropped reply".into()))?
    }

    async fn publisher_watermarks(&self) -> Result<HashMap<PublisherId, PublisherWatermark>> {
        let (reply_tx, reply_rx) = flume::bounded(1);
        self.tx
            .send(BackendMsg::Request(BackendRequest::PublisherWatermarks {
                reply: reply_tx,
            }))
            .map_err(|_| Error::StoreError("backend workers shut down".into()))?;
        reply_rx
            .recv_async()
            .await
            .map_err(|_| Error::StoreError("backend worker dropped reply".into()))
    }

    async fn gc(&self, older_than: DateTime<Utc>) -> Result<usize> {
        let (reply_tx, reply_rx) = flume::bounded(1);
        self.tx
            .send(BackendMsg::Request(BackendRequest::Gc {
                older_than,
                reply: reply_tx,
            }))
            .map_err(|_| Error::StoreError("backend workers shut down".into()))?;
        reply_rx
            .recv_async()
            .await
            .map_err(|_| Error::StoreError("backend worker dropped reply".into()))?
    }

    async fn compact(&self) -> Result<CompactionStats> {
        let (reply_tx, reply_rx) = flume::bounded(1);
        self.tx
            .send(BackendMsg::Request(BackendRequest::Compact {
                reply: reply_tx,
            }))
            .map_err(|_| Error::StoreError("backend workers shut down".into()))?;
        reply_rx
            .recv_async()
            .await
            .map_err(|_| Error::StoreError("backend worker dropped reply".into()))?
    }

    fn partition(&self) -> u32 {
        self.partition
    }
}

// ---------------------------------------------------------------------------
// Backend worker thread (runs on std::thread, NOT tokio)
// ---------------------------------------------------------------------------

/// Run a backend worker that processes messages from the shared MPMC channel.
///
/// Multiple instances of this function run in parallel on separate OS threads.
/// Each worker drains pending store requests into a batch before flushing,
/// and processes query/gc/compact requests with oneshot replies.
fn run_backend_worker(
    backend: Arc<dyn StorageBackend>,
    rx: flume::Receiver<BackendMsg>,
    worker_id: usize,
) {
    debug!(worker_id, "backend worker started");

    loop {
        let first = match rx.recv() {
            Ok(msg) => msg,
            Err(_) => break,
        };

        match first {
            BackendMsg::Shutdown => break,
            BackendMsg::StoreBatch(reqs) => {
                drain_and_flush(&backend, &rx, reqs, worker_id);
            }
            BackendMsg::Request(request) => {
                dispatch_request(&backend, request, worker_id);
            }
        }
    }

    debug!(worker_id, "backend worker stopped");
}

/// Drain any additional pending store messages from the channel, merge them
/// into `batch`, then flush. Interleaved requests are handled immediately
/// after flushing any accumulated stores so replies see the freshest data.
fn drain_and_flush(
    backend: &Arc<dyn StorageBackend>,
    rx: &flume::Receiver<BackendMsg>,
    mut batch: Vec<StoreRequest>,
    worker_id: usize,
) {
    while let Ok(msg) = rx.try_recv() {
        match msg {
            BackendMsg::StoreBatch(reqs) => batch.extend(reqs),
            BackendMsg::Shutdown => {
                flush_batch(backend, batch, worker_id);
                return;
            }
            BackendMsg::Request(request) => {
                flush_batch(backend, std::mem::take(&mut batch), worker_id);
                dispatch_request(backend, request, worker_id);
            }
        }
    }

    flush_batch(backend, batch, worker_id);
}

/// Convert a `Vec<StoreRequest>` into the tuples expected by
/// `StorageBackend::store_batch` and persist them.
fn flush_batch(
    backend: &Arc<dyn StorageBackend>,
    batch: Vec<StoreRequest>,
    worker_id: usize,
) {
    if batch.is_empty() {
        return;
    }
    let count = batch.len();
    let tuples: Vec<_> = batch
        .into_iter()
        .map(|r| (r.key, r.payload, r.metadata))
        .collect();
    if let Err(e) = backend.store_batch(tuples) {
        warn!(worker_id, count, "batch store failed: {e}");
    } else {
        trace!(worker_id, count, "batch persisted");
    }
}

/// Dispatch a single request-reply backend operation.
fn dispatch_request(
    backend: &Arc<dyn StorageBackend>,
    request: BackendRequest,
    worker_id: usize,
) {
    match request {
        BackendRequest::Query { filters, reply } => {
            let _ = reply.send(backend.query(&filters));
        }
        BackendRequest::PublisherWatermarks { reply } => {
            let _ = reply.send(backend.publisher_watermarks());
        }
        BackendRequest::Gc { older_than, reply } => {
            let _ = reply.send(backend.gc(older_than));
        }
        BackendRequest::Compact { reply } => {
            let result = backend.compact();
            if let Err(e) = &result {
                warn!(worker_id, "compact failed: {e}");
            }
            let _ = reply.send(result);
        }
    }
}

// ---------------------------------------------------------------------------
// Async tasks
// ---------------------------------------------------------------------------

/// Decoded sample ready for persistence, plus urgency info for the watermark task.
struct DecodedSample {
    request: StoreRequest,
    urgency_ms: u16,
    timestamp_nanos: i64,
}

/// Try to decode a Zenoh [`Sample`] into a [`DecodedSample`].
///
/// Returns `None` for non-Put samples, internal keys (`/_`), or samples
/// without a valid mitiflow attachment.
fn decode_sample(sample: &Sample) -> Option<DecodedSample> {
    if sample.kind() != SampleKind::Put {
        return None;
    }

    let key = sample.key_expr().as_str();
    if key.contains("/_") {
        return None;
    }

    let meta = sample
        .attachment()
        .and_then(|a| decode_metadata(a).ok())?;

    let payload = sample.payload().to_bytes().to_vec();
    let timestamp_nanos = meta.timestamp.timestamp_nanos_opt().unwrap_or(i64::MAX);

    // Extract HLC timestamp from the Zenoh sample if available.
    let hlc_timestamp = sample.timestamp().map(|ts| {
        let ntp64 = ts.get_time().0;
        // NTP64: upper 32 bits = seconds since 1900-01-01, lower 32 = fraction.
        // Convert to nanoseconds since Unix epoch (1970-01-01).
        // NTP epoch offset: 70 years = 2_208_988_800 seconds.
        const NTP_UNIX_OFFSET: u64 = 2_208_988_800;
        let seconds = (ntp64 >> 32) as u64;
        let fraction = (ntp64 & 0xFFFF_FFFF) as u64;
        let unix_seconds = seconds.saturating_sub(NTP_UNIX_OFFSET);
        let nanos = (fraction * 1_000_000_000) >> 32;
        let physical_ns = unix_seconds * 1_000_000_000 + nanos;
        // Zenoh HLC uses the ID part for logical ordering; we use a
        // simple counter of 0 here since the NTP64 already encodes
        // sub-nanosecond resolution.  A more accurate approach would
        // parse the HLC logical counter, but the fraction bits are
        // sufficient for ordering.
        HlcTimestamp {
            physical_ns,
            logical: 0,
        }
    });

    Some(DecodedSample {
        request: StoreRequest {
            key: key.to_string(),
            payload,
            metadata: EventMetadata {
                seq: meta.seq,
                publisher_id: meta.pub_id,
                event_id: meta.event_id,
                timestamp: meta.timestamp,
                key_expr: key.to_string(),
                hlc_timestamp,
            },
        },
        urgency_ms: meta.urgency_ms,
        timestamp_nanos,
    })
}

/// If the sample carries urgency, atomically update the earliest deadline
/// and wake the watermark task.
fn maybe_update_urgency(
    ds: &DecodedSample,
    earliest_deadline: &AtomicI64,
    urgency_notify: &Notify,
) {
    if ds.urgency_ms == NO_URGENCY {
        return;
    }
    let deadline_ns = ds
        .timestamp_nanos
        .saturating_add(ds.urgency_ms as i64 * 1_000_000);
    let _ = earliest_deadline.fetch_update(Ordering::AcqRel, Ordering::Acquire, |cur| {
        (deadline_ns < cur).then_some(deadline_ns)
    });
    urgency_notify.notify_one();
}

/// Subscribe to events, batch-drain from the Zenoh FIFO channel, and forward
/// each batch to the backend worker pool.
///
/// After receiving the first sample (async), uses `try_recv()` to drain any
/// additional pending samples into the same batch before sending them all as
/// one `StoreBatch` message.
async fn run_subscribe_task(
    subscriber: zenoh::pubsub::Subscriber<FifoChannelHandler<Sample>>,
    handle: BackendHandle,
    earliest_deadline: Arc<AtomicI64>,
    urgency_notify: Arc<Notify>,
    cancel: CancellationToken,
) {
    loop {
        // Wait for the first sample (or cancellation).
        let first_sample = tokio::select! {
            _ = cancel.cancelled() => break,
            res = subscriber.recv_async() => match res {
                Ok(s) => s,
                Err(_) => break,
            },
        };

        // Decode + drain: collect as many ready samples as possible.
        let mut batch = Vec::new();

        if let Some(ds) = decode_sample(&first_sample) {
            maybe_update_urgency(&ds, &earliest_deadline, &urgency_notify);
            batch.push(ds.request);
        }

        while let Ok(Some(sample)) = subscriber.try_recv() {
            if let Some(ds) = decode_sample(&sample) {
                maybe_update_urgency(&ds, &earliest_deadline, &urgency_notify);
                batch.push(ds.request);
            }
        }

        if !batch.is_empty() {
            trace!(count = batch.len(), "sending batch to backend");
            if let Err(e) = handle.send_batch(batch) {
                warn!("failed to send batch to backend: {e}");
                break;
            }
        }
    }
    debug!("store subscribe task stopped");
}

/// Serve stored events for replay queries.
async fn run_queryable_task(
    queryable: zenoh::query::Queryable<FifoChannelHandler<zenoh::query::Query>>,
    handle: BackendHandle,
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

                        let events = match handle.query(filters).await {
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
    handle: BackendHandle,
    watermark_key: String,
    interval_dur: Duration,
    earliest_deadline: Arc<AtomicI64>,
    urgency_notify: Arc<Notify>,
    cancel: CancellationToken,
) {
    let mut interval = tokio::time::interval(interval_dur);

    loop {
        let deadline_ns = earliest_deadline.load(Ordering::Acquire);

        if deadline_ns == NO_DEADLINE {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = interval.tick() => {}
                _ = urgency_notify.notified() => {
                    continue;
                }
            }
        } else {
            let now_nanos = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
            let remaining =
                Duration::from_nanos((deadline_ns - now_nanos).max(0) as u64);

            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(remaining) => {}
            }

            interval.reset();
        }

        earliest_deadline.store(NO_DEADLINE, Ordering::Release);

        let publishers = match handle.publisher_watermarks().await {
            Ok(pubs) => pubs,
            Err(e) => {
                warn!("failed to fetch publisher watermarks: {e}");
                continue;
            }
        };

        let watermark = CommitWatermark {
            partition: handle.partition(),
            publishers,
            timestamp: chrono::Utc::now(),
            epoch: 0,
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
    handle: BackendHandle,
    gc_interval: Duration,
    cancel: CancellationToken,
) {
    let mut interval = tokio::time::interval(gc_interval);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = interval.tick() => {
                let cutoff = chrono::Utc::now() - chrono::Duration::hours(1);
                match handle.gc(cutoff).await {
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

// ---------------------------------------------------------------------------
// EventStore — public API
// ---------------------------------------------------------------------------

/// Durable event store sidecar.
///
/// Subscribes to the event stream, persists events via a [`StorageBackend`],
/// publishes periodic [`CommitWatermark`]s, and answers replay queries.
///
/// All storage I/O runs on dedicated OS threads (configurable via
/// [`EventBusConfig::store_workers`]) to avoid blocking the tokio runtime.
/// Store requests are automatically batched for higher write throughput.
///
/// Spawns background work when [`EventStore::run`] is called:
/// - **N worker threads** — process store/query/gc/compact on OS threads
/// - **subscribe_task** — receives events from Zenoh, queues stores
/// - **queryable_task** — serves stored events for replay queries
/// - **watermark_task** — broadcasts commit watermarks
/// - **gc_task** — periodic garbage collection
pub struct EventStore {
    session: Session,
    backend: Option<Box<dyn StorageBackend>>,
    config: EventBusConfig,
    cancel: CancellationToken,
    handle: Option<BackendHandle>,
    _tasks: Vec<tokio::task::JoinHandle<()>>,
    _worker_threads: Vec<std::thread::JoinHandle<()>>,
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
            backend: Some(Box::new(backend)),
            config,
            cancel: CancellationToken::new(),
            handle: None,
            _tasks: Vec::new(),
            _worker_threads: Vec::new(),
        }
    }

    /// Start all background tasks and worker threads.
    pub async fn run(&mut self) -> Result<()> {
        let key_prefix = &self.config.key_prefix;
        let store_key_prefix = self.config.resolved_store_key_prefix();
        let watermark_key = self.config.resolved_watermark_key();
        let num_workers = self.config.store_workers;

        // Move backend ownership to an Arc for the worker threads.
        let backend: Arc<dyn StorageBackend> = Arc::from(
            self.backend
                .take()
                .expect("EventStore::run called more than once"),
        );
        let partition = backend.partition();

        // Create the MPMC channel.
        let (tx, rx) = flume::unbounded::<BackendMsg>();

        let handle = BackendHandle { tx, partition };
        self.handle = Some(handle.clone());

        // Spawn N worker threads that share the same receiver.
        for worker_id in 0..num_workers {
            let backend = Arc::clone(&backend);
            let rx = rx.clone();
            let thread = std::thread::Builder::new()
                .name(format!("mitiflow-store-{worker_id}"))
                .spawn(move || run_backend_worker(backend, rx, worker_id))
                .map_err(|e| Error::StoreError(format!("failed to spawn worker thread: {e}")))?;
            self._worker_threads.push(thread);
        }
        // Drop our copy of the receiver so channel closes when all workers exit.
        drop(rx);

        // Shared urgency state between subscribe and watermark tasks.
        let earliest_deadline = Arc::new(AtomicI64::new(NO_DEADLINE));
        let urgency_notify = Arc::new(Notify::new());

        let subscriber = self
            .session
            .declare_subscriber(format!("{key_prefix}/**"))
            .await?;
        self._tasks.push(tokio::spawn(run_subscribe_task(
            subscriber,
            handle.clone(),
            Arc::clone(&earliest_deadline),
            Arc::clone(&urgency_notify),
            self.cancel.clone(),
        )));

        // Declare queryable on `{store_prefix}/{partition}` so Zenoh routes
        // partition-scoped recovery queries to the correct store instance.
        let queryable_key = format!("{store_key_prefix}/{partition}");
        let queryable = self.session.declare_queryable(&queryable_key).await?;
        self._tasks.push(tokio::spawn(run_queryable_task(
            queryable,
            handle.clone(),
            self.cancel.clone(),
        )));

        self._tasks.push(tokio::spawn(run_watermark_task(
            self.session.clone(),
            handle.clone(),
            watermark_key,
            self.config.watermark_interval,
            earliest_deadline,
            urgency_notify,
            self.cancel.clone(),
        )));

        self._tasks.push(tokio::spawn(run_gc_task(
            handle,
            Duration::from_secs(60),
            self.cancel.clone(),
        )));

        Ok(())
    }

    /// Query stored events matching the given filters.
    pub async fn query(&self, filters: &QueryFilters) -> Result<Vec<StoredEvent>> {
        self.handle
            .as_ref()
            .expect("EventStore::run not called")
            .query(filters.clone())
            .await
    }

    /// Return per-publisher durability progress (committed_seq + gaps).
    pub async fn publisher_watermarks(
        &self,
    ) -> Result<HashMap<PublisherId, PublisherWatermark>> {
        self.handle
            .as_ref()
            .expect("EventStore::run not called")
            .publisher_watermarks()
            .await
    }

    /// Compact the storage: keep only the latest event per key.
    pub async fn compact(&self) -> Result<CompactionStats> {
        self.handle
            .as_ref()
            .expect("EventStore::run not called")
            .compact()
            .await
    }

    /// Run garbage collection, removing events older than the given timestamp.
    pub async fn gc(&self, older_than: DateTime<Utc>) -> Result<usize> {
        self.handle
            .as_ref()
            .expect("EventStore::run not called")
            .gc(older_than)
            .await
    }

    /// The partition this store is responsible for.
    pub fn partition(&self) -> u32 {
        self.handle
            .as_ref()
            .map(|h| h.partition())
            .unwrap_or(0)
    }

    /// Cancel all background tasks and shut down worker threads.
    pub fn shutdown(&self) {
        self.cancel.cancel();
        // Send shutdown to all workers. Each worker that receives it will exit;
        // others will exit when the channel is closed (sender dropped).
        if let Some(handle) = &self.handle {
            for _ in &self._worker_threads {
                let _ = handle.tx.send(BackendMsg::Shutdown);
            }
        }
    }

    /// Gracefully shut down: cancel tasks, await async tasks, join worker
    /// threads. Consumes `self` so `Drop` does not run.
    pub async fn shutdown_gracefully(mut self) {
        self.shutdown();
        for handle in self._tasks.drain(..) {
            let _ = handle.await;
        }
        for thread in self._worker_threads.drain(..) {
            let _ = thread.join();
        }
    }
}

impl Drop for EventStore {
    fn drop(&mut self) {
        self.shutdown();
        // Wait for worker threads to finish.
        for thread in self._worker_threads.drain(..) {
            let _ = thread.join();
        }
    }
}
