//! Event subscriber with gap detection, recovery, and deduplication.
//!
//! ## Module structure
//!
//! - `forwarder` — Zenoh subscriber lifecycle (pause/resume for offload)
//! - `recovery` — Tiered gap recovery (store → cache → backoff)
//! - `pipeline` — Gap detection, shard routing, heartbeat handling
//! - [`offload`] — Slow-consumer offload state machine (store feature)
//! - [`gap_detector`] — Per-publisher sequence tracking
//! - [`checkpoint`] — Persistent sequence checkpoints (store feature)
//! - [`consumer_group`] — Consumer group with offset management (store feature)

pub mod gap_detector;
pub(crate) mod forwarder;
pub(crate) mod pipeline;
pub(crate) mod recovery;

#[cfg(feature = "fjall-backend")]
pub mod checkpoint;

#[cfg(feature = "store")]
pub mod consumer_group;

#[cfg(feature = "store")]
pub mod offload;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio_util::sync::CancellationToken;
use zenoh::Session;

use crate::config::EventBusConfig;
use crate::error::{Error, Result};
use crate::event::{Event, RawEvent};

use forwarder::DecodedSample;

/// Subscribes to events with gap detection, automatic recovery, and dedup.
///
/// Background tasks handle heartbeat listening and gap recovery.
/// Events are delivered in-order through an internal channel.
pub struct EventSubscriber {
    /// Channel receiver for ordered event delivery.
    event_rx: flume::Receiver<RawEvent>,
    /// Configuration snapshot.
    config: EventBusConfig,
    /// Token to cancel background tasks.
    cancel: CancellationToken,
    /// Background task handles.
    _tasks: Vec<tokio::task::JoinHandle<()>>,
    /// Keeps the forwarder watch channel alive for the subscriber's lifetime.
    _fwd_control: forwarder::ForwarderControl,
    /// Offload event channel (present only when offload is enabled).
    #[cfg(feature = "store")]
    offload_event_rx: Option<flume::Receiver<offload::OffloadEvent>>,
}

impl EventSubscriber {
    /// Create a new subscriber, spawning background tasks for event processing,
    /// heartbeat listening, and gap recovery.
    pub async fn new(session: &Session, config: EventBusConfig) -> Result<Self> {
        let key_expr = format!("{}/**", config.key_prefix);
        Self::init(session, config, &[key_expr]).await
    }

    /// Create a subscriber that only receives events for the specified partitions.
    pub async fn new_partitioned(
        session: &Session,
        config: EventBusConfig,
        partitions: &[u32],
    ) -> Result<Self> {
        let key_exprs: Vec<String> = if partitions.is_empty() {
            vec![format!("{}/_none", config.key_prefix)]
        } else {
            partitions
                .iter()
                .map(|p| format!("{}/p/{p}/**", config.key_prefix))
                .collect()
        };
        Self::init(session, config, &key_exprs).await
    }

    /// Create a subscriber for events with a specific application key.
    pub async fn new_keyed(session: &Session, config: EventBusConfig, key: &str) -> Result<Self> {
        let key_expr = config.key_expr_for_key(key);
        Self::init(session, config, &[key_expr]).await
    }

    /// Create a subscriber for events matching a key prefix.
    pub async fn new_key_prefix(
        session: &Session,
        config: EventBusConfig,
        key_prefix: &str,
    ) -> Result<Self> {
        let key_expr = config.key_expr_for_key_prefix(key_prefix);
        Self::init(session, config, &[key_expr]).await
    }

    /// Create a subscriber with explicit Zenoh key expressions.
    pub async fn init_with_key_exprs(
        session: &Session,
        config: EventBusConfig,
        key_exprs: &[String],
    ) -> Result<Self> {
        Self::init(session, config, key_exprs).await
    }

    /// Shared initialization: spawn forwarders and processing pipeline.
    async fn init(session: &Session, config: EventBusConfig, key_exprs: &[String]) -> Result<Self> {
        let ch_cap = config.event_channel_capacity;
        let num_shards = config.num_processing_shards;

        // Fan-in channel: forwarders push decoded samples here.
        let (sample_tx, sample_rx) = flume::bounded::<DecodedSample>(ch_cap);
        let cancel = CancellationToken::new();
        let (event_tx, event_rx) = flume::bounded::<RawEvent>(ch_cap);

        // Spawn forwarder tasks (one per key expression).
        let fwd_handle = forwarder::spawn_forwarders(
            session, key_exprs, sample_tx, cancel.clone(),
        ).await?;

        // Destructure: keep control alive, extract tasks.
        let forwarder::ForwarderHandle { control: fwd_control, tasks: fwd_tasks } = fwd_handle;
        let mut tasks = fwd_tasks;

        #[cfg(feature = "store")]
        let mut offload_event_rx_slot: Option<flume::Receiver<offload::OffloadEvent>> = None;

        if num_shards == 1 {
            let worker = pipeline::spawn_single_shard_worker(
                session,
                &config,
                sample_rx,
                event_tx,
                cancel.clone(),
                #[cfg(feature = "store")]
                if config.offload.enabled { Some(fwd_control.clone()) } else { None },
                #[cfg(feature = "store")]
                &mut offload_event_rx_slot,
            ).await?;
            tasks.push(worker);
        } else {
            let mut worker_tasks = pipeline::spawn_multi_shard_workers(
                session, &config, sample_rx, event_tx, cancel.clone(),
                #[cfg(feature = "store")]
                if config.offload.enabled { Some(fwd_control.clone()) } else { None },
                #[cfg(feature = "store")]
                &mut offload_event_rx_slot,
            ).await?;
            tasks.append(&mut worker_tasks);
        }

        Ok(Self {
            event_rx,
            config,
            cancel,
            _tasks: tasks,
            _fwd_control: fwd_control,
            #[cfg(feature = "store")]
            offload_event_rx: offload_event_rx_slot,
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

    /// Returns a receiver for offload lifecycle events.
    ///
    /// Returns `None` if offload is not enabled in the configuration.
    #[cfg(feature = "store")]
    pub fn offload_events(&self) -> Option<&flume::Receiver<offload::OffloadEvent>> {
        self.offload_event_rx.as_ref()
    }

    /// Gracefully shut down the subscriber: cancel all background tasks and
    /// await their completion.
    pub async fn shutdown(mut self) {
        self.cancel.cancel();
        let tasks = std::mem::take(&mut self._tasks);
        for handle in tasks {
            let _ = handle.await;
        }
    }
}

impl Drop for EventSubscriber {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
