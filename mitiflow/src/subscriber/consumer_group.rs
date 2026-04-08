//! Consumer group subscriber with offset commit/fetch and auto-commit.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use zenoh::Session;

use crate::config::{CommitMode, ConsumerGroupConfig, EventBusConfig};
use crate::error::{Error, Result};
use crate::event::Event;
use crate::store::offset::OffsetCommit;
use crate::subscriber::gap_detector::{GapDetector, SequenceTracker};
use crate::types::PublisherId;

use serde::Serialize;
use serde::de::DeserializeOwned;

/// Consumer group subscriber with offset management.
///
/// Wraps an [`EventSubscriber`](super::EventSubscriber) and adds:
/// - Partition assignment via [`PartitionManager`](crate::partition::PartitionManager)
/// - Offset commit (`commit_sync`, `commit_async`)
/// - Offset fetch on rebalance
/// - Auto-commit background task
pub struct ConsumerGroupSubscriber {
    /// Zenoh session for offset queries.
    session: Session,
    /// Config snapshot.
    config: EventBusConfig,
    /// Group configuration.
    group_config: ConsumerGroupConfig,
    /// Current generation from the PartitionManager.
    generation: Arc<std::sync::atomic::AtomicU64>,
    /// Assigned partitions.
    assigned_partitions: Arc<RwLock<Vec<u32>>>,
    /// Per-(publisher, partition) last-seen sequence — shared with the subscriber.
    /// Updated by the internal subscribe loop, read by commit.
    positions: Arc<RwLock<HashMap<(PublisherId, u32), u64>>>,
    /// Internal event channel receiver.
    event_rx: flume::Receiver<crate::event::RawEvent>,
    /// Cancellation token.
    cancel: CancellationToken,
    /// Background task handles.
    _tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl ConsumerGroupSubscriber {
    /// Create a consumer group subscriber.
    ///
    /// Joins the group via `PartitionManager`, fetches committed offsets from
    /// the EventStore, and begins consuming from assigned partitions.
    pub async fn new(
        session: &Session,
        mut config: EventBusConfig,
        group_config: ConsumerGroupConfig,
    ) -> Result<Self> {
        use crate::partition::PartitionManager;

        // Resolve topic schema (AutoConfig applies orchestrator partition count, codec, etc.)
        crate::schema::resolve_schema(session, &mut config).await?;

        let pm_config = config.clone();
        let pm_config = EventBusConfig::builder(&pm_config.key_prefix)
            .num_partitions(config.num_partitions)
            .worker_id(&group_config.member_id)
            .worker_liveliness_prefix(format!(
                "{}/_workers/{}",
                config.key_prefix, group_config.group_id
            ))
            .heartbeat(config.heartbeat.clone())
            .cache_size(config.cache_size)
            .recovery_mode(config.recovery_mode.clone())
            .history_on_subscribe(config.history_on_subscribe)
            .watermark_interval(config.watermark_interval)
            .build()?;

        let pm = PartitionManager::new(session, pm_config).await?;
        // Brief settle time for initial partition discovery
        tokio::time::sleep(Duration::from_millis(100)).await;

        let my_parts = pm.my_partitions().await;
        let generation = Arc::new(std::sync::atomic::AtomicU64::new(pm.current_generation()));

        // Fetch committed offsets for each assigned partition
        let mut checkpoints: HashMap<(PublisherId, u32), u64> = HashMap::new();
        for &partition in &my_parts {
            let offsets = fetch_offsets_from_store(
                session,
                &config.key_prefix,
                partition,
                &group_config.group_id,
            )
            .await;
            for (pub_id, seq) in offsets {
                checkpoints.insert((pub_id, partition), seq);
            }
        }

        // Create subscriber scoped to assigned partitions, pre-seeded with checkpoints.
        let key_exprs: Vec<String> = if my_parts.is_empty() {
            vec![format!("{}/_none", config.key_prefix)]
        } else {
            my_parts
                .iter()
                .map(|p| format!("{}/p/{p}/**", config.key_prefix))
                .collect()
        };

        info!(
            group = %group_config.group_id,
            member = %group_config.member_id,
            partitions = ?my_parts,
            key_exprs = ?key_exprs,
            "consumer group: subscribing to partitions"
        );

        let cancel = CancellationToken::new();
        let (event_tx, event_rx) = flume::unbounded::<crate::event::RawEvent>();
        let positions: Arc<RwLock<HashMap<(PublisherId, u32), u64>>> =
            Arc::new(RwLock::new(checkpoints.clone()));
        let assigned_partitions = Arc::new(RwLock::new(my_parts));

        let mut tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();

        // Fan-in channel for decoded samples
        let (sample_tx, sample_rx) =
            flume::unbounded::<(crate::attachment::EventMeta, String, Vec<u8>)>();

        for ke in &key_exprs {
            let sub = session.declare_subscriber(ke.as_str()).await?;
            let tx = sample_tx.clone();
            let cancel_c = cancel.clone();
            tasks.push(tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = cancel_c.cancelled() => break,
                        result = sub.recv_async() => {
                            match result {
                                Ok(sample) => {
                                    if let Some(decoded) = super::forwarder::decode_sample(&sample) {
                                        let _ = tx.send(decoded);
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                    }
                }
            }));
        }
        drop(sample_tx);

        // Gap detection task with pre-seeded checkpoints
        let positions_clone = Arc::clone(&positions);
        let cancel_clone = cancel.clone();
        let sess = session.clone();
        let rc = Arc::new(super::recovery::RecoveryConfig::from_bus_config(&config));

        let handle = tokio::spawn(async move {
            let mut gd = GapDetector::with_checkpoints(checkpoints);

            loop {
                tokio::select! {
                    _ = cancel_clone.cancelled() => break,
                    sample_result = sample_rx.recv_async() => {
                        match sample_result {
                            Ok((meta, key, payload)) => {
                                let partition = crate::attachment::extract_partition(&key);
                                let result = gd.on_sample(&meta.pub_id, partition, meta.seq);
                                super::pipeline::handle_sample_result(
                                    result, &meta, &key, &payload,
                                    &event_tx, &sess, &rc,
                                );
                                // Track positions for commits
                                let mut pos = positions_clone.write().await;
                                let entry = pos.entry((meta.pub_id, partition))
                                    .or_insert(0);
                                if meta.seq > *entry {
                                    *entry = meta.seq;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
            debug!("consumer group subscriber task stopped");
        });
        tasks.push(handle);

        // Set up rebalance callback
        let pm_gen = generation.clone();
        let pm_session = session.clone();
        let pm_positions = Arc::clone(&positions);
        let pm_group_id = group_config.group_id.clone();
        let pm_member_id = group_config.member_id.clone();
        let pm_key_prefix = config.key_prefix.clone();
        let pm_assigned = Arc::clone(&assigned_partitions);

        pm.on_rebalance(move |gained, lost| {
            pm_gen.store(
                pm_gen.load(std::sync::atomic::Ordering::SeqCst) + 1,
                std::sync::atomic::Ordering::SeqCst,
            );
            let session = pm_session.clone();
            let positions = pm_positions.clone();
            let group_id = pm_group_id.clone();
            let member_id = pm_member_id.clone();
            let key_prefix = pm_key_prefix.clone();
            let assigned = pm_assigned.clone();
            let gained = gained.to_vec();
            let lost = lost.to_vec();
            let current_gen = pm_gen.load(std::sync::atomic::Ordering::SeqCst);

            tokio::spawn(async move {
                // Commit offsets for lost partitions
                for &partition in &lost {
                    let pos = positions.read().await;
                    let offsets: HashMap<PublisherId, u64> = pos
                        .iter()
                        .filter(|((_, p), _)| *p == partition)
                        .map(|((pub_id, _), seq)| (*pub_id, *seq))
                        .collect();
                    drop(pos);

                    if !offsets.is_empty() {
                        let commit = OffsetCommit {
                            group_id: group_id.clone(),
                            member_id: member_id.clone(),
                            partition,
                            offsets,
                            generation: current_gen,
                            timestamp: Utc::now(),
                        };
                        if let Ok(bytes) = serde_json::to_vec(&commit) {
                            let key = format!("{key_prefix}/_offsets/{partition}/{group_id}");
                            let _ = session.put(&key, bytes).await;
                        }
                    }
                }

                // Load offsets for gained partitions
                for &partition in &gained {
                    let offsets =
                        fetch_offsets_from_store(&session, &key_prefix, partition, &group_id).await;
                    let mut pos = positions.write().await;
                    for (pub_id, seq) in offsets {
                        pos.insert((pub_id, partition), seq);
                    }
                }

                // Update assigned partitions
                let mut assigned_guard = assigned.write().await;
                assigned_guard.retain(|p| !lost.contains(p));
                for &p in &gained {
                    if !assigned_guard.contains(&p) {
                        assigned_guard.push(p);
                    }
                }
                tracing::info!(
                    group = %group_id,
                    gained = ?gained,
                    lost = ?lost,
                    current = ?*assigned_guard,
                    "consumer group: rebalance updated subscribed partitions"
                );
            });
        })
        .await;

        // Auto-commit task if configured
        if let CommitMode::Auto { interval } = &group_config.commit_mode {
            let interval = *interval;
            let cancel_c = cancel.clone();
            let sess = session.clone();
            let cfg = config.clone();
            let gc = group_config.clone();
            let pos = Arc::clone(&positions);
            let assigned = Arc::clone(&assigned_partitions);
            let generation_clone = Arc::clone(&generation);

            tasks.push(tokio::spawn(async move {
                auto_commit_loop(
                    &sess,
                    &cfg,
                    &gc,
                    pos,
                    assigned,
                    generation_clone,
                    interval,
                    cancel_c,
                )
                .await;
            }));
        }

        // Keep PartitionManager alive
        let _pm_cancel = cancel.clone();
        tasks.push(tokio::spawn(async move {
            _pm_cancel.cancelled().await;
            drop(pm);
        }));

        Ok(Self {
            session: session.clone(),
            config,
            group_config,
            generation,
            assigned_partitions,
            positions,
            event_rx,
            cancel,
            _tasks: tasks,
        })
    }

    /// Receive the next event, deserializing the payload into type `T`.
    pub async fn recv<T: Serialize + DeserializeOwned>(&self) -> Result<Event<T>> {
        let raw = self
            .event_rx
            .recv_async()
            .await
            .map_err(|_| Error::ChannelClosed)?;
        raw.deserialize_with(self.config.codec)
    }

    /// Receive the next event as raw bytes.
    pub async fn recv_raw(&self) -> Result<crate::event::RawEvent> {
        self.event_rx
            .recv_async()
            .await
            .map_err(|_| Error::ChannelClosed)
    }

    /// Synchronous commit: sends offset as a query, waits for store ACK.
    pub async fn commit_sync(&self) -> Result<()> {
        let partitions = self.assigned_partitions.read().await.clone();
        for partition in partitions {
            self.commit_partition_sync(partition).await?;
        }
        Ok(())
    }

    /// Commit offsets for a specific partition synchronously.
    pub async fn commit_partition_sync(&self, partition: u32) -> Result<()> {
        let pos = self.positions.read().await;
        let offsets: HashMap<PublisherId, u64> = pos
            .iter()
            .filter(|((_, p), _)| *p == partition)
            .map(|((pub_id, _), seq)| (*pub_id, *seq))
            .collect();
        drop(pos);

        if offsets.is_empty() {
            return Ok(());
        }

        let commit = OffsetCommit {
            group_id: self.group_config.group_id.clone(),
            member_id: self.group_config.member_id.clone(),
            partition,
            offsets,
            generation: self.generation.load(std::sync::atomic::Ordering::SeqCst),
            timestamp: Utc::now(),
        };

        let key = format!(
            "{}/_offsets/{}/{}",
            self.config.key_prefix, partition, self.group_config.group_id
        );
        let payload = serde_json::to_vec(&commit)?;
        let replies = self.session.get(&key).payload(payload).await?;

        // Wait for reply (ACK)
        while let Ok(reply) = replies.recv_async().await {
            if let Ok(sample) = reply.result() {
                let body = sample.payload().to_bytes();
                let body_str = std::str::from_utf8(&body).unwrap_or("");
                if let Some(msg) = body_str.strip_prefix("error:") {
                    if msg.contains("stale fenced commit") {
                        // Parse as a fenced error
                        return Err(Error::StaleFencedCommit {
                            group: self.group_config.group_id.clone(),
                            commit_gen: commit.generation,
                            stored_gen: 0, // exact gen not available from error msg
                        });
                    }
                    return Err(Error::StoreError(msg.to_string()));
                }
            }
        }
        Ok(())
    }

    /// Async commit: fire-and-forget put, no durability guarantee.
    pub async fn commit_async(&self) -> Result<()> {
        let partitions = self.assigned_partitions.read().await.clone();
        for partition in partitions {
            let pos = self.positions.read().await;
            let offsets: HashMap<PublisherId, u64> = pos
                .iter()
                .filter(|((_, p), _)| *p == partition)
                .map(|((pub_id, _), seq)| (*pub_id, *seq))
                .collect();
            drop(pos);

            if offsets.is_empty() {
                continue;
            }

            let commit = OffsetCommit {
                group_id: self.group_config.group_id.clone(),
                member_id: self.group_config.member_id.clone(),
                partition,
                offsets,
                generation: self.generation.load(std::sync::atomic::Ordering::SeqCst),
                timestamp: Utc::now(),
            };

            let key = format!(
                "{}/_offsets/{}/{}",
                self.config.key_prefix, partition, self.group_config.group_id
            );
            let payload = serde_json::to_vec(&commit)?;
            self.session.put(&key, payload).await?;
        }
        Ok(())
    }

    /// Fetch committed offsets for a partition from the store.
    pub async fn load_offsets(&self, partition: u32) -> Result<HashMap<PublisherId, u64>> {
        Ok(fetch_offsets_from_store(
            &self.session,
            &self.config.key_prefix,
            partition,
            &self.group_config.group_id,
        )
        .await)
    }

    /// Currently assigned partitions.
    pub async fn assigned_partitions(&self) -> Vec<u32> {
        self.assigned_partitions.read().await.clone()
    }

    /// Current generation.
    pub fn current_generation(&self) -> u64 {
        self.generation.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Group config.
    pub fn group_config(&self) -> &ConsumerGroupConfig {
        &self.group_config
    }

    /// Gracefully shut down.
    pub async fn shutdown(mut self) {
        // Final commit on shutdown
        let _ = self.commit_async().await;
        self.cancel.cancel();
        let tasks = std::mem::take(&mut self._tasks);
        for handle in tasks {
            let _ = handle.await;
        }
    }
}

impl Drop for ConsumerGroupSubscriber {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Fetch offsets from the store via Zenoh query.
async fn fetch_offsets_from_store(
    session: &Session,
    key_prefix: &str,
    partition: u32,
    group_id: &str,
) -> HashMap<PublisherId, u64> {
    let selector =
        format!("{key_prefix}/_offsets/{partition}/{group_id}?fetch=true&group_id={group_id}");
    let mut result = HashMap::new();
    match session.get(&selector).await {
        Ok(replies) => {
            while let Ok(reply) = replies.recv_async().await {
                if let Ok(sample) = reply.result() {
                    let payload = sample.payload().to_bytes();
                    if let Ok(commit) = serde_json::from_slice::<OffsetCommit>(&payload) {
                        for (pub_id, seq) in commit.offsets {
                            result.insert(pub_id, seq);
                        }
                    }
                }
            }
        }
        Err(e) => {
            warn!("failed to fetch offsets: {e}");
        }
    }
    result
}

/// Background auto-commit loop.
#[allow(clippy::too_many_arguments)]
async fn auto_commit_loop(
    session: &Session,
    config: &EventBusConfig,
    group_config: &ConsumerGroupConfig,
    positions: Arc<RwLock<HashMap<(PublisherId, u32), u64>>>,
    assigned_partitions: Arc<RwLock<Vec<u32>>>,
    generation: Arc<std::sync::atomic::AtomicU64>,
    interval: Duration,
    cancel: CancellationToken,
) {
    let mut ticker = tokio::time::interval(interval);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                // Final commit on shutdown
                do_commit_async(
                    session, config, group_config,
                    &positions, &assigned_partitions, &generation,
                ).await;
                break;
            }
            _ = ticker.tick() => {
                do_commit_async(
                    session, config, group_config,
                    &positions, &assigned_partitions, &generation,
                ).await;
            }
        }
    }
}

async fn do_commit_async(
    session: &Session,
    config: &EventBusConfig,
    group_config: &ConsumerGroupConfig,
    positions: &Arc<RwLock<HashMap<(PublisherId, u32), u64>>>,
    assigned_partitions: &Arc<RwLock<Vec<u32>>>,
    generation: &Arc<std::sync::atomic::AtomicU64>,
) {
    let partitions = assigned_partitions.read().await.clone();
    for partition in partitions {
        let pos = positions.read().await;
        let offsets: HashMap<PublisherId, u64> = pos
            .iter()
            .filter(|((_, p), _)| *p == partition)
            .map(|((pub_id, _), seq)| (*pub_id, *seq))
            .collect();
        drop(pos);

        if offsets.is_empty() {
            continue;
        }

        let commit = OffsetCommit {
            group_id: group_config.group_id.clone(),
            member_id: group_config.member_id.clone(),
            partition,
            offsets,
            generation: generation.load(std::sync::atomic::Ordering::SeqCst),
            timestamp: Utc::now(),
        };

        let key = format!(
            "{}/_offsets/{}/{}",
            config.key_prefix, partition, group_config.group_id
        );
        if let Ok(payload) = serde_json::to_vec(&commit)
            && let Err(e) = session.put(&key, payload).await
        {
            warn!("auto-commit failed: {e}");
        }
    }
}
