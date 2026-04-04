//! Consumer group lag monitoring.
//!
//! Subscribes to `_watermark/*` and `_offsets/**`, computes per-(group, partition, publisher)
//! lag, and publishes to `_lag/{group_id}/{partition}`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::{RwLock, broadcast};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use zenoh::Session;

use mitiflow::store::OffsetCommit;
use mitiflow::store::watermark::CommitWatermark;
use mitiflow::types::PublisherId;

/// Per-(group, partition) lag report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LagReport {
    pub group_id: String,
    pub partition: u32,
    /// Per-publisher lag: watermark_seq - committed_offset_seq.
    pub publishers: HashMap<PublisherId, u64>,
    /// Sum of per-publisher lag.
    pub total: u64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

type OffsetMap = Arc<RwLock<HashMap<(String, u32, PublisherId), u64>>>;

type WaterMarkMap = Arc<RwLock<HashMap<(u32, PublisherId), u64>>>;

/// Lag monitor that aggregates watermarks and committed offsets.
pub struct LagMonitor {
    #[allow(dead_code)]
    session: Session,
    #[allow(dead_code)]
    key_prefix: String,
    /// Latest watermark per (partition, publisher).
    watermarks: WaterMarkMap,
    /// Latest committed offset per (group_id, partition, publisher).
    offsets: OffsetMap,
    cancel: CancellationToken,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    /// Optional broadcast sender for SSE lag streaming.
    /// Held to keep the channel alive; subscribers receive from cloned senders in tasks.
    #[allow(dead_code)]
    lag_tx: Option<broadcast::Sender<LagReport>>,
}

impl LagMonitor {
    /// Create and start a lag monitor.
    pub async fn new(
        session: &Session,
        key_prefix: &str,
        publish_interval: Duration,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::new_inner(session, key_prefix, publish_interval, None).await
    }

    /// Create and start a lag monitor with a broadcast sender for SSE streaming.
    pub async fn new_with_broadcast(
        session: &Session,
        key_prefix: &str,
        publish_interval: Duration,
        tx: broadcast::Sender<LagReport>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::new_inner(session, key_prefix, publish_interval, Some(tx)).await
    }

    async fn new_inner(
        session: &Session,
        key_prefix: &str,
        publish_interval: Duration,
        lag_tx: Option<broadcast::Sender<LagReport>>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let cancel = CancellationToken::new();
        let watermarks: Arc<RwLock<HashMap<(u32, PublisherId), u64>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let offsets: OffsetMap = Arc::new(RwLock::new(HashMap::new()));

        let mut tasks = Vec::new();

        // Subscribe to watermarks
        let wm_sub = session
            .declare_subscriber(format!("{key_prefix}/_watermark/*"))
            .await?;
        let wm_cancel = cancel.clone();
        let wm_map = Arc::clone(&watermarks);
        tasks.push(tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = wm_cancel.cancelled() => break,
                    result = wm_sub.recv_async() => {
                        match result {
                            Ok(sample) => {
                                let payload = sample.payload().to_bytes();
                                if let Ok(wm) = serde_json::from_slice::<CommitWatermark>(&payload) {
                                    let mut map = wm_map.write().await;
                                    for (pub_id, pw) in &wm.publishers {
                                        let entry = map.entry((wm.partition, *pub_id)).or_insert(0);
                                        if pw.committed_seq > *entry {
                                            *entry = pw.committed_seq;
                                        }
                                    }
                                }
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
            debug!("lag monitor watermark subscriber stopped");
        }));

        // Subscribe to offset commits
        let offset_sub = session
            .declare_subscriber(format!("{key_prefix}/_offsets/**"))
            .await?;
        let offset_cancel = cancel.clone();
        let offset_map = Arc::clone(&offsets);
        tasks.push(tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = offset_cancel.cancelled() => break,
                    result = offset_sub.recv_async() => {
                        match result {
                            Ok(sample) => {
                                let payload = sample.payload().to_bytes();
                                if let Ok(commit) = serde_json::from_slice::<OffsetCommit>(&payload) {
                                    let mut map = offset_map.write().await;
                                    for (pub_id, seq) in &commit.offsets {
                                        let key = (commit.group_id.clone(), commit.partition, *pub_id);
                                        let entry = map.entry(key).or_insert(0);
                                        if *seq > *entry {
                                            *entry = *seq;
                                        }
                                    }
                                }
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
            debug!("lag monitor offset subscriber stopped");
        }));

        // Periodic lag publisher
        let lag_cancel = cancel.clone();
        let lag_session = session.clone();
        let lag_prefix = key_prefix.to_string();
        let lag_wm = Arc::clone(&watermarks);
        let lag_offsets = Arc::clone(&offsets);
        let broadcast_tx = lag_tx.clone();
        tasks.push(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(publish_interval);
            loop {
                tokio::select! {
                    _ = lag_cancel.cancelled() => break,
                    _ = ticker.tick() => {
                        let wm = lag_wm.read().await;
                        let offsets = lag_offsets.read().await;

                        // Group offsets by (group_id, partition)
                        let mut groups: HashMap<(String, u32), HashMap<PublisherId, u64>> =
                            HashMap::new();
                        for ((group_id, partition, pub_id), offset_seq) in offsets.iter() {
                            let entry = groups
                                .entry((group_id.clone(), *partition))
                                .or_default();
                            let wm_seq = wm.get(&(*partition, *pub_id)).copied().unwrap_or(0);
                            let lag = wm_seq.saturating_sub(*offset_seq);
                            entry.insert(*pub_id, lag);
                        }

                        for ((group_id, partition), publishers) in &groups {
                            let total: u64 = publishers.values().sum();
                            let report = LagReport {
                                group_id: group_id.clone(),
                                partition: *partition,
                                publishers: publishers.clone(),
                                total,
                                timestamp: chrono::Utc::now(),
                            };
                            // Broadcast for SSE
                            if let Some(ref tx) = broadcast_tx {
                                let _ = tx.send(report.clone());
                            }
                            if let Ok(bytes) = serde_json::to_vec(&report) {
                                let key = format!(
                                    "{lag_prefix}/_lag/{group_id}/{partition}"
                                );
                                if let Err(e) = lag_session.put(&key, bytes).await {
                                    warn!("failed to publish lag report: {e}");
                                }
                            }
                        }
                    }
                }
            }
            debug!("lag monitor publisher stopped");
        }));

        Ok(Self {
            session: session.clone(),
            key_prefix: key_prefix.to_string(),
            watermarks,
            offsets,
            cancel,
            tasks,
            lag_tx,
        })
    }

    /// Get current lag for a specific group.
    pub async fn get_group_lag(&self, group_id: &str) -> Vec<LagReport> {
        let wm = self.watermarks.read().await;
        let offsets = self.offsets.read().await;

        let mut by_partition: HashMap<u32, HashMap<PublisherId, u64>> = HashMap::new();

        for ((gid, partition, pub_id), offset_seq) in offsets.iter() {
            if gid != group_id {
                continue;
            }
            let wm_seq = wm.get(&(*partition, *pub_id)).copied().unwrap_or(0);
            let lag = wm_seq.saturating_sub(*offset_seq);
            by_partition
                .entry(*partition)
                .or_default()
                .insert(*pub_id, lag);
        }

        by_partition
            .into_iter()
            .map(|(partition, publishers)| {
                let total: u64 = publishers.values().sum();
                LagReport {
                    group_id: group_id.to_string(),
                    partition,
                    publishers,
                    total,
                    timestamp: chrono::Utc::now(),
                }
            })
            .collect()
    }

    /// Return distinct consumer group IDs from known offsets.
    pub async fn known_groups(&self) -> Vec<String> {
        let offsets = self.offsets.read().await;
        let mut groups: Vec<String> = offsets
            .keys()
            .map(|(gid, _, _)| gid.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        groups.sort();
        groups
    }

    /// Return publisher info derived from watermarks.
    pub async fn get_publishers(&self) -> Vec<(PublisherId, Vec<u32>)> {
        let wm = self.watermarks.read().await;
        let mut by_pub: HashMap<PublisherId, Vec<u32>> = HashMap::new();
        for (partition, pub_id) in wm.keys() {
            by_pub.entry(*pub_id).or_default().push(*partition);
        }
        for partitions in by_pub.values_mut() {
            partitions.sort();
            partitions.dedup();
        }
        by_pub.into_iter().collect()
    }

    /// Shut down the lag monitor.
    pub async fn shutdown(mut self) {
        self.cancel.cancel();
        for handle in self.tasks.drain(..) {
            let _ = handle.await;
        }
    }
}

impl Drop for LagMonitor {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
