//! Aggregated cluster view — subscribes to agent status/health streams
//! and builds a single cluster-wide picture of all nodes and assignments.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use mitiflow_storage::{NodeHealth, NodeMetadata, NodeStatus, StoreState};
use serde::{Deserialize, Serialize};
use tokio::sync::{RwLock, broadcast};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};
use zenoh::Session;

use crate::http::ClusterEvent;

/// Source of a partition assignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AssignmentSource {
    /// Determined by HRW hash on the agent side.
    Computed,
    /// Manually placed by orchestrator override.
    Override,
}

/// Information about a single partition assignment derived from agent status reports.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssignmentInfo {
    pub partition: u32,
    pub replica: u32,
    pub node_id: String,
    pub state: StoreState,
    pub source: AssignmentSource,
}

/// Aggregated information about a single node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub metadata: Option<NodeMetadata>,
    pub health: Option<NodeHealth>,
    pub status: Option<NodeStatus>,
    pub online: bool,
    pub last_seen: DateTime<Utc>,
}

/// Default timeout after which offline nodes are automatically evicted.
pub const DEFAULT_EVICTION_TIMEOUT: Duration = Duration::from_secs(300); // 5 minutes

/// Callback invoked when a node is evicted from the cluster view.
pub type EvictionCallback = Arc<dyn Fn(String) + Send + Sync>;

/// Aggregated cluster-wide view built from agent status/health streams.
pub struct ClusterView {
    nodes: Arc<RwLock<HashMap<String, NodeInfo>>>,
    session: Session,
    key_prefix: String,
    cancel: CancellationToken,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    /// Optional broadcast sender for SSE cluster events.
    /// Held to keep the channel alive; subscribers receive from cloned senders in tasks.
    #[allow(dead_code)]
    cluster_tx: Option<broadcast::Sender<ClusterEvent>>,
    /// Callback invoked when a node is evicted (used to clean up overrides, lag data, etc.)
    eviction_callback: Arc<RwLock<Option<EvictionCallback>>>,
}

impl ClusterView {
    /// Create a new ClusterView and start background subscribers.
    ///
    /// Subscribes to:
    /// - `{key_prefix}/_cluster/status/*` — agent status reports
    /// - `{key_prefix}/_cluster/health/*` — agent health metrics
    /// - `{key_prefix}/_agents/*` — agent liveliness tokens
    pub async fn new(
        session: &Session,
        key_prefix: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::new_inner(session, key_prefix, None, DEFAULT_EVICTION_TIMEOUT).await
    }

    /// Create a new ClusterView with a broadcast sender for SSE streaming.
    pub async fn new_with_broadcast(
        session: &Session,
        key_prefix: &str,
        tx: broadcast::Sender<ClusterEvent>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::new_inner(session, key_prefix, Some(tx), DEFAULT_EVICTION_TIMEOUT).await
    }

    /// Create a new ClusterView with a custom eviction timeout.
    pub async fn new_with_eviction(
        session: &Session,
        key_prefix: &str,
        cluster_tx: Option<broadcast::Sender<ClusterEvent>>,
        eviction_timeout: Duration,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::new_inner(session, key_prefix, cluster_tx, eviction_timeout).await
    }

    async fn new_inner(
        session: &Session,
        key_prefix: &str,
        cluster_tx: Option<broadcast::Sender<ClusterEvent>>,
        eviction_timeout: Duration,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let nodes: Arc<RwLock<HashMap<String, NodeInfo>>> = Arc::new(RwLock::new(HashMap::new()));
        let cancel = CancellationToken::new();
        let mut tasks = Vec::new();

        // --- Status subscriber ---
        let status_key = format!("{key_prefix}/_cluster/status/*");
        let status_sub = session.declare_subscriber(&status_key).await?;
        let nodes_status = Arc::clone(&nodes);
        let cancel_status = cancel.clone();
        let status_tx = cluster_tx.clone();
        tasks.push(tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_status.cancelled() => break,
                    result = status_sub.recv_async() => {
                        match result {
                            Ok(sample) => {
                                if let Ok(status) = serde_json::from_slice::<NodeStatus>(
                                    &sample.payload().to_bytes(),
                                ) {
                                    if let Some(ref tx) = status_tx {
                                        let _ = tx.send(ClusterEvent::NodeStatus {
                                            node_id: status.node_id.clone(),
                                            data: serde_json::to_value(&status).unwrap_or_default(),
                                        });
                                    }
                                    let mut map = nodes_status.write().await;
                                    let entry = map.entry(status.node_id.clone()).or_insert_with(|| NodeInfo {
                                        metadata: None,
                                        health: None,
                                        status: None,
                                        online: true,
                                        last_seen: Utc::now(),
                                    });
                                    entry.status = Some(status);
                                    entry.last_seen = Utc::now();
                                }
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
        }));

        // --- Health subscriber ---
        let health_key = format!("{key_prefix}/_cluster/health/*");
        let health_sub = session.declare_subscriber(&health_key).await?;
        let nodes_health = Arc::clone(&nodes);
        let cancel_health = cancel.clone();
        let health_tx = cluster_tx.clone();
        tasks.push(tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_health.cancelled() => break,
                    result = health_sub.recv_async() => {
                        match result {
                            Ok(sample) => {
                                if let Ok(health) = serde_json::from_slice::<NodeHealth>(
                                    &sample.payload().to_bytes(),
                                ) {
                                    if let Some(ref tx) = health_tx {
                                        let _ = tx.send(ClusterEvent::NodeHealth {
                                            node_id: health.node_id.clone(),
                                            data: serde_json::to_value(&health).unwrap_or_default(),
                                        });
                                    }
                                    let mut map = nodes_health.write().await;
                                    let entry = map.entry(health.node_id.clone()).or_insert_with(|| NodeInfo {
                                        metadata: None,
                                        health: None,
                                        status: None,
                                        online: true,
                                        last_seen: Utc::now(),
                                    });
                                    entry.health = Some(health);
                                    entry.last_seen = Utc::now();
                                }
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
        }));

        // --- Liveliness watcher for online/offline ---
        let agents_key = format!("{key_prefix}/_agents/*");

        // Initial discovery: get existing liveliness tokens
        let replies = session.liveliness().get(&agents_key).await?;
        while let Ok(reply) = replies.recv_async().await {
            if let Ok(sample) = reply.result() {
                let key = sample.key_expr().as_str();
                if let Some(node_id) = key.rsplit('/').next() {
                    // Skip metadata sub-keys
                    if node_id.contains('/') {
                        continue;
                    }
                    let mut map = nodes.write().await;
                    let entry = map.entry(node_id.to_string()).or_insert_with(|| NodeInfo {
                        metadata: None,
                        health: None,
                        status: None,
                        online: true,
                        last_seen: Utc::now(),
                    });
                    entry.online = true;
                    entry.last_seen = Utc::now();
                }
            }
        }

        // Watch for liveliness changes
        let live_sub = session.liveliness().declare_subscriber(&agents_key).await?;
        let nodes_live = Arc::clone(&nodes);
        let cancel_live = cancel.clone();
        let kp = key_prefix.to_string();
        let live_tx = cluster_tx.clone();
        tasks.push(tokio::spawn(async move {
            let prefix = format!("{kp}/_agents/");
            loop {
                tokio::select! {
                    _ = cancel_live.cancelled() => break,
                    result = live_sub.recv_async() => {
                        match result {
                            Ok(sample) => {
                                let key = sample.key_expr().as_str();
                                if let Some(node_id) = key.strip_prefix(&prefix) {
                                    // Skip metadata sub-keys
                                    if node_id.contains('/') {
                                        continue;
                                    }
                                    let is_online = sample.kind() == zenoh::sample::SampleKind::Put;
                                    if let Some(ref tx) = live_tx {
                                        let event = if is_online {
                                            ClusterEvent::NodeOnline {
                                                node_id: node_id.to_string(),
                                                timestamp: Utc::now().to_rfc3339(),
                                            }
                                        } else {
                                            ClusterEvent::NodeOffline {
                                                node_id: node_id.to_string(),
                                                timestamp: Utc::now().to_rfc3339(),
                                            }
                                        };
                                        let _ = tx.send(event);
                                    }
                                    let mut map = nodes_live.write().await;
                                    let entry = map.entry(node_id.to_string()).or_insert_with(|| NodeInfo {
                                        metadata: None,
                                        health: None,
                                        status: None,
                                        online: is_online,
                                        last_seen: Utc::now(),
                                    });
                                    entry.online = is_online;
                                    entry.last_seen = Utc::now();
                                    debug!(node_id, is_online, "liveliness change");
                                }
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
        }));

        // --- Periodic eviction of long-offline nodes ---
        let eviction_callback: Arc<RwLock<Option<EvictionCallback>>> = Arc::new(RwLock::new(None));
        if eviction_timeout > Duration::ZERO {
            let evict_nodes = Arc::clone(&nodes);
            let evict_cancel = cancel.clone();
            let evict_tx = cluster_tx.clone();
            let evict_cb = Arc::clone(&eviction_callback);
            tasks.push(tokio::spawn(async move {
                // Check every 1/10th of the eviction timeout, minimum 5s
                let check_interval = (eviction_timeout / 10).max(Duration::from_secs(5));
                let mut ticker = tokio::time::interval(check_interval);
                loop {
                    tokio::select! {
                        _ = evict_cancel.cancelled() => break,
                        _ = ticker.tick() => {
                            let now = Utc::now();
                            let threshold = chrono::TimeDelta::from_std(eviction_timeout)
                                .unwrap_or(chrono::TimeDelta::seconds(300));

                            // Phase 1: identify nodes to evict (under read lock would
                            // work, but we need write lock to remove them anyway).
                            let evicted_ids: Vec<String> = {
                                let mut map = evict_nodes.write().await;
                                let mut to_evict = Vec::new();
                                for (node_id, info) in map.iter() {
                                    if info.online {
                                        continue;
                                    }
                                    let age = now.signed_duration_since(info.last_seen);
                                    if age > threshold {
                                        to_evict.push(node_id.clone());
                                    }
                                }
                                for id in &to_evict {
                                    if let Some(info) = map.remove(id) {
                                        info!(
                                            node_id = id,
                                            last_seen = %info.last_seen,
                                            offline_secs = now.signed_duration_since(info.last_seen).num_seconds(),
                                            "evicting offline node from cluster view"
                                        );
                                    }
                                }
                                to_evict
                            };

                            // Phase 2: fire events and callbacks outside the lock.
                            let cb = evict_cb.read().await;
                            for node_id in &evicted_ids {
                                if let Some(ref tx) = evict_tx {
                                    let _ = tx.send(ClusterEvent::NodeOffline {
                                        node_id: node_id.clone(),
                                        timestamp: now.to_rfc3339(),
                                    });
                                }
                                if let Some(ref f) = *cb {
                                    f(node_id.clone());
                                }
                            }

                            if !evicted_ids.is_empty() {
                                debug!(evicted = evicted_ids.len(), "eviction sweep completed");
                            }
                        }
                    }
                }
            }));
        }

        Ok(Self {
            nodes,
            session: session.clone(),
            key_prefix: key_prefix.to_string(),
            cancel,
            tasks,
            cluster_tx,
            eviction_callback,
        })
    }

    /// Get a shared handle to the nodes map for use by other components (e.g., admin queryable).
    pub fn nodes_handle(&self) -> Arc<RwLock<HashMap<String, NodeInfo>>> {
        Arc::clone(&self.nodes)
    }

    /// Get a snapshot of all known nodes.
    pub async fn nodes(&self) -> HashMap<String, NodeInfo> {
        self.nodes.read().await.clone()
    }

    /// Derive the assignment table from all agent status reports.
    ///
    /// Returns `(partition, replica) → AssignmentInfo` built from each agent's
    /// published `NodeStatus.partitions`.
    pub async fn assignments(&self) -> HashMap<(u32, u32), AssignmentInfo> {
        let nodes = self.nodes.read().await;
        let mut result = HashMap::new();
        for (node_id, info) in nodes.iter() {
            if let Some(ref status) = info.status {
                for ps in &status.partitions {
                    result.insert(
                        (ps.partition, ps.replica),
                        AssignmentInfo {
                            partition: ps.partition,
                            replica: ps.replica,
                            node_id: node_id.clone(),
                            state: ps.state,
                            source: AssignmentSource::Computed,
                        },
                    );
                }
            }
        }
        result
    }

    /// Get IDs of all nodes currently marked online.
    pub async fn online_nodes(&self) -> Vec<String> {
        self.nodes
            .read()
            .await
            .iter()
            .filter(|(_, info)| info.online)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Check if a specific node is online.
    pub async fn is_node_online(&self, node_id: &str) -> bool {
        self.nodes
            .read()
            .await
            .get(node_id)
            .map(|info| info.online)
            .unwrap_or(false)
    }

    /// Get the number of online nodes.
    pub async fn online_count(&self) -> usize {
        self.nodes
            .read()
            .await
            .values()
            .filter(|info| info.online)
            .count()
    }

    /// Access the underlying session.
    pub fn session(&self) -> &Session {
        &self.session
    }

    /// Access the key prefix.
    pub fn key_prefix(&self) -> &str {
        &self.key_prefix
    }

    /// Register a callback invoked when a node is evicted.
    pub async fn set_eviction_callback(&self, cb: EvictionCallback) {
        *self.eviction_callback.write().await = Some(cb);
    }

    /// Manually evict a specific node from the cluster view.
    ///
    /// Returns `true` if the node was found and removed.
    pub async fn evict_node(&self, node_id: &str) -> bool {
        let removed = {
            let mut map = self.nodes.write().await;
            map.remove(node_id).is_some()
        };
        if removed {
            info!(node_id, "node manually evicted from cluster view");
            if let Some(ref cb) = *self.eviction_callback.read().await {
                cb(node_id.to_string());
            }
        }
        removed
    }

    /// Shut down all background tasks.
    pub async fn shutdown(self) {
        self.cancel.cancel();
        for task in self.tasks {
            let _ = task.await;
        }
    }
}
