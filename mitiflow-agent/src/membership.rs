use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use mitiflow::partition::hash_ring::NodeDescriptor;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use zenoh::Session;

use crate::config::StorageAgentConfig;
use crate::error::AgentResult;
use crate::types::NodeMetadata;

/// Event describing a membership change.
#[derive(Debug, Clone)]
pub enum MembershipEvent {
    Joined(String),
    Left(String),
}

/// Callback type for membership changes.
pub type MembershipCallback =
    Arc<RwLock<Option<Box<dyn Fn(&[NodeDescriptor], &MembershipEvent) + Send + Sync>>>>;

/// Tracks storage agent membership via Zenoh liveliness tokens.
pub struct MembershipTracker {
    _session: Session,
    /// Known peer nodes (excluding self).
    nodes: Arc<RwLock<HashMap<String, NodeDescriptor>>>,
    /// Self node descriptor.
    self_node: NodeDescriptor,
    /// Background task handle.
    _task: tokio::task::JoinHandle<()>,
    /// Cancellation token for the background task.
    cancel: CancellationToken,
    /// Callback fired on membership changes.
    callback: MembershipCallback,
    /// Key prefix for agent liveliness.
    _agents_prefix: String,
}

impl MembershipTracker {
    /// Create a new membership tracker and start the background watcher.
    pub async fn new(session: &Session, config: &StorageAgentConfig) -> AgentResult<Self> {
        let agents_prefix = config.agents_prefix();
        let self_node = NodeDescriptor {
            id: config.node_id.clone(),
            capacity: config.capacity,
            labels: config.labels.clone(),
        };

        // Declare liveliness token so other agents see us.
        let token_key = config.agent_liveliness_key();
        let _token = session.liveliness().declare_token(&token_key).await?;

        // Set up metadata queryable so other agents can fetch our info.
        let meta = NodeMetadata {
            node_id: config.node_id.clone(),
            capacity: config.capacity,
            labels: config.labels.clone(),
            started_at: Utc::now(),
        };
        let meta_key = config.agent_metadata_key();
        let meta_bytes = serde_json::to_vec(&meta)?;
        let meta_queryable = session.declare_queryable(&meta_key).await?;

        // Discover existing agents.
        let nodes: Arc<RwLock<HashMap<String, NodeDescriptor>>> =
            Arc::new(RwLock::new(HashMap::new()));

        let replies = session
            .liveliness()
            .get(&format!("{agents_prefix}/*"))
            .await?;
        while let Ok(reply) = replies.recv_async().await {
            if let Ok(sample) = reply.result() {
                let key = sample.key_expr().as_str();
                if let Some(node_id) = key.strip_prefix(&format!("{agents_prefix}/")) {
                    // Skip self and metadata subkeys.
                    if node_id == config.node_id || node_id.contains('/') {
                        continue;
                    }
                    // Try to fetch metadata for the discovered node.
                    let desc = Self::fetch_node_descriptor(
                        session,
                        &agents_prefix,
                        node_id,
                    )
                    .await;
                    nodes.write().await.insert(node_id.to_string(), desc);
                }
            }
        }

        let cancel = CancellationToken::new();
        let callback: MembershipCallback = Arc::new(RwLock::new(None));

        // Spawn background watcher task.
        let task = {
            let session = session.clone();
            let agents_prefix = agents_prefix.clone();
            let self_id = config.node_id.clone();
            let nodes = Arc::clone(&nodes);
            let cancel = cancel.clone();
            let callback = Arc::clone(&callback);
            let _token = _token;

            tokio::spawn(async move {
                let _token = _token; // Keep liveliness token alive.
                // Also run the metadata queryable loop.
                let meta_task = tokio::spawn({
                    let cancel = cancel.clone();
                    let meta_key = meta_key;
                    let meta_bytes = meta_bytes;
                    async move {
                        loop {
                            tokio::select! {
                                _ = cancel.cancelled() => break,
                                result = meta_queryable.recv_async() => {
                                    match result {
                                        Ok(query) => {
                                            let _ = query.reply(&meta_key, meta_bytes.clone()).await;
                                        }
                                        Err(_) => break,
                                    }
                                }
                            }
                        }
                    }
                });
                if let Err(e) = Self::watch_loop(
                    &session,
                    &agents_prefix,
                    &self_id,
                    &nodes,
                    &callback,
                    &cancel,
                )
                .await
                {
                    warn!("membership watcher exited: {e}");
                }
                meta_task.abort();
                debug!("membership tracker stopped for {self_id}");
            })
        };

        let peer_count = nodes.read().await.len();
        info!(
            node_id = %config.node_id,
            peers = peer_count,
            "membership tracker started"
        );

        Ok(Self {
            _session: session.clone(),
            nodes,
            self_node,
            _task: task,
            cancel,
            callback,
            _agents_prefix: agents_prefix,
        })
    }

    /// Register a callback for membership changes.
    pub async fn on_change(
        &self,
        cb: impl Fn(&[NodeDescriptor], &MembershipEvent) + Send + Sync + 'static,
    ) {
        let mut guard = self.callback.write().await;
        *guard = Some(Box::new(cb));
    }

    /// Snapshot of all known nodes (including self).
    pub async fn current_nodes(&self) -> Vec<NodeDescriptor> {
        let peers = self.nodes.read().await;
        let mut all: Vec<NodeDescriptor> = vec![self.self_node.clone()];
        all.extend(peers.values().cloned());
        all
    }

    /// Snapshot of peer nodes only (excluding self).
    pub async fn peer_nodes(&self) -> Vec<NodeDescriptor> {
        self.nodes.read().await.values().cloned().collect()
    }

    /// The self node descriptor.
    pub fn self_node(&self) -> &NodeDescriptor {
        &self.self_node
    }

    /// Shutdown the background watcher.
    pub async fn shutdown(&self) {
        self.cancel.cancel();
    }

    // --- Internal ---

    async fn watch_loop(
        session: &Session,
        agents_prefix: &str,
        self_id: &str,
        nodes: &Arc<RwLock<HashMap<String, NodeDescriptor>>>,
        callback: &MembershipCallback,
        cancel: &CancellationToken,
    ) -> AgentResult<()> {
        let subscriber = session
            .liveliness()
            .declare_subscriber(&format!("{agents_prefix}/*"))
            .await?;

        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                result = subscriber.recv_async() => {
                    match result {
                        Ok(sample) => {
                            let key = sample.key_expr().as_str().to_string();
                            let Some(node_id) = key.strip_prefix(&format!("{agents_prefix}/")) else {
                                continue;
                            };
                            // Skip self and metadata subkeys.
                            if node_id == self_id || node_id.contains('/') {
                                continue;
                            }
                            let node_id = node_id.to_string();

                            match sample.kind() {
                                zenoh::sample::SampleKind::Put => {
                                    let desc = Self::fetch_node_descriptor(
                                        session,
                                        agents_prefix,
                                        &node_id,
                                    ).await;
                                    nodes.write().await.insert(node_id.clone(), desc);

                                    info!(node_id = %node_id, "agent joined");
                                    let event = MembershipEvent::Joined(node_id);
                                    Self::fire_callback(nodes, callback, &event).await;
                                }
                                zenoh::sample::SampleKind::Delete => {
                                    nodes.write().await.remove(&node_id);

                                    info!(node_id = %node_id, "agent left");
                                    let event = MembershipEvent::Left(node_id);
                                    Self::fire_callback(nodes, callback, &event).await;
                                }
                            }
                        }
                        Err(_) => break,
                    }
                }
            }
        }
        Ok(())
    }

    async fn fire_callback(
        nodes: &Arc<RwLock<HashMap<String, NodeDescriptor>>>,
        callback: &MembershipCallback,
        event: &MembershipEvent,
    ) {
        let guard = callback.read().await;
        if let Some(cb) = guard.as_ref() {
            let all: Vec<NodeDescriptor> = nodes.read().await.values().cloned().collect();
            cb(&all, event);
        }
    }

    async fn fetch_node_descriptor(
        session: &Session,
        agents_prefix: &str,
        node_id: &str,
    ) -> NodeDescriptor {
        let meta_key = format!("{agents_prefix}/{node_id}/meta");
        if let Ok(replies) = session
            .get(&meta_key)
            .timeout(std::time::Duration::from_secs(2))
            .await
        {
            while let Ok(reply) = replies.recv_async().await {
                if let Ok(sample) = reply.result() {
                    let payload = sample.payload().to_bytes();
                    if let Ok(meta) = serde_json::from_slice::<NodeMetadata>(&payload) {
                        return NodeDescriptor {
                            id: meta.node_id,
                            capacity: meta.capacity,
                            labels: meta.labels,
                        };
                    }
                }
            }
        }
        // Fallback: default descriptor if metadata not available.
        NodeDescriptor::new(node_id)
    }
}
