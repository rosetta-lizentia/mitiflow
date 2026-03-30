//! Main orchestrator that ties together config management, lag monitoring,
//! store lifecycle tracking, cluster view, override management,
//! and the admin API (Zenoh queryable).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use tokio::sync::{RwLock, broadcast};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use zenoh::Session;

use crate::cluster_view::ClusterView;
use crate::config::ConfigStore;
use crate::http::{ClusterEvent, EventSummary, HttpState};
use crate::lag::{LagMonitor, LagReport};
use crate::lifecycle::StoreTracker;
use crate::override_manager::OverrideManager;
use crate::topic_manager::TopicManager;

/// Orchestrator configuration.
pub struct OrchestratorConfig {
    /// Zenoh key prefix for the managed topic (e.g. "myapp/events").
    pub key_prefix: String,
    /// Directory for the config store (fjall).
    pub data_dir: std::path::PathBuf,
    /// Lag publish interval.
    pub lag_interval: Duration,
    /// Admin API key prefix (default: `{key_prefix}/_admin`).
    pub admin_prefix: Option<String>,
    /// Bind address for optional HTTP API (e.g. `0.0.0.0:8080`).
    pub http_bind: Option<std::net::SocketAddr>,
    /// Auth token for HTTP API. Falls back to `MITIFLOW_UI_TOKEN` env var.
    pub auth_token: Option<String>,
}

/// The orchestrator control-plane service.
pub struct Orchestrator {
    session: Session,
    config: OrchestratorConfig,
    config_store: Arc<ConfigStore>,
    lag_monitor: Option<Arc<LagMonitor>>,
    store_tracker: Option<StoreTracker>,
    cluster_view: Option<Arc<ClusterView>>,
    override_manager: Option<Arc<OverrideManager>>,
    topic_manager: Option<Arc<RwLock<TopicManager>>>,
    cancel: CancellationToken,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Orchestrator {
    /// Create a new orchestrator. Call [`run`] to start background tasks.
    pub fn new(
        session: &Session,
        config: OrchestratorConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config_store = Arc::new(ConfigStore::open(&config.data_dir)?);
        Ok(Self {
            session: session.clone(),
            config,
            config_store,
            lag_monitor: None,
            store_tracker: None,
            cluster_view: None,
            override_manager: None,
            topic_manager: None,
            cancel: CancellationToken::new(),
            tasks: Vec::new(),
        })
    }

    /// Start all background tasks: lag monitoring, store tracking, cluster view, admin API.
    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Create broadcast channels for SSE
        let (cluster_tx, _) = broadcast::channel::<ClusterEvent>(256);
        let (lag_tx, _) = broadcast::channel::<LagReport>(256);
        let (event_tx, _) = broadcast::channel::<EventSummary>(256);

        // Start lag monitor (with broadcast sender)
        let lag_monitor = LagMonitor::new_with_broadcast(
            &self.session,
            &self.config.key_prefix,
            self.config.lag_interval,
            lag_tx.clone(),
        )
        .await?;
        self.lag_monitor = Some(Arc::new(lag_monitor));

        // Start store tracker
        let store_tracker = StoreTracker::new(&self.session, &self.config.key_prefix).await?;
        self.store_tracker = Some(store_tracker);

        // Start cluster view (with broadcast sender)
        let cluster_view = ClusterView::new_with_broadcast(
            &self.session,
            &self.config.key_prefix,
            cluster_tx.clone(),
        )
        .await?;
        self.cluster_view = Some(Arc::new(cluster_view));

        // Start override manager
        let override_manager = OverrideManager::new(&self.session, &self.config.key_prefix);
        self.override_manager = Some(Arc::new(override_manager));

        // Start topic manager and bootstrap per-topic cluster views
        let mut topic_manager = TopicManager::new(&self.session);
        if let Ok(topics) = self.config_store.list_topics() {
            for topic in &topics {
                if !topic.key_prefix.is_empty()
                    && let Err(e) = topic_manager
                        .add_topic(&topic.name, &topic.key_prefix)
                        .await
                    {
                        warn!(topic = %topic.name, "failed to create per-topic cluster view: {e}");
                    }
            }
        }
        self.topic_manager = Some(Arc::new(RwLock::new(topic_manager)));

        // Start admin queryable
        let admin_prefix = self
            .config
            .admin_prefix
            .clone()
            .unwrap_or_else(|| format!("{}/_admin", self.config.key_prefix));
        let admin_queryable = self
            .session
            .declare_queryable(format!("{admin_prefix}/**"))
            .await?;

        let cancel = self.cancel.clone();
        let config_store = Arc::clone(&self.config_store);
        // Share cluster view and override manager with admin queryable
        let cv_nodes = self.cluster_view.as_ref().unwrap().nodes_handle();

        self.tasks.push(tokio::spawn(async move {
            run_admin_queryable(
                admin_queryable,
                config_store,
                cv_nodes,
                &admin_prefix,
                cancel,
            )
            .await;
        }));

        // Start _config/** queryable so agents can discover topics on join
        let config_prefix = format!("{}/_config", self.config.key_prefix);
        let config_queryable = self
            .session
            .declare_queryable(format!("{config_prefix}/**"))
            .await?;
        let config_store_for_qbl = Arc::clone(&self.config_store);
        let cancel_for_config = self.cancel.clone();

        self.tasks.push(tokio::spawn(async move {
            run_config_queryable(
                config_queryable,
                config_store_for_qbl,
                &config_prefix,
                cancel_for_config,
            )
            .await;
        }));

        // Start event tail subscriber — subscribes to {prefix}/p/** and pushes
        // EventSummary into the broadcast channel for SSE consumers.
        {
            let event_tx = event_tx.clone();
            let cancel = self.cancel.clone();
            let key_expr = format!("{}/p/**", self.config.key_prefix);
            let subscriber = self.session.declare_subscriber(&key_expr).await?;
            self.tasks.push(tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        sample = subscriber.recv_async() => {
                            let Ok(sample) = sample else { break };
                            if let Some(attachment) = sample.attachment() {
                                if let Ok(meta) = mitiflow::attachment::decode_metadata(attachment) {
                                    let key_expr_str = sample.key_expr().as_keyexpr().as_str().to_string();
                                    let key = mitiflow::attachment::extract_key(&key_expr_str).map(String::from);
                                    let partition = mitiflow::attachment::extract_partition(&key_expr_str);
                                    let payload_size = sample.payload().len();
                                    let summary = EventSummary {
                                        seq: meta.seq,
                                        partition,
                                        publisher_id: meta.pub_id.to_string(),
                                        timestamp: meta.timestamp.to_rfc3339(),
                                        key,
                                        key_expr: key_expr_str,
                                        payload_size,
                                    };
                                    let _ = event_tx.send(summary);
                                }
                            }
                        }
                    }
                }
            }));
        }

        // Distribute existing configs on startup
        self.publish_all_configs().await;

        // Optionally start HTTP API
        if let Some(bind_addr) = self.config.http_bind {
            let http_state = HttpState {
                config_store: Arc::clone(&self.config_store),
                nodes: self.cluster_view.as_ref().map(|cv| cv.nodes_handle()),
                cluster_events_tx: cluster_tx.clone(),
                lag_events_tx: lag_tx.clone(),
                event_tail_tx: event_tx.clone(),
                lag_monitor: self.lag_monitor.clone(),
                session: Some(self.session.clone()),
                topic_manager: self.topic_manager.clone(),
                override_manager: self.override_manager.clone(),
                cluster_view: self.cluster_view.clone(),
                key_prefix: self.config.key_prefix.clone(),
            };
            let auth_token = self
                .config
                .auth_token
                .clone()
                .or_else(|| std::env::var("MITIFLOW_UI_TOKEN").ok());
            let http_handle = crate::http::start_http(
                http_state,
                bind_addr,
                self.cancel.clone(),
                auth_token,
            )
            .await;
            self.tasks.push(http_handle);
        }

        info!(
            key_prefix = %self.config.key_prefix,
            "orchestrator started"
        );
        Ok(())
    }

    /// Create a topic and persist its configuration.
    pub async fn create_topic(
        &mut self,
        config: crate::config::TopicConfig,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.config_store.put_topic(&config)?;

        // Publish config via Zenoh
        let key = format!("{}/_config/{}", self.config.key_prefix, config.name);
        let bytes = serde_json::to_vec(&config)?;
        self.session.put(&key, bytes).await?;

        // Create per-topic cluster view if applicable
        if let Some(ref tm) = self.topic_manager
            && !config.key_prefix.is_empty() {
                tm.write().await.add_topic(&config.name, &config.key_prefix).await?;
            }

        info!(topic = %config.name, "topic created");
        Ok(())
    }

    /// Delete a topic configuration.
    pub async fn delete_topic(
        &mut self,
        name: &str,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let deleted = self.config_store.delete_topic(name)?;
        if deleted {
            // Publish delete via Zenoh
            let key = format!("{}/_config/{}", self.config.key_prefix, name);
            self.session.delete(&key).await?;

            // Remove per-topic cluster view
            if let Some(ref tm) = self.topic_manager {
                tm.write().await.remove_topic(name).await;
            }

            info!(topic = %name, "topic deleted");
        }
        Ok(deleted)
    }

    /// List all topics.
    pub fn list_topics(
        &self,
    ) -> Result<Vec<crate::config::TopicConfig>, Box<dyn std::error::Error + Send + Sync>> {
        self.config_store.list_topics()
    }

    /// Get a specific topic.
    pub fn get_topic(
        &self,
        name: &str,
    ) -> Result<Option<crate::config::TopicConfig>, Box<dyn std::error::Error + Send + Sync>> {
        self.config_store.get_topic(name)
    }

    /// Get lag for a consumer group.
    pub async fn get_group_lag(&self, group_id: &str) -> Vec<crate::lag::LagReport> {
        if let Some(ref lag_monitor) = self.lag_monitor {
            lag_monitor.get_group_lag(group_id).await
        } else {
            Vec::new()
        }
    }

    /// Get the store tracker.
    pub fn store_tracker(&self) -> Option<&StoreTracker> {
        self.store_tracker.as_ref()
    }

    /// Get the cluster view.
    pub fn cluster_view(&self) -> Option<&Arc<ClusterView>> {
        self.cluster_view.as_ref()
    }

    /// Get the override manager.
    pub fn override_manager(&self) -> Option<&Arc<OverrideManager>> {
        self.override_manager.as_ref()
    }

    /// Get the topic manager.
    pub fn topic_manager(&self) -> Option<&Arc<RwLock<TopicManager>>> {
        self.topic_manager.as_ref()
    }

    /// Drain a node: compute overrides to move all its partitions elsewhere.
    pub async fn drain_node(
        &self,
        node_id: &str,
        replication_factor: u32,
    ) -> Result<Vec<mitiflow_agent::OverrideEntry>, Box<dyn std::error::Error + Send + Sync>> {
        let cv = self
            .cluster_view
            .as_ref()
            .ok_or("cluster view not started")?;
        let om = self
            .override_manager
            .as_ref()
            .ok_or("override manager not started")?;
        crate::drain::drain_node(node_id, cv, om, replication_factor).await
    }

    /// Undrain a node: remove drain overrides so it regains partitions via HRW.
    pub async fn undrain_node(
        &self,
        node_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let om = self
            .override_manager
            .as_ref()
            .ok_or("override manager not started")?;
        crate::drain::undrain_node(node_id, om).await
    }

    /// Publish all stored configs via Zenoh.
    async fn publish_all_configs(&self) {
        if let Ok(topics) = self.config_store.list_topics() {
            for topic in topics {
                let key = format!("{}/_config/{}", self.config.key_prefix, topic.name);
                if let Ok(bytes) = serde_json::to_vec(&topic)
                    && let Err(e) = self.session.put(&key, bytes).await {
                        warn!("failed to publish config for {}: {e}", topic.name);
                    }
            }
        }
    }

    /// Gracefully shut down.
    pub async fn shutdown(mut self) {
        self.cancel.cancel();
        for handle in self.tasks.drain(..) {
            let _ = handle.await;
        }
        if let Some(lag) = self.lag_monitor.take() {
            if let Ok(lag) = Arc::try_unwrap(lag) {
                lag.shutdown().await;
            }
        }
        if let Some(tracker) = self.store_tracker.take() {
            tracker.shutdown().await;
        }
        if let Some(cv) = self.cluster_view.take() {
            if let Ok(cv) = Arc::try_unwrap(cv) {
                cv.shutdown().await;
            }
        }
        if let Some(tm) = self.topic_manager.take() {
            if let Ok(tm) = Arc::try_unwrap(tm) {
                tm.into_inner().shutdown().await;
            }
        }
        info!("orchestrator shut down");
    }
}

impl Drop for Orchestrator {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Cluster-wide status summary returned by `_admin/cluster/status`.
#[derive(Debug, Clone, Serialize)]
pub struct ClusterStatusSummary {
    pub total_nodes: usize,
    pub online_nodes: usize,
    pub total_partitions: usize,
}

/// Handle admin queries via Zenoh queryable.
async fn run_admin_queryable(
    queryable: zenoh::query::Queryable<zenoh::handlers::FifoChannelHandler<zenoh::query::Query>>,
    config_store: Arc<ConfigStore>,
    cv_nodes: Arc<tokio::sync::RwLock<HashMap<String, crate::cluster_view::NodeInfo>>>,
    admin_prefix: &str,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            result = queryable.recv_async() => {
                match result {
                    Ok(query) => {
                        let key = query.key_expr().as_str().to_string();
                        let suffix = key
                            .strip_prefix(admin_prefix)
                            .unwrap_or("")
                            .trim_start_matches('/');

                        match suffix {
                            "topics" => {
                                match config_store.list_topics() {
                                    Ok(topics) => {
                                        if let Ok(bytes) = serde_json::to_vec(&topics) {
                                            let _ = query.reply(query.key_expr(), bytes).await;
                                        }
                                    }
                                    Err(e) => {
                                        let _ = query
                                            .reply(query.key_expr(), format!("error:{e}").into_bytes())
                                            .await;
                                    }
                                }
                            }
                            s if s.starts_with("topics/") => {
                                let topic_name = &s["topics/".len()..];
                                match config_store.get_topic(topic_name) {
                                    Ok(Some(topic)) => {
                                        if let Ok(bytes) = serde_json::to_vec(&topic) {
                                            let _ = query.reply(query.key_expr(), bytes).await;
                                        }
                                    }
                                    Ok(None) => {
                                        let _ = query
                                            .reply(query.key_expr(), b"error:topic not found".to_vec())
                                            .await;
                                    }
                                    Err(e) => {
                                        let _ = query
                                            .reply(query.key_expr(), format!("error:{e}").into_bytes())
                                            .await;
                                    }
                                }
                            }
                            "cluster/nodes" => {
                                let nodes = cv_nodes.read().await;
                                if let Ok(bytes) = serde_json::to_vec(&*nodes) {
                                    let _ = query.reply(query.key_expr(), bytes).await;
                                }
                            }
                            "cluster/assignments" => {
                                let nodes: tokio::sync::RwLockReadGuard<'_, HashMap<String, crate::cluster_view::NodeInfo>> = cv_nodes.read().await;
                                let mut all_assignments: Vec<crate::cluster_view::AssignmentInfo> = Vec::new();
                                for (node_id, info) in nodes.iter() {
                                    if let Some(ref status) = info.status {
                                        for ps in &status.partitions {
                                            all_assignments.push(crate::cluster_view::AssignmentInfo {
                                                partition: ps.partition,
                                                replica: ps.replica,
                                                node_id: node_id.clone(),
                                                state: ps.state,
                                                source: crate::cluster_view::AssignmentSource::Computed,
                                            });
                                        }
                                    }
                                }
                                if let Ok(bytes) = serde_json::to_vec(&all_assignments) {
                                    let _ = query.reply(query.key_expr(), bytes).await;
                                }
                            }
                            "cluster/status" => {
                                let nodes: tokio::sync::RwLockReadGuard<'_, HashMap<String, crate::cluster_view::NodeInfo>> = cv_nodes.read().await;
                                let online = nodes.values().filter(|n| n.online).count();
                                let total_partitions: usize = nodes
                                    .values()
                                    .filter_map(|n| n.status.as_ref())
                                    .map(|s| s.partitions.len())
                                    .sum();
                                let summary = ClusterStatusSummary {
                                    total_nodes: nodes.len(),
                                    online_nodes: online,
                                    total_partitions,
                                };
                                if let Ok(bytes) = serde_json::to_vec(&summary) {
                                    let _ = query.reply(query.key_expr(), bytes).await;
                                }
                            }
                            _ => {
                                let _ = query
                                    .reply(query.key_expr(), b"error:unknown endpoint".to_vec())
                                    .await;
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    }
    debug!("admin queryable stopped");
}

/// Handle `_config/**` queries so agents can discover existing topics.
///
/// - `_config/*` or `_config/**` → one reply per topic
/// - `_config/{name}` → single topic config
async fn run_config_queryable(
    queryable: zenoh::query::Queryable<zenoh::handlers::FifoChannelHandler<zenoh::query::Query>>,
    config_store: Arc<ConfigStore>,
    config_prefix: &str,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            result = queryable.recv_async() => {
                match result {
                    Ok(query) => {
                        let key = query.key_expr().as_str().to_string();
                        let suffix = key
                            .strip_prefix(config_prefix)
                            .unwrap_or("")
                            .trim_start_matches('/');

                        if suffix.is_empty() || suffix == "*" || suffix == "**" {
                            // Return all topics, one reply per topic.
                            if let Ok(topics) = config_store.list_topics() {
                                for topic in &topics {
                                    let reply_key = format!("{config_prefix}/{}", topic.name);
                                    if let Ok(bytes) = serde_json::to_vec(topic) {
                                        let _ = query.reply(&reply_key, bytes).await;
                                    }
                                }
                            }
                        } else {
                            // Specific topic query.
                            match config_store.get_topic(suffix) {
                                Ok(Some(topic)) => {
                                    if let Ok(bytes) = serde_json::to_vec(&topic) {
                                        let _ = query.reply(query.key_expr(), bytes).await;
                                    }
                                }
                                Ok(None) => {
                                    // Topic not found — no reply (empty result set).
                                }
                                Err(e) => {
                                    warn!("config queryable error for {suffix}: {e}");
                                }
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    }
    debug!("config queryable stopped");
}
