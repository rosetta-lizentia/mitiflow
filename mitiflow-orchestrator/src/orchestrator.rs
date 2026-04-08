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
    /// Path to a YAML file containing topic definitions to bootstrap on startup.
    /// The file must have a `topics` array (other top-level keys are ignored).
    /// Topics already present in the config store are skipped (idempotent).
    pub bootstrap_topics_from: Option<std::path::PathBuf>,
}

/// Manages per-topic `_schema` queryables so that exact-key queries
/// (e.g. `myapp/events/_schema`) are routed to the correct responder.
struct SchemaQueryableManager {
    session: Session,
    config_store: Arc<ConfigStore>,
    cancel: CancellationToken,
    /// Per-topic key_prefix → spawned task handle.
    queryables: HashMap<String, tokio::task::JoinHandle<()>>,
}

impl SchemaQueryableManager {
    fn new(session: Session, config_store: Arc<ConfigStore>, cancel: CancellationToken) -> Self {
        Self {
            session,
            config_store,
            cancel,
            queryables: HashMap::new(),
        }
    }

    /// Declare a Zenoh queryable for a single topic and spawn its handler task.
    async fn add_topic(
        &mut self,
        topic_key_prefix: &str,
        orchestrator_key_prefix: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.queryables.contains_key(topic_key_prefix) {
            return Ok(());
        }
        let schema_key = format!("{topic_key_prefix}/_schema");
        let queryable = self.session.declare_queryable(&schema_key).await?;
        let config_store = Arc::clone(&self.config_store);
        let cancel = self.cancel.clone();
        let topic_kp = topic_key_prefix.to_string();
        let orch_kp = orchestrator_key_prefix.to_string();

        let handle = tokio::spawn(async move {
            run_per_topic_schema_queryable(queryable, config_store, &topic_kp, &orch_kp, cancel)
                .await;
        });
        self.queryables.insert(topic_key_prefix.to_string(), handle);
        Ok(())
    }

    /// Remove the queryable for a topic (task is cancelled via the shared token
    /// or by aborting the handle).
    fn remove_topic(&mut self, topic_key_prefix: &str) {
        if let Some(handle) = self.queryables.remove(topic_key_prefix) {
            handle.abort();
        }
    }

    /// Bootstrap queryables for all topics already in the config store.
    async fn bootstrap_all(
        &mut self,
        orchestrator_key_prefix: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Ok(topics) = self.config_store.list_topics() {
            for topic in &topics {
                let kp = if topic.key_prefix.is_empty() {
                    orchestrator_key_prefix.to_string()
                } else {
                    topic.key_prefix.clone()
                };
                self.add_topic(&kp, orchestrator_key_prefix).await?;
            }
        }
        Ok(())
    }
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
    schema_manager: Option<SchemaQueryableManager>,
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
            schema_manager: None,
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

        // Start per-topic _schema queryables so publishers/subscribers can
        // fetch schemas via exact-key queries (e.g. `myapp/events/_schema`).
        {
            let mut sm = SchemaQueryableManager::new(
                self.session.clone(),
                Arc::clone(&self.config_store),
                self.cancel.clone(),
            );
            sm.bootstrap_all(&self.config.key_prefix).await?;
            self.schema_manager = Some(sm);
        }

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
                            if let Some(attachment) = sample.attachment() && let Ok(meta) = mitiflow::attachment::decode_metadata(attachment) {
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
            }));
        }

        // Bootstrap topics from YAML file (idempotent — skips existing topics)
        if let Some(path) = self.config.bootstrap_topics_from.clone() {
            self.bootstrap_from_file(&path).await;
        }

        // Distribute existing configs on startup
        self.publish_all_configs().await;
        self.publish_all_schemas().await;

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
            let http_handle =
                crate::http::start_http(http_state, bind_addr, self.cancel.clone(), auth_token)
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
        // Normalize schema_version: 0 is reserved for "unset", bump to 1.
        let mut config = config;
        if config.schema_version == 0 {
            config.schema_version = 1;
        }

        self.config_store.put_topic(&config)?;

        // Publish config via Zenoh
        let key = format!("{}/_config/{}", self.config.key_prefix, config.name);
        let bytes = serde_json::to_vec(&config)?;
        self.session.put(&key, bytes).await?;

        // Publish schema so publishers/subscribers/agents can discover it
        let key_prefix = if config.key_prefix.is_empty() {
            self.config.key_prefix.clone()
        } else {
            config.key_prefix.clone()
        };
        let schema = mitiflow::schema::TopicSchema::new(
            &key_prefix,
            &config.name,
            config.num_partitions,
            config.codec,
            config.key_format,
            config.schema_version,
        );
        let schema_key = format!("{key_prefix}/_schema");
        let schema_bytes = serde_json::to_vec(&schema)?;
        self.session.put(&schema_key, schema_bytes).await?;

        // Register a per-topic schema queryable (if schema manager is running)
        if let Some(ref mut sm) = self.schema_manager {
            sm.add_topic(&key_prefix, &self.config.key_prefix).await?;
        }

        // Create per-topic cluster view if applicable
        if let Some(ref tm) = self.topic_manager
            && !config.key_prefix.is_empty()
        {
            tm.write()
                .await
                .add_topic(&config.name, &config.key_prefix)
                .await?;
        }

        info!(topic = %config.name, "topic created");
        Ok(())
    }

    /// Delete a topic configuration.
    pub async fn delete_topic(
        &mut self,
        name: &str,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        // Look up key_prefix before deleting so we can remove the queryable.
        let topic_key_prefix = self.config_store.get_topic(name)?.map(|t| {
            if t.key_prefix.is_empty() {
                self.config.key_prefix.clone()
            } else {
                t.key_prefix
            }
        });

        let deleted = self.config_store.delete_topic(name)?;
        if deleted {
            // Publish delete via Zenoh
            let key = format!("{}/_config/{}", self.config.key_prefix, name);
            self.session.delete(&key).await?;

            // Remove per-topic schema queryable
            if let Some(sm) = &mut self.schema_manager
                && let Some(kp) = &topic_key_prefix {
                    sm.remove_topic(kp);
                }

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
    ) -> Result<Vec<mitiflow_storage::OverrideEntry>, Box<dyn std::error::Error + Send + Sync>>
    {
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

    /// Bootstrap topics from a YAML file.
    ///
    /// Reads the `topics` array from the given file and creates any topics
    /// that are not already present in the config store. This allows the
    /// orchestrator to reuse the storage agent's YAML configuration directly.
    async fn bootstrap_from_file(&mut self, path: &std::path::Path) {
        let bootstrap = match crate::config::BootstrapConfig::from_file(path) {
            Ok(b) => b,
            Err(e) => {
                warn!(path = %path.display(), error = %e, "failed to read bootstrap topics file");
                return;
            }
        };

        if bootstrap.topics.is_empty() {
            debug!(path = %path.display(), "bootstrap file contains no topics");
            return;
        }

        let mut created = 0u32;
        let mut skipped = 0u32;
        for entry in bootstrap.topics {
            let name = entry.name.clone();
            match self.config_store.get_topic(&name) {
                Ok(Some(_)) => {
                    debug!(topic = %name, "bootstrap: topic already exists, skipping");
                    skipped += 1;
                }
                Ok(None) => {
                    let topic_config = entry.into_topic_config();
                    match self.create_topic(topic_config).await {
                        Ok(()) => {
                            info!(topic = %name, "bootstrap: topic created");
                            created += 1;
                        }
                        Err(e) => {
                            warn!(topic = %name, error = %e, "bootstrap: failed to create topic");
                        }
                    }
                }
                Err(e) => {
                    warn!(topic = %name, error = %e, "bootstrap: failed to check topic existence");
                }
            }
        }

        info!(
            path = %path.display(),
            created,
            skipped,
            "topic bootstrap complete"
        );
    }

    /// Publish all stored configs via Zenoh.
    async fn publish_all_configs(&self) {
        if let Ok(topics) = self.config_store.list_topics() {
            for topic in topics {
                let key = format!("{}/_config/{}", self.config.key_prefix, topic.name);
                if let Ok(bytes) = serde_json::to_vec(&topic)
                    && let Err(e) = self.session.put(&key, bytes).await
                {
                    warn!("failed to publish config for {}: {e}", topic.name);
                }
            }
        }
    }

    /// Publish schemas for all stored topics via Zenoh.
    async fn publish_all_schemas(&self) {
        if let Ok(topics) = self.config_store.list_topics() {
            for topic in &topics {
                let key_prefix = if topic.key_prefix.is_empty() {
                    self.config.key_prefix.clone()
                } else {
                    topic.key_prefix.clone()
                };
                // Normalize: version 0 is "unset", treat as 1.
                let version = if topic.schema_version == 0 {
                    1
                } else {
                    topic.schema_version
                };
                let schema = mitiflow::schema::TopicSchema::new(
                    &key_prefix,
                    &topic.name,
                    topic.num_partitions,
                    topic.codec,
                    topic.key_format,
                    version,
                );
                let schema_key = format!("{key_prefix}/_schema");
                if let Ok(bytes) = serde_json::to_vec(&schema)
                    && let Err(e) = self.session.put(&schema_key, bytes).await
                {
                    warn!("failed to publish schema for {}: {e}", topic.name);
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
        if let Some(lag) = self.lag_monitor.take()
            && let Ok(lag) = Arc::try_unwrap(lag)
        {
            lag.shutdown().await;
        }
        if let Some(tracker) = self.store_tracker.take() {
            tracker.shutdown().await;
        }
        if let Some(cv) = self.cluster_view.take()
            && let Ok(cv) = Arc::try_unwrap(cv)
        {
            cv.shutdown().await;
        }
        if let Some(tm) = self.topic_manager.take()
            && let Ok(tm) = Arc::try_unwrap(tm)
        {
            tm.into_inner().shutdown().await;
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

/// Handle `_schema` queries so publishers/subscribers can fetch topic schemas.
///
/// Handle schema queries for a **single topic** via its dedicated queryable.
///
/// The queryable is declared on `{topic_key_prefix}/_schema`, so Zenoh
/// routing guarantees that only exact-match queries arrive here.
async fn run_per_topic_schema_queryable(
    queryable: zenoh::query::Queryable<zenoh::handlers::FifoChannelHandler<zenoh::query::Query>>,
    config_store: Arc<ConfigStore>,
    topic_key_prefix: &str,
    orchestrator_key_prefix: &str,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            result = queryable.recv_async() => {
                match result {
                    Ok(query) => {
                        // Find the topic whose key_prefix matches.
                        if let Ok(topics) = config_store.list_topics() {
                            for topic in &topics {
                                let kp = if topic.key_prefix.is_empty() {
                                    orchestrator_key_prefix.to_string()
                                } else {
                                    topic.key_prefix.clone()
                                };
                                if kp == topic_key_prefix {
                                    // Normalize: version 0 → 1
                                    let version = if topic.schema_version == 0 { 1 } else { topic.schema_version };
                                    let schema = mitiflow::schema::TopicSchema::new(
                                        &kp,
                                        &topic.name,
                                        topic.num_partitions,
                                        topic.codec,
                                        topic.key_format,
                                        version,
                                    );
                                    if let Ok(bytes) = serde_json::to_vec(&schema) {
                                        let _ = query.reply(query.key_expr(), bytes).await;
                                    }
                                    break;
                                }
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    }
    debug!("per-topic schema queryable stopped for {topic_key_prefix}");
}
