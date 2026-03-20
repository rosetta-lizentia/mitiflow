//! Main orchestrator that ties together config management, lag monitoring,
//! store lifecycle tracking, and the admin API (Zenoh queryable).

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use zenoh::Session;

use crate::config::ConfigStore;
use crate::lag::LagMonitor;
use crate::lifecycle::StoreTracker;

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
}

/// The orchestrator control-plane service.
pub struct Orchestrator {
    session: Session,
    config: OrchestratorConfig,
    config_store: Arc<ConfigStore>,
    lag_monitor: Option<LagMonitor>,
    store_tracker: Option<StoreTracker>,
    cancel: CancellationToken,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Orchestrator {
    /// Create a new orchestrator. Call [`run`] to start background tasks.
    pub fn new(session: &Session, config: OrchestratorConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config_store = Arc::new(ConfigStore::open(&config.data_dir)?);
        Ok(Self {
            session: session.clone(),
            config,
            config_store,
            lag_monitor: None,
            store_tracker: None,
            cancel: CancellationToken::new(),
            tasks: Vec::new(),
        })
    }

    /// Start all background tasks: lag monitoring, store tracking, admin API.
    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Start lag monitor
        let lag_monitor = LagMonitor::new(
            &self.session,
            &self.config.key_prefix,
            self.config.lag_interval,
        )
        .await?;
        self.lag_monitor = Some(lag_monitor);

        // Start store tracker
        let store_tracker =
            StoreTracker::new(&self.session, &self.config.key_prefix).await?;
        self.store_tracker = Some(store_tracker);

        // Start admin queryable
        let admin_prefix = self.config.admin_prefix.clone().unwrap_or_else(|| {
            format!("{}/_admin", self.config.key_prefix)
        });
        let admin_queryable = self
            .session
            .declare_queryable(format!("{admin_prefix}/**"))
            .await?;

        let cancel = self.cancel.clone();
        let config_store = Arc::clone(&self.config_store);
        let _session = self.session.clone();
        let _key_prefix = self.config.key_prefix.clone();

        self.tasks.push(tokio::spawn(async move {
            run_admin_queryable(admin_queryable, config_store, &admin_prefix, cancel).await;
        }));

        // Distribute existing configs on startup
        self.publish_all_configs().await;

        info!(
            key_prefix = %self.config.key_prefix,
            "orchestrator started"
        );
        Ok(())
    }

    /// Create a topic and persist its configuration.
    pub async fn create_topic(
        &self,
        config: crate::config::TopicConfig,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.config_store.put_topic(&config)?;

        // Publish config via Zenoh
        let key = format!("{}/_config/{}", self.config.key_prefix, config.name);
        let bytes = serde_json::to_vec(&config)?;
        self.session.put(&key, bytes).await?;

        info!(topic = %config.name, "topic created");
        Ok(())
    }

    /// Delete a topic configuration.
    pub async fn delete_topic(&self, name: &str) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let deleted = self.config_store.delete_topic(name)?;
        if deleted {
            // Publish delete via Zenoh
            let key = format!("{}/_config/{}", self.config.key_prefix, name);
            self.session.delete(&key).await?;
            info!(topic = %name, "topic deleted");
        }
        Ok(deleted)
    }

    /// List all topics.
    pub fn list_topics(&self) -> Result<Vec<crate::config::TopicConfig>, Box<dyn std::error::Error + Send + Sync>> {
        self.config_store.list_topics()
    }

    /// Get a specific topic.
    pub fn get_topic(&self, name: &str) -> Result<Option<crate::config::TopicConfig>, Box<dyn std::error::Error + Send + Sync>> {
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

    /// Publish all stored configs via Zenoh.
    async fn publish_all_configs(&self) {
        if let Ok(topics) = self.config_store.list_topics() {
            for topic in topics {
                let key = format!("{}/_config/{}", self.config.key_prefix, topic.name);
                if let Ok(bytes) = serde_json::to_vec(&topic) {
                    if let Err(e) = self.session.put(&key, bytes).await {
                        warn!("failed to publish config for {}: {e}", topic.name);
                    }
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
            lag.shutdown().await;
        }
        if let Some(tracker) = self.store_tracker.take() {
            tracker.shutdown().await;
        }
        info!("orchestrator shut down");
    }
}

impl Drop for Orchestrator {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Handle admin queries via Zenoh queryable.
async fn run_admin_queryable(
    queryable: zenoh::query::Queryable<zenoh::handlers::FifoChannelHandler<zenoh::query::Query>>,
    config_store: Arc<ConfigStore>,
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
                                // List all topics
                                match config_store.list_topics() {
                                    Ok(topics) => {
                                        if let Ok(bytes) = serde_json::to_vec(&topics) {
                                            let _ = query.reply(query.key_expr(), bytes).await;
                                        }
                                    }
                                    Err(e) => {
                                        let _ = query
                                            .reply(
                                                query.key_expr(),
                                                format!("error:{e}").into_bytes(),
                                            )
                                            .await;
                                    }
                                }
                            }
                            s if s.starts_with("topics/") => {
                                // Get specific topic
                                let topic_name = &s["topics/".len()..];
                                match config_store.get_topic(topic_name) {
                                    Ok(Some(topic)) => {
                                        if let Ok(bytes) = serde_json::to_vec(&topic) {
                                            let _ = query.reply(query.key_expr(), bytes).await;
                                        }
                                    }
                                    Ok(None) => {
                                        let _ = query
                                            .reply(
                                                query.key_expr(),
                                                b"error:topic not found".to_vec(),
                                            )
                                            .await;
                                    }
                                    Err(e) => {
                                        let _ = query
                                            .reply(
                                                query.key_expr(),
                                                format!("error:{e}").into_bytes(),
                                            )
                                            .await;
                                    }
                                }
                            }
                            _ => {
                                let _ = query
                                    .reply(
                                        query.key_expr(),
                                        b"error:unknown endpoint".to_vec(),
                                    )
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
