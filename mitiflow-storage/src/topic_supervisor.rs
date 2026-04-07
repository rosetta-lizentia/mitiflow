use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use tracing::{info, warn};
use zenoh::Session;

use crate::config::{AgentConfig, TopicEntry, TopicWorkerConfig};
use crate::error::{AgentError, AgentResult};
use crate::health::HealthReporter;
use crate::schema_store::SchemaStore;
use crate::topic_worker::TopicWorker;

/// Manages multiple [`TopicWorker`] instances in a single agent process.
///
/// Each topic gets an independent `TopicWorker` with its own `MembershipTracker`,
/// `Reconciler`, `RecoveryManager`, and `StatusReporter`. The `HealthReporter`
/// is shared at the node level and aggregates across all topics.
pub struct TopicSupervisor {
    node_id: String,
    session: Session,
    base_data_dir: PathBuf,
    capacity: u32,
    labels: HashMap<String, String>,
    drain_grace_period: std::time::Duration,
    workers: HashMap<String, TopicWorker>,
    health: HealthReporter,
    schema_store: Arc<SchemaStore>,
}

impl TopicSupervisor {
    /// Create and start a supervisor, spawning workers for all configured topics.
    pub async fn start(session: &Session, config: &AgentConfig) -> AgentResult<Self> {
        // Open the SchemaStore BEFORE the health reporter so that its startup
        // cost does not delay liveliness-token declaration relative to health
        // heartbeats (which fire immediately and can cause observers to query
        // liveliness before workers have declared their tokens).
        let schema_dir = config.data_dir.join("_schemas");
        let schema_store = Arc::new(
            SchemaStore::open(&schema_dir)
                .map_err(|e| AgentError::Store(format!("failed to open schema store: {e}")))?,
        );

        // Start node-level health reporter using the global prefix.
        let health_key_prefix = &config.global_prefix;
        let health = HealthReporter::new(
            session,
            config.node_id.clone(),
            health_key_prefix,
            config.health_interval,
        )
        .await?;

        let mut supervisor = Self {
            node_id: config.node_id.clone(),
            session: session.clone(),
            base_data_dir: config.data_dir.clone(),
            capacity: config.capacity,
            labels: config.labels.clone(),
            drain_grace_period: config.drain_grace_period,
            workers: HashMap::new(),
            health,
            schema_store,
        };

        // Start workers for static topics.
        for entry in &config.topics {
            if let Err(e) = supervisor.add_topic_from_entry(entry).await {
                warn!(
                    topic = %entry.name,
                    "failed to start topic worker: {e}"
                );
            }
        }

        info!(
            node_id = %config.node_id,
            topics = supervisor.workers.len(),
            "topic supervisor started"
        );
        Ok(supervisor)
    }

    /// Add a topic from a [`TopicEntry`], creating a [`TopicWorker`].
    pub async fn add_topic_from_entry(&mut self, entry: &TopicEntry) -> AgentResult<()> {
        let config = TopicWorkerConfig::from_entry(
            entry,
            &self.base_data_dir,
            self.capacity,
            &self.labels,
            self.drain_grace_period,
        )?;
        self.add_topic(&entry.name, config).await
    }

    /// Add a topic with explicit configuration.
    ///
    /// Returns `Ok(())` if the worker was started successfully.
    /// Returns `Err` with `AgentError::Config` if the topic already exists.
    pub async fn add_topic(&mut self, name: &str, config: TopicWorkerConfig) -> AgentResult<()> {
        if self.workers.contains_key(name) {
            return Err(AgentError::Config(format!(
                "topic '{}' already exists",
                name
            )));
        }

        let worker = TopicWorker::start(
            &self.session,
            &self.node_id,
            config,
            Arc::clone(&self.schema_store),
        )
        .await?;
        self.workers.insert(name.to_string(), worker);

        // Update health with total partitions across all topics.
        self.update_health().await;

        info!(
            node_id = %self.node_id,
            topic = %name,
            "added topic"
        );
        Ok(())
    }

    /// Remove a topic, shutting down its [`TopicWorker`].
    ///
    /// Returns `true` if the topic existed and was removed.
    pub async fn remove_topic(&mut self, name: &str) -> AgentResult<bool> {
        if let Some(worker) = self.workers.remove(name) {
            worker.shutdown().await?;
            self.update_health().await;
            info!(
                node_id = %self.node_id,
                topic = %name,
                "removed topic"
            );
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// List managed topic names.
    pub fn topics(&self) -> Vec<String> {
        self.workers.keys().cloned().collect()
    }

    /// Check if a topic is managed.
    pub fn has_topic(&self, name: &str) -> bool {
        self.workers.contains_key(name)
    }

    /// Get a reference to a specific [`TopicWorker`].
    pub fn worker(&self, name: &str) -> Option<&TopicWorker> {
        self.workers.get(name)
    }

    /// Get a mutable reference to a specific [`TopicWorker`].
    pub fn worker_mut(&mut self, name: &str) -> Option<&mut TopicWorker> {
        self.workers.get_mut(name)
    }

    /// Get the node ID.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Get total partition count across all topics.
    pub async fn total_assigned_partitions(&self) -> usize {
        let mut total = 0;
        for worker in self.workers.values() {
            total += worker.assigned_partitions().await.len();
        }
        total
    }

    /// Shutdown all topic workers and the health reporter.
    pub async fn shutdown(&mut self) -> AgentResult<()> {
        info!(
            node_id = %self.node_id,
            topics = self.workers.len(),
            "shutting down topic supervisor"
        );

        let names: Vec<String> = self.workers.keys().cloned().collect();
        for name in names {
            if let Some(worker) = self.workers.remove(&name)
                && let Err(e) = worker.shutdown().await
            {
                warn!(topic = %name, "failed to shut down topic worker: {e}");
            }
        }

        self.health.shutdown().await;
        Ok(())
    }

    // --- Internal ---

    async fn update_health(&self) {
        let total_partitions = self.total_assigned_partitions().await;
        self.health
            .update(total_partitions as u32, 0, 0, 0, 0)
            .await;
    }
}
