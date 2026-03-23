use std::sync::Arc;

use tokio::sync::Mutex;
use tracing::info;
use zenoh::Session;

use crate::config::{AgentConfig, StorageAgentConfig, TopicEntry};
use crate::error::AgentResult;
use crate::topic_supervisor::TopicSupervisor;
use crate::topic_watcher::TopicWatcher;

/// Top-level storage agent daemon.
///
/// Wraps a [`TopicSupervisor`] that manages one or more topics, each with
/// its own [`TopicWorker`] owning a `MembershipTracker`, `Reconciler`,
/// `RecoveryManager`, and `StatusReporter`.
///
/// When `auto_discover_topics` is enabled, a [`TopicWatcher`] subscribes to
/// `{global_prefix}/_config/**` and auto-provisions topics published by the
/// orchestrator.
pub struct StorageAgent {
    supervisor: Arc<Mutex<TopicSupervisor>>,
    watcher: Option<TopicWatcher>,
    _session: Session,
}

impl StorageAgent {
    /// Create and start the storage agent from a legacy single-topic config.
    pub async fn start(session: &Session, config: StorageAgentConfig) -> AgentResult<Self> {
        let agent_config: AgentConfig = config.into();
        Self::start_multi(session, agent_config).await
    }

    /// Create and start the storage agent from a multi-topic configuration.
    pub async fn start_multi(session: &Session, config: AgentConfig) -> AgentResult<Self> {
        let auto_discover = config.auto_discover_topics;
        let global_prefix = config.global_prefix.clone();
        let labels = config.labels.clone();

        let supervisor = TopicSupervisor::start(session, &config).await?;

        info!(
            node_id = %config.node_id,
            topics = supervisor.topics().len(),
            auto_discover = auto_discover,
            "storage agent started"
        );

        let supervisor = Arc::new(Mutex::new(supervisor));

        // Optionally start topic watcher for dynamic provisioning.
        let watcher = if auto_discover {
            Some(
                TopicWatcher::start(session, &global_prefix, Arc::clone(&supervisor), labels)
                    .await?,
            )
        } else {
            None
        };

        Ok(Self {
            supervisor,
            watcher,
            _session: session.clone(),
        })
    }

    /// Get the current partition assignment for this node (across all topics).
    pub async fn assigned_partitions(&self) -> Vec<(u32, u32)> {
        let sup = self.supervisor.lock().await;
        let topics = sup.topics();
        if topics.len() == 1 {
            if let Some(worker) = sup.worker(&topics[0]) {
                return worker.assigned_partitions().await;
            }
        }
        let mut all = Vec::new();
        for name in &topics {
            if let Some(worker) = sup.worker(name) {
                all.extend(worker.assigned_partitions().await);
            }
        }
        all
    }

    /// Recompute assignment for all topics and reconcile.
    pub async fn recompute_and_reconcile(&self) -> AgentResult<()> {
        let sup = self.supervisor.lock().await;
        for name in sup.topics() {
            if let Some(worker) = sup.worker(&name) {
                worker.recompute_and_reconcile().await?;
            }
        }
        Ok(())
    }

    /// Add a topic at runtime (for dynamic provisioning).
    pub async fn add_topic(&self, entry: TopicEntry) -> AgentResult<()> {
        self.supervisor
            .lock()
            .await
            .add_topic_from_entry(&entry)
            .await
    }

    /// Remove a topic at runtime.
    pub async fn remove_topic(&self, name: &str) -> AgentResult<bool> {
        self.supervisor.lock().await.remove_topic(name).await
    }

    /// List managed topics.
    pub async fn topics(&self) -> Vec<String> {
        self.supervisor.lock().await.topics()
    }

    /// Check whether a topic is managed.
    pub async fn has_topic(&self, name: &str) -> bool {
        self.supervisor.lock().await.has_topic(name)
    }

    /// Shutdown the storage agent gracefully.
    pub async fn shutdown(&mut self) -> AgentResult<()> {
        if let Some(ref watcher) = self.watcher {
            watcher.shutdown().await;
        }
        self.supervisor.lock().await.shutdown().await
    }
}
