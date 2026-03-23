use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use zenoh::Session;

use crate::config::TopicEntry;
use crate::error::AgentResult;
use crate::topic_supervisor::TopicSupervisor;

/// Watches `{global_prefix}/_config/**` for topic configuration changes
/// published by the orchestrator.
///
/// On startup, queries the orchestrator for existing topic configs. Then
/// subscribes to the config key space and reacts to new/deleted topics.
/// Placement labels are checked before starting a topic on this node.
pub struct TopicWatcher {
    cancel: CancellationToken,
    _task: tokio::task::JoinHandle<()>,
}

/// Deserialized topic configuration from the orchestrator.
///
/// This mirrors `TopicConfig` from `mitiflow-orchestrator` but is defined
/// locally to avoid a circular dependency.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct RemoteTopicConfig {
    pub name: String,
    #[serde(default)]
    pub key_prefix: String,
    pub num_partitions: u32,
    pub replication_factor: u32,
    #[serde(default)]
    pub required_labels: HashMap<String, String>,
    #[serde(default)]
    pub excluded_labels: HashMap<String, String>,
}

impl TopicWatcher {
    /// Start watching for topic configs.
    ///
    /// - `global_prefix`: Well-known prefix (e.g. `"mitiflow"`).
    /// - `supervisor`: Shared supervisor for adding/removing topics.
    /// - `agent_labels`: This node's labels for placement filtering.
    pub async fn start(
        session: &Session,
        global_prefix: &str,
        supervisor: Arc<Mutex<TopicSupervisor>>,
        agent_labels: HashMap<String, String>,
    ) -> AgentResult<Self> {
        let cancel = CancellationToken::new();
        let config_key = format!("{global_prefix}/_config/**");

        // 1. Query existing topics from orchestrator.
        let discovered = Self::query_existing(session, &config_key).await;
        {
            let mut sup = supervisor.lock().await;
            for remote_config in &discovered {
                if !should_serve_topic(remote_config, &agent_labels) {
                    debug!(topic = %remote_config.name, "skipping topic (labels don't match)");
                    continue;
                }
                if sup.has_topic(&remote_config.name) {
                    continue;
                }
                let entry = to_topic_entry(remote_config);
                if let Err(e) = sup.add_topic_from_entry(&entry).await {
                    warn!(topic = %remote_config.name, "failed to add discovered topic: {e}");
                }
            }
        }

        // 2. Subscribe for live updates.
        let sub = session.declare_subscriber(&config_key).await?;
        let task_cancel = cancel.clone();
        let session_clone = session.clone();
        let global_prefix = global_prefix.to_string();
        let config_prefix = format!("{global_prefix}/_config/");

        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = task_cancel.cancelled() => break,
                    result = sub.recv_async() => {
                        match result {
                            Ok(sample) => {
                                let key = sample.key_expr().as_str();
                                let topic_name = match key.strip_prefix(&config_prefix) {
                                    Some(name) => name.to_string(),
                                    None => continue,
                                };

                                match sample.kind() {
                                    zenoh::sample::SampleKind::Put => {
                                        let payload = sample.payload().to_bytes();
                                        match serde_json::from_slice::<RemoteTopicConfig>(&payload) {
                                            Ok(remote_config) => {
                                                if !should_serve_topic(&remote_config, &agent_labels) {
                                                    debug!(topic = %topic_name, "ignoring topic config (labels)");
                                                    continue;
                                                }
                                                let mut sup = supervisor.lock().await;
                                                if sup.has_topic(&topic_name) {
                                                    debug!(topic = %topic_name, "topic already exists, skipping");
                                                    continue;
                                                }
                                                let entry = to_topic_entry(&remote_config);
                                                if let Err(e) = sup.add_topic_from_entry(&entry).await {
                                                    warn!(topic = %topic_name, "failed to add topic: {e}");
                                                }
                                            }
                                            Err(e) => {
                                                warn!(topic = %topic_name, "invalid topic config: {e}");
                                            }
                                        }
                                    }
                                    zenoh::sample::SampleKind::Delete => {
                                        let mut sup = supervisor.lock().await;
                                        match sup.remove_topic(&topic_name).await {
                                            Ok(true) => info!(topic = %topic_name, "removed topic (deleted by orchestrator)"),
                                            Ok(false) => {}
                                            Err(e) => warn!(topic = %topic_name, "failed to remove topic: {e}"),
                                        }
                                    }
                                }
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
            debug!("topic watcher stopped");
            drop(session_clone); // keep session alive for subscriber
        });

        Ok(Self {
            cancel,
            _task: task,
        })
    }

    /// Query existing topic configs from the orchestrator.
    async fn query_existing(session: &Session, config_key: &str) -> Vec<RemoteTopicConfig> {
        let mut configs = Vec::new();
        match session
            .get(config_key)
            .consolidation(zenoh::query::ConsolidationMode::None)
            .accept_replies(zenoh::query::ReplyKeyExpr::Any)
            .timeout(std::time::Duration::from_secs(5))
            .await
        {
            Ok(replies) => {
                while let Ok(reply) = replies.recv_async().await {
                    if let Ok(sample) = reply.result() {
                        let payload = sample.payload().to_bytes();
                        match serde_json::from_slice::<RemoteTopicConfig>(&payload) {
                            Ok(config) => configs.push(config),
                            Err(e) => {
                                warn!("failed to parse topic config reply: {e}");
                            }
                        }
                    }
                }
            }
            Err(e) => {
                warn!("failed to query existing topic configs: {e}");
            }
        }
        configs
    }

    /// Shutdown the watcher.
    pub async fn shutdown(&self) {
        self.cancel.cancel();
    }
}

/// Check whether this agent should serve a given topic based on placement labels.
///
/// Rules:
/// - All `required_labels` must match (agent must have the exact key-value pairs).
/// - No `excluded_labels` may match (if agent has any of these key-value pairs, skip).
/// - If both are empty, the topic is served unconditionally.
pub fn should_serve_topic(
    config: &RemoteTopicConfig,
    agent_labels: &HashMap<String, String>,
) -> bool {
    // Check required labels.
    for (k, v) in &config.required_labels {
        match agent_labels.get(k) {
            Some(agent_v) if agent_v == v => {}
            _ => return false,
        }
    }
    // Check excluded labels.
    for (k, v) in &config.excluded_labels {
        if let Some(agent_v) = agent_labels.get(k) {
            if agent_v == v {
                return false;
            }
        }
    }
    true
}

fn to_topic_entry(remote: &RemoteTopicConfig) -> TopicEntry {
    TopicEntry {
        name: remote.name.clone(),
        key_prefix: remote.key_prefix.clone(),
        num_partitions: remote.num_partitions,
        replication_factor: remote.replication_factor,
    }
}
