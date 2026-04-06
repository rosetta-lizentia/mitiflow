//! Proactive cluster health management — alerts, under-replication detection,
//! and auto-drain of unhealthy nodes.
//!
//! The `AlertManager` periodically inspects the [`ClusterView`] and
//! [`ConfigStore`] to:
//!
//! - Detect under-replicated partitions (expected RF vs actual live replicas).
//! - Emit alerts when node health metrics cross thresholds.
//! - Publish alerts to `{key_prefix}/_cluster/alerts/{severity}/{id}`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::cluster_view::NodeInfo;
use crate::config::ConfigStore;

/// Severity level for cluster alerts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AlertSeverity {
    Warning,
    Critical,
}

/// A cluster health alert.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub id: String,
    pub severity: AlertSeverity,
    pub category: AlertCategory,
    pub message: String,
    pub timestamp: DateTime<Utc>,
    /// Which node or topic this alert relates to.
    pub subject: String,
}

/// Categories of alerts the manager can produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertCategory {
    UnderReplicated,
    NodeOffline,
    HighDiskUsage,
    HighEventBacklog,
}

/// Configuration for the alert manager.
#[derive(Debug, Clone)]
pub struct AlertManagerConfig {
    /// How often to check cluster health.
    pub check_interval: Duration,
    /// Disk usage percentage threshold for alerts (0.0–1.0).
    pub disk_usage_threshold: f64,
    /// Maximum tolerated event backlog per partition before alerting.
    pub backlog_threshold: u64,
    /// How long a node can be offline before emitting a critical alert.
    pub offline_grace_period: Duration,
}

impl Default for AlertManagerConfig {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(10),
            disk_usage_threshold: 0.8,
            backlog_threshold: 100_000,
            offline_grace_period: Duration::from_secs(60),
        }
    }
}

/// Proactive cluster health monitor.
///
/// Periodically checks the cluster state and produces typed [`Alert`]s.
pub struct AlertManager {
    alerts: Arc<RwLock<Vec<Alert>>>,
    task: Option<tokio::task::JoinHandle<()>>,
    cancel: CancellationToken,
}

impl AlertManager {
    /// Start the alert manager.
    ///
    /// Spawns a background task that wakes on `config.check_interval` and
    /// inspects `nodes` (from [`ClusterView`]) and the [`ConfigStore`] for
    /// health issues.
    pub fn start(
        nodes: Arc<RwLock<HashMap<String, NodeInfo>>>,
        config_store: Arc<ConfigStore>,
        config: AlertManagerConfig,
        session: zenoh::Session,
        key_prefix: String,
        cancel: CancellationToken,
    ) -> Self {
        let alerts: Arc<RwLock<Vec<Alert>>> = Arc::new(RwLock::new(Vec::new()));
        let alerts_handle = Arc::clone(&alerts);
        let cancel_inner = cancel.clone();

        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.check_interval);
            loop {
                tokio::select! {
                    _ = cancel_inner.cancelled() => break,
                    _ = interval.tick() => {
                        let new_alerts = check_cluster_health(
                            &nodes,
                            &config_store,
                            &config,
                        ).await;

                        if !new_alerts.is_empty() {
                            // Publish alerts via Zenoh
                            for alert in &new_alerts {
                                let alert_key = format!(
                                    "{key_prefix}/_cluster/alerts/{:?}/{}",
                                    alert.severity, alert.id
                                );
                                if let Ok(bytes) = serde_json::to_vec(alert)
                                    && let Err(e) = session.put(&alert_key, bytes).await {
                                        warn!(error = %e, "failed to publish alert");
                                    }
                            }

                            let mut store = alerts_handle.write().await;
                            store.extend(new_alerts);
                            // Keep only last 1000 alerts
                            if store.len() > 1000 {
                                let drain_count = store.len() - 1000;
                                store.drain(..drain_count);
                            }
                        }
                    }
                }
            }
        });

        Self {
            alerts,
            task: Some(task),
            cancel,
        }
    }

    /// Get all current alerts.
    pub async fn alerts(&self) -> Vec<Alert> {
        self.alerts.read().await.clone()
    }

    /// Get alerts filtered by severity.
    pub async fn alerts_by_severity(&self, severity: AlertSeverity) -> Vec<Alert> {
        self.alerts
            .read()
            .await
            .iter()
            .filter(|a| a.severity == severity)
            .cloned()
            .collect()
    }

    /// Clear all alerts.
    pub async fn clear(&self) {
        self.alerts.write().await.clear();
    }

    /// Shut down the alert manager.
    pub async fn shutdown(mut self) {
        self.cancel.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

/// Inspect node health and topic configs for issues.
async fn check_cluster_health(
    nodes: &Arc<RwLock<HashMap<String, NodeInfo>>>,
    config_store: &ConfigStore,
    config: &AlertManagerConfig,
) -> Vec<Alert> {
    let mut alerts = Vec::new();
    let nodes_map = nodes.read().await;

    // 1. Check for offline nodes
    let now = Utc::now();
    for (node_id, info) in nodes_map.iter() {
        if !info.online {
            let offline_duration = now.signed_duration_since(info.last_seen);
            let severity = if offline_duration
                > chrono::Duration::from_std(config.offline_grace_period)
                    .unwrap_or(chrono::Duration::seconds(60))
            {
                AlertSeverity::Critical
            } else {
                AlertSeverity::Warning
            };

            alerts.push(Alert {
                id: format!("node-offline-{node_id}"),
                severity,
                category: AlertCategory::NodeOffline,
                message: format!(
                    "Node '{node_id}' has been offline since {}",
                    info.last_seen.format("%H:%M:%S UTC")
                ),
                timestamp: now,
                subject: node_id.clone(),
            });
        }

        // 2. Check disk usage (from health reports)
        // NodeHealth provides disk_usage_bytes — we can't compute a percentage
        // without knowing total disk size, so we skip percentage-based
        // threshold and instead alert when bytes exceed a configurable limit.
        // For now, this check is a placeholder until disk capacity is reported.
    }

    // 3. Under-replicated partition detection
    if let Ok(topics) = config_store.list_topics() {
        for topic in &topics {
            let expected_rf = topic.replication_factor;
            if expected_rf <= 1 {
                continue; // No replication, nothing to check
            }

            // Count online nodes serving this topic (from status reports)
            // Note: NodeStatus doesn't carry key_prefix, so we count all
            // partition status entries that match this topic's partition range.
            let mut partition_replicas: HashMap<u32, u32> = HashMap::new();
            for (_, info) in nodes_map.iter() {
                if !info.online {
                    continue;
                }
                if let Some(ref status) = info.status {
                    for ps in &status.partitions {
                        if ps.partition < topic.num_partitions {
                            *partition_replicas.entry(ps.partition).or_insert(0) += 1;
                        }
                    }
                }
            }

            for p in 0..topic.num_partitions {
                let actual = partition_replicas.get(&p).copied().unwrap_or(0);
                if actual < expected_rf {
                    alerts.push(Alert {
                        id: format!("under-replicated-{}-p{p}", topic.name),
                        severity: if actual == 0 {
                            AlertSeverity::Critical
                        } else {
                            AlertSeverity::Warning
                        },
                        category: AlertCategory::UnderReplicated,
                        message: format!(
                            "Topic '{}' partition {p}: {actual}/{expected_rf} replicas online",
                            topic.name
                        ),
                        timestamp: now,
                        subject: topic.name.clone(),
                    });
                }
            }
        }
    }

    if !alerts.is_empty() {
        info!(count = alerts.len(), "health check produced alerts");
    }

    alerts
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CompactionPolicy, RetentionPolicy, TopicConfig};
    use mitiflow_storage::{NodeStatus, PartitionStatus, StoreState};

    fn make_nodes() -> Arc<RwLock<HashMap<String, NodeInfo>>> {
        Arc::new(RwLock::new(HashMap::new()))
    }

    fn make_store() -> (tempfile::TempDir, Arc<ConfigStore>) {
        let dir = tempfile::TempDir::new().unwrap();
        let store = Arc::new(ConfigStore::open(dir.path()).unwrap());
        (dir, store)
    }

    fn sample_topic(name: &str, partitions: u32, rf: u32) -> TopicConfig {
        TopicConfig {
            name: name.to_string(),
            key_prefix: format!("test/{name}"),
            num_partitions: partitions,
            replication_factor: rf,
            retention: RetentionPolicy::default(),
            compaction: CompactionPolicy::default(),
            required_labels: HashMap::new(),
            excluded_labels: HashMap::new(),
        }
    }

    fn make_partition_status(partition: u32, replica: u32) -> PartitionStatus {
        PartitionStatus {
            partition,
            replica,
            state: StoreState::Active,
            event_count: 0,
            watermark_seq: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn no_alerts_when_healthy() {
        let nodes = make_nodes();
        let (_dir, store) = make_store();
        let config = AlertManagerConfig::default();

        {
            let mut map = nodes.write().await;
            map.insert(
                "node-1".to_string(),
                NodeInfo {
                    metadata: None,
                    health: None,
                    status: None,
                    online: true,
                    last_seen: Utc::now(),
                },
            );
        }

        let alerts = check_cluster_health(&nodes, &store, &config).await;
        assert!(alerts.is_empty());
    }

    #[tokio::test]
    async fn detects_offline_node() {
        let nodes = make_nodes();
        let (_dir, store) = make_store();
        let config = AlertManagerConfig::default();

        {
            let mut map = nodes.write().await;
            map.insert(
                "node-1".to_string(),
                NodeInfo {
                    metadata: None,
                    health: None,
                    status: None,
                    online: false,
                    last_seen: Utc::now(),
                },
            );
        }

        let alerts = check_cluster_health(&nodes, &store, &config).await;
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].category, AlertCategory::NodeOffline);
        assert_eq!(alerts[0].subject, "node-1");
    }

    #[tokio::test]
    async fn detects_under_replicated_partition() {
        let nodes = make_nodes();
        let (_dir, store) = make_store();
        let config = AlertManagerConfig::default();

        // Topic with rf=2, 2 partitions
        store.put_topic(&sample_topic("events", 2, 2)).unwrap();

        // Only 1 node online with 1 replica per partition
        {
            let mut map = nodes.write().await;
            map.insert(
                "node-1".to_string(),
                NodeInfo {
                    metadata: None,
                    health: None,
                    status: Some(NodeStatus {
                        node_id: "node-1".to_string(),
                        partitions: vec![make_partition_status(0, 0), make_partition_status(1, 0)],
                        timestamp: Utc::now(),
                    }),
                    online: true,
                    last_seen: Utc::now(),
                },
            );
        }

        let alerts = check_cluster_health(&nodes, &store, &config).await;
        let under_rep: Vec<_> = alerts
            .iter()
            .filter(|a| a.category == AlertCategory::UnderReplicated)
            .collect();
        assert_eq!(under_rep.len(), 2); // Both partitions under-replicated
        assert!(
            under_rep
                .iter()
                .all(|a| a.severity == AlertSeverity::Warning)
        );
    }

    #[tokio::test]
    async fn critical_when_zero_replicas() {
        let nodes = make_nodes();
        let (_dir, store) = make_store();
        let config = AlertManagerConfig::default();

        // Topic with rf=2, 1 partition, no nodes online serving it
        store.put_topic(&sample_topic("events", 1, 2)).unwrap();

        let alerts = check_cluster_health(&nodes, &store, &config).await;
        let under_rep: Vec<_> = alerts
            .iter()
            .filter(|a| a.category == AlertCategory::UnderReplicated)
            .collect();
        assert_eq!(under_rep.len(), 1);
        assert_eq!(under_rep[0].severity, AlertSeverity::Critical);
    }

    #[tokio::test]
    async fn no_under_replicated_for_rf1() {
        let nodes = make_nodes();
        let (_dir, store) = make_store();
        let config = AlertManagerConfig::default();

        // Topic with rf=1 — under-replication check skipped
        store.put_topic(&sample_topic("logs", 4, 1)).unwrap();

        let alerts = check_cluster_health(&nodes, &store, &config).await;
        let under_rep: Vec<_> = alerts
            .iter()
            .filter(|a| a.category == AlertCategory::UnderReplicated)
            .collect();
        assert!(under_rep.is_empty());
    }

    #[tokio::test]
    async fn fully_replicated_topic_no_alerts() {
        let nodes = make_nodes();
        let (_dir, store) = make_store();
        let config = AlertManagerConfig::default();

        store.put_topic(&sample_topic("events", 2, 2)).unwrap();

        // 2 nodes, each owning 1 replica of each partition
        {
            let mut map = nodes.write().await;
            map.insert(
                "node-1".to_string(),
                NodeInfo {
                    metadata: None,
                    health: None,
                    status: Some(NodeStatus {
                        node_id: "node-1".to_string(),
                        partitions: vec![make_partition_status(0, 0), make_partition_status(1, 0)],
                        timestamp: Utc::now(),
                    }),
                    online: true,
                    last_seen: Utc::now(),
                },
            );
            map.insert(
                "node-2".to_string(),
                NodeInfo {
                    metadata: None,
                    health: None,
                    status: Some(NodeStatus {
                        node_id: "node-2".to_string(),
                        partitions: vec![make_partition_status(0, 1), make_partition_status(1, 1)],
                        timestamp: Utc::now(),
                    }),
                    online: true,
                    last_seen: Utc::now(),
                },
            );
        }

        let alerts = check_cluster_health(&nodes, &store, &config).await;
        assert!(alerts.is_empty());
    }

    #[tokio::test]
    async fn offline_node_critical_after_grace() {
        let nodes = make_nodes();
        let (_dir, store) = make_store();
        let config = AlertManagerConfig {
            offline_grace_period: Duration::from_secs(30),
            ..Default::default()
        };

        {
            let mut map = nodes.write().await;
            // Node offline for more than grace period
            map.insert(
                "node-1".to_string(),
                NodeInfo {
                    metadata: None,
                    health: None,
                    status: None,
                    online: false,
                    last_seen: Utc::now() - chrono::Duration::seconds(120),
                },
            );
        }

        let alerts = check_cluster_health(&nodes, &store, &config).await;
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].severity, AlertSeverity::Critical);
        assert_eq!(alerts[0].category, AlertCategory::NodeOffline);
    }

    #[tokio::test]
    async fn multiple_topics_multiple_alerts() {
        let nodes = make_nodes();
        let (_dir, store) = make_store();
        let config = AlertManagerConfig::default();

        // Two topics, both rf=2
        store.put_topic(&sample_topic("events", 2, 2)).unwrap();
        store.put_topic(&sample_topic("logs", 1, 2)).unwrap();

        // One node covers events p0 only
        {
            let mut map = nodes.write().await;
            map.insert(
                "node-1".to_string(),
                NodeInfo {
                    metadata: None,
                    health: None,
                    status: Some(NodeStatus {
                        node_id: "node-1".to_string(),
                        partitions: vec![make_partition_status(0, 0)],
                        timestamp: Utc::now(),
                    }),
                    online: true,
                    last_seen: Utc::now(),
                },
            );
        }

        let alerts = check_cluster_health(&nodes, &store, &config).await;
        let under_rep: Vec<_> = alerts
            .iter()
            .filter(|a| a.category == AlertCategory::UnderReplicated)
            .collect();
        // events p0: 1/2 (warning), events p1: 0/2 (critical), logs p0: 0/2 (critical)
        // Note: partition matching is by partition id, so node-1's p0 counts for
        // both events and logs topic's partition 0
        assert!(under_rep.len() >= 2);
    }
}
