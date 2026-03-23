use std::sync::Arc;

use mitiflow::partition::hash_ring::{self, NodeDescriptor};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use zenoh::Session;

use crate::config::TopicWorkerConfig;
use crate::error::AgentResult;
use crate::membership::MembershipTracker;
use crate::reconciler::Reconciler;
use crate::recovery::RecoveryManager;
use crate::status::StatusReporter;
use crate::types::OverrideTable;

/// Manages a single topic's EventStore instances.
///
/// Each `TopicWorker` owns its own `MembershipTracker`, `Reconciler`,
/// `RecoveryManager`, and `StatusReporter`. This provides fault isolation
/// between topics — a panic or bug in one topic does not affect others.
pub struct TopicWorker {
    config: TopicWorkerConfig,
    node_id: String,
    membership: Arc<MembershipTracker>,
    reconciler: Arc<Reconciler>,
    _recovery: Arc<RecoveryManager>,
    status: StatusReporter,
    overrides: Arc<RwLock<OverrideTable>>,
    cancel: CancellationToken,
    _override_task: Option<tokio::task::JoinHandle<()>>,
}

impl TopicWorker {
    /// Start a topic worker for the given topic configuration.
    pub async fn start(
        session: &Session,
        node_id: &str,
        config: TopicWorkerConfig,
    ) -> AgentResult<Self> {
        let key_prefix = config.key_prefix.clone();
        let cancel = CancellationToken::new();

        // Build a temporary StorageAgentConfig so MembershipTracker can
        // re-use its existing constructor (it only reads key_prefix, node_id,
        // capacity, labels, agents_prefix).
        let agent_compat_config = crate::config::StorageAgentConfig {
            node_id: node_id.to_string(),
            data_dir: config.data_dir.clone(),
            capacity: config.capacity,
            labels: config.labels.clone(),
            num_partitions: config.num_partitions,
            replication_factor: config.replication_factor,
            drain_grace_period: config.drain_grace_period,
            health_interval: std::time::Duration::from_secs(60), // unused by MembershipTracker
            bus_config: config.bus_config.clone(),
        };

        // 1. Membership tracker.
        let membership = MembershipTracker::new(session, &agent_compat_config).await?;
        let membership_arc = Arc::new(membership);

        // 2. Recovery manager.
        let recovery = RecoveryManager::new(session, &key_prefix);
        let recovery_arc = Arc::new(recovery);

        // 3. Reconciler with recovery + membership.
        let reconciler = Arc::new(
            Reconciler::new(
                node_id.to_string(),
                config.data_dir.clone(),
                session.clone(),
                config.bus_config.clone(),
                config.drain_grace_period,
            )
            .with_recovery(Arc::clone(&recovery_arc), Arc::clone(&membership_arc)),
        );

        // 4. Status reporter.
        let status = StatusReporter::new(session, node_id.to_string(), &key_prefix).await?;

        // 5. Override subscription.
        let overrides: Arc<RwLock<OverrideTable>> = Arc::new(RwLock::new(OverrideTable::default()));
        let override_task = {
            let session = session.clone();
            let overrides = Arc::clone(&overrides);
            let cancel = cancel.clone();
            let override_key = format!("{key_prefix}/_cluster/overrides");

            Some(tokio::spawn(async move {
                if let Err(e) =
                    Self::override_watcher(&session, &override_key, &overrides, &cancel).await
                {
                    warn!(
                        override_key = %override_key,
                        "override watcher exited: {e}"
                    );
                }
            }))
        };

        // 6. Build worker.
        let worker = Self {
            config: config.clone(),
            node_id: node_id.to_string(),
            membership: membership_arc,
            reconciler,
            _recovery: recovery_arc,
            status,
            overrides,
            cancel,
            _override_task: override_task,
        };

        // 7. Initial reconciliation.
        worker.recompute_and_reconcile().await?;

        // 8. Membership change callback.
        {
            let reconciler = Arc::clone(&worker.reconciler);
            let overrides = Arc::clone(&worker.overrides);
            let node_id = node_id.to_string();
            let num_partitions = config.num_partitions;
            let replication_factor = config.replication_factor;
            let capacity = config.capacity;
            let labels = config.labels.clone();

            worker
                .membership
                .on_change(move |nodes, event| {
                    let reconciler = Arc::clone(&reconciler);
                    let overrides = Arc::clone(&overrides);
                    let node_id = node_id.clone();
                    let labels = labels.clone();
                    let mut all_nodes: Vec<NodeDescriptor> = nodes.to_vec();
                    all_nodes.push(NodeDescriptor {
                        id: node_id.clone(),
                        capacity,
                        labels,
                    });

                    debug!(
                        event = ?event,
                        nodes = all_nodes.len(),
                        "membership changed, scheduling rebalance"
                    );

                    tokio::spawn(async move {
                        let overrides_snapshot = overrides.read().await;
                        let desired = Self::compute_desired_assignment(
                            &node_id,
                            num_partitions,
                            replication_factor,
                            &all_nodes,
                            &overrides_snapshot,
                        );
                        drop(overrides_snapshot);
                        if let Err(e) = reconciler.reconcile(&desired).await {
                            warn!("reconcile after membership change failed: {e}");
                        }
                    });
                })
                .await;
        }

        info!(
            node_id = %worker.node_id,
            topic = %worker.config.topic_name,
            "topic worker started"
        );
        Ok(worker)
    }

    /// Topic name.
    pub fn topic_name(&self) -> &str {
        &self.config.topic_name
    }

    /// Node ID.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Recompute assignment based on current membership and reconcile.
    pub async fn recompute_and_reconcile(&self) -> AgentResult<()> {
        let nodes = self.membership.current_nodes().await;
        let overrides = self.overrides.read().await;
        let desired = Self::compute_desired_assignment(
            &self.node_id,
            self.config.num_partitions,
            self.config.replication_factor,
            &nodes,
            &overrides,
        );
        drop(overrides);

        let actions = self.reconciler.reconcile(&desired).await?;

        if !actions.is_empty() {
            let statuses = self.reconciler.partition_statuses().await;
            if let Err(e) = self.status.report(&statuses).await {
                warn!("failed to report status: {e}");
            }
        }

        Ok(())
    }

    /// Get the current partition assignment for this topic on this node.
    pub async fn assigned_partitions(&self) -> Vec<(u32, u32)> {
        self.reconciler.active_stores().await
    }

    /// Shutdown the topic worker gracefully.
    pub async fn shutdown(&self) -> AgentResult<()> {
        info!(
            node_id = %self.node_id,
            topic = %self.config.topic_name,
            "shutting down topic worker"
        );
        self.cancel.cancel();
        self.membership.shutdown().await;
        self.status.shutdown().await;
        self.reconciler.shutdown_all().await?;
        Ok(())
    }

    /// Compute which `(partition, replica)` tuples this node should own.
    pub fn compute_desired_assignment(
        self_node_id: &str,
        num_partitions: u32,
        replication_factor: u32,
        nodes: &[NodeDescriptor],
        overrides: &OverrideTable,
    ) -> Vec<(u32, u32)> {
        let mut desired = Vec::new();
        for p in 0..num_partitions {
            for r in 0..replication_factor {
                if let Some(entry) = overrides
                    .entries
                    .iter()
                    .find(|e| e.partition == p && e.replica == r)
                {
                    if entry.node_id == self_node_id {
                        desired.push((p, r));
                    }
                    continue;
                }
                let has_rack_labels = nodes.iter().any(|n| n.labels.contains_key("rack"));
                let replicas = if has_rack_labels {
                    hash_ring::assign_replicas_rack_aware(p, replication_factor, nodes)
                } else {
                    hash_ring::assign_replicas(p, replication_factor, nodes)
                };
                if let Some(assigned_node) = replicas.get(r as usize)
                    && assigned_node == self_node_id {
                        desired.push((p, r));
                    }
            }
        }
        desired
    }

    // --- Internal ---

    async fn override_watcher(
        session: &Session,
        override_key: &str,
        overrides: &Arc<RwLock<OverrideTable>>,
        cancel: &CancellationToken,
    ) -> AgentResult<()> {
        let subscriber = session.declare_subscriber(override_key).await?;
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                result = subscriber.recv_async() => {
                    match result {
                        Ok(sample) => {
                            let payload = sample.payload().to_bytes();
                            match serde_json::from_slice::<OverrideTable>(&payload) {
                                Ok(table) => {
                                    let mut guard = overrides.write().await;
                                    if table.epoch > guard.epoch {
                                        *guard = table;
                                        info!("received override update (epoch={})", guard.epoch);
                                    }
                                }
                                Err(e) => {
                                    warn!("failed to parse override table: {e}");
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
}
