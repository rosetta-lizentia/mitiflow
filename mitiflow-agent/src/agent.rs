use std::sync::Arc;

use mitiflow::partition::hash_ring::{self, NodeDescriptor};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use zenoh::Session;

use crate::config::StorageAgentConfig;
use crate::error::AgentResult;
use crate::health::HealthReporter;
use crate::membership::MembershipTracker;
use crate::reconciler::Reconciler;
use crate::recovery::RecoveryManager;
use crate::status::StatusReporter;
use crate::types::OverrideTable;

/// Top-level storage agent daemon.
///
/// Manages EventStore instances for assigned partitions using decentralized
/// partition assignment (weighted rendezvous hashing) and Zenoh liveliness.
pub struct StorageAgent {
    config: StorageAgentConfig,
    _session: Session,
    membership: MembershipTracker,
    reconciler: Arc<Reconciler>,
    _recovery: RecoveryManager,
    health: HealthReporter,
    status: StatusReporter,
    overrides: Arc<RwLock<OverrideTable>>,
    cancel: CancellationToken,
    _override_task: Option<tokio::task::JoinHandle<()>>,
}

impl StorageAgent {
    /// Create and start the storage agent.
    pub async fn start(session: &Session, config: StorageAgentConfig) -> AgentResult<Self> {
        let key_prefix = config.key_prefix().to_string();
        let cancel = CancellationToken::new();

        // 1. Start membership tracker.
        let membership = MembershipTracker::new(session, &config).await?;

        // 2. Create reconciler.
        let reconciler = Arc::new(Reconciler::new(
            config.node_id.clone(),
            config.data_dir.clone(),
            session.clone(),
            config.bus_config.clone(),
            config.drain_grace_period,
        ));

        // 3. Create recovery manager.
        let recovery = RecoveryManager::new(session, &key_prefix);

        // 4. Start health reporter.
        let health = HealthReporter::new(
            session,
            config.node_id.clone(),
            &key_prefix,
            config.health_interval,
        )
        .await?;

        // 5. Start status reporter.
        let status = StatusReporter::new(session, config.node_id.clone(), &key_prefix).await?;

        // 6. Subscribe to override updates.
        let overrides: Arc<RwLock<OverrideTable>> =
            Arc::new(RwLock::new(OverrideTable::default()));
        let override_task = {
            let session = session.clone();
            let overrides = Arc::clone(&overrides);
            let cancel = cancel.clone();
            let override_key = config.overrides_key();
            let reconciler = Arc::clone(&reconciler);
            let config_clone = config.clone();

            Some(tokio::spawn(async move {
                if let Err(e) = Self::override_watcher(
                    &session,
                    &override_key,
                    &overrides,
                    &reconciler,
                    &config_clone,
                    &cancel,
                )
                .await
                {
                    warn!("override watcher exited: {e}");
                }
            }))
        };

        // 7. Compute initial assignment and reconcile.
        let agent = Self {
            config,
            _session: session.clone(),
            membership,
            reconciler,
            _recovery: recovery,
            health,
            status,
            overrides,
            cancel,
            _override_task: override_task,
        };

        agent.recompute_and_reconcile().await?;

        // 8. Set up membership change callback.
        {
            let reconciler = Arc::clone(&agent.reconciler);
            let config = agent.config.clone();
            let overrides = Arc::clone(&agent.overrides);

            agent
                .membership
                .on_change(move |nodes, event| {
                    let reconciler = Arc::clone(&reconciler);
                    let config = config.clone();
                    let overrides = Arc::clone(&overrides);
                    let mut all_nodes: Vec<NodeDescriptor> = nodes.to_vec();
                    // Include self.
                    all_nodes.push(NodeDescriptor {
                        id: config.node_id.clone(),
                        capacity: config.capacity,
                        labels: config.labels.clone(),
                    });

                    debug!(
                        event = ?event,
                        nodes = all_nodes.len(),
                        "membership changed, scheduling rebalance"
                    );

                    // Spawn async rebalance since callback is sync.
                    tokio::spawn(async move {
                        let overrides_snapshot = overrides.read().await;
                        let desired = Self::compute_desired_assignment(
                            &config.node_id,
                            config.num_partitions,
                            config.replication_factor,
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

        info!(node_id = %agent.config.node_id, "storage agent started");
        Ok(agent)
    }

    /// Recompute assignment based on current membership and reconcile.
    pub async fn recompute_and_reconcile(&self) -> AgentResult<()> {
        let nodes = self.membership.current_nodes().await;
        let overrides = self.overrides.read().await;
        let desired = Self::compute_desired_assignment(
            &self.config.node_id,
            self.config.num_partitions,
            self.config.replication_factor,
            &nodes,
            &overrides,
        );
        drop(overrides);

        let actions = self.reconciler.reconcile(&desired).await?;

        if !actions.is_empty() {
            // Report status after reconciliation.
            let statuses = self.reconciler.partition_statuses().await;
            if let Err(e) = self.status.report(&statuses).await {
                warn!("failed to report status: {e}");
            }

            // Update health reporter.
            self.health
                .update(statuses.len() as u32, 0, 0, 0, 0)
                .await;
        }

        Ok(())
    }

    /// Compute which (partition, replica) tuples this node should own.
    fn compute_desired_assignment(
        self_node_id: &str,
        num_partitions: u32,
        replication_factor: u32,
        nodes: &[NodeDescriptor],
        overrides: &OverrideTable,
    ) -> Vec<(u32, u32)> {
        let mut desired = Vec::new();

        for p in 0..num_partitions {
            for r in 0..replication_factor {
                // Check overrides first.
                if let Some(entry) = overrides.entries.iter().find(|e| e.partition == p && e.replica == r)
                {
                    if entry.node_id == self_node_id {
                        desired.push((p, r));
                    }
                    continue;
                }

                // Use rack-aware assignment if any node has rack labels.
                let has_rack_labels = nodes.iter().any(|n| n.labels.contains_key("rack"));
                let replicas = if has_rack_labels {
                    hash_ring::assign_replicas_rack_aware(p, replication_factor, nodes)
                } else {
                    hash_ring::assign_replicas(p, replication_factor, nodes)
                };

                if let Some(assigned_node) = replicas.get(r as usize) {
                    if assigned_node == self_node_id {
                        desired.push((p, r));
                    }
                }
            }
        }

        desired
    }

    /// Get the node ID of this agent.
    pub fn node_id(&self) -> &str {
        &self.config.node_id
    }

    /// Get the current partition assignment for this node.
    pub async fn assigned_partitions(&self) -> Vec<(u32, u32)> {
        self.reconciler.active_stores().await
    }

    /// Shutdown the storage agent gracefully.
    pub async fn shutdown(&self) -> AgentResult<()> {
        info!(node_id = %self.config.node_id, "shutting down storage agent");
        self.cancel.cancel();
        self.membership.shutdown().await;
        self.health.shutdown().await;
        self.status.shutdown().await;
        self.reconciler.shutdown_all().await?;
        Ok(())
    }

    // --- Internal ---

    async fn override_watcher(
        session: &Session,
        override_key: &str,
        overrides: &Arc<RwLock<OverrideTable>>,
        _reconciler: &Arc<Reconciler>,
        _config: &StorageAgentConfig,
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
                                        drop(guard);

                                        // Trigger re-reconciliation (we don't have
                                        // membership snapshot here, but the reconciler
                                        // can be driven from the main agent loop).
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
