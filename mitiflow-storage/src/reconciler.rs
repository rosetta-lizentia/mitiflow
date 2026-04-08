use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use mitiflow::store::backend::StorageBackend;
use mitiflow::{EventBusConfig, EventStore, FjallBackend};
use tokio::sync::{Notify, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use zenoh::Session;

use crate::error::{AgentError, AgentResult};
use crate::membership::MembershipTracker;
use crate::recovery::RecoveryManager;
use crate::types::{PartitionStatus, StoreState};

/// An action to be taken during reconciliation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconcileAction {
    Start { partition: u32, replica: u32 },
    Drain { partition: u32, replica: u32 },
    Stop { partition: u32, replica: u32 },
}

/// A managed EventStore instance with its lifecycle state.
struct ManagedStore {
    store: EventStore,
    /// Shared backend reference — kept alive so recovery can write directly
    /// to the same storage the EventStore workers are reading from.
    #[allow(dead_code)]
    backend: Arc<dyn StorageBackend>,
    partition: u32,
    replica: u32,
    state: StoreState,
    _started_at: chrono::DateTime<Utc>,
    event_count: u64,
    drain_cancel: Option<CancellationToken>,
}

/// Reconciles desired vs actual EventStore instances on this node.
pub struct Reconciler {
    node_id: String,
    data_dir: PathBuf,
    bus_config: EventBusConfig,
    session: Session,
    drain_grace_period: Duration,
    /// Optional recovery manager for fetching historical events from peers.
    recovery: Option<Arc<RecoveryManager>>,
    /// Optional membership tracker for discovering peer node IDs.
    membership: Option<Arc<MembershipTracker>>,
    /// Currently running stores keyed by (partition, replica).
    stores: Arc<RwLock<HashMap<(u32, u32), ManagedStore>>>,
    /// Notified when a store transitions state (e.g. recovery → Active).
    state_changed: Arc<Notify>,
}

impl Reconciler {
    pub fn new(
        node_id: String,
        data_dir: PathBuf,
        session: Session,
        bus_config: EventBusConfig,
        drain_grace_period: Duration,
    ) -> Self {
        Self {
            node_id,
            data_dir,
            bus_config,
            session,
            drain_grace_period,
            recovery: None,
            membership: None,
            stores: Arc::new(RwLock::new(HashMap::new())),
            state_changed: Arc::new(Notify::new()),
        }
    }

    /// Set the recovery manager and membership tracker for peer recovery.
    pub fn with_recovery(
        mut self,
        recovery: Arc<RecoveryManager>,
        membership: Arc<MembershipTracker>,
    ) -> Self {
        self.recovery = Some(recovery);
        self.membership = Some(membership);
        self
    }

    /// Compute the diff between desired and actual state.
    pub async fn compute_actions(&self, desired: &[(u32, u32)]) -> Vec<ReconcileAction> {
        let stores = self.stores.read().await;
        let actual: Vec<(u32, u32)> = stores
            .keys()
            .filter(|k| {
                stores
                    .get(k)
                    .map(|s| s.state != StoreState::Stopped)
                    .unwrap_or(false)
            })
            .copied()
            .collect();

        let mut actions = Vec::new();

        // Partitions to start (in desired but not actual).
        for &(p, r) in desired {
            if !actual.contains(&(p, r)) {
                actions.push(ReconcileAction::Start {
                    partition: p,
                    replica: r,
                });
            }
        }

        // Partitions to drain (in actual but not desired).
        for &(p, r) in &actual {
            if !desired.contains(&(p, r)) {
                let state = stores
                    .get(&(p, r))
                    .map(|s| s.state)
                    .unwrap_or(StoreState::Stopped);
                if state != StoreState::Draining && state != StoreState::Stopped {
                    actions.push(ReconcileAction::Drain {
                        partition: p,
                        replica: r,
                    });
                }
            }
        }

        actions
    }

    /// Apply reconciliation actions.
    pub async fn apply(&self, actions: &[ReconcileAction]) -> AgentResult<()> {
        for action in actions {
            match action {
                ReconcileAction::Start { partition, replica } => {
                    self.start_store(*partition, *replica).await?;
                }
                ReconcileAction::Drain { partition, replica } => {
                    self.drain_store(*partition, *replica).await?;
                }
                ReconcileAction::Stop { partition, replica } => {
                    self.stop_store(*partition, *replica).await?;
                }
            }
        }
        Ok(())
    }

    /// Reconcile: compute actions and apply them.
    pub async fn reconcile(&self, desired: &[(u32, u32)]) -> AgentResult<Vec<ReconcileAction>> {
        let actions = self.compute_actions(desired).await;
        self.apply(&actions).await?;
        Ok(actions)
    }

    /// List active stores as (partition, replica) tuples.
    pub async fn active_stores(&self) -> Vec<(u32, u32)> {
        self.stores
            .read()
            .await
            .iter()
            .filter(|(_, s)| {
                matches!(
                    s.state,
                    StoreState::Active | StoreState::Starting | StoreState::Recovering
                )
            })
            .map(|(k, _)| *k)
            .collect()
    }

    /// Get partition status for all managed stores.
    pub async fn partition_statuses(&self) -> Vec<PartitionStatus> {
        self.stores
            .read()
            .await
            .values()
            .filter(|s| s.state != StoreState::Stopped)
            .map(|s| PartitionStatus {
                partition: s.partition,
                replica: s.replica,
                state: s.state,
                event_count: s.event_count,
                watermark_seq: HashMap::new(),
            })
            .collect()
    }

    /// Get a reference to a running EventStore for a (partition, replica).
    pub async fn get_store(&self, partition: u32, replica: u32) -> Option<()> {
        let stores = self.stores.read().await;
        stores.get(&(partition, replica)).map(|_| ())
    }

    /// Returns a `Notify` that is woken whenever a store transitions state
    /// (e.g. recovery completes and moves to Active).
    pub fn state_changed(&self) -> Arc<Notify> {
        Arc::clone(&self.state_changed)
    }

    /// Shutdown all managed stores gracefully.
    pub async fn shutdown_all(&self) -> AgentResult<()> {
        let keys: Vec<(u32, u32)> = self.stores.read().await.keys().copied().collect();
        for (p, r) in keys {
            self.stop_store(p, r).await?;
        }
        Ok(())
    }

    // --- Internal ---

    async fn start_store(&self, partition: u32, replica: u32) -> AgentResult<()> {
        let path = self.store_path(partition, replica);
        std::fs::create_dir_all(&path)
            .map_err(|e| AgentError::Store(format!("failed to create store dir: {e}")))?;

        let backend = FjallBackend::open(&path, partition)
            .map_err(|e| AgentError::Store(format!("failed to open backend: {e}")))?;

        // Wrap in Arc so the same backend is shared between EventStore and recovery.
        let backend: Arc<dyn StorageBackend> = Arc::new(backend);

        let mut store = EventStore::new(&self.session, backend.clone(), self.bus_config.clone());
        store
            .run()
            .await
            .map_err(|e| AgentError::Store(format!("failed to start store: {e}")))?;

        info!(
            node_id = %self.node_id,
            partition = partition,
            replica = replica,
            "started EventStore"
        );

        let managed = ManagedStore {
            store,
            backend: backend.clone(),
            partition,
            replica,
            state: StoreState::Starting,
            _started_at: Utc::now(),
            event_count: 0,
            drain_cancel: None,
        };

        self.stores
            .write()
            .await
            .insert((partition, replica), managed);

        // Trigger background recovery if recovery + membership are configured.
        if let (Some(recovery), Some(membership)) = (&self.recovery, &self.membership) {
            let recovery = Arc::clone(recovery);
            let membership = Arc::clone(membership);
            let stores = Arc::clone(&self.stores);
            let state_changed = Arc::clone(&self.state_changed);
            let backend = backend;

            tokio::spawn(async move {
                // Update state to Recovering.
                {
                    let mut guard = stores.write().await;
                    if let Some(m) = guard.get_mut(&(partition, replica)) {
                        m.state = StoreState::Recovering;
                    }
                }

                let peer_ids: Vec<String> = membership
                    .peer_nodes()
                    .await
                    .iter()
                    .map(|n| n.id.clone())
                    .collect();

                match recovery.recover(partition, &backend, &peer_ids).await {
                    Ok(count) => {
                        debug!(partition, recovered = count, "recovery finished");
                    }
                    Err(e) => {
                        warn!(partition, "recovery failed: {e}");
                    }
                }

                // Transition to Active regardless (store is already ingesting live events).
                let mut guard = stores.write().await;
                if let Some(m) = guard.get_mut(&(partition, replica)) {
                    m.state = StoreState::Active;
                }
                drop(guard);
                state_changed.notify_waiters();
            });
        } else {
            // No recovery configured — mark Active immediately.
            let mut guard = self.stores.write().await;
            if let Some(m) = guard.get_mut(&(partition, replica)) {
                m.state = StoreState::Active;
            }
        }

        Ok(())
    }

    async fn drain_store(&self, partition: u32, replica: u32) -> AgentResult<()> {
        let mut stores = self.stores.write().await;
        if let Some(managed) = stores.get_mut(&(partition, replica)) {
            if managed.state == StoreState::Draining {
                return Ok(());
            }
            managed.state = StoreState::Draining;
            info!(
                node_id = %self.node_id,
                partition = partition,
                replica = replica,
                "draining EventStore"
            );

            // Spawn delayed stop after grace period.
            let drain_cancel = CancellationToken::new();
            managed.drain_cancel = Some(drain_cancel.clone());
            let stores_ref = Arc::clone(&self.stores);
            let grace = self.drain_grace_period;
            let node_id = self.node_id.clone();

            tokio::spawn(async move {
                tokio::select! {
                    _ = drain_cancel.cancelled() => {
                        debug!(partition, replica, "drain cancelled");
                    }
                    _ = tokio::time::sleep(grace) => {
                        // Grace period elapsed - stop the store.
                        let mut stores = stores_ref.write().await;
                        if let Some(managed) = stores.remove(&(partition, replica)) {
                            managed.store.shutdown();
                            info!(
                                node_id = %node_id,
                                partition = partition,
                                replica = replica,
                                "stopped EventStore after drain"
                            );
                        }
                    }
                }
            });
        }
        Ok(())
    }

    async fn stop_store(&self, partition: u32, replica: u32) -> AgentResult<()> {
        let mut stores = self.stores.write().await;
        if let Some(mut managed) = stores.remove(&(partition, replica)) {
            // Cancel any pending drain timer.
            if let Some(cancel) = managed.drain_cancel.take() {
                cancel.cancel();
            }
            managed.store.shutdown();
            info!(
                node_id = %self.node_id,
                partition = partition,
                replica = replica,
                "stopped EventStore"
            );
        }
        Ok(())
    }

    fn store_path(&self, partition: u32, replica: u32) -> PathBuf {
        self.data_dir.join(format!("{partition}/{replica}"))
    }
}
