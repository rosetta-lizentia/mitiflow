//! Partitioned consumer groups via rendezvous hashing + liveliness-driven rebalancing.
//!
//! This module is gated behind the `partition` feature flag.
//!
//! # Overview
//!
//! The [`PartitionManager`] distributes partitions across workers using
//! Rendezvous (HRW) hashing. Worker membership is managed through Zenoh
//! liveliness tokens — when a worker joins or leaves, partitions are
//! automatically rebalanced with minimal disruption.

pub mod hash_ring;
pub mod rebalance;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use zenoh::Session;

use crate::config::EventBusConfig;
use crate::error::{Error, Result};

/// Type alias for the shared rebalance callback.
pub type RebalanceCb = Arc<RwLock<Option<Box<dyn Fn(&[u32], &[u32]) + Send + Sync>>>>;

/// Manages partition assignment for a single worker in a consumer group.
///
/// On construction, declares a liveliness token and discovers existing workers.
/// A background task watches for membership changes and triggers rebalancing.
///
/// # Example
///
/// ```rust,no_run
/// # async fn run() -> mitiflow::Result<()> {
/// let session = zenoh::open(zenoh::Config::default()).await.unwrap();
/// let config = mitiflow::EventBusConfig::builder("myapp/events")
///     .worker_id("worker-1")
///     .num_partitions(64)
///     .build()?;
/// let pm = mitiflow::partition::PartitionManager::new(&session, config).await?;
/// println!("My partitions: {:?}", pm.my_partitions().await);
/// # Ok(())
/// # }
/// ```
pub struct PartitionManager {
    /// This worker's unique ID.
    worker_id: String,
    /// Number of partitions.
    num_partitions: u32,
    /// Current partition assignment for this worker.
    my_partitions: Arc<RwLock<Vec<u32>>>,
    /// Current set of all known workers.
    workers: Arc<RwLock<Vec<String>>>,
    /// Cancellation token for the background rebalance task.
    cancel: CancellationToken,
    /// Background task handle.
    _task: tokio::task::JoinHandle<()>,
    /// Rebalance callback (if registered before construction, pass via builder).
    rebalance_cb: RebalanceCb,
    /// Generation counter — increments on every membership change.
    generation: Arc<AtomicU64>,
}

impl PartitionManager {
    /// Create a new partition manager and start the membership watcher.
    pub async fn new(session: &Session, config: EventBusConfig) -> Result<Self> {
        let worker_id = config.worker_id.clone().ok_or_else(|| {
            Error::InvalidConfig("worker_id is required for PartitionManager".into())
        })?;
        let num_partitions = config.num_partitions;
        let liveliness_prefix = config.resolved_worker_liveliness_prefix();

        // Declare liveliness token so other workers see us.
        let token_key = format!("{liveliness_prefix}/{worker_id}");
        let _token = session.liveliness().declare_token(&token_key).await?;

        // Discover existing workers via liveliness get.
        let mut initial_workers = vec![worker_id.clone()];
        let replies = session
            .liveliness()
            .get(&format!("{liveliness_prefix}/*"))
            .await?;
        while let Ok(reply) = replies.recv_async().await {
            if let Ok(sample) = reply.result() {
                let key = sample.key_expr().as_str();
                if let Some(wid) = key.strip_prefix(&format!("{liveliness_prefix}/")) {
                    if !initial_workers.contains(&wid.to_string()) {
                        initial_workers.push(wid.to_string());
                    }
                }
            }
        }
        initial_workers.sort();

        // Compute initial assignment.
        let assignment = hash_ring::assignments(&initial_workers, num_partitions);
        let my_parts = assignment.get(&worker_id).cloned().unwrap_or_default();

        info!(
            worker = %worker_id,
            partitions = ?my_parts,
            total_workers = initial_workers.len(),
            "initial partition assignment"
        );

        let my_partitions = Arc::new(RwLock::new(my_parts));
        let workers = Arc::new(RwLock::new(initial_workers));
        let cancel = CancellationToken::new();
        let rebalance_cb: RebalanceCb = Arc::new(RwLock::new(None));
        let generation = Arc::new(AtomicU64::new(1));

        // Spawn the membership watcher task.
        let task = {
            let ctx = rebalance::MembershipContext {
                session: session.clone(),
                liveliness_prefix: liveliness_prefix.clone(),
                my_id: worker_id.clone(),
                num_partitions,
                my_partitions: Arc::clone(&my_partitions),
                workers: Arc::clone(&workers),
                rebalance_cb: Arc::clone(&rebalance_cb),
                cancel: cancel.clone(),
                generation: Arc::clone(&generation),
                _token,
            };
            let wid = worker_id.clone();

            tokio::spawn(async move {
                if let Err(e) = rebalance::membership_watcher(ctx).await {
                    warn!("membership watcher exited with error: {e}");
                }
                debug!("partition manager background task stopped for {wid}");
            })
        };

        Ok(Self {
            worker_id,
            num_partitions,
            my_partitions,
            workers,
            cancel,
            _task: task,
            rebalance_cb,
            generation,
        })
    }

    /// Compute which partition a key belongs to.
    pub fn partition_for(&self, key: &str) -> u32 {
        hash_ring::partition_for(key, self.num_partitions)
    }

    /// Returns this worker's currently assigned partitions.
    pub async fn my_partitions(&self) -> Vec<u32> {
        self.my_partitions.read().await.clone()
    }

    /// Returns all currently known worker IDs.
    pub async fn known_workers(&self) -> Vec<String> {
        self.workers.read().await.clone()
    }

    /// Register a callback invoked on rebalance with `(gained, lost)` partitions.
    pub async fn on_rebalance(&self, cb: impl Fn(&[u32], &[u32]) + Send + Sync + 'static) {
        let mut guard = self.rebalance_cb.write().await;
        *guard = Some(Box::new(cb));
    }

    /// Build Zenoh key expressions that match only this worker's partitions.
    ///
    /// Returns one key expression per assigned partition, e.g.
    /// `["prefix/p/0/**", "prefix/p/5/**", "prefix/p/12/**"]`.
    /// These can be used to declare individual Zenoh subscribers.
    pub async fn subscription_key_exprs(&self, key_prefix: &str) -> Vec<String> {
        let parts = self.my_partitions.read().await;
        if parts.is_empty() {
            return vec![format!("{key_prefix}/_none")];
        }
        parts
            .iter()
            .map(|p| format!("{key_prefix}/p/{p}/**"))
            .collect()
    }

    /// This worker's ID.
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Number of partitions.
    pub fn num_partitions(&self) -> u32 {
        self.num_partitions
    }

    /// Current generation counter (increments on every membership change).
    pub fn current_generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }
}

impl Drop for PartitionManager {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
