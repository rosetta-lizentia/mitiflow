//! Liveliness-driven rebalancing logic.
//!
//! Watches Zenoh liveliness events and recomputes partition assignments
//! when workers join or leave the consumer group.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};
use zenoh::Session;

use super::{hash_ring, RebalanceCb};

/// Shared state and configuration for the membership watcher.
pub struct MembershipContext {
    pub session: Session,
    pub liveliness_prefix: String,
    pub my_id: String,
    pub num_partitions: u32,
    pub my_partitions: Arc<RwLock<Vec<u32>>>,
    pub workers: Arc<RwLock<Vec<String>>>,
    pub rebalance_cb: RebalanceCb,
    pub cancel: CancellationToken,
    /// Generation counter — incremented on every membership change.
    pub generation: Arc<AtomicU64>,
    /// Kept alive to maintain presence — dropped when the task ends.
    pub _token: zenoh::liveliness::LivelinessToken,
}

/// Run the membership watcher loop.
///
/// This function is spawned as a background task by [`PartitionManager::new()`].
/// It subscribes to liveliness events for workers, and on any change
/// recomputes the HRW assignment and updates the shared state.
pub async fn membership_watcher(ctx: MembershipContext) -> crate::error::Result<()> {
    let subscriber = ctx.session
        .liveliness()
        .declare_subscriber(format!("{}/*", ctx.liveliness_prefix))
        .history(true)
        .await?;

    loop {
        tokio::select! {
            _ = ctx.cancel.cancelled() => break,
            sample_result = subscriber.recv_async() => {
                match sample_result {
                    Ok(sample) => {
                        let key = sample.key_expr().as_str();
                        let worker_id = match key.strip_prefix(&format!("{}/", ctx.liveliness_prefix)) {
                            Some(wid) => wid.to_string(),
                            None => continue,
                        };

                        let is_join = sample.kind() == zenoh::sample::SampleKind::Put;

                        let new_workers = {
                            let mut wkrs = ctx.workers.write().await;
                            if is_join {
                                if !wkrs.contains(&worker_id) {
                                    wkrs.push(worker_id.clone());
                                    wkrs.sort();
                                    info!(worker = %worker_id, "worker joined");
                                }
                            } else {
                                wkrs.retain(|w| w != &worker_id);
                                info!(worker = %worker_id, "worker left");
                            }
                            wkrs.clone()
                        };

                        rebalance(
                            &ctx.my_id,
                            ctx.num_partitions,
                            &new_workers,
                            &ctx.my_partitions,
                            &ctx.rebalance_cb,
                            &ctx.generation,
                        )
                        .await;
                    }
                    Err(_) => break,
                }
            }
        }
    }

    debug!("membership watcher stopped");
    Ok(())
}

/// Recompute partition assignment and fire the rebalance callback.
async fn rebalance(
    my_id: &str,
    num_partitions: u32,
    workers: &[String],
    my_partitions: &Arc<RwLock<Vec<u32>>>,
    rebalance_cb: &RebalanceCb,
    generation: &Arc<AtomicU64>,
) {
    let assignment = hash_ring::assignments(workers, num_partitions);
    let new_parts = assignment.get(my_id).cloned().unwrap_or_default();

    let (gained, lost) = {
        let old_parts = my_partitions.read().await;
        let gained: Vec<u32> = new_parts
            .iter()
            .copied()
            .filter(|p| !old_parts.contains(p))
            .collect();
        let lost: Vec<u32> = old_parts
            .iter()
            .copied()
            .filter(|p| !new_parts.contains(p))
            .collect();
        (gained, lost)
    };

    if gained.is_empty() && lost.is_empty() {
        return;
    }

    // Increment generation on every actual rebalance.
    generation.fetch_add(1, Ordering::SeqCst);

    // Update stored assignment.
    {
        let mut parts = my_partitions.write().await;
        *parts = new_parts;
    }

    info!(
        gained = ?gained,
        lost = ?lost,
        "partition rebalance complete"
    );

    // Fire rebalance callback if registered.
    let cb_guard = rebalance_cb.read().await;
    if let Some(ref cb) = *cb_guard {
        cb(&gained, &lost);
    }
}
