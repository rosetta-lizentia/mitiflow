//! Liveliness-driven rebalancing logic.
//!
//! Watches Zenoh liveliness events and recomputes partition assignments
//! when workers join or leave the consumer group.

use std::sync::Arc;

use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};
use zenoh::Session;

use super::{hash_ring, RebalanceCb};

/// Run the membership watcher loop.
///
/// This function is spawned as a background task by [`PartitionManager::new()`].
/// It subscribes to liveliness events for workers, and on any change
/// recomputes the HRW assignment and updates the shared state.
///
/// The `_token` parameter keeps the liveliness token alive as long as this task runs.
#[allow(clippy::too_many_arguments)]
pub async fn membership_watcher(
    session: &Session,
    liveliness_prefix: &str,
    my_id: &str,
    num_partitions: u32,
    my_partitions: &Arc<RwLock<Vec<u32>>>,
    workers: &Arc<RwLock<Vec<String>>>,
    rebalance_cb: &RebalanceCb,
    cancel: CancellationToken,
    _token: zenoh::liveliness::LivelinessToken,
) -> crate::error::Result<()> {
    let subscriber = session
        .liveliness()
        .declare_subscriber(format!("{liveliness_prefix}/*"))
        .await?;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            sample_result = subscriber.recv_async() => {
                match sample_result {
                    Ok(sample) => {
                        let key = sample.key_expr().as_str();
                        let worker_id = match key.strip_prefix(&format!("{liveliness_prefix}/")) {
                            Some(wid) => wid.to_string(),
                            None => continue,
                        };

                        let is_join = sample.kind() == zenoh::sample::SampleKind::Put;

                        // Update the workers list.
                        let new_workers = {
                            let mut wkrs = workers.write().await;
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

                        // Recompute assignments.
                        rebalance(
                            my_id,
                            num_partitions,
                            &new_workers,
                            my_partitions,
                            rebalance_cb,
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
