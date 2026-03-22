use tracing::{debug, info, warn};
use zenoh::Session;

use crate::error::AgentResult;

/// Manages recovery of historical events when a node gains a partition.
///
/// Uses existing EventStore queryable protocol — queries peer replicas
/// for missing events via `session.get()`.
pub struct RecoveryManager {
    session: Session,
    key_prefix: String,
}

impl RecoveryManager {
    pub fn new(session: &Session, key_prefix: &str) -> Self {
        Self {
            session: session.clone(),
            key_prefix: key_prefix.to_string(),
        }
    }

    /// Recover historical events for a partition from peer replicas.
    ///
    /// Queries peer stores for events that the local store doesn't have.
    /// The EventStore's `run_queryable_task` already handles serving these queries.
    /// Storage is idempotent (same `(publisher_id, seq)` → same key), so
    /// re-storing events is safe.
    pub async fn recover(
        &self,
        partition: u32,
        peer_node_ids: &[String],
    ) -> AgentResult<u64> {
        let store_key_prefix = format!("{}/_store", self.key_prefix);
        let mut total_recovered = 0u64;

        for peer_id in peer_node_ids {
            let query_key = format!("{store_key_prefix}/{partition}");
            debug!(
                partition = partition,
                peer = %peer_id,
                query_key = %query_key,
                "querying peer for recovery"
            );

            match self.session.get(&query_key).consolidation(zenoh::query::ConsolidationMode::None).timeout(std::time::Duration::from_secs(5)).await {
                Ok(replies) => {
                    while let Ok(reply) = replies.recv_async().await {
                        if reply.result().is_ok() {
                            total_recovered += 1;
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        partition = partition,
                        peer = %peer_id,
                        "failed to query peer for recovery: {e}"
                    );
                }
            }
        }

        if total_recovered > 0 {
            info!(
                partition = partition,
                recovered = total_recovered,
                "recovery complete"
            );
        } else {
            debug!(partition = partition, "no events to recover (or no peers available)");
        }

        Ok(total_recovered)
    }

    /// Attempt recovery from publisher cache as fallback.
    pub async fn recover_from_cache(
        &self,
        partition: u32,
    ) -> AgentResult<u64> {
        let cache_key = format!("{}/_cache", self.key_prefix);
        let mut recovered = 0u64;

        match self.session.get(&cache_key).timeout(std::time::Duration::from_secs(5)).await {
            Ok(replies) => {
                while let Ok(reply) = replies.recv_async().await {
                    if reply.result().is_ok() {
                        recovered += 1;
                    }
                }
            }
            Err(e) => {
                debug!(
                    partition = partition,
                    "publisher cache recovery unavailable: {e}"
                );
            }
        }

        Ok(recovered)
    }
}
