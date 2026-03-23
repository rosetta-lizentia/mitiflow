use std::sync::Arc;

use mitiflow::attachment::decode_metadata;
use mitiflow::store::backend::{EventMetadata, StorageBackend};
use tracing::{debug, info, trace, warn};
use zenoh::Session;

use crate::error::AgentResult;

/// Manages recovery of historical events when a node gains a partition.
///
/// Uses existing EventStore queryable protocol — queries peer replicas
/// for missing events via `session.get()`. Recovered events are written
/// directly to the local `StorageBackend` (idempotent keyed storage).
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
    /// Queries peer stores for events and writes them into `backend`.
    /// The EventStore's `run_queryable_task` already handles serving these
    /// queries with attachment metadata. Storage is idempotent (keyed by
    /// `(publisher_id, seq)`), so re-storing events is safe.
    ///
    /// Returns the number of events successfully stored.
    pub async fn recover(
        &self,
        partition: u32,
        backend: &Arc<dyn StorageBackend>,
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

            match self
                .session
                .get(&query_key)
                .consolidation(zenoh::query::ConsolidationMode::None)
                .accept_replies(zenoh::query::ReplyKeyExpr::Any)
                .timeout(std::time::Duration::from_secs(5))
                .await
            {
                Ok(replies) => {
                    while let Ok(reply) = replies.recv_async().await {
                        if let Ok(sample) = reply.result() {
                            if let Some(count) = self.ingest_sample(sample, backend) {
                                total_recovered += count;
                            }
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
            debug!(
                partition = partition,
                "no events to recover (or no peers available)"
            );
        }

        Ok(total_recovered)
    }

    /// Attempt recovery from publisher cache as fallback.
    ///
    /// Writes recovered events into `backend`. Returns the count stored.
    pub async fn recover_from_cache(
        &self,
        partition: u32,
        backend: &Arc<dyn StorageBackend>,
    ) -> AgentResult<u64> {
        let cache_key = format!("{}/_cache", self.key_prefix);
        let mut recovered = 0u64;

        match self
            .session
            .get(&cache_key)
            .accept_replies(zenoh::query::ReplyKeyExpr::Any)
            .timeout(std::time::Duration::from_secs(5))
            .await
        {
            Ok(replies) => {
                while let Ok(reply) = replies.recv_async().await {
                    if let Ok(sample) = reply.result() {
                        if let Some(count) = self.ingest_sample(sample, backend) {
                            recovered += count;
                        }
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

    /// Decode a Zenoh reply sample's attachment and store the event in the backend.
    ///
    /// Returns `Some(1)` on success, `None` on decode/store failure.
    fn ingest_sample(
        &self,
        sample: &zenoh::sample::Sample,
        backend: &Arc<dyn StorageBackend>,
    ) -> Option<u64> {
        let attachment = sample.attachment()?;
        let meta = match decode_metadata(attachment) {
            Ok(m) => m,
            Err(e) => {
                trace!("skipping reply without valid attachment: {e}");
                return None;
            }
        };

        let key = sample.key_expr().as_str().to_string();
        let payload = sample.payload().to_bytes().to_vec();

        let event_metadata = EventMetadata {
            seq: meta.seq,
            publisher_id: meta.pub_id,
            event_id: meta.event_id,
            timestamp: meta.timestamp,
            key_expr: key.clone(),
            key: mitiflow::extract_key(&key).map(|s| s.to_owned()),
            hlc_timestamp: None,
        };

        match backend.store(&key, &payload, event_metadata) {
            Ok(()) => Some(1),
            Err(e) => {
                warn!("failed to store recovered event: {e}");
                None
            }
        }
    }
}
