use std::sync::Arc;

use chrono::Utc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use zenoh::Session;
use zenoh::bytes::ZBytes;
use zenoh::qos::CongestionControl;

use crate::error::AgentResult;
use crate::types::{NodeStatus, PartitionStatus};

/// Publishes partition assignment status on reconciliation and serves it
/// via a Zenoh queryable so the orchestrator can fetch it on demand
/// (no periodic heartbeat needed).
pub struct StatusReporter {
    session: Session,
    node_id: String,
    status_key: String,
    last_partitions: Arc<RwLock<Vec<PartitionStatus>>>,
    cancel: CancellationToken,
    _queryable_task: tokio::task::JoinHandle<()>,
}

impl StatusReporter {
    pub async fn new(session: &Session, node_id: String, key_prefix: &str) -> AgentResult<Self> {
        let status_key = format!("{key_prefix}/_cluster/status/{node_id}");
        let cancel = CancellationToken::new();
        let last_partitions: Arc<RwLock<Vec<PartitionStatus>>> = Arc::new(RwLock::new(Vec::new()));

        // Declare a queryable so the orchestrator (or any peer) can fetch
        // current partition status on demand without periodic republishing.
        let queryable_task = {
            let queryable = session.declare_queryable(&status_key).await?;
            let node_id = node_id.clone();
            let cancel = cancel.clone();
            let last_partitions = Arc::clone(&last_partitions);

            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        result = queryable.recv_async() => {
                            match result {
                                Ok(query) => {
                                    let partitions = last_partitions.read().await.clone();
                                    let status = NodeStatus {
                                        node_id: node_id.clone(),
                                        partitions,
                                        timestamp: Utc::now(),
                                    };
                                    match serde_json::to_vec(&status) {
                                        Ok(bytes) => {
                                            if let Err(e) = query.reply(query.key_expr(), ZBytes::from(bytes)).await {
                                                warn!("failed to reply to status query: {e}");
                                            }
                                        }
                                        Err(e) => warn!("failed to serialize status for query: {e}"),
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                    }
                }
                debug!("status reporter queryable stopped for {node_id}");
            })
        };

        Ok(Self {
            session: session.clone(),
            node_id,
            status_key,
            last_partitions,
            cancel,
            _queryable_task: queryable_task,
        })
    }

    /// Publish current partition status (called on reconciliation).
    pub async fn report(&self, partitions: &[PartitionStatus]) -> AgentResult<()> {
        *self.last_partitions.write().await = partitions.to_vec();
        let status = NodeStatus {
            node_id: self.node_id.clone(),
            partitions: partitions.to_vec(),
            timestamp: Utc::now(),
        };
        let bytes = serde_json::to_vec(&status)?;
        self.session
            .put(&self.status_key, bytes)
            .congestion_control(CongestionControl::Drop)
            .await
            .map_err(crate::error::AgentError::Zenoh)?;
        Ok(())
    }

    pub async fn shutdown(&self) {
        self.cancel.cancel();
    }
}
