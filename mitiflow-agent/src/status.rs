use chrono::Utc;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use zenoh::Session;
use zenoh::qos::CongestionControl;

use crate::error::AgentResult;
use crate::types::{NodeStatus, PartitionStatus};

/// Publishes partition assignment status on-change and periodically.
pub struct StatusReporter {
    session: Session,
    node_id: String,
    status_key: String,
    cancel: CancellationToken,
    _periodic_task: tokio::task::JoinHandle<()>,
}

impl StatusReporter {
    pub async fn new(session: &Session, node_id: String, key_prefix: &str) -> AgentResult<Self> {
        let status_key = format!("{key_prefix}/_cluster/status/{node_id}");
        let cancel = CancellationToken::new();

        // Periodic heartbeat task (every 30s).
        let periodic_task = {
            let session = session.clone();
            let status_key = status_key.clone();
            let node_id = node_id.clone();
            let cancel = cancel.clone();

            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(std::time::Duration::from_secs(30));
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        _ = ticker.tick() => {
                            // Publish empty status as heartbeat.
                            let status = NodeStatus {
                                node_id: node_id.clone(),
                                partitions: Vec::new(),
                                timestamp: Utc::now(),
                            };
                            if let Ok(bytes) = serde_json::to_vec(&status) {
                                let _ = session
                                    .put(&status_key, bytes)
                                    .congestion_control(CongestionControl::Drop)
                                    .await;
                            }
                        }
                    }
                }
                debug!("status reporter stopped for {node_id}");
            })
        };

        Ok(Self {
            session: session.clone(),
            node_id,
            status_key,
            cancel,
            _periodic_task: periodic_task,
        })
    }

    /// Publish current partition status (called on reconciliation).
    pub async fn report(&self, partitions: &[PartitionStatus]) -> AgentResult<()> {
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
            .map_err(|e| crate::error::AgentError::Zenoh(e))?;
        Ok(())
    }

    pub async fn shutdown(&self) {
        self.cancel.cancel();
    }
}
