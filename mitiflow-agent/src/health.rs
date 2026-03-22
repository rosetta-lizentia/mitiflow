use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use zenoh::Session;
use zenoh::qos::CongestionControl;

use crate::error::AgentResult;
use crate::types::NodeHealth;

/// Periodically publishes node health metrics.
pub struct HealthReporter {
    health: Arc<RwLock<NodeHealth>>,
    _task: tokio::task::JoinHandle<()>,
    cancel: CancellationToken,
}

impl HealthReporter {
    pub async fn new(
        session: &Session,
        node_id: String,
        key_prefix: &str,
        interval: Duration,
    ) -> AgentResult<Self> {
        let health_key = format!("{key_prefix}/_cluster/health/{node_id}");
        let health = Arc::new(RwLock::new(NodeHealth {
            node_id: node_id.clone(),
            partitions_owned: 0,
            events_stored: 0,
            disk_usage_bytes: 0,
            store_latency_p99_us: 0,
            error_count: 0,
            timestamp: Utc::now(),
        }));

        let cancel = CancellationToken::new();
        let task = {
            let session = session.clone();
            let health = Arc::clone(&health);
            let cancel = cancel.clone();

            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        _ = ticker.tick() => {
                            let mut h = health.write().await;
                            h.timestamp = Utc::now();
                            let snapshot = h.clone();
                            drop(h);

                            match serde_json::to_vec(&snapshot) {
                                Ok(bytes) => {
                                    if let Err(e) = session
                                        .put(&health_key, bytes)
                                        .congestion_control(CongestionControl::Drop)
                                        .await
                                    {
                                        warn!("failed to publish health: {e}");
                                    }
                                }
                                Err(e) => warn!("failed to serialize health: {e}"),
                            }
                        }
                    }
                }
                debug!("health reporter stopped for {node_id}");
            })
        };

        Ok(Self {
            health,
            _task: task,
            cancel,
        })
    }

    /// Update the health stats (called by the agent after reconciliation).
    pub async fn update(
        &self,
        partitions_owned: u32,
        events_stored: u64,
        disk_usage_bytes: u64,
        store_latency_p99_us: u64,
        error_count: u64,
    ) {
        let mut h = self.health.write().await;
        h.partitions_owned = partitions_owned;
        h.events_stored = events_stored;
        h.disk_usage_bytes = disk_usage_bytes;
        h.store_latency_p99_us = store_latency_p99_us;
        h.error_count = error_count;
    }

    pub async fn shutdown(&self) {
        self.cancel.cancel();
    }
}
