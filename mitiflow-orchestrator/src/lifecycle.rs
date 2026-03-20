//! Store lifecycle tracking via liveliness.
//!
//! Tracks EventStore instances that register via liveliness tokens at
//! `{key_prefix}/_store/{partition}`. Detects when stores go offline.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use zenoh::Session;

/// State of a tracked store instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreInfo {
    pub partition: u32,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub online: bool,
}

/// Tracks EventStore instances via liveliness.
pub struct StoreTracker {
    #[allow(dead_code)]
    session: Session,
    #[allow(dead_code)]
    key_prefix: String,
    /// Map of partition → store info.
    stores: Arc<RwLock<HashMap<u32, StoreInfo>>>,
    cancel: CancellationToken,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl StoreTracker {
    /// Start tracking stores for the given key prefix.
    pub async fn new(session: &Session, key_prefix: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let cancel = CancellationToken::new();
        let stores: Arc<RwLock<HashMap<u32, StoreInfo>>> =
            Arc::new(RwLock::new(HashMap::new()));

        // Discover existing stores via liveliness get
        let liveliness_key = format!("{key_prefix}/_store/*");
        let replies = session.liveliness().get(&liveliness_key).await?;
        let mut initial_stores = HashMap::new();
        while let Ok(reply) = replies.recv_async().await {
            if let Ok(sample) = reply.result() {
                let key = sample.key_expr().as_str();
                if let Some(partition) = extract_store_partition(key, key_prefix) {
                    let now = Utc::now();
                    initial_stores.insert(
                        partition,
                        StoreInfo {
                            partition,
                            first_seen: now,
                            last_seen: now,
                            online: true,
                        },
                    );
                }
            }
        }
        {
            let mut guard = stores.write().await;
            *guard = initial_stores;
        }

        // Watch for changes
        let sub = session
            .liveliness()
            .declare_subscriber(&liveliness_key)
            .await?;
        let watch_cancel = cancel.clone();
        let watch_stores = Arc::clone(&stores);
        let watch_prefix = key_prefix.to_string();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = watch_cancel.cancelled() => break,
                    result = sub.recv_async() => {
                        match result {
                            Ok(sample) => {
                                let key = sample.key_expr().as_str();
                                let kind = sample.kind();
                                if let Some(partition) = extract_store_partition(key, &watch_prefix) {
                                    let mut map = watch_stores.write().await;
                                    match kind {
                                        zenoh::sample::SampleKind::Put => {
                                            let now = Utc::now();
                                            let info = map.entry(partition).or_insert(StoreInfo {
                                                partition,
                                                first_seen: now,
                                                last_seen: now,
                                                online: true,
                                            });
                                            info.last_seen = now;
                                            info.online = true;
                                            info!("store for partition {partition} came online");
                                        }
                                        zenoh::sample::SampleKind::Delete => {
                                            if let Some(info) = map.get_mut(&partition) {
                                                info.online = false;
                                                warn!("store for partition {partition} went offline");
                                            }
                                        }
                                    }
                                }
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
            debug!("store tracker task stopped");
        });

        Ok(Self {
            session: session.clone(),
            key_prefix: key_prefix.to_string(),
            stores,
            cancel,
            task: Some(task),
        })
    }

    /// Get all tracked stores.
    pub async fn stores(&self) -> HashMap<u32, StoreInfo> {
        self.stores.read().await.clone()
    }

    /// Get online stores.
    pub async fn online_stores(&self) -> Vec<StoreInfo> {
        self.stores
            .read()
            .await
            .values()
            .filter(|s| s.online)
            .cloned()
            .collect()
    }

    /// Get offline stores.
    pub async fn offline_stores(&self) -> Vec<StoreInfo> {
        self.stores
            .read()
            .await
            .values()
            .filter(|s| !s.online)
            .cloned()
            .collect()
    }

    /// Check if a specific partition has an online store.
    pub async fn is_partition_online(&self, partition: u32) -> bool {
        self.stores
            .read()
            .await
            .get(&partition)
            .is_some_and(|s| s.online)
    }

    /// Shut down the tracker.
    pub async fn shutdown(mut self) {
        self.cancel.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for StoreTracker {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Extract partition number from a key like `prefix/_store/3`.
fn extract_store_partition(key: &str, prefix: &str) -> Option<u32> {
    let suffix = key.strip_prefix(&format!("{prefix}/_store/"))?;
    suffix.parse().ok()
}
