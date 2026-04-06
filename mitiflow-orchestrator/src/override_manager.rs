//! Override manager — publishes partition placement overrides consumed by agents.

use std::sync::Arc;

use chrono::{Duration as ChronoDuration, Utc};
use mitiflow_storage::{OverrideEntry, OverrideTable};
use tokio::sync::RwLock;
use tracing::info;
use zenoh::Session;

/// Manages partition placement overrides published via Zenoh.
///
/// Agents subscribe to `{key_prefix}/_cluster/overrides` and apply overrides
/// that take precedence over HRW-computed assignments.
pub struct OverrideManager {
    session: Session,
    overrides_key: String,
    current: Arc<RwLock<OverrideTable>>,
}

impl OverrideManager {
    /// Create a new override manager.
    pub fn new(session: &Session, key_prefix: &str) -> Self {
        Self {
            session: session.clone(),
            overrides_key: format!("{key_prefix}/_cluster/overrides"),
            current: Arc::new(RwLock::new(OverrideTable::default())),
        }
    }

    /// Publish override entries. Replaces all current entries.
    ///
    /// Auto-increments epoch. If `ttl` is provided, sets `expires_at` relative to now.
    pub async fn publish_entries(
        &self,
        entries: Vec<OverrideEntry>,
        ttl: Option<std::time::Duration>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut current = self.current.write().await;
        let new_epoch = current.epoch + 1;
        let expires_at = ttl.map(|d| {
            Utc::now() + ChronoDuration::from_std(d).unwrap_or(ChronoDuration::seconds(0))
        });

        let table = OverrideTable {
            entries,
            epoch: new_epoch,
            expires_at,
        };

        let bytes = serde_json::to_vec(&table)?;
        self.session.put(&self.overrides_key, bytes).await?;

        info!(
            epoch = new_epoch,
            entries = table.entries.len(),
            "overrides published"
        );
        *current = table;
        Ok(())
    }

    /// Get the current override table.
    pub async fn current(&self) -> OverrideTable {
        self.current.read().await.clone()
    }

    /// Clear all overrides by publishing an empty table with epoch+1.
    pub async fn clear(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.publish_entries(Vec::new(), None).await
    }

    /// Remove all override entries targeting a specific node.
    pub async fn remove_entries_for_node(
        &self,
        node_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let remaining: Vec<OverrideEntry> = self
            .current
            .read()
            .await
            .entries
            .iter()
            .filter(|e| e.node_id != node_id)
            .cloned()
            .collect();
        self.publish_entries(remaining, None).await
    }

    /// Add overrides for specific entries without removing existing ones.
    ///
    /// If an entry for the same `(partition, replica)` already exists, it is replaced.
    pub async fn add_entries(
        &self,
        new_entries: Vec<OverrideEntry>,
        ttl: Option<std::time::Duration>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut merged: Vec<OverrideEntry> = self.current.read().await.entries.clone();

        for new in new_entries {
            // Remove existing entry for same (partition, replica)
            merged.retain(|e| !(e.partition == new.partition && e.replica == new.replica));
            merged.push(new);
        }

        self.publish_entries(merged, ttl).await
    }
}
