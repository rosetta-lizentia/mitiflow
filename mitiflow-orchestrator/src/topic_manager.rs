//! Manages per-topic [`ClusterView`] and [`LagMonitor`] instances.
//!
//! When a topic specifies its own `key_prefix`, the orchestrator creates
//! a dedicated `ClusterView` and `LagMonitor` so it can track
//! agents/partitions/lag scoped to that prefix independently.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info, warn};
use zenoh::Session;

use crate::cluster_view::ClusterView;
use crate::lag::LagMonitor;

/// Default per-topic lag publish interval.
const DEFAULT_LAG_INTERVAL: Duration = Duration::from_secs(5);

/// Tracks a `ClusterView` and `LagMonitor` for each topic that has its own key prefix.
pub struct TopicManager {
    session: Session,
    views: HashMap<String, ClusterView>,
    lag_monitors: HashMap<String, Arc<LagMonitor>>,
}

impl TopicManager {
    /// Create an empty `TopicManager`.
    pub fn new(session: &Session) -> Self {
        Self {
            session: session.clone(),
            views: HashMap::new(),
            lag_monitors: HashMap::new(),
        }
    }

    /// Create a `ClusterView` and `LagMonitor` for the given topic prefix.
    ///
    /// Returns `Ok(true)` if a new view was created, `Ok(false)` if one
    /// already existed for this prefix.
    pub async fn add_topic(
        &mut self,
        topic_name: &str,
        key_prefix: &str,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if key_prefix.is_empty() {
            debug!(topic = %topic_name, "topic has no dedicated prefix, skipping per-topic view");
            return Ok(false);
        }
        if self.views.contains_key(topic_name) {
            return Ok(false);
        }
        let cv = ClusterView::new(&self.session, key_prefix).await?;
        self.views.insert(topic_name.to_string(), cv);

        // Also create a per-topic lag monitor
        match LagMonitor::new(&self.session, key_prefix, DEFAULT_LAG_INTERVAL).await {
            Ok(lm) => {
                self.lag_monitors
                    .insert(topic_name.to_string(), Arc::new(lm));
                info!(topic = %topic_name, key_prefix = %key_prefix, "per-topic cluster view and lag monitor created");
            }
            Err(e) => {
                warn!(topic = %topic_name, "per-topic lag monitor failed: {e}");
                info!(topic = %topic_name, key_prefix = %key_prefix, "per-topic cluster view created (lag monitor unavailable)");
            }
        }
        Ok(true)
    }

    /// Remove and shut down the `ClusterView` and `LagMonitor` for the given topic.
    ///
    /// Returns `true` if a view existed and was removed.
    pub async fn remove_topic(&mut self, topic_name: &str) -> bool {
        let had_view = if let Some(cv) = self.views.remove(topic_name) {
            cv.shutdown().await;
            true
        } else {
            false
        };
        if let Some(lm) = self.lag_monitors.remove(topic_name)
            && let Ok(lm) = Arc::try_unwrap(lm) {
                lm.shutdown().await;
            }
        if had_view {
            info!(topic = %topic_name, "per-topic cluster view and lag monitor removed");
        }
        had_view
    }

    /// Get a reference to the `ClusterView` for a topic.
    pub fn get_view(&self, topic_name: &str) -> Option<&ClusterView> {
        self.views.get(topic_name)
    }

    /// Get a reference to the `LagMonitor` for a topic.
    pub fn get_lag_monitor(&self, topic_name: &str) -> Option<&Arc<LagMonitor>> {
        self.lag_monitors.get(topic_name)
    }

    /// List all topics that have a dedicated `ClusterView`.
    pub fn tracked_topics(&self) -> Vec<&str> {
        self.views.keys().map(|k| k.as_str()).collect()
    }

    /// Shut down all per-topic cluster views and lag monitors.
    pub async fn shutdown(self) {
        for (name, cv) in self.views {
            debug!(topic = %name, "shutting down per-topic cluster view");
            cv.shutdown().await;
        }
        for (name, lm) in self.lag_monitors {
            debug!(topic = %name, "shutting down per-topic lag monitor");
            if let Ok(lm) = Arc::try_unwrap(lm) {
                lm.shutdown().await;
            }
        }
    }
}
