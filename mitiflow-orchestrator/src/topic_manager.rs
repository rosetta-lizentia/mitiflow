//! Manages per-topic [`ClusterView`] instances.
//!
//! When a topic specifies its own `key_prefix`, the orchestrator creates
//! a dedicated `ClusterView` so it can track agents/partitions scoped to
//! that prefix independently.

use std::collections::HashMap;

use tracing::{debug, info};
use zenoh::Session;

use crate::cluster_view::ClusterView;

/// Tracks a `ClusterView` for each topic that has its own key prefix.
pub struct TopicManager {
    session: Session,
    views: HashMap<String, ClusterView>,
}

impl TopicManager {
    /// Create an empty `TopicManager`.
    pub fn new(session: &Session) -> Self {
        Self {
            session: session.clone(),
            views: HashMap::new(),
        }
    }

    /// Create a `ClusterView` for the given topic prefix.
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
        info!(topic = %topic_name, key_prefix = %key_prefix, "per-topic cluster view created");
        Ok(true)
    }

    /// Remove and shut down the `ClusterView` for the given topic.
    ///
    /// Returns `true` if a view existed and was removed.
    pub async fn remove_topic(&mut self, topic_name: &str) -> bool {
        if let Some(cv) = self.views.remove(topic_name) {
            cv.shutdown().await;
            info!(topic = %topic_name, "per-topic cluster view removed");
            true
        } else {
            false
        }
    }

    /// Get a reference to the `ClusterView` for a topic.
    pub fn get_view(&self, topic_name: &str) -> Option<&ClusterView> {
        self.views.get(topic_name)
    }

    /// List all topics that have a dedicated `ClusterView`.
    pub fn tracked_topics(&self) -> Vec<&str> {
        self.views.keys().map(|k| k.as_str()).collect()
    }

    /// Shut down all per-topic cluster views.
    pub async fn shutdown(self) {
        for (name, cv) in self.views {
            debug!(topic = %name, "shutting down per-topic cluster view");
            cv.shutdown().await;
        }
    }
}
