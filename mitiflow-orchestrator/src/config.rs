//! Topic and partition configuration management.

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Topic configuration managed by the orchestrator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConfig {
    pub name: String,
    /// Per-topic Zenoh key prefix. When set, the orchestrator creates a
    /// dedicated [`ClusterView`] for this prefix.  Defaults to `""` for
    /// backwards-compatible configs that share the orchestrator-level prefix.
    #[serde(default)]
    pub key_prefix: String,
    pub num_partitions: u32,
    pub replication_factor: u32,
    pub retention: RetentionPolicy,
    pub compaction: CompactionPolicy,
    /// Labels that an agent **must** have to serve this topic.
    /// An agent serves this topic only if its own labels contain all
    /// of these key-value pairs.
    #[serde(default)]
    pub required_labels: HashMap<String, String>,
    /// Labels that **exclude** an agent from serving this topic.
    /// If an agent's labels match any of these key-value pairs, it
    /// will not serve this topic.
    #[serde(default)]
    pub excluded_labels: HashMap<String, String>,
}

/// Retention policy for events.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RetentionPolicy {
    /// Max age of events before GC.
    pub max_age: Option<Duration>,
    /// Max total size in bytes per partition.
    pub max_bytes: Option<u64>,
    /// Max number of events per partition.
    pub max_events: Option<u64>,
}

/// Compaction policy.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CompactionPolicy {
    pub enabled: bool,
    /// Compaction interval.
    pub interval: Option<Duration>,
}

/// Persistent config store backed by fjall.
pub struct ConfigStore {
    #[allow(dead_code)]
    db: fjall::Database,
    topics: fjall::Keyspace,
}

impl ConfigStore {
    /// Open or create a config store at the given directory.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, fjall::Error> {
        let db = fjall::Database::builder(dir).open()?;
        let topics = db.keyspace("topics", fjall::KeyspaceCreateOptions::default)?;
        Ok(Self { db, topics })
    }

    /// Store a topic configuration.
    pub fn put_topic(
        &self,
        config: &TopicConfig,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let value = serde_json::to_vec(config)?;
        self.topics.insert(&config.name, value)?;
        Ok(())
    }

    /// Get a topic configuration by name.
    pub fn get_topic(
        &self,
        name: &str,
    ) -> Result<Option<TopicConfig>, Box<dyn std::error::Error + Send + Sync>> {
        match self.topics.get(name)? {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    /// List all topic configurations.
    pub fn list_topics(
        &self,
    ) -> Result<Vec<TopicConfig>, Box<dyn std::error::Error + Send + Sync>> {
        let mut topics = Vec::new();
        for guard in self.topics.iter() {
            let kv =
                guard
                    .into_inner()
                    .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                        Box::new(std::io::Error::other(format!("iter error: {e:?}")))
                    })?;
            let config: TopicConfig = serde_json::from_slice(&kv.1)?;
            topics.push(config);
        }
        Ok(topics)
    }

    /// Delete a topic configuration.
    pub fn delete_topic(&self, name: &str) -> Result<bool, fjall::Error> {
        if self.topics.get(name)?.is_some() {
            self.topics.remove(name)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
