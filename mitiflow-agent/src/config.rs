use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use mitiflow::EventBusConfig;

use crate::error::AgentError;

/// Configuration for a StorageAgent node.
#[derive(Debug, Clone)]
pub struct StorageAgentConfig {
    /// Unique node identifier. Default: random UUID.
    pub node_id: String,
    /// Base directory for all partition data.
    pub data_dir: PathBuf,
    /// Node capacity weight for HRW assignment (default: 100).
    pub capacity: u32,
    /// Labels for rack-aware placement.
    pub labels: HashMap<String, String>,
    /// Number of partitions to manage.
    pub num_partitions: u32,
    /// Replication factor (default: 1).
    pub replication_factor: u32,
    /// Grace period before stopping a draining store (default: 30s).
    pub drain_grace_period: Duration,
    /// How often to publish health metrics (default: 10s).
    pub health_interval: Duration,
    /// EventBusConfig for the managed topic.
    pub bus_config: EventBusConfig,
}

/// Builder for [`StorageAgentConfig`].
pub struct StorageAgentConfigBuilder {
    node_id: Option<String>,
    data_dir: PathBuf,
    capacity: u32,
    labels: HashMap<String, String>,
    num_partitions: u32,
    replication_factor: u32,
    drain_grace_period: Duration,
    health_interval: Duration,
    bus_config: EventBusConfig,
}

impl StorageAgentConfigBuilder {
    pub fn new(data_dir: PathBuf, bus_config: EventBusConfig) -> Self {
        Self {
            node_id: None,
            data_dir,
            capacity: 100,
            labels: HashMap::new(),
            num_partitions: 16,
            replication_factor: 1,
            drain_grace_period: Duration::from_secs(30),
            health_interval: Duration::from_secs(10),
            bus_config,
        }
    }

    pub fn node_id(mut self, id: impl Into<String>) -> Self {
        self.node_id = Some(id.into());
        self
    }

    pub fn capacity(mut self, capacity: u32) -> Self {
        self.capacity = capacity;
        self
    }

    pub fn labels(mut self, labels: HashMap<String, String>) -> Self {
        self.labels = labels;
        self
    }

    pub fn num_partitions(mut self, n: u32) -> Self {
        self.num_partitions = n;
        self
    }

    pub fn replication_factor(mut self, rf: u32) -> Self {
        self.replication_factor = rf;
        self
    }

    pub fn drain_grace_period(mut self, d: Duration) -> Self {
        self.drain_grace_period = d;
        self
    }

    pub fn health_interval(mut self, d: Duration) -> Self {
        self.health_interval = d;
        self
    }

    pub fn build(self) -> Result<StorageAgentConfig, AgentError> {
        let node_id = self
            .node_id
            .unwrap_or_else(|| uuid::Uuid::now_v7().to_string());

        if self.num_partitions == 0 {
            return Err(AgentError::Config(
                "num_partitions must be > 0".into(),
            ));
        }
        if self.replication_factor == 0 {
            return Err(AgentError::Config(
                "replication_factor must be > 0".into(),
            ));
        }

        Ok(StorageAgentConfig {
            node_id,
            data_dir: self.data_dir,
            capacity: self.capacity,
            labels: self.labels,
            num_partitions: self.num_partitions,
            replication_factor: self.replication_factor,
            drain_grace_period: self.drain_grace_period,
            health_interval: self.health_interval,
            bus_config: self.bus_config,
        })
    }
}

impl StorageAgentConfig {
    /// Start building a configuration.
    pub fn builder(data_dir: PathBuf, bus_config: EventBusConfig) -> StorageAgentConfigBuilder {
        StorageAgentConfigBuilder::new(data_dir, bus_config)
    }

    /// Key prefix from the underlying EventBusConfig.
    pub fn key_prefix(&self) -> &str {
        &self.bus_config.key_prefix
    }

    /// Liveliness key for this agent node: `{key_prefix}/_agents/{node_id}`.
    pub fn agent_liveliness_key(&self) -> String {
        format!("{}/_agents/{}", self.key_prefix(), self.node_id)
    }

    /// Metadata key for this agent: `{key_prefix}/_agents/{node_id}/meta`.
    pub fn agent_metadata_key(&self) -> String {
        format!("{}/_agents/{}/meta", self.key_prefix(), self.node_id)
    }

    /// Health key for this agent: `{key_prefix}/_cluster/health/{node_id}`.
    pub fn health_key(&self) -> String {
        format!("{}/_cluster/health/{}", self.key_prefix(), self.node_id)
    }

    /// Status key for this agent: `{key_prefix}/_cluster/status/{node_id}`.
    pub fn status_key(&self) -> String {
        format!("{}/_cluster/status/{}", self.key_prefix(), self.node_id)
    }

    /// Override subscription key: `{key_prefix}/_cluster/overrides`.
    pub fn overrides_key(&self) -> String {
        format!("{}/_cluster/overrides", self.key_prefix())
    }

    /// Agents liveliness prefix for discovery: `{key_prefix}/_agents`.
    pub fn agents_prefix(&self) -> String {
        format!("{}/_agents", self.key_prefix())
    }
}
