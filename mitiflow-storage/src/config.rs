use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use mitiflow::EventBusConfig;
use tracing::info;

use crate::error::AgentError;

// ---------------------------------------------------------------------------
// Node identity persistence
// ---------------------------------------------------------------------------

const NODE_ID_FILENAME: &str = ".node_id";

/// Resolve a stable node ID using the following priority:
///
/// 1. `MITIFLOW_NODE_ID` environment variable (highest priority)
/// 2. Existing `.node_id` file in `data_dir`
/// 3. Generate a new UUID v7, persist it to `data_dir/.node_id`, and return it
///
/// This ensures a storage agent keeps the same identity across restarts as long
/// as its data directory is preserved.
fn resolve_or_persist_node_id(data_dir: &Path) -> String {
    // 1. Environment variable takes precedence
    if let Ok(id) = std::env::var("MITIFLOW_NODE_ID")
        && !id.is_empty()
    {
        info!(node_id = %id, source = "env", "resolved node identity");
        return id;
    }

    // 2. Read from persisted file
    let id_path = data_dir.join(NODE_ID_FILENAME);
    if let Ok(contents) = std::fs::read_to_string(&id_path) {
        let id = contents.trim().to_string();
        if !id.is_empty() {
            info!(node_id = %id, source = "file", path = %id_path.display(), "resolved node identity");
            return id;
        }
    }

    // 3. Generate new UUID v7 and persist it
    let id = uuid::Uuid::now_v7().to_string();
    if let Err(e) = std::fs::create_dir_all(data_dir) {
        tracing::warn!(error = %e, "failed to create data_dir for node_id persistence");
    }
    if let Err(e) = std::fs::write(&id_path, &id) {
        tracing::warn!(error = %e, path = %id_path.display(), "failed to persist node_id (will regenerate on next restart)");
    } else {
        info!(node_id = %id, source = "generated", path = %id_path.display(), "persisted new node identity");
    }
    id
}

// ---------------------------------------------------------------------------
// TopicEntry — static topic entry for multi-topic agent config
// ---------------------------------------------------------------------------

/// A static topic entry in multi-topic agent configuration.
#[derive(Debug, Clone)]
pub struct TopicEntry {
    /// Human-readable topic name (also used as directory name).
    pub name: String,
    /// Zenoh key prefix for this topic.
    pub key_prefix: String,
    /// Number of partitions for this topic.
    pub num_partitions: u32,
    /// Replication factor for this topic.
    pub replication_factor: u32,
}

// ---------------------------------------------------------------------------
// TopicWorkerConfig — per-topic configuration for a TopicWorker
// ---------------------------------------------------------------------------

/// Configuration for a single [`TopicWorker`].
///
/// Contains all settings needed to run one topic's set of `EventStore`
/// instances: membership tracking, reconciliation, recovery, and status
/// reporting.
#[derive(Debug, Clone)]
pub struct TopicWorkerConfig {
    /// Topic name (used in data path and logging).
    pub topic_name: String,
    /// Zenoh key prefix for this topic.
    pub key_prefix: String,
    /// Number of partitions to manage.
    pub num_partitions: u32,
    /// Replication factor (default: 1).
    pub replication_factor: u32,
    /// Node capacity weight for HRW assignment.
    pub capacity: u32,
    /// Labels for rack-aware placement.
    pub labels: HashMap<String, String>,
    /// Grace period before stopping a draining store.
    pub drain_grace_period: Duration,
    /// Base directory for this topic's partition data.
    /// Typically `{agent_data_dir}/{topic_name}`.
    pub data_dir: PathBuf,
    /// EventBusConfig for the managed topic.
    pub bus_config: EventBusConfig,
}

impl TopicWorkerConfig {
    /// Derive per-topic config from an agent-level config and a [`TopicEntry`].
    pub fn from_entry(
        entry: &TopicEntry,
        base_data_dir: &Path,
        capacity: u32,
        labels: &HashMap<String, String>,
        drain_grace_period: Duration,
    ) -> Result<Self, AgentError> {
        if entry.num_partitions == 0 {
            return Err(AgentError::Config(format!(
                "topic '{}': num_partitions must be > 0",
                entry.name
            )));
        }
        if entry.replication_factor == 0 {
            return Err(AgentError::Config(format!(
                "topic '{}': replication_factor must be > 0",
                entry.name
            )));
        }
        let bus_config = EventBusConfig::builder(&entry.key_prefix)
            .cache_size(100)
            .build()
            .map_err(|e| AgentError::Config(format!("topic '{}': {e}", entry.name)))?;
        Ok(Self {
            topic_name: entry.name.clone(),
            key_prefix: entry.key_prefix.clone(),
            num_partitions: entry.num_partitions,
            replication_factor: entry.replication_factor,
            capacity,
            labels: labels.clone(),
            drain_grace_period,
            data_dir: base_data_dir.join(&entry.name),
            bus_config,
        })
    }
}

// ---------------------------------------------------------------------------
// AgentConfig — node-level configuration for multi-topic agent
// ---------------------------------------------------------------------------

/// Configuration for a multi-topic storage agent node.
///
/// Separates node-level concerns (identity, capacity, labels) from per-topic
/// settings. Each topic in [`topics`] spawns an independent [`TopicWorker`].
#[derive(Debug, Clone)]
pub struct AgentConfig {
    /// Unique node identifier. Default: random UUID v7.
    pub node_id: String,
    /// Base directory for all topic data. Each topic creates a subdirectory.
    pub data_dir: PathBuf,
    /// Node capacity weight for HRW assignment (default: 100).
    pub capacity: u32,
    /// Labels for rack-aware placement.
    pub labels: HashMap<String, String>,
    /// How often to publish health metrics (default: 10s).
    pub health_interval: Duration,
    /// Grace period before stopping a draining store (default: 30s).
    pub drain_grace_period: Duration,
    /// Well-known global prefix for topic discovery (default: `"mitiflow"`).
    pub global_prefix: String,
    /// Whether to subscribe to `{global_prefix}/_config/**` and auto-serve
    /// topics published by the orchestrator (Phase B).
    pub auto_discover_topics: bool,
    /// Static topic list (for running without orchestrator).
    pub topics: Vec<TopicEntry>,
}

/// Builder for [`AgentConfig`].
pub struct AgentConfigBuilder {
    node_id: Option<String>,
    data_dir: PathBuf,
    capacity: u32,
    labels: HashMap<String, String>,
    health_interval: Duration,
    drain_grace_period: Duration,
    global_prefix: String,
    auto_discover_topics: bool,
    topics: Vec<TopicEntry>,
}

impl AgentConfigBuilder {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            node_id: None,
            data_dir,
            capacity: 100,
            labels: HashMap::new(),
            health_interval: Duration::from_secs(10),
            drain_grace_period: Duration::from_secs(30),
            global_prefix: "mitiflow".to_string(),
            auto_discover_topics: false,
            topics: Vec::new(),
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

    pub fn health_interval(mut self, d: Duration) -> Self {
        self.health_interval = d;
        self
    }

    pub fn drain_grace_period(mut self, d: Duration) -> Self {
        self.drain_grace_period = d;
        self
    }

    pub fn global_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.global_prefix = prefix.into();
        self
    }

    pub fn auto_discover_topics(mut self, enabled: bool) -> Self {
        self.auto_discover_topics = enabled;
        self
    }

    pub fn topic(mut self, entry: TopicEntry) -> Self {
        self.topics.push(entry);
        self
    }

    pub fn topics(mut self, entries: Vec<TopicEntry>) -> Self {
        self.topics = entries;
        self
    }

    pub fn build(self) -> Result<AgentConfig, AgentError> {
        let node_id = self
            .node_id
            .unwrap_or_else(|| resolve_or_persist_node_id(&self.data_dir));

        if self.topics.is_empty() && !self.auto_discover_topics {
            return Err(AgentError::Config(
                "at least one topic or auto_discover_topics must be set".into(),
            ));
        }

        Ok(AgentConfig {
            node_id,
            data_dir: self.data_dir,
            capacity: self.capacity,
            labels: self.labels,
            health_interval: self.health_interval,
            drain_grace_period: self.drain_grace_period,
            global_prefix: self.global_prefix,
            auto_discover_topics: self.auto_discover_topics,
            topics: self.topics,
        })
    }
}

impl AgentConfig {
    pub fn builder(data_dir: PathBuf) -> AgentConfigBuilder {
        AgentConfigBuilder::new(data_dir)
    }
}

/// Convert a legacy [`StorageAgentConfig`] into [`AgentConfig`] + one topic.
impl From<StorageAgentConfig> for AgentConfig {
    fn from(old: StorageAgentConfig) -> Self {
        let key_prefix = old.bus_config.key_prefix.clone();
        let topic = TopicEntry {
            name: key_prefix.replace('/', "_"),
            key_prefix: key_prefix.clone(),
            num_partitions: old.num_partitions,
            replication_factor: old.replication_factor,
        };
        AgentConfig {
            node_id: old.node_id,
            data_dir: old.data_dir,
            capacity: old.capacity,
            labels: old.labels,
            health_interval: old.health_interval,
            drain_grace_period: old.drain_grace_period,
            // Use the topic's key_prefix so health/liveliness keys stay in the
            // same namespace as before (backward compat for single-topic agents).
            global_prefix: key_prefix,
            auto_discover_topics: false,
            topics: vec![topic],
        }
    }
}

// ---------------------------------------------------------------------------
// StorageAgentConfig — legacy single-topic config (kept for backward compat)
// ---------------------------------------------------------------------------

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
            .unwrap_or_else(|| resolve_or_persist_node_id(&self.data_dir));

        if self.num_partitions == 0 {
            return Err(AgentError::Config("num_partitions must be > 0".into()));
        }
        if self.replication_factor == 0 {
            return Err(AgentError::Config("replication_factor must be > 0".into()));
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

// ---------------------------------------------------------------------------
// YAML configuration — file-based config with env-var overrides
// ---------------------------------------------------------------------------

/// YAML file configuration for a storage agent.
///
/// ```yaml
/// node:
///   id: auto
///   data_dir: /var/lib/mitiflow
///   capacity: 100
///   health_interval: 10s
///   drain_grace_period: 30s
///   labels:
///     rack: us-east-1a
///
/// cluster:
///   global_prefix: mitiflow
///   auto_discover_topics: true
///
/// topics:
///   - name: events
///     key_prefix: app/events
///     num_partitions: 16
///     replication_factor: 2
/// ```
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct AgentYamlConfig {
    #[serde(default)]
    pub node: NodeYamlConfig,
    #[serde(default)]
    pub cluster: ClusterYamlConfig,
    #[serde(default)]
    pub topics: Vec<TopicYamlEntry>,
}

/// Node-level settings.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct NodeYamlConfig {
    /// Node ID. `"auto"` or absent → UUID v7.
    #[serde(default = "default_auto")]
    pub id: String,
    /// Base data directory (default: `/tmp/mitiflow-storage`).
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    /// Capacity weight (default: 100).
    #[serde(default = "default_capacity")]
    pub capacity: u32,
    /// Health-check publish interval.
    #[serde(default = "default_health_interval", with = "humantime_serde")]
    pub health_interval: Duration,
    /// Grace period before stopping a draining store.
    #[serde(default = "default_drain_grace", with = "humantime_serde")]
    pub drain_grace_period: Duration,
    /// Placement labels.
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

impl Default for NodeYamlConfig {
    fn default() -> Self {
        Self {
            id: "auto".into(),
            data_dir: PathBuf::from("/tmp/mitiflow-storage"),
            capacity: 100,
            health_interval: Duration::from_secs(10),
            drain_grace_period: Duration::from_secs(30),
            labels: HashMap::new(),
        }
    }
}

/// Cluster-level settings.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ClusterYamlConfig {
    /// Well-known prefix (default: `"mitiflow"`).
    #[serde(default = "default_global_prefix")]
    pub global_prefix: String,
    /// Auto-discover topics from orchestrator.
    #[serde(default)]
    pub auto_discover_topics: bool,
}

impl Default for ClusterYamlConfig {
    fn default() -> Self {
        Self {
            global_prefix: "mitiflow".into(),
            auto_discover_topics: false,
        }
    }
}

/// A topic entry in the YAML config.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct TopicYamlEntry {
    pub name: String,
    pub key_prefix: String,
    #[serde(default = "default_num_partitions")]
    pub num_partitions: u32,
    #[serde(default = "default_replication_factor")]
    pub replication_factor: u32,
}

fn default_auto() -> String {
    "auto".into()
}
fn default_data_dir() -> PathBuf {
    PathBuf::from("/tmp/mitiflow-storage")
}
fn default_capacity() -> u32 {
    100
}
fn default_health_interval() -> Duration {
    Duration::from_secs(10)
}
fn default_drain_grace() -> Duration {
    Duration::from_secs(30)
}
fn default_global_prefix() -> String {
    "mitiflow".into()
}
fn default_num_partitions() -> u32 {
    16
}
fn default_replication_factor() -> u32 {
    1
}

impl AgentYamlConfig {
    /// Parse from a YAML string.
    pub fn from_yaml(yaml: &str) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
    }

    /// Parse from a YAML file path.
    pub fn from_file(path: &std::path::Path) -> Result<Self, AgentError> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| AgentError::Config(format!("failed to read config file: {e}")))?;
        Self::from_yaml(&content)
            .map_err(|e| AgentError::Config(format!("invalid YAML config: {e}")))
    }

    /// Convert to [`AgentConfig`], applying env-var overrides.
    pub fn into_agent_config(self) -> Result<AgentConfig, AgentError> {
        let data_dir = std::env::var("MITIFLOW_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or(self.node.data_dir);

        let node_id = if self.node.id == "auto" {
            resolve_or_persist_node_id(&data_dir)
        } else {
            self.node.id
        };

        let global_prefix =
            std::env::var("MITIFLOW_GLOBAL_PREFIX").unwrap_or(self.cluster.global_prefix);

        let topics: Vec<TopicEntry> = self
            .topics
            .into_iter()
            .map(|t| TopicEntry {
                name: t.name,
                key_prefix: t.key_prefix,
                num_partitions: t.num_partitions,
                replication_factor: t.replication_factor,
            })
            .collect();

        if topics.is_empty() && !self.cluster.auto_discover_topics {
            return Err(AgentError::Config(
                "at least one topic or auto_discover_topics must be set".into(),
            ));
        }

        Ok(AgentConfig {
            node_id,
            data_dir,
            capacity: self.node.capacity,
            labels: self.node.labels,
            health_interval: self.node.health_interval,
            drain_grace_period: self.node.drain_grace_period,
            global_prefix,
            auto_discover_topics: self.cluster.auto_discover_topics,
            topics,
        })
    }
}
