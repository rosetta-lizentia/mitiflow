//! YAML configuration schema for topology definitions.
//!
//! Parses the complete topology YAML file into strongly-typed structs.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::serde_helpers;

// ---------------------------------------------------------------------------
// Root
// ---------------------------------------------------------------------------

/// Root topology configuration parsed from YAML.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyConfig {
    /// Zenoh session settings.
    #[serde(default)]
    pub zenoh: ZenohConfig,

    /// Default values inherited by topics and components.
    #[serde(default)]
    pub defaults: DefaultsConfig,

    /// Logging configuration.
    #[serde(default)]
    pub logging: LoggingConfig,

    /// Topic definitions — central source of truth.
    #[serde(default)]
    pub topics: Vec<TopicDef>,

    /// Component definitions (producers, consumers, processors, etc.).
    pub components: Vec<ComponentDef>,

    /// Chaos engineering schedule.
    #[serde(default)]
    pub chaos: ChaosConfig,

    /// Manifest output configuration.
    #[serde(default)]
    pub manifest: ManifestConfig,
}

impl TopologyConfig {
    /// Parse a topology from a YAML string.
    pub fn from_yaml(yaml: &str) -> std::result::Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
    }

    /// Parse a topology from a YAML file.
    pub fn from_file(path: &std::path::Path) -> crate::error::Result<Self> {
        let content = std::fs::read_to_string(path).map_err(|_| {
            crate::error::EmulatorError::FileNotFound {
                path: path.to_path_buf(),
            }
        })?;
        Self::from_yaml(&content).map_err(Into::into)
    }
}

// ---------------------------------------------------------------------------
// Zenoh
// ---------------------------------------------------------------------------

/// Zenoh session configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZenohConfig {
    /// Zenoh mode: peer, client, or router.
    #[serde(default = "default_zenoh_mode")]
    pub mode: ZenohMode,

    /// Endpoints to listen on.
    #[serde(default)]
    pub listen: Vec<String>,

    /// Endpoints to connect to.
    #[serde(default)]
    pub connect: Vec<String>,

    /// Auto-start a local Zenoh router for large topologies.
    #[serde(default)]
    pub auto_router: bool,
}

impl Default for ZenohConfig {
    fn default() -> Self {
        Self {
            mode: ZenohMode::Peer,
            listen: Vec::new(),
            connect: Vec::new(),
            auto_router: false,
        }
    }
}

/// Zenoh session mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ZenohMode {
    Peer,
    Client,
    Router,
}

fn default_zenoh_mode() -> ZenohMode {
    ZenohMode::Peer
}

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

/// Default values inherited by topics and components.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefaultsConfig {
    /// Default codec for all topics.
    #[serde(default = "default_codec")]
    pub codec: CodecConfig,

    /// Default execution backend.
    #[serde(default)]
    pub isolation: IsolationMode,

    /// Default publisher cache size.
    #[serde(default = "default_cache_size")]
    pub cache_size: usize,

    /// Default heartbeat interval in milliseconds.
    #[serde(default = "default_heartbeat_ms")]
    pub heartbeat_ms: u64,

    /// Default recovery mode.
    #[serde(default = "default_recovery_mode")]
    pub recovery_mode: RecoveryModeConfig,
}

impl Default for DefaultsConfig {
    fn default() -> Self {
        Self {
            codec: CodecConfig::Json,
            isolation: IsolationMode::Process,
            cache_size: 256,
            heartbeat_ms: 1000,
            recovery_mode: RecoveryModeConfig::Both,
        }
    }
}

fn default_codec() -> CodecConfig {
    CodecConfig::Json
}
fn default_cache_size() -> usize {
    256
}
fn default_heartbeat_ms() -> u64 {
    1000
}
fn default_recovery_mode() -> RecoveryModeConfig {
    RecoveryModeConfig::Both
}

/// Codec format selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CodecConfig {
    Json,
    Msgpack,
    Postcard,
}

/// Recovery mode selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryModeConfig {
    Heartbeat,
    PeriodicQuery,
    Both,
}

/// Execution isolation backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum IsolationMode {
    #[default]
    Process,
    Container,
}

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

/// Logging configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Output mode.
    #[serde(default = "default_log_mode")]
    pub mode: LogMode,

    /// Directory for log files (when mode is file or both).
    #[serde(default = "default_log_directory")]
    pub directory: PathBuf,

    /// Log format.
    #[serde(default)]
    pub format: LogFormat,

    /// Minimum log level.
    #[serde(default = "default_log_level")]
    pub level: LogLevel,

    /// Create separate log file per component.
    #[serde(default)]
    pub per_component: bool,
}

/// Manifest output configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestConfig {
    #[serde(default)]
    pub enabled: bool,

    #[serde(default = "default_manifest_dir")]
    pub directory: PathBuf,
}

impl Default for ManifestConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            directory: default_manifest_dir(),
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            mode: LogMode::Stdout,
            directory: PathBuf::from("./logs"),
            format: LogFormat::Text,
            level: LogLevel::Info,
            per_component: false,
        }
    }
}

fn default_log_mode() -> LogMode {
    LogMode::Stdout
}
fn default_log_directory() -> PathBuf {
    PathBuf::from("./logs")
}
fn default_log_level() -> LogLevel {
    LogLevel::Info
}

fn default_manifest_dir() -> PathBuf {
    PathBuf::from("./manifests")
}

/// Where to write logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogMode {
    Stdout,
    File,
    Both,
}

/// Log line format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum LogFormat {
    #[default]
    Text,
    Json,
}

/// Log level filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum LogLevel {
    Trace,
    Debug,
    #[default]
    Info,
    Warn,
    Error,
}

// ---------------------------------------------------------------------------
// Topics
// ---------------------------------------------------------------------------

/// A topic definition — central place for key prefix, codec, partitions, etc.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicDef {
    /// Topic name (referenced by components).
    pub name: String,

    /// Zenoh key expression prefix.
    pub key_prefix: String,

    /// Codec override for this topic.
    pub codec: Option<CodecConfig>,

    /// Number of partitions.
    #[serde(default = "default_num_partitions")]
    pub num_partitions: u32,

    /// Replication factor.
    #[serde(default = "default_replication_factor")]
    pub replication_factor: u32,

    /// Retention policy.
    pub retention: Option<RetentionConfig>,

    /// Compaction policy.
    pub compaction: Option<CompactionConfig>,
}

fn default_num_partitions() -> u32 {
    16
}
fn default_replication_factor() -> u32 {
    1
}

/// Retention policy for stored events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionConfig {
    /// Maximum age of events (e.g. "24h", "7d").
    #[serde(default, with = "humantime_serde")]
    pub max_age: Option<std::time::Duration>,

    /// Maximum total bytes.
    pub max_bytes: Option<u64>,
}

/// Compaction policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionConfig {
    /// Whether compaction is enabled.
    #[serde(default)]
    pub enabled: bool,
}

// ---------------------------------------------------------------------------
// Components
// ---------------------------------------------------------------------------

/// A component definition (producer, consumer, processor, storage_agent, orchestrator).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentDef {
    /// Component name (unique within topology).
    pub name: String,

    /// Component kind.
    pub kind: ComponentKind,

    /// Number of instances to spawn.
    #[serde(default = "default_instances")]
    pub instances: u32,

    // -- Topic references --
    /// Topic name (for producer, consumer, storage_agent).
    pub topic: Option<String>,

    /// Input topic name (for processor).
    pub input_topic: Option<String>,

    /// Output topic name (for processor).
    pub output_topic: Option<String>,

    // -- Producer settings --
    /// Events per second (per instance unless `rate_per_instance` is used).
    pub rate: Option<f64>,

    /// Whether to use durable publish.
    #[serde(default)]
    pub durable: bool,

    /// Durable publish urgency in milliseconds.
    /// Controls how quickly the store must broadcast a watermark.
    /// `0` = immediate, `None` = use default (100ms).
    pub urgency_ms: Option<u64>,

    /// Publisher cache size override.
    pub cache_size: Option<usize>,

    /// Heartbeat interval override (ms).
    pub heartbeat_ms: Option<u64>,

    /// Payload generation config.
    pub payload: Option<PayloadConfig>,

    /// Rate per instance (alternative to `rate`).
    pub rate_per_instance: Option<f64>,

    /// Ramp-up time in seconds.
    pub ramp_up_sec: Option<f64>,

    /// Starting rate for ramp-up.
    pub ramp_start_rate: Option<f64>,

    /// Burst factor for rate limiting.
    pub burst_factor: Option<f64>,

    // -- Processor settings --
    /// Processing pipeline config.
    pub processing: Option<ProcessingConfig>,

    // -- Consumer settings --
    /// Consumer group config.
    pub consumer_group: Option<ConsumerGroupDef>,

    /// Output mode for consumers.
    pub output: Option<OutputConfig>,

    /// Artificial per-event processing delay in milliseconds.
    ///
    /// Simulates a slow consumer: the consumer loop sleeps this many milliseconds
    /// after processing each event. A value of 10ms limits throughput to ~100 eps.
    /// Used to test backpressure and buffering behavior without changing actual business logic.
    pub processing_delay_ms: Option<u64>,

    /// Enable slow-consumer offload (automatic pub/sub → store query demotion).
    ///
    /// When enabled, a consumer that falls behind the live stream will
    /// transparently switch to batched store queries and resume live
    /// pub/sub once caught up — without back-pressuring the publisher.
    /// Requires a `storage_agent` to be running for the topic.
    #[serde(default)]
    pub offload_enabled: bool,

    /// Number of processing shards for the subscriber pipeline.
    /// Defaults to 1 (single-shard fast path).
    #[serde(default)]
    pub num_processing_shards: Option<usize>,

    // -- Storage agent settings --
    /// Data directory for storage agents, agents, and orchestrator.
    pub data_dir: Option<PathBuf>,

    /// Node capacity weight for HRW assignment.
    pub capacity: Option<u32>,

    // -- Multi-topic agent settings --
    /// List of topic names this agent serves (for `kind: agent`).
    /// In YAML, use `topics: [...]` on the agent component.
    #[serde(
        default,
        alias = "topics",
        deserialize_with = "serde_helpers::deserialize_managed_topics",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub managed_topics: Vec<String>,

    /// Placement labels for the agent node (rack-aware assignment).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub labels: Option<HashMap<String, String>>,

    /// Well-known global prefix for topic discovery and health reporting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub global_prefix: Option<String>,

    /// Enable dynamic topic discovery from orchestrator via TopicWatcher.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_discover_topics: Option<bool>,

    // -- Orchestrator settings --
    /// Lag monitoring interval in milliseconds.
    pub lag_interval_ms: Option<u64>,

    /// HTTP port for the orchestrator GUI/API server.
    pub http_port: Option<u16>,

    /// Override RUST_LOG for this component (e.g. "debug", "mitiflow_storage=trace").
    pub log_level: Option<String>,

    // -- General --
    /// Components that must be ready before this one starts.
    #[serde(default)]
    pub depends_on: Vec<String>,

    /// Execution isolation override (process or container).
    pub isolation: Option<IsolationMode>,

    /// Codec override at component level.
    pub codec: Option<CodecConfig>,
}

fn default_instances() -> u32 {
    1
}

/// Component kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComponentKind {
    Producer,
    Consumer,
    Processor,
    StorageAgent,
    /// Multi-topic storage agent backed by `mitiflow-storage` `AgentConfig`.
    Agent,
    Orchestrator,
}

impl ComponentKind {
    /// Binary name for this component kind.
    pub fn binary_name(&self) -> &'static str {
        match self {
            Self::Producer => "mitiflow-emulator-producer",
            Self::Consumer => "mitiflow-emulator-consumer",
            Self::Processor => "mitiflow-emulator-processor",
            Self::StorageAgent => "mitiflow-emulator-storage-agent",
            Self::Agent => "mitiflow-emulator-agent",
            Self::Orchestrator => "mitiflow-emulator-orchestrator",
        }
    }

    /// Startup tier (lower = starts first).
    pub fn tier(&self) -> u8 {
        match self {
            Self::Orchestrator => 1,
            Self::StorageAgent | Self::Agent => 2,
            Self::Producer => 3,
            Self::Processor => 4,
            Self::Consumer => 5,
        }
    }
}

// ---------------------------------------------------------------------------
// Payload
// ---------------------------------------------------------------------------

/// Payload generation configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PayloadConfig {
    /// Generator type.
    pub generator: GeneratorType,

    /// Target payload size in bytes.
    #[serde(default = "default_payload_size")]
    pub size_bytes: usize,

    /// Schema definition (used when generator = schema).
    #[serde(default)]
    pub schema: HashMap<String, SchemaFieldDef>,

    /// Custom content for fixed generator (base64 encoded).
    pub content: Option<String>,

    /// Prefix for counter generator.
    pub prefix: Option<String>,
}

fn default_payload_size() -> usize {
    256
}

/// Payload generator type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GeneratorType {
    RandomJson,
    Fixed,
    Counter,
    Schema,
}

/// Schema field definition for the schema generator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaFieldDef {
    /// Field type.
    #[serde(rename = "type")]
    pub field_type: SchemaFieldType,

    /// Minimum value (for float/int).
    pub min: Option<f64>,

    /// Maximum value (for float/int).
    pub max: Option<f64>,

    /// String pattern with `{N-M}` range expansions.
    pub pattern: Option<String>,

    /// Probability for bool fields (0.0–1.0).
    pub probability: Option<f64>,

    /// Values list for enum fields.
    pub values: Option<Vec<String>>,
}

/// Schema field types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SchemaFieldType {
    Float,
    Int,
    String,
    Uuid,
    Datetime,
    Bool,
    Enum,
}

// ---------------------------------------------------------------------------
// Processing (Processor)
// ---------------------------------------------------------------------------

/// Processing mode for processor components.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingConfig {
    /// Processing mode.
    #[serde(default = "default_processing_mode")]
    pub mode: ProcessingMode,

    /// Simulated processing delay in milliseconds.
    pub delay_ms: Option<u64>,

    /// Drop probability for filter mode (0.0–1.0).
    pub drop_probability: Option<f64>,
}

fn default_processing_mode() -> ProcessingMode {
    ProcessingMode::Passthrough
}

/// Processing mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProcessingMode {
    Passthrough,
    Delay,
    Filter,
    Map,
}

// ---------------------------------------------------------------------------
// Consumer Group
// ---------------------------------------------------------------------------

/// Consumer group configuration within a component.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupDef {
    /// Group identifier.
    pub group_id: String,

    /// Offset commit mode.
    #[serde(default = "default_commit_mode")]
    pub commit_mode: CommitModeDef,

    /// Auto-commit interval in milliseconds.
    #[serde(default = "default_auto_commit_interval")]
    pub auto_commit_interval_ms: u64,
}

fn default_commit_mode() -> CommitModeDef {
    CommitModeDef::Auto
}
fn default_auto_commit_interval() -> u64 {
    5000
}

/// Offset commit mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CommitModeDef {
    Manual,
    Auto,
}

// ---------------------------------------------------------------------------
// Output (Consumer)
// ---------------------------------------------------------------------------

/// Consumer output configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputConfig {
    /// Output mode.
    #[serde(default = "default_output_mode")]
    pub mode: OutputMode,

    /// Reporting interval in seconds (for count mode).
    #[serde(default = "default_report_interval")]
    pub report_interval_sec: u64,

    /// File path (for file mode).
    pub file_path: Option<PathBuf>,
}

fn default_output_mode() -> OutputMode {
    OutputMode::Count
}
fn default_report_interval() -> u64 {
    5
}

/// Consumer output mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OutputMode {
    Count,
    Log,
    Discard,
    File,
}

// ---------------------------------------------------------------------------
// Chaos Engineering
// ---------------------------------------------------------------------------

/// Chaos engineering configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ChaosConfig {
    /// Whether chaos is enabled.
    #[serde(default)]
    pub enabled: bool,

    /// Optional RNG seed for reproducible random chaos.
    #[serde(default)]
    pub seed: Option<u64>,

    /// Scheduled chaos events.
    #[serde(default)]
    pub schedule: Vec<ChaosEventDef>,

    /// Random/stochastic chaos configuration.
    #[serde(default)]
    pub random: Option<RandomChaosConfig>,
}

/// A single chaos event definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChaosEventDef {
    /// Fire once at this offset from start (e.g. "10s").
    #[serde(default, with = "humantime_serde")]
    pub at: Option<std::time::Duration>,

    /// Fire repeatedly at this interval (e.g. "20s").
    #[serde(default, with = "humantime_serde")]
    pub every: Option<std::time::Duration>,

    /// The chaos action to perform.
    pub action: ChaosAction,

    /// Target component name.
    pub target: Option<String>,

    /// Target instance index (None = all instances).
    pub instance: Option<usize>,

    /// Restart delay after kill.
    #[serde(default, with = "humantime_serde")]
    pub restart_after: Option<std::time::Duration>,

    /// Duration for pause/slow actions.
    #[serde(default, with = "humantime_serde")]
    pub duration: Option<std::time::Duration>,

    /// Network delay for slow action (ms).
    pub delay_ms: Option<u64>,

    /// Target components to partition from (isolate target from these).
    #[serde(default)]
    pub partition_from: Vec<String>,

    /// Duration before healing the partition.
    #[serde(default, with = "humantime_serde")]
    pub heal_after: Option<std::time::Duration>,

    /// Packet loss percentage for slow action (0-100).
    pub loss_percent: Option<f64>,

    /// Bandwidth limit for slow action (e.g. "1mbit").
    pub bandwidth: Option<String>,

    /// Network jitter for slow action (ms).
    pub jitter_ms: Option<u64>,

    /// Pool of targets for kill_random.
    #[serde(default)]
    pub pool: Vec<String>,
}

/// Chaos action types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChaosAction {
    Kill,
    Pause,
    Restart,
    Slow,
    NetworkPartition,
    KillRandom,
}

// ---------------------------------------------------------------------------
// Random chaos
// ---------------------------------------------------------------------------

/// Stochastic (Poisson-process) chaos mode configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RandomChaosConfig {
    pub fault_rate: f64,
    #[serde(with = "humantime_serde")]
    pub duration: Duration,
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent_faults: usize,
    #[serde(default = "default_min_interval", with = "humantime_serde")]
    pub min_interval: Duration,
    pub actions: HashMap<String, RandomActionConfig>,
}

/// Per-action configuration for random chaos.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RandomActionConfig {
    pub probability: f64,
    #[serde(default)]
    pub pool: Vec<String>,
    #[serde(default, with = "humantime_range_serde")]
    pub restart_after: Option<[Duration; 2]>,
    #[serde(default)]
    pub delay_ms: Option<[u64; 2]>,
    #[serde(default, with = "humantime_range_serde")]
    pub heal_after: Option<[Duration; 2]>,
    #[serde(default, with = "humantime_range_serde")]
    pub pause_duration: Option<[Duration; 2]>,
}

fn default_max_concurrent() -> usize {
    3
}

fn default_min_interval() -> Duration {
    Duration::from_secs(1)
}

/// Serde module for `Option<[Duration; 2]>` serialized as `["2s", "10s"]`.
pub mod humantime_range_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(value: &Option<[Duration; 2]>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            None => serializer.serialize_none(),
            Some([min, max]) => {
                let pair = [
                    humantime::format_duration(*min).to_string(),
                    humantime::format_duration(*max).to_string(),
                ];
                pair.serialize(serializer)
            }
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<[Duration; 2]>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt: Option<Vec<String>> = Option::deserialize(deserializer)?;
        match opt {
            None => Ok(None),
            Some(v) => {
                if v.len() != 2 {
                    return Err(serde::de::Error::custom(format!(
                        "expected exactly 2 duration strings, got {}",
                        v.len()
                    )));
                }
                let min = humantime::parse_duration(&v[0]).map_err(|e| {
                    serde::de::Error::custom(format!("invalid duration \"{}\": {}", v[0], e))
                })?;
                let max = humantime::parse_duration(&v[1]).map_err(|e| {
                    serde::de::Error::custom(format!("invalid duration \"{}\": {}", v[1], e))
                })?;
                if min > max {
                    return Err(serde::de::Error::custom(format!(
                        "duration range minimum ({}) must be <= maximum ({})",
                        v[0], v[1]
                    )));
                }
                Ok(Some([min, max]))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::validation::validate;
    use std::time::Duration;

    #[test]
    fn default_zenoh_config() {
        let config = ZenohConfig::default();
        assert_eq!(config.mode, ZenohMode::Peer);
        assert!(config.listen.is_empty());
        assert!(config.connect.is_empty());
        assert!(!config.auto_router);
    }

    #[test]
    fn default_defaults_config() {
        let config = DefaultsConfig::default();
        assert_eq!(config.codec, CodecConfig::Json);
        assert_eq!(config.isolation, IsolationMode::Process);
        assert_eq!(config.cache_size, 256);
        assert_eq!(config.heartbeat_ms, 1000);
        assert_eq!(config.recovery_mode, RecoveryModeConfig::Both);
    }

    #[test]
    fn component_kind_tier_ordering() {
        assert!(ComponentKind::Orchestrator.tier() < ComponentKind::StorageAgent.tier());
        assert!(ComponentKind::Orchestrator.tier() < ComponentKind::Agent.tier());
        assert_eq!(
            ComponentKind::StorageAgent.tier(),
            ComponentKind::Agent.tier()
        );
        assert!(ComponentKind::StorageAgent.tier() < ComponentKind::Producer.tier());
        assert!(ComponentKind::Producer.tier() < ComponentKind::Processor.tier());
        assert!(ComponentKind::Processor.tier() < ComponentKind::Consumer.tier());
    }

    #[test]
    fn component_kind_binary_names() {
        assert_eq!(
            ComponentKind::Producer.binary_name(),
            "mitiflow-emulator-producer"
        );
        assert_eq!(
            ComponentKind::StorageAgent.binary_name(),
            "mitiflow-emulator-storage-agent"
        );
        assert_eq!(
            ComponentKind::Agent.binary_name(),
            "mitiflow-emulator-agent"
        );
    }

    #[test]
    fn parse_network_partition_chaos_action() {
        let yaml = r#"
components:
  - name: c1
    kind: producer
    topic: t1
chaos:
  enabled: true
  schedule:
    - at: 10s
      action: network_partition
      target: c1
      partition_from: [c2, c3]
      heal_after: 5s
      loss_percent: 25.5
      bandwidth: 1mbit
"#;

        let config = TopologyConfig::from_yaml(yaml).expect("yaml should parse");
        let event = &config.chaos.schedule[0];
        assert_eq!(event.action, ChaosAction::NetworkPartition);
        assert_eq!(event.partition_from, vec!["c2", "c3"]);
        assert_eq!(event.heal_after, Some(Duration::from_secs(5)));
        assert_eq!(event.loss_percent, Some(25.5));
        assert_eq!(event.bandwidth.as_deref(), Some("1mbit"));
    }

    #[test]
    fn parse_random_chaos_config() {
        let yaml = r#"
components:
  - name: c1
    kind: producer
    topic: t1
chaos:
  enabled: true
  seed: 42
  random:
    fault_rate: 0.75
    duration: 30s
    max_concurrent_faults: 5
    min_interval: 2s
    actions:
      kill:
        probability: 0.4
        pool: [c1]
        restart_after: [2s, 10s]
      pause:
        probability: 0.2
        pool: [c1]
        pause_duration: [500ms, 2s]
      slow:
        probability: 0.25
        pool: [c1]
        delay_ms: [100, 250]
      network_partition:
        probability: 0.15
        pool: [c1]
        heal_after: [5s, 15s]
"#;

        let config = TopologyConfig::from_yaml(yaml).expect("yaml should parse");
        assert_eq!(config.chaos.seed, Some(42));

        let random = config.chaos.random.expect("random config should exist");
        assert_eq!(random.fault_rate, 0.75);
        assert_eq!(random.duration, Duration::from_secs(30));
        assert_eq!(random.max_concurrent_faults, 5);
        assert_eq!(random.min_interval, Duration::from_secs(2));
        assert_eq!(random.actions.len(), 4);

        let kill = random.actions.get("kill").expect("kill action");
        assert_eq!(kill.probability, 0.4);
        assert_eq!(kill.pool, vec!["c1"]);
        assert_eq!(
            kill.restart_after,
            Some([Duration::from_secs(2), Duration::from_secs(10)])
        );

        let pause = random.actions.get("pause").expect("pause action");
        assert_eq!(
            pause.pause_duration,
            Some([Duration::from_millis(500), Duration::from_secs(2)])
        );

        let slow = random.actions.get("slow").expect("slow action");
        assert_eq!(slow.delay_ms, Some([100, 250]));

        let partition = random
            .actions
            .get("network_partition")
            .expect("partition action");
        assert_eq!(
            partition.heal_after,
            Some([Duration::from_secs(5), Duration::from_secs(15)])
        );
    }

    #[test]
    fn parse_random_chaos_config_defaults() {
        let yaml = r#"
components:
  - name: c1
    kind: producer
    topic: t1
chaos:
  enabled: true
"#;

        let config = TopologyConfig::from_yaml(yaml).expect("yaml should parse");
        assert_eq!(config.chaos.seed, None);
        assert_eq!(config.chaos.random, None);
    }

    #[test]
    fn parse_random_chaos_config_partial() {
        let yaml = r#"
components:
  - name: c1
    kind: producer
    topic: t1
topics:
  - name: t1
    key_prefix: demo/t1
chaos:
  enabled: true
  random:
    fault_rate: 1.5
    duration: 45s
    actions:
      kill:
        probability: 1.0
        pool: [c1]
"#;

        let config = TopologyConfig::from_yaml(yaml).expect("yaml should parse");
        let random = config.chaos.random.expect("random config should exist");
        assert_eq!(random.fault_rate, 1.5);
        assert_eq!(random.duration, Duration::from_secs(45));
        assert_eq!(random.max_concurrent_faults, 3);
        assert_eq!(random.min_interval, Duration::from_secs(1));
    }

    #[test]
    fn parse_random_chaos_actions() {
        let yaml = r#"
components:
  - name: c1
    kind: producer
    topic: t1
topics:
  - name: t1
    key_prefix: demo/t1
chaos:
  enabled: true
  random:
    fault_rate: 1.0
    duration: 20s
    actions:
      kill:
        probability: 0.25
        pool: [c1]
        restart_after: [1s, 3s]
      pause:
        probability: 0.25
        pool: [c1]
        pause_duration: [250ms, 2s]
      slow:
        probability: 0.25
        pool: [c1]
        delay_ms: [50, 100]
      network_partition:
        probability: 0.25
        pool: [c1]
        heal_after: [2s, 6s]
"#;

        let config = TopologyConfig::from_yaml(yaml).expect("yaml should parse");
        let random = config.chaos.random.expect("random config should exist");
        let kill = random.actions.get("kill").expect("kill action");
        assert_eq!(
            kill.restart_after,
            Some([Duration::from_secs(1), Duration::from_secs(3)])
        );
        assert_eq!(kill.pool, vec!["c1"]);
        let pause = random.actions.get("pause").expect("pause action");
        assert_eq!(
            pause.pause_duration,
            Some([Duration::from_millis(250), Duration::from_secs(2)])
        );
        let slow = random.actions.get("slow").expect("slow action");
        assert_eq!(slow.delay_ms, Some([50, 100]));
        let partition = random
            .actions
            .get("network_partition")
            .expect("partition action");
        assert_eq!(
            partition.heal_after,
            Some([Duration::from_secs(2), Duration::from_secs(6)])
        );
    }

    #[test]
    fn random_chaos_validation_rejects_invalid_fault_rate() {
        let yaml = r#"
components:
  - name: c1
    kind: producer
    topic: t1
topics:
  - name: t1
    key_prefix: demo/t1
chaos:
  enabled: true
  random:
    fault_rate: 0
    duration: 1s
    actions:
      kill:
        probability: 1.0
        pool: [c1]
"#;

        let config = TopologyConfig::from_yaml(yaml).expect("yaml should parse");
        let err = validate(&config)
            .expect_err("validation should fail")
            .to_string();
        assert!(
            err.contains("fault_rate"),
            "error should mention fault_rate: {err}"
        );
    }

    #[test]
    fn random_chaos_validation_rejects_empty_actions() {
        let yaml = r#"
components:
  - name: c1
    kind: producer
    topic: t1
topics:
  - name: t1
    key_prefix: demo/t1
chaos:
  enabled: true
  random:
    fault_rate: 1.0
    duration: 1s
    actions: {}
"#;

        let config = TopologyConfig::from_yaml(yaml).expect("yaml should parse");
        let err = validate(&config)
            .expect_err("validation should fail")
            .to_string();
        assert!(
            err.contains("actions"),
            "error should mention actions: {err}"
        );
    }

    #[test]
    fn random_chaos_validation_rejects_empty_pool_for_targeted_action() {
        let yaml = r#"
components:
  - name: c1
    kind: producer
    topic: t1
topics:
  - name: t1
    key_prefix: demo/t1
chaos:
  enabled: true
  random:
    fault_rate: 1.0
    duration: 1s
    actions:
      kill:
        probability: 1.0
        pool: []
"#;

        let config = TopologyConfig::from_yaml(yaml).expect("yaml should parse");
        let err = validate(&config)
            .expect_err("validation should fail")
            .to_string();
        assert!(err.contains("pool"), "error should mention pool: {err}");
    }

    #[test]
    fn random_chaos_validation_rejects_bad_range() {
        let yaml = r#"
components:
  - name: c1
    kind: producer
    topic: t1
topics:
  - name: t1
    key_prefix: demo/t1
chaos:
  enabled: true
  random:
    fault_rate: 1.0
    duration: 1s
    actions:
      kill:
        probability: 1.0
        pool: [c1]
        restart_after: [10s, 2s]
"#;

        let err = TopologyConfig::from_yaml(yaml)
            .expect_err("yaml should fail")
            .to_string();
        assert!(
            err.contains("minimum"),
            "error should mention range ordering: {err}"
        );
    }

    #[test]
    fn parse_manifest_config_and_defaults() {
        let yaml = r#"
components:
  - name: c1
    kind: producer
    topic: t1
manifest:
  enabled: true
  directory: /tmp/test
"#;

        let config = TopologyConfig::from_yaml(yaml).expect("yaml should parse");
        assert!(config.manifest.enabled);
        assert_eq!(config.manifest.directory, PathBuf::from("/tmp/test"));

        let yaml_without_manifest = r#"
components:
  - name: c1
    kind: producer
    topic: t1
"#;

        let config = TopologyConfig::from_yaml(yaml_without_manifest).expect("yaml should parse");
        assert!(!config.manifest.enabled);
        assert_eq!(config.manifest.directory, PathBuf::from("./manifests"));
    }

    #[test]
    fn parse_all_existing_topology_yaml_files() {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("topologies");
        for entry in std::fs::read_dir(&dir).expect("topologies dir should exist") {
            let entry = entry.expect("dir entry should be readable");
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("yaml") {
                continue;
            }

            let yaml = std::fs::read_to_string(&path).expect("topology yaml should be readable");
            TopologyConfig::from_yaml(&yaml)
                .unwrap_or_else(|err| panic!("failed to parse {}: {err}", path.display()));
        }
    }

    #[test]
    fn backward_compat_chaos_config_no_random() {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("topologies");
        let yaml =
            std::fs::read_to_string(dir.join("06_chaos.yaml")).expect("06_chaos.yaml should exist");
        let config: TopologyConfig = serde_yaml::from_str(&yaml).expect("parse 06_chaos.yaml");
        assert!(
            config.chaos.random.is_none(),
            "existing topology should have no random config"
        );
        assert!(
            config.chaos.seed.is_none(),
            "existing topology should have no seed"
        );
        assert!(
            !config.chaos.schedule.is_empty(),
            "06_chaos should have schedule events"
        );
    }

    #[test]
    fn parse_15_chaos_random() {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("topologies");
        let yaml = std::fs::read_to_string(dir.join("15_chaos_random.yaml"))
            .expect("15_chaos_random.yaml should exist");
        let config: TopologyConfig =
            serde_yaml::from_str(&yaml).expect("parse 15_chaos_random.yaml");

        assert!(config.chaos.enabled);
        assert_eq!(config.chaos.seed, Some(42));
        assert!(config.chaos.random.is_some());
        assert_eq!(config.chaos.schedule.len(), 1);

        let random = config.chaos.random.as_ref().expect("random config");
        assert!((random.fault_rate - 0.5).abs() < f64::EPSILON);
        assert_eq!(random.max_concurrent_faults, 3);
        assert_eq!(random.min_interval, Duration::from_secs(1));
        assert_eq!(random.actions.len(), 4);
        assert!(random.actions.contains_key("kill"));
        assert!(random.actions.contains_key("slow"));
        assert!(random.actions.contains_key("network_partition"));
        assert!(random.actions.contains_key("pause"));
    }
}
