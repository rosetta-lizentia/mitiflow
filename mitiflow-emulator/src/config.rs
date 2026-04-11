//! YAML configuration schema for topology definitions.
//!
//! Parses the complete topology YAML file into strongly-typed structs.

use std::collections::HashMap;
use std::path::PathBuf;

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

    /// Scheduled chaos events.
    #[serde(default)]
    pub schedule: Vec<ChaosEventDef>,
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

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(event.heal_after, Some(std::time::Duration::from_secs(5)));
        assert_eq!(event.loss_percent, Some(25.5));
        assert_eq!(event.bandwidth.as_deref(), Some("1mbit"));
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
}
