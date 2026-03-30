//! Per-role serializable configurations passed via `MITIFLOW_EMU_CONFIG`.
//!
//! Each role binary decodes these from a base64-encoded JSON env var.

use std::collections::HashMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::config::{
    CodecConfig, CommitModeDef, ConsumerGroupDef, GeneratorType, OutputConfig, OutputMode,
    PayloadConfig, ProcessingMode, RecoveryModeConfig, SchemaFieldDef,
};

/// Zenoh session config passed to role binaries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZenohRoleConfig {
    pub mode: String,
    #[serde(default)]
    pub listen: Vec<String>,
    #[serde(default)]
    pub connect: Vec<String>,
}

/// Producer role configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerRoleConfig {
    pub key_prefix: String,
    pub codec: CodecConfig,
    pub cache_size: usize,
    pub heartbeat_ms: u64,
    pub recovery_mode: RecoveryModeConfig,
    pub durable: bool,
    pub urgency_ms: Option<u64>,
    pub rate: Option<f64>,
    pub rate_per_instance: Option<f64>,
    pub ramp_up_sec: Option<f64>,
    pub ramp_start_rate: Option<f64>,
    pub burst_factor: Option<f64>,
    pub payload: PayloadRoleConfig,
    pub num_partitions: u32,
}

/// Payload config for role binaries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PayloadRoleConfig {
    pub generator: GeneratorType,
    pub size_bytes: usize,
    #[serde(default)]
    pub schema: HashMap<String, SchemaFieldDef>,
    pub content: Option<String>,
    pub prefix: Option<String>,
}

impl From<&PayloadConfig> for PayloadRoleConfig {
    fn from(p: &PayloadConfig) -> Self {
        Self {
            generator: p.generator,
            size_bytes: p.size_bytes,
            schema: p.schema.clone(),
            content: p.content.clone(),
            prefix: p.prefix.clone(),
        }
    }
}

impl From<&PayloadRoleConfig> for PayloadConfig {
    fn from(p: &PayloadRoleConfig) -> Self {
        Self {
            generator: p.generator,
            size_bytes: p.size_bytes,
            schema: p.schema.clone(),
            content: p.content.clone(),
            prefix: p.prefix.clone(),
        }
    }
}

/// Consumer role configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerRoleConfig {
    pub key_prefix: String,
    pub codec: CodecConfig,
    pub cache_size: usize,
    pub heartbeat_ms: u64,
    pub recovery_mode: RecoveryModeConfig,
    pub num_partitions: u32,
    pub consumer_group: Option<ConsumerGroupRoleConfig>,
    pub output: OutputRoleConfig,
    /// Artificial per-event processing delay in milliseconds (see `ComponentDef::processing_delay_ms`).
    pub processing_delay_ms: Option<u64>,
    /// Enable slow-consumer offload.
    pub offload_enabled: bool,
    /// Number of processing shards (1 = single-shard fast path).
    pub num_processing_shards: usize,
}

/// Consumer group config for role binaries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupRoleConfig {
    pub group_id: String,
    pub commit_mode: CommitModeDef,
    pub auto_commit_interval_ms: u64,
}

impl From<&ConsumerGroupDef> for ConsumerGroupRoleConfig {
    fn from(cg: &ConsumerGroupDef) -> Self {
        Self {
            group_id: cg.group_id.clone(),
            commit_mode: cg.commit_mode,
            auto_commit_interval_ms: cg.auto_commit_interval_ms,
        }
    }
}

/// Output config for consumer role binaries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputRoleConfig {
    pub mode: OutputMode,
    pub report_interval_sec: u64,
    pub file_path: Option<PathBuf>,
}

impl From<Option<&OutputConfig>> for OutputRoleConfig {
    fn from(o: Option<&OutputConfig>) -> Self {
        match o {
            Some(config) => Self {
                mode: config.mode,
                report_interval_sec: config.report_interval_sec,
                file_path: config.file_path.clone(),
            },
            None => Self {
                mode: OutputMode::Count,
                report_interval_sec: 5,
                file_path: None,
            },
        }
    }
}

/// Processor role configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessorRoleConfig {
    pub input_key_prefix: String,
    pub output_key_prefix: String,
    pub codec: CodecConfig,
    pub cache_size: usize,
    pub heartbeat_ms: u64,
    pub recovery_mode: RecoveryModeConfig,
    pub num_partitions: u32,
    pub processing_mode: ProcessingMode,
    pub delay_ms: Option<u64>,
    pub drop_probability: Option<f64>,
    pub consumer_group: Option<ConsumerGroupRoleConfig>,
}

/// Storage agent role configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageAgentRoleConfig {
    pub key_prefix: String,
    pub data_dir: PathBuf,
    pub num_partitions: u32,
    pub replication_factor: u32,
    pub capacity: u32,
    pub node_id: Option<String>,
    pub codec: CodecConfig,
    pub cache_size: usize,
    pub heartbeat_ms: u64,
    pub recovery_mode: RecoveryModeConfig,
    /// Override RUST_LOG for the storage agent process.
    pub log_level: Option<String>,
}

/// Orchestrator role configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestratorRoleConfig {
    pub key_prefix: String,
    pub data_dir: PathBuf,
    pub lag_interval_ms: u64,
    /// All topic definitions for auto-registration.
    pub topics: Vec<TopicRegistration>,
    /// Optional HTTP port for the GUI/API server.
    pub http_port: Option<u16>,
}

/// Topic info passed to orchestrator for auto-registration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicRegistration {
    pub name: String,
    pub key_prefix: String,
    pub num_partitions: u32,
    pub replication_factor: u32,
}

/// Encode a role config to base64 JSON for the environment variable.
pub fn encode_config<T: Serialize>(config: &T) -> crate::error::Result<String> {
    let json = serde_json::to_vec(config)?;
    use base64::Engine;
    Ok(base64::engine::general_purpose::STANDARD.encode(&json))
}

/// Decode a role config from a base64 JSON environment variable.
pub fn decode_config<T: for<'de> Deserialize<'de>>(b64: &str) -> crate::error::Result<T> {
    use base64::Engine;
    let json = base64::engine::general_purpose::STANDARD.decode(b64)?;
    let config = serde_json::from_slice(&json)?;
    Ok(config)
}
