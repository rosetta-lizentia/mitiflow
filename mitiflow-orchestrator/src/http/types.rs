//! Request, response, and shared types for the HTTP API.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::config::{CompactionPolicy, RetentionPolicy};
use crate::lag::LagReport;
use mitiflow::codec::CodecFormat;
use mitiflow::schema::KeyFormat;

// =========================================================================
// SSE event types (broadcast payloads)
// =========================================================================

/// Events emitted by ClusterView for SSE streaming.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClusterEvent {
    NodeOnline {
        node_id: String,
        timestamp: String,
    },
    NodeOffline {
        node_id: String,
        timestamp: String,
    },
    NodeHealth {
        node_id: String,
        #[serde(flatten)]
        data: serde_json::Value,
    },
    NodeStatus {
        node_id: String,
        #[serde(flatten)]
        data: serde_json::Value,
    },
}

/// Lightweight event summary for the live tail SSE stream.
#[derive(Debug, Clone, Serialize)]
pub struct EventSummary {
    pub seq: u64,
    pub partition: u32,
    pub publisher_id: String,
    pub timestamp: String,
    pub key: Option<String>,
    pub key_expr: String,
    pub payload_size: usize,
}

// =========================================================================
// Topic request / response types
// =========================================================================

/// Request body for creating a topic.
#[derive(Debug, Deserialize)]
pub struct CreateTopicRequest {
    pub name: String,
    #[serde(default)]
    pub key_prefix: String,
    #[serde(default = "default_partitions")]
    pub num_partitions: u32,
    #[serde(default = "default_rf")]
    pub replication_factor: u32,
    #[serde(default)]
    pub required_labels: HashMap<String, String>,
    #[serde(default)]
    pub excluded_labels: HashMap<String, String>,
    #[serde(default)]
    pub codec: CodecFormat,
    #[serde(default)]
    pub key_format: KeyFormat,
    #[serde(default)]
    pub schema_version: u32,
}

fn default_partitions() -> u32 {
    16
}
pub(super) fn default_rf() -> u32 {
    1
}

/// Request body for updating a topic (partial).
#[derive(Debug, Deserialize)]
pub struct UpdateTopicRequest {
    pub replication_factor: Option<u32>,
    pub retention: Option<RetentionPolicy>,
    pub compaction: Option<CompactionPolicy>,
    pub required_labels: Option<HashMap<String, String>>,
    pub excluded_labels: Option<HashMap<String, String>>,
    pub codec: Option<CodecFormat>,
    pub key_format: Option<KeyFormat>,
    pub schema_version: Option<u32>,
}

// =========================================================================
// Cluster types
// =========================================================================

/// Cluster status summary.
#[derive(Debug, Serialize)]
pub struct ClusterStatus {
    pub total_nodes: usize,
    pub online_nodes: usize,
    pub total_partitions: usize,
}

/// Partition info for topic detail.
#[derive(Debug, Serialize)]
pub struct PartitionInfo {
    pub partition: u32,
    pub replicas: Vec<ReplicaInfo>,
}

/// Per-replica assignment info.
#[derive(Debug, Serialize)]
pub struct ReplicaInfo {
    pub replica: u32,
    pub node_id: String,
    pub state: String,
    pub source: String,
}

/// Per-topic partition assignments for a single node.
#[derive(Debug, Serialize)]
pub struct NodeTopicPartitions {
    pub topic: String,
    pub partitions: Vec<NodePartitionEntry>,
}

/// A partition entry for the node detail view.
#[derive(Debug, Serialize)]
pub struct NodePartitionEntry {
    pub partition: u32,
    pub replica: u32,
    pub state: String,
    pub source: String,
}

/// Publisher info derived from watermarks.
#[derive(Debug, Serialize)]
pub struct PublisherInfo {
    pub publisher_id: String,
    pub partitions: HashMap<u32, u64>,
}

// =========================================================================
// Client discovery types (liveliness)
// =========================================================================

/// A live client (publisher or consumer) discovered via Zenoh liveliness.
#[derive(Debug, Clone, Serialize)]
pub struct LiveClient {
    pub id: String,
    pub role: ClientRole,
    /// For consumers in a group: the group_id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,
}

/// Role of a live client.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientRole {
    Publisher,
    Consumer,
}

/// Live clients for a single topic.
#[derive(Debug, Serialize)]
pub struct TopicClients {
    pub topic: String,
    pub publishers: Vec<LiveClient>,
    pub consumers: Vec<LiveClient>,
}

// =========================================================================
// Consumer group types
// =========================================================================

/// Topic lag per consumer group.
#[derive(Debug, Serialize)]
pub struct TopicLagSummary {
    pub group_id: String,
    pub partitions: Vec<LagReport>,
    pub total_lag: u64,
}

/// Consumer group summary.
#[derive(Debug, Serialize)]
pub struct ConsumerGroupSummary {
    pub group_id: String,
    pub total_lag: u64,
}

// =========================================================================
// Drain / override types
// =========================================================================

/// Drain request.
#[derive(Debug, Deserialize)]
pub struct DrainRequest {
    #[serde(default = "default_rf")]
    pub replication_factor: u32,
}

/// Drain response.
#[derive(Debug, Serialize)]
pub struct DrainResponse {
    pub node_id: String,
    pub overrides: Vec<mitiflow_storage::OverrideEntry>,
}

/// Add overrides request.
#[derive(Debug, Deserialize)]
pub struct AddOverridesRequest {
    pub entries: Vec<OverrideEntryRequest>,
    pub ttl_seconds: Option<u64>,
}

/// Override entry in a request.
#[derive(Debug, Deserialize)]
pub struct OverrideEntryRequest {
    pub partition: u32,
    pub replica: u32,
    pub node_id: String,
    #[serde(default)]
    pub reason: String,
}

// =========================================================================
// Query param types
// =========================================================================

/// Query params for SSE lag stream.
#[derive(Debug, Deserialize)]
pub struct LagStreamParams {
    pub group: Option<String>,
}

/// Query params for SSE event stream.
#[derive(Debug, Deserialize)]
pub struct EventStreamParams {
    pub topic: Option<String>,
    pub partition: Option<u32>,
}

/// Query params for the event query-through endpoint.
#[derive(Debug, Deserialize)]
pub struct EventQueryParams {
    pub topic: Option<String>,
    pub partition: Option<u32>,
    pub after_seq: Option<u64>,
    pub before_seq: Option<u64>,
    pub after_time: Option<String>,
    pub before_time: Option<String>,
    pub publisher_id: Option<String>,
    pub key: Option<String>,
    pub limit: Option<usize>,
}

/// Request body for consumer group offset reset.
#[derive(Debug, Deserialize)]
pub struct ResetOffsetsRequest {
    pub topic: String,
    pub partition: u32,
    pub strategy: ResetStrategy,
}

/// Offset reset strategy.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResetStrategy {
    Earliest,
    Latest,
    ToSeq(u64),
}
