use std::collections::HashMap;

use chrono::{DateTime, Utc};
use mitiflow::PublisherId;
use serde::{Deserialize, Serialize};

/// Lifecycle state of a managed EventStore instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoreState {
    /// Opening backend, preparing recovery.
    Starting,
    /// Querying peers for missing events.
    Recovering,
    /// Fully caught up, serving reads and writes.
    Active,
    /// Shutting down gracefully, still accepting in-flight events.
    Draining,
    /// Shut down, backend closed.
    Stopped,
}

/// Metadata published once on startup by each storage agent node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetadata {
    pub node_id: String,
    pub capacity: u32,
    pub labels: HashMap<String, String>,
    pub started_at: DateTime<Utc>,
}

/// Health metrics published periodically by each storage agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeHealth {
    pub node_id: String,
    pub partitions_owned: u32,
    pub events_stored: u64,
    pub disk_usage_bytes: u64,
    pub store_latency_p99_us: u64,
    pub error_count: u64,
    pub timestamp: DateTime<Utc>,
}

/// Per-node partition assignment status published on reconciliation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStatus {
    pub node_id: String,
    pub partitions: Vec<PartitionStatus>,
    pub timestamp: DateTime<Utc>,
}

/// Status of a single (partition, replica) managed by this node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionStatus {
    pub partition: u32,
    pub replica: u32,
    pub state: StoreState,
    pub event_count: u64,
    pub watermark_seq: HashMap<PublisherId, u64>,
}

/// Override table published by the orchestrator, consumed by agents.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct OverrideTable {
    pub entries: Vec<OverrideEntry>,
    pub epoch: u64,
    pub expires_at: Option<DateTime<Utc>>,
}

/// A single override entry specifying manual partition placement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OverrideEntry {
    pub partition: u32,
    pub replica: u32,
    pub node_id: String,
    pub reason: String,
}

