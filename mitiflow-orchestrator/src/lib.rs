//! mitiflow-orchestrator: control-plane service for mitiflow
//!
//! Provides configuration management, lag monitoring, store lifecycle tracking,
//! cluster view aggregation, override management, drain operations,
//! and an admin API (Zenoh queryable). Does **not** sit in the event data path.

pub mod cluster_view;
pub mod config;
pub mod drain;
pub mod lag;
pub mod lifecycle;
pub mod orchestrator;
pub mod override_manager;
pub mod topic_manager;

pub use cluster_view::{AssignmentInfo, AssignmentSource, ClusterView, NodeInfo};
pub use config::{CompactionPolicy, RetentionPolicy, TopicConfig};
pub use lag::LagMonitor;
pub use lifecycle::StoreTracker;
pub use orchestrator::Orchestrator;
pub use override_manager::OverrideManager;
pub use topic_manager::TopicManager;

// Re-export agent types for consumers of this crate.
pub use mitiflow_agent::{
    NodeHealth, NodeMetadata, NodeStatus, OverrideEntry, OverrideTable, PartitionStatus, StoreState,
};
