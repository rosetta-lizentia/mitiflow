#![doc = include_str!("../README.md")]
#![allow(rustdoc::broken_intra_doc_links)]

pub mod alert_manager;
pub mod cluster_view;
pub mod config;
pub mod drain;
pub mod http;
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
