//! mitiflow-agent — Distributed storage management for mitiflow.
//!
//! Per-node daemon that manages `EventStore` instances via decentralized
//! partition assignment using weighted rendezvous hashing and Zenoh liveliness.
//!
//! Supports multi-topic operation: a single agent process can serve any number
//! of topics, each with an independent [`TopicWorker`] owning its own
//! `MembershipTracker`, `Reconciler`, `RecoveryManager`, and `StatusReporter`.

pub mod agent;
pub mod config;
pub mod error;
pub mod health;
pub mod membership;
pub mod reconciler;
pub mod recovery;
pub mod status;
pub mod topic_supervisor;
pub mod topic_watcher;
pub mod topic_worker;
pub mod types;

pub use agent::StorageAgent;
pub use config::{
    AgentConfig, AgentConfigBuilder, AgentYamlConfig, StorageAgentConfig,
    StorageAgentConfigBuilder, TopicEntry, TopicWorkerConfig,
};
pub use error::{AgentError, AgentResult};
pub use topic_supervisor::TopicSupervisor;
pub use topic_watcher::{TopicWatcher, should_serve_topic, RemoteTopicConfig};
pub use topic_worker::TopicWorker;
pub use types::{
    NodeHealth, NodeMetadata, NodeStatus, OverrideEntry, OverrideTable, PartitionStatus, StoreState,
};
