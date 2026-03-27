#![doc = include_str!("../README.md")]
#![allow(rustdoc::broken_intra_doc_links)]

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
pub use topic_watcher::{RemoteTopicConfig, TopicWatcher, should_serve_topic};
pub use topic_worker::TopicWorker;
pub use types::{
    NodeHealth, NodeMetadata, NodeStatus, OverrideEntry, OverrideTable, PartitionStatus, StoreState,
};
