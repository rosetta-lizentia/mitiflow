//! mitiflow-agent — Distributed storage management for mitiflow.
//!
//! Per-node daemon that manages `EventStore` instances via decentralized
//! partition assignment using weighted rendezvous hashing and Zenoh liveliness.

pub mod agent;
pub mod config;
pub mod error;
pub mod health;
pub mod membership;
pub mod reconciler;
pub mod recovery;
pub mod status;
pub mod types;

pub use agent::StorageAgent;
pub use config::{StorageAgentConfig, StorageAgentConfigBuilder};
pub use error::{AgentError, AgentResult};
pub use types::{
    NodeHealth, NodeMetadata, NodeStatus, OverrideEntry, OverrideTable, PartitionStatus, StoreState,
};
