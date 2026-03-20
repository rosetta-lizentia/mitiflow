//! mitiflow-orchestrator: control-plane service for mitiflow
//!
//! Provides configuration management, lag monitoring, store lifecycle tracking,
//! and an admin API (Zenoh queryable). Does **not** sit in the event data path.

pub mod config;
pub mod lag;
pub mod lifecycle;
pub mod orchestrator;

pub use config::{CompactionPolicy, RetentionPolicy, TopicConfig};
pub use lag::LagMonitor;
pub use lifecycle::StoreTracker;
pub use orchestrator::Orchestrator;
