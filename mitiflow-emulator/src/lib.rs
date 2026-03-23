//! mitiflow-emulator — YAML-driven topology runner and chaos testbed.
//!
//! Spawns mitiflow components (producers, consumers, processors, storage agents,
//! orchestrator) as separate OS processes or Docker containers, manages lifecycle,
//! aggregates logs, and schedules chaos events for fault injection testing.

pub mod backend;
pub mod chaos;
pub mod config;
pub mod container_backend;
pub mod error;
pub mod generator;
pub mod log_aggregator;
pub mod process_backend;
pub mod role_config;
pub mod supervisor;
pub mod validation;

pub use config::TopologyConfig;
pub use error::{EmulatorError, Result};
pub use validation::{ValidationResult, validate};
