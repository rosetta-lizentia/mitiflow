//! mitiflow-emulator — YAML-driven topology runner and chaos testbed.
//!
//! Spawns mitiflow components (producers, consumers, processors, storage agents,
//! orchestrator) as separate OS processes or Docker containers, manages lifecycle,
//! aggregates logs, and schedules chaos events for fault injection testing.

pub mod config;
pub mod error;
pub mod validation;
pub mod generator;
pub mod backend;
pub mod process_backend;
pub mod container_backend;
pub mod log_aggregator;
pub mod role_config;
pub mod supervisor;
pub mod chaos;

pub use config::TopologyConfig;
pub use error::{EmulatorError, Result};
pub use validation::{validate, ValidationResult};
