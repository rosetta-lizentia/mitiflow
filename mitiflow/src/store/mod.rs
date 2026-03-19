//! Event store — durable persistence sidecar with watermark-based durability confirmation.
//!
//! This module is gated behind the `store` feature flag.

pub mod backend;
pub mod query;
pub mod watermark;

pub use backend::{CompactionStats, EventMetadata, StorageBackend, StoredEvent};
pub use query::QueryFilters;
pub use watermark::{CommitWatermark, PublisherWatermark};

#[cfg(feature = "store")]
pub use backend::FjallBackend;

#[cfg(feature = "store")]
mod runner;

#[cfg(feature = "store")]
pub use runner::EventStore;
