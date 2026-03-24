//! Event store — durable persistence sidecar with watermark-based durability confirmation.
//!
//! This module is gated behind the `store` feature flag.

pub mod backend;
pub mod lifecycle;
pub mod offset;
pub mod query;
pub mod watermark;

pub use backend::{CompactionStats, EventMetadata, HlcTimestamp, StorageBackend, StoredEvent};
pub use offset::OffsetCommit;
pub use query::{QueryFilters, ReplayFilters};
pub use watermark::{CommitWatermark, PublisherWatermark};

#[cfg(feature = "fjall-backend")]
pub use backend::FjallBackend;

#[cfg(feature = "store")]
mod runner;

#[cfg(feature = "fjall-backend")]
mod manager;

#[cfg(feature = "store")]
pub use runner::EventStore;

#[cfg(feature = "fjall-backend")]
pub use manager::StoreManager;
