//! # mitiflow
//!
//! Production-grade event streaming for Zenoh.
//!
//! Layers Kafka-class reliability (sequencing, gap detection, recovery, durability,
//! consumer groups) on top of Zenoh's microsecond-latency pub/sub using only stable APIs.

pub mod attachment;
pub mod codec;
pub mod config;
pub mod dlq;
pub mod error;
pub mod event;
pub mod publisher;
pub mod subscriber;
pub mod types;

#[cfg(feature = "store")]
pub mod store;

#[cfg(feature = "partition")]
pub mod partition;

// Public re-exports
pub use codec::CodecFormat;
pub use config::{
    CommitMode, ConsumerGroupConfig, EventBusConfig, EventBusConfigBuilder, HeartbeatMode,
    OffsetReset, RecoveryMode,
};
pub use dlq::{BackoffStrategy, DeadLetterQueue, DlqConfig, RetryOutcome};
pub use error::{Error, Result};
pub use event::Event;
pub use publisher::EventPublisher;
pub use subscriber::EventSubscriber;
pub use types::{EventId, PublisherId};

#[cfg(feature = "store")]
pub use store::{EventStore, FjallBackend, OffsetCommit, StoreManager};

#[cfg(feature = "store")]
pub use subscriber::checkpoint::SequenceCheckpoint;

#[cfg(feature = "partition")]
pub use partition::{NodeDescriptor, PartitionManager};

#[cfg(all(feature = "store", feature = "partition"))]
pub use subscriber::consumer_group::ConsumerGroupSubscriber;
