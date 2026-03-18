use std::ops::Range;

use thiserror::Error;

use crate::types::PublisherId;

/// Alias for `std::result::Result<T, mitiflow::Error>`.
pub type Result<T> = std::result::Result<T, Error>;

/// Structured error type for all mitiflow operations.
///
/// Marked `#[non_exhaustive]` so new variants can be added in minor releases
/// without breaking downstream matches.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// A Zenoh operation failed.
    #[error("zenoh error: {0}")]
    Zenoh(#[from] zenoh::Error),

    /// Serialization or deserialization of an event payload failed.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// The subscriber detected a gap but could not recover the missing events
    /// from the publisher's cache.
    #[error("gap recovery failed for publisher {publisher_id}, missed sequences {missed:?}")]
    GapRecoveryFailed {
        publisher_id: PublisherId,
        missed: Range<u64>,
    },

    /// `publish_durable()` timed out waiting for the watermark to confirm
    /// persistence of the given sequence number.
    #[error("durability timeout: watermark did not cover seq {seq}")]
    DurabilityTimeout { seq: u64 },

    /// A sequence checkpoint read/write operation failed.
    #[error("checkpoint error: {0}")]
    CheckpointError(String),

    /// The storage backend encountered an I/O or internal error.
    #[error("store error: {0}")]
    StoreError(String),

    /// An internal channel was closed unexpectedly.
    #[error("internal channel closed")]
    ChannelClosed,

    /// A Zenoh attachment could not be decoded into the expected metadata format.
    #[error("invalid attachment: {0}")]
    InvalidAttachment(String),

    /// The configuration is invalid.
    #[error("invalid config: {0}")]
    InvalidConfig(String),
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Serialization(e.to_string())
    }
}
