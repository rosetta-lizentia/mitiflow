#![allow(unused_assignments)]

use std::ops::Range;

use miette::Diagnostic;
use thiserror::Error;

use crate::types::PublisherId;

/// Alias for `std::result::Result<T, mitiflow::Error>`.
pub type Result<T> = std::result::Result<T, Error>;

/// Structured error type for all mitiflow operations.
///
/// Marked `#[non_exhaustive]` so new variants can be added in minor releases
/// without breaking downstream matches.
#[derive(Debug, Error, Diagnostic)]
#[non_exhaustive]
pub enum Error {
    /// A Zenoh operation failed.
    #[error("zenoh error: {0}")]
    #[diagnostic(help(
        "Check Zenoh connectivity and configuration. Ensure the Zenoh router or peer is reachable."
    ))]
    Zenoh(#[from] zenoh::Error),

    /// Serialization or deserialization of an event payload failed.
    #[error("serialization error: {0}")]
    #[diagnostic(help("Verify that publisher and subscriber use the same CodecFormat."))]
    Serialization(String),

    /// The subscriber detected a gap but could not recover the missing events
    /// from the publisher's cache.
    #[error("gap recovery failed for publisher {publisher_id}, missed sequences {missed:?}")]
    #[diagnostic(
        code(mitiflow::gap_recovery),
        help("The publisher's cache may be too small. Increase cache_size in EventBusConfig.")
    )]
    GapRecoveryFailed {
        publisher_id: PublisherId,
        missed: Range<u64>,
    },

    /// `publish_durable()` timed out waiting for the watermark to confirm
    /// persistence of the given sequence number.
    #[error("durability timeout: watermark did not cover seq {seq}")]
    #[diagnostic(
        code(mitiflow::durability_timeout),
        help(
            "The event store may be down or slow. Check store health and increase the durability timeout if needed."
        )
    )]
    DurabilityTimeout { seq: u64 },

    /// A sequence checkpoint read/write operation failed.
    #[error("checkpoint error: {0}")]
    CheckpointError(String),

    /// The storage backend encountered an I/O or internal error.
    #[error("store error: {0}")]
    #[diagnostic(
        code(mitiflow::store),
        help(
            "Check disk space and permissions for the data directory. Another process may be locking the store."
        )
    )]
    StoreError(String),

    /// An internal channel was closed unexpectedly.
    #[error("internal channel closed")]
    #[diagnostic(help("This usually indicates the owning task was cancelled or panicked."))]
    ChannelClosed,

    /// A Zenoh attachment could not be decoded into the expected metadata format.
    #[error("invalid attachment: {0}")]
    #[diagnostic(help("This may indicate a version mismatch between publisher and subscriber."))]
    InvalidAttachment(String),

    /// The configuration is invalid.
    #[error("invalid config: {0}")]
    #[diagnostic(
        code(mitiflow::config),
        help("Check the configuration values and ensure all required fields are set.")
    )]
    InvalidConfig(String),

    /// A consumer group offset commit was rejected because the commit's
    /// generation is older than the stored generation (zombie fencing).
    #[error(
        "stale fenced commit for group {group}: commit gen {commit_gen} < stored gen {stored_gen}"
    )]
    #[diagnostic(
        code(mitiflow::stale_commit),
        help(
            "A new consumer generation has started. This consumer instance should stop processing."
        )
    )]
    StaleFencedCommit {
        group: String,
        commit_gen: u64,
        stored_gen: u64,
    },

    /// An event key is invalid (empty, contains `*` or `$`).
    #[error("invalid key: {0}")]
    #[diagnostic(help("Keys must be non-empty and must not contain '*' or '$' characters."))]
    InvalidKey(String),

    /// The slow-consumer offload to store-based catch-up failed.
    #[error("offload failed: {0}")]
    #[diagnostic(
        code(mitiflow::offload),
        help(
            "The event store may be unavailable or data has been compacted past the consumer's position."
        )
    )]
    OffloadFailed(String),
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Serialization(e.to_string())
    }
}

impl From<rmp_serde::encode::Error> for Error {
    fn from(e: rmp_serde::encode::Error) -> Self {
        Error::Serialization(e.to_string())
    }
}

impl From<rmp_serde::decode::Error> for Error {
    fn from(e: rmp_serde::decode::Error) -> Self {
        Error::Serialization(e.to_string())
    }
}

impl From<postcard::Error> for Error {
    fn from(e: postcard::Error) -> Self {
        Error::Serialization(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use miette::Diagnostic;

    #[test]
    fn store_error_has_diagnostic_help() {
        let err = Error::StoreError("partition locked".into());
        let help = err.help().map(|h| h.to_string());
        assert!(help.is_some());
        assert!(help.unwrap().contains("disk space"));
    }

    #[test]
    fn store_error_has_diagnostic_code() {
        let err = Error::StoreError("partition locked".into());
        let code = err.code().map(|c| c.to_string());
        assert_eq!(code.as_deref(), Some("mitiflow::store"));
    }

    #[test]
    fn gap_recovery_error_has_help() {
        let err = Error::GapRecoveryFailed {
            publisher_id: PublisherId::from_uuid(uuid::Uuid::nil()),
            missed: 5..10,
        };
        let help = err.help().map(|h| h.to_string());
        assert!(help.is_some());
        assert!(help.unwrap().contains("cache_size"));
    }

    #[test]
    fn config_error_has_code() {
        let err = Error::InvalidConfig("missing key_prefix".into());
        let code = err.code().map(|c| c.to_string());
        assert_eq!(code.as_deref(), Some("mitiflow::config"));
    }

    #[test]
    fn durability_timeout_has_help() {
        let err = Error::DurabilityTimeout { seq: 42 };
        let help = err.help().map(|h| h.to_string());
        assert!(help.is_some());
        assert!(help.unwrap().contains("store"));
    }

    #[test]
    fn invalid_key_has_help() {
        let err = Error::InvalidKey("my*key".into());
        let help = err.help().map(|h| h.to_string());
        assert!(help.is_some());
        assert!(help.unwrap().contains("'*'"));
    }

    #[test]
    fn stale_commit_has_code() {
        let err = Error::StaleFencedCommit {
            group: "grp".into(),
            commit_gen: 1,
            stored_gen: 2,
        };
        let code = err.code().map(|c| c.to_string());
        assert_eq!(code.as_deref(), Some("mitiflow::stale_commit"));
    }
}
