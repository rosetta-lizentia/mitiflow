//! Consumer group offset commit types.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::PublisherId;

use super::backend::HlcTimestamp;
use crate::subscriber::keyed_consumer::KeyFilter;

/// An offset commit from a consumer group member.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetCommit {
    /// Consumer group identifier.
    pub group_id: String,
    /// Member that is committing (for audit/debugging).
    pub member_id: String,
    /// Partition this commit applies to.
    pub partition: u32,
    /// Per-publisher committed sequence numbers.
    pub offsets: HashMap<PublisherId, u64>,
    /// Generation ID — for fencing stale commits.
    pub generation: u64,
    /// Commit timestamp.
    pub timestamp: DateTime<Utc>,
}

/// An offset commit for key-scoped consumers.
///
/// Commits an HLC cursor position for a specific key filter within a consumer group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyedOffsetCommit {
    /// Consumer group identifier.
    pub group_id: String,
    /// Member that is committing.
    pub member_id: String,
    /// The key filter this offset applies to.
    pub key_filter: KeyFilter,
    /// Last delivered HLC timestamp (cursor position).
    pub last_hlc: HlcTimestamp,
    /// Generation ID for fencing stale commits.
    pub generation: u64,
    /// Commit timestamp.
    pub timestamp: DateTime<Utc>,
}
