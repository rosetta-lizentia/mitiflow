//! Consumer group offset commit types.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::PublisherId;

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
