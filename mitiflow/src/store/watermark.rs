//! Watermark types for durability confirmation.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::PublisherId;

/// Per-publisher durability progress.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublisherWatermark {
    /// Highest contiguous sequence number durably stored for this publisher.
    pub committed_seq: u64,
    /// Sequence numbers below `committed_seq` that are still missing (gaps).
    pub gaps: Vec<u64>,
}

impl PublisherWatermark {
    /// Check whether the given sequence number is confirmed durable.
    pub fn is_durable(&self, seq: u64) -> bool {
        seq <= self.committed_seq && !self.gaps.contains(&seq)
    }
}

/// Commit watermark broadcast by the Event Store.
///
/// Published periodically on `{key_prefix}/_watermark`. Publishers subscribe
/// to this stream and use it to confirm that their events have been durably stored.
///
/// Tracks durability progress **per publisher**, since each publisher maintains
/// its own independent monotonic sequence counter.
///
/// See [03_durability.md](../../docs/03_durability.md) § B for the full protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitWatermark {
    /// Per-publisher durability progress.
    pub publishers: HashMap<PublisherId, PublisherWatermark>,
    /// Event Store wall-clock time when this watermark was generated.
    pub timestamp: DateTime<Utc>,
}

impl CommitWatermark {
    /// Check whether the given sequence from a specific publisher is confirmed durable.
    ///
    /// Returns `false` if the publisher is unknown to the store.
    pub fn is_durable(&self, publisher_id: &PublisherId, seq: u64) -> bool {
        self.publishers
            .get(publisher_id)
            .is_some_and(|pw| pw.is_durable(seq))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_wm(entries: Vec<(PublisherId, u64, Vec<u64>)>) -> CommitWatermark {
        let mut publishers = HashMap::new();
        for (id, committed, gaps) in entries {
            publishers.insert(id, PublisherWatermark { committed_seq: committed, gaps });
        }
        CommitWatermark { publishers, timestamp: Utc::now() }
    }

    #[test]
    fn durable_below_committed() {
        let pub_id = PublisherId::new();
        let wm = make_wm(vec![(pub_id, 100, vec![])]);
        assert!(wm.is_durable(&pub_id, 50));
        assert!(wm.is_durable(&pub_id, 100));
        assert!(!wm.is_durable(&pub_id, 101));
    }

    #[test]
    fn not_durable_if_in_gaps() {
        let pub_id = PublisherId::new();
        let wm = make_wm(vec![(pub_id, 100, vec![42, 77])]);
        assert!(!wm.is_durable(&pub_id, 42));
        assert!(!wm.is_durable(&pub_id, 77));
        assert!(wm.is_durable(&pub_id, 50));
        assert!(wm.is_durable(&pub_id, 100));
    }

    #[test]
    fn unknown_publisher_not_durable() {
        let pub_a = PublisherId::new();
        let pub_b = PublisherId::new();
        let wm = make_wm(vec![(pub_a, 100, vec![])]);
        assert!(!wm.is_durable(&pub_b, 0));
    }

    #[test]
    fn multiple_publishers_independent() {
        let pub_a = PublisherId::new();
        let pub_b = PublisherId::new();
        let wm = make_wm(vec![
            (pub_a, 50, vec![10]),
            (pub_b, 200, vec![]),
        ]);
        assert!(wm.is_durable(&pub_a, 30));
        assert!(!wm.is_durable(&pub_a, 10)); // in gaps
        assert!(!wm.is_durable(&pub_a, 51)); // above committed
        assert!(wm.is_durable(&pub_b, 200));
        assert!(!wm.is_durable(&pub_b, 201));
    }
}
