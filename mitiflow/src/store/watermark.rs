//! Watermark types for durability confirmation.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Commit watermark broadcast by the Event Store.
///
/// Published periodically on `{key_prefix}/$watermark`. Publishers subscribe
/// to this stream and use it to confirm that their events have been durably stored.
///
/// See [03_durability.md](../../docs/03_durability.md) § B for the full protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitWatermark {
    /// Highest contiguous sequence number that has been durably stored.
    pub committed_seq: u64,
    /// Sequence numbers below `committed_seq` that are still missing (gaps).
    pub gaps: Vec<u64>,
    /// Event Store wall-clock time when this watermark was generated.
    pub timestamp: DateTime<Utc>,
}

impl CommitWatermark {
    /// Check whether the given sequence number is confirmed durable.
    ///
    /// A sequence is durable if it's at or below the committed watermark
    /// and not listed in the gaps vector.
    pub fn is_durable(&self, seq: u64) -> bool {
        seq <= self.committed_seq && !self.gaps.contains(&seq)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_below_committed() {
        let wm = CommitWatermark {
            committed_seq: 100,
            gaps: vec![],
            timestamp: Utc::now(),
        };
        assert!(wm.is_durable(50));
        assert!(wm.is_durable(100));
        assert!(!wm.is_durable(101));
    }

    #[test]
    fn not_durable_if_in_gaps() {
        let wm = CommitWatermark {
            committed_seq: 100,
            gaps: vec![42, 77],
            timestamp: Utc::now(),
        };
        assert!(!wm.is_durable(42));
        assert!(!wm.is_durable(77));
        assert!(wm.is_durable(50));
        assert!(wm.is_durable(100));
    }
}
