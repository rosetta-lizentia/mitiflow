//! Per-publisher sequence tracking, gap detection, and duplicate filtering.
//!
//! The [`GapDetector`] is a pure-logic component with no Zenoh dependency,
//! making it fully unit-testable.

use std::collections::HashMap;
use std::ops::Range;

use crate::types::PublisherId;

/// Result of presenting a sample to the sequence tracker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SampleResult {
    /// The sample has the expected next sequence number — deliver it.
    Deliver,
    /// The sample's sequence number was already seen — drop it.
    Duplicate,
    /// One or more intermediate sequences were skipped — a gap exists.
    Gap(MissInfo),
}

/// Describes a set of missed sequence numbers from a single publisher.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissInfo {
    /// The publisher that produced the missed events.
    pub source: PublisherId,
    /// Half-open range `[start..end)` of missed sequence numbers.
    pub missed: Range<u64>,
}

/// Trait for sequence tracking and gap detection.
///
/// Abstracting this behind a trait allows tests to substitute mock trackers
/// and makes it possible to swap in checkpoint-backed implementations later
/// (Phase 4: cross-restart dedup).
pub trait SequenceTracker: Send + Sync {
    /// Record an incoming sample and determine whether it should be delivered,
    /// dropped as a duplicate, or flagged as a gap.
    fn on_sample(&mut self, pub_id: &PublisherId, seq: u64) -> SampleResult;

    /// Process a heartbeat beacon from a publisher. If the publisher's advertised
    /// `current_seq` is ahead of our last-seen value, return the missing ranges.
    fn on_heartbeat(&mut self, pub_id: &PublisherId, current_seq: u64) -> Vec<MissInfo>;

    /// Return the last sequence number seen from the given publisher, if any.
    fn last_seen(&self, pub_id: &PublisherId) -> Option<u64>;
}

/// Default in-memory gap detector using a per-publisher sequence counter.
///
/// Tracks the highest delivered sequence for each publisher. When a sample
/// arrives with `seq > expected`, the gap `[expected..seq)` is reported.
/// Samples with `seq < expected` are treated as duplicates.
pub struct GapDetector {
    /// Maps publisher ID → last delivered sequence number.
    last_seen: HashMap<PublisherId, u64>,
}

impl GapDetector {
    pub fn new() -> Self {
        Self {
            last_seen: HashMap::new(),
        }
    }

    /// Create a gap detector pre-seeded with known sequence positions.
    ///
    /// Used for cross-restart recovery: the subscriber loads persisted
    /// checkpoints and injects them so that events at or below the
    /// checkpoint are treated as duplicates.
    pub fn with_checkpoints(checkpoints: HashMap<PublisherId, u64>) -> Self {
        Self {
            last_seen: checkpoints,
        }
    }
}

impl Default for GapDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl SequenceTracker for GapDetector {
    fn on_sample(&mut self, pub_id: &PublisherId, seq: u64) -> SampleResult {
        let expected = self.last_seen.get(pub_id).map(|last| last + 1).unwrap_or(0);

        if seq == expected {
            // Normal: exactly the next expected sequence.
            self.last_seen.insert(*pub_id, seq);
            SampleResult::Deliver
        } else if seq > expected {
            // Gap: we missed [expected..seq).
            self.last_seen.insert(*pub_id, seq);
            SampleResult::Gap(MissInfo {
                source: *pub_id,
                missed: expected..seq,
            })
        } else {
            // seq < expected → duplicate.
            SampleResult::Duplicate
        }
    }

    fn on_heartbeat(&mut self, pub_id: &PublisherId, current_seq: u64) -> Vec<MissInfo> {
        let expected = self.last_seen.get(pub_id).map(|last| last + 1).unwrap_or(0);

        if current_seq >= expected && expected <= current_seq {
            let next = current_seq + 1;
            if expected < next {
                return vec![MissInfo {
                    source: *pub_id,
                    missed: expected..next,
                }];
            }
        }
        vec![]
    }

    fn last_seen(&self, pub_id: &PublisherId) -> Option<u64> {
        self.last_seen.get(pub_id).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_pub_id() -> PublisherId {
        PublisherId::new()
    }

    #[test]
    fn sequential_delivery() {
        let mut det = GapDetector::new();
        let pub_id = make_pub_id();

        assert_eq!(det.on_sample(&pub_id, 0), SampleResult::Deliver);
        assert_eq!(det.on_sample(&pub_id, 1), SampleResult::Deliver);
        assert_eq!(det.on_sample(&pub_id, 2), SampleResult::Deliver);
        assert_eq!(det.last_seen(&pub_id), Some(2));
    }

    #[test]
    fn gap_detection() {
        let mut det = GapDetector::new();
        let pub_id = make_pub_id();

        assert_eq!(det.on_sample(&pub_id, 0), SampleResult::Deliver);
        // Skip seq 1, 2
        let result = det.on_sample(&pub_id, 3);
        assert_eq!(
            result,
            SampleResult::Gap(MissInfo {
                source: pub_id,
                missed: 1..3,
            })
        );
        assert_eq!(det.last_seen(&pub_id), Some(3));
    }

    #[test]
    fn duplicate_detection() {
        let mut det = GapDetector::new();
        let pub_id = make_pub_id();

        assert_eq!(det.on_sample(&pub_id, 0), SampleResult::Deliver);
        assert_eq!(det.on_sample(&pub_id, 1), SampleResult::Deliver);
        assert_eq!(det.on_sample(&pub_id, 0), SampleResult::Duplicate);
        assert_eq!(det.on_sample(&pub_id, 1), SampleResult::Duplicate);
    }

    #[test]
    fn multi_publisher_independent_tracking() {
        let mut det = GapDetector::new();
        let pub_a = make_pub_id();
        let pub_b = make_pub_id();

        assert_eq!(det.on_sample(&pub_a, 0), SampleResult::Deliver);
        assert_eq!(det.on_sample(&pub_b, 0), SampleResult::Deliver);
        assert_eq!(det.on_sample(&pub_a, 1), SampleResult::Deliver);
        assert_eq!(
            det.on_sample(&pub_b, 5),
            SampleResult::Gap(MissInfo {
                source: pub_b,
                missed: 1..5,
            })
        );
        assert_eq!(det.last_seen(&pub_a), Some(1));
        assert_eq!(det.last_seen(&pub_b), Some(5));
    }

    #[test]
    fn heartbeat_triggers_gap() {
        let mut det = GapDetector::new();
        let pub_id = make_pub_id();

        // No samples seen yet — heartbeat says current_seq=5
        let misses = det.on_heartbeat(&pub_id, 5);
        assert_eq!(misses.len(), 1);
        assert_eq!(misses[0].missed, 0..6);
    }

    #[test]
    fn heartbeat_no_gap_when_caught_up() {
        let mut det = GapDetector::new();
        let pub_id = make_pub_id();

        det.on_sample(&pub_id, 0);
        det.on_sample(&pub_id, 1);
        det.on_sample(&pub_id, 2);

        let misses = det.on_heartbeat(&pub_id, 2);
        assert!(misses.is_empty());
    }

    #[test]
    fn with_checkpoints() {
        let pub_id = make_pub_id();
        let mut checkpoints = HashMap::new();
        checkpoints.insert(pub_id, 10);

        let mut det = GapDetector::with_checkpoints(checkpoints);

        // seq 10 and below should be duplicate
        assert_eq!(det.on_sample(&pub_id, 10), SampleResult::Duplicate);
        assert_eq!(det.on_sample(&pub_id, 9), SampleResult::Duplicate);
        // seq 11 is the expected next
        assert_eq!(det.on_sample(&pub_id, 11), SampleResult::Deliver);
    }

    #[test]
    fn first_sample_nonzero() {
        let mut det = GapDetector::new();
        let pub_id = make_pub_id();

        // First sample is seq 5 (late join) — reports gap [0..5)
        let result = det.on_sample(&pub_id, 5);
        assert_eq!(
            result,
            SampleResult::Gap(MissInfo {
                source: pub_id,
                missed: 0..5,
            })
        );
    }
}
