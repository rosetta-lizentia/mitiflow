//! Per-publisher sequence tracking, gap detection, and duplicate filtering.
//!
//! The [`GapDetector`] is a pure-logic component with no Zenoh dependency,
//! making it fully unit-testable.

use std::collections::HashMap;
use std::ops::Range;
use std::time::{Duration, Instant};

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

/// Describes a set of missed sequence numbers from a single publisher on a partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissInfo {
    /// The publisher that produced the missed events.
    pub source: PublisherId,
    /// The partition where the gap was detected.
    pub partition: u32,
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
    fn on_sample(&mut self, pub_id: &PublisherId, partition: u32, seq: u64) -> SampleResult;

    /// Process a heartbeat beacon from a publisher. For each partition the publisher
    /// has written to, if the advertised seq is ahead of our last-seen value,
    /// return the missing ranges.
    fn on_heartbeat(
        &mut self,
        pub_id: &PublisherId,
        partition_seqs: &HashMap<u32, u64>,
    ) -> Vec<MissInfo>;

    /// Return the last sequence number seen from the given publisher on a partition, if any.
    fn last_seen(&self, pub_id: &PublisherId, partition: u32) -> Option<u64>;
}

/// Default in-memory gap detector using a per-publisher sequence counter.
///
/// Tracks the highest delivered sequence for each publisher. When a sample
/// arrives with `seq > expected`, the gap `[expected..seq)` is reported.
/// Samples with `seq < expected` are treated as duplicates.
pub struct GapDetector {
    /// Maps (publisher ID, partition) → last delivered sequence number.
    last_seen: HashMap<(PublisherId, u32), u64>,
    /// Maps (publisher ID, partition) → wall-clock time of last activity.
    last_activity: HashMap<(PublisherId, u32), Instant>,
}

impl GapDetector {
    pub fn new() -> Self {
        Self {
            last_seen: HashMap::new(),
            last_activity: HashMap::new(),
        }
    }

    /// Create a gap detector pre-seeded with known sequence positions.
    ///
    /// Used for cross-restart recovery: the subscriber loads persisted
    /// checkpoints and injects them so that events at or below the
    /// checkpoint are treated as duplicates.
    pub fn with_checkpoints(checkpoints: HashMap<(PublisherId, u32), u64>) -> Self {
        Self {
            last_seen: checkpoints,
            last_activity: HashMap::new(),
        }
    }

    /// Remove tracking state for publishers whose last activity is older than `age`.
    ///
    /// Returns the number of entries evicted.
    pub fn evict_older_than(&mut self, age: Duration) -> usize {
        let now = Instant::now();
        let stale: Vec<(PublisherId, u32)> = self
            .last_activity
            .iter()
            .filter(|(_, t)| now.duration_since(**t) > age)
            .map(|(key, _)| *key)
            .collect();
        let count = stale.len();
        for key in stale {
            self.last_seen.remove(&key);
            self.last_activity.remove(&key);
        }
        count
    }
}

impl Default for GapDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl SequenceTracker for GapDetector {
    fn on_sample(&mut self, pub_id: &PublisherId, partition: u32, seq: u64) -> SampleResult {
        let key = (*pub_id, partition);
        let expected = self.last_seen.get(&key).map(|last| last + 1).unwrap_or(0);
        self.last_activity.insert(key, Instant::now());

        if seq == expected {
            self.last_seen.insert(key, seq);
            SampleResult::Deliver
        } else if seq > expected {
            self.last_seen.insert(key, seq);
            SampleResult::Gap(MissInfo {
                source: *pub_id,
                partition,
                missed: expected..seq,
            })
        } else {
            SampleResult::Duplicate
        }
    }

    fn on_heartbeat(
        &mut self,
        pub_id: &PublisherId,
        partition_seqs: &HashMap<u32, u64>,
    ) -> Vec<MissInfo> {
        let mut misses = Vec::new();
        for (&partition, &current_seq) in partition_seqs {
            let key = (*pub_id, partition);
            let expected = self.last_seen.get(&key).map(|last| last + 1).unwrap_or(0);
            self.last_activity.insert(key, Instant::now());

            if current_seq >= expected {
                let next = current_seq + 1;
                if expected < next {
                    misses.push(MissInfo {
                        source: *pub_id,
                        partition,
                        missed: expected..next,
                    });
                }
            }
        }
        misses
    }

    fn last_seen(&self, pub_id: &PublisherId, partition: u32) -> Option<u64> {
        self.last_seen.get(&(*pub_id, partition)).copied()
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

        assert_eq!(det.on_sample(&pub_id, 0, 0), SampleResult::Deliver);
        assert_eq!(det.on_sample(&pub_id, 0, 1), SampleResult::Deliver);
        assert_eq!(det.on_sample(&pub_id, 0, 2), SampleResult::Deliver);
        assert_eq!(det.last_seen(&pub_id, 0), Some(2));
    }

    #[test]
    fn gap_detection() {
        let mut det = GapDetector::new();
        let pub_id = make_pub_id();

        assert_eq!(det.on_sample(&pub_id, 0, 0), SampleResult::Deliver);
        // Skip seq 1, 2
        let result = det.on_sample(&pub_id, 0, 3);
        assert_eq!(
            result,
            SampleResult::Gap(MissInfo {
                source: pub_id,
                partition: 0,
                missed: 1..3,
            })
        );
        assert_eq!(det.last_seen(&pub_id, 0), Some(3));
    }

    #[test]
    fn duplicate_detection() {
        let mut det = GapDetector::new();
        let pub_id = make_pub_id();

        assert_eq!(det.on_sample(&pub_id, 0, 0), SampleResult::Deliver);
        assert_eq!(det.on_sample(&pub_id, 0, 1), SampleResult::Deliver);
        assert_eq!(det.on_sample(&pub_id, 0, 0), SampleResult::Duplicate);
        assert_eq!(det.on_sample(&pub_id, 0, 1), SampleResult::Duplicate);
    }

    #[test]
    fn multi_publisher_independent_tracking() {
        let mut det = GapDetector::new();
        let pub_a = make_pub_id();
        let pub_b = make_pub_id();

        assert_eq!(det.on_sample(&pub_a, 0, 0), SampleResult::Deliver);
        assert_eq!(det.on_sample(&pub_b, 0, 0), SampleResult::Deliver);
        assert_eq!(det.on_sample(&pub_a, 0, 1), SampleResult::Deliver);
        assert_eq!(
            det.on_sample(&pub_b, 0, 5),
            SampleResult::Gap(MissInfo {
                source: pub_b,
                partition: 0,
                missed: 1..5,
            })
        );
        assert_eq!(det.last_seen(&pub_a, 0), Some(1));
        assert_eq!(det.last_seen(&pub_b, 0), Some(5));
    }

    #[test]
    fn heartbeat_triggers_gap() {
        let mut det = GapDetector::new();
        let pub_id = make_pub_id();

        // No samples seen yet — heartbeat says partition 0 current_seq=5
        let seqs = HashMap::from([(0u32, 5u64)]);
        let misses = det.on_heartbeat(&pub_id, &seqs);
        assert_eq!(misses.len(), 1);
        assert_eq!(misses[0].missed, 0..6);
        assert_eq!(misses[0].partition, 0);
    }

    #[test]
    fn heartbeat_no_gap_when_caught_up() {
        let mut det = GapDetector::new();
        let pub_id = make_pub_id();

        det.on_sample(&pub_id, 0, 0);
        det.on_sample(&pub_id, 0, 1);
        det.on_sample(&pub_id, 0, 2);

        let seqs = HashMap::from([(0u32, 2u64)]);
        let misses = det.on_heartbeat(&pub_id, &seqs);
        assert!(misses.is_empty());
    }

    #[test]
    fn with_checkpoints() {
        let pub_id = make_pub_id();
        let mut checkpoints = HashMap::new();
        checkpoints.insert((pub_id, 0), 10);

        let mut det = GapDetector::with_checkpoints(checkpoints);

        // seq 10 and below should be duplicate
        assert_eq!(det.on_sample(&pub_id, 0, 10), SampleResult::Duplicate);
        assert_eq!(det.on_sample(&pub_id, 0, 9), SampleResult::Duplicate);
        // seq 11 is the expected next
        assert_eq!(det.on_sample(&pub_id, 0, 11), SampleResult::Deliver);
    }

    #[test]
    fn first_sample_nonzero() {
        let mut det = GapDetector::new();
        let pub_id = make_pub_id();

        // First sample is seq 5 (late join) — reports gap [0..5)
        let result = det.on_sample(&pub_id, 0, 5);
        assert_eq!(
            result,
            SampleResult::Gap(MissInfo {
                source: pub_id,
                partition: 0,
                missed: 0..5,
            })
        );
    }

    #[test]
    fn cross_partition_independence() {
        let mut det = GapDetector::new();
        let pub_id = make_pub_id();

        // Publisher writes to partition 0 and partition 1 independently.
        assert_eq!(det.on_sample(&pub_id, 0, 0), SampleResult::Deliver);
        assert_eq!(det.on_sample(&pub_id, 1, 0), SampleResult::Deliver);
        assert_eq!(det.on_sample(&pub_id, 0, 1), SampleResult::Deliver);
        assert_eq!(det.on_sample(&pub_id, 1, 1), SampleResult::Deliver);

        // Gap on partition 1 only.
        assert_eq!(
            det.on_sample(&pub_id, 1, 5),
            SampleResult::Gap(MissInfo {
                source: pub_id,
                partition: 1,
                missed: 2..5,
            })
        );
        // Partition 0 unaffected.
        assert_eq!(det.on_sample(&pub_id, 0, 2), SampleResult::Deliver);
    }
}
