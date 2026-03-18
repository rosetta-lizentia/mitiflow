//! Integration tests for cross-restart deduplication via SequenceCheckpoint.

use std::collections::HashMap;

use mitiflow::SequenceCheckpoint;
use mitiflow::subscriber::gap_detector::{GapDetector, SampleResult, SequenceTracker};
use mitiflow::types::PublisherId;

#[test]
fn checkpoint_persist_and_restore() {
    let dir = tempfile::tempdir().unwrap();
    let pub1 = PublisherId::new();
    let pub2 = PublisherId::new();

    // Write checkpoints.
    {
        let cp = SequenceCheckpoint::open(dir.path().join("cp")).unwrap();
        cp.ack(&pub1, 10).unwrap();
        cp.ack(&pub2, 20).unwrap();
    }

    // Reopen and restore.
    let cp = SequenceCheckpoint::open(dir.path().join("cp")).unwrap();
    let restored = cp.restore().unwrap();
    assert_eq!(restored[&pub1], 10);
    assert_eq!(restored[&pub2], 20);
}

#[test]
fn checkpoint_feeds_gap_detector_dedup() {
    let dir = tempfile::tempdir().unwrap();
    let pub_id = PublisherId::new();

    // Simulate first run: process up to seq 5.
    {
        let cp = SequenceCheckpoint::open(dir.path().join("cp")).unwrap();
        cp.ack(&pub_id, 5).unwrap();
    }

    // Simulate restart: load checkpoint, seed gap detector.
    let cp = SequenceCheckpoint::open(dir.path().join("cp")).unwrap();
    let checkpoints = cp.restore().unwrap();
    let mut gd = GapDetector::with_checkpoints(checkpoints);

    // Events 0–5 should be duplicates.
    for seq in 0..=5 {
        assert_eq!(
            gd.on_sample(&pub_id, seq),
            SampleResult::Duplicate,
            "seq {seq} should be duplicate"
        );
    }

    // Event 6 should be delivered.
    assert_eq!(gd.on_sample(&pub_id, 6), SampleResult::Deliver);
}

#[test]
fn checkpoint_gap_detector_detects_gap_after_restore() {
    let dir = tempfile::tempdir().unwrap();
    let pub_id = PublisherId::new();

    {
        let cp = SequenceCheckpoint::open(dir.path().join("cp")).unwrap();
        cp.ack(&pub_id, 3).unwrap();
    }

    let cp = SequenceCheckpoint::open(dir.path().join("cp")).unwrap();
    let checkpoints = cp.restore().unwrap();
    let mut gd = GapDetector::with_checkpoints(checkpoints);

    // Next expected is seq 4. If seq 7 arrives, gap [4..7) should be reported.
    match gd.on_sample(&pub_id, 7) {
        SampleResult::Gap(miss) => {
            assert_eq!(miss.missed, 4..7);
            assert_eq!(miss.source, pub_id);
        }
        other => panic!("expected Gap, got {other:?}"),
    }
}

#[test]
fn multiple_publishers_independent_checkpoints() {
    let dir = tempfile::tempdir().unwrap();
    let pub_a = PublisherId::new();
    let pub_b = PublisherId::new();

    let cp = SequenceCheckpoint::open(dir.path().join("cp")).unwrap();
    cp.ack(&pub_a, 10).unwrap();
    cp.ack(&pub_b, 5).unwrap();

    let mut checkpoints = HashMap::new();
    checkpoints.insert(pub_a, 10);
    checkpoints.insert(pub_b, 5);
    let mut gd = GapDetector::with_checkpoints(checkpoints);

    // pub_a seq 10 → duplicate, seq 11 → deliver.
    assert_eq!(gd.on_sample(&pub_a, 10), SampleResult::Duplicate);
    assert_eq!(gd.on_sample(&pub_a, 11), SampleResult::Deliver);

    // pub_b seq 5 → duplicate, seq 6 → deliver.
    assert_eq!(gd.on_sample(&pub_b, 5), SampleResult::Duplicate);
    assert_eq!(gd.on_sample(&pub_b, 6), SampleResult::Deliver);
}
