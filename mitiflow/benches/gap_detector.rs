use std::collections::HashMap;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mitiflow::subscriber::gap_detector::{GapDetector, SampleResult, SequenceTracker};
use mitiflow::types::PublisherId;

fn bench_gap_detector_normal(c: &mut Criterion) {
    c.bench_function("gap_detector/normal_delivery", |b| {
        let pub_id = PublisherId::new();
        b.iter_custom(|iters| {
            let mut det = GapDetector::new();
            let start = std::time::Instant::now();
            for seq in 0..iters {
                std::hint::black_box(det.on_sample(&pub_id, 0, seq));
            }
            start.elapsed()
        });
    });
}

fn bench_gap_detector_gap(c: &mut Criterion) {
    c.bench_function("gap_detector/gap_detection", |b| {
        let pub_id = PublisherId::new();
        b.iter_custom(|iters| {
            let mut det = GapDetector::new();
            let start = std::time::Instant::now();
            for i in 0..iters {
                // Every sample skips one seq to trigger a gap
                let seq = i * 2;
                std::hint::black_box(det.on_sample(&pub_id, 0, seq));
            }
            start.elapsed()
        });
    });
}

fn bench_gap_detector_duplicate(c: &mut Criterion) {
    c.bench_function("gap_detector/duplicate", |b| {
        let pub_id = PublisherId::new();
        let mut det = GapDetector::new();
        // Advance to seq 1000
        for seq in 0..=1000 {
            det.on_sample(&pub_id, 0, seq);
        }
        b.iter(|| {
            // All of these are duplicates
            assert_eq!(
                det.on_sample(&pub_id, 0, 500),
                SampleResult::Duplicate
            );
        });
    });
}

fn bench_gap_detector_many_publishers(c: &mut Criterion) {
    let mut group = c.benchmark_group("gap_detector/many_publishers");

    for &num_publishers in &[100, 1000] {
        let publishers: Vec<PublisherId> = (0..num_publishers).map(|_| PublisherId::new()).collect();

        // Pre-seed the detector with some state for each publisher
        let mut det = GapDetector::new();
        for pub_id in &publishers {
            for seq in 0..10 {
                det.on_sample(pub_id, 0, seq);
            }
        }

        group.bench_with_input(
            BenchmarkId::from_parameter(num_publishers),
            &publishers,
            |b, publishers| {
                let mut seq = 10u64;
                b.iter(|| {
                    // Deliver next seq for a random publisher
                    let pub_id = &publishers[seq as usize % publishers.len()];
                    // Use on_sample — the HashMap lookup is the hot path
                    std::hint::black_box(det.on_sample(pub_id, 0, seq));
                    seq += 1;
                });
            },
        );
    }
    group.finish();
}

fn bench_gap_detector_heartbeat(c: &mut Criterion) {
    let mut group = c.benchmark_group("gap_detector/heartbeat");

    for &num_partitions in &[4, 16, 64] {
        let pub_id = PublisherId::new();
        let mut det = GapDetector::new();

        // Pre-seed: each partition has seen up to seq 100
        for p in 0..num_partitions {
            for seq in 0..=100 {
                det.on_sample(&pub_id, p, seq);
            }
        }

        // Heartbeat reports seq 200 for each partition (gap of 100..201)
        let partition_seqs: HashMap<u32, u64> =
            (0..num_partitions).map(|p| (p, 200)).collect();

        group.bench_with_input(
            BenchmarkId::from_parameter(num_partitions),
            &partition_seqs,
            |b, partition_seqs| {
                b.iter(|| {
                    std::hint::black_box(det.on_heartbeat(&pub_id, partition_seqs));
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_gap_detector_normal,
    bench_gap_detector_gap,
    bench_gap_detector_duplicate,
    bench_gap_detector_many_publishers,
    bench_gap_detector_heartbeat
);
criterion_main!(benches);
