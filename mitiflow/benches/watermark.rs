use std::collections::HashMap;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mitiflow::store::watermark::{CommitWatermark, PublisherWatermark};
use mitiflow::types::PublisherId;

fn make_commit_watermark(num_publishers: usize) -> CommitWatermark {
    let mut publishers = HashMap::new();
    for _ in 0..num_publishers {
        let pub_id = PublisherId::new();
        publishers.insert(
            pub_id,
            PublisherWatermark {
                committed_seq: 10_000,
                gaps: vec![],
            },
        );
    }
    CommitWatermark {
        partition: 0,
        publishers,
        timestamp: chrono::Utc::now(),
        epoch: 1,
    }
}

fn bench_watermark_serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("watermark_serialize");

    for &num_publishers in &[10, 100] {
        let wm = make_commit_watermark(num_publishers);

        group.bench_with_input(BenchmarkId::from_parameter(num_publishers), &wm, |b, wm| {
            b.iter(|| serde_json::to_vec(wm).unwrap());
        });
    }
    group.finish();
}

fn bench_watermark_deserialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("watermark_deserialize");

    for &num_publishers in &[10, 100] {
        let wm = make_commit_watermark(num_publishers);
        let bytes = serde_json::to_vec(&wm).unwrap();

        group.bench_with_input(
            BenchmarkId::from_parameter(num_publishers),
            &bytes,
            |b, bytes| {
                b.iter(|| serde_json::from_slice::<CommitWatermark>(bytes).unwrap());
            },
        );
    }
    group.finish();
}

fn bench_commit_watermark_is_durable(c: &mut Criterion) {
    let mut group = c.benchmark_group("watermark_is_durable");

    for &num_publishers in &[10, 100] {
        let wm = make_commit_watermark(num_publishers);
        // Pick the first publisher for the lookup
        let target_pub_id = *wm.publishers.keys().next().unwrap();

        group.bench_with_input(
            BenchmarkId::from_parameter(num_publishers),
            &(wm.clone(), target_pub_id),
            |b, (wm, pub_id)| {
                b.iter(|| std::hint::black_box(wm.is_durable(pub_id, 5_000)));
            },
        );
    }
    group.finish();
}

fn bench_publisher_watermark_is_durable(c: &mut Criterion) {
    let mut group = c.benchmark_group("publisher_watermark_is_durable");

    for &num_gaps in &[0, 10, 100] {
        let gaps: Vec<u64> = (0..num_gaps).map(|i| i * 100 + 50).collect();
        let pw = PublisherWatermark {
            committed_seq: 10_000,
            gaps,
        };

        group.bench_with_input(BenchmarkId::from_parameter(num_gaps), &pw, |b, pw| {
            b.iter(|| {
                // Check a seq that exists (not in gaps)
                std::hint::black_box(pw.is_durable(5_000));
                // Check a seq that is in a gap (worst case for linear scan)
                if num_gaps > 0 {
                    std::hint::black_box(pw.is_durable(50));
                }
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_watermark_serialize,
    bench_watermark_deserialize,
    bench_commit_watermark_is_durable,
    bench_publisher_watermark_is_durable
);
criterion_main!(benches);
