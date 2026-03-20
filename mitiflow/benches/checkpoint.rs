use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mitiflow::subscriber::checkpoint::SequenceCheckpoint;
use mitiflow::types::PublisherId;

fn bench_checkpoint_ack(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let checkpoint = SequenceCheckpoint::open(dir.path()).unwrap();
    let pub_id = PublisherId::new();

    let mut seq = 0u64;
    c.bench_function("checkpoint_ack", |b| {
        b.iter(|| {
            checkpoint.ack(&pub_id, 0, seq).unwrap();
            seq += 1;
        });
    });
}

fn bench_checkpoint_last(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let checkpoint = SequenceCheckpoint::open(dir.path()).unwrap();
    let pub_id = PublisherId::new();

    // Pre-populate
    checkpoint.ack(&pub_id, 0, 1000).unwrap();

    c.bench_function("checkpoint_last", |b| {
        b.iter(|| {
            let last = checkpoint.last_checkpoint(&pub_id, 0).unwrap();
            std::hint::black_box(last);
        });
    });
}

fn bench_checkpoint_restore(c: &mut Criterion) {
    let mut group = c.benchmark_group("checkpoint_restore");

    for &num_entries in &[100, 1000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_entries),
            &num_entries,
            |b, &num_entries| {
                b.iter_with_setup(
                    || {
                        let dir = tempfile::tempdir().unwrap();
                        let checkpoint = SequenceCheckpoint::open(dir.path()).unwrap();

                        // Write entries from different publishers across different partitions
                        for i in 0..num_entries {
                            let pub_id = PublisherId::new();
                            let partition = (i % 16) as u32;
                            checkpoint.ack(&pub_id, partition, i as u64 * 100).unwrap();
                        }

                        (dir, checkpoint)
                    },
                    |(_dir, checkpoint)| {
                        let map = checkpoint.restore().unwrap();
                        std::hint::black_box(map.len());
                    },
                );
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_checkpoint_ack,
    bench_checkpoint_last,
    bench_checkpoint_restore
);
criterion_main!(benches);
