use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mitiflow::partition::hash_ring;

fn bench_partition_for(c: &mut Criterion) {
    let keys: Vec<String> = (0..1000).map(|i| format!("order/{i}")).collect();

    c.bench_function("partition_for/64_partitions", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            let key = &keys[idx % keys.len()];
            idx += 1;
            std::hint::black_box(hash_ring::partition_for(key, 64));
        });
    });
}

fn bench_worker_for(c: &mut Criterion) {
    let mut group = c.benchmark_group("worker_for");

    for &num_workers in &[3, 10, 50] {
        let workers: Vec<String> = (0..num_workers).map(|i| format!("worker-{i}")).collect();

        group.bench_with_input(
            BenchmarkId::from_parameter(num_workers),
            &workers,
            |b, workers| {
                let mut partition = 0u32;
                b.iter(|| {
                    partition = (partition + 1) % 64;
                    std::hint::black_box(hash_ring::worker_for(partition, workers));
                });
            },
        );
    }
    group.finish();
}

fn bench_assignments(c: &mut Criterion) {
    let mut group = c.benchmark_group("assignments");

    for &(num_workers, num_partitions) in &[(3, 64), (10, 64), (50, 64), (3, 256), (10, 256)] {
        let workers: Vec<String> = (0..num_workers).map(|i| format!("worker-{i}")).collect();
        let label = format!("{num_workers}w_{num_partitions}p");

        group.bench_with_input(
            BenchmarkId::from_parameter(label),
            &workers,
            |b, workers| {
                b.iter(|| {
                    std::hint::black_box(hash_ring::assignments(workers, num_partitions));
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_partition_for,
    bench_worker_for,
    bench_assignments
);
criterion_main!(benches);
