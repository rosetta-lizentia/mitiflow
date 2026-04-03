use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mitiflow::store::FjallBackend;
use mitiflow::store::backend::{EventMetadata, HlcTimestamp, StorageBackend};
use mitiflow::store::query::{QueryFilters, ReplayFilters};
use mitiflow::types::{EventId, PublisherId};

fn make_metadata(pub_id: &PublisherId, seq: u64) -> EventMetadata {
    EventMetadata {
        seq,
        publisher_id: *pub_id,
        event_id: EventId::new(),
        timestamp: chrono::Utc::now(),
        key_expr: format!("bench/events/{seq}"),
        key: None,
        hlc_timestamp: Some(HlcTimestamp {
            physical_ns: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
            logical: seq as u32,
        }),
    }
}

fn make_payload(size: usize) -> Vec<u8> {
    vec![0xAB; size]
}

/// Populate a backend with `count` events from a single publisher.
fn populate_backend(backend: &FjallBackend, pub_id: &PublisherId, count: u64) {
    let payload = make_payload(256);
    let batch: Vec<_> = (0..count)
        .map(|seq| {
            let meta = make_metadata(pub_id, seq);
            let key = meta.key_expr.clone();
            (key, payload.clone(), meta)
        })
        .collect();
    // Store in batches of 100 to avoid huge allocations
    for chunk in batch.chunks(100) {
        backend.store_batch(chunk.to_vec()).unwrap();
    }
}

fn bench_store_write_single(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let backend = FjallBackend::open(dir.path(), 0).unwrap();
    let pub_id = PublisherId::new();
    let payload = make_payload(256);

    let mut seq = 0u64;
    c.bench_function("store_write/single", |b| {
        b.iter(|| {
            let meta = make_metadata(&pub_id, seq);
            let key = meta.key_expr.clone();
            backend.store(&key, &payload, meta).unwrap();
            seq += 1;
        });
    });
}

fn bench_store_write_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("store_write/batch");

    for &batch_size in &[1, 10, 100] {
        let dir = tempfile::tempdir().unwrap();
        let backend = FjallBackend::open(dir.path(), 0).unwrap();
        let pub_id = PublisherId::new();
        let payload = make_payload(256);

        let mut base_seq = 0u64;
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &batch_size| {
                b.iter(|| {
                    let batch: Vec<_> = (0..batch_size)
                        .map(|i| {
                            let seq = base_seq + i as u64;
                            let meta = make_metadata(&pub_id, seq);
                            let key = meta.key_expr.clone();
                            (key, payload.clone(), meta)
                        })
                        .collect();
                    backend.store_batch(batch).unwrap();
                    base_seq += batch_size as u64;
                });
            },
        );
    }
    group.finish();
}

fn bench_store_query_by_seq(c: &mut Criterion) {
    let mut group = c.benchmark_group("store_query/by_seq");

    for &dataset_size in &[1_000u64, 10_000, 100_000] {
        let dir = tempfile::tempdir().unwrap();
        let backend = FjallBackend::open(dir.path(), 0).unwrap();
        let pub_id = PublisherId::new();
        populate_backend(&backend, &pub_id, dataset_size);

        let mid = dataset_size / 2;
        let filters = QueryFilters {
            after_seq: Some(mid),
            before_seq: Some(mid + 100),
            publisher_id: Some(pub_id),
            limit: Some(100),
            ..Default::default()
        };

        group.bench_with_input(
            BenchmarkId::from_parameter(dataset_size),
            &filters,
            |b, filters| {
                b.iter(|| {
                    let results = backend.query(filters).unwrap();
                    std::hint::black_box(results.len());
                });
            },
        );
    }
    group.finish();
}

fn bench_store_query_by_time(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let backend = FjallBackend::open(dir.path(), 0).unwrap();
    let pub_id = PublisherId::new();
    populate_backend(&backend, &pub_id, 10_000);

    let now = chrono::Utc::now();
    let filters = QueryFilters {
        after_time: Some(now - chrono::Duration::hours(1)),
        before_time: Some(now + chrono::Duration::hours(1)),
        limit: Some(100),
        ..Default::default()
    };

    c.bench_function("store_query/by_time_10k", |b| {
        b.iter(|| {
            let results = backend.query(&filters).unwrap();
            std::hint::black_box(results.len());
        });
    });
}

fn bench_store_query_replay(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let backend = FjallBackend::open(dir.path(), 0).unwrap();
    let pub_id = PublisherId::new();
    populate_backend(&backend, &pub_id, 10_000);

    let filters = ReplayFilters {
        after_hlc: Some(HlcTimestamp {
            physical_ns: 0,
            logical: 0,
        }),
        before_hlc: None,
        limit: Some(100),
        key: None,
        key_prefix: None,
    };

    c.bench_function("store_query/replay_10k", |b| {
        b.iter(|| {
            let results = backend.query_replay(&filters).unwrap();
            std::hint::black_box(results.len());
        });
    });
}

fn bench_store_gc(c: &mut Criterion) {
    c.bench_function("store_gc/100k_50pct", |b| {
        b.iter_with_setup(
            || {
                let dir = tempfile::tempdir().unwrap();
                let backend = FjallBackend::open(dir.path(), 0).unwrap();
                let pub_id = PublisherId::new();
                populate_backend(&backend, &pub_id, 100_000);
                let cutoff = chrono::Utc::now() + chrono::Duration::hours(1);
                (dir, backend, cutoff)
            },
            |(_dir, backend, cutoff)| {
                let removed = backend.gc(cutoff).unwrap();
                std::hint::black_box(removed);
            },
        );
    });
}

fn bench_store_compact(c: &mut Criterion) {
    c.bench_function("store_compact/10k_keys_10_versions", |b| {
        b.iter_with_setup(
            || {
                let dir = tempfile::tempdir().unwrap();
                let backend = FjallBackend::open(dir.path(), 0).unwrap();
                let pub_id = PublisherId::new();
                let payload = make_payload(256);
                // 10K unique keys, each written 10 times (different seqs)
                let mut seq = 0u64;
                for key_idx in 0..10_000 {
                    for _version in 0..10 {
                        let mut meta = make_metadata(&pub_id, seq);
                        meta.key_expr = format!("bench/key/{key_idx}");
                        backend
                            .store(&meta.key_expr.clone(), &payload, meta)
                            .unwrap();
                        seq += 1;
                    }
                }
                (dir, backend)
            },
            |(_dir, backend)| {
                let stats = backend.compact().unwrap();
                std::hint::black_box(stats);
            },
        );
    });
}

fn bench_store_watermarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("store_watermarks");

    for &num_publishers in &[10, 100] {
        let dir = tempfile::tempdir().unwrap();
        let backend = FjallBackend::open(dir.path(), 0).unwrap();
        let payload = make_payload(256);

        // Create events from N different publishers
        for _ in 0..num_publishers {
            let pub_id = PublisherId::new();
            for seq in 0..100u64 {
                let meta = make_metadata(&pub_id, seq);
                let key = meta.key_expr.clone();
                backend.store(&key, &payload, meta).unwrap();
            }
        }

        group.bench_with_input(
            BenchmarkId::from_parameter(num_publishers),
            &num_publishers,
            |b, _| {
                b.iter(|| {
                    let wm = backend.publisher_watermarks();
                    std::hint::black_box(wm.len());
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_store_write_single,
    bench_store_write_batch,
    bench_store_query_by_seq,
    bench_store_query_by_time,
    bench_store_query_replay,
    bench_store_gc,
    bench_store_compact,
    bench_store_watermarks
);
criterion_main!(benches);
