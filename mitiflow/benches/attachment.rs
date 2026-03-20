use criterion::{Criterion, criterion_group, criterion_main};
use mitiflow::attachment::{NO_URGENCY, decode_metadata, encode_metadata, extract_partition};
use mitiflow::types::{EventId, PublisherId};

fn bench_attachment_encode(c: &mut Criterion) {
    let pub_id = PublisherId::new();
    let event_id = EventId::new();
    let ts = chrono::Utc::now();

    c.bench_function("attachment_encode", |b| {
        b.iter(|| encode_metadata(&pub_id, 42, &event_id, &ts, NO_URGENCY));
    });
}

fn bench_attachment_decode(c: &mut Criterion) {
    let pub_id = PublisherId::new();
    let event_id = EventId::new();
    let ts = chrono::Utc::now();
    let encoded = encode_metadata(&pub_id, 42, &event_id, &ts, NO_URGENCY);

    c.bench_function("attachment_decode", |b| {
        b.iter(|| decode_metadata(&encoded).unwrap());
    });
}

fn bench_extract_partition(c: &mut Criterion) {
    let keys = [
        "mitiflow/p/0/events",
        "mitiflow/p/42/orders/123",
        "mitiflow/p/63/telemetry",
        "mitiflow/events/no-partition",
    ];

    c.bench_function("extract_partition", |b| {
        b.iter(|| {
            for key in &keys {
                std::hint::black_box(extract_partition(key));
            }
        });
    });
}

criterion_group!(
    benches,
    bench_attachment_encode,
    bench_attachment_decode,
    bench_extract_partition
);
criterion_main!(benches);
