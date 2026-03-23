use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mitiflow::codec::CodecFormat;
use mitiflow::event::{Event, RawEvent};
use mitiflow::types::{EventId, PublisherId};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
struct TestPayload {
    field_a: String,
    field_b: u64,
    data: Vec<u8>,
}

fn make_payload(size: usize) -> TestPayload {
    TestPayload {
        field_a: "benchmark-event".to_string(),
        field_b: 42,
        data: vec![0xAB; size],
    }
}

fn bench_event_new(c: &mut Criterion) {
    let mut group = c.benchmark_group("event_new");

    for &size in &[64, 1024, 65536] {
        let payload = make_payload(size);
        group.bench_with_input(BenchmarkId::from_parameter(size), &payload, |b, payload| {
            b.iter(|| std::hint::black_box(Event::new(payload.clone())));
        });
    }
    group.finish();
}

fn bench_event_to_bytes(c: &mut Criterion) {
    let mut group = c.benchmark_group("event_to_bytes");

    for &size in &[64, 1024, 65536] {
        let event = Event::new(make_payload(size));
        group.bench_with_input(BenchmarkId::from_parameter(size), &event, |b, event| {
            b.iter(|| event.to_bytes().unwrap());
        });
    }
    group.finish();
}

fn bench_event_from_bytes(c: &mut Criterion) {
    let mut group = c.benchmark_group("event_from_bytes");

    for &size in &[64, 1024, 65536] {
        let event = Event::new(make_payload(size));
        let bytes = event.to_bytes().unwrap();
        group.bench_with_input(BenchmarkId::from_parameter(size), &bytes, |b, bytes| {
            b.iter(|| Event::<TestPayload>::from_bytes(bytes).unwrap());
        });
    }
    group.finish();
}

fn bench_raw_event_deserialize(c: &mut Criterion) {
    let codecs = [
        ("json", CodecFormat::Json),
        ("msgpack", CodecFormat::MsgPack),
        ("postcard", CodecFormat::Postcard),
    ];

    let mut group = c.benchmark_group("raw_event_deserialize");

    for (codec_name, codec) in &codecs {
        let payload = make_payload(256);
        let encoded = codec.encode(&payload).unwrap();

        let raw = RawEvent {
            id: EventId::new(),
            seq: 42,
            publisher_id: PublisherId::new(),
            key_expr: "bench/test".to_string(),
            payload: encoded,
            timestamp: chrono::Utc::now(),
        };

        group.bench_with_input(BenchmarkId::new(*codec_name, 256), &raw, |b, raw| {
            b.iter(|| raw.deserialize_with::<TestPayload>(*codec).unwrap());
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_event_new,
    bench_event_to_bytes,
    bench_event_from_bytes,
    bench_raw_event_deserialize
);
criterion_main!(benches);
