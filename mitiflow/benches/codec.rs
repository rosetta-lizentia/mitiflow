use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mitiflow::codec::CodecFormat;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
struct TestPayload {
    field_a: String,
    field_b: u64,
    field_c: Vec<u8>,
}

fn make_payload(size: usize) -> TestPayload {
    TestPayload {
        field_a: "benchmark-event".to_string(),
        field_b: 42,
        field_c: vec![0xAB; size],
    }
}

fn bench_codec_encode(c: &mut Criterion) {
    let codecs = [
        ("json", CodecFormat::Json),
        ("msgpack", CodecFormat::MsgPack),
        ("postcard", CodecFormat::Postcard),
    ];
    let sizes = [64, 1024, 65536];

    let mut group = c.benchmark_group("codec_encode");
    for (codec_name, codec) in &codecs {
        for &size in &sizes {
            let payload = make_payload(size);
            group.bench_with_input(
                BenchmarkId::new(*codec_name, size),
                &payload,
                |b, payload| {
                    b.iter(|| codec.encode(payload).unwrap());
                },
            );
        }
    }
    group.finish();
}

fn bench_codec_decode(c: &mut Criterion) {
    let codecs = [
        ("json", CodecFormat::Json),
        ("msgpack", CodecFormat::MsgPack),
        ("postcard", CodecFormat::Postcard),
    ];
    let sizes = [64, 1024, 65536];

    let mut group = c.benchmark_group("codec_decode");
    for (codec_name, codec) in &codecs {
        for &size in &sizes {
            let payload = make_payload(size);
            let encoded = codec.encode(&payload).unwrap();
            group.bench_with_input(
                BenchmarkId::new(*codec_name, size),
                &encoded,
                |b, encoded| {
                    b.iter(|| codec.decode::<TestPayload>(encoded).unwrap());
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, bench_codec_encode, bench_codec_decode);
criterion_main!(benches);
