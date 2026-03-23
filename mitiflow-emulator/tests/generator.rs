//! Tests for payload generation.

use mitiflow_emulator::config::{GeneratorType, PayloadConfig, SchemaFieldDef, SchemaFieldType};
use mitiflow_emulator::generator::PayloadGenerator;
use std::collections::HashMap;

fn make_payload_config(gen_type: GeneratorType) -> PayloadConfig {
    PayloadConfig {
        generator: gen_type,
        size_bytes: 256,
        schema: HashMap::new(),
        content: None,
        prefix: None,
    }
}

#[test]
fn random_json_produces_valid_json() {
    let config = make_payload_config(GeneratorType::RandomJson);
    let mut generator = PayloadGenerator::from_config(&config);
    let payload = generator.generate();
    let val: serde_json::Value = serde_json::from_slice(&payload).unwrap();
    assert!(val.get("data").is_some());
}

#[test]
fn random_json_approximate_size() {
    let mut config = make_payload_config(GeneratorType::RandomJson);
    config.size_bytes = 1024;
    let mut generator = PayloadGenerator::from_config(&config);
    let payload = generator.generate();
    // Allow some overhead for JSON framing.
    assert!(
        payload.len() >= 1000,
        "payload too small: {}",
        payload.len()
    );
}

#[test]
fn fixed_generator_constant_output() {
    let config = PayloadConfig {
        generator: GeneratorType::Fixed,
        size_bytes: 64,
        schema: HashMap::new(),
        content: None,
        prefix: None,
    };
    let mut generator = PayloadGenerator::from_config(&config);
    let p1 = generator.generate();
    let p2 = generator.generate();
    assert_eq!(p1, p2, "fixed generator should produce identical payloads");
    assert_eq!(p1.len(), 64);
    assert!(p1.iter().all(|&b| b == 0x42));
}

#[test]
fn fixed_generator_with_base64_content() {
    use base64::Engine;
    let data = b"hello world test payload";
    let b64 = base64::engine::general_purpose::STANDARD.encode(data);
    let config = PayloadConfig {
        generator: GeneratorType::Fixed,
        size_bytes: 32,
        schema: HashMap::new(),
        content: Some(b64),
        prefix: None,
    };
    let mut generator = PayloadGenerator::from_config(&config);
    let payload = generator.generate();
    assert_eq!(payload.len(), 32);
    assert_eq!(&payload[..data.len()], data);
}

#[test]
fn counter_increments() {
    let config = PayloadConfig {
        generator: GeneratorType::Counter,
        size_bytes: 256,
        schema: HashMap::new(),
        content: None,
        prefix: Some("test".into()),
    };
    let mut generator = PayloadGenerator::from_config(&config);

    for i in 0u64..5 {
        let payload = generator.generate();
        let val: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(val["seq"], i, "seq should increment");
        assert_eq!(val["prefix"], "test");
    }
}

#[test]
fn counter_default_prefix() {
    let config = make_payload_config(GeneratorType::Counter);
    let mut generator = PayloadGenerator::from_config(&config);
    let payload = generator.generate();
    let val: serde_json::Value = serde_json::from_slice(&payload).unwrap();
    assert_eq!(val["prefix"], "");
    assert_eq!(val["seq"], 0);
}

#[test]
fn schema_float_field() {
    let mut schema = HashMap::new();
    schema.insert(
        "temperature".into(),
        SchemaFieldDef {
            field_type: SchemaFieldType::Float,
            min: Some(20.0),
            max: Some(40.0),
            pattern: None,
            probability: None,
            values: None,
        },
    );
    let config = PayloadConfig {
        generator: GeneratorType::Schema,
        size_bytes: 256,
        schema,
        content: None,
        prefix: None,
    };
    let mut generator = PayloadGenerator::from_config(&config);

    for _ in 0..100 {
        let payload = generator.generate();
        let val: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        let temp = val["temperature"].as_f64().unwrap();
        assert!(
            (20.0..=40.0).contains(&temp),
            "temperature out of range: {}",
            temp
        );
    }
}

#[test]
fn schema_int_field() {
    let mut schema = HashMap::new();
    schema.insert(
        "count".into(),
        SchemaFieldDef {
            field_type: SchemaFieldType::Int,
            min: Some(0.0),
            max: Some(10.0),
            pattern: None,
            probability: None,
            values: None,
        },
    );
    let config = PayloadConfig {
        generator: GeneratorType::Schema,
        size_bytes: 256,
        schema,
        content: None,
        prefix: None,
    };
    let mut generator = PayloadGenerator::from_config(&config);

    for _ in 0..100 {
        let payload = generator.generate();
        let val: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        let count = val["count"].as_i64().unwrap();
        assert!((0..=10).contains(&count), "count out of range: {}", count);
    }
}

#[test]
fn schema_uuid_field() {
    let mut schema = HashMap::new();
    schema.insert(
        "id".into(),
        SchemaFieldDef {
            field_type: SchemaFieldType::Uuid,
            min: None,
            max: None,
            pattern: None,
            probability: None,
            values: None,
        },
    );
    let config = PayloadConfig {
        generator: GeneratorType::Schema,
        size_bytes: 256,
        schema,
        content: None,
        prefix: None,
    };
    let mut generator = PayloadGenerator::from_config(&config);
    let payload = generator.generate();
    let val: serde_json::Value = serde_json::from_slice(&payload).unwrap();
    let id = val["id"].as_str().unwrap();
    // UUID should be 36 chars (8-4-4-4-12).
    assert_eq!(id.len(), 36, "UUID wrong length: {}", id);
    assert!(uuid::Uuid::parse_str(id).is_ok(), "invalid UUID: {}", id);
}

#[test]
fn schema_bool_field() {
    let mut schema = HashMap::new();
    schema.insert(
        "flag".into(),
        SchemaFieldDef {
            field_type: SchemaFieldType::Bool,
            min: None,
            max: None,
            pattern: None,
            probability: Some(1.0), // always true
            values: None,
        },
    );
    let config = PayloadConfig {
        generator: GeneratorType::Schema,
        size_bytes: 256,
        schema,
        content: None,
        prefix: None,
    };
    let mut generator = PayloadGenerator::from_config(&config);
    for _ in 0..10 {
        let payload = generator.generate();
        let val: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert!(val["flag"].as_bool().unwrap());
    }
}

#[test]
fn schema_enum_field() {
    let mut schema = HashMap::new();
    schema.insert(
        "region".into(),
        SchemaFieldDef {
            field_type: SchemaFieldType::Enum,
            min: None,
            max: None,
            pattern: None,
            probability: None,
            values: Some(vec!["us-east".into(), "us-west".into(), "eu".into()]),
        },
    );
    let config = PayloadConfig {
        generator: GeneratorType::Schema,
        size_bytes: 256,
        schema,
        content: None,
        prefix: None,
    };
    let mut generator = PayloadGenerator::from_config(&config);
    let expected = ["us-east", "us-west", "eu"];
    for _ in 0..50 {
        let payload = generator.generate();
        let val: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        let region = val["region"].as_str().unwrap();
        assert!(expected.contains(&region), "unexpected region: {}", region);
    }
}

#[test]
fn schema_string_pattern_expansion() {
    let mut schema = HashMap::new();
    schema.insert(
        "rack".into(),
        SchemaFieldDef {
            field_type: SchemaFieldType::String,
            min: None,
            max: None,
            pattern: Some("rack-{0-99}".into()),
            probability: None,
            values: None,
        },
    );
    let config = PayloadConfig {
        generator: GeneratorType::Schema,
        size_bytes: 256,
        schema,
        content: None,
        prefix: None,
    };
    let mut generator = PayloadGenerator::from_config(&config);
    for _ in 0..100 {
        let payload = generator.generate();
        let val: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        let rack = val["rack"].as_str().unwrap();
        assert!(rack.starts_with("rack-"), "bad prefix: {}", rack);
        let num: i64 = rack.strip_prefix("rack-").unwrap().parse().unwrap();
        assert!((0..=99).contains(&num), "rack number out of range: {}", num);
    }
}

#[test]
fn schema_datetime_field() {
    let mut schema = HashMap::new();
    schema.insert(
        "ts".into(),
        SchemaFieldDef {
            field_type: SchemaFieldType::Datetime,
            min: None,
            max: None,
            pattern: None,
            probability: None,
            values: None,
        },
    );
    let config = PayloadConfig {
        generator: GeneratorType::Schema,
        size_bytes: 256,
        schema,
        content: None,
        prefix: None,
    };
    let mut generator = PayloadGenerator::from_config(&config);
    let payload = generator.generate();
    let val: serde_json::Value = serde_json::from_slice(&payload).unwrap();
    let ts = val["ts"].as_str().unwrap();
    // Should be a valid RFC 3339 datetime.
    assert!(
        ts.contains('T') && ts.contains('+') || ts.contains('Z'),
        "bad datetime: {}",
        ts
    );
}

#[test]
fn schema_multiple_fields() {
    let mut schema = HashMap::new();
    schema.insert(
        "temp".into(),
        SchemaFieldDef {
            field_type: SchemaFieldType::Float,
            min: Some(0.0),
            max: Some(100.0),
            pattern: None,
            probability: None,
            values: None,
        },
    );
    schema.insert(
        "id".into(),
        SchemaFieldDef {
            field_type: SchemaFieldType::Uuid,
            min: None,
            max: None,
            pattern: None,
            probability: None,
            values: None,
        },
    );
    schema.insert(
        "active".into(),
        SchemaFieldDef {
            field_type: SchemaFieldType::Bool,
            min: None,
            max: None,
            pattern: None,
            probability: Some(0.5),
            values: None,
        },
    );
    let config = PayloadConfig {
        generator: GeneratorType::Schema,
        size_bytes: 256,
        schema,
        content: None,
        prefix: None,
    };
    let mut generator = PayloadGenerator::from_config(&config);
    let payload = generator.generate();
    let val: serde_json::Value = serde_json::from_slice(&payload).unwrap();
    // All three fields should be present.
    assert!(val.get("temp").is_some());
    assert!(val.get("id").is_some());
    assert!(val.get("active").is_some());
}
