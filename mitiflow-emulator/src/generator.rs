//! Payload generation strategies for emulator producers.

use rand::RngExt;
use serde_json::json;

use crate::config::{GeneratorType, PayloadConfig, SchemaFieldDef, SchemaFieldType};

/// Payload generator that produces event data.
#[derive(Clone)]
pub enum PayloadGenerator {
    /// Random JSON objects of approximately `size_bytes`.
    RandomJson { size_bytes: usize },
    /// Fixed-size payload (constant content).
    Fixed { content: Vec<u8> },
    /// Sequential counter: `{"seq": N, "prefix": "..."}`.
    Counter { prefix: String, seq: u64 },
    /// Schema-driven structured JSON.
    Schema {
        fields: Vec<(String, FieldGenerator)>,
    },
}

/// Per-field generator for schema mode.
#[derive(Debug, Clone)]
pub enum FieldGenerator {
    Float { min: f64, max: f64 },
    Int { min: i64, max: i64 },
    Uuid,
    DateTime,
    Bool { probability: f64 },
    Enum { values: Vec<String> },
    StringPattern { pattern: String },
}

impl PayloadGenerator {
    /// Create a generator from YAML config.
    pub fn from_config(config: &PayloadConfig) -> Self {
        match config.generator {
            GeneratorType::RandomJson => Self::RandomJson {
                size_bytes: config.size_bytes,
            },
            GeneratorType::Fixed => {
                let content = if let Some(ref b64) = config.content {
                    use base64::Engine;
                    let decoded = base64::engine::general_purpose::STANDARD
                        .decode(b64)
                        .unwrap_or_default();
                    let mut buf = decoded;
                    buf.resize(config.size_bytes, 0);
                    buf
                } else {
                    vec![0x42; config.size_bytes]
                };
                Self::Fixed { content }
            }
            GeneratorType::Counter => Self::Counter {
                prefix: config.prefix.clone().unwrap_or_default(),
                seq: 0,
            },
            GeneratorType::Schema => {
                let fields = config
                    .schema
                    .iter()
                    .map(|(name, def)| (name.clone(), FieldGenerator::from_def(def)))
                    .collect();
                Self::Schema { fields }
            }
        }
    }

    /// Generate one payload as bytes.
    pub fn generate(&mut self) -> Vec<u8> {
        match self {
            Self::RandomJson { size_bytes } => {
                let mut rng = rand::rng();
                let padding_len = size_bytes.saturating_sub(20);
                let padding: String = (0..padding_len)
                    .map(|_| {
                        let idx = rng.random_range(0..62);
                        match idx {
                            0..=9 => (b'0' + idx) as char,
                            10..=35 => (b'a' + idx - 10) as char,
                            _ => (b'A' + idx - 36) as char,
                        }
                    })
                    .collect();
                let obj = json!({ "data": padding });
                serde_json::to_vec(&obj).unwrap()
            }
            Self::Fixed { content } => content.clone(),
            Self::Counter { prefix, seq } => {
                let val = *seq;
                *seq += 1;
                let obj = json!({ "seq": val, "prefix": *prefix });
                serde_json::to_vec(&obj).unwrap()
            }
            Self::Schema { fields } => {
                let mut rng = rand::rng();
                let mut map = serde_json::Map::new();
                for (name, field_gen) in fields.iter() {
                    map.insert(name.clone(), field_gen.generate(&mut rng));
                }
                serde_json::to_vec(&serde_json::Value::Object(map)).unwrap()
            }
        }
    }
}

impl FieldGenerator {
    fn from_def(def: &SchemaFieldDef) -> Self {
        match def.field_type {
            SchemaFieldType::Float => Self::Float {
                min: def.min.unwrap_or(0.0),
                max: def.max.unwrap_or(100.0),
            },
            SchemaFieldType::Int => Self::Int {
                min: def.min.unwrap_or(0.0) as i64,
                max: def.max.unwrap_or(100.0) as i64,
            },
            SchemaFieldType::Uuid => Self::Uuid,
            SchemaFieldType::Datetime => Self::DateTime,
            SchemaFieldType::Bool => Self::Bool {
                probability: def.probability.unwrap_or(0.5),
            },
            SchemaFieldType::Enum => Self::Enum {
                values: def.values.clone().unwrap_or_default(),
            },
            SchemaFieldType::String => Self::StringPattern {
                pattern: def.pattern.clone().unwrap_or_default(),
            },
        }
    }

    fn generate(&self, rng: &mut dyn rand::rand_core::Rng) -> serde_json::Value {
        match self {
            Self::Float { min, max } => {
                let val = rng.random_range(*min..=*max);
                serde_json::Value::from(val)
            }
            Self::Int { min, max } => {
                let val = rng.random_range(*min..=*max);
                serde_json::Value::from(val)
            }
            Self::Uuid => serde_json::Value::String(uuid::Uuid::now_v7().to_string()),
            Self::DateTime => serde_json::Value::String(chrono::Utc::now().to_rfc3339()),
            Self::Bool { probability } => serde_json::Value::Bool(rng.random_bool(*probability)),
            Self::Enum { values } => {
                if values.is_empty() {
                    serde_json::Value::Null
                } else {
                    let idx = rng.random_range(0..values.len());
                    serde_json::Value::String(values[idx].clone())
                }
            }
            Self::StringPattern { pattern } => {
                let expanded = expand_pattern(pattern, rng);
                serde_json::Value::String(expanded)
            }
        }
    }
}

/// Expand `{N-M}` range patterns in a string.
/// E.g. `"rack-{0-99}"` → `"rack-42"`.
fn expand_pattern(pattern: &str, rng: &mut dyn rand::Rng) -> String {
    let mut result = String::new();
    let mut chars = pattern.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '{' {
            // Try to parse {N-M}.
            let mut range_str = String::new();
            let mut found_close = false;
            for inner in chars.by_ref() {
                if inner == '}' {
                    found_close = true;
                    break;
                }
                range_str.push(inner);
            }
            if found_close
                && let Some((min_s, max_s)) = range_str.split_once('-')
                && let (Ok(min), Ok(max)) = (min_s.parse::<i64>(), max_s.parse::<i64>())
            {
                let val = rng.random_range(min..=max);
                result.push_str(&val.to_string());
                continue;
            }
            // Fallback: not a valid range.
            result.push('{');
            result.push_str(&range_str);
            if found_close {
                result.push('}');
            }
        } else {
            result.push(ch);
        }
    }
    result
}
