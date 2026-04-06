//! Topic schema registry — wire-level contract validation and auto-configuration.
//!
//! A [`TopicSchema`] captures the fields that **all** publishers and subscribers
//! on a topic must agree on (codec, partition count, key format). The schema
//! can be fetched from storage agents or the orchestrator via Zenoh `get`,
//! then validated against a local [`EventBusConfig`] or used to build one
//! automatically.
//!
//! See `docs/18_topic_schema_registry.md` for the full design.

use std::fmt;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use zenoh::Session;

use crate::codec::CodecFormat;
use crate::config::EventBusConfig;
use crate::error::{Error, Result};

/// Wire-level contract for a topic.
///
/// All publishers and subscribers on the same `key_prefix` must agree on
/// `codec`, `num_partitions`, and `key_format`. Other [`EventBusConfig`]
/// fields (cache size, heartbeat, recovery mode, etc.) are local tuning
/// and are not part of the schema.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopicSchema {
    /// Topic name (matches orchestrator `TopicConfig.name`).
    pub name: String,
    /// Zenoh key prefix (e.g., `"myapp/events"`).
    pub key_prefix: String,
    /// Serialization codec. All participants must use the same codec.
    pub codec: CodecFormat,
    /// Number of partitions. Must match across all participants for
    /// consistent hash-ring routing.
    pub num_partitions: u32,
    /// Key format — whether events carry application-level keys.
    pub key_format: KeyFormat,
    /// Monotonically increasing version. Only forward-version writes
    /// are accepted, preventing accidental downgrades.
    pub schema_version: u32,
    /// When this schema was first created.
    pub created_at: DateTime<Utc>,
    /// When this schema was last modified.
    pub updated_at: DateTime<Utc>,
}

/// Key format for events on a topic.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum KeyFormat {
    /// Events have no application key: `{prefix}/p/{partition}/{seq}`
    #[default]
    Unkeyed,
    /// Events carry an exact key: `{prefix}/p/{partition}/k/{key}/{seq}`
    Keyed,
    /// Events carry a hierarchical key prefix: `{prefix}/p/{partition}/k/{prefix}/**`
    KeyPrefix,
}

impl fmt::Display for KeyFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KeyFormat::Unkeyed => write!(f, "Unkeyed"),
            KeyFormat::Keyed => write!(f, "Keyed"),
            KeyFormat::KeyPrefix => write!(f, "KeyPrefix"),
        }
    }
}

/// Controls how the publisher/subscriber interacts with the topic schema
/// registry at creation time.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum TopicSchemaMode {
    /// No schema validation (current behavior, backward-compatible).
    #[default]
    Disabled,
    /// Validate local config against registry, fail on mismatch.
    Validate,
    /// Load full config from registry (auto-configuration).
    /// Local overrides (cache_size, heartbeat, etc.) still apply.
    AutoConfig,
    /// Register schema if absent, validate if present (dev/test).
    RegisterOrValidate,
}

/// Default timeout for schema fetch queries.
const SCHEMA_FETCH_TIMEOUT: Duration = Duration::from_secs(2);

/// Timeout for the initial probe in `RegisterOrValidate` mode.
const SCHEMA_PROBE_TIMEOUT: Duration = Duration::from_secs(1);

impl TopicSchema {
    /// Construct a `TopicSchema` from an [`EventBusConfig`], using the provided
    /// topic name and key format. Sets `schema_version` to 1 and timestamps
    /// to now.
    pub fn from_config(config: &EventBusConfig, name: impl Into<String>, key_format: KeyFormat) -> Self {
        let now = Utc::now();
        Self {
            name: name.into(),
            key_prefix: config.key_prefix.clone(),
            codec: config.codec,
            num_partitions: config.num_partitions,
            key_format,
            schema_version: 1,
            created_at: now,
            updated_at: now,
        }
    }

    /// Construct a `TopicSchema` directly from individual fields (used by the orchestrator).
    pub fn new(
        key_prefix: impl Into<String>,
        name: impl Into<String>,
        num_partitions: u32,
        codec: CodecFormat,
        key_format: KeyFormat,
        schema_version: u32,
    ) -> Self {
        let now = Utc::now();
        Self {
            name: name.into(),
            key_prefix: key_prefix.into(),
            codec,
            num_partitions,
            key_format,
            schema_version,
            created_at: now,
            updated_at: now,
        }
    }

    /// Validate that a local [`EventBusConfig`] is compatible with this schema.
    ///
    /// Returns `Ok(())` if all critical fields match, or
    /// `Err(Error::TopicSchemaMismatch)` on the first mismatch found.
    pub fn validate(&self, config: &EventBusConfig) -> Result<()> {
        if config.codec != self.codec {
            return Err(Error::TopicSchemaMismatch {
                field: "codec".into(),
                local: format!("{:?}", config.codec),
                registered: format!("{:?}", self.codec),
            });
        }
        if config.num_partitions != self.num_partitions {
            return Err(Error::TopicSchemaMismatch {
                field: "num_partitions".into(),
                local: config.num_partitions.to_string(),
                registered: self.num_partitions.to_string(),
            });
        }
        Ok(())
    }

    /// Apply schema fields onto an existing [`EventBusConfig`], overwriting
    /// the wire-level fields while preserving local tuning fields.
    pub fn apply_to_config(&self, config: &mut EventBusConfig) {
        config.codec = self.codec;
        config.num_partitions = self.num_partitions;
        config.key_prefix = self.key_prefix.clone();
    }

    /// Zenoh key expression for this schema.
    pub fn schema_key(key_prefix: &str) -> String {
        format!("{key_prefix}/_schema")
    }

    /// Serialize this schema to JSON bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(Into::into)
    }

    /// Deserialize a schema from JSON bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).map_err(Into::into)
    }
}

/// Fetch a [`TopicSchema`] from the Zenoh network.
///
/// Queries `{key_prefix}/_schema` and returns the first valid reply.
/// Returns `Error::TopicSchemaNotFound` if no respondent answers within
/// the timeout.
pub async fn fetch_schema(session: &Session, key_prefix: &str) -> Result<TopicSchema> {
    fetch_schema_with_timeout(session, key_prefix, SCHEMA_FETCH_TIMEOUT).await
}

/// Fetch a [`TopicSchema`] with a custom timeout.
pub async fn fetch_schema_with_timeout(
    session: &Session,
    key_prefix: &str,
    timeout: Duration,
) -> Result<TopicSchema> {
    let key = TopicSchema::schema_key(key_prefix);
    let replies = session
        .get(&key)
        .timeout(timeout)
        .await
        .map_err(Error::Zenoh)?;

    while let Ok(reply) = replies.recv_async().await {
        match reply.into_result() {
            Ok(sample) => {
                let bytes = sample.payload().to_bytes();
                return TopicSchema::from_bytes(&bytes);
            }
            Err(_) => continue,
        }
    }

    Err(Error::TopicSchemaNotFound {
        key_prefix: key_prefix.to_string(),
    })
}

/// Register a schema on the Zenoh network via `session.put()`.
///
/// Storage agents and orchestrators subscribing to `{key_prefix}/_schema`
/// will pick this up and persist it.
pub async fn register_schema(session: &Session, schema: &TopicSchema) -> Result<()> {
    let key = TopicSchema::schema_key(&schema.key_prefix);
    let bytes = schema.to_bytes()?;
    session
        .put(&key, bytes)
        .await
        .map_err(Error::Zenoh)?;
    Ok(())
}

/// Execute the schema mode protocol for a given config.
///
/// This is called by `EventPublisher::new()` and `EventSubscriber::new()`
/// when `schema_mode` is not `Disabled`.
///
/// Returns `Some(TopicSchema)` when the caller registered a new schema
/// (i.e. `RegisterOrValidate` found no existing schema). The caller should
/// declare an ephemeral queryable on `{key_prefix}/_schema` to serve this
/// schema to peers until a storage agent picks it up.
pub async fn resolve_schema(
    session: &Session,
    config: &mut EventBusConfig,
) -> Result<Option<TopicSchema>> {
    match &config.schema_mode {
        TopicSchemaMode::Disabled => Ok(None),

        TopicSchemaMode::Validate => {
            let schema = fetch_schema(session, &config.key_prefix).await?;
            schema.validate(config)?;
            Ok(None)
        }

        TopicSchemaMode::AutoConfig => {
            let schema = fetch_schema(session, &config.key_prefix).await?;
            schema.apply_to_config(config);
            Ok(None)
        }

        TopicSchemaMode::RegisterOrValidate => {
            match fetch_schema_with_timeout(session, &config.key_prefix, SCHEMA_PROBE_TIMEOUT).await
            {
                Ok(schema) => {
                    schema.validate(config)?;
                    Ok(None)
                }
                Err(Error::TopicSchemaNotFound { .. }) => {
                    let schema = TopicSchema::from_config(
                        config,
                        &config.key_prefix,
                        KeyFormat::default(),
                    );
                    register_schema(session, &schema).await?;
                    tracing::info!(
                        key_prefix = %config.key_prefix,
                        "registered topic schema (first publisher)"
                    );
                    Ok(Some(schema))
                }
                Err(e) => Err(e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_schema() -> TopicSchema {
        let now = Utc::now();
        TopicSchema {
            name: "orders".into(),
            key_prefix: "myapp/events".into(),
            codec: CodecFormat::Postcard,
            num_partitions: 16,
            key_format: KeyFormat::Keyed,
            schema_version: 1,
            created_at: now,
            updated_at: now,
        }
    }

    fn sample_config(codec: CodecFormat, num_partitions: u32) -> EventBusConfig {
        EventBusConfig::builder("myapp/events")
            .codec(codec)
            .num_partitions(num_partitions)
            .build()
            .unwrap()
    }

    #[test]
    fn validate_matching_config() {
        let schema = sample_schema();
        let config = sample_config(CodecFormat::Postcard, 16);
        assert!(schema.validate(&config).is_ok());
    }

    #[test]
    fn validate_codec_mismatch() {
        let schema = sample_schema();
        let config = sample_config(CodecFormat::Json, 16);
        let err = schema.validate(&config).unwrap_err();
        match err {
            Error::TopicSchemaMismatch { ref field, .. } => assert_eq!(field, "codec"),
            _ => panic!("expected TopicSchemaMismatch, got {err:?}"),
        }
    }

    #[test]
    fn validate_partition_mismatch() {
        let schema = sample_schema();
        let config = sample_config(CodecFormat::Postcard, 32);
        let err = schema.validate(&config).unwrap_err();
        match err {
            Error::TopicSchemaMismatch { ref field, .. } => assert_eq!(field, "num_partitions"),
            _ => panic!("expected TopicSchemaMismatch, got {err:?}"),
        }
    }

    #[test]
    fn from_config_roundtrip() {
        let config = sample_config(CodecFormat::MsgPack, 8);
        let schema = TopicSchema::from_config(&config, "test-topic", KeyFormat::Unkeyed);
        assert_eq!(schema.name, "test-topic");
        assert_eq!(schema.key_prefix, "myapp/events");
        assert_eq!(schema.codec, CodecFormat::MsgPack);
        assert_eq!(schema.num_partitions, 8);
        assert_eq!(schema.key_format, KeyFormat::Unkeyed);
        assert_eq!(schema.schema_version, 1);
    }

    #[test]
    fn apply_to_config_overwrites_wire_fields() {
        let schema = sample_schema();
        let mut config = sample_config(CodecFormat::Json, 64);
        schema.apply_to_config(&mut config);
        assert_eq!(config.codec, CodecFormat::Postcard);
        assert_eq!(config.num_partitions, 16);
        assert_eq!(config.key_prefix, "myapp/events");
    }

    #[test]
    fn schema_serialization_roundtrip() {
        let schema = sample_schema();
        let bytes = schema.to_bytes().unwrap();
        let decoded = TopicSchema::from_bytes(&bytes).unwrap();
        assert_eq!(schema, decoded);
    }

    #[test]
    fn schema_key_format() {
        assert_eq!(TopicSchema::schema_key("app/events"), "app/events/_schema");
    }

    #[test]
    fn key_format_display() {
        assert_eq!(KeyFormat::Unkeyed.to_string(), "Unkeyed");
        assert_eq!(KeyFormat::Keyed.to_string(), "Keyed");
        assert_eq!(KeyFormat::KeyPrefix.to_string(), "KeyPrefix");
    }

    #[test]
    fn key_format_default_is_unkeyed() {
        assert_eq!(KeyFormat::default(), KeyFormat::Unkeyed);
    }

    #[test]
    fn schema_mode_default_is_disabled() {
        assert_eq!(TopicSchemaMode::default(), TopicSchemaMode::Disabled);
    }
}
