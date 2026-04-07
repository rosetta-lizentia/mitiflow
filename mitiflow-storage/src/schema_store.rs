//! Persistent schema store backed by a fjall keyspace.
//!
//! Shared across all topics on a storage agent node. Keyed by topic
//! `key_prefix`, stores [`TopicSchema`] JSON blobs with a monotonic
//! version guard to prevent accidental downgrades.

use std::path::Path;

use mitiflow::TopicSchema;
use tracing::warn;

use crate::error::{AgentError, AgentResult};

/// Persistent schema store backed by a fjall `"schemas"` keyspace.
///
/// One instance per storage agent node (shared across all topics).
pub struct SchemaStore {
    #[allow(dead_code)]
    db: fjall::Database,
    schemas: fjall::Keyspace,
}

impl SchemaStore {
    /// Open or create a schema store at the given directory.
    pub fn open(dir: impl AsRef<Path>) -> AgentResult<Self> {
        let db = fjall::Database::builder(dir)
            .open()
            .map_err(|e| AgentError::Store(e.to_string()))?;
        let schemas = db
            .keyspace("schemas", fjall::KeyspaceCreateOptions::default)
            .map_err(|e| AgentError::Store(e.to_string()))?;
        Ok(Self { db, schemas })
    }

    /// Retrieve a schema by key prefix.
    pub fn get(&self, key_prefix: &str) -> AgentResult<Option<TopicSchema>> {
        match self
            .schemas
            .get(key_prefix)
            .map_err(|e| AgentError::Store(e.to_string()))?
        {
            Some(bytes) => {
                let schema = TopicSchema::from_bytes(&bytes)
                    .map_err(|e| AgentError::Store(e.to_string()))?;
                Ok(Some(schema))
            }
            None => Ok(None),
        }
    }

    /// Store a schema unconditionally.
    pub fn put(&self, schema: &TopicSchema) -> AgentResult<()> {
        let bytes = schema
            .to_bytes()
            .map_err(|e| AgentError::Store(e.to_string()))?;
        self.schemas
            .insert(&schema.key_prefix, bytes)
            .map_err(|e| AgentError::Store(e.to_string()))?;
        Ok(())
    }

    /// Store a schema only if its version is strictly greater than the
    /// currently persisted version. Returns `true` if the write was accepted.
    pub fn put_if_newer(&self, schema: &TopicSchema) -> AgentResult<bool> {
        if let Some(existing) = self.get(&schema.key_prefix)?
            && schema.schema_version <= existing.schema_version
        {
            warn!(
                key_prefix = %schema.key_prefix,
                incoming = schema.schema_version,
                current = existing.schema_version,
                "rejected stale schema version"
            );
            return Ok(false);
        }
        self.put(schema)?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use mitiflow::{CodecFormat, KeyFormat};

    use super::*;

    fn sample_schema(key_prefix: &str, version: u32) -> TopicSchema {
        let now = Utc::now();
        TopicSchema {
            name: "orders".into(),
            key_prefix: key_prefix.into(),
            codec: CodecFormat::Postcard,
            num_partitions: 16,
            key_format: KeyFormat::Keyed,
            schema_version: version,
            created_at: now,
            updated_at: now,
        }
    }

    #[test]
    fn schema_store_put_get() {
        let dir = tempfile::tempdir().unwrap();
        let store = SchemaStore::open(dir.path()).unwrap();

        let schema = sample_schema("app/events", 1);
        store.put(&schema).unwrap();

        let got = store.get("app/events").unwrap().unwrap();
        assert_eq!(got.name, "orders");
        assert_eq!(got.codec, CodecFormat::Postcard);
        assert_eq!(got.num_partitions, 16);
        assert_eq!(got.schema_version, 1);
    }

    #[test]
    fn schema_store_get_missing() {
        let dir = tempfile::tempdir().unwrap();
        let store = SchemaStore::open(dir.path()).unwrap();
        assert!(store.get("nonexistent").unwrap().is_none());
    }

    #[test]
    fn schema_store_put_if_newer_accepts() {
        let dir = tempfile::tempdir().unwrap();
        let store = SchemaStore::open(dir.path()).unwrap();

        store.put(&sample_schema("app/events", 1)).unwrap();
        let accepted = store.put_if_newer(&sample_schema("app/events", 2)).unwrap();
        assert!(accepted);

        let got = store.get("app/events").unwrap().unwrap();
        assert_eq!(got.schema_version, 2);
    }

    #[test]
    fn schema_store_put_if_newer_rejects_stale() {
        let dir = tempfile::tempdir().unwrap();
        let store = SchemaStore::open(dir.path()).unwrap();

        store.put(&sample_schema("app/events", 3)).unwrap();
        let accepted = store.put_if_newer(&sample_schema("app/events", 2)).unwrap();
        assert!(!accepted);

        let got = store.get("app/events").unwrap().unwrap();
        assert_eq!(got.schema_version, 3); // unchanged
    }

    #[test]
    fn schema_store_put_if_newer_rejects_equal() {
        let dir = tempfile::tempdir().unwrap();
        let store = SchemaStore::open(dir.path()).unwrap();

        store.put(&sample_schema("app/events", 1)).unwrap();
        let accepted = store.put_if_newer(&sample_schema("app/events", 1)).unwrap();
        assert!(!accepted);
    }

    #[test]
    fn schema_store_persistence() {
        let dir = tempfile::tempdir().unwrap();

        // Write and drop
        {
            let store = SchemaStore::open(dir.path()).unwrap();
            store.put(&sample_schema("app/events", 5)).unwrap();
        }

        // Reopen and verify
        let store = SchemaStore::open(dir.path()).unwrap();
        let got = store.get("app/events").unwrap().unwrap();
        assert_eq!(got.schema_version, 5);
        assert_eq!(got.name, "orders");
    }
}
