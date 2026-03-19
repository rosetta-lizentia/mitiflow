//! Pluggable storage backend trait and fjall implementation.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::types::{EventId, PublisherId};

#[cfg(feature = "store")]
use crate::error::Error;

use super::query::QueryFilters;

/// Metadata associated with a stored event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMetadata {
    pub seq: u64,
    pub publisher_id: PublisherId,
    pub event_id: EventId,
    pub timestamp: DateTime<Utc>,
    pub key_expr: String,
}

/// An event as stored in the backend.
#[derive(Debug, Clone)]
pub struct StoredEvent {
    pub key: String,
    pub payload: Vec<u8>,
    pub metadata: EventMetadata,
}

/// Statistics returned by a compaction run.
#[derive(Debug, Clone, Default)]
pub struct CompactionStats {
    pub removed: usize,
    pub retained: usize,
}

/// Trait abstracting the durable storage layer.
///
/// Implementations must be `Send + Sync` to be used from async tasks.
/// The default implementation (`FjallBackend`) uses an LSM-tree via the `fjall` crate.
pub trait StorageBackend: Send + Sync {
    /// Persist an event.
    fn store(&self, key: &str, event: &[u8], metadata: EventMetadata) -> Result<()>;

    /// Query stored events matching the given filters.
    fn query(&self, filters: &QueryFilters) -> Result<Vec<StoredEvent>>;

    /// Return the highest contiguous sequence number that has been durably stored.
    fn committed_seq(&self) -> u64;

    /// Return sequence numbers below `committed_seq` that are still missing (gaps).
    fn gap_sequences(&self) -> Vec<u64>;

    /// Remove events older than the given timestamp. Returns number of events removed.
    fn gc(&self, older_than: DateTime<Utc>) -> Result<usize>;

    /// Compact the storage: keep only the latest event per key. Returns stats.
    fn compact(&self) -> Result<CompactionStats>;
}

// ---------------------------------------------------------------------------
// FjallBackend — LSM-tree backed implementation using the fjall crate.
// ---------------------------------------------------------------------------

#[cfg(feature = "store")]
mod fjall_impl {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Key encoding: 12 bytes = partition (4 BE) + seq (8 BE).
    /// Big-endian ensures natural sort order = insertion order within a partition.
    fn encode_event_key(partition: u32, seq: u64) -> [u8; 12] {
        let mut buf = [0u8; 12];
        buf[..4].copy_from_slice(&partition.to_be_bytes());
        buf[4..].copy_from_slice(&seq.to_be_bytes());
        buf
    }

    fn decode_event_key(key: &[u8]) -> Option<(u32, u64)> {
        if key.len() < 12 {
            return None;
        }
        let partition = u32::from_be_bytes(key[..4].try_into().ok()?);
        let seq = u64::from_be_bytes(key[4..12].try_into().ok()?);
        Some((partition, seq))
    }

    /// Persistent event value: metadata JSON + raw payload bytes.
    /// Layout: `[meta_len: u32 LE][metadata JSON][payload bytes]`
    fn encode_event_value(meta: &EventMetadata, payload: &[u8]) -> Vec<u8> {
        let meta_bytes = serde_json::to_vec(meta).expect("EventMetadata is always serializable");
        let mut buf = Vec::with_capacity(4 + meta_bytes.len() + payload.len());
        buf.extend_from_slice(&(meta_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&meta_bytes);
        buf.extend_from_slice(payload);
        buf
    }

    fn decode_event_value(value: &[u8]) -> Result<(EventMetadata, Vec<u8>)> {
        if value.len() < 4 {
            return Err(Error::StoreError("corrupted event value: too short".into()));
        }
        let meta_len = u32::from_le_bytes(value[..4].try_into().unwrap()) as usize;
        if value.len() < 4 + meta_len {
            return Err(Error::StoreError(
                "corrupted event value: metadata truncated".into(),
            ));
        }
        let meta: EventMetadata = serde_json::from_slice(&value[4..4 + meta_len])
            .map_err(|e| Error::StoreError(format!("invalid metadata JSON: {e}")))?;
        let payload = value[4 + meta_len..].to_vec();
        Ok((meta, payload))
    }

    /// LSM-tree backed storage using the `fjall` crate.
    ///
    /// Uses three keyspaces:
    /// - `events`   — primary event data, keyed by `(partition, seq)`
    /// - `metadata` — watermark state (`committed_seq`, `gaps`)
    /// - `keys`     — key_expr → `(partition, seq)` mapping for compaction
    pub struct FjallBackend {
        db: fjall::Database,
        events: fjall::Keyspace,
        metadata: fjall::Keyspace,
        keys: fjall::Keyspace,
        /// Default partition id for non-partitioned mode.
        partition: u32,
        /// Cached committed_seq for fast reads.
        cached_committed_seq: AtomicU64,
    }

    impl FjallBackend {
        /// Open (or create) a FjallBackend at the given filesystem path.
        pub fn open(path: impl AsRef<std::path::Path>, partition: u32) -> Result<Self> {
            let db = fjall::Database::builder(path)
                .open()
                .map_err(|e| Error::StoreError(format!("failed to open fjall database: {e}")))?;

            let events = db
                .keyspace("events", fjall::KeyspaceCreateOptions::default)
                .map_err(|e| Error::StoreError(format!("failed to open events keyspace: {e}")))?;

            let metadata = db
                .keyspace("metadata", fjall::KeyspaceCreateOptions::default)
                .map_err(|e| Error::StoreError(format!("failed to open metadata keyspace: {e}")))?;

            let keys = db
                .keyspace("keys", fjall::KeyspaceCreateOptions::default)
                .map_err(|e| Error::StoreError(format!("failed to open keys keyspace: {e}")))?;

            // Load cached committed_seq from metadata if available.
            let committed = metadata
                .get("committed_seq")
                .ok()
                .flatten()
                .and_then(|v| {
                    if v.len() == 8 {
                        Some(u64::from_le_bytes(v[..8].try_into().unwrap()))
                    } else {
                        None
                    }
                })
                .unwrap_or(0);

            Ok(Self {
                db,
                events,
                metadata,
                keys,
                partition,
                cached_committed_seq: AtomicU64::new(committed),
            })
        }

        /// Recompute the committed_seq by scanning events for the highest contiguous sequence.
        #[allow(dead_code)] // Used by EventStore::run() on startup
        fn recompute_committed_seq(&self) -> u64 {
            let prefix = self.partition.to_be_bytes();
            let mut max_contiguous: u64 = 0;
            let mut found_any = false;

            for guard in self.events.prefix(prefix) {
                let Ok(kv) = guard.into_inner() else {
                    break;
                };
                if let Some((_, seq)) = decode_event_key(&kv.0) {
                    if !found_any {
                        max_contiguous = seq;
                        found_any = true;
                    } else if seq == max_contiguous + 1 {
                        max_contiguous = seq;
                    } else if seq > max_contiguous + 1 {
                        // Gap found — stop at the last contiguous seq.
                        break;
                    }
                }
            }

            if found_any {
                // Store the computed value.
                let _ = self
                    .metadata
                    .insert("committed_seq", max_contiguous.to_le_bytes());
                self.cached_committed_seq
                    .store(max_contiguous, Ordering::Release);
            }
            max_contiguous
        }
    }

    impl StorageBackend for FjallBackend {
        fn store(&self, key: &str, event: &[u8], metadata: EventMetadata) -> Result<()> {
            let event_key = encode_event_key(self.partition, metadata.seq);
            let event_value = encode_event_value(&metadata, event);

            // Atomic batch: write event + key index + update metadata.
            let mut batch = self.db.batch();
            batch.insert(&self.events, event_key, event_value);
            batch.insert(&self.keys, key.as_bytes(), event_key);

            // Update committed_seq if this event extends the contiguous range.
            let current = self.cached_committed_seq.load(Ordering::Acquire);
            if metadata.seq == 0 || metadata.seq == current + 1 {
                let new_committed = metadata.seq;
                batch.insert(&self.metadata, "committed_seq", new_committed.to_le_bytes());
                // commit batch first, then update cache
                batch
                    .commit()
                    .map_err(|e| Error::StoreError(format!("batch commit failed: {e}")))?;
                self.cached_committed_seq
                    .store(new_committed, Ordering::Release);
            } else {
                batch
                    .commit()
                    .map_err(|e| Error::StoreError(format!("batch commit failed: {e}")))?;
            }

            Ok(())
        }

        fn query(&self, filters: &QueryFilters) -> Result<Vec<StoredEvent>> {
            let prefix = self.partition.to_be_bytes();
            let mut results = Vec::new();

            for guard in self.events.prefix(prefix) {
                let kv = guard
                    .into_inner()
                    .map_err(|e| Error::StoreError(format!("scan error: {e}")))?;

                let Some((_, seq)) = decode_event_key(&kv.0) else {
                    continue;
                };

                // Apply sequence filters.
                if let Some(after) = filters.after_seq {
                    if seq <= after {
                        continue;
                    }
                }
                if let Some(before) = filters.before_seq {
                    if seq >= before {
                        continue;
                    }
                }

                let (meta, payload) = decode_event_value(&kv.1)?;

                // Apply time filters.
                if let Some(after_time) = filters.after_time {
                    if meta.timestamp <= after_time {
                        continue;
                    }
                }
                if let Some(before_time) = filters.before_time {
                    if meta.timestamp >= before_time {
                        continue;
                    }
                }

                results.push(StoredEvent {
                    key: meta.key_expr.clone(),
                    payload,
                    metadata: meta,
                });

                if let Some(limit) = filters.limit {
                    if results.len() >= limit {
                        break;
                    }
                }
            }

            Ok(results)
        }

        fn committed_seq(&self) -> u64 {
            self.cached_committed_seq.load(Ordering::Acquire)
        }

        fn gap_sequences(&self) -> Vec<u64> {
            let prefix = self.partition.to_be_bytes();
            let mut gaps = Vec::new();
            let mut expected: Option<u64> = None;

            for guard in self.events.prefix(prefix) {
                let Ok(kv) = guard.into_inner() else {
                    break;
                };
                let Some((_, seq)) = decode_event_key(&kv.0) else {
                    continue;
                };

                match expected {
                    None => {
                        // First event — gaps before this are unknown (we start from 0).
                        for gap_seq in 0..seq {
                            gaps.push(gap_seq);
                        }
                        expected = Some(seq + 1);
                    }
                    Some(exp) => {
                        for gap_seq in exp..seq {
                            gaps.push(gap_seq);
                        }
                        expected = Some(seq + 1);
                    }
                }
            }

            gaps
        }

        fn gc(&self, older_than: DateTime<Utc>) -> Result<usize> {
            let prefix = self.partition.to_be_bytes();
            let mut removed = 0usize;
            let mut to_remove = Vec::new();

            for guard in self.events.prefix(prefix) {
                let kv = guard
                    .into_inner()
                    .map_err(|e| Error::StoreError(format!("scan error: {e}")))?;

                let (meta, _) = decode_event_value(&kv.1)?;
                if meta.timestamp < older_than {
                    to_remove.push((kv.0.to_vec(), meta.key_expr.clone()));
                }
            }

            for (event_key, key_expr) in &to_remove {
                let mut batch = self.db.batch();
                batch.remove(&self.events, event_key.as_slice());
                batch.remove(&self.keys, key_expr.as_bytes());
                batch
                    .commit()
                    .map_err(|e| Error::StoreError(format!("gc batch commit failed: {e}")))?;
                removed += 1;
            }

            Ok(removed)
        }

        fn compact(&self) -> Result<CompactionStats> {
            // Compaction: for each key_expr, keep only the event with the highest seq.
            // Scan the events keyspace to find all event_key → key_expr mappings,
            // tracking the latest seq per key_expr.
            let prefix = self.partition.to_be_bytes();
            let mut latest: std::collections::HashMap<String, (Vec<u8>, u64)> =
                std::collections::HashMap::new();
            let mut all_entries: Vec<(Vec<u8>, String, u64)> = Vec::new();

            for guard in self.events.prefix(prefix) {
                let kv = guard
                    .into_inner()
                    .map_err(|e| Error::StoreError(format!("compact scan error: {e}")))?;
                let Some((_, seq)) = decode_event_key(&kv.0) else {
                    continue;
                };
                let (meta, _) = decode_event_value(&kv.1)?;
                let event_key = kv.0.to_vec();
                let key_expr = meta.key_expr;

                all_entries.push((event_key.clone(), key_expr.clone(), seq));
                let entry = latest.entry(key_expr).or_insert((event_key.clone(), seq));
                if seq > entry.1 {
                    *entry = (event_key, seq);
                }
            }

            let mut removed = 0usize;
            let mut retained = 0usize;

            for (event_key, key_expr, _seq) in &all_entries {
                if let Some((latest_key, _)) = latest.get(key_expr) {
                    if event_key != latest_key {
                        self.events.remove(event_key.as_slice()).map_err(|e| {
                            Error::StoreError(format!("compact remove failed: {e}"))
                        })?;
                        removed += 1;
                    } else {
                        retained += 1;
                    }
                }
            }

            Ok(CompactionStats { removed, retained })
        }
    }
}

#[cfg(feature = "store")]
pub use fjall_impl::FjallBackend;
