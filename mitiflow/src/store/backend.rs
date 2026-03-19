//! Pluggable storage backend trait and fjall implementation.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::types::{EventId, PublisherId};

#[cfg(feature = "store")]
use crate::error::Error;

use super::query::{QueryFilters, ReplayFilters};
use super::watermark::PublisherWatermark;

/// Metadata associated with a stored event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMetadata {
    pub seq: u64,
    pub publisher_id: PublisherId,
    pub event_id: EventId,
    pub timestamp: DateTime<Utc>,
    pub key_expr: String,
    /// Zenoh HLC timestamp for deterministic cross-replica replay ordering.
    /// `None` if the source sample had no HLC timestamp (fallback to `timestamp`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hlc_timestamp: Option<HlcTimestamp>,
}

/// Serializable representation of a Zenoh Hybrid Logical Clock timestamp.
///
/// Used as the sort key for the replay index:
/// `(physical_ns, logical, publisher_id, seq)` provides a deterministic
/// total order that is the same on every replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct HlcTimestamp {
    /// Physical time in nanoseconds since epoch.
    pub physical_ns: u64,
    /// Logical counter for events at the same physical time.
    pub logical: u32,
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
    /// The partition this backend is responsible for.
    fn partition(&self) -> u32;

    /// Persist an event.
    fn store(&self, key: &str, event: &[u8], metadata: EventMetadata) -> Result<()>;

    /// Persist a batch of events atomically where supported.
    ///
    /// The default implementation calls [`store`](StorageBackend::store) in a
    /// loop; backends that support atomic batch writes (e.g. fjall) should
    /// override this for better throughput.
    fn store_batch(&self, events: Vec<(String, Vec<u8>, EventMetadata)>) -> Result<()> {
        for (key, payload, metadata) in events {
            self.store(&key, &payload, metadata)?;
        }
        Ok(())
    }

    /// Query stored events matching the given filters.
    fn query(&self, filters: &QueryFilters) -> Result<Vec<StoredEvent>>;

    /// Query stored events in deterministic HLC replay order.
    ///
    /// Returns events sorted by `(hlc_timestamp, publisher_id, seq)`, which
    /// produces the same ordering on any replica. Events without HLC timestamps
    /// are ordered by `(timestamp, publisher_id, seq)` as a fallback.
    fn query_replay(&self, filters: &ReplayFilters) -> Result<Vec<StoredEvent>>;

    /// Return per-publisher durability progress (committed_seq + gaps).
    fn publisher_watermarks(&self) -> HashMap<PublisherId, PublisherWatermark>;

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
    use std::collections::BTreeSet;

    /// Key encoding: 24 bytes = publisher_id (16) + seq (8 BE).
    /// Big-endian seq ensures natural sort order within each publisher.
    /// Partition is implicit — each `FjallBackend` instance owns one partition.
    fn encode_event_key(publisher_id: &PublisherId, seq: u64) -> [u8; 24] {
        let mut buf = [0u8; 24];
        buf[..16].copy_from_slice(&publisher_id.to_bytes());
        buf[16..].copy_from_slice(&seq.to_be_bytes());
        buf
    }

    fn decode_event_key(key: &[u8]) -> Option<(PublisherId, u64)> {
        if key.len() < 24 {
            return None;
        }
        let pub_bytes: [u8; 16] = key[..16].try_into().ok()?;
        let publisher_id = PublisherId::from_bytes(pub_bytes);
        let seq = u64::from_be_bytes(key[16..24].try_into().ok()?);
        Some((publisher_id, seq))
    }

    /// Replay index key encoding: 36 bytes.
    /// `[hlc_physical_ns: 8 BE][hlc_logical: 4 BE][publisher_id: 16][seq: 8 BE]`
    ///
    /// This key order ensures deterministic replay: events are sorted by
    /// approximately-physical time, then by publisher, then by sequence.
    /// Because the HLC timestamp is intrinsic to the event (assigned by the
    /// publisher's Zenoh session), this ordering is identical on every replica.
    fn encode_replay_key(hlc: &HlcTimestamp, publisher_id: &PublisherId, seq: u64) -> [u8; 36] {
        let mut buf = [0u8; 36];
        buf[..8].copy_from_slice(&hlc.physical_ns.to_be_bytes());
        buf[8..12].copy_from_slice(&hlc.logical.to_be_bytes());
        buf[12..28].copy_from_slice(&publisher_id.to_bytes());
        buf[28..36].copy_from_slice(&seq.to_be_bytes());
        buf
    }

    fn decode_replay_key(key: &[u8]) -> Option<(HlcTimestamp, PublisherId, u64)> {
        if key.len() < 36 {
            return None;
        }
        let physical_ns = u64::from_be_bytes(key[..8].try_into().ok()?);
        let logical = u32::from_be_bytes(key[8..12].try_into().ok()?);
        let pub_bytes: [u8; 16] = key[12..28].try_into().ok()?;
        let publisher_id = PublisherId::from_bytes(pub_bytes);
        let seq = u64::from_be_bytes(key[28..36].try_into().ok()?);
        Some((
            HlcTimestamp { physical_ns, logical },
            publisher_id,
            seq,
        ))
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

    /// Per-publisher sequence tracking state.
    ///
    /// Tracks only the **missing** sequences (gaps) per publisher, which is
    /// optimal for stable networks where gaps are rare — the set stays near-empty.
    /// A `BTreeSet<u64>` acts as a sparse bitset over the u64 sequence space:
    /// O(1) memory when empty, O(log g) insert/remove/contains where g = gap count.
    #[derive(Debug, Default)]
    struct PublisherSeqState {
        /// Highest sequence number ever seen from this publisher.
        /// `None` means no events stored yet.
        highest_seen: Option<u64>,
        /// Highest contiguous sequence number durably stored.
        committed_seq: u64,
        /// Missing sequence numbers below `highest_seen` (the gaps).
        gaps: BTreeSet<u64>,
    }

    /// LSM-tree backed storage using the `fjall` crate.
    ///
    /// Each instance owns exactly one partition. Uses four keyspaces:
    /// - `events`   — primary event data, keyed by `(publisher_id, seq)`
    /// - `metadata` — watermark state (per-publisher `committed_seq`)
    /// - `keys`     — key_expr → event key mapping for compaction
    /// - `replay`   — HLC-ordered replay index for deterministic cross-replica replay
    pub struct FjallBackend {
        db: fjall::Database,
        events: fjall::Keyspace,
        metadata: fjall::Keyspace,
        keys: fjall::Keyspace,
        replay: fjall::Keyspace,
        /// Partition id this backend is responsible for.
        partition: u32,
        /// Per-publisher gap tracking. Updated incrementally on each `store()`
        /// to avoid O(N) scans in `publisher_watermarks()`.
        publisher_states: scc::HashMap<PublisherId, PublisherSeqState>,
    }

    /// Metadata key for a publisher's committed_seq in the metadata keyspace.
    fn committed_seq_key(pub_id: &PublisherId) -> Vec<u8> {
        let mut key = Vec::with_capacity(17);
        key.push(b'c'); // prefix to distinguish from other metadata
        key.extend_from_slice(&pub_id.to_bytes());
        key
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

            let replay = db
                .keyspace("replay", fjall::KeyspaceCreateOptions::default)
                .map_err(|e| Error::StoreError(format!("failed to open replay keyspace: {e}")))?;

            // One-time scan to rebuild per-publisher gap state from existing data.
            let mut states: HashMap<PublisherId, PublisherSeqState> = HashMap::new();

            for guard in events.iter() {
                let Ok(kv) = guard.into_inner() else { break };
                if let Some((pub_id, seq)) = decode_event_key(&kv.0) {
                    let state = states.entry(pub_id).or_default();
                    let expected = state.highest_seen.map_or(0, |h| h + 1);
                    // Record any gaps between expected and this seq.
                    for gap_seq in expected..seq {
                        state.gaps.insert(gap_seq);
                    }
                    state.highest_seen = Some(seq);
                }
            }

            // Compute committed_seq for each publisher from gap state.
            for state in states.values_mut() {
                if let Some(highest) = state.highest_seen {
                    let mut committed = 0u64;
                    // Advance past filled positions.
                    while committed < highest && !state.gaps.contains(&(committed + 1)) {
                        committed += 1;
                    }
                    // Handle the case where seq 0 exists (committed should be at least 0).
                    if !state.gaps.contains(&0) {
                        state.committed_seq = committed;
                    }
                }
            }

            let publisher_states = scc::HashMap::new();
            for (k, v) in states {
                let _ = publisher_states.insert_sync(k, v);
            }

            Ok(Self {
                db,
                events,
                metadata,
                keys,
                replay,
                partition,
                publisher_states,
            })
        }
    }

    impl StorageBackend for FjallBackend {
        fn partition(&self) -> u32 {
            self.partition
        }

        fn store(&self, key: &str, event: &[u8], metadata: EventMetadata) -> Result<()> {
            let seq = metadata.seq;
            let pub_id = metadata.publisher_id;
            let event_key = encode_event_key(&pub_id, seq);
            let event_value = encode_event_value(&metadata, event);

            let mut batch = self.db.batch();
            batch.insert(&self.events, event_key, &event_value);
            batch.insert(&self.keys, key.as_bytes(), event_key);

            // Write to HLC replay index if HLC timestamp is available.
            if let Some(hlc) = &metadata.hlc_timestamp {
                let replay_key = encode_replay_key(hlc, &pub_id, seq);
                batch.insert(&self.replay, replay_key, event_key);
            }

            // --- Incremental per-publisher gap tracking ---
            let mut entry = match self.publisher_states.entry_sync(pub_id) {
                scc::hash_map::Entry::Occupied(o) => o,
                scc::hash_map::Entry::Vacant(v) => v.insert_entry(PublisherSeqState::default()),
            };
            let state = entry.get_mut();

            // Remove this seq from gaps (filling a gap, or no-op).
            state.gaps.remove(&seq);

            match state.highest_seen {
                None => {
                    // First event from this publisher — seqs 0..seq are gaps.
                    for gap_seq in 0..seq {
                        state.gaps.insert(gap_seq);
                    }
                    state.highest_seen = Some(seq);
                }
                Some(highest) if seq > highest => {
                    // Extending beyond highest — seqs (highest+1)..seq are gaps.
                    for gap_seq in (highest + 1)..seq {
                        state.gaps.insert(gap_seq);
                    }
                    state.highest_seen = Some(seq);
                }
                _ => {} // seq <= highest, already tracked
            }

            // Advance committed_seq past any filled positions.
            if let Some(highest) = state.highest_seen {
                while state.committed_seq < highest
                    && !state.gaps.contains(&(state.committed_seq + 1))
                {
                    state.committed_seq += 1;
                }
            }

            let committed = state.committed_seq;
            drop(entry); // release the entry guard before batch commit
            let meta_key = committed_seq_key(&pub_id);
            batch.insert(&self.metadata, meta_key, committed.to_le_bytes());

            batch
                .commit()
                .map_err(|e| Error::StoreError(format!("batch commit failed: {e}")))?;

            Ok(())
        }

        fn store_batch(&self, events: Vec<(String, Vec<u8>, EventMetadata)>) -> Result<()> {
            if events.is_empty() {
                return Ok(());
            }

            let mut batch = self.db.batch();

            // Group events by publisher for efficient gap tracking.
            // We process all events into the batch and update states in one pass.
            for (key, payload, metadata) in &events {
                let seq = metadata.seq;
                let pub_id = metadata.publisher_id;
                let event_key = encode_event_key(&pub_id, seq);
                let event_value = encode_event_value(metadata, payload);

                batch.insert(&self.events, event_key, event_value);
                batch.insert(&self.keys, key.as_bytes(), event_key);

                // Write to HLC replay index if HLC timestamp is available.
                if let Some(hlc) = &metadata.hlc_timestamp {
                    let replay_key = encode_replay_key(hlc, &pub_id, seq);
                    batch.insert(&self.replay, replay_key, event_key);
                }

                // --- Incremental per-publisher gap tracking ---
                let mut entry = match self.publisher_states.entry_sync(pub_id) {
                    scc::hash_map::Entry::Occupied(o) => o,
                    scc::hash_map::Entry::Vacant(v) => {
                        v.insert_entry(PublisherSeqState::default())
                    }
                };
                let state = entry.get_mut();

                state.gaps.remove(&seq);

                match state.highest_seen {
                    None => {
                        for gap_seq in 0..seq {
                            state.gaps.insert(gap_seq);
                        }
                        state.highest_seen = Some(seq);
                    }
                    Some(highest) if seq > highest => {
                        for gap_seq in (highest + 1)..seq {
                            state.gaps.insert(gap_seq);
                        }
                        state.highest_seen = Some(seq);
                    }
                    _ => {}
                }

                if let Some(highest) = state.highest_seen {
                    while state.committed_seq < highest
                        && !state.gaps.contains(&(state.committed_seq + 1))
                    {
                        state.committed_seq += 1;
                    }
                }

                let committed = state.committed_seq;
                drop(entry);
                let meta_key = committed_seq_key(&pub_id);
                batch.insert(&self.metadata, meta_key, committed.to_le_bytes());
            }

            batch
                .commit()
                .map_err(|e| Error::StoreError(format!("batch commit failed: {e}")))?;

            Ok(())
        }

        fn query(&self, filters: &QueryFilters) -> Result<Vec<StoredEvent>> {
            let mut results = Vec::new();

            for guard in self.events.iter() {
                let kv = guard
                    .into_inner()
                    .map_err(|e| Error::StoreError(format!("scan error: {e}")))?;

                let Some((pub_id, seq)) = decode_event_key(&kv.0) else {
                    continue;
                };

                // Apply publisher_id filter early (before decoding value).
                if let Some(ref filter_pub_id) = filters.publisher_id {
                    if pub_id != *filter_pub_id {
                        continue;
                    }
                }

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

        fn query_replay(&self, filters: &ReplayFilters) -> Result<Vec<StoredEvent>> {
            let mut results = Vec::new();

            for guard in self.replay.iter() {
                let kv = guard
                    .into_inner()
                    .map_err(|e| Error::StoreError(format!("replay scan error: {e}")))?;

                let Some((hlc, _pub_id, _seq)) = decode_replay_key(&kv.0) else {
                    continue;
                };

                // Apply HLC range filters.
                if let Some(ref after) = filters.after_hlc {
                    if hlc <= *after {
                        continue;
                    }
                }
                if let Some(ref before) = filters.before_hlc {
                    if hlc >= *before {
                        break; // replay keys are sorted, no more matches
                    }
                }

                // Look up full event from primary index.
                let event_key_bytes = kv.1.to_vec();
                let event_value = self
                    .events
                    .get(&event_key_bytes)
                    .map_err(|e| Error::StoreError(format!("replay lookup error: {e}")))?;

                let Some(event_value) = event_value else {
                    continue; // event was GC'd but replay entry lingers
                };

                let (meta, payload) = decode_event_value(&event_value)?;

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

        fn publisher_watermarks(&self) -> HashMap<PublisherId, PublisherWatermark> {
            let mut result = HashMap::new();
            self.publisher_states.iter_sync(|pub_id, state| {
                let pw = PublisherWatermark {
                    committed_seq: state.committed_seq,
                    gaps: state.gaps.iter().copied().collect(),
                };
                result.insert(*pub_id, pw);
                true
            });
            result
        }

        fn gc(&self, older_than: DateTime<Utc>) -> Result<usize> {
            let mut removed = 0usize;
            let mut to_remove: Vec<(Vec<u8>, String, Option<HlcTimestamp>)> = Vec::new();

            for guard in self.events.iter() {
                let kv = guard
                    .into_inner()
                    .map_err(|e| Error::StoreError(format!("scan error: {e}")))?;

                let (meta, _) = decode_event_value(&kv.1)?;
                if meta.timestamp < older_than {
                    to_remove.push((kv.0.to_vec(), meta.key_expr.clone(), meta.hlc_timestamp));
                }
            }

            for (event_key, key_expr, hlc_ts) in &to_remove {
                let mut batch = self.db.batch();
                batch.remove(&self.events, event_key.as_slice());
                batch.remove(&self.keys, key_expr.as_bytes());
                // Clean up replay index entry using the stored HLC timestamp.
                if let (Some(hlc), Some((pub_id, seq))) =
                    (hlc_ts, decode_event_key(event_key))
                {
                    let replay_key = encode_replay_key(hlc, &pub_id, seq);
                    batch.remove(&self.replay, replay_key);
                }
                batch
                    .commit()
                    .map_err(|e| Error::StoreError(format!("gc batch commit failed: {e}")))?;
                removed += 1;
            }

            Ok(removed)
        }

        fn compact(&self) -> Result<CompactionStats> {
            let mut latest: std::collections::HashMap<String, (Vec<u8>, u64)> =
                std::collections::HashMap::new();
            let mut all_entries: Vec<(Vec<u8>, String, u64)> = Vec::new();

            for guard in self.events.iter() {
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
