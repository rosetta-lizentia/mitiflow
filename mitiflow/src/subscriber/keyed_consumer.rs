//! Pull-based key-scoped consumer that polls the store for events.
//!
//! [`KeyedConsumer`] queries the store's queryable in HLC order, filtered by
//! application key or key prefix. It tracks an HLC cursor that advances with
//! each poll, ensuring exactly-once delivery across successive polls.
//!
//! See `docs/19_key_scoped_subscribing.md` (Mode 2) for design rationale.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::warn;
use zenoh::Session;

use crate::attachment::decode_metadata;
use crate::config::EventBusConfig;
use crate::error::{Error, Result};
use crate::store::backend::{EventMetadata, HlcTimestamp, StoredEvent};

/// Distinguishes exact-key vs prefix-key filtering.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KeyFilter {
    /// Match events whose application key equals this value exactly.
    Exact(String),
    /// Match events whose application key starts with this prefix.
    Prefix(String),
}

impl KeyFilter {
    /// Encode this filter as a Zenoh selector parameter fragment.
    pub fn to_selector_param(&self) -> String {
        match self {
            KeyFilter::Exact(k) => format!("key={k}"),
            KeyFilter::Prefix(p) => format!("key_prefix={p}"),
        }
    }

    /// Stable hash of this filter for use in offset storage keys.
    pub fn hash(&self) -> u64 {
        let input = match self {
            KeyFilter::Exact(k) => format!("exact:{k}"),
            KeyFilter::Prefix(p) => format!("prefix:{p}"),
        };
        // FNV-1a hash (same family as group_id hash in backend.rs)
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in input.as_bytes() {
            h ^= *byte as u64;
            h = h.wrapping_mul(0x100_0000_01b3);
        }
        h
    }
}

/// Pull-based key-scoped consumer.
///
/// Polls the store's queryable for events matching a key filter, returning
/// them in HLC order. The cursor advances automatically so successive polls
/// yield non-overlapping result sets.
pub struct KeyedConsumer {
    session: Session,
    config: EventBusConfig,
    key_filter: KeyFilter,
    cursor: HlcTimestamp,
    batch_size: usize,
    query_timeout: Duration,
    /// Full queryable selector base, e.g. `myapp/_store/*` or `myapp/_store/0`.
    selector_base: String,
}

impl KeyedConsumer {
    /// Start building a new `KeyedConsumer`.
    pub fn builder(session: &Session, config: EventBusConfig) -> KeyedConsumerBuilder {
        KeyedConsumerBuilder {
            session: session.clone(),
            config,
            key_filter: None,
            partition: None,
            batch_size: 500,
            query_timeout: Duration::from_secs(5),
        }
    }

    /// Poll the store for the next batch of events matching the key filter.
    ///
    /// Returns events in HLC order. The internal cursor advances past the last
    /// returned event so the next `poll()` yields only newer events.
    ///
    /// Returns an empty `Vec` when there are no new matching events (not an error).
    pub async fn poll(&mut self) -> Result<Vec<StoredEvent>> {
        let selector = format!(
            "{}?{}&after_hlc={}:{}&limit={}",
            self.selector_base,
            self.key_filter.to_selector_param(),
            self.cursor.physical_ns,
            self.cursor.logical,
            self.batch_size,
        );

        tracing::debug!(%selector, "keyed consumer poll");

        let replies = self
            .session
            .get(&selector)
            .accept_replies(zenoh::query::ReplyKeyExpr::Any)
            .consolidation(zenoh::query::ConsolidationMode::None)
            .timeout(self.query_timeout)
            .await
            .map_err(Error::Zenoh)?;

        let mut events = Vec::new();
        while let Ok(reply) = replies.recv_async().await {
            match reply.result() {
                Ok(sample) => {
                    let meta = match sample.attachment().and_then(|a| decode_metadata(a).ok()) {
                        Some(m) => m,
                        None => {
                            warn!("keyed consumer: reply without valid attachment, skipping");
                            continue;
                        }
                    };

                    let key_expr = sample.key_expr().as_str().to_string();
                    let payload = sample.payload().to_bytes().to_vec();

                    // Extract application key from key expression (before moving key_expr).
                    let app_key = crate::attachment::extract_key(&key_expr).map(|s| s.to_string());

                    // Extract HLC from the attachment extension (62-byte format)
                    // or fall back to chrono timestamp.
                    let hlc = if let (Some(phys), Some(log)) =
                        (meta.hlc_physical_ns, meta.hlc_logical)
                    {
                        HlcTimestamp {
                            physical_ns: phys,
                            logical: log,
                        }
                    } else {
                        HlcTimestamp {
                            physical_ns: meta.timestamp.timestamp_nanos_opt().unwrap_or(0)
                                as u64,
                            logical: 0,
                        }
                    };

                    let event_meta = EventMetadata {
                        seq: meta.seq,
                        publisher_id: meta.pub_id,
                        event_id: meta.event_id,
                        timestamp: meta.timestamp,
                        key_expr,
                        key: app_key,
                        hlc_timestamp: Some(hlc),
                    };

                    events.push(StoredEvent {
                        key: event_meta.key_expr.clone(),
                        payload,
                        metadata: event_meta,
                    });
                }
                Err(err) => {
                    warn!(
                        "keyed consumer: reply error: {}",
                        err.payload().try_to_string().unwrap_or_default()
                    );
                }
            }
        }

        // Sort by HLC (replies may arrive out of order).
        events.sort_by(|a, b| {
            let hlc_a = a.metadata.hlc_timestamp.unwrap_or_default();
            let hlc_b = b.metadata.hlc_timestamp.unwrap_or_default();
            hlc_a
                .cmp(&hlc_b)
                .then(a.metadata.seq.cmp(&b.metadata.seq))
        });

        // Advance cursor past the last returned event.
        if let Some(last) = events.last()
            && let Some(hlc) = &last.metadata.hlc_timestamp
        {
            self.cursor = *hlc;
        }

        Ok(events)
    }

    /// Set the cursor to a specific HLC timestamp.
    ///
    /// The next `poll()` will return events strictly after this timestamp.
    pub fn seek(&mut self, hlc: HlcTimestamp) {
        self.cursor = hlc;
    }

    /// Return the current cursor position.
    pub fn cursor(&self) -> &HlcTimestamp {
        &self.cursor
    }

    /// Return a reference to the key filter.
    pub fn key_filter(&self) -> &KeyFilter {
        &self.key_filter
    }

    /// Return a reference to the config.
    pub fn config(&self) -> &EventBusConfig {
        &self.config
    }

    /// Commit the current cursor as the group's offset for this key filter.
    ///
    /// The offset is published to `{key_prefix}/_offsets/key/{group_id}/{filter_hash}`
    /// and persisted by the store.
    pub async fn commit(&self, group_id: &str) -> Result<()> {
        use crate::store::offset::KeyedOffsetCommit;

        let commit = KeyedOffsetCommit {
            group_id: group_id.to_string(),
            member_id: String::new(),
            key_filter: self.key_filter.clone(),
            last_hlc: self.cursor,
            generation: 0,
            timestamp: chrono::Utc::now(),
        };
        let key = format!(
            "{}/_offsets/key/{}/{}",
            self.config.key_prefix,
            group_id,
            self.key_filter.hash(),
        );
        let payload = serde_json::to_vec(&commit)
            .map_err(|e| Error::Serialization(e.to_string()))?;
        self.session
            .put(&key, payload)
            .await
            .map_err(Error::Zenoh)?;
        Ok(())
    }

    /// Fetch the last committed HLC cursor for this group + key filter.
    ///
    /// Queries `{key_prefix}/_offsets/key/{group_id}/{filter_hash}` from the store.
    pub async fn fetch_offset(&self, group_id: &str) -> Result<Option<HlcTimestamp>> {
        let selector = format!(
            "{}/_offsets/key/{}/{}?fetch=true&group_id={}&key_filter_hash={}",
            self.config.key_prefix,
            group_id,
            self.key_filter.hash(),
            group_id,
            self.key_filter.hash(),
        );
        let replies = self
            .session
            .get(&selector)
            .timeout(self.query_timeout)
            .await
            .map_err(Error::Zenoh)?;

        let mut result: Option<HlcTimestamp> = None;
        while let Ok(reply) = replies.recv_async().await {
            if let Ok(sample) = reply.result() {
                let bytes = sample.payload().to_bytes();
                if let Ok(Some(hlc)) = serde_json::from_slice::<Option<HlcTimestamp>>(&bytes) {
                    result = Some(hlc);
                }
            }
        }

        Ok(result)
    }
}

/// Builder for [`KeyedConsumer`].
pub struct KeyedConsumerBuilder {
    session: Session,
    config: EventBusConfig,
    key_filter: Option<KeyFilter>,
    partition: Option<u32>,
    batch_size: usize,
    query_timeout: Duration,
}

impl KeyedConsumerBuilder {
    /// Filter by exact application key.
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.key_filter = Some(KeyFilter::Exact(key.into()));
        self
    }

    /// Filter by application key prefix.
    pub fn key_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.key_filter = Some(KeyFilter::Prefix(prefix.into()));
        self
    }

    /// Query a specific partition (default: all partitions via wildcard).
    pub fn partition(mut self, partition: u32) -> Self {
        self.partition = Some(partition);
        self
    }

    /// Maximum number of events per poll (default: 500).
    pub fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Timeout for each store query (default: 5s).
    pub fn query_timeout(mut self, timeout: Duration) -> Self {
        self.query_timeout = timeout;
        self
    }

    /// Build the consumer. Requires exactly one of `key()` or `key_prefix()`.
    pub fn build(self) -> Result<KeyedConsumer> {
        let key_filter = self
            .key_filter
            .ok_or_else(|| Error::InvalidConfig("KeyedConsumer requires key() or key_prefix()".into()))?;

        let store_prefix = self.config.resolved_store_key_prefix();
        let selector_base = match self.partition {
            Some(p) => format!("{store_prefix}/{p}"),
            None => format!("{store_prefix}/*"),
        };

        Ok(KeyedConsumer {
            session: self.session,
            config: self.config,
            key_filter,
            cursor: HlcTimestamp::default(),
            batch_size: self.batch_size,
            query_timeout: self.query_timeout,
            selector_base,
        })
    }
}
