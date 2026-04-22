use std::collections::{HashSet, VecDeque};
use std::time::Duration;

use futures::Stream;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use zenoh::Session;

use crate::attachment::decode_metadata;
use crate::config::EventBusConfig;
use crate::error::{Error, Result};
use crate::event::{Event, RawEvent};
use crate::store::backend::{EventMetadata, HlcTimestamp, StoredEvent};
use crate::store::query::ReplayFilters;
use crate::subscriber::EventSubscriber;
use crate::types::PublisherId;

/// What to replay — scopes the query to a subset of the event log.
#[derive(Debug, Clone)]
pub enum ReplayScope {
    All,
    Partition(u32),
    Key(String),
    KeyPrefix(String),
    Publisher(PublisherId),
}

/// Where to start the replay cursor.
#[derive(Debug, Clone)]
pub enum ReplayPosition {
    Earliest,
    AfterHlc(HlcTimestamp),
    AfterTime(chrono::DateTime<chrono::Utc>),
    CommittedOffset { group_id: String },
}

/// When to stop (or whether to keep going).
#[derive(Debug, Clone)]
pub enum ReplayEnd {
    Bounded { limit: usize },
    AtHlc(HlcTimestamp),
    AtTime(chrono::DateTime<chrono::Utc>),
    Tailing { poll_interval: Duration },
    ThenLive,
}

impl Default for ReplayEnd {
    fn default() -> Self {
        ReplayEnd::Bounded { limit: usize::MAX }
    }
}

/// Deterministic event replayer with cursor-based pagination.
///
/// Created via [`EventReplayer::builder`]. Supports remote replay over Zenoh
/// queries and an opt-in local fast path via `Arc<dyn StorageBackend>`.
pub struct EventReplayer {
    session: Session,
    config: EventBusConfig,
    scope: ReplayScope,
    _position: ReplayPosition,
    end: ReplayEnd,
    cursor: HlcTimestamp,
    batch_size: usize,
    query_timeout: Duration,
    selector_base: String,
    buffer: VecDeque<StoredEvent>,
    delivered: usize,
    exhausted: bool,
    cancel: CancellationToken,
    local_backend: Option<std::sync::Arc<dyn crate::store::backend::StorageBackend>>,
    live_rx: Option<flume::Receiver<RawEvent>>,
    live_tasks: Vec<tokio::task::JoinHandle<()>>,
    overlap_buffer: VecDeque<RawEvent>,
    seen_seqs: HashSet<(PublisherId, u64)>,
    live_active: bool,
}

impl EventReplayer {
    pub fn builder(session: &Session, config: EventBusConfig) -> EventReplayerBuilder {
        EventReplayerBuilder {
            session: session.clone(),
            config,
            scope: ReplayScope::All,
            position: ReplayPosition::Earliest,
            end: ReplayEnd::default(),
            batch_size: 500,
            query_timeout: Duration::from_secs(5),
            local_backend: None,
        }
    }
}

pub struct EventReplayerBuilder {
    session: Session,
    config: EventBusConfig,
    scope: ReplayScope,
    position: ReplayPosition,
    end: ReplayEnd,
    batch_size: usize,
    query_timeout: Duration,
    local_backend: Option<std::sync::Arc<dyn crate::store::backend::StorageBackend>>,
}

impl EventReplayerBuilder {
    pub fn all(mut self) -> Self {
        self.scope = ReplayScope::All;
        self
    }

    pub fn partition(mut self, partition: u32) -> Self {
        self.scope = ReplayScope::Partition(partition);
        self
    }

    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.scope = ReplayScope::Key(key.into());
        self
    }

    pub fn key_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.scope = ReplayScope::KeyPrefix(prefix.into());
        self
    }

    pub fn publisher(mut self, publisher_id: PublisherId) -> Self {
        self.scope = ReplayScope::Publisher(publisher_id);
        self
    }

    pub fn start(mut self, position: ReplayPosition) -> Self {
        self.position = position;
        self
    }

    pub fn end(mut self, end: ReplayEnd) -> Self {
        self.end = end;
        self
    }

    pub fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = size.max(1);
        self
    }

    pub fn query_timeout(mut self, timeout: Duration) -> Self {
        self.query_timeout = timeout;
        self
    }

    pub fn local_backend(
        mut self,
        backend: std::sync::Arc<dyn crate::store::backend::StorageBackend>,
    ) -> Self {
        self.local_backend = Some(backend);
        self
    }

    pub async fn build(self) -> Result<EventReplayer> {
        let store_prefix = self.config.resolved_store_key_prefix();
        let selector_base = match &self.scope {
            ReplayScope::All => format!("{store_prefix}/*"),
            ReplayScope::Partition(p) => format!("{store_prefix}/{p}"),
            ReplayScope::Key(_)
            | ReplayScope::KeyPrefix(_)
            | ReplayScope::Publisher(_) => format!("{store_prefix}/*"),
        };

        let cursor = match &self.position {
            ReplayPosition::Earliest => HlcTimestamp::default(),
            ReplayPosition::AfterHlc(hlc) => *hlc,
            ReplayPosition::AfterTime(t) => HlcTimestamp {
                physical_ns: t.timestamp_nanos_opt().unwrap_or(0) as u64,
                logical: 0,
            },
            ReplayPosition::CommittedOffset { group_id } => {
                fetch_replay_offset(&self.session, &self.config, group_id, self.query_timeout)
                    .await?
                    .ok_or_else(|| Error::OffsetNotFound {
                        group_id: group_id.clone(),
                    })?
            }
        };

        Ok(EventReplayer {
            session: self.session,
            config: self.config,
            scope: self.scope,
            _position: self.position,
            end: self.end,
            cursor,
            batch_size: self.batch_size,
            query_timeout: self.query_timeout,
            selector_base,
            buffer: VecDeque::new(),
            delivered: 0,
            exhausted: false,
            cancel: CancellationToken::new(),
            local_backend: self.local_backend,
            live_rx: None,
            live_tasks: Vec::new(),
            overlap_buffer: VecDeque::new(),
            seen_seqs: HashSet::new(),
            live_active: false,
        })
    }
}

impl EventReplayer {
    pub async fn poll(&mut self) -> Result<Vec<StoredEvent>> {
        if self.exhausted {
            return Err(Error::EndOfReplay);
        }

        let poll_limit = match &self.end {
            ReplayEnd::Bounded { limit } => {
                let remaining = limit.saturating_sub(self.delivered);
                if remaining == 0 {
                    self.exhausted = true;
                    return Err(Error::EndOfReplay);
                }
                remaining.min(self.batch_size)
            }
            _ => self.batch_size,
        };

        let before_hlc = match &self.end {
            ReplayEnd::AtHlc(hlc) => Some(*hlc),
            ReplayEnd::AtTime(t) => Some(HlcTimestamp {
                physical_ns: t.timestamp_nanos_opt().unwrap_or(0) as u64,
                logical: u32::MAX,
            }),
            _ => None,
        };

        let events = if let Some(ref backend) = self.local_backend {
            self.poll_local(backend.clone(), poll_limit, before_hlc)?
        } else {
            self.poll_remote(poll_limit, before_hlc).await?
        };

        if let Some(last) = events.last()
            && let Some(hlc) = &last.metadata.hlc_timestamp
        {
            self.cursor = *hlc;
        }

        self.delivered += events.len();

        if events.is_empty() {
            match &self.end {
                ReplayEnd::Bounded { .. } | ReplayEnd::AtHlc(_) | ReplayEnd::AtTime(_) => {
                    self.exhausted = true;
                }
                ReplayEnd::Tailing { .. } | ReplayEnd::ThenLive => {}
            }
        }

        if let ReplayEnd::Bounded { limit } = &self.end
            && self.delivered >= *limit
        {
            self.exhausted = true;
        }

        Ok(events)
    }

    pub async fn recv<T: Serialize + DeserializeOwned>(&mut self) -> Result<Event<T>> {
        let raw = self.recv_raw().await?;
        raw.deserialize_with(self.config.codec)
    }

    pub async fn recv_raw(&mut self) -> Result<RawEvent> {
        loop {
            if let Some(stored) = self.buffer.pop_front() {
                let raw = stored_to_raw(stored);
                if matches!(self.end, ReplayEnd::ThenLive) {
                    self.seen_seqs.insert((raw.publisher_id, raw.seq));
                }
                return Ok(raw);
            }

            if let Some(raw) = self.overlap_buffer.pop_front() {
                return Ok(raw);
            }

            if self.live_active {
                if let Some(ref rx) = self.live_rx {
                    return rx.recv_async().await.map_err(|_| Error::ChannelClosed);
                }
                return Err(Error::ChannelClosed);
            }

            if self.exhausted && !matches!(self.end, ReplayEnd::ThenLive) {
                return Err(Error::EndOfReplay);
            }

            let batch = self.poll().await;

            match batch {
                Ok(events) if events.is_empty() => {
                    match &self.end {
                        ReplayEnd::Tailing { poll_interval } => {
                            tokio::time::sleep(*poll_interval).await;
                            continue;
                        }
                        ReplayEnd::ThenLive => {
                            self.start_live_subscriber().await?;
                            self.flush_overlap_buffer();
                            self.live_active = true;
                            self.seen_seqs.clear();
                            if let Some(ref rx) = self.live_rx {
                                return rx.recv_async().await.map_err(|_| Error::ChannelClosed);
                            }
                            return Err(Error::ChannelClosed);
                        }
                        _ => {
                            if self.exhausted {
                                return Err(Error::EndOfReplay);
                            }
                            continue;
                        }
                    }
                }
                Ok(events) => {
                    if matches!(self.end, ReplayEnd::ThenLive) && self.live_rx.is_none() {
                        self.start_live_subscriber().await?;
                    }
                    self.buffer.extend(events);
                }
                Err(Error::EndOfReplay) => {
                    if matches!(self.end, ReplayEnd::ThenLive) {
                        self.start_live_subscriber().await?;
                        self.flush_overlap_buffer();
                        self.live_active = true;
                        self.seen_seqs.clear();
                        if let Some(ref rx) = self.live_rx {
                            return rx.recv_async().await.map_err(|_| Error::ChannelClosed);
                        }
                        return Err(Error::ChannelClosed);
                    }
                    return Err(Error::EndOfReplay);
                }
                Err(e) => return Err(e),
            }
        }
    }

    pub fn stream<T: Serialize + DeserializeOwned>(
        &mut self,
    ) -> impl Stream<Item = Result<Event<T>>> + '_ {
        futures::stream::unfold(self, |replayer| async move {
            match replayer.recv::<T>().await {
                Ok(event) => Some((Ok(event), replayer)),
                Err(Error::EndOfReplay) => None,
                Err(Error::ChannelClosed) => None,
                Err(e) => Some((Err(e), replayer)),
            }
        })
    }

    pub fn seek(&mut self, hlc: HlcTimestamp) {
        self.cursor = hlc;
        self.buffer.clear();
        self.exhausted = false;
    }

    pub fn cursor(&self) -> &HlcTimestamp {
        &self.cursor
    }

    pub fn config(&self) -> &EventBusConfig {
        &self.config
    }

    pub async fn commit(&self, group_id: &str) -> Result<()> {
        let key = format!(
            "{}/_replay_offsets/{}",
            self.config.key_prefix, group_id,
        );
        let payload =
            serde_json::to_vec(&self.cursor).map_err(|e| Error::Serialization(e.to_string()))?;
        self.session
            .put(&key, payload)
            .await
            .map_err(Error::Zenoh)?;
        debug!(group_id, cursor = ?self.cursor, "committed replay offset");
        Ok(())
    }

    pub async fn seek_to_committed(&mut self, group_id: &str) -> Result<bool> {
        match fetch_replay_offset(&self.session, &self.config, group_id, self.query_timeout).await?
        {
            Some(hlc) => {
                self.seek(hlc);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    pub async fn into_live(self, session: &Session) -> Result<EventSubscriber> {
        let config = self.config.clone();
        let subscriber = match &self.scope {
            ReplayScope::All => EventSubscriber::new(session, config).await?,
            ReplayScope::Partition(p) => {
                EventSubscriber::new_partitioned(session, config, &[*p]).await?
            }
            ReplayScope::Key(k) => EventSubscriber::new_keyed(session, config, k).await?,
            ReplayScope::KeyPrefix(p) => {
                EventSubscriber::new_key_prefix(session, config, p).await?
            }
            ReplayScope::Publisher(_) => EventSubscriber::new(session, config).await?,
        };
        self.cancel.cancel();
        Ok(subscriber)
    }

    pub async fn shutdown(mut self) {
        self.cancel.cancel();
        let tasks = std::mem::take(&mut self.live_tasks);
        for handle in tasks {
            let _ = handle.await;
        }
    }
}

impl EventReplayer {
    async fn start_live_subscriber(&mut self) -> Result<()> {
        if self.live_rx.is_some() {
            return Ok(());
        }

        let (tx, rx) = flume::bounded::<RawEvent>(self.config.event_channel_capacity);
        let cancel = self.cancel.clone();

        let key_expr = match &self.scope {
            ReplayScope::All => format!("{}/**", self.config.key_prefix),
            ReplayScope::Partition(p) => format!("{}/p/{p}/**", self.config.key_prefix),
            ReplayScope::Key(k) => self.config.key_expr_for_key(k),
            ReplayScope::KeyPrefix(p) => self.config.key_expr_for_key_prefix(p),
            ReplayScope::Publisher(_) => format!("{}/**", self.config.key_prefix),
        };

        let session = self.session.clone();
        let scope_publisher = match &self.scope {
            ReplayScope::Publisher(id) => Some(*id),
            _ => None,
        };

        let task = tokio::spawn(async move {
            let sub = match session.declare_subscriber(&key_expr).await {
                Ok(s) => s,
                Err(e) => {
                    warn!("replayer: failed to declare live subscriber: {e}");
                    return;
                }
            };

            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    sample_result = sub.recv_async() => {
                        match sample_result {
                            Ok(sample) => {
                                let ke = sample.key_expr().as_str();
                                if ke.contains("/_") {
                                    continue;
                                }
                                let meta = match sample.attachment().and_then(|a| decode_metadata(a).ok()) {
                                    Some(m) => m,
                                    None => continue,
                                };
                                if let Some(filter_pub) = scope_publisher
                                    && meta.pub_id != filter_pub
                                {
                                    continue;
                                }
                                let raw = RawEvent {
                                    id: meta.event_id,
                                    seq: meta.seq,
                                    publisher_id: meta.pub_id,
                                    key_expr: ke.to_string(),
                                    payload: sample.payload().to_bytes().to_vec(),
                                    timestamp: meta.timestamp,
                                };
                                if tx.send_async(raw).await.is_err() {
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
        });

        self.live_rx = Some(rx);
        self.live_tasks.push(task);
        debug!("replayer: live subscriber started for ThenLive transition");
        Ok(())
    }

    fn flush_overlap_buffer(&mut self) {
        if let Some(ref rx) = self.live_rx {
            while let Ok(raw) = rx.try_recv() {
                if !self.seen_seqs.contains(&(raw.publisher_id, raw.seq)) {
                    self.overlap_buffer.push_back(raw);
                }
            }
        }
    }

    async fn poll_remote(
        &self,
        limit: usize,
        before_hlc: Option<HlcTimestamp>,
    ) -> Result<Vec<StoredEvent>> {
        let mut params = format!(
            "after_hlc={}:{}&limit={}",
            self.cursor.physical_ns, self.cursor.logical, limit,
        );

        match &self.scope {
            ReplayScope::Key(k) => {
                params.push_str(&format!("&key={k}"));
            }
            ReplayScope::KeyPrefix(p) => {
                params.push_str(&format!("&key_prefix={p}"));
            }
            ReplayScope::Publisher(id) => {
                params.push_str(&format!("&publisher_id={}", id.as_uuid()));
            }
            ReplayScope::All | ReplayScope::Partition(_) => {}
        }

        if let Some(before) = before_hlc {
            params.push_str(&format!(
                "&before_hlc={}:{}",
                before.physical_ns, before.logical
            ));
        }

        let selector = format!("{}?{}", self.selector_base, params);
        debug!(%selector, "replayer poll");

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
                            warn!("replayer: reply without valid attachment, skipping");
                            continue;
                        }
                    };

                    let key_expr = sample.key_expr().as_str().to_string();
                    let payload = sample.payload().to_bytes().to_vec();
                    let app_key =
                        crate::attachment::extract_key(&key_expr).map(|s| s.to_string());

                    let hlc = if let (Some(phys), Some(log)) =
                        (meta.hlc_physical_ns, meta.hlc_logical)
                    {
                        HlcTimestamp {
                            physical_ns: phys,
                            logical: log,
                        }
                    } else {
                        HlcTimestamp {
                            physical_ns: meta.timestamp.timestamp_nanos_opt().unwrap_or(0) as u64,
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
                        "replayer: reply error: {}",
                        err.payload().try_to_string().unwrap_or_default()
                    );
                }
            }
        }

        events.sort_by(|a, b| {
            let hlc_a = a.metadata.hlc_timestamp.unwrap_or_default();
            let hlc_b = b.metadata.hlc_timestamp.unwrap_or_default();
            hlc_a
                .cmp(&hlc_b)
                .then(a.metadata.publisher_id.to_bytes().cmp(&b.metadata.publisher_id.to_bytes()))
                .then(a.metadata.seq.cmp(&b.metadata.seq))
        });

        Ok(events)
    }

    fn poll_local(
        &self,
        backend: std::sync::Arc<dyn crate::store::backend::StorageBackend>,
        limit: usize,
        before_hlc: Option<HlcTimestamp>,
    ) -> Result<Vec<StoredEvent>> {
        let mut filters = ReplayFilters {
            after_hlc: Some(self.cursor),
            before_hlc,
            limit: Some(limit),
            ..Default::default()
        };

        match &self.scope {
            ReplayScope::Key(k) => filters.key = Some(k.clone()),
            ReplayScope::KeyPrefix(p) => filters.key_prefix = Some(p.clone()),
            ReplayScope::Publisher(id) => filters.publisher_id = Some(*id),
            ReplayScope::All | ReplayScope::Partition(_) => {}
        }

        backend.query_replay(&filters)
    }
}

async fn fetch_replay_offset(
    session: &Session,
    config: &EventBusConfig,
    group_id: &str,
    timeout: Duration,
) -> Result<Option<HlcTimestamp>> {
    let selector = format!(
        "{}/_replay_offsets/{}",
        config.key_prefix, group_id,
    );
    let replies = session
        .get(&selector)
        .timeout(timeout)
        .await
        .map_err(Error::Zenoh)?;

    let mut result: Option<HlcTimestamp> = None;
    while let Ok(reply) = replies.recv_async().await {
        if let Ok(sample) = reply.result() {
            let bytes = sample.payload().to_bytes();
            if let Ok(hlc) = serde_json::from_slice::<HlcTimestamp>(&bytes) {
                result = Some(hlc);
            }
        }
    }
    Ok(result)
}

fn stored_to_raw(stored: StoredEvent) -> RawEvent {
    RawEvent {
        id: stored.metadata.event_id,
        seq: stored.metadata.seq,
        publisher_id: stored.metadata.publisher_id,
        key_expr: stored.metadata.key_expr,
        payload: stored.payload,
        timestamp: stored.metadata.timestamp,
    }
}

impl Drop for EventReplayer {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_end_default_is_bounded() {
        let end = ReplayEnd::default();
        assert!(matches!(end, ReplayEnd::Bounded { limit } if limit == usize::MAX));
    }

    #[test]
    fn stored_to_raw_preserves_fields() {
        let pub_id = PublisherId::new();
        let event_id = crate::types::EventId::new();
        let now = chrono::Utc::now();

        let stored = StoredEvent {
            key: "test/key".to_string(),
            payload: b"hello".to_vec(),
            metadata: EventMetadata {
                seq: 42,
                publisher_id: pub_id,
                event_id,
                timestamp: now,
                key_expr: "test/key".to_string(),
                key: Some("mykey".to_string()),
                hlc_timestamp: Some(HlcTimestamp {
                    physical_ns: 1000,
                    logical: 5,
                }),
            },
        };

        let raw = stored_to_raw(stored);
        assert_eq!(raw.seq, 42);
        assert_eq!(raw.publisher_id, pub_id);
        assert_eq!(raw.id, event_id);
        assert_eq!(raw.payload, b"hello");
        assert_eq!(raw.key_expr, "test/key");
    }
}
