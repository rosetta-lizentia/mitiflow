# Cache & Recovery Design

How mitiflow recovers when a subscriber detects a missed event.

---

## Current Design & Problems

Today, the only recovery source is the publisher's in-memory
`RwLock<VecDeque<CachedSample>>` (default 10,000 entries). On every
`publish()`, the payload is `Vec<u8>::clone()`-d into the cache behind an async
write-lock. On gap detection, the subscriber sends `session.get()` to the
publisher's cache queryable.

```
Publisher                     Subscriber
  │ publish(seq=5)              │
  │ ────(Zenoh)─────────────────> gap detected: missed seq 3,4
  │                              │
  │  ┌────────────────────┐      │ session.get("prefix/_cache/{pub_id}?after_seq=3")
  │  │ RwLock<VecDeque>   │ <────│
  │  │ [seq=2..seq=8]     │ ────>│ replies with cached samples
  │  └────────────────────┘      │
```

| Problem | Impact |
|---------|--------|
| Cache is bounded & in-memory | Old gaps become irrecoverable |
| Publisher crash = cache lost | Events in transit disappear forever |
| Publisher must be online | Subscriber cannot recover if publisher is gone |
| `Vec<u8>::clone()` + `RwLock::write()` per publish | Memcpy + contention on the hot path |
| No store fallback | EventStore has the data on disk, but subscriber never asks it |

---

## Redesign: Tiered Recovery with ZBytes Cache

### Design Goals

1. **EventStore is the primary recovery source** — it already has events persisted.
2. **Publisher cache is a fast fallback** — covers the store-lag window
   (the ~1 ms between Zenoh `put()` and store persistence).
3. **Zero-copy cache** — use `ZBytes` (Zenoh's native ref-counted bytes) instead
   of `Vec<u8>` to eliminate memcpy on cache insertion.
4. **Works without `"store"` feature** — degrades to cache-only recovery.

---

## 1. Publisher Cache with ZBytes

### Why ZBytes

`zenoh::bytes::ZBytes` is Zenoh's native payload type. It is internally
reference-counted (`Arc<[u8]>`) and cheaply cloneable — `ZBytes::clone()` is an
atomic increment, no memcpy. The publisher already builds a `ZBytes` for
`session.put()`. Sharing the same `ZBytes` with the cache means the payload
bytes exist exactly once in memory.

Using `ZBytes` instead of a third-party `bytes::Bytes` avoids a redundant
type-conversion on the cache queryable reply path: the queryable can hand the
`ZBytes` directly to `query.reply()` without `ZBytes::from(payload.clone())`.

### CachedSample

```rust
use zenoh::bytes::ZBytes;

/// A sample cached in the publisher's recovery buffer.
#[derive(Clone)]
pub struct CachedSample {
    pub seq: u64,
    pub key_expr: String,
    pub payload: ZBytes,      // ref-counted, zero-copy clone
    pub attachment: ZBytes,   // pre-encoded metadata attachment
}
```

Both `payload` and `attachment` are `ZBytes`. On a cache-query reply the
publisher calls `query.reply(key, sample.payload.clone()).attachment(sample.attachment.clone())`
— two atomic increments, no allocation, no encoding.

### Publish Path

```rust
async fn put_payload(
    &self,
    key: &str,
    payload: Vec<u8>,
    seq: u64,
    event_id: EventId,
    timestamp: DateTime<Utc>,
    urgency_ms: u16,
) -> Result<()> {
    let attachment = encode_metadata(&self.publisher_id, seq, &event_id, &timestamp, urgency_ms);
    let zbytes_payload = ZBytes::from(payload);  // takes ownership, no copy

    // Cache insertion: ZBytes::clone() = atomic ref-count increment
    if self.config.cache_size > 0 {
        let mut cache = self.cache.write().await;
        if cache.len() >= self.config.cache_size {
            cache.pop_front();
        }
        cache.push_back(CachedSample {
            seq,
            key_expr: key.to_string(),
            payload: zbytes_payload.clone(),     // cheap
            attachment: attachment.clone(),       // cheap
        });
    }

    self.session
        .put(key, zbytes_payload)
        .attachment(attachment)
        .congestion_control(self.config.congestion_control)
        .await?;
    Ok(())
}
```

**Hot-path cost comparison:**

| Operation | Before (Vec) | After (ZBytes) |
|-----------|-------------|----------------|
| Payload into cache | `payload.clone()` — memcpy N bytes | `zbytes.clone()` — atomic increment |
| Attachment into cache | (not cached — re-encoded on reply) | `attachment.clone()` — atomic increment |
| Cache reply to query | re-encode attachment + `Vec → ZBytes` conversion | direct `ZBytes` handoff |
| Lock | `RwLock::write().await` | Same (see § Lock-Free Option below) |

### Default Cache Size

Reduce from 10,000 to **256**. The cache now only needs to cover the store-lag
window (events published but not yet persisted by the EventStore). At 100K
msg/s with 1 ms store lag, ~100 events are in flight. 256 provides 2.5×
headroom.

`cache_size = 0` disables the cache entirely — pure store-only recovery.

### Lock-Free Option (Future)

Replace `RwLock<VecDeque<CachedSample>>` with `crossbeam::queue::ArrayQueue`.
The publisher pushes (MPSC-safe), the queryable task pops/peeks. This
eliminates the write-lock on the publish path. The queryable copies a snapshot
of the ring at query time.

Not required for the initial change — the `RwLock` overhead is now dwarfed by
the `session.put()` await. Can be added as an independent optimization.

---

## 2. Consumer Recovery Behavior

When a subscriber's `GapDetector` reports a gap, the subscriber must recover
the missing events. This section defines the exact recovery state machine.

### 2.1 Gap Detection Triggers

A gap can be detected in two ways:

| Trigger | How | Timing |
|---------|-----|--------|
| **Inline gap** | `GapDetector::on_sample()` returns `SampleResult::Gap(miss)` when an arriving event's seq > expected | Immediate — the triggering event is delivered first, then recovery spawns |
| **Heartbeat gap** | `GapDetector::on_heartbeat()` compares the publisher's advertised `partition_seqs` against local `last_seen` | Periodic — detected on heartbeat arrival (default 1s) |

Both produce a `MissInfo { source: PublisherId, partition: u32, missed: Range<u64> }`.

### 2.2 Recovery State Machine

```
                    ┌───────────────┐
                    │  Gap Detected │
                    └──────┬────────┘
                           │
                    ┌──────▼────────┐
              ┌─────│ Has Store?    │
              │     └──────┬────────┘
              │ No         │ Yes
              │     ┌──────▼────────────────┐
              │     │  Wait recovery_delay  │  (default: 50ms)
              │     │  to let store persist │
              │     └──────┬────────────────┘
              │            │
              │     ┌──────▼────────────────┐
              │     │  Query EventStore     │
              │     │  (publisher_id, part, │
              │     │   seq range)          │
              │     └──────┬────────────────┘
              │            │
              │       ┌────▼────┐
              │       │ Full?   │──── Yes ──> Done ✓
              │       └────┬────┘
              │            │ Partial / Empty
              │            │
              ├────────────┘
              │
       ┌──────▼──────────────────┐
       │  Query Publisher Cache  │
       │  (session.get _cache/)  │
       └──────┬──────────────────┘
              │
         ┌────▼────┐
         │ Full?   │──── Yes ──> Done ✓
         └────┬────┘
              │ Partial / Empty
              │
       ┌──────▼──────────────────┐
       │  Retry Store            │
       │  (backoff, up to        │
       │   max_recovery_attempts)│
       └──────┬──────────────────┘
              │
         ┌────▼────┐
         │ Full?   │──── Yes ──> Done ✓
         └────┬────┘
              │ Still missing
              │
       ┌──────▼──────────────────┐
       │  Irrecoverable Gap      │
       │  - log warning          │
       │  - invoke miss_handler  │
       │  - deliver what we have │
       └─────────────────────────┘
```

### 2.3 "Full?" Check — Tracking Recovered Sequences

The subscriber tracks which specific sequences within the `missed` range have
been recovered, not just a count. This handles out-of-order replies and
partial recovery correctly.

```rust
/// Tracks which sequences in a gap have been recovered.
struct RecoveryTracker {
    expected: Range<u64>,
    recovered: HashSet<u64>,
}

impl RecoveryTracker {
    fn new(missed: Range<u64>) -> Self {
        Self { expected: missed, recovered: HashSet::new() }
    }

    fn record(&mut self, seq: u64) {
        if self.expected.contains(&seq) {
            self.recovered.insert(seq);
        }
    }

    fn is_complete(&self) -> bool {
        self.recovered.len() == self.expected.len()
    }

    /// Sequences still missing after a recovery attempt.
    fn remaining(&self) -> Vec<u64> {
        self.expected.clone()
            .filter(|s| !self.recovered.contains(s))
            .collect()
    }
}
```

### 2.4 Recovery Implementation

```rust
/// Attempt to recover missed events using tiered sources.
///
/// Order: EventStore → Publisher Cache → Retry Store (with backoff).
/// Recovered events are delivered through `tx` as they arrive.
async fn recover_gap(
    session: &Session,
    config: &RecoveryConfig,
    miss: &MissInfo,
    tx: &flume::Sender<RawEvent>,
) -> Result<()> {
    let mut tracker = RecoveryTracker::new(miss.missed.clone());

    // ── Step 1: Query EventStore (if available) ──
    if let Some(store_prefix) = &config.store_key_prefix {
        // Brief delay to let the store persist in-flight events.
        tokio::time::sleep(config.recovery_delay).await;

        query_source(session, store_prefix, miss, tx, &mut tracker).await;
        if tracker.is_complete() {
            return Ok(());
        }
    }

    // ── Step 2: Query Publisher Cache ──
    // Covers the store-lag window and works without `"store"` feature.
    let cache_key = format!(
        "{}/_cache/{}?after_seq={}",
        config.key_prefix, miss.source, miss.missed.start
    );
    query_and_deliver(session, &cache_key, tx, &mut tracker).await;
    if tracker.is_complete() {
        return Ok(());
    }

    // ── Step 3: Retry Store with exponential backoff ──
    if let Some(store_prefix) = &config.store_key_prefix {
        for attempt in 1..config.max_recovery_attempts {
            let backoff = config.recovery_delay * 2u32.pow(attempt);
            tokio::time::sleep(backoff).await;

            query_source(session, store_prefix, miss, tx, &mut tracker).await;
            if tracker.is_complete() {
                return Ok(());
            }
        }
    }

    // ── Step 4: Irrecoverable ──
    let remaining = tracker.remaining();
    warn!(
        publisher = %miss.source,
        partition = miss.partition,
        remaining = ?remaining,
        "irrecoverable gap — {} events lost",
        remaining.len()
    );
    Err(Error::GapRecoveryFailed {
        publisher_id: miss.source,
        missed: miss.missed.clone(),
    })
}
```

### 2.5 Store Query Protocol

The subscriber queries the EventStore queryable with a selector that includes
the publisher ID and sequence range:

```
Key:    {prefix}/_store
Params: publisher_id={uuid}&after_seq={start-1}&before_seq={end}&partition={p}
```

This requires extending `QueryFilters` with a `publisher_id` field:

```rust
pub struct QueryFilters {
    pub after_seq: Option<u64>,
    pub before_seq: Option<u64>,
    pub publisher_id: Option<PublisherId>,  // NEW
    pub partition: Option<u32>,             // NEW (for multi-partition stores)
    pub limit: Option<usize>,
    // ... existing fields
}
```

The store's queryable handler (`run_queryable_task`) already parses selector
parameters and passes them to `backend.query()`. Adding `publisher_id` is a
straightforward extension to the filter chain.

```rust
/// Build the store recovery query selector.
fn store_recovery_selector(
    store_prefix: &str,
    miss: &MissInfo,
) -> String {
    format!(
        "{}?publisher_id={}&after_seq={}&before_seq={}&partition={}",
        store_prefix,
        miss.source,
        miss.missed.start.saturating_sub(1),
        miss.missed.end,
        miss.partition,
    )
}
```

### 2.6 Query Source Helper

Both store and cache queries share the same reply-processing logic:

```rust
/// Query a Zenoh queryable and deliver any matching events.
async fn query_and_deliver(
    session: &Session,
    selector: &str,
    tx: &flume::Sender<RawEvent>,
    tracker: &mut RecoveryTracker,
) {
    let replies = match session.get(selector).await {
        Ok(r) => r,
        Err(e) => {
            warn!("recovery query failed: {e}");
            return;
        }
    };

    while let Ok(reply) = replies.recv_async().await {
        match reply.result() {
            Ok(sample) => {
                let key = sample.key_expr().as_str();
                let payload_bytes = sample.payload().to_bytes().to_vec();

                match sample.attachment().and_then(|a| decode_metadata(a).ok()) {
                    Some(meta) if tracker.expected.contains(&meta.seq) => {
                        // Only deliver events that are part of the gap.
                        // Prevents double-delivery if the same seq was already
                        // recovered from a previous source.
                        if tracker.recovered.contains(&meta.seq) {
                            trace!(seq = meta.seq, "already recovered, skipping");
                            continue;
                        }
                        deliver_event(tx, meta.pub_id, meta.seq, key, &payload_bytes,
                                      meta.event_id, meta.timestamp);
                        tracker.record(meta.seq);
                    }
                    Some(_) => {
                        // seq outside the gap range — ignore
                    }
                    None => {
                        warn!("recovery reply without valid attachment, skipping");
                    }
                }
            }
            Err(err) => {
                warn!("recovery reply error: {}",
                      err.payload().try_to_string().unwrap_or_default());
            }
        }
    }
}
```

### 2.7 Deduplication on Recovery

Recovered events feed back into the subscriber's delivery channel, but the
`GapDetector` has already advanced `last_seen` past the gap (it saw the
triggering event that caused the gap). So recovered events arrive with
`seq < last_seen` — they would be classified as `Duplicate` if re-processed
through `on_sample()`.

**Solution:** Recovered events bypass the `GapDetector` entirely. They are
delivered directly to the `event_rx` channel. The `RecoveryTracker` (§ 2.3)
prevents double-delivery within a single recovery attempt. Cross-attempt
dedup is handled by checking `tracker.recovered.contains(&seq)` before
delivering.

If the subscriber has a `SequenceCheckpoint` (Phase 4), recovered events are
recorded there to prevent re-recovery after restart.

### 2.8 Concurrent Recovery Isolation

Multiple gaps can be detected simultaneously (from different publishers or
different partitions). Each gap spawns its own `tokio::spawn(recover_gap(...))`
task with its own `RecoveryTracker`.

**Concern:** Two concurrent recoveries for overlapping ranges from the same
publisher. This can happen if a heartbeat detects a wider gap while an inline
recovery is still in progress.

**Mitigation:** The subscriber can maintain a
`HashSet<(PublisherId, u32)>` of in-flight recoveries. Before spawning a new
recovery, check if one is already running for the same `(publisher, partition)`.
If so, skip or merge the ranges.

```rust
/// Guard against concurrent overlapping recoveries.
fn should_spawn_recovery(
    in_flight: &HashSet<(PublisherId, u32)>,
    miss: &MissInfo,
) -> bool {
    !in_flight.contains(&(miss.source, miss.partition))
}
```

---

## 3. Configuration

### New Fields

```rust
pub struct EventBusConfig {
    // ... existing fields ...

    /// Publisher in-memory cache size.
    /// 0 = disabled (store-only recovery).
    /// Default: 256 (covers ~2.5ms of store-lag at 100K msg/s).
    pub cache_size: usize,

    /// Delay before the first store recovery query, giving the EventStore
    /// time to persist in-flight events.
    /// Default: 50ms.
    pub recovery_delay: Duration,

    /// Maximum number of store recovery attempts (with exponential backoff)
    /// before declaring a gap irrecoverable.
    /// Default: 3.
    pub max_recovery_attempts: u32,
}
```

### RecoveryConfig (internal)

Extracted from `EventBusConfig` for the recovery function:

```rust
struct RecoveryConfig {
    key_prefix: String,
    store_key_prefix: Option<String>,  // None when feature = "store" is off
    recovery_delay: Duration,
    max_recovery_attempts: u32,
}
```

---

## 4. Timeline: What Happens When an Event Goes Missing

Concrete scenario: publisher P1 publishes seq 0–10, subscriber misses seq 5.

```
Time ─────────────────────────────────────────────────────────>

Publisher P1          EventStore             Subscriber
 │                      │                      │
 │ put(seq=4)           │                      │
 │ ────────────────────>│ store(4)             │
 │ ─────────────────────────────────────────────> recv seq=4  ✓
 │                      │                      │
 │ put(seq=5)           │                      │
 │ ────────────────────>│ store(5)             │
 │ ────────────── X ────────────────────────────> (lost in transit)
 │                      │                      │
 │ put(seq=6)           │                      │
 │ ────────────────────>│ store(6)             │
 │ ─────────────────────────────────────────────> recv seq=6
 │                      │                      │ GapDetector: expected 5, got 6
 │                      │                      │   → Gap([5..6))
 │                      │                      │   → deliver seq=6
 │                      │                      │   → spawn recover_gap
 │                      │                      │
 │                      │                      │ [wait 50ms recovery_delay]
 │                      │                      │
 │                      │   <───────────────────│ session.get("_store?publisher_id=P1
 │                      │                      │   &after_seq=4&before_seq=6&partition=0")
 │                      │   ───────────────────>│ reply: seq=5 from fjall
 │                      │                      │ deliver seq=5 ✓
 │                      │                      │ RecoveryTracker: complete
```

If the store hadn't persisted seq 5 yet (store-lag), the flow continues:

```
 │                      │                      │ store returned 0 events
 │                      │                      │
 │ ┌─────────────┐      │                      │
 │ │ ZBytes cache│ <────────────────────────────│ session.get("_cache/P1?after_seq=5")
 │ │ [..., 5, 6] │ ────────────────────────────>│ reply: seq=5 from cache
 │ └─────────────┘      │                      │ deliver seq=5 ✓
```

---

## 5. Alternatives Considered

| Alternative | Why not default |
|-------------|-----------------|
| **Store-only (no cache)** | Cannot cover the store-lag window without retries; set `cache_size=0` to opt in |
| **Publisher write-through (WAL)** | Adds disk I/O to every publish; appropriate for `feature="wal"` (Strategy D in [03_durability.md](03_durability.md)) |
| **NACK re-publish** | Publisher must be online; complex dedup; good for multicast fan-out but not the common case |
| **Per-event ACK from store** | O(N × event_rate) network cost; watermark batching is already better |

---

## 6. Migration Path

Each step is independently shippable and backwards-compatible:

1. **Extend `QueryFilters` with `publisher_id` and `partition`.**
   Pure additive — existing queries still work.

2. **Change `recover_gap()` to try store first, then cache.**
   Biggest reliability improvement. Existing cache still works as fallback.

3. **Switch `CachedSample::payload` from `Vec<u8>` to `ZBytes`.**
   Eliminates memcpy on every publish. Cache queryable replies become
   zero-copy.

4. **Add `recovery_delay`, `max_recovery_attempts` to config.**
   With defaults matching current behavior (0ms delay, 1 attempt = today).

5. **Reduce default `cache_size` from 10,000 to 256.**
   Cache is now a fallback, not the primary recovery source.

6. **(Optional) Replace `RwLock<VecDeque>` with lock-free ring.**
   Only if profiling shows the lock is a bottleneck after the ZBytes change.
