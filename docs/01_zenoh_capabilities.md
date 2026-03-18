# Zenoh Stable Foundation

What Zenoh (v1.x) provides via its **stable** core APIs, and what mitiflow builds on top.

> **Note:** Zenoh's `zenoh_ext` crate offers `AdvancedPublisher`/`AdvancedSubscriber` with built-in sequencing and recovery, but these are currently marked **unstable**. mitiflow intentionally avoids `zenoh_ext` and implements the reliability layer itself using only stable APIs.

---

## 1. Stable APIs Used by mitiflow

| API | Stability | How mitiflow Uses It |
|-----|-----------|---------------------|
| `session.declare_publisher()` | ✅ Stable | Core event publishing |
| `session.declare_subscriber()` | ✅ Stable | Core event consumption |
| `session.declare_queryable()` | ✅ Stable | Event Store query serving, publisher cache |
| `session.get()` | ✅ Stable | Recovery queries, history replay, Event Store queries |
| `session.liveliness()` | ✅ Stable | Worker presence detection, partition rebalancing |
| `put().attachment()` | ✅ Stable | Event metadata (sequence numbers, publisher ID) |
| `CongestionControl::Block` | ✅ Stable | Backpressure (prevents silent message drop) |

---

## 2. What mitiflow Builds on Top

### 2.1 Sequence Numbers (via Attachments)

Zenoh core has no per-publisher sequence numbering. mitiflow assigns a monotonic `u64` per publisher and attaches it to every sample:

```rust
// Publisher side
let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
let attachment = Attachment::new()
    .insert("seq", seq.to_be_bytes())
    .insert("pub_id", self.publisher_id.as_bytes());

self.publisher
    .put(payload)
    .attachment(attachment)
    .congestion_control(CongestionControl::Block)
    .await?;
```

### 2.2 Gap Detection (Subscriber-Side Tracking)

The subscriber maintains a `HashMap<PublisherId, u64>` tracking the last-seen sequence per publisher:

```rust
// Subscriber side
fn on_sample(&mut self, sample: Sample) -> Option<MissInfo> {
    let (pub_id, seq) = extract_metadata(&sample);
    let expected = self.last_seen.get(&pub_id).map(|s| s + 1).unwrap_or(0);

    if seq > expected {
        // GAP: missed sequences [expected..seq)
        let miss = MissInfo { source: pub_id, missed: expected..seq };
        self.last_seen.insert(pub_id, seq);
        return Some(miss);
    }

    if seq <= expected.saturating_sub(1) {
        // DUPLICATE: already seen (dedup)
        return None; // skip
    }

    self.last_seen.insert(pub_id, seq);
    None // normal delivery
}
```

### 2.3 Publisher Cache + Recovery (via Queryable)

Each publisher declares a Queryable on a cache key expression. On gap detection, the subscriber queries the publisher's cache:

```rust
// Publisher side — cache recent samples
let queryable = session
    .declare_queryable(&format!("{}/cache/{}", prefix, pub_id))
    .await?;

// Background task: answer cache queries
while let Ok(query) = queryable.recv_async().await {
    let params = query.parameters(); // e.g., ?after_seq=42
    let after_seq: u64 = params.get("after_seq").parse().unwrap_or(0);
    for cached in self.cache.iter_from(after_seq) {
        query.reply(&cached.key, &cached.payload).await?;
    }
}
```

```rust
// Subscriber side — recover on gap
async fn recover(&self, pub_id: &str, from_seq: u64) -> Result<Vec<Sample>> {
    let replies = self.session
        .get(&format!("{}/cache/{}?after_seq={}", self.prefix, pub_id, from_seq))
        .await?;
    // Process recovered samples
}
```

### 2.4 Heartbeat (Periodic Sequence Beacon)

The publisher periodically publishes its current sequence number so subscribers can detect stale connections:

```rust
// Publisher background task
async fn heartbeat_loop(&self) {
    let mut interval = tokio::time::interval(self.config.heartbeat_interval); // 1s
    loop {
        interval.tick().await;
        let beacon = HeartbeatBeacon {
            pub_id: self.publisher_id,
            current_seq: self.next_seq.load(Ordering::SeqCst) - 1,
            timestamp: chrono::Utc::now(),
        };
        self.session
            .put(&self.heartbeat_key, serde_json::to_vec(&beacon).unwrap())
            .await
            .ok();
    }
}
```

Subscriber uses heartbeats to detect gaps in otherwise quiet streams — if the publisher says `current_seq: 50` but the subscriber last saw `seq: 45`, it triggers recovery.

### 2.5 History on Late Join (via Get)

When a new subscriber starts, it queries the Event Store for historical data:

```rust
// On subscriber init
async fn fetch_history(&self) -> Result<Vec<Event>> {
    let replies = self.session
        .get(&format!("{}/**?from_seq=0", self.store_prefix))
        .await?;
    // Returns stored events from the Event Store's Queryable
}
```

This replaces `zenoh_ext`'s `history()` config — instead of querying publisher caches (which are bounded and in-memory), we query the Event Store (which is durable and unbounded).

### 2.6 Presence Detection (Liveliness)

```rust
// Publisher declares presence
let token = session.liveliness()
    .declare_token(&format!("{}/publishers/{}", prefix, pub_id))
    .await?;

// Subscriber/PartitionManager watches for join/leave
let watcher = session.liveliness()
    .declare_subscriber(&format!("{}/publishers/*", prefix))
    .await?;
```

---

## 3. What Stable Zenoh Does NOT Provide

| Feature | Status | mitiflow Layer |
|---------|--------|---------------|
| Per-publisher sequence numbers | ❌ Not in core | L1 — via `put().attachment()` |
| Gap detection | ❌ Not in core | L1 — subscriber-side seq tracking |
| Automatic retransmission | ❌ Not in core | L1 — publisher cache Queryable + subscriber `get()` |
| Heartbeat/liveness | ❌ Not in core | L1 — periodic seq beacon |
| Consumer groups | ❌ Not in core | L3 — PartitionManager + liveliness |
| Durable persistence | ❌ Not in core | L2 — Event Store sidecar |
| Cross-restart dedup | ❌ Not in core | L4 — SequenceCheckpoint |
| Dead letter queue | ❌ Not in core | L5 — DLQ routing |

---

## 4. CongestionControl

- `CongestionControl::Block` — `put()` blocks until the transport layer accepts the message. Guarantees no silent drop at the publisher side.
- `CongestionControl::Drop` — drops on congestion. Faster but lossy.

mitiflow defaults to `Block` for all event publishing.

---

## 5. Why Not Just Wait for `zenoh_ext` to Stabilize?

1. **Timeline unknown** — unstable APIs may take years to stabilize, or may change significantly
2. **Our needs are specific** — `zenoh_ext`'s recovery is designed for generic pub/sub. mitiflow needs event-streaming-specific behavior (watermark integration, Event Store as recovery source, partition-aware recovery)
3. **Event Store supersedes publisher cache** — `zenoh_ext` recovers from publisher's in-memory cache (bounded, lost on crash). mitiflow recovers from the Event Store (durable, queryable, unbounded)
4. **Full control over dedup** — `zenoh_ext` tracks sequences in-memory only. mitiflow can persist checkpoints for cross-restart dedup
5. **Simpler dependency** — just `zenoh` core, no additional unstable crate
