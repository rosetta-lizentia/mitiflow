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

Zenoh core has no per-publisher sequence numbering. mitiflow assigns a monotonic `u64` per (partition, publisher) and encodes metadata in a **50-byte binary attachment header**:

```
[seq: u64 LE][publisher_id: UUID 16 bytes][event_id: UUID 16 bytes][timestamp_ns: i64 LE][urgency_ms: u16 LE]
```

The payload contains only the serialized `T` — metadata travels out-of-band. Encoding/decoding in `src/attachment.rs` (`encode_attachment()` / `decode_attachment()`).

### 2.2 Gap Detection (Subscriber-Side Tracking)

The subscriber tracks the last-seen sequence per (publisher, partition) via the `SequenceTracker` trait (impl: `GapDetector` in `subscriber/gap_detector.rs`):

- **Normal:** `seq == expected` → deliver
- **Gap:** `seq > expected` → trigger recovery for missed `[expected..seq)`
- **Duplicate:** `seq < expected` → drop

### 2.3 Publisher Cache + Recovery (via Queryable)

Each publisher declares a queryable on `{prefix}/_cache/{publisher_id}` serving cached recent samples from a bounded `VecDeque<CachedSample>`. On gap detection, the subscriber queries this cache first, then falls back to the Event Store if present.

See [09_cache_recovery_design.md](09_cache_recovery_design.md) for the tiered recovery design (publisher cache → Event Store).

### 2.4 Heartbeat (Periodic Sequence Beacon)

The publisher periodically publishes a `HeartbeatPayload` to `{prefix}/_heartbeat/{publisher_id}` containing its per-partition sequence map (`partition_seqs: HashMap<u32, u64>`) plus publisher metadata.

Subscribers use heartbeats to detect gaps in otherwise quiet streams — if the publisher reports a higher sequence than the subscriber last saw, it triggers recovery.

### 2.5 History on Late Join (via Get)

When a new subscriber starts, it can query the Event Store for historical events via `session.get("{store_prefix}/**")`. This replaces `zenoh_ext`'s `history()` config — instead of querying publisher caches (bounded, in-memory), we query the Event Store (durable, unbounded).

### 2.6 Presence Detection (Liveliness)

Publishers declare liveliness tokens at `{prefix}/_publishers/{publisher_id}`. The `PartitionManager` and `MembershipTracker` watch liveliness tokens to detect join/leave events for consumer groups and partition rebalancing.

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
