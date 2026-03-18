# Durability Strategies

How `mitiflow` ensures events are never lost, without requiring every producer to maintain local storage.

---

## The Problem

Zenoh's reliability pipeline is in-memory. If the producer process crashes after `put()` returns but before any subscriber has durably stored the event, the event is lost.

```
Producer                     Zenoh Transport              Event Store
   │                              │                           │
   │ 1. put(payload, Block)       │                           │
   │ ─────────────────────────────>                           │
   │                              │ 2. reliable delivery      │
   │                              │ ──────────────────────────>
   │                              │                           │ 3. persist (fsync)
   │                              │                           │
   │          DURABILITY GAP: steps 1→3 are not atomic        │
```

---

## Strategy Matrix

| Strategy | Guarantee | Latency | Producer State | Complexity |
|----------|-----------|---------|---------------|------------|
| **A. Accept the gap** | At-least-once, best-effort durable | Lowest (~µs) | None | None |
| **B. Watermark stream** ⭐ | Confirmed durable | +batch interval (~100ms) | None | Low |
| **C. Inbox pattern** | Durable before broadcast | +1 write | None | Medium |
| **D. Producer WAL** | Crash-proof durable | +1 fsync (~ms) | fjall dir | Medium |

---

## Strategy A: Accept the Gap

Use `CongestionControl::Block` + publisher cache + Event Store recovery.

The only loss scenario: **producer crash while Event Store is simultaneously unreachable**. The window is microseconds wide.

**Use for:** UI updates, trace steps, status broadcasts, telemetry — any event where rare loss is acceptable.

---

## Strategy B: Watermark Stream ⭐

The primary innovation. The Event Store publishes batched commit progress; publishers subscribe and block until covered.

### Protocol

```
Time ──────────────────────────────────────────────────────>

Producer        Event Store Watermark
  │                  │
  │ put(seq=100)     │
  │─────────────────>│ persists seq 100
  │                  │
  │ put(seq=101)     │
  │─────────────────>│ persists seq 101
  │                  │
  │                  │ [100ms tick]
  │     watermark    │ put(watermark, {committed: 101, gaps: []})
  │<─────────────────│
  │                  │
  │ "seq 100 ≤ 101   │
  │  && 100 ∉ gaps"  │
  │ → DURABLE ✓      │
```

### CommitWatermark Type

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitWatermark {
    /// Highest contiguous sequence durably stored
    pub committed_seq: u64,
    /// Sequences below committed_seq that are still missing
    pub gaps: Vec<u64>,
    /// Event Store wall-clock time
    pub timestamp: chrono::DateTime<chrono::Utc>,
}
```

### Event Store Side

```rust
impl EventStore {
    async fn watermark_loop(&self) {
        let mut interval = tokio::time::interval(self.config.watermark_interval); // 100ms
        loop {
            interval.tick().await;
            let wm = CommitWatermark {
                committed_seq: self.backend.committed_seq(),
                gaps: self.backend.gap_sequences(),
                timestamp: chrono::Utc::now(),
            };
            self.session
                .put(&self.watermark_key, serde_json::to_vec(&wm).unwrap())
                .congestion_control(CongestionControl::Block)
                .await
                .ok();
        }
    }
}
```

### Publisher Side

```rust
impl EventPublisher {
    pub async fn publish_durable<T: Serialize>(&self, event: &Event<T>) -> Result<()> {
        let seq = self.publish(event).await?;  // fast path
        
        tokio::time::timeout(self.config.durable_timeout, self.wait_for_watermark(seq))
            .await
            .map_err(|_| Error::DurabilityTimeout { seq })?
    }
    
    async fn wait_for_watermark(&self, target: u64) -> Result<()> {
        loop {
            let wm = self.watermark_rx.recv_async().await?;
            if wm.committed_seq >= target && !wm.gaps.contains(&target) {
                return Ok(());
            }
        }
    }
}
```

### Why Watermark > Per-Event ACK

| Aspect | Watermark Stream | Per-Event Query ACK |
|--------|-----------------|---------------------|
| Network cost | 1 broadcast / batch interval | 1 query + 1 reply per event |
| Publisher latency | Batch interval (configurable) | 1 full RTT per event |
| Event Store load | 1 `put()` per batch | 1 queryable handler per event |
| Scales with publishers | O(1) | O(N × event_rate) |

### Gap Handling

If the Event Store's subscriber detects a missed sequence (via mitiflow's gap detector), it includes those in `gaps[]`. The publisher sees its sequence in the gap list and continues waiting. Once the Event Store recovers the gap (via `session.get()` to the publisher's cache queryable), the next watermark clears it.

If the gap is **irrecoverable** (publisher cache evicted), the Event Store's miss handler can:
1. Log the gap via tracing
2. Keep it in `gaps[]` permanently (publisher eventually times out with `DurabilityTimeout`)
3. Optionally alert via a separate Zenoh topic

---

## Strategy C: Inbox Pattern

The producer writes to an inbox key expression that the Event Store subscribes to and persists **before** re-publishing to the main event stream.

```
Producer → put("myapp/inbox/{event_id}", payload) → Event Store persists first
                                                   → Event Store re-publishes to "myapp/events/..."
```

The Event Store becomes the authoritative publisher. Strongest guarantee, but changes the publish flow:
- Events go through store before consumers see them
- Adds write latency
- Event Store is on the critical path for all publishes

**Use for:** financial transactions, audit trails — when zero loss is non-negotiable and the extra hop is acceptable.

---

## Strategy D: Producer WAL (Optional Feature)

For truly crash-proof publishing, embed a local WAL in the producer:

```rust
pub struct DurablePublisher {
    inner: EventPublisher,
    wal: fjall::Keyspace,
}

impl DurablePublisher {
    pub async fn publish<T: Serialize>(&self, event: &Event<T>) -> Result<()> {
        let seq = self.wal.append(&event)?;   // fsync
        self.inner.publish(event).await?;      // to Zenoh
        Ok(())
    }
    
    /// On startup, re-publish any events that weren't confirmed
    pub async fn recover(&self) -> Result<usize> {
        let unpublished = self.wal.replay_from(self.last_confirmed_seq()?)?;
        for event in unpublished {
            self.inner.publish(&event).await?;
        }
        Ok(unpublished.len())
    }
}
```

Enabled via `feature = "wal"`.

**Use for:** regulatory requirements where "event in WAL = legally committed."

---

## Recommended Defaults

| Event Criticality | Strategy | Method |
|-------------------|----------|--------|
| Telemetry, UI updates | A (Accept gap) | `publisher.publish(&event)` |
| Business events | B (Watermark) | `publisher.publish_durable(&event)` |
| Financial / audit | C or D | Inbox or WAL |

---

## Tuning Knobs

| Parameter | Default | Effect |
|-----------|---------|--------|
| `watermark_interval` | 100ms | Lower = faster confirmation, more network traffic |
| `durable_timeout` | 5s | How long `publish_durable()` waits before error |
| `cache_size` | 10,000 | Larger = longer recovery window, more memory |
| `heartbeat` | Sporadic(1s) | Faster = quicker gap detection, more traffic |
