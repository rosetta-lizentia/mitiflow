# Slow Consumer Offload: Automatic Pub/Sub → Query Demotion

How mitiflow handles consumers that fall behind the live stream — by
transparently switching from real-time pub/sub to store-based query replay,
without back-pressuring the publisher or losing events.

---

## The Problem

Zenoh's flow control operates at the **transport level** and offers only two
choices per publisher declaration:

| Mode | Behavior | Problem for slow consumers |
|------|----------|--------------------------|
| `CongestionControl::Block` | Publisher blocks when any downstream transport buffer fills | One slow consumer stalls the publisher and all other consumers |
| `CongestionControl::Drop` | Zenoh silently drops messages for congested links | Slow consumer loses events with no recovery signal |

Both are unacceptable for event streaming:

- **Block** violates a fundamental streaming property: independent consumers
  should not affect each other's throughput. A 10ms-per-event consumer should
  never slow down a nanosecond-latency subscriber reading the same topic.
- **Drop** breaks at-least-once delivery. The consumer has no way to know what
  it missed (the drop happens in the Zenoh transport before mitiflow's gap
  detector even sees a sample).

The deeper issue is that **Zenoh's flow control is publisher-scoped, not
subscriber-scoped**. There is no per-subscriber backpressure channel —
congestion on one link affects the `put()` call seen by all subscribers.

### What We Want

A slow consumer should:

1. **Never back-pressure the publisher** — even with `CongestionControl::Block`
2. **Never silently lose events** — even under sustained lag
3. **Automatically catch up** using the durable Event Store as a secondary source
4. **Resume live pub/sub** once caught up — with no duplicate processing
5. **Require zero manual intervention** — the consumer detects and handles lag autonomously

---

## Constraint Space

| Dimension | Requirement |
|-----------|------------|
| Correctness | Every event is delivered exactly once (modulo application-level dedup) |
| Ordering | Per-(publisher, partition) order is preserved |
| Latency (normal) | Sub-millisecond — live pub/sub path, no store round-trip |
| Latency (catch-up) | Bounded by store query throughput, not by pub rate |
| Publisher impact | Zero — publisher never observes the slow consumer |
| Complexity (user) | `recv()` API unchanged — offload is transparent |
| Complexity (impl) | Fits within existing subscriber + store abstractions |
| Store dependency | Requires an Event Store for the topic (graceful degradation if absent) |

---

## Design: Three-State Consumer

The consumer operates in one of three states:

```
                    ┌──────────────────────────────────────────┐
                    │                                          │
                    ▼                                          │
            ┌──────────────┐                                   │
            │              │   lag > threshold                 │
    ────────│    LIVE      │──────────────────┐                │
   (start)  │  (pub/sub)   │                  │                │
            │              │                  ▼                │
            └──────────────┘          ┌──────────────┐         │
                    ▲                 │              │         │
                    │                 │   DRAINING   │         │
                    │                 │  (buffered)  │         │
                    │                 │              │         │
                    │                 └──────┬───────┘         │
                    │                        │                 │
                    │                        ▼                 │
                    │                 ┌──────────────┐         │
                    │   caught up     │              │         │
                    └─────────────────│   CATCHING   │         │
                                      │    UP        │─────────┘
                                      │  (queries)   │  still behind
                                      │              │  after query batch
                                      └──────────────┘
```

### State: LIVE

Normal operation. The consumer receives events via Zenoh pub/sub with
sub-millisecond latency. The gap detector tracks per-(publisher, partition)
sequences. Events flow through the bounded flume channel (capacity 1024) into
the application's `recv()` loop.

**Transition → DRAINING:** When lag exceeds a configurable threshold.

### State: DRAINING

Transitional state. The consumer has decided to offload but must first drain
events already buffered in its flume channel and Zenoh transport buffers. During
DRAINING:

- The Zenoh subscriber is still active (receiving into channel)
- The consumer processes remaining buffered events
- Once the channel is empty, the consumer records its position and transitions

**Transition → CATCHING_UP:** When the buffer is fully drained. The consumer
records the last-delivered sequence per (publisher, partition) as the
**switchover cursor**.

### State: CATCHING_UP

The consumer unsubscribes from Zenoh pub/sub and reads events from the Event
Store via `session.get()` queries. Events are returned in deterministic order
(HLC-sorted per the replay protocol in [04_sequencing_and_replay.md](04_sequencing_and_replay.md)).

The consumer queries in batches, advancing its cursor after each batch.

**Transition → LIVE:** When the consumer's cursor is within the **re-subscribe
threshold** of the store's committed watermark (i.e., it has nearly caught up).

**Transition → CATCHING_UP (loop):** If the consumer finishes a batch but is
still behind, it queries the next batch.

---

## Challenge Breakdown

### Challenge 1: Detecting Lag Accurately

**Problem:** How does the consumer know it's "too far behind"? Possible signals:

- **Channel fullness:** The bounded flume channel is near capacity (e.g., >80%)
- **Heartbeat delta:** Publisher heartbeat reports `current_seq=10000` but
  consumer last processed `seq=5000`
- **Watermark delta:** The store's committed watermark is far ahead of the
  consumer's position
- **Wall-clock lag:** Time between event timestamp and consumer processing time

Each signal has failure modes:

| Signal | False positives | False negatives |
|--------|----------------|-----------------|
| Channel fullness | Transient burst fills channel briefly | Slow but steady consumer keeps channel partially full |
| Heartbeat delta | Publisher batch-publishes (large seq jump ≠ real lag) | Heartbeat interval masks slow drift |
| Watermark delta | Watermark bunches commits (delta spikes on batch) | Watermark not available without store |
| Wall-clock lag | Clock skew between publisher and consumer | None (most reliable if clocks are synced) |

**Solution: Composite lag score.** Combine multiple signals into a single lag
estimate that must exceed the threshold for a sustained duration (debounce
window) before triggering offload:

```rust
struct LagDetector {
    /// Pending items in the delivery channel.
    channel_len: usize,
    channel_capacity: usize,

    /// Per-(publisher, partition) sequence gap relative to heartbeat.
    heartbeat_lag: HashMap<(PublisherId, u32), u64>,

    /// Configuration thresholds.
    config: OffloadConfig,

    /// Debounce: lag must exceed threshold for this long before triggering.
    sustained_since: Option<Instant>,
}

impl LagDetector {
    fn is_lagging(&mut self) -> bool {
        let channel_ratio = self.channel_len as f64 / self.channel_capacity as f64;
        let max_seq_lag = self.heartbeat_lag.values().copied().max().unwrap_or(0);

        let lagging = channel_ratio > self.config.channel_fullness_threshold  // e.g., 0.8
            || max_seq_lag > self.config.seq_lag_threshold;                    // e.g., 10_000

        match (lagging, self.sustained_since) {
            (true, None) => {
                self.sustained_since = Some(Instant::now());
                false // not sustained yet
            }
            (true, Some(since)) => {
                since.elapsed() > self.config.debounce_window // e.g., 2s
            }
            (false, _) => {
                self.sustained_since = None; // reset
                false
            }
        }
    }
}
```

**Debounce** prevents flapping on transient bursts. A consumer that spikes to
80% channel usage for 500ms during a burst but recovers should not offload.

### Challenge 2: Clean Switchover (LIVE → CATCHING_UP)

**Problem:** When the consumer unsubscribes from pub/sub and switches to store
queries, there is a window where events might be:

- In the Zenoh transport buffer (received by Zenoh but not yet in flume channel)
- In the flume channel (received but not yet processed)
- Published but not yet stored (store lag)

If the consumer simply unsubscribes and queries the store from its last-seen
sequence, it may miss events in the transport buffer — or re-process events
that are in both the buffer and the store.

**Solution: DRAINING state with overlap-safe cursor.**

```
Timeline:
  ──────────────────────────────────────────────────>
  
  [LIVE events]     [DRAINING]            [CATCHING_UP queries]
  ......seq 5000    5001..5023 (in buffer) 5024... (from store)
                    ↑                      ↑
                    trigger detected        query starts here
                    
  cursor = max(last_delivered_seq) per (pub, partition) at drain-complete
```

1. **Lag triggers.** The consumer enters DRAINING state.
2. **Drain the channel.** Continue calling `recv()` until the channel is empty
   ***and*** a brief quiet period confirms no in-flight samples.
3. **Record switchover cursor.** For each (publisher, partition), record the
   last delivered sequence number. This is the consumer's exact position.
4. **Unsubscribe.** Drop the Zenoh subscriber declaration. The consumer is now
   disconnected from the live stream.
5. **Query from cursor.** Ask the store for events where
   `seq > cursor[pub_id][partition]` for each publisher/partition.

**Dedup guarantee:** Events are keyed by `(publisher_id, partition, seq)`. If
the store returns an event the consumer already processed during DRAINING, the
consumer skips it (simple set-membership check against the cursor map). Since
sequences are monotonic, this is a single comparison: `if seq <= cursor → skip`.

### Challenge 3: Efficient Store Catch-Up

**Problem:** A consumer that is 1 million events behind cannot issue a single
query for all of them — this would:

- Overwhelm the store's query handler
- Consume unbounded memory building the response
- Block the consumer on a single long-running query

**Solution: Batched query with adaptive sizing.**

```rust
struct CatchUpReader {
    session: Session,
    store_key: String,
    cursors: HashMap<(PublisherId, u32), u64>,
    batch_size: usize,      // events per query (default: 10_000)
    config: OffloadConfig,
}

impl CatchUpReader {
    async fn next_batch(&mut self) -> Result<Vec<RawEvent>> {
        // Query store with current cursors, limited to batch_size
        let filters = QueryFilters {
            after_cursors: self.cursors.clone(),
            limit: Some(self.batch_size),
            order: ReplayOrder::Hlc, // deterministic cross-replica order
        };

        let events = self.query_store(&filters).await?;

        // Advance cursors based on returned events
        for event in &events {
            let key = (event.meta.publisher_id, event.meta.partition);
            let entry = self.cursors.entry(key).or_insert(0);
            *entry = (*entry).max(event.meta.seq);
        }

        Ok(events)
    }
}
```

**Adaptive batch size:** If a batch completes quickly (store is fast), increase
the batch size. If it takes too long, decrease. This auto-tunes to the store's
throughput capacity.

```rust
fn adjust_batch_size(&mut self, batch_duration: Duration) {
    if batch_duration < self.config.target_batch_duration / 2 {
        self.batch_size = (self.batch_size * 2).min(self.config.max_batch_size);
    } else if batch_duration > self.config.target_batch_duration * 2 {
        self.batch_size = (self.batch_size / 2).max(self.config.min_batch_size);
    }
}
```

### Challenge 4: Re-Subscribing Without Gaps or Duplicates

**Problem:** When the consumer catches up and re-subscribes to live pub/sub,
there is another switchover window. Events published between "last store query
returned" and "Zenoh subscriber declared" could be missed.

**Solution: Overlap window with dedup.**

```
Timeline:
  ──────────────────────────────────────────────────>
  
  [store queries]  [overlap]  [LIVE pub/sub]
  .......seq 9990  9991-10005 (from both store + sub)
                   ↑           ↑
                   re-subscribe store catch-up confirms ≤ watermark
                   here        subscriber sees 10001+
```

1. **Re-subscribe first, keep querying.** Before the final store batch completes,
   declare the Zenoh subscriber. Events now arrive via both store queries and
   the live subscriber.
2. **Dedup via cursor.** The consumer's cursor map tracks the highest delivered
   sequence per (publisher, partition). Events from the Zenoh subscriber with
   `seq <= cursor` are dropped (already delivered from store).
3. **Transition to LIVE** once the store query returns no new events (consumer
   is caught up) and the cursor matches or exceeds the watermark.

The overlap window is intentionally conservative — it's better to receive
duplicates (which are cheaply filtered) than to miss events.

```rust
async fn transition_to_live(&mut self) -> Result<()> {
    // Step 1: Re-subscribe to the Zenoh topic.
    //         Events start arriving into a holding buffer.
    let subscriber = self.session.declare_subscriber(&self.key_expr).await?;

    // Step 2: Run final store catch-up queries.
    //         This closes the gap between last query and re-subscribe time.
    loop {
        let batch = self.catch_up.next_batch().await?;
        if batch.is_empty() {
            break; // fully caught up with store
        }
        for event in batch {
            self.deliver(event).await?;
        }
    }

    // Step 3: Drain the holding buffer, deduplicating against cursors.
    //         Events with seq <= cursor are skipped.
    while let Ok(sample) = subscriber.try_recv() {
        let meta = extract_meta(&sample);
        if !self.is_already_delivered(&meta) {
            self.deliver_sample(sample).await?;
        }
    }

    // Step 4: Enter LIVE state.
    //         From now on, all events come from pub/sub.
    self.state = ConsumerState::Live { subscriber };
    Ok(())
}
```

### Challenge 5: Consumer Group Coordination

**Problem:** In a consumer group, partitions are assigned to specific members.
If a member offloads to store queries, the partition assignment must remain
stable — the group should not rebalance just because a member is reading from
the store instead of pub/sub.

**Solution: Offload is local to the consumer, invisible to the group.**

- The consumer group protocol uses **liveliness tokens** for membership. As long
  as the offloaded consumer maintains its liveliness token, the group sees it as
  alive and does not re-assign its partitions.
- The offloaded consumer continues to process events (from the store) and commit
  offsets normally.
- From the group's perspective, the member is simply "slow" — offload is an
  internal optimization, not a membership event.

**Edge case:** If the consumer is so slow that it cannot keep up even with store
queries, it should eventually be expelled from the group (via heartbeat timeout).
This is the existing behavior — a consumer that stops committing offsets within
`session_timeout` is evicted.

### Challenge 6: Store Availability During Catch-Up

**Problem:** The consumer relies on the Event Store for catch-up. If the store
is unavailable (crashed, partitioned, compacted past the consumer's cursor),
the consumer is stuck.

**Solution: Tiered fallback with clear error semantics.**

| Store state | Behavior |
|-------------|----------|
| Available, has data | Normal catch-up via batched queries |
| Available, data compacted | Consumer receives a `CompactedPastCursor` error; must reset to earliest available offset or fail |
| Temporarily unavailable | Retry with exponential backoff (same as gap recovery) |
| Permanently unavailable | After max retries, emit `OffloadFailed` error; consumer can choose to re-subscribe at current position (accepting a gap) or shut down |

If the store has compacted past the consumer's cursor (log compaction in
key-based topics), the catch-up reader returns a well-typed error:

```rust
enum CatchUpError {
    /// Store has data but consumer's cursor is within range. Normal.
    /// (not an error — included for completeness)

    /// The store has compacted past the consumer's requested position.
    /// `earliest_available` is the oldest sequence still in the store.
    CompactedPastCursor {
        publisher_id: PublisherId,
        partition: u32,
        requested_seq: u64,
        earliest_available: u64,
    },

    /// Store query failed after all retries.
    StoreUnavailable {
        attempts: u32,
        last_error: Box<dyn std::error::Error + Send + Sync>,
    },
}
```

### Challenge 7: Configuration & Tuning

**Problem:** The offload thresholds need sensible defaults but must be tunable
for different workloads.

**Solution: `OffloadConfig` with builder-pattern integration.**

```rust
pub struct OffloadConfig {
    /// Enable automatic offload (default: true when store feature is active).
    pub enabled: bool,

    /// Channel fullness ratio (0.0–1.0) that triggers lag detection.
    /// Default: 0.8 (80% of channel capacity).
    pub channel_fullness_threshold: f64,

    /// Maximum sequence lag (via heartbeat) before triggering offload.
    /// Default: 10_000 sequences behind.
    pub seq_lag_threshold: u64,

    /// Lag must be sustained for this duration before offloading.
    /// Default: 2 seconds.
    pub debounce_window: Duration,

    /// Number of events per store query batch during catch-up.
    /// Default: 10_000.
    pub catch_up_batch_size: usize,

    /// Minimum/maximum for adaptive batch sizing.
    pub min_batch_size: usize,   // Default: 1_000
    pub max_batch_size: usize,   // Default: 100_000

    /// Target duration for a single batch query.
    /// Used by adaptive batch sizer. Default: 100ms.
    pub target_batch_duration: Duration,

    /// How close to the watermark the consumer must be to re-subscribe.
    /// Expressed as max sequence delta. Default: 1_000.
    pub re_subscribe_threshold: u64,

    /// Quiet period after channel drain before recording switchover cursor.
    /// Default: 50ms.
    pub drain_quiet_period: Duration,
}
```

Integrated into `EventBusConfig::builder()`:

```rust
let config = EventBusConfig::builder("myapp/events")
    .offload(OffloadConfig {
        seq_lag_threshold: 50_000,
        debounce_window: Duration::from_secs(5),
        ..Default::default()
    })
    .build()?;
```

---

## Options Analysis

Three approaches to implementing this, with increasing scope:

### Option A: Subscriber-Internal Offload (Transparent)

**Approach:** The offload logic lives entirely inside `EventSubscriber`. The
application calls `recv()` as usual and is unaware of the state transition.
The subscriber's background task manages LIVE/DRAINING/CATCHING_UP internally.

**Pros:**
- Zero API change — existing consumers get offload for free
- Simpler mental model for users
- All state machine complexity is encapsulated

**Cons:**
- Less control for advanced users who want to customize catch-up behavior
- Harder to test (state transitions are internal)
- The subscriber must hold a reference to the session for store queries (already
  true for gap recovery)

**Migration cost:** Low — additive feature behind `store` feature flag.

**Fits when:** Most consumers are simple `recv()` loops that don't need
fine-grained control over replay behavior.

### Option B: Explicit Offload API (User-Controlled)

**Approach:** Expose the three-state machine to the user via an enum return
type or separate method. The subscriber signals that it detected lag; the
application decides whether to offload.

```rust
enum RecvResult<T> {
    Event(Event<T>),
    LagDetected { lag: LagInfo, offload: OffloadHandle },
}
```

**Pros:**
- Full user control — application can log, alert, or deny offload
- Easier to test and reason about
- Applications can implement custom catch-up logic

**Cons:**
- API change — existing code must handle `RecvResult::LagDetected`
- More burden on application developers
- Most users will just call `offload.proceed()` anyway

**Migration cost:** Medium — API-breaking change to `recv()`.

**Fits when:** Advanced users need visibility into state transitions or want to
implement custom offload strategies.

### Option C: Hybrid (Transparent Default + Observable)

**Approach:** Offload is automatic and transparent (as in Option A), but the
subscriber emits **observable events** via a separate channel that applications
can optionally monitor:

```rust
enum OffloadEvent {
    LagDetected { lag: LagInfo },
    Draining { buffered: usize },
    CatchingUp { cursor: CursorMap, behind_by: u64 },
    CaughtUp { elapsed: Duration },
    Resumed,
    OffloadFailed { error: CatchUpError },
}

impl EventSubscriber {
    /// Returns a receiver for offload lifecycle events.
    /// Optional — callers who don't care simply ignore it.
    pub fn offload_events(&self) -> flume::Receiver<OffloadEvent> { ... }
}
```

**Pros:**
- Zero API change to `recv()` — offload is transparent
- Full observability for users who want it
- Can hook into alerting/metrics without changing application logic
- Testable via the event channel

**Cons:**
- Slightly more implementation complexity (dual channel)
- Users must know to check `offload_events()` for error handling

**Migration cost:** Low — additive API, no breaking changes.

**Fits when:** Transparency is desired but observability cannot be sacrificed.

### Comparison

| Dimension | A: Transparent | B: Explicit | C: Hybrid |
|-----------|---------------|-------------|-----------|
| API impact | None | Breaking | Additive |
| User complexity | Lowest | Highest | Low |
| Observability | Logs only | Full (return type) | Full (event channel) |
| Testability | Integration tests | Unit-testable | Unit-testable |
| Customizability | Config-only | Full control | Config + hooks |
| Migration cost | Low | Medium | Low |
| Implementation effort | Medium | Medium | Medium-High |

### Recommendation

**Option C (Hybrid)** is the best fit for mitiflow:

1. **Preserves the simple `recv()` API** — consistent with the project's DX
   goals ([16_dx_and_multi_topic.md](16_dx_and_multi_topic.md)).
2. **Observable without being intrusive** — operators can monitor offload events
   in production; applications that don't care simply ignore the channel.
3. **Aligns with existing patterns** — the subscriber already has background tasks
   for gap recovery, heartbeat processing, and shard dispatch. Adding an offload
   state machine to this task set is natural.
4. **Low migration cost** — no existing code breaks; offload is additive.

---

## Implementation Sketch

### New Types

```rust
// In subscriber/offload.rs

pub struct OffloadStateMachine {
    state: ConsumerState,
    lag_detector: LagDetector,
    catch_up: Option<CatchUpReader>,
    cursors: HashMap<(PublisherId, u32), u64>,
    config: OffloadConfig,
    offload_tx: flume::Sender<OffloadEvent>,
}

enum ConsumerState {
    Live,
    Draining { since: Instant },
    CatchingUp { started: Instant },
}
```

### Integration Point

The offload state machine runs as a **wrapper around the shard dispatch task**
inside `EventSubscriber`. In the existing architecture:

```
Zenoh subscriber → sample fan-in channel → shard dispatcher → gap detector → event_rx
```

With offload:

```
                  ┌──────────────────────────────────────────────────────┐
                  │               OffloadStateMachine                    │
                  │                                                      │
Zenoh sub ───────►│  [LIVE] → shard dispatch → gap detect → event_rx    │
                  │                                                      │
                  │  [DRAINING] → drain channel → record cursor          │
                  │                                                      │
Store queries ───►│  [CATCHING_UP] → batch query → event_rx             │
                  │                                                      │
                  └──────────────────────────────────────────────────────┘
```

The `event_rx` channel (what the application reads via `recv()`) is fed by
**either** the live pub/sub path or the store query path, transparently.

### Module Layout

```
mitiflow/src/subscriber/
  mod.rs                  # EventSubscriber (unchanged public API)
  gap_detector.rs         # existing
  consumer_group.rs       # existing
  offload.rs              # NEW: OffloadStateMachine, LagDetector, CatchUpReader
  offload_config.rs       # NEW: OffloadConfig, OffloadEvent
```

### Feature Gate

Offload requires the `store` feature (it queries the Event Store). Without
`store`, the offload module is compiled out:

```rust
#[cfg(feature = "store")]
pub mod offload;
```

When `store` is not enabled, the subscriber behaves exactly as today — there is
no store to offload to, so the configuration is ignored.

---

## Observability

### Metrics (via tracing)

```
mitiflow.consumer.state           = "live" | "draining" | "catching_up"
mitiflow.consumer.offload.count   = total offload transitions
mitiflow.consumer.catch_up.batch  = events per catch-up batch
mitiflow.consumer.catch_up.rate   = events/sec during catch-up
mitiflow.consumer.lag.sequences   = current max sequence lag
mitiflow.consumer.lag.channel     = current channel fill ratio
```

### Structured Logging

```
INFO  consumer entering DRAINING state (lag=15234 sequences, channel=92%)
INFO  consumer entering CATCHING_UP state (cursor={P1/p0: 5023, P2/p0: 3801})
INFO  catch-up batch complete (10000 events in 85ms, 117647 events/sec)
INFO  consumer caught up (elapsed=2.3s, total_recovered=45234)
INFO  consumer resumed LIVE pub/sub
WARN  catch-up failed: store unavailable after 3 attempts
```

---

## Design Decisions

1. **Offload must NOT reset the gap detector by default.** Resetting per-publisher
   sequence state compromises correctness — the consumer would lose track of what
   it has already processed and could re-deliver or skip events. Instead, the gap
   detector state is **preserved** across the offload cycle. During CATCHING_UP,
   the store query cursor (derived from the gap detector's last-seen state) is
   the source of truth. On re-subscribe (CATCHING_UP → LIVE), the gap detector
   resumes from the cursor position — no reset needed.

   A `reset_on_offload: bool` config option (default `false`) is available for
   edge cases where the user explicitly accepts potential re-delivery, but the
   default behavior is to fail with a clear error if state is inconsistent
   rather than silently reset.

2. **The `offload_events` channel is unbounded.** Offload lifecycle events are
   low-frequency (one per state transition, not per event), so unbounded growth
   is not a concern. Bounding the channel risks blocking the state machine if
   the application never reads offload events — an unacceptable failure mode
   for an internal mechanism that should be invisible to the application.

3. **`history_on_subscribe` is dead code — remove it.** Verified in the codebase:
   `history_on_subscribe` exists as a config field in `EventBusConfig` and is
   threaded through the builder, but it is never read or acted upon in
   `EventSubscriber::new()` or any subscriber background task. It was a
   placeholder for an earlier design that was never implemented. The correct
   action is to remove the field from `EventBusConfig`, the builder method, and
   all test references. This is independent of the offload feature.

## Out of Scope (Future Work)

4. **Multiple concurrent offloads in a consumer group.** If all members of a
   consumer group offload simultaneously, the store absorbs N× query load.
   Coordinated jittering or staggered offload triggers per member would mitigate
   thundering herd, but this is an optimization for later — the store already
   handles concurrent queries, and the adaptive batch sizer self-limits per
   consumer.

5. **Keyed compaction interaction.** If the store runs `compact_keyed()` during
   catch-up, the consumer might miss intermediate states of a key. For
   event-sourced consumers that need every state change, compaction must be
   deferred or the consumer must detect and handle `CompactedPastCursor`.
   This depends on log compaction (Key-Based Publishing Phase 3), which is
   itself not yet implemented.

---

## Implementation Status

**Implemented:** Option C (Hybrid) — transparent offload + observable
`OffloadEvent` channel. Applies to `EventSubscriber` only (single-shard path).
Feature-gated behind `#[cfg(feature = "store")]`.

### What was built

| Component | Location | Description |
|-----------|----------|-------------|
| `OffloadConfig` | `config.rs` | 11-field configuration struct with `Default` impl, integrated into `EventBusConfig` builder |
| `LagDetector` | `subscriber/offload.rs` | Composite lag scoring (channel fullness + heartbeat seq delta) with debounce window |
| `CatchUpReader` | `subscriber/offload.rs` | Batched store queries via `session.get()` with 5s timeout, adaptive batch sizing |
| `OffloadManager` | `subscriber/offload.rs` | Orchestrates the three-state lifecycle; runs inline in subscriber processing task |
| `OffloadEvent` | `subscriber/offload.rs` | 6-variant enum published to unbounded flume channel for observability |
| `ConsumerState` | `subscriber/offload.rs` | `Live`, `Draining`, `CatchingUp` states |
| `event_channel_capacity` | `config.rs` | Configurable bounded channel size (default 1024) for event delivery |
| `snapshot_cursors()` | `subscriber/gap_detector.rs` | New method on `SequenceTracker` trait, returns per-(publisher, partition) cursor map |
| `OffloadFailed` | `error.rs` | New error variant for offload failures |

### Architecture decisions (deviation from design doc)

1. **No separate `OffloadStateMachine` struct.** The state machine is embedded
   directly in `OffloadManager`, which owns `LagDetector` and creates
   `CatchUpReader` on demand. This avoids an extra layer of indirection.

2. **Inline execution, not a separate task.** The offload check runs inside the
   subscriber's existing processing task select loop (on every sample, before
   `handle_sample_result`). When triggered, `run_offload_cycle()` runs inline
   in the same task. This avoids `Arc<Mutex<>>` overhead on the `GapDetector`.

3. **`send_async().await` in offload cycle.** During drain and catch-up, events
   are sent to `event_tx` via `send_async().await` (not sync `send()`) to avoid
   blocking tokio worker threads, which would deadlock with the store's queryable
   task running on the same runtime.

4. **GapDetector cursor advancement during catch-up.** Each event replayed from
   the store is passed through `gap_detector.on_sample()` to advance cursors.
   This prevents duplicate delivery when the live stream resumes, since the gap
   detector knows exactly where the consumer left off.

5. **Single-shard only.** Multi-shard (consumer group) subscribers log a warning
   if offload is enabled but do not activate offload. Consumer group offload
   requires per-shard state management and is tracked as future work.

### Tests

- **14 unit tests** in `subscriber/offload.rs`: LagDetector debounce/reset/seq
  lag/composite scoring, adaptive batch sizing, caught-up detection,
  OffloadEvent variant construction, ConsumerState transitions.
- **6 E2E tests** in `tests/offload.rs`:
  - `offload_disabled_no_transition` — offload_events returns None
  - `fast_consumer_never_offloads` — no offload on fast consumption
  - `offload_events_channel_available` — offload_events returns Some
  - `slow_consumer_triggers_offload_lifecycle` — full lifecycle, no duplicates
  - `offload_preserves_ordering` — monotonically increasing sequences
  - `debounce_prevents_flapping` — long debounce prevents offload on burst
- **Example** in `examples/slow_consumer_offload.rs`: fast publisher, slow
  consumer, OffloadEvent monitoring, transparent catch-up.

### Not yet implemented

- Consumer group offload (multi-shard `ConsumerGroupSubscriber`)
- `CompactedPastCursor` / `StoreUnavailable` error types (keyed compaction
  not yet implemented)
- `reset_on_offload` config option (gap detector state is always preserved)
- OpenTelemetry metrics for offload state transitions
