# Error Handling & DLQ Integration

**Status:** Proposed — Composable error handling trait with subscriber integration and structured dead-letter envelopes.

---

## Problem Statement

The existing `DeadLetterQueue` provides low-level retry counting and DLQ
routing, but all wiring is manual: receive → process → match outcome → sleep
for backoff → ack/retry — roughly 50 lines of boilerplate per consumer (see
`examples/dead_letter_queue.rs`). There is no error classification (transient
vs permanent), no structured DLQ metadata (the envelope is raw payload only),
and no way to inspect DLQ events programmatically (the UI page is a placeholder).

Every other subscriber behavior (gap detection, recovery, offload, heartbeat)
is configured via `EventBusConfig` and executed internally by the subscriber.
The DLQ is the only feature that requires manual orchestration.

This design proposes an `ErrorHandler` trait that plugs into the subscriber via
the builder pattern, with built-in implementations covering 90% of use cases
and a structured `DlqEnvelope` for inspectable dead-letter events.

---

## Design Goals

1. **Composable** — `ErrorHandler` trait with swappable implementations.
2. **Classified errors** — transient, permanent, fatal severity drives behavior.
3. **Subscriber-integrated** — configured via builder, subscriber owns the loop.
4. **Rich DLQ metadata** — `DlqEnvelope` carries error context, not just payload.
5. **Inspectable** — `DlqReader` for querying DLQ events over Zenoh.
6. **Backward-compatible** — `recv()` unchanged; standalone `DeadLetterQueue` stays.
7. **Consistent** — follows the same builder + internal execution pattern as
   offload, recovery, and heartbeat.

---

## Core Types

### Error Classification

```rust
/// Severity classification — determines retry behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ErrorSeverity {
    /// Temporary failure (network timeout, service unavailable).
    /// The same event may succeed on retry.
    Transient,
    /// Permanent failure (deserialization, validation, schema mismatch).
    /// Retrying will not help — route to DLQ immediately.
    Permanent,
    /// Unrecoverable failure (auth, config, resource exhausted).
    /// The consumer should stop processing entirely.
    Fatal,
}

/// Error wrapper carrying severity classification and the source error.
#[derive(Debug)]
pub struct ProcessingError {
    pub severity: ErrorSeverity,
    pub source: Box<dyn std::error::Error + Send + Sync>,
    /// Optional application-defined error code (e.g., "VALIDATION_001").
    pub code: Option<String>,
}
```

Convenience constructors:

```rust
ProcessingError::transient(err)
ProcessingError::permanent(err)
ProcessingError::fatal(err)
ProcessingError::with_code("VALIDATION_001", ErrorSeverity::Permanent, err)
```

### Default Classification of Known Errors

The `ErrorHandler::classify()` default method maps known mitiflow errors:

| `mitiflow::Error` variant | Default severity |
|---|---|
| `Serialization` | Permanent |
| `InvalidAttachment` | Permanent |
| `InvalidKey` | Permanent |
| `TopicSchemaMismatch` | Permanent |
| `Zenoh` | Transient |
| `GapRecoveryFailed` | Transient |
| `ChannelClosed` | Fatal |
| `InvalidConfig` | Fatal |
| Unknown / application errors | Transient |

Transient as the default for unknown errors is the safe choice: retry before
giving up, then dead-letter if retries are exhausted.

---

## ErrorContext and ErrorAction

```rust
/// Full context passed to the error handler on each failure.
pub struct ErrorContext<'a> {
    /// The event that failed processing.
    pub event: &'a RawEvent,
    /// The classified error from the latest attempt.
    pub error: &'a ProcessingError,
    /// Current attempt number (1-based: first attempt = 1).
    pub attempt: u32,
    /// Timestamp of the first failure for this event.
    pub first_failure: DateTime<Utc>,
    /// Timestamp of this failure.
    pub timestamp: DateTime<Utc>,
    /// The event's application key (extracted from key_expr), if any.
    pub key: Option<&'a str>,
}

/// What the subscriber should do after a processing failure.
pub enum ErrorAction {
    /// Retry after the given delay.
    Retry { delay: Duration },
    /// Route to the dead letter queue — do not retry further.
    DeadLetter,
    /// Skip this event (log at debug level, move on).
    Skip,
    /// Stop the consumer with this error.
    Abort(Error),
}
```

The subscriber's internal processing loop:

```
receive event
  → call user's processing closure
  → on Ok: call handler.on_success(event)
  → on Err: classify → build ErrorContext → call handler.on_error(ctx)
      → match ErrorAction:
          Retry { delay } → sleep, re-invoke closure
          DeadLetter      → call handler.on_dead_letter(ctx), move on
          Skip            → log, move on
          Abort(e)        → return Err(e) from the processing method
```

---

## ErrorHandler Trait

```rust
#[async_trait]
pub trait ErrorHandler: Send + Sync {
    /// Initialize the handler with a Zenoh session. Called by the subscriber
    /// during init(). Default: no-op. Used by RetryThenDlq to set up DLQ publishing.
    async fn init(&self, _session: &Session) -> Result<()> { Ok(()) }

    /// Classify an application error into a ProcessingError.
    /// Default: maps known mitiflow errors to severity, unknown to Transient.
    fn classify(&self, error: Box<dyn std::error::Error + Send + Sync>) -> ProcessingError { ... }

    /// Decide what to do after a classified failure.
    async fn on_error(&self, ctx: &ErrorContext<'_>) -> ErrorAction;

    /// Called after successful processing. Default: no-op.
    /// Used for cleanup (clearing retry counters) and metrics.
    fn on_success(&self, _event: &RawEvent) {}

    /// Called when an event is dead-lettered. Responsible for building the
    /// DlqEnvelope and publishing it. Default: logs a warning, does not publish.
    async fn on_dead_letter(&self, ctx: &ErrorContext<'_>) -> Result<()> {
        Ok(())
    }
}
```

### Built-in Implementations

#### `RetryThenDlq` — Primary handler

The standard error handler for production use. Replaces most manual DLQ wiring.

- Transient errors → retry with backoff up to `max_retries`, then dead-letter.
- Permanent errors → dead-letter immediately (zero retries).
- Fatal errors → abort the consumer.
- `on_success` → clears retry counter via internal `DeadLetterQueue::ack()`.
- `on_dead_letter` → builds `DlqEnvelope`, publishes to DLQ topic.

Wraps the existing `DeadLetterQueue` internally for retry tracking. The Zenoh
session for DLQ publishing is injected at subscriber initialization time (the
subscriber passes its session to the handler), not at config time.

```rust
let handler = RetryThenDlq::new(DlqConfig::new("app/orders/_dlq", 3));
```

| Error severity | `on_error` behavior |
|---|---|
| Transient, attempt < max_retries | `Retry { delay }` (backoff from `DlqConfig`) |
| Transient, attempt >= max_retries | `DeadLetter` |
| Permanent | `DeadLetter` (skips all retries) |
| Fatal | `Abort(err)` |

#### `SkipAndLog` — Non-critical streams

For event streams where failures should not block processing.

- Transient → retry up to N times (configurable, default 1), then skip.
- Permanent → skip immediately.
- Fatal → abort.
- No DLQ publishing. Logs skipped events at warn level.

```rust
let handler = SkipAndLog::new(1); // retry transient once, then skip
```

#### `FailFast` — Testing and strict-ordering

Any failure immediately aborts the consumer.

```rust
let handler = FailFast::new();
```

---

## DlqEnvelope

Structured metadata published to the DLQ topic by `RetryThenDlq::on_dead_letter`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqEnvelope {
    // --- Original event ---
    /// Raw payload bytes (preserves original encoding).
    pub payload: Vec<u8>,
    pub event_id: EventId,
    pub publisher_id: PublisherId,
    pub seq: u64,
    pub key_expr: String,
    pub event_timestamp: DateTime<Utc>,

    // --- Error context ---
    pub error_message: String,
    pub error_code: Option<String>,
    pub error_severity: ErrorSeverity,

    // --- Processing context ---
    pub attempts: u32,
    pub first_failure: DateTime<Utc>,
    pub dead_lettered_at: DateTime<Utc>,
    /// Which consumer produced this dead letter.
    pub handler_id: String,
    /// Source topic key prefix (for future reprocessing).
    pub source_topic: String,
}
```

Published to `{dlq_key_prefix}/{event_id}` as the serialized envelope (JSON or
MessagePack, matching the topic's `CodecFormat`).

**Breaking change vs current `DeadLetterQueue`:** The existing
`DeadLetterQueue::send_to_dlq()` publishes raw payload bytes only. The new
`RetryThenDlq` publishes a `DlqEnvelope`. These are different formats on the
same key space. The standalone `DeadLetterQueue` API is unchanged — only the
integrated `RetryThenDlq` path uses envelopes. Users migrating from manual DLQ
to `RetryThenDlq` should update any DLQ consumers to expect `DlqEnvelope`.

---

## DlqReader

Read-only query interface for inspecting DLQ events. Works over Zenoh — same
remote-first pattern as `EventReplayer`.

```rust
pub struct DlqReader { ... }

impl DlqReader {
    /// Create a reader for a DLQ topic.
    pub fn new(session: &Session, dlq_key_prefix: &str) -> Self;

    /// Subscribe to live DLQ events as they arrive.
    pub async fn subscribe(&self) -> Result<flume::Receiver<DlqEnvelope>>;

    /// Query stored DLQ events (requires EventStore subscribed to the DLQ prefix).
    pub async fn query(&self, filters: DlqQueryFilters) -> Result<Vec<DlqEnvelope>>;

    /// Count of DLQ events matching filters.
    pub async fn count(&self, filters: DlqQueryFilters) -> Result<usize>;
}

pub struct DlqQueryFilters {
    /// Only return events dead-lettered after this time.
    pub after_time: Option<DateTime<Utc>>,
    /// Only return events dead-lettered before this time.
    pub before_time: Option<DateTime<Utc>>,
    /// Filter by error severity.
    pub severity: Option<ErrorSeverity>,
    /// Filter by application error code.
    pub error_code: Option<String>,
    /// Filter by source topic.
    pub source_topic: Option<String>,
    /// Maximum number of results.
    pub limit: Option<usize>,
}
```

`DlqReader::query()` uses Zenoh `session.get()` against the store queryable for
the DLQ key prefix. This requires an `EventStore` configured to subscribe to
`{dlq_key_prefix}/**`. The reader deserializes stored payloads as `DlqEnvelope`.

`DlqReader::subscribe()` declares a Zenoh subscriber on `{dlq_key_prefix}/**`
for real-time DLQ monitoring.

---

## Subscriber Integration

### Builder API

```rust
EventBusConfig::builder("app/orders")
    .error_handler(RetryThenDlq::new(DlqConfig::new("app/orders/_dlq", 3)))
    .build()
```

`EventBusConfig` stores `Option<Arc<dyn ErrorHandler>>`. When set, the
`process()` method is available. Handlers that need a Zenoh session (e.g.,
`RetryThenDlq` for DLQ publishing) receive it during `EventSubscriber::init()`,
which calls an `ErrorHandler::init(&self, session: &Session)` lifecycle method
(default: no-op).

### New Methods on `EventSubscriber`

```rust
impl EventSubscriber {
    /// Process events with automatic error handling.
    ///
    /// Calls the closure for each event. On failure, the configured
    /// ErrorHandler drives retry/DLQ/skip/abort behavior. Returns only
    /// when the handler returns Abort or the subscriber shuts down.
    ///
    /// Requires an ErrorHandler configured via the builder. Returns
    /// Error::InvalidConfig if none is set.
    pub async fn process<T, F, Fut>(&self, f: F) -> Result<()>
    where
        T: Serialize + DeserializeOwned,
        F: Fn(Event<T>) -> Fut,
        Fut: Future<Output = std::result::Result<(), ProcessingError>>,
    { ... }

    /// Same as process(), but receives RawEvent (no deserialization).
    pub async fn process_raw<F, Fut>(&self, f: F) -> Result<()>
    where
        F: Fn(RawEvent) -> Fut,
        Fut: Future<Output = std::result::Result<(), ProcessingError>>,
    { ... }
}
```

**When no `ErrorHandler` is configured:** `recv()` / `recv_raw()` work exactly
as today. No behavior change. The `process()` method requires an error handler.

**Deserialization errors in `process::<T>()`:** If the event payload cannot be
deserialized into `T`, this is classified as `Permanent` automatically (the
closure is never called). The handler's `on_error` decides whether to
dead-letter or skip.

**Interaction with `EventReplayer`:** The future `EventReplayer` (see
[19_replay_api.md](19_replay_api.md)) would expose the same `process()` /
`process_raw()` methods, using the same `ErrorHandler` trait.

### Migration from Manual DLQ

| Before (manual) | After (integrated) |
|---|---|
| Construct `DeadLetterQueue` separately | `.error_handler(RetryThenDlq::new(...))` on builder |
| `recv_raw()` in a loop | `subscriber.process(\|event\| async { ... })` |
| Call `dlq.on_failure()` manually | Handled by `RetryThenDlq::on_error()` |
| Match `RetryOutcome`, sleep for backoff | Handled internally by subscriber |
| Call `dlq.ack()` on success | Handled by `RetryThenDlq::on_success()` |
| ~50 lines of boilerplate per consumer | ~5 lines |

---

## Interaction with Existing Types

| Type | Relationship |
|---|---|
| `DeadLetterQueue` | Wrapped internally by `RetryThenDlq` for retry tracking. Public API unchanged, soft-deprecated |
| `DlqConfig` | Reused as-is by `RetryThenDlq`. `BackoffStrategy` shared |
| `RetryOutcome` | Internal to `RetryThenDlq` — no longer exposed in the integrated path |
| `EventSubscriber` | Gains `process()` / `process_raw()`. `recv()` unchanged |
| `EventBusConfig` | Gains `.error_handler()` builder method. Stores `Option<Arc<dyn ErrorHandler>>` |
| `EventReplayer` (doc 19) | Future: same `process()` API, same `ErrorHandler` trait |
| `Error` enum | No new variants needed — `ProcessingError` is a separate type for application errors |
| `RawEvent` | Passed through to `ErrorContext` and `DlqEnvelope` as-is |

---

## Implementation Phases

| Phase | Scope | Key Deliverable |
|---|---|---|
| 1 | Core types | `ProcessingError`, `ErrorSeverity`, `ErrorContext`, `ErrorAction`, default `classify()` |
| 2 | `ErrorHandler` trait + built-ins | `ErrorHandler` trait, `RetryThenDlq`, `SkipAndLog`, `FailFast` |
| 3 | `DlqEnvelope` | Structured envelope, `RetryThenDlq::on_dead_letter` publishes it |
| 4 | Subscriber integration | `process()` / `process_raw()` on `EventSubscriber`, `.error_handler()` on `EventBusConfig` builder |
| 5 | `DlqReader` | Read-only DLQ query/subscribe API over Zenoh |
| 6 | Deprecation path | Soft-deprecate standalone `DeadLetterQueue` with docs pointing to `RetryThenDlq` |

---

## Out of Scope (Future Work)

- **Reprocessing / republish from DLQ** — requires feedback loop with reprocess counter.
- **High-level `ProcessingPipeline`** — builds on `process()` + `ErrorHandler` with
  dataflow composition (fan-out, join, transform).
- **DLQ UI backend** — consumes `DlqReader`, separate design.
- **Metrics / observability hooks** — can be added as default methods on
  `ErrorHandler` later (e.g., `on_retry`, `on_skip` callbacks for counters).

---

## Open Questions

1. **`handler_id` in `DlqEnvelope`:** Should this be auto-generated (e.g., from
   `PublisherId` of the consumer) or user-supplied via config? **Proposed:**
   Optional in config, defaults to a generated ID.

2. **DLQ codec:** Should `DlqEnvelope` always serialize as JSON (human-readable
   for inspection) regardless of the source topic's codec? **Proposed:** Yes —
   DLQ is for debugging, readability matters more than compactness.

3. **Retry state persistence:** Currently retry counters are in-memory
   (`HashMap`). If the consumer restarts mid-retry, the count resets. Should
   retry state be persisted? **Proposed:** No for now — the DLQ catches
   persistent failures regardless of restart. Persistent retry state is a
   future enhancement.
