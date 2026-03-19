# 10 — Graceful Termination

## Problem

EventPublisher, EventSubscriber, and EventStore all spawn background tokio tasks
but only cancel them via `CancellationToken` in their `Drop` impls. The task
`JoinHandle`s stored in `_tasks` are never awaited, meaning:

1. **Resource leak** — Zenoh sessions, subscribers, queryables, and publishers
   referenced by background tasks are not deterministically released.
2. **Data loss risk** — The publisher cache queryable or store subscribe task
   may still be processing when the owning struct is dropped, leading to partial
   writes or ignored queries.
3. **Benchmark inaccuracy** — `bench_durable_pubsub.rs` drops publisher /
   subscriber state without awaiting task completion, causing log noise and
   potential late writes that confuse consumer counters.
4. **Store async tasks ignored** — `EventStore::shutdown()` cancels the token
   and joins worker threads, but the four async tasks (subscribe, queryable,
   watermark, GC) are abandoned.

### lightbench lifecycle

lightbench v0.3.2 provides `cleanup(state)` hooks on both `ProducerWork` and
`ConsumerWork`. The framework calls `cleanup` after all workers finish:

```
spawn_producer: init() → produce() loop → cleanup(state)
spawn_consumer: init() → run(state, recorder) → cleanup(state)
```

The current mitiflow transport impls do **not** override `cleanup`; they rely on
the default no-op, which simply drops the state. Adding `cleanup` overrides is
the correct fix — no framework changes required.

## Design

### Approach: explicit `async fn shutdown(self)`

Add a consuming `shutdown` method to `EventPublisher`, `EventSubscriber`, and
`EventStore`. Keep `Drop` as a best-effort synchronous fallback that only
cancels the token (same as today).

```
pub async fn shutdown(self)
  1. self.cancel.cancel()          — signal all tasks to stop
  2. join all _tasks handles       — await completion
  3. (EventStore only) join worker threads
```

Because `shutdown` takes `self` by value, the caller gives up ownership and no
`Drop` runs afterward.

### EventPublisher::shutdown

```rust
pub async fn shutdown(self) {
    self.cancel.cancel();
    for handle in self._tasks {
        let _ = handle.await;
    }
}
```

Tasks cancelled:
- Heartbeat beacon task
- Cache queryable task (answers recovery queries from subscribers)
- Watermark listener task (store-feature only)

### EventSubscriber::shutdown

```rust
pub async fn shutdown(self) {
    self.cancel.cancel();
    for handle in self._tasks {
        let _ = handle.await;
    }
}
```

Tasks cancelled:
- Main subscribe + dedup + delivery task
- Heartbeat listener task
- Gap recovery dispatcher (spawns sub-tasks via `tokio::spawn`;
  these are already short-lived and observe the same cancellation token)

### EventStore::shutdown (extended)

The existing `shutdown(&self)` stays unchanged (cancels + sends Shutdown msgs).
Add a new consuming variant:

```rust
pub async fn shutdown_gracefully(self) {
    self.shutdown();              // cancel + Shutdown msgs
    for handle in self._tasks {
        let _ = handle.await;     // await async tasks
    }
    for thread in self._worker_threads {
        let _ = thread.join();    // join worker threads
    }
}
```

Drop remains as-is for the non-async fallback.

### Shutdown ordering

In a full deployment the correct shutdown sequence is:

```
1. Publisher.shutdown()      — stop producing, drain heartbeat/cache tasks
2. Subscriber.shutdown()     — stop consuming, drain recovery tasks
3. EventStore.shutdown()     — stop ingesting, flush & close backend
4. session.close().await     — close Zenoh session
```

Publishers must stop first to prevent new messages entering the pipeline after
subscribers and store have shut down. Subscribers shut down next so that no
recovery queries target a closing store. The store shuts down last to ensure all
in-flight writes are flushed.

### Benchmark integration

Override the lightbench `cleanup` trait methods in the transport impls:

```rust
// ProducerWork for MitiflowDurableProducer
async fn cleanup(&self, state: MitiflowDurableProducerState) {
    state.publisher.shutdown().await;
}

// ConsumerWork for MitiflowConsumer
async fn cleanup(&self, state: EventSubscriber) {
    state.shutdown().await;
}
```

For `bench_durable_pubsub.rs`, after `run_durable_pubsub()` returns:

```rust
store.shutdown_gracefully().await;    // flush store
pub_session.close().await;
sub_session.close().await;
```

### Drop as fallback

`Drop` impls remain synchronous and only call `self.cancel.cancel()`.
This is intentional — `Drop` cannot be async, and spawning a blocking
`tokio::runtime::Handle::block_on` inside Drop would risk deadlocks.
The cancel token ensures tasks will eventually terminate even if `shutdown()`
is never called (e.g. panic unwinds).

## Changes summary

| File | Change |
|------|--------|
| `publisher/mod.rs` | Add `pub async fn shutdown(self)` |
| `subscriber/mod.rs` | Add `pub async fn shutdown(self)` |
| `store/runner.rs` | Add `pub async fn shutdown_gracefully(self)` |
| `transport/mitiflow.rs` | Override `cleanup()` on `MitiflowProducer`, `MitiflowDurableProducer`, `MitiflowConsumer`, `MitiflowDurableWork` |
| `bench_durable_pubsub.rs` | Call `store.shutdown_gracefully()`, close sessions after run |
