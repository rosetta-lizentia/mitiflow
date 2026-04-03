# Mitiflow — Project Guidelines

Mitiflow is a brokerless event streaming platform built on Zenoh, providing Kafka-like reliability (sequencing, gap detection, durable storage, consumer groups) with microsecond-latency pub/sub.

## Architecture

Seven crates in a Cargo workspace (plus a standalone Svelte UI):

| Crate | Purpose |
|-------|--------|
| `mitiflow` | Core library — publisher, subscriber, event store, partitions, DLQ |
| `mitiflow-orchestrator` | Control plane — config CRUD, lag monitoring, HTTP API |
| `mitiflow-agent` | Storage agent — distributed partition management, multi-topic |
| `mitiflow-cli` | Unified CLI binary — `agent`, `orchestrator`, `ctl`, `dev` subcommands |
| `mitiflow-emulator` | YAML-driven topology runner and chaos testbed |
| `mitiflow-bench` | Comparative benchmarks vs Kafka, NATS, Redis, Redpanda |
| `mitiflow-gateway` | Kafka protocol gateway (stub, not yet implemented) |

`mitiflow-ui` is a standalone Svelte 5 dashboard (not in the Cargo workspace).

**Core abstractions:** `EventPublisher` → Zenoh pub/sub → `EventSubscriber`, with `EventStore` (fjall LSM) for durability and `PartitionManager` (rendezvous hashing) for consumer groups.

**Metadata travels in Zenoh attachments** (50-byte binary header: seq, publisher_id, event_id, timestamp, urgency) — payload contains only serialized `T`. This enables zero-copy routing without deserializing the body.

See [docs/02_architecture.md](docs/02_architecture.md) for detailed design, [docs/implementation_plan.md](docs/implementation_plan.md) for status, and [docs/ROADMAP.md](docs/ROADMAP.md) for planned features.

## Build & Test

```bash
cargo build -p mitiflow --features full     # Build with all features
cargo nextest run --features full            # Run all tests (preferred)
cargo test --features full                   # Alternative test runner
cargo bench -p mitiflow                      # Criterion micro-benchmarks
```

**Feature flags** (in `mitiflow` crate):
- `store` (default) — EventStore + storage backend trait
- `fjall-backend` — Concrete fjall LSM-tree backend (implies `store`)
- `wal` — Write-ahead log for durable publisher
- `full` — All of the above

## Code Conventions

### Rust Patterns
- **Edition 2024**, Rust 1.93+
- **Builder pattern** for configuration: `EventBusConfig::builder("key/prefix").cache_size(100).build()?`
- **Error handling:** `thiserror`-derived non-exhaustive `Error` enum; use the crate-level `Result<T>` alias
- **Async runtime:** Tokio multi-thread (required by Zenoh). Never use `flavor = "current_thread"` in tests
- **Lock-free hot paths:** `scc::HashMap` for per-partition sequence counters; `Arc<RwLock<VecDeque>>` for publisher cache
- **IDs:** UUID v7 (time-ordered) for `EventId` and `PublisherId`
- **Serialization codecs:** JSON, MessagePack, Postcard — selected via `CodecFormat`

### Zenoh-Specific
- **`$` in key expressions is reserved** for `$*` only. Use `_` prefix for internal channels (e.g., `_store`, `_watermark`, `_heartbeat`, `_cache`, `_publishers`)
- Zenoh runtime requires `#[tokio::test(flavor = "multi_thread", worker_threads = 2)]` for all integration tests
- Publisher cache is queryable via Zenoh queryable (subscribers query it for gap recovery)

### Naming
- Types: `PascalCase` (`EventPublisher`, `StorageBackend`)
- Functions: `snake_case` (`publish_durable`, `recv_raw`)
- Modules: `snake_case` (`gap_detector`, `hash_ring`)
- Zenoh key expressions: slash-separated (`demo/sensors`, `myapp/events/p/0`, `myapp/events/p/0/k/order-123/42`)
- Keyed event key expressions: `{prefix}/p/{partition}/k/{key}/{seq}` — the `/k/` segment separates application keys from partition/sequence. See [docs/15_key_based_publishing.md](docs/15_key_based_publishing.md).
- Internal Zenoh keys: `_` prefix (`{prefix}/_store`, `{prefix}/_watermark`)

## Testing

Tests use shared helpers from `mitiflow/tests/common/mod.rs`:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_something() {
    let (_session, publisher, subscriber) = common::setup_pubsub("unique_test_name").await;
    common::publish_n(&publisher, 10).await;
    let events = common::recv_n(&subscriber, 10).await;
    // assertions...
}
```

**Key rules:**
- Each test uses a unique `test_name` → scoped key prefix prevents cross-test interference
- Use `common::setup_pubsub()` for standard pub/sub setup, `common::temp_dir()` for store tests
- Always drop publisher/subscriber before `session.close().await`
- Tests requiring storage use `#[cfg(feature = "store")]`; tests requiring the fjall backend use `#[cfg(feature = "fjall-backend")]`

## Key Files

| Area | Path |
|------|------|
| Public API | `mitiflow/src/lib.rs` |
| Event envelope | `mitiflow/src/event.rs` |
| Configuration | `mitiflow/src/config.rs` |
| Publisher | `mitiflow/src/publisher/mod.rs` |
| Subscriber + gap detection | `mitiflow/src/subscriber/mod.rs`, `subscriber/gap_detector.rs` |
| Slow consumer offload | `mitiflow/src/subscriber/offload.rs` |
| Keyed consumer | `mitiflow/src/subscriber/keyed_consumer.rs` |
| Sequence checkpoint | `mitiflow/src/subscriber/checkpoint.rs` |
| Storage backend trait | `mitiflow/src/store/backend.rs` |
| Partition assignment | `mitiflow/src/partition/mod.rs`, `partition/hash_ring.rs` |
| Consumer group subscriber | `mitiflow/src/subscriber/consumer_group.rs` |
| Test helpers | `mitiflow/tests/common/mod.rs` |
| Storage agent | `mitiflow-agent/src/agent.rs`, `topic_supervisor.rs` |
| Orchestrator | `mitiflow-orchestrator/src/orchestrator.rs` |
| Unified CLI | `mitiflow-cli/src/main.rs` |
| Emulator | `mitiflow-emulator/src/` |
| Key-based publishing design | `docs/15_key_based_publishing.md` |
| Implementation roadmap | `docs/implementation_plan.md` |
