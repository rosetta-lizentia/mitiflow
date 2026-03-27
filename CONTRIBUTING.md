# Contributing to Mitiflow

Thank you for your interest in contributing to mitiflow! This document covers
the development setup, conventions, and process for submitting changes.

## Getting Started

### Prerequisites

- **Rust 1.93+** (edition 2024)
- **cargo-nextest** (preferred test runner): `cargo install cargo-nextest`
- **just** (optional, task runner): `cargo install just`

### Building

```bash
cargo build -p mitiflow --features full     # Core library with all features
cargo build --workspace                      # Everything
```

### Running Tests

```bash
cargo nextest run --features full            # Preferred — parallel, fast
cargo test --features full                   # Alternative
just test                                    # Via justfile
```

### Feature Flags (mitiflow crate)

| Flag | Description |
|------|-------------|
| `store` (default) | EventStore + storage backend trait |
| `fjall-backend` | Concrete fjall LSM-tree backend |
| `wal` | Write-ahead log for durable publisher |
| `full` | All of the above |

## Code Conventions

### Rust Patterns

- **Builder pattern** for configuration: `EventBusConfig::builder("prefix").cache_size(100).build()?`
- **Error handling:** `thiserror`-derived `#[non_exhaustive]` `Error` enum; use the crate-level `Result<T>` alias
- **Async runtime:** Tokio multi-thread (required by Zenoh)
- **Lock-free hot paths:** `scc::HashMap` for per-partition sequence counters
- **IDs:** UUID v7 (time-ordered) for `EventId` and `PublisherId`
- **Serialization:** JSON, MessagePack, Postcard — selected via `CodecFormat`

### Zenoh-Specific

- `$` in key expressions is reserved for `$*` only. Use `_` prefix for internal channels (e.g., `_store`, `_heartbeat`)
- Zenoh runtime requires `#[tokio::test(flavor = "multi_thread", worker_threads = 2)]` for all integration tests
- Publisher cache is queryable via Zenoh queryable (subscribers query it for gap recovery)

### Naming

- Types: `PascalCase` (`EventPublisher`, `StorageBackend`)
- Functions: `snake_case` (`publish_durable`, `recv_raw`)
- Zenoh key expressions: slash-separated (`demo/sensors`, `myapp/events/p/0`)
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
- Each test uses a unique `test_name` to scope key prefixes and prevent cross-test interference
- Use `common::setup_pubsub()` for standard pub/sub, `common::temp_dir()` for store tests
- Always drop publisher/subscriber before `session.close().await`
- Tests requiring storage use `#[cfg(feature = "store")]`; partition tests use `#[cfg(feature = "partition")]`
- Never use `#[tokio::test(flavor = "current_thread")]` — Zenoh requires multi-thread

## Submitting Changes

1. **Fork** the repository and create a feature branch from `main`.
2. **Write tests** for any new functionality.
3. **Run the full suite** before submitting: `cargo nextest run --workspace --features full`
4. **Run lints**: `cargo clippy --workspace --all-targets --features full` and `cargo fmt --all`
5. **Open a pull request** against `main` with a clear description of the change.

## Project Structure

| Crate | Purpose |
|-------|---------|
| `mitiflow` | Core library — publisher, subscriber, event store, partitions, DLQ |
| `mitiflow-orchestrator` | Control plane — config CRUD, lag monitoring, HTTP API |
| `mitiflow-agent` | Storage agent — distributed partition management |
| `mitiflow-cli` | Unified CLI binary — agent, orchestrator, ctl, dev mode |
| `mitiflow-emulator` | YAML-driven topology runner and chaos testbed |
| `mitiflow-bench` | Comparative benchmarks (Kafka, NATS, Redis, Redpanda) |
| `mitiflow-gateway` | Kafka protocol gateway (not yet implemented) |

## License

By contributing to mitiflow, you agree that your contributions will be licensed
under the Apache License 2.0.
