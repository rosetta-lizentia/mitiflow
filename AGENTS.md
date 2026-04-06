# AGENTS.md — Mitiflow

Brokerless event streaming on Zenoh. Rust 2024 edition workspace + standalone Svelte 5 UI.

## Workspace Layout

| Crate | Purpose |
|-------|---------|
| `mitiflow` | Core library — publisher, subscriber, event store, partitions, DLQ |
| `mitiflow-storage` | Storage agent — distributed partition management |
| `mitiflow-orchestrator` | Control plane — config CRUD, lag monitoring, HTTP API (axum) |
| `mitiflow-cli` | Unified CLI binary — `agent`, `orchestrator`, `ctl`, `dev` subcommands |
| `mitiflow-emulator` | YAML-driven topology runner and chaos testbed |
| `mitiflow-bench` | Comparative benchmarks (Kafka, NATS, Redis, Redpanda) |
| `mitiflow-gateway` | Kafka protocol gateway (stub, not yet implemented) |
| `mitiflow-ui/` | Standalone Svelte 5 + Tailwind CSS 4 dashboard (NOT in Cargo workspace) |

## Build & Run

```bash
# Rust — requires Rust 1.93+ (edition 2024)
cargo build -p mitiflow --features full       # Core with all features
cargo build --workspace                        # Everything
just build                                     # Shortcut

# UI — requires pnpm
cd mitiflow-ui && pnpm install && pnpm build   # Or: just ui-build

# Orchestrator with embedded UI
just build-with-ui
```

## Testing

```bash
# Rust tests (cargo-nextest preferred)
cargo nextest run --features full                        # All core tests
cargo nextest run --workspace --features full             # Full workspace
cargo nextest run --features full -E 'test(test_name)'   # Single test by name
cargo nextest run --features full -p mitiflow             # Single crate
cargo test --features full -- test_name                   # Alternative: single test via cargo test
just test                                                 # Quick: nextest, no features
just test-all                                             # Full: nextest --workspace --features full

# Benchmarks (Criterion)
cargo bench -p mitiflow                                   # All benchmarks
cargo bench -p mitiflow --bench codec                     # Single benchmark

# UI tests
cd mitiflow-ui && pnpm test                               # Vitest
```

## Lint & Format

```bash
cargo fmt --all                                           # Format
cargo fmt --all -- --check                                # Check formatting
cargo clippy --workspace --all-targets --features full -- -D warnings  # Lint
just check                                                # fmt + clippy + nextest (full CI)
```

No rustfmt.toml or clippy.toml — uses default rustfmt and clippy settings.
VS Code is configured with `rust-analyzer.cargo.features: ["full"]`.

## Feature Flags (mitiflow crate)

| Flag | Default | Description |
|------|---------|-------------|
| `store` | Yes | EventStore + storage backend trait |
| `fjall-backend` | No | Concrete fjall LSM-tree backend (implies `store`) |
| `wal` | No | Write-ahead log for durable publisher |
| `full` | No | All of the above |

Always pass `--features full` when building/testing to enable all code paths.

## Code Style — Rust

### Formatting & Naming
- Default `rustfmt` — no custom config
- Types: `PascalCase` (`EventPublisher`, `StorageBackend`)
- Functions/methods: `snake_case` (`publish_durable`, `recv_raw`)
- Modules: `snake_case` (`gap_detector`, `hash_ring`)
- Constants: `SCREAMING_SNAKE_CASE`

### Imports
- Group: std → external crates → workspace crates → `crate::` / `super::`
- Use `crate::` for intra-crate imports; `use super::*` in test modules
- Workspace crates imported by name: `use mitiflow::...`

### Error Handling
- Core crate: `thiserror` + `miette::Diagnostic` for structured errors
- Error type: `#[non_exhaustive]` enum in each crate's `error.rs`
- Crate-level `Result<T>` type alias: `pub type Result<T> = std::result::Result<T, Error>;`
- CLI/binaries: `anyhow::Result` for top-level, `miette` for pretty errors
- Never use `.unwrap()` in library code; OK in tests and examples

### Async
- Runtime: Tokio multi-thread (required by Zenoh)
- Use `tokio::spawn` for background tasks, `tokio::select!` for multiplexing
- `async-trait` crate for async trait methods
- Channels: `flume` for mpmc, `tokio::sync` for watches/oneshots

### Configuration
- Builder pattern: `EventBusConfig::builder("key/prefix").cache_size(100).build()?`
- CLI parsing: `clap` with derive macros
- Config files: `serde_yaml`

### Key Patterns
- Lock-free hot paths: `scc::HashMap` for concurrent maps
- IDs: UUID v7 (time-ordered) via `uuid` crate for `EventId`, `PublisherId`
- Serialization codecs: JSON (`serde_json`), MessagePack (`rmp-serde`), Postcard — selected via `CodecFormat`
- Public API re-exported from `lib.rs`; internal modules are `pub mod` but items gated by feature flags

### Zenoh-Specific
- `$` in key expressions reserved for `$*` only
- Internal channels use `_` prefix: `{prefix}/_store`, `{prefix}/_watermark`, `{prefix}/_heartbeat`
- Keyed event key format: `{prefix}/p/{partition}/k/{key}/{seq}`
- Publisher cache is Zenoh-queryable for gap recovery

## Testing Conventions — Rust

### Test Attribute (CRITICAL)
```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
```
**Never** use `flavor = "current_thread"` — Zenoh requires multi-thread runtime.

### Test Helpers
Shared helpers in `mitiflow/tests/common/mod.rs`:
```rust
let (_session, publisher, subscriber) = common::setup_pubsub("unique_test_name").await;
common::publish_n(&publisher, 10).await;
let events = common::recv_n(&subscriber, 10).await;
```

### Test Rules
- Each test must use a **unique `test_name`** → scoped Zenoh key prefix prevents interference
- Use `common::setup_pubsub()` for pub/sub, `common::temp_dir()` for store tests
- Always drop publisher/subscriber **before** `session.close().await`
- Feature-gate tests: `#[cfg(feature = "store")]`, `#[cfg(feature = "fjall-backend")]`
- Unit tests go in `#[cfg(test)] mod tests` inside the source file
- Integration tests go in `mitiflow/tests/`

## Code Style — UI (mitiflow-ui/)

- **Framework**: Svelte 5 with TypeScript (strict mode)
- **Styling**: Tailwind CSS 4 via `@tailwindcss/vite`
- **Routing**: `svelte-spa-router`
- **Icons**: `lucide-svelte`
- **Build**: Vite 8, output to `build/`
- **Tests**: Vitest + `@testing-library/svelte`, jsdom environment
- **Package manager**: pnpm
- **Path alias**: `$lib` → `src/lib/`
- **Dev proxy**: `/api` → `http://localhost:8080` (orchestrator backend)

## Key Files

| Area | Path |
|------|------|
| Public API & re-exports | `mitiflow/src/lib.rs` |
| Error types | `mitiflow/src/error.rs` |
| Event envelope | `mitiflow/src/event.rs` |
| Configuration + builder | `mitiflow/src/config.rs` |
| Publisher | `mitiflow/src/publisher/mod.rs` |
| Subscriber + gap detection | `mitiflow/src/subscriber/mod.rs`, `subscriber/gap_detector.rs` |
| Consumer groups | `mitiflow/src/subscriber/consumer_group.rs` |
| Keyed consumer | `mitiflow/src/subscriber/keyed_consumer.rs` |
| Slow consumer offload | `mitiflow/src/subscriber/offload.rs` |
| Storage backend trait | `mitiflow/src/store/backend.rs` |
| Partition management | `mitiflow/src/partition/mod.rs`, `partition/hash_ring.rs` |
| Test helpers | `mitiflow/tests/common/mod.rs` |
| Storage agent | `mitiflow-storage/src/agent.rs` |
| Orchestrator HTTP API | `mitiflow-orchestrator/src/orchestrator.rs` |
| CLI entry point | `mitiflow-cli/src/main.rs` |
| Design documents | `docs/` |

## Documentation

- API docs: `cargo doc --workspace --no-deps --features full --open`
- Architecture: `docs/02_architecture.md`
- Design docs in `docs/` cover durability, sequencing, consumer groups, distributed storage, key-based publishing, slow consumer offload
- Copilot instructions: `.github/copilot-instructions.md`
- GitHub Agents: `.github/agents/architect.agent.md`
