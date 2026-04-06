# Mitiflow Documentation

Brokerless event streaming built on [Zenoh](https://zenoh.io). These documents cover everything from first setup to production deployment to internal design decisions.

---

## Tutorials & Guides

Start here if you're new to Mitiflow.

| Document | Description |
|----------|-------------|
| [Getting Started](getting_started.md) | Step-by-step tutorial: installation, pub/sub, keyed events, durable writes, consumer groups, CLI |
| [Deployment Guide](deployment.md) | Dev mode, container builds, compose stack, Zenoh topology, monitoring, troubleshooting |
| [Configuration Reference](configuration.md) | Complete reference for `EventBusConfig`, consumer groups, offload tuning, codec selection |

## Runnable Examples

All examples live in [`mitiflow/examples/`](../mitiflow/examples/) and can be run with:

```bash
cargo run -p mitiflow --features full --example <name>
```

| Example | What it demonstrates | Related docs |
|---------|---------------------|--------------|
| [`basic_pubsub`](../mitiflow/examples/basic_pubsub.rs) | Core pub/sub with gap detection and sequencing | [Getting Started](getting_started.md#2-basic-pubsub), [Sequencing](04_sequencing_and_replay.md) |
| [`keyed_pubsub`](../mitiflow/examples/keyed_pubsub.rs) | Key-filtered subscriptions and partition routing | [Getting Started](getting_started.md#3-key-based-publishing), [Key-Based Publishing](15_key_based_publishing.md) |
| [`durable_publish`](../mitiflow/examples/durable_publish.rs) | Watermark-confirmed persistent writes | [Getting Started](getting_started.md#4-durable-publishing), [Durability](03_durability.md) |
| [`consumer_groups`](../mitiflow/examples/consumer_groups.rs) | Partition assignment, offset commits, rebalancing | [Getting Started](getting_started.md#5-consumer-groups), [Consumer Groups](11_consumer_group_commits.md) |
| [`event_store`](../mitiflow/examples/event_store.rs) | Store queries, replay, compaction, GC | [Getting Started](getting_started.md#6-event-store-queries), [Architecture](02_architecture.md) |
| [`dead_letter_queue`](../mitiflow/examples/dead_letter_queue.rs) | Retry logic and poison message isolation | [Getting Started](getting_started.md#7-dead-letter-queue) |
| [`slow_consumer_offload`](../mitiflow/examples/slow_consumer_offload.rs) | Automatic store-based catch-up for slow subscribers | [Getting Started](getting_started.md#8-slow-consumer-offload), [Slow Consumer Offload](17_slow_consumer_offload.md) |
| [`test_queryable`](../mitiflow/examples/test_queryable.rs) | Publisher cache recovery via Zenoh queryable | [Cache Recovery](09_cache_recovery_design.md) |

---

## Architecture & Design

Deep dives into how Mitiflow works internally. Read these to understand design trade-offs or contribute to the project.

### Core Design

| Document | Topic |
|----------|-------|
| [Proposal & Motivation](00_proposal.md) | Why Zenoh + Mitiflow — vision and goals |
| [Zenoh Capabilities](01_zenoh_capabilities.md) | Stable Zenoh APIs used as the foundation |
| [Architecture](02_architecture.md) | System design, 7-crate structure, metadata protocol, feature flags |
| [Comparison](06_comparison.md) | Performance benchmarks vs Kafka, NATS, Pulsar, Redis, Redpanda |

### Reliability & Durability

| Document | Topic |
|----------|-------|
| [Durability](03_durability.md) | Watermark protocol, durability strategy matrix, Event Store role |
| [Sequencing & Replay](04_sequencing_and_replay.md) | Per-(partition, publisher) sequencing, HLC replay ordering |
| [Replication](05_replication.md) | Multi-store fan-out, quorum watermarks, no Raft needed |
| [Cache Recovery](09_cache_recovery_design.md) | Three-tier recovery: publisher cache → Event Store, ZBytes optimization |

### Consumer Patterns

| Document | Topic |
|----------|-------|
| [Consumer Group Commits](11_consumer_group_commits.md) | Offset commits, zombie fencing, generation management, auto-commit |
| [Key-Based Publishing](15_key_based_publishing.md) | 4-phase design: partitioning, store key index, log compaction, filtering |
| [Slow Consumer Offload](17_slow_consumer_offload.md) | Lag detection, debouncing, store catch-up, adaptive batch sizing |

### Operations & Infrastructure

| Document | Topic |
|----------|-------|
| [Distributed Storage](13_distributed_storage.md) | StorageAgent, rendezvous hashing, two-tier architecture, recovery |
| [Multi-Topic & DX](16_dx_and_multi_topic.md) | Dynamic topic discovery, provisioning protocol, deployment modes |
| [Topic Schema Registry](18_topic_schema_registry.md) | Distributed schema validation, auto-configuration, codec/partition agreement |
| [Graceful Termination](10_graceful_termination.md) | Async `shutdown()`, background task cancellation |

---

## Project Status

| Document | Description |
|----------|-------------|
| [Implementation Plan](implementation_plan.md) | Phase-by-phase status — what's done and what's in progress |
| [Roadmap](ROADMAP.md) | Planned features: Kafka gateway, pull-based consumers, and more |

---

## API Documentation

Generate and browse the full Rust API docs:

```bash
cargo doc --workspace --no-deps --features full --open
```

---

## Crate Map

| Crate | Purpose | Key entry point |
|-------|---------|-----------------|
| [`mitiflow`](../mitiflow/) | Core library — publisher, subscriber, event store, partitions, DLQ | [`src/lib.rs`](../mitiflow/src/lib.rs) |
| [`mitiflow-storage`](../mitiflow-storage/) | Storage agent — distributed partition management | [`src/agent.rs`](../mitiflow-storage/src/agent.rs) |
| [`mitiflow-orchestrator`](../mitiflow-orchestrator/) | Control plane — config CRUD, lag monitoring, HTTP API | [`src/orchestrator.rs`](../mitiflow-orchestrator/src/orchestrator.rs) |
| [`mitiflow-cli`](../mitiflow-cli/) | Unified CLI binary | [`src/main.rs`](../mitiflow-cli/src/main.rs) |
| [`mitiflow-emulator`](../mitiflow-emulator/) | YAML-driven topology runner and chaos testbed | [`src/`](../mitiflow-emulator/src/) |
| [`mitiflow-bench`](../mitiflow-bench/) | Comparative benchmarks | [`src/`](../mitiflow-bench/src/) |
| [`mitiflow-ui`](../mitiflow-ui/) | Svelte 5 dashboard (standalone, not in Cargo workspace) | [`src/`](../mitiflow-ui/src/) |
