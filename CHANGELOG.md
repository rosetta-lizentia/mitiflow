# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] — Unreleased

Initial release of mitiflow — a brokerless event streaming platform built on Zenoh.

### Added

#### Core Library (`mitiflow`)
- **Publisher** — `EventPublisher` with standard, keyed, and durable publish variants
- **Subscriber** — `EventSubscriber` with gap detection, recovery, and processing shards
- **Key-based publishing** — `publish_keyed()` family with automatic partition routing via `{prefix}/p/{partition}/k/{key}/{seq}` key expressions
- **Keyed subscriptions** — `EventSubscriber::new_keyed()` and `new_key_prefix()` for server-side key filtering
- **Consumer groups** — `ConsumerGroupSubscriber` with offset commits, zombie fencing (generation-based), auto-commit, and rebalancing
- **Event store** — `EventStore` with fjall LSM backend, replay ordering (HLC), key index, log compaction, and GC
- **Partitions** — `PartitionManager` using rendezvous (HRW) hashing with liveliness-based membership
- **Dead letter queue** — `DeadLetterQueue` with configurable backoff strategies (fixed, exponential)
- **Slow consumer offload** — automatic switchover to store-based catch-up when subscriber falls behind
- **Codecs** — JSON, MessagePack, and Postcard serialization via `CodecFormat`
- **Metadata attachments** — 50-byte binary header in Zenoh attachments (zero-copy routing without payload deserialization)
- **Durable publishing** — watermark-confirmed publishes with configurable timeout

#### Storage Agent (`mitiflow-storage`)
- **Distributed partition assignment** — weighted rendezvous hashing with rack-aware replica placement
- **Multi-topic support** — `TopicSupervisor` + `TopicWatcher` + `TopicWorker` for dynamic topic discovery
- **Reconciler** — desired vs actual state diffing with automatic store start/stop
- **Recovery** — peer-to-peer partition recovery via store queries with cache fallback
- **Membership** — liveliness-based node discovery with metadata propagation
- **Health & status reporting** — periodic heartbeats and on-change status updates
- **Override support** — manual partition assignment overrides with epoch and expiry
- **YAML configuration** — `serde_yaml` + `humantime_serde` config files

#### Orchestrator (`mitiflow-orchestrator`)
- **Topic configuration** — CRUD via fjall-backed `ConfigStore` with Zenoh pub/sub distribution
- **Lag monitoring** — per-(group, partition, publisher) lag computation
- **Store lifecycle tracking** — online/offline detection via liveliness
- **Cluster view** — aggregated node status, health, and partition assignments
- **Override & drain management** — node evacuation for maintenance
- **Alert manager** — under-replicated partition and node offline alerts
- **HTTP REST API** — axum-based endpoints for topics, cluster, and health
- **Zenoh admin API** — queryable endpoints on `_admin/**`

#### CLI (`mitiflow-cli`)
- **Unified binary** — `mitiflow agent`, `mitiflow orchestrator`, `mitiflow ctl`, `mitiflow dev`
- **Admin commands** — topics list/get/create/delete, cluster nodes/status/drain/undrain
- **Diagnostics** — `mitiflow ctl diagnose` for connectivity and health checks
- **Dev mode** — co-located orchestrator + agent for local development

#### Emulator (`mitiflow-emulator`)
- **YAML topology runner** — declarative topology definition with roles
- **Chaos engineering** — fault injection (network partition, latency, process kill)
- **Multiple backends** — native process spawning and Docker/Podman containers
- **Log aggregation** — centralized log collection from distributed roles

#### Benchmarks (`mitiflow-bench`)
- **Comparative benchmarks** — mitiflow vs Kafka, NATS, Redis, Redpanda
- **Criterion micro-benchmarks** — codec, attachment, gap detector, partition, store, watermark, checkpoint
