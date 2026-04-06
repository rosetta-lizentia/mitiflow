# Deployment Guide

This guide covers deploying Mitiflow — from single-node development to multi-node container-based production setups.

> **Prerequisite:** Familiar with Mitiflow concepts? If not, start with the [Getting Started](getting_started.md) guide.

---

## Table of Contents

1. [Deployment Modes](#1-deployment-modes)
2. [Dev Mode (Single Process)](#2-dev-mode-single-process)
3. [Production with Containers](#3-production-with-containers)
4. [Environment Variables](#4-environment-variables)
5. [Building Container Images](#5-building-container-images)
6. [Compose Stack](#6-compose-stack)
7. [Without Containers](#7-without-containers)
8. [Zenoh Network Topology](#8-zenoh-network-topology)
9. [Monitoring & Observability](#9-monitoring--observability)
10. [Troubleshooting](#10-troubleshooting)

---

## 1. Deployment Modes

Mitiflow is brokerless — there is no central process that routes messages. You choose how many operational components to run based on your needs:

| Mode | Components | Use case |
|------|-----------|----------|
| **Library only** | Your app + Mitiflow crate | Embedded streaming, no separate processes |
| **Dev mode** | `mitiflow dev` (all-in-one) | Local development and testing |
| **Production** | Storage agent(s) + Orchestrator (optional) + Zenoh router (optional) | Multi-node deployments |

**Key insight:** The orchestrator and storage agent are *operational utilities*, not message routers. Your publishers and subscribers communicate directly over Zenoh peer-to-peer — the system works without any infrastructure processes.

> **See also:** [Architecture](02_architecture.md) for component roles, [Distributed Storage](13_distributed_storage.md) for multi-node storage design.

---

## 2. Dev Mode (Single Process)

The fastest way to run the full stack locally:

```bash
# Install the CLI
cargo install --features full --path mitiflow-cli/
# Or: just install-cli

# Start everything in one process
mitiflow dev --topics "my-topic:8:1"
#                       name:partitions:replication_factor
```

This starts:
- An embedded Zenoh session (peer mode, no router)
- An Event Store with fjall backend
- The orchestrator HTTP API on `http://localhost:8080`
- Storage agent for partition management

You can interact immediately:
```bash
# Check cluster status
mitiflow ctl cluster status

# List topics
mitiflow ctl topics list

# Run diagnostics
mitiflow ctl diagnose
```

---

## 3. Production with Containers

### Architecture

```
┌──────────────┐      ┌──────────────────┐      ┌──────────────┐
│ Zenoh Router │◄────►│   Orchestrator    │◄────►│    Storage    │
│  (optional)  │      │  (HTTP API :8080) │      │    Agent      │
│    :7447     │      │  + embedded UI    │      │  (fjall LSM)  │
└──────┬───────┘      └──────────────────┘      └───────────────┘
       │
       │  Zenoh peer-to-peer mesh
       │
┌──────┴───────┐      ┌──────────────────┐
│  Publisher   │◄────►│   Subscriber     │
│  (your app)  │      │   (your app)     │
└──────────────┘      └──────────────────┘
```

All components connect to the Zenoh router (or directly in peer-to-peer mode). Messages flow directly between publishers and subscribers — they never pass through the router.

---

## 4. Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MITIFLOW_KEY_PREFIX` | `mitiflow` | Zenoh key expression prefix for all topics |
| `MITIFLOW_DATA_DIR` | `/data` | Directory for persistent storage (fjall LSM) |
| `MITIFLOW_HTTP_BIND` | `0.0.0.0:8080` | Orchestrator HTTP bind address |
| `MITIFLOW_NUM_PARTITIONS` | `16` | Default partition count per topic |
| `ZENOH_CONNECT` | — | Zenoh endpoint (e.g., `tcp/zenoh-router:7447`) |
| `RUST_LOG` | `info` | Logging filter (e.g., `mitiflow_storage=debug`) |

---

## 5. Building Container Images

The [`Containerfile`](../Containerfile) uses a multi-stage build optimized for caching:

```bash
# Build the storage agent image
podman build --build-arg PACKAGE=mitiflow-storage -t mitiflow-storage .

# Build the orchestrator (with embedded web UI)
podman build \
    --build-arg PACKAGE=mitiflow-orchestrator \
    --build-arg BUILD_UI=true \
    -t mitiflow-orchestrator .

# Or use the justfile shortcuts:
just container-agent
just container-orchestrator
just container-all          # Both images
```

**Build stages:** cargo-chef recipe → dependency cache → Svelte UI (if `BUILD_UI=true`) → Rust binary → Debian slim runtime with `tini` for signal handling.

---

## 6. Compose Stack

The [`docker-compose.yml`](../docker-compose.yml) defines a ready-to-use three-service stack:

```bash
# Start all services
podman compose up -d

# Start only storage (with Zenoh router dependency)
podman compose up -d storage

# View logs
podman compose logs -f

# Stop and clean up
podman compose down -v
```

### Services

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| `zenoh-router` | `eclipse/zenoh:latest` | 7447 | Zenoh message routing (optional in peer-to-peer mode) |
| `orchestrator` | `mitiflow-orchestrator` | 8080 | HTTP admin API + embedded web UI |
| `storage` | `mitiflow-storage` | — | Multi-topic storage agent |

### Persistent volumes

- `orchestrator_data` → `/data` in orchestrator container
- `storage_data` → `/data` in storage container

---

## 7. Without Containers

Run each component as a standalone binary:

```bash
# Terminal 1: Zenoh router (optional)
zenohd

# Terminal 2: Storage agent
mitiflow storage --config agent.yaml

# Terminal 3: Orchestrator
mitiflow orchestrator --config orchestrator.yaml

# Your application links against the mitiflow crate directly.
```

### Minimal agent config (`agent.yaml`)

```yaml
key_prefix: myapp
data_dir: /var/lib/mitiflow/storage
num_partitions: 16
zenoh:
  connect:
    - tcp/localhost:7447
```

### Minimal orchestrator config (`orchestrator.yaml`)

```yaml
key_prefix: myapp
data_dir: /var/lib/mitiflow/orchestrator
http_bind: 0.0.0.0:8080
zenoh:
  connect:
    - tcp/localhost:7447
```

---

## 8. Zenoh Network Topology

Mitiflow supports two Zenoh topologies:

### Peer-to-Peer (default for dev)

All nodes discover each other via multicast scouting. Simple but limited to a single network segment.

```
┌─────────┐     multicast     ┌─────────┐
│  Node A │◄────scouting────►│  Node B │
└─────────┘                   └─────────┘
```

### Routed (recommended for production)

A Zenoh router provides a stable rendezvous point. Nodes connect via TCP. Works across network boundaries.

```
┌─────────┐     tcp/7447     ┌──────────┐     tcp/7447     ┌─────────┐
│  Node A │────────────────►│  Router  │◄────────────────│  Node B │
└─────────┘                  └──────────┘                  └─────────┘
```

Set `ZENOH_CONNECT=tcp/<router-host>:7447` on each node, or configure it in YAML:

```yaml
zenoh:
  connect:
    - tcp/router.example.com:7447
```

> **See also:** [Zenoh Capabilities](01_zenoh_capabilities.md) for the stable Zenoh APIs Mitiflow relies on.

---

## 9. Monitoring & Observability

### Orchestrator HTTP API

When the orchestrator is running, it exposes REST endpoints on the configured bind address:

```bash
# Cluster overview
curl http://localhost:8080/api/cluster/status

# List topics
curl http://localhost:8080/api/topics

# Topic details
curl http://localhost:8080/api/topics/my-topic

# Consumer group lag
curl http://localhost:8080/api/groups/my-group/lag
```

### Web UI

Build the orchestrator with `BUILD_UI=true` to embed the Svelte dashboard at `http://localhost:8080/`. It provides:
- Cluster status and node health
- Topic management
- Consumer group lag monitoring

### Logging

All components use `tracing` with `RUST_LOG` filter:

```bash
# Storage agent debug logging
RUST_LOG=mitiflow_storage=debug mitiflow storage --config agent.yaml

# Verbose Zenoh protocol tracing
RUST_LOG=mitiflow=debug,zenoh=trace mitiflow dev --topics "test:4:1"
```

### CLI Diagnostics

```bash
# Health check — tests Zenoh connectivity and store responsiveness
mitiflow ctl diagnose --timeout 10
```

---

## 10. Troubleshooting

### "Unable to push non droppable network message — Closing transport!"

**Cause:** Stale Zenoh peers from previous runs remain in the multicast scouting mesh, causing transport buffer overflow.

**Fix:** Kill orphaned processes before restarting:
```bash
pkill -9 -f mitiflow
```

### Store not responding to queries

**Check:**
1. Is the storage agent running? `mitiflow ctl cluster status`
2. Is the Zenoh key prefix matching? Both publisher and store must use the same `key_prefix`.
3. Are partitions assigned? `mitiflow ctl topics get <topic>`

### Consumer group not rebalancing

**Cause:** Liveliness tokens require Zenoh router or peer-to-peer scouting to propagate leave events.

**Fix:** Ensure all members use the same `worker_liveliness_prefix` and are on the same Zenoh network.

> **See also:** [Consumer Group Commits](11_consumer_group_commits.md) for the rebalancing protocol, [Graceful Termination](10_graceful_termination.md) for clean shutdown.

### Durable publish timeouts

**Cause:** No Event Store is running, or the store hasn't subscribed to the publisher's key prefix.

**Fix:**
1. Start an Event Store on the same key prefix.
2. Check `watermark_interval` — smaller values reduce latency but increase overhead.
3. Increase `durable_timeout` if the store is under heavy load.

> **See also:** [Durability](03_durability.md) for the watermark protocol and tuning.

### Slow subscriber performance degradation

If CPU is fine but throughput is low, check:
1. **Channel capacity** — increase `event_channel_capacity` (default: 1024).
2. **Processing shards** — set `num_processing_shards > 1` for multi-publisher streams.
3. **Offload** — enable slow consumer offload to avoid backpressure-induced drops. See [Slow Consumer Offload](17_slow_consumer_offload.md).
