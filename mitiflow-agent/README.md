# mitiflow-agent

Distributed storage management for mitiflow.

Per-node daemon that manages `EventStore` instances via decentralized partition
assignment using weighted rendezvous hashing and Zenoh liveliness.

## Features

- **Weighted HRW** — capacity-proportional partition assignment
- **Rack-aware replicas** — diversity-first replica placement via labels
- **Multi-topic** — single agent process serves multiple topics, each with
  independent `TopicWorker` (reconciler, membership, recovery, status)
- **Dynamic topic discovery** — `TopicWatcher` subscribes to orchestrator
  config events; agents automatically start/stop workers
- **Reconciler** — desired vs actual state diffing with automatic store lifecycle
- **Peer recovery** — partition data recovery via store queries with cache fallback
- **Override support** — manual partition assignment with epoch and expiry
- **Health & status** — periodic heartbeats and on-change status updates
- **YAML configuration** — `serde_yaml` + `humantime_serde` config files

## Usage

```bash
# Via unified CLI
mitiflow agent --config agent.yaml

# Directly
cargo run -p mitiflow-agent -- --config agent.yaml
```

## Configuration

```yaml
node:
  id: node-1
  data_dir: /var/lib/mitiflow
  labels:
    rack: us-east-1a

cluster:
  global_prefix: mitiflow
  replication_factor: 2
  auto_discover_topics: true

topics:
  - name: events
    key_prefix: myapp/events
    num_partitions: 16
```

## License

Apache-2.0 — see [LICENSE](../LICENSE).
