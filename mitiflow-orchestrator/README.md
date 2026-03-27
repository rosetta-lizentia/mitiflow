# mitiflow-orchestrator

Control-plane service for mitiflow. Does **not** sit in the event data path.

## Features

- **Topic configuration** — CRUD via fjall-backed `ConfigStore` with Zenoh pub/sub distribution
- **Lag monitoring** — per-(group, partition, publisher) lag computation
- **Store lifecycle tracking** — online/offline detection via Zenoh liveliness
- **Cluster view** — aggregated node status, health, and partition assignments
- **Override & drain management** — node evacuation for maintenance
- **Alert manager** — under-replicated partition and node offline alerts
- **HTTP REST API** — embedded axum server for topics, cluster, and health
- **Zenoh admin API** — queryable endpoints on `_admin/**`

## Usage

```bash
# Via unified CLI
mitiflow orchestrator --config orchestrator.yaml

# HTTP API
curl http://localhost:8080/api/v1/health
curl http://localhost:8080/api/v1/topics
curl http://localhost:8080/api/v1/cluster/status
```

## License

Apache-2.0 — see [LICENSE](../LICENSE).
