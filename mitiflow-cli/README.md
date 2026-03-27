# mitiflow-cli

Unified command-line interface for the mitiflow event streaming platform.

Single binary with subcommands for running all mitiflow components and
administering a running cluster.

## Subcommands

### `mitiflow agent`

Run a storage agent (multi-topic capable).

```bash
mitiflow agent --config agent.yaml
```

### `mitiflow orchestrator`

Run the orchestrator control plane.

```bash
mitiflow orchestrator --config orchestrator.yaml
```

### `mitiflow ctl`

Admin CLI for managing topics and cluster state.

```bash
mitiflow ctl topics list
mitiflow ctl topics get my-topic
mitiflow ctl topics create --name my-topic --partitions 8 --replication-factor 2
mitiflow ctl cluster status
mitiflow ctl cluster drain node-1
mitiflow ctl diagnose
```

### `mitiflow dev`

All-in-one development mode — co-locates orchestrator and a single-node agent.

```bash
mitiflow dev --topics "my-topic:8:1"
```

## Installation

```bash
cargo install --path mitiflow-cli/
```

## License

Apache-2.0 — see [LICENSE](../LICENSE).
