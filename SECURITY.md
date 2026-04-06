# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in mitiflow, please report it
responsibly. **Do not open a public GitHub issue.**

Instead, please email security concerns to the maintainers via GitHub's
private vulnerability reporting feature:

1. Go to the [Security tab](../../security) of this repository.
2. Click **"Report a vulnerability"**.
3. Provide a description of the issue, steps to reproduce, and potential impact.


## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | Yes       |

## Scope

The following are in scope for security reports:

- **mitiflow** core library (publisher, subscriber, store, partitions)
- **mitiflow-storage** (storage agent, membership, reconciler)
- **mitiflow-orchestrator** (control plane, HTTP API)
- **mitiflow-cli** (unified binary)

The following are **out of scope**:

- **mitiflow-gateway** — not yet implemented (stub only)
- **mitiflow-bench** — benchmarking tool, not production code
- **mitiflow-emulator** — testing tool, not production code
- Vulnerabilities in upstream dependencies (report those to the respective projects)
