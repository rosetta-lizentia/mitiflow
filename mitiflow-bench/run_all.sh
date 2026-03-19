#!/usr/bin/env bash
set -euo pipefail

# Cross-system messaging benchmark runner.
# Starts broker infrastructure via podman compose, runs benchmarks, collects CSVs.
#
# Usage:
#   ./run_all.sh                    # Run all benchmarks
#   ./run_all.sh pubsub             # Run only pub/sub benchmarks
#   ./run_all.sh durable            # Run only durable publish benchmarks
#   ./run_all.sh --transports zenoh,mitiflow  # Specific transports only

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
RESULTS_DIR="$SCRIPT_DIR/results"
CARGO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Defaults
DURATION=${DURATION:-10}
RATE=${RATE:-0}
PAYLOAD_SIZE=${PAYLOAD_SIZE:-256}
WORKERS=${WORKERS:-1}
CONSUMERS=${CONSUMERS:-1}
MODE="${1:-all}"

mkdir -p "$RESULTS_DIR"

# --- Infrastructure ---

start_services() {
    echo "==> Starting broker services via podman compose..."
    podman compose -f "$COMPOSE_FILE" up -d
    echo "==> Waiting for services to be healthy..."
    sleep 10
    # Check health
    for svc in kafka redpanda nats redis zenoh-router; do
        if podman compose -f "$COMPOSE_FILE" ps "$svc" 2>/dev/null | grep -q "Up"; then
            echo "    $svc: UP"
        else
            echo "    $svc: NOT RUNNING (skipping in benchmarks)"
        fi
    done
}

stop_services() {
    echo "==> Stopping broker services..."
    podman compose -f "$COMPOSE_FILE" down
}

# --- Build ---

build_bench() {
    echo "==> Building benchmarks (release mode, all features)..."
    cargo build --release -p mitiflow-bench --features full \
        --manifest-path "$CARGO_ROOT/Cargo.toml"
}

# --- Benchmark runners ---

run_pubsub() {
    local transport=$1
    local label="${2:-$1}"
    local extra_args="${3:-}"
    local csv_file="$RESULTS_DIR/pubsub_${label}_${PAYLOAD_SIZE}b.csv"

    local rate_display="${RATE:-unlimited}"
    echo "--- Pub/Sub: $label (payload=${PAYLOAD_SIZE}B, rate=${rate_display}, workers=${WORKERS}, consumers=${CONSUMERS}) ---"
    local rate_args=()
    if [[ -n "${RATE:-}" ]]; then
        rate_args=(--rate "$RATE")
    fi
    cargo run --release -p mitiflow-bench --features full \
        --manifest-path "$CARGO_ROOT/Cargo.toml" \
        --bin bench_pubsub -- \
        --transport "$transport" \
        --payload-size "$PAYLOAD_SIZE" \
        "${rate_args[@]+${rate_args[@]}}" \
        --workers "$WORKERS" \
        --consumers "$CONSUMERS" \
        --duration "$DURATION" \
        --csv "$csv_file" \
        $extra_args \
        || echo "  [WARN] $label pubsub benchmark failed"
    echo ""
}

run_durable() {
    local transport=$1
    local label="${2:-$1}"
    local extra_args="${3:-}"
    local csv_file="$RESULTS_DIR/durable_${label}_${PAYLOAD_SIZE}b.csv"

    local rate_display="${RATE:-unlimited}"
    echo "--- Durable: $label (payload=${PAYLOAD_SIZE}B, rate=${rate_display}, workers=${WORKERS}) ---"
    local rate_args=()
    if [[ -n "${RATE:-}" ]]; then
        rate_args=(--rate "$RATE")
    fi
    cargo run --release -p mitiflow-bench --features full \
        --manifest-path "$CARGO_ROOT/Cargo.toml" \
        --bin bench_durable -- \
        --transport "$transport" \
        --payload-size "$PAYLOAD_SIZE" \
        "${rate_args[@]+${rate_args[@]}}" \
        --workers "$WORKERS" \
        --duration "$DURATION" \
        --csv "$csv_file" \
        $extra_args \
        || echo "  [WARN] $label durable benchmark failed"
    echo ""
}

# --- Main ---

trap stop_services EXIT

start_services
build_bench

echo ""
echo "=============================="
echo " Benchmark Suite"
echo " Duration: ${DURATION}s per test"
echo " Payload:  ${PAYLOAD_SIZE}B"
echo " Rate:     ${RATE:-unlimited} msg/s"
echo " Results:  ${RESULTS_DIR}/"
echo "=============================="
echo ""

if [[ "$MODE" == "all" || "$MODE" == "pubsub" ]]; then
    echo "========== PUB/SUB BENCHMARKS =========="

    # Zenoh peer-to-peer (no router)
    run_pubsub zenoh zenoh-peer

    # Zenoh via router
    run_pubsub zenoh zenoh-router "--zenoh-connect tcp/localhost:7447"

    # Zenoh Advanced peer-to-peer
    run_pubsub zenoh-advanced zenoh-advanced-peer

    # Zenoh Advanced via router
    run_pubsub zenoh-advanced zenoh-advanced-router "--zenoh-connect tcp/localhost:7447"

    # Mitiflow peer-to-peer
    run_pubsub mitiflow mitiflow-peer

    # Mitiflow via router
    run_pubsub mitiflow mitiflow-router "--zenoh-connect tcp/localhost:7447"

    # Kafka
    run_pubsub kafka kafka "--kafka-broker localhost:9092"

    # Redpanda
    run_pubsub redpanda redpanda "--kafka-broker localhost:29092"

    # NATS
    run_pubsub nats nats "--nats-url nats://localhost:4222"

    # Redis Streams
    run_pubsub redis redis "--redis-url redis://localhost:6379"
fi

if [[ "$MODE" == "all" || "$MODE" == "durable" ]]; then
    echo "========== DURABLE PUBLISH BENCHMARKS =========="

    # Zenoh baseline peer-to-peer (fire-and-forget)
    run_durable zenoh zenoh-peer

    # Zenoh baseline via router (fire-and-forget)
    run_durable zenoh zenoh-router "--zenoh-connect tcp/localhost:7447"

    # Mitiflow peer-to-peer (publish_durable with EventStore)
    run_durable mitiflow mitiflow-peer

    # Mitiflow via router (publish_durable with EventStore)
    run_durable mitiflow mitiflow-router "--zenoh-connect tcp/localhost:7447"

    # Kafka acks=all
    run_durable kafka kafka "--kafka-broker localhost:9092"

    # Redpanda acks=all
    run_durable redpanda redpanda "--kafka-broker localhost:29092"

    # NATS JetStream
    run_durable nats nats "--nats-url nats://localhost:4222"

    # Redis XADD
    run_durable redis redis "--redis-url redis://localhost:6379"
fi

echo ""
echo "=============================="
echo " All benchmarks complete!"
echo " Results in: ${RESULTS_DIR}/"
echo "=============================="
ls -la "$RESULTS_DIR/"
