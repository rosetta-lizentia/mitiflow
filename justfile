

build:
    cargo build

test:
    cargo nextest run --no-fail-fast

test-all:
    cargo nextest run --workspace --features full --no-fail-fast

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all -- --check

clippy:
    cargo clippy --workspace --all-targets --features full -- -D warnings

doc:
    cargo doc --workspace --no-deps --features full

doc-open:
    cargo doc --workspace --no-deps --features full --open

bench:
    cargo bench -p mitiflow

publish-dry:
    cargo publish --dry-run -p mitiflow

check:
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets --features full -- -D warnings
    cargo nextest run --workspace --features full --no-fail-fast

install-cli:
    cargo install --features full --path mitiflow-cli/

# Build the Svelte UI (requires pnpm)
ui-build:
    cd mitiflow-ui && pnpm install && pnpm build

# Build the orchestrator with embedded UI
build-with-ui: ui-build
    cargo build -p mitiflow-orchestrator --features ui