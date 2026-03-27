

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
    cargo install --path mitiflow-cli/