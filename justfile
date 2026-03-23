

build:
    cargo build

test:
    cargo nextest run --no-fail-fast

install-cli:
    cargo install --path mitiflow-cli/