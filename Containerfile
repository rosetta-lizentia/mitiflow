# syntax=docker/dockerfile:1.7
# Production Containerfile for Mitiflow (Agent & Orchestrator)
# Multi-stage build with cargo-chef for optimal layer caching
#
# Build with:
#   podman build --build-arg PACKAGE=mitiflow-storage -t mitiflow-storage .
#   podman build --build-arg PACKAGE=mitiflow-orchestrator --build-arg BUILD_UI=true -t mitiflow-orchestrator .

ARG PACKAGE=mitiflow-storage

# ============================================
# Stage 1: Chef (install cargo-chef)
# ============================================
FROM docker.io/library/rust:1.94-slim-bookworm AS chef

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    apt-get update && apt-get install -y \
    libssl-dev \
    pkg-config \
    && cargo install cargo-chef --locked \
    && cargo install sccache --locked

ENV SCCACHE_DIR=/sccache \
    RUSTC_WRAPPER=/usr/local/cargo/bin/sccache

WORKDIR /app

# ============================================
# Stage 2: Planner (generate recipe.json)
# ============================================
FROM chef AS planner

# Copy workspace manifests
COPY Cargo.toml Cargo.lock ./

# Rewrite workspace members to include only production crates
RUN sed -i '/^members/,/^\]/c\members = [\n    "mitiflow",\n    "mitiflow-storage",\n    "mitiflow-orchestrator",\n]' Cargo.toml

COPY mitiflow ./mitiflow
COPY mitiflow-storage ./mitiflow-storage
COPY mitiflow-orchestrator ./mitiflow-orchestrator

RUN cargo chef prepare --recipe-path recipe.json

# ============================================
# Stage 3: Dependency Builder (shared cache layer)
# ============================================
FROM chef AS deps

COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    --mount=type=cache,target=/sccache,sharing=locked \
    cargo chef cook --release --recipe-path recipe.json

# ============================================
# Stage 4a: UI Builder (Node.js, only for orchestrator)
# ============================================
FROM docker.io/library/node:24-slim AS ui-builder

ARG BUILD_UI=false

RUN if [ "$BUILD_UI" = "true" ]; then \
      corepack enable && corepack prepare pnpm@10 --activate; \
    fi

WORKDIR /app

COPY mitiflow-ui/package.json mitiflow-ui/pnpm-lock.yaml ./mitiflow-ui/

RUN --mount=type=cache,target=/pnpm/store,sharing=locked \
    if [ "$BUILD_UI" = "true" ]; then \
      pnpm config set store-dir /pnpm/store && \
      cd mitiflow-ui && pnpm install --frozen-lockfile; \
    fi

COPY mitiflow-ui ./mitiflow-ui

RUN --mount=type=cache,target=/pnpm/store,sharing=locked \
    mkdir -p /app/mitiflow-ui/build && \
    if [ "$BUILD_UI" = "true" ]; then \
      pnpm config set store-dir /pnpm/store && \
      cd mitiflow-ui && pnpm build; \
    fi

# ============================================
# Stage 4b: Rust Builder (build specific package)
# ============================================
FROM deps AS builder

ARG PACKAGE
ARG BUILD_UI=false

# Copy workspace manifests
COPY Cargo.toml Cargo.lock ./

# Rewrite workspace members to include only production crates
RUN sed -i '/^members/,/^\]/c\members = [\n    "mitiflow",\n    "mitiflow-storage",\n    "mitiflow-orchestrator",\n]' Cargo.toml

COPY mitiflow ./mitiflow
COPY mitiflow-storage ./mitiflow-storage
COPY mitiflow-orchestrator ./mitiflow-orchestrator

# Copy pre-built UI assets for orchestrator build
COPY --from=ui-builder /app/mitiflow-ui/build ./mitiflow-ui/build

# Build the specified package (dependencies already cached)
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    --mount=type=cache,target=/sccache,sharing=locked \
    if [ "$BUILD_UI" = "true" ]; then \
      cargo build --release --package ${PACKAGE} --features ui; \
    else \
      cargo build --release --package ${PACKAGE}; \
    fi

# Copy binary from cache mount to a persistent location in the image layer
# Remove the source directory first to avoid cp placing the binary inside it
RUN --mount=type=cache,target=/app/target,sharing=locked \
    rm -rf /app/${PACKAGE} && \
    cp /app/target/release/${PACKAGE} /app/${PACKAGE}

# ============================================
# Stage 5: Runtime
# ============================================
FROM docker.io/library/debian:bookworm-slim AS runtime

ARG PACKAGE

WORKDIR /app

# Install runtime dependencies
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    curl \
    tini

# Create non-root user
RUN useradd -m -u 1000 -s /bin/bash appuser

# Create writable data directory (orchestrator defaults to ./orchestrator_data)
RUN mkdir -p /app/orchestrator_data && chown -R appuser:appuser /app

# Copy built binary from builder stage
COPY --from=builder /app/${PACKAGE} /usr/local/bin/app
RUN chmod +x /usr/local/bin/app && chown appuser:appuser /usr/local/bin/app

USER appuser

# Expose port (orchestrator HTTP API)
EXPOSE 8080

# Health check (orchestrator has HTTP endpoint, agent checks process)
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -sf http://localhost:8080/health 2>/dev/null || pgrep -x app || exit 1

# Use tini as PID 1 to properly forward signals (SIGTERM) to the app.
# This is essential for graceful shutdown in containers where the app
# runs as PID 2+ and Tokio signal handlers need a real PID 1 to proxy signals.
ENTRYPOINT ["tini", "--", "app"]
