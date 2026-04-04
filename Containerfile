# Production Containerfile for Mitiflow (Agent & Orchestrator)
# Multi-stage build with cargo-chef for optimal layer caching
#
# Build with:
#   podman build --build-arg PACKAGE=mitiflow-agent -t mitiflow-agent .
#   podman build --build-arg PACKAGE=mitiflow-orchestrator --build-arg BUILD_UI=true -t mitiflow-orchestrator .

ARG PACKAGE=mitiflow-agent

# ============================================
# Stage 1: Chef (install cargo-chef)
# ============================================
FROM docker.io/library/rust:1.94-slim-bookworm AS chef

RUN apt-get update && apt-get install -y \
    libssl-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/* \
    && cargo install cargo-chef

WORKDIR /app

# ============================================
# Stage 2: Planner (generate recipe.json)
# ============================================
FROM chef AS planner

# Copy workspace manifests
COPY Cargo.toml Cargo.lock ./

# Rewrite workspace members to include only production crates
RUN sed -i '/^members/,/^\]/c\members = [\n    "mitiflow",\n    "mitiflow-agent",\n    "mitiflow-orchestrator",\n]' Cargo.toml

COPY mitiflow ./mitiflow
COPY mitiflow-agent ./mitiflow-agent
COPY mitiflow-orchestrator ./mitiflow-orchestrator

RUN cargo chef prepare --recipe-path recipe.json

# ============================================
# Stage 3: Dependency Builder (shared cache layer)
# ============================================
FROM chef AS deps

COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

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

RUN if [ "$BUILD_UI" = "true" ]; then \
      cd mitiflow-ui && pnpm install --frozen-lockfile; \
    fi

COPY mitiflow-ui ./mitiflow-ui

RUN mkdir -p /app/mitiflow-ui/build && \
    if [ "$BUILD_UI" = "true" ]; then \
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
RUN sed -i '/^members/,/^\]/c\members = [\n    "mitiflow",\n    "mitiflow-agent",\n    "mitiflow-orchestrator",\n]' Cargo.toml

COPY mitiflow ./mitiflow
COPY mitiflow-agent ./mitiflow-agent
COPY mitiflow-orchestrator ./mitiflow-orchestrator

# Copy pre-built UI assets for orchestrator build
COPY --from=ui-builder /app/mitiflow-ui/build ./mitiflow-ui/build

# Build the specified package (dependencies already cached)
RUN if [ "$BUILD_UI" = "true" ]; then \
      cargo build --release --package ${PACKAGE} --features ui; \
    else \
      cargo build --release --package ${PACKAGE}; \
    fi

# ============================================
# Stage 5: Runtime
# ============================================
FROM docker.io/library/debian:bookworm-slim AS runtime

ARG PACKAGE

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    curl \
    tini \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 -s /bin/bash appuser

# Create writable data directory (orchestrator defaults to ./orchestrator_data)
RUN mkdir -p /app/orchestrator_data && chown -R appuser:appuser /app

# Copy built binary from builder stage
RUN --mount=from=builder,source=/app/target/release,target=/build \
    cp /build/${PACKAGE} /usr/local/bin/app; \
    chown appuser:appuser /usr/local/bin/app

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
