# Multi-stage build for Lance Distributed binaries
FROM rust:1.88-bookworm AS builder

WORKDIR /build
COPY . .

RUN CARGO_BUILD_JOBS=1 cargo build --release \
    -p lance-distributed-coordinator \
    -p lance-distributed-worker && \
    cp target/release/lance-coordinator /usr/local/bin/ && \
    cp target/release/lance-worker /usr/local/bin/

# Runtime image
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libssl3 curl python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

# Install Python deps for init container
RUN pip install --break-system-packages lance pyarrow numpy pyyaml 2>/dev/null || true

# Non-root user for security
RUN groupadd -r lance && useradd -r -g lance -d /home/lance -s /sbin/nologin lance

COPY --from=builder /usr/local/bin/lance-coordinator /usr/local/bin/
COPY --from=builder /usr/local/bin/lance-worker /usr/local/bin/

USER lance

EXPOSE 50050 50100

HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
    CMD curl -sf http://localhost:${HEALTH_PORT:-50051}/healthz || exit 1

ENTRYPOINT []
