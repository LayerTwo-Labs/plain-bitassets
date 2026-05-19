# syntax=docker/dockerfile:1.7
# Stable Rust version, as of January 2025.
FROM rust:1.84-slim-bookworm AS builder
WORKDIR /workspace
COPY . .

RUN --mount=type=cache,id=plain-bitassets-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=plain-bitassets-cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=plain-bitassets-target-amd64,target=/workspace/target \
    cargo build --locked --release && \
    mkdir -p /artifacts && \
    cp /workspace/target/release/plain_bitassets_app /artifacts/plain_bitassets_app && \
    cp /workspace/target/release/plain_bitassets_app_cli /artifacts/plain_bitassets_app_cli

# Runtime stage
FROM debian:bookworm-slim

COPY --from=builder /artifacts/plain_bitassets_app /bin/plain_bitassets_app
COPY --from=builder /artifacts/plain_bitassets_app_cli /bin/plain_bitassets_app_cli

# Verify we placed the binaries in the right place,
# and that it's executable.
RUN plain_bitassets_app --help
RUN plain_bitassets_app_cli --help

ENTRYPOINT ["plain_bitassets_app"]
