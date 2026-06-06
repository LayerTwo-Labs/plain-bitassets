# Stable Rust version, as of January 2025.
FROM rust:1.84-slim-bookworm AS builder
WORKDIR /workspace
COPY . .

RUN cargo build --locked --release && \
    mkdir -p /artifacts && \
    cp /workspace/target/release/liquid_simplicity_app /artifacts/plain_bitassets_app && \
    cp /workspace/target/release/liquid_simplicity_app_cli /artifacts/plain_bitassets_app_cli

# Runtime stage
FROM debian:bookworm-slim

COPY --from=builder /artifacts/plain_bitassets_app /bin/plain_bitassets_app
COPY --from=builder /artifacts/plain_bitassets_app_cli /bin/plain_bitassets_app_cli

# Verify we placed the binaries in the right place,
# and that it's executable.
RUN plain_bitassets_app --help
RUN plain_bitassets_app_cli --help

ENTRYPOINT ["plain_bitassets_app"]
