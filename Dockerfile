# Stable Rust version, as of January 2025. 
FROM rust:1.84-slim-bookworm AS builder
WORKDIR /workspace
COPY . .

RUN cargo build --locked --release

# Runtime stage
FROM debian:bookworm-slim

COPY --from=builder /workspace/target/release/plain_bitassets_app /bin/plain_bitassets_app
COPY --from=builder /workspace/target/release/plain_bitassets_app_cli /bin/plain_bitassets_app_cli

# Verify we placed the binaries in the right place, 
# and that it's executable.
RUN plain_bitassets_app --help
RUN plain_bitassets_app_cli --help

ENTRYPOINT ["plain_bitassets_app"]

