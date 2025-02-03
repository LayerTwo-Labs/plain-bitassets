# Stable Rust version, as of January 2025. 
FROM rust:1.84-slim-bookworm AS builder
WORKDIR /workspace
COPY . .

RUN cargo build --locked --release

# Runtime stage
FROM debian:bookworm-slim

COPY --from=builder /workspace/target/release/plain_bitnames_app /bin/plain_bitnames_app
COPY --from=builder /workspace/target/release/plain_bitnames_app_cli /bin/plain_bitnames_app_cli

# Verify we placed the binaries in the right place, 
# and that it's executable.
RUN plain_bitnames_app --help
RUN plain_bitnames_app_cli --help

ENTRYPOINT ["plain_bitnames_app"]

