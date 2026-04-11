# Use Rust 1.85 official image (Debian slim variant)
FROM rust:1.85-slim-bookworm AS builder

WORKDIR /app

# Install system dependencies for Postgres client (openssl)
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency manifests first to cache dependencies
COPY Cargo.toml Cargo.lock ./

# Create a dummy main.rs to build dependencies only
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release
RUN rm -rf src

# Copy the actual source code
COPY src ./src

# Build the actual binary (touch main.rs to force rebuild)
RUN touch src/main.rs && cargo build --release

# Final runtime stage
FROM debian:bookworm-slim

# Install runtime libraries
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the compiled binary from builder
COPY --from=builder /app/target/release/ingest-rust .

# Run the binary
CMD ["./ingest-rust"]
