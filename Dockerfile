FROM rust:1.85-slim-bookworm AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy only Cargo.toml (and Cargo.lock if it exists)
COPY Cargo.toml ./
# If Cargo.lock exists, copy it; otherwise ignore
COPY Cargo.lock* ./

# Dummy main.rs to build dependencies
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release || cargo build --release
RUN rm -rf src

# Copy real source
COPY src ./src

# Force rebuild
RUN touch src/main.rs && cargo build --release

# Runtime
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/ingest-rust .
CMD ["./ingest-rust"]
