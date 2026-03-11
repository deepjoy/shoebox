# Stage 1: Base image with Rust + cargo-chef
FROM lukemathwalker/cargo-chef:latest-rust-slim AS chef
WORKDIR /build

# Stage 2: Generate dependency recipe
FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY src ./src
RUN cargo chef prepare --recipe-path recipe.json

# Stage 3: Build dependencies (cached layer when deps unchanged)
FROM chef AS builder
RUN apt-get update && apt-get install -y pkg-config libsqlite3-dev && rm -rf /var/lib/apt/lists/*
COPY --from=planner /build/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Stage 4: Build application
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY migrations ./migrations
COPY src ./src
RUN cargo build --release

# Stage 5: Runtime
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y libsqlite3-0 ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/shoebox /usr/local/bin/shoebox

# Default data directory
RUN mkdir -p /data

EXPOSE 9000

ENTRYPOINT ["shoebox"]
CMD ["/data"]
