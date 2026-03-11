# Stage 1: Generate dependency recipe
FROM rust:slim AS planner
RUN cargo install cargo-chef
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY src ./src
RUN cargo chef prepare --recipe-path recipe.json

# Stage 2: Build dependencies (cached layer when deps unchanged)
FROM rust:slim AS builder
RUN cargo install cargo-chef
RUN apt-get update && apt-get install -y pkg-config libsqlite3-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY --from=planner /build/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Stage 3: Build application
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY migrations ./migrations
COPY src ./src
RUN cargo build --release

# Stage 4: Runtime
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y libsqlite3-0 ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/shoebox /usr/local/bin/shoebox

# Default data directory
RUN mkdir -p /data

EXPOSE 9000

ENTRYPOINT ["shoebox"]
CMD ["/data"]
