# Stage 1: Build
FROM rust:slim AS builder

WORKDIR /build

# Install build dependencies for SQLite (required by sqlx)
RUN apt-get update && apt-get install -y pkg-config libsqlite3-dev && rm -rf /var/lib/apt/lists/*

# Cache dependency build: copy manifests first, create dummy src, build deps
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && echo "" > src/lib.rs
RUN cargo build --release && rm -rf src

# Build the real binary
COPY src ./src
RUN touch src/main.rs src/lib.rs && cargo build --release

# Stage 2: Runtime
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y libsqlite3-0 ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/shoebox /usr/local/bin/shoebox

# Default data directory
RUN mkdir -p /data

EXPOSE 9000

ENTRYPOINT ["shoebox"]
CMD ["/data"]
