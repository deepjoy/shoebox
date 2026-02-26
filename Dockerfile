# Stage 1: Build
FROM rust:slim AS builder

ARG TARGETARCH

WORKDIR /build

# Install build dependencies for SQLite (required by sqlx)
RUN apt-get update && apt-get install -y pkg-config libsqlite3-dev && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY migrations ./migrations
COPY src ./src

# Cache mounts: cargo registry is arch-independent, target dir is per-arch
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/target,id=cargo-target-${TARGETARCH} \
    cargo build --release && cp target/release/shoebox /usr/local/bin/shoebox

# Stage 2: Runtime
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y libsqlite3-0 ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/bin/shoebox /usr/local/bin/shoebox

# Default data directory
RUN mkdir -p /data

EXPOSE 9000

ENTRYPOINT ["shoebox"]
CMD ["/data"]
