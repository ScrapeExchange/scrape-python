# syntax=docker/dockerfile:1

# ── Builder ──────────────────────────────────────────────────────
FROM rust:1.97-slim-trixie AS builder
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked \
    && cp target/release/backlog-server /usr/local/bin/backlog-server

# ── Runtime ──────────────────────────────────────────────────────
FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        redis-tools \
        rsync \
        openssh-client \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/bin/backlog-server /usr/local/bin/
# Runs as the scrape user so every file it writes is owned by 1002.
USER 1002:1002
ENTRYPOINT ["backlog-server"]
CMD ["serve"]
