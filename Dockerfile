# syntax=docker/dockerfile:1

# ── Builder stage ────────────────────────────────────────────────
FROM python:3.14-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        libcurl4-openssl-dev \
        libssl-dev \
        unzip \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Install dependencies first (layer caching)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Copy project source and install it
COPY scrape_exchange/ scrape_exchange/
COPY tools/ tools/
RUN uv sync --frozen --no-dev

# ── Runtime stage ────────────────────────────────────────────────
FROM python:3.14-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        libcurl4 \
        unzip \
    && rm -rf /var/lib/apt/lists/*

# Install Deno (required by video scraper for PO token handling)
ENV DENO_INSTALL=/usr/local/deno
RUN curl -fsSL https://deno.land/install.sh \
    | DENO_INSTALL=${DENO_INSTALL} sh
ENV PATH="${DENO_INSTALL}/bin:${PATH}"
ENV DENO_PATH="${DENO_INSTALL}/bin/deno"

WORKDIR /app

# Copy the venv from builder
COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONPATH=/app

# Copy project source
COPY scrape_exchange/ scrape_exchange/
COPY tools/ tools/

# Create default directories
RUN mkdir -p /data/videos /data/channels \
    /var/log/scrape/scraper \
    /var/tmp/yt_dlp_cache

# Sensible defaults for container environment
ENV YOUTUBE_VIDEO_DATA_DIR=/data/videos \
    YOUTUBE_CHANNEL_DATA_DIR=/data/channels \
    YOUTUBE_CHANNEL_LIST=/data/channels.lst \
    YOUTUBE_CHANNEL_MAP_FILE=/data/channel_map.csv \
    RSS_QUEUE_FILE=/data/rss-queue.json \
    NO_FEEDS_FILE=/data/rss-no-feeds.txt \
    YTDLP_CACHE_DIR=/var/tmp/yt_dlp_cache \
    LOG_FORMAT=json \
    PO_TOKEN_URL=http://localhost:4416

ENTRYPOINT ["python"]
