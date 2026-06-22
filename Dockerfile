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

# Fetch runtime assets while download tools are available in this
# disposable stage. Only the installed artifacts are copied below.
ENV DENO_INSTALL=/usr/local/deno
RUN curl -fsSL https://deno.land/install.sh \
    | DENO_INSTALL=${DENO_INSTALL} sh \
    && PATH="/app/.venv/bin:${PATH}" camoufox fetch

# Wheels often include caches, test suites, development headers and
# native debug symbols. None are needed by the running application.
RUN find /app/.venv -type d -name __pycache__ \
        -prune -exec rm -rf '{}' + \
    && find /app/.venv -type f \( -name '*.pyc' -o -name '*.pyo' \) \
        -delete \
    && rm -rf \
        /app/.venv/lib/python3.14/site-packages/numpy/*/tests \
        /app/.venv/lib/python3.14/site-packages/numpy/tests \
        /app/.venv/lib/python3.14/site-packages/pandas/tests \
        /app/.venv/lib/python3.14/site-packages/pyarrow/include \
        /app/.venv/lib/python3.14/site-packages/pyarrow/src \
        /app/.venv/lib/python3.14/site-packages/pyarrow/tests \
    && find /app/.venv -type f \
        \( -perm /111 -o -name '*.so' -o -name '*.so.*' \) \
        -exec sh -c 'for file do \
            if readelf -h "$file" >/dev/null 2>&1; then \
                strip --strip-unneeded "$file"; \
            fi; \
        done' sh '{}' +

# ── Runtime stage ────────────────────────────────────────────────
FROM python:3.14-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        libasound2t64 \
        libatk-bridge2.0-0t64 \
        libatk1.0-0t64 \
        libcairo2 \
        libcups2t64 \
        libdbus-1-3 \
        libdbus-glib-1-2 \
        libfontconfig1 \
        libfreetype6 \
        libglib2.0-0t64 \
        libdrm2 \
        libgbm1 \
        libgtk-3-0t64 \
        libnss3 \
        libpango-1.0-0 \
        libpangocairo-1.0-0 \
        libx11-xcb1 \
        libxcb-shm0 \
        libxcomposite1 \
        libxdamage1 \
        libxext6 \
        libxfixes3 \
        libxrandr2 \
        libxrender1 \
        libxss1 \
        libxt6t64 \
    && rm -rf /var/lib/apt/lists/*

# Deno is required by the video scraper for PO token handling.
ENV DENO_INSTALL=/usr/local/deno
COPY --from=builder /usr/local/deno /usr/local/deno
ENV PATH="${DENO_INSTALL}/bin:${PATH}"
ENV DENO_PATH="${DENO_INSTALL}/bin/deno"

WORKDIR /app

# Copy the venv from builder
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /root/.cache/camoufox /root/.cache/camoufox
ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONPATH=/app \
    PYTHONDONTWRITEBYTECODE=1

# Copy project source
COPY scrape_exchange/ scrape_exchange/
COPY tools/ tools/

# Create default directories
RUN mkdir -p /data/videos /data/channels \
    /data/tiktok/creators /data/tiktok/videos \
    /data/tiktok/session-state \
    /var/log/scrape/scraper \
    /var/tmp/yt_dlp_cache

# Sensible defaults for container environment
ENV YOUTUBE_VIDEO_DATA_DIR=/data/videos \
    YOUTUBE_CHANNEL_DATA_DIR=/data/channels \
    TIKTOK_CREATOR_DATA_DIR=/data/tiktok/creators \
    TIKTOK_VIDEO_DATA_DIR=/data/tiktok/videos \
    TIKTOK_SESSION_STATE_DIR=/data/tiktok/session-state \
    YOUTUBE_CHANNEL_LIST=/data/channels.lst \
    YOUTUBE_CHANNEL_MAP_FILE=/data/channel_map.csv \
    RSS_QUEUE_FILE=/data/rss-queue.json \
    NO_FEEDS_FILE=/data/rss-no-feeds.txt \
    YTDLP_CACHE_DIR=/var/tmp/yt_dlp_cache \
    LOG_FORMAT=json \
    PO_TOKEN_URL=http://localhost:4416

# Ensure scraper output remains readable by host-side users even when
# the container runtime supplies a restrictive default umask.
ENTRYPOINT ["sh", "-c", "umask 0022; exec python \"$@\"", "--"]
