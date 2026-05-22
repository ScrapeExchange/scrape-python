# Architecture

This document describes the end-to-end architecture of `scrape-python`, with emphasis on data flow, resiliency boundaries, idempotency, and rate-limit enforcement.

## 1) System goals

- Discover and track YouTube creators/channels and related entities.
- Scrape channel/video/post/playlist/course metadata from multiple input paths.
- Normalize and validate outgoing payloads against exchange schemas.
- Upload data to downstream exchange APIs safely at sustained throughput.
- Operate continuously with bounded retries and observability.

## 2) Main components

### Runtime orchestration
- **`tools/yt_channel_scrape.py`**, **`tools/yt_rss_scrape.py`**,
  **`tools/yt_video_scrape.py`**, **`tools/yt_channel_upload.py`**,
  and **`tools/yt_video_upload.py`**: operational entrypoints for
  the YouTube scrape and upload workers.
- **`scrape_exchange.scraper_supervisor`**: lifecycle management for workers and coordinated shutdown.
- **`scrape_exchange.scraper_runner`**: shared runtime scaffolding for
  worker tools: supervisor dispatch, logging, metrics, rate-limiter
  setup, Scrape Exchange client setup, signal handling, and graceful
  drain. Tool modules still own their worker loops and platform logic.

### Work coordination and claims
- **`scrape_exchange.channel_scrape_queue`**: Redis-backed channel
  scrape queue and workflow state under `youtube:channel:*`.
- **`scrape_exchange.creator_queue`**: RSS creator scheduling queue;
  the RSS path can use file or Redis-backed implementations.
- **`scrape_exchange.video_scrape_queue`**: Redis-backed video scrape
  queue and terminal state under `youtube:video:*`.
- **`scrape_exchange.redis_claim` / `scrape_exchange.content_claim`**: claim/lease semantics to reduce duplicate concurrent processing.

### YouTube extraction and normalization
- **`scrape_exchange.youtube.youtube_client`**: retrieval of YouTube resources.
- **`scrape_exchange.youtube.*` models**: canonical objects for channels, videos, posts, products, playlists, captions, chapters, tabs, thumbnails, etc.
- **`scrape_exchange.youtube.youtube_video_innertube`**: alternate extraction path for video details where applicable.

### Validation and upload
- **Scraped data directories**: handoff boundary between scrape tools
  and upload tools. Scrapers write compressed channel and video JSON
  files; uploaders read, validate, upload, and move accepted files
  into their `uploaded/` directories.
- **`scrape_exchange.schema_validator`**: schema checks for outbound
  entities before upload; uploaders validate disk-backed channel and
  video records, and the RSS path validates the lite channel records
  it writes for the channel uploader.
- **`scrape_exchange.bulk_upload`** and **`scrape_exchange.exchange_client`**: batch transport and exchange API interactions.

### Throughput control and networking
- **`scrape_exchange.rate_limiter`**, **`scrape_exchange.scrape_exchange_rate_limiter`**, and YouTube-specific limiter modules: bounded request pacing across services.
- **`scrape_exchange.proxy_loader` / `scrape_exchange.proxy_phase_metrics`**: proxy pool loading and per-phase proxy observability.
- **`scrape_exchange.http_timeouts`** and pooled client helpers: network timeout and connection reuse policy.

### Observability
- **`scrape_exchange.logging`**: shared logging setup and conventions.
- **`scrape_exchange.metrics_server`** and **`scrape_exchange.scraper_metrics`**: Prometheus-oriented metric export.
- Dashboard/alert artifacts in `files/` and guidance in `OBSERVABILITY.md`.

## 3) End-to-end data flow

The YouTube worker fleet has separate scrape and upload stages. Redis
coordinates current scrape work; files on disk are the handoff from a
scraper to its uploader.

### Channel path

1. **Seed or discovery input**
   - Operator and importer tools enqueue channel IDs or handles on the
     Redis channel scrape queue.
2. **Queue scheduling + identity resolution**
   - `yt_channel_scrape.py` resolves handles where needed and advances
     channel workflow state in `youtube:channel:*`.
3. **Channel scrape**
   - The scraper gathers channel metadata and related channel content
     such as video IDs, posts, playlists, products, and tab data.
4. **Disk handoff**
   - The scraper writes compressed channel JSON files into
     `YOUTUBE_CHANNEL_DATA_DIR`.
5. **Validation + upload**
   - `yt_channel_upload.py` reads channel files, validates outbound
     records against the channel schema, and uploads them through the
     exchange client/bulk-upload path.
6. **Uploaded-file disposition + metrics**
   - Successfully uploaded files move under `uploaded/`; workers emit
     queue, scrape, upload, failure, and latency metrics along the way.

### RSS/video path

1. **Known-channel scheduling**
   - `yt_rss_scrape.py` polls creators from the RSS creator queue.
2. **RSS fetch + discovery**
   - RSS identifies newly published video IDs and emits lite
     channel-stat files for the channel uploader.
3. **Video queueing**
   - Discovered video IDs are enqueued on the Redis video scrape queue.
4. **Video scrape**
   - `yt_video_scrape.py` consumes queued video IDs and scrapes video
     metadata through the configured video detail path.
5. **Disk handoff**
   - The video scraper writes per-video JSON files into
     `YOUTUBE_VIDEO_DATA_DIR`.
6. **Validation + upload**
   - `yt_video_upload.py` reads video files, validates outbound video
     records, and uploads them through the exchange client/bulk-upload
     path.
7. **Uploaded-file disposition + metrics**
   - Successfully uploaded video files move under `uploaded/`; Redis
     queue state and Prometheus metrics preserve operational progress.

## 4) Ingestion paths and authoritative fields

Multiple acquisition paths can represent overlapping entities:

- **RSS path**: fast detection of newly published content with partial metadata.
- **Primary API/client path**: richer canonical metadata for stable fields.
- **Innertube/video fallback path**: supplementary detail recovery when primary path is incomplete.

Authority guidance for overlapping fields:

1. Stable entity identity fields (channel/video IDs): **primary API/client**.
2. Fresh publication detection timestamps: **RSS ingestion event time + source publish fields**.
3. Detailed video metadata (formats, chapters/captions when available): **video detail path**.

When a path merges overlapping records, preserve source markers and
last-updated timestamps so conflicts remain auditable.

## 5) Idempotency boundaries

Idempotency is enforced in layers:

- **Work-level idempotency**: Redis queue state plus claim/lease keys
  reduce parallel duplicate processing for the same content scope.
- **Payload-level idempotency**: deterministic identity fields (`channel_id`, `video_id`, etc.) allow downstream upsert behavior.
- **Batch-level idempotency**: bulk upload retries should be safe for already-accepted records.
- **Tooling idempotency**: maintenance scripts should prefer merge/upsert and explicit dedup steps.

Operational guidance:
- Keep lease TTLs greater than normal scrape latency p95.
- Use explicit dedup keys in queue feed generation.
- Record source + ingestion time for replay/debug of eventual consistency issues.

## 6) Retry and failure boundaries

### Expected transient failures
- upstream throttling / 429s
- proxy saturation or bad exits
- intermittent timeout/connection errors
- temporary exchange API failures

### Boundary strategy
- **Within scrape phase**: bounded per-request retries with backoff and rate-limiter awareness.
- **Within upload phase**: bounded retriable class handling, preserving non-retriable validation failures.
- **Across worker loop**: failed units are released, requeued, or
  moved to queue terminal state based on failure classification.
- **Across process restarts**: durable claims/queue state (where configured) avoids total progress loss.

### Failure classes
- **Retriable**: network timeout, transient HTTP 5xx/429, proxy transport failure.
- **Conditionally retriable**: source parse anomalies if alternate path exists.
- **Non-retriable**: schema contract violation for malformed payload; permanently missing identifiers.

## 7) Rate-limit enforcement map

Rate limits are applied at multiple layers:

- **Global scraper pacing**: generic limiter(s) cap overall outbound pressure.
- **YouTube-specific pacing**: service-specific limiter(s) control API/detail request cadence.
- **Exchange upload pacing**: dedicated limiter(s) protect downstream ingest APIs.
- **Cross-process coordination**: redis/file backends (as configured) coordinate limits between workers.

Practical policy:
- Treat 429 responses as a signal to reduce effective request concurrency.
- Use separate limiter buckets per upstream system (YouTube vs exchange).
- Export limiter wait/error metrics for tuning.

## 8) Deployment and operations notes

- Containerized execution is supported (`Dockerfile`, `docker-compose.yml`).
- CLI tools under `tools/` support backfills, reconciliations, imports/exports, and cleanup tasks.
- Observability assets in `files/` include Grafana dashboard JSON and Prometheus alert examples.

## 9) Architectural guardrails for future changes

1. Keep scrape acquisition separate from normalization and upload transport.
2. Preserve deterministic identity keys in all model transformations.
3. Any new ingestion path must declare field authority + merge precedence.
4. Add metrics before increasing concurrency.
5. For new scripts, reuse shared bootstrap patterns (settings/logging/client init) to reduce drift.

## 10) Related docs

- `README.md`
- `docs/SCHEMA.md`
- `OBSERVABILITY.md`
- `docs/adr/0001-channel-scrape-queue-on-redis.md`
