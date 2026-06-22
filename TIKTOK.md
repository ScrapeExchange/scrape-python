# TikTok scraping strategy

This document describes how the TikTok tools cooperate to collect
creator and video metadata while staying inside TikTok's per-IP soft
limits. It covers the strategy choices that shape runtime behaviour:
what runs in parallel, what runs sequentially, when each tool backs
off, and which state lives where.

Operational tuning knobs (env vars, defaults) are in `.env-example`;
this document is the "why".

## 1. The pipeline

```
tt_discover_search.py ──► discovered usernames JSONL
                                  │
                                  ▼
                         Redis creator queue
                                  │
                                  ▼
                       tt_creator_scrape.py ──► TIKTOK_CREATOR_DATA_DIR
                                  │                       │
                                  ▼                       ▼
                         Redis identity maps      tt_creator_upload.py
                                  │
                                  ▼
                         Redis video queue
                                  │
                                  ▼
                       tt_video_scrape.py ──► TIKTOK_VIDEO_DATA_DIR
                                                          │
                                                          ▼
                                                 tt_video_upload.py
```

The creator and video scrapers are decoupled by Redis. The creator
scraper reads due creators, writes creator records to disk, refreshes
TikTok identity maps, and enqueues discovered video URLs. The video
scraper reads those video URLs at its own pace and writes one record
per item. Backpressure surfaces as Redis queue growth rather than as
a synchronous stall.

There is no production file-based fallback for the main TikTok
creator or video loops. `tt_creator_scrape.py` and
`tt_video_scrape.py` require `REDIS_DSN` plus their respective data
directories before they start.

## 2. Session pool and rate limiter

TikTok scraping uses `TikTokSessionPool`, backed by Camoufox and
TikTokApi. Each ready proxy gets one browser/session, its own
persisted Chromium profile, and an `ms_token` stored under
`TIKTOK_SESSION_STATE_DIR`. Scrapers run API calls through the page's
main world so requests carry the current session state.

Session startup and token refresh are rate-limited separately from
creator and video API calls. `TikTokRateLimiter` has per-proxy
buckets for:

- `CREATOR_API` for `User.info`, `User.videos`, reposts, liked
  videos, and playlist reads in the creator scraper.
- `VIDEO_API` for `Video.info` in the video scraper.
- `BOOTSTRAP` for session creation and `ms_token` refresh.
- `API` as a legacy shared TikTokApi bucket for callers that have
  not selected a scraper-specific type.

With Redis configured, bucket state is shared across every process
and host that uses the same Redis. The Redis key prefix is `tiktok`,
so TikTok buckets are disjoint from YouTube buckets even when both
stacks share the same rate-limiter backend. If Redis is not set, the
base limiter can use a shared-file backend or an in-process backend,
but multi-host coordination requires Redis.

The session pool also runs a refresh loop. A token is refreshed once
its age exceeds `TIKTOK_MS_TOKEN_REFRESH_FRACTION` of
`TIKTOK_MS_TOKEN_TTL`, checked every
`TIKTOK_MS_TOKEN_REFRESH_INTERVAL` seconds. Proxies that fail
bootstrap are excluded from the ready set; a worker only starts if at
least one proxy bootstraps.

## 3. Creator scraper (`tt_creator_scrape.py`)

### What it does

Consumes a Redis-backed tiered creator queue. For each due creator,
it calls `User.info`, records the public profile fields, collects
compact references for visible videos, reposts, liked videos, and
playlists, writes `tiktok-creator-<username>.json.br`, and releases
the creator back into the right follower-count tier.

The creator scraper also maintains TikTok identity maps:

- `tiktok:creator_map` stores `user_id -> username`.
- `tiktok:handle_map` stores `username -> user_id`.
- `tiktok:name_map` stores `nickname -> sec_uid`.

These maps mirror the YouTube identity-map pattern, but TikTok
identity is handle-centric. A username can rename over time, while
`user_id` and `sec_uid` are the stronger API identities learned from
successful profile scrapes.

### Tiered polling cadence

Creators are bucketed by follower count via
`TIKTOK_CREATOR_PRIORITY_QUEUES`, a comma-separated
`interval_hours:min_followers` list. Higher-follower creators are
checked more often; the final tier must end in `:0` so every creator
has a home. After each successful scrape, the creator is re-tiered
using the freshly observed follower count.

One maintenance loop recovers stale claims and publishes queue-size
metrics. A worker crash therefore leaves a temporary claim, not a
permanently lost creator.

### One async worker per active proxy

Each worker process bootstraps one session per selected proxy. The
creator scraper then starts one asyncio worker per active ready proxy.
Each async worker claims one creator at a time, scrapes it through
that proxy's session, and sleeps until the next due creator when the
queue is empty.

`TIKTOK_CREATOR_CONCURRENCY=0` means "use the proxy count." When
`TIKTOK_CREATOR_NUM_PROCESSES > 1`, the first invocation becomes a
supervisor. It chooses the effective fleet-wide concurrency, splits
that budget across child processes, and gives each child a disjoint
proxy slice with its own browsers and metrics port.

### Creator-to-video handoff

The creator scraper enqueues full TikTok video URLs, not bare video
IDs. It uses every unique URL discovered from the creator's public
videos, reposts, and liked posts. This is the TikTok video queue
contract: video workers expect entries like
`https://www.tiktok.com/@<username>/video/<id>`.

Keeping full URLs in the queue preserves the author handle needed by
TikTokApi's `api.video(url=...)` entrypoint and avoids guessing when
only an item ID is available.

### Short URLs in the creator queue

The creator queue may contain `vm.tiktok.com` or `vt.tiktok.com`
short URLs. A creator worker resolves those through the same proxy
gate before scraping. Resolved short URLs graduate into the creator
queue as handles, transient resolver failures are rescheduled, and
unusable destinations are discarded.

Graduated handles start near the next-to-lowest priority tier and are
re-tiered after their first successful profile scrape.

### Failure handling

Creator failures are classified by `classify_tiktok_error()`:

- `unavailable` creators are removed from the queue.
- `transient`, `auth`, and `rate_limit` failures are released with a
  retry floor and jitter.
- Other failures are released at the normal tier interval.

Rate-limit and bot-detection failures also feed a per-proxy circuit.
After `TIKTOK_CREATOR_BOT_FAILURE_THRESHOLD` consecutive
bot-detection outcomes on one proxy, the session is quarantined, the
worker sleeps with exponential cooldown, and the session is rebuilt.
If rebuild fails, that proxy worker retires.

## 4. Video scraper (`tt_video_scrape.py`)

### What it does

Consumes full video URLs from the platform-scoped Redis video queue,
calls `Video.info`, maps the item payload to `TikTokVideo`, and writes
`tiktok-video-<video_id>.json.br` to `TIKTOK_VIDEO_DATA_DIR`.

The video model keeps public web payload data that can be collected
without login or the official TikTok API: author fields, stats,
formats, thumbnails, captions, sounds, hashtags, mentions, photo-post
images, availability, and the raw item payload.

### Queue-driven loop

One producer pops `TIKTOK_VIDEO_QUEUE_BATCH` entries from Redis and
feeds an in-process asyncio queue. Consumer tasks read from that
queue and scrape through their assigned proxy session. When Redis is
empty, the producer sleeps for
`TIKTOK_VIDEO_QUEUE_IDLE_POLL_SECONDS` before polling again.

The scraper rejects malformed queue entries. A TikTok video queue
member must be a full `www.tiktok.com/@.../video/...` URL.

### Transient retries

Transient video failures (`transient`, `rate_limit`, `auth`) are
retried up to `TIKTOK_VIDEO_TRANSIENT_MAX_ATTEMPTS`, with
`TIKTOK_VIDEO_TRANSIENT_BACKOFF_SECONDS` between attempts. Exhausted
transients are marked `FAILED`.

`unavailable` videos are marked `UNAVAILABLE` and are not retried.
Other non-transient failures are marked `FAILED` immediately.

### Parallelism model

`TIKTOK_VIDEO_CONCURRENCY=0` means "use the proxy count." The worker
caps active sessions at the number of ready proxies, so raising
concurrency above the ready proxy count does not create extra
throughput. When `TIKTOK_VIDEO_NUM_PROCESSES > 1`, the supervisor
splits the effective concurrency and proxy pool across child
processes, just like the creator scraper.

Unlike the YouTube yt-dlp path, TikTok video scraping is async I/O
through TikTokApi and Camoufox. The main ceilings are ready proxy
count, TikTok API latency, session health, and the Redis-backed
per-proxy rate-limiter buckets.

## 5. Discovery (`tt_discover_search.py`)

`tt_discover_search.py` is a browser-based discovery helper. It opens
TikTok explore/category pages and optional random TikTok search result
pages, scrolls them, extracts usernames from profile links, visible
text, and page data, and appends JSONL rows to
`TIKTOK_DISCOVER_OUTPUT_FILE`.

Discovery is intentionally separate from the scraper queue contract.
Its output is an audit-friendly list of discovered usernames; an
operator or import step can decide which handles to enqueue. It uses
a browser-navigation rate limiter with its own conservative browse
bucket, because discovery traffic has a different shape from
TikTokApi profile and video calls.

### Command path

Browser discovery uses Camoufox, not Playwright's browser installer.
Fetch the Camoufox browser assets before running the tool:

```bash
uv sync --frozen
uv run camoufox fetch
uv run tools/tt_discover_search.py \
  --proxy-files ~/proxies/proxy-seller.proxies.lst,~/proxies/vpn.proxies.lst \
  --search-term-count 10
```

Use `--max-categories`, `--search-term-count`, `--max-scrolls`, and
`--output-file` for bounded smoke tests before a full discovery run.

## 6. Uploaders

`tt_creator_upload.py` and `tt_video_upload.py` drain the creator and
video data directories, validate each Brotli JSON record against the
TikTok schema, bulk upload to scrape.exchange, and move successful
files to `uploaded/`.

Uploaders share `tools/_tt_upload_common.py`. They require API
credentials and the relevant data directory, but they deliberately
clear the proxy catalog at startup. Upload-only containers therefore
do not fail just because scraper proxy files are absent.

Creator uploads strip the embedded `videos` list before upload. The
creator record still captures public profile metadata, while videos
are uploaded as first-class `video` records by the video uploader.

Corrupt creator files can be rescheduled into the creator queue when
Redis is configured. Corrupt video files are not automatically
requeued, because the filename only preserves the video ID while the
video queue contract requires the original full URL.

## 7. Output files and schemas

All TikTok scrapers write through `AssetFileManagement`, which owns
the on-disk file lifecycle for scraping tools. Current file prefixes
are:

- `tiktok-creator-<username>.json.br` for creator records.
- `tiktok-video-<video_id>.json.br` for video records.

Schemas live under `tests/collateral/`:

- `drand-tiktok-creator-schema.json`
- `drand-tiktok-video-schema.json`
- `drand-tiktok-hashtag-schema.json`

The upload tools default to schema owner `drand` and version `0.0.1`,
overridable with `TIKTOK_SCHEMA_OWNER` and `TIKTOK_SCHEMA_VERSION`.

## 8. Cross-cutting observability

TikTok scrapers and uploaders use the shared platform/entity metrics
labels:

- `platform="tiktok"`
- `scraper="tiktok_creator"` or `scraper="tiktok_video"`
- `entity="creator"` or `entity="video"`

Important signals include:

- `scrapes_completed_total` and `scrape_failures_total`
- `scrape_duration_seconds`
- `scrape_queue_size`
- `scrape_queue_enqueue_total`
- `scrape_retry_total`
- `tiktok_api_call_total` and `tiktok_api_call_duration_seconds`
- `tiktok_session_pool_size`
- `tiktok_session_acquire_wait_seconds`
- `tiktok_ms_token_refresh_total`
- `rate_limit_*` metrics with `platform="tiktok"`

For the creator scraper, queue growth with rising `rate_limit` or
bot-detection failures points at TikTok pressure or unhealthy
sessions. Queue growth with low API calls usually means workers are
not reaching the session pool or no proxies bootstrapped.

For the video scraper, queue growth with normal API latency usually
means more ready proxy sessions or child processes can help. Queue
growth with high API latency or rising transient failures means
scaling up may only increase pressure.

The Grafana dashboard JSON for the TikTok stack is in
`files/grafana-scraper.json`.

## 9. Operational cautions

Do not collapse TikTok creator and video queue parsing into generic
queue CLI logic. Entity-specific normalization belongs in the TikTok
creator or video queue adapter, because creator handles and video URLs
have different contracts.

Keep scraper and uploader mounts aligned. Uploaders only see files in
their configured data directories, so a scraper writing to one path
and an uploader watching another will look like an empty pipeline.

Use explicit `LOG_FILE` or scraper-specific log-file variables for
host-visible logs. Container stdout is useful during development, but
production log collection expects stable files.

When repairing `.invalid` TikTok video files, stop the video scraper
and video uploader first so concurrent writers do not recreate
invalid files while the repair is running. Validate the whole target
directory after the repair completes.
