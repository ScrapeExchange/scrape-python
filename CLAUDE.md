read settings, connectionstrings from LOCAL.md if the file exists.
remember conversations about this code base and repo between sessions
use the skills of the superpowers plugin at all times
use uv for python package management
use unittest for testing instead of pytest
use pydantic-settings for environment variables and command line arguments
use f-strings instead of strings with '%s %d'
This is a public repository, so do not include any sensitive information in the code or in the commit messages. If you need to include sensitive information, please use environment variables or a secure vault. The only hostnames that should be mentioned in the code is localhost, scrape.exchange, appserver.scrape.exchange, www.scrape.exchange, files.scrape.exchange and api.scrape.exchange. IP addresses should not be mentioned in the code
Do not make any changes until you have 95% confidence that the change is correct. Ask me follow-up questions until you reach that confidence level.
Code layout: All unit tests under tests/unit, all integration tests under tests/integration.
After writing database scripts (MongoDB, Postgres), always test with a dry-run or LIMIT/findOne query first to verify field names and null handling before running full operations.

The scrape.exchange API server is highly available and performant. The network path between the host running the scrapers and the API server is high capacity, has no packet loss and is highly available
Management of files produced by the scraping tools is done by the AssetFileManagement class in server_exchange/file_management.py

## YouTube scraping tools and models
YouTube scraping tools are tools/yt_{video,rss,channel}.py for scraping videos, RSS feeds, and channels.
YouTube rate limiter is scrape_exchange/youtube/youtube_rate_limiter.py YouTubeRateLimiter, which derives from scrape_exchange/RateLimiter(), and its cookie jar is scrape_exchange/youtube/youtube_cookiejar.py. The rate limiter is responsible for enforcing YouTube's rate limits and managing cookie files for authentication, while the cookie jar handles the creation and renewal of these cookie files.
The YouTube video model is scrape_exchange/youtube/youtube_video.py, which defines the structure of a YouTube video object and includes methods for scraping video details and saving them to files.
The YouTube channel model is scrape_exchange/youtube/youtube_channel.py, which defines the structure of a YouTube channel object and includes methods for scraping channel details and saving them to files.
Additional data modelling for YouTube videos and channels are in scrape_exchange/youtube/youtube_{caption,course,external_link,format,playlist,post,product,thumbnail,videochapter}.py, which includes data classes for representing video and channel metadata.
The ChannelSettings, RssSettings,VideoSettings classes in the scraping tools are derived from scrape_exchange/youtube/settings.py:YouTubeScraperSettings(ScraperSettings), which derives from scrape_exchange/settings.py:ScraperSettings(pydantic.BaseSettings)

## TikTok scraping tools and models
TikTok scraping tools are tools/tt_{creator,video}.py for scraping videos, creators.
TikTok rate limiter is scrape_exchange/tiktok/tiktok_rate_limiter.py TikTokRateLimiter, which derives from scrape_exchange/rate_limiter:RateLimiter() and its session jar is scrape_exchange/tiktok/tiktok_sessionjar.py. The rate limiter is responsible for enforcing TikTok's rate limits and managing session files for authentication, while the session jar handles the creation and renewal of these session files.
The TikTok video model is scrape_exchange/tiktok/tiktok_video.py, which defines the structure of a TikTok video object and includes methods for scraping video details and saving them to files.
The TikTok creator model is scrape_exchange/tiktok/tiktok_creator.py, which defines the structure of a TikTok creator object and includes methods for scraping creator details and saving them to files.
Additional data modelling for TikTok creators and videos are in scrape_exchange/tiktok/tiktok_{caption,course,external_link,format,playlist,post,product,thumbnail,videochapter}.py, which includes data classes for representing video and creator metadata.
The CreatorSettings, VideoSettings classes in the scraping tools are derived from scrape_exchange/tiktok/settings.py:TikTokScraperSettings(ScraperSettings), which derives from scrape_exchange/settings.py:ScraperSettings(pydantic.BaseSettings)

for linting, don't generate lines longer than 80 characters
always specify typing when using variables
assume that the scraping tools will be used by others with no knowledge of the code base, and keep the tools easy to use for them
The number of proxies available in production is 18
terminology: each supervisor with num_processes>1 launches a number of worker processes (aka workers), each worker process launching concurrency amount of asyncio tasks (aka tasks)
The YouTube video model is scrape_exchange/youtube/youtube_video.py, which defines the structure of a YouTube video object and includes methods for scraping video details and saving them to files.
The YouTube channel model is scrape_exchange/youtube/youtube_channel.py, which defines the structure of a YouTube channel object and includes methods for scraping channel details and saving them to files.



## Performance & scaling notes

### Video scraper parallelism model

The video scraper's parallelism is bounded by three independent
ceilings, in this order:

1. **`CONCURRENCY` (per process)** — number of async workers spawned
   inside one Python process. Workers are assigned proxies
   round-robin: `concurrency < len(proxies)` leaves trailing proxies
   idle, `concurrency > len(proxies)` gives some proxies multiple
   workers.
2. **Default `ThreadPoolExecutor` (per process)** — yt-dlp's
   `extract_info` runs via `loop.run_in_executor(None, ...)` in
   `scrape_exchange/youtube/youtube_video.py:_scrape_video`. The
   default executor size is `min(32, os.cpu_count() + 4)`, typically
   8–16 threads. Async workers in excess of that size queue on the
   executor and contribute zero throughput until a slot frees up.
3. **GIL** — `extract_info` is CPU-bound Python (parsing, signature
   decryption). Even when threads do run, the GIL serializes them, so
   per-process parallelism caps well below the executor size.

The `extract_info_active` gauge (labelled by proxy) shows
actual thread-pool occupancy. If it sits pinned at the executor size
while `scrapes_completed_total{scraper="video_scraper"}` is low,
the bottleneck is the executor / GIL — not the rate limiter or
YouTube.

To scale beyond one process's effective parallelism, set
`NUM_PROCESSES > 1` on the video scraper. The first invocation
becomes a supervisor that splits `PROXIES` into N disjoint chunks and
spawns N children, each with `NUM_PROCESSES=1`, its own `PROXIES`
slice, and `METRICS_PORT=base+index`. Disjoint chunks are a
load-balancing choice, not a correctness requirement: with the Redis
rate-limiter backend (see below) overlapping proxies are still held
to the configured per-proxy ceilings fleet-wide. Overlap just means
multiple workers contend for the same token bucket.

### Rate limiter: cross-host coordination via Redis

`YouTubeRateLimiter` selects a backend at construction time based on
the settings it's handed (see `RateLimiter.__init__`):

1. ``redis_dsn`` (or env ``REDIS_DSN``) → `_RedisBackend`. Per-proxy
   bucket state lives in Redis and is shared by every process on
   every host that points at the same Redis. **This is production.**
2. ``rate_limiter_state_dir`` → `_SharedFileBackend`. Single-host
   cross-process coordination via files.
3. Neither set → `_InProcessBackend`. Per-process only; every process
   drives its own private buckets.

In production both hosts (`homeserver`, `homedata`) set
`REDIS_DSN=redis://mongo.scrape.exchange:6379/0`, so all scraper
types — video, RSS, channel, discover — across both hosts share one
set of per-proxy buckets. YouTube sees aggregate traffic per IP and
so does the limiter: running multiple scraper types against the same
proxy pool is safe.

If Redis is unreachable, Redis-backend operations raise; they do not
silently fall back to in-process. Watch
`rate_limit_redis_ops_total` and its failure-label counterpart to
confirm the backend is live.

### Diagnosing low scrape rate from the Grafana dashboard

When throughput is unexpectedly low, walk these panels in order:

1. **Queue sizes** — flat queues with low throughput means workers
   aren't consuming.
2. **rate-limit acquired requests by api** — if low while
   `global bucket tokens (per proxy)` is near max (≈29/30), the rate
   limiter is *not* the bottleneck and workers aren't even reaching
   `acquire()`. Look at #3 next.
3. **yt-dlp extract_info active (thread-pool occupancy)** — if this
   pins at ~12 per process while `videos_scraped` rate is low, the
   default `ThreadPoolExecutor` is your cap. Raise `NUM_PROCESSES`
   (each new child gets its own executor).
4. **yt-dlp scrape duration p95** — if scrapes themselves are slow,
   `effective rate ≈ workers / p95`. Compare against expected.
5. **Worker sleep (seconds) before next work** — non-zero values mean
   workers are in failure-backoff, not actively scraping.
6. **Video scrape failures by reason** — if any reason is climbing,
   that's what's forcing the sleeps in #5.
7. **scrape.exchange request latency p95** — if API latency is
   spiking, the background upload queue may be backing up. Background
   uploads do not block workers, but a full queue means
   `enqueue_upload` is dropping silently.

### Setting expectations

For a single video scraper process, expected steady-state throughput
is roughly `min(CONCURRENCY, executor_size) / p50_scrape_seconds`,
further reduced by GIL contention. With a typical 8-CPU host and p50
≈ 3s, that's roughly **2–4 scrapes/s per process**. To get 20+
scrapes/s, run the supervisor with 6+ child processes. Proxy slices
can overlap without over-driving YouTube thanks to the Redis-backed
limiter, but disjoint slices spread load more evenly.


## Scraper fleet KPI review (Prometheus)

Walk these signals in order when asked to review live scraper health.
All queries go through `prom_query.py query '<promql>'`.

### 1. Fleet layout (sanity check)

```
sum by (scraper, role) (scraper_num_processes)
```

Supervisor values should match the configured `{video,rss,channel}_num_processes`.
**Gotcha:** the per-scraper Prometheus scrape config strips the port
from `instance`, so every worker collapses into a single series per
`{job, scraper, role}` combo. That means
`count(scraper_num_processes{role="worker"})` and
`sum(scraper_concurrency{role="worker"})` **always read 1 / single-worker
value** regardless of how many workers are actually running. Trust the
`role="supervisor"` gauges instead — the supervisor publishes the
authoritative child count and per-child concurrency. Use
`scraper_num_processes{role="supervisor"} * scraper_concurrency{role="supervisor"}`
for fleet-total async task count.

For actual port-level liveness use `targets` and inspect the
`scrapeUrl` + `health` columns on `youtube_{video,rss,channel}` jobs.

### 2. Throughput (per scraper)

| Scraper | Success counter | Failure counter |
|---|---|---|
| video | `scrapes_completed_total{scraper="video_scraper"}` | `scrape_failures_total{scraper="video_scraper"}` (label: `reason`) |
| rss | `scrapes_completed_total{scraper="rss_scraper"}` | `scrape_failures_total{scraper="rss_scraper"}` |
| channel | `scrapes_completed_total{scraper="channel_scraper"}` | `scrape_failures_total{scraper="channel_scraper"}` |

Compare 5m vs 30m vs 1h rates to detect ramp-up / stall. Compute
success fraction as
`rate(success[15m]) / (rate(success[15m]) + rate(failures[15m]))`.
Drop below ~0.7 usually means YouTube pressure, not local issues.

### 3. Failure breakdown (video)

```
sum by (reason) (increase(scrape_failures_total{scraper="video_scraper"}[1h]))
```

Recognised `reason` values include `rate_limit`, `transient`,
`unavailable`, `premiere`, `other`. `rate_limit` climbing while
throughput is flat = per-IP ceiling hit; scaling `video_num_processes`
**down** (not up) is usually the right move. See
`_classify_yt_dlp_error` in `tools/yt_video_scrape.py` for the
substring → reason table.

### 4. Bottleneck isolation (video)

- `sum(extract_info_active)` — thread-pool occupancy. Pinned
  near `num_processes * 12` = executor cap; near zero while throughput
  is low = workers blocked *before* `extract_info` (rate limiter,
  network, backoff).
- `histogram_quantile(0.50, sum by (le)(rate(scrape_duration_seconds_bucket{scraper="video_scraper"}[10m])))`
  — p50 scrape latency. Baseline ≈3s; 15+ s indicates slow proxies or
  heavy retries inside yt-dlp.
- `sum(scrape_queue_size{scraper="video_scraper"})` — unscraped backlog.
- `max(worker_sleep_seconds{scraper="video_scraper"})` — non-zero means
  at least one worker is in failure backoff.

### 5. Backlog & queues (rss / channel)

- `scrape_queue_size{scraper="rss_scraper"}`,
  `worker_sleep_seconds{scraper="rss_scraper"}`
- `pending_channel_id_resolutions`, `files_pending_upload`
- `channel_no_content_found_total` — most "failures" in the channel
  scraper are legitimate empty channels; subtract this before raising
  alarms about the failure rate.

### 6. Report format

Present one table per scraper with the KPIs from steps 2–5. Separate
symptoms (throughput, latency, queue) from causes (rate_limit failures,
sleep, extract_info_active). Call out the expected vs observed
direction for each signal, and finish with one explicit action
recommendation per scraper (scale up, scale down, wait, investigate).

