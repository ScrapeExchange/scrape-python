# Hardware sizing

This document gives practical sizing guidance for scrape-python
deployments, from a single-host test setup to a larger fleet where
supervisors launch multiple worker processes and each worker runs
multiple async scraping tasks.

The numbers below are planning estimates. Real memory use depends on
platform responses, Python allocator behaviour, browser versions,
proxy quality, logging volume, and whether optional enrichment paths
such as yt-dlp are enabled. Leave headroom and validate with
Prometheus, container memory metrics, and host `ps`/`top` during
ramp-up.

## 1. Capacity model

Each scraper has two independent knobs:

```
total_async_tasks = NUM_PROCESSES * CONCURRENCY
```

When `NUM_PROCESSES > 1`, the first process becomes a supervisor. The
supervisor does not scrape. It splits the proxy pool, starts child
worker processes, publishes aggregate metrics, and restarts workers
that crash or become unresponsive.

Each child worker owns:

- Its Python process and imports.
- Its async scraping tasks.
- Its share of the proxy pool.
- Its own logs and Prometheus multiprocess metric files.
- For YouTube yt-dlp mode, its own default thread executor.
- For TikTok, browser sessions for selected proxies.

For YouTube scrapers, `CONCURRENCY` is normally per child process. For
TikTok scrapers in this branch, `TIKTOK_CREATOR_CONCURRENCY` and
`TIKTOK_VIDEO_CONCURRENCY` are fleet-wide budgets that are split
across child processes.

## 2. What scales with what

RAM has four main drivers:

- **Processes:** every child has a Python interpreter, imported
  modules, metrics, queues, and HTTP clients.
- **Live tasks:** each async task holds request state, response data,
  parsed models, and retry state.
- **Anti-bot/session modules:** YouTube cookie/PO-token state is
  lightweight; TikTok Camoufox sessions are heavyweight.
- **Upload batches:** uploaders can temporarily hold prepared NDJSON
  batch bytes in memory while posting a bulk batch.

Queue size and historical item count mostly affect Redis, disk, and
startup scans. They should not grow scraper worker RSS linearly unless
a feature explicitly loads a full set into memory.

Disk grows with scraped items:

```
disk_needed ~= item_count * average_compressed_record_size * retention
```

Use the observed size of `.json.br` files in your data directories for
`average_compressed_record_size`. Video records with formats, captions,
raw TikTok items, or yt-dlp output are larger than lightweight RSS or
channel-stat records.

Redis grows with queued items, identity maps, retry metadata, uploaded
sets, rate-limit buckets, and TikTok session/rate state:

```
redis_memory ~= queued_items + known_items + identity_maps + metadata
```

For planning, treat Redis as persistent coordination state. Watch
`used_memory`, key count, and latency as the fleet grows.

## 3. Deployment tiers

### Tiny local test

Goal: validate credentials, compose wiring, schemas, and one or two
scrapes.

Suggested host:

- 2-4 CPU cores
- 8 GiB RAM
- 20-50 GiB free disk
- Local or remote Redis

Suggested settings:

- `*_NUM_PROCESSES=1`
- `*_CONCURRENCY=1`
- `VIDEO_USE_YT_DLP=false`
- TikTok concurrency 1, with 1-2 proxies
- Upload concurrency 1

Run one platform at a time if using TikTok. A single TikTok creator or
video scraper can launch a browser session per selected proxy, so a
small host can run out of memory quickly if many proxies are selected.

### Small single-host deployment

Goal: continuous scraping with modest queues and uploaders.

Suggested host:

- 4-8 CPU cores
- 16-32 GiB RAM
- SSD storage sized for retention
- Redis on the same host or a nearby host

Suggested settings:

- YouTube channel/RSS/video: 1 process, concurrency 2-4
- TikTok creator/video: 1 process, fleet concurrency 2-4
- Upload concurrency 2-3
- Keep `VIDEO_USE_YT_DLP=false` until baseline throughput is stable

This is the first tier where running both YouTube and TikTok together
is reasonable. Keep TikTok concurrency below available proxy count and
watch browser memory before raising it.

### Medium single-host deployment

Goal: higher throughput while keeping operational complexity modest.

Suggested host:

- 8-16 CPU cores
- 32-64 GiB RAM
- SSD or NVMe storage
- Redis with persistence enabled

Suggested settings:

- YouTube channel/RSS: 1-2 processes, concurrency 4-8 total
- YouTube video InnerTube-only: 1-3 processes, concurrency 4-12 total
- YouTube video with yt-dlp: 2-4 processes, lower per-process
  concurrency
- TikTok creator/video: fleet concurrency 4-10 total
- Uploaders: concurrency 3-6, with bounded bulk batch bytes

Use multiple video processes when yt-dlp is enabled. yt-dlp runs
`extract_info` in the default `ThreadPoolExecutor`, and CPU-bound
Python work is limited by executor slots and the GIL inside each
process. More child processes give the video scraper more independent
executors.

### Large single-host deployment

Goal: push one large machine while keeping Redis and logs healthy.

Suggested host:

- 16-32 CPU cores
- 96-256 GiB RAM
- NVMe storage for data and logs
- Redis on the same host only if it has reserved RAM and I/O

Suggested settings:

- YouTube video: 4-8 child processes when yt-dlp is enabled
- YouTube channel/RSS: 2-4 child processes if queues justify it
- TikTok creator/video: fleet concurrency 8-18, depending on RAM
- Uploaders: tune by API acceptance rate and batch memory

At this tier, host-level limits matter: file descriptors, ephemeral
ports, log volume, Prometheus scrape cardinality, Redis latency, and
disk write throughput. Add capacity gradually and keep at least
25-40% RAM free for browser spikes, Python fragmentation, and upload
batches.

### Multi-host fleet

Goal: scale beyond one host or isolate platforms.

Suggested layout:

- Shared Redis reachable from every scraper host
- One or more YouTube hosts
- One or more TikTok hosts
- Optional separate uploader host if bulk uploads compete for RAM
- Central log and Prometheus collection

Every host that points at the same `REDIS_DSN` shares the relevant
Redis-backed queues and rate-limit buckets. Supervisors still split
only the proxy pool visible to their own process. To avoid accidental
overlap, keep proxy files intentional and watch per-proxy metrics.

For TikTok, multi-host scaling is often RAM-bound before CPU-bound
because each selected proxy can mean a live browser session. For
YouTube yt-dlp video scraping, CPU and executor pressure are usually
the first ceilings.

## 4. Memory by scraper

These are approximate resident-memory planning ranges. Use the high
end when response payloads are large, logging is verbose, proxies are
unreliable, or the process has run long enough for Python allocator
fragmentation to accumulate.

- `yt_channel_scrape.py`
  - Drivers: Python process, channel tabs, InnerTube responses,
    cookie state.
  - Plan: 300-800 MiB per child plus 50-150 MiB per active channel.
- `yt_rss_scrape.py`
  - Drivers: Python process, RSS HTTP responses, optional per-video
    enrichment, circuit state.
  - Plan: 250-600 MiB per child plus 20-80 MiB per streamer.
- `yt_video_scrape.py`, InnerTube only
  - Drivers: Python process, InnerTube parser, HTTP clients, queue
    batch.
  - Plan: 300-800 MiB per child plus 40-120 MiB per active video.
- `yt_video_scrape.py`, with yt-dlp
  - Drivers: InnerTube cost plus yt-dlp, Deno/PO-token path, executor
    threads.
  - Plan: 600 MiB-2 GiB per child plus 150-500 MiB per active
    `extract_info`.
- `tt_creator_scrape.py`
  - Drivers: Python process, TikTokApi, Camoufox sessions, one active
    worker per selected proxy.
  - Plan: 300-700 MiB per child plus 400 MiB-1.5 GiB per ready proxy.
- `tt_video_scrape.py`
  - Drivers: Python process, TikTokApi, Camoufox sessions, video item
    payloads.
  - Plan: 300-700 MiB per child plus 400 MiB-1.5 GiB per ready proxy.
- Uploaders
  - Drivers: Python process, validators, prepared NDJSON batch,
    progress state.
  - Plan: 250-600 MiB base plus up to current bulk batch bytes.
- Discovery tools
  - Drivers: browser or HTTP clients, output buffers.
  - Plan: 500 MiB-2 GiB depending on browser count and page weight.

TikTok browser memory is the largest uncertainty. Size by selected
proxies, not by configured proxy files:

```
tiktok_ready_sessions <= min(tiktok_concurrency, proxy_count)
tiktok_ram ~= process_base + ready_sessions * browser_session_size
```

For YouTube workers, proxy count mostly affects connection pools,
cookie files, rate-limit buckets, and metrics labels. It does not
normally create one browser per proxy.

## 5. Item-count effects

Scraped item count affects RAM indirectly:

- More known creators/channels means more Redis identity and schedule
  state.
- More queued items means larger Redis sorted sets and metadata.
- More on-disk files means larger directory scans for uploaders.
- Larger records mean larger upload batches and more validation work.
- More failures mean more retry metadata and possibly more logs.

Uploader memory is the part most directly affected by item size. A
bulk uploader prepares NDJSON bytes for the current batch before
posting it. Keep `BULK_BATCH_SIZE` and `BULK_MAX_BATCH_BYTES` below
the amount of RAM you can afford to lose temporarily.

Rule of thumb:

```
uploader_ram ~= base + min(BULK_MAX_BATCH_BYTES, batch_payload_bytes)
               + validation_concurrency_overhead
```

If an uploader shares a host with TikTok scrapers, keep bulk batch
bytes conservative. Browser memory spikes and a large upload batch can
arrive at the same time.

## 6. Proxy-count effects

Proxy count increases safe platform throughput only when the platform
and limiter can use those proxies.

YouTube:

- More proxies provide more per-IP rate-limit buckets.
- Channel scraping benefits when large tab walks wait on rate limits.
- RSS benefits only if the proxies are healthy for RSS traffic.
- InnerTube video scraping benefits when API latency/rate limits are
  the ceiling.
- yt-dlp video scraping may remain CPU/executor-bound even with many
  proxies.

TikTok:

- More proxies only help if they bootstrap into ready sessions.
- Each selected ready proxy can cost hundreds of MiB or more.
- Concurrency above ready proxy count does not add throughput.
- Bot-detected proxies can be quarantined and rebuilt, causing memory
  and latency churn.

Do not set TikTok concurrency equal to a large proxy file on a small
host. Start low, verify ready session count and RSS, then increase.

## 7. Anti-bot module costs

YouTube anti-bot support is comparatively light:

- `YouTubeRateLimiter` holds per-proxy token buckets in Redis, files,
  or process memory.
- `YouTubeCookieJar` keeps per-proxy cookie files and small in-memory
  entries.
- The PO token provider is a separate service.
- yt-dlp can add substantial CPU and memory when enabled.

TikTok anti-bot support is heavy:

- `TikTokSessionPool` uses Camoufox browser sessions.
- Each selected proxy has profile/session state and an `ms_token`.
- Bootstrap and refresh loops keep browser/session state alive.
- TikTokApi calls run through those sessions.

That is why TikTok sizing should start from:

```
selected_proxies * browser_session_size
```

and YouTube sizing should start from:

```
processes * python_base + active_tasks * per_task_work
```

## 8. Sizing workflow

1. Pick the platform and scraper mix.
2. Choose initial `NUM_PROCESSES=1` and low concurrency.
3. Set upload batch byte limits to fit available RAM.
4. Start services and confirm queues drain without failures.
5. Watch CPU, RAM, Redis latency, scrape duration, and failure rates.
6. Increase concurrency until rate limits, CPU, RAM, or latency bite.
7. Add child processes when one process is CPU/executor-bound.
8. Add hosts when one host cannot carry the RAM, CPU, or disk load.

Scale down when failure rate or bot-detection rate rises faster than
successful scrape rate. More workers are only useful when the current
bottleneck is local capacity, not platform pressure.

## 9. Worked examples

### Small YouTube-only host

```
CHANNEL_NUM_PROCESSES=1
CHANNEL_CONCURRENCY=2
RSS_NUM_PROCESSES=1
RSS_CONCURRENCY=3
VIDEO_NUM_PROCESSES=1
VIDEO_CONCURRENCY=3
VIDEO_USE_YT_DLP=false
```

Plan for roughly 4-8 GiB RAM for scraper containers plus Redis, logs,
and OS cache. Increase video concurrency first if queue latency is high
and CPU/RAM remain low.

### YouTube video host with yt-dlp

```
VIDEO_NUM_PROCESSES=4
VIDEO_CONCURRENCY=4
VIDEO_USE_YT_DLP=true
```

This creates 4 child processes and up to 16 async video tasks. Each
child has its own executor, so the host needs enough CPU and RAM for
multiple active `extract_info` calls. Plan for tens of GiB rather than
single-digit GiB.

### Small TikTok host

```
TIKTOK_CREATOR_NUM_PROCESSES=1
TIKTOK_CREATOR_CONCURRENCY=2
TIKTOK_VIDEO_NUM_PROCESSES=1
TIKTOK_VIDEO_CONCURRENCY=2
```

Plan for 8-16 GiB RAM if both TikTok scrapers run together. The host
may have many proxies in `PROXY_FILES`, but only the selected ready
sessions should be active. Raise concurrency one step at a time.

### Larger TikTok host

```
TIKTOK_CREATOR_NUM_PROCESSES=2
TIKTOK_CREATOR_CONCURRENCY=8
TIKTOK_VIDEO_NUM_PROCESSES=2
TIKTOK_VIDEO_CONCURRENCY=8
```

This is a browser-heavy deployment. It can create up to 16 selected
TikTok browser sessions across both scrapers if all proxies bootstrap.
Plan for 64 GiB or more, then validate with real browser RSS before
adding more concurrency.

## 10. Signs that hardware is the bottleneck

Scale hardware or processes up when:

- CPU is saturated and success rate is healthy.
- yt-dlp `extract_info_active` is pinned near executor capacity.
- Redis latency is low but queues grow.
- TikTok ready sessions are healthy and RAM has headroom.
- Uploaders lag because batch prep, compression, or validation is CPU
  bound.

Scale down or investigate platform pressure when:

- Rate-limit, bot-detection, auth, or transient failures climb.
- TikTok session bootstrap succeeds rarely.
- RSS 404s rise across many known-good channels.
- Scrape duration spikes while CPU is not saturated.
- Upload batch memory competes with scraper browser memory.
