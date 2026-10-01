# YouTube scraping strategy

This document describes how the three YouTube scrapers
(`yt_channel_scrape.py`, `yt_rss_scrape.py`,
`yt_video_scrape.py`) cooperate to collect channel and
video metadata while staying inside YouTube's per-IP soft
limits. It covers the strategy choices that shape the
runtime behaviour — what runs in parallel, what runs
sequentially, when each tool stops or backs off, and which
state lives where.

Operational tuning knobs (env vars, defaults) are in
`.env-example`; this document is the "why".

## 1. The pipeline

```
yt_channel_queue.py ──► Redis channel scrape queue
                              │
                              ▼
                     yt_channel_scrape.py ──► YOUTUBE_CHANNEL_DATA_DIR
                              │                       │
                              ▼                       ▼
                     Redis identity maps     yt_channel_upload.py
                              │
                              ▼
RSS feeds ──► yt_rss_scrape.py ──► Redis video scrape queue
                  │                       │
                  ▼                       ▼
       channel-rss-*.json.br      yt_video_scrape.py
       in CHANNEL_DATA_DIR                │
                  │                       ▼
                  ▼               YOUTUBE_VIDEO_DATA_DIR
        yt_channel_upload.py              │
                                          ▼
                                  yt_video_upload.py
```

The three scrapers are decoupled by Redis: each writes
into a shared queue or identity map, and each reads from
the upstream queue at its own pace. Backpressure surfaces
as a growing queue rather than as a synchronous stall.

> **Legacy path (file-based, `REDIS_DSN` unset).** When
> `REDIS_DSN` is not configured, the channel scraper
> falls back to a deprecated file-based path that reads
> `YOUTUBE_CHANNEL_LIST` (typically `channels.lst`).
> This path is not used in production and the function
> that implements it (`scrape_channels`) is marked
> `# DEPRECATED: replaced by _queue_driven_loop` in
> `tools/yt_channel_scrape.py`. The rest of this
> document describes the Redis path only.

## 2. Channel scraper (`yt_channel_scrape.py`)

### What it does

Pops entries from the Redis channel scrape queue
(populated by `tools/yt_channel_queue.py`), resolves
each handle or channel-ID to a canonical
`(creator_id, handle)` pair, then scrapes every visible
tab on the channel: Videos, Shorts, Live, Podcasts,
Courses, Playlists, Posts, Store. Output is one
`channel-<handle>.json.br` per channel.

### Priority directory drain

In Redis mode, worker `WORKER_ID=1` periodically drains the channel
priority directory into the Redis channel scrape queue at priority.
Each pending filename may be a bare `UC...` channel ID, an
`@handle`, or a bare handle. Accepted entries are resolved, bound in
the channel identity maps, enqueued, and atomically renamed with a
`.processed` suffix. Resolution or identity conflicts are renamed to
`.failed`. Both suffixes are audit markers and are skipped by later
drain cycles.

### Forced re-scrape modes

Normal scheduled channel refreshes check scrape.exchange before
scraping. If the channel already exists there, the worker performs a
metadata-only refresh and skips `video_ids`; otherwise it runs the
full channel-content path.

Operators can override that decision with `yt_channel_queue.py
rescrape --mode full KEY` or `--mode metadata KEY`. The command stores
the requested mode on the Redis queue meta record and makes the channel
due immediately. `full` forces the channel-content path and writes
`video_ids`; `metadata` forces `with_video_ids=False`. The worker clears
the force fields only after a successful scrape, so transient retries
continue to honor the operator request.

### Channel-to-channel discovery

After every successful scrape, the featured channels found on the
channel home page (`channel_links`, parsed from the "Featured
Channels" shelf by `extract_linked_channels`) feed channel-to-channel
discovery. Each linked channel is resolved to its channel_id (bare
`UC…` ids and `channel/UC…` paths pass through; bare handles resolve
via the shared handle_map, falling back to an InnerTube call) and then:

1. skipped when it links to itself,
2. skipped when the creator_map already knows it (already scraped),
3. skipped when scrape.exchange already has a record for it
   (the same `/api/v1/data/content/youtube/channel/<id>` existence
   check the scrape decision uses) — also skipped when that check
   fails with a network error, so a flaky exchange never floods the
   queue; the link is simply rediscovered on the next full scrape.
   The check retries transient failures (network errors and
   502/503/504) once after a one-second backoff, absorbing the
   slow-connect errors seen on hosts with a lossy direct route to
   the exchange; the link is simply rediscovered on the next full
   scrape,
4. otherwise enqueued on the scheduled scrape queue with
   `source=discovered_link` (non-priority).

All scrape.exchange traffic (the `ExchangeClient` uploads/GETs, the
worker's existence-check client) — the proxy fetch stack
(`proxy_loader` pooled clients, RSS fetching, TikTok short-URL
resolution, `derived_metadata`, `yt_video_scrape`/`yt_video_queue`
error classification) — and the discovery word-source clients run on
**httpx2** with `http2=True` (servers without h2 fall back to
HTTP/1.1 via ALPN; proxied connections negotiate h2 inside the
CONNECT tunnel). Pool caps plus long `keepalive_expiry` mean
concurrent sweeps multiplex onto one warm connection instead of
opening dozens of TCP connections per second. Still on httpx
0.23.3: the curl_cffi-impersonated YouTube client
(`youtube_client.py`, `youtube_cookiejar.py`) and the
innertube-package-coupled modules (`youtube_channel_tabs.py`,
`youtube_video_innertube.py`, pinned via the `h11` override in
`pyproject.toml`). Exception classes are NOT shared — code touching
httpx2 clients must catch `httpx2.TimeoutException`/
`httpx2.TransportError`, not the httpx equivalents. Note httpcore2
requires the `trace` extension callback to be an async function
(the RSS phase-trace callback already is; the sync InnerTube
callback stays on httpx).

Links whose subscriber count is below
`CHANNEL_DISCOVERY_MIN_SUBSCRIBERS` (default 0, i.e. all links) are
never enqueued. Discovery is capped at 4 concurrent handle resolutions
per scrape and is best-effort: a per-link failure is logged and
counted but never fails the parent scrape. Set
`CHANNEL_DISCOVER_LINKED_CHANNELS=false` to turn the fan-out off, e.g.
to let the dedicated `yt_discover_channels.py` BFS tool own channel
discovery instead.

Outcomes are visible in `channel_link_discovery_outcomes_total{outcome}`
with labels `below_min_subscribers`, `resolve_failed`, `self_link`,
`already_scraped`, `on_exchange`, `exchange_check_failed`, and
`enqueued`. Each scraped channel with featured-channel links also
logs a `discovery: channel link summary` record with `links_found`,
`links_enqueued`, `links_known`, and a per-outcome
`outcome_<name>` count.

### Parallelism: across tabs, not within a tab

All channel tabs are dispatched with `asyncio.gather()`,
so an 8-tab channel issues up to 8 concurrent InnerTube
`browse` calls. Inside a single tab, however, pagination
is strictly sequential: each page returns a continuation
token that names the cursor for the next page, and that
token is opaque — it cannot be derived ahead of time. The
loop is therefore

```
page = browse(tab_params)
while page.continuation_token:
    page = browse(continuation_token=page.continuation_token)
```

This is the dominant latency cost for large channels.
A 5-year-old creator with 2,000 uploads needs ~20 sequential
`browse` calls just to drain the Videos tab; the Shorts
and Live tabs add more. The only way to compress this
latency is more proxies (each carries an independent
`PLAYER`/`BROWSE`/`NEXT` rate-limit bucket), not more
concurrency on the same proxy.

### Resolve vs scrape

The channel scraper distinguishes two operations:

- **Resolve** — turn a bare `channel_id` (UC…) into a
  `handle` (@…) via InnerTube. Cheap, claim-deduplicated
  fleet-wide via `youtube:resolving:<id>`.
- **Scrape** — full tab walk. Expensive, claim-deduplicated
  fleet-wide.

`CHANNEL_QUEUE_RESOLVE_BATCH` lets the foreground loop pop
resolve work in bounded batches. Scrape work is different:
`CHANNEL_CONCURRENCY` starts that many long-lived workers,
and each worker pops one scheduled channel, scrapes it, then
immediately pops the next. This avoids head-of-line blocking
where one huge channel can hold a whole scrape batch open.
`CHANNEL_QUEUE_SCRAPE_BATCH` is deprecated and ignored by
`yt_channel_scrape.py`.

### Failure handling

Channels classified `unavailable` accumulate a counter; a
hard threshold (`CHANNEL_UNAVAILABLE_HARD_THRESHOLD`,
default 3) marks the channel as gone. Soft failures
re-enter the queue after
`CHANNEL_UNAVAILABLE_SOFT_RETRY_SECONDS` (default 24 h).
If a successful scrape cannot determine the subscriber
count, the scraper preserves the channel's current tier and
schedules a full scrape after
`CHANNEL_MISSING_SUBSCRIBER_RETRY_SECONDS` (default 24 h).
Most "failures" in this scraper are legitimate empty
channels (counted by `channel_no_content_found_total`) —
subtract them before raising alarms about failure rate.

### Periodic channel video discovery

The Redis channel scraper enumerates all video IDs on the
initial successful scrape, then on successful scrapes 11,
21, and so on. Other refreshes fetch channel metadata.
Redis stores `successful_scrapes` and `next_full_scrape` in
`youtube:channel:meta:i:<channel_id>`, shared across
workers and hosts. Existing channels without these fields
start with an initial-scrape baseline; their tenth
successful refresh after rollout performs full enumeration.

Each full scrape adds missing videos to the Redis video
scrape queue. Uploaded IDs, local video output files, and
videos already present on scrape.exchange are skipped.
Normal enqueue also preserves existing queue entries and
terminal states. Enumeration, persistence, lookup, or
enqueue failures do not advance the cadence. Partial
delivery is safe to retry. Only successful, schedulable
channel scrapes advance the counter; terminal channels
retain their existing workflow behavior.

Forced full scrapes also discover missing videos without
shifting an upcoming periodic refresh. Forced metadata
scrapes leave a due full scrape pending for the next
ordinary refresh.

A missing subscriber count does not block video discovery
or completion. The scraper preserves the current tier and
any previously known count, and retries metadata after
`CHANNEL_MISSING_SUBSCRIBER_RETRY_SECONDS` (default:
24 hours). These automatic metadata retries still respect
the periodic full-scrape cadence. Missing counts remain
unknown, not zero.

Every full attempt logs its `channel_id`, outcome,
`video_ids_found`, `video_ids_existing`,
`video_ids_added`, `video_ids_queue_known`, and
`video_ids_failed`. The message includes the actual number
added, including zero and partial additions before a
failure. Prometheus exposes:

- `channel_full_scrapes_total{outcome}`: completed,
  failed, terminal, or superseded attempts (`success`,
  `failure`, `terminal`, `superseded`).
- `channel_full_scrape_video_ids_total{outcome}`: counts
  labelled `found`, `existing`, `added`, `queue_known`, or
  `failed`.
- `channel_full_scrape_video_ids_added`: histogram of IDs
  added per full attempt, including zero; `_sum` gives
  total additions.

Channel IDs appear only in logs, not metric labels. To
inspect additions:

```promql
sum(rate(channel_full_scrape_video_ids_total{outcome="added"}[15m]))
```

## 3. RSS scraper (`yt_rss_scrape.py`)

### What it does

For every known channel, polls
`https://www.youtube.com/feeds/videos.xml?channel_id=<id>`
on a tier-driven cadence and writes new video IDs onto
the Redis video scrape queue. It also writes a lite
channel-stat record (subscriber/view/video count) as
`channel-rss-<handle>.json.br` directly into
`YOUTUBE_CHANNEL_DATA_DIR` each time it polls
successfully. The channel uploader recognises files with
the `channel-rss-` prefix and POSTs them alongside the
full `channel-<handle>` records, so subscriber counts
stay fresh without a full channel rescrape.

### Tiered polling cadence

Channels are bucketed by subscriber count into priority
tiers via `RSS_PRIORITY_QUEUES`. Each tier has its own
target interval — a 10M-subscriber channel might be
polled hourly, a 1k-subscriber channel weekly. Tiers are
re-evaluated after every successful scrape (which
re-records the subscriber count). A channel becomes
eligible to be re-polled after
`RSS_ELIGIBILITY_FRACTION` (default 0.5) of its tier
interval has elapsed — the fractional headroom lets the
SLA panel report on-time fetches rather than "always
overdue."

### Stream-processor concurrency model

The worker spawns `RSS_CONCURRENCY` independent
asyncio "streamers." Each streamer claims one channel
at a time, fetches the RSS feed, optionally calls the
InnerTube `next` endpoint for each new video, and writes
results back to Redis. One-channel-per-streamer (rather
than `claim_batch(N) → asyncio.gather(N)`) means a slow
channel only delays its own streamer; sibling streamers
keep running at the rate-limiter's natural pace. Empty
claims are absorbed by each streamer's own backoff
without worker-level coordination.

### The RSS 404 problem

YouTube's RSS endpoint silently serves HTTP 404 to IPs
its WAF considers suspicious — the channel still exists,
the feed is still there, the request just gets bounced.
404s are therefore *not* proof that a channel is dead;
they are noisy signals that need filtering on two axes:
per-channel and fleet-wide.

**Per-channel filter — failure counters.** Each channel
keeps a consecutive-404 counter. Two thresholds apply:

- `RSS_MAX_NO_FEED_FAILURES` (default 10) — for channels
  that have never served a feed. After N consecutive
  404s the channel is dropped from the polling queue.
- `RSS_MAX_NO_FEED_FAILURES_HAD_FEED` (default 50) —
  for channels that have *previously* served at least
  one successful feed. The larger threshold avoids
  losing established channels to transient soft-bans.

A single successful fetch clears the counter, so a
channel that flaps in and out of the WAF's view is never
permanently lost.

> **Operator rule of thumb (from CLAUDE.md):** if RSS
> returns 404 for a channel that has been scraped
> before, assume the YouTube WAF returned it. Confirm
> with a manual `curl` from a known-good IP before
> treating the channel as deleted.

**Fleet-wide filter — RSS circuit breaker.** When 404s
arrive in a burst across many channels, the per-channel
counters can't react fast enough — a thousand healthy
channels would each accumulate counter ticks before
anyone gives up. A second layer trips a fleet-wide
circuit breaker:

- Window: last `RSS_CIRCUIT_WINDOW_SIZE` attempts on
  previously-scraped channels (default 10).
- Trip condition: `RSS_CIRCUIT_FAIL_THRESHOLD` 404s in
  the window (default 8).
- First open: `RSS_CIRCUIT_INITIAL_OPEN_SECONDS`
  (default 60 s) of silence.
- Subsequent re-opens in "impaired" mode: cooldown
  doubles each time, capped at
  `RSS_CIRCUIT_MAX_OPEN_SECONDS` (default 2 h).
- Recovery: `RSS_CIRCUIT_RECOVERY_THRESHOLD` consecutive
  successes (default 50) return the breaker to regular
  mode.

While the circuit is open, every streamer that wants to
poll is told to wait. Each wait is offset by a random
jitter up to `RSS_CIRCUIT_WAIT_JITTER_SECONDS` (default
30 s) so workers don't all wake simultaneously and
thunder-herd into the recovery window — only one probe
should leak per recovery interval.

### Daily WAF window — fleet stop 04:25–09:25 UTC

YouTube's WAF has a recurring period each morning (roughly
04:25–09:25 UTC, observed empirically) during which it
returns 404 for a large fraction of healthy channels
across all of our proxies. The circuit breaker by itself
would handle this — open, double, eventually pin at the
2 h ceiling — but burning the circuit's "impaired" state
on a known daily event means real failures arriving
later in the day inherit a long doubled cooldown.

To keep the breaker state useful for real incidents, we
stop the RSS scraper containers fleet-wide each morning
across this window and restart them after it ends. This
is a fleet-wide operational practice (cron on each host),
not built into the scraper code. The expected operational
signature during the pause:

- No `youtube_rss_scraper_*` Prometheus series.
- `scrape_queue_size{scraper="rss_scraper"}` flat (last
  value carries forward — the gauge is init-only).
- Other scrapers (channel, video) keep running normally.

## 4. Video scraper (`yt_video_scrape.py`)

### What it does

Consumes the Redis video scrape queue populated by the
RSS scraper and writes one `video-min-<id>.json.br` (and
optionally `video-dlp-<id>.json.br`) per video.

### Queue-driven loop

Workers pop `VIDEO_QUEUE_BATCH` (default 50) ids at a
time. When the queue is empty they sleep
`VIDEO_QUEUE_IDLE_POLL_SECONDS` (default 2 s) and try
again. Already-uploaded videos are filtered out at
pop-time against a Redis-backed set of uploaded IDs —
the per-process frozenset cache was retired because it
ballooned memory on the production server.

### Two backends, two cost profiles

- **InnerTube only (default).** Async I/O all the way
  down. Per-process throughput is `CONCURRENCY /
  p50_scrape_seconds`. No GIL or executor cap applies.
- **InnerTube + yt-dlp (`VIDEO_USE_YT_DLP=true`).**
  Adds formats, captions, availability, heatmaps, etc.
  yt-dlp's `extract_info` is CPU-bound Python, runs in
  the default `ThreadPoolExecutor` (~12 threads on an
  8-core host), and is serialised by the GIL. Per-
  process throughput then caps near
  `min(CONCURRENCY, executor_size) / p50` and the
  `extract_info_active` gauge pins at the executor
  size. Scaling past one process requires
  `VIDEO_NUM_PROCESSES > 1`; each child gets its own
  executor.

### Per-attempt proxy rotation and transient retries

A video that fails with a transient error (rate-limit,
DNS, TLS, connection reset, premiere-not-yet-live) is
retried up to `VIDEO_TRANSIENT_MAX_ATTEMPTS` (default 3)
times, with `VIDEO_TRANSIENT_BACKOFF_SECONDS` (default
30 s) between attempts. Each retry **selects a fresh
proxy** rather than reusing the original — a rate-limit
on one IP shouldn't sentence a video to repeated
failures on that same IP.

Permanent classes (`unavailable`, `private`,
`age_restricted`) are recorded once and not retried.
See `_classify_yt_dlp_error()` in `yt_video_scrape.py`
for the substring → reason table.

## 5. Cross-cutting

### Rate limiter is shared across scrapers and hosts

`YouTubeRateLimiter` runs per-proxy token buckets for
five call types — `BROWSE`, `PLAYER`, `NEXT`, `HTML`,
`RSS` — plus a global per-proxy ceiling. With
`REDIS_DSN` set, buckets live in Redis and every process
on every host that points at the same Redis sees the
same per-IP ceiling. This is why running the channel,
RSS, and video scrapers against the same proxy pool on
multiple hosts is safe: YouTube sees aggregate traffic
per IP, and so does the limiter.

Without `REDIS_DSN`, the limiter falls back to a shared-
file backend on the local host, and beyond that to
per-process in-memory buckets. Multi-host coordination
requires Redis.

### Rate limits (observed / reverse-engineered)

> **Note:** YouTube does not publish official rate
> limits. All values below are community-observed and
> subject to change without notice.

| Method | Soft Limit | Hard Limit | Ban Type | yt_channel_scrape | yt_rss_scrape | yt_video_scrape |
|---|---|---|---|---|---|---|
| HTTP GET (no cookies) | ~1 req/s | ~5k/day/IP | Silent degradation | — | `RSS` | — |
| HTTP GET (with cookies) | ~3–5 req/s | ~20k/day/IP | Captcha redirect | `HTML` | — | — |
| Innertube (no context) | ~60 req/min | Variable | HTTP 429 | — | — | — |
| Innertube (valid context) | ~300–600 req/min | ~10 min sliding window | HTTP 429, recoverable | `BROWSE` | `BROWSE` `PLAYER` `NEXT` | `PLAYER` `NEXT` |
| yt-dlp (no cookies) | ~500 channels/hr | Variable | HTTP 429 + IP block | — | — | — |
| yt-dlp (with cookies) | ~1,000 channels/hr | Variable | HTTP 429, recoverable | — | — | `PLAYER` |
| Data API v3 | ~100 req/s | 10,000 units/day | Hard 429 until midnight PT reset | — | — | — |

### Rate limiter token buckets

The `YouTubeRateLimiter` enforces a separate token bucket
per call type, plus a shared global bucket across all
types. Each scraping tool draws from the buckets shown
below.

| Token | Burst | Sustained rate | Jitter | yt_channel_scrape | yt_rss_scrape | yt_video_scrape | Endpoint |
|---|---|---|---|---|---|---|---|
| `BROWSE` | 20 | ~150 req/min | 0.3–1.2 s | ✓ channel tabs | ✓ channel update | — | InnerTube `browse` |
| `PLAYER` | 3 | ~20 req/min¹ | 1.0–3.0 s | — | ✓ per-video | ✓ per-video | InnerTube `player` + yt-dlp |
| `NEXT` | 20 | ~150 req/min | 0.3–1.0 s | — | ✓ per-video | ✓ per-video | InnerTube `next` |
| `HTML` | 10 | ~90 req/min | 1.5–4.0 s | ✓ about page | — | — | HTTP page scrape |
| `RSS` | 15 | ~60 req/min | 0.2–0.8 s | — | ✓ per channel | — | YouTube RSS XML feed |
| *(global)* | 30 | ~300 req/min | none | shared | shared | shared | aggregate IP ceiling |

> ¹ yt-dlp issues ~5 sub-requests per `extract_info` call,
> so the PLAYER bucket is sized for 20 tokens/min ≈ 100
> actual YouTube requests/min at steady state.

Notes:

- **HTTP GETs** rarely return a hard 429 — YouTube
  silently serves degraded or bot-detected pages instead,
  making failures invisible without response validation.
- **Innertube** limits are per-IP on a sliding ~10-minute
  window. A valid `INNERTUBE_CONTEXT` (matching browser
  fingerprint, cookies, consent state) significantly
  raises effective limits.
- **yt-dlp** with `--cookies-from-browser chrome` is the
  single biggest factor in raising limits — it makes
  requests indistinguishable from a real browser session.
- **Data API v3** quota resets daily at midnight Pacific
  Time. `search.list` costs 100 units/call and should be
  avoided for bulk work; `channels.list` costs 1
  unit/call with up to 50 IDs per request.
- **with cookies** means using a valid browser cookie jar
  with consent cookies and optionally authenticated
  session cookies.
- Datacenter IPs are penalised much more aggressively
  than residential IPs across all methods.

### Proxy slicing

When `*_NUM_PROCESSES > 1`, the supervisor splits the
proxy pool into N disjoint chunks and gives each child
its own chunk. This is a load-balancing choice, not a
correctness requirement — the Redis-backed limiter would
keep overlapping chunks safe — but disjoint chunks
spread load evenly without contention on shared buckets.

### Identity is the contract

Every video and every channel file the scrapers write
carries a canonical `(creator_id, handle)` derived from
the same Redis identity maps (`youtube:creator_map`,
`youtube:handle_map`, `youtube:name_map`). The RSS
scraper trusts the channel scraper to populate these
maps; the video scraper trusts the RSS scraper to stamp
them onto each video. The uploaders trust whatever is on
disk. See `CONTEXT.md` for the full identity model.

## 6. Operator queue CLI (`yt_channel_queue.py`)

Channels enter the system through the Redis-backed
channel scrape queue. The `tools/yt_channel_queue.py` CLI
is the operator interface for that queue: add, remove,
search, mark, count, and bulk-import channels. It reads
`REDIS_DSN` from `.env`, so the same credentials and
connection string used by the scrapers apply.

### Add a single YouTube channel

```bash
# By handle (with or without the leading @)
PYTHONPATH=. uv run tools/yt_channel_queue.py add @veritasium

# By channel ID
PYTHONPATH=. uv run tools/yt_channel_queue.py add UCHnyfMqiRRG1u-2MsSQLbXA

# Multiple entries in one call
PYTHONPATH=. uv run tools/yt_channel_queue.py add \
    @veritasium @kurzgesagt UCHnyfMqiRRG1u-2MsSQLbXA
```

Resolvable inputs (a full `UC…` ID, or a handle whose
`(creator_id, handle)` mapping is already in the identity
store) are enqueued directly on the scheduled queue. Bare
handles that the queue can't resolve yet are enqueued on
the unresolved queue and the channel scraper resolves them
lazily.

### Bulk import from a file

```bash
PYTHONPATH=. uv run tools/yt_channel_queue.py import channels.lst
```

The file contains one entry per line — a `UC…` ID, an
`@handle`, or a JSON object with `channel_id` and/or
`channel_handle` fields.

### Read from stdin

```bash
cat channels.lst \
  | PYTHONPATH=. uv run tools/yt_channel_queue.py add -
```

Useful for piping discovery output (see
`tools/yt_discover_channels.py`) straight into the queue.

### Inspect and manage the queue

```bash
# Counts across all states
PYTHONPATH=. uv run tools/yt_channel_queue.py stats

# How many channels in tier 0 (highest priority)?
PYTHONPATH=. uv run tools/yt_channel_queue.py count --tier 0

# Show metadata for one channel
PYTHONPATH=. uv run tools/yt_channel_queue.py show @veritasium

# Search by handle / channel_id / name
PYTHONPATH=. uv run tools/yt_channel_queue.py search --by handle veri

# Re-scrape a channel even if it was recently scraped
PYTHONPATH=. uv run tools/yt_channel_queue.py rescrape @veritasium

# Force a full channel-content scrape, including video_ids
PYTHONPATH=. uv run tools/yt_channel_queue.py rescrape \
    --mode full @veritasium

# Force a metadata-only scrape, without video_ids
PYTHONPATH=. uv run tools/yt_channel_queue.py rescrape \
    --mode metadata @veritasium

# Remove from the queue
PYTHONPATH=. uv run tools/yt_channel_queue.py remove @veritasium

# Mark a terminal state (not_found, terminated, topic,
# no_videos, low_subs, etc.)
PYTHONPATH=. uv run tools/yt_channel_queue.py mark @veritasium not_found
```

Run `yt_channel_queue.py --help` for the full subcommand
list.

For bulk operator workflows, the channel scraper also
drains its priority directory. Drop a bare `UC...` channel
ID, `@handle`, or bare handle filename there to resolve
and enqueue it at priority; Redis-mode drains rename
accepted entries to `.processed` and resolution failures
to `.failed` for audit.

### Running the CLI in a container

If you don't want to install `uv` on the host, run the CLI
inside the existing `yt-channel` image:

```bash
docker compose run --rm yt-channel \
    tools/yt_channel_queue.py add @veritasium
```

## 7. Observability

Per-scraper KPI walkthroughs (which Prometheus panels to
read, in what order, when throughput drops or queues
grow) live in `CLAUDE.md` under "Diagnosing low scrape
rate from the Grafana dashboard" and "Scraper fleet KPI
review." This document covers strategy; that one covers
diagnosis.
