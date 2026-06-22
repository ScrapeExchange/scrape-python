<div align="center">
  <a href="https://scrape.exchange">
    <img src="https://scrape.exchange/logo-full.svg" alt="ScrapeExchange Logo"/>
  </a>
</div>
<br>
<div align="center">
  <img src="files/grafana-scrape-small.png?raw=true" alt="Scrape Dashboard"/>
</div>

Python tooling to scrape content from various social media platforms and upload it to the [scrape.exchange](https://scrape.exchange). This repo
is focussed on bulk scraping while avoiding bot detection. Currently supported are:
- [YouTube](YOUTUBE.md): stable, used for scraping 650k channels and 60m videos
- [TikTok](TIKTOK.md): alpha, used for scraping 1k creators and 10k videos

For deployment planning, see
[HARDWARE-SIZING.md](HARDWARE-SIZING.md).

The goal is to support additional platforms in the future. The tools do not download any media such as images or videos, but they do scrape metadata about the content, such as titles, descriptions, and URLs. The scraped metadata is then uploaded to the [scrape.exchange](https://scrape.exchange), where it can be accessed by other users and applications, either through the web interface, the anonymous API, or using torrents.

To upload data to the exchange, you need to have a (forever-free) account and an API key. You can create an account on the [scrape.exchange](https://scrape.exchange) website, and you can download the API key from your account settings page.

While the scraping tools have a lot of capabilities, they can be used with the default settings, without much configuration effort. The tools are designed to be easy to use and to require minimal setup, so you can start scraping and uploading data to the exchange with just a few steps. The tools are also designed to be flexible and configurable, so you can customize them to fit your specific needs and use cases. For example, you can configure the tools to scrape specific channels or videos, to use proxies to avoid bot detection, and to adjust rate limits to avoid getting blocked by the platforms. The tools also support running multiple worker processes in parallel while sharing the rate limits, which can help to speed up the scraping process while still respecting the rate limits of the platforms.

In addition to the scraping tools, there is also a websocket listener tool that allows you to listen for new content being uploaded to the exchange in real-time. This can be useful for testing and debugging, as well as for getting real-time updates on new content being uploaded to the exchange. You can look at the  [Firehose page on the scrape.exchange](https://scrape.exchange/firehose) website for an example to see what kind of data you can collect with the listener.

# Quick start
The fastest way to get started with scraping and uploading data
is using Docker Compose, which can run all scrapers, their uploaders
and the YouTube PO token provider in containers. The scrapers
require a Redis instance for scrape queues, identity
maps, rate-limiter state, and uploaded-content tracking.
All cross-tool coordination goes through Redis.

1. Create an account on the
   [scrape.exchange](https://scrape.exchange) and get
   your API key from your account settings page.
2. Provision a Redis instance reachable from every host
   that will run scrapers. Redis 6.2 or newer is
   required (the rate limiter uses the `TIME` command
   inside Lua and `ZADD ... GT`). You can run Redis
   separately or add a `redis` service in your local
   Docker Compose override.
3. Log in to a Linux machine with Docker and Docker
   Compose installed, then run:

```bash
git clone https://github.com/scrape-python/scrape-python.git
cd scrape-python

# Create host directories for scraped data, sessions, and logs
mkdir -p data/{channels,videos,logs,tiktok/creators,tiktok/videos,tiktok/session-state}

# Configure your credentials and settings
cp .env-example .env
# Edit .env:
#   - API_KEY_ID, API_KEY_SECRET   (your scrape.exchange credentials)
#   - REDIS_DSN                    (e.g. redis://192.0.2.10:6379/0)
# All other settings have sensible defaults — see the
# comments in .env-example for what each one does.

# Start all services
docker compose up -d
```

This starts the services defined in `docker-compose.yml`:
- **po-token-provider** — generates PO tokens used by
  both InnerTube and yt-dlp to look like a real browser
- **yt-channel** — scrapes YouTube channel metadata (about page,
  video/playlist/podcast/courses/store/community tabs)
  via the InnerTube API
- **yt-channel-upload** — uploads previously scraped
  YouTube channel data to scrape.exchange
- **yt-rss** — polls each YouTube channel's RSS feed
  for new videos and writes lite channel-stat records
  for the channel uploader
- **yt-video** — scrapes per-video metadata via InnerTube
  by default. Set `VIDEO_USE_YT_DLP=true` to
  additionally run yt-dlp for formats, captions,
  heatmaps, etc.
- **yt-video-upload** — uploads previously scraped
  YouTube video data to scrape.exchange
- **tt-creator** — scrapes TikTok creator/profile
  records and queues creator video URLs for the TikTok
  video scraper
- **tt-creator-upload** — uploads previously scraped
  TikTok creator data to scrape.exchange
- **tt-video** — consumes the TikTok video scrape queue
  and writes per-video metadata files
- **tt-video-upload** — uploads previously scraped
  TikTok video data to scrape.exchange

You can start individual services instead of the full
fleet:
```bash
# Start only the YouTube channel scraper and its dependency
docker compose up -d po-token-provider yt-channel

# Or just the YouTube RSS scraper
docker compose up -d yt-rss

# Or the TikTok creator scraper and uploader
docker compose up -d tt-creator tt-creator-upload

# Or the TikTok video scraper and uploader
docker compose up -d tt-video tt-video-upload
```

Note: at first start the scrape queues are empty, so the
scrapers will sit idle. Continue with "Queueing channels
for scraping" below to feed YouTube work, or with
"TikTok Scrapers" for TikTok creator queue examples.

## Queueing channels for scraping

Channels enter the system through the Redis-backed
channel scrape queue. The `tools/yt_channel_queue.py`
CLI is the operator interface for that queue: add,
remove, search, mark, count, and bulk-import channels.

The tool reads `REDIS_DSN` from `.env`, so the same
credentials and connection string used by the scrapers
apply.

### Add a single channel

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
`(creator_id, handle)` mapping is already in the
identity store) are enqueued directly on the scheduled
queue. Bare handles that the queue can't resolve yet
are enqueued on the unresolved queue and the channel
scraper resolves them lazily.

### Bulk import from a file

```bash
PYTHONPATH=. uv run tools/yt_channel_queue.py import channels.lst
```

The file contains one entry per line — a `UC…` ID, an
`@handle`, or a JSON object with `channel_id` and/or
`channel_handle` fields. Pass `--merge` to add to the
existing queue (the default) or `--replace` to wipe the
queue first.

### Read from stdin

```bash
cat channels.lst \
  | PYTHONPATH=. uv run tools/yt_channel_queue.py add -
```

Useful for piping discovery output (see
`tools/yt_discover_channels.py`) straight into the
queue.

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

Run `yt_channel_queue.py --help` for the full
subcommand list.

For bulk operator workflows, the channel scraper also drains its
priority directory. Drop a bare `UC...` channel ID, `@handle`, or
bare handle filename there to resolve and enqueue it at priority;
Redis-mode drains rename accepted entries to `.processed` and
resolution failures to `.failed` for audit.

### Running the CLI in a container

If you don't want to install `uv` on the host, run the
CLI inside the existing `yt-channel` image:
```bash
docker compose run --rm yt-channel \
    tools/yt_channel_queue.py add @veritasium
```

## Mapping host directories into containers

By default the containers store scraped data inside the
container filesystem, which means data is lost when the
container is removed. To persist data on your host, you
need to mount host directories as volumes. All cross-
tool coordination state (queues, identity maps, rate
limiter, no-feeds, uploaded-video IDs) lives in Redis
and does not need a host mount.

The containers expect data in these paths:

| Container path | Purpose |
|---|---|
| `/data/proxies` | Optional proxy files referenced by `PROXY_FILES` |
| `/data/channels` | Scraped channel metadata (`channel-*.json.br`) |
| `/data/videos` | Scraped video metadata (`video-min-*.json.br`, `video-dlp-*.json.br`) |
| `/data/tiktok/creators` | Scraped TikTok creator metadata (`tiktok-creator-*.json.br`) |
| `/data/tiktok/videos` | Scraped TikTok video metadata (`tiktok-video-*.json.br`) |
| `/data/tiktok/session-state` | TikTok browser session and `ms_token` state |
| `/var/log/scrape/scraper` | Scraper log files |
| `/var/tmp/yt_dlp_cache` | yt-dlp cache directory (only relevant when `VIDEO_USE_YT_DLP=true`) |

To map your own host directories to these paths, create
a `docker-compose.override.yml` file in the repository
root. Docker Compose automatically picks up this file
alongside the base `docker-compose.yml`, so you just run
`docker compose up -d` as usual. A generalized template
is included as `docker-compose.override.yml-example`:

```bash
cp docker-compose.override.yml-example docker-compose.override.yml
```

The override file is also a good place to add a Redis
service if you want everything self-contained on one
host:
```yaml
x-data-volumes: &data-volumes
  - type: bind
    source: /srv/scrape/proxies
    target: /data/proxies
    bind:
      create_host_path: false
  - type: bind
    source: /srv/scrape/youtube/channels
    target: /data/channels
    bind:
      create_host_path: false
  - type: bind
    source: /srv/scrape/youtube/videos
    target: /data/videos
    bind:
      create_host_path: false
  - type: bind
    source: /srv/scrape/logs
    target: /var/log/scrape/scraper
    bind:
      create_host_path: false

x-tt-data-volumes: &tt-data-volumes
  - type: bind
    source: /srv/scrape/proxies
    target: /data/proxies
    bind:
      create_host_path: false
  - type: bind
    source: /srv/scrape/tiktok/creators
    target: /data/tiktok/creators
    bind:
      create_host_path: true
  - type: bind
    source: /srv/scrape/tiktok/videos
    target: /data/tiktok/videos
    bind:
      create_host_path: true
  - type: bind
    source: /srv/scrape/tiktok/session-state
    target: /data/tiktok/session-state
    bind:
      create_host_path: true
  # Bind the exact scraper log directory. Binding only
  # /var/log/scrape can leave Docker's nested TikTok log volume mounted
  # at /var/log/scrape/scraper, hiding current tt*.log files from the
  # host log shipper.
  - type: bind
    source: /srv/scrape/logs
    target: /var/log/scrape/scraper
    bind:
      create_host_path: true

services:
  redis:
    image: redis:7-alpine
    restart: unless-stopped
    network_mode: host
    command: ["redis-server", "--save", "60", "1000", "--appendonly", "yes"]
    volumes:
      - ./data/redis:/data

  yt-video:
    volumes: *data-volumes
  yt-video-upload:
    volumes: *data-volumes
  yt-channel:
    volumes: *data-volumes
  yt-channel-upload:
    volumes: *data-volumes
  yt-rss:
    volumes: *data-volumes
  tt-creator:
    volumes: *tt-data-volumes
  tt-video:
    volumes: *tt-data-volumes
  tt-creator-upload:
    volumes: *tt-data-volumes
  tt-video-upload:
    volumes: *tt-data-volumes
```

With Redis on the same host, set
`REDIS_DSN=redis://127.0.0.1:6379/0` in your `.env`.
For a remote Redis, point `REDIS_DSN` at the host
that runs it and omit the `redis` service from the
override.

You can also use the override file to tune parallelism
per service:
```yaml
services:
  yt-video:
    environment:
      VIDEO_NUM_PROCESSES: 2
      VIDEO_CONCURRENCY: 4
  tt-creator:
    environment:
      TIKTOK_CREATOR_NUM_PROCESSES: 1
      TIKTOK_CREATOR_CONCURRENCY: 3
  tt-video:
    environment:
      TIKTOK_VIDEO_NUM_PROCESSES: 1
      TIKTOK_VIDEO_CONCURRENCY: 3
```

For TikTok scrapers, `TIKTOK_CREATOR_CONCURRENCY` and
`TIKTOK_VIDEO_CONCURRENCY` are fleet-wide upper bounds, not
per-process values. If multiple worker processes are enabled,
the configured concurrency budget is split across those workers.
This keeps the number of Camoufox browser-backed sessions bounded
even when many proxies are configured. When the concurrency budget
is smaller than the proxy pool, each run samples a random proxy
subset before splitting work across processes, reducing the chance
that multiple scraping hosts converge on the same proxies.
Use `TIKTOK_CREATOR_NUM_PROCESSES` and
`TIKTOK_VIDEO_NUM_PROCESSES` to control the number of child
processes.

As you can see from the contents of the `.env` file, there
are many configuration options available for the scrapers,
but you can get started with changing just a few of them.
The required settings are the Scrape.Exchange API key
(`API_KEY_ID`, `API_KEY_SECRET`) and the Redis DSN
(`REDIS_DSN`). The data directories are handled by the
container configuration automatically. The other settings
can be left at their default values for now, and you can
adjust them later as you become more familiar with the
scrapers and based on your specific use case.

# Avoiding bot detection and rate limits
The scraping tools maximize the number of scrapes that
can run while minimizing the risk of being blocked for
too many requests. They use per-platform rate limiters,
proxy-aware token buckets, retry/backoff handling, and
platform-specific browser/session state to make scraper
traffic look like ordinary user traffic.

The YouTube stack uses InnerTube, yt-dlp when enabled,
the YouTube cookie jar, and the PO token provider. The
observed YouTube soft limits are documented below and in
[YOUTUBE.md](YOUTUBE.md).

The TikTok stack uses TikTokApi through Camoufox-backed
browser sessions. Each ready proxy has browser profile
state and an `ms_token` under `TIKTOK_SESSION_STATE_DIR`.
Session bootstrap, token refresh, creator API calls, and
video API calls draw from TikTok-specific buckets. See
[TIKTOK.md](TIKTOK.md) for the TikTok scraper strategy.

With `REDIS_DSN` configured, rate-limiter bucket state is
shared across processes and hosts. Buckets are still
platform-scoped, so YouTube and TikTok can coordinate
against the same Redis without sharing token counters.

# Process management and observability
Each scraper can run as a single process or with worker
processes managed by a supervisor. The supervisor splits
the configured concurrency across children, assigns proxy
slices, and restarts workers that crash or become
unresponsive.

Scrapers expose Prometheus metrics for queue depth,
scrape success and failure rates, scrape duration,
rate-limiter activity, process configuration, and
platform-specific signals such as TikTok session health.
Grafana dashboards are included as
`files/grafana-youtube.json` and
`files/grafana-scraper.json`. YouTube alert rules are in
`files/prometheus-alerts-youtube.yml`.

Logs are emitted by default in structured JSON, which can
be ingested by log management systems such as
Elasticsearch or Splunk.
For more info about observability of the scrapers, see the [OBSERVABILITY.md](OBSERVABILITY.md) doc.


# Using a proxy

To avoid more stringent bot checking to access content, you can use web proxies. To do this, set the `PROXY_FILES` setting to a comma-separated list of files where each line is a proxy URL (or a `local://x.x.x.x` egress IP). For example in your .env file:
```env
PROXY_FILES=./proxies.txt,./more_proxies.txt
```

Each line in those files looks like one of:
```
http://host:port
http://user:pass@host:port
http://host:port:user:pass
local://203.0.113.7
```

The rate limiter will apply the rate limits per proxy server, so using multiple proxy servers can help to increase the overall rate of scraping while still avoiding triggering bot detection. If you don't have a proxy server provider, but you do subscribe to a VPN service, you can use the VPN's proxy server. Check your VPN provider's documentation for the proxy server details.

If you have a VPN subscription (ie., NordVPN, ProtonVPN, etc.), you can also set up your own proxy server using your VPN service. For example, you can use the Gluetun Docker image to set up a VPN connection and a Squid proxy server. Here's how you can do it:
- Install docker as described above.
- Save the following as `docker-compose.yml`:

```yaml
version: '3'
services:
  gluetun:
    image: qmcgaw/gluetun
    container_name: gluetun
    pull_policy: missing
    restart: unless-stopped
    cap_add:
      - NET_ADMIN
    devices:
      - /dev/net/tun:/dev/net/tun
    ports:
      - 3128:3128
      - 8000:8000   # https://github.com/qdm12/gluetun-wiki/blob/main/setup/advanced/control-server.md
    volumes: []
    environment:
      - VPN_SERVICE_PROVIDER=protonvpn
      - VPN_TYPE=wireguard
      - PORT_FORWARD_ONLY=on        # Only select VPN servers that support port forwarding
      - VPN_PORT_FORWARDING=on
      - WIREGUARD_PRIVATE_KEY=<your_wireguard_private_key>
      - UPDATER_VPN_SERVICE_PROVIDERS=protonvpn
      - UPDATER_PERIOD=168h
      - SERVER_COUNTRIES='United States'

  squid:
    image: ubuntu/squid:latest
    container_name: squid
    pull_policy: missing
    network_mode: "service:gluetun"
    restart: unless-stopped
    depends_on:
      - gluetun
```

You may have to change the `VPN_SERVICE_PROVIDER`, `VPN_TYPE`, and `SERVER_COUNTRIES` environment variables to match your VPN provider and preferences. You'll have to change the `WIREGUARD_PRIVATE_KEY` environment variable to your own WireGuard private key. Then run `docker-compose up -d` to start the containers. The Squid proxy server will be available on port 3128 of your host machine, and it will route traffic through the Gluetun VPN container. For more information on setting up Gluetun, see the [Gluetun Wiki](https://github.com/qdm12/gluetun/wiki).

# JSONSchema
The uploaders validate data with JSON Schema before it is
uploaded to [scrape.exchange](https://scrape.exchange).
This helps people use the data you share more easily. If
you add fields or change record formats, create and upload
your own JSON Schema. See [docs/SCHEMA.md](docs/SCHEMA.md)
for details.

Repository schema fixtures live under `tests/collateral/`.
The current YouTube fixtures are
`boinko-youtube-channel-schema.json` and
`boinko-youtube-video-schema.json`. The current TikTok
fixtures are `drand-tiktok-creator-schema.json`,
`drand-tiktok-video-schema.json`, and
`drand-tiktok-hashtag-schema.json`.

You can upload new schemas using
[the website](https://scrape.exchange/schema) or
`tools/upload_schema.py`.

# Running the tools

In addition to running the scrape tools in a container,
the tools described below can also be run from the root
of the repository using the `uv` tool. We'll need to set
the PYTHONPATH environment variable because scrape-python
is not installed as a package.

```bash
# For the YouTube channel scraper:
PYTHONPATH=. uv run tools/yt_channel_scrape.py -h

# For the TikTok creator scraper:
PYTHONPATH=. uv run tools/tt_creator_scrape.py -h

# For the TikTok queue CLI:
PYTHONPATH=. uv run tools/scrape_queue.py -h

# For the websocket listener:
PYTHONPATH=. uv run tools/listen_messages.py
```

The first time you run one of the tools, or after you pull new changes from the repository, `uv` will automatically install any new dependencies specified in the `pyproject.toml` file. After that, it will run the tool using the installed dependencies.

## YouTube Scrapers
There are five tools available in this repository for scraping YouTube content and uploading it to the [scrape.exchange](https://scrape.exchange): three scrapers (`yt_channel_scrape.py`, `yt_rss_scrape.py`, `yt_video_scrape.py`) and two uploaders (`yt_channel_upload.py`, `yt_video_upload.py`).

To scrape one or more YouTube channels, you enqueue them on the Redis-backed channel scrape queue with `tools/yt_channel_queue.py` (see "Queueing channels for scraping" above). Each entry is a `UC…` channel-ID, an `@handle`, or a JSON object combining both. The channel scraper pops entries off this queue, resolves identity through the shared Redis identity maps (`youtube:creator_map`, `youtube:handle_map`, `youtube:name_map`), and saves the scraped channel data to `YOUTUBE_CHANNEL_DATA_DIR`. The RSS scraper polls each known channel's RSS feed for newly published videos and enqueues their IDs onto the Redis video scrape queue. The video scraper consumes that queue and writes per-video JSON files to `YOUTUBE_VIDEO_DATA_DIR`. The channel and video uploaders then POST the scraped files to the Scrape.Exchange API.

The data flow between the tools is:

```
yt_channel_queue.py  -> Redis channel scrape queue
                                │
                                ▼
                       yt_channel_scrape.py -> YOUTUBE_CHANNEL_DATA_DIR
                                                       │
                                                       ▼
                                              yt_channel_upload.py -> scrape.exchange

RSS feeds  -> yt_rss_scrape.py  -> Redis video scrape queue
                                          │
                                          ▼
                                  yt_video_scrape.py  -> YOUTUBE_VIDEO_DATA_DIR
                                                                │
                                                                ▼
                                                       yt_video_upload.py -> scrape.exchange
```

The data directories store files after scraping. Upon successful upload of the data to the API, the files are moved to a subdirectory called "uploaded", so you can keep track of which data has been uploaded and which is still pending. The uploaders also run in watch mode by default (`CHANNEL_UPLOAD_WATCH=true`, `VIDEO_UPLOAD_WATCH=true`), so any new files dropped into the data directories are picked up automatically.

Data is stored in these directories in compressed JSON files with the .json.br extension. The files are compressed using Brotli to save disk space. Each file contains the metadata for a single channel or video. The filename format is `channel-<channel_handle>.json.br` for channels, `video-min-<video_id>.json.br` for InnerTube-only video records, and `video-dlp-<video_id>.json.br` for records augmented with yt-dlp. By default the video scraper produces "min" files only; set `VIDEO_USE_YT_DLP=true` to additionally run yt-dlp and produce "dlp" files (formats, captions, heatmaps, etc.). You will also see files in the data directory with extensions like `.unresolved`, `.not_found`, and `_failed`, which indicate channels or videos that could not be scraped successfully.

These scripts share a set of command line arguments, which can also be set using environment variables. They also support `.env` files, which is easiest to use. A sample .env file is included in the repository as `.env-example`.
- **yt_channel_queue.py** (operator CLI): Adds, removes, searches and inspects entries on the Redis channel scrape queue. This is how channels enter the system.
- **yt_channel_scrape.py**: Pops channels from the Redis channel scrape queue, scrapes them (about page, video/playlist/community tabs, merch, etc.) using the InnerTube API, and saves the scraped metadata as JSON files in `YOUTUBE_CHANNEL_DATA_DIR`.
- **yt_channel_upload.py**: Watches `YOUTUBE_CHANNEL_DATA_DIR` (and its priority sub-directory) and uploads channel files to the Scrape.Exchange API.
- **yt_rss_scrape.py**: For each known channel, polls the YouTube RSS feed for newly published videos and enqueues their IDs onto the Redis video scrape queue. It also writes lite channel-stat records (subscriber/view/video counts) as `channel-rss-<handle>.json.br` into `YOUTUBE_CHANNEL_DATA_DIR` for the channel uploader to POST.
- **yt_video_scrape.py**: Consumes the Redis video scrape queue and writes per-video JSON files to `YOUTUBE_VIDEO_DATA_DIR`. Uses InnerTube by default; opts in to yt-dlp via `VIDEO_USE_YT_DLP=true`.
- **yt_video_upload.py**: Watches `YOUTUBE_VIDEO_DATA_DIR` (and its priority sub-directory) and uploads video files to the Scrape.Exchange API.

These scripts use a rate limiter to avoid making too many requests to YouTube in a short period of time, which can trigger bot detection and lead to temporary or permanent bans. The rate limiter is implemented in the `YouTubeRateLimiter` class in the `youtube_rate_limiter.py` module. The rate limiter uses a token bucket algorithm to limit the number of requests that can be made in a given time period. The rate limits are based on the observed behavior of YouTube's bot detection mechanisms, but they may need to be adjusted over time as YouTube changes its algorithms.
The rate limiter is tuned to comply with the soft-limits from this table:

### YouTube Rate Limits (Observed / Reverse-Engineered)

> **Note:** YouTube does not publish official rate limits. All values below are
> community-observed and subject to change without notice.

## Rate Limit Summary

| Method | Soft Limit | Hard Limit | Ban Type | yt_channel_scrape | yt_rss_scrape | yt_video_scrape |
|---|---|---|---|---|---|---|
| HTTP GET (no cookies) | ~1 req/s | ~5k/day/IP | Silent degradation | — | `RSS` | — |
| HTTP GET (with cookies) | ~3–5 req/s | ~20k/day/IP | Captcha redirect | `HTML` | — | — |
| Innertube (no context) | ~60 req/min | Variable | HTTP 429 | — | — | — |
| Innertube (valid context) | ~300–600 req/min | ~10 min sliding window | HTTP 429, recoverable | `BROWSE` | `BROWSE` `PLAYER` `NEXT` | `PLAYER` `NEXT` |
| yt-dlp (no cookies) | ~500 channels/hr | Variable | HTTP 429 + IP block | — | — | — |
| yt-dlp (with cookies) | ~1,000 channels/hr | Variable | HTTP 429, recoverable | — | — | `PLAYER` |
| Data API v3 | ~100 req/s | 10,000 units/day | Hard 429 until midnight PT reset | — | — | — |

### Rate Limiter Token Buckets

The `YouTubeRateLimiter` enforces a separate token bucket per call type, plus a shared global bucket across all types. Each scraping tool draws from the buckets shown below.

| Token | Burst | Sustained rate | Jitter | yt_channel_scrape | yt_rss_scrape | yt_video_scrape | Endpoint |
|---|---|---|---|---|---|---|---|
| `BROWSE` | 20 | ~150 req/min | 0.3–1.2 s | ✓ channel tabs | ✓ channel update | — | InnerTube `browse` |
| `PLAYER` | 3 | ~20 req/min¹ | 1.0–3.0 s | — | ✓ per-video | ✓ per-video | InnerTube `player` + yt-dlp |
| `NEXT` | 20 | ~150 req/min | 0.3–1.0 s | — | ✓ per-video | ✓ per-video | InnerTube `next` |
| `HTML` | 10 | ~90 req/min | 1.5–4.0 s | ✓ about page | — | — | HTTP page scrape |
| `RSS` | 15 | ~60 req/min | 0.2–0.8 s | — | ✓ per channel | — | YouTube RSS XML feed |
| *(global)* | 30 | ~300 req/min | none | shared | shared | shared | aggregate IP ceiling |

> ¹ yt-dlp issues ~5 sub-requests per `extract_info` call, so the PLAYER bucket is sized for 20 tokens/min ≈ 100 actual YouTube requests/min at steady state.

## Notes

- **HTTP GETs** rarely return a hard 429 — YouTube silently serves degraded or
  bot-detected pages instead, making failures invisible without response validation.
- **Innertube** limits are per-IP on a sliding ~10-minute window. A valid
  `INNERTUBE_CONTEXT` (matching browser fingerprint, cookies, consent state)
  significantly raises effective limits.
- **yt-dlp** with `--cookies-from-browser chrome` is the single biggest factor
  in raising limits — it makes requests indistinguishable from a real browser session.
- **Data API v3** quota resets daily at midnight Pacific Time. `search.list`
  costs 100 units/call and should be avoided for bulk work; `channels.list`
  costs 1 unit/call with up to 50 IDs per request.
- **with cookies** means using a valid browser cookie jar with consent cookies and optionally authenticated session cookies.
- Datacenter IPs are penalised much more aggressively than residential IPs
  across all methods.

## TikTok Scrapers

There are four main TikTok daemon/upload tools:
two scrapers (`tt_creator_scrape.py`, `tt_video_scrape.py`)
and two uploaders (`tt_creator_upload.py`,
`tt_video_upload.py`). The TikTok tools use the shared
Redis queues, proxy catalog, rate limiter, and
Camoufox-backed session pool. They store browser session
state under `TIKTOK_SESSION_STATE_DIR` so restarted
containers can reuse working sessions.

TikTok creators enter through the generic scrape queue:

```bash
PYTHONPATH=. uv run tools/scrape_queue.py add @tiktok
PYTHONPATH=. uv run tools/scrape_queue.py stats
```

The creator scraper consumes the TikTok creator queue,
writes `tiktok-creator-*.json.br` files to
`TIKTOK_CREATOR_DATA_DIR`, and enqueues creator video
URLs onto the TikTok video scrape queue. The video
scraper consumes those URLs and writes
`tiktok-video-*.json.br` files to `TIKTOK_VIDEO_DATA_DIR`.
The uploaders validate records against the Scrape.Exchange
TikTok JSON Schemas before uploading them.

The data flow between the TikTok tools is:

```
tools/scrape_queue.py -> Redis TikTok creator queue
                                  │
                                  ▼
                         tt_creator_scrape.py -> TIKTOK_CREATOR_DATA_DIR
                                  │                         │
                                  │                         ▼
                                  │                tt_creator_upload.py
                                  │                         │
                                  ▼                         ▼
                    Redis TikTok video queue         scrape.exchange
                                  │
                                  ▼
                         tt_video_scrape.py -> TIKTOK_VIDEO_DATA_DIR
                                                            │
                                                            ▼
                                                   tt_video_upload.py
                                                            │
                                                            ▼
                                                   scrape.exchange
```

These scripts are the main TikTok entry points:

- **scrape_queue.py** (operator CLI): Adds, removes,
  inspects, and schedules TikTok creator queue entries.
  It defaults to `--platform tiktok --entity creator`.
- **tt_creator_scrape.py**: Scrapes TikTok creator/profile
  metadata, compact video references, and playlist
  references, then writes creator JSON files.
- **tt_creator_upload.py**: Watches
  `TIKTOK_CREATOR_DATA_DIR`, validates creator records,
  and uploads them to the Scrape.Exchange API.
- **tt_video_scrape.py**: Consumes TikTok video URLs from
  the Redis video queue and writes per-video JSON files.
- **tt_video_upload.py**: Watches `TIKTOK_VIDEO_DATA_DIR`,
  validates video records, and uploads them to the API.

`tt_discover_search.py` is an optional discovery helper
that writes discovered TikTok usernames to JSONL from
Explore pages and TikTok search results. Import or add
those usernames with `tools/scrape_queue.py` when you want
the creator scraper to process them.

TikTok upload schema selection is configured with
`TIKTOK_SCHEMA_OWNER` and `TIKTOK_SCHEMA_VERSION`; the
uploaders pass `platform=tiktok` and the relevant entity
(`creator` or `video`) when fetching the JSON Schema from
the exchange.

## Websocket listener
With tools/listen_messages.py, you can listen to the websocket for new channels and videos being uploaded to the [scrape.exchange](https://scrape.exchange). This is useful for testing and debugging, as well as for getting real-time updates on new content being uploaded to the exchange. Depending on your filtering criteria, this can be a very high volume of messages, so use it with caution.
```bash
PYTHONPATH=. uv run tools/listen_messages.py --platform youtube
```
