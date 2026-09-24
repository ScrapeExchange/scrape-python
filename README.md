<div align="center">
  <a href="https://scrape.exchange">
    <img src="https://scrape.exchange/logo-full.svg" alt="ScrapeExchange Logo"/>
  </a>
</div>
<br>
<div align="center">
  <img src="files/grafana-scrape-small.png?raw=true" alt="Scrape Dashboard"/>
</div>

Python tooling to scrape content from various social media platforms and upload it to the [scrape.exchange](https://scrape.exchange). This repo is focussed on bulk scraping while avoiding bot detection. Currently supported are:
- [YouTube](YOUTUBE.md): stable, used for scraping 4m channels and 100m videos
- [TikTok](TIKTOK.md): alpha, used for scraping 1k creators and 10k videos
- [Instagram](INSTAGRAM.md): alpha, used for scraping 1k creators and 10k posts
- [Twitch](docs/twitch-creator-scraper.md): anonymous public profile scraper

The goal is to support additional platforms in the future.

To run the tools, you need access to a Linux computer that you can run Docker containers on. If you plan to scrape at scale, there is guidance available on deployment planning, see [HARDWARE-SIZING.md](HARDWARE-SIZING.md).

The tools do not download any media such as images or videos, but they do scrape metadata about the content, such as titles, descriptions, and URLs. The scraped metadata is then uploaded to the [scrape.exchange](https://scrape.exchange), where it can be accessed by other users and applications, either through the web interface, the anonymous API, or using torrents.

To upload data to the exchange, you need to have a (forever-free) account and an API key. You can create an account on the [scrape.exchange](https://scrape.exchange) website, and you can download the API key from your account settings page.

While the scraping tools have a lot of capabilities, they can be used with the default settings without much configuration effort. The tools are designed to be easy to use and to require minimal setup, so you can start scraping and uploading data to the exchange with just a few steps. The tools are also designed to be flexible and configurable, so you can customize them to fit your specific needs and use cases. For example, you can configure the tools to scrape specific channels or videos, to use proxies to avoid bot detection, and to adjust rate limits to avoid getting blocked by the platforms. The tools also support running multiple worker processes in parallel while sharing the rate limits, which can help to speed up the scraping process while still respecting the rate limits of the platforms.

In addition to the scraping tools, there is also a websocket listener tool that allows you to listen for new content being uploaded to the exchange in real-time. This can be useful for testing and debugging, as well as for getting real-time updates on new content being uploaded to the exchange. You can look at the  [Firehose page on the scrape.exchange](https://scrape.exchange/firehose) website for an example to see what kind of data you can collect with the listener.

# Quick start
The fastest way to get started with scraping and uploading data
is using Docker Compose, which can run all scrapers, their uploaders
and the YouTube PO token provider in containers. The scrapers
require a Redis instance for scrape queues, identity
maps, rate-limiter state, and uploaded-content tracking.
All cross-tool coordination goes through Redis.

1. Create an account on the [scrape.exchange](https://scrape.exchange) and get your API key from your account settings page.
2. You need a Linux machine that has docker and docker-compose installed, as well as the Python package management tool UV:

```bash
sudo apt install docker.io docker-compose-v2
curl -LsSf https://astral.sh/uv/install.sh | sh
```

3. Now execute the following commands to clone the repository, create the required directories, configure your credentials and settings, and start all services:

```bash
git clone https://github.com/scrape-python/scrape-python.git
cd scrape-python

# Create host directories for scraped data, sessions, and logs
mkdir -p data/{channels,videos,logs,proxies}
mkdir -p data/{tiktok/creators,tiktok/videos,tiktok/session-state}

# Configure your credentials and settings
cp .env-example .env
# Edit .env:
#   - API_KEY_ID, API_KEY_SECRET   (your scrape.exchange credentials)
# All other settings have sensible defaults — see the
# comments in .env-example for what each one does.
```

4. If you do not have a Redis instance running, you can uncomment the Redis section in docker-compose.yml to run a Redis container alongside the scrapers. If you have a Redis instance running on the host or on another machine, you can leave the Redis section commented out and set `REDIS_DSN` in your `.env` file to point at that instance.

5. Copy docker-compose.override.yml-example to docker-compose.override.yml and edit it to map the host directories you created above into the containers.

6. Start all services

```bash
docker compose --profile scrape-upload up -d
```

This starts the services defined in `docker-compose.yml`:
- **po-token-provider** — generates PO tokens used by   both InnerTube and yt-dlp to look like a real browser
- **redis** — optional Redis instance for scrape queues, identity maps, rate-limiter state, and uploaded-content tracking
- **yt-channel** — scrapes YouTube channel metadata (about page, video/playlist/podcast/courses/store/community tabs) via the InnerTube API
- **yt-rss** — polls each YouTube channel's RSS feed for new videos and writes lite channel-stat records for the channel uploader
- **yt-video** — scrapes per-video metadata via InnerTube by default. Set `VIDEO_USE_YT_DLP=true` to additionally run yt-dlp for additional formats, captions, heatmaps, etc.
- **tt-creator** — scrapes TikTok creator/profile records and queues creator video URLs for the TikTok video scraper
- **tt-video** — consumes the TikTok video scrape queue and writes per-video metadata files
- **ig-creator** — scrapes Instagram creator/profile records
- **scrape-upload** — watches the data directories and uploads channel, video, and creator files to the Scrape.Exchange API

 You can start individual services instead of the full fleet:

```bash
# If you have uncommented the Redis section in docker-compose.yml, you can start the Redis service:
docker compose up -d redis

# Start only the YouTube channel scraper and its dependency and the uploader
docker compose up -d po-token-provider yt-channel scrape-upload

# Or just the YouTube RSS scraper and the uploader
docker compose up -d yt-rss scrape-upload

# Or the TikTok creator and video scrapers and the uploader
docker compose up -d tt-creator tt-video scrape-upload

# Or the Instagram creator scraper and uploader
docker compose up -d ig-creator scrape-upload
```

Note: at first start the scrape queues are empty, so the scrapers will sit idle. Continue with "Queueing channels
for scraping" below to feed YouTube work, or with "TikTok Scrapers" for TikTok creator queue examples.

## Queueing channels for scraping

Channels enter the system through the Redis-backed channel scrape queue. The `tools/yt_channel_queue.py`
CLI is the operator interface for that queue: add, remove, search, mark, count, and bulk-import channels.
See [YOUTUBE.md](YOUTUBE.md) for the full CLI walkthrough and examples.

## Mapping host directories into containers

By default the containers store scraped data inside the
container filesystem, which means data is lost when the
container is removed. To persist data on your host, you
need to mount host directories as volumes. Nearly all cross-
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

The YouTube tooling — three scrapers (`yt_channel_scrape.py`,
`yt_rss_scrape.py`, `yt_video_scrape.py`), two uploaders
(`yt_channel_upload.py`, `yt_video_upload.py`), and the operator CLI
(`yt_channel_queue.py`) — is documented in [YOUTUBE.md](YOUTUBE.md),
including the pipeline data flow, the observed rate limits and
token-bucket configuration, and the scraping strategy for each tool.

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

## The generic scrape queue CLI (`scrape_queue.py`)

`tools/scrape_queue.py` is the platform-agnostic operator
CLI for scrape queues. It holds no platform logic of its
own: it resolves a `(platform, entity)` adapter from
`scrape_exchange.queue_admin` and dispatches subcommands
to it. It defaults to `--platform tiktok --entity
creator`; use `--platform`/`--entity` to target other
platforms (Twitch and Instagram creators, OnlyFans
creators, and so on).

> **Do not use `scrape_queue.py` for YouTube.** The
> YouTube queues have their own dedicated operator CLI,
> `tools/yt_channel_queue.py`, which understands YouTube's
> tiered priority queues, channel identity resolution, and
> re-scrape modes. `scrape_queue.py` has no YouTube
> adapter and cannot manage the YouTube channel or video
> scrape queues. See [YOUTUBE.md](YOUTUBE.md) for
> `yt_channel_queue.py` usage.

### Usage

```bash
PYTHONPATH=. uv run tools/scrape_queue.py -h

# Add entries (defaults to the TikTok creator queue)
PYTHONPATH=. uv run tools/scrape_queue.py add @username

# Target a specific platform/entity
PYTHONPATH=. uv run tools/scrape_queue.py \
    --platform twitch --entity creator add somecreator

# Queue statistics and per-entry inspection
PYTHONPATH=. uv run tools/scrape_queue.py stats
PYTHONPATH=. uv run tools/scrape_queue.py show @username
PYTHONPATH=. uv run tools/scrape_queue.py search --by handle someuser

# Remove an entry or schedule a re-scrape
PYTHONPATH=. uv run tools/scrape_queue.py remove @username
PYTHONPATH=. uv run tools/scrape_queue.py rescrape @username

# Bulk import / export
PYTHONPATH=. uv run tools/scrape_queue.py import creators.lst
PYTHONPATH=. uv run tools/scrape_queue.py export creators-out.lst
```

The tool reads `REDIS_DSN` from `.env`, so the same
credentials and connection string used by the scrapers
apply. Run `<subcommand> -h` for the options of each
subcommand.

## Websocket listener
With tools/listen_messages.py, you can listen to the websocket for new channels and videos being uploaded to the [scrape.exchange](https://scrape.exchange). This is useful for testing and debugging, as well as for getting real-time updates on new content being uploaded to the exchange. Depending on your filtering criteria, this can be a very high volume of messages, so use it with caution.
```bash
PYTHONPATH=. uv run tools/listen_messages.py --platform youtube
```
