![ScrapeExchange Logo](https://scrape.exchange/logo-full.svg)

Python tools to scrape content from various platforms and upload it to the [scrape.exchange](https://scrape.exchange). At this time, only YouTube is supported, but the goal is to support more platforms in the future. The tool does not download any media such as images or videos, but it does scrape metadata about the content, such as titles, descriptions, and URLs. The scraped metadata is then uploaded to the [scrape.exchange](https://scrape.exchange), where it can be accessed by other users and applications, either through the web interface, the anonymous API, or using torrents. To upload data to the exchange, you need to have an account and an API key. You can create an account on the [scrape.exchange](https://scrape.exchange) website, and you can generate an API key from your account settings.
In addition to the scraping tools, there is also a websocket listener tool that allows you to listen for new content being uploaded to the exchange in real-time. This can be useful for testing and debugging, as well as for getting real-time updates on new content being uploaded to the exchange. You can look at the  [Firehose page on the scrape.exchange](https://scrape.exchange/firehose) website for an example to see what kind of data you can collect with the listener.

# Avoiding bot detection and rate limits
The scraping tools in this directory maximize the number of scrapes that you can do while minimizing the risk of being blocked by the platform for making too many requests. To do this, the tools use a rate limiter to limit the number of requests that can be made in a given time period. The rate limits are based on the observed behavior of YouTube's bot detection mechanisms, but they may need to be adjusted over time as YouTube changes its algorithms. The tools also support using proxies to route requests through different IP addresses, which can help to avoid triggering bot detection. They use the [InnerTube module](https://github.com/yt-dlp/yt-dlp/wiki/InnerTube) to interact with YouTube's internal API. Furthermore, it sets up the [yt-dlp](https://github.com/yt-dlp/yt-dlp) module with the appropriate cookies and headers to make the requests look like they are coming from a real browser session, which can also help to avoid triggering bot detection. Finally, as per the instructions of yt-dlp, it uses deno and the po-token-provider to generate the necessary tokens for making requests to YouTube, which can also help to avoid triggering bot detection. When a scraper receives a response from YouTube that indicates that it has been rate limited or blocked, it will back off and retry the request after a certain amount of time. The backoff time is increased exponentially with each subsequent failure, up to a maximum backoff time. This way, the scraper can recover from temporary blocks and continue scraping without getting permanently blocked.

The scrapers share a common rate limiter so you can run multiple tools at the same time without worrying about them interfering with each other and causing you to get blocked. Currently, all scrapers need to run on the same host to share the rate limiter, but in the future, the rate limiter could be extended to work across multiple hosts using a shared database or cache.

# Process management and observability
Each of the scrapers in this repo can be configured to either run as a single process or with multiple worker processes managed by a supervisor process. When running with multiple worker processes, the supervisor process will automatically restart any worker processes that crash or become unresponsive. The scrapers also expose Prometheus metrics about their performance and configuration, which can be used to monitor the scrapers and alert on any issues. The metrics include information about the number of channels and videos scraped, the number of requests made to YouTube, the number of requests that were rate limited, and the current configuration of the scraper. The metrics are exposed on a configurable port, and they can be scraped by a Prometheus server for monitoring and alerting. A Grafana dashboard is included in the repository as `grafana_dashboard.json` that can be imported into Grafana to visualize the metrics. There is also a configuration file included in the repository as `prometheus-alerts-youtube.yml` that can be used to configure a Prometheus server to scrape the metrics from the scrapers. Logs are emitted by default in a structured JSON format, which can be easily ingested by log management systems such as Elasticsearch or Splunk. A command to de-jsonify the logs is provided below for users who want to read the logs in a more human-readable format.
Configuration of the scrapers can be done through environment variables or command line arguments. A sample `.env` file is included in the repository as `.env.example`, which can be copied to `.env` and modified with the appropriate values for your setup.

# Installation
These instructions assume you have access to a Linux computer. It needs to have Python3, the 'uv' tool, docker, brotli, and jq installed. The instructions for installing 'uv' are available at [uv documentation](https://docs.astral.sh/uv/getting-started/installation/). Instructions for installing docker can be found on the [docker website](https://docs.docker.com/desktop/setup/install/linux/). Brotli and jq can be installed using `yum install brotli jq` on RHEL-based distributions, `apt install brotli jq` on Debian-based distributions.

The YouTube scrapers can be used to scrape metadata about videos and channels from YouTube. The code leverages the `InnerTube` and `yt-dlp` packages to scrape the metadata from YouTube. The `yt-dlp` package needs some bits installed and configured to work properly with YouTube.
- [deno](https://docs.deno.com/runtime/getting_started/installation/): ```curl -fsSL https://deno.land/install.sh | sh```
- [po-token-provider](https://github.com/yt-dlp/yt-dlp/wiki/PO-Token-Guide): ```docker run --name bgutil-provider -d -p 4416:4416 --init brainicism/bgutil-ytdlp-pot-provider``` or `docker-compose.yml`:

```yaml
version: '3'
services:
  po-token-provider:
    image: brainicism/bgutil-ytdlp-pot-provider
    container_name: bgutil-provider
    restart: unless-stopped
    init: true
    networks:
      - po-token-provider
    ports:
      - 4416:4416
```

# Using a proxy

To avoid more stringent bot checking to access content, you can use web proxies. To do this, set the `PROXIES` setting to one or more URLs of your proxy server, comma-separated. For example in your .env file:
```env
PROXIES=http://your-proxy-server:port,http://your-proxy-server:port2
```

The rate limiter will apply the rate limits per proxy server, so using multiple proxy servers can help to increase the overall rate of scraping while still avoiding triggering bot detection.
If you don't have a proxy server provider, but you do subscribe to a VPN service, you can use the VPN's proxy server. Check your VPN provider's documentation for the proxy server details.

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
The scrapers in this repository use JSONSchema to validate the data before it is uploaded to the [scrape.exchange](https://scrape.exchange). This helps people to use the data you shared more easily. If you want to make changes to the data that is being uploaded, such as adding new fields or changing the format of existing fields, you should create your own JSONSchema. The JSONSchemas for the YouTube channel and video metadata are included in the repository as `youtube_channel_schema.json` and `youtube_video_schema.json`, respectively. You can upload new schemas using [the website](https://scrape.exchange/schema) or you can use the `tools/upload_schema.py` script to upload the new schema to the exchange.

# The tools

The tools described below should be run from the root of the repository using the `uv` tool. We'll need to set the PYTHONPATH environment variable because scrape-python is not installed as a package.

```bash
# For the YouTube channel scraper:
PYTHONPATH=. uv run tools/yt_channel_scrape.py -h

# For the websocket listener:
PYTHONPATH=. uv run tools/listen_messages.py
```

The first time you run one of the tools, or after you pull new changes from the repository, `uv` will automatically install any new dependencies specified in the `pyproject.toml` file. After that, it will run the tool using the installed dependencies.

## YouTube Scrapers
There are three tools available in this repository for scraping YouTube content and uploading it to the [scrape.exchange](https://scrape.exchange). If you want to scrape one or more YouTube channels, you put them in the <YOUTUBE_CHANNEL_LIST> file. You can add either the channel-ID (a 24-character string starting with "UC") or the channel handle (a string starting with "@"). You can then run the `yt_channel_scrape.py` script to scrape the channel information and save it to the <YOUTUBE_CHANNEL_DATA_DIR> directory. It also writes to <CHANNEL_MAP_FILE> to store mappings from channel IDs ('UC<22 characters>') to channel handle. You then run the `yt_rss_reader.py` script to scrape the latest videos from the channels and save them to the <YOUTUBE_VIDEO_DATA_DIR> directory, while also uploading the metadata to the Scrape.Exchange API. Finally, you run the `yt_video_upload.py` script to augment the video metadata with data collected with yt-dlp and upload it to the API.

These tools share a directory structure:

```
YOUTUBE_CHANNEL_LIST -> yt_channel_scrape.py -> YOUTUBE_CHANNEL_DATA_DIR
YOUTUBE_CHANNEL_DATA_DIR -> yt_rss_scrape.py -> YOUTUBE_VIDEO_DATA_DIR
YOUTUBE_VIDEO_DATA_DIR -> yt_video_scrape.py -> YOUTUBE_VIDEO_DATA_DIR
```

This directory structure acts as a pipeline for scraping and uploading YouTube content. The data directories store files after scraping. Upon succesfull upload of the data to the API, the files are moved to a subdirectory called "uploaded". This way, you can keep track of which data has been uploaded and which data is still pending upload. When you run the `yt_channel_scrape.py` and `yt_video_scrape.py` scripts, they will first attempt to upload any existing files in the data directories before scraping new data. This way, you can ensure that all scraped data is eventually uploaded to the API, even if there are temporary issues with the API or your internet connection.

Data is stored in these directories in compressed JSON files with the .json.br extension. The files are compressed using Brotli to save disk space. Each file contains the metadata for a single channel or video, depending on the script that created it. The filename format is `channel-<channel_handle>.json.br` for channels and `video-min-<video_id>.json.br` and `video-dlp-<video_id.json.br` for videos, where `<channel_handle>` and `<video_id>` are the unique identifiers for the channel and video on YouTube. For the video files, the RSS scraper uses the InnerTube API and saves the file with the 'min' label. This InnerTube API provides a limited set of metadata. The video scraper uses the yt-dlp script to get additional metadata, hence the "min" and "dlp" in the filenames. The `yt_video_upload.py` script will read the "min" files and augment that data with the scraping functionality of the YT-DLP module and then rename the file from 'MIN' to 'DLP'. You will also see files in the data directory with extensions like .unresolved, .not_found, and _failed, which indicate channels or videos that could not be scraped successfully. T

These scripts share a set of command line arguments, which can also be set using environment variables. They also support `.env` files, which is easiest to use. A sample .env file is included in the repository as `.env.example`.
- yt_channel_scrape.py: Scrapes channels and the information about their videos, shorts, playlists, merch etc. using the Innertube library, saves the scraped metadata as JSON files in the 'YOUTUBE_CHANNEL_DATA_DIR' directory and calls the Scrape.Exchange API to upload the channel metadata.
- yt_rss_reader.py: reads the channels from the YOUTUBE_CHANNEL_DATA_DIR directory. For each channel, it does a quick scrape of the About page of the channel to get latest counters for subscribers, views, and videos, and calls the YouTube RSS feed to get the latest videos. For each video it collects additional data using the InnerTube API. It then uploads the channel- and video metadata to the Scrape.Exchange API and saves a copy of the scraped video metadata as JSON files in the 'YOUTUBE_VIDEO_DATA_DIR' directory.
- yt_video_upload.py: reads the video metadata from the YOUTUBE_VIDEO_DATA_DIR directory, augments it with data scraped using yt-dlp, and uploads it to the API.

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


## Websocket listener
With tools/listen_messages.py, you can listen to the websocket for new channels and videos being uploaded to the [scrape.exchange](https://scrape.exchange). This is useful for testing and debugging, as well as for getting real-time updates on new content being uploaded to the exchange. Depending on your filtering criteria, this can be a very high volume of messages, so use it with caution.
```bash
PYTHONPATH=. uv run tools/listen_messages.py --platform youtube
```
