# scrape-python

Python tools to scrape content from various platforms and upload it to the ![ScrapeExchange Logo](https://scrape.exchange/logo-full.svg)[scrape.exchange](https://scrape.exchange). At this time, only YouTube is supported, but the goal is to support more platforms in the future. The tool does not download any media such as images or videos, but it does scrape metadata about the content, such as titles, descriptions, and URLs. The scraped metadata is then uploaded to the [scrape.exchange](https://scrape.exchange), where it can be accessed by other users and applications, either through the web interface, the anonymous API, or using torrents.

# Installation

These instructions assume you have access to a Linux computer. It needs to have Python3, the 'uv' tool, and docker installed. The instructions for installing 'uv' are available at [uv documentation](https://docs.astral.sh/uv/getting-started/installation/). Instructions for installing docker can bbe found on the [docker website](https://docs.docker.com/desktop/setup/install/linux/).

The YouTube scrapers can be used to scrape metadata about videos and channels from YouTube. The code leverages the `InnerTube` and `yt-dlp` packages to scrape the metadata from YouTube. The `yt-dlp` package needs some bits installed and configured to work properly with YouTube.
- [deno](https://docs.deno.com/runtime/getting_started/installation/): ```curl -fsSL https://deno.land/install.sh | sh```
- [po-token-provider](https://github.com/yt-dlp/yt-dlp/wiki/PO-Token-Guide): ```docker run --name bgutil-provider -d -p 4416:4416 --init brainicism/bgutil-ytdlp-pot-provider```

# Using a proxy

To avoid more stringent bot checking to access content, you can use a proxy. To do this, set the `HTTP_PROXY` and `HTTPS_PROXY` environment variables to the URL of your proxy server. For example:

```bash
export HTTP_PROXY=http://your-proxy-server:port
export HTTPS_PROXY=http://your-proxy-server:port
export NO_PROXY=https://scrape.exchange
```

If you don't have a proxy server, but you do have a VPN service, you can use the VPN's proxy server. Check your VPN provider's documentation for the proxy server details.

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
There are three tools available in this repository for scraping YouTube content and uploading it to the [scrape.exchange](https://scrape.exchange). They share a directory structure:

```
YOUTUBE_CHANNEL_LIST -> yt_channel_scrape.py -> YOUTUBE_CHANNEL_DATA_DIR
YOUTUBE_CHANNEL_DATA_DIR -> yt_rss_scrape.py -> YOUTUBE_VIDEO_DATA_DIR
YOUTUBE_VIDEO_DATA_DIR -> yt_video_scrape.py -> YOUTUBE_VIDEO_DATA_DIR
```

These scripts share a set of command line arguments, which can also be set using environment variables. They also support `.env` files, which is easiest to use. A sample .env file is included in the repository as `.env.example`.
- yt_channel_upload.py: Scrapes channels and the information about their videos, shorts, playlists, merch etc. using the Innertube library, saves the scraped metadata as JSON files in the 'YOUTUBE_CHANNEL_DATA_DIR' directory.
- yt_rss_reader.py: reads the channels from the YOUTUBE_CHANNEL_DATA_DIR directory. For each channel, it does a quick scrape of the About page of the channel to get latest counters for subscribers, views, and videos, and calls the YouTube RSS feed to get the latest videos. It then uploads the channel- and video metadata to the API and saves a copy of the scraped video metadata as JSON files in the 'YOUTUBE_VIDEO_DATA_DIR' directory.
- yt_video_upload.py: reads the video metadata from the YOUTUBE_VIDEO_DATA_DIR directory, augments it with data scraped using yt-dlp, and uploads it to the API.

## Websocket listener
With tools/listen_messages.py, you can listen to the websocket for new channels and videos being uploaded to the [scrape.exchange](https://scrape.exchange). This is useful for testing and debugging, as well as for getting real-time updates on new content being uploaded to the exchange. Depending on your filtering criteria, this can be a very high volume of messages, so use it with caution.
```bash
PYTHONPATH=. uv run tools/listen_messages.py --platform youtube
```

## Schema upload
If you want to upload new JSONSchemas to the exchange, you can use the [schema page](https://scrape.exchange/schema) on the website.  The `tools/upload_schema.py` script is also available for this purpose. This is useful for updating the schemas used by the exchange, or for adding new schemas for new types of content.
