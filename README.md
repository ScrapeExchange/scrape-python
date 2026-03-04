# scrape-python

Python tools to scrape content from various platforms and upload it to scrape.exchange. At this time, only YouTube is supported, but more platforms will be added in the future. The tool does not download any media such as images or videos, but it does scrape metadata about the content, such as titles, descriptions, and URLs. The scraped metadata is then uploaded to scrape.exchange using the API.

# Installation

These instructions assume you have access to a Linux computer. It needs to have Python3, the 'uv' tool, and docker installed. The instructions for installing 'uv' are available at [uv documentation](https://docs.astral.sh/uv/getting-started/installation/). Instructions for installing docker can bbe found on the [docker website](https://docs.docker.com/desktop/setup/install/linux/).

The YouTube scraper can be used to scrape metadata about videos and channels from YouTube. The code leverages the `InnerTube` and `yt-dlp` libraries to scrape the metadata from YouTube. The `yt-dlp` library needs some bits installed and configured to work properly with YouTube.
- [deno](https://docs.deno.com/runtime/getting_started/installation/): ```curl -fsSL https://deno.land/install.sh | sh```
- [po-token-provider](https://github.com/yt-dlp/yt-dlp/wiki/PO-Token-Guide): ```docker run --name bgutil-provider -d -p 4416:4416 --init brainicism/bgutil-ytdlp-pot-provider```

# Using a proxy

To avoid more stringent bot checking to access content, you can use a proxy. To do this, set the `HTTP_PROXY` and `HTTPS_PROXY` environment variables to the URL of your proxy server. For example:

```bash
export HTTP_PROXY=http://your-proxy-server:port
export HTTPS_PROXY=http://your-proxy-server:port
```

If you don't have a proxy server, but you have a VPN service, you can use the VPN's proxy server. Check your VPN provider's documentation for the proxy server details.

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
      - 8000:8000   # https://github.com/qdm12/gluetun-wiki/blob/main/setup/advanced/control-server. md
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
      - FIREWALL_OUTBOUND_SUBNETS=192.168.0.0/16

  squid:
    image: ubuntu/squid:6.13-25.04_beta
    container_name: squid
    pull_policy: missing
    network_mode: "service:gluetun"
    restart: unless-stopped
    depends_on:
      - gluetun
```

You may have to change the `VPN_SERVICE_PROVIDER`, `VPN_TYPE`, and `SERVER_COUNTRIES` environment variables to match your VPN provider and preferences. You'll have to change the `WIREGUARD_PRIVATE_KEY` environment variable to your own WireGuard private key. Then run `docker-compose up -d` to start the containers. The Squid proxy server will be available on port 3128 of your host machine, and it will route traffic through the Gluetun VPN container. For more information on setting up Gluetun, see the [Gluetun Wiki](https://github.com/qdm12/gluetun/wiki).
