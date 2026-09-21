# YouTube search discovery container

The `yt-discover-search` service uses the same image and `.env` as the
other scrapers. It reads `~/proxies/vpn.proxies.lst` and writes to
`data/searched_channels.jsonl` in the repository. Both directories must
exist before starting. Credentials belong in `.env`, never in Compose.

The service has a `discovery` profile, so it starts only when explicitly
selected or when that profile is enabled. Docker restarts it after exits
and daemon restarts unless it was explicitly stopped.

From `~/src/scrape-python`:

```sh
docker compose up -d --no-deps yt-discover-search
install -m 755 files/docker/yt_discover_search.sh ~/yt_discover_search.sh
docker compose logs -f yt-discover-search
```

Stop any previous nohup or systemd discovery process before the first
container start. For the previous user service, disable and remove it:

```sh
systemctl --user disable --now yt-discover-search.service
rm ~/.config/systemd/user/yt-discover-search.service
systemctl --user daemon-reload
```

Keep the daily cron entry pointing to `~/yt_discover_search.sh`. It stops
the container, rotates output to a unique dated archive, starts the
container, and imports the archive using the host's existing uv setup.
Archives remain available if imports fail. Logs for rotation and import
append to `/tmp/cron-discover.log`; discovery logs go to Docker.

The container PID file lives on tmpfs so restarts cannot confuse an old
PID with a new process using the same container PID.
