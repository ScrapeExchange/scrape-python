# Observability

The scrapers in this repository are designed with observability in mind, allowing you to monitor their performance and health using [Prometheus](https://prometheus.io/), [Grafana](https://grafana.com/) and [ELK](https://www.elastic.co/what-is/elk-stack).
Each scraper exposes metrics on a configurable HTTP port, which can be scraped by a Prometheus server. The metrics include information about the number of channels and videos scraped, the number of requests made to YouTube, the number of requests that were rate limited, and the current configuration of the scraper.

## Metrics and monitoring

Each scraper exposes metrics on a configurable HTTP port, which can be scraped by a Prometheus server. The metrics include information about the number of channels and videos scraped, the number of requests made to YouTube, the number of requests that were rate limited, and the current configuration of the scraper.
When running with a single process, the scrapers will use a TCP port for the HTTP server to export metrics as configured or using their default port. When running with multiple processes, each process will use a different port, starting from the configured port and incrementing by 1 for each process. For example, if you configure the `CHANNEL_METRICS_PORT` to be 9600 and you run 3 processes for the channel scraper, the first process, the 'supervisor', will use port 9600 and three worker processes will use port 9601, 9602 and 9603. If you run the channel- or video-scraper in 'upload-only' mode then it will use the default port minus 1 (ie. 9599), unless you configure otherwise.

You can then configure Prometheus to scrape the metrics from these ports and visualize them in Grafana using the [provided dashboards](files/grafana-youtube.json) To install Prometheus and Grafana, you can use Docker. Here are the steps to set up Prometheus and Grafana using Docker:
1. Create a `docker-compose.yml` file with the following content:

```yaml
version: '3'
services:
  prometheus:
    image: prom/prometheus:latest
    container_name: prometheus
    restart: unless-stopped
    ports:
      - 9090:9090
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
  grafana:
    image: grafana/grafana:latest
    container_name: grafana
    restart: unless-stopped
    ports:
      - 3000:3000
    volumes:
      - ./grafana:/var/lib/grafana

```
2. Create a `prometheus.yml` file with the following content, which configures Prometheus to scrape the metrics from the scrapers:

```yaml
global:
  scrape_interval: 15s
scrape_configs:
  - job_name: 'yt_channel_scrape'
    static_configs:
      - targets: ['localhost:9600', 'localhost:9601', 'localhost:9602', 'localhost:9603']
  - job_name: 'yt_rss_scrape'
    static_configs:
      - targets: ['localhost:9500', 'localhost:9501', 'localhost:9502', 'localhost:9503']
  - job_name: 'yt_video_scrape'
    static_configs:
      - targets: ['localhost:9400', 'localhost:9401', 'localhost:9402', 'localhost:9403']
```

3. Run `docker-compose up -d` to start the Prometheus and Grafana containers.
4. Access Grafana at `http://localhost:3000` and log in with the default credentials (username: `admin`, password: `admin`).
5. Import the provided Grafana dashboard by going to "Create" -> "Import" and uploading the `grafana-youtube.json` file. You should now see the metrics from the scrapers visualized in Grafana.

## Logs
The scrapers emit logs in a structured JSON format by default, which can be easily ingested by log management systems such as Elasticsearch or Splunk. If you want to read the logs in a more human-readable format, you can use the following command to de-jsonify the logs:

```bash
alias jqlog='jq -Rr '\''
. as $raw |
try (
  fromjson |
  ["timestamp","level","logger","file","func","line","message","exception","stack"] as $std |
  ([.level, .timestamp, .file, "\(.func)()", (.line|tostring), .message] | join(":")) as $base |
  ([to_entries[] | select(.key as $k | $std | index($k) | not) | if (.value|type) == "object" and .value.type? and .value.message? then "\(.key)=\(.value.type): \(.value.message)" else
 "\(.key)=\(.value|tostring)" end] | join(" ")) as $extras |
  if $extras == "" then $base else "\($base) \($extras)" end +
  (if .exception then "\n\(.exception)" else "" end)
) catch $raw
'\'''

cat <log_file> | jqlog
```
