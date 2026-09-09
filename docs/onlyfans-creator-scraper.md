# OnlyFans creator scraper

Scrape anonymous public creator profiles with the repository's Camoufox
browser, proxy loader, rate limiter and `AssetFileManagement`. By default
the tool consumes the Redis creator queue continuously, checking every
60 seconds when no creators are due. Explicit username/file arguments
remain available for manual batches.

## Run

Install dependencies and the browser once:

```bash
uv sync
uv run python -m camoufox fetch
```

Start the queue worker with `REDIS_DSN` configured:

```bash
uv run python -m tools.of_creator_scrape
```

Populate and inspect its queue using the existing operator CLI:

```bash
uv run python -m tools.scrape_queue --platform onlyfans add @onlyfans
uv run python -m tools.scrape_queue --platform onlyfans import creators.lst
uv run python -m tools.scrape_queue --platform onlyfans stats
uv run python -m tools.scrape_queue --platform onlyfans show @onlyfans
```

Queue keys use `scrape:onlyfans:*`. Creator usernames are normalized at
intake. The operator CLI's `--weight` is the number of likes received.
New entries are immediately due. Redis claims prevent simultaneous work
on the same creator; maintenance recovers expired claims every 60 seconds.

`ONLYFANS_CREATOR_PRIORITY_QUEUES` contains comma-separated
`interval_hours:minimum_likes` pairs, ordered from highest to lowest
priority, ending with a zero threshold. The default is
`24:1000000,72:100000,168:10000,336:0`:

| Likes received | Refresh interval |
| --- | --- |
| 1,000,000 or more | 24 hours |
| 100,000 or more | 72 hours |
| 10,000 or more | 168 hours |
| Below 10,000 | 336 hours |

After saving a successful scrape, the worker selects the first matching
like tier and schedules the creator that many hours into the future.
Unknown likes preserve the current tier and last observed like count;
zero is an observed value and selects the lowest tier. Fan counts do not
affect scheduling. Queue state records the count as `last_like_count`.

Failed scrapes remain queued and retry after
`ONLYFANS_CREATOR_RETRY_INTERVAL_SECONDS` (300 seconds by default).
Blocks also respect `ONLYFANS_BLOCKED_COOLDOWN_SECONDS`. Lost claims are
left to their current owner; cancelled work is recovered after claim expiry.

For a manual one-off scrape using your configured proxy file:

```bash
uv run python -m tools.of_creator_scrape \
  --username @onlyfans \
  --proxy-files /path/to/proxies.txt \
  --creator-data-directory /path/to/output
```

For a batch, use one username, `@handle` or public profile URL per line.
Blank lines and lines starting with `#` are ignored. Duplicate usernames
are processed once. All input is validated before scraping starts.

```bash
uv run python -m tools.of_creator_scrape \
  --creator-file /path/to/creators.txt \
  --proxy-files /path/to/proxies.txt \
  --creator-data-directory /path/to/output \
  --concurrency 4
```

Supply neither input option for queue mode, or exactly one of `--username`
and `--creator-file` for a manual batch. Leave `ONLYFANS_USERNAME` and
`ONLYFANS_CREATOR_FILE` unset for the daemon. Use `--help` for options.
The output directory defaults to `data/onlyfans/creators`.
No proxy configuration means direct access. A failed proxy never falls
back to a direct connection. Native `local://` source-address entries
are unsupported by this browser; use HTTP(S) proxies for those routes.

## Docker Compose

The shared image includes Camoufox and the scraper. Build/publish an image
containing this change, or tag a local build with the image name configured
in `docker-compose.yml`.

Use `docker-compose.override.yml-example` as the per-host mount template.
Set its `/path/to/onlyfans` source to your host directory. That directory
contains `creators/` for compressed output. Queue input is stored in Redis;
no input-list bind mount is needed.
Set the other mount sources, including `/path/to/proxies`, for your host.

Configure the `ONLYFANS_*` settings in `.env`; `.env-example` documents
them. The container output path is `/data/onlyfans/creators`.
Shared `PROXY_FILES` must refer to
files under the mounted `/data/proxies`; `REDIS_DSN` selects the existing
shared limiter. The optional metrics listener uses port 9920 in the
example configuration, through the shared host network.

```bash
docker compose -f docker-compose.yml \
  -f docker-compose.override.yml up -d of-creator
docker compose -f docker-compose.yml \
  -f docker-compose.override.yml logs -f of-creator
```

The service uses an init process to reap browser children and inherits
`restart: unless-stopped`, like the other continuous scrapers. SIGTERM
cancels workers and closes browsers and Redis clients. To run a manual
batch in Docker, use `docker compose run --rm of-creator` with the script
path and its explicit input arguments.

## Settings and rate limits

CLI options override environment variables and the repository `.env`.
The inherited `PROXY_FILES` and legacy `PROXIES` settings use the existing
proxy parser, including proxy authentication and file deduplication.

| Environment variable | Default | Purpose |
| --- | --- | --- |
| `ONLYFANS_USERNAME` | unset | Optional manual profile override |
| `ONLYFANS_CREATOR_FILE` | unset | Optional manual batch input |
| `ONLYFANS_CREATOR_DATA_DIR` | `data/onlyfans/creators` | Output directory |
| `ONLYFANS_CONCURRENCY` | `1` | Maximum simultaneous async tasks |
| `ONLYFANS_CREATOR_RPM` | `2` | Navigations/minute/proxy |
| `ONLYFANS_DATA_RPM` | `30` | Website API requests/minute/proxy |
| `ONLYFANS_PROFILE_TIMEOUT_SECONDS` | `90` | Profile deadline with waits |
| `ONLYFANS_BROWSER_TIMEOUT_SECONDS` | `90` | Browser startup deadline |
| `ONLYFANS_BLOCKED_COOLDOWN_SECONDS` | `300` | Shared cooldown after blocks |
| `ONLYFANS_METRICS_PORT` | unset | Optional Prometheus HTTP port |

Queue settings are `ONLYFANS_CREATOR_PRIORITY_QUEUES` (tiers above),
`ONLYFANS_CREATOR_CLAIM_TTL_SECONDS` (300), and
`ONLYFANS_CREATOR_RETRY_INTERVAL_SECONDS` (300). The claim lifetime must
exceed the browser and profile deadlines combined by more than 60 seconds.
The idle poll interval is fixed at 60 seconds.

The RPM defaults are conservative starting settings, not published
OnlyFans limits. The navigation bucket allows a burst of one; the API
bucket allows four. An aggregate bucket also limits combined traffic.
Website documents and API requests acquire permission before dispatch.
Images, audio, video and fonts are blocked; image URLs come from metadata.

Queue tasks receive disjoint slices of the configured proxy pool and
rotate through their slice as creators become due. Each task retains at
most one browser session and rebuilds it before changing proxies.
`concurrency` bounds active sessions; every proxy can be used even when
concurrency is lower than the proxy count. Anonymous session cookies
are temporary and are never loaded from or saved to a login profile.

`OnlyFansRateLimiter` derives from the shared `RateLimiter`:

1. `REDIS_DSN` selects cross-host Redis coordination, under `rl:onlyfans:*`.
2. Otherwise `RATE_LIMITER_STATE_DIR` selects shared local files in its
   `onlyfans/` subdirectory, isolated from other platforms.
3. An empty state directory and no Redis select in-process buckets.

Redis failures prevent requests from being dispatched. HTTP 401, 403 and
429 on profile access, or a detected login/challenge redirect, penalise
both buckets. Queue workers reschedule the creator and wait for the
cooldown before taking more work. In manual batch mode, a block stops
the remaining work assigned to that proxy. Other tasks can continue.
Missing profiles and timeouts never produce empty records.

Queue workers continue after individual scrape failures; an infrastructure
failure exits nonzero so the container can restart. In batch mode the CLI
returns zero when all creators succeed, one for scrape failures, two for
invalid configuration/input and 130 for keyboard interruption. Failed and
unattempted creators count toward batch failure. Existing records remain
available when a new scrape fails. Raw response bodies and browser errors
are not logged; proxy credentials are excluded from rate-limit labels.

## Output and schema

Files are named `onlyfans-creator-{username}.json.br`. The JSON Schema is
[`drand-onlyfans-creator-schema.json`][schema]. Unknown fields are explicitly
`null`, including hidden counts; observed zero and false are retained.

| Fields | Meaning |
| --- | --- |
| `username`, `handle` | Unique profile slug and the same slug with `@` |
| `display_name` | Human-readable profile name |
| `user_id`, `url` | Account ID and canonical profile URL |
| `verified` | Public `isVerified` value |
| `like_count` | Likes received (`favoritedCount`), not likes given |
| `photo_count`, `video_count`, `audio_count` | Public media counts |
| `banner_url`, `avatar_url` | Image URLs; images are not downloaded |
| `fan_count`, `fan_count_visible` | Fans with explicit visibility opt-in |
| `biography` | Public `about` value, preserving its original text/markup |
| `subscription_price` | Regular monthly price (`subscribePrice`) |
| `subscription_currency` | USD when the regular price is known |
| `subscription_status` | `free` for zero, `paid` above zero, otherwise null |
| `scraped_timestamp`, `extractor_version` | UTC scrape time and extractor |

Free/paid describes the regular subscription. It does not describe
pay-per-view posts, whether a viewer has paid, discounts or free trials.
The model ignores `currentSubscribePrice` and promotional prices when
classifying the regular subscription. A hidden media-count setting also
suppresses photo, video and audio counts.

Validate a saved record:

```bash
uv run python -m tools.jsonschema_validate \
  --schema tests/collateral/drand-onlyfans-creator-schema.json \
  /path/to/output/onlyfans-creator-onlyfans.json.br
```

The creator scraper writes local metadata. The generic `scrape-upload`
worker reads `ONLYFANS_CREATOR_DATA_DIR` (one directory or a comma-separated
list) and uploads `onlyfans-creator-*.json.br` using the
`drand/onlyfans/creator/0.0.1` schema. That schema must be registered on the
API server before the uploader starts.

The API's `x-scrape-field` and `x-scrape-gauge` validators require types
on the annotated property itself. Nullable avatar/count fields therefore
use `type: ["string", "null"]` or `type: ["integer", "null"]`, with
`format: "uri"` on the avatar property. The model emits this representation
so regenerated schemas preserve compatibility and unknown values stay null.

OnlyFans uses the same startup bulk recovery, initial directory drain,
file watching, and optional background upload mode as other platforms.
Records are validated with the OnlyFans model and server schema. Successful
uploads are moved to `uploaded/` by `AssetFileManagement`; invalid records
are marked `.invalid`. The existing Docker volume and environment settings
already expose the creator directory to `scrape-upload`.

After deploying an image with this integration, recreate `scrape-upload`.
Recreating a container from an older image does not add uploader support.
Recovery of existing jobs finishes before new files are processed.

## Verification

```bash
uv run python -m unittest \
  tests.unit.test_onlyfans_creator tests.unit.test_onlyfans_scraper \
  tests.unit.test_onlyfans_queue
```

Run the opt-in anonymous live round-trip test:

```bash
RUN_INTEGRATION=1 ONLYFANS_LIVE_ENABLED=true \
  uv run python -m unittest \
  tests.integration.test_onlyfans_creator_live_scrape
```

The live test uses isolated temporary output and limiter files, and never
uses production Redis. It defaults to the public `onlyfans` account.
Set `ONLYFANS_LIVE_USERNAME` for another account and
`ONLYFANS_LIVE_PROXY_FILES` to exercise a selected proxy file.

Verified on 2026-09-08: the live anonymous round trip passed for the
public `onlyfans` account using direct access. Proxy routing and shared
file/Redis rate limits are covered by unit tests; a live production proxy
was not used. The focused and shared-helper suite passed 199 tests.

[schema]: ../tests/collateral/drand-onlyfans-creator-schema.json

## Logging and metrics

The creator scraper uses the shared logging configuration and Prometheus
server used by the Instagram and TikTok scrapers. Logs default to JSON
on stdout. Set `LOG_FORMAT=text` for the shared text format, and use
`ONLYFANS_CREATOR_LOG_LEVEL` and `ONLYFANS_CREATOR_LOG_FILE` to override
`LOG_LEVEL` and `LOG_FILE` for this scraper. Log files support external
rotation through the shared watched-file handler.

Set `ONLYFANS_METRICS_PORT=9920` to expose Prometheus metrics. The shared
series use `platform="onlyfans"`, `scraper="onlyfans_creator"`,
`entity="creator"`, and `api="browser"` where applicable:

- `scrapes_completed_total` and `scrape_failures_total`: completed and
  failed attempts, including queue browser-startup failures.
- `scrape_duration_seconds`: duration of an attempt, including browser
  startup in queue mode.
- `scrape_retry_total`: retries successfully scheduled in Redis.
- `scrape_records_written_total`: metadata records successfully saved.
- `scrape_queue_size`: queued creators, refreshed every 60 seconds.
- `worker_sleep_seconds`: pending sleep per stable task ID; resets to
  zero on wake-up or cancellation.
- `scraper_num_processes` and `scraper_concurrency`: one worker process
  and its effective task concurrency.

Scrape outcome labels use the stable process worker ID, and proxy labels
contain only the proxy host, port, and source-file label. Creator names
and Redis claim UUIDs are excluded from metrics. Lost claims are recorded
as duration observations, without counting a success or scrape failure.
Cancellation does not count as a failed scrape. Manual batches also
publish outcomes for attempted creators; unattempted creators skipped
following a block are not counted as failed attempts.
