# Twitch profile scraper

`tools/tw_creator_scrape.py` collects public creator profiles using a
fresh anonymous Camoufox browser. No account, OAuth token, registered
application, supplied cookie file or scrape.exchange API key is required.
Browser-generated anonymous cookies exist only for the browser session.

The scraper observes the website's profile responses. It does not call
the official developer API, replay GraphQL requests or supply a client
identifier. It writes metadata only; it does not download streams,
videos, clips, images or follower lists. Uploading is a separate workflow.

`scrape-upload` discovers `twitch-creator-*.json.br` files using
`TWITCH_CREATOR_DATA_DIR` (a comma-separated list is also supported).
Both bulk and background upload modes use the `drand` Twitch creator
schema version `0.0.1`, which must be published on the Exchange server.
The uploader validates records and manages uploaded files through
`AssetFileManagement`. It requires the usual Exchange upload credentials;
the scraper itself remains credential-free.

## Setup

Install dependencies with `uv sync`. Download the browser with
`uv run python -m camoufox fetch`. The host also needs Firefox's system
libraries; browser startup failures can indicate a missing library.

Browser startup uses public-IP discovery for GeoIP configuration. An
`InvalidIP` error means that discovery failed. The scraper retries this
failure up to three times, waiting two then four seconds, within the
configured bootstrap timeout. Persistent failures require checking access
to the lookup services through the configured proxy (or direct connection).

Set `TWITCH_CREATOR_DATA_DIR` (or `--creator-data-directory`) to the
output directory. The public website and GraphQL endpoints are fixed.

Scrape one creator without a queue:

```bash
uv run python -m tools.tw_creator_scrape --username twitch
```

The CLI-only `--username` option also accepts an `@username` or a profile/About URL
on the public Twitch website. The scraper creates the output directory.
Failures exit unsuccessfully and do not replace the previous profile.
Use `--help` to see available settings. Inherited Exchange options are
unused: this tool disables Exchange client setup entirely.

## Container usage

The shared Docker image includes Camoufox and its system dependencies,
including the audio library needed to start Firefox. Twitch records
default to `/data/twitch/creators` inside the container.

Configure `REDIS_DSN` in `.env` for daemon mode. Keep
`TWITCH_CREATOR_DATA_DIR=/data/twitch/creators` when using the example
mount, or change the mount target and setting together.

Copy `docker-compose.override.yml-example` into your local override and
replace its host source paths. Twitch uses the shared `*scraper-volumes`
mounts, which include `/data/twitch/creators`. For direct access, set
`TWITCH_CREATOR_DISABLE_PROXIES=true`.

The service starts with the other default services when running
`docker compose up -d`. To start only Twitch:

```bash
docker compose up -d tw-creator
docker compose logs -f tw-creator
```

The base Compose file references the published shared image, so that
image must contain this implementation.
To test the current checkout before publishing, build it with the same
image tag referenced by `x-scraper-common`, then use `--pull never` when
starting the service. No image is built automatically by Compose.

## Daemon and queue

Set `REDIS_DSN` to the Redis instance used for coordination. For a local
Redis without authentication, it can be `redis://localhost:6379/0`.
Remote infrastructure may have its own authentication requirements;
these are independent of the public website scraper.

```bash
uv run scrape-queue --platform twitch --entity creator add twitch
uv run scrape-queue --platform twitch --entity creator show twitch
uv run python -m tools.tw_creator_scrape
```

The operator CLI supports `import`, `export`, `remove`, `rescrape`,
`search`, `count` and `stats`, as for the existing creator scrapers.
Import files accept one username or public Twitch profile URL per
line, or JSONL with `username`, `creator_id` or `handle`.

Queue keys use `scrape:twitch:*`. Claims prevent simultaneous processing;
maintenance recovers expired claims. Refresh tiers default to:

| Minimum followers | Refresh interval |
| --- | --- |
| 1,000,000 | 24 hours |
| 100,000 | 72 hours |
| 10,000 | 168 hours |
| 0 | 336 hours |

Override with `TWITCH_CREATOR_PRIORITY_QUEUES`, using comma-separated
`interval_hours:minimum_followers` pairs. Unknown follower counts retain
the current tier and never become zero. A rounded DOM count is marked
as approximate; structured counts take precedence.

## Files and identity

`AssetFileManagement` writes `twitch-creator-{username}.json.br` records.
Fields include the username, account ID when observed, display name,
biography, image URLs, followers, partner/affiliate flags, social links,
and standard About panels. Interactive extension panels are not executed
to extract their data. Missing fields are omitted from JSON.

The output schema is
[`drand-twitch-creator-schema.json`](../tests/collateral/drand-twitch-creator-schema.json).
Validate a saved record, including Brotli-compressed files, with:

```bash
uv run python -m tools.jsonschema_validate \
  --schema tests/collateral/drand-twitch-creator-schema.json \
  /path/to/twitch-creator-example.json.br
```

Each record includes the scrape timestamp, extractor version, sources
and completeness. `complete` means the core identity, biography, avatar
and follower fields were observed; optional panels and links may be empty.
An offline channel is a valid profile and needs no live broadcast.

The schema requires account ID, display name, biography, avatar URL and
follower count for `complete` records. These fields remain optional for
`partial` records, including HTML fallback results without an account ID.
It also validates username syntax, extraction sources, unique links and
panels, nonempty panels, and the extractor version. An approximate count
must include `follower_count`; zero followers and empty biographies remain
valid. Timestamp format validation requires a JSON Schema validator with
RFC 3339 format checking enabled.

The queue also retains the last account ID across hosts. If the same
username now resolves to another ID, the scraper preserves existing
files and parks the creator with `identity_conflict`. A missing ID after
a previously identified scrape causes a retry. Renames are not inferred
from display names. Account conflicts need operator investigation before
replacing old data; `rescrape` alone does not bypass the identity check.

An explicit `UserDoesNotExist` verdict parks a creator as `unavailable`.
Generic HTTP errors do not establish account deletion. Blocked requests
and login walls trigger cooldowns; timeouts and extraction failures retry.
Existing records survive failures. Redis errors propagate, without
silently switching to an independent in-process limiter.

## Concurrency and monitoring

Start with the defaults: one worker process, one async task, two profile
navigations per minute per proxy. These are conservative starting values,
not a claim about Twitch's permitted or sustainable rate.

| Setting | Default |
| --- | --- |
| `TWITCH_CREATOR_NUM_PROCESSES` | 1 |
| `TWITCH_CREATOR_CONCURRENCY` | 1, total tasks across worker processes |
| `TWITCH_CREATOR_RPM` | 2 profile navigations per minute per proxy |
| `TWITCH_DATA_RPM` | 60 profile HTTP requests per minute per proxy |
| `TWITCH_BOOTSTRAP_RPM` | 2 browser starts per minute per proxy |
| `TWITCH_CREATOR_PROFILE_TIMEOUT_SECONDS` | 60 |
| `TWITCH_CREATOR_DATA_WAIT_SECONDS` | 15 after navigation |
| `TWITCH_CREATOR_CLAIM_TTL_SECONDS` | 300 |
| `TWITCH_CREATOR_METRICS_PORT` | 9910 |

The supervisor splits proxies into disjoint slices and distributes total
concurrency between worker processes. Each task owns one active browser
session. Concurrency is capped by the number of proxies; direct access
uses one session. The available fleet pool of 18 proxies is an upper
capacity, not the default number of browsers.

The existing `PROXIES`/`PROXY_FILES` configuration is supported. Set
`TWITCH_CREATOR_DISABLE_PROXIES=true` to force direct access. CLI settings
are exported to children, which use their assigned proxy slice.

Shared metrics use `platform="twitch"`, `scraper="twitch_creator"` and
`entity="creator"`: completed scrapes, failures by reason, duration,
queue sizes, retries and records written. Additional metrics are
`twitch_session_pool_size`, `twitch_session_active` and
`twitch_session_wait_seconds`. Metrics have no username labels.

The shared supervisor exposes multiprocess metrics through its collector;
workers inherit that arrangement. A single-profile invocation writes its
file and exits; use daemon mode for ongoing fleet metrics.

## Validation

Run offline tests:

```bash
uv run python -m unittest discover -s tests/unit -p 'test_twitch*.py'
```

Live integration tests are opt-in, use direct anonymous access and a
temporary output directory, and never use production Redis. Run:

```bash
RUN_INTEGRATION=1 TWITCH_LIVE_ENABLED=true uv run python -m unittest \
  tests.integration.test_twitch_creator_live_scrape
```

`TWITCH_LIVE_PROFILES` accepts a JSON array of usernames. Optionally set
`TWITCH_LIVE_MISSING_USERNAME` to a known nonexistent username to check
the terminal classification. The test checks that no observed request
carries authentication credentials (the website's literal `undefined`
placeholder is not a token) and that records survive a JSON round trip.
