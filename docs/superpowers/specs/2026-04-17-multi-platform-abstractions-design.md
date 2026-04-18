# Multi-Platform Abstractions Design

**Date:** 2026-04-17
**Status:** Draft
**Goal:** Prepare the `scrape_exchange` shared layer and tool
scaffolding for multiple social media platforms, with TikTok as the
first addition and Reddit likely second.

## Context

The codebase currently scrapes YouTube exclusively.  The shared layer
(`scrape_exchange/`) is already mostly platform-agnostic — the rate
limiter, creator queue, file management, exchange client, and
supervisor are reusable as-is.  However, a few modules use
YouTube-specific naming or hard-coded keys, and the three YouTube
tools each duplicate ~100 lines of identical startup/shutdown
boilerplate.

This design introduces the minimum abstractions needed so that adding
a new platform means:

1. Creating a `scrape_exchange/{platform}/` sub-package with
   platform-specific settings, rate limiter, client, and domain
   models.
2. Writing `tools/{prefix}_*.py` scraper tools that use the shared
   `ScraperRunner` for boilerplate and implement their own worker
   loop.

## Design principles

- **No abstract Scraper interface.** Platforms have fundamentally
  different workflows (YouTube has three-phase discover/scrape/upload;
  other platforms may combine phases or have entirely different
  flows).  Each tool owns its worker loop.
- **No shared domain model base.** A TikTok video and a YouTube
  video are different structures.  No `BaseVideo` or `BaseCreator`.
- **No platform registry or plugin system.** Adding a platform is
  creating files, not registering in a framework.
- **No shared HTTP client base.** Anti-bot requirements differ too
  much per platform.  `httpx` is the common dependency.
- **Session/cookie management stays platform-specific.** YouTube's
  `YouTubeCookieJar` and `AsyncYouTubeClient` remain in the YouTube
  sub-package; TikTok will have its own equivalent.

## 1. Package layout

### Current

```
scrape_exchange/
    settings.py, rate_limiter.py, channel_map.py,
    video_claim.py, ...
    youtube/
        settings.py, youtube_rate_limiter.py,
        youtube_video.py, ...
tools/
    yt_video_scrape.py, yt_rss_scrape.py,
    yt_channel_scrape.py
```

### Proposed

```
scrape_exchange/
    settings.py              # ScraperSettings (unchanged)
    rate_limiter.py          # RateLimiter (unchanged)
    creator_map.py           # renamed from channel_map.py
    content_claim.py         # renamed from video_claim.py
    scraper_runner.py        # NEW — shared tool scaffolding
    creator_queue.py         # unchanged, already generic
    file_management.py       # unchanged, already generic
    exchange_client.py       # unchanged
    scraper_supervisor.py    # unchanged
    datatypes.py             # unchanged (already has 11 platforms)
    ...
    youtube/                 # unchanged internally
    tiktok/                  # future
        __init__.py
        settings.py
        tiktok_rate_limiter.py
        tiktok_client.py
        tiktok_video.py
        tiktok_user.py
        ...
tools/
    yt_video_scrape.py       # refactored to use ScraperRunner
    yt_rss_scrape.py         # refactored to use ScraperRunner
    yt_channel_scrape.py     # refactored to use ScraperRunner
    tt_video_scrape.py       # future TikTok tools
    ...
```

Key decisions:

- One sub-package per platform under `scrape_exchange/`, each fully
  owning its client, rate limiter, session management, and domain
  models.
- One set of tools per platform with platform-prefixed names
  (`yt_`, `tt_`, `rd_`).
- The shared layer stays flat in `scrape_exchange/` — no new
  nesting.

## 2. Shared module changes

### 2.1 `creator_map.py` (renamed from `channel_map.py`)

- Rename `ChannelMap` → `CreatorMap`.
- Method signatures unchanged: `get`, `put`, `get_all`, `put_many`,
  `contains`, `size`.
- Parameter terminology: `channel_id` → `creator_id`,
  `handle` → `creator_handle`.
- `FileCreatorMap` — same rename, no logic change.
- `NullCreatorMap` — same rename, no logic change.
- `RedisCreatorMap` — new `platform: str` constructor parameter.
  Redis key becomes `{platform}:creator_map` instead of the
  hard-coded `yt:channel_map`.

### 2.2 `content_claim.py` (renamed from `video_claim.py`)

- Rename `VideoClaim` → `ContentClaim`.
- Method signatures unchanged: `acquire`, `release`,
  `cleanup_stale`.
- Parameter terminology: `video_id` → `content_id`.
- `FileContentClaim` — claim files become
  `.claim-{content_id}` (same pattern, renamed).
- `RedisContentClaim` — new `platform: str` constructor
  parameter.  Key becomes `{platform}:claim:{content_id}`
  instead of `video:claim:{video_id}`.

### 2.3 `YouTubeRateLimiter.acquire()` return type fix

Currently `YouTubeRateLimiter.acquire()` returns
`tuple[str, str | None]` (proxy, cookie_file), diverging from the
base `RateLimiter.acquire()` which returns `str | None`.

Fix: `acquire()` returns just the proxy (matching the base class
contract).  A separate `get_cookie_file(proxy: str) -> str | None`
method is added for callers that need the cookie path.

Call sites change from:

```python
proxy, cookie_file = await rate_limiter.acquire(call_type)
```

to:

```python
proxy = await rate_limiter.acquire(call_type)
cookie_file = rate_limiter.get_cookie_file(proxy)
```

This keeps the base class contract clean for platforms that have no
cookie management.

## 3. `ScraperRunner` — shared tool scaffolding

### 3.1 What `ScraperRunner` owns

The following boilerplate is currently duplicated across all three
YouTube tools (~100 lines each):

1. **Supervisor check & launch** — if `num_processes > 1`, call
   `run_supervisor()` then `sys.exit()`.
2. **Logging setup** — `configure_logging()`.
3. **Metrics server** — `start_http_server()` with try/except
   fallback, plus `publish_config_metrics()`.
4. **Proxy list logging** — metrics_port, proxies_count,
   first_proxy, last_proxy.
5. **Rate limiter init** — call the platform's rate limiter
   `.get()` and `.set_proxies()`, plus
   `ScrapeExchangeRateLimiter.get()`.
6. **ExchangeClient setup** — `ExchangeClient.setup()` with
   error handling (configurable: mandatory vs optional).
7. **Signal handlers** — SIGINT/SIGTERM → cancel main task.
8. **Graceful drain** — `client.drain_uploads(timeout=...)` in a
   finally block.

### 3.2 What `ScraperRunner` does NOT own

- The worker loop itself (queue-based, priority-queue,
  watch-based — too different across tools).
- Domain model construction.
- Upload payload construction (differs per tool/entity).
- Platform-specific resource setup (e.g. channel scraper's
  `RLIMIT_NOFILE` bump).

### 3.3 Interface

```python
@dataclass
class ScraperRunContext:
    '''Passed to the worker function by ScraperRunner.'''
    settings: ScraperSettings
    client: ExchangeClient | None
    rate_limiter: RateLimiter
    proxies: list[str]


class ScraperRunner:
    def __init__(
        self,
        settings: ScraperSettings,
        scraper_label: str,
        platform: str,
        num_processes: int,
        concurrency: int,
        metrics_port: int,
        log_file: str,
        log_level: str,
        rate_limiter_factory: Callable[
            [ScraperSettings], RateLimiter
        ],
        client_required: bool = True,
    ) -> None: ...

    async def run(
        self,
        worker_func: Callable[
            [ScraperRunContext], Coroutine
        ],
    ) -> None:
        '''
        Supervisor check → logging → metrics →
        rate limiter → client → signals → call
        worker_func(context) → drain on exit.
        '''

    def run_sync(
        self,
        worker_func: Callable[
            [ScraperRunContext], Coroutine
        ],
    ) -> int:
        '''
        Entry point for main().  Runs the supervisor
        check synchronously (before entering asyncio),
        then wraps asyncio.run() around self.run().
        Returns exit code (0 on success, 1 on failure).
        '''
```

### 3.4 Tool usage example

A YouTube tool's `main()` becomes:

```python
def main() -> None:
    settings = VideoSettings()

    if settings.video_upload_only:
        settings.video_num_processes = 1
        settings.video_metrics_port -= 1

    runner = ScraperRunner(
        settings=settings,
        scraper_label='video',
        platform='youtube',
        num_processes=settings.video_num_processes,
        concurrency=settings.video_concurrency,
        metrics_port=settings.video_metrics_port,
        log_file=settings.video_log_file,
        log_level=settings.video_log_level,
        rate_limiter_factory=lambda s: (
            YouTubeRateLimiter.get(
                state_dir=s.rate_limiter_state_dir,
                redis_dsn=s.redis_dsn,
            )
        ),
    )
    sys.exit(runner.run_sync(worker_loop))
```

The tool still defines its own `worker_loop(ctx: ScraperRunContext)`
with whatever workflow it needs.

Platform-specific pre-flight (API key validation,
`RLIMIT_NOFILE`, upload-only port adjustment) happens in `main()`
before constructing the runner, or at the start of `worker_func`.

## 4. Platform sub-package contract

Each platform sub-package is a convention, not an enforced
interface.  A platform provides:

| Component | Convention | Required? |
|-----------|-----------|-----------|
| `settings.py` | `{Platform}ScraperSettings(ScraperSettings)` | Yes |
| `{platform}_rate_limiter.py` | `{Platform}RateLimiter(RateLimiter[CallType])` with per-endpoint bucket configs | Yes |
| Domain models | Platform-specific data classes | Yes |
| HTTP client | Platform-specific, if needed | Optional |
| Session/cookie management | Platform-specific, if needed | Optional |

### What stays shared across all platforms

| Module | Purpose |
|--------|---------|
| `ScraperSettings` | Base config (exchange_url, API keys, proxies, redis_dsn, logging) |
| `RateLimiter` + backends | Token-bucket rate limiting (in-process / file / Redis) |
| `ScraperRunner` | Supervisor, metrics, signals, client setup, drain |
| `ExchangeClient` | Upload to scrape.exchange API |
| `AssetFileManagement` | On-disk file lifecycle |
| `CreatorMap` | Creator ID → handle mappings |
| `ContentClaim` | Cross-process content dedup locks |
| `CreatorQueue` | Priority queue for discovery |
| `scraper_supervisor` | Multi-process proxy splitting |
| `datatypes` | Platform / PlatformEntityType / IngestStatus enums |

## 5. Migration and backward compatibility

### 5.1 Redis key migration

| Old key | New key | Strategy |
|---------|---------|----------|
| `yt:channel_map` | `youtube:creator_map` | One-time `RENAME` command. If key doesn't exist, no-op. |
| `video:claim:{id}` | `youtube:claim:{id}` | No action needed — claims have 1-hour TTL and expire naturally. |

`CreatorQueue` keys already use `rss:{platform}:queue:*` format
and are unaffected.

### 5.2 Python import paths

All internal callers update in the same change:

- `from scrape_exchange.channel_map import ...` →
  `from scrape_exchange.creator_map import ...`
- `from scrape_exchange.video_claim import ...` →
  `from scrape_exchange.content_claim import ...`

No backward-compat re-exports.  This is an internal codebase.

### 5.3 YouTube tool refactoring

The three `yt_*.py` tools get refactored to use `ScraperRunner`
but their external behavior is unchanged: same CLI arguments,
same env vars, same file output, same Prometheus metric names.

### 5.4 Rollout order

1. Rename modules + fix Redis key namespacing + fix `acquire()`
   return type.
2. Introduce `ScraperRunner`, refactor YouTube tools to use it.
3. (Future) Add `scrape_exchange/tiktok/` and `tools/tt_*.py`.

Steps 1 and 2 can be one PR or two.  Step 3 is a separate effort.

## 6. Intentional non-goals

1. No abstract `Scraper` interface — platforms have different
   workflows.
2. No shared domain model base — platform entities have different
   fields.
3. No platform registry or plugin discovery — adding a platform
   is creating files.
4. No shared HTTP client base — anti-bot requirements differ too
   much.
5. No cross-platform tool (`scrape.py --platform X`) — each
   platform gets its own tools.
6. No changes to `AssetFileManagement` — `prefix_rankings`
   parameter already supports custom prefixes.
7. No changes to `CreatorQueue` — already platform-parameterized.
8. No changes to `ExchangeClient` — already fully generic.
