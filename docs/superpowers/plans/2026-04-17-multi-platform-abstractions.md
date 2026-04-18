# Multi-Platform Abstractions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prepare the `scrape_exchange` shared layer for multiple
social media platforms by renaming YouTube-specific modules,
fixing the rate limiter return-type mismatch, and extracting
shared tool scaffolding into a `ScraperRunner`.

**Architecture:** Rename `channel_map` → `creator_map` and
`video_claim` → `content_claim` with platform-parameterized Redis
keys.  Fix `YouTubeRateLimiter.acquire()` to match the base class
contract (return proxy only, not a tuple).  Introduce
`ScraperRunner` to own the duplicated supervisor/metrics/signal/
drain boilerplate from all three YouTube tools.

**Tech Stack:** Python 3.12, pydantic-settings, asyncio,
prometheus_client, Redis, unittest, fakeredis

**Spec:** `docs/superpowers/specs/2026-04-17-multi-platform-abstractions-design.md`

---

## File map

### New files

| File | Responsibility |
|------|----------------|
| `scrape_exchange/creator_map.py` | Renamed from `channel_map.py` — `CreatorMap` ABC + File/Redis/Null backends |
| `scrape_exchange/content_claim.py` | Renamed from `video_claim.py` — `ContentClaim` ABC + File/Redis/Null backends |
| `scrape_exchange/scraper_runner.py` | `ScraperRunner` + `ScraperRunContext` — shared tool scaffolding |
| `tests/unit/test_creator_map.py` | Renamed from `test_channel_map.py` |
| `tests/unit/test_content_claim.py` | Renamed from `test_video_claim.py` |
| `tests/unit/test_scraper_runner.py` | Tests for `ScraperRunner` |

### Deleted files

| File | Reason |
|------|--------|
| `scrape_exchange/channel_map.py` | Renamed to `creator_map.py` |
| `scrape_exchange/video_claim.py` | Renamed to `content_claim.py` |
| `tests/unit/test_channel_map.py` | Renamed to `test_creator_map.py` |
| `tests/unit/test_video_claim.py` | Renamed to `test_content_claim.py` |

### Modified files

| File | Change |
|------|--------|
| `scrape_exchange/youtube/youtube_rate_limiter.py` | `acquire()` returns `str \| None` instead of tuple; `get_cookie_file()` becomes sync |
| `scrape_exchange/youtube/youtube_video.py:991` | Unpack `acquire()` → use separate `get_cookie_file()` |
| `scrape_exchange/youtube/youtube_client.py:172` | `acquire()` return value no longer a tuple (already discarded) |
| `scrape_exchange/youtube/youtube_channel_tabs.py:522` | Same — return value already discarded |
| `scrape_exchange/youtube/youtube_video_innertube.py:299` | Same — return value already discarded |
| `tools/yt_video_scrape.py` | Update imports (video_claim → content_claim); refactor `main()`/`_run_worker()` to use `ScraperRunner` |
| `tools/yt_rss_scrape.py` | Update imports (channel_map → creator_map); refactor `main()`/`_run_worker()` to use `ScraperRunner` |
| `tools/yt_channel_scrape.py` | Update imports (channel_map → creator_map); refactor `main()`/`_run_worker()` to use `ScraperRunner` |
| `tools/export_channel_map.py` | Update imports (channel_map → creator_map) |
| `tests/unit/test_youtube_rate_limiter.py` | Update `acquire()` assertions (no longer tuple) |
| `tests/integration/test_youtube_rate_limiter_integration.py` | Update `acquire()` assertions (no longer tuple) |

---

## Task 1: Rename `channel_map` → `creator_map`

**Files:**
- Create: `scrape_exchange/creator_map.py`
- Create: `tests/unit/test_creator_map.py`
- Delete: `scrape_exchange/channel_map.py`
- Delete: `tests/unit/test_channel_map.py`
- Modify: `tools/yt_rss_scrape.py`
- Modify: `tools/yt_channel_scrape.py`
- Modify: `tools/export_channel_map.py`

This task uses `git mv` to preserve history, then applies the
renames via search-and-replace.

- [ ] **Step 1: Rename the module file**

```bash
git mv scrape_exchange/channel_map.py \
       scrape_exchange/creator_map.py
```

- [ ] **Step 2: Rename the test file**

```bash
git mv tests/unit/test_channel_map.py \
       tests/unit/test_creator_map.py
```

- [ ] **Step 3: Update `scrape_exchange/creator_map.py`**

Apply these renames throughout the file:

| Old | New |
|-----|-----|
| `channel_id to channel_handle` (docstring) | `creator_id to creator_handle` |
| `ChannelMap` | `CreatorMap` |
| `FileChannelMap` | `FileCreatorMap` |
| `RedisChannelMap` | `RedisCreatorMap` |
| `NullChannelMap` | `NullCreatorMap` |
| `_REDIS_KEY: str = 'yt:channel_map'` | remove module-level constant |
| `channel_id` parameter names | `creator_id` |
| `handle` parameter names | `creator_handle` |
| `channel map` in docstrings/comments | `creator map` |
| `channel scraper` in docstrings | `creator scraper` |

For `RedisCreatorMap`, add a `platform: str` constructor parameter
and compute the key dynamically:

```python
class RedisCreatorMap(CreatorMap):
    '''
    Redis hash-backed creator map.
    Key ``{platform}:creator_map`` with
    field=creator_id, value=creator_handle.
    '''

    def __init__(
        self, redis_dsn: str, platform: str,
    ) -> None:
        import redis.asyncio as aioredis
        self._redis: aioredis.Redis = (
            aioredis.from_url(
                redis_dsn, decode_responses=True,
            )
        )
        self._key: str = f'{platform}:creator_map'
```

- [ ] **Step 4: Update `tests/unit/test_creator_map.py`**

Apply the same class/parameter renames as step 3. Update the
import to:

```python
from scrape_exchange.creator_map import (
    FileCreatorMap,
    NullCreatorMap,
    RedisCreatorMap,
)
```

Update test class names:
- `TestFileChannelMap` → `TestFileCreatorMap`
- `TestRedisChannelMap` → `TestRedisCreatorMap`
- `TestNullChannelMap` → `TestNullCreatorMap`
- `TestExportImportRoundTrip` — keep name, update class
  references

In `TestRedisCreatorMap.asyncSetUp`, pass `platform='youtube'` and
update the key assertion:

```python
    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(
                decode_responses=True,
            )
        )
        self.cm: RedisCreatorMap = (
            RedisCreatorMap.__new__(RedisCreatorMap)
        )
        self.cm._redis = self.redis
        self.cm._key = 'youtube:creator_map'
```

In `TestExportImportRoundTrip.asyncSetUp`, same key update:

```python
        self.redis_cm._key = 'youtube:creator_map'
```

- [ ] **Step 5: Update callers**

In `tools/yt_rss_scrape.py`, change:

```python
from scrape_exchange.channel_map import (
    ChannelMap,
    FileChannelMap,
    RedisChannelMap,
)
```

to:

```python
from scrape_exchange.creator_map import (
    CreatorMap,
    FileCreatorMap,
    RedisCreatorMap,
)
```

Then rename all usage: `ChannelMap` → `CreatorMap`,
`FileChannelMap` → `FileCreatorMap`,
`RedisChannelMap` → `RedisCreatorMap`,
`channel_map_backend` → `creator_map_backend` (variable name).
Add `platform='youtube'` to `RedisCreatorMap(...)` constructor
calls.

Apply the same pattern to `tools/yt_channel_scrape.py` and
`tools/export_channel_map.py`.

- [ ] **Step 6: Run tests**

```bash
python -m pytest tests/unit/test_creator_map.py -v
```

Expected: all tests pass.

- [ ] **Step 7: Add Redis key migration note**

The production Redis instance has the key `yt:channel_map`
which must be renamed to `youtube:creator_map`.  This is a
one-time operation to run on the production Redis before or
after deploying the code change:

```bash
redis-cli -h mongo.scrape.exchange RENAME \
    yt:channel_map youtube:creator_map
```

If the key doesn't exist (fresh install), this is a no-op
error that can be safely ignored.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "Rename channel_map to creator_map with platform-parameterized Redis key"
```

---

## Task 2: Rename `video_claim` → `content_claim`

**Files:**
- Create: `scrape_exchange/content_claim.py`
- Create: `tests/unit/test_content_claim.py`
- Delete: `scrape_exchange/video_claim.py`
- Delete: `tests/unit/test_video_claim.py`
- Modify: `tools/yt_video_scrape.py`

Same approach as Task 1: `git mv` then search-and-replace.

- [ ] **Step 1: Rename files**

```bash
git mv scrape_exchange/video_claim.py \
       scrape_exchange/content_claim.py
git mv tests/unit/test_video_claim.py \
       tests/unit/test_content_claim.py
```

- [ ] **Step 2: Update `scrape_exchange/content_claim.py`**

Renames throughout:

| Old | New |
|-----|-----|
| `VideoClaim` | `ContentClaim` |
| `FileVideoClaim` | `FileContentClaim` |
| `RedisVideoClaim` | `RedisContentClaim` |
| `NullVideoClaim` | `NullContentClaim` |
| `video_id` parameter | `content_id` |
| `video scraping` / `video ID` in docstrings | `content scraping` / `content ID` |
| `_KEY_PREFIX: str = 'video:claim:'` | remove, compute dynamically |

For `RedisContentClaim`, add `platform: str`:

```python
class RedisContentClaim(ContentClaim):
    '''
    Redis-backed claim lock using ``SET NX EX``.

    Claims are stored as keys
    ``{platform}:claim:{content_id}`` with a TTL
    that acts as automatic stale-lock cleanup.

    :param redis_dsn: Redis connection string.
    :param platform: Platform name for key
        namespacing.
    :param ttl: Seconds for the claim key TTL.
    '''

    def __init__(
        self,
        redis_dsn: str,
        platform: str,
        ttl: int = DEFAULT_CLAIM_TTL,
    ) -> None:
        try:
            import redis.asyncio as aioredis
        except ImportError:
            raise ImportError(
                'redis package is required for '
                'RedisContentClaim. Install with: '
                'pip install "redis[hiredis]>=5.0.0"'
            ) from None
        self._redis: aioredis.Redis = (
            aioredis.from_url(
                redis_dsn, decode_responses=True,
            )
        )
        self._ttl: int = ttl
        self._pid: str = str(os.getpid())
        self._key_prefix: str = (
            f'{platform}:claim:'
        )

    def _key(self, content_id: str) -> str:
        return f'{self._key_prefix}{content_id}'
```

- [ ] **Step 3: Update `tests/unit/test_content_claim.py`**

Same class/parameter renames.  Update import:

```python
from scrape_exchange.content_claim import (
    FileContentClaim,
    NullContentClaim,
    RedisContentClaim,
    ContentClaim,
)
```

Update test class names:
- `TestFileVideoClaimAcquire` → `TestFileContentClaimAcquire`
- `TestFileVideoClaimCleanup` → `TestFileContentClaimCleanup`
- `TestRedisVideoClaimAcquire` → `TestRedisContentClaimAcquire`
- `TestRedisVideoClaimRelease` → `TestRedisContentClaimRelease`
- `TestRedisVideoClaimCleanup` → `TestRedisContentClaimCleanup`
- `TestNullVideoClaim` → `TestNullContentClaim`
- `TestVideoClaimAbstractBase` → `TestContentClaimAbstractBase`

In Redis claim tests, update the `RedisContentClaim` constructor
to pass `platform='youtube'`, and update key assertions:

```python
self._claim: RedisContentClaim = RedisContentClaim(
    'redis://localhost:6379/0', platform='youtube',
    ttl=600,
)
```

Key assertions change from `'video:claim:vid001'` to
`'youtube:claim:vid001'`, and scan pattern from
`'video:claim:*'` to `'youtube:claim:*'`.

- [ ] **Step 4: Update `tools/yt_video_scrape.py` imports**

Change:

```python
from scrape_exchange.video_claim import (
    FileVideoClaim,
    NullVideoClaim,
    RedisVideoClaim,
    VideoClaim,
)
```

to:

```python
from scrape_exchange.content_claim import (
    FileContentClaim,
    NullContentClaim,
    RedisContentClaim,
    ContentClaim,
)
```

Then rename all usage in the file.  Add `platform='youtube'` to
`RedisContentClaim(...)` constructor call.

- [ ] **Step 5: Run tests**

```bash
python -m pytest tests/unit/test_content_claim.py -v
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "Rename video_claim to content_claim with platform-parameterized Redis key"
```

---

## Task 3: Fix `YouTubeRateLimiter.acquire()` return type

**Files:**
- Modify: `scrape_exchange/youtube/youtube_rate_limiter.py:168-193`
- Modify: `scrape_exchange/youtube/youtube_video.py:991-996`
- Modify: `tests/unit/test_youtube_rate_limiter.py`
- Modify: `tests/integration/test_youtube_rate_limiter_integration.py`

The base `RateLimiter.acquire()` returns `str | None`.
`YouTubeRateLimiter.acquire()` currently returns
`tuple[str | None, str | None]`.  This task aligns them.

- [ ] **Step 1: Update `YouTubeRateLimiter.acquire()`**

In `scrape_exchange/youtube/youtube_rate_limiter.py`, replace the
`acquire` method (lines 168-193) with:

```python
    async def acquire(
        self,
        call_type: YouTubeCallType,
        proxy: str | None = None,
    ) -> str | None:
        '''
        Wait until a request of *call_type* is
        permitted, then return the selected proxy.

        Use :meth:`get_cookie_file_cached` to obtain
        the cookie path for the returned proxy.
        '''
        return await super().acquire(
            call_type, proxy=proxy,
        )

    def get_cookie_file_cached(
        self, proxy: str | None,
    ) -> str | None:
        '''
        Return the cached cookie file path for
        *proxy* without triggering network
        acquisition.

        This is the synchronous, non-blocking
        companion to :meth:`acquire`.  Call it
        immediately after ``acquire()`` returns
        when you need the cookie path for yt-dlp's
        ``--cookiefile`` flag.

        :returns: Filesystem path to the temp
            cookie file, or ``None`` if no valid
            cached entry exists for the proxy.
        '''
        from .youtube_cookiejar import YouTubeCookieJar
        entry = (
            YouTubeCookieJar.get()._entries.get(proxy)
        )
        if entry is not None and not entry.is_expired():
            return entry.path
        return None
```

Note: the existing async `get_cookie_file()` method (lines
203-215) remains unchanged — it triggers network acquisition and
is used by `warm_cookie_jar`.  The new
`get_cookie_file_cached()` is the sync, non-blocking variant
that replaces the inline cookie lookup that was in `acquire()`.

- [ ] **Step 2: Update the call site in `youtube_video.py`**

In `scrape_exchange/youtube/youtube_video.py`, change line 991
onwards from:

```python
        proxy, cookie_file = await YouTubeRateLimiter.get().acquire(
            YouTubeCallType.PLAYER, proxy=proxy,
        )
```

to:

```python
        limiter: YouTubeRateLimiter = (
            YouTubeRateLimiter.get()
        )
        proxy = await limiter.acquire(
            YouTubeCallType.PLAYER, proxy=proxy,
        )
        cookie_file: str | None = (
            limiter.get_cookie_file_cached(proxy)
        )
```

The lines after (994-996) that use `proxy` and `cookie_file`
remain unchanged.

- [ ] **Step 3: Update unit tests**

In `tests/unit/test_youtube_rate_limiter.py`, all `acquire()`
calls that unpack a tuple need updating.

Change every `result_proxy, _ = await limiter.acquire(...)` to
`result_proxy = await limiter.acquire(...)`.

Change the tuple assertion test (around line 275):

From:

```python
    '''acquire() must return a (proxy, cookie_file) two-tuple.'''
    result = await limiter.acquire(YouTubeCallType.RSS, proxy=PROXIES[0])
```

To:

```python
    '''acquire() must return the proxy string.'''
    result = await limiter.acquire(
        YouTubeCallType.RSS, proxy=PROXIES[0],
    )
    self.assertIsInstance(result, str)
```

Remove any `self.assertIsInstance(result, tuple)` or
`self.assertEqual(len(result), 2)` assertions.

- [ ] **Step 4: Update integration tests**

In `tests/integration/test_youtube_rate_limiter_integration.py`,
apply the same pattern:

- `result_proxy, _ = await limiter.acquire(...)` →
  `result_proxy = await limiter.acquire(...)`
- `result_proxy, cookie_file = await limiter.acquire(...)` →
  `result_proxy = await limiter.acquire(...)` then
  `cookie_file = limiter.get_cookie_file_cached(result_proxy)`
- Remove tuple-shape assertions; replace with
  `self.assertIsInstance(result, str)`.

- [ ] **Step 5: Run all rate limiter tests**

```bash
python -m pytest tests/unit/test_youtube_rate_limiter.py \
                 tests/integration/test_youtube_rate_limiter_integration.py -v
```

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "Fix YouTubeRateLimiter.acquire() to return proxy only, matching base class contract"
```

---

## Task 4: Create `ScraperRunner`

**Files:**
- Create: `scrape_exchange/scraper_runner.py`
- Create: `tests/unit/test_scraper_runner.py`

- [ ] **Step 1: Write the test file**

Create `tests/unit/test_scraper_runner.py`:

```python
'''Unit tests for scrape_exchange.scraper_runner.'''

import asyncio
import signal
import sys
import unittest
from unittest.mock import (
    AsyncMock,
    MagicMock,
    patch,
)

from scrape_exchange.scraper_runner import (
    ScraperRunContext,
    ScraperRunner,
)


def _make_settings_mock(
    proxies: str = 'http://p1:8080,http://p2:8080',
    api_key_id: str = 'key_id',
    api_key_secret: str = 'key_secret',
    exchange_url: str = 'https://scrape.exchange',
    rate_limiter_state_dir: str = '/tmp/rl',
    redis_dsn: str | None = None,
    log_format: str = 'json',
) -> MagicMock:
    s = MagicMock()
    s.proxies = proxies
    s.api_key_id = api_key_id
    s.api_key_secret = api_key_secret
    s.exchange_url = exchange_url
    s.rate_limiter_state_dir = rate_limiter_state_dir
    s.redis_dsn = redis_dsn
    s.log_format = log_format
    return s


def _make_rate_limiter_mock() -> MagicMock:
    rl = MagicMock()
    rl.set_proxies = MagicMock()
    return rl


class TestScraperRunContext(unittest.TestCase):
    '''Tests for ScraperRunContext dataclass.'''

    def test_fields(self) -> None:
        ctx = ScraperRunContext(
            settings=MagicMock(),
            client=None,
            rate_limiter=MagicMock(),
            proxies=['http://p1:8080'],
        )
        self.assertIsNone(ctx.client)
        self.assertEqual(
            ctx.proxies, ['http://p1:8080'],
        )


class TestScraperRunnerInit(unittest.TestCase):
    '''Tests for ScraperRunner construction.'''

    def test_stores_parameters(self) -> None:
        settings = _make_settings_mock()
        rl_factory = MagicMock(
            return_value=_make_rate_limiter_mock(),
        )
        runner = ScraperRunner(
            settings=settings,
            scraper_label='video',
            platform='youtube',
            num_processes=2,
            concurrency=3,
            metrics_port=9400,
            log_file='/dev/stdout',
            log_level='INFO',
            rate_limiter_factory=rl_factory,
        )
        self.assertEqual(runner._scraper_label, 'video')
        self.assertEqual(runner._platform, 'youtube')
        self.assertEqual(runner._num_processes, 2)


class TestScraperRunnerSupervisor(
    unittest.TestCase,
):
    '''Test that run_sync delegates to supervisor
    when num_processes > 1.'''

    @patch(
        'scrape_exchange.scraper_runner.run_supervisor',
    )
    def test_supervisor_dispatched(
        self, mock_supervisor: MagicMock,
    ) -> None:
        mock_supervisor.return_value = 0
        settings = _make_settings_mock()
        runner = ScraperRunner(
            settings=settings,
            scraper_label='video',
            platform='youtube',
            num_processes=4,
            concurrency=3,
            metrics_port=9400,
            log_file='/dev/stdout',
            log_level='INFO',
            rate_limiter_factory=MagicMock(),
        )
        worker_fn = AsyncMock()
        code: int = runner.run_sync(worker_fn)
        self.assertEqual(code, 0)
        mock_supervisor.assert_called_once()
        worker_fn.assert_not_called()


class TestScraperRunnerWorker(
    unittest.IsolatedAsyncioTestCase,
):
    '''Test the worker path (num_processes == 1).'''

    @patch(
        'scrape_exchange.scraper_runner'
        '.start_http_server',
    )
    @patch(
        'scrape_exchange.scraper_runner'
        '.publish_config_metrics',
    )
    @patch(
        'scrape_exchange.scraper_runner'
        '.ScrapeExchangeRateLimiter',
    )
    @patch(
        'scrape_exchange.scraper_runner'
        '.ExchangeClient',
    )
    @patch(
        'scrape_exchange.scraper_runner'
        '.configure_logging',
    )
    async def test_worker_func_called_with_context(
        self,
        mock_logging: MagicMock,
        mock_client_cls: MagicMock,
        mock_se_rl: MagicMock,
        mock_publish: MagicMock,
        mock_http_server: MagicMock,
    ) -> None:
        mock_client = AsyncMock()
        mock_client.drain_uploads = AsyncMock()
        mock_client_cls.setup = AsyncMock(
            return_value=mock_client,
        )

        settings = _make_settings_mock()
        rl = _make_rate_limiter_mock()

        worker_fn = AsyncMock()

        runner = ScraperRunner(
            settings=settings,
            scraper_label='video',
            platform='youtube',
            num_processes=1,
            concurrency=3,
            metrics_port=9400,
            log_file='/dev/stdout',
            log_level='INFO',
            rate_limiter_factory=lambda s: rl,
        )
        await runner.run(worker_fn)

        worker_fn.assert_awaited_once()
        ctx: ScraperRunContext = (
            worker_fn.call_args[0][0]
        )
        self.assertIs(ctx.settings, settings)
        self.assertIs(ctx.client, mock_client)
        self.assertIs(ctx.rate_limiter, rl)
        rl.set_proxies.assert_called_once_with(
            settings.proxies,
        )

    @patch(
        'scrape_exchange.scraper_runner'
        '.start_http_server',
    )
    @patch(
        'scrape_exchange.scraper_runner'
        '.publish_config_metrics',
    )
    @patch(
        'scrape_exchange.scraper_runner'
        '.ScrapeExchangeRateLimiter',
    )
    @patch(
        'scrape_exchange.scraper_runner'
        '.ExchangeClient',
    )
    @patch(
        'scrape_exchange.scraper_runner'
        '.configure_logging',
    )
    async def test_drain_called_on_exit(
        self,
        mock_logging: MagicMock,
        mock_client_cls: MagicMock,
        mock_se_rl: MagicMock,
        mock_publish: MagicMock,
        mock_http_server: MagicMock,
    ) -> None:
        mock_client = AsyncMock()
        mock_client.drain_uploads = AsyncMock()
        mock_client_cls.setup = AsyncMock(
            return_value=mock_client,
        )

        settings = _make_settings_mock()
        rl = _make_rate_limiter_mock()

        async def worker_fn(
            ctx: ScraperRunContext,
        ) -> None:
            pass

        runner = ScraperRunner(
            settings=settings,
            scraper_label='video',
            platform='youtube',
            num_processes=1,
            concurrency=3,
            metrics_port=9400,
            log_file='/dev/stdout',
            log_level='INFO',
            rate_limiter_factory=lambda s: rl,
        )
        await runner.run(worker_fn)

        mock_client.drain_uploads.assert_awaited_once()

    @patch(
        'scrape_exchange.scraper_runner'
        '.start_http_server',
    )
    @patch(
        'scrape_exchange.scraper_runner'
        '.publish_config_metrics',
    )
    @patch(
        'scrape_exchange.scraper_runner'
        '.ScrapeExchangeRateLimiter',
    )
    @patch(
        'scrape_exchange.scraper_runner'
        '.ExchangeClient',
    )
    @patch(
        'scrape_exchange.scraper_runner'
        '.configure_logging',
    )
    async def test_client_optional(
        self,
        mock_logging: MagicMock,
        mock_client_cls: MagicMock,
        mock_se_rl: MagicMock,
        mock_publish: MagicMock,
        mock_http_server: MagicMock,
    ) -> None:
        mock_client_cls.setup = AsyncMock(
            side_effect=Exception('no server'),
        )
        settings = _make_settings_mock()
        rl = _make_rate_limiter_mock()
        worker_fn = AsyncMock()

        runner = ScraperRunner(
            settings=settings,
            scraper_label='video',
            platform='youtube',
            num_processes=1,
            concurrency=3,
            metrics_port=9400,
            log_file='/dev/stdout',
            log_level='INFO',
            rate_limiter_factory=lambda s: rl,
            client_required=False,
        )
        await runner.run(worker_fn)

        worker_fn.assert_awaited_once()
        ctx: ScraperRunContext = (
            worker_fn.call_args[0][0]
        )
        self.assertIsNone(ctx.client)


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: Run tests — expect failures**

```bash
python -m pytest tests/unit/test_scraper_runner.py -v
```

Expected: ImportError — `scraper_runner` does not exist yet.

- [ ] **Step 3: Create `scrape_exchange/scraper_runner.py`**

```python
'''
Shared scaffolding for scraper tools.

:class:`ScraperRunner` consolidates the startup and
shutdown boilerplate that every scraper tool repeats:
supervisor check, logging, metrics, rate-limiter init,
ExchangeClient setup, signal handlers, and graceful
drain.

The tool's own worker logic is passed as an async
callable that receives a :class:`ScraperRunContext`.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import logging
import signal
import sys

from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from prometheus_client import start_http_server

from scrape_exchange.exchange_client import (
    ExchangeClient,
)
from scrape_exchange.logging import configure_logging
from scrape_exchange.rate_limiter import RateLimiter
from scrape_exchange.scrape_exchange_rate_limiter import (
    ScrapeExchangeRateLimiter,
)
from scrape_exchange.scraper_supervisor import (
    SupervisorConfig,
    publish_config_metrics,
    run_supervisor,
)
from scrape_exchange.settings import ScraperSettings


@dataclass
class ScraperRunContext:
    '''Passed to the worker function by
    ScraperRunner.'''
    settings: ScraperSettings
    client: ExchangeClient | None
    rate_limiter: RateLimiter
    proxies: list[str]


class ScraperRunner:
    '''
    Shared startup/shutdown scaffolding for scraper
    tools.

    Owns: supervisor dispatch, logging, metrics,
    rate-limiter init, ExchangeClient setup, signal
    handlers, graceful drain.

    Does NOT own: the worker loop, domain models,
    upload payloads, platform-specific setup.
    '''

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
    ) -> None:
        self._settings: ScraperSettings = settings
        self._scraper_label: str = scraper_label
        self._platform: str = platform
        self._num_processes: int = num_processes
        self._concurrency: int = concurrency
        self._metrics_port: int = metrics_port
        self._log_file: str = log_file
        self._log_level: str = log_level
        self._rl_factory: Callable[
            [ScraperSettings], RateLimiter
        ] = rate_limiter_factory
        self._client_required: bool = client_required

    def run_sync(
        self,
        worker_func: Callable[
            ['ScraperRunContext'],
            Awaitable[None],
        ],
    ) -> int:
        '''
        Synchronous entry point for ``main()``.

        Runs the supervisor check before entering
        asyncio.  If ``num_processes > 1``, dispatches
        to the supervisor and returns its exit code.
        Otherwise enters ``asyncio.run(self.run(...))``.
        '''
        configure_logging(
            level=self._log_level,
            filename=self._log_file,
            log_format=self._settings.log_format,
        )

        if self._num_processes > 1:
            return run_supervisor(SupervisorConfig(
                scraper_label=self._scraper_label,
                num_processes_env_var=(
                    f'{self._scraper_label.upper()}'
                    '_NUM_PROCESSES'
                ),
                log_file_env_var=(
                    f'{self._scraper_label.upper()}'
                    '_LOG_FILE'
                ),
                num_processes=self._num_processes,
                concurrency=self._concurrency,
                proxies=self._settings.proxies,
                metrics_port=self._metrics_port,
                log_file=self._log_file or None,
                api_key_id=self._settings.api_key_id,
                api_key_secret=(
                    self._settings.api_key_secret
                ),
                exchange_url=(
                    self._settings.exchange_url
                ),
            ))

        try:
            asyncio.run(self.run(worker_func))
        except asyncio.CancelledError:
            logging.info(
                '%s scraper shutdown complete',
                self._scraper_label,
            )
        return 0

    async def run(
        self,
        worker_func: Callable[
            ['ScraperRunContext'],
            Awaitable[None],
        ],
    ) -> None:
        '''
        Async entry point.  Sets up metrics, rate
        limiter, client, signal handlers, then calls
        ``worker_func(context)``.  Drains uploads on
        exit.
        '''
        proxies: list[str] = [
            p.strip()
            for p in self._settings.proxies.split(',')
            if p.strip()
        ] if self._settings.proxies else []

        logging.info(
            'Scraper worker started',
            extra={
                'scraper': self._scraper_label,
                'platform': self._platform,
                'metrics_port': self._metrics_port,
                'proxies_count': len(proxies),
                'first_proxy': (
                    proxies[0] if proxies else None
                ),
                'last_proxy': (
                    proxies[-1] if proxies else None
                ),
            },
        )

        try:
            start_http_server(self._metrics_port)
            logging.info(
                'Prometheus metrics available',
                extra={
                    'metrics_port': self._metrics_port,
                },
            )
        except OSError as exc:
            logging.warning(
                'Failed to bind Prometheus metrics '
                'port; worker will run without metrics',
                exc=exc,
                extra={
                    'metrics_port': self._metrics_port,
                },
            )
        publish_config_metrics(
            role='worker',
            scraper_label=self._scraper_label,
            num_processes=1,
            concurrency=self._concurrency,
        )

        rate_limiter: RateLimiter = self._rl_factory(
            self._settings,
        )
        rate_limiter.set_proxies(
            self._settings.proxies,
        )

        post_rate: float = float(max(
            1,
            self._num_processes * self._concurrency,
        ))
        ScrapeExchangeRateLimiter.get(
            state_dir=(
                self._settings.rate_limiter_state_dir
            ),
            post_rate=post_rate,
            redis_dsn=self._settings.redis_dsn,
        )

        client: ExchangeClient | None = None
        try:
            client = await ExchangeClient.setup(
                api_key_id=self._settings.api_key_id,
                api_key_secret=(
                    self._settings.api_key_secret
                ),
                exchange_url=(
                    self._settings.exchange_url
                ),
            )
        except Exception as exc:
            if self._client_required:
                logging.critical(
                    'Failed to connect to '
                    'Scrape Exchange API',
                    exc=exc,
                    extra={
                        'exchange_url': (
                            self._settings.exchange_url
                        ),
                    },
                )
                logging.shutdown()
                sys.exit(1)
            logging.warning(
                'ExchangeClient setup failed; '
                'continuing without upload capability',
                exc=exc,
            )

        loop: asyncio.AbstractEventLoop = (
            asyncio.get_running_loop()
        )
        main_task: asyncio.Task[Any] = (
            asyncio.current_task()
        )
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(
                    sig, main_task.cancel,
                )
            except NotImplementedError:
                pass

        try:
            ctx: ScraperRunContext = ScraperRunContext(
                settings=self._settings,
                client=client,
                rate_limiter=rate_limiter,
                proxies=proxies,
            )
            await worker_func(ctx)
        except asyncio.CancelledError:
            logging.info(
                'Shutdown signal received; '
                'draining background uploads',
            )
            raise
        finally:
            if client is not None:
                await client.drain_uploads(
                    timeout=10.0,
                )
```

- [ ] **Step 4: Run tests**

```bash
python -m pytest tests/unit/test_scraper_runner.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add scrape_exchange/scraper_runner.py \
        tests/unit/test_scraper_runner.py
git commit -m "Add ScraperRunner for shared scraper tool scaffolding"
```

---

## Task 5: Refactor `yt_video_scrape.py` to use `ScraperRunner`

**Files:**
- Modify: `tools/yt_video_scrape.py`

The `main()` and `_run_worker()` functions get simplified.
`main()` constructs a `ScraperRunner` and calls `run_sync`.
`_run_worker()` becomes the `worker_func` that receives a
`ScraperRunContext`.

- [ ] **Step 1: Update imports**

Remove imports that `ScraperRunner` now owns:

```python
# Remove these:
from prometheus_client import start_http_server
from scrape_exchange.logging import configure_logging
from scrape_exchange.scraper_supervisor import (
    SupervisorConfig, publish_config_metrics,
    run_supervisor,
)
from scrape_exchange.scrape_exchange_rate_limiter import (
    ScrapeExchangeRateLimiter,
)
```

Add:

```python
from scrape_exchange.scraper_runner import (
    ScraperRunContext,
    ScraperRunner,
)
```

Keep the `YouTubeRateLimiter` import — the factory lambda needs
it.  Keep `signal` import if used elsewhere; remove if only used
in the now-deleted signal handler block.

- [ ] **Step 2: Rewrite `main()`**

Replace the existing `main()` function (lines 377-416) with:

```python
def main() -> None:
    '''
    Top-level entry point. Reads settings and
    dispatches to either the shared supervisor
    (when ``video_num_processes > 1``) or the
    in-process scraper worker.
    '''

    settings: VideoSettings = VideoSettings()
    os.makedirs(
        settings.ytdlp_cache_dir, exist_ok=True,
    )

    if settings.video_upload_only:
        settings.video_num_processes = 1
        settings.metrics_port = (
            settings.metrics_port - 1
        )

    runner: ScraperRunner = ScraperRunner(
        settings=settings,
        scraper_label='video',
        platform='youtube',
        num_processes=settings.video_num_processes,
        concurrency=settings.video_concurrency,
        metrics_port=settings.metrics_port,
        log_file=settings.video_log_file,
        log_level=settings.video_log_level,
        rate_limiter_factory=lambda s: (
            YouTubeRateLimiter.get(
                state_dir=s.rate_limiter_state_dir,
                redis_dsn=s.redis_dsn,
            )
        ),
        client_required=not settings.video_no_upload,
    )
    sys.exit(runner.run_sync(_run_worker))
```

- [ ] **Step 3: Rewrite `_run_worker()`**

Change the signature from `_run_worker(settings: VideoSettings)`
to `_run_worker(ctx: ScraperRunContext)` and remove all the
boilerplate that `ScraperRunner` now handles:

- Remove the proxy-list parsing and logging block
- Remove the `start_http_server` / `publish_config_metrics` block
- Remove the `YouTubeRateLimiter.get().set_proxies()` block
- Remove the `ScrapeExchangeRateLimiter.get()` block
- Remove the signal handler wiring block
- Remove the `try/except CancelledError` wrapper

The body of `_run_worker` keeps only the domain-specific setup
(AssetFileManagement, VideoClaim selection) and the call to
`worker_loop`, extracting `settings` from `ctx.settings`:

```python
async def _run_worker(
    ctx: ScraperRunContext,
) -> None:
    settings: VideoSettings = ctx.settings

    video_fm: AssetFileManagement = (
        AssetFileManagement(
            settings.video_data_directory,
        )
    )
    logging.info(
        'Starting YouTube video scrape tool',
        extra={
            'settings': (
                settings.model_dump_json(indent=2)
            ),
        },
    )

    claim: ContentClaim
    if settings.video_num_processes > 1:
        if settings.redis_dsn:
            claim = RedisContentClaim(
                settings.redis_dsn,
                platform='youtube',
            )
        else:
            claim = FileContentClaim(
                settings.video_data_directory,
            )
        await claim.cleanup_stale()
    else:
        claim = NullContentClaim()

    await worker_loop(settings, video_fm, claim)
```

Note: `worker_loop` still takes `settings` directly (not `ctx`),
since it also uses `ctx.client` implicitly via
`ExchangeClient`.  If `worker_loop` needs the `ExchangeClient`
reference, pass it explicitly.  Check the existing `worker_loop`
signature — if it currently sets up its own client, that code
also needs to be updated to receive the client from `ctx`.

- [ ] **Step 4: Verify the video scraper starts**

```bash
python tools/yt_video_scrape.py --help
```

Expected: same help output as before.

- [ ] **Step 5: Run existing tests**

```bash
python -m pytest tests/ -v -k "video" --timeout=30
```

Expected: all existing video-related tests pass.

- [ ] **Step 6: Commit**

```bash
git add tools/yt_video_scrape.py
git commit -m "Refactor yt_video_scrape to use ScraperRunner"
```

---

## Task 6: Refactor `yt_channel_scrape.py` to use `ScraperRunner`

**Files:**
- Modify: `tools/yt_channel_scrape.py`

Same pattern as Task 5.

- [ ] **Step 1: Update imports**

Same import changes as Task 5: remove supervisor/metrics/logging
imports, add `ScraperRunner`/`ScraperRunContext`.

- [ ] **Step 2: Rewrite `main()`**

```python
def main() -> None:
    settings: ChannelSettings = ChannelSettings()
    _validate_settings(settings)

    if settings.channel_upload_only:
        settings.channel_num_processes = 1
        settings.metrics_port = (
            settings.metrics_port - 1
        )

    runner: ScraperRunner = ScraperRunner(
        settings=settings,
        scraper_label='channel',
        platform='youtube',
        num_processes=(
            settings.channel_num_processes
        ),
        concurrency=settings.channel_concurrency,
        metrics_port=settings.metrics_port,
        log_file=settings.channel_log_file,
        log_level=settings.channel_log_level,
        rate_limiter_factory=lambda s: (
            YouTubeRateLimiter.get(
                state_dir=s.rate_limiter_state_dir,
                redis_dsn=s.redis_dsn,
            )
        ),
    )
    sys.exit(runner.run_sync(_run_worker))
```

Note: `_validate_settings()` runs before the runner since
it must happen before supervisor dispatch.  It uses `print()`
not `logging`, so logging need not be configured yet — the
runner handles that.

- [ ] **Step 3: Rewrite `_run_worker()`**

Change signature to `_run_worker(ctx: ScraperRunContext)`.
Keep: `RLIMIT_NOFILE` bump, `AssetFileManagement`, channel map
backend selection, no-proxies concurrency override.
Remove: metrics, proxy logging, rate limiter, client setup,
signal handlers, drain, CancelledError wrapper.

The `client` comes from `ctx.client`.  The try/finally drain
is handled by `ScraperRunner`.

```python
async def _run_worker(
    ctx: ScraperRunContext,
) -> None:
    settings: ChannelSettings = ctx.settings

    _soft, _hard = resource.getrlimit(
        resource.RLIMIT_NOFILE,
    )
    _target: int = (
        _hard
        if _hard != resource.RLIM_INFINITY
        else 1048576
    )
    resource.setrlimit(
        resource.RLIMIT_NOFILE, (_target, _hard),
    )

    logging.info(
        'Starting YouTube channel upload tool',
        extra={'settings': settings.model_dump()},
    )

    fm: AssetFileManagement = AssetFileManagement(
        settings.channel_data_directory,
    )

    creator_map_backend: CreatorMap
    if settings.redis_dsn:
        creator_map_backend = RedisCreatorMap(
            settings.redis_dsn,
            platform='youtube',
        )
    else:
        creator_map_backend = FileCreatorMap(
            settings.channel_map_file,
        )

    if not settings.proxies:
        logging.info(
            'No proxies configured, using direct '
            'connection for scraping',
        )
        settings.channel_concurrency = 1

    if not settings.channel_no_upload:
        await upload_channels(
            settings, ctx.client, fm,
        )

    if settings.channel_upload_only:
        await _watch_and_upload_channels(
            settings, ctx.client, fm,
        )
    else:
        await scrape_channels(
            settings, ctx.client, fm,
            creator_map_backend,
        )
```

- [ ] **Step 4: Verify and test**

```bash
python tools/yt_channel_scrape.py --help
python -m pytest tests/ -v -k "channel" --timeout=30
```

- [ ] **Step 5: Commit**

```bash
git add tools/yt_channel_scrape.py
git commit -m "Refactor yt_channel_scrape to use ScraperRunner"
```

---

## Task 7: Refactor `yt_rss_scrape.py` to use `ScraperRunner`

**Files:**
- Modify: `tools/yt_rss_scrape.py`

Same pattern as Tasks 5-6.

- [ ] **Step 1: Update imports**

Same changes: remove supervisor/metrics/logging imports, add
`ScraperRunner`/`ScraperRunContext`.

- [ ] **Step 2: Rewrite `main()`**

```python
def main() -> None:
    settings: RssSettings = RssSettings()

    if not settings.api_key_id or (
        not settings.api_key_secret
    ):
        print(
            'Error: API key ID and secret must be '
            'provided via --api-key-id/--api-key-'
            'secret, environment variables '
            'API_KEY_ID/API_KEY_SECRET, or a .env '
            'file'
        )
        sys.exit(1)

    runner: ScraperRunner = ScraperRunner(
        settings=settings,
        scraper_label='rss',
        platform='youtube',
        num_processes=settings.rss_num_processes,
        concurrency=settings.rss_concurrency,
        metrics_port=settings.metrics_port,
        log_file=settings.rss_log_file,
        log_level=settings.rss_log_level,
        rate_limiter_factory=lambda s: (
            YouTubeRateLimiter.get(
                state_dir=s.rate_limiter_state_dir,
                redis_dsn=s.redis_dsn,
            )
        ),
    )
    sys.exit(runner.run_sync(_run_worker))
```

- [ ] **Step 3: Rewrite `_run_worker()`**

Change signature to `_run_worker(ctx: ScraperRunContext)`.
Keep: AssetFileManagement, video_data_directory makedirs,
CreatorQueue backend selection, ChannelMap backend selection,
TierConfig parsing, worker_loop call with `async with client`.
Remove: metrics, proxy logging, rate limiter, client setup,
signal handlers.

The RSS scraper uses `async with client:` for drain.  With
`ScraperRunner` owning drain, replace:

```python
    try:
        async with client:
            await worker_loop(...)
    except asyncio.CancelledError:
        ...
```

with just:

```python
    await worker_loop(
        settings, ctx.client, channel_fm,
        creator_queue, tiers, creator_map_backend,
    )
```

`ScraperRunner` handles drain in its `finally` block.

```python
async def _run_worker(
    ctx: ScraperRunContext,
) -> None:
    settings: RssSettings = ctx.settings

    channel_fm: AssetFileManagement = (
        AssetFileManagement(
            settings.channel_data_directory,
        )
    )
    os.makedirs(
        settings.video_data_directory, exist_ok=True,
    )

    creator_queue: CreatorQueue
    if settings.redis_dsn:
        creator_queue = RedisCreatorQueue(
            settings.redis_dsn,
            get_worker_id(),
            platform='youtube',
        )
    else:
        creator_queue = FileCreatorQueue(
            settings.queue_file,
            settings.no_feeds_file,
        )

    creator_map_backend: CreatorMap
    if settings.redis_dsn:
        creator_map_backend = RedisCreatorMap(
            settings.redis_dsn,
            platform='youtube',
        )
    else:
        creator_map_backend = FileCreatorMap(
            settings.channel_map_file,
        )

    tiers: list[TierConfig] = parse_priority_queues(
        settings.priority_queues,
    )

    await worker_loop(
        settings, ctx.client, channel_fm,
        creator_queue, tiers, creator_map_backend,
    )
```

- [ ] **Step 4: Verify and test**

```bash
python tools/yt_rss_scrape.py --help
python -m pytest tests/ -v -k "rss" --timeout=30
```

- [ ] **Step 5: Commit**

```bash
git add tools/yt_rss_scrape.py
git commit -m "Refactor yt_rss_scrape to use ScraperRunner"
```

---

## Task 8: Full test suite and final validation

**Files:** None (validation only)

- [ ] **Step 1: Run the full unit test suite**

```bash
python -m pytest tests/unit/ -v --timeout=30
```

Expected: all tests pass.

- [ ] **Step 2: Run integration tests (non-network)**

```bash
python -m pytest tests/integration/ -v --timeout=60
```

Expected: all tests pass (some may skip if Redis/YouTube
not available — that's fine).

- [ ] **Step 3: Verify all three tools still start**

```bash
python tools/yt_video_scrape.py --help
python tools/yt_channel_scrape.py --help
python tools/yt_rss_scrape.py --help
```

Expected: same help output as before for all three.

- [ ] **Step 4: Verify no stale imports**

```bash
grep -r 'from scrape_exchange.channel_map' \
     scrape_exchange/ tools/ tests/ \
     --include='*.py' || echo 'Clean'
grep -r 'from scrape_exchange.video_claim' \
     scrape_exchange/ tools/ tests/ \
     --include='*.py' || echo 'Clean'
```

Expected: both print "Clean".

- [ ] **Step 5: Commit if any fixups needed**

```bash
git add -A
git commit -m "Fix any remaining issues from multi-platform refactoring"
```

Only commit if there are changes.  If the suite was clean,
skip this step.
