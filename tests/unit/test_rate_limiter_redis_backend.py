'''
Unit tests for the Redis-backed rate-limiter backend.

Uses ``fakeredis`` with Lua scripting support so no real
Redis server is required.
'''

import asyncio
import os
import unittest

from unittest.mock import AsyncMock, patch

from redis.exceptions import (
    AuthenticationError as RedisAuthenticationError,
    ConnectionError as RedisConnectionError,
    ResponseError as RedisResponseError,
    TimeoutError as RedisTimeoutError,
)

try:
    import fakeredis
    import fakeredis.aioredis
    HAS_FAKEREDIS: bool = True
except ImportError:
    HAS_FAKEREDIS = False

from scrape_exchange.worker_id import get_worker_id
from scrape_exchange.rate_limiter import (
    RateLimiterRedisRetrySettings,
    _BucketConfig,
    _RedisBackend,
    _proxy_filename,
    METRIC_REDIS_OPS,
)
from scrape_exchange.youtube.youtube_rate_limiter import (
    YouTubeCallType,
)  # reuse its enum as a concrete CallTypeT


def _run(coro):
    loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


_TEST_CALL_TYPE = YouTubeCallType.BROWSE
_TEST_CONFIGS: dict[YouTubeCallType, _BucketConfig] = {
    YouTubeCallType.BROWSE: _BucketConfig(
        burst=10, refill_rate=5.0,
        jitter_min=0.0, jitter_max=0.0,
    ),
}
_TEST_GLOBAL: _BucketConfig = _BucketConfig(
    burst=20, refill_rate=10.0,
    jitter_min=0.0, jitter_max=0.0,
)


def _make_backend(server=None):
    '''Create a _RedisBackend wired to a fakeredis server.

    The fakeredis client must be created inside the same
    event loop that runs the coroutines, so this function
    returns a backend whose ``_redis`` attribute must be
    replaced inside an async context. Use ``_wire()``
    instead for most tests.
    '''
    backend: _RedisBackend = _RedisBackend.__new__(
        _RedisBackend
    )
    backend._default_configs = _TEST_CONFIGS
    backend._global_config = _TEST_GLOBAL
    backend._platform = 'test'
    backend._cache = {}
    backend._try_acquire_sha = None
    backend._penalise_sha = None
    backend._scripts_loaded = False
    # No retry budget: a Redis error raises at once, as before the
    # retry existed. TestRedisBackendRetry sets its own budget.
    backend._retry_settings = RateLimiterRedisRetrySettings(
        max_seconds=0.0,
    )
    # _redis will be set by _wire()
    backend._redis = None
    backend._server = server or fakeredis.FakeServer()
    return backend


def _wire(backend: _RedisBackend) -> None:
    '''Attach a fakeredis async client (must be called inside
    the running event loop).'''
    backend._redis = fakeredis.aioredis.FakeRedis(
        server=backend._server,
        decode_responses=True,
    )


@unittest.skipUnless(
    HAS_FAKEREDIS, 'fakeredis[lua] not installed',
)
class TestRedisBackendPeekTokens(unittest.TestCase):

    def test_fresh_proxy_returns_burst(self) -> None:
        backend = _make_backend()
        tokens: float = backend.peek_tokens(
            _TEST_CALL_TYPE, None,
        )
        self.assertEqual(tokens, 10.0)

    def test_returns_cached_after_acquire(self) -> None:
        backend = _make_backend()

        async def run():
            _wire(backend)
            await backend.try_acquire(
                _TEST_CALL_TYPE, None,
            )
            return backend.peek_tokens(
                _TEST_CALL_TYPE, None,
            )

        tokens: float = _run(run())
        self.assertLess(tokens, 10.0)


@unittest.skipUnless(
    HAS_FAKEREDIS, 'fakeredis[lua] not installed',
)
class TestRedisBackendTryAcquire(unittest.TestCase):

    def test_first_acquire_succeeds(self) -> None:
        backend = _make_backend()

        async def run():
            _wire(backend)
            return await backend.try_acquire(
                _TEST_CALL_TYPE, None,
            )

        wait, bt, gt = _run(run())
        self.assertEqual(wait, 0.0)
        self.assertAlmostEqual(bt, 9.0, delta=0.5)
        self.assertAlmostEqual(gt, 19.0, delta=0.5)

    def test_exhaust_bucket_returns_wait(self) -> None:
        backend = _make_backend()

        async def run():
            _wire(backend)
            for _ in range(10):
                await backend.try_acquire(
                    _TEST_CALL_TYPE, None,
                )
            return await backend.try_acquire(
                _TEST_CALL_TYPE, None,
            )

        wait, bt, gt = _run(run())
        self.assertGreater(wait, 0.0)

    def test_two_backends_share_state(self) -> None:
        server = fakeredis.FakeServer()
        b1 = _make_backend(server=server)
        b2 = _make_backend(server=server)

        async def run():
            _wire(b1)
            _wire(b2)
            w1, bt1, _ = await b1.try_acquire(
                _TEST_CALL_TYPE, None,
            )
            w2, bt2, _ = await b2.try_acquire(
                _TEST_CALL_TYPE, None,
            )
            return w1, bt1, w2, bt2

        w1, bt1, w2, bt2 = _run(run())
        self.assertEqual(w1, 0.0)
        self.assertEqual(w2, 0.0)
        self.assertLess(bt2, bt1 + 0.5)


@unittest.skipUnless(
    HAS_FAKEREDIS, 'fakeredis[lua] not installed',
)
class TestRedisBackendPenalise(unittest.TestCase):

    def test_penalise_drains_tokens(self) -> None:
        backend = _make_backend()

        async def run():
            _wire(backend)
            await backend.try_acquire(
                _TEST_CALL_TYPE, None,
            )
            await backend.penalise(
                _TEST_CALL_TYPE, None, 5.0,
            )
            return await backend.try_acquire(
                _TEST_CALL_TYPE, None,
            )

        wait, bt, _ = _run(run())
        self.assertGreater(wait, 0.0)
        self.assertLess(bt, 0.0)

    def test_penalise_clamps_at_minus_burst(self) -> None:
        backend = _make_backend()

        async def run():
            _wire(backend)
            await backend.try_acquire(
                _TEST_CALL_TYPE, None,
            )
            await backend.penalise(
                _TEST_CALL_TYPE, None, 1000.0,
            )
            return await backend.try_acquire(
                _TEST_CALL_TYPE, None,
            )

        wait, bt, _ = _run(run())
        self.assertGreaterEqual(bt, -10.5)


@unittest.skipUnless(
    HAS_FAKEREDIS, 'fakeredis[lua] not installed',
)
class TestRedisBackendKeyTTL(unittest.TestCase):

    def test_ttl_is_set_after_acquire(self) -> None:
        backend = _make_backend()

        async def run():
            _wire(backend)
            await backend.try_acquire(
                _TEST_CALL_TYPE, None,
            )
            key: str = backend._key(None)
            return await backend._redis.ttl(key)

        ttl: int = _run(run())
        self.assertGreater(ttl, 0)
        self.assertLessEqual(
            ttl, _RedisBackend._KEY_TTL,
        )


@unittest.skipUnless(
    HAS_FAKEREDIS, 'fakeredis[lua] not installed',
)
class TestRedisBackendDegradation(unittest.TestCase):

    def test_try_acquire_raises_on_error(self) -> None:
        '''
        ``try_acquire`` must re-raise the underlying Redis error
        (after logging a WARNING) so the caller knows the rate
        limiter is unavailable rather than silently letting a
        request through. See commit 7baca18.
        '''
        backend = _make_backend()

        async def run():
            _wire(backend)
            await backend._ensure_lua_scripts()
            backend._redis = AsyncMock()
            backend._redis.evalsha = AsyncMock(
                side_effect=ConnectionError('down'),
            )
            return await backend.try_acquire(
                _TEST_CALL_TYPE, None,
            )

        with self.assertLogs(
            'scrape_exchange.rate_limiter', level='WARNING',
        ):
            with self.assertRaises(ConnectionError):
                _run(run())

    def test_penalise_degrades_on_error(self) -> None:
        '''
        ``penalise`` is best-effort — it logs a WARNING and returns
        rather than raising, because a lost penalty is less harmful
        than forcing every penalty site to handle Redis faults.
        '''
        backend = _make_backend()

        async def run():
            _wire(backend)
            await backend._ensure_lua_scripts()
            backend._redis = AsyncMock()
            backend._redis.evalsha = AsyncMock(
                side_effect=ConnectionError('down'),
            )
            await backend.penalise(
                _TEST_CALL_TYPE, None, 5.0,
            )

        with self.assertLogs(
            'scrape_exchange.rate_limiter', level='WARNING',
        ):
            _run(run())  # should not raise


@unittest.skipUnless(
    HAS_FAKEREDIS, 'fakeredis[lua] not installed',
)
class TestRedisBackendMetrics(unittest.TestCase):

    def test_success_metric_incremented(self) -> None:
        backend = _make_backend()
        labels: dict[str, str] = {
            'operation': 'try_acquire',
            'result': 'success',
            'platform': 'test',
            'worker_id': get_worker_id(),
        }

        async def run():
            _wire(backend)
            before: float = (
                METRIC_REDIS_OPS
                .labels(**labels)._value.get()
            )
            await backend.try_acquire(
                _TEST_CALL_TYPE, None,
            )
            after: float = (
                METRIC_REDIS_OPS
                .labels(**labels)._value.get()
            )
            return after - before

        delta: float = _run(run())
        self.assertEqual(delta, 1.0)


@unittest.skipUnless(
    HAS_FAKEREDIS, 'fakeredis[lua] not installed',
)
class TestRedisBackendKeyNaming(unittest.TestCase):

    def test_key_uses_proxy_hash(self) -> None:
        backend = _make_backend()
        key: str = backend._key('http://proxy:8080')
        h: str = _proxy_filename(
            'http://proxy:8080'
        ).removesuffix('.state')
        self.assertEqual(key, f'rl:test:{h}')

    def test_none_proxy_key(self) -> None:
        backend = _make_backend()
        key: str = backend._key(None)
        h: str = _proxy_filename(None).removesuffix(
            '.state'
        )
        self.assertEqual(key, f'rl:test:{h}')


_OK_RESULT: list[str] = ['0', '9.0', '19.0']


class _FakeClock:
    '''Stands in for time.monotonic(); each sleep advances it.'''

    def __init__(self) -> None:
        self.now: float = 1000.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    async def sleep(self, delay: float) -> None:
        self.sleeps.append(delay)
        self.now += delay


class TestRedisBackendRetry(unittest.TestCase):
    '''try_acquire rides out a slow or unreachable Redis (e.g. a
    Dragonfly snapshot) instead of failing the scrape at once.'''

    def _backend(
        self, side_effect: list[object], max_seconds: float = 300.0,
    ) -> _RedisBackend:
        backend: _RedisBackend = _RedisBackend.__new__(_RedisBackend)
        backend._default_configs = _TEST_CONFIGS
        backend._global_config = _TEST_GLOBAL
        backend._platform = 'test'
        backend._cache = {}
        backend._scripts_loaded = True
        backend._try_acquire_sha = 'sha'
        backend._penalise_sha = 'sha'
        backend._retry_settings = RateLimiterRedisRetrySettings(
            max_seconds=max_seconds, base_seconds=0.5,
            cap_seconds=10.0,
        )
        backend._evalsha_with_reload = AsyncMock(
            side_effect=side_effect,
        )
        return backend

    def _acquire(
        self, backend: _RedisBackend, clock: _FakeClock,
    ) -> tuple[float, float, float]:
        with patch(
            'scrape_exchange.rate_limiter.paced_sleep', clock.sleep,
        ), patch(
            'scrape_exchange.rate_limiter.time.monotonic',
            clock.monotonic,
        ):
            return _run(backend.try_acquire(_TEST_CALL_TYPE, None))

    def test_retries_pool_exhaustion_then_succeeds(self) -> None:
        clock: _FakeClock = _FakeClock()
        backend: _RedisBackend = self._backend([
            RedisConnectionError('No connection available.'),
            RedisTimeoutError('Timeout reading from hws:6380'),
            _OK_RESULT,
        ])
        wait, bt, gt = self._acquire(backend, clock)
        self.assertEqual((wait, bt, gt), (0.0, 9.0, 19.0))
        self.assertEqual(clock.sleeps, [0.5, 1.0])
        self.assertEqual(backend._evalsha_with_reload.await_count, 3)

    def test_backoff_is_capped(self) -> None:
        clock: _FakeClock = _FakeClock()
        errors: list[object] = [
            RedisTimeoutError('slow') for _ in range(7)
        ]
        backend: _RedisBackend = self._backend(errors + [_OK_RESULT])
        self._acquire(backend, clock)
        self.assertEqual(
            clock.sleeps, [0.5, 1.0, 2.0, 4.0, 8.0, 10.0, 10.0],
        )

    def test_gives_up_after_budget(self) -> None:
        clock: _FakeClock = _FakeClock()
        backend: _RedisBackend = self._backend(
            [RedisTimeoutError('slow') for _ in range(100)],
            max_seconds=30.0,
        )
        with self.assertLogs(
            'scrape_exchange.rate_limiter', level='WARNING',
        ), self.assertRaises(RedisTimeoutError):
            self._acquire(backend, clock)
        # 0.5+1+2+4+8 = 15.5; another 10 fits (25.5); a further 10
        # would end past 30, so it raises instead of sleeping.
        self.assertEqual(clock.sleeps, [0.5, 1.0, 2.0, 4.0, 8.0, 10.0])

    def test_does_not_retry_other_errors(self) -> None:
        for error in (
            RedisResponseError('WRONGTYPE'),
            RedisAuthenticationError('invalid password'),
        ):
            with self.subTest(error=type(error).__name__):
                clock: _FakeClock = _FakeClock()
                backend: _RedisBackend = self._backend([error])
                with self.assertLogs(
                    'scrape_exchange.rate_limiter', level='WARNING',
                ), self.assertRaises(type(error)):
                    self._acquire(backend, clock)
                self.assertEqual(clock.sleeps, [])

    def test_each_failed_attempt_counts_as_error(self) -> None:
        labels: dict[str, str] = {
            'operation': 'try_acquire',
            'result': 'error',
            'platform': 'test',
            'worker_id': get_worker_id(),
        }
        before: float = METRIC_REDIS_OPS.labels(**labels)._value.get()
        backend: _RedisBackend = self._backend([
            RedisConnectionError('No connection available.'),
            RedisConnectionError('No connection available.'),
            _OK_RESULT,
        ])
        self._acquire(backend, _FakeClock())
        after: float = METRIC_REDIS_OPS.labels(**labels)._value.get()
        self.assertEqual(after - before, 2.0)


class TestRateLimiterRedisRetrySettings(unittest.TestCase):

    def test_defaults(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            settings: RateLimiterRedisRetrySettings = (
                RateLimiterRedisRetrySettings()
            )
        self.assertEqual(settings.max_seconds, 600.0)
        self.assertEqual(settings.base_seconds, 0.5)
        self.assertEqual(settings.cap_seconds, 10.0)

    def test_env_override(self) -> None:
        with patch.dict(os.environ, {
            'RATE_LIMITER_REDIS_RETRY_MAX_SECONDS': '120',
        }, clear=True):
            settings: RateLimiterRedisRetrySettings = (
                RateLimiterRedisRetrySettings()
            )
        self.assertEqual(settings.max_seconds, 120.0)
