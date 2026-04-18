'''
Unit tests for the Redis-backed rate-limiter backend.

Uses ``fakeredis`` with Lua scripting support so no real
Redis server is required.
'''

import asyncio
import unittest

from unittest.mock import AsyncMock

try:
    import fakeredis
    import fakeredis.aioredis
    HAS_FAKEREDIS: bool = True
except ImportError:
    HAS_FAKEREDIS = False

from scrape_exchange.worker_id import get_worker_id
from scrape_exchange.rate_limiter import (
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

    def test_try_acquire_degrades_on_error(self) -> None:
        backend = _make_backend()

        async def run():
            _wire(backend)
            await backend._ensure_scripts()
            backend._redis = AsyncMock()
            backend._redis.evalsha = AsyncMock(
                side_effect=ConnectionError('down'),
            )
            return await backend.try_acquire(
                _TEST_CALL_TYPE, None,
            )

        wait, bt, gt = _run(run())
        self.assertEqual(wait, 0.0)
        self.assertEqual(bt, 0.0)
        self.assertEqual(gt, 0.0)

    def test_penalise_degrades_on_error(self) -> None:
        backend = _make_backend()

        async def run():
            _wire(backend)
            await backend._ensure_scripts()
            backend._redis = AsyncMock()
            backend._redis.evalsha = AsyncMock(
                side_effect=ConnectionError('down'),
            )
            await backend.penalise(
                _TEST_CALL_TYPE, None, 5.0,
            )

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
