'''
Unit tests for the proxy-aware YouTubeRateLimiter.

These tests use no network I/O — jitter and cookie-jar calls are patched
out so that acquire() returns almost immediately and only the token-bucket
logic is exercised.
'''

import os
import unittest
from unittest.mock import patch

from scrape_exchange.rate_limiter import _ProxyBuckets, _InProcessBackend
from scrape_exchange.youtube.youtube_rate_limiter import (
    YouTubeRateLimiter,
    YouTubeCallType,
    _DEFAULT_CONFIGS,
)


def _in_process_backend(
    limiter: YouTubeRateLimiter,
) -> _InProcessBackend:
    '''
    Narrow the limiter's backend to the in-process variant used by
    every test in this file. Asserts the limiter was constructed
    without a shared state dir.
    '''
    backend: object = limiter._backend
    assert isinstance(backend, _InProcessBackend), (
        'tests expect the in-process backend; set '
        'RATE_LIMITER_STATE_DIR="" or call reset() before the first '
        'get() to force it'
    )
    return backend

PROXIES_FILE: str = os.path.join(
    os.path.dirname(__file__), '..', 'collateral', 'local', 'proxies.list',
)


def _load_proxies(path: str = PROXIES_FILE) -> list[str]:
    with open(path) as fh:
        return [line.strip() for line in fh if line.strip()]


PROXIES: list[str] = _load_proxies()


class _InProcessTestBase(unittest.TestCase):
    '''Force the in-process backend for every test in this file.'''

    def setUp(self) -> None:
        self._env_patcher = patch.dict(
            os.environ, {'RATE_LIMITER_STATE_DIR': ''}, clear=False,
        )
        self._env_patcher.start()
        YouTubeRateLimiter.reset()

    def tearDown(self) -> None:
        YouTubeRateLimiter.reset()
        self._env_patcher.stop()


class TestSetProxies(_InProcessTestBase):

    def test_set_proxies_from_list(self) -> None:
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        limiter.set_proxies(PROXIES)
        self.assertEqual(limiter._proxies, PROXIES)

    def test_set_proxies_from_comma_string(self) -> None:
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        limiter.set_proxies('http://a:3128,http://b:3128')
        self.assertEqual(limiter._proxies, ['http://a:3128', 'http://b:3128'])

    def test_set_proxies_none_clears(self) -> None:
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        limiter.set_proxies(PROXIES)
        limiter.set_proxies(None)
        self.assertIsNone(limiter._proxies)

    def test_set_proxies_empty_string(self) -> None:
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        limiter.set_proxies('')
        self.assertIsNone(limiter._proxies)

    def test_set_proxies_empty_list(self) -> None:
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        limiter.set_proxies([])
        self.assertIsNone(limiter._proxies)


class TestPerProxyBuckets(_InProcessTestBase):

    def test_different_proxies_get_independent_buckets(self) -> None:
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        limiter.set_proxies(['http://a:3128', 'http://b:3128'])
        backend: _InProcessBackend = _in_process_backend(limiter)
        pb_a: _ProxyBuckets = backend._get_or_create('http://a:3128')
        pb_b: _ProxyBuckets = backend._get_or_create('http://b:3128')
        self.assertIsNot(pb_a, pb_b)
        self.assertIsNot(
            pb_a.buckets[YouTubeCallType.BROWSE],
            pb_b.buckets[YouTubeCallType.BROWSE],
        )

    def test_none_proxy_gets_own_bucket(self) -> None:
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        limiter.set_proxies(PROXIES)
        backend: _InProcessBackend = _in_process_backend(limiter)
        pb_none: _ProxyBuckets = backend._get_or_create(None)
        pb_proxy: _ProxyBuckets = backend._get_or_create(PROXIES[0])
        self.assertIsNot(pb_none, pb_proxy)

    def test_same_proxy_returns_same_bucket(self) -> None:
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        backend: _InProcessBackend = _in_process_backend(limiter)
        pb1: _ProxyBuckets = backend._get_or_create('http://x:3128')
        pb2: _ProxyBuckets = backend._get_or_create('http://x:3128')
        self.assertIs(pb1, pb2)


class TestSelectBestProxy(_InProcessTestBase):

    def test_no_proxies_returns_none(self) -> None:
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        result: str | None = limiter.select_proxy(YouTubeCallType.BROWSE)
        self.assertIsNone(result)

    def test_selects_proxy_with_most_tokens(self) -> None:
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        limiter.set_proxies(['http://a:3128', 'http://b:3128'])
        backend: _InProcessBackend = _in_process_backend(limiter)

        # Drain proxy A's BROWSE bucket
        pb_a: _ProxyBuckets = backend._get_or_create('http://a:3128')
        pb_a.buckets[YouTubeCallType.BROWSE].tokens = 0.0

        # Proxy B still has full tokens
        best: str | None = limiter.select_proxy(YouTubeCallType.BROWSE)
        self.assertEqual(best, 'http://b:3128')

    def test_selects_proxy_with_most_tokens_reversed(self) -> None:
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        limiter.set_proxies(['http://a:3128', 'http://b:3128'])
        backend: _InProcessBackend = _in_process_backend(limiter)

        # Drain proxy B's PLAYER bucket
        pb_b: _ProxyBuckets = backend._get_or_create('http://b:3128')
        pb_b.buckets[YouTubeCallType.PLAYER].tokens = 0.0

        best: str | None = limiter.select_proxy(YouTubeCallType.PLAYER)
        self.assertEqual(best, 'http://a:3128')

    def test_equal_tokens_picks_randomly(self) -> None:
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        proxies: list[str] = ['http://a:3128', 'http://b:3128']
        limiter.set_proxies(proxies)
        # With equal tokens both proxies must be eligible; running many times
        # must eventually return each proxy at least once.
        seen: set[str] = set()
        for _ in range(50):
            seen.add(limiter.select_proxy(YouTubeCallType.RSS))
        self.assertEqual(seen, set(proxies))


class TestAcquireWithProxy(unittest.IsolatedAsyncioTestCase):
    '''Token-bucket behaviour tests — jitter and cookie I/O are patched out.'''

    def setUp(self) -> None:
        self._env_patcher = patch.dict(
            os.environ, {'RATE_LIMITER_STATE_DIR': ''}, clear=False,
        )
        self._env_patcher.start()
        YouTubeRateLimiter.reset()
        self._jitter_patcher = patch(
            'scrape_exchange.rate_limiter.random.uniform', return_value=0.0,
        )
        self._jitter_patcher.start()
        # Prevent any network I/O for cookie acquisition in these unit tests.
        self._cookie_patcher = patch.object(
            YouTubeRateLimiter, 'get_cookie_file', return_value=None,
        )
        self._cookie_patcher.start()
        # Prevent set_proxies() from scheduling background cookie warm-up.
        self._svc_patcher = patch.object(
            YouTubeRateLimiter, '_start_cookie_services', return_value=None,
        )
        self._svc_patcher.start()

    def tearDown(self) -> None:
        self._jitter_patcher.stop()
        self._cookie_patcher.stop()
        self._svc_patcher.stop()
        YouTubeRateLimiter.reset()
        self._env_patcher.stop()

    async def test_acquire_explicit_proxy(self) -> None:
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        proxy: str = PROXIES[0]
        result_proxy = await limiter.acquire(
            YouTubeCallType.BROWSE, proxy=proxy,
        )
        self.assertEqual(result_proxy, proxy)

    async def test_acquire_explicit_proxy_consumes_token(self) -> None:
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        proxy: str = PROXIES[0]
        burst: int = _DEFAULT_CONFIGS[YouTubeCallType.BROWSE].burst
        await limiter.acquire(YouTubeCallType.BROWSE, proxy=proxy)
        backend: _InProcessBackend = _in_process_backend(limiter)
        pb: _ProxyBuckets = backend._get_or_create(proxy)
        self.assertAlmostEqual(
            pb.buckets[YouTubeCallType.BROWSE].tokens,
            burst - 1,
            delta=0.1,
        )

    async def test_acquire_no_proxy_no_pool_returns_none(self) -> None:
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        result_proxy = await limiter.acquire(YouTubeCallType.HTML)
        self.assertIsNone(result_proxy)

    async def test_acquire_auto_selects_best_proxy(self) -> None:
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        limiter.set_proxies(['http://a:3128', 'http://b:3128'])
        backend: _InProcessBackend = _in_process_backend(limiter)

        # Drain proxy A
        pb_a: _ProxyBuckets = backend._get_or_create('http://a:3128')
        pb_a.buckets[YouTubeCallType.BROWSE].tokens = 0.0

        result_proxy = await limiter.acquire(YouTubeCallType.BROWSE)
        self.assertEqual(result_proxy, 'http://b:3128')

    async def test_acquire_does_not_cross_pollinate(self) -> None:
        '''Acquiring on proxy A must not affect proxy B's tokens.'''
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        limiter.set_proxies(['http://a:3128', 'http://b:3128'])

        burst: int = _DEFAULT_CONFIGS[YouTubeCallType.PLAYER].burst
        await limiter.acquire(YouTubeCallType.PLAYER, proxy='http://a:3128')

        backend: _InProcessBackend = _in_process_backend(limiter)
        pb_b: _ProxyBuckets = backend._get_or_create('http://b:3128')
        self.assertAlmostEqual(
            pb_b.buckets[YouTubeCallType.PLAYER].tokens,
            burst,
            delta=0.1,
        )

    async def test_acquire_returns_proxy_used(self) -> None:
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        limiter.set_proxies(['http://only:3128'])
        result_proxy = await limiter.acquire(YouTubeCallType.RSS)
        self.assertEqual(result_proxy, 'http://only:3128')

    async def test_multiple_acquires_round_robin_effect(self) -> None:
        '''After draining one proxy, auto-select should shift to the other.'''
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        limiter.set_proxies(['http://a:3128', 'http://b:3128'])

        burst: int = _DEFAULT_CONFIGS[YouTubeCallType.HTML].burst  # 2

        # Exhaust proxy A's HTML tokens
        for _ in range(burst):
            await limiter.acquire(YouTubeCallType.HTML, proxy='http://a:3128')

        # Auto-select should now prefer B
        result_proxy = await limiter.acquire(YouTubeCallType.HTML)
        self.assertEqual(result_proxy, 'http://b:3128')

    async def test_acquire_returns_proxy_string(self) -> None:
        '''acquire() must return the proxy string.'''
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        result = await limiter.acquire(
            YouTubeCallType.RSS, proxy=PROXIES[0],
        )
        self.assertIsInstance(result, str)

    async def test_get_cookie_file_cached_returns_cached_entry(
        self,
    ) -> None:
        '''get_cookie_file_cached() reads cookie from jar; no network I/O.'''
        from scrape_exchange.youtube.youtube_cookiejar import (
            YouTubeCookieJar, _CookieEntry,
        )
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        jar: YouTubeCookieJar = YouTubeCookieJar.get()  # noqa: SLF001
        jar._entries[PROXIES[0]] = _CookieEntry(path='/tmp/yt_test.txt')
        self.addCleanup(YouTubeCookieJar.reset)

        await limiter.acquire(
            YouTubeCallType.RSS, proxy=PROXIES[0],
        )
        cookie_file: str | None = limiter.get_cookie_file_cached(PROXIES[0])
        self.assertEqual(cookie_file, '/tmp/yt_test.txt')

    async def test_get_cookie_file_cached_none_when_cache_empty(
        self,
    ) -> None:
        '''get_cookie_file_cached() returns None when jar cache has no entry.'''
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        await limiter.acquire(
            YouTubeCallType.RSS, proxy=PROXIES[0],
        )
        cookie_file: str | None = limiter.get_cookie_file_cached(PROXIES[0])
        self.assertIsNone(cookie_file)


class TestSingleton(_InProcessTestBase):

    def test_get_returns_same_instance(self) -> None:
        a: YouTubeRateLimiter = YouTubeRateLimiter.get()
        b: YouTubeRateLimiter = YouTubeRateLimiter.get()
        self.assertIs(a, b)

    def test_reset_clears_instance(self) -> None:
        a: YouTubeRateLimiter = YouTubeRateLimiter.get()
        YouTubeRateLimiter.reset()
        b: YouTubeRateLimiter = YouTubeRateLimiter.get()
        self.assertIsNot(a, b)


class TestLoadProxiesFromFile(unittest.TestCase):
    def test_proxies_file_exists_and_has_entries(self) -> None:
        proxies: list[str] = _load_proxies()
        self.assertIsInstance(proxies, list)
        self.assertGreater(len(proxies), 0)
        for p in proxies:
            self.assertTrue(p.startswith('http'))


if __name__ == '__main__':
    unittest.main()
