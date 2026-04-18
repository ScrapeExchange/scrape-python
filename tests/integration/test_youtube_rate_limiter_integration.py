'''
Integration tests for the proxy-aware YouTubeRateLimiter.

These tests exercise the rate limiter with real asyncio timing to verify
that token consumption, wait behaviour, and proxy selection work under
realistic concurrency.  Proxies are loaded from the collateral file.

Cookie-jar tests (TestAcquireReturnsCookieFile, TestWarmCookieJar) make
real HTTP requests to YouTube through the proxies in the collateral file
and therefore require working proxy connectivity.
'''

import asyncio
import os
import unittest
from unittest.mock import patch

from scrape_exchange.rate_limiter import _Bucket, _ProxyBuckets
from scrape_exchange.youtube.youtube_cookiejar import YouTubeCookieJar
from scrape_exchange.youtube.youtube_rate_limiter import (
    YouTubeRateLimiter,
    YouTubeCallType,
    _DEFAULT_CONFIGS,
)

PROXIES_FILE: str = os.path.join(
    os.path.dirname(__file__), '..', 'collateral', 'local', 'proxies.list',
)


def _read_file(path: str) -> str:
    with open(path) as fh:
        return fh.read()


def _load_proxies(path: str = PROXIES_FILE) -> list[str]:
    with open(path) as fh:
        return [line.strip() for line in fh if line.strip()]


PROXIES: list[str] = _load_proxies()


class TestAcquireTiming(unittest.IsolatedAsyncioTestCase):
    '''Verify that acquire actually waits when tokens are exhausted.'''

    def setUp(self) -> None:
        YouTubeRateLimiter.reset()
        # Patch jitter to zero so tests complete in milliseconds; the real
        # asyncio event loop and token-bucket logic are still exercised.
        self._jitter_patcher = patch(
            'scrape_exchange.rate_limiter.random.uniform', return_value=0.0,
        )
        self._jitter_patcher.start()

    def tearDown(self) -> None:
        self._jitter_patcher.stop()
        YouTubeRateLimiter.reset()

    async def test_burst_acquires_consume_correct_tokens(self) -> None:
        '''Acquiring N tokens from a full bucket leaves burst - N tokens.'''
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        proxy: str = PROXIES[0]
        burst: int = _DEFAULT_CONFIGS[YouTubeCallType.RSS].burst
        acquires: int = burst // 2

        for _ in range(acquires):
            await limiter.acquire(YouTubeCallType.RSS, proxy=proxy)

        pb: _ProxyBuckets = limiter._backend._get_or_create(proxy)
        remaining: float = pb.buckets[YouTubeCallType.RSS].tokens
        self.assertAlmostEqual(remaining, burst - acquires, delta=0.1)

    async def test_exhausted_bucket_causes_wait(self) -> None:
        '''After exhausting burst, bucket must report a non-zero wait.'''
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        proxy: str = PROXIES[0]

        burst: int = _DEFAULT_CONFIGS[YouTubeCallType.RSS].burst

        for _ in range(burst):
            await limiter.acquire(YouTubeCallType.RSS, proxy=proxy)

        pb: _ProxyBuckets = limiter._backend._get_or_create(proxy)
        bucket: _Bucket = pb.buckets[YouTubeCallType.RSS]
        wait_estimate: float = bucket.time_until_available()
        self.assertGreater(wait_estimate, 0.0)


class TestConcurrentProxySelection(unittest.IsolatedAsyncioTestCase):
    '''Proxy selection and concurrency correctness with the file proxy.'''

    def setUp(self) -> None:
        YouTubeRateLimiter.reset()
        # Patch jitter to zero and suppress background warm-up so tests stay
        # focused purely on token-bucket concurrency behaviour.
        self._jitter_patcher = patch(
            'scrape_exchange.rate_limiter.random.uniform', return_value=0.0,
        )
        self._jitter_patcher.start()
        self._svc_patcher = patch.object(
            YouTubeRateLimiter, '_start_cookie_services', return_value=None,
        )
        self._svc_patcher.start()

    def tearDown(self) -> None:
        self._jitter_patcher.stop()
        self._svc_patcher.stop()
        YouTubeRateLimiter.reset()

    async def test_concurrent_acquires_all_return_registered_proxy(
        self,
    ) -> None:
        '''Burst-many concurrent auto-selected acquires return PROXIES[0].'''
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        limiter.set_proxies(PROXIES)

        burst: int = _DEFAULT_CONFIGS[YouTubeCallType.RSS].burst

        coros = [limiter.acquire(YouTubeCallType.RSS) for _ in range(burst)]
        results: list[str | None] = await asyncio.gather(*coros)

        for result_proxy in results:
            self.assertEqual(result_proxy, PROXIES[0])

    async def test_auto_select_returns_registered_proxy_after_partial_drain(
        self,
    ) -> None:
        '''Auto-select returns the registered proxy after a partial drain.'''
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        limiter.set_proxies(PROXIES)

        burst: int = _DEFAULT_CONFIGS[YouTubeCallType.BROWSE].burst

        # Partially drain the proxy's bucket
        for _ in range(burst // 2):
            await limiter.acquire(YouTubeCallType.BROWSE, proxy=PROXIES[0])

        result_proxy = await limiter.acquire(YouTubeCallType.BROWSE)
        self.assertEqual(result_proxy, PROXIES[0])


class TestProxiesFromFile(unittest.IsolatedAsyncioTestCase):
    '''Verify end-to-end flow using the proxies.list collateral file.'''

    def setUp(self) -> None:
        YouTubeRateLimiter.reset()
        # Patch jitter to zero and suppress background warm-up so these tests
        # stay focused on proxy selection behaviour.
        self._jitter_patcher = patch(
            'scrape_exchange.rate_limiter.random.uniform', return_value=0.0,
        )
        self._jitter_patcher.start()
        self._svc_patcher = patch.object(
            YouTubeRateLimiter, '_start_cookie_services', return_value=None,
        )
        self._svc_patcher.start()

    def tearDown(self) -> None:
        self._jitter_patcher.stop()
        self._svc_patcher.stop()
        YouTubeRateLimiter.reset()

    async def test_file_proxies_work_with_limiter(self) -> None:
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        limiter.set_proxies(PROXIES)

        result_proxy = await limiter.acquire(YouTubeCallType.BROWSE)
        self.assertIn(result_proxy, PROXIES)

    async def test_explicit_proxy_from_file(self) -> None:
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        for proxy in PROXIES:
            result_proxy = await limiter.acquire(
                YouTubeCallType.PLAYER, proxy=proxy,
            )
            self.assertEqual(result_proxy, proxy)


class TestAcquireReturnsCookieFile(unittest.IsolatedAsyncioTestCase):
    '''
    Verify that acquire() returns (proxy, cookie_file) with a valid cookie
    file acquired through the real YouTubeCookieJar.

    Requires working proxy connectivity — cookie files are fetched from
    YouTube through the proxies in the collateral file.
    '''

    def setUp(self) -> None:
        YouTubeRateLimiter.reset()
        YouTubeCookieJar.reset()
        self.addCleanup(YouTubeRateLimiter.reset)
        self.addCleanup(YouTubeCookieJar.reset)

    async def test_acquire_returns_proxy_string(self) -> None:
        '''acquire() must return the proxy string.'''
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        result = await limiter.acquire(
            YouTubeCallType.RSS, proxy=PROXIES[0],
        )
        self.assertIsInstance(result, str)

    async def test_acquire_returns_correct_proxy(self) -> None:
        '''acquire() must return the proxy that was used.'''
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        result_proxy = await limiter.acquire(
            YouTubeCallType.RSS, proxy=PROXIES[0],
        )
        self.assertEqual(result_proxy, PROXIES[0])

    async def test_get_cookie_file_cached_after_warm(self) -> None:
        '''After warming the jar, get_cookie_file_cached returns a valid path.'''
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        limiter.set_proxies(PROXIES[:1])
        await limiter.warm_cookie_jar()

        proxy = await limiter.acquire(
            YouTubeCallType.RSS, proxy=PROXIES[0],
        )
        cookie_file: str | None = limiter.get_cookie_file_cached(proxy)
        self.assertIsNotNone(cookie_file)
        self.assertTrue(
            os.path.exists(cookie_file),
            f'Cookie file does not exist on disk: {cookie_file}',
        )

    async def test_cookie_file_is_netscape_format(self) -> None:
        '''Cookie file must start with the Netscape magic header line.'''
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        limiter.set_proxies(PROXIES[:1])
        await limiter.warm_cookie_jar()

        proxy = await limiter.acquire(
            YouTubeCallType.RSS, proxy=PROXIES[0],
        )
        cookie_file: str | None = limiter.get_cookie_file_cached(proxy)
        self.assertIsNotNone(cookie_file)
        content: str = await asyncio.to_thread(_read_file, cookie_file)
        first_line: str = content.splitlines()[0].strip()
        self.assertEqual(first_line, '# Netscape HTTP Cookie File')

    async def test_cookie_file_cached_across_acquires(self) -> None:
        '''Two consecutive acquires for the same proxy return the same file.'''
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        limiter.set_proxies(PROXIES[:1])
        # Warm first so the cache is populated before acquire() reads it.
        await limiter.warm_cookie_jar()

        proxy_1 = await limiter.acquire(
            YouTubeCallType.RSS, proxy=PROXIES[0],
        )
        cookie_file_1: str | None = limiter.get_cookie_file_cached(proxy_1)
        proxy_2 = await limiter.acquire(
            YouTubeCallType.RSS, proxy=PROXIES[0],
        )
        cookie_file_2: str | None = limiter.get_cookie_file_cached(proxy_2)
        self.assertIsNotNone(cookie_file_1)
        self.assertEqual(cookie_file_1, cookie_file_2)

    async def test_auto_selected_proxy_has_matching_cookie_file(self) -> None:
        '''When proxy is auto-selected, cookie file matches the proxy.'''
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        limiter.set_proxies(PROXIES)
        await limiter.warm_cookie_jar()

        result_proxy = await limiter.acquire(YouTubeCallType.RSS)
        self.assertIn(result_proxy, PROXIES)
        cookie_file: str | None = limiter.get_cookie_file_cached(result_proxy)
        # The cookie file returned must be the one for the selected proxy.
        jar: YouTubeCookieJar = YouTubeCookieJar.get()
        expected: str | None = await jar.get_cookie_file(result_proxy)
        self.assertEqual(cookie_file, expected)


class TestWarmCookieJar(unittest.IsolatedAsyncioTestCase):
    '''
    Verify warm_cookie_jar() pre-acquires cookie files for all registered
    proxies via real YouTube HTTP sessions.

    Requires working proxy connectivity.
    '''

    def setUp(self) -> None:
        YouTubeRateLimiter.reset()
        YouTubeCookieJar.reset()
        self.addCleanup(YouTubeRateLimiter.reset)
        self.addCleanup(YouTubeCookieJar.reset)

    async def test_warm_creates_files_for_all_proxies(self) -> None:
        '''warm_cookie_jar() must successfully create a file for each proxy.'''
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        limiter.set_proxies(PROXIES)
        await limiter.warm_cookie_jar()

        jar: YouTubeCookieJar = YouTubeCookieJar.get()
        for proxy in PROXIES:
            cookie_file: str | None = await jar.get_cookie_file(proxy)
            self.assertIsNotNone(
                cookie_file, f'No cookie file acquired for proxy={proxy}',
            )
            self.assertTrue(
                os.path.exists(cookie_file),
                f'Cookie file missing from disk: {cookie_file}',
            )

    async def test_warm_no_proxies_acquires_for_direct_connection(
        self,
    ) -> None:
        '''warm_cookie_jar() with no proxies acquires via direct connection.'''
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        # No set_proxies call — limiter has no proxy pool.
        await limiter.warm_cookie_jar()

        jar: YouTubeCookieJar = YouTubeCookieJar.get()
        cookie_file: str | None = await jar.get_cookie_file(None)
        self.assertIsNotNone(
            cookie_file, 'No cookie file acquired for direct connection',
        )
        self.assertTrue(os.path.exists(cookie_file))

    async def test_warm_is_idempotent(self) -> None:
        '''Calling warm_cookie_jar() twice returns the same cached files.'''
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        limiter.set_proxies(PROXIES[:1])

        await limiter.warm_cookie_jar()
        jar: YouTubeCookieJar = YouTubeCookieJar.get()
        path_first: str | None = await jar.get_cookie_file(PROXIES[0])

        await limiter.warm_cookie_jar()
        path_second: str | None = await jar.get_cookie_file(PROXIES[0])

        # Second warm must not discard and re-acquire the still-valid file.
        self.assertEqual(path_first, path_second)

    async def test_cookie_files_contain_youtube_domain_entries(self) -> None:
        '''Cookie files must contain at least one .youtube.com domain entry.'''
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        limiter.set_proxies(PROXIES[:1])
        await limiter.warm_cookie_jar()

        jar: YouTubeCookieJar = YouTubeCookieJar.get()
        cookie_file: str | None = await jar.get_cookie_file(PROXIES[0])
        self.assertIsNotNone(cookie_file)
        content: str = await asyncio.to_thread(_read_file, cookie_file)
        self.assertIn('.youtube.com', content)


if __name__ == '__main__':
    unittest.main()
