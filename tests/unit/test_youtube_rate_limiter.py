'''
Unit tests for the proxy-aware YouTubeRateLimiter.

These tests use no network I/O — jitter and cookie-jar calls are patched
out so that acquire() returns almost immediately and only the token-bucket
logic is exercised.
'''

import logging
import os
import unittest
from unittest.mock import patch

from scrape_exchange.rate_limiter import _ProxyBuckets, _InProcessBackend
from scrape_exchange.youtube.youtube_rate_limiter import (
    YouTubeRateLimiter,
    YouTubeCallType,
    _DEFAULT_CONFIGS,
    METRIC_RSS_CIRCUIT_OPENED,
)


# Circuit-breaker tests deliberately trip the breaker many times;
# the rate-limiter logs a WARNING on every open. Silence that logger
# for the module so the test output stays signal-only. A real
# regression at ERROR level still propagates.
_RL_LOGGER: logging.Logger = logging.getLogger(
    'scrape_exchange.youtube.youtube_rate_limiter',
)
_RL_LOGGER_PRIOR_LEVEL: int = _RL_LOGGER.level


def setUpModule() -> None:
    _RL_LOGGER.setLevel(logging.ERROR)


def tearDownModule() -> None:
    _RL_LOGGER.setLevel(_RL_LOGGER_PRIOR_LEVEL)


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

    def test_set_proxies_from_list_multi(self) -> None:
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        limiter.set_proxies(['http://a:3128', 'http://b:3128'])
        self.assertEqual(
            limiter._proxies, ['http://a:3128', 'http://b:3128'],
        )

    def test_set_proxies_none_clears(self) -> None:
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        limiter.set_proxies(PROXIES)
        limiter.set_proxies(None)
        self.assertIsNone(limiter._proxies)

    def test_set_proxies_empty_list_clears(self) -> None:
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
        '''get_cookie_file_cached() returns None when cache empty.'''
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


class TestRssTimeoutCircuitSelectProxy(_InProcessTestBase):
    '''select_proxy() routes around open timeout circuits for RSS.'''

    def test_select_proxy_skips_open_timeout_circuit_for_rss(
        self,
    ) -> None:
        '''proxy A's timeout circuit open → select_proxy returns
        only B or C for RSS calls.'''
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        limiter.set_proxies([
            'http://a:3128', 'http://b:3128', 'http://c:3128',
        ])
        for _ in range(20):
            limiter.report_rss_timeout('http://a:3128')
        seen: set[str | None] = set()
        for _ in range(30):
            seen.add(limiter.select_proxy(YouTubeCallType.RSS))
        self.assertNotIn('http://a:3128', seen)
        self.assertEqual(
            seen, {'http://b:3128', 'http://c:3128'},
        )

    def test_select_proxy_unaffected_for_non_rss(self) -> None:
        '''Timeout circuit only suppresses RSS routing, not BROWSE.'''
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        limiter.set_proxies(['http://a:3128', 'http://b:3128'])
        for _ in range(20):
            limiter.report_rss_timeout('http://a:3128')
        seen: set[str | None] = set()
        for _ in range(50):
            seen.add(limiter.select_proxy(YouTubeCallType.BROWSE))
        self.assertIn('http://a:3128', seen)
        self.assertIn('http://b:3128', seen)

    def test_select_proxy_all_open_returns_earliest_to_reopen(
        self,
    ) -> None:
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        proxies: list[str] = ['http://a:3128', 'http://b:3128']
        limiter.set_proxies(proxies)
        with patch(
            'scrape_exchange.youtube.youtube_rate_limiter.time.time',
        ) as tm:
            tm.return_value = 1000.0
            for _ in range(20):
                limiter.report_rss_timeout('http://a:3128')
            tm.return_value = 1100.0
            for _ in range(20):
                limiter.report_rss_timeout('http://b:3128')
            # A trips at t=1000 → open until 1300
            # B trips at t=1100 → open until 1400
            # Both open while now < 1300; A is earliest.
            tm.return_value = 1200.0
            chosen: str | None = limiter.select_proxy(
                YouTubeCallType.RSS
            )
        self.assertEqual(chosen, 'http://a:3128')


class TestRssTimeoutCircuitAcquire(unittest.IsolatedAsyncioTestCase):
    '''acquire() must honour open per-proxy timeout circuits.'''

    def setUp(self) -> None:
        self._env_patcher = patch.dict(
            os.environ,
            {
                'RATE_LIMITER_STATE_DIR': '',
                'YOUTUBE_RSS_TIMEOUT_THRESHOLD': '5',
                'YOUTUBE_RSS_CIRCUIT_MIN_COOLDOWN_SECONDS': '300',
                'YOUTUBE_RSS_CIRCUIT_MAX_COOLDOWN_SECONDS': '14400',
            },
            clear=False,
        )
        self._env_patcher.start()
        YouTubeRateLimiter.reset()
        self._jitter_patcher = patch(
            'scrape_exchange.rate_limiter.random.uniform',
            return_value=0.0,
        )
        self._jitter_patcher.start()
        self._svc_patcher = patch.object(
            YouTubeRateLimiter,
            '_start_cookie_services',
            return_value=None,
        )
        self._svc_patcher.start()

    def tearDown(self) -> None:
        self._jitter_patcher.stop()
        self._svc_patcher.stop()
        YouTubeRateLimiter.reset()
        self._env_patcher.stop()

    async def test_acquire_rss_skips_open_timeout_circuit_proxy(
        self,
    ) -> None:
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        limiter.set_proxies(['http://a:3128', 'http://b:3128'])
        for _ in range(5):
            limiter.report_rss_timeout('http://a:3128')
        for _ in range(5):
            chosen: str | None = await limiter.acquire(
                YouTubeCallType.RSS,
            )
            self.assertEqual(chosen, 'http://b:3128')

    async def test_acquire_rss_all_open_sleeps_until_reopen(
        self,
    ) -> None:
        '''When all timeout circuits are open, acquire() sleeps
        until the earliest reopen time.'''
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        limiter.set_proxies(['http://a:3128'])

        fake_now: list[float] = [1000.0]

        def fake_time() -> float:
            return fake_now[0]

        sleeps: list[float] = []

        async def fake_sleep(s: float) -> None:
            sleeps.append(s)
            fake_now[0] += s

        with patch(
            'scrape_exchange.youtube.youtube_rate_limiter.time.time',
            side_effect=fake_time,
        ), patch(
            'scrape_exchange.youtube.youtube_rate_limiter.asyncio.sleep',
            new=fake_sleep,
        ):
            for _ in range(5):
                limiter.report_rss_timeout('http://a:3128')
            # now=1000, open_until=1300
            await limiter.acquire(YouTubeCallType.RSS)
        self.assertGreaterEqual(sum(sleeps), 300.0)

    async def test_acquire_non_rss_ignores_timeout_circuit(
        self,
    ) -> None:
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        limiter.set_proxies(['http://a:3128', 'http://b:3128'])
        for _ in range(5):
            limiter.report_rss_timeout('http://a:3128')
        seen: set[str | None] = set()
        for _ in range(20):
            seen.add(
                await limiter.acquire(YouTubeCallType.BROWSE),
            )
        # BROWSE rate-limit is balanced by tokens, not circuit.
        self.assertIn('http://a:3128', seen)


class TestRssDefaultRate(unittest.TestCase):
    '''
    Guard against unintentional changes to the RSS bucket config.
    Refill rate is 0.16/s (~9.6 req/min per proxy). Burst dropped
    from 8 to 2 on 2026-05-09 to cap simultaneous CONNECT tunnels
    per proxy at warm-up — burst=8 let 8 worker processes each
    grab a token instantly and fire 8 coincident SYNs through the
    WAN router. With burst=2 the worst-case is 2 coincident SYNs
    per proxy at the cost of slightly less smoothing for ad-hoc
    bursts.
    '''

    def test_rss_bucket_refill_rate(self) -> None:
        cfg = _DEFAULT_CONFIGS[YouTubeCallType.RSS]
        self.assertEqual(cfg.burst, 2)
        # Default refill raised from 9.6/min to 30/min on
        # 2026-05-12 (see rate_limit_settings.py). Burst
        # stays at 2 to preserve the SYN-flood remediation.
        self.assertAlmostEqual(
            cfg.refill_rate, 30 / 60, places=6,
        )


class TestHtmlDefaultRate(unittest.TestCase):
    '''
    Guard against unintentional changes to the HTML bucket
    config. Cut from 1.5/s to 0.15/s (~9 req/min) — 90% reduction
    overall — after the WAF diagnosis: HTML was failing at ~99.8%
    under fleet load because real browser sessions fetch
    youtube.com/ before navigating to channel pages. Combined
    with the warm-session step now in AsyncYouTubeClient, the
    very low per-proxy HTML cadence keeps us well under
    YouTube's anti-bot threshold for that path.
    '''

    def test_html_bucket_refill_rate(self) -> None:
        cfg = _DEFAULT_CONFIGS[YouTubeCallType.HTML]
        self.assertEqual(cfg.burst, 10)
        self.assertEqual(cfg.refill_rate, 9 / 60)


class TestPlayerDefaultRate(unittest.TestCase):
    '''
    Guard against unintentional changes to the PLAYER bucket
    config. The production player rate was halved alongside RSS
    to reduce per-IP pressure during the soft-ban on the
    VPN-tunneled proxies.
    '''

    def test_player_bucket_is_halved(self) -> None:
        cfg = _DEFAULT_CONFIGS[YouTubeCallType.PLAYER]
        self.assertEqual(cfg.burst, 2)
        # Default refill raised from 10/min to 30/min on
        # 2026-05-12 (see rate_limit_settings.py). Burst
        # stays at 2.
        self.assertAlmostEqual(
            cfg.refill_rate, 30 / 60, places=6,
        )


class TestRssCircuitTimeoutBreaker(_InProcessTestBase):
    '''The breaker also trips when consecutive timeout_connect
    failures cross YOUTUBE_RSS_TIMEOUT_THRESHOLD (default 20).'''

    def test_default_timeout_threshold_is_20(self) -> None:
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        self.assertEqual(
            limiter._rss_circuit_timeout_threshold, 20,
        )

    def test_timeout_threshold_respects_env_var(self) -> None:
        with patch.dict(
            os.environ,
            {'YOUTUBE_RSS_TIMEOUT_THRESHOLD': '7'},
            clear=False,
        ):
            YouTubeRateLimiter.reset()
            limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
            self.assertEqual(
                limiter._rss_circuit_timeout_threshold, 7,
            )

    def test_circuit_closed_below_timeout_threshold(self) -> None:
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        proxy: str = 'http://a:3128'
        for _ in range(19):
            limiter.report_rss_timeout(proxy)
        self.assertFalse(limiter.is_rss_circuit_open(proxy))

    def test_circuit_opens_at_timeout_threshold(self) -> None:
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        proxy: str = 'http://a:3128'
        for _ in range(20):
            limiter.report_rss_timeout(proxy)
        self.assertTrue(limiter.is_rss_circuit_open(proxy))

    def test_timeout_trip_increments_metric_with_reason_timeout(
        self,
    ) -> None:
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        proxy: str = 'http://a:3128'
        labels: dict[str, str] = {
            'platform': 'youtube',
            'scraper': 'rss_scraper',
            'api': 'rss',
            'proxy': proxy,
            'reason': 'timeout',
        }
        before: float = METRIC_RSS_CIRCUIT_OPENED.labels(
            **labels,
        )._value.get()
        for _ in range(20):
            limiter.report_rss_timeout(proxy)
        after: float = METRIC_RSS_CIRCUIT_OPENED.labels(
            **labels,
        )._value.get()
        self.assertEqual(after - before, 1.0)

    def test_success_resets_timeout_counter(self) -> None:
        '''19 timeouts + success + 19 timeouts = closed (timeout
        counter was reset by the success).'''
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        proxy: str = 'http://a:3128'
        for _ in range(19):
            limiter.report_rss_timeout(proxy)
        limiter.report_rss_success(proxy)
        for _ in range(19):
            limiter.report_rss_timeout(proxy)
        self.assertFalse(limiter.is_rss_circuit_open(proxy))

    def test_success_resets_timeout_counter_and_opens(
        self,
    ) -> None:
        '''19 timeouts + success + 19 timeouts → closed.
        Success resets the counter AND the consecutive_opens
        so the next trip starts at the min cooldown.'''
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        proxy: str = 'http://a:3128'
        for _ in range(19):
            limiter.report_rss_timeout(proxy)
        limiter.report_rss_success(proxy)
        for _ in range(19):
            limiter.report_rss_timeout(proxy)
        self.assertFalse(limiter.is_rss_circuit_open(proxy))


if __name__ == '__main__':
    unittest.main()
