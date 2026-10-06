'''Test that fetch_rss uses a tight HTTP timeout so slow upstreams
do not occupy worker slots for the full default budget.'''

import importlib.util
import unittest

from pathlib import Path
from types import ModuleType
from unittest.mock import AsyncMock, MagicMock, patch


def _load_yt_rss_scrape() -> ModuleType:
    import sys
    for _key in ('yt_rss_scrape', 'tools.yt_rss_scrape'):
        if _key in sys.modules:
            return sys.modules[_key]

    repo_root: Path = Path(__file__).resolve().parents[2]
    module_path: Path = repo_root / 'tools' / 'yt_rss_scrape.py'
    spec = importlib.util.spec_from_file_location(
        'yt_rss_scrape', module_path,
    )
    assert spec is not None and spec.loader is not None
    module: ModuleType = importlib.util.module_from_spec(spec)
    sys.modules['yt_rss_scrape'] = module
    sys.modules['tools.yt_rss_scrape'] = module
    spec.loader.exec_module(module)
    return module


yt_rss_scrape: ModuleType = _load_yt_rss_scrape()


class _StubResponse:
    text: str = '<feed xmlns="http://www.w3.org/2005/Atom"></feed>'

    def raise_for_status(self) -> None:
        return None


class TestFetchRssTimeout(unittest.IsolatedAsyncioTestCase):
    '''fetch_rss must pass a 5s read / 3s connect timeout to
    the pooled httpx client's per-call ``get(..., timeout=...)``
    so a slow upstream cannot occupy a worker slot for longer.
    Connect was 1s; bumped to 3s after Kibana showed 99.94% of
    RSS timeouts were ConnectTimeout under fleet load, where 1s
    was tight enough to clip otherwise-recoverable handshakes.'''

    async def test_fetch_rss_uses_short_timeout(self) -> None:
        captured: dict = {}

        class _StubClient:
            async def get(
                self, url: str, **kwargs: object,
            ) -> _StubResponse:
                captured['timeout'] = kwargs.get('timeout')
                return _StubResponse()

        rate_limiter: MagicMock = MagicMock()
        rate_limiter.acquire = AsyncMock(return_value=None)
        rate_limiter.report_proxy_result = AsyncMock()
        rate_limiter.report_rss_success = MagicMock()

        with patch.object(
            yt_rss_scrape,
            'borrow_pooled_httpx_client_for_entry',
            lambda entry: _StubClient(),
        ), patch.object(
            yt_rss_scrape.YouTubeRateLimiter,
            'get',
            return_value=rate_limiter,
        ):
            await yt_rss_scrape.fetch_rss(
                rss_url=(
                    'https://example/feeds/videos.xml?channel_id=UC0'
                ),
                channel_handle='Test',
            )

        timeout = captured.get('timeout')
        self.assertIsNotNone(timeout)
        # Defaults from scrape_exchange.http_timeouts:
        # RSS_REQUEST_TIMEOUT=30s, RSS_CONNECT_TIMEOUT=5s,
        # overridable per scraper via env vars.
        self.assertEqual(timeout.read, 30.0)
        self.assertEqual(timeout.connect, 5.0)


import httpx2 as httpx


class TestFetchRssTimeoutCircuitWiring(
    unittest.IsolatedAsyncioTestCase,
):
    '''fetch_rss must call YouTubeRateLimiter.report_rss_timeout
    only for ConnectTimeout. ReadTimeout, PoolTimeout, and a
    plain TimeoutException are recorded on the failure metric
    but must not advance the circuit breaker.'''

    async def _run_with_exc(
        self, exc: BaseException,
    ) -> MagicMock:
        rate_limiter: MagicMock = MagicMock()
        rate_limiter.acquire = AsyncMock(return_value=None)
        rate_limiter.report_proxy_result = AsyncMock()
        rate_limiter.report_rss_success = MagicMock()
        rate_limiter.report_rss_failure = MagicMock()
        rate_limiter.report_rss_timeout = MagicMock()

        class _RaisingClient:
            async def get(
                self, url: str, **kwargs: object,
            ) -> _StubResponse:
                raise exc

        with patch.object(
            yt_rss_scrape,
            'borrow_pooled_httpx_client_for_entry',
            lambda entry: _RaisingClient(),
        ), patch.object(
            yt_rss_scrape.YouTubeRateLimiter,
            'get',
            return_value=rate_limiter,
        ):
            with self.assertRaises(httpx.TimeoutException):
                await yt_rss_scrape.fetch_rss(
                    rss_url=(
                        'https://example/feeds/videos.xml?channel_id=UC0'
                    ),
                    channel_handle='Test',
                )
        return rate_limiter

    async def test_connect_timeout_calls_report_rss_timeout(
        self,
    ) -> None:
        rate_limiter: MagicMock = await self._run_with_exc(
            httpx.ConnectTimeout('connect timed out'),
        )
        self.assertEqual(
            rate_limiter.report_rss_timeout.call_count, 1,
        )

    async def test_read_timeout_does_not_call_report_rss_timeout(
        self,
    ) -> None:
        rate_limiter: MagicMock = await self._run_with_exc(
            httpx.ReadTimeout('read timed out'),
        )
        self.assertEqual(
            rate_limiter.report_rss_timeout.call_count, 0,
        )

    async def test_pool_timeout_does_not_call_report_rss_timeout(
        self,
    ) -> None:
        rate_limiter: MagicMock = await self._run_with_exc(
            httpx.PoolTimeout('pool timed out'),
        )
        self.assertEqual(
            rate_limiter.report_rss_timeout.call_count, 0,
        )

    async def test_other_timeout_does_not_call_report_rss_timeout(
        self,
    ) -> None:
        rate_limiter: MagicMock = await self._run_with_exc(
            httpx.TimeoutException('generic timed out'),
        )
        self.assertEqual(
            rate_limiter.report_rss_timeout.call_count, 0,
        )


class TestFetchRssRetiresPooledClient(unittest.IsolatedAsyncioTestCase):
    '''Connection-establishment failures retire the pooled feed client
    generation so leaked proxy tunnels cannot exhaust its pool; read
    timeouts keep the healthy keep-alive client.'''

    async def _run(self, exc: BaseException) -> AsyncMock:
        rate_limiter: MagicMock = MagicMock()
        rate_limiter.acquire = AsyncMock(return_value='http://p.test:3128')
        rate_limiter.report_proxy_result = AsyncMock()
        rate_limiter.report_rss_timeout = MagicMock()

        class _RaisingClient:
            async def get(self, url: str, **kwargs: object) -> None:
                raise exc

        client: _RaisingClient = _RaisingClient()
        retire: AsyncMock = AsyncMock(return_value=True)
        release: AsyncMock = AsyncMock()
        with patch.object(
            yt_rss_scrape, 'borrow_pooled_httpx_client_for_entry',
            lambda entry: client,
        ), patch.object(
            yt_rss_scrape, 'release_pooled_httpx_client', release,
        ), patch.object(
            yt_rss_scrape, 'retire_pooled_httpx_client', retire,
        ), patch.object(
            yt_rss_scrape, 'jitter_pool_warmup', AsyncMock(),
        ), patch.object(
            yt_rss_scrape.YouTubeRateLimiter, 'get',
            return_value=rate_limiter,
        ):
            with self.assertRaises(Exception):
                await yt_rss_scrape.fetch_rss(
                    rss_url=(
                        'https://example/feeds/videos.xml?channel_id=UC0'
                    ),
                    channel_handle='Test',
                )
        release.assert_awaited_once_with(client)
        retire.client = client
        return retire

    async def test_connection_failures_retire(self) -> None:
        exc: BaseException
        for exc in (
            httpx.PoolTimeout('pool'), httpx.ConnectError('down'),
            httpx.ConnectTimeout('slow'), httpx.ProxyError('proxy'),
        ):
            with self.subTest(exc=type(exc).__name__):
                retire: AsyncMock = await self._run(exc)
                retire.assert_awaited_once_with(
                    'http://p.test:3128', expected=retire.client,
                )

    async def test_read_timeout_keeps_client(self) -> None:
        retire: AsyncMock = await self._run(httpx.ReadTimeout('read'))
        retire.assert_not_awaited()


if __name__ == '__main__':
    unittest.main()
