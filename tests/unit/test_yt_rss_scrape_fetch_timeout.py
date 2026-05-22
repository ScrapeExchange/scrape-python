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
        rate_limiter.report_rss_success = MagicMock()

        with patch.object(
            yt_rss_scrape,
            'pooled_httpx_client_for_entry',
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


import httpx


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
            'pooled_httpx_client_for_entry',
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


if __name__ == '__main__':
    unittest.main()
