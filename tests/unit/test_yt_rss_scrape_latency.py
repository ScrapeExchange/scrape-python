'''fetch_rss must emit one METRIC_SCRAPE_DURATION observation per
call, with outcome=success on the success path and outcome=failure
when the underlying HTTP call raises. Labels must match the
scraper's existing counter labels (scraper=rss_scraper,
entity=rss_feed, api=rss, platform=youtube).'''

import importlib.util
import sys
import unittest

from pathlib import Path
from types import ModuleType
from unittest.mock import AsyncMock, MagicMock, patch


def _load_yt_rss_scrape() -> ModuleType:
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


_FEED_XML: str = (
    '<?xml version="1.0" encoding="UTF-8"?>\n'
    '<feed xmlns="http://www.w3.org/2005/Atom">\n'
    '  <title>Test channel</title>\n'
    '</feed>\n'
)


class _StubResponse:
    text: str = _FEED_XML

    def raise_for_status(self) -> None:
        return None


class _StubClient:
    async def get(
        self, url: str, **kwargs: object,
    ) -> _StubResponse:
        return _StubResponse()


class _RaisingClient:
    async def get(
        self, url: str, **kwargs: object,
    ) -> object:
        raise RuntimeError('boom')


class TestFetchRssLatency(unittest.IsolatedAsyncioTestCase):

    async def test_success_path_records_observation(self) -> None:
        rate_limiter: MagicMock = MagicMock()
        rate_limiter.acquire = AsyncMock(return_value=None)
        rate_limiter.report_rss_success = MagicMock()
        with patch.object(
            yt_rss_scrape, 'METRIC_SCRAPE_DURATION',
        ) as duration, patch.object(
            yt_rss_scrape,
            'pooled_httpx_client_for_entry',
            lambda entry: _StubClient(),
        ), patch.object(
            yt_rss_scrape.YouTubeRateLimiter,
            'get', return_value=rate_limiter,
        ):
            await yt_rss_scrape.fetch_rss(
                rss_url=(
                    'https://example/feeds/videos.xml?'
                    'channel_id=UC0'
                ),
                channel_handle='Test',
            )
        duration.labels.assert_called_once()
        kwargs: dict = duration.labels.call_args.kwargs
        self.assertEqual(kwargs['outcome'], 'success')
        self.assertEqual(kwargs['scraper'], 'rss_scraper')
        self.assertEqual(kwargs['entity'], 'rss_feed')
        self.assertEqual(kwargs['api'], 'rss')
        self.assertEqual(kwargs['platform'], 'youtube')
        duration.labels.return_value.observe.assert_called_once()

    async def test_fetch_rss_sends_browser_headers_and_cookies(self) -> None:
        captured: dict = {}

        class _CapturingClient:
            async def get(
                self, url: str, **kwargs: object,
            ) -> _StubResponse:
                captured.update(kwargs)
                return _StubResponse()

        rate_limiter: MagicMock = MagicMock()
        rate_limiter.acquire = AsyncMock(return_value='proxy-a')
        rate_limiter.get_cookie_file_cached = MagicMock(return_value=None)
        rate_limiter.report_rss_success = MagicMock()

        with patch.object(
            yt_rss_scrape,
            'pooled_httpx_client_for_entry',
            lambda entry: _CapturingClient(),
        ), patch.object(
            yt_rss_scrape.YouTubeRateLimiter,
            'get', return_value=rate_limiter,
        ):
            await yt_rss_scrape.fetch_rss(
                rss_url=(
                    'https://example/feeds/videos.xml?'
                    'channel_id=UC0'
                ),
                channel_handle='Test',
                proxy='proxy-a',
            )

        headers = captured['headers']
        cookies = captured['cookies']
        self.assertIn('Chrome/', headers['User-Agent'])
        self.assertEqual(headers['Sec-Fetch-Mode'], 'navigate')
        self.assertEqual(headers['X-YouTube-Client-Name'], '1')
        self.assertIn('CONSENT', cookies)
        self.assertIn('SOCS', cookies)
        self.assertIn('VISITOR_INFO1_LIVE', cookies)

    async def test_failure_path_records_observation(self) -> None:
        rate_limiter: MagicMock = MagicMock()
        rate_limiter.acquire = AsyncMock(return_value=None)
        rate_limiter.report_rss_success = MagicMock()
        with patch.object(
            yt_rss_scrape, 'METRIC_SCRAPE_DURATION',
        ) as duration, patch.object(
            yt_rss_scrape,
            'pooled_httpx_client_for_entry',
            lambda entry: _RaisingClient(),
        ), patch.object(
            yt_rss_scrape.YouTubeRateLimiter,
            'get', return_value=rate_limiter,
        ):
            with self.assertRaises(RuntimeError):
                await yt_rss_scrape.fetch_rss(
                    rss_url=(
                        'https://example/feeds/videos.xml?'
                        'channel_id=UC0'
                    ),
                    channel_handle='Test',
                )
        duration.labels.assert_called_once()
        kwargs: dict = duration.labels.call_args.kwargs
        self.assertEqual(kwargs['outcome'], 'failure')
        self.assertEqual(kwargs['scraper'], 'rss_scraper')
        self.assertEqual(kwargs['entity'], 'rss_feed')
        self.assertEqual(kwargs['api'], 'rss')
        self.assertEqual(kwargs['platform'], 'youtube')
        duration.labels.return_value.observe.assert_called_once()


if __name__ == '__main__':
    unittest.main()
