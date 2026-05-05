'''Test that fetch_rss uses a tight HTTP timeout so slow upstreams
do not occupy worker slots for the full default budget.'''

import importlib.util
import unittest

from pathlib import Path
from types import ModuleType
from unittest.mock import AsyncMock, MagicMock, patch


def _load_yt_rss_scrape() -> ModuleType:
    import sys
    if 'yt_rss_scrape' in sys.modules:
        return sys.modules['yt_rss_scrape']

    repo_root: Path = Path(__file__).resolve().parents[2]
    module_path: Path = repo_root / 'tools' / 'yt_rss_scrape.py'
    spec = importlib.util.spec_from_file_location(
        'yt_rss_scrape', module_path,
    )
    assert spec is not None and spec.loader is not None
    module: ModuleType = importlib.util.module_from_spec(spec)
    sys.modules['yt_rss_scrape'] = module
    spec.loader.exec_module(module)
    return module


yt_rss_scrape: ModuleType = _load_yt_rss_scrape()


class _StubResponse:
    text: str = '<feed xmlns="http://www.w3.org/2005/Atom"></feed>'

    def raise_for_status(self) -> None:
        return None


class TestFetchRssTimeout(unittest.IsolatedAsyncioTestCase):
    '''fetch_rss must pass a 5s read / 1s connect timeout to httpx
    so a slow upstream cannot occupy a worker slot for longer.'''

    async def test_fetch_rss_uses_short_timeout(self) -> None:
        captured: dict = {}

        class _StubClient:
            def __init__(
                self, *args: object, **kwargs: object,
            ) -> None:
                pass

            async def __aenter__(self) -> '_StubClient':
                return self

            async def __aexit__(self, *exc: object) -> None:
                return None

            async def get(
                self, url: str, **kwargs: object,
            ) -> _StubResponse:
                captured['timeout'] = kwargs.get('timeout')
                return _StubResponse()

        rate_limiter: MagicMock = MagicMock()
        rate_limiter.acquire = AsyncMock(return_value=None)
        rate_limiter.report_rss_success = MagicMock()

        with patch.object(
            yt_rss_scrape.httpx, 'AsyncClient', _StubClient,
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
        self.assertEqual(timeout.read, 5.0)
        self.assertEqual(timeout.connect, 1.0)


if __name__ == '__main__':
    unittest.main()
