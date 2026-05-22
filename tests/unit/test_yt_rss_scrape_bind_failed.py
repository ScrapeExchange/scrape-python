'''
Unit test: fetch_rss classifies OSError(EADDRNOTAVAIL) wrapped in
httpx.NetworkError as bind_failed and records the proxy_file label
from the active catalog.
'''

import errno
import importlib.util
import unittest
from pathlib import Path
from types import ModuleType
from unittest.mock import AsyncMock, MagicMock, patch

import httpx

from scrape_exchange.proxy_loader import (
    ProxyCatalog,
    set_active_catalog,
)


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


class TestFetchRssBindFailed(unittest.IsolatedAsyncioTestCase):
    '''When an httpx NetworkError wraps OSError(EADDRNOTAVAIL),
    the failure is classified as bind_failed and the proxy_file
    label is taken from the active catalog.'''

    def setUp(self) -> None:
        catalog: ProxyCatalog = ProxyCatalog(
            entries=['local://192.0.2.99'],
            source={'local://192.0.2.99': 'local-egress'},
        )
        set_active_catalog(catalog)

    def tearDown(self) -> None:
        set_active_catalog(ProxyCatalog())

    async def test_bind_failure_classified_as_bind_failed(
        self,
    ) -> None:
        recorded: dict = {}

        def fake_record(
            reason: str,
            ip: str | None,
            file_label: str,
        ) -> None:
            recorded['reason'] = reason
            recorded['proxy_ip'] = ip
            recorded['proxy_file'] = file_label

        class _ConnFailClient:
            async def get(
                self, url: str, **kwargs: object,
            ) -> object:
                inner: OSError = OSError(
                    errno.EADDRNOTAVAIL,
                    'Cannot assign requested address',
                )
                err: httpx.NetworkError = httpx.NetworkError(
                    'bind failure',
                )
                err.__cause__ = inner
                raise err

        rate_limiter: MagicMock = MagicMock()
        rate_limiter.acquire = AsyncMock(
            return_value='local://192.0.2.99',
        )
        rate_limiter.report_rss_success = MagicMock()

        with patch.object(
            yt_rss_scrape,
            '_record_rss_failure',
            fake_record,
        ), patch.object(
            yt_rss_scrape,
            'pooled_httpx_client_for_entry',
            lambda entry: _ConnFailClient(),
        ), patch.object(
            yt_rss_scrape.YouTubeRateLimiter,
            'get',
            return_value=rate_limiter,
        ):
            with self.assertRaises(httpx.NetworkError):
                await yt_rss_scrape.fetch_rss(
                    rss_url=(
                        'https://example/feeds/x?channel_id=UC0'
                    ),
                    channel_handle='Test',
                )

        self.assertEqual(recorded['reason'], 'bind_failed')
        self.assertEqual(recorded['proxy_ip'], '192.0.2.99')
        self.assertEqual(recorded['proxy_file'], 'local-egress')
