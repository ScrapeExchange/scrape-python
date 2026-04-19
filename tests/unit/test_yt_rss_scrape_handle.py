'''Tests for RSS update_channel handle resolution.'''

import importlib.util
import unittest

from pathlib import Path
from types import ModuleType
from unittest.mock import AsyncMock, MagicMock, patch


def _load_yt_rss_scrape() -> ModuleType:
    '''Load tools/yt_rss_scrape.py under the bare name
    ``yt_rss_scrape`` — matching test_yt_rss_scrape_sla.py — so the
    top-level Prometheus metrics do not get double-registered when
    both test modules run in the same discovery.
    '''
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
update_channel = yt_rss_scrape.update_channel


class TestRssUpdateChannelHandleResolution(
    unittest.IsolatedAsyncioTestCase,
):
    async def test_canonical_handle_written_and_used(self) -> None:
        from scrape_exchange.creator_map import NullCreatorMap

        cm: NullCreatorMap = NullCreatorMap()
        cm.put = AsyncMock()
        exchange_client: MagicMock = MagicMock()
        exchange_client.exchange_url = 'http://api.example'
        exchange_client.enqueue_upload = MagicMock(return_value=True)

        browse_response: dict = {
            'metadata': {
                'channelMetadataRenderer': {
                    'vanityChannelUrl': (
                        'http://www.youtube.com/@Canonical'
                    ),
                    'title': 'Canonical',
                    'description': '',
                },
            },
        }

        with patch.object(
            yt_rss_scrape, 'YouTubeChannelTabs',
        ) as tabs_cls:
            tabs_cls.return_value.browse_channel = AsyncMock(
                return_value=browse_response,
            )
            result = await update_channel(
                exchange_client,
                channel_name='input_casing',
                channel_id='UC1234567890abcdefghij',
                creator_map_backend=cm,
            )

        success: bool = result[0]
        handle: str | None = result[2]
        self.assertTrue(success)
        self.assertEqual(handle, 'Canonical')
        cm.put.assert_awaited_once_with(
            'UC1234567890abcdefghij', 'Canonical',
        )
        _args, kwargs = exchange_client.enqueue_upload.call_args
        self.assertEqual(
            kwargs['json']['platform_creator_id'], 'Canonical',
        )
        self.assertEqual(
            kwargs['json']['platform_content_id'], 'Canonical',
        )

    async def test_handle_less_channel_uses_fallback(self) -> None:
        from scrape_exchange.creator_map import NullCreatorMap

        cm: NullCreatorMap = NullCreatorMap()
        cm.put = AsyncMock()
        exchange_client: MagicMock = MagicMock()
        exchange_client.exchange_url = 'http://api.example'
        exchange_client.enqueue_upload = MagicMock(return_value=True)

        browse_response: dict = {
            'metadata': {
                'channelMetadataRenderer': {
                    'title': 'Legacy Channel',
                },
            },
        }

        with patch.object(
            yt_rss_scrape, 'YouTubeChannelTabs',
        ) as tabs_cls:
            tabs_cls.return_value.browse_channel = AsyncMock(
                return_value=browse_response,
            )
            result = await update_channel(
                exchange_client,
                channel_name='INPUT',
                channel_id='UC1234567890abcdefghij',
                creator_map_backend=cm,
            )

        self.assertTrue(result[0])
        self.assertEqual(result[2], 'input')
        cm.put.assert_awaited_once_with(
            'UC1234567890abcdefghij', 'input',
        )
        _args, kwargs = exchange_client.enqueue_upload.call_args
        self.assertEqual(
            kwargs['json']['platform_creator_id'], 'input',
        )

    async def test_browse_failure_returns_failure(self) -> None:
        from scrape_exchange.creator_map import NullCreatorMap

        cm: NullCreatorMap = NullCreatorMap()
        cm.put = AsyncMock()
        exchange_client: MagicMock = MagicMock()

        with patch.object(
            yt_rss_scrape, 'YouTubeChannelTabs',
        ) as tabs_cls:
            tabs_cls.return_value.browse_channel = AsyncMock(
                side_effect=RuntimeError('innertube down'),
            )
            result = await update_channel(
                exchange_client,
                channel_name='input',
                channel_id='UC1234567890abcdefghij',
                creator_map_backend=cm,
            )

        self.assertFalse(result[0])
        self.assertIsNone(result[2])
        cm.put.assert_not_awaited()
