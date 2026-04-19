'''Tests for video scraper handle resolution.'''

import importlib.util
import unittest

from pathlib import Path
from types import ModuleType
from unittest.mock import AsyncMock, patch


def _load_yt_video_scrape() -> ModuleType:
    '''Load tools/yt_video_scrape.py under the bare name
    ``yt_video_scrape`` so top-level Prometheus metrics do not get
    double-registered alongside any other test modules that import
    the same tool file.
    '''
    import sys
    if 'yt_video_scrape' in sys.modules:
        return sys.modules['yt_video_scrape']

    repo_root: Path = Path(__file__).resolve().parents[2]
    module_path: Path = repo_root / 'tools' / 'yt_video_scrape.py'
    spec = importlib.util.spec_from_file_location(
        'yt_video_scrape', module_path,
    )
    assert spec is not None and spec.loader is not None
    module: ModuleType = importlib.util.module_from_spec(spec)
    sys.modules['yt_video_scrape'] = module
    spec.loader.exec_module(module)
    return module


yt_video_scrape: ModuleType = _load_yt_video_scrape()
resolve_video_upload_handle = (
    yt_video_scrape.resolve_video_upload_handle
)


class TestResolveVideoUploadHandle(
    unittest.IsolatedAsyncioTestCase,
):
    def _make_video(
        self, channel_id: str, channel_name: str,
    ):
        from scrape_exchange.youtube.youtube_video import (
            YouTubeVideo,
        )
        v: YouTubeVideo = YouTubeVideo(video_id='vid1234')
        v.channel_id = channel_id
        v.channel_name = channel_name
        return v

    async def test_map_hit_returns_cached_handle(self) -> None:
        from scrape_exchange.creator_map import NullCreatorMap

        cm: NullCreatorMap = NullCreatorMap()
        cm.get = AsyncMock(return_value='Canonical')
        cm.put = AsyncMock()

        video = self._make_video(
            'UC1234567890abcdefghij', 'Display Title',
        )
        result: str | None = await resolve_video_upload_handle(
            video, cm, proxy=None,
        )

        self.assertEqual(result, 'Canonical')
        cm.put.assert_not_awaited()

    async def test_map_miss_resolves_via_innertube(self) -> None:
        from scrape_exchange.creator_map import NullCreatorMap

        cm: NullCreatorMap = NullCreatorMap()
        cm.get = AsyncMock(return_value=None)
        cm.put = AsyncMock()

        video = self._make_video(
            'UC1234567890abcdefghij', 'Display Title',
        )
        with patch.object(
            yt_video_scrape.YouTubeChannel,
            'resolve_channel_id',
            new=AsyncMock(return_value='Canonical'),
        ):
            result: str | None = await resolve_video_upload_handle(
                video, cm, proxy=None,
            )

        self.assertEqual(result, 'Canonical')
        cm.put.assert_awaited_once_with(
            'UC1234567890abcdefghij', 'Canonical',
        )

    async def test_innertube_failure_returns_none(self) -> None:
        from scrape_exchange.creator_map import NullCreatorMap

        cm: NullCreatorMap = NullCreatorMap()
        cm.get = AsyncMock(return_value=None)
        cm.put = AsyncMock()

        video = self._make_video(
            'UC1234567890abcdefghij', 'Display Title',
        )
        with patch.object(
            yt_video_scrape.YouTubeChannel,
            'resolve_channel_id',
            new=AsyncMock(side_effect=RuntimeError('innertube down')),
        ):
            result: str | None = await resolve_video_upload_handle(
                video, cm, proxy=None,
            )

        self.assertIsNone(result)
        cm.put.assert_not_awaited()

    async def test_handle_less_channel_uses_fallback(self) -> None:
        from scrape_exchange.creator_map import NullCreatorMap

        cm: NullCreatorMap = NullCreatorMap()
        cm.get = AsyncMock(return_value=None)
        cm.put = AsyncMock()

        video = self._make_video(
            'UC1234567890abcdefghij', 'Legacy Title',
        )
        with patch.object(
            yt_video_scrape.YouTubeChannel,
            'resolve_channel_id',
            new=AsyncMock(return_value=None),
        ):
            result: str | None = await resolve_video_upload_handle(
                video, cm, proxy=None,
            )

        self.assertEqual(result, 'legacy title')
        cm.put.assert_awaited_once_with(
            'UC1234567890abcdefghij', 'legacy title',
        )
