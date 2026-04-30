'''Tests for video scraper handle resolution.'''

import unittest

from unittest.mock import AsyncMock, patch

from tools import yt_video_scrape
from tools.yt_video_scrape import resolve_video_upload_handle


class TestResolveVideoUploadHandle(
    unittest.IsolatedAsyncioTestCase,
):
    def _make_video(
        self, channel_id: str, channel_handle: str,
    ):
        from scrape_exchange.youtube.youtube_video import (
            YouTubeVideo,
        )
        v: YouTubeVideo = YouTubeVideo(video_id='vid1234')
        v.channel_id = channel_id
        v.channel_handle = channel_handle
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
            # resolve_video_upload_handle logs a WARNING via the
            # root logger when InnerTube resolution fails.
            with self.assertLogs(level='WARNING'):
                result: str | None = await resolve_video_upload_handle(
                    video, cm, proxy=None,
                )

        self.assertIsNone(result)
        cm.put.assert_not_awaited()

    async def test_empty_channel_id_and_handle_returns_none(
        self,
    ) -> None:
        '''
        A degenerate record from the RSS path (or a corrupt write)
        with neither channel_id nor channel_handle must NOT cause
        ``fallback_handle('')`` to raise — the resolver returns
        ``None`` so the caller skips the upload, exactly like an
        InnerTube failure.
        '''
        from scrape_exchange.creator_map import NullCreatorMap

        cm: NullCreatorMap = NullCreatorMap()
        cm.get = AsyncMock(return_value=None)
        cm.put = AsyncMock()

        video = self._make_video('', '')
        with self.assertLogs(level='WARNING'):
            result: str | None = await resolve_video_upload_handle(
                video, cm, proxy=None,
            )

        self.assertIsNone(result)
        cm.get.assert_not_awaited()
        cm.put.assert_not_awaited()

    async def test_empty_channel_id_falls_back_to_handle(
        self,
    ) -> None:
        '''
        When channel_id is empty but channel_handle is set,
        fall back to ``fallback_handle(channel_handle)`` rather
        than returning None — this is the legacy/no-id case.
        '''
        from scrape_exchange.creator_map import NullCreatorMap

        cm: NullCreatorMap = NullCreatorMap()
        cm.get = AsyncMock(return_value=None)
        cm.put = AsyncMock()

        video = self._make_video('', '@SomeChannel')
        result: str | None = await resolve_video_upload_handle(
            video, cm, proxy=None,
        )

        self.assertEqual(result, 'somechannel')
        cm.get.assert_not_awaited()
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
