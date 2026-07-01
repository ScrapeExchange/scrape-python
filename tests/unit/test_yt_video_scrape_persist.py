'''Video scrape persistence contracts.'''

import unittest
from unittest.mock import AsyncMock, MagicMock, patch


class TestScrapeToDisk(unittest.IsolatedAsyncioTestCase):

    @patch('tools.yt_video_scrape.YouTubeVideo')
    async def test_scrape_writes_video_file_once(
        self,
        mock_video_cls: MagicMock,
    ) -> None:
        video: MagicMock = MagicMock()
        video.to_file = AsyncMock()
        mock_video_cls.scrape = AsyncMock(
            return_value=video,
        )
        settings: MagicMock = MagicMock()
        settings.ytdlp_cache_dir = '/tmp/ytdlp-cache'
        settings.video_data_directory = '/tmp/videos'
        settings.log_level = 'INFO'
        settings.video_use_yt_dlp = True

        from tools.yt_video_scrape import _scrape_to_disk

        await _scrape_to_disk(
            'dQw4w9WgXcQ',
            settings=settings,
            proxy=None,
            download_client=None,
        )

        self.assertIsNone(
            mock_video_cls.scrape.await_args.kwargs['save_dir'],
        )
        self.assertIsNone(
            mock_video_cls.scrape.await_args.kwargs['channel_handle'],
        )
        video.to_file.assert_awaited_once()

    async def test_resolve_handle_uses_creator_map_hit(
        self,
    ) -> None:
        from scrape_exchange.creator_map import InMemoryCreatorMap
        from scrape_exchange.youtube.youtube_video import YouTubeVideo
        from tools.yt_video_scrape import _resolve_video_channel_handle

        creator_map: InMemoryCreatorMap = InMemoryCreatorMap()
        await creator_map.put('UC1234567890abcdefghij', 'Canonical')
        video: YouTubeVideo = YouTubeVideo(
            video_id='C7TICwKPG5g',
            channel_handle='Display Name',
        )
        video.channel_id = 'UC1234567890abcdefghij'

        with patch(
            'tools.yt_video_scrape.YouTubeChannel.resolve_channel_id',
            new=AsyncMock(),
        ) as resolve:
            await _resolve_video_channel_handle(
                video,
                creator_map_backend=creator_map,
                proxy=None,
            )

        self.assertEqual(video.channel_handle, 'Canonical')
        resolve.assert_not_awaited()

    async def test_scrape_resolves_and_caches_handle_before_write(
        self,
    ) -> None:
        from scrape_exchange.creator_map import InMemoryCreatorMap
        from tools.yt_video_scrape import _scrape_to_disk

        video: MagicMock = MagicMock()
        video.channel_id = 'UC1234567890abcdefghij'
        video.channel_handle = ''
        video.to_file = AsyncMock()
        settings: MagicMock = MagicMock()
        settings.ytdlp_cache_dir = '/tmp/ytdlp-cache'
        settings.video_data_directory = '/tmp/videos'
        settings.log_level = 'INFO'
        settings.video_use_yt_dlp = False
        creator_map: InMemoryCreatorMap = InMemoryCreatorMap()

        with patch(
            'tools.yt_video_scrape.YouTubeVideo.scrape',
            new=AsyncMock(return_value=video),
        ), patch(
            'tools.yt_video_scrape.YouTubeChannel.resolve_channel_id',
            new=AsyncMock(return_value='Canonical'),
        ):
            await _scrape_to_disk(
                'C7TICwKPG5g',
                settings=settings,
                proxy=None,
                download_client=None,
                creator_map_backend=creator_map,
            )

        self.assertEqual(video.channel_handle, 'Canonical')
        self.assertEqual(
            await creator_map.get('UC1234567890abcdefghij'),
            'Canonical',
        )
        video.to_file.assert_awaited_once()

    async def test_scrape_applies_partial_queue_channel_context(
        self,
    ) -> None:
        from scrape_exchange.video_scrape_queue import (
            VideoQueueChannelContext,
        )
        from tools.yt_video_scrape import _scrape_to_disk

        video: MagicMock = MagicMock()
        video.channel_id = None
        video.channel_handle = ''
        video.channel_url = None
        video.channel_is_verified = None
        video.to_file = AsyncMock()
        settings: MagicMock = MagicMock()
        settings.ytdlp_cache_dir = '/tmp/ytdlp-cache'
        settings.video_data_directory = '/tmp/videos'
        settings.log_level = 'INFO'
        settings.video_use_yt_dlp = False

        with patch(
            'tools.yt_video_scrape.YouTubeVideo.scrape',
            new=AsyncMock(return_value=video),
        ):
            await _scrape_to_disk(
                'C7TICwKPG5g',
                settings=settings,
                proxy=None,
                download_client=None,
                channel_context=VideoQueueChannelContext(
                    channel_id='UC1234567890abcdefghij',
                    channel_handle=None,
                    channel_url='https://www.youtube.com/@SomeHandle',
                    channel_is_verified=None,
                ),
            )

        self.assertEqual(
            video.channel_id, 'UC1234567890abcdefghij',
        )
        self.assertEqual(video.channel_handle, '')
        self.assertEqual(
            video.channel_url,
            'https://www.youtube.com/@SomeHandle',
        )
        self.assertIsNone(video.channel_is_verified)
        video.to_file.assert_awaited_once()

    async def test_scrape_enriches_derived_metadata_before_write(
        self,
    ) -> None:
        from tools.yt_video_scrape import _scrape_to_disk

        video: MagicMock = MagicMock()
        video.channel_id = 'UC1234567890abcdefghij'
        video.category = 'Music'
        video.to_file = AsyncMock()
        settings: MagicMock = MagicMock()
        settings.ytdlp_cache_dir = '/tmp/ytdlp-cache'
        settings.video_data_directory = '/tmp/videos'
        settings.channel_data_directory = '/tmp/channels'
        settings.log_level = 'INFO'
        settings.video_use_yt_dlp = False
        redis: MagicMock = MagicMock()

        with patch(
            'tools.yt_video_scrape.YouTubeVideo.scrape',
            new=AsyncMock(return_value=video),
        ), patch(
            'tools.yt_video_scrape.enrich_video_channel_country',
            new=AsyncMock(),
        ) as enrich, patch(
            'tools.yt_video_scrape.increment_channel_category_count',
            new=AsyncMock(),
        ) as increment:
            await _scrape_to_disk(
                'C7TICwKPG5g',
                settings=settings,
                proxy=None,
                download_client=None,
                redis=redis,
            )

        enrich.assert_awaited_once()
        increment.assert_awaited_once_with(
            redis, 'UC1234567890abcdefghij', 'Music',
        )
        video.to_file.assert_awaited_once()
