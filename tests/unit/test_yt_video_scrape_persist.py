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
        video.to_file.assert_awaited_once()
