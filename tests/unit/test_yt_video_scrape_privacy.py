'''
Privacy limits in the YouTube video scraper: videos of channels that
are not in the channel scrape queue are saved as ``.unknown_channel``,
videos of tiny channels as ``.low_subs``, videos of small channels are
masked.
'''

import os
import tempfile
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import fakeredis

from scrape_exchange.file_management import (
    VIDEO_MIN_FILE_PREFIX,
    VIDEO_YTDLP_FILE_PREFIX,
)
from scrape_exchange.youtube.youtube_video import YouTubeVideo


VIDEO_ID: str = 'dQw4w9WgXcQ'
CHANNEL_ID: str = 'UCabc00000000000000000000'
DESCRIPTION: str = 'call +31 6 12345678 or mail jan@mail.example'


def _settings(tmp: str, use_yt_dlp: bool = False) -> MagicMock:
    settings: MagicMock = MagicMock()
    settings.video_data_directory = tmp
    settings.channel_data_directory = tmp
    settings.video_use_yt_dlp = use_yt_dlp
    settings.channel_min_subscribers = 10
    settings.channel_privacy_subscribers = 4000
    settings.channel_views_per_subscriber = 100
    settings.extra_allowed_domains = frozenset()
    return settings


def _video(channel_id: str | None = CHANNEL_ID) -> YouTubeVideo:
    video: YouTubeVideo = YouTubeVideo(video_id=VIDEO_ID)
    video.channel_id = channel_id
    video.channel_handle = 'handle'
    video.description = DESCRIPTION
    return video


class TestVideoPrivacy(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self.tmp: tempfile.TemporaryDirectory = (
            tempfile.TemporaryDirectory()
        )
        self.addCleanup(self.tmp.cleanup)
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(decode_responses=True)
        )
        patcher = patch(
            'tools.yt_video_scrape.enrich_video_channel_country',
            new_callable=AsyncMock,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    async def _meta(self, **fields: str) -> None:
        await self.redis.hset(
            f'youtube:channel:meta:i:{CHANNEL_ID}',
            mapping={'state': 'scheduled', **fields},
        )

    async def _scrape(
        self, video: YouTubeVideo, use_yt_dlp: bool = False,
    ) -> None:
        from tools.yt_video_scrape import _scrape_to_disk
        with patch(
            'tools.yt_video_scrape.YouTubeVideo.scrape',
            new=AsyncMock(return_value=video),
        ):
            await _scrape_to_disk(
                VIDEO_ID,
                settings=_settings(self.tmp.name, use_yt_dlp),
                proxy=None,
                download_client=None,
                redis=self.redis,
            )

    def _path(
        self, suffix: str = '', prefix: str = VIDEO_MIN_FILE_PREFIX,
    ) -> str:
        return (
            f'{YouTubeVideo.get_filepath(VIDEO_ID, self.tmp.name, prefix)}'
            f'{suffix}'
        )

    async def _saved(
        self, prefix: str = VIDEO_MIN_FILE_PREFIX,
    ) -> YouTubeVideo:
        return await YouTubeVideo.from_file(
            VIDEO_ID, self.tmp.name, prefix,
        )

    async def test_channel_not_in_redis(self) -> None:
        await self._scrape(_video())
        self.assertTrue(os.path.exists(self._path('.unknown_channel')))
        self.assertFalse(os.path.exists(self._path()))

    async def test_missing_channel_id(self) -> None:
        await self._scrape(_video(channel_id=None))
        self.assertTrue(os.path.exists(self._path('.unknown_channel')))
        self.assertFalse(os.path.exists(self._path()))

    async def test_tiny_channel(self) -> None:
        await self._meta(subscriber_count='3')
        await self._scrape(_video())
        self.assertTrue(os.path.exists(self._path('.low_subs')))
        self.assertFalse(os.path.exists(self._path()))

    async def test_channel_without_counts(self) -> None:
        await self._meta()
        await self._scrape(_video())
        self.assertTrue(os.path.exists(self._path('.low_subs')))
        self.assertFalse(os.path.exists(self._path()))

    async def test_view_count_fallback(self) -> None:
        await self._meta(view_count='5000000')
        await self._scrape(_video())
        self.assertTrue(os.path.exists(self._path()))
        self.assertEqual((await self._saved()).description, DESCRIPTION)

    async def test_small_channel_masked(self) -> None:
        await self._meta(subscriber_count='500')
        await self._scrape(_video())
        self.assertTrue(os.path.exists(self._path()))
        self.assertEqual(
            (await self._saved()).description,
            'call <phone hidden> or mail <email hidden>',
        )

    async def test_large_channel_unmasked(self) -> None:
        await self._meta(subscriber_count='50000')
        await self._scrape(_video())
        self.assertEqual((await self._saved()).description, DESCRIPTION)

    async def test_yt_dlp_path_also_gated(self) -> None:
        await self._scrape(_video(), use_yt_dlp=True)
        self.assertTrue(os.path.exists(
            self._path('.unknown_channel', VIDEO_YTDLP_FILE_PREFIX)))
        self.assertFalse(
            os.path.exists(self._path('', VIDEO_YTDLP_FILE_PREFIX)))


class TestToFileMarkerSuffix(unittest.IsolatedAsyncioTestCase):

    async def test_marker_suffix_appended(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path: str = await _video().to_file(
                tmp, VIDEO_MIN_FILE_PREFIX, marker_suffix='.low_subs',
            )
            self.assertTrue(path.endswith('.json.br.low_subs'))
            self.assertTrue(os.path.exists(path))


if __name__ == '__main__':
    unittest.main()
