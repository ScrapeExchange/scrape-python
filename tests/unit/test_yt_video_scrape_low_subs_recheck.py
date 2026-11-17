'''
The ``--recheck-low-subs`` mode of ``tools/yt_video_scrape.py``: release
the ``.low_subs`` videos of channels that are no longer tiny, using a
local index so files are opened only once.
'''

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import fakeredis.aioredis

from scrape_exchange.file_management import (
    VIDEO_MIN_FILE_PREFIX,
    VIDEO_YTDLP_FILE_PREFIX,
)
from scrape_exchange.youtube.low_subs_recheck import LOW_SUBS_RELEASED_KEY
from scrape_exchange.youtube.youtube_video import YouTubeVideo
from tools import yt_video_scrape
from tools.yt_video_scrape import (
    LOW_SUBS_INDEX_FILENAME,
    VideoRecheckSummary,
    recheck_low_subs_videos,
)

VIDEO_ID: str = 'dQw4w9WgXcQ'
CHANNEL_ID: str = 'UCabc00000000000000000000'
DESCRIPTION: str = 'call +31 6 12345678 or mail jan@mail.example'


def _settings(video_dir: str | None = None) -> MagicMock:
    settings: MagicMock = MagicMock()
    settings.video_data_directory = video_dir
    settings.redis_dsn = 'redis://localhost:6379/0'
    settings.channel_min_subscribers = 100
    settings.channel_privacy_subscribers = 4000
    settings.channel_views_per_subscriber = 100
    settings.extra_allowed_domains = frozenset()
    settings.recheck_low_subs = True
    settings.recheck_apply = False
    return settings


class TestRecheckLowSubsVideos(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self.tmp: tempfile.TemporaryDirectory = (
            tempfile.TemporaryDirectory()
        )
        self.addCleanup(self.tmp.cleanup)
        self.dir: str = self.tmp.name
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(decode_responses=True)
        )

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    async def _meta(self, subscriber_count: str) -> None:
        await self.redis.hset(
            f'youtube:channel:meta:i:{CHANNEL_ID}',
            mapping={
                'state': 'scheduled',
                'subscriber_count': subscriber_count,
            },
        )

    async def _low_subs_file(
        self,
        video_id: str = VIDEO_ID,
        prefix: str = VIDEO_MIN_FILE_PREFIX,
    ) -> str:
        video: YouTubeVideo = YouTubeVideo(video_id=video_id)
        video.channel_id = CHANNEL_ID
        video.description = DESCRIPTION
        return await video.to_file(
            self.dir, prefix, marker_suffix='.low_subs',
        )

    def _plain(
        self,
        video_id: str = VIDEO_ID,
        prefix: str = VIDEO_MIN_FILE_PREFIX,
    ) -> str:
        return YouTubeVideo.get_filepath(video_id, self.dir, prefix)

    async def _run(self, apply: bool = True) -> VideoRecheckSummary:
        return await recheck_low_subs_videos(
            self.dir, self.redis, _settings(self.dir), apply=apply,
        )

    def _listing(self) -> set[str]:
        return set(os.listdir(self.dir)) - {LOW_SUBS_INDEX_FILENAME}

    async def test_new_file_large_released(self) -> None:
        await self._meta('5000')
        low_subs: str = await self._low_subs_file()
        summary: VideoRecheckSummary = await self._run()
        self.assertEqual(summary.released_unmasked, 1)
        self.assertEqual(summary.scanned_new, 1)
        self.assertFalse(os.path.exists(low_subs))
        saved: YouTubeVideo = await YouTubeVideo.from_file(
            VIDEO_ID, self.dir, VIDEO_MIN_FILE_PREFIX,
        )
        self.assertEqual(saved.description, DESCRIPTION)

    async def test_new_file_small_masked(self) -> None:
        await self._meta('500')
        await self._low_subs_file()
        summary: VideoRecheckSummary = await self._run()
        self.assertEqual(summary.released_masked, 1)
        saved: YouTubeVideo = await YouTubeVideo.from_file(
            VIDEO_ID, self.dir, VIDEO_MIN_FILE_PREFIX,
        )
        self.assertNotIn('12345678', saved.description)
        self.assertNotIn('jan@mail.example', saved.description)

    async def test_still_tiny_indexed_and_left(self) -> None:
        await self._meta('50')
        low_subs: str = await self._low_subs_file()
        summary: VideoRecheckSummary = await self._run()
        self.assertEqual(summary.still_tiny, 1)
        self.assertEqual(summary.indexed, 1)
        self.assertTrue(os.path.exists(low_subs))
        self.assertFalse(os.path.exists(self._plain()))

    async def test_second_run_opens_nothing(self) -> None:
        await self._meta('50')
        await self._low_subs_file()
        await self._run()
        reader: AsyncMock = AsyncMock()
        with patch.object(yt_video_scrape, 'brotli_read_async', reader):
            summary: VideoRecheckSummary = await self._run()
        reader.assert_not_awaited()
        self.assertEqual(summary.scanned_new, 0)

    async def test_released_set_releases_indexed_files(self) -> None:
        await self._meta('50')
        low_subs: str = await self._low_subs_file()
        await self._run()
        await self._meta('150')
        await self.redis.sadd(LOW_SUBS_RELEASED_KEY, CHANNEL_ID)
        summary: VideoRecheckSummary = await self._run()
        self.assertEqual(summary.channels_released, 1)
        self.assertEqual(summary.released_masked, 1)
        self.assertFalse(os.path.exists(low_subs))
        self.assertTrue(os.path.exists(self._plain()))
        self.assertFalse(await self.redis.sismember(
            LOW_SUBS_RELEASED_KEY, CHANNEL_ID,
        ))

    async def test_unknown_channel_left(self) -> None:
        low_subs: str = await self._low_subs_file()
        summary: VideoRecheckSummary = await self._run()
        self.assertEqual(summary.unknown_channel, 1)
        self.assertTrue(os.path.exists(low_subs))

    async def test_plain_copy_exists(self) -> None:
        await self._meta('5000')
        low_subs: str = await self._low_subs_file()
        plain: YouTubeVideo = YouTubeVideo(video_id=VIDEO_ID)
        plain.channel_id = CHANNEL_ID
        plain.description = 'newer scrape'
        await plain.to_file(self.dir, VIDEO_MIN_FILE_PREFIX)
        summary: VideoRecheckSummary = await self._run()
        self.assertEqual(summary.already_exists, 1)
        self.assertFalse(os.path.exists(low_subs))
        saved: YouTubeVideo = await YouTubeVideo.from_file(
            VIDEO_ID, self.dir, VIDEO_MIN_FILE_PREFIX,
        )
        self.assertEqual(saved.description, 'newer scrape')

    async def test_corrupt_file_counted(self) -> None:
        await self._meta('5000')
        await self._low_subs_file()
        bad: str = os.path.join(
            self.dir, f'{VIDEO_MIN_FILE_PREFIX}xxxxxxxxxxx.json.br.low_subs',
        )
        Path(bad).write_bytes(b'not brotli')
        summary: VideoRecheckSummary = await self._run()
        self.assertEqual(summary.errors, 1)
        self.assertEqual(summary.released_unmasked, 1)
        self.assertTrue(os.path.exists(bad))

    async def test_dlp_prefix_preserved(self) -> None:
        await self._meta('5000')
        await self._low_subs_file(prefix=VIDEO_YTDLP_FILE_PREFIX)
        await self._run()
        self.assertTrue(os.path.exists(
            self._plain(prefix=VIDEO_YTDLP_FILE_PREFIX),
        ))
        self.assertTrue(
            self._plain(prefix=VIDEO_YTDLP_FILE_PREFIX).endswith(
                f'video-dlp-{VIDEO_ID}.json.br',
            ),
        )

    async def test_dry_run_changes_nothing(self) -> None:
        await self._meta('5000')
        await self._low_subs_file()
        await self.redis.sadd(LOW_SUBS_RELEASED_KEY, CHANNEL_ID)
        before: set[str] = self._listing()
        summary: VideoRecheckSummary = await self._run(apply=False)
        self.assertEqual(summary.released_unmasked, 1)
        self.assertEqual(self._listing(), before)
        self.assertTrue(await self.redis.sismember(
            LOW_SUBS_RELEASED_KEY, CHANNEL_ID,
        ))
        self.assertTrue(os.path.exists(
            os.path.join(self.dir, LOW_SUBS_INDEX_FILENAME),
        ))

    async def test_new_files_indexed_in_one_transaction(self) -> None:
        await self._meta('50')
        video_id: str
        for video_id in ('aaaaaaaaaaa', 'bbbbbbbbbbb', 'ccccccccccc'):
            await self._low_subs_file(video_id=video_id)
        with patch.object(
            yt_video_scrape.LowSubsIndex, 'add',
            autospec=True,
            side_effect=yt_video_scrape.LowSubsIndex.add,
        ) as add:
            summary: VideoRecheckSummary = await self._run()
        self.assertEqual(summary.indexed, 3)
        self.assertEqual(add.call_count, 1)

    async def test_unreleased_files_not_remembered(self) -> None:
        await self._meta('50')
        await self._low_subs_file()
        captured: list[set[str]] = []
        original = yt_video_scrape._LowSubsVideoRecheck.release_channels

        async def spy(recheck: object) -> None:
            captured.append(set(recheck._handled))
            await original(recheck)

        with patch.object(
            yt_video_scrape._LowSubsVideoRecheck, 'release_channels',
            spy,
        ):
            await self._run()
        self.assertEqual(captured, [set()])

    def test_summary_line(self) -> None:
        summary: VideoRecheckSummary = VideoRecheckSummary(
            scanned_new=3, indexed=2, still_tiny=1, errors=1,
        )
        self.assertEqual(
            summary.line(apply=True),
            'recheck-low-subs-videos: scanned_new=3 indexed=2 '
            'released_masked=0 released_unmasked=0 still_tiny=1 '
            'unknown_channel=0 already_exists=0 errors=1 '
            'channels_released=0 dry_run=false',
        )


class TestRunVideoRecheck(unittest.IsolatedAsyncioTestCase):

    async def test_missing_settings(self) -> None:
        recheck: AsyncMock = AsyncMock()
        with patch.object(
            yt_video_scrape, 'recheck_low_subs_videos', recheck,
        ):
            self.assertEqual(
                await yt_video_scrape._run_video_low_subs_recheck(
                    _settings(None),
                ),
                2,
            )
            settings: MagicMock = _settings('/tmp')
            settings.redis_dsn = None
            self.assertEqual(
                await yt_video_scrape._run_video_low_subs_recheck(
                    settings,
                ),
                2,
            )
        recheck.assert_not_awaited()


class TestMain(unittest.TestCase):

    def test_main_branches_before_runner(self) -> None:
        run: AsyncMock = AsyncMock(return_value=0)
        with patch.object(
            yt_video_scrape, 'VideoSettings',
            return_value=_settings('/unused'),
        ), patch.object(
            yt_video_scrape.os, 'makedirs',
        ) as makedirs, patch.object(
            yt_video_scrape, 'ScraperRunner',
        ) as runner, patch.object(
            yt_video_scrape, '_run_video_low_subs_recheck', run,
        ), self.assertRaises(SystemExit) as raised:
            yt_video_scrape.main()
        self.assertEqual(raised.exception.code, 0)
        runner.assert_not_called()
        makedirs.assert_not_called()
        run.assert_awaited_once()

    def test_cli_flags(self) -> None:
        settings: yt_video_scrape.VideoSettings = (
            yt_video_scrape.VideoSettings(_cli_parse_args=[
                '--recheck-low-subs', 'true', '--recheck-apply', 'true',
            ])
        )
        self.assertTrue(settings.recheck_low_subs)
        self.assertTrue(settings.recheck_apply)


if __name__ == '__main__':
    unittest.main()
