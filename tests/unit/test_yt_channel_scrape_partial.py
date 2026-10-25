'''
A partial channel scrape is written with only the keys it fetched and
a loss-free video_ids list, then the channel is retried soon instead
of being counted as scraped.
'''

import json
import tempfile
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.channel_scrape_queue import (
    LISTING_RESUME_FIELD,
    VIDEO_IDS_ENUMERATED_FIELD,
    ChannelScrapeProgress,
)
from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.youtube.channel_video_refresh import FilterQueryError
from scrape_exchange.youtube.youtube_channel import (
    ABOUT_FIELDS,
    YouTubeChannel,
)
from tools import yt_channel_scrape as scraper
from tools.yt_channel_scrape import ChannelPartialScrapeError


CHANNEL_ID: str = 'UCabc00000000000000000000'
MEMBER: str = f'i:{CHANNEL_ID}'
FILENAME: str = f'channel-{CHANNEL_ID}.json.br'


def _settings(directory: str) -> MagicMock:
    settings: MagicMock = MagicMock()
    settings.exchange_url = 'https://scrape.exchange'
    settings.channel_data_directory = directory
    settings.video_data_directory = directory
    settings.channel_min_subscribers = 10
    settings.channel_privacy_subscribers = 4000
    settings.channel_views_per_subscriber = 100
    settings.channel_first_scrape_max_videos = 200
    settings.extra_allowed_domains = frozenset()
    return settings


def _partial_channel() -> YouTubeChannel:
    channel: YouTubeChannel = YouTubeChannel(
        'channel', channel_id=CHANNEL_ID, with_download_client=False,
    )
    channel.title = 'Fresh title'
    channel.subscriber_count = 1_000_000
    channel.fetched_fields = set(ABOUT_FIELDS)
    channel.partial_error = 'Incomplete channel content: boom'
    channel.video_ids_complete = False
    channel.video_listing_complete = False
    return channel


class TestWritePartialRecord(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self.directory: tempfile.TemporaryDirectory = (
            tempfile.TemporaryDirectory()
        )
        self.addCleanup(self.directory.cleanup)
        self.fm: AssetFileManagement = AssetFileManagement(
            self.directory.name,
        )
        self.channel: YouTubeChannel = _partial_channel()
        self.video_ids: AsyncMock = AsyncMock(return_value=set())
        self.privacy: AsyncMock = AsyncMock(return_value=False)
        for target, value in (
            ('YouTubeChannel', MagicMock(return_value=self.channel)),
            ('_try_scrape_channel_typed', AsyncMock(return_value=None)),
            ('_reject_topic_channel_if_needed',
             AsyncMock(return_value=False)),
            ('_apply_privacy_limits', self.privacy),
            ('fetch_exchange_video_ids', self.video_ids),
        ):
            patcher = patch.object(scraper, target, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    async def _scrape(self) -> None:
        await scraper._do_scrape_channel_to_disk_typed(
            _settings(self.directory.name), self.fm, 'channel', FILENAME,
            {'channel_id': CHANNEL_ID}, http_client=AsyncMock(),
        )

    async def test_partial_record_written_and_raised(self) -> None:
        await self.fm.write_file(FILENAME, {
            'channel_id': CHANNEL_ID, 'title': 'Old title',
            'video_ids': ['old1'],
        })
        with self.assertRaises(ChannelPartialScrapeError) as ctx:
            await self._scrape()
        self.assertIs(ctx.exception.channel, self.channel)
        data: dict = await self.fm.read_file(FILENAME)
        self.assertEqual(data['title'], 'Fresh title')
        self.assertEqual(data['video_ids'], ['old1'])
        self.assertNotIn('playlists', data)
        self.assertNotIn('merch', data)

    async def test_new_channel_metadata_only_written(self) -> None:
        # No content at all: the no-content check must not stop it.
        with self.assertRaises(ChannelPartialScrapeError):
            await self._scrape()
        data: dict = await self.fm.read_file(FILENAME)
        self.assertEqual(data['title'], 'Fresh title')
        self.assertEqual(data['video_ids'], [])

    async def test_lookup_failure_writes_nothing(self) -> None:
        self.video_ids.side_effect = FilterQueryError('down')
        with self.assertRaises(RuntimeError) as ctx:
            await self._scrape()
        self.assertNotIsInstance(ctx.exception, ChannelPartialScrapeError)
        with self.assertRaises(Exception):
            await self.fm.read_file(FILENAME)

    async def test_privacy_limits_still_apply(self) -> None:
        self.privacy.return_value = True
        with self.assertRaises(scraper.ChannelTooSmallError):
            await self._scrape()
        self.video_ids.assert_not_awaited()


class TestScrapeOnePartial(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self.directory: tempfile.TemporaryDirectory = (
            tempfile.TemporaryDirectory()
        )
        self.addCleanup(self.directory.cleanup)
        self.queue: AsyncMock = AsyncMock()
        self.queue.get_scrape_progress.return_value = (
            ChannelScrapeProgress(1, 11)
        )
        self.queue.get_meta.return_value = {}
        self.queue.update_tier.return_value = True
        self.queue.mark_partial.return_value = True
        self.channel: MagicMock = MagicMock()
        self.channel.channel_id = CHANNEL_ID
        self.channel.channel_handle = 'channel'
        self.channel.title = 'Channel'
        self.channel.subscriber_count = 1_000_000
        self.channel.view_count = None
        self.channel.video_count = 50
        self.channel.video_ids = {'v1'}
        self.channel.video_ids_complete = False
        self.channel.video_listing_complete = False
        self.channel.listing_state = {'videos': 'token1'}
        self.channel.partial_error = 'Failed channel tabs: videos'
        self.mocks: dict[str, MagicMock] = {}
        for name, value in (
            ('_channel_exists_on_exchange', AsyncMock(return_value=True)),
            ('_do_scrape_channel_to_disk_typed', AsyncMock(
                side_effect=ChannelPartialScrapeError(self.channel),
            )),
            ('queue_channel_videos', AsyncMock()),
            ('_enqueue_discovered_channel_links', AsyncMock()),
            ('_add_channel_to_rss_queue', AsyncMock()),
            ('CHANNEL_SCRAPE_OUTCOMES', MagicMock()),
            ('CHANNEL_FORCE_RESCRAPE_TOTAL', MagicMock()),
        ):
            patcher = patch.object(scraper, name, value)
            self.mocks[name] = patcher.start()
            self.addCleanup(patcher.stop)

    async def _run(self) -> None:
        creator_map: AsyncMock = AsyncMock()
        creator_map.get.return_value = 'channel'
        await scraper._scrape_one_queued(
            CHANNEL_ID, queue=self.queue,
            settings=_settings(self.directory.name), fm=MagicMock(),
            creator_map_backend=creator_map, http_client=MagicMock(),
        )

    async def test_partial_queues_ids_and_retries_soon(self) -> None:
        await self._run()
        self.mocks['queue_channel_videos'].assert_awaited_once()
        self.mocks['_enqueue_discovered_channel_links'].assert_awaited()
        self.queue.mark_partial.assert_awaited_once()
        call = self.queue.mark_partial.await_args
        self.assertEqual(call.args, (CHANNEL_ID,))
        self.assertEqual(
            call.kwargs['last_error'], 'Failed channel tabs: videos',
        )
        self.queue.mark_soft_unavailable.assert_not_awaited()
        self.queue.update_tier.assert_not_awaited()
        self.queue.clear_force_rescrape.assert_not_awaited()
        self.mocks['_add_channel_to_rss_queue'].assert_not_awaited()
        self.mocks['CHANNEL_SCRAPE_OUTCOMES'].labels.assert_called_with(
            outcome='partial',
        )

    async def test_partial_saves_resume_point_only(self) -> None:
        await self._run()
        fields: set[str] = set()
        for call in self.queue.set_meta.await_args_list:
            fields |= set(call.kwargs)
        self.assertNotIn(VIDEO_IDS_ENUMERATED_FIELD, fields)
        self.assertIn(LISTING_RESUME_FIELD, fields)
        saved: list[str] = [
            call.kwargs[LISTING_RESUME_FIELD]
            for call in self.queue.set_meta.await_args_list
            if LISTING_RESUME_FIELD in call.kwargs
        ]
        self.assertEqual(json.loads(saved[-1]), {'videos': 'token1'})

    async def test_partial_keeps_force_flag(self) -> None:
        self.queue.get_meta.return_value = {
            VIDEO_IDS_ENUMERATED_FIELD: '1790000000',
            'force_rescrape_mode': 'full',
        }
        await self._run()
        self.queue.clear_force_rescrape.assert_not_awaited()
        self.mocks[
            'CHANNEL_FORCE_RESCRAPE_TOTAL'
        ].labels.assert_called_with(mode='full', outcome='partial')


    async def test_used_up_retries_count_as_success(self) -> None:
        self.queue.mark_partial.return_value = False
        await self._run()
        self.queue.update_tier.assert_awaited_once()
        self.queue.mark_soft_unavailable.assert_not_awaited()
        fields: set[str] = set()
        for call in self.queue.set_meta.await_args_list:
            fields |= set(call.kwargs)
        # The listing is still incomplete: never marked enumerated.
        self.assertNotIn(VIDEO_IDS_ENUMERATED_FIELD, fields)
        self.assertNotIn(
            'partial',
            [c.kwargs.get('outcome') for c in
             self.mocks['CHANNEL_SCRAPE_OUTCOMES'].labels.call_args_list],
        )


if __name__ == '__main__':
    unittest.main()
