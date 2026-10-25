'''
A channel scrape that collects useful data while part of the content
fetch fails returns a partial result instead of raising, and records
which record keys it fetched.
'''

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.schema_validator import SchemaValidator
from scrape_exchange.youtube.channel_errors import ChannelTerminatedError
from scrape_exchange.youtube.youtube_channel import (
    ABOUT_FIELDS,
    INNERTUBE_FIELDS,
    TAB_RECORD_FIELDS,
    YouTubeChannel,
)
from scrape_exchange.youtube.youtube_playlist import YouTubePlaylist


CHANNEL_ID: str = 'UCabc00000000000000000000'
SCHEMA_PATH: Path = (
    Path(__file__).parent.parent
    / 'collateral' / 'boinko-youtube-channel-schema.json'
)


def _channel() -> YouTubeChannel:
    channel: YouTubeChannel = YouTubeChannel(
        channel_handle='handle', channel_id=CHANNEL_ID,
    )
    channel.browse_client = MagicMock()
    return channel


def _tabs_mock(failed: set[str] | None = None) -> MagicMock:
    tabs: MagicMock = MagicMock()
    tabs.browse_channel = AsyncMock(return_value={})
    tabs.scrape_loaded_tabs = AsyncMock(
        return_value=({'vid00000001'}, set(), set(), set(), set(), set()),
    )
    tabs.failed_tabs = set(failed or set())
    tabs.enumeration_complete = not failed
    tabs.listing_state = {}
    tabs.listing_complete = False
    return tabs


async def _about_ok(channel: YouTubeChannel) -> None:
    channel.title = 'Title'
    channel.description = 'About description'


async def _about_fails(channel: YouTubeChannel) -> None:
    raise RuntimeError('about failed')


class PartialScrapeTestCase(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self.tmp: tempfile.TemporaryDirectory = (
            tempfile.TemporaryDirectory()
        )
        self.addCleanup(self.tmp.cleanup)
        patcher = patch.object(
            YouTubeChannel, 'parse_channel_video_data', autospec=True,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    async def _scrape(
        self, channel: YouTubeChannel, tabs: MagicMock, about,
    ) -> None:
        async def scrape_about(*_args, **_kwargs) -> None:
            await about(channel)

        with patch(
            'scrape_exchange.youtube.youtube_channel.YouTubeChannelTabs',
            return_value=tabs,
        ), patch.object(channel, 'scrape_about_page', scrape_about):
            await channel.scrape(
                save_dir=self.tmp.name, with_about_page=True,
                require_complete_video_ids=True,
            )


class TestPartialScrape(PartialScrapeTestCase):

    async def test_complete_scrape_is_not_partial(self) -> None:
        channel: YouTubeChannel = _channel()
        await self._scrape(channel, _tabs_mock(), _about_ok)
        self.assertIsNone(channel.partial_error)
        self.assertEqual(
            channel.fetched_fields,
            ABOUT_FIELDS | INNERTUBE_FIELDS | TAB_RECORD_FIELDS,
        )

    async def test_failed_tab_makes_scrape_partial(self) -> None:
        channel: YouTubeChannel = _channel()
        await self._scrape(
            channel, _tabs_mock(failed={'playlists'}), _about_ok,
        )
        self.assertIsNotNone(channel.partial_error)
        self.assertTrue(channel.content_partial)
        self.assertNotIn('playlists', channel.fetched_fields)
        self.assertIn('merch', channel.fetched_fields)
        self.assertIn('video_ids', channel.fetched_fields)

    async def test_failed_video_tab_keeps_video_ids(self) -> None:
        channel: YouTubeChannel = _channel()
        await self._scrape(
            channel, _tabs_mock(failed={'videos'}), _about_ok,
        )
        self.assertIsNotNone(channel.partial_error)
        self.assertIn('video_ids', channel.fetched_fields)
        self.assertEqual(channel.video_ids, {'vid00000001'})
        self.assertFalse(channel.video_ids_complete)

    async def test_browse_failure_with_about_is_partial(self) -> None:
        channel: YouTubeChannel = _channel()
        tabs: MagicMock = _tabs_mock()
        tabs.browse_channel = AsyncMock(side_effect=RuntimeError('boom'))
        await self._scrape(channel, tabs, _about_ok)
        self.assertIsNotNone(channel.partial_error)
        self.assertTrue(channel.content_partial)
        self.assertEqual(channel.fetched_fields, ABOUT_FIELDS)
        self.assertFalse(channel.video_ids_complete)
        self.assertFalse(channel.video_listing_complete)

    async def test_about_failure_makes_scrape_partial(self) -> None:
        channel: YouTubeChannel = _channel()
        await self._scrape(channel, _tabs_mock(), _about_fails)
        self.assertIsNotNone(channel.partial_error)
        self.assertFalse(channel.content_partial)
        self.assertEqual(
            channel.fetched_fields, INNERTUBE_FIELDS | TAB_RECORD_FIELDS,
        )
        self.assertTrue(channel.video_ids_complete)

    async def test_no_about_page_requested_is_not_partial(self) -> None:
        channel: YouTubeChannel = _channel()
        with patch(
            'scrape_exchange.youtube.youtube_channel.YouTubeChannelTabs',
            return_value=_tabs_mock(),
        ):
            await channel.scrape(
                save_dir=self.tmp.name, with_about_page=False,
                require_complete_video_ids=True,
            )
        self.assertIsNone(channel.partial_error)

    async def test_browse_and_about_failure_raises(self) -> None:
        channel: YouTubeChannel = _channel()
        tabs: MagicMock = _tabs_mock()
        tabs.browse_channel = AsyncMock(side_effect=RuntimeError('boom'))
        with self.assertRaises(RuntimeError):
            await self._scrape(channel, tabs, _about_fails)
        self.assertIsNone(channel.partial_error)

    async def test_channel_error_in_browse_propagates(self) -> None:
        channel: YouTubeChannel = _channel()
        tabs: MagicMock = _tabs_mock()
        tabs.browse_channel = AsyncMock(
            side_effect=ChannelTerminatedError('gone'),
        )
        with self.assertRaises(ChannelTerminatedError):
            await self._scrape(channel, tabs, _about_ok)

    async def test_channel_error_in_tabs_is_not_partial(self) -> None:
        channel: YouTubeChannel = _channel()
        tabs: MagicMock = _tabs_mock()
        tabs.scrape_loaded_tabs = AsyncMock(
            side_effect=ChannelTerminatedError('gone'),
        )
        with self.assertRaises(RuntimeError):
            await self._scrape(channel, tabs, _about_ok)
        self.assertIsNone(channel.partial_error)


class TestOnlyFetched(unittest.TestCase):

    def test_only_fetched_omits_unfetched_keys(self) -> None:
        channel: YouTubeChannel = _channel()
        channel.title = 'Title'
        channel.video_ids = {'vid00000001'}
        channel.fetched_fields = {'title'}
        data: dict = channel.to_dict(only_fetched=True, with_video_ids=True)
        self.assertEqual(
            set(data),
            {'channel_id', 'channel_handle', 'url', 'title', 'video_ids'},
        )

    def test_finished_empty_tab_emits_empty_list(self) -> None:
        channel: YouTubeChannel = _channel()
        channel.fetched_fields = {'posts'}
        self.assertEqual(channel.to_dict(only_fetched=True)['posts'], [])

    def test_unset_count_is_omitted(self) -> None:
        channel: YouTubeChannel = _channel()
        channel.fetched_fields = {'subscriber_count'}
        self.assertNotIn(
            'subscriber_count', channel.to_dict(only_fetched=True),
        )

    def test_full_to_dict_is_unchanged(self) -> None:
        channel: YouTubeChannel = _channel()
        channel.fetched_fields = {'title'}
        self.assertIn('playlists', channel.to_dict())

    def test_partial_record_passes_schema(self) -> None:
        channel: YouTubeChannel = _channel()
        channel.title = 'Title'
        channel.description = None
        channel.playlists = {YouTubePlaylist(
            playlist_id='PL1', title='Playlist', video_count=1,
            channel_id=CHANNEL_ID,
        )}
        channel.video_ids = {'vid00000001'}
        channel.fetched_fields = ABOUT_FIELDS | {'playlists', 'video_ids'}
        with open(SCHEMA_PATH) as schema_file:
            validator: SchemaValidator = SchemaValidator(
                json.load(schema_file),
            )
        self.assertIsNone(validator.validate(
            channel.to_dict(only_fetched=True, with_video_ids=True),
        ))



class TestRoundTripKeepsKeys(unittest.TestCase):

    def test_partial_file_round_trips_to_its_own_keys(self) -> None:
        data: dict = {
            'channel_id': CHANNEL_ID, 'channel_handle': 'handle',
            'url': f'https://www.youtube.com/channel/{CHANNEL_ID}',
            'title': 'Title', 'posts': [], 'video_ids': ['v1'],
        }
        channel: YouTubeChannel = YouTubeChannel.from_dict(data)
        self.assertEqual(
            set(channel.to_dict(only_fetched=True, with_video_ids=True)),
            set(data),
        )
        self.assertEqual(
            set(channel.to_dict(only_fetched=True)), set(data) - {'video_ids'},
        )

    def test_full_file_round_trips_to_every_key(self) -> None:
        full: dict = _channel().to_dict(with_video_ids=True)
        channel: YouTubeChannel = YouTubeChannel.from_dict(full)
        self.assertEqual(
            set(channel.to_dict(only_fetched=True, with_video_ids=True)),
            set(full),
        )


if __name__ == '__main__':
    unittest.main()
