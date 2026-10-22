'''
Privacy limits in the YouTube channel scraper: tiny channels are kept
on disk as ``.low_subs`` and marked LOW_SUBS, small channels are
masked, large channels are unchanged.
'''

import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.channel_scrape_queue import ChannelState
from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.youtube.youtube_channel import YouTubeChannel

from tests.unit.test_yt_channel_scrape_scrape_phase import (
    ScrapePhaseTestCase,
    _mock_queue,
    _mock_settings,
)


CHANNEL_ID: str = 'UCabc00000000000000000000'
FILENAME: str = f'channel-{CHANNEL_ID}.json.br'


def _channel(
    subscriber_count: int | None,
    view_count: int | None = None,
    description: str | None = 'mail jan@mail.example',
) -> YouTubeChannel:
    channel: YouTubeChannel = YouTubeChannel(
        channel_handle='handle', channel_id=CHANNEL_ID,
    )
    channel.subscriber_count = subscriber_count
    channel.view_count = view_count
    channel.description = description
    channel.video_ids = {'vid00000001'}
    return channel


class TestApplyPrivacyLimits(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self.tmp: tempfile.TemporaryDirectory = (
            tempfile.TemporaryDirectory()
        )
        self.addCleanup(self.tmp.cleanup)
        self.fm: AssetFileManagement = AssetFileManagement(self.tmp.name)

    async def _apply(self, channel: YouTubeChannel) -> bool:
        from tools.yt_channel_scrape import _apply_privacy_limits
        return await _apply_privacy_limits(
            self.fm, FILENAME, channel, _mock_settings(),
        )

    def _path(self, suffix: str = '') -> Path:
        return Path(self.tmp.name) / f'{FILENAME}{suffix}'

    async def test_tiny_channel_written_as_low_subs(self) -> None:
        self.assertTrue(await self._apply(_channel(3)))
        self.assertTrue(self._path('.low_subs').exists())
        self.assertFalse(self._path().exists())
        data: dict = await self.fm.read_file(f'{FILENAME}.low_subs')
        self.assertEqual(data['channel_id'], CHANNEL_ID)

    async def test_unknown_counts_are_tiny(self) -> None:
        self.assertTrue(await self._apply(_channel(None, None)))
        self.assertTrue(self._path('.low_subs').exists())

    async def test_unknown_subs_high_views_not_tiny(self) -> None:
        channel: YouTubeChannel = _channel(None, 2_000_000)
        self.assertFalse(await self._apply(channel))
        self.assertFalse(self._path('.low_subs').exists())
        self.assertEqual(channel.description, 'mail jan@mail.example')

    async def test_stored_count_used_when_scrape_has_none(self) -> None:
        from tools.yt_channel_scrape import _apply_privacy_limits
        channel: YouTubeChannel = _channel(None, None)
        self.assertFalse(await _apply_privacy_limits(
            self.fm, FILENAME, channel, _mock_settings(),
            stored_counts=(12_000, None),
        ))
        self.assertFalse(self._path('.low_subs').exists())
        self.assertEqual(channel.description, 'mail jan@mail.example')

    async def test_scraped_count_wins_over_stored(self) -> None:
        from tools.yt_channel_scrape import _apply_privacy_limits
        self.assertTrue(await _apply_privacy_limits(
            self.fm, FILENAME, _channel(3), _mock_settings(),
            stored_counts=(12_000, None),
        ))

    def test_end_state_uses_stored_count(self) -> None:
        from tools.yt_channel_scrape import _channel_end_state
        self.assertIsNone(_channel_end_state(
            _channel(None, None), _mock_settings(),
            stored_counts=(12_000, None),
        ))

    async def test_small_channel_masked(self) -> None:
        channel: YouTubeChannel = _channel(500)
        self.assertFalse(await self._apply(channel))
        self.assertEqual(channel.description, 'mail <email hidden>')
        self.assertFalse(self._path('.low_subs').exists())

    async def test_large_channel_unmasked(self) -> None:
        channel: YouTubeChannel = _channel(50_000)
        self.assertFalse(await self._apply(channel))
        self.assertEqual(channel.description, 'mail jan@mail.example')


class TestTypedScrapeToDisk(unittest.IsolatedAsyncioTestCase):

    async def test_tiny_raises_after_low_subs_file(self) -> None:
        from tools.yt_channel_scrape import (
            ChannelTooSmallError,
            _do_scrape_channel_to_disk_typed,
        )
        received: dict = {}

        async def fake_scrape(channel, settings, extra, **kwargs):
            received.update(kwargs)
            channel.subscriber_count = 3
            channel.channel_handle = 'handle'
            channel.video_ids = {'vid00000001'}
            return None

        settings: MagicMock = _mock_settings()
        with tempfile.TemporaryDirectory() as tmp:
            settings.channel_data_directory = tmp
            fm: AssetFileManagement = AssetFileManagement(tmp)
            with patch(
                'tools.yt_channel_scrape._try_scrape_channel_typed',
                side_effect=fake_scrape,
            ):
                with self.assertRaises(ChannelTooSmallError) as ctx:
                    await _do_scrape_channel_to_disk_typed(
                        settings, fm, 'handle', FILENAME,
                        {'channel_id': CHANNEL_ID},
                    )
            self.assertTrue(
                (Path(tmp) / f'{FILENAME}.low_subs').exists())
            self.assertFalse((Path(tmp) / FILENAME).exists())
        self.assertEqual(ctx.exception.channel.subscriber_count, 3)
        stop_if = received['stop_if']
        tiny: YouTubeChannel = _channel(3)
        unknown: YouTubeChannel = _channel(None, None)
        large: YouTubeChannel = _channel(50_000)
        self.assertTrue(stop_if(tiny))
        self.assertFalse(stop_if(unknown))
        self.assertFalse(stop_if(large))


class TestScrapeOneTooSmall(ScrapePhaseTestCase):

    @patch(
        'tools.yt_channel_scrape._channel_exists_on_exchange',
        new_callable=AsyncMock,
    )
    @patch(
        'tools.yt_channel_scrape._do_scrape_channel_to_disk_typed',
        new_callable=AsyncMock,
    )
    async def test_too_small_marks_low_subs(
        self, mock_scrape: AsyncMock, mock_exists: AsyncMock,
    ) -> None:
        from tools.yt_channel_scrape import (
            ChannelTooSmallError,
            _scrape_one_queued,
        )
        mock_exists.return_value = False
        mock_scrape.side_effect = ChannelTooSmallError(
            _channel(3, 120),
        )
        queue: AsyncMock = _mock_queue()
        creator_map: AsyncMock = AsyncMock()
        creator_map.get.return_value = 'foo'
        with patch(
            'tools.yt_channel_scrape.queue_channel_videos',
            new_callable=AsyncMock,
        ) as mock_queue_videos:
            await _scrape_one_queued(
                CHANNEL_ID,
                queue=queue,
                settings=_mock_settings(),
                fm=MagicMock(),
                creator_map_backend=creator_map,
                http_client=MagicMock(),
            )
        mock_queue_videos.assert_not_awaited()
        queue.update_tier.assert_not_awaited()
        self.assertEqual(
            queue.mark.await_args.kwargs['state'], ChannelState.LOW_SUBS)
        queue.set_meta.assert_any_await(
            f'i:{CHANNEL_ID}', subscriber_count='3', view_count='120',
        )

    @patch(
        'tools.yt_channel_scrape._channel_exists_on_exchange',
        new_callable=AsyncMock,
    )
    @patch(
        'tools.yt_channel_scrape._do_scrape_channel_to_disk_typed',
        new_callable=AsyncMock,
    )
    async def test_view_count_stored_on_success(
        self, mock_scrape: AsyncMock, mock_exists: AsyncMock,
    ) -> None:
        from tools.yt_channel_scrape import _scrape_one_queued
        mock_exists.return_value = False
        mock_scrape.return_value = _channel(50_000, 9_000_000)
        queue: AsyncMock = _mock_queue()
        creator_map: AsyncMock = AsyncMock()
        creator_map.get.return_value = 'foo'
        await _scrape_one_queued(
            CHANNEL_ID,
            queue=queue,
            settings=_mock_settings(),
            fm=MagicMock(),
            creator_map_backend=creator_map,
            http_client=MagicMock(),
        )
        queue.set_meta.assert_any_await(
            f'i:{CHANNEL_ID}',
            subscriber_count='50000', view_count='9000000',
        )


if __name__ == '__main__':
    unittest.main()
