'''
A channel scrape stops before enumerating the video tabs once
``stop_if`` says the channel is too small.
'''

import asyncio
import tempfile
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.youtube.youtube_channel import YouTubeChannel


CHANNEL_ID: str = 'UCabc00000000000000000000'


def _channel() -> YouTubeChannel:
    channel: YouTubeChannel = YouTubeChannel(
        channel_handle='handle', channel_id=CHANNEL_ID,
    )
    channel.browse_client = MagicMock()
    channel.description = None
    return channel


def _tabs_mock() -> MagicMock:
    tabs: MagicMock = MagicMock()
    tabs.browse_channel = AsyncMock(return_value={})
    tabs.scrape_loaded_tabs = AsyncMock(
        return_value=(set(), set(), set(), set(), set(), set()),
    )
    tabs.enumeration_complete = True
    return tabs


def _set_small_count(channel: YouTubeChannel, _page: dict) -> None:
    channel.subscriber_count = 3


class TestStopIfContent(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self.tmp: tempfile.TemporaryDirectory = (
            tempfile.TemporaryDirectory()
        )
        self.addCleanup(self.tmp.cleanup)
        self.tabs: MagicMock = _tabs_mock()
        patchers = (
            patch(
                'scrape_exchange.youtube.youtube_channel'
                '.YouTubeChannelTabs',
                return_value=self.tabs,
            ),
            patch.object(
                YouTubeChannel, 'parse_channel_video_data',
                autospec=True, side_effect=_set_small_count,
            ),
        )
        for patcher in patchers:
            patcher.start()
            self.addCleanup(patcher.stop)

    async def test_stops_before_tab_enumeration(self) -> None:
        channel: YouTubeChannel = _channel()
        await channel.scrape_channel_content(
            save_dir=self.tmp.name,
            stop_if=lambda c: c.subscriber_count == 3,
        )
        self.tabs.scrape_loaded_tabs.assert_not_awaited()
        self.assertTrue(channel.scrape_stopped_early)

    async def test_no_stop_if_keeps_behaviour(self) -> None:
        channel: YouTubeChannel = _channel()
        await channel.scrape_channel_content(save_dir=self.tmp.name)
        self.tabs.scrape_loaded_tabs.assert_awaited_once()
        self.assertFalse(channel.scrape_stopped_early)

    async def test_stop_if_false_keeps_behaviour(self) -> None:
        channel: YouTubeChannel = _channel()
        await channel.scrape_channel_content(
            save_dir=self.tmp.name, stop_if=lambda c: False,
        )
        self.tabs.scrape_loaded_tabs.assert_awaited_once()
        self.assertFalse(channel.scrape_stopped_early)


class TestStopIfAbout(unittest.IsolatedAsyncioTestCase):

    async def test_stop_after_about_cancels_content_task(self) -> None:
        channel: YouTubeChannel = _channel()
        started: asyncio.Event = asyncio.Event()
        cancelled: list[bool] = []

        async def about(*_args, **_kwargs) -> None:
            await started.wait()
            channel.subscriber_count = 3

        async def content(*_args, **_kwargs) -> int:
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.append(True)
                raise
            return 0

        with tempfile.TemporaryDirectory() as tmp, \
                patch.object(channel, 'scrape_about_page', about), \
                patch.object(channel, 'scrape_channel_content', content):
            await asyncio.wait_for(channel.scrape(
                save_dir=tmp, with_about_page=True,
                stop_if=lambda c: c.subscriber_count == 3,
            ), timeout=5)
        self.assertTrue(channel.scrape_stopped_early)
        self.assertEqual(cancelled, [True])

    async def test_outer_cancellation_is_not_swallowed(self) -> None:
        channel: YouTubeChannel = _channel()
        child_cancelled: asyncio.Event = asyncio.Event()
        release: asyncio.Event = asyncio.Event()

        async def slow_to_cancel() -> int:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                child_cancelled.set()
                await release.wait()
                raise
            return 0

        content_task: asyncio.Task[int] = asyncio.create_task(
            slow_to_cancel(),
        )
        await asyncio.sleep(0)
        outer: asyncio.Task[None] = asyncio.create_task(
            channel._stop_content_task(content_task),
        )
        await child_cancelled.wait()
        outer.cancel()
        await asyncio.sleep(0)
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await outer

    async def test_discarded_enumeration_error_is_ignored(self) -> None:
        channel: YouTubeChannel = _channel()

        async def failing() -> int:
            raise ConnectionError('proxy dropped')

        content_task: asyncio.Task[int] = asyncio.create_task(failing())
        await asyncio.sleep(0)
        await channel._stop_content_task(content_task)
        self.assertTrue(channel.scrape_stopped_early)

    async def test_stop_if_passed_to_content(self) -> None:
        channel: YouTubeChannel = _channel()
        received: list[object] = []

        async def content(*_args, **kwargs) -> int:
            received.append(kwargs.get('stop_if'))
            return 0

        def stop_if(_channel: YouTubeChannel) -> bool:
            return False

        with tempfile.TemporaryDirectory() as tmp, \
                patch.object(channel, 'scrape_channel_content', content):
            await channel.scrape(save_dir=tmp, stop_if=stop_if)
        self.assertEqual(received, [stop_if])
        self.assertFalse(channel.scrape_stopped_early)


if __name__ == '__main__':
    unittest.main()
