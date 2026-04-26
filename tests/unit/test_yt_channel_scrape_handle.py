'''Tests that the channel scraper uses channel_handle for upload.'''

import unittest
from unittest.mock import AsyncMock

from scrape_exchange.creator_map import NullCreatorMap
from scrape_exchange.name_map import NullNameMap


class TestChannelScraperHandleResolution(
    unittest.IsolatedAsyncioTestCase,
):
    async def test_channel_handle_used_and_mapped(self) -> None:
        from scrape_exchange.youtube.youtube_channel import (
            YouTubeChannel,
        )
        from tools.yt_channel_scrape import (
            resolve_channel_upload_handle,
        )

        channel: YouTubeChannel = YouTubeChannel(
            channel_handle='InputCasing',
        )
        channel.channel_id = 'UC1234567890abcdefghij'

        cm: NullCreatorMap = NullCreatorMap()
        cm.put = AsyncMock()
        nm: NullNameMap = NullNameMap()
        nm.put = AsyncMock()

        handle: str = await resolve_channel_upload_handle(
            channel, cm, nm,
        )

        self.assertEqual(handle, 'InputCasing')
        cm.put.assert_awaited_once_with(
            'UC1234567890abcdefghij', 'InputCasing',
        )
        # No title set on the channel, so name_map is not written.
        nm.put.assert_not_awaited()

    async def test_no_creator_map_write_without_channel_id(
        self,
    ) -> None:
        from scrape_exchange.youtube.youtube_channel import (
            YouTubeChannel,
        )
        from tools.yt_channel_scrape import (
            resolve_channel_upload_handle,
        )

        channel: YouTubeChannel = YouTubeChannel(
            channel_handle='InputCasing',
        )
        channel.channel_id = None

        cm: NullCreatorMap = NullCreatorMap()
        cm.put = AsyncMock()
        nm: NullNameMap = NullNameMap()
        nm.put = AsyncMock()

        handle: str = await resolve_channel_upload_handle(
            channel, cm, nm,
        )

        self.assertEqual(handle, 'InputCasing')
        cm.put.assert_not_awaited()
        nm.put.assert_not_awaited()
