'''Tests that the channel scraper uses channel_handle for upload.'''

import unittest
from unittest.mock import AsyncMock

from scrape_exchange.creator_map import NullCreatorMap


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

        handle: str = await resolve_channel_upload_handle(channel, cm)

        self.assertEqual(handle, 'InputCasing')
        cm.put.assert_awaited_once_with(
            'UC1234567890abcdefghij', 'InputCasing',
        )

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

        handle: str = await resolve_channel_upload_handle(channel, cm)

        self.assertEqual(handle, 'InputCasing')
        cm.put.assert_not_awaited()
