'''Tests that the channel scraper uses the canonical handle for upload.'''

import unittest
from unittest.mock import AsyncMock

from scrape_exchange.creator_map import NullCreatorMap


class TestChannelScraperHandleResolution(
    unittest.IsolatedAsyncioTestCase,
):
    async def test_canonical_handle_used_and_mapped(self) -> None:
        from scrape_exchange.youtube.youtube_channel import (
            YouTubeChannel,
        )
        from tools.yt_channel_scrape import (
            resolve_channel_upload_handle,
        )

        channel: YouTubeChannel = YouTubeChannel(name='INPUT_CASING')
        channel.channel_id = 'UC1234567890abcdefghij'
        channel.canonical_handle = 'InputCasing'

        cm: NullCreatorMap = NullCreatorMap()
        cm.put = AsyncMock()

        handle: str = await resolve_channel_upload_handle(channel, cm)

        self.assertEqual(handle, 'InputCasing')
        cm.put.assert_awaited_once_with(
            'UC1234567890abcdefghij', 'InputCasing',
        )

    async def test_fallback_when_no_canonical_handle(self) -> None:
        from scrape_exchange.youtube.youtube_channel import (
            YouTubeChannel,
        )
        from tools.yt_channel_scrape import (
            resolve_channel_upload_handle,
        )

        channel: YouTubeChannel = YouTubeChannel(name='Legacy Channel')
        channel.channel_id = 'UC1234567890abcdefghij'
        channel.canonical_handle = None

        cm: NullCreatorMap = NullCreatorMap()
        cm.put = AsyncMock()

        handle: str = await resolve_channel_upload_handle(channel, cm)

        self.assertEqual(handle, 'legacy channel')
        cm.put.assert_awaited_once_with(
            'UC1234567890abcdefghij', 'legacy channel',
        )
