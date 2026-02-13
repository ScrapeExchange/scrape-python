#!/usr/bin/env python3
'''

Integration tests for AsyncYouTubeClient that perform real HTTP calls to
YouTube.

These tests hit live YouTube endpoints and may be slow or flaky depending on
network conditions. They are intended to run separately from unit tests.
'''


import unittest

from datetime import UTC, datetime

import orjson
import aiofiles

from scrape_exchange.youtube.youtube_channel import (
    YouTubeChannel, YouTubeChannelLink
)


from scrape_exchange.youtube.youtube_video import DENO_PATH, PO_TOKEN_URL

YOUTUBE_CHANNEL: str = 'HistoryMatters'
OUTPUT_DIR: str = 'tests/collateral/youtube_channels'


class TestIntegration(unittest.IsolatedAsyncioTestCase):
    async def test_youtube_channel(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(
            name=YOUTUBE_CHANNEL, deno_path=DENO_PATH,
            po_token_url=PO_TOKEN_URL, debug=True, save_dir=OUTPUT_DIR
        )
        await channel.scrape()
        self.assertEqual(channel.name, YOUTUBE_CHANNEL)
        self.assertEqual(channel.channel_id, 'UC22BdTgxefuvUivrjesETjg')
        self.assertEqual(channel.title, 'History Matters')
        self.assertEqual(channel.joined_date, datetime(2015, 8, 2, tzinfo=UTC))
        self.assertEqual(
            channel.rss_url,
            'https://www.youtube.com/feeds/videos.xml?channel_id=UC22BdTgxefuvUivrjesETjg'  # noqa: E501
        )
        self.assertTrue(channel.verified)
        self.assertGreaterEqual(channel.subscriber_count, 1880000)
        self.assertGreaterEqual(channel.video_count, 384)
        self.assertGreaterEqual(channel.view_count, 755841320)

        output_file: str = f'{OUTPUT_DIR}/{YOUTUBE_CHANNEL}.json'
        async with aiofiles.open(output_file, 'w') as f:
            await f.write(
                orjson.dumps(
                    channel.to_dict(), option=orjson.OPT_INDENT_2
                ).decode('utf-8'))

        async with aiofiles.open(output_file, 'r') as f:
            content: str = await f.read()
            channel_data: dict[str, any] = orjson.loads(content)

            loaded_channel: YouTubeChannel = YouTubeChannel.from_dict(
                channel_data
            )
            self.assertEqual(loaded_channel, channel)

    async def test_channel_links(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(
            name='ComedyCentral', deno_path=DENO_PATH,
            po_token_url=PO_TOKEN_URL, debug=True, save_dir=OUTPUT_DIR
        )
        await channel.scrape()
        self.assertGreaterEqual(len(channel.channel_links), 10)



if __name__ == '__main__':
    unittest.main()
