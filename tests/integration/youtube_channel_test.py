#!/usr/bin/env python3
'''

Integration tests for AsyncYouTubeClient that perform real HTTP calls to
YouTube.

These tests hit live YouTube endpoints and may be slow or flaky depending on
network conditions. They are intended to run separately from unit tests.
'''


import os
import shutil
import unittest
import tempfile

from datetime import UTC, datetime

import orjson
import aiofiles
from scrape_exchange.youtube.youtube_channel import YouTubeChannel

from scrape_exchange.youtube.youtube_video import DENO_PATH, PO_TOKEN_URL

YOUTUBE_CHANNEL: str = 'HistoryMatters'
OUTPUT_DIR: str = 'tests/collateral/youtube_channels'


class TestIntegration(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.api_key_id: str = os.environ.get('API_KEY_ID', '')
        self.api_key_secret: str = os.environ.get('API_KEY_SECRET', '')
        self.api_base_url: str = os.environ.get(
            'API_BASE_URL', 'https://scrape.exchange'
        )
        self.temp_dir: str = tempfile.mkdtemp()
        os.mkdir(f'{self.temp_dir}/uploaded')

    async def asyncTearDown(self) -> None:
        if self.temp_dir and os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    async def test_youtube_channel(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(
            name=YOUTUBE_CHANNEL, deno_path=DENO_PATH,
            po_token_url=PO_TOKEN_URL, debug=True, save_dir=OUTPUT_DIR
        )
        await channel.scrape_about_page()
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
        await channel.scrape_about_page()
        self.assertGreaterEqual(len(channel.channel_links), 10)

    async def test_videos_scrape(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(
            name=YOUTUBE_CHANNEL, deno_path=DENO_PATH,
            po_token_url=PO_TOKEN_URL, debug=True, save_dir=OUTPUT_DIR
        )
        await channel.scrape_videos(
            max_videos_per_channel=3, save_dir=self.temp_dir
        )
        self.assertGreaterEqual(len(channel.video_ids), 3)
        video_count: int = 0
        for video_id in channel.video_ids:
            file_path: str = os.path.join(self.temp_dir, f'{video_id}.json')
            if os.path.exists(file_path):
                video_count += 1
        self.assertEqual(video_count, 3)

    # @unittest.skip('Shorts scraping is currently unreliable and needs fixes')
    async def test_shorts_scrape(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(
            name='GMHikaru', deno_path=DENO_PATH,
            po_token_url=PO_TOKEN_URL, debug=True, save_dir=OUTPUT_DIR
        )
        await channel.scrape_videos(
            max_videos_per_channel=3, save_dir=self.temp_dir,
            shorts=True
        )
        self.assertGreaterEqual(len(channel.video_ids), 3)
        video_count: int = 0
        for video_id in channel.video_ids:
            file_path: str = os.path.join(self.temp_dir, f'{video_id}.json')
            if os.path.exists(file_path):
                video_count += 1
        self.assertEqual(video_count, 3)


if __name__ == '__main__':
    unittest.main()
