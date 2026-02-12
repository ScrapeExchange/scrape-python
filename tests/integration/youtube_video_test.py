#!/usr/bin/env python3
'''

Integration tests for AsyncYouTubeClient that perform real HTTP calls to
YouTube.

These tests hit live YouTube endpoints and may be slow or flaky depending on
network conditions. They are intended to run separately from unit tests.
'''

import os
import unittest

import orjson
import aiofiles

from scrape_exchange.youtube.youtube_video import YouTubeVideo

YOUTUBE_VIDEO_ID: str = 'dQw4w9WgXcQ'
OUTPUT_DIR: str = 'tests/collateral/youtube_videos'

DENO_PATH: str = os.environ.get('HOME', '') + '/.deno/bin/deno'
PO_TOKEN_URL: str = 'http://localhost:4416'


class TestIntegration(unittest.IsolatedAsyncioTestCase):
    async def test_youtube_video(self) -> None:

        video: YouTubeVideo = await YouTubeVideo.scrape(
            video_id=YOUTUBE_VIDEO_ID,
            channel_name='Rick Astley', channel_thumbnail=None,
            deno_path=DENO_PATH, po_token_url=PO_TOKEN_URL, debug=True,
            save_dir=OUTPUT_DIR
        )
        self.assertEqual(video.video_id, YOUTUBE_VIDEO_ID)

        async with aiofiles.open(
                f'{OUTPUT_DIR}/{YOUTUBE_VIDEO_ID}.json', 'r') as f:
            video_data: str = orjson.loads(await f.read())
            loaded_video: YouTubeVideo = YouTubeVideo.from_dict(video_data)
            self.assertEqual(video, loaded_video)


if __name__ == '__main__':
    unittest.main()
