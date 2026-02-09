#!/usr/bin/env python3
'''

Integration tests for AsyncYouTubeClient that perform real HTTP calls to
YouTube.

These tests hit live YouTube endpoints and may be slow or flaky depending on
network conditions. They are intended to run separately from unit tests.
'''

import unittest

from scrape_exchange.youtube.youtube_client import AsyncYouTubeClient


YOUTUBE_HOME = 'https://www.youtube.com'
YOUTUBE_VIDEO = 'https://www.youtube.com/watch?v=dQw4w9WgXcQ'


class TestIntegration(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_youtube_homepage(self) -> None:
        async with AsyncYouTubeClient() as client:
            body: str | None = await client.get(YOUTUBE_HOME, timeout=15)

        self.assertIsInstance(body, str)
        self.assertGreater(len(body), 100)
        self.assertIn('<html', body.lower())

    async def test_fetch_youtube_video_page(self) -> None:
        async with AsyncYouTubeClient() as client:
            body: str | None = await client.get(YOUTUBE_VIDEO, timeout=15)

        self.assertIsInstance(body, str)
        self.assertGreater(len(body), 100)
        self.assertTrue('watch' in body.lower() or '<html' in body.lower())


if __name__ == '__main__':
    unittest.main()
