#!/usr/bin/env python3
'''

Integration tests for AsyncYouTubeClient that perform real HTTP calls to
YouTube to find the channel handle of a given channel ID.

These tests hit live YouTube endpoints and may be slow or flaky depending on
network conditions. They are intended to run separately from unit tests.
'''


import unittest

from scrape_exchange.youtube.youtube_channel import YouTubeChannel


YOUTUBE_HISTORYMATTERS_CHANNEL: str = 'HistoryMatters'
YOUTUBE_HISTORYMATTERS_CHANNEL_ID: str = 'UC22BdTgxefuvUivrjesETjg'


class TestIntegration(unittest.IsolatedAsyncioTestCase):
    async def test_resolve_channel_id(self):
        channel_name: str | None = await YouTubeChannel.resolve_channel_id(
            YOUTUBE_HISTORYMATTERS_CHANNEL_ID
        )
        self.assertEqual(channel_name, YOUTUBE_HISTORYMATTERS_CHANNEL)


if __name__ == '__main__':
    unittest.main()