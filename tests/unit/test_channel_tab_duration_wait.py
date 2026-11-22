'''channel_tab_scrape_duration_seconds leaves out rate-limiter waits,
and those waits still reach the channel scrape's timer when the tab
runs in a child task.'''

import asyncio
import unittest
from typing import Any
from unittest.mock import MagicMock, patch

from scrape_exchange.wait_tracking import ScrapeTimer, tracked_sleep
from scrape_exchange.youtube.youtube_channel_tabs import YouTubeChannelTabs

CHANNEL_ID: str = 'UCtest0000000000000000aa'

TAB_CONTENT: dict[str, Any] = {
    'content': {
        'richGridRenderer': {
            'contents': [{
                'richItemRenderer': {
                    'content': {'videoRenderer': {'videoId': 'vid1'}},
                },
            }],
        },
    },
}


class TestTabDurationWait(unittest.IsolatedAsyncioTestCase):

    async def test_tab_duration_excludes_wait(self) -> None:
        tabs: YouTubeChannelTabs = YouTubeChannelTabs(CHANNEL_ID)
        tab_renderer: dict[str, Any] = {
            'title': 'Videos',
            'endpoint': {'browseEndpoint': {'params': 'videos_p'}},
        }

        async def browse(**kwargs: Any) -> dict[str, Any]:
            # Stands in for limiter.acquire() waiting for a token.
            await tracked_sleep(0.2)
            return {}

        duration: MagicMock = MagicMock()
        channel_timer: ScrapeTimer = ScrapeTimer.start()
        with patch.object(
            tabs, '_browse', side_effect=browse,
        ), patch.object(
            tabs, 'get_tab', return_value=TAB_CONTENT,
        ), patch(
            'scrape_exchange.youtube.youtube_channel_tabs'
            '.CHANNEL_TAB_DURATION', duration,
        ):
            video_ids, *_ = await asyncio.create_task(
                tabs._scrape_tab(tab_renderer, 'videos'),
            )

        self.assertEqual(video_ids, {'vid1'})
        observed: float = (
            duration.labels.return_value.observe.call_args.args[0]
        )
        self.assertLess(observed, 0.15)
        self.assertGreaterEqual(channel_timer.waited(), 0.2)
        self.assertLess(channel_timer.active(), 0.15)


if __name__ == '__main__':
    unittest.main()
