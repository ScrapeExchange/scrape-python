'''Tests for channel queue Prometheus tier labels.'''

import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, call, patch

from tools import yt_channel_scrape


class TestPublishChannelQueueSizes(
    unittest.IsolatedAsyncioTestCase,
):
    async def test_publishes_one_based_tier_labels(self) -> None:
        '''Internal channel tiers 0 and 1 are exported as 1 and 2.'''
        queue: MagicMock = MagicMock()
        queue.count_by_state = AsyncMock(return_value={})
        queue.count_by_tier = AsyncMock(
            return_value={0: 7, 1: 11},
        )
        queue.count_listing = AsyncMock(return_value={})
        shutdown_event: asyncio.Event = asyncio.Event()
        metric: MagicMock = MagicMock()
        metric.labels.return_value.set.side_effect = (
            lambda _count: shutdown_event.set()
        )

        with patch.object(
            yt_channel_scrape,
            'CHANNEL_QUEUE_TIER_SIZE_NEW',
            metric,
        ):
            await yt_channel_scrape._publish_channel_queue_sizes(
                queue,
                interval=1.0,
                shutdown_event=shutdown_event,
            )

        self.assertEqual(
            metric.labels.call_args_list,
            [call(tier='1'), call(tier='2')],
        )
        self.assertEqual(
            metric.labels.return_value.set.call_args_list,
            [call(7), call(11)],
        )

    async def test_publishes_listing_status_per_tier(self) -> None:
        '''Every tier gets all four statuses; missing ones are 0.'''
        queue: MagicMock = MagicMock()
        queue.count_by_state = AsyncMock(return_value={})
        queue.count_by_tier = AsyncMock(return_value={0: 3})
        queue.count_listing = AsyncMock(return_value={
            (0, 'complete'): 2, (0, 'partial'): 1, (1, 'partial'): 4,
        })
        shutdown_event: asyncio.Event = asyncio.Event()
        shutdown_event.set()
        published: dict[tuple[str, str], int] = {}

        def _labels(*, tier: str, status: str) -> MagicMock:
            child: MagicMock = MagicMock()
            child.set.side_effect = (
                lambda n: published.__setitem__((tier, status), n)
            )
            return child

        metric: MagicMock = MagicMock()
        metric.labels.side_effect = _labels
        with patch.object(
            yt_channel_scrape, 'CHANNEL_VIDEO_LISTING_STATUS', metric,
        ):
            shutdown_event.clear()
            queue.count_by_state.side_effect = (
                lambda: shutdown_event.set() or {}
            )
            await yt_channel_scrape._publish_channel_queue_sizes(
                queue, interval=1.0, shutdown_event=shutdown_event,
            )

        self.assertEqual(published, {
            ('1', 'complete'): 2, ('1', 'partial'): 1,
            ('1', 'pre_tracking'): 0, ('1', 'never_scraped'): 0,
            ('2', 'complete'): 0, ('2', 'partial'): 4,
            ('2', 'pre_tracking'): 0, ('2', 'never_scraped'): 0,
        })


if __name__ == '__main__':
    unittest.main()
