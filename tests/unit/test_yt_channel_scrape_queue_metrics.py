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


if __name__ == '__main__':
    unittest.main()
