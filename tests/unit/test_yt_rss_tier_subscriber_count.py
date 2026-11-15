'''The RSS tier of a polled channel whose page shows no subscriber
count comes from the last known count, else the lowest tier, so such
channels do not stay in tier 1 for good.'''

import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.creator_queue import (
    RedisCreatorQueue,
    TierConfig,
    tier_for_subscriber_count,
)
from tools import yt_rss_scrape


def _redis_queue() -> MagicMock:
    queue: MagicMock = MagicMock(spec=RedisCreatorQueue)
    queue.redis_client = MagicMock()
    return queue


class TestTierSubscriberCount(unittest.IsolatedAsyncioTestCase):
    async def _count(
        self, queue: MagicMock, sub_count: int | None,
        lookup: AsyncMock | None = None,
    ) -> tuple[int, str]:
        metric: MagicMock = MagicMock()
        lookup = lookup or AsyncMock(return_value=None)
        with (
            patch.object(yt_rss_scrape, 'lookup_channel_counts', lookup),
            patch.object(
                yt_rss_scrape, 'METRIC_RSS_TIER_COUNT_SOURCE', metric,
            ),
        ):
            count: int = await yt_rss_scrape._tier_subscriber_count(
                queue, 'UCX', sub_count,
            )
        return count, metric.labels.call_args.kwargs['source']

    async def test_parsed_count_is_used(self) -> None:
        lookup: AsyncMock = AsyncMock()
        self.assertEqual(
            await self._count(_redis_queue(), 1_500_000, lookup),
            (1_500_000, 'parsed'),
        )
        lookup.assert_not_awaited()

    async def test_last_known_count_when_page_shows_none(self) -> None:
        queue: MagicMock = _redis_queue()
        lookup: AsyncMock = AsyncMock(return_value=(2_000_000, None))
        self.assertEqual(
            await self._count(queue, None, lookup),
            (2_000_000, 'last_known'),
        )
        lookup.assert_awaited_once_with(queue.redis_client, 'UCX')

    async def test_lowest_tier_without_any_count(self) -> None:
        for lookup in (
            AsyncMock(return_value=None),
            AsyncMock(return_value=(None, 5000)),
            AsyncMock(side_effect=ConnectionError('down')),
        ):
            self.assertEqual(
                await self._count(_redis_queue(), None, lookup),
                (0, 'lowest_tier'),
            )

    async def test_non_redis_queue_goes_to_lowest_tier(self) -> None:
        lookup: AsyncMock = AsyncMock()
        self.assertEqual(
            await self._count(MagicMock(), None, lookup),
            (0, 'lowest_tier'),
        )
        lookup.assert_not_awaited()

    def test_zero_lands_in_lowest_tier(self) -> None:
        # Production RSS_PRIORITY_QUEUES; tier numbers are positions.
        tiers: list[TierConfig] = [
            TierConfig(tier=i, min_subscribers=m, interval_hours=h)
            for i, (h, m) in enumerate((
                (1, 100_000_000), (2, 10_000_000), (4, 1_000_000),
                (12, 100_000), (72, 4000), (168, 0),
            ), start=1)
        ]
        self.assertEqual(tier_for_subscriber_count(tiers, 0), 6)
        self.assertEqual(tier_for_subscriber_count(tiers, None), 1)
