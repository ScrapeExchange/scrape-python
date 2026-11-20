'''Startup of ``tools.yt_rss_scrape.worker_loop`` must not load the
creator map when the RSS queue lives in Redis.

Loading the whole creator map (millions of channels) in every worker
cost hundreds of MB per process that the allocator never returned. New
channels reach the Redis RSS queue from the channel scraper and from
``yt_channel_queue reconcile --repair rss-seed`` instead. Only the
file-backed (no Redis) queue is still populated from the creator map
at startup.

Each test stops ``worker_loop`` at ``cleanup_stale_claims``, which runs
right after the startup population step.
'''

from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, MagicMock

from scrape_exchange.creator_queue import (
    FileCreatorQueue,
    RedisCreatorQueue,
    TierConfig,
)
from tools import yt_rss_scrape

_TIERS: list[TierConfig] = [
    TierConfig(tier=1, min_subscribers=0, interval_hours=24),
]


class _StopStartup(Exception):
    '''Raised by the cleanup_stale_claims mock to end the test.'''


async def _run_startup(
    creator_queue: MagicMock, creator_map: AsyncMock,
) -> None:
    settings: MagicMock = MagicMock()
    settings.rss_enrich_subscriber_counts = False
    with unittest.TestCase().assertRaises(_StopStartup):
        await yt_rss_scrape.worker_loop(
            settings, MagicMock(), MagicMock(), creator_queue,
            _TIERS, creator_map, MagicMock(), MagicMock(),
            MagicMock(),
        )


def _creator_map() -> AsyncMock:
    creator_map: AsyncMock = AsyncMock()
    creator_map.get_all.return_value = {'UCa': 'a'}
    creator_map.size.return_value = 1
    return creator_map


class TestWorkerLoopStartup(unittest.IsolatedAsyncioTestCase):

    async def test_redis_queue_is_prepared_not_populated(self) -> None:
        queue: MagicMock = MagicMock(spec=RedisCreatorQueue)
        queue.prepare = AsyncMock()
        queue.populate = AsyncMock()
        queue.known_creator_ids = AsyncMock()
        queue.cleanup_stale_claims = AsyncMock(side_effect=_StopStartup)
        creator_map: AsyncMock = _creator_map()

        await _run_startup(queue, creator_map)

        queue.prepare.assert_awaited_once_with(_TIERS)
        queue.populate.assert_not_awaited()
        queue.known_creator_ids.assert_not_awaited()
        creator_map.get_all.assert_not_awaited()
        creator_map.size.assert_awaited_once()

    async def test_file_queue_is_populated_from_creator_map(
        self,
    ) -> None:
        queue: MagicMock = MagicMock(spec=FileCreatorQueue)
        queue.populate = AsyncMock(return_value=1)
        queue.known_creator_ids = AsyncMock(return_value=set())
        queue.cleanup_stale_claims = AsyncMock(side_effect=_StopStartup)
        creator_map: AsyncMock = _creator_map()

        await _run_startup(queue, creator_map)

        creator_map.get_all.assert_awaited_once()
        queue.populate.assert_awaited_once()
        self.assertEqual(
            queue.populate.await_args.args[0], {'UCa': 'a'},
        )


if __name__ == '__main__':
    unittest.main()
