'''
Unit tests that the RSS scraper's startup loops signal progress to the
liveness watchdog, so a slow (high-latency) Redis link does not get the
worker killed before its streamers start.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import tempfile
import unittest
from unittest import mock

import fakeredis.aioredis

from scrape_exchange import creator_queue
from scrape_exchange.creator_queue import RedisCreatorQueue, TierConfig
from scrape_exchange.file_management import AssetFileManagement


_TIERS: list[TierConfig] = [
    TierConfig(tier=1, min_subscribers=1_000_000, interval_hours=6),
    TierConfig(tier=2, min_subscribers=0, interval_hours=24),
]


class TestPopulateTouchesWatchdog(unittest.IsolatedAsyncioTestCase):

    async def test_populate_and_orphan_scan_touch(self) -> None:
        redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(decode_responses=True)
        )
        q: RedisCreatorQueue = RedisCreatorQueue(
            redis_dsn='redis://fake', worker_id='w1',
            platform='youtube', key_namespace='rss',
        )
        q._redis = redis
        creators: dict[str, str] = {
            f'UC{i:022d}': f'name{i}' for i in range(1200)
        }
        watchdog: mock.Mock = mock.Mock()
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            creator_queue.Watchdog, 'get', return_value=watchdog,
        ):
            added: int = await q.populate(
                creators, AssetFileManagement(tmp), _TIERS, {},
            )
        await redis.aclose()
        self.assertEqual(added, 1200)
        # populate: 1200 candidates in chunks of 500 -> 3 touches;
        # the boot orphan scan touches at least once more.
        self.assertGreaterEqual(watchdog.touch_work.call_count, 4)


if __name__ == '__main__':
    unittest.main()
