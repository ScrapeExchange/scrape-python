'''
Unit tests that the Redis-backed RSS creator queue does not consult
local .not_found / .unresolved marker files: with Redis, the channel
queue's state is the source of truth.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import tempfile
import unittest
from pathlib import Path
from unittest import mock

import fakeredis.aioredis

from scrape_exchange.creator_queue import RedisCreatorQueue, TierConfig
from scrape_exchange.file_management import AssetFileManagement

_TIERS: list[TierConfig] = [
    TierConfig(tier=1, min_subscribers=0, interval_hours=24),
]


class TestRedisCreatorQueueIgnoresMarkers(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(decode_responses=True)
        )
        self.queue: RedisCreatorQueue = RedisCreatorQueue(
            redis_dsn='redis://fake', worker_id='w1',
            platform='youtube', key_namespace='rss',
        )
        self.queue._redis = self.redis
        self.tmp: tempfile.TemporaryDirectory = (
            tempfile.TemporaryDirectory()
        )
        self.addCleanup(self.tmp.cleanup)
        self.fm: AssetFileManagement = AssetFileManagement(self.tmp.name)
        # Markers that FileCreatorQueue would honour.
        Path(self.tmp.name, 'channel-gonehandle.not_found').touch()
        Path(self.tmp.name, 'channel-UCunresolved.unresolved').touch()

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    async def test_populate_does_not_scan_or_honour_markers(
        self,
    ) -> None:
        creators: dict[str, str] = {
            'UCgone': 'gonehandle',
            'UCunresolved': 'unresolvedhandle',
        }
        with mock.patch(
            'scrape_exchange.creator_queue.os.scandir',
        ) as scandir:
            added: int = await self.queue.populate(
                creators, self.fm, _TIERS, {},
            )
        self.assertEqual(added, 2)
        scandir.assert_not_called()

    async def test_add_creator_does_not_honour_markers(self) -> None:
        added: bool = await self.queue.add_creator(
            'UCunresolved', 'gonehandle', _TIERS, None,
        )
        self.assertTrue(added)


if __name__ == '__main__':
    unittest.main()
