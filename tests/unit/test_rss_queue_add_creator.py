'''
Unit tests for RedisCreatorQueue.add_creator — the lightweight
single-channel enqueue the channel scraper uses once a new channel's
first full scrape has completed.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import tempfile
import unittest

import fakeredis.aioredis

from scrape_exchange.creator_queue import (
    RedisCreatorQueue,
    TierConfig,
    parse_priority_queues,
)
from scrape_exchange.file_management import AssetFileManagement


class TestAddCreator(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(decode_responses=True)
        )
        self.queue: RedisCreatorQueue = RedisCreatorQueue(
            redis_dsn='redis://fake', worker_id='w1', platform='youtube',
        )
        self.queue._redis = self.redis
        self.tiers: list[TierConfig] = parse_priority_queues(
            '1:1000000,12:4000,168:0',
        )
        self.directory: tempfile.TemporaryDirectory = (
            tempfile.TemporaryDirectory()
        )
        self.addCleanup(self.directory.cleanup)
        self.fm: AssetFileManagement = AssetFileManagement(
            self.directory.name,
        )

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    async def test_adds_to_tier_for_subscriber_count(self) -> None:
        added: bool = await self.queue.add_creator(
            'UCnew', 'newhandle', self.tiers, 5000, self.fm,
        )
        self.assertTrue(added)
        self.assertIsNotNone(
            await self.redis.zscore('rss:youtube:queue:2', 'UCnew'),
        )
        self.assertEqual(
            await self.redis.hget('rss:youtube:tiers', 'UCnew'), '2',
        )
        self.assertEqual(
            await self.redis.hget('rss:youtube:creators', 'UCnew'),
            'newhandle',
        )
        self.assertTrue(
            await self.redis.sismember('rss:youtube:names', 'newhandle'),
        )

    async def test_existing_creator_is_not_re_added(self) -> None:
        await self.queue.add_creator(
            'UCnew', 'newhandle', self.tiers, 5000, self.fm,
        )
        self.assertFalse(await self.queue.add_creator(
            'UCnew', 'newhandle', self.tiers, 2_000_000, self.fm,
        ))
        self.assertEqual(
            await self.redis.hget('rss:youtube:tiers', 'UCnew'), '2',
        )

    async def test_excluded_creator_is_not_added(self) -> None:
        await self.redis.sadd('rss:youtube:excluded', 'UCgone')
        self.assertFalse(await self.queue.add_creator(
            'UCgone', 'gone', self.tiers, 10, self.fm,
        ))
        self.assertEqual(await self.redis.zcard('rss:youtube:queue:3'), 0)


if __name__ == '__main__':
    unittest.main()
