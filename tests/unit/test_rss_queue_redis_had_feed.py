'''
Unit tests for RedisCreatorQueue.mark_had_feed /
has_had_feed — the had-feed flag that identifies
creators for which at least one RSS fetch has
previously succeeded.

Uses fakeredis to simulate the Redis backend without a
live server. Mirrors the pattern used by the other
RedisCreatorQueue unit test modules in this directory.
'''

import unittest

try:
    import fakeredis.aioredis
    HAS_FAKEREDIS: bool = True
except ImportError:
    HAS_FAKEREDIS = False

from scrape_exchange.creator_queue import RedisCreatorQueue


TEST_PLATFORM: str = 'test'


@unittest.skipUnless(
    HAS_FAKEREDIS,
    'fakeredis not installed',
)
class TestRedisCreatorQueueHadFeed(
    unittest.IsolatedAsyncioTestCase,
):

    async def _queue(self) -> RedisCreatorQueue:
        redis = fakeredis.aioredis.FakeRedis(
            decode_responses=True,
        )
        q: RedisCreatorQueue = RedisCreatorQueue(
            redis_dsn='redis://fake',
            worker_id='w1',
            platform=TEST_PLATFORM,
        )
        q._redis = redis
        return q

    async def test_has_had_feed_false_by_default(self) -> None:
        q: RedisCreatorQueue = await self._queue()
        self.assertFalse(
            await q.has_had_feed('UC_x'),
        )

    async def test_mark_had_feed_sets_flag(self) -> None:
        q: RedisCreatorQueue = await self._queue()
        await q.mark_had_feed('UC_x')
        self.assertTrue(await q.has_had_feed('UC_x'))
        self.assertFalse(await q.has_had_feed('UC_y'))

    async def test_mark_had_feed_is_idempotent(self) -> None:
        q: RedisCreatorQueue = await self._queue()
        await q.mark_had_feed('UC_x')
        await q.mark_had_feed('UC_x')
        await q.mark_had_feed('UC_x')
        size: int = await q._redis.scard(
            q._key_had_feed,
        )
        self.assertEqual(size, 1)

    async def test_had_feed_key_is_namespaced(self) -> None:
        q: RedisCreatorQueue = await self._queue()
        self.assertEqual(
            q._key_had_feed,
            f'rss:{TEST_PLATFORM}:had_feed',
        )

    async def test_multiple_creators_isolated(self) -> None:
        q: RedisCreatorQueue = await self._queue()
        await q.mark_had_feed('UC_a')
        await q.mark_had_feed('UC_b')
        self.assertTrue(await q.has_had_feed('UC_a'))
        self.assertTrue(await q.has_had_feed('UC_b'))
        self.assertFalse(await q.has_had_feed('UC_c'))


if __name__ == '__main__':
    unittest.main()
