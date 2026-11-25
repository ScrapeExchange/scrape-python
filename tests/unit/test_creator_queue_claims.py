'''
Unit tests for RedisCreatorQueue claims, which live in one hash per
queue (creator id -> worker id) with a TTL on each field instead of a
key per creator. Uses fakeredis with Lua scripting.
'''

import asyncio
import unittest
from datetime import UTC, datetime

try:
    import fakeredis.aioredis
    HAS_FAKEREDIS: bool = True
except ImportError:
    HAS_FAKEREDIS = False

from scrape_exchange.creator_queue import (
    RedisCreatorQueue,
    TierConfig,
)

TEST_PLATFORM: str = 'test'

TIERS: list[TierConfig] = [
    TierConfig(tier=1, min_subscribers=1_000_000, interval_hours=4.0),
    TierConfig(tier=2, min_subscribers=0, interval_hours=12.0),
]


@unittest.skipUnless(HAS_FAKEREDIS, 'fakeredis not installed')
class TestCreatorQueueClaims(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self.redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
        self.q: RedisCreatorQueue = RedisCreatorQueue(
            redis_dsn='redis://fake',
            worker_id='w1',
            platform=TEST_PLATFORM,
        )
        self.q._redis = self.redis
        self.q._tiers = TIERS
        self.q._key_queues = self.q._build_queue_keys(TIERS)

    async def asyncTearDown(self) -> None:
        await self.redis.flushall()
        await self.redis.aclose()

    async def _seed_due(self, *cids: str) -> None:
        past: float = datetime.now(UTC).timestamp() - 60
        for cid in cids:
            await self.redis.zadd(self.q._queue_key(1), {cid: past})
            await self.redis.hset(self.q._key_creators, cid, cid)
            await self.redis.hset(self.q._key_tiers, cid, '1')

    async def test_claim_stores_worker_with_ttl_in_claims_hash(
        self,
    ) -> None:
        await self._seed_due('UCa', 'UCb')
        batch = await self.q.claim_batch(10, 'w1', claim_ttl=300)
        self.assertEqual(sorted(c for c, _, _ in batch), ['UCa', 'UCb'])
        self.assertEqual(
            await self.redis.hgetall(self.q._key_claims),
            {'UCa': 'w1', 'UCb': 'w1'},
        )
        ttls: list[int] = await self.redis.httl(
            self.q._key_claims, 'UCa', 'UCb',
        )
        for ttl in ttls:
            self.assertGreater(ttl, 0)
            self.assertLessEqual(ttl, 300)
        # No key per creator any more.
        self.assertEqual(
            await self.redis.keys(f'rss:{TEST_PLATFORM}:claim:*'), [],
        )

    async def test_claim_owner_and_release(self) -> None:
        await self._seed_due('UCa')
        await self.q.claim_batch(1, 'w7')
        self.assertEqual(await self.q.claim_owner('UCa'), 'w7')
        self.assertIsNone(await self.q.claim_owner('UCzz'))
        await self.q.release('UCa')
        self.assertIsNone(await self.q.claim_owner('UCa'))
        self.assertIsNotNone(
            await self.redis.zscore(self.q._queue_key(1), 'UCa'),
        )

    async def test_live_claim_blocks_second_claim(self) -> None:
        await self._seed_due('UCa')
        await self.redis.hset(self.q._key_claims, 'UCa', 'other')
        await self.redis.hexpire(self.q._key_claims, 300, 'UCa')
        self.assertEqual(await self.q.claim_batch(1, 'w1'), [])
        self.assertEqual(await self.q.claim_owner('UCa'), 'other')

    async def test_expired_claim_frees_creator(self) -> None:
        '''A claim from a worker that died expires with its field
        TTL, after which the creator can be scheduled again.'''
        await self._seed_due('UCa')
        await self.q.claim_batch(1, 'w1', claim_ttl=1)
        self.assertFalse(
            await self.q.schedule_if_absent('UCa', 'UCa', 0),
        )
        await asyncio.sleep(1.1)
        self.assertIsNone(await self.q.claim_owner('UCa'))
        self.assertTrue(
            await self.q.schedule_if_absent('UCa', 'UCa', 0),
        )

    async def test_count_by_state_counts_live_claims(self) -> None:
        await self._seed_due('UCa', 'UCb', 'UCc')
        await self.q.claim_batch(2, 'w1')
        counts: dict[str, int] = await self.q.count_by_state()
        self.assertEqual(counts['claimed'], 2)
        self.assertEqual(counts['queued'], 1)


if __name__ == '__main__':
    unittest.main()
