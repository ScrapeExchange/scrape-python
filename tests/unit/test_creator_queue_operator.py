'''
Unit tests for the operator-facing additions to
``RedisCreatorQueue`` (exclude/removed-state, add_member,
show_member, search_members, reschedule, count_by_state) and the
durable-removal guard in the recover Lua. Uses fakeredis, mirroring
the existing test_rss_queue_redis_* setup.
'''

import unittest

try:
    import fakeredis.aioredis
    _HAVE_FAKEREDIS: bool = True
except ImportError:
    _HAVE_FAKEREDIS = False

from scrape_exchange.creator_queue import (
    RedisCreatorQueue,
    TierConfig,
)

_PLATFORM: str = 'tiktok'
_TIERS: list[TierConfig] = [
    TierConfig(tier=1, min_subscribers=1_000_000, interval_hours=6),
    TierConfig(tier=2, min_subscribers=100_000, interval_hours=24),
    TierConfig(tier=3, min_subscribers=10_000, interval_hours=72),
    TierConfig(tier=4, min_subscribers=0, interval_hours=168),
]


@unittest.skipUnless(_HAVE_FAKEREDIS, 'fakeredis not installed')
class TestOperatorQueue(unittest.IsolatedAsyncioTestCase):

    async def _queue(self) -> RedisCreatorQueue:
        redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
        q: RedisCreatorQueue = RedisCreatorQueue(
            redis_dsn='redis://fake',
            worker_id='w1',
            platform=_PLATFORM,
            key_namespace='scrape',
        )
        q._redis = redis
        q._tiers = _TIERS
        q._key_queues = q._build_queue_keys(_TIERS)
        return q

    async def _zscore_any(
        self, q: RedisCreatorQueue, cid: str,
    ) -> float | None:
        for key in q._key_queues:
            score = await q._redis.zscore(key, cid)
            if score is not None:
                return score
        return None

    async def test_add_member_queues_new(self) -> None:
        q: RedisCreatorQueue = await self._queue()
        added: bool = await q.add_member('alice', 'alice', 5_000_000)
        self.assertTrue(added)
        self.assertIsNotNone(await self._zscore_any(q, 'alice'))
        # tier 1 for 5M followers
        self.assertEqual(
            await q._redis.hget(q._key_tiers, 'alice'), '1',
        )
        # idempotent: second add does not re-queue
        again: bool = await q.add_member('alice', 'alice', 5_000_000)
        self.assertFalse(again)

    async def test_exclude_moves_to_removed(self) -> None:
        q: RedisCreatorQueue = await self._queue()
        await q.add_member('bob', 'bob', 50_000)
        await q.exclude('bob')
        self.assertIsNone(await self._zscore_any(q, 'bob'))
        self.assertEqual(
            await q._redis.sismember(q._key_excluded, 'bob'), 1,
        )
        rec = await q.show_member('bob')
        self.assertEqual(rec['state'], 'removed')

    async def test_add_member_restores_removed(self) -> None:
        q: RedisCreatorQueue = await self._queue()
        await q.add_member('carol', 'carol', 50_000)
        await q.exclude('carol')
        restored: bool = await q.add_member('carol', 'carol', 50_000)
        self.assertTrue(restored)
        self.assertEqual(
            await q._redis.sismember(q._key_excluded, 'carol'), 0,
        )
        self.assertIsNotNone(await self._zscore_any(q, 'carol'))

    async def test_show_member_states(self) -> None:
        q: RedisCreatorQueue = await self._queue()
        await q.add_member('q', 'q', 50_000)
        await q.add_member('c', 'c', 50_000)
        await q._redis.set(f'{q._claim_prefix}c', 'w1')
        await q._redis.zrem('scrape:tiktok:queue:3', 'c')
        rec_q = await q.show_member('q')
        rec_c = await q.show_member('c')
        self.assertEqual(rec_q['state'], 'queued')
        self.assertIsNotNone(rec_q['score'])
        self.assertEqual(rec_c['state'], 'claimed')
        self.assertIsNone(await q.show_member('nope'))

    async def test_search_members_by_id_and_name(self) -> None:
        q: RedisCreatorQueue = await self._queue()
        await q.add_member('charli', 'Charli DAmelio', 50_000)
        await q.add_member('khaby', 'Khaby Lame', 50_000)
        by_id = await q.search_members('char', 10)
        self.assertEqual(
            [r['creator_id'] for r in by_id], ['charli'],
        )
        by_name = await q.search_members('lame', 10)
        self.assertEqual(
            [r['creator_id'] for r in by_name], ['khaby'],
        )

    async def test_reschedule_sets_due_now_and_unexcludes(
        self,
    ) -> None:
        q: RedisCreatorQueue = await self._queue()
        await q.add_member('dora', 'dora', 50_000)
        # push score far into the future, then reschedule
        await q._redis.zadd(
            'scrape:tiktok:queue:3', {'dora': 9_999_999_999.0},
        )
        await q.exclude('dora')
        ok: bool = await q.reschedule('dora')
        self.assertTrue(ok)
        score = await self._zscore_any(q, 'dora')
        self.assertIsNotNone(score)
        self.assertLess(score, 9_999_999_999.0)
        self.assertEqual(
            await q._redis.sismember(q._key_excluded, 'dora'), 0,
        )

    async def test_count_by_state(self) -> None:
        q: RedisCreatorQueue = await self._queue()
        await q.add_member('q1', 'q1', 50_000)
        await q.add_member('q2', 'q2', 50_000)
        await q.add_member('c1', 'c1', 50_000)
        await q._redis.set(f'{q._claim_prefix}c1', 'w1')
        await q._redis.zrem('scrape:tiktok:queue:3', 'c1')
        await q.add_member('r1', 'r1', 50_000)
        await q.exclude('r1')
        counts: dict[str, int] = await q.count_by_state()
        self.assertEqual(counts['queued'], 2)
        self.assertEqual(counts['claimed'], 1)
        self.assertEqual(counts['removed'], 1)

    async def test_excluded_member_not_resurrected(self) -> None:
        '''The load-bearing durability test: an excluded member must
        survive a recover pass, while a non-excluded orphan is
        re-enqueued.'''
        q: RedisCreatorQueue = await self._queue()
        # excluded member: in creators+tiers hash, excluded, not queued
        await q.add_member('gone', 'gone', 50_000)
        await q.exclude('gone')
        # orphan: in creators+tiers hash, not queued, not claimed,
        # not excluded
        await q._redis.hset(q._key_creators, 'orphan', 'orphan')
        await q._redis.hset(q._key_tiers, 'orphan', '3')

        recovered: int = await q.cleanup_stale_claims()

        # orphan re-enqueued, gone stays out
        self.assertIsNotNone(await self._zscore_any(q, 'orphan'))
        self.assertIsNone(await self._zscore_any(q, 'gone'))
        self.assertGreaterEqual(recovered, 1)


if __name__ == '__main__':
    unittest.main()
