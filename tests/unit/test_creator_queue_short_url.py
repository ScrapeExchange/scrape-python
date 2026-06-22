'''Unit tests for the short-URL graduation queue primitives:
reschedule_in, discard_member, schedule_if_absent.'''

import time
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
    TierConfig(tier=4, min_subscribers=0, interval_hours=720),
]


@unittest.skipUnless(_HAVE_FAKEREDIS, 'fakeredis not installed')
class TestRescheduleIn(unittest.IsolatedAsyncioTestCase):

    async def _queue(self) -> RedisCreatorQueue:
        redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
        q: RedisCreatorQueue = RedisCreatorQueue(
            redis_dsn='redis://fake', worker_id='w1',
            platform=_PLATFORM, key_namespace='scrape',
        )
        q._redis = redis
        q._tiers = _TIERS
        q._key_queues = q._build_queue_keys(_TIERS)
        return q

    async def test_reschedule_in_uses_fixed_delay_not_tier(self) -> None:
        q = await self._queue()
        # Admit a member into the slowest tier (720h) and claim it.
        await q.add_member('https://vm.tiktok.com/ZG', 'x', 0)
        await q._redis.set(
            'scrape:tiktok:claim:https://vm.tiktok.com/ZG', '1',
        )
        before: float = time.time()
        await q.reschedule_in('https://vm.tiktok.com/ZG', 300)
        # Score must be ~now+300, NOT now + 720h.
        score: float | None = await q._redis.zscore(
            'scrape:tiktok:queue:4', 'https://vm.tiktok.com/ZG',
        )
        self.assertIsNotNone(score)
        self.assertLess(abs(score - (before + 300)), 30)
        # Claim cleared.
        self.assertEqual(
            await q._redis.exists(
                'scrape:tiktok:claim:'
                'https://vm.tiktok.com/ZG',
            ),
            0,
        )


@unittest.skipUnless(_HAVE_FAKEREDIS, 'fakeredis not installed')
class TestDiscardMember(unittest.IsolatedAsyncioTestCase):

    async def _queue(self) -> RedisCreatorQueue:
        redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
        q: RedisCreatorQueue = RedisCreatorQueue(
            redis_dsn='redis://fake', worker_id='w1',
            platform=_PLATFORM, key_namespace='scrape',
        )
        q._redis = redis
        q._tiers = _TIERS
        q._key_queues = q._build_queue_keys(_TIERS)
        return q

    async def test_discard_erases_all_structures_incl_lower_name(
        self,
    ) -> None:
        q = await self._queue()
        # Admit a MIXED-CASE short URL (name stored lowercased).
        member: str = 'https://vm.tiktok.com/ZGJEytV2E'
        await q.add_member(member, member, 0)
        await q._redis.set(f'scrape:tiktok:claim:{member}', '1')
        await q.discard_member(member)
        # Gone from every tier ZSET.
        for key in q._key_queues:
            self.assertIsNone(await q._redis.zscore(key, member))
        # Gone from creators, tiers, claim.
        self.assertIsNone(
            await q._redis.hget(
                'scrape:tiktok:creators', member,
            ),
        )
        self.assertIsNone(
            await q._redis.hget(
                'scrape:tiktok:tiers', member,
            ),
        )
        self.assertEqual(
            await q._redis.exists(
                f'scrape:tiktok:claim:{member}',
            ),
            0,
        )
        # Names index purged using the LOWERCASED stored name.
        self.assertEqual(
            await q._redis.sismember(
                'scrape:tiktok:names', member.lower(),
            ),
            0,
        )

    async def test_discard_no_absent_residue_in_state(self) -> None:
        q = await self._queue()
        member: str = 'https://vm.tiktok.com/ZG'
        await q.add_member(member, member, 0)
        await q.discard_member(member)
        state, _score = await q._state_of(member)
        self.assertEqual(state, 'absent')
        self.assertIsNone(await q.show_member(member))


@unittest.skipUnless(_HAVE_FAKEREDIS, 'fakeredis not installed')
class TestScheduleIfAbsent(unittest.IsolatedAsyncioTestCase):

    async def _queue(self) -> RedisCreatorQueue:
        redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
        q: RedisCreatorQueue = RedisCreatorQueue(
            redis_dsn='redis://fake', worker_id='w1',
            platform=_PLATFORM, key_namespace='scrape',
        )
        q._redis = redis
        q._tiers = _TIERS
        q._key_queues = q._build_queue_keys(_TIERS)
        return q

    async def test_schedules_when_absent(self) -> None:
        q = await self._queue()
        scheduled: bool = await q.schedule_if_absent('alice', 'alice', 0)
        self.assertTrue(scheduled)
        state, _ = await q._state_of('alice')
        self.assertEqual(state, 'queued')
        # name stored lowercased, tier hash written.
        self.assertEqual(
            await q._redis.sismember(
                'scrape:tiktok:names', 'alice',
            ),
            1,
        )

    async def test_noop_when_already_queued_other_tier(self) -> None:
        q = await self._queue()
        # Already queued in tier 1 (high follower count).
        await q.add_member('alice', 'alice', 5_000_000)
        scheduled: bool = await q.schedule_if_absent('alice', 'alice', 0)
        self.assertFalse(scheduled)
        # NOT added to the weight=0 tier (tier 4): no cross-tier dup.
        self.assertIsNone(
            await q._redis.zscore(
                'scrape:tiktok:queue:4', 'alice',
            ),
        )

    async def test_noop_when_claimed(self) -> None:
        q = await self._queue()
        await q._redis.set('scrape:tiktok:claim:alice', '1')
        self.assertFalse(await q.schedule_if_absent('alice', 'alice', 0))

    async def test_noop_when_excluded(self) -> None:
        q = await self._queue()
        await q._redis.sadd('scrape:tiktok:excluded', 'alice')
        self.assertFalse(await q.schedule_if_absent('alice', 'alice', 0))


if __name__ == '__main__':
    unittest.main()
