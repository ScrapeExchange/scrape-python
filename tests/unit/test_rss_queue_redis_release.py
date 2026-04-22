'''
Unit tests for RedisCreatorQueue.release() idempotency.

Uses fakeredis with Lua scripting so no live Redis is required.
'''

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

DEFAULT_TIERS: list[TierConfig] = [
    TierConfig(
        tier=1,
        min_subscribers=1_000_000,
        interval_hours=4.0,
    ),
    TierConfig(
        tier=2,
        min_subscribers=100_000,
        interval_hours=12.0,
    ),
    TierConfig(
        tier=3,
        min_subscribers=0,
        interval_hours=48.0,
    ),
]


@unittest.skipUnless(
    HAS_FAKEREDIS,
    'fakeredis not installed',
)
class TestReleaseIdempotency(
    unittest.IsolatedAsyncioTestCase,
):
    '''
    release() must repair a missing tier hash entry instead of
    silently ZADDing into the fallback tier without recording
    the cid's tier. Otherwise the queue drifts into
    "zset orphan" state — cid is in queue:N but not in the
    tier hash, so reconciliation cannot tell where it belongs.
    '''

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
        q._tiers = DEFAULT_TIERS
        q._key_queues = q._build_queue_keys(DEFAULT_TIERS)
        return q

    async def test_release_restores_missing_tier_hash_entry(
        self,
    ) -> None:
        q: RedisCreatorQueue = await self._queue()
        cid: str = 'UCtest1234567890abcdef'

        # Simulate a cid that was claimed (no queue
        # membership) and whose tier hash entry is missing —
        # the exact orphan scenario produced by historical
        # schema drift.
        await q._redis.set(
            f'{q._claim_prefix}{cid}', 'w1',
        )
        # _key_tiers has no entry for cid.

        await q.release(cid)

        # After release: cid must appear in SOME tier queue,
        # AND its tier hash entry must be present so future
        # reconciliation / update_tier can find it.
        tier_str: str | None = await q._redis.hget(
            q._key_tiers, cid,
        )
        self.assertIsNotNone(
            tier_str,
            msg=(
                'release() must write a tier hash entry '
                'when none exists, otherwise the cid '
                'becomes a zset orphan'
            ),
        )

        tier: int = int(tier_str)
        score: float | None = await q._redis.zscore(
            f'rss:{TEST_PLATFORM}:queue:{tier}', cid,
        )
        self.assertIsNotNone(
            score,
            msg=(
                'cid must be in the zset matching the '
                'tier hash entry'
            ),
        )
        self.assertGreater(score, 0)

    async def test_release_preserves_existing_tier_hash_entry(
        self,
    ) -> None:
        '''
        When tier hash entry exists, release() must not overwrite
        it — the existing value is the source of truth, set by
        populate() or update_tier().
        '''
        q: RedisCreatorQueue = await self._queue()
        cid: str = 'UCtest1234567890abcdef'

        await q._redis.hset(q._key_tiers, cid, '1')
        await q._redis.set(
            f'{q._claim_prefix}{cid}', 'w1',
        )

        await q.release(cid)

        tier_str: str | None = await q._redis.hget(
            q._key_tiers, cid,
        )
        self.assertEqual(tier_str, '1')
        score: float | None = await q._redis.zscore(
            f'rss:{TEST_PLATFORM}:queue:1', cid,
        )
        self.assertIsNotNone(score)


@unittest.skipUnless(
    HAS_FAKEREDIS,
    'fakeredis not installed',
)
class TestReleaseRetryInterval(
    unittest.IsolatedAsyncioTestCase,
):
    '''release() accepts an optional retry_interval_seconds
    floor so that failed channels are held back at least
    that long, on top of the normal tier interval.'''

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
        q._tiers = DEFAULT_TIERS
        q._key_queues = q._build_queue_keys(DEFAULT_TIERS)
        return q

    async def _release_and_read_score(
        self,
        q: RedisCreatorQueue,
        cid: str,
        tier: int,
        retry_interval_seconds: float | None,
    ) -> tuple[float, float]:
        before: float = datetime.now(UTC).timestamp()
        await q.release(
            cid,
            retry_interval_seconds=retry_interval_seconds,
        )
        score: float | None = await q._redis.zscore(
            f'rss:{TEST_PLATFORM}:queue:{tier}', cid,
        )
        self.assertIsNotNone(score)
        return score, before

    async def test_no_retry_interval_uses_tier_interval(
        self,
    ) -> None:
        q: RedisCreatorQueue = await self._queue()
        cid: str = 'UCtest1234567890abcdef'
        await q._redis.hset(q._key_tiers, cid, '1')
        await q._redis.set(
            f'{q._claim_prefix}{cid}', 'w1',
        )
        score: float
        before: float
        score, before = (
            await self._release_and_read_score(
                q, cid, 1, None,
            )
        )
        self.assertAlmostEqual(
            score - before, 4.0 * 3600, delta=5.0,
        )

    async def test_retry_interval_raises_floor(
        self,
    ) -> None:
        q: RedisCreatorQueue = await self._queue()
        cid: str = 'UCtest1234567890abcdef'
        await q._redis.hset(q._key_tiers, cid, '1')
        await q._redis.set(
            f'{q._claim_prefix}{cid}', 'w1',
        )
        score: float
        before: float
        score, before = (
            await self._release_and_read_score(
                q, cid, 1, 24.0 * 3600,
            )
        )
        # 24h > 4h → retry floor wins.
        self.assertAlmostEqual(
            score - before, 24.0 * 3600, delta=5.0,
        )

    async def test_retry_interval_below_tier_is_ignored(
        self,
    ) -> None:
        q: RedisCreatorQueue = await self._queue()
        cid: str = 'UCtest1234567890abcdef'
        await q._redis.hset(q._key_tiers, cid, '3')
        await q._redis.set(
            f'{q._claim_prefix}{cid}', 'w1',
        )
        score: float
        before: float
        score, before = (
            await self._release_and_read_score(
                q, cid, 3, 4.0 * 3600,
            )
        )
        # 48h > 4h → tier interval wins.
        self.assertAlmostEqual(
            score - before, 48.0 * 3600, delta=5.0,
        )
