'''
Integration tests for scrape_exchange.creator_queue.RedisCreatorQueue
with the tier-aware interface.

Requires a live Redis instance at redis://192.168.1.13:6379/1.
All keys use the 'test' platform prefix (rss:test:*) to avoid
colliding with production data.
'''

import os
import shutil
import tempfile
import unittest

from datetime import UTC, datetime

import redis.asyncio as aioredis

from scrape_exchange.file_management import (
    AssetFileManagement,
)
from scrape_exchange.creator_queue import (
    CHANNEL_FILENAME_PREFIX,
    RedisCreatorQueue,
    TierConfig,
)

REDIS_DSN: str = 'redis://192.168.1.13:6379/1'
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
        min_subscribers=10_000,
        interval_hours=24.0,
    ),
    TierConfig(
        tier=4,
        min_subscribers=0,
        interval_hours=48.0,
    ),
]


def _queue_key(tier: int) -> str:
    return f'rss:{TEST_PLATFORM}:queue:{tier}'


def _creators_key() -> str:
    return f'rss:{TEST_PLATFORM}:creators'


def _tiers_key() -> str:
    return f'rss:{TEST_PLATFORM}:tiers'


def _claim_key(creator_id: str) -> str:
    return f'rss:{TEST_PLATFORM}:claim:{creator_id}'


async def _clean(r: aioredis.Redis) -> None:
    keys: list[str] = await r.keys('rss:test:*')
    if keys:
        await r.delete(*keys)


class TestRedisCreatorQueuePopulate(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self._redis: aioredis.Redis = (
            aioredis.from_url(
                REDIS_DSN, decode_responses=True,
            )
        )
        await _clean(self._redis)
        self.tmp: str = tempfile.mkdtemp()
        self.fm: AssetFileManagement = (
            AssetFileManagement(self.tmp)
        )
        self.q: RedisCreatorQueue = RedisCreatorQueue(
            REDIS_DSN,
            'test-worker',
            platform=TEST_PLATFORM,
        )

    async def asyncTearDown(self) -> None:
        await _clean(self._redis)
        await self._redis.aclose()
        shutil.rmtree(self.tmp, ignore_errors=True)

    async def test_populate_assigns_correct_tiers(
        self,
    ) -> None:
        # UCmega: 5M subs → tier 1
        # UCmid:  500K subs → tier 2
        # UCsmall: 50K subs → tier 3
        # UCnano: 1K subs → tier 4
        creators: dict[str, str] = {
            'UCmega': 'mega-channel',
            'UCmid': 'mid-channel',
            'UCsmall': 'small-channel',
            'UCnano': 'nano-channel',
        }
        sub_counts: dict[str, int] = {
            'UCmega': 5_000_000,
            'UCmid': 500_000,
            'UCsmall': 50_000,
            'UCnano': 1_000,
        }
        added: int = await self.q.populate(
            creators,
            self.fm,
            DEFAULT_TIERS,
            sub_counts,
        )
        self.assertEqual(added, 4)

        # Verify tier queue membership.
        mega_score: float | None = (
            await self._redis.zscore(
                _queue_key(1), 'UCmega',
            )
        )
        self.assertIsNotNone(mega_score)

        mid_score: float | None = (
            await self._redis.zscore(
                _queue_key(2), 'UCmid',
            )
        )
        self.assertIsNotNone(mid_score)

        small_score: float | None = (
            await self._redis.zscore(
                _queue_key(3), 'UCsmall',
            )
        )
        self.assertIsNotNone(small_score)

        nano_score: float | None = (
            await self._redis.zscore(
                _queue_key(4), 'UCnano',
            )
        )
        self.assertIsNotNone(nano_score)

        # Verify tiers hash.
        tier_val: str | None = (
            await self._redis.hget(
                _tiers_key(), 'UCmid',
            )
        )
        self.assertEqual(tier_val, '2')

    async def test_populate_does_not_overwrite_scores(
        self,
    ) -> None:
        creators: dict[str, str] = {
            'UCaaa': 'alpha',
        }
        await self.q.populate(
            creators, self.fm, DEFAULT_TIERS, {},
        )

        # Manually set a custom score to verify
        # ZADD NX does not overwrite it.
        custom_ts: float = 9_999_999_999.0
        await self._redis.zadd(
            _queue_key(1),
            {'UCaaa': custom_ts},
        )

        added: int = await self.q.populate(
            creators, self.fm, DEFAULT_TIERS, {},
        )
        self.assertEqual(added, 0)

        score: float | None = (
            await self._redis.zscore(
                _queue_key(1), 'UCaaa',
            )
        )
        self.assertIsNotNone(score)
        self.assertAlmostEqual(
            score, custom_ts, places=0,
        )

    async def test_populate_unknown_sub_count_tier1(
        self,
    ) -> None:
        # No subscriber count supplied (None) → tier 1.
        creators: dict[str, str] = {
            'UCunknown': 'unknown-channel',
        }
        await self.q.populate(
            creators, self.fm, DEFAULT_TIERS, {},
        )
        score: float | None = (
            await self._redis.zscore(
                _queue_key(1), 'UCunknown',
            )
        )
        self.assertIsNotNone(score)

    async def test_populate_zero_sub_count_lowest_tier(
        self,
    ) -> None:
        # Subscriber count of 0 → lowest tier (not tier 1).
        creators: dict[str, str] = {
            'UCzero': 'zero-channel',
        }
        await self.q.populate(
            creators, self.fm, DEFAULT_TIERS,
            {'UCzero': 0},
        )
        score: float | None = (
            await self._redis.zscore(
                _queue_key(4), 'UCzero',
            )
        )
        self.assertIsNotNone(score)
        # Confirm it's NOT in tier 1.
        tier1_score: float | None = (
            await self._redis.zscore(
                _queue_key(1), 'UCzero',
            )
        )
        self.assertIsNone(tier1_score)

    async def test_populate_recovers_orphaned_creators(
        self,
    ) -> None:
        # Insert a creator into the creators hash but
        # not any queue and with no claim key.
        # populate() should recover it.
        await self._redis.hset(
            _creators_key(), 'UCorphan', 'orphan-ch',
        )
        # Also put an entry in the tiers hash so the
        # Lua script routes it correctly.
        await self._redis.hset(
            _tiers_key(), 'UCorphan', '4',
        )
        added: int = await self.q.populate(
            {}, self.fm, DEFAULT_TIERS, {},
        )
        self.assertGreaterEqual(added, 1)

        # Should now be in one of the tier queues.
        found: bool = False
        for tier in range(1, 5):
            sc: float | None = (
                await self._redis.zscore(
                    _queue_key(tier), 'UCorphan',
                )
            )
            if sc is not None:
                found = True
                break
        self.assertTrue(found)

    async def test_populate_skips_not_found_marker(
        self,
    ) -> None:
        channel_name: str = 'mychannel'
        marker: str = (
            f'{CHANNEL_FILENAME_PREFIX}'
            f'{channel_name}.not_found'
        )
        open(
            os.path.join(self.tmp, marker), 'w',
        ).close()
        creators: dict[str, str] = {
            'UCaaa': channel_name,
        }
        added: int = await self.q.populate(
            creators, self.fm, DEFAULT_TIERS, {},
        )
        self.assertEqual(added, 0)
        size: int = await self.q.queue_size()
        self.assertEqual(size, 0)


class TestRedisCreatorQueueClaimBatch(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self._redis: aioredis.Redis = (
            aioredis.from_url(
                REDIS_DSN, decode_responses=True,
            )
        )
        await _clean(self._redis)
        self.tmp: str = tempfile.mkdtemp()
        self.fm: AssetFileManagement = (
            AssetFileManagement(self.tmp)
        )
        self.q: RedisCreatorQueue = RedisCreatorQueue(
            REDIS_DSN,
            'test-worker',
            platform=TEST_PLATFORM,
        )
        # Prime tiers so claim_batch knows key names.
        await self.q.populate(
            {}, self.fm, DEFAULT_TIERS, {},
        )

    async def asyncTearDown(self) -> None:
        await _clean(self._redis)
        await self._redis.aclose()
        shutil.rmtree(self.tmp, ignore_errors=True)

    async def _seed_tier(
        self,
        creators: dict[str, str],
        tier: int,
        ts: float,
    ) -> None:
        '''Insert creators directly into a tier queue.'''
        pipe = self._redis.pipeline()
        for cid, name in creators.items():
            pipe.zadd(_queue_key(tier), {cid: ts})
            pipe.hset(_creators_key(), cid, name)
            pipe.hset(_tiers_key(), cid, str(tier))
        await pipe.execute()

    async def test_claim_batch_tier1_before_tier2(
        self,
    ) -> None:
        past: float = (
            datetime.now(UTC).timestamp() - 60
        )
        await self._seed_tier(
            {'UCt1': 'tier1-ch'}, 1, past,
        )
        await self._seed_tier(
            {'UCt2': 'tier2-ch'}, 2, past,
        )
        # Request batch_size=1 to confirm tier 1 wins.
        batch: list[tuple[str, str]] = (
            await self.q.claim_batch(1, 'test-worker')
        )
        self.assertEqual(len(batch), 1)
        self.assertEqual(batch[0][0], 'UCt1')

    async def test_claim_batch_sets_claim_key_with_ttl(
        self,
    ) -> None:
        past: float = (
            datetime.now(UTC).timestamp() - 60
        )
        await self._seed_tier(
            {'UCaaa': 'alpha'}, 1, past,
        )
        await self.q.claim_batch(
            10, 'test-worker', claim_ttl=300,
        )
        ttl: int = await self._redis.ttl(
            _claim_key('UCaaa'),
        )
        self.assertGreater(ttl, 0)
        self.assertLessEqual(ttl, 300)

    async def test_claim_batch_empty_when_nothing_due(
        self,
    ) -> None:
        future: float = (
            datetime.now(UTC).timestamp() + 3600
        )
        await self._seed_tier(
            {'UCaaa': 'alpha'}, 1, future,
        )
        batch: list[tuple[str, str]] = (
            await self.q.claim_batch(10, 'test-worker')
        )
        self.assertEqual(batch, [])

    async def test_claim_batch_respects_batch_size(
        self,
    ) -> None:
        past: float = (
            datetime.now(UTC).timestamp() - 60
        )
        creators: dict[str, str] = {
            f'UC{i:04d}': f'ch{i}'
            for i in range(10)
        }
        await self._seed_tier(creators, 1, past)
        batch: list[tuple[str, str]] = (
            await self.q.claim_batch(3, 'test-worker')
        )
        self.assertEqual(len(batch), 3)

    async def test_claim_batch_skips_already_claimed(
        self,
    ) -> None:
        past: float = (
            datetime.now(UTC).timestamp() - 60
        )
        await self._seed_tier(
            {'UCaaa': 'alpha', 'UCbbb': 'beta'},
            1,
            past,
        )
        # Pre-claim UCaaa; also remove it from the
        # queue (claim implies already popped).
        await self._redis.set(
            _claim_key('UCaaa'), 'other-worker', ex=300,
        )
        await self._redis.zrem(_queue_key(1), 'UCaaa')

        batch: list[tuple[str, str]] = (
            await self.q.claim_batch(10, 'test-worker')
        )
        cids: set[str] = {cid for cid, _ in batch}
        self.assertNotIn('UCaaa', cids)
        self.assertIn('UCbbb', cids)


class TestRedisCreatorQueueRelease(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self._redis: aioredis.Redis = (
            aioredis.from_url(
                REDIS_DSN, decode_responses=True,
            )
        )
        await _clean(self._redis)
        self.tmp: str = tempfile.mkdtemp()
        self.fm: AssetFileManagement = (
            AssetFileManagement(self.tmp)
        )
        self.q: RedisCreatorQueue = RedisCreatorQueue(
            REDIS_DSN,
            'test-worker',
            platform=TEST_PLATFORM,
        )
        await self.q.populate(
            {}, self.fm, DEFAULT_TIERS, {},
        )

    async def asyncTearDown(self) -> None:
        await _clean(self._redis)
        await self._redis.aclose()
        shutil.rmtree(self.tmp, ignore_errors=True)

    async def test_release_deletes_claim_and_reenqueues(
        self,
    ) -> None:
        past: float = (
            datetime.now(UTC).timestamp() - 60
        )
        # Seed in tier 2 (interval 12h).
        pipe = self._redis.pipeline()
        pipe.zadd(_queue_key(2), {'UCaaa': past})
        pipe.hset(_creators_key(), 'UCaaa', 'alpha')
        pipe.hset(_tiers_key(), 'UCaaa', '2')
        await pipe.execute()

        batch: list[tuple[str, str]] = (
            await self.q.claim_batch(10, 'test-worker')
        )
        self.assertEqual(len(batch), 1)

        await self.q.release('UCaaa')

        # Claim key must be gone.
        exists: int = await self._redis.exists(
            _claim_key('UCaaa'),
        )
        self.assertEqual(exists, 0)

        # Creator must be back in tier 2 queue.
        score: float | None = (
            await self._redis.zscore(
                _queue_key(2), 'UCaaa',
            )
        )
        self.assertIsNotNone(score)

        # Score must be approximately now + 12h.
        expected: float = (
            datetime.now(UTC).timestamp() + 12 * 3600
        )
        assert score is not None
        self.assertAlmostEqual(
            score, expected, delta=5,
        )

    async def test_release_score_from_tier_interval(
        self,
    ) -> None:
        # Tier 4 has interval_hours=48.
        past: float = (
            datetime.now(UTC).timestamp() - 60
        )
        pipe = self._redis.pipeline()
        pipe.zadd(_queue_key(4), {'UCtier4': past})
        pipe.hset(_creators_key(), 'UCtier4', 't4ch')
        pipe.hset(_tiers_key(), 'UCtier4', '4')
        await pipe.execute()

        await self.q.claim_batch(10, 'test-worker')
        await self.q.release('UCtier4')

        score: float | None = (
            await self._redis.zscore(
                _queue_key(4), 'UCtier4',
            )
        )
        self.assertIsNotNone(score)
        expected: float = (
            datetime.now(UTC).timestamp() + 48 * 3600
        )
        assert score is not None
        self.assertAlmostEqual(
            score, expected, delta=5,
        )


class TestRedisCreatorQueueTierManagement(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self._redis: aioredis.Redis = (
            aioredis.from_url(
                REDIS_DSN, decode_responses=True,
            )
        )
        await _clean(self._redis)
        self.tmp: str = tempfile.mkdtemp()
        self.fm: AssetFileManagement = (
            AssetFileManagement(self.tmp)
        )
        self.q: RedisCreatorQueue = RedisCreatorQueue(
            REDIS_DSN,
            'test-worker',
            platform=TEST_PLATFORM,
        )
        await self.q.populate(
            {}, self.fm, DEFAULT_TIERS, {},
        )

    async def asyncTearDown(self) -> None:
        await _clean(self._redis)
        await self._redis.aclose()
        shutil.rmtree(self.tmp, ignore_errors=True)

    async def test_update_tier_writes_tiers_hash(
        self,
    ) -> None:
        # Set initial tier via tiers hash.
        await self._redis.hset(
            _tiers_key(), 'UCaaa', '4',
        )
        # 2M subs → tier 1.
        await self.q.update_tier('UCaaa', 2_000_000)
        tier_str: str | None = (
            await self._redis.hget(
                _tiers_key(), 'UCaaa',
            )
        )
        self.assertEqual(tier_str, '1')

    async def test_get_tier_reads_tiers_hash(
        self,
    ) -> None:
        await self._redis.hset(
            _tiers_key(), 'UCbbb', '3',
        )
        tier: int = await self.q.get_tier('UCbbb')
        self.assertEqual(tier, 3)

    async def test_update_tier_then_release_new_queue(
        self,
    ) -> None:
        # Seed creator in tier 4.
        past: float = (
            datetime.now(UTC).timestamp() - 60
        )
        pipe = self._redis.pipeline()
        pipe.zadd(_queue_key(4), {'UCmove': past})
        pipe.hset(_creators_key(), 'UCmove', 'mv-ch')
        pipe.hset(_tiers_key(), 'UCmove', '4')
        await pipe.execute()

        await self.q.claim_batch(10, 'test-worker')

        # While claimed, subscriber count jumps: → tier 1.
        await self.q.update_tier('UCmove', 5_000_000)

        await self.q.release('UCmove')

        # Should land in tier 1 queue.
        score_t1: float | None = (
            await self._redis.zscore(
                _queue_key(1), 'UCmove',
            )
        )
        self.assertIsNotNone(score_t1)

        # Must NOT be in tier 4 queue.
        score_t4: float | None = (
            await self._redis.zscore(
                _queue_key(4), 'UCmove',
            )
        )
        self.assertIsNone(score_t4)


class TestRedisCreatorQueueOperations(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self._redis: aioredis.Redis = (
            aioredis.from_url(
                REDIS_DSN, decode_responses=True,
            )
        )
        await _clean(self._redis)
        self.tmp: str = tempfile.mkdtemp()
        self.fm: AssetFileManagement = (
            AssetFileManagement(self.tmp)
        )
        self.q: RedisCreatorQueue = RedisCreatorQueue(
            REDIS_DSN,
            'test-worker',
            platform=TEST_PLATFORM,
        )
        await self.q.populate(
            {}, self.fm, DEFAULT_TIERS, {},
        )

    async def asyncTearDown(self) -> None:
        await _clean(self._redis)
        await self._redis.aclose()
        shutil.rmtree(self.tmp, ignore_errors=True)

    async def test_next_due_time_returns_min_across_tiers(
        self,
    ) -> None:
        ts1: float = 1000.0
        ts2: float = 2000.0
        await self._redis.zadd(
            _queue_key(2), {'UCbbb': ts2},
        )
        await self._redis.zadd(
            _queue_key(1), {'UCaaa': ts1},
        )
        result: float | None = (
            await self.q.next_due_time()
        )
        self.assertIsNotNone(result)
        self.assertAlmostEqual(
            result, ts1, places=0,
        )

    async def test_next_due_time_none_when_empty(
        self,
    ) -> None:
        result: float | None = (
            await self.q.next_due_time()
        )
        self.assertIsNone(result)

    async def test_queue_size_sums_all_tiers(
        self,
    ) -> None:
        creators: dict[str, str] = {
            'UCaaa': 'alpha',
            'UCbbb': 'beta',
            'UCccc': 'gamma',
        }
        sub_counts: dict[str, int] = {
            'UCaaa': 5_000_000,  # tier 1
            'UCbbb': 500_000,    # tier 2
            'UCccc': 1_000,      # tier 4
        }
        await self.q.populate(
            creators, self.fm, DEFAULT_TIERS, sub_counts,
        )
        size: int = await self.q.queue_size()
        self.assertEqual(size, 3)

    async def test_no_feeds_get_returns_none_absent(
        self,
    ) -> None:
        result: tuple[str, str, int] | None = (
            await self.q.get_no_feeds('UCaaa')
        )
        self.assertIsNone(result)

    async def test_no_feeds_set_and_get(
        self,
    ) -> None:
        await self.q.set_no_feeds(
            'UCaaa',
            'https://example.com/feed',
            'my-channel',
            1,
        )
        result: tuple[str, str, int] | None = (
            await self.q.get_no_feeds('UCaaa')
        )
        self.assertIsNotNone(result)
        assert result is not None
        url: str
        name: str
        count: int
        url, name, count = result
        self.assertEqual(url, 'https://example.com/feed')
        self.assertEqual(name, 'my-channel')
        self.assertEqual(count, 1)

        no_feeds_key: str = (
            f'rss:{TEST_PLATFORM}:no_feeds:UCaaa'
        )
        ttl: int = await self._redis.ttl(no_feeds_key)
        self.assertGreater(ttl, 0)

    async def test_no_feeds_increments_count(
        self,
    ) -> None:
        await self.q.set_no_feeds(
            'UCaaa', 'https://example.com/feed',
            'my-channel', 1,
        )
        await self.q.set_no_feeds(
            'UCaaa', 'https://example.com/feed',
            'my-channel', 1,
        )
        result: tuple[str, str, int] | None = (
            await self.q.get_no_feeds('UCaaa')
        )
        self.assertIsNotNone(result)
        assert result is not None
        _, _, count = result
        self.assertEqual(count, 2)

    async def test_no_feeds_clear_deletes_key(
        self,
    ) -> None:
        await self.q.set_no_feeds(
            'UCaaa', 'https://example.com/feed',
            'my-channel', 1,
        )
        await self.q.clear_no_feeds('UCaaa')
        result: tuple[str, str, int] | None = (
            await self.q.get_no_feeds('UCaaa')
        )
        self.assertIsNone(result)


class TestRedisCreatorQueueCleanupStaleClaims(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self._redis: aioredis.Redis = (
            aioredis.from_url(
                REDIS_DSN, decode_responses=True,
            )
        )
        await _clean(self._redis)
        self.q: RedisCreatorQueue = RedisCreatorQueue(
            REDIS_DSN,
            'test-worker',
            platform=TEST_PLATFORM,
        )
        self.tmp: str = tempfile.mkdtemp()
        self.fm: AssetFileManagement = (
            AssetFileManagement(self.tmp)
        )
        await self.q.populate(
            {}, self.fm, DEFAULT_TIERS, {},
        )

    async def asyncTearDown(self) -> None:
        await _clean(self._redis)
        await self._redis.aclose()
        shutil.rmtree(self.tmp, ignore_errors=True)

    async def test_cleanup_stale_claims_reenqueues(
        self,
    ) -> None:
        # Simulate a worker that claimed a creator,
        # then crashed before release().  The claim TTL
        # has already expired (no claim key present).
        # Creator is in creators hash and tiers hash
        # but absent from all tier queues.
        await self._redis.hset(
            _creators_key(), 'UCstale', 'stale-channel',
        )
        await self._redis.hset(
            _tiers_key(), 'UCstale', '4',
        )

        recovered: int = (
            await self.q.cleanup_stale_claims()
        )
        self.assertGreaterEqual(recovered, 1)

        # UCstale must now appear in some tier queue.
        found: bool = False
        for tier in range(1, 5):
            sc: float | None = (
                await self._redis.zscore(
                    _queue_key(tier), 'UCstale',
                )
            )
            if sc is not None:
                found = True
                break
        self.assertTrue(found)

    async def test_cleanup_routes_to_correct_tier(
        self,
    ) -> None:
        # Creator has tier=2 recorded in tiers hash.
        await self._redis.hset(
            _creators_key(), 'UCst2', 'stale2-ch',
        )
        await self._redis.hset(
            _tiers_key(), 'UCst2', '2',
        )

        await self.q.cleanup_stale_claims()

        score: float | None = (
            await self._redis.zscore(
                _queue_key(2), 'UCst2',
            )
        )
        self.assertIsNotNone(score)


if __name__ == '__main__':
    unittest.main()
