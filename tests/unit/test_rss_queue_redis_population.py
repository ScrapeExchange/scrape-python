'''
Unit tests for RedisCreatorQueue.scan_and_recover_orphans.

Uses fakeredis to simulate the Redis backend without a live
server. The test patterns mirror
tests/unit/test_rss_queue_redis_release.py.
'''

import asyncio
import importlib.util
import sys
import unittest

from pathlib import Path
from types import ModuleType
from unittest.mock import AsyncMock

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
class TestScanAndRecoverOrphans(
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
        q._tiers = DEFAULT_TIERS
        q._key_queues = q._build_queue_keys(DEFAULT_TIERS)
        return q

    def _empty_breakdown(self) -> dict:
        return {
            tc.tier: {
                'queued': 0, 'claimed': 0,
                'no_feeds': 0, 'orphan': 0,
            }
            for tc in DEFAULT_TIERS
        }

    async def test_empty_tiers_hash(self) -> None:
        q: RedisCreatorQueue = await self._queue()
        breakdown = await q.scan_and_recover_orphans()
        self.assertEqual(breakdown, self._empty_breakdown())

    async def test_empty_tiers_list_early_exit(
        self,
    ) -> None:
        '''When ``_tiers`` is empty (not yet populated),
        the method must return an empty dict via the
        early-exit branch without issuing any HSCAN.'''
        q: RedisCreatorQueue = await self._queue()
        q._tiers = []
        q._key_queues = []
        await q._redis.hset(
            q._key_tiers, 'UC_should_be_ignored', '1',
        )
        breakdown = await q.scan_and_recover_orphans()
        self.assertEqual(breakdown, {})

    async def test_classifies_each_state(self) -> None:
        q: RedisCreatorQueue = await self._queue()

        # Tier 1, state=queued
        await q._redis.hset(
            q._key_tiers, 'UC_q', '1',
        )
        await q._redis.zadd(
            f'rss:{TEST_PLATFORM}:queue:1',
            {'UC_q': 100.0},
        )

        # Tier 2, state=claimed
        await q._redis.hset(
            q._key_tiers, 'UC_c', '2',
        )
        await q._redis.set(
            f'{q._claim_prefix}UC_c', 'w1',
        )

        # Tier 3, state=no_feeds
        await q._redis.hset(
            q._key_tiers, 'UC_n', '3',
        )
        await q._redis.set(
            f'{q._no_feeds_prefix}UC_n',
            'url\tname\t1',
        )

        # Tier 1, state=orphan (no queue, claim, or no_feeds)
        await q._redis.hset(
            q._key_tiers, 'UC_o', '1',
        )

        breakdown = await q.scan_and_recover_orphans(
            recover=False,
        )
        self.assertEqual(breakdown[1]['queued'], 1)
        self.assertEqual(breakdown[1]['orphan'], 1)
        self.assertEqual(breakdown[2]['claimed'], 1)
        self.assertEqual(breakdown[3]['no_feeds'], 1)

    async def test_priority_order_queued_wins(
        self,
    ) -> None:
        '''queued > claimed > no_feeds > orphan.'''
        q: RedisCreatorQueue = await self._queue()
        cid: str = 'UC_multi'

        # cid is simultaneously in a queue AND flagged
        # no_feeds — should classify as queued.
        await q._redis.hset(
            q._key_tiers, cid, '1',
        )
        await q._redis.zadd(
            f'rss:{TEST_PLATFORM}:queue:1',
            {cid: 100.0},
        )
        await q._redis.set(
            f'{q._no_feeds_prefix}{cid}',
            'url\tname\t1',
        )

        breakdown = await q.scan_and_recover_orphans(
            recover=False,
        )
        self.assertEqual(breakdown[1]['queued'], 1)
        self.assertEqual(breakdown[1]['no_feeds'], 0)

    async def test_priority_order_claimed_beats_no_feeds(
        self,
    ) -> None:
        q: RedisCreatorQueue = await self._queue()
        cid: str = 'UC_claim_nf'

        await q._redis.hset(
            q._key_tiers, cid, '2',
        )
        await q._redis.set(
            f'{q._claim_prefix}{cid}', 'w1',
        )
        await q._redis.set(
            f'{q._no_feeds_prefix}{cid}',
            'url\tname\t1',
        )

        breakdown = await q.scan_and_recover_orphans(
            recover=False,
        )
        self.assertEqual(breakdown[2]['claimed'], 1)
        self.assertEqual(breakdown[2]['no_feeds'], 0)

    async def test_recover_reenqueues_orphan_to_hash_tier(
        self,
    ) -> None:
        '''Orphan with tier hash value "2" must go into
        queue:2, not queue:1.'''
        q: RedisCreatorQueue = await self._queue()
        cid: str = 'UC_orphan'

        await q._redis.hset(
            q._key_tiers, cid, '2',
        )
        # No queue entry, no claim, no no_feeds.

        breakdown = await q.scan_and_recover_orphans(
            recover=True,
        )
        # Breakdown is pre-recovery.
        self.assertEqual(breakdown[2]['orphan'], 1)

        # Post-recovery: cid must now be in queue:2 only.
        score_q1: float | None = await q._redis.zscore(
            f'rss:{TEST_PLATFORM}:queue:1', cid,
        )
        score_q2: float | None = await q._redis.zscore(
            f'rss:{TEST_PLATFORM}:queue:2', cid,
        )
        self.assertIsNone(score_q1)
        self.assertIsNotNone(score_q2)

    async def test_recover_false_does_not_enqueue(
        self,
    ) -> None:
        q: RedisCreatorQueue = await self._queue()
        cid: str = 'UC_orphan_ro'

        await q._redis.hset(
            q._key_tiers, cid, '1',
        )

        breakdown = await q.scan_and_recover_orphans(
            recover=False,
        )
        self.assertEqual(breakdown[1]['orphan'], 1)

        score: float | None = await q._redis.zscore(
            f'rss:{TEST_PLATFORM}:queue:1', cid,
        )
        self.assertIsNone(score)

    async def test_per_tier_tally(self) -> None:
        '''Two cids of each state across 3 tiers.'''
        q: RedisCreatorQueue = await self._queue()

        for i, tier in enumerate([1, 2, 3]):
            for suffix in ('a', 'b'):
                # queued
                cid_q: str = f'UC_q_{tier}_{suffix}'
                await q._redis.hset(
                    q._key_tiers, cid_q, str(tier),
                )
                await q._redis.zadd(
                    f'rss:{TEST_PLATFORM}:queue:{tier}',
                    {cid_q: float(i)},
                )
                # claimed
                cid_c: str = f'UC_c_{tier}_{suffix}'
                await q._redis.hset(
                    q._key_tiers, cid_c, str(tier),
                )
                await q._redis.set(
                    f'{q._claim_prefix}{cid_c}', 'w1',
                )
                # no_feeds
                cid_n: str = f'UC_n_{tier}_{suffix}'
                await q._redis.hset(
                    q._key_tiers, cid_n, str(tier),
                )
                await q._redis.set(
                    f'{q._no_feeds_prefix}{cid_n}',
                    'url\tname\t1',
                )
                # orphan
                cid_o: str = f'UC_o_{tier}_{suffix}'
                await q._redis.hset(
                    q._key_tiers, cid_o, str(tier),
                )

        breakdown = await q.scan_and_recover_orphans(
            recover=False,
        )
        for tier in (1, 2, 3):
            self.assertEqual(
                breakdown[tier],
                {
                    'queued': 2, 'claimed': 2,
                    'no_feeds': 2, 'orphan': 2,
                },
                msg=f'tier {tier} counts mismatched',
            )


def _load_yt_rss_scrape() -> ModuleType:
    '''Load tools/yt_rss_scrape.py as an importable
    module. Cached via ``sys.modules`` so re-calls do
    not re-register Prometheus metrics.
    '''

    if 'yt_rss_scrape' in sys.modules:
        return sys.modules['yt_rss_scrape']
    repo_root: Path = (
        Path(__file__).resolve().parents[2]
    )
    module_path: Path = (
        repo_root / 'tools' / 'yt_rss_scrape.py'
    )
    spec = importlib.util.spec_from_file_location(
        'yt_rss_scrape', module_path,
    )
    assert (
        spec is not None and spec.loader is not None
    )
    module: ModuleType = (
        importlib.util.module_from_spec(spec)
    )
    sys.modules['yt_rss_scrape'] = module
    spec.loader.exec_module(module)
    return module


class TestScanAndRecoverLoop(
    unittest.IsolatedAsyncioTestCase,
):
    '''Exercises one iteration of
    _scan_and_recover_loop and asserts the metrics
    were updated. Uses a mock queue to avoid any Redis
    dependency.'''

    async def test_one_iteration_publishes_metrics(
        self,
    ) -> None:
        yt_rss_scrape: ModuleType = (
            _load_yt_rss_scrape()
        )

        queue = AsyncMock()
        queue.scan_and_recover_orphans.return_value = {
            1: {
                'queued': 10, 'claimed': 2,
                'no_feeds': 5, 'orphan': 3,
            },
        }

        task = asyncio.create_task(
            yt_rss_scrape._scan_and_recover_loop(
                queue,
                interval_seconds=0.01,
            ),
        )
        # Yield long enough for at least one loop body
        # to complete and the first sleep to start.
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        queue.scan_and_recover_orphans.assert_called_with(
            recover=True,
        )
        # Gauge value for (tier=1, state=orphan)
        # should be 3 (the last value .set() sets,
        # overwriting any earlier value).
        gauge_sample: float = (
            yt_rss_scrape.METRIC_TIER_POPULATION
            .labels(tier='1', state='orphan')
            ._value.get()
        )
        self.assertEqual(gauge_sample, 3)
        # Counter accumulates across iterations; after
        # several cycles at 0.01s intervals it must be
        # strictly greater than zero.
        counter_sample: float = (
            yt_rss_scrape.METRIC_ORPHANS_RECOVERED
            .labels(tier='1')
            ._value.get()
        )
        self.assertGreater(counter_sample, 0)
