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

    for _key in ('yt_rss_scrape', 'tools.yt_rss_scrape'):
        if _key in sys.modules:
            return sys.modules[_key]
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
    sys.modules['tools.yt_rss_scrape'] = module
    spec.loader.exec_module(module)
    return module


@unittest.skipUnless(
    HAS_FAKEREDIS,
    'fakeredis not installed',
)
class TestTierPopulationSummary(
    unittest.IsolatedAsyncioTestCase,
):
    '''``tier_population_summary`` returns ``{tier: count}`` for
    every cid in the ``rss:<platform>:tiers`` HASH bucketed by its
    assigned tier value. The metric driven by this method has been
    observed to undercount in production by ~100x compared to
    ``HLEN`` of the same hash; the rewrite uses ``hscan_iter`` and
    a larger COUNT hint for resilience to whatever environmental
    cause produced that.
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

    async def test_empty_hash_returns_zero_for_each_tier(
        self,
    ) -> None:
        q: RedisCreatorQueue = await self._queue()
        counts: dict[int, int] = (
            await q.tier_population_summary()
        )
        self.assertEqual(counts, {1: 0, 2: 0, 3: 0})

    async def test_empty_tiers_list_returns_empty_dict(
        self,
    ) -> None:
        q: RedisCreatorQueue = await self._queue()
        q._tiers = []
        await q._redis.hset(
            q._key_tiers, 'UC_should_be_ignored', '1',
        )
        counts: dict[int, int] = (
            await q.tier_population_summary()
        )
        self.assertEqual(counts, {})

    async def test_typical_distribution(self) -> None:
        '''A small spread across configured tiers must be
        reported exactly. Total must equal HLEN.'''
        q: RedisCreatorQueue = await self._queue()
        for i in range(7):
            await q._redis.hset(
                q._key_tiers, f'UC_t1_{i}', '1',
            )
        for i in range(11):
            await q._redis.hset(
                q._key_tiers, f'UC_t2_{i}', '2',
            )
        for i in range(13):
            await q._redis.hset(
                q._key_tiers, f'UC_t3_{i}', '3',
            )
        counts: dict[int, int] = (
            await q.tier_population_summary()
        )
        self.assertEqual(counts, {1: 7, 2: 11, 3: 13})
        self.assertEqual(
            sum(counts.values()),
            await q._redis.hlen(q._key_tiers),
        )

    async def test_skips_unconfigured_tier_values(
        self,
    ) -> None:
        '''Hash entries whose tier value is outside the
        configured tier set are silently skipped. They do not
        crash the scan and they do not inflate any counter.'''
        q: RedisCreatorQueue = await self._queue()
        await q._redis.hset(q._key_tiers, 'UC_t1', '1')
        await q._redis.hset(q._key_tiers, 'UC_t2', '2')
        # tier 99 is not configured.
        await q._redis.hset(q._key_tiers, 'UC_t99', '99')
        counts: dict[int, int] = (
            await q.tier_population_summary()
        )
        self.assertEqual(counts, {1: 1, 2: 1, 3: 0})

    async def test_skips_invalid_tier_values(self) -> None:
        '''Non-integer tier values are silently skipped without
        breaking the iteration.'''
        q: RedisCreatorQueue = await self._queue()
        await q._redis.hset(q._key_tiers, 'UC_ok', '1')
        await q._redis.hset(q._key_tiers, 'UC_garb', 'nope')
        await q._redis.hset(q._key_tiers, 'UC_blank', '')
        counts: dict[int, int] = (
            await q.tier_population_summary()
        )
        self.assertEqual(counts, {1: 1, 2: 0, 3: 0})

    async def test_large_hash_full_coverage(self) -> None:
        '''The dominant production failure mode: total returned
        must equal HLEN even when the hash is much larger than
        a single HSCAN batch. 5_000 entries forces several
        iterations regardless of COUNT hint.'''
        q: RedisCreatorQueue = await self._queue()
        for i in range(5_000):
            tier_value: int = (i % 3) + 1
            await q._redis.hset(
                q._key_tiers, f'UC{i:06d}', str(tier_value),
            )
        counts: dict[int, int] = (
            await q.tier_population_summary()
        )
        self.assertEqual(
            sum(counts.values()),
            await q._redis.hlen(q._key_tiers),
        )
        # 5000 / 3 -> 1667, 1667, 1666
        self.assertEqual(counts[1], 1667)
        self.assertEqual(counts[2], 1667)
        self.assertEqual(counts[3], 1666)


@unittest.skipUnless(
    HAS_FAKEREDIS,
    'fakeredis not installed',
)
class TestClaimBatchStaleClaims(
    unittest.IsolatedAsyncioTestCase,
):
    '''``claim_batch`` previously called ZRANGEBYSCORE with
    ``LIMIT 0, batch_size`` and tried to ``SET .. NX`` each
    candidate. cids whose claim key already existed (stale claim
    from a worker that died without releasing) were silently
    skipped — but the script did NOT then fetch additional
    candidates, so the returned batch was short by the number of
    skips. Under fleet load this caused workers to receive
    near-empty batches even when the queue had thousands of
    claimable channels behind a few stale-claim cids at the head.

    The fix is to keep iterating past stale candidates until
    either ``batch_size`` successful claims accumulate or the
    queue is genuinely exhausted.
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

    async def _seed_tier(
        self, q: RedisCreatorQueue,
        cids: list[str], tier: int, score: float,
    ) -> None:
        '''Insert *cids* into *tier* zset and the creators+tiers
        hashes — mirrors the seed helper in the integration tests.
        '''
        key: str = f'rss:{TEST_PLATFORM}:queue:{tier}'
        for cid in cids:
            await q._redis.zadd(key, {cid: score})
            await q._redis.hset(
                q._key_creators, cid, f'name-{cid}',
            )
            await q._redis.hset(
                q._key_tiers, cid, str(tier),
            )

    async def test_skips_past_stale_claims_to_fill_batch(
        self,
    ) -> None:
        q: RedisCreatorQueue = await self._queue()
        past: float = 100.0  # well before any cutoff we use

        # 3 stale-claim cids first by score, then 5 fresh.
        stale: list[str] = ['UC_stale_a', 'UC_stale_b', 'UC_stale_c']
        fresh: list[str] = [f'UC_fresh_{i}' for i in range(5)]

        # Seed all 8 with the SAME tier-1 score so order is
        # determined by lex order on cid; "UC_stale_*" sorts before
        # "UC_fresh_*" only if we deliberately pick scores. To make
        # ordering deterministic, give stale ones an EARLIER score
        # so they sit at the head of the queue.
        for i, cid in enumerate(stale):
            await self._seed_tier(q, [cid], 1, past + i)
        for i, cid in enumerate(fresh):
            await self._seed_tier(q, [cid], 1, past + 100 + i)

        # Pre-set claim keys for the stale cids — simulates a
        # crashed worker that claimed but never released.
        for cid in stale:
            await q._redis.set(
                f'{q._claim_prefix}{cid}', 'old_worker',
                ex=300,
            )

        # Request a batch of 5. With the fix, claim_batch must
        # return 5 fresh cids (skipping past the 3 stale ones).
        batch: list[tuple[str, str, float]] = (
            await q.claim_batch(5, 'w1')
        )
        cids_returned: list[str] = [b[0] for b in batch]

        self.assertEqual(
            len(batch), 5,
            f'expected 5 successful claims past stale heads; '
            f'got {len(batch)}: {cids_returned}',
        )
        self.assertEqual(set(cids_returned), set(fresh))
        # Stale cids must NOT have been claimed (not in batch),
        # and their claim keys must still exist.
        for cid in stale:
            self.assertIn(cid, cids_returned, msg=None) \
                if False else None  # noqa - structural placeholder
            self.assertNotIn(cid, cids_returned)
            self.assertEqual(
                await q._redis.get(
                    f'{q._claim_prefix}{cid}',
                ),
                'old_worker',
            )

    async def test_returns_empty_when_all_due_are_stale(
        self,
    ) -> None:
        '''If every due cid has a stale claim, the script must
        terminate (not loop forever) and return an empty batch.'''
        q: RedisCreatorQueue = await self._queue()
        cids: list[str] = [f'UC_stale_{i}' for i in range(6)]
        for i, cid in enumerate(cids):
            await self._seed_tier(q, [cid], 1, 100.0 + i)
        for cid in cids:
            await q._redis.set(
                f'{q._claim_prefix}{cid}', 'old_worker',
                ex=300,
            )
        batch: list[tuple[str, str, float]] = (
            await q.claim_batch(5, 'w1')
        )
        self.assertEqual(batch, [])

    async def test_no_stale_claims_returns_full_batch(
        self,
    ) -> None:
        '''Regression guard: with no stale claims the new loop
        must still return the full requested batch in one pass.'''
        q: RedisCreatorQueue = await self._queue()
        cids: list[str] = [f'UC_fresh_{i}' for i in range(10)]
        for i, cid in enumerate(cids):
            await self._seed_tier(q, [cid], 1, 100.0 + i)
        batch: list[tuple[str, str, float]] = (
            await q.claim_batch(5, 'w1')
        )
        self.assertEqual(len(batch), 5)
        self.assertEqual(
            {b[0] for b in batch},
            set(cids[:5]),
        )


class TestTimedPhase(
    unittest.IsolatedAsyncioTestCase,
):
    '''``_timed(phase, coro)`` wraps an awaitable and observes
    its wall-clock duration into ``METRIC_PHASE_DURATION`` under
    the given ``phase`` label. Used inside ``process_channel``
    to attribute time to sub-phases (``fetch_rss``,
    ``update_channel``, ``check_existence``,
    ``enrich_videos``) so the streamer-level ``process`` phase
    can be decomposed.
    '''

    async def test_observes_phase_duration_on_success(
        self,
    ) -> None:
        yt_rss_scrape: ModuleType = (
            _load_yt_rss_scrape()
        )

        async def _work() -> int:
            return 42

        before: float = (
            yt_rss_scrape.METRIC_PHASE_DURATION.labels(
                platform='youtube',
                scraper='rss_scraper',
                phase='fetch_rss',
                worker_id=yt_rss_scrape.get_worker_id(),
            )._sum.get()
        )
        result: int = await yt_rss_scrape._timed(
            'fetch_rss', _work(),
        )
        self.assertEqual(result, 42)
        after: float = (
            yt_rss_scrape.METRIC_PHASE_DURATION.labels(
                platform='youtube',
                scraper='rss_scraper',
                phase='fetch_rss',
                worker_id=yt_rss_scrape.get_worker_id(),
            )._sum.get()
        )
        # _sum is monotonically increasing across the call —
        # cumulative wall-clock time spent in this phase.
        self.assertGreater(after, before)

    async def test_observes_phase_duration_on_exception(
        self,
    ) -> None:
        '''Exceptions inside the wrapped coroutine must still
        observe the phase (via finally:), then propagate.'''
        yt_rss_scrape: ModuleType = (
            _load_yt_rss_scrape()
        )

        async def _boom() -> None:
            raise RuntimeError('synthetic')

        before: float = (
            yt_rss_scrape.METRIC_PHASE_DURATION.labels(
                platform='youtube',
                scraper='rss_scraper',
                phase='enrich_videos',
                worker_id=yt_rss_scrape.get_worker_id(),
            )._sum.get()
        )
        with self.assertRaises(RuntimeError):
            await yt_rss_scrape._timed(
                'enrich_videos', _boom(),
            )
        after: float = (
            yt_rss_scrape.METRIC_PHASE_DURATION.labels(
                platform='youtube',
                scraper='rss_scraper',
                phase='enrich_videos',
                worker_id=yt_rss_scrape.get_worker_id(),
            )._sum.get()
        )
        self.assertGreaterEqual(after, before)


class TestStreamProcessor(
    unittest.IsolatedAsyncioTestCase,
):
    '''Each streamer is an independent claim-1-process-release
    loop. N streamers per worker process replace the previous
    ``claim_batch(N) → asyncio.gather(N)`` pattern, eliminating
    the gather tail-latency where one slow channel gates the
    whole batch's progression. Phase-duration histogram
    observations identify where time is being spent.
    '''

    async def test_streamer_claims_processes_releases(
        self,
    ) -> None:
        '''One iteration of the streamer must:
        1. claim_batch(1)
        2. invoke process_channel for the claimed cid
        3. release the cid back via creator_queue.release
        4. record per-phase duration observations
        '''
        yt_rss_scrape: ModuleType = (
            _load_yt_rss_scrape()
        )

        queue = AsyncMock()
        # First iteration returns one channel; second returns
        # empty so the loop shifts into idle backoff and we can
        # cancel cleanly.
        queue.claim_batch.side_effect = [
            [('UC_test', 'name-test', 100.0)],
            [],
        ]
        queue.get_tier.return_value = 1
        # ``get_tier_interval`` is a sync method on CreatorQueue;
        # override the AsyncMock-default to keep it synchronous so
        # the streamer's ``* 3600`` arithmetic doesn't multiply a
        # coroutine.
        queue.get_tier_interval = unittest.mock.MagicMock(
            return_value=1.0,
        )

        client = AsyncMock()
        creator_map_backend = AsyncMock()
        name_map_backend = AsyncMock()
        channel_validator = unittest.mock.MagicMock()
        settings = unittest.mock.MagicMock(
            retry_interval=300,
            eligibility_fraction=1.0,
        )

        video_queue = AsyncMock()
        with unittest.mock.patch.object(
            yt_rss_scrape, 'process_channel',
            new=AsyncMock(return_value=True),
        ) as proc_mock:
            task = asyncio.create_task(
                yt_rss_scrape._stream_processor(
                    streamer_id=0,
                    creator_queue=queue,
                    client=client,
                    creator_map_backend=creator_map_backend,
                    name_map_backend=name_map_backend,
                    channel_validator=channel_validator,
                    settings=settings,
                    video_queue=video_queue,
                ),
            )
            # Yield enough for two iterations (claim + process +
            # release on the first; empty + idle backoff start on
            # the second).
            for _ in range(40):
                await asyncio.sleep(0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        queue.claim_batch.assert_called()
        proc_mock.assert_called_once()
        # First positional arg of process_channel is the channel
        # name, second is cid.
        call_args = proc_mock.call_args
        self.assertEqual(call_args.args[0], 'name-test')
        self.assertEqual(call_args.args[1], 'UC_test')
        queue.release.assert_called_once()

        # Phase duration observations: at least one each of
        # 'claim' and 'process'. 'release' fires when result is
        # truthy.
        phase_metric = yt_rss_scrape.METRIC_PHASE_DURATION
        for phase in ('claim', 'process', 'release'):
            samples: float = (
                phase_metric.labels(
                    platform='youtube',
                    scraper='rss_scraper',
                    phase=phase,
                    worker_id=yt_rss_scrape.get_worker_id(),
                )._sum.get()
            )
            self.assertGreater(
                samples, 0,
                f'expected phase={phase} to record at least one '
                f'observation; got cumulative sum {samples}',
            )

    async def test_streamer_handles_empty_batch_with_backoff(
        self,
    ) -> None:
        '''When claim_batch returns empty, the streamer must not
        spin — it sleeps MIN_SLEEP_SECONDS, increments the idle
        phase counter, and retries.'''
        yt_rss_scrape: ModuleType = (
            _load_yt_rss_scrape()
        )

        queue = AsyncMock()
        queue.claim_batch.return_value = []
        client = AsyncMock()
        creator_map_backend = AsyncMock()
        name_map_backend = AsyncMock()
        channel_validator = unittest.mock.MagicMock()
        settings = unittest.mock.MagicMock(
            retry_interval=300,
            eligibility_fraction=1.0,
        )

        video_queue = AsyncMock()
        with unittest.mock.patch.object(
            yt_rss_scrape, 'MIN_SLEEP_SECONDS', 0.001,
        ), unittest.mock.patch.object(
            yt_rss_scrape, 'process_channel',
            new=AsyncMock(),
        ) as proc_mock:
            task = asyncio.create_task(
                yt_rss_scrape._stream_processor(
                    streamer_id=0,
                    creator_queue=queue,
                    client=client,
                    creator_map_backend=creator_map_backend,
                    name_map_backend=name_map_backend,
                    channel_validator=channel_validator,
                    settings=settings,
                    video_queue=video_queue,
                ),
            )
            await asyncio.sleep(0.02)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Multiple empty claim_batch calls must have occurred
        # without ever invoking process_channel.
        self.assertGreater(queue.claim_batch.call_count, 1)
        proc_mock.assert_not_called()

        # Idle phase observation present.
        idle_sum: float = (
            yt_rss_scrape.METRIC_PHASE_DURATION.labels(
                platform='youtube',
                scraper='rss_scraper',
                phase='idle',
                worker_id=yt_rss_scrape.get_worker_id(),
            )._sum.get()
        )
        self.assertGreater(idle_sum, 0)


class TestScanAndRecoverLoop(
    unittest.IsolatedAsyncioTestCase,
):
    '''Exercises one iteration of _scan_and_recover_loop.
    Metric publishing has been moved out of this loop;
    its sole responsibility is now orphan recovery and
    the orphans-recovered counter.
    '''

    async def test_orphan_recovery_increments_counter(
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
        before: float = (
            yt_rss_scrape.METRIC_ORPHANS_RECOVERED.labels(
                platform='youtube',
                scraper='rss_scraper',
                tier='1',
            )._value.get()
        )

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
        after: float = (
            yt_rss_scrape.METRIC_ORPHANS_RECOVERED.labels(
                platform='youtube',
                scraper='rss_scraper',
                tier='1',
            )._value.get()
        )
        self.assertGreater(after, before)


class TestPublishQueueMetricsLoop(
    unittest.IsolatedAsyncioTestCase,
):
    '''The dedicated queue-metrics loop publishes both
    scrape_queue_size (per-tier ZCARD) and
    channel_tier_population (per-tier total cid count)
    on a fast cadence, decoupled from the slow
    orphan-recovery scan.
    '''

    async def test_one_iteration_publishes_both_gauges(
        self,
    ) -> None:
        yt_rss_scrape: ModuleType = (
            _load_yt_rss_scrape()
        )

        queue = AsyncMock()
        queue.queue_sizes_by_tier.return_value = {
            1: 7, 2: 11,
        }
        queue.tier_population_summary.return_value = {
            1: 100, 2: 200,
        }

        task = asyncio.create_task(
            yt_rss_scrape._publish_queue_metrics_loop(
                queue,
                interval_seconds=0.01,
            ),
        )
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        # scrape_queue_size: shared-state caller passes worker_id=''.
        qs_t1: float = (
            yt_rss_scrape.METRIC_QUEUE_SIZE.labels(
                platform='youtube',
                scraper='rss_scraper',
                entity='rss_feed',
                tier='1',
                worker_id='',
            )._value.get()
        )
        self.assertEqual(qs_t1, 7)

        # channel_tier_population without state label.
        tp_t1: float = (
            yt_rss_scrape.METRIC_TIER_POPULATION.labels(
                platform='youtube',
                scraper='rss_scraper',
                tier='1',
            )._value.get()
        )
        self.assertEqual(tp_t1, 100)
