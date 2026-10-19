'''Unit tests for scrape_exchange.channel_scrape_queue.'''

import json
import random
import time
import unittest
from typing import Any

import fakeredis.aioredis

from scrape_exchange.channel_scrape_queue import (
    ChannelScrapeProgress,
    ChannelScrapeQueueSettings,
    ChannelState,
    RedisChannelScrapeQueue,
    parse_channel_priority_queues,
    parse_channel_priority_weights,
)


class TestChannelStateEnum(unittest.TestCase):

    def test_enum_values_are_lower_snake(self) -> None:
        self.assertEqual(
            ChannelState.NOT_FOUND.value, 'not_found',
        )
        self.assertEqual(
            ChannelState.SOFT_UNAVAILABLE.value,
            'soft_unavailable',
        )
        self.assertEqual(
            ChannelState.PENDING_RESOLUTION.value,
            'pending_resolution',
        )
        self.assertEqual(ChannelState.TOPIC.value, 'topic')
        self.assertEqual(
            ChannelState.NO_VIDEOS.value,
            'no_videos',
        )
        self.assertEqual(
            ChannelState.LOW_SUBS.value,
            'low_subs',
        )

    def test_terminal_states_set(self) -> None:
        terminal: frozenset[ChannelState] = (
            ChannelState.terminal_states()
        )
        self.assertIn(ChannelState.NOT_FOUND, terminal)
        self.assertIn(
            ChannelState.SOFT_UNAVAILABLE, terminal,
        )
        self.assertIn(ChannelState.TOPIC, terminal)
        self.assertIn(ChannelState.NO_VIDEOS, terminal)
        self.assertIn(ChannelState.LOW_SUBS, terminal)
        self.assertNotIn(
            ChannelState.SCHEDULED, terminal,
        )
        self.assertNotIn(
            ChannelState.PENDING_RESOLUTION, terminal,
        )


class TestChannelScrapeQueueSettings(unittest.TestCase):

    def test_default_priority_queues(self) -> None:
        s: ChannelScrapeQueueSettings = (
            ChannelScrapeQueueSettings()
        )
        self.assertEqual(
            s.channel_priority_queues,
            '7:1000000,30:100000,90:10000,180:1000,365:0',
        )

    def test_default_resolve_max_attempts(self) -> None:
        s: ChannelScrapeQueueSettings = (
            ChannelScrapeQueueSettings()
        )
        self.assertEqual(s.channel_resolve_max_attempts, 5)

    def test_default_not_found_terminal_threshold(self) -> None:
        s: ChannelScrapeQueueSettings = (
            ChannelScrapeQueueSettings()
        )
        self.assertEqual(
            s.channel_not_found_terminal_threshold, 3,
        )

    def test_default_missing_subscriber_retry_seconds(self) -> None:
        s: ChannelScrapeQueueSettings = (
            ChannelScrapeQueueSettings()
        )
        self.assertEqual(
            s.channel_missing_subscriber_retry_seconds,
            24 * 60 * 60,
        )


class _RedisQueueTestBase(
    unittest.IsolatedAsyncioTestCase,
):
    '''Shared fixture for RedisChannelScrapeQueue tests.'''

    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(
                decode_responses=True,
            )
        )
        self.settings: ChannelScrapeQueueSettings = \
            ChannelScrapeQueueSettings()
        self.queue: RedisChannelScrapeQueue = \
            RedisChannelScrapeQueue(
                self.redis, self.settings,
            )

    async def asyncTearDown(self) -> None:
        await self.redis.flushall()
        await self.redis.aclose()


class TestKeyPrefixes(_RedisQueueTestBase):

    async def test_key_helpers(self) -> None:
        self.assertEqual(
            self.queue._k_unresolved(),
            'youtube:channel:queue:unresolved',
        )
        self.assertEqual(
            self.queue._k_scheduled(2),
            'youtube:channel:queue:scheduled:2',
        )
        self.assertEqual(
            self.queue._k_meta('i:UC123'),
            'youtube:channel:meta:i:UC123',
        )
        self.assertEqual(
            self.queue._k_state(
                ChannelState.NOT_FOUND,
            ),
            'youtube:channel:not_found',
        )
        self.assertEqual(
            self.queue._k_tiers(),
            'youtube:channel:tiers',
        )


class TestEnqueueUnresolved(_RedisQueueTestBase):

    async def test_creates_zset_entry_with_now_score(
        self,
    ) -> None:
        before: float = time.time()
        await self.queue.enqueue_unresolved(
            'LinusTechTips', source='cli',
        )
        score: float | None = await self.redis.zscore(
            'youtube:channel:queue:unresolved',
            'h:LinusTechTips',
        )
        self.assertIsNotNone(score)
        assert score is not None
        self.assertGreaterEqual(score, before)

    async def test_priority_uses_zero_score(
        self,
    ) -> None:
        await self.queue.enqueue_unresolved(
            'urgent', source='cli', priority=True,
        )
        score: float | None = await self.redis.zscore(
            'youtube:channel:queue:unresolved',
            'h:urgent',
        )
        self.assertEqual(score, 0.0)

    async def test_idempotent(self) -> None:
        await self.queue.enqueue_unresolved(
            'foo', source='cli',
        )
        await self.queue.enqueue_unresolved(
            'foo', source='importer',
        )
        size: int = await self.redis.zcard(
            'youtube:channel:queue:unresolved',
        )
        self.assertEqual(size, 1)
        meta: dict[str, str] = await self.redis.hgetall(
            'youtube:channel:meta:h:foo',
        )
        # source is HSETNX — first write wins.
        self.assertEqual(meta.get('source'), 'cli')

    async def test_writes_meta(self) -> None:
        await self.queue.enqueue_unresolved(
            'foo', source='importer',
        )
        meta: dict[str, str] = await self.redis.hgetall(
            'youtube:channel:meta:h:foo',
        )
        self.assertEqual(meta.get('handle'), 'foo')
        self.assertEqual(
            meta.get('state'), 'pending_resolution',
        )
        self.assertEqual(
            meta.get('source'), 'importer',
        )
        self.assertIn('created_at', meta)

    async def test_strips_at_prefix(self) -> None:
        await self.queue.enqueue_unresolved(
            '@LinusTechTips', source='cli',
        )
        size: int = await self.redis.zcard(
            'youtube:channel:queue:unresolved',
        )
        self.assertEqual(size, 1)
        score: float | None = await self.redis.zscore(
            'youtube:channel:queue:unresolved',
            'h:LinusTechTips',
        )
        self.assertIsNotNone(score)

    async def test_empty_handle_raises(self) -> None:
        with self.assertRaises(ValueError):
            await self.queue.enqueue_unresolved(
                '@', source='cli',
            )
        with self.assertRaises(ValueError):
            await self.queue.enqueue_unresolved(
                '   ', source='cli',
            )

    async def test_leading_whitespace_then_at_stripped(
        self,
    ) -> None:
        await self.queue.enqueue_unresolved(
            '  @LinusTechTips  ', source='cli',
        )
        score: float | None = await self.redis.zscore(
            'youtube:channel:queue:unresolved',
            'h:LinusTechTips',
        )
        self.assertIsNotNone(score)

    async def test_state_not_overwritten_on_re_enqueue(
        self,
    ) -> None:
        # Simulate a prior promote_to_scheduled by
        # writing meta state=scheduled directly.
        await self.redis.hset(
            'youtube:channel:meta:h:foo',
            mapping={
                'handle': 'foo',
                'state': 'scheduled',
            },
        )
        await self.queue.enqueue_unresolved(
            'foo', source='cli',
        )
        state: str | None = await self.redis.hget(
            'youtube:channel:meta:h:foo', 'state',
        )
        # Idempotent re-enqueue must not clobber the
        # downstream state.
        self.assertEqual(state, 'scheduled')


class TestEnqueueScheduled(_RedisQueueTestBase):

    async def test_lands_in_tier_zero_by_default(
        self,
    ) -> None:
        before: float = time.time()
        await self.queue.enqueue_scheduled(
            'UCabc123', source='cli',
        )
        score: float | None = await self.redis.zscore(
            'youtube:channel:queue:scheduled:0',
            'i:UCabc123',
        )
        self.assertIsNotNone(score)
        assert score is not None
        self.assertGreaterEqual(score, before)

    async def test_records_state_scheduled(
        self,
    ) -> None:
        await self.queue.enqueue_scheduled(
            'UCabc123', source='rss_discover',
        )
        state: str | None = await self.redis.hget(
            'youtube:channel:meta:i:UCabc123', 'state',
        )
        self.assertEqual(state, 'scheduled')
        cid: str | None = await self.redis.hget(
            'youtube:channel:meta:i:UCabc123',
            'channel_id',
        )
        self.assertEqual(cid, 'UCabc123')

    async def test_refuses_terminal_state_atomically(
        self,
    ) -> None:
        # Discovery re-enqueue over an operator removal must be
        # refused: no queue entry, and the meta state stays
        # removed (no queued-but-removed split record).
        await self.queue.enqueue_scheduled(
            'UCabc123', source='cli',
        )
        await self.queue.mark(
            'i:UCabc123', state=ChannelState.REMOVED,
        )
        enqueued: bool = await self.queue.enqueue_scheduled(
            'UCabc123', source='discovered_link',
        )
        self.assertFalse(enqueued)
        self.assertIsNone(await self.redis.zscore(
            'youtube:channel:queue:scheduled:0',
            'i:UCabc123',
        ))
        self.assertEqual(
            await self.redis.hget(
                'youtube:channel:meta:i:UCabc123', 'state',
            ),
            'removed',
        )

    async def test_non_terminal_state_still_enqueues(
        self,
    ) -> None:
        # Idempotent re-enqueue over an active (non-terminal)
        # state keeps returning True.
        await self.queue.enqueue_scheduled(
            'UCabc123', source='cli',
        )
        enqueued: bool = await self.queue.enqueue_scheduled(
            'UCabc123', source='discovered_link',
        )
        self.assertTrue(enqueued)
        self.assertIsNotNone(await self.redis.zscore(
            'youtube:channel:queue:scheduled:0',
            'i:UCabc123',
        ))

    async def test_existing_tier_is_respected(
        self,
    ) -> None:
        # Pre-existing channel already at tier 2
        await self.redis.hset(
            'youtube:channel:tiers',
            'UCabc123',
            '2',
        )
        before: float = time.time()
        await self.queue.enqueue_scheduled(
            'UCabc123', source='cli',
        )
        score_t0: float | None = (
            await self.redis.zscore(
                'youtube:channel:queue:scheduled:0',
                'i:UCabc123',
            )
        )
        score_t2: float | None = (
            await self.redis.zscore(
                'youtube:channel:queue:scheduled:2',
                'i:UCabc123',
            )
        )
        self.assertIsNone(score_t0)
        self.assertIsNotNone(score_t2)
        assert score_t2 is not None
        self.assertGreaterEqual(score_t2, before)

    async def test_priority_writes_score_zero(
        self,
    ) -> None:
        await self.queue.enqueue_scheduled(
            'UCabc123', source='cli', priority=True,
        )
        score: float | None = await self.redis.zscore(
            'youtube:channel:queue:scheduled:0',
            'i:UCabc123',
        )
        self.assertEqual(score, 0.0)

    async def test_state_not_overwritten_on_re_enqueue(
        self,
    ) -> None:
        # Seed meta with a terminal state.
        await self.redis.hset(
            'youtube:channel:meta:i:UCabc123',
            mapping={
                'channel_id': 'UCabc123',
                'state': 'not_found',
            },
        )
        await self.queue.enqueue_scheduled(
            'UCabc123', source='cli',
        )
        state: str | None = await self.redis.hget(
            'youtube:channel:meta:i:UCabc123', 'state',
        )
        self.assertEqual(state, 'not_found')

    async def test_invalid_channel_id_raises(
        self,
    ) -> None:
        with self.assertRaises(ValueError):
            await self.queue.enqueue_scheduled(
                'not-a-channel-id', source='cli',
            )


class TestPopUnresolved(_RedisQueueTestBase):

    async def test_returns_oldest_first(
        self,
    ) -> None:
        await self.redis.zadd(
            'youtube:channel:queue:unresolved',
            {'h:a': 100.0, 'h:b': 200.0, 'h:c': 50.0},
        )
        popped: list[str] = (
            await self.queue.pop_unresolved(2)
        )
        self.assertEqual(popped, ['c', 'a'])

    async def test_removes_from_queue(self) -> None:
        await self.redis.zadd(
            'youtube:channel:queue:unresolved',
            {'h:a': 100.0},
        )
        await self.queue.pop_unresolved(1)
        size: int = await self.redis.zcard(
            'youtube:channel:queue:unresolved',
        )
        self.assertEqual(size, 0)

    async def test_empty_queue_returns_empty_list(
        self,
    ) -> None:
        popped: list[str] = (
            await self.queue.pop_unresolved(10)
        )
        self.assertEqual(popped, [])

    async def test_returned_handles_have_no_prefix(
        self,
    ) -> None:
        await self.redis.zadd(
            'youtube:channel:queue:unresolved',
            {'h:LinusTechTips': 1.0},
        )
        popped: list[str] = (
            await self.queue.pop_unresolved(1)
        )
        self.assertEqual(popped, ['LinusTechTips'])


class TestPopScheduled(_RedisQueueTestBase):

    async def test_batch_fills_from_multiple_tiers(
        self,
    ) -> None:
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:0',
            {'i:UCt0a': 0.0},
        )
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:1',
            {'i:UCt1a': 0.0},
        )
        popped: list[str] = (
            await self.queue.pop_scheduled(
                2, now=1.0,
            )
        )
        self.assertCountEqual(
            popped, ['UCt0a', 'UCt1a'],
        )

    async def test_only_due_entries(self) -> None:
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:0',
            {'i:UCdue': 5.0, 'i:UClater': 100.0},
        )
        popped: list[str] = (
            await self.queue.pop_scheduled(
                10, now=10.0,
            )
        )
        self.assertEqual(popped, ['UCdue'])

    async def test_respects_batch_limit(self) -> None:
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:0',
            {f'i:UC{i:022}': 0.0 for i in range(5)},
        )
        popped: list[str] = (
            await self.queue.pop_scheduled(
                3, now=1.0,
            )
        )
        self.assertEqual(len(popped), 3)

    async def test_stops_when_no_due_entries(
        self,
    ) -> None:
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:0',
            {'i:UClate': 99.0},
        )
        popped: list[str] = (
            await self.queue.pop_scheduled(
                10, now=1.0,
            )
        )
        self.assertEqual(popped, [])


class TestParseChannelPriorityWeights(unittest.TestCase):

    def setUp(self) -> None:
        self.tiers = parse_channel_priority_queues(
            '3:100000000,7:10000000,365:0',
        )

    def test_default_is_inverse_interval_days(self) -> None:
        weights: list[float] = parse_channel_priority_weights(
            '', self.tiers,
        )
        self.assertEqual(weights, [1 / 3, 1 / 7, 1 / 365])

    def test_no_respawn_tier_defaults_to_one(self) -> None:
        tiers = parse_channel_priority_queues('-1:1000,30:0')
        self.assertEqual(
            parse_channel_priority_weights('', tiers),
            [1.0, 1 / 30],
        )

    def test_explicit_weights(self) -> None:
        self.assertEqual(
            parse_channel_priority_weights(
                '5, 2.5,1', self.tiers,
            ),
            [5.0, 2.5, 1.0],
        )

    def test_wrong_count_raises(self) -> None:
        with self.assertRaises(ValueError):
            parse_channel_priority_weights('1,1', self.tiers)

    def test_non_positive_raises(self) -> None:
        bad: str
        for bad in ('1,0,1', '1,-2,1', '1,x,1', '1,nan,1', '1,inf,1'):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    parse_channel_priority_weights(
                        bad, self.tiers,
                    )

    def test_queue_rejects_bad_weights(self) -> None:
        settings: ChannelScrapeQueueSettings = (
            ChannelScrapeQueueSettings(
                _env_file=None,
                channel_priority_queues='7:1000,365:0',
                channel_priority_weights='1',
            )
        )
        with self.assertRaises(ValueError):
            RedisChannelScrapeQueue(
                fakeredis.aioredis.FakeRedis(), settings,
            )


class TestPopScheduledWeighted(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(decode_responses=True)
        )

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    def _queue(self, weights: str) -> RedisChannelScrapeQueue:
        settings: ChannelScrapeQueueSettings = (
            ChannelScrapeQueueSettings(
                _env_file=None,
                channel_priority_queues='3:1000,365:0',
                channel_priority_weights=weights,
            )
        )
        return RedisChannelScrapeQueue(
            self.redis, settings, rng=random.Random(42),
        )

    async def _fill(self, tier: int, count: int) -> None:
        await self.redis.zadd(
            f'youtube:channel:queue:scheduled:{tier}',
            {f'i:UC{tier}x{i:020}': 0.0 for i in range(count)},
        )

    async def test_share_follows_weights(self) -> None:
        queue: RedisChannelScrapeQueue = self._queue('3,1')
        await self._fill(0, 2000)
        await self._fill(1, 2000)
        tier0: int = 0
        _: int
        for _ in range(1000):
            popped: list[str] = await queue.pop_scheduled(
                1, now=1.0,
            )
            self.assertEqual(len(popped), 1)
            if popped[0].startswith('UC0x'):
                tier0 += 1
        self.assertGreater(tier0, 700)
        self.assertLess(tier0, 800)

    async def test_lower_tier_not_starved(self) -> None:
        queue: RedisChannelScrapeQueue = self._queue('')
        await self._fill(0, 500)
        await self._fill(1, 1)
        popped: list[str] = []
        _: int
        for _ in range(400):
            popped.extend(await queue.pop_scheduled(1, now=1.0))
        self.assertIn('UC1x' + f'{0:020}', popped)

    async def test_empty_tier_share_goes_to_others(self) -> None:
        queue: RedisChannelScrapeQueue = self._queue('1000,1')
        await self._fill(1, 5)
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:0',
            {'i:UCnotdue': 50.0},
        )
        popped: list[str] = []
        _: int
        for _ in range(5):
            popped.extend(await queue.pop_scheduled(1, now=1.0))
        self.assertEqual(len(popped), 5)
        self.assertTrue(all(m.startswith('UC1x') for m in popped))


class TestEnqueueNew(_RedisQueueTestBase):
    '''enqueue_new only adds channels the queue has never seen.'''

    async def test_enqueues_unknown_channel(self) -> None:
        result: str = await self.queue.enqueue_new(
            'UCnew0001', source='discovered_popular',
        )
        self.assertEqual(result, 'enqueued')
        self.assertIsNotNone(await self.redis.zscore(
            'youtube:channel:queue:scheduled:0', 'i:UCnew0001',
        ))
        meta: dict[str, str] = await self.redis.hgetall(
            'youtube:channel:meta:i:UCnew0001',
        )
        self.assertEqual(meta['state'], 'scheduled')
        self.assertEqual(meta['source'], 'discovered_popular')

    async def test_leaves_scheduled_channel_untouched(self) -> None:
        await self.queue.enqueue_scheduled('UCknown001', source='cli')
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:0', {'i:UCknown001': 999.0},
        )
        result: str = await self.queue.enqueue_new(
            'UCknown001', source='discovered_popular',
        )
        self.assertEqual(result, 'known')
        self.assertEqual(await self.redis.zscore(
            'youtube:channel:queue:scheduled:0', 'i:UCknown001',
        ), 999.0)
        self.assertEqual(await self.redis.hget(
            'youtube:channel:meta:i:UCknown001', 'source',
        ), 'cli')

    async def test_does_not_requeue_channel_being_scraped(self) -> None:
        await self.queue.enqueue_scheduled('UCflight01', source='cli')
        popped: list[str] = await self.queue.pop_scheduled(
            1, now=time.time() + 1,
        )
        self.assertEqual(popped, ['UCflight01'])
        result: str = await self.queue.enqueue_new(
            'UCflight01', source='discovered_popular',
        )
        self.assertEqual(result, 'known')
        tier: int
        for tier in range(6):
            self.assertIsNone(await self.redis.zscore(
                f'youtube:channel:queue:scheduled:{tier}', 'i:UCflight01',
            ))

    async def test_reports_terminal_channel(self) -> None:
        await self.queue.enqueue_scheduled('UCgone0001', source='cli')
        await self.queue.mark('i:UCgone0001', state=ChannelState.LOW_SUBS)
        result: str = await self.queue.enqueue_new(
            'UCgone0001', source='discovered_popular',
        )
        self.assertEqual(result, 'terminal')
        self.assertEqual(await self.redis.hget(
            'youtube:channel:meta:i:UCgone0001', 'state',
        ), 'low_subs')

    async def test_rejects_non_channel_id(self) -> None:
        with self.assertRaises(ValueError):
            await self.queue.enqueue_new('handle', source='x')


class TestPromoteToScheduled(_RedisQueueTestBase):

    async def test_removes_unresolved_adds_scheduled(
        self,
    ) -> None:
        await self.queue.enqueue_unresolved(
            'foo', source='cli',
        )
        await self.queue.promote_to_scheduled(
            'foo', 'UCfoo00000000000000000000',
        )
        size_u: int = await self.redis.zcard(
            'youtube:channel:queue:unresolved',
        )
        self.assertEqual(size_u, 0)
        score: float | None = await self.redis.zscore(
            'youtube:channel:queue:scheduled:0',
            'i:UCfoo00000000000000000000',
        )
        self.assertEqual(score, 0.0)

    async def test_meta_carries_over(self) -> None:
        await self.queue.enqueue_unresolved(
            'foo', source='importer',
        )
        await self.queue.promote_to_scheduled(
            'foo', 'UCfoo00000000000000000000',
        )
        meta_new: dict[str, str] = (
            await self.redis.hgetall(
                'youtube:channel:meta:i:UCfoo'
                '00000000000000000000',
            )
        )
        self.assertEqual(meta_new.get('handle'), 'foo')
        self.assertEqual(
            meta_new.get('channel_id'),
            'UCfoo00000000000000000000',
        )
        self.assertEqual(
            meta_new.get('state'), 'scheduled',
        )
        self.assertEqual(
            meta_new.get('source'), 'importer',
        )
        size_old: int = await self.redis.exists(
            'youtube:channel:meta:h:foo',
        )
        self.assertEqual(size_old, 0)

    async def test_existing_tier_is_respected(
        self,
    ) -> None:
        await self.redis.hset(
            'youtube:channel:tiers',
            'UCfoo00000000000000000000', '2',
        )
        await self.queue.enqueue_unresolved(
            'foo', source='cli',
        )
        await self.queue.promote_to_scheduled(
            'foo', 'UCfoo00000000000000000000',
        )
        in_t0: float | None = (
            await self.redis.zscore(
                'youtube:channel:queue:scheduled:0',
                'i:UCfoo00000000000000000000',
            )
        )
        in_t2: float | None = (
            await self.redis.zscore(
                'youtube:channel:queue:scheduled:2',
                'i:UCfoo00000000000000000000',
            )
        )
        self.assertIsNone(in_t0)
        self.assertEqual(in_t2, 0.0)

    async def test_invalid_channel_id_raises(
        self,
    ) -> None:
        with self.assertRaises(ValueError):
            await self.queue.promote_to_scheduled(
                'foo', 'not-a-channel-id',
            )

    async def test_destination_meta_preserved(
        self,
    ) -> None:
        # Pre-existing meta on destination id.
        await self.redis.hset(
            'youtube:channel:meta:i:UCfoo'
            '00000000000000000000',
            mapping={
                'last_attempt_at': '999',
                'name': 'old name',
            },
        )
        await self.queue.enqueue_unresolved(
            'foo', source='cli',
        )
        await self.queue.promote_to_scheduled(
            'foo', 'UCfoo00000000000000000000',
        )
        meta: dict[str, str] = (
            await self.redis.hgetall(
                'youtube:channel:meta:i:UCfoo'
                '00000000000000000000',
            )
        )
        # Forced overwrite.
        self.assertEqual(
            meta.get('channel_id'),
            'UCfoo00000000000000000000',
        )
        self.assertEqual(
            meta.get('state'), 'scheduled',
        )
        # Preserved.
        self.assertEqual(
            meta.get('last_attempt_at'), '999',
        )
        self.assertEqual(
            meta.get('name'), 'old name',
        )


class TestUpdateTier(_RedisQueueTestBase):

    async def test_first_scrape_assigns_tier(
        self,
    ) -> None:
        await self.queue.enqueue_scheduled(
            'UCa', source='cli',
        )
        await self.queue.pop_scheduled(
            1, now=time.time(),
        )
        await self.queue.update_tier(
            'UCa', sub_count=1_000_000, now=100.0,
        )
        tier: str | None = await self.redis.hget(
            'youtube:channel:tiers', 'UCa',
        )
        self.assertEqual(tier, '0')
        score: float | None = await self.redis.zscore(
            'youtube:channel:queue:scheduled:0',
            'i:UCa',
        )
        self.assertEqual(score, 100.0 + 604800.0)

    async def test_low_sub_count_uses_tier_minus_one(
        self,
    ) -> None:
        # Override the default spec to put -1:0 in the
        # lowest tier so this test exercises the "drop
        # after one scrape" branch regardless of the
        # production default.
        custom_settings: ChannelScrapeQueueSettings = (
            ChannelScrapeQueueSettings(
                channel_priority_queues=(
                    '7:1000000,30:100000,'
                    '90:10000,180:1000,-1:0'
                ),
            )
        )
        queue: RedisChannelScrapeQueue = (
            RedisChannelScrapeQueue(
                self.redis, custom_settings,
            )
        )
        await queue.update_tier(
            'UCa', sub_count=50, now=100.0,
        )
        score: float | None = await self.redis.zscore(
            'youtube:channel:queue:scheduled:4',
            'i:UCa',
        )
        self.assertIsNone(score)
        tier: str | None = await self.redis.hget(
            'youtube:channel:tiers', 'UCa',
        )
        self.assertEqual(tier, '4')

    async def test_tier_change_clears_old_zset_entry(
        self,
    ) -> None:
        await self.redis.hset(
            'youtube:channel:tiers', 'UCa', '0',
        )
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:0',
            {'i:UCa': 50.0},
        )
        await self.queue.update_tier(
            'UCa', sub_count=10_000, now=100.0,
        )
        old: float | None = await self.redis.zscore(
            'youtube:channel:queue:scheduled:0',
            'i:UCa',
        )
        new: float | None = await self.redis.zscore(
            'youtube:channel:queue:scheduled:2',
            'i:UCa',
        )
        self.assertIsNone(old)
        self.assertIsNotNone(new)

    async def test_clears_unavailable_attempts(
        self,
    ) -> None:
        await self.redis.hset(
            'youtube:channel:meta:i:UCa',
            'unavailable_attempts',
            '2',
        )
        await self.queue.update_tier(
            'UCa', sub_count=1_000_000, now=100.0,
        )
        leftover: str | None = await self.redis.hget(
            'youtube:channel:meta:i:UCa',
            'unavailable_attempts',
        )
        self.assertIsNone(leftover)

    async def test_updates_meta_state_and_last_attempt(
        self,
    ) -> None:
        await self.queue.update_tier(
            'UCa', sub_count=1_000_000, now=100.0,
        )
        meta: dict[str, str] = (
            await self.redis.hgetall(
                'youtube:channel:meta:i:UCa',
            )
        )
        self.assertEqual(meta.get('state'), 'scheduled')
        self.assertEqual(
            meta.get('last_attempt_at'), '100',
        )

    async def test_does_not_override_terminal_state(
        self,
    ) -> None:
        await self.queue.enqueue_scheduled(
            'UCa', source='cli',
        )
        await self.queue.mark(
            'i:UCa', state=ChannelState.TERMINATED,
        )
        # Simulating an in-flight scrape that finishes
        # AFTER the operator marked terminated.
        await self.queue.update_tier(
            'UCa', sub_count=1_000_000, now=200.0,
        )
        state: str | None = await self.redis.hget(
            'youtube:channel:meta:i:UCa', 'state',
        )
        self.assertEqual(state, 'terminated')
        # Must not be re-added to any scheduled tier.
        for t in range(5):
            score: float | None = (
                await self.redis.zscore(
                    f'youtube:channel:queue:scheduled:{t}',
                    'i:UCa',
                )
            )
            self.assertIsNone(score)


class TestRetryMissingSubscriberCount(_RedisQueueTestBase):

    async def test_preserves_tier_and_schedules_metadata_retry(
        self,
    ) -> None:
        settings: ChannelScrapeQueueSettings = (
            ChannelScrapeQueueSettings(
                channel_missing_subscriber_retry_seconds=600,
            )
        )
        queue: RedisChannelScrapeQueue = (
            RedisChannelScrapeQueue(self.redis, settings)
        )
        channel_id: str = 'UCa'
        member: str = f'i:{channel_id}'
        await self.redis.hset(
            'youtube:channel:tiers', channel_id, '2',
        )
        # A count from an earlier scrape: the channel keeps its tier.
        await self.redis.hset(
            f'youtube:channel:meta:{member}', 'subscriber_count', '50000',
        )
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:2',
            {member: 0.0},
        )
        await queue.pop_scheduled(1, now=100.0)

        await queue.retry_missing_subscriber_count(
            channel_id=channel_id,
            now=100.0,
        )

        score: float | None = await self.redis.zscore(
            'youtube:channel:queue:scheduled:2', member,
        )
        self.assertEqual(score, 700.0)
        tier: str | None = await self.redis.hget(
            'youtube:channel:tiers', channel_id,
        )
        self.assertEqual(tier, '2')
        meta: dict[str, str] = await self.redis.hgetall(
            f'youtube:channel:meta:{member}',
        )
        self.assertEqual(meta.get('state'), 'scheduled')
        self.assertEqual(meta.get('last_attempt_at'), '100')
        self.assertEqual(meta.get('force_rescrape_mode'), 'metadata')
        self.assertEqual(
            meta.get('force_source'),
            'missing_subscriber_count',
        )

    async def test_never_counted_channel_uses_catch_all_tier(
        self,
    ) -> None:
        # No stored count: the channel goes to the catch-all tier
        # (index 4 of the default tiers), not tier 0.
        channel_id: str = 'UCa'
        member: str = f'i:{channel_id}'

        await self.queue.retry_missing_subscriber_count(
            channel_id=channel_id,
            now=100.0,
        )

        tier: str | None = await self.redis.hget(
            'youtube:channel:tiers', channel_id,
        )
        self.assertEqual(tier, '4')
        score: float | None = await self.redis.zscore(
            'youtube:channel:queue:scheduled:4', member,
        )
        self.assertEqual(score, 100.0 + 86400.0)

    async def test_counted_channel_without_tier_uses_tier_0(
        self,
    ) -> None:
        channel_id: str = 'UCb'
        member: str = f'i:{channel_id}'
        await self.redis.hset(
            f'youtube:channel:meta:{member}', 'subscriber_count', '50000',
        )

        await self.queue.retry_missing_subscriber_count(
            channel_id=channel_id,
            now=100.0,
        )

        self.assertEqual(
            await self.redis.hget('youtube:channel:tiers', channel_id),
            '0',
        )


class TestRequeueWithBackoff(_RedisQueueTestBase):

    async def test_pushes_score_into_future(
        self,
    ) -> None:
        await self.queue.enqueue_scheduled(
            'UCa', source='cli', priority=True,
        )
        await self.queue.requeue_with_backoff(
            'UCa', seconds=300, now=1000.0,
        )
        score: float | None = await self.redis.zscore(
            'youtube:channel:queue:scheduled:0',
            'i:UCa',
        )
        self.assertEqual(score, 1300.0)

    async def test_handle_keyed_backoff(self) -> None:
        await self.queue.enqueue_unresolved(
            'foo', source='cli', priority=True,
        )
        await self.queue.requeue_with_backoff(
            'foo', seconds=60, now=500.0,
            unresolved=True,
        )
        score: float | None = await self.redis.zscore(
            'youtube:channel:queue:unresolved',
            'h:foo',
        )
        self.assertEqual(score, 560.0)

    async def test_xx_skips_when_not_present(
        self,
    ) -> None:
        # No prior enqueue → XX means no insert.
        await self.queue.requeue_with_backoff(
            'UCnope', seconds=300, now=1000.0,
        )
        size: int = await self.redis.zcard(
            'youtube:channel:queue:scheduled:0',
        )
        self.assertEqual(size, 0)


class TestMark(_RedisQueueTestBase):

    async def test_moves_scheduled_to_not_found(
        self,
    ) -> None:
        await self.queue.enqueue_scheduled(
            'UCa', source='cli',
        )
        await self.queue.mark(
            'i:UCa',
            state=ChannelState.NOT_FOUND,
            last_error='HTTP 404',
        )
        # No longer in any scheduled ZSET.
        for t in range(5):
            score: float | None = (
                await self.redis.zscore(
                    f'youtube:channel:queue'
                    f':scheduled:{t}',
                    'i:UCa',
                )
            )
            self.assertIsNone(score)
        record: str | None = await self.redis.hget(
            'youtube:channel:not_found', 'i:UCa',
        )
        self.assertIsNotNone(record)
        state: str | None = await self.redis.hget(
            'youtube:channel:meta:i:UCa', 'state',
        )
        self.assertEqual(state, 'not_found')

    async def test_not_found_requires_confirmations(
        self,
    ) -> None:
        await self.queue.enqueue_scheduled(
            'UCa', source='cli',
        )
        terminal: bool = (
            await self.queue.mark_not_found_confirmed(
                'i:UCa',
                last_error='HTTP 404',
            )
        )
        self.assertFalse(terminal)
        score: float | None = await self.redis.zscore(
            'youtube:channel:queue:scheduled:0',
            'i:UCa',
        )
        self.assertIsNotNone(score)
        record: str | None = await self.redis.hget(
            'youtube:channel:not_found', 'i:UCa',
        )
        self.assertIsNone(record)
        attempts: str | None = await self.redis.hget(
            'youtube:channel:meta:i:UCa',
            'not_found_attempts',
        )
        self.assertEqual(attempts, '1')

    async def test_third_not_found_marks_terminal(
        self,
    ) -> None:
        await self.queue.enqueue_scheduled(
            'UCa', source='cli',
        )
        await self.queue.mark_not_found_confirmed(
            'i:UCa', last_error='HTTP 404',
        )
        await self.queue.mark_not_found_confirmed(
            'i:UCa', last_error='HTTP 404',
        )
        terminal: bool = (
            await self.queue.mark_not_found_confirmed(
                'i:UCa', last_error='HTTP 404',
            )
        )
        self.assertTrue(terminal)
        record: str | None = await self.redis.hget(
            'youtube:channel:not_found', 'i:UCa',
        )
        self.assertIsNotNone(record)

    async def test_moves_unresolved_to_invalid(
        self,
    ) -> None:
        await self.queue.enqueue_unresolved(
            'badhandle', source='cli',
        )
        await self.queue.mark(
            'h:badhandle',
            state=ChannelState.INVALID_HANDLE,
        )
        in_queue: float | None = (
            await self.redis.zscore(
                'youtube:channel:queue:unresolved',
                'h:badhandle',
            )
        )
        self.assertIsNone(in_queue)
        record: str | None = await self.redis.hget(
            'youtube:channel:invalid_handle',
            'h:badhandle',
        )
        self.assertIsNotNone(record)

    async def test_rejects_non_terminal_state(
        self,
    ) -> None:
        with self.assertRaises(ValueError):
            await self.queue.mark(
                'i:UCa',
                state=ChannelState.SCHEDULED,
            )

    async def test_note_field_stored(self) -> None:
        await self.queue.enqueue_scheduled(
            'UCa', source='cli',
        )
        await self.queue.mark(
            'i:UCa',
            state=ChannelState.TERMINATED,
            note='manually reviewed 2026-05-18',
        )
        record: str | None = await self.redis.hget(
            'youtube:channel:terminated', 'i:UCa',
        )
        self.assertIsNotNone(record)
        assert record is not None
        decoded: dict[str, Any] = json.loads(record)
        self.assertEqual(
            decoded['note'],
            'manually reviewed 2026-05-18',
        )

    async def test_terminal_transition_clears_old(
        self,
    ) -> None:
        # First mark as not_found.
        await self.queue.enqueue_scheduled(
            'UCa', source='cli',
        )
        await self.queue.mark(
            'i:UCa',
            state=ChannelState.NOT_FOUND,
        )
        # Now transition to terminated (operator
        # observed a different signal).
        await self.queue.mark(
            'i:UCa',
            state=ChannelState.TERMINATED,
        )
        nf: str | None = await self.redis.hget(
            'youtube:channel:not_found', 'i:UCa',
        )
        term: str | None = await self.redis.hget(
            'youtube:channel:terminated', 'i:UCa',
        )
        self.assertIsNone(nf)
        self.assertIsNotNone(term)

    async def test_new_terminal_transition_clears_old(
        self,
    ) -> None:
        await self.queue.enqueue_scheduled(
            'UCa', source='cli',
        )
        await self.queue.mark(
            'i:UCa',
            state=ChannelState.NO_VIDEOS,
        )
        await self.queue.mark(
            'i:UCa',
            state=ChannelState.LOW_SUBS,
        )
        no_videos: str | None = await self.redis.hget(
            'youtube:channel:no_videos', 'i:UCa',
        )
        low_subs: str | None = await self.redis.hget(
            'youtube:channel:low_subs', 'i:UCa',
        )
        self.assertIsNone(no_videos)
        self.assertIsNotNone(low_subs)


class TestMarkSoftUnavailable(_RedisQueueTestBase):

    async def test_first_failure_sets_counter_one(
        self,
    ) -> None:
        await self.queue.enqueue_scheduled(
            'UCa', source='cli',
        )
        await self.queue.mark_soft_unavailable(
            'UCa', last_error='timeout',
        )
        count: str | None = await self.redis.hget(
            'youtube:channel:meta:i:UCa',
            'unavailable_attempts',
        )
        self.assertEqual(count, '1')
        record: str | None = await self.redis.hget(
            'youtube:channel:soft_unavailable',
            'i:UCa',
        )
        self.assertIsNotNone(record)
        state: str | None = await self.redis.hget(
            'youtube:channel:meta:i:UCa', 'state',
        )
        self.assertEqual(state, 'soft_unavailable')

    async def test_third_failure_escalates_to_hard(
        self,
    ) -> None:
        await self.queue.enqueue_scheduled(
            'UCa', source='cli',
        )
        for _ in range(3):
            await self.queue.mark_soft_unavailable(
                'UCa', last_error='timeout',
            )
        soft: str | None = await self.redis.hget(
            'youtube:channel:soft_unavailable',
            'i:UCa',
        )
        hard: str | None = await self.redis.hget(
            'youtube:channel:hard_unavailable',
            'i:UCa',
        )
        self.assertIsNone(soft)
        self.assertIsNotNone(hard)
        state: str | None = await self.redis.hget(
            'youtube:channel:meta:i:UCa', 'state',
        )
        self.assertEqual(state, 'hard_unavailable')

    async def test_next_retry_at_one_day_later(
        self,
    ) -> None:
        await self.queue.enqueue_scheduled(
            'UCa', source='cli',
        )
        before: float = time.time()
        await self.queue.mark_soft_unavailable(
            'UCa', last_error='timeout',
        )
        raw: str | None = await self.redis.hget(
            'youtube:channel:soft_unavailable',
            'i:UCa',
        )
        assert raw is not None
        decoded: dict[str, Any] = json.loads(raw)
        self.assertGreaterEqual(
            decoded['next_retry_at'],
            before + 86400 - 1,
        )


class TestUnmark(_RedisQueueTestBase):

    async def test_unmark_returns_to_scheduled(
        self,
    ) -> None:
        await self.queue.enqueue_scheduled(
            'UCa', source='cli',
        )
        await self.queue.mark(
            'i:UCa',
            state=ChannelState.NOT_FOUND,
        )
        await self.queue.unmark('i:UCa')
        in_state: str | None = await self.redis.hget(
            'youtube:channel:not_found', 'i:UCa',
        )
        self.assertIsNone(in_state)
        score: float | None = await self.redis.zscore(
            'youtube:channel:queue:scheduled:0',
            'i:UCa',
        )
        self.assertEqual(score, 0.0)
        state: str | None = await self.redis.hget(
            'youtube:channel:meta:i:UCa', 'state',
        )
        self.assertEqual(state, 'scheduled')

    async def test_unmark_handle_to_unresolved(
        self,
    ) -> None:
        await self.queue.enqueue_unresolved(
            'foo', source='cli',
        )
        await self.queue.mark(
            'h:foo',
            state=ChannelState.INVALID_HANDLE,
        )
        await self.queue.unmark('h:foo')
        score: float | None = await self.redis.zscore(
            'youtube:channel:queue:unresolved',
            'h:foo',
        )
        self.assertIsNotNone(score)
        state: str | None = await self.redis.hget(
            'youtube:channel:meta:h:foo', 'state',
        )
        self.assertEqual(state, 'pending_resolution')

    async def test_unmark_clears_unavailable_counter(
        self,
    ) -> None:
        await self.queue.enqueue_scheduled(
            'UCa', source='cli',
        )
        await self.queue.mark_soft_unavailable(
            'UCa', last_error='timeout',
        )
        await self.queue.unmark('i:UCa')
        c: str | None = await self.redis.hget(
            'youtube:channel:meta:i:UCa',
            'unavailable_attempts',
        )
        self.assertIsNone(c)

    async def test_unmark_respects_existing_tier(
        self,
    ) -> None:
        await self.redis.hset(
            'youtube:channel:tiers', 'UCa', '2',
        )
        await self.queue.enqueue_scheduled(
            'UCa', source='cli',
        )
        await self.queue.mark(
            'i:UCa',
            state=ChannelState.NOT_FOUND,
        )
        await self.queue.unmark('i:UCa')
        in_t2: float | None = await self.redis.zscore(
            'youtube:channel:queue:scheduled:2',
            'i:UCa',
        )
        self.assertEqual(in_t2, 0.0)

    async def test_unmark_with_score_overrides_due_now(
        self,
    ) -> None:
        await self.queue.enqueue_scheduled(
            'UCa', source='cli',
        )
        await self.queue.mark(
            'i:UCa',
            state=ChannelState.UNRESOLVED,
        )
        await self.queue.unmark('i:UCa', score=12345.0)
        score: float | None = await self.redis.zscore(
            'youtube:channel:queue:scheduled:0',
            'i:UCa',
        )
        self.assertEqual(score, 12345.0)

    async def test_unmark_invalid_prefix_raises(
        self,
    ) -> None:
        with self.assertRaises(ValueError):
            await self.queue.unmark('bad_prefix')


class TestForceRescrape(_RedisQueueTestBase):

    async def test_scheduled_writes_force_metadata(
        self,
    ) -> None:
        await self.queue.enqueue_scheduled(
            'UCa', source='cli',
        )
        await self.redis.hset(
            'youtube:channel:meta:i:UCa',
            mapping={
                'unavailable_attempts': '2',
                'resolve_attempts': '1',
                'not_found_attempts': '1',
            },
        )
        await self.queue.force_rescrape(
            'i:UCa',
            mode='full',
            source='cli',
            now=123.0,
        )
        score: float | None = await self.redis.zscore(
            'youtube:channel:queue:scheduled:0',
            'i:UCa',
        )
        self.assertEqual(score, 0.0)
        meta: dict[str, str] = await self.redis.hgetall(
            'youtube:channel:meta:i:UCa',
        )
        self.assertEqual(meta.get('state'), 'scheduled')
        self.assertEqual(
            meta.get('force_rescrape_mode'), 'full',
        )
        self.assertEqual(meta.get('force_source'), 'cli')
        self.assertEqual(meta.get('force_requested_at'), '123')
        self.assertNotIn('unavailable_attempts', meta)
        self.assertNotIn('resolve_attempts', meta)
        self.assertNotIn('not_found_attempts', meta)

    async def test_default_mode_clears_force_metadata(
        self,
    ) -> None:
        await self.queue.enqueue_scheduled(
            'UCa', source='cli',
        )
        await self.redis.hset(
            'youtube:channel:meta:i:UCa',
            mapping={
                'force_rescrape_mode': 'full',
                'force_requested_at': '1',
                'force_source': 'cli',
            },
        )
        await self.queue.force_rescrape(
            'i:UCa',
            mode='default',
            now=456.0,
        )
        meta: dict[str, str] = await self.redis.hgetall(
            'youtube:channel:meta:i:UCa',
        )
        self.assertNotIn('force_rescrape_mode', meta)
        self.assertNotIn('force_requested_at', meta)
        self.assertNotIn('force_source', meta)
        score: float | None = await self.redis.zscore(
            'youtube:channel:queue:scheduled:0',
            'i:UCa',
        )
        self.assertEqual(score, 0.0)

    async def test_terminal_channel_restored_to_scheduled(
        self,
    ) -> None:
        await self.queue.enqueue_scheduled(
            'UCa', source='cli',
        )
        await self.queue.mark(
            'i:UCa',
            state=ChannelState.NOT_FOUND,
        )
        await self.queue.force_rescrape(
            'i:UCa',
            mode='metadata',
            now=789.0,
        )
        terminal_record: str | None = await self.redis.hget(
            'youtube:channel:not_found', 'i:UCa',
        )
        self.assertIsNone(terminal_record)
        score: float | None = await self.redis.zscore(
            'youtube:channel:queue:scheduled:0',
            'i:UCa',
        )
        self.assertEqual(score, 0.0)
        meta: dict[str, str] = await self.redis.hgetall(
            'youtube:channel:meta:i:UCa',
        )
        self.assertEqual(meta.get('state'), 'scheduled')
        self.assertEqual(
            meta.get('force_rescrape_mode'), 'metadata',
        )

    async def test_handle_restored_to_unresolved(
        self,
    ) -> None:
        await self.queue.enqueue_unresolved(
            'foo', source='cli',
        )
        await self.queue.mark(
            'h:foo',
            state=ChannelState.INVALID_HANDLE,
        )
        await self.queue.force_rescrape(
            'h:foo',
            mode='full',
            now=999.0,
        )
        score: float | None = await self.redis.zscore(
            'youtube:channel:queue:unresolved',
            'h:foo',
        )
        self.assertEqual(score, 0.0)
        meta: dict[str, str] = await self.redis.hgetall(
            'youtube:channel:meta:h:foo',
        )
        self.assertEqual(
            meta.get('state'), 'pending_resolution',
        )
        self.assertEqual(
            meta.get('force_rescrape_mode'), 'full',
        )

    async def test_clear_force_rescrape(
        self,
    ) -> None:
        await self.queue.enqueue_scheduled(
            'UCa', source='cli',
        )
        await self.redis.hset(
            'youtube:channel:meta:i:UCa',
            mapping={
                'force_rescrape_mode': 'full',
                'force_requested_at': '1',
                'force_source': 'cli',
            },
        )
        await self.queue.clear_force_rescrape('i:UCa')
        meta: dict[str, str] = await self.redis.hgetall(
            'youtube:channel:meta:i:UCa',
        )
        self.assertNotIn('force_rescrape_mode', meta)
        self.assertNotIn('force_requested_at', meta)
        self.assertNotIn('force_source', meta)

    async def test_invalid_force_mode_raises(
        self,
    ) -> None:
        with self.assertRaises(ValueError):
            await self.queue.force_rescrape(
                'i:UCa',
                mode='bogus',
            )

    async def test_invalid_member_prefix_raises(
        self,
    ) -> None:
        with self.assertRaises(ValueError):
            await self.queue.force_rescrape(
                'bad_prefix',
                mode='full',
            )


class TestReapSoftUnavailable(_RedisQueueTestBase):

    async def test_due_entries_move_back_to_scheduled(
        self,
    ) -> None:
        await self.queue.enqueue_scheduled(
            'UCa', source='cli',
        )
        await self.queue.mark_soft_unavailable(
            'UCa', last_error='timeout',
        )
        # Force next_retry_at into the past.
        record: dict[str, Any] = {
            'ts': 0,
            'next_retry_at': 1.0,
            'last_error': 'timeout',
        }
        await self.redis.hset(
            'youtube:channel:soft_unavailable',
            'i:UCa',
            json.dumps(record),
        )
        reaped: int = (
            await self.queue.reap_soft_unavailable(
                now=100.0,
            )
        )
        self.assertEqual(reaped, 1)
        score: float | None = await self.redis.zscore(
            'youtube:channel:queue:scheduled:0',
            'i:UCa',
        )
        self.assertEqual(score, 0.0)
        soft: str | None = await self.redis.hget(
            'youtube:channel:soft_unavailable',
            'i:UCa',
        )
        self.assertIsNone(soft)

    async def test_undue_entries_stay_parked(
        self,
    ) -> None:
        await self.queue.enqueue_scheduled(
            'UCa', source='cli',
        )
        await self.queue.mark_soft_unavailable(
            'UCa', last_error='timeout',
        )
        reaped: int = (
            await self.queue.reap_soft_unavailable(
                now=time.time(),
            )
        )
        self.assertEqual(reaped, 0)

    async def test_updates_meta_state_on_reap(
        self,
    ) -> None:
        await self.queue.enqueue_scheduled(
            'UCa', source='cli',
        )
        await self.queue.mark_soft_unavailable(
            'UCa', last_error='timeout',
        )
        await self.redis.hset(
            'youtube:channel:soft_unavailable',
            'i:UCa',
            json.dumps({
                'ts': 0,
                'next_retry_at': 1.0,
                'last_error': 'timeout',
            }),
        )
        await self.queue.reap_soft_unavailable(
            now=100.0,
        )
        state: str | None = await self.redis.hget(
            'youtube:channel:meta:i:UCa', 'state',
        )
        self.assertEqual(state, 'scheduled')

    async def test_reap_uses_existing_tier(
        self,
    ) -> None:
        await self.redis.hset(
            'youtube:channel:tiers', 'UCa', '2',
        )
        await self.queue.enqueue_scheduled(
            'UCa', source='cli',
        )
        await self.queue.mark_soft_unavailable(
            'UCa', last_error='timeout',
        )
        await self.redis.hset(
            'youtube:channel:soft_unavailable',
            'i:UCa',
            json.dumps({
                'ts': 0,
                'next_retry_at': 1.0,
                'last_error': 'timeout',
            }),
        )
        await self.queue.reap_soft_unavailable(
            now=100.0,
        )
        in_t0: float | None = await self.redis.zscore(
            'youtube:channel:queue:scheduled:0', 'i:UCa',
        )
        in_t2: float | None = await self.redis.zscore(
            'youtube:channel:queue:scheduled:2', 'i:UCa',
        )
        self.assertIsNone(in_t0)
        self.assertEqual(in_t2, 0.0)


class TestReadOps(_RedisQueueTestBase):

    async def test_get_state_scheduled(self) -> None:
        await self.queue.enqueue_scheduled(
            'UCa', source='cli',
        )
        s: ChannelState | None = (
            await self.queue.get_state('i:UCa')
        )
        self.assertEqual(s, ChannelState.SCHEDULED)

    async def test_get_state_missing(self) -> None:
        s: ChannelState | None = (
            await self.queue.get_state('i:UCnope')
        )
        self.assertIsNone(s)

    async def test_in_state(self) -> None:
        await self.queue.enqueue_unresolved(
            'foo', source='cli',
        )
        self.assertTrue(
            await self.queue.in_state(
                'h:foo',
                ChannelState.PENDING_RESOLUTION,
            ),
        )
        self.assertFalse(
            await self.queue.in_state(
                'h:foo', ChannelState.SCHEDULED,
            ),
        )

    async def test_count_by_state(self) -> None:
        await self.queue.enqueue_scheduled(
            'UCa', source='cli',
        )
        await self.queue.enqueue_scheduled(
            'UCb', source='cli',
        )
        await self.queue.enqueue_unresolved(
            'foo', source='cli',
        )
        await self.queue.mark(
            'i:UCb',
            state=ChannelState.NOT_FOUND,
        )
        counts: dict[ChannelState, int] = (
            await self.queue.count_by_state()
        )
        self.assertEqual(
            counts[ChannelState.SCHEDULED], 1,
        )
        self.assertEqual(
            counts[ChannelState.PENDING_RESOLUTION],
            1,
        )
        self.assertEqual(
            counts[ChannelState.NOT_FOUND], 1,
        )

    async def test_count_by_tier(self) -> None:
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:0',
            {'i:UCa': 1.0, 'i:UCb': 2.0},
        )
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:1',
            {'i:UCc': 3.0},
        )
        counts: dict[int, int] = (
            await self.queue.count_by_tier()
        )
        self.assertEqual(counts[0], 2)
        self.assertEqual(counts[1], 1)
        self.assertEqual(counts[2], 0)

    async def test_search_meta_by_handle(
        self,
    ) -> None:
        await self.queue.enqueue_unresolved(
            'LinusTechTips', source='cli',
        )
        await self.queue.enqueue_unresolved(
            'LinusTechTipsClips', source='cli',
        )
        await self.queue.enqueue_unresolved(
            'MarquesBrownlee', source='cli',
        )
        matches: list[str] = (
            await self.queue.search_meta(
                'Linus*', fields=('handle',),
            )
        )
        self.assertEqual(len(matches), 2)
        self.assertTrue(
            all(
                m.startswith('h:Linus')
                for m in matches
            )
        )

    async def test_get_set_meta(self) -> None:
        await self.queue.enqueue_unresolved(
            'foo', source='cli',
        )
        await self.queue.set_meta(
            'h:foo', note='reviewed',
        )
        meta: dict[str, str] = (
            await self.queue.get_meta('h:foo')
        )
        self.assertEqual(meta.get('note'), 'reviewed')


class TestAwaitingFirstFullScrape(unittest.TestCase):

    def test_rule(self) -> None:
        from scrape_exchange.channel_scrape_queue import (
            awaiting_first_full_scrape,
        )
        cases: list[tuple[tuple[str | None, ...], bool]] = [
            ((None, None, None), False),
            (('scheduled', '0', None), True),
            (('scheduled', '0', '1786000000'), True),
            (('scheduled', None, None), True),
            (('low_subs', None, None), True),
            (('scheduled', None, '1786000000'), False),
            (('scheduled', '1', None), False),
            (('scheduled', '12', '1786000000'), False),
        ]
        fields: tuple[str | None, ...]
        expected: bool
        for fields, expected in cases:
            with self.subTest(fields=fields):
                self.assertEqual(
                    awaiting_first_full_scrape(*fields), expected,
                )


class _MissingSubscriberTestBase(_RedisQueueTestBase):
    '''Fixture for the missing-subscriber retry backoff.'''

    # 1 day for tier 0, 7 days for the catch-all: the retry delay
    # doubles from BASE and clamps at the catch-all interval, so a
    # test run reaches the cap in four misses.
    BASE: int = 24 * 60 * 60
    QUEUES: str = '1:1000000,7:0'
    META: str = 'youtube:channel:meta:i:{channel_id}'

    def _settings(self, queues: str = '') -> ChannelScrapeQueueSettings:
        return ChannelScrapeQueueSettings(
            _env_file=None,
            channel_priority_queues=queues or self.QUEUES,
            channel_missing_subscriber_retry_seconds=self.BASE,
        )

    def _queue(self, queues: str = '') -> RedisChannelScrapeQueue:
        return RedisChannelScrapeQueue(
            self.redis, self._settings(queues),
        )

    def _scheduled_key(self, tier: int) -> str:
        return f'youtube:channel:queue:scheduled:{tier}'

    def _meta_key(self, channel_id: str) -> str:
        return self.META.format(channel_id=channel_id)

    async def _seed(
        self, channel_id: str, tier: int = 1, *, schedule: bool = True,
        subscriber_count: int | None = 5000,
    ) -> str:
        '''A scheduled channel sitting in *tier*. By default it has a
        stored subscriber count from an earlier scrape, so a miss is
        retried with backoff; ``subscriber_count=None`` seeds a channel
        that never had one.'''

        member: str = f'i:{channel_id}'
        await self.redis.hset(
            'youtube:channel:tiers', channel_id, str(tier),
        )
        fields: dict[str, str] = {
            'state': ChannelState.SCHEDULED.value,
            'successful_scrapes': '0',
        }
        if subscriber_count is not None:
            fields['subscriber_count'] = str(subscriber_count)
        await self.redis.hset(self._meta_key(channel_id), mapping=fields)
        if schedule:
            await self.redis.zadd(self._scheduled_key(tier), {member: 0.0})
        return member

    async def _attempts(self, channel_id: str) -> str | None:
        return await self.redis.hget(
            self._meta_key(channel_id), 'missing_subscriber_attempts',
        )

    async def _score(self, channel_id: str, tier: int) -> float | None:
        return await self.redis.zscore(
            self._scheduled_key(tier), f'i:{channel_id}',
        )


class TestMissingSubscriberBackoff(_MissingSubscriberTestBase):
    '''Successive misses back off exponentially and clamp at the
    catch-all interval, on both the Lua and the Python retry path.'''

    # (counter after the miss, delay scheduled for that miss)
    EXPECTED: tuple[tuple[int, int], ...] = (
        (1, 24 * 60 * 60),        # 1 day
        (2, 2 * 24 * 60 * 60),    # 2 days
        (3, 4 * 24 * 60 * 60),    # 4 days
        (4, 7 * 24 * 60 * 60),    # 8 days -> capped at 7 days
        (5, 7 * 24 * 60 * 60),    # still capped
    )

    async def test_lua_path_backoff_doubles_then_caps(self) -> None:
        queue: RedisChannelScrapeQueue = self._queue()
        channel_id: str = 'UCmissing1'
        member: str = await self._seed(channel_id)
        progress: ChannelScrapeProgress = ChannelScrapeProgress(
            successful_scrapes=0, next_full_scrape=1,
        )
        now: float = 1000.0
        expected_attempts: int
        delay: int
        for expected_attempts, delay in self.EXPECTED:
            await queue.update_tier(
                channel_id, sub_count=None, now=now,
                progress=progress,
            )
            self.assertEqual(
                await self._score(channel_id, 1), now + delay,
            )
            self.assertEqual(
                await self._attempts(channel_id),
                str(expected_attempts),
            )
            # The script committed the scrape: successful_scrapes
            # advanced by one, which the next snapshot must match.
            progress = ChannelScrapeProgress(
                successful_scrapes=expected_attempts,
                next_full_scrape=1,
            )
        self.assertEqual(
            await self.redis.hget(
                self._meta_key(channel_id), 'successful_scrapes',
            ),
            '5',
        )
        self.assertNotIn(member, await self.redis.zrange(
            self._scheduled_key(0), 0, -1,
        ))

    async def test_python_path_backoff_doubles_then_caps(self) -> None:
        queue: RedisChannelScrapeQueue = self._queue()
        channel_id: str = 'UCmissing2'
        member: str = await self._seed(channel_id)
        now: float = 1000.0
        expected_attempts: int
        delay: int
        for expected_attempts, delay in self.EXPECTED:
            await queue.retry_missing_subscriber_count(
                channel_id=channel_id, now=now,
            )
            self.assertEqual(
                await self._score(channel_id, 1), now + delay,
            )
            self.assertEqual(
                await self._attempts(channel_id),
                str(expected_attempts),
            )
        self.assertNotIn(member, await self.redis.zrange(
            self._scheduled_key(0), 0, -1,
        ))


class TestMissingSubscriberBackoffState(_MissingSubscriberTestBase):
    '''Counter reset, tier preservation, terminal states, the
    negative catch-all, and the retry log record.'''

    async def test_count_resets_counter_in_lua_path(self) -> None:
        queue: RedisChannelScrapeQueue = self._queue()
        channel_id: str = 'UCreset1'
        await self._seed(channel_id)
        progress: ChannelScrapeProgress = ChannelScrapeProgress(
            successful_scrapes=0, next_full_scrape=1,
        )
        attempts: int
        for attempts in (0, 1):
            await queue.update_tier(
                channel_id, sub_count=None, now=1000.0,
                progress=progress,
            )
            progress = ChannelScrapeProgress(
                successful_scrapes=attempts + 1,
                next_full_scrape=1,
            )
        self.assertEqual(await self._attempts(channel_id), '2')

        await queue.update_tier(
            channel_id, sub_count=2_000_000, now=2000.0,
            progress=progress,
        )

        # A scrape that carries a count clears the miss counter and
        # moves the channel to its real tier (>= 1M subscribers: 0).
        self.assertIsNone(await self._attempts(channel_id))
        self.assertEqual(
            await self.redis.hget('youtube:channel:tiers', channel_id),
            '0',
        )
        self.assertEqual(
            await self._score(channel_id, 0), 2000.0 + 24 * 60 * 60,
        )

    async def test_count_resets_counter_in_python_path(self) -> None:
        queue: RedisChannelScrapeQueue = self._queue()
        channel_id: str = 'UCreset2'
        await self._seed(channel_id)
        await queue.retry_missing_subscriber_count(
            channel_id=channel_id, now=1000.0,
        )
        await queue.retry_missing_subscriber_count(
            channel_id=channel_id, now=2000.0,
        )
        self.assertEqual(await self._attempts(channel_id), '2')

        await queue.update_tier(
            channel_id, sub_count=2_000_000, now=3000.0,
        )

        self.assertIsNone(await self._attempts(channel_id))
        self.assertEqual(
            await self.redis.hget('youtube:channel:tiers', channel_id),
            '0',
        )

    async def test_tier_is_preserved_on_a_miss(self) -> None:
        queue: RedisChannelScrapeQueue = self._queue()
        channel_id: str = 'UCkeep'
        await self._seed(channel_id, tier=1)
        progress: ChannelScrapeProgress = ChannelScrapeProgress(
            successful_scrapes=0, next_full_scrape=1,
        )

        await queue.update_tier(
            channel_id, sub_count=None, now=1000.0,
            progress=progress,
        )

        self.assertEqual(
            await self.redis.hget('youtube:channel:tiers', channel_id),
            '1',
        )
        self.assertEqual(
            await self._score(channel_id, 1), 1000.0 + 24 * 60 * 60,
        )
        # The retry stays in the last (catch-all) tier, not tier 0.
        self.assertIsNone(await self._score(channel_id, 0))

        # The Python path shares the counter, so this miss doubles.
        await queue.retry_missing_subscriber_count(
            channel_id=channel_id, now=2000.0,
        )
        self.assertEqual(
            await self.redis.hget('youtube:channel:tiers', channel_id),
            '1',
        )
        self.assertEqual(
            await self._score(channel_id, 1), 2000.0 + 2 * 24 * 60 * 60,
        )

    async def test_terminal_state_is_not_overridden(self) -> None:
        queue: RedisChannelScrapeQueue = self._queue()
        channel_id: str = 'UCgone'
        member: str = await self._seed(channel_id, schedule=False)
        await self.redis.hset(
            self._meta_key(channel_id),
            'state', ChannelState.TERMINATED.value,
        )

        await queue.retry_missing_subscriber_count(
            channel_id=channel_id, now=1000.0,
        )

        self.assertEqual(
            await self.redis.hget(self._meta_key(channel_id), 'state'),
            ChannelState.TERMINATED.value,
        )
        self.assertIsNone(await self._score(channel_id, 1))
        self.assertIsNone(await self._attempts(channel_id))

        # The Lua compare-and-set refuses a non-scheduled channel.
        committed: bool | None = await queue.update_tier(
            channel_id, sub_count=None, now=2000.0,
            progress=ChannelScrapeProgress(
                successful_scrapes=0, next_full_scrape=1,
            ),
        )
        self.assertFalse(committed)
        self.assertIsNone(await self._score(channel_id, 1))
        self.assertIsNone(await self._attempts(channel_id))
        self.assertNotIn(member, await self.redis.zrange(
            self._scheduled_key(0), 0, -1,
        ))

    async def test_negative_catch_all_interval_drops_channel(
        self,
    ) -> None:
        # "Drop after one successful scrape" applied to the catch-all
        # tier: a miss removes the channel from the schedule.
        queue: RedisChannelScrapeQueue = self._queue('1:1000000,-1:0')
        channel_id: str = 'UCdrop'
        await self._seed(channel_id, tier=1)

        await queue.update_tier(
            channel_id, sub_count=None, now=1000.0,
            progress=ChannelScrapeProgress(
                successful_scrapes=0, next_full_scrape=1,
            ),
        )

        self.assertEqual(
            await self.redis.hget('youtube:channel:tiers', channel_id),
            '1',
        )
        self.assertIsNone(await self._score(channel_id, 1))
        self.assertIsNone(await self._score(channel_id, 0))
        self.assertEqual(await self._attempts(channel_id), '1')

        await queue.retry_missing_subscriber_count(
            channel_id=channel_id, now=2000.0,
        )
        self.assertIsNone(await self._score(channel_id, 1))
        self.assertIsNone(await self._score(channel_id, 0))

    async def test_retry_is_logged_with_attempts_and_delay(self) -> None:
        queue: RedisChannelScrapeQueue = self._queue()
        channel_id: str = 'UClog'
        await self._seed(channel_id)

        with self.assertLogs(
            'scrape_exchange.channel_scrape_queue', level='INFO',
        ) as captured:
            await queue.retry_missing_subscriber_count(
                channel_id=channel_id, now=1000.0,
            )
        records: list = self._retry_records(captured.records)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].channel_id, channel_id)
        self.assertEqual(records[0].missing_subscriber_attempts, 1)
        self.assertEqual(
            records[0].retry_delay_seconds, float(self.BASE),
        )

        with self.assertLogs(
            'scrape_exchange.channel_scrape_queue', level='INFO',
        ) as captured:
            await queue.update_tier(
                channel_id, sub_count=None, now=2000.0,
                progress=ChannelScrapeProgress(
                    successful_scrapes=0, next_full_scrape=1,
                ),
            )
        records = self._retry_records(captured.records)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].channel_id, channel_id)
        self.assertEqual(records[0].missing_subscriber_attempts, 2)
        self.assertEqual(
            records[0].retry_delay_seconds, float(self.BASE * 2),
        )

    async def test_logged_attempts_are_not_clamped(self) -> None:
        # The exponent is clamped to keep 2 ** n finite; the logged
        # attempt count must still be the stored counter.
        queue: RedisChannelScrapeQueue = self._queue()
        cap: int = 7 * 24 * 60 * 60
        channel_id: str = 'UCmany'
        await self._seed(channel_id)
        await self.redis.hset(
            self._meta_key(channel_id),
            'missing_subscriber_attempts', '25',
        )

        with self.assertLogs(
            'scrape_exchange.channel_scrape_queue', level='INFO',
        ) as captured:
            await queue.update_tier(
                channel_id, sub_count=None, now=1000.0,
                progress=ChannelScrapeProgress(
                    successful_scrapes=0, next_full_scrape=1,
                ),
            )
        records: list = self._retry_records(captured.records)
        self.assertEqual(records[0].missing_subscriber_attempts, 26)
        self.assertEqual(records[0].retry_delay_seconds, float(cap))

        with self.assertLogs(
            'scrape_exchange.channel_scrape_queue', level='INFO',
        ) as captured:
            await queue.retry_missing_subscriber_count(
                channel_id=channel_id, now=2000.0,
            )
        records = self._retry_records(captured.records)
        self.assertEqual(records[0].missing_subscriber_attempts, 27)
        self.assertEqual(records[0].retry_delay_seconds, float(cap))

    @staticmethod
    def _retry_records(records: list) -> list:
        return [
            record for record in records
            if record.getMessage() == 'Scheduling missing-subscriber retry'
        ]



class TestUnpublishedSubscriberCount(_MissingSubscriberTestBase):
    '''Channels that never had a subscriber count: one retry in the
    catch-all tier, then the count is treated as not public.'''

    DAY: int = 24 * 60 * 60
    CATCH_ALL: int = 1
    CATCH_ALL_INTERVAL: int = 7 * 24 * 60 * 60

    def _progress(self, scrapes: int) -> ChannelScrapeProgress:
        return ChannelScrapeProgress(
            successful_scrapes=scrapes, next_full_scrape=1,
        )

    async def _field(self, channel_id: str, field: str) -> str | None:
        return await self.redis.hget(self._meta_key(channel_id), field)

    async def test_lua_path_retries_once_then_not_public(self) -> None:
        queue: RedisChannelScrapeQueue = self._queue()
        channel_id: str = 'UCtiny1'
        await self._seed(channel_id, tier=0, subscriber_count=None)

        await queue.update_tier(
            channel_id, sub_count=None, now=1000.0,
            progress=self._progress(0),
        )
        # First miss: one retry, in the catch-all tier, not tier 0.
        self.assertIsNone(await self._score(channel_id, 0))
        self.assertEqual(
            await self._score(channel_id, self.CATCH_ALL),
            1000.0 + self.DAY,
        )
        self.assertEqual(
            await self.redis.hget('youtube:channel:tiers', channel_id),
            str(self.CATCH_ALL),
        )
        self.assertEqual(await self._attempts(channel_id), '1')

        with self.assertLogs(
            'scrape_exchange.channel_scrape_queue', level='INFO',
        ) as captured:
            await queue.update_tier(
                channel_id, sub_count=None, now=2000.0,
                progress=self._progress(1),
            )
        # Second miss: not public, scheduled normally.
        self.assertEqual(
            await self._score(channel_id, self.CATCH_ALL),
            2000.0 + self.CATCH_ALL_INTERVAL,
        )
        self.assertEqual(
            await self._field(channel_id, 'subscriber_count_not_public'),
            '2000',
        )
        self.assertIsNone(await self._attempts(channel_id))
        self.assertIsNone(await self._field(channel_id, 'force_source'))
        self.assertIsNone(
            await self._field(channel_id, 'force_rescrape_mode'),
        )
        self.assertIn(
            'Subscriber count not public',
            [r.getMessage() for r in captured.records],
        )

    async def test_python_path_retries_once_then_not_public(self) -> None:
        queue: RedisChannelScrapeQueue = self._queue()
        channel_id: str = 'UCtiny2'
        await self._seed(channel_id, tier=0, subscriber_count=None)

        await queue.retry_missing_subscriber_count(
            channel_id=channel_id, now=1000.0,
        )
        self.assertIsNone(await self._score(channel_id, 0))
        self.assertEqual(
            await self._score(channel_id, self.CATCH_ALL),
            1000.0 + self.DAY,
        )
        self.assertEqual(await self._attempts(channel_id), '1')

        await queue.retry_missing_subscriber_count(
            channel_id=channel_id, now=2000.0,
        )
        self.assertEqual(
            await self._score(channel_id, self.CATCH_ALL),
            2000.0 + self.CATCH_ALL_INTERVAL,
        )
        self.assertEqual(
            await self._field(channel_id, 'subscriber_count_not_public'),
            '2000',
        )
        self.assertIsNone(await self._attempts(channel_id))
        self.assertIsNone(await self._field(channel_id, 'force_source'))

    async def test_stored_zero_counts_as_no_count(self) -> None:
        queue: RedisChannelScrapeQueue = self._queue()
        channel_id: str = 'UCzero'
        await self._seed(channel_id, tier=0, subscriber_count=0)
        await self.redis.hset(
            self._meta_key(channel_id), 'missing_subscriber_attempts', '1',
        )

        await queue.update_tier(
            channel_id, sub_count=None, now=1000.0,
            progress=self._progress(0),
        )
        self.assertEqual(
            await self._field(channel_id, 'subscriber_count_not_public'),
            '1000',
        )
        self.assertIsNone(await self._score(channel_id, 0))

    async def test_counted_channel_keeps_retrying_in_its_tier(self) -> None:
        queue: RedisChannelScrapeQueue = self._queue()
        channel_id: str = 'UCknown'
        await self._seed(channel_id, tier=0, subscriber_count=2_000_000)
        await self.redis.hset(
            self._meta_key(channel_id), 'missing_subscriber_attempts', '3',
        )

        await queue.update_tier(
            channel_id, sub_count=None, now=1000.0,
            progress=self._progress(0),
        )
        self.assertEqual(
            await self._score(channel_id, 0), 1000.0 + self.CATCH_ALL_INTERVAL,
        )
        self.assertEqual(await self._attempts(channel_id), '4')
        self.assertIsNone(
            await self._field(channel_id, 'subscriber_count_not_public'),
        )

    async def test_not_public_channel_is_not_retried_again(self) -> None:
        queue: RedisChannelScrapeQueue = self._queue()
        channel_id: str = 'UCyearly'
        await self._seed(channel_id, tier=1, subscriber_count=None)
        await self.redis.hset(
            self._meta_key(channel_id),
            'subscriber_count_not_public', '500',
        )

        await queue.update_tier(
            channel_id, sub_count=None, now=1000.0,
            progress=self._progress(0),
        )
        self.assertEqual(
            await self._score(channel_id, self.CATCH_ALL),
            1000.0 + self.CATCH_ALL_INTERVAL,
        )
        self.assertIsNone(await self._attempts(channel_id))

        await queue.retry_missing_subscriber_count(
            channel_id=channel_id, now=2000.0,
        )
        self.assertEqual(
            await self._score(channel_id, self.CATCH_ALL),
            2000.0 + self.CATCH_ALL_INTERVAL,
        )
        self.assertIsNone(await self._attempts(channel_id))

    async def test_found_count_clears_not_public(self) -> None:
        queue: RedisChannelScrapeQueue = self._queue()
        channel_id: str = 'UCfound'
        await self._seed(channel_id, tier=1, subscriber_count=None)
        await self.redis.hset(
            self._meta_key(channel_id),
            'subscriber_count_not_public', '500',
        )

        await queue.update_tier(
            channel_id, sub_count=2_000_000, now=1000.0,
            progress=self._progress(0),
        )
        self.assertIsNone(
            await self._field(channel_id, 'subscriber_count_not_public'),
        )

    async def test_found_count_clears_not_public_python_path(
        self,
    ) -> None:
        queue: RedisChannelScrapeQueue = self._queue()
        channel_id: str = 'UCfound2'
        await self._seed(channel_id, tier=1, subscriber_count=None)
        await self.redis.hset(
            self._meta_key(channel_id),
            'subscriber_count_not_public', '500',
        )

        await queue.update_tier(
            channel_id, sub_count=2_000_000, now=1000.0,
        )
        self.assertIsNone(
            await self._field(channel_id, 'subscriber_count_not_public'),
        )


class TestFixUnpublishedSubscriberRetries(_MissingSubscriberTestBase):
    '''One-off cleanup of channels stuck retrying a count YouTube
    does not publish.'''

    CATCH_ALL_INTERVAL: int = 7 * 24 * 60 * 60

    async def _stuck(
        self, channel_id: str, *, attempts: str = '2',
        subscriber_count: int | None = None, flagged: bool = True,
    ) -> str:
        member: str = await self._seed(
            channel_id, tier=0, subscriber_count=subscriber_count,
        )
        fields: dict[str, str] = {'missing_subscriber_attempts': attempts}
        if flagged:
            fields.update({
                'force_source': 'missing_subscriber_count',
                'force_rescrape_mode': 'metadata',
                'force_requested_at': '1',
            })
        await self.redis.hset(self._meta_key(channel_id), mapping=fields)
        return member

    async def _setup(self) -> RedisChannelScrapeQueue:
        await self._stuck('UCstuck1')
        await self._stuck('UCstuck2', subscriber_count=0)
        await self._stuck('UCcounted', subscriber_count=5000)
        await self._stuck('UCnoattempt', attempts='0')
        await self._stuck('UCnotflagged', flagged=False)
        return self._queue()

    async def test_dry_run_counts_and_changes_nothing(self) -> None:
        queue: RedisChannelScrapeQueue = await self._setup()
        result: dict[str, int] = (
            await queue.fix_unpublished_subscriber_retries(
                apply=False, now=1000.0,
            )
        )
        self.assertEqual(result['eligible'], 2)
        self.assertEqual(result['moved'], 0)
        self.assertEqual(
            await self.redis.zcard(self._scheduled_key(0)), 5,
        )

    async def test_apply_moves_only_eligible_channels(self) -> None:
        queue: RedisChannelScrapeQueue = await self._setup()
        result: dict[str, int] = (
            await queue.fix_unpublished_subscriber_retries(
                apply=True, now=1000.0,
            )
        )
        self.assertEqual(result['moved'], 2)
        channel_id: str
        for channel_id in ('UCstuck1', 'UCstuck2'):
            self.assertIsNone(await self._score(channel_id, 0))
            score: float | None = await self._score(channel_id, 1)
            assert score is not None
            self.assertGreaterEqual(score, 1000.0)
            self.assertLessEqual(score, 1000.0 + self.CATCH_ALL_INTERVAL)
            self.assertEqual(
                await self.redis.hget('youtube:channel:tiers', channel_id),
                '1',
            )
            meta: dict[str, str] = await self.redis.hgetall(
                self._meta_key(channel_id),
            )
            self.assertEqual(meta.get('subscriber_count_not_public'), '1000')
            self.assertNotIn('force_source', meta)
            self.assertNotIn('missing_subscriber_attempts', meta)
        for channel_id in ('UCcounted', 'UCnoattempt', 'UCnotflagged'):
            self.assertIsNotNone(await self._score(channel_id, 0))

    async def test_in_flight_channel_is_left_alone(self) -> None:
        queue: RedisChannelScrapeQueue = self._queue()
        await self._stuck('UCbusy')
        # A worker popped it between the scan and the move.
        await self.redis.zrem(self._scheduled_key(0), 'i:UCbusy')
        await self.redis.zadd(self._scheduled_key(0), {'i:UCother': 5.0})
        result: dict[str, int] = (
            await queue.fix_unpublished_subscriber_retries(
                apply=True, now=1000.0,
            )
        )
        self.assertEqual(result['moved'], 0)
        self.assertIsNone(await self._score('UCbusy', 1))

class TestListingCounts(_RedisQueueTestBase):
    '''Per-tier video listing status counters.'''

    async def _counts(self) -> dict[tuple[int, str], int]:
        counts: dict[tuple[int, str], int] = (
            await self.queue.count_listing()
        )
        return {key: n for key, n in counts.items() if n}

    async def test_new_channel_progresses_to_complete(self) -> None:
        self.assertEqual(
            await self.queue.enqueue_new('UCa', source='test'),
            'enqueued',
        )
        self.assertEqual(await self._counts(), {(0, 'never_scraped'): 1})
        progress: ChannelScrapeProgress = (
            await self.queue.get_scrape_progress('UCa', existing=False)
        )
        self.assertEqual(await self._counts(), {(0, 'never_scraped'): 1})
        self.assertTrue(await self.queue.update_tier(
            'UCa', sub_count=50, now=time.time(), progress=progress,
        ))
        self.assertEqual(await self._counts(), {(4, 'partial'): 1})
        await self.queue.set_meta(
            'i:UCa', video_ids_enumerated_at=str(int(time.time())),
        )
        self.assertEqual(await self._counts(), {(4, 'complete'): 1})

    async def test_pre_tracking_channel_becomes_partial(self) -> None:
        await self.redis.hset('youtube:channel:meta:i:UCold', mapping={
            'state': 'scheduled', 'last_attempt_at': '1',
        })
        await self.redis.hset('youtube:channel:tiers', 'UCold', '2')
        await self.queue.recount_listing('i:UCold')
        self.assertEqual(await self._counts(), {(2, 'pre_tracking'): 1})
        await self.queue.get_scrape_progress('UCold', existing=True)
        self.assertEqual(await self._counts(), {(2, 'partial'): 1})

    async def test_terminal_channels_are_not_counted(self) -> None:
        await self.queue.enqueue_new('UCa', source='test')
        await self.queue.mark('i:UCa', state=ChannelState.TOPIC)
        self.assertEqual(await self._counts(), {})
        self.assertIsNone(await self.redis.hget(
            'youtube:channel:meta:i:UCa', 'listing_bucket',
        ))
        await self.queue.unmark('i:UCa')
        self.assertEqual(await self._counts(), {(0, 'never_scraped'): 1})

    async def test_force_rescrape_revives_count(self) -> None:
        await self.queue.enqueue_new('UCa', source='test')
        await self.queue.mark('i:UCa', state=ChannelState.REMOVED)
        await self.queue.force_rescrape('i:UCa', mode='full')
        self.assertEqual(await self._counts(), {(0, 'never_scraped'): 1})

    async def test_handles_are_not_counted(self) -> None:
        await self.queue.enqueue_unresolved('@someone', source='test')
        await self.queue.recount_listing('h:someone')
        self.assertEqual(await self._counts(), {})
        await self.queue.promote_to_scheduled('someone', 'UCx')
        self.assertEqual(await self._counts(), {(0, 'never_scraped'): 1})

    async def test_recount_heals_unhooked_write(self) -> None:
        await self.queue.enqueue_new('UCa', source='test')
        await self.redis.hset(
            'youtube:channel:meta:i:UCa', 'successful_scrapes', '3',
        )
        self.assertEqual(await self._counts(), {(0, 'never_scraped'): 1})
        await self.queue.recount_listing('i:UCa')
        await self.queue.recount_listing('i:UCa')
        self.assertEqual(await self._counts(), {(0, 'partial'): 1})

    async def test_reset_and_recount(self) -> None:
        await self.queue.enqueue_new('UCa', source='test')
        await self.queue.enqueue_new('UCb', source='test')
        await self.queue.reset_listing_counts()
        self.assertEqual(await self._counts(), {})
        await self.queue.reset_listing_counts('i:UCa', 'i:UCb')
        await self.queue.recount_listing('i:UCa', 'i:UCb')
        self.assertEqual(await self._counts(), {(0, 'never_scraped'): 2})
