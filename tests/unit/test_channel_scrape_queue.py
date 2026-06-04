'''Unit tests for scrape_exchange.channel_scrape_queue.'''

import json
import time
import unittest
from typing import Any

import fakeredis.aioredis

from scrape_exchange.channel_scrape_queue import (
    ChannelScrapeQueue,
    ChannelScrapeQueueSettings,
    ChannelState,
    RedisChannelScrapeQueue,
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

    def test_terminal_states_set(self) -> None:
        terminal: frozenset[ChannelState] = (
            ChannelState.terminal_states()
        )
        self.assertIn(ChannelState.NOT_FOUND, terminal)
        self.assertIn(
            ChannelState.SOFT_UNAVAILABLE, terminal,
        )
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

    async def test_drains_tier_zero_before_one(
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
        self.assertEqual(
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
