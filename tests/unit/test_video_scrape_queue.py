'''Unit tests for scrape_exchange.video_scrape_queue.'''

import json
import time
import unittest
from typing import Any

import fakeredis.aioredis

from scrape_exchange.video_scrape_queue import (
    RedisVideoScrapeQueue,
    VideoScrapeQueue,
    VideoScrapeQueueSettings,
    VideoState,
)


class TestVideoStateEnum(unittest.TestCase):

    def test_enum_values_are_lower_snake(self) -> None:
        self.assertEqual(
            VideoState.QUEUED.value, 'queued',
        )
        self.assertEqual(
            VideoState.UNAVAILABLE.value, 'unavailable',
        )
        self.assertEqual(
            VideoState.FAILED.value, 'failed',
        )
        self.assertEqual(
            VideoState.REMOVED.value, 'removed',
        )

    def test_terminal_states_set(self) -> None:
        terminal: frozenset[VideoState] = (
            VideoState.terminal_states()
        )
        self.assertIn(VideoState.UNAVAILABLE, terminal)
        self.assertIn(VideoState.FAILED, terminal)
        self.assertIn(VideoState.REMOVED, terminal)
        self.assertNotIn(VideoState.QUEUED, terminal)


class TestVideoScrapeQueueSettings(unittest.TestCase):

    def test_defaults(self) -> None:
        s: VideoScrapeQueueSettings = (
            VideoScrapeQueueSettings()
        )
        self.assertEqual(s.video_queue_batch, 50)
        self.assertEqual(
            s.video_queue_idle_poll_seconds, 2.0,
        )
        self.assertEqual(
            s.video_transient_max_attempts, 3,
        )
        self.assertEqual(
            s.video_transient_backoff_seconds, 30,
        )


class _RedisQueueTestBase(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(
                decode_responses=True,
            )
        )
        self.settings: VideoScrapeQueueSettings = (
            VideoScrapeQueueSettings()
        )
        self.queue: RedisVideoScrapeQueue = (
            RedisVideoScrapeQueue(
                self.redis, self.settings,
            )
        )

    async def asyncTearDown(self) -> None:
        await self.redis.flushall()
        await self.redis.aclose()


class TestKeyPrefixes(_RedisQueueTestBase):

    async def test_inherits_abc(self) -> None:
        self.assertIsInstance(
            self.queue, VideoScrapeQueue,
        )

    async def test_key_helpers(self) -> None:
        self.assertEqual(
            self.queue._k_queue(),
            'youtube:video:queue',
        )
        self.assertEqual(
            self.queue._k_meta('dQw4w9WgXcQ'),
            'youtube:video:meta:dQw4w9WgXcQ',
        )
        self.assertEqual(
            self.queue._k_state(VideoState.UNAVAILABLE),
            'youtube:video:unavailable',
        )
        self.assertEqual(
            self.queue._k_state(VideoState.FAILED),
            'youtube:video:failed',
        )
        self.assertEqual(
            self.queue._k_state(VideoState.REMOVED),
            'youtube:video:removed',
        )


class TestEnqueue(_RedisQueueTestBase):

    async def test_creates_zset_entry_with_now_score(
        self,
    ) -> None:
        # Enqueue uses int(time.time()) as the score
        # so capture the lower bound as an integer to
        # avoid sub-second comparison failures.
        before: int = int(time.time())
        added: bool = await self.queue.enqueue(
            'dQw4w9WgXcQ', source='rss',
        )
        self.assertTrue(added)
        score: float | None = await self.redis.zscore(
            'youtube:video:queue', 'dQw4w9WgXcQ',
        )
        self.assertIsNotNone(score)
        assert score is not None
        self.assertGreaterEqual(score, before)

    async def test_idempotent(self) -> None:
        added_first: bool = await self.queue.enqueue(
            'dQw4w9WgXcQ', source='rss',
        )
        added_second: bool = await self.queue.enqueue(
            'dQw4w9WgXcQ', source='cli',
        )
        self.assertTrue(added_first)
        self.assertFalse(added_second)
        size: int = await self.redis.zcard(
            'youtube:video:queue',
        )
        self.assertEqual(size, 1)
        meta_source: str | None = await self.redis.hget(
            'youtube:video:meta:dQw4w9WgXcQ', 'source',
        )
        self.assertEqual(meta_source, 'rss')

    async def test_writes_meta(self) -> None:
        await self.queue.enqueue(
            'dQw4w9WgXcQ', source='rss',
        )
        meta: dict[str, str] = await self.redis.hgetall(
            'youtube:video:meta:dQw4w9WgXcQ',
        )
        self.assertEqual(meta.get('source'), 'rss')
        self.assertEqual(meta.get('state'), 'queued')
        self.assertIn('created_at', meta)

    async def test_state_not_overwritten_on_re_enqueue(
        self,
    ) -> None:
        await self.redis.hset(
            'youtube:video:meta:dQw4w9WgXcQ',
            mapping={
                'source': 'rss', 'state': 'failed',
            },
        )
        added: bool = await self.queue.enqueue(
            'dQw4w9WgXcQ', source='rss',
        )
        self.assertFalse(added)
        state: str | None = await self.redis.hget(
            'youtube:video:meta:dQw4w9WgXcQ', 'state',
        )
        self.assertEqual(state, 'failed')
        # Additionally: re-enqueue must NOT add to
        # the queue when meta state is terminal.
        size: int = await self.redis.zcard(
            'youtube:video:queue',
        )
        self.assertEqual(size, 0)

    async def test_re_enqueue_after_terminal_skipped(
        self,
    ) -> None:
        '''Once a video is in a terminal state, a
        producer's enqueue() must not re-add it to
        the ZSET. The operator (or the scraper) is
        the only path back to QUEUED, via unmark()
        or the CLI rescrape command.'''
        await self.queue.enqueue(
            'dQw4w9WgXcQ', source='rss',
        )
        await self.queue.mark(
            'dQw4w9WgXcQ',
            state=VideoState.FAILED,
        )
        # Producer tries again.
        added: bool = await self.queue.enqueue(
            'dQw4w9WgXcQ', source='rss',
        )
        self.assertFalse(added)
        # Must NOT be in the queue.
        score: float | None = await self.redis.zscore(
            'youtube:video:queue', 'dQw4w9WgXcQ',
        )
        self.assertIsNone(score)
        # Must still be in the terminal HASH.
        record: str | None = await self.redis.hget(
            'youtube:video:failed', 'dQw4w9WgXcQ',
        )
        self.assertIsNotNone(record)
        # meta.state must remain terminal.
        state: str | None = await self.redis.hget(
            'youtube:video:meta:dQw4w9WgXcQ',
            'state',
        )
        self.assertEqual(state, 'failed')

    async def test_empty_video_id_raises(self) -> None:
        with self.assertRaises(ValueError):
            await self.queue.enqueue(
                '', source='rss',
            )


class TestPop(_RedisQueueTestBase):

    async def test_returns_oldest_first(self) -> None:
        await self.redis.zadd(
            'youtube:video:queue',
            {'aaa': 100.0, 'bbb': 200.0, 'ccc': 50.0},
        )
        popped: list[str] = await self.queue.pop(2)
        self.assertEqual(popped, ['ccc', 'aaa'])

    async def test_removes_from_queue(self) -> None:
        await self.redis.zadd(
            'youtube:video:queue', {'aaa': 100.0},
        )
        await self.queue.pop(1)
        size: int = await self.redis.zcard(
            'youtube:video:queue',
        )
        self.assertEqual(size, 0)

    async def test_empty_queue_returns_empty_list(
        self,
    ) -> None:
        popped: list[str] = await self.queue.pop(10)
        self.assertEqual(popped, [])

    async def test_respects_batch_limit(self) -> None:
        await self.redis.zadd(
            'youtube:video:queue',
            {f'v{i:010}': float(i) for i in range(5)},
        )
        popped: list[str] = await self.queue.pop(3)
        self.assertEqual(len(popped), 3)


class TestComplete(_RedisQueueTestBase):

    async def test_complete_removes_queue_and_meta(
        self,
    ) -> None:
        await self.queue.enqueue('aaa', source='rss')
        await self.queue.complete('aaa')
        in_queue: float | None = await self.redis.zscore(
            'youtube:video:queue', 'aaa',
        )
        self.assertIsNone(in_queue)
        meta_exists: int = await self.redis.exists(
            'youtube:video:meta:aaa',
        )
        self.assertEqual(meta_exists, 0)

    async def test_complete_no_terminal_hash_entry(
        self,
    ) -> None:
        await self.queue.enqueue('aaa', source='rss')
        await self.queue.complete('aaa')
        for state in (
            'unavailable', 'failed', 'removed',
        ):
            in_state: str | None = await self.redis.hget(
                f'youtube:video:{state}', 'aaa',
            )
            self.assertIsNone(in_state)

    async def test_complete_clears_terminal_entries(
        self,
    ) -> None:
        '''If a video is in a terminal HASH when
        complete() is called (e.g. race between a
        late-arriving mark and a successful
        scrape), complete must clean up so the
        operator never sees both states.'''
        await self.queue.enqueue('aaa', source='rss')
        await self.queue.mark(
            'aaa', state=VideoState.FAILED,
        )
        await self.queue.complete('aaa')
        for state in (
            'unavailable', 'failed', 'removed',
        ):
            v: str | None = await self.redis.hget(
                f'youtube:video:{state}', 'aaa',
            )
            self.assertIsNone(v)


class TestMark(_RedisQueueTestBase):

    async def test_mark_unavailable(self) -> None:
        await self.queue.enqueue('aaa', source='rss')
        await self.queue.mark(
            'aaa', state=VideoState.UNAVAILABLE,
            last_error='private',
        )
        in_queue: float | None = await self.redis.zscore(
            'youtube:video:queue', 'aaa',
        )
        self.assertIsNone(in_queue)
        record: str | None = await self.redis.hget(
            'youtube:video:unavailable', 'aaa',
        )
        self.assertIsNotNone(record)
        state: str | None = await self.redis.hget(
            'youtube:video:meta:aaa', 'state',
        )
        self.assertEqual(state, 'unavailable')

    async def test_mark_records_fields(self) -> None:
        await self.queue.enqueue('aaa', source='rss')
        await self.queue.mark(
            'aaa', state=VideoState.FAILED,
            last_error='timeout',
            note='manual',
        )
        raw: str | None = await self.redis.hget(
            'youtube:video:failed', 'aaa',
        )
        assert raw is not None
        record: dict[str, Any] = json.loads(raw)
        self.assertEqual(
            record['last_error'], 'timeout',
        )
        self.assertEqual(record['note'], 'manual')
        self.assertEqual(record['source'], 'rss')
        self.assertIn('ts', record)

    async def test_rejects_non_terminal_state(
        self,
    ) -> None:
        with self.assertRaises(ValueError):
            await self.queue.mark(
                'aaa', state=VideoState.QUEUED,
            )

    async def test_terminal_transition_clears_old(
        self,
    ) -> None:
        await self.queue.enqueue('aaa', source='rss')
        await self.queue.mark(
            'aaa', state=VideoState.FAILED,
        )
        await self.queue.mark(
            'aaa', state=VideoState.UNAVAILABLE,
        )
        failed: str | None = await self.redis.hget(
            'youtube:video:failed', 'aaa',
        )
        unavail: str | None = await self.redis.hget(
            'youtube:video:unavailable', 'aaa',
        )
        self.assertIsNone(failed)
        self.assertIsNotNone(unavail)


class TestUnmark(_RedisQueueTestBase):

    async def test_unmark_returns_to_queue(
        self,
    ) -> None:
        await self.queue.enqueue('aaa', source='rss')
        await self.queue.mark(
            'aaa', state=VideoState.FAILED,
        )
        await self.queue.unmark('aaa')
        in_state: str | None = await self.redis.hget(
            'youtube:video:failed', 'aaa',
        )
        self.assertIsNone(in_state)
        score: float | None = await self.redis.zscore(
            'youtube:video:queue', 'aaa',
        )
        self.assertIsNotNone(score)
        state: str | None = await self.redis.hget(
            'youtube:video:meta:aaa', 'state',
        )
        self.assertEqual(state, 'queued')

    async def test_unmark_clears_all_terminal(
        self,
    ) -> None:
        await self.redis.hset(
            'youtube:video:failed', 'aaa', '{}',
        )
        await self.redis.hset(
            'youtube:video:unavailable', 'aaa', '{}',
        )
        await self.queue.unmark('aaa')
        for s in (
            'failed', 'unavailable', 'removed',
        ):
            v: str | None = await self.redis.hget(
                f'youtube:video:{s}', 'aaa',
            )
            self.assertIsNone(v)


class TestMetaOps(_RedisQueueTestBase):

    async def test_bump_attempts_increments(
        self,
    ) -> None:
        await self.queue.enqueue('aaa', source='rss')
        count_1: int = await self.queue.bump_attempts(
            'aaa', last_error='timeout',
        )
        self.assertEqual(count_1, 1)
        count_2: int = await self.queue.bump_attempts(
            'aaa', last_error='timeout',
        )
        self.assertEqual(count_2, 2)
        attempts: str | None = await self.redis.hget(
            'youtube:video:meta:aaa', 'attempts',
        )
        self.assertEqual(attempts, '2')
        last_err: str | None = await self.redis.hget(
            'youtube:video:meta:aaa', 'last_error',
        )
        self.assertEqual(last_err, 'timeout')

    async def test_get_state_queued(self) -> None:
        await self.queue.enqueue('aaa', source='rss')
        s: VideoState | None = (
            await self.queue.get_state('aaa')
        )
        self.assertEqual(s, VideoState.QUEUED)

    async def test_get_state_missing(self) -> None:
        s: VideoState | None = (
            await self.queue.get_state('nope')
        )
        self.assertIsNone(s)

    async def test_get_set_meta(self) -> None:
        await self.queue.enqueue('aaa', source='rss')
        await self.queue.set_meta(
            'aaa', note='reviewed',
        )
        meta: dict[str, str] = (
            await self.queue.get_meta('aaa')
        )
        self.assertEqual(meta.get('note'), 'reviewed')


class TestCountAndSearch(_RedisQueueTestBase):

    async def test_count_by_state(self) -> None:
        await self.queue.enqueue('aaa', source='rss')
        await self.queue.enqueue('bbb', source='rss')
        await self.queue.enqueue('ccc', source='rss')
        await self.queue.mark(
            'bbb', state=VideoState.FAILED,
        )
        await self.queue.mark(
            'ccc', state=VideoState.UNAVAILABLE,
        )
        counts: dict[VideoState, int] = (
            await self.queue.count_by_state()
        )
        self.assertEqual(counts[VideoState.QUEUED], 1)
        self.assertEqual(counts[VideoState.FAILED], 1)
        self.assertEqual(
            counts[VideoState.UNAVAILABLE], 1,
        )
        self.assertEqual(counts[VideoState.REMOVED], 0)

    async def test_search_meta_by_source(
        self,
    ) -> None:
        await self.queue.enqueue('aaa', source='rss')
        await self.queue.enqueue('bbb', source='cli')
        await self.queue.enqueue(
            'ccc', source='migration',
        )
        matches: list[str] = (
            await self.queue.search_meta(
                'rss', fields=('source',),
            )
        )
        self.assertEqual(matches, ['aaa'])
