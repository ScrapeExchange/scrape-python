'''
Unit tests for refilling the Redis hot window from the MongoDB
video backlog.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import unittest
from typing import Any

from prometheus_client import REGISTRY

import fakeredis.aioredis
from mongomock_motor import AsyncMongoMockClient

from scrape_exchange.video_backlog import (
    MongoVideoBacklog,
    backlog_document,
)
from scrape_exchange.video_queue_refill import (
    ReconcileStats,
    VideoQueueRefill,
)
from scrape_exchange.video_scrape_queue import (
    RedisVideoScrapeQueue,
    VideoScrapeQueueEntry,
    VideoScrapeQueueSettings,
    qmeta_bucket,
)


class TestVideoQueueRefill(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self.coll: Any = AsyncMongoMockClient()['scraper']['youtube_videos']
        self.backlog: MongoVideoBacklog = MongoVideoBacklog(self.coll)
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(decode_responses=True)
        )
        await self.backlog.add_many([
            backlog_document(
                f'v{i:03d}', state='queued', enqueued_at=1000 + i,
                source='channel', channel_id='UCx',
            )
            for i in range(10)
        ])

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    def _refill(self, low: int, high: int, batch: int = 3) -> VideoQueueRefill:
        return VideoQueueRefill(
            self.redis, self.backlog, low_watermark=low,
            high_watermark=high, batch_size=batch,
        )

    async def _state(self, video_id: str) -> str | None:
        doc: dict[str, Any] | None = await self.coll.find_one(
            {'_id': video_id},
        )
        return None if doc is None else doc['state']

    async def test_refill_moves_oldest_up_to_high_watermark(self) -> None:
        moved: int = await self._refill(low=2, high=5).refill_once()
        self.assertEqual(moved, 5)
        self.assertEqual(
            await self.redis.zrange('youtube:video:queue', 0, -1),
            [f'v{i:03d}' for i in range(5)],
        )
        self.assertEqual(
            await self.redis.zscore('youtube:video:queue', 'v000'), 1000,
        )
        self.assertEqual(await self._state('v004'), 'hot')
        self.assertEqual(await self._state('v005'), 'queued')
        queue: RedisVideoScrapeQueue = RedisVideoScrapeQueue(
            self.redis, VideoScrapeQueueSettings(), backlog=self.backlog,
        )
        entries: list[VideoScrapeQueueEntry] = await queue.pop_entries(1)
        self.assertEqual(entries[0].video_id, 'v000')
        self.assertEqual(entries[0].channel.channel_id, 'UCx')
        self.assertEqual(entries[0].source, 'channel')

    async def test_hot_size_gauge_updated_after_every_batch(self) -> None:
        refill: VideoQueueRefill = self._refill(low=2, high=5, batch=2)
        seen: list[int] = []
        real_record = refill._record_batch

        def _capture(count: int, hot_now: int, **steps: float) -> None:
            seen.append(int(REGISTRY.get_sample_value(
                'video_queue_hot_size', {'platform': 'youtube'},
            )))
            real_record(count, hot_now, **steps)

        refill._record_batch = _capture
        await refill.refill_once()
        self.assertEqual(seen, [2, 4, 5])

    async def test_batch_steps_are_timed(self) -> None:
        def _count(step: str) -> float:
            return REGISTRY.get_sample_value(
                'video_queue_refill_step_seconds_count',
                {'platform': 'youtube', 'step': step},
            ) or 0.0

        steps: tuple[str, ...] = (
            'mongo_read', 'redis_copy', 'mongo_mark_hot',
        )
        before: dict[str, float] = {step: _count(step) for step in steps}
        with self.assertLogs(
            'scrape_exchange.video_queue_refill', level='INFO',
        ) as logs:
            await self._refill(low=2, high=5, batch=3).refill_once()
        for step in steps:
            self.assertEqual(_count(step) - before[step], 2)
        batch_logs: list = [
            r for r in logs.records if r.getMessage() == 'Refill batch copied'
        ]
        self.assertEqual(len(batch_logs), 2)
        self.assertEqual(batch_logs[-1].hot_size, 5)
        self.assertGreaterEqual(batch_logs[-1].mongo_mark_hot_seconds, 0)

    async def test_mark_hot_runs_chunks_concurrently(self) -> None:
        refill: VideoQueueRefill = VideoQueueRefill(
            self.redis, self.backlog, low_watermark=2, high_watermark=10,
            batch_size=10, mark_hot_chunk_size=3, mark_hot_concurrency=2,
        )
        real_set_state = self.backlog.set_state
        chunks: list[list[str]] = []
        in_flight: int = 0
        peak: int = 0

        async def _spy(ids: list[str], **kwargs: str) -> int:
            nonlocal in_flight, peak
            chunks.append(list(ids))
            in_flight += 1
            peak = max(peak, in_flight)
            await asyncio.sleep(0)
            try:
                return await real_set_state(ids, **kwargs)
            finally:
                in_flight -= 1

        self.backlog.set_state = _spy
        self.assertEqual(await refill.refill_once(), 10)
        self.assertEqual([len(c) for c in chunks], [3, 3, 3, 1])
        self.assertEqual(peak, 2)
        for i in range(10):
            self.assertEqual(await self._state(f'v{i:03d}'), 'hot')

    def test_rejects_bad_mark_hot_settings(self) -> None:
        with self.assertRaises(ValueError):
            VideoQueueRefill(
                self.redis, self.backlog, mark_hot_chunk_size=0,
            )
        with self.assertRaises(ValueError):
            VideoQueueRefill(
                self.redis, self.backlog, mark_hot_concurrency=0,
            )

    async def test_no_refill_at_or_above_low_watermark(self) -> None:
        refill: VideoQueueRefill = self._refill(low=2, high=5)
        await refill.refill_once()
        await self.redis.zpopmin('youtube:video:queue', 3)
        self.assertEqual(await refill.refill_once(), 0)
        await self.redis.zpopmin('youtube:video:queue', 1)
        self.assertEqual(await refill.refill_once(), 4)
        self.assertEqual(await refill.hot_size(), 5)

    async def test_refill_stops_when_backlog_empty(self) -> None:
        self.assertEqual(await self._refill(low=50, high=100).refill_once(), 10)
        self.assertEqual(await self._refill(low=50, high=100).refill_once(), 0)

    async def test_crash_between_redis_and_mongo_is_harmless(self) -> None:
        refill: VideoQueueRefill = self._refill(low=2, high=3)
        docs: list[dict[str, Any]] = await self.backlog.oldest_queued(3)
        # Redis written, but the process died before marking hot.
        await refill._copy_to_redis(docs)
        self.assertEqual(await refill.refill_once(), 0)
        await self.redis.delete('youtube:video:queue')
        self.assertEqual(await refill.refill_once(), 3)
        self.assertEqual(
            await self.redis.zrange('youtube:video:queue', 0, -1),
            ['v000', 'v001', 'v002'],
        )

    async def test_reconcile_requeues_lost_hot_documents(self) -> None:
        refill: VideoQueueRefill = self._refill(low=4, high=4, batch=10)
        await refill.refill_once()
        # A Redis restart lost two entries.
        lost: str
        for lost in ('v001', 'v003'):
            await self.redis.zrem('youtube:video:queue', lost)
            await self.redis.hdel(
                f'youtube:video:qmeta:{qmeta_bucket(lost)}', lost,
            )
        stats: ReconcileStats = await refill.reconcile(page_size=1)
        self.assertEqual(stats.checked, 4)
        self.assertEqual(stats.requeued, 2)
        self.assertEqual(await self._state('v001'), 'queued')
        self.assertEqual(await self._state('v000'), 'hot')

    def test_rejects_bad_watermarks(self) -> None:
        with self.assertRaises(ValueError):
            VideoQueueRefill(
                self.redis, self.backlog, low_watermark=5,
                high_watermark=4,
            )


if __name__ == '__main__':
    unittest.main()
