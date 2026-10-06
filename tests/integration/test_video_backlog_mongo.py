'''Real MongoDB + Redis checks for the video backlog.

TEST_MONGO_DSN and TEST_REDIS_DSN must name *test* instances: the test
uses a throwaway collection and a throwaway platform namespace and
drops both afterwards.
'''

import os
import unittest
from typing import Any
from uuid import uuid4

import redis.asyncio as aioredis
from pymongo import AsyncMongoClient

from scrape_exchange.video_backlog import MongoVideoBacklog, backlog_document
from scrape_exchange.video_queue_refill import VideoQueueRefill
from scrape_exchange.video_scrape_queue import (
    RedisVideoScrapeQueue,
    VideoScrapeQueueSettings,
    VideoState,
)


@unittest.skipUnless(
    os.environ.get('TEST_MONGO_DSN') and os.environ.get('TEST_REDIS_DSN'),
    'needs test MongoDB and Redis',
)
class TestVideoBacklogMongo(unittest.IsolatedAsyncioTestCase):

    VIDEO_IDS: tuple[str, ...] = ('aaa', 'bbb')

    async def asyncSetUp(self) -> None:
        self.platform: str = f'itest{uuid4().hex[:8]}'
        self.client: AsyncMongoClient = AsyncMongoClient(
            os.environ['TEST_MONGO_DSN'],
        )
        self.coll: Any = self.client.get_default_database(
            default='scraper_test',
        )[f'{self.platform}_videos']
        self.backlog: MongoVideoBacklog = MongoVideoBacklog(self.coll)
        self.redis: aioredis.Redis = aioredis.Redis.from_url(
            os.environ['TEST_REDIS_DSN'], decode_responses=True,
        )
        self.queue: RedisVideoScrapeQueue = RedisVideoScrapeQueue(
            self.redis, VideoScrapeQueueSettings(),
            platform=self.platform, backlog=self.backlog,
        )

    async def asyncTearDown(self) -> None:
        await self.coll.drop()
        # Delete the test's own keys; never SCAN a shared Redis.
        keys: list[str] = [
            self.queue._k_queue(), *self.queue._k_terminal(),
        ]
        video_id: str
        for video_id in self.VIDEO_IDS:
            keys.append(self.queue._k_qmeta(video_id))
            keys.append(self.queue._k_meta(video_id))
        await self.redis.delete(*keys)
        await self.redis.aclose()
        await self.client.close()

    async def test_produce_refill_scrape_cycle(self) -> None:
        self.assertTrue(await self.queue.enqueue(
            'aaa', source='channel', channel_id='UCx',
        ))
        self.assertFalse(await self.queue.enqueue('aaa', source='rss'))
        self.assertEqual(
            await self.backlog.add_many([
                backlog_document('aaa', state='queued', enqueued_at=1),
                backlog_document('bbb', state='queued', enqueued_at=2),
            ]),
            1,
        )
        refill: VideoQueueRefill = VideoQueueRefill(
            self.redis, self.backlog, platform=self.platform,
            low_watermark=5, high_watermark=10,
        )
        self.assertEqual(await refill.refill_once(), 2)
        self.assertEqual((await refill.reconcile()).requeued, 0)
        entries: list = await self.queue.pop_entries(10)
        self.assertEqual(len(entries), 2)
        await self.queue.complete(entries[0].video_id)
        await self.queue.mark(
            entries[1].video_id, state=VideoState.FAILED, last_error='x',
        )
        self.assertIsNone(await self.backlog.get(entries[0].video_id))
        self.assertEqual(
            await self.queue.get_state(entries[1].video_id),
            VideoState.FAILED,
        )
        self.assertEqual(
            await self.queue.force_enqueue(
                entries[1].video_id, source='cli',
            ),
            'revived',
        )
        counts: dict[VideoState, int] = await self.queue.count_by_state()
        self.assertEqual(counts[VideoState.QUEUED], 1)


if __name__ == '__main__':
    unittest.main()
