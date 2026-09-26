'''Real Redis retention checks; TEST_REDIS_DSN must name a test instance.'''

import json
import os
import time
import unittest
from uuid import uuid4

import redis.asyncio as aioredis

from scrape_exchange.video_queue_retention import (
    RetentionStats,
    VideoQueueRetention,
)
from scrape_exchange.video_scrape_queue import (
    RedisVideoScrapeQueue,
    VideoScrapeQueueSettings,
    VideoState,
)


@unittest.skipUnless(os.environ.get('TEST_REDIS_DSN'), 'needs test Redis')
class TestVideoRetentionRedis(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.redis: aioredis.Redis = aioredis.Redis.from_url(
            os.environ['TEST_REDIS_DSN'], decode_responses=True,
        )
        self.video_id: str = f'retention-test-{uuid4().hex}'
        self.meta_key: str = f'youtube:video:meta:{self.video_id}'
        self.queue: RedisVideoScrapeQueue = RedisVideoScrapeQueue(
            self.redis, VideoScrapeQueueSettings(),
        )
        self.retention: VideoQueueRetention = VideoQueueRetention(self.redis)

    async def asyncTearDown(self) -> None:
        await self.queue.complete(self.video_id)
        await self.redis.zrem('youtube:video:queue', self.video_id)
        await self.redis.aclose()

    async def test_cleanup_then_normal_and_forced_enqueue(self) -> None:
        now: int = int(time.time())
        await self.queue.mark(self.video_id, state=VideoState.FAILED)
        raw: str = json.dumps({
            'ts': now - 31 * 86400, 'last_error': 'diagnostic ' * 200,
        })
        await self.redis.hset('youtube:video:failed', self.video_id, raw)
        dry: RetentionStats = await self.retention.run(now=now)
        self.assertGreaterEqual(dry.records_compacted, 1)
        self.assertTrue(await self.redis.exists(self.meta_key))
        await self.retention.run(apply=True, now=now)
        self.assertFalse(await self.redis.exists(self.meta_key))
        self.assertEqual(
            json.loads(await self.redis.hget(
                'youtube:video:failed', self.video_id,
            )),
            {'ts': now - 31 * 86400},
        )
        self.assertFalse(await self.queue.enqueue(self.video_id, source='rss'))
        self.assertEqual(
            await self.queue.get_state(self.video_id), VideoState.FAILED,
        )
        self.assertEqual(
            await self.queue.force_enqueue(self.video_id, source='cli'),
            'revived',
        )
        self.assertEqual(
            await self.queue.get_state(self.video_id), VideoState.QUEUED,
        )
        self.assertFalse(await self.redis.hexists(
            'youtube:video:failed', self.video_id,
        ))

    async def test_expiry_keeps_terminal_membership(self) -> None:
        await self.queue.mark(self.video_id, state=VideoState.REMOVED)
        await self.redis.expire(self.meta_key, 0)
        self.assertFalse(await self.queue.enqueue(self.video_id, source='rss'))
        self.assertEqual(
            await self.queue.get_state(self.video_id), VideoState.REMOVED,
        )
