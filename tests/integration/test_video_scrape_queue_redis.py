'''Integration tests for RedisVideoScrapeQueue.

Requires a live Redis at redis://localhost:6379/1.
Uses keys with the `youtube:video:*` namespace and
clears them before/after each test so the test does
not collide with any other use of db 1.
'''

import unittest

import redis.asyncio as aioredis

from scrape_exchange.video_scrape_queue import (
    RedisVideoScrapeQueue,
    VideoScrapeQueueSettings,
    VideoState,
)


REDIS_DSN: str = 'redis://localhost:6379/1'


class TestRedisVideoScrapeQueueIntegration(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self.redis: aioredis.Redis = (
            aioredis.from_url(
                REDIS_DSN, decode_responses=True,
            )
        )
        await self._clear()
        self.queue: RedisVideoScrapeQueue = (
            RedisVideoScrapeQueue(
                self.redis,
                VideoScrapeQueueSettings(),
            )
        )

    async def asyncTearDown(self) -> None:
        await self._clear()
        await self.redis.aclose()

    async def _clear(self) -> None:
        async for key in self.redis.scan_iter(
            match='youtube:video:*',
        ):
            await self.redis.delete(key)

    async def test_enqueue_pop_complete_cycle(
        self,
    ) -> None:
        await self.queue.enqueue(
            'intgtestvid', source='cli',
        )
        popped: list[str] = await self.queue.pop(10)
        self.assertEqual(popped, ['intgtestvid'])
        await self.queue.complete('intgtestvid')
        meta: dict[str, str] = (
            await self.queue.get_meta('intgtestvid')
        )
        self.assertEqual(meta, {})

    async def test_mark_then_unmark(self) -> None:
        await self.queue.enqueue(
            'intgtestvid', source='cli',
        )
        await self.queue.mark(
            'intgtestvid', state=VideoState.FAILED,
        )
        s: VideoState | None = (
            await self.queue.get_state('intgtestvid')
        )
        self.assertEqual(s, VideoState.FAILED)
        await self.queue.unmark('intgtestvid')
        s2: VideoState | None = (
            await self.queue.get_state('intgtestvid')
        )
        self.assertEqual(s2, VideoState.QUEUED)
