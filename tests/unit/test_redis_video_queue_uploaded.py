'''Unit tests for the Redis queue's upload-state methods.'''

import unittest
from typing import Any

import fakeredis.aioredis

from scrape_exchange.video_scrape_queue import (
    RedisVideoScrapeQueue,
    VideoScrapeQueueSettings,
)

# The fleet-wide Bloom filter key. The name is fixed: it is what
# production already holds and what the rollout deletes.
BF_KEY: str = 'youtube:video:uploaded_bf'


class TestRedisUploadState(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self.redis: Any = fakeredis.aioredis.FakeRedis(
            decode_responses=True,
        )

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    def queue(
        self, platform: str, supported: bool,
    ) -> RedisVideoScrapeQueue:
        return RedisVideoScrapeQueue(
            self.redis, VideoScrapeQueueSettings(), platform,
            supports_uploaded=supported,
        )

    async def test_youtube_mark_then_read(self) -> None:
        queue: RedisVideoScrapeQueue = self.queue('youtube', True)
        ids: list[str] = ['a' * 11, 'b' * 11]
        await queue.mark_uploaded([ids[0]])
        self.assertEqual(
            await queue.uploaded_many(ids),
            {ids[0]: True, ids[1]: False},
        )
        self.assertEqual(
            await queue.done_many(ids),
            {ids[0]: True, ids[1]: False},
        )

    async def test_tiktok_writes_nothing(self) -> None:
        queue: RedisVideoScrapeQueue = self.queue('tiktok', False)
        await queue.mark_uploaded(['a' * 11])
        self.assertEqual(await self.redis.exists(BF_KEY), 0)
        self.assertEqual(
            await queue.uploaded_many(['a' * 11]), {'a' * 11: False},
        )
        self.assertEqual(
            await queue.done_many(['a' * 11]), {'a' * 11: False},
        )

    async def test_state_names_are_the_four_states(self) -> None:
        queue: RedisVideoScrapeQueue = self.queue('youtube', True)
        self.assertEqual(
            queue.state_names(),
            ['queued', 'unavailable', 'failed', 'removed'],
        )


if __name__ == '__main__':
    unittest.main()
