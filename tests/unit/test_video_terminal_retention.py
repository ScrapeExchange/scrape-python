'''Terminal membership must survive expiry of diagnostic metadata.'''

import unittest

import fakeredis.aioredis

from scrape_exchange.video_scrape_queue import (
    RedisVideoScrapeQueue,
    VideoScrapeQueueSettings,
    VideoState,
)


class TestExpiredVideoMetadata(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(decode_responses=True)
        )
        self.queue: RedisVideoScrapeQueue = RedisVideoScrapeQueue(
            self.redis, VideoScrapeQueueSettings(),
        )

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    async def expire_metadata(self, state: VideoState) -> None:
        await self.queue.mark('old-video', state=state)
        await self.redis.delete('youtube:video:meta:old-video')

    async def test_enqueue_respects_expired_terminal_metadata(self) -> None:
        state: VideoState
        for state in VideoState.terminal_states():
            with self.subTest(state=state):
                await self.redis.flushall()
                await self.expire_metadata(state)
                self.assertFalse(
                    await self.queue.enqueue('old-video', source='rss'),
                )
                self.assertEqual(await self.queue.pop(1), [])

    async def test_state_reads_resolve_tombstones(self) -> None:
        await self.expire_metadata(VideoState.REMOVED)
        self.assertEqual(
            await self.queue.get_state('old-video'), VideoState.REMOVED,
        )
        self.assertEqual(
            await self.queue.get_states(['old-video', 'unknown']),
            {'old-video': VideoState.REMOVED, 'unknown': None},
        )
        self.assertEqual(
            await self.queue.get_meta('old-video'), {'state': 'removed'},
        )

    async def test_force_revives_and_removes_expired_tombstone(self) -> None:
        await self.expire_metadata(VideoState.FAILED)
        self.assertEqual(
            await self.queue.force_enqueue('old-video', source='cli'),
            'revived',
        )
        self.assertFalse(
            await self.redis.hexists('youtube:video:failed', 'old-video'),
        )
        self.assertEqual(await self.queue.pop(1), ['old-video'])
        self.assertEqual(
            await self.queue.get_state('old-video'), VideoState.QUEUED,
        )
        meta: dict[str, str] = await self.queue.get_meta('old-video')
        self.assertEqual(meta.get('source'), 'cli')
        self.assertIsNotNone(meta.get('created_at'))


if __name__ == '__main__':
    unittest.main()
