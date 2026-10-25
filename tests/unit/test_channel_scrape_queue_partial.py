'''
A partial channel scrape is retried after
``channel_partial_retry_seconds`` without counting toward the
unavailable threshold; after ``channel_partial_max_retries``
consecutive partial scrapes the caller counts it as a success.
'''

import time
import unittest

import fakeredis.aioredis

from scrape_exchange.channel_scrape_queue import (
    ChannelScrapeProgress,
    ChannelScrapeQueueSettings,
    ChannelState,
    RedisChannelScrapeQueue,
)


CHANNEL_ID: str = 'UCa'
MEMBER: str = f'i:{CHANNEL_ID}'
META: str = f'youtube:channel:meta:{MEMBER}'


class TestMarkPartial(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(decode_responses=True)
        )
        self.queue: RedisChannelScrapeQueue = RedisChannelScrapeQueue(
            self.redis, ChannelScrapeQueueSettings(),
        )
        await self.queue.enqueue_scheduled(CHANNEL_ID, source='test')
        await self.redis.hset('youtube:channel:tiers', CHANNEL_ID, '2')
        await self.queue.pop_scheduled(10, now=time.time())

    async def asyncTearDown(self) -> None:
        await self.redis.flushall()
        await self.redis.aclose()

    def test_defaults(self) -> None:
        settings: ChannelScrapeQueueSettings = ChannelScrapeQueueSettings()
        self.assertEqual(settings.channel_partial_retry_seconds, 6 * 3600)
        self.assertEqual(settings.channel_partial_max_retries, 3)

    async def test_reschedules_in_stored_tier(self) -> None:
        retried: bool = await self.queue.mark_partial(
            CHANNEL_ID, last_error='boom', now=1000.0,
        )
        self.assertTrue(retried)
        self.assertEqual(
            await self.redis.zscore(
                'youtube:channel:queue:scheduled:2', MEMBER,
            ),
            1000.0 + 6 * 3600,
        )
        self.assertEqual(
            await self.queue.get_state(MEMBER), ChannelState.SCHEDULED,
        )
        meta: dict[str, str] = await self.queue.get_meta(MEMBER)
        self.assertEqual(meta['partial_attempts'], '1')
        self.assertEqual(meta['last_partial_error'], 'boom')

    async def test_unavailable_attempts_untouched(self) -> None:
        await self.redis.hset(META, 'unavailable_attempts', '2')
        await self.queue.mark_partial(
            CHANNEL_ID, last_error='boom', now=1000.0,
        )
        self.assertEqual(
            await self.redis.hget(META, 'unavailable_attempts'), '2',
        )

    async def test_gives_up_after_max_retries(self) -> None:
        results: list[bool] = [
            await self.queue.mark_partial(
                CHANNEL_ID, last_error='boom', now=1000.0 + index,
            )
            for index in range(4)
        ]
        self.assertEqual(results, [True, True, True, False])

    async def test_terminal_state_set_during_scrape_is_kept(
        self,
    ) -> None:
        await self.queue.mark(MEMBER, state=ChannelState.REMOVED)
        retried: bool | None = await self.queue.mark_partial(
            CHANNEL_ID, last_error='boom', now=1000.0,
        )
        self.assertIsNone(retried)
        self.assertEqual(
            await self.queue.get_state(MEMBER), ChannelState.REMOVED,
        )
        tier: int
        for tier in range(len(self.queue._tiers)):
            self.assertIsNone(await self.redis.zscore(
                f'youtube:channel:queue:scheduled:{tier}', MEMBER,
            ))
        self.assertIsNone(await self.redis.hget(META, 'partial_attempts'))

    async def test_superseded_scrape_is_ignored(self) -> None:
        stored: ChannelScrapeProgress = (
            await self.queue.get_scrape_progress(CHANNEL_ID, existing=True)
        )
        stale: ChannelScrapeProgress = ChannelScrapeProgress(
            stored.successful_scrapes - 1, stored.next_full_scrape,
        )
        self.assertIsNone(await self.queue.mark_partial(
            CHANNEL_ID, last_error='boom', now=1000.0, progress=stale,
        ))
        self.assertTrue(await self.queue.mark_partial(
            CHANNEL_ID, last_error='boom', now=1000.0, progress=stored,
        ))

    async def test_success_clears_partial_attempts(self) -> None:
        await self.queue.mark_partial(
            CHANNEL_ID, last_error='boom', now=1000.0,
        )
        stored: ChannelScrapeProgress = (
            await self.queue.get_scrape_progress(CHANNEL_ID, existing=True)
        )
        for progress in (None, stored):
            with self.subTest(progress=progress):
                await self.redis.hset(META, 'partial_attempts', '2')
                await self.queue.update_tier(
                    CHANNEL_ID, sub_count=1_000_000, now=2000.0,
                    progress=progress,
                )
                self.assertIsNone(
                    await self.redis.hget(META, 'partial_attempts'),
                )


if __name__ == '__main__':
    unittest.main()
