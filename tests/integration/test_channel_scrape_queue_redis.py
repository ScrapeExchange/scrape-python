'''Integration tests for RedisChannelScrapeQueue.

Requires a live Redis at redis://localhost:6379/1.
Uses keys with the `youtube:channel:*` namespace but
clears them before/after each test so the test does
not collide with any other use of db 1.
'''

import time
import unittest

import redis.asyncio as aioredis

from scrape_exchange.channel_scrape_queue import (
    ChannelScrapeQueueSettings,
    ChannelState,
    RedisChannelScrapeQueue,
)


REDIS_DSN: str = 'redis://localhost:6379/1'


class TestRedisChannelScrapeQueueIntegration(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self.redis: aioredis.Redis = (
            aioredis.from_url(
                REDIS_DSN, decode_responses=True,
            )
        )
        await self._clear()
        settings: ChannelScrapeQueueSettings = (
            ChannelScrapeQueueSettings()
        )
        self.queue: RedisChannelScrapeQueue = (
            RedisChannelScrapeQueue(
                self.redis, settings,
            )
        )

    async def asyncTearDown(self) -> None:
        await self._clear()
        await self.redis.aclose()

    async def _clear(self) -> None:
        async for key in self.redis.scan_iter(
            match='youtube:channel:*',
        ):
            await self.redis.delete(key)

    async def test_resolve_to_scrape_to_tier_cycle(
        self,
    ) -> None:
        # Operator adds a handle they don't know
        # the id for.
        await self.queue.enqueue_unresolved(
            'IntegTestChannel', source='cli',
        )
        popped: list[str] = (
            await self.queue.pop_unresolved(10)
        )
        self.assertEqual(
            popped, ['IntegTestChannel'],
        )
        # Resolver finds the id.
        await self.queue.promote_to_scheduled(
            'IntegTestChannel',
            'UCintegtest0000000000000',
        )
        # Scrape phase pops it.
        scheduled: list[str] = (
            await self.queue.pop_scheduled(
                10, now=time.time(),
            )
        )
        self.assertEqual(
            scheduled, ['UCintegtest0000000000000'],
        )
        # Successful scrape with 50k subs lands in
        # tier 2.
        await self.queue.update_tier(
            'UCintegtest0000000000000',
            sub_count=50_000,
            now=time.time(),
        )
        s: ChannelState | None = (
            await self.queue.get_state(
                'i:UCintegtest0000000000000',
            )
        )
        self.assertEqual(s, ChannelState.SCHEDULED)
        counts: dict[int, int] = (
            await self.queue.count_by_tier()
        )
        self.assertEqual(counts[2], 1)

    async def test_mark_then_unmark(self) -> None:
        await self.queue.enqueue_scheduled(
            'UCintegtest1111111111111', source='cli',
        )
        await self.queue.mark(
            'i:UCintegtest1111111111111',
            state=ChannelState.NOT_FOUND,
        )
        state_before: ChannelState | None = (
            await self.queue.get_state(
                'i:UCintegtest1111111111111',
            )
        )
        self.assertEqual(
            state_before, ChannelState.NOT_FOUND,
        )
        await self.queue.unmark(
            'i:UCintegtest1111111111111',
        )
        state_after: ChannelState | None = (
            await self.queue.get_state(
                'i:UCintegtest1111111111111',
            )
        )
        self.assertEqual(
            state_after, ChannelState.SCHEDULED,
        )
