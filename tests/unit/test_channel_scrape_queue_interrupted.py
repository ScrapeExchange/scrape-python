'''
A channel whose scrape was interrupted (the worker was cancelled on
shutdown) goes back into its scheduled tier, due now, unless its
state changed or another scrape rescheduled it meanwhile.
'''

import time
import unittest

import fakeredis.aioredis

from scrape_exchange.channel_scrape_queue import (
    ChannelScrapeQueueSettings,
    ChannelState,
    RedisChannelScrapeQueue,
)


CHANNEL_ID: str = 'UCa'
MEMBER: str = f'i:{CHANNEL_ID}'


class TestRequeueInterrupted(unittest.IsolatedAsyncioTestCase):

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

    async def _scores(self) -> list[float | None]:
        return [
            await self.redis.zscore(
                f'youtube:channel:queue:scheduled:{tier}', MEMBER,
            )
            for tier in range(len(self.queue._tiers))
        ]

    async def test_requeues_in_stored_tier_due_now(self) -> None:
        self.assertTrue(
            await self.queue.requeue_interrupted(CHANNEL_ID, now=1000.0),
        )
        scores: list[float | None] = await self._scores()
        self.assertEqual(scores[2], 1000.0)
        self.assertEqual(
            [s for i, s in enumerate(scores) if i != 2],
            [None] * (len(scores) - 1),
        )

    async def test_state_change_is_kept(self) -> None:
        await self.queue.mark(MEMBER, state=ChannelState.REMOVED)
        self.assertFalse(
            await self.queue.requeue_interrupted(CHANNEL_ID, now=1000.0),
        )
        self.assertEqual(
            await self.queue.get_state(MEMBER), ChannelState.REMOVED,
        )
        self.assertEqual(
            await self._scores(), [None] * len(self.queue._tiers),
        )

    async def test_already_rescheduled_is_left_alone(self) -> None:
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:4', {MEMBER: 5000.0},
        )
        self.assertFalse(
            await self.queue.requeue_interrupted(CHANNEL_ID, now=1000.0),
        )
        scores: list[float | None] = await self._scores()
        self.assertEqual(scores[4], 5000.0)
        self.assertIsNone(scores[2])


if __name__ == '__main__':
    unittest.main()
