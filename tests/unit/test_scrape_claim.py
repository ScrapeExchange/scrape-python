'''Unit tests for the cross-host scrape claim
(``youtube:scraping:<handle>``) used by tools/yt_channel_scrape.py
to deduplicate channel scrapes across hosts.'''

import unittest
from unittest.mock import MagicMock

import fakeredis.aioredis

from scrape_exchange.creator_map import RedisCreatorMap
from tools.yt_channel_scrape import (
    METRIC_CHANNEL_SCRAPE_CLAIM,
    _try_acquire_scrape_claim,
)


class TestTryAcquireScrapeClaim(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(
                decode_responses=True,
            )
        )
        self.cm: RedisCreatorMap = (
            RedisCreatorMap.__new__(RedisCreatorMap)
        )
        self.cm._redis = self.redis
        self.cm._key = 'youtube:creator_map'

    async def asyncTearDown(self) -> None:
        await self.redis.flushall()
        await self.redis.aclose()

    async def test_first_caller_wins_and_writes_key(
        self,
    ) -> None:
        claim, status = await _try_acquire_scrape_claim(
            'examplehandle', self.cm,
        )
        self.assertEqual(status, 'won')
        self.assertIsNotNone(claim)
        # Live key with TTL ~300 s.
        ttl: int = await self.redis.ttl(
            'youtube:scraping:examplehandle',
        )
        self.assertGreater(ttl, 0)
        self.assertLessEqual(ttl, 300)

    async def test_second_caller_loses(self) -> None:
        first_claim, first_status = (
            await _try_acquire_scrape_claim(
                'examplehandle', self.cm,
            )
        )
        self.assertEqual(first_status, 'won')

        second_claim, second_status = (
            await _try_acquire_scrape_claim(
                'examplehandle', self.cm,
            )
        )
        self.assertEqual(second_status, 'lost')
        self.assertIsNone(second_claim)

        await first_claim.release('examplehandle')

    async def test_release_lets_peer_claim(self) -> None:
        claim, status = await _try_acquire_scrape_claim(
            'examplehandle', self.cm,
        )
        self.assertEqual(status, 'won')
        await claim.release('examplehandle')

        retry_claim, retry_status = (
            await _try_acquire_scrape_claim(
                'examplehandle', self.cm,
            )
        )
        self.assertEqual(retry_status, 'won')
        await retry_claim.release('examplehandle')

    async def test_no_redis_backend_returns_no_redis(
        self,
    ) -> None:
        '''FileCreatorMap-style backend (no Redis) → no claim;
        caller should proceed without cross-host coordination.'''
        backend: MagicMock = MagicMock()
        backend.redis_client = None

        claim, status = await _try_acquire_scrape_claim(
            'examplehandle', backend,
        )
        self.assertEqual(status, 'no_redis')
        self.assertIsNone(claim)

    async def test_won_metric_increments(self) -> None:
        before: float = (
            METRIC_CHANNEL_SCRAPE_CLAIM.labels(
                platform='youtube',
                scraper='channel_scraper',
                entity='channel',
                outcome='won',
                worker_id='0',
            )._value.get()
        )
        claim, status = await _try_acquire_scrape_claim(
            'metricwonhandle', self.cm,
        )
        self.assertEqual(status, 'won')
        after: float = (
            METRIC_CHANNEL_SCRAPE_CLAIM.labels(
                platform='youtube',
                scraper='channel_scraper',
                entity='channel',
                outcome='won',
                worker_id='0',
            )._value.get()
        )
        self.assertEqual(after - before, 1)
        await claim.release('metricwonhandle')

    async def test_lost_metric_increments(self) -> None:
        first_claim, _ = await _try_acquire_scrape_claim(
            'metriclosthandle', self.cm,
        )
        before: float = (
            METRIC_CHANNEL_SCRAPE_CLAIM.labels(
                platform='youtube',
                scraper='channel_scraper',
                entity='channel',
                outcome='lost',
                worker_id='0',
            )._value.get()
        )
        _, status = await _try_acquire_scrape_claim(
            'metriclosthandle', self.cm,
        )
        self.assertEqual(status, 'lost')
        after: float = (
            METRIC_CHANNEL_SCRAPE_CLAIM.labels(
                platform='youtube',
                scraper='channel_scraper',
                entity='channel',
                outcome='lost',
                worker_id='0',
            )._value.get()
        )
        self.assertEqual(after - before, 1)
        await first_claim.release('metriclosthandle')


if __name__ == '__main__':
    unittest.main()
