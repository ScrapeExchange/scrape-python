'''Unit tests for RedisExchangeChannelsSet.'''

import unittest

import fakeredis.aioredis

from scrape_exchange.youtube.exchange_channels_set import (
    RedisExchangeChannelsSet,
)


class TestRedisExchangeChannelsSet(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(
                decode_responses=True,
            )
        )
        self.s: RedisExchangeChannelsSet = (
            RedisExchangeChannelsSet(self.redis)
        )

    async def asyncTearDown(self) -> None:
        await self.redis.flushall()
        await self.redis.aclose()

    async def test_add_many_then_contains_many(
        self,
    ) -> None:
        await self.s.add_many({'alpha', 'bravo'})
        result: dict[str, bool] = (
            await self.s.contains_many(
                ['alpha', 'bravo', 'charlie'],
            )
        )
        self.assertEqual(
            result,
            {
                'alpha': True,
                'bravo': True,
                'charlie': False,
            },
        )

    async def test_add_many_empty_is_noop(
        self,
    ) -> None:
        await self.s.add_many(set())
        self.assertEqual(await self.s.size(), 0)

    async def test_contains_many_empty_returns_empty(
        self,
    ) -> None:
        result: dict[str, bool] = (
            await self.s.contains_many([])
        )
        self.assertEqual(result, {})

    async def test_contains_many_before_add_is_all_false(
        self,
    ) -> None:
        '''Cold-start scenario: the SET key has never been
        written, so SISMEMBER returns False for every
        handle.'''
        result: dict[str, bool] = (
            await self.s.contains_many(
                ['alpha', 'bravo'],
            )
        )
        self.assertEqual(
            result, {'alpha': False, 'bravo': False},
        )

    async def test_add_many_is_idempotent(
        self,
    ) -> None:
        '''Adding the same handle twice does not change the
        SET cardinality.'''
        await self.s.add_many(['alpha', 'bravo'])
        await self.s.add_many(['alpha'])
        self.assertEqual(await self.s.size(), 2)

    async def test_contains_many_with_duplicates(
        self,
    ) -> None:
        '''Duplicate handles in the input list collapse to a
        single key in the returned dict (documented
        behaviour).'''
        await self.s.add_many(['alpha'])
        result: dict[str, bool] = (
            await self.s.contains_many(['alpha', 'alpha'])
        )
        self.assertEqual(result, {'alpha': True})
