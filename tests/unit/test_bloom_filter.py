'''Unit tests for bloom_add_many.'''

import unittest

import fakeredis.aioredis
from redis.exceptions import ResponseError

from scrape_exchange.bloom_filter import bloom_add_many

KEY: str = 'test:bf'


class TestBloomAddMany(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(decode_responses=True)
        )

    async def asyncTearDown(self) -> None:
        await self.redis.flushall()
        await self.redis.aclose()

    async def _add(self, items: list[str]) -> None:
        await bloom_add_many(
            self.redis, KEY, items,
            error_rate=0.00001, capacity=5000, expansion=2,
        )

    async def test_first_add_creates_filter_with_sizing(self) -> None:
        await self._add(['a', 'b'])
        # redis-py parses BF.INFO <field> into {'Capacity': n}.
        info: dict[str, int] = await self.redis.execute_command(
            'BF.INFO', KEY, 'CAPACITY',
        )
        self.assertEqual(info, {'Capacity': 5000})
        self.assertEqual(
            await self.redis.execute_command('BF.MEXISTS', KEY, 'a', 'z'),
            [1, 0],
        )

    async def test_second_add_ignores_existing_filter(self) -> None:
        await self._add(['a'])
        await self._add(['b'])
        self.assertEqual(
            await self.redis.execute_command('BF.MEXISTS', KEY, 'a', 'b'),
            [1, 1],
        )

    async def test_empty_items_writes_nothing(self) -> None:
        await self._add([])
        self.assertFalse(await self.redis.exists(KEY))

    async def test_other_errors_are_raised(self) -> None:
        await self.redis.set(KEY, 'not a bloom filter')
        with self.assertRaises(ResponseError):
            await self._add(['a'])


if __name__ == '__main__':
    unittest.main()
