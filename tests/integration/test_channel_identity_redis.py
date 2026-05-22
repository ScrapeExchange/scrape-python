'''Integration test for RedisHandleMap against a
fakeredis backend.

Exercises the Redis-hash CRUD surface (get, get_many,
get_all, put) without requiring a live Redis instance.
'''

import unittest

import fakeredis.aioredis

from scrape_exchange.handle_map import RedisHandleMap


class TestRedisHandleMap(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self.client: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(
                decode_responses=True,
            )
        )
        self.map: RedisHandleMap = RedisHandleMap(self.client)

    async def asyncTearDown(self) -> None:
        await self.client.flushall()
        await self.client.aclose()

    async def test_put_then_get(self) -> None:
        await self.map.put(
            'LinusTechTips', 'UCXuqSBlHAE6Xw-yeJA0Tunw',
        )
        self.assertEqual(
            await self.map.get('LinusTechTips'),
            'UCXuqSBlHAE6Xw-yeJA0Tunw',
        )

    async def test_get_unknown_returns_none(self) -> None:
        self.assertIsNone(await self.map.get('Unknown'))

    async def test_get_many_empty_list_returns_empty(
        self,
    ) -> None:
        self.assertEqual(await self.map.get_many([]), {})

    async def test_get_many_partial_hits(self) -> None:
        await self.map.put('Foo', 'UC1')
        await self.map.put('Bar', 'UC2')
        result: dict[str, str | None] = (
            await self.map.get_many(['Foo', 'Bar', 'Missing'])
        )
        self.assertEqual(result['Foo'], 'UC1')
        self.assertEqual(result['Bar'], 'UC2')
        # 'Missing' is included as None per dict[str, str|None]
        self.assertIn('Missing', result)
        self.assertIsNone(result['Missing'])

    async def test_get_all(self) -> None:
        await self.map.put('Foo', 'UC1')
        await self.map.put('Bar', 'UC2')
        self.assertEqual(
            await self.map.get_all(),
            {'Foo': 'UC1', 'Bar': 'UC2'},
        )
