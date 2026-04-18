'''Unit tests for scrape_exchange.creator_map.'''

import os
import tempfile
import unittest

import fakeredis.aioredis

from scrape_exchange.creator_map import (
    FileCreatorMap,
    NullCreatorMap,
    RedisCreatorMap,
)


class TestFileCreatorMap(unittest.IsolatedAsyncioTestCase):
    '''Tests for the CSV-backed creator map.'''

    def setUp(self) -> None:
        self.tmpdir: str = tempfile.mkdtemp()
        self.csv_path: str = os.path.join(
            self.tmpdir, 'creator_map.csv',
        )

    async def test_get_all_empty_when_file_missing(
        self,
    ) -> None:
        cm: FileCreatorMap = FileCreatorMap(
            self.csv_path,
        )
        result: dict[str, str] = await cm.get_all()
        self.assertEqual(result, {})

    async def test_put_and_get(self) -> None:
        cm: FileCreatorMap = FileCreatorMap(
            self.csv_path,
        )
        await cm.put('UC123', 'TestChannel')
        result: str | None = await cm.get('UC123')
        self.assertEqual(result, 'TestChannel')

    async def test_put_many_and_get_all(self) -> None:
        cm: FileCreatorMap = FileCreatorMap(
            self.csv_path,
        )
        mapping: dict[str, str] = {
            'UC111': 'Alpha',
            'UC222': 'Beta',
        }
        await cm.put_many(mapping)
        result: dict[str, str] = await cm.get_all()
        self.assertEqual(result, mapping)

    async def test_contains(self) -> None:
        cm: FileCreatorMap = FileCreatorMap(
            self.csv_path,
        )
        await cm.put('UC123', 'TestChannel')
        self.assertTrue(await cm.contains('UC123'))
        self.assertFalse(await cm.contains('UC999'))

    async def test_size(self) -> None:
        cm: FileCreatorMap = FileCreatorMap(
            self.csv_path,
        )
        self.assertEqual(await cm.size(), 0)
        await cm.put('UC123', 'TestChannel')
        self.assertEqual(await cm.size(), 1)

    async def test_skips_comments_and_blanks(
        self,
    ) -> None:
        with open(self.csv_path, 'w') as f:
            f.write('# comment\n')
            f.write('\n')
            f.write('UC123,TestChannel\n')
        cm: FileCreatorMap = FileCreatorMap(
            self.csv_path,
        )
        result: dict[str, str] = await cm.get_all()
        self.assertEqual(
            result, {'UC123': 'TestChannel'},
        )

    async def test_duplicate_ids_last_wins(
        self,
    ) -> None:
        with open(self.csv_path, 'w') as f:
            f.write('UC123,First\n')
            f.write('UC123,Second\n')
        cm: FileCreatorMap = FileCreatorMap(
            self.csv_path,
        )
        result: str | None = await cm.get('UC123')
        self.assertEqual(result, 'Second')

    async def test_put_appends_to_file(self) -> None:
        with open(self.csv_path, 'w') as f:
            f.write('UC111,Existing\n')
        cm: FileCreatorMap = FileCreatorMap(
            self.csv_path,
        )
        await cm.put('UC222', 'New')
        with open(self.csv_path, 'r') as f:
            lines: list[str] = (
                f.read().strip().split('\n')
            )
        self.assertEqual(len(lines), 2)
        self.assertEqual(lines[0], 'UC111,Existing')
        self.assertEqual(lines[1], 'UC222,New')


class TestRedisCreatorMap(
    unittest.IsolatedAsyncioTestCase,
):
    '''Tests for the Redis-backed creator map.'''

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

    async def test_get_all_empty(self) -> None:
        result: dict[str, str] = (
            await self.cm.get_all()
        )
        self.assertEqual(result, {})

    async def test_put_and_get(self) -> None:
        await self.cm.put('UC123', 'TestChannel')
        result: str | None = await self.cm.get('UC123')
        self.assertEqual(result, 'TestChannel')

    async def test_put_many_and_get_all(self) -> None:
        mapping: dict[str, str] = {
            'UC111': 'Alpha',
            'UC222': 'Beta',
        }
        await self.cm.put_many(mapping)
        result: dict[str, str] = (
            await self.cm.get_all()
        )
        self.assertEqual(result, mapping)

    async def test_contains(self) -> None:
        await self.cm.put('UC123', 'TestChannel')
        self.assertTrue(
            await self.cm.contains('UC123'),
        )
        self.assertFalse(
            await self.cm.contains('UC999'),
        )

    async def test_size(self) -> None:
        self.assertEqual(await self.cm.size(), 0)
        await self.cm.put('UC123', 'TestChannel')
        self.assertEqual(await self.cm.size(), 1)

    async def test_put_many_empty(self) -> None:
        await self.cm.put_many({})
        self.assertEqual(await self.cm.size(), 0)

    async def test_duplicate_last_wins(self) -> None:
        await self.cm.put('UC123', 'First')
        await self.cm.put('UC123', 'Second')
        result: str | None = await self.cm.get('UC123')
        self.assertEqual(result, 'Second')


class TestNullCreatorMap(
    unittest.IsolatedAsyncioTestCase,
):
    '''Tests for the no-op creator map.'''

    async def test_get_returns_none(self) -> None:
        cm: NullCreatorMap = NullCreatorMap()
        self.assertIsNone(await cm.get('UC123'))

    async def test_get_all_returns_empty(self) -> None:
        cm: NullCreatorMap = NullCreatorMap()
        self.assertEqual(await cm.get_all(), {})

    async def test_put_is_noop(self) -> None:
        cm: NullCreatorMap = NullCreatorMap()
        await cm.put('UC123', 'Test')
        self.assertIsNone(await cm.get('UC123'))

    async def test_size_always_zero(self) -> None:
        cm: NullCreatorMap = NullCreatorMap()
        self.assertEqual(await cm.size(), 0)


class TestExportImportRoundTrip(
    unittest.IsolatedAsyncioTestCase,
):
    '''Round-trip test: CSV -> Redis -> CSV.'''

    async def asyncSetUp(self) -> None:
        self.tmpdir: str = tempfile.mkdtemp()
        self.csv_in: str = os.path.join(
            self.tmpdir, 'input.csv',
        )
        self.csv_out: str = os.path.join(
            self.tmpdir, 'output.csv',
        )
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(
                decode_responses=True,
            )
        )
        self.redis_cm: RedisCreatorMap = (
            RedisCreatorMap.__new__(RedisCreatorMap)
        )
        self.redis_cm._redis = self.redis
        self.redis_cm._key = 'youtube:creator_map'

    async def asyncTearDown(self) -> None:
        await self.redis.flushall()
        await self.redis.aclose()

    async def test_round_trip(self) -> None:
        with open(self.csv_in, 'w') as f:
            f.write('UC111,Alpha\n')
            f.write('UC222,Beta\n')
            f.write('UC333,Gamma\n')

        file_cm: FileCreatorMap = FileCreatorMap(
            self.csv_in,
        )
        data: dict[str, str] = await file_cm.get_all()
        await self.redis_cm.put_many(data)

        exported: dict[str, str] = (
            await self.redis_cm.get_all()
        )
        with open(self.csv_out, 'w') as f:
            for creator_id, creator_handle in sorted(
                exported.items(),
                key=lambda x: x[1].lower(),
            ):
                f.write(
                    f'{creator_id},{creator_handle}\n'
                )

        out_cm: FileCreatorMap = FileCreatorMap(
            self.csv_out,
        )
        result: dict[str, str] = await out_cm.get_all()
        self.assertEqual(result, data)
