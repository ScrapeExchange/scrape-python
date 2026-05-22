'''Unit tests for the Redis uploaded-video-ids SET wrapper.'''

import unittest

import fakeredis.aioredis

from scrape_exchange.youtube.uploaded_video_ids import (
    UploadedVideoIds,
)


class TestUploadedVideoIds(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(
                decode_responses=True,
            )
        )
        self.uploaded: UploadedVideoIds = UploadedVideoIds(
            'redis://unused',
        )
        self.uploaded._client = self.redis

    async def asyncTearDown(self) -> None:
        await self.redis.flushall()
        await self.redis.aclose()

    async def test_contains_missing_returns_false(self) -> None:
        self.assertFalse(
            await self.uploaded.contains('dQw4w9WgXcQ'),
        )

    async def test_add_then_contains_returns_true(self) -> None:
        await self.uploaded.add('dQw4w9WgXcQ')
        self.assertTrue(
            await self.uploaded.contains('dQw4w9WgXcQ'),
        )

    async def test_contains_many_returns_ordered_dict(self) -> None:
        await self.uploaded.add('aaa111bbb22')
        found: dict[str, bool] = await self.uploaded.contains_many(
            ['aaa111bbb22', 'ccc333ddd44'],
        )
        self.assertEqual(
            found,
            {'aaa111bbb22': True, 'ccc333ddd44': False},
        )
        self.assertEqual(
            list(found), ['aaa111bbb22', 'ccc333ddd44'],
        )

    async def test_add_is_idempotent(self) -> None:
        await self.uploaded.add('dQw4w9WgXcQ')
        await self.uploaded.add('dQw4w9WgXcQ')
        self.assertEqual(
            await self.redis.scard(UploadedVideoIds._KEY),
            1,
        )

    async def test_redis_error_propagates(self) -> None:
        self.uploaded._client = _BrokenRedis()
        with self.assertRaises(ConnectionError):
            await self.uploaded.contains('dQw4w9WgXcQ')


class _BrokenRedis:

    async def sismember(self, key: str, member: str) -> bool:
        raise ConnectionError('redis unavailable')


if __name__ == '__main__':
    unittest.main()
