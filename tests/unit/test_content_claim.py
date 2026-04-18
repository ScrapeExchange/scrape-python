'''
Unit tests for scrape_exchange/content_claim.py.

Covers FileContentClaim, RedisContentClaim, and
NullContentClaim.
'''

import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.content_claim import (
    ContentClaim,
    FileContentClaim,
    NullContentClaim,
    RedisContentClaim,
)


class TestFileContentClaimAcquire(
    unittest.IsolatedAsyncioTestCase
):
    '''Tests for FileContentClaim.acquire().'''

    async def asyncSetUp(self) -> None:
        self._tmpdir: tempfile.TemporaryDirectory = (
            tempfile.TemporaryDirectory()
        )
        self._claim: FileContentClaim = FileContentClaim(
            self._tmpdir.name
        )

    async def asyncTearDown(self) -> None:
        self._tmpdir.cleanup()

    async def test_acquire_first_call_returns_true(
        self,
    ) -> None:
        result: bool = await self._claim.acquire('vid001')
        self.assertTrue(result)

    async def test_acquire_second_call_returns_false(
        self,
    ) -> None:
        await self._claim.acquire('vid001')
        result: bool = await self._claim.acquire('vid001')
        self.assertFalse(result)

    async def test_acquire_after_release_succeeds(
        self,
    ) -> None:
        await self._claim.acquire('vid001')
        await self._claim.release('vid001')
        result: bool = await self._claim.acquire('vid001')
        self.assertTrue(result)

    async def test_release_is_idempotent(self) -> None:
        await self._claim.acquire('vid001')
        await self._claim.release('vid001')
        # Second release must not raise
        try:
            await self._claim.release('vid001')
        except Exception as exc:  # noqa: BLE001
            self.fail(
                f'release raised unexpectedly: {exc!r}'
            )

    async def test_different_content_ids_claimed_concurrently(
        self,
    ) -> None:
        result_a: bool = await self._claim.acquire('vidA')
        result_b: bool = await self._claim.acquire('vidB')
        self.assertTrue(result_a)
        self.assertTrue(result_b)

    async def test_claim_file_created(self) -> None:
        await self._claim.acquire('vid001')
        claim_path: Path = (
            Path(self._tmpdir.name) / '.claim-vid001'
        )
        self.assertTrue(claim_path.exists())

    async def test_claim_file_removed_after_release(
        self,
    ) -> None:
        await self._claim.acquire('vid001')
        await self._claim.release('vid001')
        claim_path: Path = (
            Path(self._tmpdir.name) / '.claim-vid001'
        )
        self.assertFalse(claim_path.exists())

    async def test_expired_claim_is_reclaimed(
        self,
    ) -> None:
        '''A claim older than TTL is treated as stale
        and can be reclaimed by another caller.'''
        claim: FileContentClaim = FileContentClaim(
            self._tmpdir.name, ttl=1,
        )
        await claim.acquire('vid_exp')
        # Back-date the claim file by 2 seconds so
        # it exceeds the 1-second TTL.
        path: Path = (
            Path(self._tmpdir.name) / '.claim-vid_exp'
        )
        old_time: float = time.time() - 2
        os.utime(str(path), (old_time, old_time))
        # Second acquire should succeed (stale claim).
        result: bool = await claim.acquire('vid_exp')
        self.assertTrue(result)

    async def test_non_expired_claim_not_reclaimed(
        self,
    ) -> None:
        '''A claim within TTL cannot be reclaimed.'''
        claim: FileContentClaim = FileContentClaim(
            self._tmpdir.name, ttl=3600,
        )
        await claim.acquire('vid_fresh')
        result: bool = await claim.acquire('vid_fresh')
        self.assertFalse(result)


class TestFileContentClaimCleanup(
    unittest.IsolatedAsyncioTestCase
):
    '''Tests for FileContentClaim.cleanup_stale().'''

    async def asyncSetUp(self) -> None:
        self._tmpdir: tempfile.TemporaryDirectory = (
            tempfile.TemporaryDirectory()
        )
        self._claim: FileContentClaim = FileContentClaim(
            self._tmpdir.name
        )

    async def asyncTearDown(self) -> None:
        self._tmpdir.cleanup()

    async def test_cleanup_removes_all_claim_files(
        self,
    ) -> None:
        await self._claim.acquire('vidX')
        await self._claim.acquire('vidY')
        await self._claim.acquire('vidZ')
        removed: int = await self._claim.cleanup_stale()
        self.assertEqual(removed, 3)
        remaining: list[Path] = list(
            Path(self._tmpdir.name).iterdir()
        )
        self.assertEqual(remaining, [])

    async def test_cleanup_returns_count_removed(
        self,
    ) -> None:
        await self._claim.acquire('vidA')
        await self._claim.acquire('vidB')
        removed: int = await self._claim.cleanup_stale()
        self.assertEqual(removed, 2)

    async def test_cleanup_does_not_remove_non_claim_files(
        self,
    ) -> None:
        other_file: Path = (
            Path(self._tmpdir.name) / 'other.txt'
        )
        other_file.write_text('data')
        await self._claim.acquire('vid001')
        removed: int = await self._claim.cleanup_stale()
        self.assertEqual(removed, 1)
        self.assertTrue(other_file.exists())

    async def test_cleanup_empty_dir_returns_zero(
        self,
    ) -> None:
        removed: int = await self._claim.cleanup_stale()
        self.assertEqual(removed, 0)


def _make_redis_mocks() -> tuple[MagicMock, MagicMock]:
    '''
    Build a (redis_module_mock, redis_instance_mock) pair.

    The redis_instance_mock has async methods wired up so
    tests can configure return values without touching the
    real Redis library.
    '''
    redis_instance: MagicMock = MagicMock()
    redis_instance.set = AsyncMock()
    redis_instance.delete = AsyncMock()
    redis_instance.scan = AsyncMock()

    aioredis_mod: MagicMock = MagicMock()
    aioredis_mod.from_url = MagicMock(
        return_value=redis_instance
    )

    redis_mod: MagicMock = MagicMock()
    redis_mod.asyncio = aioredis_mod

    return redis_mod, redis_instance


class TestRedisContentClaimAcquire(
    unittest.IsolatedAsyncioTestCase
):
    '''Tests for RedisContentClaim.acquire().'''

    def setUp(self) -> None:
        self._redis_mod, self._redis_inst = (
            _make_redis_mocks()
        )
        self._patcher = patch.dict(
            sys.modules,
            {
                'redis': self._redis_mod,
                'redis.asyncio': self._redis_mod.asyncio,
            },
        )
        self._patcher.start()
        self._claim: RedisContentClaim = RedisContentClaim(
            'redis://localhost:6379/0',
            platform='youtube',
            ttl=600,
        )

    def tearDown(self) -> None:
        self._patcher.stop()

    async def test_acquire_returns_true_when_set_nx_true(
        self,
    ) -> None:
        self._redis_inst.set.return_value = True
        result: bool = await self._claim.acquire('vid001')
        self.assertTrue(result)
        self._redis_inst.set.assert_awaited_once_with(
            'youtube:claim:vid001',
            str(__import__('os').getpid()),
            nx=True,
            ex=600,
        )

    async def test_acquire_returns_false_when_set_nx_none(
        self,
    ) -> None:
        self._redis_inst.set.return_value = None
        result: bool = await self._claim.acquire('vid001')
        self.assertFalse(result)


class TestRedisContentClaimRelease(
    unittest.IsolatedAsyncioTestCase
):
    '''Tests for RedisContentClaim.release().'''

    def setUp(self) -> None:
        self._redis_mod, self._redis_inst = (
            _make_redis_mocks()
        )
        self._patcher = patch.dict(
            sys.modules,
            {
                'redis': self._redis_mod,
                'redis.asyncio': self._redis_mod.asyncio,
            },
        )
        self._patcher.start()
        self._claim: RedisContentClaim = RedisContentClaim(
            'redis://localhost:6379/0',
            platform='youtube',
        )

    def tearDown(self) -> None:
        self._patcher.stop()

    async def test_release_deletes_correct_key(
        self,
    ) -> None:
        await self._claim.release('vid001')
        self._redis_inst.delete.assert_awaited_once_with(
            'youtube:claim:vid001'
        )


class TestRedisContentClaimCleanup(
    unittest.IsolatedAsyncioTestCase
):
    '''Tests for RedisContentClaim.cleanup_stale().'''

    def setUp(self) -> None:
        self._redis_mod, self._redis_inst = (
            _make_redis_mocks()
        )
        self._patcher = patch.dict(
            sys.modules,
            {
                'redis': self._redis_mod,
                'redis.asyncio': self._redis_mod.asyncio,
            },
        )
        self._patcher.start()
        self._claim: RedisContentClaim = RedisContentClaim(
            'redis://localhost:6379/0',
            platform='youtube',
        )

    def tearDown(self) -> None:
        self._patcher.stop()

    async def test_cleanup_scans_and_deletes_all_keys(
        self,
    ) -> None:
        keys: list[str] = [
            'youtube:claim:v1',
            'youtube:claim:v2',
            'youtube:claim:v3',
        ]
        # Single-page scan: cursor goes 0 → 0
        self._redis_inst.scan.return_value = (0, keys)
        removed: int = (
            await self._claim.cleanup_stale()
        )
        self.assertEqual(removed, 3)
        self._redis_inst.delete.assert_awaited_once_with(
            *keys
        )

    async def test_cleanup_multi_page_scan(self) -> None:
        page1: list[str] = [
            'youtube:claim:v1',
            'youtube:claim:v2',
        ]
        page2: list[str] = ['youtube:claim:v3']
        # First call returns cursor=42 (more pages),
        # second call returns cursor=0 (done).
        self._redis_inst.scan.side_effect = [
            (42, page1),
            (0, page2),
        ]
        removed: int = (
            await self._claim.cleanup_stale()
        )
        self.assertEqual(removed, 3)
        self.assertEqual(
            self._redis_inst.delete.await_count, 2
        )

    async def test_cleanup_no_keys_returns_zero(
        self,
    ) -> None:
        self._redis_inst.scan.return_value = (0, [])
        removed: int = (
            await self._claim.cleanup_stale()
        )
        self.assertEqual(removed, 0)
        self._redis_inst.delete.assert_not_awaited()


class TestNullContentClaim(unittest.IsolatedAsyncioTestCase):
    '''Tests for NullContentClaim.'''

    async def asyncSetUp(self) -> None:
        self._claim: NullContentClaim = NullContentClaim()

    async def test_acquire_always_returns_true(
        self,
    ) -> None:
        result: bool = await self._claim.acquire('vid001')
        self.assertTrue(result)

    async def test_acquire_multiple_times_returns_true(
        self,
    ) -> None:
        for _ in range(5):
            result: bool = await self._claim.acquire(
                'vid001'
            )
            self.assertTrue(result)

    async def test_release_does_not_raise(self) -> None:
        try:
            await self._claim.release('vid001')
        except Exception as exc:  # noqa: BLE001
            self.fail(
                f'release raised unexpectedly: {exc!r}'
            )

    async def test_cleanup_stale_returns_zero(
        self,
    ) -> None:
        removed: int = await self._claim.cleanup_stale()
        self.assertEqual(removed, 0)


class TestContentClaimAbstractBase(unittest.TestCase):
    '''Structural tests for the ContentClaim ABC.'''

    def test_is_abstract(self) -> None:
        self.assertTrue(
            hasattr(ContentClaim, '__abstractmethods__')
        )

    def test_file_claim_is_subclass(self) -> None:
        self.assertTrue(
            issubclass(FileContentClaim, ContentClaim)
        )

    def test_redis_claim_is_subclass(self) -> None:
        self.assertTrue(
            issubclass(RedisContentClaim, ContentClaim)
        )

    def test_null_claim_is_subclass(self) -> None:
        self.assertTrue(
            issubclass(NullContentClaim, ContentClaim)
        )


if __name__ == '__main__':
    unittest.main()
