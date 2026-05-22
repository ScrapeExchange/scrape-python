'''Unit tests for the uploaded-video-ID Redis migration tool.'''

import unittest

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import fakeredis.aioredis

from tools.yt_uploaded_ids_migrate import (
    UploadedIdsMigrateSettings,
    migrate_uploaded_ids,
)


class _FakeUploadedVideoIds:

    _KEY: str = 'youtube:video:uploaded'
    client: fakeredis.aioredis.FakeRedis

    def __init__(self, redis_dsn: str) -> None:
        self._client = self.client


class TestUploadedIdsMigrate(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(
                decode_responses=True,
            )
        )
        _FakeUploadedVideoIds.client = self.redis

    async def asyncTearDown(self) -> None:
        await self.redis.flushall()
        await self.redis.aclose()

    async def test_migrates_ids_from_file(self) -> None:
        with TemporaryDirectory() as tmp:
            source: Path = Path(tmp) / 'uploaded.lst'
            source.write_text(
                ''.join(f'vid{i:08d}\n' for i in range(100)),
                encoding='utf-8',
            )
            settings = UploadedIdsMigrateSettings(
                _cli_parse_args=[],
                uploaded_video_list=str(source),
                redis_dsn='redis://test',
                batch_size=10,
            )
            with patch(
                'tools.yt_uploaded_ids_migrate.UploadedVideoIds',
                _FakeUploadedVideoIds,
            ):
                result = await migrate_uploaded_ids(settings)
        self.assertEqual(result, (100, 100))
        self.assertEqual(
            await self.redis.scard(_FakeUploadedVideoIds._KEY),
            100,
        )

    async def test_rerun_is_idempotent(self) -> None:
        with TemporaryDirectory() as tmp:
            source: Path = Path(tmp) / 'uploaded.lst'
            source.write_text(
                ''.join(f'vid{i:08d}\n' for i in range(100)),
                encoding='utf-8',
            )
            settings = UploadedIdsMigrateSettings(
                _cli_parse_args=[],
                uploaded_video_list=str(source),
                redis_dsn='redis://test',
                batch_size=25,
            )
            with patch(
                'tools.yt_uploaded_ids_migrate.UploadedVideoIds',
                _FakeUploadedVideoIds,
            ):
                await migrate_uploaded_ids(settings)
                await migrate_uploaded_ids(settings)
        self.assertEqual(
            await self.redis.scard(_FakeUploadedVideoIds._KEY),
            100,
        )

    async def test_duplicate_and_blank_lines_do_not_report_drift(self) -> None:
        with TemporaryDirectory() as tmp:
            source: Path = Path(tmp) / 'uploaded.lst'
            source.write_text(
                'vid00000001\n\nvid00000001\nvid00000002\n',
                encoding='utf-8',
            )
            settings = UploadedIdsMigrateSettings(
                _cli_parse_args=[],
                uploaded_video_list=str(source),
                redis_dsn='redis://test',
                batch_size=2,
            )
            with patch(
                'tools.yt_uploaded_ids_migrate.UploadedVideoIds',
                _FakeUploadedVideoIds,
            ):
                result = await migrate_uploaded_ids(settings)
        self.assertEqual(result, (4, 2))
        self.assertEqual(
            await self.redis.scard(_FakeUploadedVideoIds._KEY),
            2,
        )


if __name__ == '__main__':
    unittest.main()
