'''Unit tests for tools/tt_video_queue_migrate.py.'''

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

try:
    import fakeredis.aioredis
    _HAVE_FAKEREDIS: bool = True
except ImportError:
    _HAVE_FAKEREDIS = False

from scrape_exchange.video_scrape_queue import (
    RedisVideoScrapeQueue,
    VideoScrapeQueueSettings,
    VideoState,
)
from tools import tt_video_queue_migrate as mod

URL: str = 'https://www.tiktok.com/@someone/video/{}'


class TestHelpers(unittest.TestCase):

    def test_video_id_from_member(self) -> None:
        self.assertEqual(
            mod.video_id_from_member('7691750271394827541'),
            '7691750271394827541',
        )
        self.assertEqual(
            mod.video_id_from_member(URL.format('7691750271394827541')),
            '7691750271394827541',
        )
        self.assertEqual(
            mod.video_id_from_member(
                URL.format('7691750271394827541') + '?lang=en',
            ),
            '7691750271394827541',
        )
        for junk in (
            '', 'abc', '0123', URL.format('12ab'),
            '18446744073709551616',
        ):
            with self.subTest(junk=junk):
                self.assertIsNone(mod.video_id_from_member(junk))

    def test_terminal_entry_maps_reason_and_ts(self) -> None:
        self.assertEqual(
            mod.terminal_entry(
                'failed',
                json.dumps({'ts': 1700000000, 'last_error': 'rate_limit'}),
            ),
            ('failed', 'rate_limit', 1700000000),
        )
        self.assertEqual(
            mod.terminal_entry('unavailable', 'not json'),
            ('unavailable', 'none', 0),
        )
        self.assertEqual(
            mod.terminal_entry(
                'failed',
                json.dumps({'last_error': 'transient retries exhausted: x'}),
            ),
            ('failed', 'retries_exhausted', 0),
        )

    def test_line_formats(self) -> None:
        self.assertEqual(mod.format_queued_line('5', 12), '5\t12')
        self.assertEqual(
            mod.format_terminal_line('5', ('failed', 'other', 9)),
            '5\tfailed\tother\t9',
        )


@unittest.skipUnless(_HAVE_FAKEREDIS, 'fakeredis not installed')
class TestRedisQueue(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self.redis: Any = fakeredis.aioredis.FakeRedis(
            decode_responses=True,
        )
        queue: RedisVideoScrapeQueue = RedisVideoScrapeQueue(
            self.redis, VideoScrapeQueueSettings(), platform='tiktok',
        )
        # Old URL entries, a bare id from scrape_upload, the same video
        # as both URL and id, one popped and never completed, and one
        # marked failed.
        for member in (
            URL.format('101'), URL.format('102'), '103',
            URL.format('104'), '104', URL.format('105'),
            URL.format('106'),
        ):
            await queue.enqueue(member, source='test')
        # Equal enqueue scores, so which one pops depends on member
        # order; the test only needs to know which.
        popped: list[str] = await queue.pop(1)
        self.popped_id: str | None = mod.video_id_from_member(popped[0])
        await queue.mark(
            URL.format('106'), state=VideoState.FAILED,
            last_error='rate_limit',
        )
        # Unrelated keys that must survive delete-redis.
        await self.redis.set('tiktok:video:uploaded_bf', 'x')
        await self.redis.zadd('scrape:tiktok:queue:1', {'someone': 1})
        await self.redis.zadd('youtube:video:queue', {'dQw4w9WgXcQ': 1})

    async def test_collect_converts_and_classifies(self) -> None:
        counts: mod.QueueCounts = mod.QueueCounts()
        export: mod.QueueExport = await mod.collect_redis_queue(
            self.redis, counts,
        )
        self.assertEqual(
            sorted(export.queued), ['101', '102', '103', '104', '105'],
        )
        # The popped one goes to the front.
        self.assertEqual(export.queued[self.popped_id], 0)
        self.assertGreater(export.queued['102'], 0)
        self.assertEqual(
            export.terminal['106'][:2], ('failed', 'rate_limit'),
        )
        self.assertEqual(counts.waiting, 4)
        self.assertEqual(counts.in_progress, 1)
        self.assertEqual(counts.terminal, 1)
        self.assertEqual(counts.duplicates, 1)

    async def test_export_queue_writes_import_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings: mod.MigrateSettings = mod.MigrateSettings(
                _cli_parse_args=False,
                command='export-queue',
                redis_dsn='redis://x',
                output_dir=Path(tmp),
            )
            self.redis.aclose = mock.AsyncMock()
            with mock.patch.object(
                mod, 'redis_from_url', return_value=self.redis,
            ):
                summary: dict = await mod.export_queue(settings)
            queued: list[str] = (
                Path(tmp, 'queued.tsv').read_text().splitlines()
            )
            terminal: list[str] = (
                Path(tmp, 'terminal.tsv').read_text().splitlines()
            )
        self.assertEqual(summary['queued_lines'], 5)
        self.assertEqual(
            sorted(line.split('\t')[0] for line in queued),
            ['101', '102', '103', '104', '105'],
        )
        self.assertEqual(len(terminal), 1)
        self.assertTrue(terminal[0].startswith('106\tfailed\trate_limit\t'))

    async def test_delete_redis_needs_apply(self) -> None:
        settings: mod.MigrateSettings = mod.MigrateSettings(
            _cli_parse_args=False, command='delete-redis',
            redis_dsn='redis://x',
        )
        self.redis.aclose = mock.AsyncMock()
        with mock.patch.object(
            mod, 'redis_from_url', return_value=self.redis,
        ):
            summary: dict = await mod.delete_redis(settings)
        self.assertFalse(summary['applied'])
        self.assertEqual(summary['would_delete']['tiktok:video:queue'], 1)
        self.assertTrue(await self.redis.exists('tiktok:video:queue'))

    async def test_delete_redis_removes_only_queue_keys(self) -> None:
        settings: mod.MigrateSettings = mod.MigrateSettings(
            _cli_parse_args=False, command='delete-redis',
            redis_dsn='redis://x', backlog_server_dsn='redis://y',
            apply=True,
        )
        self.redis.aclose = mock.AsyncMock()
        with mock.patch.object(
            mod, 'redis_from_url', return_value=self.redis,
        ), mock.patch.object(
            mod, '_backlog_queue_info',
            mock.AsyncMock(return_value={'queued': 5}),
        ):
            summary: dict = await mod.delete_redis(settings)
        self.assertTrue(summary['applied'])
        remaining: list[str] = sorted(
            [key async for key in self.redis.scan_iter(match='*')],
        )
        self.assertEqual(
            remaining,
            [
                'scrape:tiktok:queue:1',
                'tiktok:video:uploaded_bf',
                'youtube:video:queue',
            ],
        )

    async def test_delete_redis_refuses_without_backlog_queue(
        self,
    ) -> None:
        settings: mod.MigrateSettings = mod.MigrateSettings(
            _cli_parse_args=False, command='delete-redis',
            redis_dsn='redis://x', backlog_server_dsn='redis://y',
            apply=True,
        )
        self.redis.aclose = mock.AsyncMock()
        with mock.patch.object(
            mod, 'redis_from_url', return_value=self.redis,
        ), mock.patch.object(
            mod, '_backlog_queue_info',
            mock.AsyncMock(side_effect=RuntimeError('no such queue')),
        ), self.assertRaises(RuntimeError):
            await mod.delete_redis(settings)
        self.assertTrue(await self.redis.exists('tiktok:video:queue'))


if __name__ == '__main__':
    unittest.main()
