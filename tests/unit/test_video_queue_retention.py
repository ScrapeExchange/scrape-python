'''Cleanup keeps terminal membership and protects concurrent queue work.'''

import json
import time
import unittest

import fakeredis.aioredis

from scrape_exchange.video_queue_retention import (
    RetentionStats,
    VideoQueueRetention,
)
from scrape_exchange.video_scrape_queue import (
    RedisVideoScrapeQueue,
    VideoScrapeQueueSettings,
    VideoState,
)


class TestVideoQueueRetention(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(decode_responses=True)
        )
        self.now: int = int(time.time())
        self.old: int = self.now - 31 * 86400
        self.retention: VideoQueueRetention = VideoQueueRetention(
            self.redis, platform='youtube',
        )
        self.queue: RedisVideoScrapeQueue = RedisVideoScrapeQueue(
            self.redis, VideoScrapeQueueSettings(),
        )
        self.record: str = json.dumps({
            'ts': self.old, 'last_error': 'diagnostic ' * 100,
            'source': 'rss', 'note': None,
        })
        await self.redis.hset(
            'youtube:video:failed', 'old', self.record,
        )
        await self.redis.hset('youtube:video:meta:old', mapping={
            'state': 'failed', 'last_error': 'old diagnostic',
        })

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    async def test_dry_run_reports_without_mutation(self) -> None:
        stats: RetentionStats = await self.retention.run(
            now=self.now, pause_seconds=0,
        )
        self.assertEqual(stats.records_compacted, 1)
        self.assertEqual(stats.metadata_deleted, 1)
        self.assertEqual(
            await self.redis.hget('youtube:video:failed', 'old'),
            self.record,
        )
        self.assertEqual(
            await self.redis.ttl('youtube:video:meta:old'), -1,
        )

    async def test_apply_preserves_membership_and_is_idempotent(self) -> None:
        await self.redis.sadd('youtube:video:uploaded', 'uploaded')
        stats: RetentionStats = await self.retention.run(
            apply=True, now=self.now, pause_seconds=0,
        )
        self.assertEqual(stats.records_compacted, 1)
        self.assertEqual(stats.metadata_deleted, 1)
        self.assertEqual(
            json.loads(await self.redis.hget('youtube:video:failed', 'old')),
            {'ts': self.old},
        )
        self.assertFalse(await self.redis.exists('youtube:video:meta:old'))
        self.assertFalse(await self.queue.enqueue('old', source='rss'))
        self.assertEqual(await self.queue.get_state('old'), VideoState.FAILED)
        self.assertTrue(
            await self.redis.sismember('youtube:video:uploaded', 'uploaded'),
        )
        again: RetentionStats = await self.retention.run(
            apply=True, now=self.now, pause_seconds=0,
        )
        self.assertEqual(again.records_compacted, 0)
        self.assertEqual(again.metadata_deleted, 0)

    async def test_recent_diagnostics_kept_and_legacy_ttl_backfilled(
        self,
    ) -> None:
        recent: str = json.dumps({
            'ts': self.now - 86400, 'last_error': 'recent',
        })
        await self.redis.hset('youtube:video:failed', 'old', recent)
        stats: RetentionStats = await self.retention.run(
            apply=True, now=self.now, pause_seconds=0,
        )
        self.assertEqual(stats.records_compacted, 0)
        self.assertEqual(stats.ttls_added, 1)
        self.assertEqual(
            await self.redis.hget('youtube:video:failed', 'old'), recent,
        )
        ttl: int = await self.redis.ttl('youtube:video:meta:old')
        self.assertGreater(ttl, 28 * 86400)
        self.assertLessEqual(ttl, 29 * 86400)

    async def test_queued_and_inflight_metadata_are_untouched(self) -> None:
        await self.redis.hset('youtube:video:meta:old', 'state', 'queued')
        stats: RetentionStats = await self.retention.run(
            apply=True, now=self.now, pause_seconds=0,
        )
        self.assertEqual(stats.skipped, 1)
        self.assertEqual(
            await self.redis.hget('youtube:video:failed', 'old'),
            self.record,
        )
        self.assertEqual(
            await self.redis.ttl('youtube:video:meta:old'), -1,
        )

    async def test_queue_membership_blocks_cleanup_even_with_stale_meta(
        self,
    ) -> None:
        await self.redis.zadd('youtube:video:queue', {'old': self.now})
        stats: RetentionStats = await self.retention.run(
            apply=True, now=self.now, pause_seconds=0,
        )
        self.assertEqual(stats.skipped, 1)
        self.assertTrue(await self.redis.exists('youtube:video:meta:old'))

    async def test_changed_record_is_not_overwritten(self) -> None:
        await self.redis.hset(
            'youtube:video:failed', 'old', '{"ts":1,"note":"changed"}',
        )
        result: list[int] | None = await self.retention.process_record(
            'failed', 'old', self.record, now=self.now, apply=True,
        )
        self.assertEqual(result, [0, 0, 0, 0])
        self.assertEqual(
            await self.redis.hget('youtube:video:failed', 'old'),
            '{"ts":1,"note":"changed"}',
        )

    async def test_bad_or_missing_timestamps_are_skipped(self) -> None:
        raw: str
        for raw in ('null', '{}', '{"ts":null}', '{"ts":true}',
                    '{"ts":-1}', '{"ts":"123"}', 'invalid'):
            with self.subTest(raw=raw):
                await self.redis.hset('youtube:video:failed', 'old', raw)
                stats: RetentionStats = await self.retention.run(
                    apply=True, now=self.now, pause_seconds=0,
                )
                self.assertEqual(stats.invalid, 1)
                self.assertTrue(
                    await self.redis.exists('youtube:video:meta:old'),
                )

    async def test_limit_caps_records_examined(self) -> None:
        await self.redis.hset('youtube:video:failed', mapping={
            f'other-{index}': self.record for index in range(5)
        })
        stats: RetentionStats = await self.retention.run(
            apply=True, now=self.now, limit=2, pause_seconds=0,
        )
        self.assertEqual(stats.scanned, 2)
        self.assertEqual(stats.records_compacted, 2)

    async def test_platform_scope(self) -> None:
        retention: VideoQueueRetention = VideoQueueRetention(
            self.redis, platform='tiktok',
        )
        stats: RetentionStats = await retention.run(apply=True, pause_seconds=0)
        self.assertEqual(stats.scanned, 0)
        self.assertEqual(
            await self.redis.hget('youtube:video:failed', 'old'),
            self.record,
        )
