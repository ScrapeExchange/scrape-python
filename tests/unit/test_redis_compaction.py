'''
Unit tests for the compact video-queue meta layout and the one-off
migration of legacy Redis structures to compact formats.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import unittest

import fakeredis.aioredis

from scrape_exchange.redis_compaction import (
    LEGACY_EXCHANGE_CHANNELS_KEY,
    LEGACY_UPLOADED_KEY,
    SetCopyStats,
    VideoMetaStats,
    copy_set_to_bloom,
    migrate_video_meta,
)
from scrape_exchange.video_scrape_queue import (
    QMETA_BUCKETS,
    RedisVideoScrapeQueue,
    VideoScrapeQueueEntry,
    VideoScrapeQueueSettings,
    VideoState,
    pack_qmeta,
    qmeta_bucket,
    unpack_qmeta,
)
from scrape_exchange.youtube.exchange_channels_set import (
    RedisExchangeChannelsSet,
)
from scrape_exchange.youtube.uploaded_video_ids import UploadedVideoIds


class TestPacking(unittest.TestCase):

    def test_round_trip(self) -> None:
        packed: str = pack_qmeta(
            source='channel', channel_id='UC1234567890abcdefghij',
            channel_handle='Some.Handle_1', channel_is_verified=False,
        )
        self.assertEqual(
            packed, 'channel|UC1234567890abcdefghij|Some.Handle_1|0',
        )
        self.assertEqual(unpack_qmeta(packed), {
            'source': 'channel',
            'channel_id': 'UC1234567890abcdefghij',
            'channel_handle': 'Some.Handle_1',
            'channel_is_verified': '0',
        })

    def test_empty_fields_omitted(self) -> None:
        self.assertEqual(
            unpack_qmeta(pack_qmeta(source='rss')), {'source': 'rss'},
        )
        self.assertEqual(unpack_qmeta('|||'), {})

    def test_separator_stripped_from_values(self) -> None:
        packed: str = pack_qmeta(source='a|b', channel_handle='x|y')
        self.assertEqual(
            unpack_qmeta(packed), {'source': 'ab', 'channel_handle': 'xy'},
        )

    def test_bucket_is_stable_and_in_range(self) -> None:
        bucket: int = qmeta_bucket('dQw4w9WgXcQ')
        self.assertEqual(bucket, qmeta_bucket('dQw4w9WgXcQ'))
        self.assertTrue(0 <= bucket < QMETA_BUCKETS)


class _QueueBase(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(decode_responses=True)
        )
        self.queue: RedisVideoScrapeQueue = RedisVideoScrapeQueue(
            self.redis, VideoScrapeQueueSettings(),
        )

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()


class TestCompactQueue(_QueueBase):

    async def test_queued_video_uses_one_bucket_field(self) -> None:
        await self.queue.enqueue(
            'aaa', source='channel', channel_id='UCx',
            channel_handle='h', channel_url='https://example/x',
        )
        keys: list[str] = sorted(await self.redis.keys('*'))
        self.assertEqual(
            keys, sorted([
                'youtube:video:queue', self.queue._k_qmeta('aaa'),
            ]),
        )
        self.assertEqual(
            await self.redis.hget(self.queue._k_qmeta('aaa'), 'aaa'),
            'channel|UCx|h|',
        )

    async def test_in_flight_video_is_still_known(self) -> None:
        await self.queue.enqueue('aaa', source='rss')
        self.assertEqual(await self.queue.pop(1), ['aaa'])
        self.assertFalse(await self.queue.enqueue('aaa', source='rss'))
        self.assertEqual(
            await self.queue.get_state('aaa'), VideoState.QUEUED,
        )

    async def test_force_on_queued_merges_context(self) -> None:
        await self.queue.enqueue(
            'aaa', source='rss', channel_id='UCx', channel_handle='h',
        )
        await self.queue.force_enqueue(
            'aaa', source='cli', channel_handle='h2',
        )
        meta: dict[str, str] = await self.queue.get_meta('aaa')
        self.assertEqual(meta['source'], 'rss')
        self.assertEqual(meta['channel_id'], 'UCx')
        self.assertEqual(meta['channel_handle'], 'h2')
        self.assertEqual(meta['force'], '1')

    async def test_retry_diagnostics_merge_into_entry(self) -> None:
        await self.queue.enqueue('aaa', source='rss', channel_id='UCx')
        await self.queue.bump_attempts('aaa', last_error='timeout')
        entries: list[VideoScrapeQueueEntry] = (
            await self.queue.pop_entries(1)
        )
        self.assertEqual(entries[0].channel.channel_id, 'UCx')
        self.assertEqual(entries[0].meta['attempts'], '1')
        self.assertEqual(entries[0].meta['state'], 'queued')
        await self.queue.complete('aaa')
        self.assertEqual(await self.redis.keys('*'), [])

    async def test_iter_members_and_search(self) -> None:
        await self.queue.enqueue('aaa', source='rss')
        await self.queue.enqueue('bbb', source='channel')
        await self.queue.bump_attempts('bbb', last_error='boom')
        await self.queue.enqueue('ccc', source='cli')
        await self.queue.bump_attempts('ccc', last_error='gone')
        await self.queue.mark('ccc', state=VideoState.FAILED)
        members: dict[str, dict] = {
            rec['video_id']: rec
            async for rec in self.queue.iter_members()
        }
        self.assertEqual(set(members), {'aaa', 'bbb', 'ccc'})
        self.assertEqual(members['bbb']['state'], 'queued')
        self.assertEqual(members['ccc']['state'], 'failed')
        self.assertEqual(
            sorted(await self.queue.search_meta('boom')), ['bbb'],
        )
        self.assertEqual(
            sorted(await self.queue.search_meta('rss')), ['aaa'],
        )


class TestMigrateVideoMeta(_QueueBase):

    async def _legacy(self, video_id: str, **fields: str) -> None:
        await self.redis.hset(
            f'youtube:video:meta:{video_id}', mapping=fields,
        )

    async def test_dry_run_writes_nothing(self) -> None:
        await self._legacy('aaa', state='queued', source='rss')
        await self.redis.zadd('youtube:video:queue', {'aaa': 100})
        stats: VideoMetaStats = await migrate_video_meta(self.redis)
        self.assertEqual(stats.migrated, 1)
        self.assertEqual(
            await self.redis.exists('youtube:video:meta:aaa'), 1,
        )
        self.assertIsNone(
            await self.redis.hget(self.queue._k_qmeta('aaa'), 'aaa'),
        )

    async def test_apply_converts_queued_and_keeps_terminal(self) -> None:
        await self._legacy(
            'aaa', state='queued', source='channel', created_at='100',
            channel_id='UCx', channel_handle='h',
            channel_url='https://example/x', channel_is_verified='1',
        )
        await self.redis.zadd('youtube:video:queue', {'aaa': 100})
        # Popped by a scraper that died: not in the queue.
        await self._legacy(
            'bbb', state='queued', source='rss', created_at='200',
            force='1', attempts='2',
        )
        await self._legacy('ccc', state='failed', last_error='x')
        # One SCAN page: fakeredis' offset cursor skips keys deleted
        # mid-scan, unlike Redis' full-iteration guarantee.
        stats: VideoMetaStats = await migrate_video_meta(
            self.redis, apply=True, requeue_stranded=True,
            batch_size=100,
        )
        self.assertEqual(stats.scanned, 3)
        self.assertEqual(stats.migrated, 2)
        self.assertEqual(stats.stranded, 1)
        self.assertEqual(stats.requeued, 1)
        self.assertEqual(stats.meta_deleted, 1)
        self.assertEqual(stats.meta_kept, 1)
        self.assertEqual(stats.skipped, 1)

        self.assertEqual(
            await self.redis.exists('youtube:video:meta:aaa'), 0,
        )
        meta: dict[str, str] = await self.queue.get_meta('aaa')
        self.assertEqual(meta['state'], 'queued')
        self.assertEqual(meta['channel_id'], 'UCx')
        self.assertEqual(meta['channel_handle'], 'h')
        self.assertEqual(meta['channel_is_verified'], '1')
        self.assertEqual(meta['created_at'], '100')

        self.assertEqual(
            await self.redis.zscore('youtube:video:queue', 'bbb'), 200,
        )
        self.assertEqual(
            await self.redis.hgetall('youtube:video:meta:bbb'),
            {'force': '1', 'attempts': '2'},
        )
        self.assertTrue(await self.queue.consume_force('bbb'))

        self.assertEqual(
            await self.redis.hgetall('youtube:video:meta:ccc'),
            {'state': 'failed', 'last_error': 'x'},
        )

    async def test_stranded_not_requeued_by_default(self) -> None:
        await self._legacy('bbb', state='queued', source='rss')
        stats: VideoMetaStats = await migrate_video_meta(
            self.redis, apply=True,
        )
        self.assertEqual(stats.stranded, 1)
        self.assertEqual(stats.requeued, 0)
        self.assertEqual(await self.redis.zcard('youtube:video:queue'), 0)
        # Still known, as before the migration.
        self.assertFalse(await self.queue.enqueue('bbb', source='rss'))

    async def test_limit_and_rerun_are_safe(self) -> None:
        video_id: str
        for video_id in ('aaa', 'bbb', 'ccc'):
            await self._legacy(video_id, state='queued', source='rss')
            await self.redis.zadd('youtube:video:queue', {video_id: 1})
        first: VideoMetaStats = await migrate_video_meta(
            self.redis, apply=True, limit=1, batch_size=1,
        )
        self.assertEqual(first.scanned, 1)
        await migrate_video_meta(self.redis, apply=True)
        again: VideoMetaStats = await migrate_video_meta(
            self.redis, apply=True,
        )
        self.assertEqual(again.scanned, 0)
        self.assertEqual(
            await self.queue.get_states(['aaa', 'bbb', 'ccc']),
            {v: VideoState.QUEUED for v in ('aaa', 'bbb', 'ccc')},
        )


class TestCopySetToBloom(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(decode_responses=True)
        )

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    async def test_uploaded_copy_and_delete(self) -> None:
        ids: list[str] = [f'v{i:010d}' for i in range(2500)]
        await self.redis.sadd(LEGACY_UPLOADED_KEY, *ids)
        uploaded: UploadedVideoIds = UploadedVideoIds(
            '', redis_client=self.redis,
        )
        dry: SetCopyStats = await copy_set_to_bloom(
            self.redis, legacy_key=LEGACY_UPLOADED_KEY,
            add_many=uploaded.add_many, delete_legacy=True,
        )
        self.assertEqual(dry.copied, 2500)
        self.assertFalse(dry.legacy_deleted)
        self.assertFalse(await uploaded.contains(ids[0]))

        stats: SetCopyStats = await copy_set_to_bloom(
            self.redis, legacy_key=LEGACY_UPLOADED_KEY,
            add_many=uploaded.add_many, apply=True,
            delete_legacy=True, batch_size=1000,
        )
        self.assertEqual(stats.copied, 2500)
        self.assertTrue(stats.legacy_deleted)
        self.assertEqual(await self.redis.exists(LEGACY_UPLOADED_KEY), 0)
        found: dict[str, bool] = await uploaded.contains_many(ids)
        self.assertTrue(all(found.values()))

    async def test_limited_copy_keeps_legacy_set(self) -> None:
        await self.redis.sadd(
            LEGACY_EXCHANGE_CHANNELS_KEY, 'UCa', 'UCb', 'UCc',
        )
        channels: RedisExchangeChannelsSet = RedisExchangeChannelsSet(
            self.redis,
        )
        stats: SetCopyStats = await copy_set_to_bloom(
            self.redis, legacy_key=LEGACY_EXCHANGE_CHANNELS_KEY,
            add_many=channels.add_many, apply=True,
            delete_legacy=True, limit=2,
        )
        self.assertEqual(stats.copied, 2)
        self.assertFalse(stats.legacy_deleted)
        self.assertEqual(
            await self.redis.scard(LEGACY_EXCHANGE_CHANNELS_KEY), 3,
        )
        self.assertEqual(await channels.size(), 2)


if __name__ == '__main__':
    unittest.main()
