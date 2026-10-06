'''
Unit tests for migrating the Redis video queue into the MongoDB
backlog.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import json
import unittest
from typing import Any

import fakeredis.aioredis
from mongomock_motor import AsyncMongoMockClient

from scrape_exchange.video_backlog import MongoVideoBacklog
from scrape_exchange.video_backlog_migration import (
    HotStats,
    OffloadStats,
    TerminalStats,
    migrate_terminal,
    offload_backlog,
    record_hot_window,
    terminal_document,
)
from scrape_exchange.video_scrape_queue import (
    RedisVideoScrapeQueue,
    VideoScrapeQueueSettings,
    VideoState,
    pack_qmeta,
)


class _Base(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self.coll: Any = AsyncMongoMockClient()['scraper']['youtube_videos']
        self.backlog: MongoVideoBacklog = MongoVideoBacklog(self.coll)
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(decode_responses=True)
        )
        self.legacy: RedisVideoScrapeQueue = RedisVideoScrapeQueue(
            self.redis, VideoScrapeQueueSettings(),
        )

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    async def _queued(self, count: int) -> None:
        i: int
        for i in range(count):
            video_id: str = f'v{i:03d}'
            await self.redis.zadd(
                'youtube:video:queue', {video_id: 1000 + i},
            )
            await self.redis.hset(
                self.legacy._k_qmeta(video_id), video_id,
                pack_qmeta(
                    source='channel', channel_id='UCx',
                    channel_handle='h', channel_is_verified=True,
                ),
            )

    async def _doc(self, video_id: str) -> dict[str, Any] | None:
        return await self.coll.find_one({'_id': video_id})


class TestTerminal(_Base):

    def test_terminal_document(self) -> None:
        doc: dict[str, Any] | None = terminal_document(
            'aaa', 'failed', json.dumps({
            'ts': 1234, 'last_error': 'gone', 'note': None,
            'source': 'rss', 'channel_id': 'UCx',
            'channel_is_verified': '1',
            }),
        )
        self.assertEqual(doc, {
            '_id': 'aaa', 'state': 'failed', 'enqueued_at': 1234,
            'source': 'rss', 'channel_id': 'UCx',
            'channel_is_verified': True,
            'record': {'ts': 1234, 'last_error': 'gone'},
        })
        self.assertIsNone(terminal_document('b', 'failed', 'not json'))
        self.assertIsNone(terminal_document('b', 'failed', '[1]'))

    async def test_migrate_terminal(self) -> None:
        await self.redis.hset('youtube:video:failed', mapping={
            'aaa': json.dumps({'ts': 10, 'last_error': 'x'}),
            'bad': 'not json',
        })
        await self.redis.hset(
            'youtube:video:removed', 'ccc', json.dumps({'ts': 20}),
        )
        dry: TerminalStats = await migrate_terminal(
            self.redis, self.backlog, delete_legacy=True,
        )
        self.assertEqual((dry.scanned, dry.inserted, dry.invalid), (3, 2, 1))
        self.assertIsNone(await self._doc('aaa'))

        stats: TerminalStats = await migrate_terminal(
            self.redis, self.backlog, apply=True, delete_legacy=True,
        )
        self.assertEqual(stats.inserted, 2)
        self.assertEqual(stats.hashes_deleted, 2)
        self.assertEqual(await self.redis.keys('*'), [])
        self.assertEqual((await self._doc('aaa'))['state'], 'failed')
        self.assertEqual((await self._doc('ccc'))['state'], 'removed')
        queue: RedisVideoScrapeQueue = RedisVideoScrapeQueue(
            self.redis, VideoScrapeQueueSettings(), backlog=self.backlog,
        )
        self.assertFalse(await queue.enqueue('aaa', source='rss'))

    async def test_limited_copy_keeps_hashes(self) -> None:
        await self.redis.hset('youtube:video:failed', mapping={
            'aaa': json.dumps({'ts': 10}), 'bbb': json.dumps({'ts': 11}),
        })
        stats: TerminalStats = await migrate_terminal(
            self.redis, self.backlog, apply=True, delete_legacy=True,
            limit=1,
        )
        self.assertEqual(stats.inserted, 1)
        self.assertEqual(stats.hashes_deleted, 0)
        self.assertEqual(await self.redis.hlen('youtube:video:failed'), 2)


class TestOffloadAndHot(_Base):

    async def test_dry_run_counts_only(self) -> None:
        await self._queued(10)
        stats: OffloadStats = await offload_backlog(
            self.redis, self.backlog, keep=4,
        )
        self.assertEqual(stats.scanned, 6)
        self.assertEqual(await self.redis.zcard('youtube:video:queue'), 10)
        self.assertEqual(await self.coll.count_documents({}), 0)

    async def test_offload_then_hot_then_refill_flow(self) -> None:
        await self._queued(10)
        stats: OffloadStats = await offload_backlog(
            self.redis, self.backlog, keep=4, apply=True, batch_size=4,
        )
        self.assertEqual(stats.inserted, 6)
        self.assertEqual(stats.removed_from_redis, 6)
        self.assertEqual(
            await self.redis.zrange('youtube:video:queue', 0, -1),
            ['v000', 'v001', 'v002', 'v003'],
        )
        self.assertIsNone(
            await self.redis.hget(self.legacy._k_qmeta('v009'), 'v009'),
        )
        doc: dict[str, Any] | None = await self._doc('v009')
        self.assertEqual(doc, {
            '_id': 'v009', 'state': 'queued', 'enqueued_at': 1009,
            'source': 'channel', 'channel_id': 'UCx',
            'channel_handle': 'h', 'channel_is_verified': True,
        })

        hot: HotStats = await record_hot_window(
            self.redis, self.backlog, apply=True, batch_size=3,
        )
        self.assertEqual((hot.scanned, hot.inserted), (4, 4))
        self.assertEqual((await self._doc('v000'))['state'], 'hot')

        again: OffloadStats = await offload_backlog(
            self.redis, self.backlog, keep=4, apply=True,
        )
        self.assertEqual(again.scanned, 0)
        hot_again: HotStats = await record_hot_window(
            self.redis, self.backlog, apply=True,
        )
        self.assertEqual(hot_again.inserted, 0)

        queue: RedisVideoScrapeQueue = RedisVideoScrapeQueue(
            self.redis, VideoScrapeQueueSettings(), backlog=self.backlog,
        )
        self.assertEqual(
            await queue.get_states(['v000', 'v009', 'new']),
            {'v000': VideoState.QUEUED, 'v009': VideoState.QUEUED,
             'new': None},
        )
        self.assertFalse(await queue.enqueue('v009', source='rss'))

    async def test_hot_before_backlog_is_corrected(self) -> None:
        await self._queued(5)
        await record_hot_window(self.redis, self.backlog, apply=True)
        await offload_backlog(self.redis, self.backlog, keep=2, apply=True)
        self.assertEqual((await self._doc('v004'))['state'], 'queued')
        self.assertEqual((await self._doc('v001'))['state'], 'hot')


if __name__ == '__main__':
    unittest.main()
