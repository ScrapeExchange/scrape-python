'''
Unit tests for the MongoDB video backlog and the video queue in
backlog mode (MongoDB source of truth, Redis hot window).

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import time
import unittest
from typing import Any

import fakeredis.aioredis
from mongomock_motor import AsyncMongoMockClient

from scrape_exchange.video_backlog import (
    MongoVideoBacklog,
    backlog_document,
)
from scrape_exchange.video_scrape_queue import (
    FORCE_PRIORITY_SECONDS,
    RedisVideoScrapeQueue,
    VideoScrapeQueueEntry,
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
        self.queue: RedisVideoScrapeQueue = RedisVideoScrapeQueue(
            self.redis, VideoScrapeQueueSettings(),
            backlog=self.backlog,
        )

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    async def _hot(self, video_id: str, score: float = 100.0) -> None:
        '''Put *video_id* in the Redis hot window, as the refill
        service does.'''
        await self.coll.insert_one(backlog_document(
            video_id, state='hot', enqueued_at=score, source='rss',
        ))
        await self.redis.zadd('youtube:video:queue', {video_id: score})
        await self.redis.hset(
            self.queue._k_qmeta(video_id), video_id,
            pack_qmeta(source='rss'),
        )

    async def _doc(self, video_id: str) -> dict[str, Any] | None:
        return await self.coll.find_one({'_id': video_id})


class TestBacklog(_Base):

    async def test_add_dedupes(self) -> None:
        self.assertTrue(await self.backlog.add(
            'aaa', source='channel', channel_id='UCx',
            channel_handle='h', channel_is_verified=True,
            enqueued_at=50,
        ))
        self.assertFalse(await self.backlog.add('aaa', source='rss'))
        self.assertEqual(await self._doc('aaa'), {
            '_id': 'aaa', 'state': 'queued', 'enqueued_at': 50,
            'source': 'channel', 'channel_id': 'UCx',
            'channel_handle': 'h', 'channel_is_verified': True,
        })

    async def test_add_many_skips_existing(self) -> None:
        await self.backlog.add('aaa', source='rss')
        inserted: int = await self.backlog.add_many([
            backlog_document('aaa', state='queued', enqueued_at=1),
            backlog_document('bbb', state='queued', enqueued_at=2),
        ])
        self.assertEqual(inserted, 1)
        self.assertEqual(await self.backlog.add_many([]), 0)

    async def test_oldest_queued_and_set_state(self) -> None:
        await self.backlog.add_many([
            backlog_document('new', state='queued', enqueued_at=30),
            backlog_document('old', state='queued', enqueued_at=10),
            backlog_document('hot', state='hot', enqueued_at=5),
        ])
        docs: list[dict[str, Any]] = await self.backlog.oldest_queued(5)
        self.assertEqual([d['_id'] for d in docs], ['old', 'new'])
        changed: int = await self.backlog.set_state(
            ['old', 'hot'], from_state='queued', to_state='hot',
        )
        self.assertEqual(changed, 1)
        self.assertEqual(
            await self.backlog.hot_page('', 10), ['hot', 'old'],
        )
        self.assertEqual(await self.backlog.hot_page('hot', 10), ['old'])

    async def test_ensure_indexes(self) -> None:
        await self.backlog.ensure_indexes()
        info: dict[str, Any] = await self.coll.index_information()
        self.assertEqual(
            info['state_enqueued_at']['key'],
            [('state', 1), ('enqueued_at', 1)],
        )
        self.assertEqual(info['state_id']['key'], [('state', 1), ('_id', 1)])

    async def test_hot_page_creates_index_first(self) -> None:
        await self.coll.insert_one(
            backlog_document('hot', state='hot', enqueued_at=1),
        )
        self.assertEqual(await self.backlog.hot_page('', 10), ['hot'])
        self.assertIn('state_id', await self.coll.index_information())

    async def test_counts_derive_queued_and_cache(self) -> None:
        await self.backlog.add_many([
            backlog_document('a', state='queued', enqueued_at=1),
            backlog_document('b', state='hot', enqueued_at=1),
            backlog_document('c', state='failed', enqueued_at=1),
        ])
        self.assertEqual(await self.backlog.counts(), {
            'unavailable': 0, 'failed': 1, 'removed': 0, 'queued': 2,
        })
        await self.backlog.add('d', source='rss')
        self.assertEqual((await self.backlog.counts())['queued'], 2)


class TestQueueBacklogMode(_Base):

    async def test_enqueue_writes_mongo_only(self) -> None:
        self.assertTrue(await self.queue.enqueue(
            'aaa', source='channel', channel_id='UCx',
        ))
        self.assertFalse(await self.queue.enqueue('aaa', source='rss'))
        self.assertEqual(await self.redis.keys('*'), [])
        self.assertEqual((await self._doc('aaa'))['state'], 'queued')

    async def test_enqueue_hot_video_is_known(self) -> None:
        await self._hot('aaa')
        self.assertFalse(await self.queue.enqueue('aaa', source='rss'))

    async def test_get_states(self) -> None:
        await self._hot('hot')
        await self.backlog.add('cold', source='rss')
        await self.coll.insert_one(
            backlog_document('dead', state='removed', enqueued_at=1),
        )
        self.assertEqual(
            await self.queue.get_states(['hot', 'cold', 'dead', 'new']),
            {
                'hot': VideoState.QUEUED, 'cold': VideoState.QUEUED,
                'dead': VideoState.REMOVED, 'new': None,
            },
        )

    async def test_pop_complete_deletes_everywhere(self) -> None:
        await self._hot('aaa')
        entries: list[VideoScrapeQueueEntry] = (
            await self.queue.pop_entries(5)
        )
        self.assertEqual([e.video_id for e in entries], ['aaa'])
        self.assertEqual(entries[0].source, 'rss')
        await self.queue.complete('aaa')
        self.assertIsNone(await self._doc('aaa'))
        self.assertEqual(await self.redis.keys('*'), [])

    async def test_mark_moves_state_to_mongo(self) -> None:
        await self._hot('aaa')
        await self.queue.bump_attempts('aaa', last_error='timeout')
        await self.queue.mark(
            'aaa', state=VideoState.FAILED, last_error='gone',
        )
        doc: dict[str, Any] | None = await self._doc('aaa')
        assert doc is not None
        self.assertEqual(doc['state'], 'failed')
        self.assertEqual(doc['record']['last_error'], 'gone')
        self.assertEqual(doc['source'], 'rss')
        self.assertEqual(await self.redis.zcard('youtube:video:queue'), 0)
        self.assertIsNone(
            await self.redis.hget(self.queue._k_qmeta('aaa'), 'aaa'),
        )
        # No Redis tombstone; the retained diagnostics expire.
        self.assertIsNone(
            await self.redis.hget('youtube:video:failed', 'aaa'),
        )
        self.assertGreater(
            await self.redis.ttl('youtube:video:meta:aaa'), 0,
        )
        self.assertEqual(
            await self.queue.get_state('aaa'), VideoState.FAILED,
        )
        self.assertFalse(await self.queue.enqueue('aaa', source='rss'))
        meta: dict[str, str] = await self.queue.get_meta('aaa')
        self.assertEqual(meta['state'], 'failed')
        self.assertEqual(meta['last_error'], 'gone')
        self.assertEqual(meta['attempts'], '1')

    async def test_unmark_returns_to_backlog(self) -> None:
        await self._hot('aaa')
        await self.queue.mark('aaa', state=VideoState.UNAVAILABLE)
        await self.queue.unmark('aaa')
        doc: dict[str, Any] | None = await self._doc('aaa')
        assert doc is not None
        self.assertEqual(doc['state'], 'queued')
        self.assertNotIn('record', doc)
        self.assertEqual(
            await self.queue.get_state('aaa'), VideoState.QUEUED,
        )

    async def test_force_absent_goes_to_front(self) -> None:
        before: int = int(time.time())
        await self._hot('waiting', score=before - 86400)
        outcome: str = await self.queue.force_enqueue(
            'new', source='cli', channel_id='UCx',
        )
        self.assertEqual(outcome, 'added')
        self.assertEqual(
            await self.redis.zrange('youtube:video:queue', 0, -1),
            ['new', 'waiting'],
        )
        score: float | None = await self.redis.zscore(
            'youtube:video:queue', 'new',
        )
        assert score is not None
        self.assertGreaterEqual(score, before - FORCE_PRIORITY_SECONDS)
        doc: dict[str, Any] | None = await self._doc('new')
        assert doc is not None
        self.assertEqual(doc['state'], 'hot')
        self.assertEqual(doc['source'], 'cli')
        self.assertTrue(await self.queue.consume_force('new'))
        meta: dict[str, str] = await self.queue.get_meta('new')
        self.assertEqual(meta['channel_id'], 'UCx')

    async def test_force_revives_terminal_keeping_source(self) -> None:
        await self._hot('aaa')
        await self.queue.mark('aaa', state=VideoState.FAILED)
        outcome: str = await self.queue.force_enqueue(
            'aaa', source='cli',
        )
        self.assertEqual(outcome, 'revived')
        doc: dict[str, Any] | None = await self._doc('aaa')
        assert doc is not None
        self.assertEqual(doc['state'], 'hot')
        self.assertEqual(doc['source'], 'rss')
        self.assertNotIn('record', doc)
        self.assertEqual(
            (await self.queue.get_meta('aaa'))['source'], 'rss',
        )
        self.assertEqual(
            await self.redis.zrange('youtube:video:queue', 0, -1),
            ['aaa'],
        )

    async def test_force_cold_and_waiting_are_pending(self) -> None:
        await self.backlog.add('cold', source='rss')
        self.assertEqual(
            await self.queue.force_enqueue('cold', source='cli'),
            'forced_pending',
        )
        self.assertEqual((await self._doc('cold'))['state'], 'hot')
        await self._hot('waiting', score=10 ** 10)
        self.assertEqual(
            await self.queue.force_enqueue('waiting', source='cli'),
            'forced_pending',
        )
        self.assertLess(
            await self.redis.zscore('youtube:video:queue', 'waiting'),
            10 ** 10,
        )

    async def test_count_by_state(self) -> None:
        await self.backlog.add('a', source='rss')
        await self.coll.insert_one(
            backlog_document('b', state='unavailable', enqueued_at=1),
        )
        self.assertEqual(await self.queue.count_by_state(), {
            VideoState.QUEUED: 1, VideoState.UNAVAILABLE: 1,
            VideoState.FAILED: 0, VideoState.REMOVED: 0,
        })

    async def test_iter_members_and_search_include_backlog(self) -> None:
        await self._hot('hot')
        await self.backlog.add('cold', source='channel')
        await self._hot('dead')
        await self.queue.mark(
            'dead', state=VideoState.FAILED, last_error='boom',
        )
        members: dict[str, dict] = {
            rec['video_id']: rec
            async for rec in self.queue.iter_members()
        }
        self.assertEqual(set(members), {'hot', 'cold', 'dead'})
        self.assertEqual(members['cold']['state'], 'queued')
        self.assertEqual(members['dead']['state'], 'failed')
        self.assertEqual(await self.queue.search_meta('boom'), ['dead'])

    async def test_redis_only_without_backlog(self) -> None:
        plain: RedisVideoScrapeQueue = RedisVideoScrapeQueue(
            self.redis, VideoScrapeQueueSettings(),
        )
        self.assertIsNone(plain.backlog)
        await plain.enqueue('aaa', source='rss')
        self.assertEqual(await self.redis.zcard('youtube:video:queue'), 1)
        self.assertIsNone(await self._doc('aaa'))


class TestFromDsn(unittest.IsolatedAsyncioTestCase):

    async def test_shared_per_dsn_and_platform(self) -> None:
        dsn: str = 'mongodb://localhost:27017/scraper_unit_test'
        first: MongoVideoBacklog = MongoVideoBacklog.from_dsn(dsn)
        self.assertIs(first, MongoVideoBacklog.from_dsn(dsn))
        self.assertIsNot(
            first, MongoVideoBacklog.from_dsn(dsn, platform='tiktok'),
        )
        self.assertEqual(first._coll.name, 'youtube_videos')
        self.assertEqual(first._coll.database.name, 'scraper_unit_test')


if __name__ == '__main__':
    unittest.main()


class TestEnqueueMany(_Base):

    async def test_matches_single_enqueue_documents(self) -> None:
        added: int = await self.queue.enqueue_many(
            ['aaa', 'bbb'], source='channel', channel_id='UCx',
            channel_handle='h',
        )
        self.assertEqual(added, 2)
        await self.queue.enqueue(
            'ccc', source='channel', channel_id='UCx', channel_handle='h',
        )
        many: dict[str, Any] = await self._doc('aaa')
        single: dict[str, Any] = await self._doc('ccc')
        for doc in (many, single):
            doc.pop('_id')
            doc.pop('enqueued_at')
        self.assertEqual(many, single)

    async def test_skips_known_ids_and_counts_only_new(self) -> None:
        await self.backlog.add('aaa', source='rss', enqueued_at=1)
        await self._hot('bbb')
        added: int = await self.queue.enqueue_many(
            ['aaa', 'bbb', 'ccc'], source='channel',
        )
        self.assertEqual(added, 1)
        self.assertEqual((await self._doc('aaa'))['source'], 'rss')
        self.assertEqual((await self._doc('bbb'))['state'], 'hot')
        self.assertEqual((await self._doc('ccc'))['state'], 'queued')

    async def test_inserts_in_chunks(self) -> None:
        calls: list[int] = []
        real_add_many = self.backlog.add_many

        async def _spy(docs: list[dict[str, Any]]) -> int:
            calls.append(len(docs))
            return await real_add_many(docs)

        self.backlog.add_many = _spy
        ids: list[str] = [f'v{i:02d}' for i in range(7)]
        added: int = await self.queue.enqueue_many(
            ids, source='channel', chunk_size=3,
        )
        self.assertEqual(added, 7)
        self.assertEqual(calls, [3, 3, 1])

    async def test_empty_list_is_a_no_op(self) -> None:
        self.assertEqual(
            await self.queue.enqueue_many([], source='channel'), 0,
        )

    async def test_rejects_empty_video_id(self) -> None:
        with self.assertRaises(ValueError):
            await self.queue.enqueue_many(['aaa', ''], source='channel')
