'''
Unit tests for scrape_exchange.queue_admin — the agnostic
OperatorQueue interface, the (platform, entity) registry, and the
TikTokCreatorQueueAdapter (wrapping a fakeredis-backed
RedisCreatorQueue).
'''

import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

try:
    import fakeredis.aioredis
    _HAVE_FAKEREDIS: bool = True
except ImportError:
    _HAVE_FAKEREDIS = False

from scrape_exchange.creator_queue import (
    RedisCreatorQueue,
    TierConfig,
)
from scrape_exchange import queue_admin
from scrape_exchange.video_scrape_queue import (
    RedisVideoScrapeQueue,
    VideoScrapeQueueSettings,
    VideoState,
)


_TIERS: list[TierConfig] = [
    TierConfig(tier=1, min_subscribers=1_000_000, interval_hours=6),
    TierConfig(tier=2, min_subscribers=100_000, interval_hours=24),
    TierConfig(tier=3, min_subscribers=10_000, interval_hours=72),
    TierConfig(tier=4, min_subscribers=0, interval_hours=168),
]


def _settings() -> MagicMock:
    s: MagicMock = MagicMock()
    s.redis_dsn = 'redis://fake'
    s.worker_id = 'w1'
    s.creator_priority_queues = (
        '6:1000000,24:100000,72:10000,168:0'
    )
    return s


@unittest.skipUnless(_HAVE_FAKEREDIS, 'fakeredis not installed')
class TestTikTokCreatorAdapter(unittest.IsolatedAsyncioTestCase):

    async def _adapter(
        self,
    ) -> queue_admin.TikTokCreatorQueueAdapter:
        redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
        q: RedisCreatorQueue = RedisCreatorQueue(
            redis_dsn='redis://fake', worker_id='w1',
            platform='tiktok', key_namespace='scrape',
        )
        q._redis = redis
        q._tiers = _TIERS
        q._key_queues = q._build_queue_keys(_TIERS)
        return queue_admin.TikTokCreatorQueueAdapter(q, _TIERS)

    async def test_metadata(self) -> None:
        a = await self._adapter()
        self.assertEqual(a.platform, 'tiktok')
        self.assertEqual(a.entity, 'creator')
        self.assertEqual(a.member_label, 'username')
        self.assertEqual(
            a.states(), ['queued', 'claimed', 'removed'],
        )

    async def test_add_then_count_and_show(self) -> None:
        a = await self._adapter()
        added: int = await a.add([('alice', 5_000_000)])
        self.assertEqual(added, 1)
        rec = await a.show('alice')
        self.assertEqual(rec['state'], 'queued')
        self.assertEqual(rec['tier'], 1)
        counts = await a.count_by_state()
        self.assertEqual(counts['queued'], 1)

    async def test_remove_sets_removed(self) -> None:
        a = await self._adapter()
        await a.add([('bob', 50_000)])
        ok: bool = await a.remove('bob')
        self.assertTrue(ok)
        rec = await a.show('bob')
        self.assertEqual(rec['state'], 'removed')

    async def test_rescrape_counts_rescheduled(self) -> None:
        a = await self._adapter()
        await a.add([('carol', 50_000)])
        n: int = await a.rescrape(['carol'])
        self.assertEqual(n, 1)

    async def test_search(self) -> None:
        a = await self._adapter()
        await a.add([('charli', 50_000)])
        hits = await a.search('char', 10)
        self.assertEqual(
            [h['creator_id'] for h in hits], ['charli'],
        )

    async def test_import_members_uses_next_to_lowest_tier(
        self,
    ) -> None:
        a = await self._adapter()
        with tempfile.TemporaryDirectory() as d:
            path: str = os.path.join(d, 'list.txt')
            with open(path, 'w') as fd:
                fd.write('# header\n\nalice\nbob\n')
            report = await a.import_members(path)
        self.assertEqual(report.total_lines, 4)
        self.assertEqual(report.added, 2)
        self.assertEqual(report.comments, 1)
        self.assertEqual(report.blank, 1)
        # next-to-lowest tier is _TIERS[-2] => tier 3
        rec = await a.show('alice')
        self.assertEqual(rec['tier'], 3)

    async def test_export_yields_all_states(self) -> None:
        a = await self._adapter()
        await a.add([('alice', 5_000_000)])
        await a.add([('bob', 50_000)])
        await a.remove('bob')
        recs: list[dict] = [r async for r in a.export()]
        self.assertEqual(len(recs), 2)
        by_id: dict[str, dict] = {
            r['creator_id']: r for r in recs
        }
        self.assertEqual(by_id['alice']['state'], 'queued')
        self.assertEqual(by_id['bob']['state'], 'removed')

    async def test_import_members_normalizes_handles(
        self,
    ) -> None:
        a = await self._adapter()
        with tempfile.TemporaryDirectory() as d:
            path: str = os.path.join(d, 'list.txt')
            with open(path, 'w') as fd:
                fd.write(
                    '@alice\n'
                    'https://www.tiktok.com/@bob?x=y\n'
                    'bad@handle\n'
                )
            report = await a.import_members(path)
        self.assertEqual(report.added, 2)
        self.assertEqual(report.invalid, 1)
        self.assertIsNotNone(await a.show('alice'))
        bob = await a.show('bob')
        self.assertEqual(bob['tier'], 3)
        self.assertIsNone(await a.show('bad@handle'))

    async def test_import_members_accepts_discovery_jsonl(
        self,
    ) -> None:
        a = await self._adapter()
        with tempfile.TemporaryDirectory() as d:
            path: str = os.path.join(d, 'discovered.jsonl')
            with open(path, 'w') as fd:
                fd.write(
                    '{"platform":"tiktok","username":"@alice"}\n'
                    '{"username":"https://www.tiktok.com/@bob?x=y"}\n'
                    '{"creator_id":"carol"}\n'
                    '{"handle":"alice"}\n'
                    '{"category_url":"https://www.tiktok.com/explore"}\n'
                    '{bad json}\n'
                )
            report = await a.import_members(path)
        self.assertEqual(report.total_lines, 6)
        self.assertEqual(report.added, 3)
        self.assertEqual(report.duplicates, 1)
        self.assertEqual(report.invalid, 2)
        self.assertIsNotNone(await a.show('alice'))
        self.assertIsNotNone(await a.show('bob'))
        self.assertIsNotNone(await a.show('carol'))

    async def test_import_members_accepts_discovered_punctuation_handles(
        self,
    ) -> None:
        a = await self._adapter()
        with tempfile.TemporaryDirectory() as d:
            path: str = os.path.join(d, 'discovered.jsonl')
            with open(path, 'w') as fd:
                fd.write(
                    '{"username":"_ichgoninaaa_"}\n'
                    '{"username":".memescreens"}\n'
                    '{"username":"..k_95"}\n'
                    '{"username":"_.crush3m._"}\n'
                )
            report = await a.import_members(path)
        self.assertEqual(report.total_lines, 4)
        self.assertEqual(report.added, 4)
        self.assertEqual(report.invalid, 0)
        self.assertIsNotNone(await a.show('_ichgoninaaa_'))
        self.assertIsNotNone(await a.show('.memescreens'))
        self.assertIsNotNone(await a.show('..k_95'))
        self.assertIsNotNone(await a.show('_.crush3m._'))

    async def test_import_members_accepts_short_url(self) -> None:
        a = await self._adapter()
        with tempfile.TemporaryDirectory() as d:
            path: str = os.path.join(d, 'list.txt')
            with open(path, 'w') as fd:
                fd.write('https://vm.tiktok.com/ZGJEytV2E/?x=y\n')
            report = await a.import_members(path)
        self.assertEqual(report.added, 1)
        rec = await a.show('https://vm.tiktok.com/ZGJEytV2E')
        self.assertIsNotNone(rec)
        self.assertEqual(rec['state'], 'queued')

    async def test_import_members_reports_duplicates(self) -> None:
        a = await self._adapter()
        await a.add([('existing', 50_000)])
        with tempfile.TemporaryDirectory() as d:
            path: str = os.path.join(d, 'list.txt')
            with open(path, 'w') as fd:
                fd.write(
                    'existing\n'
                    '@alice\n'
                    'https://www.tiktok.com/@alice\n'
                )
            report = await a.import_members(path)
        self.assertEqual(report.total_lines, 3)
        self.assertEqual(report.added, 1)
        self.assertEqual(report.duplicates, 2)


@unittest.skipUnless(_HAVE_FAKEREDIS, 'fakeredis not installed')
class TestTikTokVideoAdapter(unittest.IsolatedAsyncioTestCase):

    async def _adapter(self) -> queue_admin.TikTokVideoQueueAdapter:
        redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
        queue: RedisVideoScrapeQueue = RedisVideoScrapeQueue(
            redis,
            VideoScrapeQueueSettings(),
            platform='tiktok',
        )
        return queue_admin.TikTokVideoQueueAdapter(queue)

    async def test_add_normalizes_urls(self) -> None:
        adapter = await self._adapter()
        added: int = await adapter.add([
            (
                'https://www.tiktok.com/@author/video/'
                '7000000000000000001?x=y',
                0,
            ),
        ])
        self.assertEqual(added, 1)
        rec = await adapter.show('7000000000000000001')
        self.assertEqual(rec['video_id'], '7000000000000000001')
        self.assertEqual(rec['state'], 'queued')

    async def test_import_members_accepts_jsonl_video_id(
        self,
    ) -> None:
        adapter = await self._adapter()
        with tempfile.TemporaryDirectory() as d:
            path: str = os.path.join(d, 'videos.jsonl')
            with open(path, 'w') as fd:
                fd.write(
                    '{"video_id":"7000000000000000001"}\n'
                    '{"video_id":"https://www.tiktok.com/@a/video/'
                    '7000000000000000002"}\n'
                    '{"nope":"x"}\n'
                    '{bad json}\n'
                    'not-a-video\n'
                )
            report = await adapter.import_members(path)
        self.assertEqual(report.total_lines, 5)
        self.assertEqual(report.added, 2)
        self.assertEqual(report.invalid, 3)
        counts = await adapter.count_by_state()
        self.assertEqual(counts[VideoState.QUEUED.value], 2)

    async def test_import_members_reports_duplicates(self) -> None:
        adapter = await self._adapter()
        await adapter.add([('7000000000000000001', 0)])
        with tempfile.TemporaryDirectory() as d:
            path: str = os.path.join(d, 'videos.txt')
            with open(path, 'w') as fd:
                fd.write(
                    '7000000000000000001\n'
                    '7000000000000000002\n'
                    '7000000000000000002\n'
                )
            report = await adapter.import_members(path)
        self.assertEqual(report.total_lines, 3)
        self.assertEqual(report.added, 1)
        self.assertEqual(report.duplicates, 2)

    async def test_rescrape_force_enqueues(self) -> None:
        adapter = await self._adapter()
        count: int = await adapter.rescrape(
            ['7000000000000000001'],
        )
        self.assertEqual(count, 1)
        rec = await adapter.show('7000000000000000001')
        self.assertEqual(rec['force'], '1')

    async def test_export_yields_members(self) -> None:
        adapter = await self._adapter()
        await adapter.add([('7000000000000000001', 0)])
        recs: list[dict] = [r async for r in adapter.export()]
        self.assertTrue(
            any(
                r.get('video_id') == '7000000000000000001'
                for r in recs
            ),
        )
        matching: list[dict] = [
            r for r in recs
            if r.get('video_id') == '7000000000000000001'
        ]
        self.assertEqual(matching[0]['state'], 'queued')


class TestRegistry(unittest.TestCase):

    def test_unknown_pair_raises(self) -> None:
        with self.assertRaises(ValueError):
            queue_admin.get_adapter(
                'myspace', 'creator', _settings(),
            )

    def test_known_pair_builds_adapter(self) -> None:
        with patch.object(
            queue_admin, 'RedisCreatorQueue',
        ) as rq:
            adapter = queue_admin.get_adapter(
                'tiktok', 'creator', _settings(),
            )
        self.assertIsInstance(
            adapter, queue_admin.TikTokCreatorQueueAdapter,
        )
        rq.assert_called_once()

    def test_tiktok_video_pair_builds_adapter(self) -> None:
        fake_queue: MagicMock = MagicMock()
        with patch.object(
            queue_admin, 'redis_from_url',
            return_value=MagicMock(),
        ), patch.object(
            queue_admin, 'RedisVideoScrapeQueue',
            return_value=fake_queue,
        ) as rq:
            adapter = queue_admin.get_adapter(
                'tiktok', 'video', _settings(),
            )
        self.assertIsInstance(
            adapter, queue_admin.TikTokVideoQueueAdapter,
        )
        rq.assert_called_once()


if __name__ == '__main__':
    unittest.main()


class TestNormalizeTikTokCreatorHandle(unittest.TestCase):

    def test_bare_handle(self) -> None:
        self.assertEqual(
            queue_admin.normalize_tiktok_creator_handle('charlidamelio'),
            'charlidamelio',
        )

    def test_handle_with_at_prefix(self) -> None:
        self.assertEqual(
            queue_admin.normalize_tiktok_creator_handle('@charlidamelio'),
            'charlidamelio',
        )

    def test_full_url_with_www(self) -> None:
        self.assertEqual(
            queue_admin.normalize_tiktok_creator_handle(
                'https://www.tiktok.com/@charlidamelio?x=y',
            ),
            'charlidamelio',
        )

    def test_full_url_without_www(self) -> None:
        self.assertEqual(
            queue_admin.normalize_tiktok_creator_handle(
                'https://tiktok.com/@charlidamelio',
            ),
            'charlidamelio',
        )

    def test_full_url_with_query_params(self) -> None:
        self.assertEqual(
            queue_admin.normalize_tiktok_creator_handle(
                'https://www.tiktok.com/@charlidamelio?feature=share',
            ),
            'charlidamelio',
        )

    def test_url_with_trailing_slash(self) -> None:
        self.assertEqual(
            queue_admin.normalize_tiktok_creator_handle(
                'https://www.tiktok.com/@charlidamelio/?x=y',
            ),
            'charlidamelio',
        )

    def test_handle_with_underscore(self) -> None:
        self.assertEqual(
            queue_admin.normalize_tiktok_creator_handle('test_user'),
            'test_user',
        )

    def test_handle_with_hyphen(self) -> None:
        self.assertEqual(
            queue_admin.normalize_tiktok_creator_handle('test-user'),
            'test-user',
        )

    def test_handle_with_period(self) -> None:
        self.assertEqual(
            queue_admin.normalize_tiktok_creator_handle('test.user'),
            'test.user',
        )

    def test_handle_with_leading_underscore(self) -> None:
        self.assertEqual(
            queue_admin.normalize_tiktok_creator_handle('_ichgoninaaa_'),
            '_ichgoninaaa_',
        )

    def test_handle_with_leading_period(self) -> None:
        self.assertEqual(
            queue_admin.normalize_tiktok_creator_handle('.memescreens'),
            '.memescreens',
        )

    def test_handle_with_double_leading_period(self) -> None:
        self.assertEqual(
            queue_admin.normalize_tiktok_creator_handle('..k_95'),
            '..k_95',
        )

    def test_empty_string(self) -> None:
        self.assertIsNone(
            queue_admin.normalize_tiktok_creator_handle(''),
        )

    def test_only_at(self) -> None:
        self.assertIsNone(
            queue_admin.normalize_tiktok_creator_handle('@'),
        )

    def test_whitespace_only(self) -> None:
        self.assertIsNone(
            queue_admin.normalize_tiktok_creator_handle('   '),
        )

    def test_handle_with_space(self) -> None:
        self.assertIsNone(
            queue_admin.normalize_tiktok_creator_handle('bad handle'),
        )

    def test_non_tiktok_url(self) -> None:
        self.assertIsNone(
            queue_admin.normalize_tiktok_creator_handle(
                'https://youtube.com/@charlidamelio',
            ),
        )


class TestNormalizeTikTokCreatorSubmission(unittest.TestCase):

    def test_handle_passes_through(self) -> None:
        self.assertEqual(
            queue_admin.normalize_tiktok_creator_submission('@alice'),
            'alice',
        )

    def test_short_url_canonicalised(self) -> None:
        self.assertEqual(
            queue_admin.normalize_tiktok_creator_submission(
                'https://vm.tiktok.com/ZGJEytV2E/?x=y',
            ),
            'https://vm.tiktok.com/ZGJEytV2E',
        )

    def test_garbage_returns_none(self) -> None:
        self.assertIsNone(
            queue_admin.normalize_tiktok_creator_submission('bad handle'),
        )
