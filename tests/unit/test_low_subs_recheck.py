'''
Re-check of channels parked in the ``low_subs`` state: selecting the
channels that are due and deciding what happens to each.
'''

import json
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import fakeredis.aioredis

from scrape_exchange.channel_scrape_queue import (
    ChannelScrapeQueueSettings,
    ChannelState,
    RedisChannelScrapeQueue,
)
from scrape_exchange.youtube.low_subs_recheck import (
    LOW_SUBS_RELEASED_KEY,
    BrowseResult,
    LowSubsEntry,
    RecheckAction,
    RecheckSummary,
    decide,
    effective_count,
    parse_low_subs_entries,
    recheck_low_subs_channels,
    select_due,
)
from scrape_exchange.youtube.youtube_channel import YouTubeChannel

NOW: float = 1_000_000.0
DAY: float = 86400.0


def _settings() -> MagicMock:
    settings: MagicMock = MagicMock()
    settings.channel_min_subscribers = 100
    settings.channel_privacy_subscribers = 4000
    settings.channel_views_per_subscriber = 100
    return settings


def _entry(
    channel_id: str,
    subscriber_count: int | None,
    checked_at: float | None,
) -> LowSubsEntry:
    return LowSubsEntry(
        member=f'i:{channel_id}',
        channel_id=channel_id,
        subscriber_count=subscriber_count,
        view_count=None,
        checked_at=checked_at,
        record={},
    )


class TestSelection(unittest.TestCase):

    def test_parse_skips_handle_members(self) -> None:
        entries: list[LowSubsEntry] = parse_low_subs_entries([
            ('h:@x', '{}'),
            ('i:UCa', '{"subscriber_count": "50"}'),
        ])
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].channel_id, 'UCa')
        self.assertEqual(entries[0].member, 'i:UCa')
        self.assertEqual(entries[0].subscriber_count, 50)

    def test_parse_bad_records(self) -> None:
        raw: str | None
        for raw in (
            'not json', None, '[]', '{"checked_at": "soon"}',
        ):
            entries: list[LowSubsEntry] = parse_low_subs_entries(
                [('i:UCb', raw)],
            )
            self.assertEqual(len(entries), 1, raw)
            self.assertIsNone(entries[0].subscriber_count, raw)
            self.assertIsNone(entries[0].checked_at, raw)

    def test_parse_reads_views_and_checked_at(self) -> None:
        entries: list[LowSubsEntry] = parse_low_subs_entries([(
            'i:UCc',
            '{"view_count": "1200", "checked_at": 900000}',
        )])
        self.assertEqual(entries[0].view_count, 1200)
        self.assertEqual(entries[0].checked_at, 900000.0)

    def test_select_skips_recent(self) -> None:
        recent: LowSubsEntry = _entry('UCa', 10, NOW - 10 * DAY)
        old: LowSubsEntry = _entry('UCb', 10, NOW - 31 * DAY)
        due: list[LowSubsEntry]
        skipped: int
        due, skipped = select_due(
            [recent, old], now=NOW, recheck_after_days=30, limit=None,
        )
        self.assertEqual(due, [old])
        self.assertEqual(skipped, 1)

    def test_select_order(self) -> None:
        checked: LowSubsEntry = _entry('UCa', 90, NOW - 40 * DAY)
        n20: LowSubsEntry = _entry('UCb', 20, None)
        unknown: LowSubsEntry = _entry('UCc', None, None)
        n80: LowSubsEntry = _entry('UCd', 80, None)
        due: list[LowSubsEntry]
        due, _ = select_due(
            [checked, n20, unknown, n80],
            now=NOW, recheck_after_days=30, limit=None,
        )
        self.assertEqual(due, [n80, n20, unknown, checked])

    def test_select_limit(self) -> None:
        n20: LowSubsEntry = _entry('UCb', 20, None)
        n80: LowSubsEntry = _entry('UCd', 80, None)
        n50: LowSubsEntry = _entry('UCe', 50, None)
        due: list[LowSubsEntry]
        due, _ = select_due(
            [n20, n80, n50], now=NOW, recheck_after_days=30, limit=2,
        )
        self.assertEqual(due, [n80, n50])


class TestDecide(unittest.TestCase):

    def test_effective_count(self) -> None:
        self.assertEqual(effective_count(None, 500), 500)
        self.assertEqual(effective_count(60, 500), 60)
        self.assertIsNone(effective_count(None, None))

    def test_decide(self) -> None:
        cases: list[tuple[int | None, int | None, bool, RecheckAction]] = [
            (150, None, False, RecheckAction.REQUEUE),
            (99, None, False, RecheckAction.STILL_TINY),
            (None, 10_000, False, RecheckAction.REQUEUE),
            (None, 9_999, False, RecheckAction.STILL_TINY),
            (None, None, False, RecheckAction.STILL_TINY),
            (5, None, True, RecheckAction.TOPIC),
        ]
        subs: int | None
        views: int | None
        topic: bool
        expected: RecheckAction
        for subs, views, topic, expected in cases:
            self.assertEqual(
                decide(subs, views, topic, _settings()), expected,
                (subs, views, topic),
            )


if __name__ == '__main__':
    unittest.main()


class TestRecheckChannels(unittest.IsolatedAsyncioTestCase):
    '''The channel pass against a fakeredis-backed channel queue.'''

    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(decode_responses=True)
        )
        self.queue: RedisChannelScrapeQueue = RedisChannelScrapeQueue(
            self.redis, ChannelScrapeQueueSettings(),
        )
        self.fresh: dict[str, int | None] = {'subs': None, 'views': None}
        self.browse: AsyncMock = AsyncMock(
            return_value=BrowseResult(data={}, is_topic=False),
        )
        subs_patch = patch.object(
            YouTubeChannel, 'parse_subscriber_count_with_outcome',
            side_effect=lambda data: (self.fresh['subs'], 'found'),
        )
        views_patch = patch.object(
            YouTubeChannel, 'parse_view_count',
            side_effect=lambda data: self.fresh['views'],
        )
        subs_patch.start()
        views_patch.start()
        self.addCleanup(subs_patch.stop)
        self.addCleanup(views_patch.stop)

    async def _park(
        self, channel_id: str, subs: str = '50', **record: object,
    ) -> str:
        member: str = f'i:{channel_id}'
        await self.queue.mark(
            member, state=ChannelState.LOW_SUBS,
            extra={'subscriber_count': subs, **record},
        )
        return member

    async def _run(self, apply: bool = True) -> RecheckSummary:
        return await recheck_low_subs_channels(
            self.queue, _settings(), self.browse,
            apply=apply, now=NOW, recheck_after_days=30,
            limit=None, concurrency=4,
        )

    async def _record(self, member: str) -> dict:
        raw: str | None = await self.redis.hget(
            self.queue._k_state(ChannelState.LOW_SUBS), member,
        )
        return json.loads(raw)

    async def _snapshot(self) -> dict[str, object]:
        snapshot: dict[str, object] = {}
        key: str
        for key in await self.redis.keys('*'):
            kind: str = await self.redis.type(key)
            if kind == 'hash':
                snapshot[key] = await self.redis.hgetall(key)
            elif kind == 'zset':
                snapshot[key] = await self.redis.zrange(
                    key, 0, -1, withscores=True,
                )
            elif kind == 'set':
                snapshot[key] = await self.redis.smembers(key)
            else:
                snapshot[key] = (kind, await self.redis.dump(key))
        return snapshot

    async def test_grown_channel_requeued(self) -> None:
        member: str = await self._park('UCa')
        self.fresh['subs'] = 150
        summary: RecheckSummary = await self._run()
        self.assertEqual(summary.requeued, 1)
        self.assertEqual(summary.checked, 1)
        self.assertNotEqual(
            await self.queue.get_state(member), ChannelState.LOW_SUBS,
        )
        meta: dict[str, str] = await self.queue.get_meta(member)
        self.assertEqual(meta['subscriber_count'], '150')
        self.assertTrue(
            await self.redis.sismember(LOW_SUBS_RELEASED_KEY, 'UCa'),
        )

    async def test_still_tiny_gets_checked_at(self) -> None:
        member: str = await self._park('UCa')
        self.fresh['subs'] = 60
        summary: RecheckSummary = await self._run()
        self.assertEqual(summary.still_tiny, 1)
        self.assertEqual(
            await self.queue.get_state(member), ChannelState.LOW_SUBS,
        )
        record: dict = await self._record(member)
        self.assertEqual(record['checked_at'], int(NOW))
        self.assertEqual(record['subscriber_count'], '60')
        self.assertFalse(
            await self.redis.sismember(LOW_SUBS_RELEASED_KEY, 'UCa'),
        )

    async def test_missing_fresh_count_uses_stored(self) -> None:
        member: str = await self._park('UCa')
        await self.queue.set_meta(member, subscriber_count='500')
        summary: RecheckSummary = await self._run()
        self.assertEqual(summary.requeued, 1)
        meta: dict[str, str] = await self.queue.get_meta(member)
        self.assertEqual(meta['subscriber_count'], '500')

    async def test_recent_skipped_no_browse(self) -> None:
        await self._park('UCa', checked_at=int(NOW - DAY))
        summary: RecheckSummary = await self._run()
        self.browse.assert_not_awaited()
        self.assertEqual(summary.skipped_recent, 1)
        self.assertEqual(summary.checked, 0)

    async def test_browse_failure(self) -> None:
        member: str = await self._park('UCa')
        self.browse.side_effect = RuntimeError('blocked')
        summary: RecheckSummary = await self._run()
        self.assertEqual(summary.failed, 1)
        self.assertEqual(summary.checked, 1)
        self.assertEqual(
            await self.queue.get_state(member), ChannelState.LOW_SUBS,
        )
        record: dict = await self._record(member)
        self.assertEqual(record['checked_at'], int(NOW))

    async def test_topic_requeued_to_topic_tier(self) -> None:
        member: str = await self._park('UCa', subs='5')
        self.browse.return_value = BrowseResult(data={}, is_topic=True)
        summary: RecheckSummary = await self._run()
        self.assertEqual(summary.topic, 1)
        meta: dict[str, str] = await self.queue.get_meta(member)
        self.assertEqual(meta['topic'], '1')
        self.assertNotEqual(
            await self.queue.get_state(member), ChannelState.LOW_SUBS,
        )
        self.assertTrue(
            await self.redis.sismember(LOW_SUBS_RELEASED_KEY, 'UCa'),
        )

    async def test_dry_run_writes_nothing(self) -> None:
        await self._park('UCa')
        self.fresh['subs'] = 150
        before: dict[str, object] = await self._snapshot()
        summary: RecheckSummary = await self._run(apply=False)
        self.assertEqual(summary.requeued, 1)
        self.assertEqual(await self._snapshot(), before)

    def test_summary_line(self) -> None:
        summary: RecheckSummary = RecheckSummary(
            checked=2, requeued=1, still_tiny=1,
        )
        self.assertEqual(
            summary.line(apply=False),
            'recheck-low-subs: checked=2 requeued=1 topic=0 '
            'still_tiny=1 failed=0 skipped_recent=0 dry_run=true',
        )
