'''
Unit tests that the channel-queue drift scan checks members a page at a
time, in pipelines, instead of one Redis round trip per member.

The full scan walks every channel (millions in production). With one
round trip per member it ran for hours after a repair.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import unittest
from unittest.mock import patch

import fakeredis.aioredis

from scrape_exchange.channel_queue_reconcile import (
    ChannelQueueAuditor,
)
from scrape_exchange.channel_scrape_queue import (
    ChannelScrapeQueueSettings,
    ChannelState,
    RedisChannelScrapeQueue,
)

# Commands the old scan sent once per member, outside a pipeline.
PER_MEMBER_COMMANDS: tuple[str, ...] = (
    'exists', 'hexists', 'hget', 'hgetall', 'zscore',
)


def _cid(index: int) -> str:
    return f'UC{index:022d}'


async def _seed(
    redis: fakeredis.aioredis.FakeRedis,
    queue: RedisChannelScrapeQueue,
) -> None:
    '''A small queue with at least one instance of every drift class
    the batched scans detect, plus clean members around them.'''
    index: int
    for index in range(12):
        cid: str = _cid(index)
        member: str = f'i:{cid}'
        await redis.zadd(queue._k_scheduled(index % 3), {member: 1.0})
        if index != 0:  # zset_missing_meta
            await redis.hset(
                queue._k_meta(member),
                mapping={'state': 'scheduled', 'handle': f'h{index}'},
            )
        if index not in (1, 2):  # missing RSS accounting
            await redis.hset('rss:youtube:creators', cid, f'h{index}')
        if index != 3:  # identity map missing
            await redis.hset('youtube:creator_map', cid, f'h{index}')
        # tier mismatch for every fourth member
        desired: int = (index % 3 + 1) % 3 if index % 4 == 0 else index % 3
        await redis.hset(queue._k_tiers(), cid, str(desired))
    # Terminal members: one without meta, one whose meta disagrees.
    for index, meta_state in ((20, None), (21, 'scheduled'), (22, 'removed')):
        member = f'i:{_cid(index)}'
        await redis.hset(
            queue._k_state(ChannelState.REMOVED), member, '{}',
        )
        if meta_state:
            await redis.hset(
                queue._k_meta(member), mapping={'state': meta_state},
            )
    # Meta orphans: scheduled, pending and terminal meta with no
    # matching queue structure.
    await redis.hset(
        queue._k_meta(f'i:{_cid(30)}'), mapping={'state': 'scheduled'},
    )
    await redis.hset(
        queue._k_meta('h:orphan'),
        mapping={'state': 'pending_resolution'},
    )
    await redis.hset(
        queue._k_meta(f'i:{_cid(31)}'), mapping={'state': 'not_found'},
    )
    await redis.zadd(queue._k_unresolved(), {'h:queued': 0.0})
    await redis.hset(
        queue._k_meta('h:queued'),
        mapping={'state': 'pending_resolution'},
    )


class _AuditorTestCase(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(decode_responses=True)
        )
        self.queue: RedisChannelScrapeQueue = RedisChannelScrapeQueue(
            self.redis, ChannelScrapeQueueSettings(),
        )
        await _seed(self.redis, self.queue)

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    async def _scan(
        self, batch_size: int = 1000, limit: int | None = None,
    ) -> dict:
        auditor: ChannelQueueAuditor = ChannelQueueAuditor(
            self.queue, self.redis,
            batch_size=batch_size, sample_size=100, limit=limit,
        )
        return (await auditor.scan()).to_dict()


class TestScanUsesPipelines(_AuditorTestCase):

    async def test_no_per_member_round_trips(self) -> None:
        direct: dict[str, int] = {}
        originals: dict[str, object] = {}
        name: str
        for name in PER_MEMBER_COMMANDS:
            original = getattr(self.redis, name)
            originals[name] = original

            def counting(*args, _name=name, _orig=original, **kwargs):
                direct[_name] = direct.get(_name, 0) + 1
                return _orig(*args, **kwargs)

            setattr(self.redis, name, counting)
        try:
            report: dict = await self._scan(batch_size=4)
        finally:
            for name, original in originals.items():
                setattr(self.redis, name, original)
        self.assertEqual(direct, {})
        self.assertGreater(
            sum(d['count'] for d in report['drift'].values()), 0,
        )


class TestScanResults(_AuditorTestCase):

    async def test_every_batched_class_finds_its_drift(self) -> None:
        drift: dict = (await self._scan())['drift']
        expected: dict[str, int] = {
            'zset_missing_meta': 1,
            'channel_state_missing_rss_accounting': 5,
            'identity_map_missing_for_channel_state': 4,
            'scheduled_tier_mismatch': 3,
            'terminal_hash_missing_meta': 1,
            'terminal_hash_meta_disagrees': 1,
            # The orphan, plus the removed member whose meta still
            # says scheduled.
            'meta_scheduled_missing_zset': 2,
            'meta_pending_missing_unresolved': 1,
            'meta_terminal_missing_hash': 1,
        }
        kind: str
        count: int
        for kind, count in expected.items():
            self.assertEqual(drift[kind]['count'], count, kind)

    async def test_results_do_not_depend_on_batch_size(self) -> None:
        reference: dict = await self._scan(batch_size=1000)
        batch_size: int
        for batch_size in (1, 2, 5):
            self.assertEqual(
                await self._scan(batch_size=batch_size), reference,
                batch_size,
            )

    async def test_limit_does_not_depend_on_batch_size(self) -> None:
        limit: int
        for limit in (1, 4, 9):
            reference: dict = await self._scan(
                batch_size=1000, limit=limit,
            )
            self.assertEqual(
                await self._scan(batch_size=3, limit=limit),
                reference, limit,
            )

    async def test_tier_scan_stops_at_limit(self) -> None:
        with patch.object(
            ChannelQueueAuditor, '_record_tier_mismatches',
            autospec=True,
        ) as record:
            auditor: ChannelQueueAuditor = ChannelQueueAuditor(
                self.queue, self.redis, batch_size=1000, limit=2,
            )
            await auditor._scan_tier_placement()
        checked: int = sum(
            len(call.args[3]) for call in record.call_args_list
        )
        self.assertEqual(checked, 2)


if __name__ == '__main__':
    unittest.main()
