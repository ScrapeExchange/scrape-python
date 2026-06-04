'''Unit tests for scrape_exchange.channel_queue_reconcile.'''

import unittest

import fakeredis.aioredis

from scrape_exchange.channel_queue_reconcile import (
    ChannelQueueAuditor,
    ChannelQueueRepairer,
    DriftClassReport,
    DriftSample,
    RepairOptions,
    ReconcileReport,
)
from scrape_exchange.channel_scrape_queue import (
    ChannelState,
    ChannelScrapeQueueSettings,
    RedisChannelScrapeQueue,
)


class TestDriftDataclasses(unittest.TestCase):

    def test_drift_sample_holds_member_and_context(
        self,
    ) -> None:
        s: DriftSample = DriftSample(
            member='i:UCabcdefghijklmnopqrstuv',
            context={'rss_label': 'veritasium'},
        )
        self.assertEqual(
            s.member, 'i:UCabcdefghijklmnopqrstuv',
        )
        self.assertEqual(
            s.context['rss_label'], 'veritasium',
        )

    def test_drift_class_report_defaults(self) -> None:
        r: DriftClassReport = DriftClassReport(
            kind='rss_creator_missing_channel_state',
        )
        self.assertEqual(r.count, 0)
        self.assertEqual(r.samples, [])

    def test_drift_class_report_to_dict(self) -> None:
        r: DriftClassReport = DriftClassReport(
            kind='rss_creator_missing_channel_state',
            count=2,
            samples=[
                DriftSample(
                    member='i:UCabc',
                    context={'rss_label': 'a'},
                ),
                DriftSample(member='i:UCdef'),
            ],
        )
        d: dict[str, object] = r.to_dict()
        self.assertEqual(
            d['kind'],
            'rss_creator_missing_channel_state',
        )
        self.assertEqual(d['count'], 2)
        self.assertEqual(
            d['samples'], ['i:UCabc', 'i:UCdef'],
        )
        self.assertEqual(
            d['sample_context'],
            [{'rss_label': 'a'}, {}],
        )

    def test_reconcile_report_defaults(self) -> None:
        r: ReconcileReport = ReconcileReport()
        self.assertEqual(r.inspected, {})
        self.assertEqual(r.drift, {})
        self.assertTrue(r.dry_run)

    def test_reconcile_report_to_dict_matches_spec(
        self,
    ) -> None:
        r: ReconcileReport = ReconcileReport(
            inspected={'rss_creators': 100},
            drift={
                'rss_creator_missing_channel_state':
                    DriftClassReport(
                        kind=(
                            'rss_creator_missing_channel_state'
                        ),
                        count=3,
                        samples=[
                            DriftSample(member='i:UC1'),
                            DriftSample(member='i:UC2'),
                        ],
                    ),
            },
        )
        d: dict[str, object] = r.to_dict()
        self.assertEqual(
            d['inspected']['rss_creators'], 100,
        )
        self.assertEqual(
            d['drift']
             ['rss_creator_missing_channel_state']
             ['count'],
            3,
        )
        self.assertEqual(
            d['drift']
             ['rss_creator_missing_channel_state']
             ['samples'],
            ['i:UC1', 'i:UC2'],
        )
        self.assertTrue(d['dry_run'])


class TestAuditorInspectedCardinalities(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(
                decode_responses=True,
            )
        )
        self.queue: RedisChannelScrapeQueue = (
            RedisChannelScrapeQueue(
                self.redis,
                ChannelScrapeQueueSettings(),
            )
        )
        self.auditor: ChannelQueueAuditor = (
            ChannelQueueAuditor(
                self.queue, self.redis,
            )
        )

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    async def test_empty_redis_returns_zero_cardinalities(
        self,
    ) -> None:
        report: ReconcileReport = (
            await self.auditor.scan()
        )
        self.assertEqual(
            report.inspected['rss_creators'], 0,
        )
        self.assertEqual(
            report.inspected['rss_suppressed'], 0,
        )
        self.assertEqual(
            report.inspected['channel_meta'], 0,
        )
        self.assertEqual(
            report.inspected['scheduled_members'], 0,
        )
        self.assertEqual(
            report.inspected['unresolved_members'], 0,
        )

    async def test_cardinalities_match_seeded_keys(
        self,
    ) -> None:
        await self.redis.hset(
            'rss:youtube:creators',
            mapping={
                'UCaaaaaaaaaaaaaaaaaaaaaa': 'a',
                'UCbbbbbbbbbbbbbbbbbbbbbb': 'b',
            },
        )
        await self.redis.hset(
            'rss:youtube:suppressed',
            'UCcccccccccccccccccccccc',
            '{"reason":"x"}',
        )
        report: ReconcileReport = (
            await self.auditor.scan()
        )
        self.assertEqual(
            report.inspected['rss_creators'], 2,
        )
        self.assertEqual(
            report.inspected['rss_suppressed'], 1,
        )


class TestRssMissingScan(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self.redis = fakeredis.aioredis.FakeRedis(
            decode_responses=True,
        )
        self.queue: RedisChannelScrapeQueue = (
            RedisChannelScrapeQueue(
                self.redis,
                ChannelScrapeQueueSettings(),
            )
        )
        self.auditor: ChannelQueueAuditor = (
            ChannelQueueAuditor(
                self.queue,
                self.redis,
                sample_size=3,
            )
        )

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    async def test_rss_creator_in_scheduled_is_not_drift(
        self,
    ) -> None:
        await self.redis.hset(
            'rss:youtube:creators',
            'UCaaaaaaaaaaaaaaaaaaaaaa', 'a',
        )
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:0',
            {'i:UCaaaaaaaaaaaaaaaaaaaaaa': 0.0},
        )
        report: ReconcileReport = (
            await self.auditor.scan()
        )
        self.assertEqual(
            report.drift.get(
                'rss_creator_missing_channel_state',
                DriftClassReport(
                    kind=(
                        'rss_creator_missing_channel_state'
                    ),
                ),
            ).count,
            0,
        )

    async def test_rss_creator_absent_from_channel_side_is_drift(
        self,
    ) -> None:
        await self.redis.hset(
            'rss:youtube:creators',
            'UCxxxxxxxxxxxxxxxxxxxxxx', 'x',
        )
        report: ReconcileReport = (
            await self.auditor.scan()
        )
        drift: DriftClassReport = report.drift[
            'rss_creator_missing_channel_state'
        ]
        self.assertEqual(drift.count, 1)
        self.assertEqual(
            drift.samples[0].member,
            'i:UCxxxxxxxxxxxxxxxxxxxxxx',
        )

    async def test_rss_creator_in_terminal_state_is_not_drift(
        self,
    ) -> None:
        await self.redis.hset(
            'rss:youtube:creators',
            'UCyyyyyyyyyyyyyyyyyyyyyy', 'y',
        )
        await self.redis.hset(
            'youtube:channel:not_found',
            'i:UCyyyyyyyyyyyyyyyyyyyyyy', '{}',
        )
        report: ReconcileReport = (
            await self.auditor.scan()
        )
        drift: DriftClassReport = report.drift.get(
            'rss_creator_missing_channel_state',
            DriftClassReport(
                kind='rss_creator_missing_channel_state',
            ),
        )
        self.assertEqual(drift.count, 0)

    async def test_malformed_channel_id_skipped(
        self,
    ) -> None:
        await self.redis.hset(
            'rss:youtube:creators',
            'not-a-channel-id', 'bad',
        )
        report: ReconcileReport = (
            await self.auditor.scan()
        )
        drift: DriftClassReport = report.drift.get(
            'rss_creator_missing_channel_state',
            DriftClassReport(
                kind='rss_creator_missing_channel_state',
            ),
        )
        self.assertEqual(drift.count, 0)

    async def test_samples_capped_at_sample_size(
        self,
    ) -> None:
        for i in range(5):
            cid: str = f'UC{chr(ord("a") + i) * 22}'
            await self.redis.hset(
                'rss:youtube:creators', cid, cid,
            )
        report: ReconcileReport = (
            await self.auditor.scan()
        )
        drift: DriftClassReport = report.drift[
            'rss_creator_missing_channel_state'
        ]
        self.assertEqual(drift.count, 5)
        self.assertEqual(len(drift.samples), 3)


class TestStateMissingRssScan(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self.redis = fakeredis.aioredis.FakeRedis(
            decode_responses=True,
        )
        self.queue: RedisChannelScrapeQueue = (
            RedisChannelScrapeQueue(
                self.redis,
                ChannelScrapeQueueSettings(),
            )
        )
        self.auditor: ChannelQueueAuditor = (
            ChannelQueueAuditor(
                self.queue, self.redis, sample_size=5,
            )
        )

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    async def test_scheduled_with_rss_active_is_not_drift(
        self,
    ) -> None:
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:0',
            {'i:UCaaaaaaaaaaaaaaaaaaaaaa': 0.0},
        )
        await self.redis.hset(
            'rss:youtube:creators',
            'UCaaaaaaaaaaaaaaaaaaaaaa', 'a',
        )
        report: ReconcileReport = (
            await self.auditor.scan()
        )
        drift: DriftClassReport = report.drift[
            'channel_state_missing_rss_accounting'
        ]
        self.assertEqual(drift.count, 0)

    async def test_scheduled_missing_rss_accounting_is_drift(
        self,
    ) -> None:
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:0',
            {'i:UCbbbbbbbbbbbbbbbbbbbbbb': 0.0},
        )
        # Seed identity so label is available
        await self.redis.hset(
            'youtube:creator_map',
            'UCbbbbbbbbbbbbbbbbbbbbbb',
            'bchannel',
        )
        report: ReconcileReport = (
            await self.auditor.scan()
        )
        drift: DriftClassReport = report.drift[
            'channel_state_missing_rss_accounting'
        ]
        self.assertEqual(drift.count, 1)
        self.assertEqual(
            drift.samples[0].member,
            'i:UCbbbbbbbbbbbbbbbbbbbbbb',
        )
        # Label resolved -> not 'unseedable'
        unseedable: DriftClassReport = report.drift[
            'channel_state_missing_rss_unseedable'
        ]
        self.assertEqual(unseedable.count, 0)

    async def test_scheduled_missing_rss_no_label_is_unseedable(
        self,
    ) -> None:
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:0',
            {'i:UCcccccccccccccccccccccc': 0.0},
        )
        report: ReconcileReport = (
            await self.auditor.scan()
        )
        drift: DriftClassReport = report.drift[
            'channel_state_missing_rss_accounting'
        ]
        unseedable: DriftClassReport = report.drift[
            'channel_state_missing_rss_unseedable'
        ]
        self.assertEqual(drift.count, 1)
        self.assertEqual(unseedable.count, 1)
        self.assertEqual(
            unseedable.samples[0].member,
            'i:UCcccccccccccccccccccccc',
        )

    async def test_rss_suppressed_satisfies_accounting(
        self,
    ) -> None:
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:0',
            {'i:UCdddddddddddddddddddddd': 0.0},
        )
        await self.redis.hset(
            'rss:youtube:suppressed',
            'UCdddddddddddddddddddddd',
            '{"reason":"x"}',
        )
        report: ReconcileReport = (
            await self.auditor.scan()
        )
        drift: DriftClassReport = report.drift[
            'channel_state_missing_rss_accounting'
        ]
        self.assertEqual(drift.count, 0)


class TestRssSuppressionScan(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self.redis = fakeredis.aioredis.FakeRedis(
            decode_responses=True,
        )
        self.queue: RedisChannelScrapeQueue = (
            RedisChannelScrapeQueue(
                self.redis,
                ChannelScrapeQueueSettings(),
            )
        )
        self.auditor: ChannelQueueAuditor = (
            ChannelQueueAuditor(
                self.queue, self.redis, sample_size=5,
            )
        )

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    async def test_terminal_not_found_without_rss_suppression_is_drift(
        self,
    ) -> None:
        await self.redis.hset(
            'youtube:channel:not_found',
            'i:UCaaaaaaaaaaaaaaaaaaaaaa', '{}',
        )
        report: ReconcileReport = (
            await self.auditor.scan()
        )
        drift: DriftClassReport = report.drift[
            'channel_terminal_missing_rss_suppression'
        ]
        self.assertEqual(drift.count, 1)
        self.assertEqual(
            drift.samples[0].member,
            'i:UCaaaaaaaaaaaaaaaaaaaaaa',
        )
        self.assertEqual(
            drift.samples[0].context['state'],
            'not_found',
        )

    async def test_terminal_with_rss_suppression_is_not_drift(
        self,
    ) -> None:
        await self.redis.hset(
            'youtube:channel:not_found',
            'i:UCbbbbbbbbbbbbbbbbbbbbbb', '{}',
        )
        await self.redis.hset(
            'rss:youtube:suppressed',
            'UCbbbbbbbbbbbbbbbbbbbbbb',
            '{"reason":"x"}',
        )
        report: ReconcileReport = (
            await self.auditor.scan()
        )
        drift: DriftClassReport = report.drift[
            'channel_terminal_missing_rss_suppression'
        ]
        self.assertEqual(drift.count, 0)

    async def test_soft_unavailable_does_not_trigger_drift(
        self,
    ) -> None:
        # soft_unavailable is NOT in RSS_SUPPRESSING_STATES
        await self.redis.hset(
            'youtube:channel:soft_unavailable',
            'i:UCcccccccccccccccccccccc', '{}',
        )
        report: ReconcileReport = (
            await self.auditor.scan()
        )
        drift: DriftClassReport = report.drift[
            'channel_terminal_missing_rss_suppression'
        ]
        self.assertEqual(drift.count, 0)

    async def test_unresolved_excluded(
        self,
    ) -> None:
        # unresolved is terminal but not in RSS-suppressing
        # states, AND it's keyed by handle so RSS doesn't
        # know about it
        await self.redis.hset(
            'youtube:channel:unresolved',
            'h:somehandle', '{}',
        )
        report: ReconcileReport = (
            await self.auditor.scan()
        )
        drift: DriftClassReport = report.drift[
            'channel_terminal_missing_rss_suppression'
        ]
        self.assertEqual(drift.count, 0)


class TestMetaOrphanScan(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self.redis = fakeredis.aioredis.FakeRedis(
            decode_responses=True,
        )
        self.queue: RedisChannelScrapeQueue = (
            RedisChannelScrapeQueue(
                self.redis,
                ChannelScrapeQueueSettings(),
            )
        )
        self.auditor: ChannelQueueAuditor = (
            ChannelQueueAuditor(
                self.queue, self.redis, sample_size=5,
            )
        )

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    async def test_meta_scheduled_with_zset_membership_is_not_drift(
        self,
    ) -> None:
        await self.redis.hset(
            'youtube:channel:meta:i:UCaaaaaaaaaaaaaaaaaaaaaa',
            mapping={
                'channel_id': 'UCaaaaaaaaaaaaaaaaaaaaaa',
                'state': 'scheduled',
            },
        )
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:0',
            {'i:UCaaaaaaaaaaaaaaaaaaaaaa': 0.0},
        )
        report: ReconcileReport = (
            await self.auditor.scan()
        )
        drift: DriftClassReport = report.drift[
            'meta_scheduled_missing_zset'
        ]
        self.assertEqual(drift.count, 0)

    async def test_meta_scheduled_missing_zset_is_drift(
        self,
    ) -> None:
        await self.redis.hset(
            'youtube:channel:meta:i:UCbbbbbbbbbbbbbbbbbbbbbb',
            mapping={
                'channel_id': 'UCbbbbbbbbbbbbbbbbbbbbbb',
                'state': 'scheduled',
            },
        )
        report: ReconcileReport = (
            await self.auditor.scan()
        )
        drift: DriftClassReport = report.drift[
            'meta_scheduled_missing_zset'
        ]
        self.assertEqual(drift.count, 1)
        self.assertEqual(
            drift.samples[0].member,
            'i:UCbbbbbbbbbbbbbbbbbbbbbb',
        )

    async def test_meta_pending_with_unresolved_membership_is_not_drift(
        self,
    ) -> None:
        await self.redis.hset(
            'youtube:channel:meta:h:somehandle',
            mapping={
                'handle': 'somehandle',
                'state': 'pending_resolution',
            },
        )
        await self.redis.zadd(
            'youtube:channel:queue:unresolved',
            {'h:somehandle': 0.0},
        )
        report: ReconcileReport = (
            await self.auditor.scan()
        )
        drift: DriftClassReport = report.drift[
            'meta_pending_missing_unresolved'
        ]
        self.assertEqual(drift.count, 0)

    async def test_meta_pending_missing_unresolved_is_drift(
        self,
    ) -> None:
        await self.redis.hset(
            'youtube:channel:meta:h:orphan',
            mapping={
                'handle': 'orphan',
                'state': 'pending_resolution',
            },
        )
        report: ReconcileReport = (
            await self.auditor.scan()
        )
        drift: DriftClassReport = report.drift[
            'meta_pending_missing_unresolved'
        ]
        self.assertEqual(drift.count, 1)
        self.assertEqual(
            drift.samples[0].member, 'h:orphan',
        )

    async def test_meta_terminal_state_missing_hash_is_reported(
        self,
    ) -> None:
        await self.redis.hset(
            'youtube:channel:meta:i:UCccccccccccccccccccccc1',
            mapping={
                'channel_id':
                    'UCccccccccccccccccccccc1',
                'state': 'not_found',
            },
        )
        report: ReconcileReport = (
            await self.auditor.scan()
        )
        drift: DriftClassReport = report.drift[
            'meta_terminal_missing_hash'
        ]
        self.assertEqual(drift.count, 1)
        self.assertEqual(
            drift.samples[0].member,
            'i:UCccccccccccccccccccccc1',
        )
        self.assertEqual(
            report.drift[
                'meta_scheduled_missing_zset'
            ].count,
            0,
        )
        self.assertEqual(
            report.drift[
                'meta_pending_missing_unresolved'
            ].count,
            0,
        )


class TestRemainingDriftScans(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self.redis = fakeredis.aioredis.FakeRedis(
            decode_responses=True,
        )
        self.queue: RedisChannelScrapeQueue = (
            RedisChannelScrapeQueue(
                self.redis,
                ChannelScrapeQueueSettings(),
            )
        )
        self.auditor: ChannelQueueAuditor = (
            ChannelQueueAuditor(
                self.queue, self.redis, sample_size=5,
            )
        )

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    async def test_rss_suppressed_channel_active_is_reported(
        self,
    ) -> None:
        await self.redis.hset(
            'rss:youtube:suppressed',
            'UCaaaaaaaaaaaaaaaaaaaaaa',
            '{"reason":"rss_not_found"}',
        )
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:0',
            {'i:UCaaaaaaaaaaaaaaaaaaaaaa': 0.0},
        )
        report: ReconcileReport = await self.auditor.scan()
        self.assertEqual(
            report.drift['rss_suppressed_channel_active'].count,
            1,
        )

    async def test_zset_missing_meta_is_reported(
        self,
    ) -> None:
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:0',
            {'i:UCbbbbbbbbbbbbbbbbbbbbbb': 0.0},
        )
        report: ReconcileReport = await self.auditor.scan()
        self.assertEqual(
            report.drift['zset_missing_meta'].count,
            1,
        )

    async def test_terminal_hash_missing_meta_is_reported(
        self,
    ) -> None:
        await self.redis.hset(
            'youtube:channel:not_found',
            'i:UCcccccccccccccccccccccc',
            '{}',
        )
        report: ReconcileReport = await self.auditor.scan()
        self.assertEqual(
            report.drift['terminal_hash_missing_meta'].count,
            1,
        )

    async def test_terminal_hash_meta_disagrees_is_reported(
        self,
    ) -> None:
        await self.redis.hset(
            'youtube:channel:not_found',
            'i:UCdddddddddddddddddddddd',
            '{}',
        )
        await self.redis.hset(
            'youtube:channel:meta:i:UCdddddddddddddddddddddd',
            mapping={
                'channel_id': 'UCdddddddddddddddddddddd',
                'state': 'scheduled',
            },
        )
        report: ReconcileReport = await self.auditor.scan()
        self.assertEqual(
            report.drift['terminal_hash_meta_disagrees'].count,
            1,
        )

    async def test_scheduled_tier_mismatch_is_reported(
        self,
    ) -> None:
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:0',
            {'i:UCeeeeeeeeeeeeeeeeeeeeee': 10.0},
        )
        await self.redis.hset(
            'youtube:channel:tiers',
            'UCeeeeeeeeeeeeeeeeeeeeee',
            '2',
        )
        report: ReconcileReport = await self.auditor.scan()
        self.assertEqual(
            report.drift['scheduled_tier_mismatch'].count,
            1,
        )

    async def test_identity_map_missing_is_reported(
        self,
    ) -> None:
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:0',
            {'i:UCffffffffffffffffffffff': 0.0},
        )
        report: ReconcileReport = await self.auditor.scan()
        self.assertEqual(
            report.drift[
                'identity_map_missing_for_channel_state'
            ].count,
            1,
        )

    async def test_member_in_multiple_states_is_reported(
        self,
    ) -> None:
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:0',
            {'i:UCgggggggggggggggggggggg': 0.0},
        )
        await self.redis.hset(
            'youtube:channel:not_found',
            'i:UCgggggggggggggggggggggg',
            '{}',
        )
        report: ReconcileReport = await self.auditor.scan()
        self.assertEqual(
            report.drift['member_in_multiple_states'].count,
            1,
        )


class TestEndToEndScan(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(
                decode_responses=True,
            )
        )
        self.queue: RedisChannelScrapeQueue = (
            RedisChannelScrapeQueue(
                self.redis,
                ChannelScrapeQueueSettings(),
            )
        )
        self.auditor: ChannelQueueAuditor = (
            ChannelQueueAuditor(
                self.queue, self.redis, sample_size=2,
            )
        )

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    async def test_one_of_each_drift_class(
        self,
    ) -> None:
        # rss_creator_missing_channel_state
        # UCmissingxxxxxxxxxxxxxxx = UC + 22 chars ✓
        await self.redis.hset(
            'rss:youtube:creators',
            'UCmissingxxxxxxxxxxxxxxx', 'm',
        )
        # channel_state_missing_rss_accounting
        # (label present -> not unseedable)
        # UCstatemissingxxxxxxxxxx = UC + 22 chars ✓
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:0',
            {'i:UCstatemissingxxxxxxxxxx': 0.0},
        )
        await self.redis.hset(
            'youtube:creator_map',
            'UCstatemissingxxxxxxxxxx',
            'state-missing',
        )
        # channel_state_missing_rss_unseedable
        # UCunseedablexxxxxxxxxxxx = UC + 22 chars ✓
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:0',
            {'i:UCunseedablexxxxxxxxxxxx': 0.0},
        )
        # channel_terminal_missing_rss_suppression
        # UCterminalsuppxxxxxxxxxx = UC + 22 chars ✓
        await self.redis.hset(
            'youtube:channel:not_found',
            'i:UCterminalsuppxxxxxxxxxx', '{}',
        )
        # meta_scheduled_missing_zset
        # UCmetaorphxxxxxxxxxxxxxx = UC + 22 chars ✓
        await self.redis.hset(
            'youtube:channel:meta:'
            'i:UCmetaorphxxxxxxxxxxxxxx',
            mapping={
                'channel_id':
                    'UCmetaorphxxxxxxxxxxxxxx',
                'state': 'scheduled',
            },
        )
        # meta_pending_missing_unresolved
        await self.redis.hset(
            'youtube:channel:meta:h:orphanhandle',
            mapping={
                'handle': 'orphanhandle',
                'state': 'pending_resolution',
            },
        )

        report: ReconcileReport = (
            await self.auditor.scan()
        )

        self.assertEqual(
            report.drift[
                'rss_creator_missing_channel_state'
            ].count,
            1,
        )
        # _scan_state_missing_rss walks both scheduled
        # zsets AND terminal hashes.
        # UCstatemissingxxxxxxxxxx and UCunseedablexx
        # are in the scheduled zset; UCterminalsuppxx
        # is in the not_found terminal hash. All three
        # lack RSS accounting -> count = 3.
        self.assertEqual(
            report.drift[
                'channel_state_missing_rss_accounting'
            ].count,
            3,
        )
        # UCunseedablexxxxxxxxxxxx and UCterminalsuppxx
        # both have no label in creator_map/meta ->
        # both are unseedable -> count = 2.
        self.assertEqual(
            report.drift[
                'channel_state_missing_rss_unseedable'
            ].count,
            2,
        )
        self.assertEqual(
            report.drift[
                'channel_terminal_missing_rss_suppression'
            ].count,
            1,
        )
        self.assertEqual(
            report.drift[
                'meta_scheduled_missing_zset'
            ].count,
            1,
        )
        self.assertEqual(
            report.drift[
                'meta_pending_missing_unresolved'
            ].count,
            1,
        )


class TestRepairStateMembership(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(
                decode_responses=True,
            )
        )
        self.queue: RedisChannelScrapeQueue = (
            RedisChannelScrapeQueue(
                self.redis,
                ChannelScrapeQueueSettings(),
            )
        )
        self.repairer: ChannelQueueRepairer = ChannelQueueRepairer(
            self.queue,
            self.redis,
        )
        self.auditor: ChannelQueueAuditor = ChannelQueueAuditor(
            self.queue,
            self.redis,
        )

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    async def test_terminal_member_removed_from_active_queue(
        self,
    ) -> None:
        member = 'i:UCstatefixxxxxxxxxxxxxxx'
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:0',
            {member: 0.0},
        )
        await self.redis.hset(
            'youtube:channel:not_found',
            member,
            '{}',
        )
        await self.redis.hset(
            f'youtube:channel:meta:{member}',
            mapping={
                'channel_id': member[2:],
                'state': ChannelState.SCHEDULED.value,
            },
        )

        repaired = await self.repairer.repair(
            RepairOptions(mode='state-membership'),
        )

        self.assertEqual(repaired['state-membership'], 1)
        self.assertIsNone(await self.redis.zscore(
            'youtube:channel:queue:scheduled:0',
            member,
        ))
        self.assertEqual(
            await self.redis.hget(
                f'youtube:channel:meta:{member}',
                'state',
            ),
            ChannelState.NOT_FOUND.value,
        )
        report: ReconcileReport = await self.auditor.scan()
        self.assertEqual(
            report.drift['member_in_multiple_states'].count,
            0,
        )

    async def test_all_safe_runs_state_membership_repair(
        self,
    ) -> None:
        member = 'i:UCallstatefixxxxxxxxxxxx'
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:0',
            {member: 0.0},
        )
        await self.redis.hset(
            'youtube:channel:removed',
            member,
            '{}',
        )

        repaired = await self.repairer.repair(
            RepairOptions(mode='all-safe'),
        )

        self.assertEqual(repaired['state-membership'], 1)
        self.assertIsNone(await self.redis.zscore(
            'youtube:channel:queue:scheduled:0',
            member,
        ))


_NO_HANDLE_REC: str = (
    '{"ts": 1779487596, "last_error": '
    '"no handle in creator_map for \'%s\'", "note": null}'
)


class TestRepairUnresolvedRevive(
    unittest.IsolatedAsyncioTestCase,
):
    '''unresolved-revive re-schedules i: members that were marked
    terminal-unresolved only because creator_map lacked their handle.'''

    async def asyncSetUp(self) -> None:
        self.redis = fakeredis.aioredis.FakeRedis(
            decode_responses=True,
        )
        self.queue = RedisChannelScrapeQueue(
            self.redis, ChannelScrapeQueueSettings(),
        )
        self.repairer = ChannelQueueRepairer(self.queue, self.redis)
        self.auditor = ChannelQueueAuditor(self.queue, self.redis)

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    async def _put_unresolved(
        self, member: str, record: str,
    ) -> None:
        await self.redis.hset(
            'youtube:channel:unresolved', member, record,
        )
        await self.redis.hset(
            f'youtube:channel:meta:{member}',
            mapping={'state': ChannelState.UNRESOLVED.value},
        )

    async def test_revives_no_handle_member(self) -> None:
        cid = 'UCrevivexxxxxxxxxxxxxxxx'
        member = f'i:{cid}'
        await self._put_unresolved(member, _NO_HANDLE_REC % cid)

        repaired = await self.repairer.repair(
            RepairOptions(mode='unresolved-revive'),
        )

        self.assertEqual(repaired['unresolved-revive'], 1)
        self.assertIsNone(await self.redis.hget(
            'youtube:channel:unresolved', member,
        ))
        score = await self.redis.zscore(
            'youtube:channel:queue:scheduled:0', member,
        )
        self.assertIsNotNone(score)
        self.assertEqual(
            await self.redis.hget(
                f'youtube:channel:meta:{member}', 'state',
            ),
            ChannelState.SCHEDULED.value,
        )

    async def test_spreads_scores_not_all_due_now(self) -> None:
        members = [f'i:UCspread{i:016d}' for i in range(5)]
        for m in members:
            await self._put_unresolved(m, _NO_HANDLE_REC % m[2:])

        await self.repairer.repair(
            RepairOptions(mode='unresolved-revive'),
        )

        scores = [
            await self.redis.zscore(
                'youtube:channel:queue:scheduled:0', m,
            )
            for m in members
        ]
        # Spread => not every member collapses onto 0.0.
        self.assertTrue(any(s and s > 0.0 for s in scores), scores)

    async def test_skips_other_last_error_and_handle_members(
        self,
    ) -> None:
        other = 'i:UCotherxxxxxxxxxxxxxxxxx'
        await self._put_unresolved(
            other, '{"ts": 1, "last_error": "boom", "note": null}',
        )
        hmember = 'h:somehandle'
        await self.redis.hset(
            'youtube:channel:unresolved', hmember,
            _NO_HANDLE_REC % 'somehandle',
        )

        repaired = await self.repairer.repair(
            RepairOptions(mode='unresolved-revive'),
        )

        self.assertEqual(repaired.get('unresolved-revive', 0), 0)
        self.assertIsNotNone(await self.redis.hget(
            'youtube:channel:unresolved', other,
        ))
        self.assertIsNotNone(await self.redis.hget(
            'youtube:channel:unresolved', hmember,
        ))

    async def test_all_safe_includes_revive(self) -> None:
        cid = 'UCallsafexxxxxxxxxxxxxxxx'
        member = f'i:{cid}'
        await self._put_unresolved(member, _NO_HANDLE_REC % cid)

        repaired = await self.repairer.repair(
            RepairOptions(mode='all-safe'),
        )

        self.assertEqual(repaired.get('unresolved-revive'), 1)

    async def test_auditor_reports_revivable_count(self) -> None:
        cid = 'UCcountxxxxxxxxxxxxxxxxxx'
        member = f'i:{cid}'
        await self._put_unresolved(member, _NO_HANDLE_REC % cid)

        report = await self.auditor.scan()
        self.assertEqual(report.revivable_unresolved, 1)
