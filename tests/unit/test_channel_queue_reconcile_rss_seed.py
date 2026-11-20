'''
Unit tests for the reconcile repairs that keep the RSS queue seeded now
that the RSS workers no longer populate it at startup: ``rss-seed`` and
``creator-map-missing``.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import unittest

import fakeredis.aioredis

from scrape_exchange.channel_queue_reconcile import (
    ChannelQueueRepairer,
    RepairOptions,
)
from scrape_exchange.channel_scrape_queue import (
    ChannelScrapeQueueSettings,
    RedisChannelScrapeQueue,
)

META: str = 'youtube:channel:meta:i:'


def _cid(tag: str) -> str:
    '''A valid 24-character channel ID ending in *tag*.'''
    return f'UC{tag.rjust(22, "x")}'


class _RepairTestCase(unittest.IsolatedAsyncioTestCase):

    batch_size: int = 1000

    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(decode_responses=True)
        )
        self.queue: RedisChannelScrapeQueue = RedisChannelScrapeQueue(
            self.redis, ChannelScrapeQueueSettings(),
        )
        self.repairer: ChannelQueueRepairer = ChannelQueueRepairer(
            self.queue, self.redis, batch_size=self.batch_size,
        )

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    async def _scheduled(
        self, channel_id: str, meta: dict[str, str],
        label: str | None = 'handle',
    ) -> None:
        await self.redis.zadd(
            self.queue._k_scheduled(0), {f'i:{channel_id}': 1.0},
        )
        await self.redis.hset(f'{META}{channel_id}', mapping=meta)
        if label:
            await self.redis.hset(
                'youtube:creator_map', channel_id, f'{label}{channel_id}',
            )

    async def _in_rss(self, channel_id: str) -> bool:
        return bool(
            await self.redis.hexists('rss:youtube:creators', channel_id),
        )


class TestRssSeedHoldBack(_RepairTestCase):

    async def test_holds_back_channels_without_first_full_scrape(
        self,
    ) -> None:
        pending: str = _cid('pending')
        zero: str = _cid('zero')
        done: str = _cid('done')
        progressless: str = _cid('progressless')
        await self._scheduled(pending, {'state': 'scheduled'})
        await self._scheduled(
            zero, {'state': 'scheduled', 'successful_scrapes': '0'},
        )
        await self._scheduled(
            done, {'state': 'scheduled', 'successful_scrapes': '1'},
        )
        # Scraped before scrape-progress tracking existed: no
        # successful_scrapes, but a recorded attempt.
        await self._scheduled(
            progressless,
            {'state': 'scheduled', 'last_attempt_at': '1786000000'},
        )

        repaired: dict[str, int] = await self.repairer.repair(
            RepairOptions(mode='rss-seed'),
        )

        self.assertEqual(repaired['rss-seed'], 2)
        self.assertFalse(await self._in_rss(pending))
        self.assertFalse(await self._in_rss(zero))
        self.assertTrue(await self._in_rss(done))
        self.assertTrue(await self._in_rss(progressless))

    async def test_skips_suppressing_state_and_suppressed_hash(
        self,
    ) -> None:
        low_subs: str = _cid('lowsubs')
        suppressed: str = _cid('suppressed')
        await self._scheduled(
            low_subs, {'state': 'low_subs', 'successful_scrapes': '2'},
        )
        await self.redis.hset(
            'youtube:channel:low_subs', f'i:{low_subs}', '{}',
        )
        await self._scheduled(
            suppressed, {'state': 'scheduled', 'successful_scrapes': '2'},
        )
        await self.redis.hset('rss:youtube:suppressed', suppressed, '{}')

        repaired: dict[str, int] = await self.repairer.repair(
            RepairOptions(mode='rss-seed'),
        )

        self.assertEqual(repaired['rss-seed'], 0)
        self.assertFalse(await self._in_rss(low_subs))
        self.assertFalse(await self._in_rss(suppressed))

    async def test_existing_rss_creator_only_heals_names_index(
        self,
    ) -> None:
        channel_id: str = _cid('known')
        await self._scheduled(
            channel_id,
            {'state': 'scheduled', 'successful_scrapes': '1'},
        )
        await self.redis.hset(
            'rss:youtube:creators', channel_id, 'KnownHandle',
        )

        first: dict[str, int] = await self.repairer.repair(
            RepairOptions(mode='rss-seed'),
        )
        second: dict[str, int] = await self.repairer.repair(
            RepairOptions(mode='rss-seed'),
        )

        self.assertEqual(first['rss-seed'], 1)
        self.assertEqual(second['rss-seed'], 0)
        self.assertTrue(
            await self.redis.sismember('rss:youtube:names', 'knownhandle'),
        )
        self.assertEqual(await self.redis.zcard('rss:youtube:queue:4'), 0)

    async def test_channel_without_label_is_not_seeded(self) -> None:
        channel_id: str = _cid('nolabel')
        await self._scheduled(
            channel_id,
            {'state': 'scheduled', 'successful_scrapes': '1'},
            label=None,
        )

        repaired: dict[str, int] = await self.repairer.repair(
            RepairOptions(mode='rss-seed'),
        )

        self.assertEqual(repaired['rss-seed'], 0)
        self.assertFalse(await self._in_rss(channel_id))


class TestRssSeedBudgetAcrossBatches(_RepairTestCase):

    batch_size: int = 2

    async def test_max_repairs_is_exact_across_batches(self) -> None:
        index: int
        for index in range(5):
            await self._scheduled(
                _cid(f'b{index}'),
                {'state': 'scheduled', 'successful_scrapes': '1'},
            )

        repaired: dict[str, int] = await self.repairer.repair(
            RepairOptions(mode='rss-seed', max_repairs=3),
        )

        self.assertEqual(repaired['rss-seed'], 3)
        self.assertEqual(
            await self.redis.hlen('rss:youtube:creators'), 3,
        )

    async def test_all_members_seeded_without_budget(self) -> None:
        index: int
        for index in range(5):
            await self._scheduled(
                _cid(f'c{index}'),
                {'state': 'scheduled', 'successful_scrapes': '1'},
            )

        repaired: dict[str, int] = await self.repairer.repair(
            RepairOptions(mode='rss-seed'),
        )

        self.assertEqual(repaired['rss-seed'], 5)


class TestCreatorMapMissing(_RepairTestCase):

    batch_size: int = 2

    async def test_schedules_only_channels_unknown_to_channel_queue(
        self,
    ) -> None:
        unknown: str = _cid('unknown')
        scheduled: str = _cid('sched')
        terminal: str = _cid('terminal')
        unresolved: str = _cid('unresolved')
        channel_id: str
        for channel_id in (unknown, scheduled, terminal, unresolved):
            await self.redis.hset(
                'youtube:creator_map', channel_id, f'h{channel_id}',
            )
        await self.redis.hset('youtube:creator_map', 'not-a-channel', 'x')
        await self.redis.zadd(
            self.queue._k_scheduled(1), {f'i:{scheduled}': 0.0},
        )
        await self.redis.hset(
            'youtube:channel:not_found', f'i:{terminal}', '{}',
        )
        await self.redis.zadd(
            self.queue._k_unresolved(), {f'i:{unresolved}': 5.0},
        )

        repaired: dict[str, int] = await self.repairer.repair(
            RepairOptions(mode='creator-map-missing'),
        )

        self.assertEqual(repaired['creator-map-missing'], 1)
        meta: dict[str, str] = await self.redis.hgetall(
            f'{META}{unknown}',
        )
        self.assertEqual(meta['state'], 'scheduled')
        self.assertEqual(
            meta['reconcile_reason'], 'creator_map_missing_channel_state',
        )
        # Scheduled, not seeded into RSS: the channel scraper adds it
        # to RSS after its first full scrape.
        self.assertFalse(await self._in_rss(unknown))
        self.assertEqual(
            await self.redis.hgetall(f'{META}{terminal}'), {},
        )
        self.assertIsNone(
            await self.redis.zscore(
                self.queue._k_scheduled(0), f'i:{terminal}',
            ),
        )

    async def test_all_safe_includes_creator_map_missing(self) -> None:
        await self.redis.hset(
            'youtube:creator_map', _cid('allsafe'), 'h',
        )

        repaired: dict[str, int] = await self.repairer.repair(
            RepairOptions(mode='all-safe'),
        )

        self.assertEqual(repaired['creator-map-missing'], 1)


if __name__ == '__main__':
    unittest.main()
