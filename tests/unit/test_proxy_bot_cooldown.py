'''
Unit tests for the per-process cooldown that a YouTube bot check
opens in scrape_exchange.proxy_health.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import random
import unittest
from unittest.mock import patch

import fakeredis.aioredis
from prometheus_client import REGISTRY

from scrape_exchange import proxy_health as ph
from scrape_exchange.proxy_health import ProxyHealth

PROXIES: list[str] = [f'http://bot{i}.test:3128' for i in range(3)]
BASE: float = ph.BOT_BASE_COOLDOWN_SECONDS


class _Clock:
    '''Settable stand-in for time.time in proxy_health.'''

    def __init__(self) -> None:
        self.now: float = 1000.0

    def __call__(self) -> float:
        return self.now


class _ClockCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.clock: _Clock = _Clock()
        patcher = patch.object(ph.time, 'time', self.clock)
        patcher.start()
        self.addCleanup(patcher.stop)


class TestBotCooldown(_ClockCase):

    def test_opens_base_cooldown(self) -> None:
        health: ProxyHealth = ProxyHealth(None, 'youtube')
        self.assertTrue(health.report_bot_detection(PROXIES[0], 'player'))
        self.assertFalse(health.available(PROXIES[0]))
        self.assertTrue(health.available(PROXIES[1]))
        self.clock.now += BASE
        self.assertTrue(health.available(PROXIES[0]))

    def test_check_during_cooldown_does_not_extend(self) -> None:
        health: ProxyHealth = ProxyHealth(None, 'youtube')
        health.report_bot_detection(PROXIES[0], 'player')
        self.clock.now += 10
        self.assertFalse(health.report_bot_detection(PROXIES[0], 'player'))
        self.assertEqual(health._bot[PROXIES[0]].until, 1000.0 + BASE)
        self.assertEqual(health._bot[PROXIES[0]].open_count, 1)

    def test_check_after_cooldown_doubles_up_to_max(self) -> None:
        health: ProxyHealth = ProxyHealth(None, 'youtube')
        lengths: list[float] = []
        for _ in range(8):
            start: float = self.clock.now
            health.report_bot_detection(PROXIES[0], 'player')
            until: float = health._bot[PROXIES[0]].until
            lengths.append(until - start)
            self.clock.now = until
        self.assertEqual(
            lengths,
            [120.0, 240.0, 480.0, 960.0, 1800.0, 1800.0, 1800.0,
             1800.0],
        )

    def test_same_api_success_after_cooldown_resets(self) -> None:
        health: ProxyHealth = ProxyHealth(None, 'youtube')
        health.report_bot_detection(PROXIES[0], 'player')
        self.clock.now += BASE
        health.report_bot_detection(PROXIES[0], 'player')
        self.clock.now = health._bot[PROXIES[0]].until
        health.report_api_success(PROXIES[0], 'player')
        self.assertNotIn(PROXIES[0], health._bot)
        start: float = self.clock.now
        health.report_bot_detection(PROXIES[0], 'player')
        self.assertEqual(health._bot[PROXIES[0]].until, start + BASE)

    async def test_other_api_success_does_not_reset(self) -> None:
        health: ProxyHealth = ProxyHealth(None, 'youtube')
        health.report_bot_detection(PROXIES[0], 'player')
        self.clock.now = health._bot[PROXIES[0]].until
        health.report_api_success(PROXIES[0], 'next')
        await health.report(PROXIES[0], True)
        self.assertEqual(health._bot[PROXIES[0]].open_count, 1)
        start: float = self.clock.now
        health.report_bot_detection(PROXIES[0], 'player')
        self.assertEqual(
            health._bot[PROXIES[0]].until, start + 2 * BASE,
        )

    def test_success_during_cooldown_keeps_it(self) -> None:
        health: ProxyHealth = ProxyHealth(None, 'youtube')
        health.report_bot_detection(PROXIES[0], 'player')
        self.clock.now += 1
        health.report_api_success(PROXIES[0], 'player')
        self.assertFalse(health.available(PROXIES[0]))
        self.assertEqual(health._bot[PROXIES[0]].open_count, 1)

    def test_none_proxy_is_ignored(self) -> None:
        health: ProxyHealth = ProxyHealth(None, 'youtube')
        self.assertFalse(health.report_bot_detection(None, 'player'))
        self.assertEqual(health._bot, {})


class TestChooseSkipsBotCooldown(_ClockCase):

    def test_skips_proxy_in_bot_cooldown(self) -> None:
        health: ProxyHealth = ProxyHealth(
            None, 'youtube', rng=random.Random(1),
        )
        health.report_bot_detection(PROXIES[0], 'player')
        chosen: set[str] = {
            health.choose(PROXIES, lambda p: 1.0) for _ in range(50)
        }
        self.assertNotIn(PROXIES[0], chosen)

    def test_all_cooling_returns_earliest_to_reopen(self) -> None:
        health: ProxyHealth = ProxyHealth(None, 'youtube')
        health.report_bot_detection(PROXIES[0], 'player')
        self.clock.now += 10
        health.report_bot_detection(PROXIES[1], 'player')
        self.assertEqual(
            health.choose(PROXIES[:2], lambda p: 1.0), PROXIES[0],
        )


class TestBotCooldownIsLocal(unittest.IsolatedAsyncioTestCase):

    async def test_other_process_keeps_using_proxy(self) -> None:
        redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(decode_responses=True)
        )
        self.addAsyncCleanup(redis.aclose)
        one: ProxyHealth = ProxyHealth(redis, 'youtube')
        two: ProxyHealth = ProxyHealth(redis, 'youtube')
        one.report_bot_detection(PROXIES[0], 'player')
        await two.refresh_if_stale(PROXIES, force=True)
        self.assertFalse(one.available(PROXIES[0]))
        self.assertTrue(two.available(PROXIES[0]))
        self.assertEqual(await redis.keys('proxy_health:*'), [])


class TestBotCooldownCounter(unittest.IsolatedAsyncioTestCase):

    async def test_counted_once_per_opening(self) -> None:
        platform: str = 'bot-openings-test'
        labels: dict[str, str] = {
            'platform': platform, 'proxy_file': 'none',
            'worker_id': ph.get_worker_id(), 'reason': 'bot_detection',
        }
        health: ProxyHealth = ProxyHealth(None, platform)
        await health.refresh_if_stale(list(PROXIES))
        self.assertEqual(
            REGISTRY.get_sample_value(
                'proxy_health_cooldown_openings_total', labels,
            ),
            0,
        )
        health.report_bot_detection(PROXIES[0], 'player')
        health.report_bot_detection(PROXIES[0], 'player')
        self.assertEqual(
            REGISTRY.get_sample_value(
                'proxy_health_cooldown_openings_total', labels,
            ),
            1,
        )


if __name__ == '__main__':
    unittest.main()
