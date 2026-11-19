'''
Unit tests for scrape_exchange.proxy_health: load-aware
power-of-two-choices proxy selection and fleet-wide proxy health.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import random
import unittest
from collections import Counter

import fakeredis.aioredis

from scrape_exchange import proxy_health as ph
from scrape_exchange.proxy_health import ProxyHealth, ProxyHealthState


PROXIES: list[str] = [f'http://p{i}.test:3128' for i in range(4)]


def _full(proxy: str) -> float:
    return 1.0


class TestApplyOutcome(unittest.TestCase):

    def test_consecutive_failures_open_with_backoff(self) -> None:
        state: ProxyHealthState = ProxyHealthState()
        self.assertFalse(ph.apply_outcome(state, False, 100.0))
        self.assertFalse(ph.apply_outcome(state, False, 100.0))
        self.assertTrue(ph.apply_outcome(state, False, 100.0))
        self.assertEqual(
            state.cooldown_until, 100.0 + ph.BASE_COOLDOWN_SECONDS,
        )
        self.assertEqual(state.open_count, 1)
        # While open, further failures do not extend the cooldown.
        self.assertFalse(ph.apply_outcome(state, False, 110.0))
        # After the cooldown the next opening doubles it.
        now: float = state.cooldown_until + 1
        opened: bool = False
        while not opened:
            opened = ph.apply_outcome(state, False, now)
        self.assertEqual(
            state.cooldown_until, now + 2 * ph.BASE_COOLDOWN_SECONDS,
        )

    def test_cooldown_is_capped(self) -> None:
        state: ProxyHealthState = ProxyHealthState(open_count=20)
        state.consecutive_failures = ph.FAIL_THRESHOLD_CONSECUTIVE - 1
        self.assertTrue(ph.apply_outcome(state, False, 0.0))
        self.assertEqual(state.cooldown_until, ph.MAX_COOLDOWN_SECONDS)

    def test_successes_recover(self) -> None:
        state: ProxyHealthState = ProxyHealthState(
            fail_ewma=0.9, consecutive_failures=2, open_count=3,
        )
        _: int
        for _ in range(20):
            ph.apply_outcome(state, True, 0.0)
        self.assertLess(state.fail_ewma, 0.05)
        self.assertEqual(state.consecutive_failures, 0)
        self.assertEqual(state.open_count, 0)


class TestChoose(unittest.TestCase):

    def setUp(self) -> None:
        self.health: ProxyHealth = ProxyHealth(
            None, 'test', rng=random.Random(7),
        )

    def test_spreads_load_evenly(self) -> None:
        picks: Counter = Counter(
            self.health.choose(PROXIES, _full, now=1000.0)
            for _ in range(4000)
        )
        self.assertEqual(set(picks), set(PROXIES))
        self.assertLess(max(picks.values()) - min(picks.values()), 40)

    def test_skips_proxies_in_cooldown(self) -> None:
        self.health._states[PROXIES[0]] = ProxyHealthState(
            cooldown_until=2000.0,
        )
        picks: set[str] = {
            self.health.choose(PROXIES, _full, now=1000.0)
            for _ in range(200)
        }
        self.assertNotIn(PROXIES[0], picks)

    def test_all_in_cooldown_returns_earliest_to_reopen(self) -> None:
        proxy: str
        for index, proxy in enumerate(PROXIES):
            self.health._states[proxy] = ProxyHealthState(
                cooldown_until=2000.0 + index,
            )
        self.assertEqual(
            self.health.choose(PROXIES, _full, now=1000.0), PROXIES[0],
        )

    def test_failing_proxy_is_picked_less(self) -> None:
        self.health._states[PROXIES[0]] = ProxyHealthState(fail_ewma=0.4)
        picks: Counter = Counter(
            self.health.choose(PROXIES, _full, now=1000.0)
            for _ in range(4000)
        )
        self.assertLess(picks[PROXIES[0]], min(
            picks[p] for p in PROXIES[1:]
        ))

    def test_prefers_proxy_with_tokens(self) -> None:
        def tokens(proxy: str) -> float:
            return 0.0 if proxy == PROXIES[0] else 1.0

        picks: Counter = Counter(
            self.health.choose(PROXIES[:2], tokens, now=1000.0)
            for _ in range(1)
        )
        self.assertEqual(picks[PROXIES[1]], 1)


class TestFleetWideHealth(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(decode_responses=True)
        )

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    async def test_cooldown_is_shared_between_processes(self) -> None:
        one: ProxyHealth = ProxyHealth(self.redis, 'youtube')
        two: ProxyHealth = ProxyHealth(self.redis, 'youtube')
        _: int
        for _ in range(ph.FAIL_THRESHOLD_CONSECUTIVE):
            await one.report(PROXIES[0], False)
        self.assertFalse(one.available(PROXIES[0]))
        self.assertTrue(two.available(PROXIES[0]))
        await two.refresh_if_stale(PROXIES, force=True)
        self.assertFalse(two.available(PROXIES[0]))
        self.assertTrue(two.available(PROXIES[1]))

    async def test_success_on_clean_proxy_skips_redis(self) -> None:
        health: ProxyHealth = ProxyHealth(self.redis, 'youtube')
        await health.report(PROXIES[1], True)
        self.assertEqual(await self.redis.keys('proxy_health:*'), [])

    async def test_success_after_failure_is_written(self) -> None:
        health: ProxyHealth = ProxyHealth(self.redis, 'youtube')
        await health.report(PROXIES[1], False)
        await health.report(PROXIES[1], True)
        keys: list[str] = await self.redis.keys('proxy_health:*')
        self.assertEqual(len(keys), 1)
        self.assertEqual(
            await self.redis.hget(keys[0], 'consecutive_failures'), '0',
        )

    async def test_redis_errors_do_not_raise(self) -> None:
        health: ProxyHealth = ProxyHealth(_FailingRedis(), 'youtube')
        await health.report(PROXIES[0], False)
        await health.refresh_if_stale(PROXIES, force=True)
        self.assertEqual(
            health._states[PROXIES[0]].consecutive_failures, 1,
        )


class TestRateLimiterIntegration(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        from unittest.mock import patch
        from scrape_exchange.youtube.youtube_rate_limiter import (
            YouTubeRateLimiter,
        )
        self._env = patch.dict(
            'os.environ',
            {'RATE_LIMITER_STATE_DIR': '', 'REDIS_DSN': ''},
        )
        self._env.start()
        YouTubeRateLimiter.reset()
        self.limiter = YouTubeRateLimiter.get()
        self.limiter.set_proxies(PROXIES)

    def tearDown(self) -> None:
        from scrape_exchange.youtube.youtube_rate_limiter import (
            YouTubeRateLimiter,
        )
        YouTubeRateLimiter.reset()
        self._env.stop()

    async def test_failing_proxy_is_not_selected(self) -> None:
        from scrape_exchange.youtube.youtube_rate_limiter import (
            YouTubeCallType,
        )
        _: int
        for _ in range(ph.FAIL_THRESHOLD_CONSECUTIVE):
            await self.limiter.report_proxy_result(PROXIES[2], False)
        picks: set[str] = {
            self.limiter.select_proxy(YouTubeCallType.BROWSE)
            for _ in range(200)
        }
        self.assertNotIn(PROXIES[2], picks)
        self.assertEqual(picks, set(PROXIES) - {PROXIES[2]})


class _FailingRedis:
    def __getattr__(self, name: str) -> object:
        raise ConnectionError('redis down')


if __name__ == '__main__':
    unittest.main()


def _ewma_sample(proxy: str) -> float | None:
    from prometheus_client import REGISTRY
    return REGISTRY.get_sample_value(
        'proxy_health_fail_ewma',
        {'platform': 'youtube', 'proxy': proxy, 'worker_id': '0'},
    )


def _cooldown_sample(proxy: str) -> float | None:
    from prometheus_client import REGISTRY
    return REGISTRY.get_sample_value(
        'proxy_health_in_cooldown',
        {'platform': 'youtube', 'proxy': proxy, 'worker_id': '0'},
    )


class TestHealthGaugeCardinality(unittest.IsolatedAsyncioTestCase):
    '''The health gauges only carry series for proxies that have
    been unhealthy, and with Redis only the designated publisher
    process exports them.'''

    async def asyncSetUp(self) -> None:
        from unittest.mock import patch
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(decode_responses=True)
        )
        self._env = patch.dict(
            'os.environ', {ph.PUBLISHER_ENV_VAR: '1'},
        )
        self._env.start()

    async def asyncTearDown(self) -> None:
        self._env.stop()
        await self.redis.aclose()

    def test_gauges_drop_pid_label_in_multiprocess_mode(self) -> None:
        self.assertEqual(
            ph.METRIC_PROXY_FAIL_EWMA._multiprocess_mode, 'livemax',
        )
        self.assertEqual(
            ph.METRIC_PROXY_IN_COOLDOWN._multiprocess_mode, 'livemax',
        )

    async def test_healthy_proxy_has_no_series(self) -> None:
        proxy: str = 'http://card-healthy.test:3128'
        health: ProxyHealth = ProxyHealth(self.redis, 'youtube')
        await health.report(proxy, True)
        await health.refresh_if_stale([proxy], force=True)
        self.assertIsNone(_ewma_sample(proxy))
        self.assertIsNone(_cooldown_sample(proxy))

    async def test_failed_proxy_keeps_series_after_recovery(
        self,
    ) -> None:
        proxy: str = 'http://card-recovers.test:3128'
        health: ProxyHealth = ProxyHealth(self.redis, 'youtube')
        await health.report(proxy, False)
        self.assertAlmostEqual(_ewma_sample(proxy), ph.FAILURE_ALPHA)
        self.assertEqual(_cooldown_sample(proxy), 0.0)
        await self.redis.flushall()
        await health.refresh_if_stale([proxy], force=True)
        # Recovered (key expired): the series stays, reading zero.
        self.assertEqual(_ewma_sample(proxy), 0.0)

    async def test_refresh_publishes_proxy_failed_elsewhere(
        self,
    ) -> None:
        proxy: str = 'http://card-elsewhere.test:3128'
        other: ProxyHealth = ProxyHealth(self.redis, 'youtube')
        _: int
        for _ in range(ph.FAIL_THRESHOLD_CONSECUTIVE):
            await other.report(proxy, False)
        health: ProxyHealth = ProxyHealth(self.redis, 'youtube')
        await health.refresh_if_stale([proxy], force=True)
        self.assertEqual(_cooldown_sample(proxy), 1.0)

    async def test_non_publisher_with_redis_exports_nothing(
        self,
    ) -> None:
        from unittest.mock import patch
        proxy: str = 'http://card-silent.test:3128'
        with patch.dict('os.environ', {ph.PUBLISHER_ENV_VAR: '0'}):
            health: ProxyHealth = ProxyHealth(self.redis, 'youtube')
        _: int
        for _ in range(ph.FAIL_THRESHOLD_CONSECUTIVE):
            await health.report(proxy, False)
        await health.refresh_if_stale([proxy], force=True)
        self.assertFalse(health.available(proxy))
        self.assertIsNone(_ewma_sample(proxy))
        self.assertIsNone(_cooldown_sample(proxy))

    async def test_non_publisher_without_redis_still_exports(
        self,
    ) -> None:
        # Without Redis each process has its own health view, so
        # every process must publish it.
        from unittest.mock import patch
        proxy: str = 'http://card-local.test:3128'
        with patch.dict('os.environ', {ph.PUBLISHER_ENV_VAR: '0'}):
            health: ProxyHealth = ProxyHealth(None, 'youtube')
        await health.report(proxy, False)
        self.assertAlmostEqual(_ewma_sample(proxy), ph.FAILURE_ALPHA)

    async def test_publisher_by_default_when_unsupervised(self) -> None:
        from unittest.mock import patch
        import os
        proxy: str = 'http://card-standalone.test:3128'
        with patch.dict('os.environ', {}):
            os.environ.pop(ph.PUBLISHER_ENV_VAR, None)
            health: ProxyHealth = ProxyHealth(self.redis, 'youtube')
        await health.report(proxy, False)
        self.assertAlmostEqual(_ewma_sample(proxy), ph.FAILURE_ALPHA)
