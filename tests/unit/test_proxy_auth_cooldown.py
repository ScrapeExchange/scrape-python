'''
Unit tests for the 407 Proxy Authentication Required cooldown in
scrape_exchange.proxy_health and its rate limiter entry points.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import time
import unittest

import fakeredis.aioredis
import httpx

from scrape_exchange import proxy_health as ph
from scrape_exchange.proxy_health import (
    ProxyHealth,
    ProxyHealthState,
    apply_outcome,
    is_proxy_auth_error,
)

PROXIES: list[str] = [f'http://p{i}.test:3128' for i in range(3)]


class TestIsProxyAuthError(unittest.TestCase):

    def test_httpx_proxy_error_407(self) -> None:
        exc: httpx.ProxyError = httpx.ProxyError(
            '407 Proxy Authentication Required',
        )
        self.assertTrue(is_proxy_auth_error(exc))

    def test_other_proxy_error(self) -> None:
        exc: httpx.ProxyError = httpx.ProxyError('502 Bad Gateway')
        self.assertFalse(is_proxy_auth_error(exc))

    def test_connect_error(self) -> None:
        exc: httpx.ConnectError = httpx.ConnectError(
            'All connection attempts failed',
        )
        self.assertFalse(is_proxy_auth_error(exc))

    def test_none(self) -> None:
        self.assertFalse(is_proxy_auth_error(None))

    def test_curl_tunnel_message(self) -> None:
        exc: RuntimeError = RuntimeError(
            'CONNECT tunnel failed, response 407',
        )
        self.assertTrue(is_proxy_auth_error(exc))

    def test_wrapped_in_runtime_error(self) -> None:
        try:
            try:
                raise httpx.ProxyError(
                    '407 Proxy Authentication Required',
                )
            except httpx.ProxyError as inner:
                raise RuntimeError('InnerTube API call failed') from inner
        except RuntimeError as outer:
            self.assertTrue(is_proxy_auth_error(outer))

    def test_unrelated_407_in_text(self) -> None:
        # A video id or count containing 407 is not an auth failure.
        exc: ValueError = ValueError('video abc407 not found')
        self.assertFalse(is_proxy_auth_error(exc))


class TestApplyAuthOutcome(unittest.TestCase):

    def test_single_auth_failure_opens_long_cooldown(self) -> None:
        state: ProxyHealthState = ProxyHealthState()
        now: float = 1000.0
        self.assertTrue(apply_outcome(state, False, now, True))
        self.assertEqual(
            state.cooldown_until, now + ph.AUTH_COOLDOWN_SECONDS,
        )
        self.assertTrue(state.auth_failed)
        self.assertFalse(state.is_clean())

    def test_repeat_auth_failure_does_not_reopen(self) -> None:
        state: ProxyHealthState = ProxyHealthState()
        apply_outcome(state, False, 1000.0, True)
        self.assertFalse(apply_outcome(state, False, 1010.0, True))
        self.assertEqual(
            state.cooldown_until, 1010.0 + ph.AUTH_COOLDOWN_SECONDS,
        )

    def test_auth_failure_extends_short_cooldown(self) -> None:
        state: ProxyHealthState = ProxyHealthState()
        for _ in range(ph.FAIL_THRESHOLD_CONSECUTIVE):
            apply_outcome(state, False, 1000.0)
        self.assertLess(state.cooldown_until, 1000.0 + 3600)
        self.assertTrue(apply_outcome(state, False, 1001.0, True))
        self.assertEqual(
            state.cooldown_until, 1001.0 + ph.AUTH_COOLDOWN_SECONDS,
        )

    def test_success_during_cooldown_keeps_it(self) -> None:
        state: ProxyHealthState = ProxyHealthState()
        apply_outcome(state, False, 1000.0, True)
        apply_outcome(state, True, 1001.0)
        self.assertTrue(state.auth_failed)
        self.assertEqual(
            state.cooldown_until, 1000.0 + ph.AUTH_COOLDOWN_SECONDS,
        )

    def test_success_after_cooldown_clears_flag(self) -> None:
        state: ProxyHealthState = ProxyHealthState()
        apply_outcome(state, False, 1000.0, True)
        apply_outcome(
            state, True, 1000.0 + ph.AUTH_COOLDOWN_SECONDS + 1,
        )
        self.assertFalse(state.auth_failed)


class TestAuthCooldownLocal(unittest.IsolatedAsyncioTestCase):

    async def test_report_and_clear(self) -> None:
        health: ProxyHealth = ProxyHealth(None, 'youtube')
        await health.report(PROXIES[0], False, auth_failure=True)
        for _ in range(ph.FAIL_THRESHOLD_CONSECUTIVE):
            await health.report(PROXIES[1], False)
        self.assertFalse(health.available(PROXIES[0]))
        self.assertFalse(health.available(PROXIES[1]))
        self.assertEqual(
            await health.clear_auth_cooldowns(PROXIES), 1,
        )
        self.assertTrue(health.available(PROXIES[0]))
        # A cooldown from ordinary transport failures is kept.
        self.assertFalse(health.available(PROXIES[1]))

    async def test_auth_flag_ignored_on_success(self) -> None:
        health: ProxyHealth = ProxyHealth(None, 'youtube')
        await health.report(PROXIES[0], True, auth_failure=True)
        self.assertTrue(health.available(PROXIES[0]))


class TestAuthCooldownRedis(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(decode_responses=True)
        )

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    async def test_auth_cooldown_is_fleet_wide(self) -> None:
        one: ProxyHealth = ProxyHealth(self.redis, 'youtube')
        two: ProxyHealth = ProxyHealth(self.redis, 'youtube')
        before: float = time.time()
        await one.report(PROXIES[0], False, auth_failure=True)
        self.assertFalse(one.available(PROXIES[0]))
        await two.refresh_if_stale(PROXIES, force=True)
        self.assertFalse(two.available(PROXIES[0]))
        state: ProxyHealthState = two._states[PROXIES[0]]
        self.assertTrue(state.auth_failed)
        # Lua's tostring() keeps 14 significant digits.
        self.assertAlmostEqual(
            state.cooldown_until, before + ph.AUTH_COOLDOWN_SECONDS,
            delta=1.0,
        )

    async def test_clear_is_fleet_wide_and_selective(self) -> None:
        one: ProxyHealth = ProxyHealth(self.redis, 'youtube')
        two: ProxyHealth = ProxyHealth(self.redis, 'youtube')
        await one.report(PROXIES[0], False, auth_failure=True)
        for _ in range(ph.FAIL_THRESHOLD_CONSECUTIVE):
            await one.report(PROXIES[1], False)
        self.assertEqual(await two.clear_auth_cooldowns(PROXIES), 1)
        await one.refresh_if_stale(PROXIES, force=True)
        self.assertTrue(one.available(PROXIES[0]))
        self.assertFalse(one.available(PROXIES[1]))
        # Clearing again is a no-op.
        self.assertEqual(await two.clear_auth_cooldowns(PROXIES), 0)

    async def test_repeat_auth_failure_counts_one_cooldown(self) -> None:
        health: ProxyHealth = ProxyHealth(self.redis, 'youtube')
        counter = ph.METRIC_PROXY_COOLDOWNS.labels(
            platform='youtube', proxy=PROXIES[2],
            worker_id=ph.get_worker_id(), reason='proxy_auth',
        )
        start: float = counter._value.get()
        await health.report(PROXIES[2], False, auth_failure=True)
        await health.report(PROXIES[2], False, auth_failure=True)
        self.assertEqual(counter._value.get() - start, 1)


class TestClearAuthCooldownsInRedis(unittest.IsolatedAsyncioTestCase):

    async def test_supervisor_helper_clears_and_closes(self) -> None:
        from unittest.mock import patch
        server: fakeredis.FakeServer = fakeredis.FakeServer()
        writer: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(
                server=server, decode_responses=True,
            )
        )
        await ProxyHealth(writer, 'youtube').report(
            PROXIES[0], False, auth_failure=True,
        )
        helper_client: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(
                server=server, decode_responses=True,
            )
        )
        with patch(
            'scrape_exchange.redis_client.redis_from_url',
            return_value=helper_client,
        ):
            cleared: int = await ph.clear_auth_cooldowns_in_redis(
                'redis://localhost:6379/1', PROXIES, 'youtube',
            )
        self.assertEqual(cleared, 1)
        reader: ProxyHealth = ProxyHealth(writer, 'youtube')
        await reader.refresh_if_stale(PROXIES, force=True)
        self.assertTrue(reader.available(PROXIES[0]))
        await writer.aclose()


class TestRateLimiterReport(unittest.IsolatedAsyncioTestCase):

    async def test_exception_decides_auth_failure(self) -> None:
        from scrape_exchange.youtube.youtube_rate_limiter import (
            YouTubeRateLimiter,
        )
        YouTubeRateLimiter.reset()
        self.addCleanup(YouTubeRateLimiter.reset)
        limiter: YouTubeRateLimiter = YouTubeRateLimiter(
            state_dir=None, redis_dsn=None, auto_warm_cookies=False,
        )
        limiter.set_proxies(list(PROXIES))
        await limiter.report_proxy_result(
            PROXIES[0], False,
            httpx.ProxyError('407 Proxy Authentication Required'),
        )
        await limiter.report_proxy_result(
            PROXIES[1], False, httpx.ConnectError('refused'),
        )
        health: ProxyHealth = limiter._health
        self.assertFalse(health.available(PROXIES[0]))
        self.assertTrue(health.available(PROXIES[1]))
        self.assertEqual(await limiter.clear_proxy_auth_cooldowns(), 1)
        self.assertTrue(health.available(PROXIES[0]))


if __name__ == '__main__':
    unittest.main()
