'''
Unit tests for the rate limiter's sleep metrics: the sleep-duration
histogram has no proxy label (it would multiply by the bucket
count), while a per-proxy counter keeps the per-proxy average sleep
available as rate(slept_seconds_total) / rate(wait_events_total).

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import unittest
from unittest.mock import AsyncMock, patch

from prometheus_client import REGISTRY

from scrape_exchange import rate_limiter as rl
from scrape_exchange.youtube.youtube_rate_limiter import (
    YouTubeCallType,
    YouTubeRateLimiter,
)

PROXY: str = 'http://sleep-metrics.test:3128'


class TestSleepMetrics(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self._env = patch.dict(
            'os.environ',
            {'RATE_LIMITER_STATE_DIR': '', 'REDIS_DSN': ''},
        )
        self._env.start()
        YouTubeRateLimiter.reset()
        self.limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        self.limiter.set_proxies([PROXY])

    def tearDown(self) -> None:
        YouTubeRateLimiter.reset()
        self._env.stop()

    def test_histogram_has_no_proxy_label(self) -> None:
        self.assertNotIn('proxy', rl.METRIC_SLEEP_SECONDS._labelnames)
        self.assertIn('api', rl.METRIC_SLEEP_SECONDS._labelnames)
        self.assertIn(
            'worker_id', rl.METRIC_SLEEP_SECONDS._labelnames,
        )

    async def test_sleep_counted_per_proxy(self) -> None:
        labels: dict[str, str] = self.limiter._labels(
            YouTubeCallType.BROWSE, PROXY,
        )
        before_slept: float = REGISTRY.get_sample_value(
            'rate_limit_slept_seconds_total', labels,
        ) or 0.0
        before_events: float = REGISTRY.get_sample_value(
            'rate_limit_wait_events_total', labels,
        ) or 0.0
        self.limiter._backend.try_acquire = AsyncMock(
            side_effect=[(0.75, 0.0, 0.0), (0.0, 1.0, 1.0)],
        )
        with patch.object(rl.asyncio, 'sleep', AsyncMock()):
            await self.limiter.acquire(YouTubeCallType.BROWSE, PROXY)
        self.assertAlmostEqual(
            REGISTRY.get_sample_value(
                'rate_limit_slept_seconds_total', labels,
            ) - before_slept,
            0.75,
        )
        self.assertEqual(
            REGISTRY.get_sample_value(
                'rate_limit_wait_events_total', labels,
            ) - before_events,
            1.0,
        )


if __name__ == '__main__':
    unittest.main()
