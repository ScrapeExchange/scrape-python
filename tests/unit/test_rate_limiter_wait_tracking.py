'''RateLimiter.acquire counts as waiting for ScrapeTimer, so scrape
durations leave out the time spent waiting for a token.'''

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from scrape_exchange.rate_limiter import RateLimitWaitExceeded
from scrape_exchange.wait_tracking import ScrapeTimer
from scrape_exchange.youtube.youtube_rate_limiter import (
    YouTubeCallType,
    YouTubeRateLimiter,
)

PROXY: str = 'http://wait-tracking.test:3128'


class TestAcquireRecordsWait(unittest.IsolatedAsyncioTestCase):

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

    async def test_token_wait_is_not_active_time(self) -> None:
        self.limiter._backend.try_acquire = AsyncMock(
            side_effect=[(0.2, 0.0, 0.0), (0.0, 1.0, 1.0)],
        )
        timer: ScrapeTimer = ScrapeTimer.start()
        await self.limiter.acquire(YouTubeCallType.BROWSE, PROXY)
        self.assertGreaterEqual(timer.waited(), 0.2)
        self.assertLess(timer.active(), 0.1)

    async def test_wait_recorded_when_max_wait_exceeded(self) -> None:
        self.limiter._backend.try_acquire = AsyncMock(
            side_effect=[(0.1, 0.0, 0.0), (5.0, 0.0, 0.0)],
        )
        timer: ScrapeTimer = ScrapeTimer.start()
        with self.assertRaises(RateLimitWaitExceeded):
            await self.limiter.acquire(
                YouTubeCallType.BROWSE, PROXY, max_wait=1.0,
            )
        self.assertGreaterEqual(timer.waited(), 0.1)

    async def test_request_after_acquire_is_active_time(self) -> None:
        self.limiter._backend.try_acquire = AsyncMock(
            return_value=(0.0, 1.0, 1.0),
        )
        timer: ScrapeTimer = ScrapeTimer.start()
        await self.limiter.acquire(YouTubeCallType.BROWSE, PROXY)
        await asyncio.sleep(0.1)
        self.assertGreaterEqual(timer.active(), 0.1)


if __name__ == '__main__':
    unittest.main()
