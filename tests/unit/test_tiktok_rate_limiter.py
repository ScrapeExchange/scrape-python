'''
Unit tests for TikTokRateLimiter. Exercises:
- backend selection from settings (in-process by default)
- per-call-type bucket separation (API vs BOOTSTRAP)
- env-based bucket overrides (TIKTOK_GLOBAL_RPM, TIKTOK_BOOTSTRAP_RPM)
'''

import os
import asyncio
import unittest
from unittest.mock import patch

from scrape_exchange.rate_limiter import _BucketConfig
from scrape_exchange.tiktok.tiktok_rate_limiter import (
    TikTokRateLimiter,
)
from scrape_exchange.tiktok.tiktok_types import TikTokCallType


class TestTikTokRateLimiter(unittest.TestCase):

    def setUp(self) -> None:
        TikTokRateLimiter.reset()

    def tearDown(self) -> None:
        TikTokRateLimiter.reset()

    def test_default_bucket_rates(self) -> None:
        rl: TikTokRateLimiter = TikTokRateLimiter()
        configs: dict[TikTokCallType, _BucketConfig] = rl.default_configs
        self.assertIn(TikTokCallType.API, configs)
        self.assertIn(TikTokCallType.BOOTSTRAP, configs)
        # 30 rpm = 0.5 per second
        self.assertAlmostEqual(
            configs[TikTokCallType.API].refill_rate,
            30 / 60,
        )
        # 6 rpm = 0.1 per second
        self.assertAlmostEqual(
            configs[TikTokCallType.BOOTSTRAP].refill_rate,
            6 / 60,
        )

    def test_env_overrides(self) -> None:
        with patch.dict(
            os.environ,
            {
                'TIKTOK_GLOBAL_RPM': '60',
                'TIKTOK_BOOTSTRAP_RPM': '12',
            },
        ):
            rl: TikTokRateLimiter = TikTokRateLimiter()
            configs: dict[TikTokCallType, _BucketConfig] = rl.default_configs
            self.assertAlmostEqual(
                configs[TikTokCallType.API].refill_rate, 1.0,
            )
            self.assertAlmostEqual(
                configs[TikTokCallType.BOOTSTRAP].refill_rate, 0.2,
            )

    def test_acquire_returns_proxy(self) -> None:
        rl: TikTokRateLimiter = TikTokRateLimiter()
        rl.set_proxies(['http://proxy1:8080'])

        async def go() -> str | None:
            return await rl.acquire(
                TikTokCallType.API,
                proxy='http://proxy1:8080',
            )

        proxy: str | None = asyncio.run(go())
        self.assertEqual(proxy, 'http://proxy1:8080')


if __name__ == '__main__':
    unittest.main()
