'''
Integration test: do a real Playwright bootstrap against
tiktok.com on a single proxy, verify an ms_token is captured,
verify the pool reports the proxy as ready.

Skipped unless TIKTOK_LIVE_INTEGRATION=1. Reads PROXIES from
env (comma-separated, like the production scrapers).
'''

import asyncio
import os
import tempfile
import unittest

from scrape_exchange.tiktok.tiktok_rate_limiter import (
    TikTokRateLimiter,
)
from scrape_exchange.tiktok.tiktok_session_pool import (
    TikTokSessionPool,
)


def _gated() -> bool:
    return os.environ.get(
        'TIKTOK_LIVE_INTEGRATION', '0',
    ) == '1'


@unittest.skipUnless(
    _gated(),
    'set TIKTOK_LIVE_INTEGRATION=1 + PROXIES to run',
)
class TestSessionBootstrap(unittest.TestCase):

    def test_one_proxy_bootstraps(self) -> None:
        proxies_csv: str | None = os.environ.get('PROXIES')
        self.assertIsNotNone(
            proxies_csv,
            'PROXIES env var required for live bootstrap test',
        )
        proxy: str = proxies_csv.split(',')[0].strip()

        TikTokRateLimiter.reset()
        rl: TikTokRateLimiter = TikTokRateLimiter()

        async def go() -> tuple[list[str], list[str]]:
            with tempfile.TemporaryDirectory() as tmp:
                pool: TikTokSessionPool = TikTokSessionPool(
                    proxies=[proxy],
                    state_dir=tmp,
                    ms_token_ttl_seconds=14400,
                    rate_limiter=rl,
                    scraper_label='integration',
                )
                await pool.bootstrap()
                ready: list[str] = pool.ready_proxies()
                failed: list[str] = pool.failed_proxies()
                await pool.shutdown()
                return ready, failed

        ready, failed = asyncio.run(go())
        self.assertIn(proxy, ready)
        self.assertEqual(failed, [])


if __name__ == '__main__':
    unittest.main()
