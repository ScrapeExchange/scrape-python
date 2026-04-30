'''
Unit tests for TikTokSessionPool. TikTokApi is mocked.
'''

import asyncio
import tempfile
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.tiktok.tiktok_rate_limiter import (
    TikTokRateLimiter,
)
from scrape_exchange.tiktok.tiktok_session_pool import (
    TikTokSessionPool,
    SessionUnavailable,
)


def _make_session(name: str) -> MagicMock:
    s: MagicMock = MagicMock()
    s.name = name
    return s


class TestTikTokSessionPool(unittest.TestCase):

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.state_dir: str = self._tmp.name
        TikTokRateLimiter.reset()
        self.rl: TikTokRateLimiter = TikTokRateLimiter()

    def tearDown(self) -> None:
        self._tmp.cleanup()
        TikTokRateLimiter.reset()

    def test_acquire_returns_session_for_proxy(self) -> None:
        proxies: list[str] = ['http://1.2.3.4:8080']
        api: MagicMock = MagicMock()
        api.create_sessions = AsyncMock()
        api.close_sessions = AsyncMock()
        api.stop_playwright = AsyncMock()
        bound: MagicMock = _make_session('s1')
        api.sessions = [bound]

        async def go() -> object:
            with patch(
                'scrape_exchange.tiktok.tiktok_session_pool'
                '.TikTokApi', return_value=api,
            ):
                pool: TikTokSessionPool = TikTokSessionPool(
                    proxies=proxies,
                    state_dir=self.state_dir,
                    ms_token_ttl_seconds=3600,
                    rate_limiter=self.rl,
                    scraper_label='test_scraper',
                )
                await pool.bootstrap()
                async with pool.session_for(
                    proxies[0],
                ) as sess:
                    return sess

        sess: object = asyncio.run(go())
        self.assertIs(sess, bound)

    def test_no_multiplex(self) -> None:
        '''Two concurrent acquires for the same proxy must
        serialise.'''
        proxies: list[str] = ['http://1.2.3.4:8080']
        api: MagicMock = MagicMock()
        api.create_sessions = AsyncMock()
        api.close_sessions = AsyncMock()
        api.stop_playwright = AsyncMock()
        bound: MagicMock = _make_session('s1')
        api.sessions = [bound]

        order: list[str] = []

        async def hold(pool: TikTokSessionPool, tag: str) -> None:
            async with pool.session_for(proxies[0]):
                order.append(f'enter:{tag}')
                await asyncio.sleep(0.05)
                order.append(f'exit:{tag}')

        async def go() -> None:
            with patch(
                'scrape_exchange.tiktok.tiktok_session_pool'
                '.TikTokApi', return_value=api,
            ):
                pool: TikTokSessionPool = TikTokSessionPool(
                    proxies=proxies,
                    state_dir=self.state_dir,
                    ms_token_ttl_seconds=3600,
                    rate_limiter=self.rl,
                    scraper_label='test_scraper',
                )
                await pool.bootstrap()
                await asyncio.gather(
                    hold(pool, 'a'),
                    hold(pool, 'b'),
                )

        asyncio.run(go())
        # The interleaving must be enter/exit/enter/exit, not
        # enter/enter/exit/exit.
        self.assertIn(order, [
            ['enter:a', 'exit:a', 'enter:b', 'exit:b'],
            ['enter:b', 'exit:b', 'enter:a', 'exit:a'],
        ])

    def test_failed_bootstrap_excludes_proxy(self) -> None:
        proxies: list[str] = [
            'http://1.2.3.4:8080',
            'http://5.6.7.8:8080',
        ]
        api: MagicMock = MagicMock()
        api.create_sessions = AsyncMock(
            side_effect=RuntimeError('chromium crash'),
        )
        api.close_sessions = AsyncMock()
        api.stop_playwright = AsyncMock()
        api.sessions = []

        async def go() -> TikTokSessionPool:
            with patch(
                'scrape_exchange.tiktok.tiktok_session_pool'
                '.TikTokApi', return_value=api,
            ):
                pool: TikTokSessionPool = TikTokSessionPool(
                    proxies=proxies,
                    state_dir=self.state_dir,
                    ms_token_ttl_seconds=3600,
                    rate_limiter=self.rl,
                    scraper_label='test_scraper',
                )
                await pool.bootstrap()
                return pool

        pool: TikTokSessionPool = asyncio.run(go())
        # Both proxies should be marked failed, none ready.
        self.assertEqual(pool.ready_proxies(), [])
        self.assertEqual(
            sorted(pool.failed_proxies()), sorted(proxies),
        )

        async def acq() -> None:
            async with pool.session_for(proxies[0]):
                pass

        with self.assertRaises(SessionUnavailable):
            asyncio.run(acq())


if __name__ == '__main__':
    unittest.main()
