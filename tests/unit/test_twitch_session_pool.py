'''Session locking and cleanup without an external browser.'''

import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.twitch.settings import TwitchScraperSettings
from scrape_exchange.twitch.twitch_session_pool import TwitchSessionPool


class TestTwitchSessionPool(unittest.IsolatedAsyncioTestCase):
    async def test_pages_are_closed_and_same_proxy_is_serialized(
        self,
    ) -> None:
        settings: TwitchScraperSettings = TwitchScraperSettings(
            _env_file=None, _cli_parse_args=[],
        )
        browser: MagicMock = MagicMock(close=AsyncMock())
        context: MagicMock = MagicMock(close=AsyncMock())
        page: MagicMock = MagicMock(close=AsyncMock())
        context.new_page = AsyncMock(return_value=page)
        browser.new_context = AsyncMock(return_value=context)
        pool: TwitchSessionPool = TwitchSessionPool(
            [], settings, MagicMock(acquire=AsyncMock()), 'test',
        )
        pool._playwright = MagicMock(stop=AsyncMock())
        with patch.object(pool, '_launch', AsyncMock(return_value=browser)):
            await pool.bootstrap()
        entered: asyncio.Event = asyncio.Event()

        async def second() -> None:
            async with pool.session_for('direct'):
                entered.set()

        async with pool.session_for('direct'):
            task: asyncio.Task[None] = asyncio.create_task(second())
            await asyncio.sleep(0)
            self.assertFalse(entered.is_set())
        await task
        self.assertTrue(entered.is_set())
        self.assertEqual(page.close.await_count, 2)
        await pool.shutdown()
        self.assertEqual(pool.ready_proxies(), [])
        browser.close.assert_awaited_once()

    async def test_failed_context_creation_closes_browser(self) -> None:
        settings: TwitchScraperSettings = TwitchScraperSettings(
            _env_file=None, _cli_parse_args=[],
        )
        browser: MagicMock = MagicMock(
            new_context=AsyncMock(side_effect=RuntimeError('failed')),
            close=AsyncMock(),
        )
        pool: TwitchSessionPool = TwitchSessionPool(
            [], settings, MagicMock(acquire=AsyncMock()), 'test',
        )
        pool._playwright = MagicMock(stop=AsyncMock())
        with patch.object(pool, '_launch', AsyncMock(return_value=browser)):
            await pool.bootstrap()
        self.assertEqual(pool.ready_proxies(), [])
        browser.close.assert_awaited_once()
        await pool.shutdown()
