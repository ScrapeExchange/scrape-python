'''Opt-in anonymous website smoke test; never uses production Redis.'''

import tempfile
import unittest
from pathlib import Path

from playwright.async_api import Request
from pydantic_settings import BaseSettings, SettingsConfigDict

from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.twitch.settings import TwitchScraperSettings
from scrape_exchange.twitch.twitch_browser import fetch_profile
from scrape_exchange.twitch.twitch_creator import TwitchCreator
from scrape_exchange.twitch.twitch_error_classification import (
    ProfileUnavailableError,
)
from scrape_exchange.twitch.twitch_rate_limiter import (
    TwitchCallType,
    TwitchRateLimiter,
)
from scrape_exchange.twitch.twitch_session_pool import TwitchSessionPool
from tools.tw_creator_scrape import save_creator


class LiveSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix='TWITCH_LIVE_')

    enabled: bool = False
    profiles: list[str] = ['twitch']
    missing_username: str | None = None


_LIVE: LiveSettings = LiveSettings()


@unittest.skipUnless(_LIVE.enabled, 'Set TWITCH_LIVE_ENABLED=true to run')
class TestTwitchCreatorLive(unittest.IsolatedAsyncioTestCase):
    async def test_anonymous_profile_round_trip(self) -> None:
        settings: TwitchScraperSettings = TwitchScraperSettings(
            _env_file=None, _cli_parse_args=[], redis_dsn='',
            creator_disable_proxies=True,
        )
        with tempfile.TemporaryDirectory() as directory:
            settings.rate_limiter_state_dir = str(Path(directory) / 'limiter')
            limiter: TwitchRateLimiter = TwitchRateLimiter(settings)
            pool: TwitchSessionPool = TwitchSessionPool(
                [], settings, limiter, 'integration',
            )
            fm: AssetFileManagement = AssetFileManagement(
                directory, prefix_rankings={'creator': ['twitch-creator-']},
            )
            authenticated: list[bool] = []

            def observe(request: Request) -> None:
                value: str | None = request.headers.get('authorization')
                # The logged-out website sometimes emits the literal
                # JavaScript placeholder; it is not an authentication token.
                authenticated.append(value not in (None, '', 'undefined'))

            try:
                await pool.bootstrap()
                self.assertEqual(pool.ready_proxies(), ['direct'])
                for username in _LIVE.profiles:
                    with self.subTest(username=username):
                        await limiter.acquire(TwitchCallType.CREATOR, 'direct')
                        async with pool.session_for('direct') as page:
                            page.on('request', observe)
                            creator: TwitchCreator = await fetch_profile(
                                page, username, settings, limiter, 'direct',
                            )
                        self.assertEqual(creator.username, username)
                        self.assertIsNotNone(creator.user_id)
                        self.assertIn('structured', creator.sources)
                        await save_creator(creator, fm)
                        record: dict = await fm.read_file(
                            f'twitch-creator-{username}.json.br',
                        )
                        self.assertEqual(
                            TwitchCreator.model_validate(record), creator,
                        )
                if _LIVE.missing_username:
                    await limiter.acquire(TwitchCallType.CREATOR, 'direct')
                    async with pool.session_for('direct') as page:
                        with self.assertRaises(ProfileUnavailableError):
                            await fetch_profile(
                                page, _LIVE.missing_username, settings,
                                limiter, 'direct',
                            )
                self.assertTrue(authenticated)
                self.assertFalse(any(authenticated))
            finally:
                await pool.shutdown()
