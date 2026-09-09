'''Opt-in live anonymous profile scrape, with isolated rate-limit state.'''

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker
from pydantic_settings import BaseSettings, SettingsConfigDict

from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.onlyfans.onlyfans_browser import (
    anonymous_browser,
    fetch_profile,
)
from scrape_exchange.onlyfans.onlyfans_creator import OnlyFansCreator
from scrape_exchange.onlyfans.onlyfans_rate_limiter import OnlyFansRateLimiter
from scrape_exchange.onlyfans.settings import OnlyFansScraperSettings
from tools.of_creator_scrape import save_creator


class LiveSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix='ONLYFANS_LIVE_')

    enabled: bool = False
    username: str = 'onlyfans'
    proxy_files: str | None = None


_LIVE: LiveSettings = LiveSettings()


@unittest.skipUnless(_LIVE.enabled, 'Set ONLYFANS_LIVE_ENABLED=true to run')
class TestOnlyFansCreatorLive(unittest.IsolatedAsyncioTestCase):
    async def test_anonymous_profile_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            settings: OnlyFansScraperSettings = OnlyFansScraperSettings(
                _env_file=None, _cli_parse_args=[], redis_dsn='',
                rate_limiter_state_dir=str(Path(directory) / 'limiter'),
                proxy_files=_LIVE.proxy_files, proxies_env=None,
            )
            limiter: OnlyFansRateLimiter = OnlyFansRateLimiter(settings)
            proxy: str | None = (
                settings.proxies[0] if settings.proxies else None
            )
            async with anonymous_browser(proxy, settings) as context:
                self.assertEqual(await context.cookies(), [])
                creator: OnlyFansCreator = await fetch_profile(
                    context, _LIVE.username, limiter, proxy, settings,
                )
            self.assertEqual(creator.username, _LIVE.username.lower())
            self.assertIsNotNone(creator.display_name)
            self.assertIsNotNone(creator.like_count)
            self.assertIsNotNone(creator.subscription_price)
            fm: AssetFileManagement = AssetFileManagement(directory)
            await save_creator(creator, fm)
            record: dict[str, Any] = await fm.read_file(
                f'onlyfans-creator-{creator.username}.json.br',
            )
            schema: dict[str, Any] = json.loads(Path(
                'tests/collateral/drand-onlyfans-creator-schema.json',
            ).read_text())
            Draft202012Validator(
                schema, format_checker=FormatChecker(),
            ).validate(record)
            self.assertEqual(OnlyFansCreator.model_validate(record), creator)
