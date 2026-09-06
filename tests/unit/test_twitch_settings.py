'''Configuration validation and Twitch-specific rate limits.'''

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pydantic import ValidationError

from scrape_exchange.twitch.settings import TwitchScraperSettings
from scrape_exchange.twitch.twitch_rate_limiter import (
    TwitchCallType,
    TwitchRateLimiter,
)
from tools.tw_creator_scrape import _export_child_settings


class TestTwitchSettings(unittest.TestCase):
    def test_child_preserves_cli_settings_and_uses_proxy_slice(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            parent: TwitchScraperSettings = TwitchScraperSettings(
                _env_file=None, _cli_parse_args=[
                    '--creator-concurrency', '4',
                    '--creator-num-processes', '2',
                    '--data-rpm', '90',
                ],
            )
            _export_child_settings(parent)
            os.environ['TWITCH_CREATOR_CONCURRENCY'] = '2'
            os.environ['TWITCH_CREATOR_NUM_PROCESSES'] = '1'
            os.environ['PROXIES'] = 'http://localhost:8080'
            child: TwitchScraperSettings = self.settings()
        self.assertEqual(child.creator_concurrency, 2)
        self.assertEqual(child.creator_num_processes, 1)
        self.assertEqual(child.data_rpm, 90)
        self.assertEqual(list(child.proxies), ['http://localhost:8080'])

    def settings(self, **kwargs) -> TwitchScraperSettings:
        return TwitchScraperSettings(
            _env_file=None, _cli_parse_args=[], **kwargs,
        )

    def test_environment_and_cli_without_credentials(self) -> None:
        with patch.dict(os.environ, {
            'TWITCH_CREATOR_CONCURRENCY': '2',
            'TWITCH_PROFILE_BASE_URL': 'https://localhost/',
            'TWITCH_GRAPHQL_URL': 'https://localhost/gql',
            'TWITCH_USERNAME': 'ignored',
        }, clear=True):
            settings: TwitchScraperSettings = TwitchScraperSettings(
                _env_file=None,
                _cli_parse_args=['--creator-concurrency', '3'],
            )
        self.assertEqual(settings.creator_concurrency, 3)
        self.assertNotIn('profile_base_url', type(settings).model_fields)
        self.assertNotIn('graphql_url', type(settings).model_fields)
        self.assertIsNone(settings.username)
        self.assertIsNone(settings.api_key_id)
        self.assertIsNone(settings.api_key_secret)

    def test_username_is_cli_only_even_with_dotenv(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path: Path = Path(directory) / '.env'
            path.write_text(
                'TWITCH_USERNAME=ignored\n'
                'TWITCH_PROFILE_BASE_URL=https://localhost\n'
                'TWITCH_GRAPHQL_URL=https://localhost/gql\n',
            )
            with patch.dict(os.environ, {}, clear=True):
                settings: TwitchScraperSettings = TwitchScraperSettings(
                    _env_file=path, _cli_parse_args=[],
                )
                self.assertIsNone(settings.username)
                settings = TwitchScraperSettings(
                    _env_file=path,
                    _cli_parse_args=['--username', 'example'],
                )
                self.assertEqual(settings.username, 'example')
                _export_child_settings(settings)
                self.assertNotIn('TWITCH_USERNAME', os.environ)

    def test_rejects_invalid_limits(self) -> None:
        for kwargs in (
            {'creator_claim_ttl_seconds': 1},
            {'creator_concurrency': 0}, {'creator_rpm': 0},
        ):
            with self.subTest(kwargs=kwargs), self.assertRaises(
                ValidationError,
            ), patch.dict(os.environ, {}, clear=True):
                self.settings(**kwargs)

    def test_rate_buckets_use_settings_and_twitch_namespace(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            settings: TwitchScraperSettings = self.settings(
                creator_rpm=6, data_rpm=120,
            )
            limiter: TwitchRateLimiter = TwitchRateLimiter(settings)
        self.assertEqual(limiter._platform, 'twitch')
        self.assertEqual(
            limiter.default_configs[TwitchCallType.CREATOR].refill_rate,
            0.1,
        )
        self.assertEqual(
            limiter.default_configs[TwitchCallType.DATA].refill_rate, 2,
        )
