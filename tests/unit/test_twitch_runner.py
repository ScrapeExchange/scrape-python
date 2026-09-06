'''A scrape-only runner must not initiate Exchange authentication.'''

import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.scraper_runner import ScraperRunner
from scrape_exchange.twitch.settings import TwitchScraperSettings


class TestTwitchRunner(unittest.IsolatedAsyncioTestCase):
    def test_supervisor_does_not_prefetch_exchange_credentials(self) -> None:
        settings: TwitchScraperSettings = TwitchScraperSettings(
            _env_file=None, _cli_parse_args=[],
            api_key_id='unused', api_key_secret='unused',
        )
        runner: ScraperRunner = ScraperRunner(
            settings=settings, scraper_label='twitch_creator',
            platform='twitch', num_processes=2, concurrency=2,
            metrics_port=9910, log_file='/dev/stdout', log_level='INFO',
            rate_limiter_factory=MagicMock(),
            client_required=False, client_enabled=False,
        )
        with patch('scrape_exchange.scraper_runner.configure_logging'), patch(
            'scrape_exchange.scraper_runner.run_supervisor', return_value=0,
        ) as supervisor:
            self.assertEqual(runner.run_sync(AsyncMock()), 0)
        self.assertIsNone(supervisor.call_args.args[0].api_key_id)
        self.assertIsNone(supervisor.call_args.args[0].api_key_secret)

    async def test_disabled_client_never_connects_to_exchange(self) -> None:
        settings: TwitchScraperSettings = TwitchScraperSettings(
            _env_file=None, _cli_parse_args=[], watchdog_enabled=False,
        )
        worker: AsyncMock = AsyncMock()
        runner: ScraperRunner = ScraperRunner(
            settings=settings, scraper_label='twitch_creator',
            platform='twitch', num_processes=1, concurrency=1,
            metrics_port=0, log_file='/dev/stdout', log_level='INFO',
            rate_limiter_factory=MagicMock(),
            client_required=False, client_enabled=False,
        )
        with patch(
            'scrape_exchange.scraper_runner.ExchangeClient.setup',
            new_callable=AsyncMock,
        ) as setup, patch(
            'scrape_exchange.scraper_runner._start_metrics_server_or_skip',
        ):
            await runner.run(worker)
        setup.assert_not_awaited()
        self.assertIsNone(worker.call_args.args[0].client)
