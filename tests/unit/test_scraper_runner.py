'''Unit tests for scrape_exchange.scraper_runner.'''

import asyncio
import signal
import sys
import unittest
from unittest.mock import (
    AsyncMock,
    MagicMock,
    patch,
)

from scrape_exchange.scraper_runner import (
    ScraperRunContext,
    ScraperRunner,
)


def _make_settings_mock(
    proxies: str = 'http://p1:8080,http://p2:8080',
    api_key_id: str = 'key_id',
    api_key_secret: str = 'key_secret',
    exchange_url: str = 'https://scrape.exchange',
    rate_limiter_state_dir: str = '/tmp/rl',
    redis_dsn: str | None = None,
    log_format: str = 'json',
) -> MagicMock:
    s = MagicMock()
    s.proxies = proxies
    s.api_key_id = api_key_id
    s.api_key_secret = api_key_secret
    s.exchange_url = exchange_url
    s.rate_limiter_state_dir = rate_limiter_state_dir
    s.redis_dsn = redis_dsn
    s.log_format = log_format
    return s


def _make_rate_limiter_mock() -> MagicMock:
    rl = MagicMock()
    rl.set_proxies = MagicMock()
    return rl


class TestScraperRunContext(unittest.TestCase):
    '''Tests for ScraperRunContext dataclass.'''

    def test_fields(self) -> None:
        ctx = ScraperRunContext(
            settings=MagicMock(),
            client=None,
            rate_limiter=MagicMock(),
            proxies=['http://p1:8080'],
        )
        self.assertIsNone(ctx.client)
        self.assertEqual(
            ctx.proxies, ['http://p1:8080'],
        )


class TestScraperRunnerInit(unittest.TestCase):
    '''Tests for ScraperRunner construction.'''

    def test_stores_parameters(self) -> None:
        settings = _make_settings_mock()
        rl_factory = MagicMock(
            return_value=_make_rate_limiter_mock(),
        )
        runner = ScraperRunner(
            settings=settings,
            scraper_label='video',
            platform='youtube',
            num_processes=2,
            concurrency=3,
            metrics_port=9400,
            log_file='/dev/stdout',
            log_level='INFO',
            rate_limiter_factory=rl_factory,
        )
        self.assertEqual(
            runner._scraper_label, 'video',
        )
        self.assertEqual(
            runner._platform, 'youtube',
        )
        self.assertEqual(runner._num_processes, 2)


class TestScraperRunnerSupervisor(
    unittest.TestCase,
):
    '''Test that run_sync delegates to supervisor
    when num_processes > 1.'''

    @patch(
        'scrape_exchange.scraper_runner'
        '.run_supervisor',
    )
    @patch(
        'scrape_exchange.scraper_runner'
        '.configure_logging',
    )
    def test_supervisor_dispatched(
        self,
        mock_logging: MagicMock,
        mock_supervisor: MagicMock,
    ) -> None:
        mock_supervisor.return_value = 0
        settings = _make_settings_mock()
        runner = ScraperRunner(
            settings=settings,
            scraper_label='video',
            platform='youtube',
            num_processes=4,
            concurrency=3,
            metrics_port=9400,
            log_file='/dev/stdout',
            log_level='INFO',
            rate_limiter_factory=MagicMock(),
        )
        worker_fn = AsyncMock()
        code: int = runner.run_sync(worker_fn)
        self.assertEqual(code, 0)
        mock_supervisor.assert_called_once()
        worker_fn.assert_not_called()


class TestScraperRunnerWorker(
    unittest.IsolatedAsyncioTestCase,
):
    '''Test the worker path (num_processes == 1).'''

    @patch(
        'scrape_exchange.scraper_runner'
        '.start_http_server',
    )
    @patch(
        'scrape_exchange.scraper_runner'
        '.publish_config_metrics',
    )
    @patch(
        'scrape_exchange.scraper_runner'
        '.ScrapeExchangeRateLimiter',
    )
    @patch(
        'scrape_exchange.scraper_runner'
        '.ExchangeClient',
    )
    @patch(
        'scrape_exchange.scraper_runner'
        '.configure_logging',
    )
    async def test_worker_func_called_with_context(
        self,
        mock_logging: MagicMock,
        mock_client_cls: MagicMock,
        mock_se_rl: MagicMock,
        mock_publish: MagicMock,
        mock_http_server: MagicMock,
    ) -> None:
        mock_client = AsyncMock()
        mock_client.drain_uploads = AsyncMock()
        mock_client_cls.setup = AsyncMock(
            return_value=mock_client,
        )

        settings = _make_settings_mock()
        rl = _make_rate_limiter_mock()

        worker_fn = AsyncMock()

        runner = ScraperRunner(
            settings=settings,
            scraper_label='video',
            platform='youtube',
            num_processes=1,
            concurrency=3,
            metrics_port=9400,
            log_file='/dev/stdout',
            log_level='INFO',
            rate_limiter_factory=lambda s: rl,
        )
        await runner.run(worker_fn)

        worker_fn.assert_awaited_once()
        ctx: ScraperRunContext = (
            worker_fn.call_args[0][0]
        )
        self.assertIs(ctx.settings, settings)
        self.assertIs(ctx.client, mock_client)
        self.assertIs(ctx.rate_limiter, rl)
        rl.set_proxies.assert_called_once_with(
            settings.proxies,
        )

    @patch(
        'scrape_exchange.scraper_runner'
        '.start_http_server',
    )
    @patch(
        'scrape_exchange.scraper_runner'
        '.publish_config_metrics',
    )
    @patch(
        'scrape_exchange.scraper_runner'
        '.ScrapeExchangeRateLimiter',
    )
    @patch(
        'scrape_exchange.scraper_runner'
        '.ExchangeClient',
    )
    @patch(
        'scrape_exchange.scraper_runner'
        '.configure_logging',
    )
    async def test_drain_called_on_exit(
        self,
        mock_logging: MagicMock,
        mock_client_cls: MagicMock,
        mock_se_rl: MagicMock,
        mock_publish: MagicMock,
        mock_http_server: MagicMock,
    ) -> None:
        mock_client = AsyncMock()
        mock_client.drain_uploads = AsyncMock()
        mock_client_cls.setup = AsyncMock(
            return_value=mock_client,
        )

        settings = _make_settings_mock()
        rl = _make_rate_limiter_mock()

        async def worker_fn(
            ctx: ScraperRunContext,
        ) -> None:
            pass

        runner = ScraperRunner(
            settings=settings,
            scraper_label='video',
            platform='youtube',
            num_processes=1,
            concurrency=3,
            metrics_port=9400,
            log_file='/dev/stdout',
            log_level='INFO',
            rate_limiter_factory=lambda s: rl,
        )
        await runner.run(worker_fn)

        mock_client.drain_uploads\
            .assert_awaited_once()

    @patch(
        'scrape_exchange.scraper_runner'
        '.start_http_server',
    )
    @patch(
        'scrape_exchange.scraper_runner'
        '.publish_config_metrics',
    )
    @patch(
        'scrape_exchange.scraper_runner'
        '.ScrapeExchangeRateLimiter',
    )
    @patch(
        'scrape_exchange.scraper_runner'
        '.ExchangeClient',
    )
    @patch(
        'scrape_exchange.scraper_runner'
        '.configure_logging',
    )
    async def test_client_optional(
        self,
        mock_logging: MagicMock,
        mock_client_cls: MagicMock,
        mock_se_rl: MagicMock,
        mock_publish: MagicMock,
        mock_http_server: MagicMock,
    ) -> None:
        mock_client_cls.setup = AsyncMock(
            side_effect=Exception('no server'),
        )
        settings = _make_settings_mock()
        rl = _make_rate_limiter_mock()
        worker_fn = AsyncMock()

        runner = ScraperRunner(
            settings=settings,
            scraper_label='video',
            platform='youtube',
            num_processes=1,
            concurrency=3,
            metrics_port=9400,
            log_file='/dev/stdout',
            log_level='INFO',
            rate_limiter_factory=lambda s: rl,
            client_required=False,
        )
        # ScraperRunner.run logs a WARNING via the root logger
        # when the optional ExchangeClient setup fails.
        with self.assertLogs(level='WARNING'):
            await runner.run(worker_fn)

        worker_fn.assert_awaited_once()
        ctx: ScraperRunContext = (
            worker_fn.call_args[0][0]
        )
        self.assertIsNone(ctx.client)


if __name__ == '__main__':
    unittest.main()
