'''OnlyFans uses the shared logging and Prometheus conventions.'''

import asyncio
import unittest
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

from prometheus_client import REGISTRY

from scrape_exchange.onlyfans.onlyfans_browser import ProfileBlockedError
from scrape_exchange.onlyfans.onlyfans_creator import extract_profile
from scrape_exchange.onlyfans.settings import OnlyFansScraperSettings
from tests.unit import test_onlyfans_queue as queue_tests
from tests.unit.test_onlyfans_creator import public_profile
from tools import of_creator_scrape as scraper


def sample(name: str, **labels: str) -> float:
    return sum(
        item.value
        for metric in REGISTRY.collect() for item in metric.samples
        if item.name == name
        and all(item.labels.get(key) == value for key, value in labels.items())
    )


class TestOnlyFansStartup(unittest.TestCase):
    def test_shared_logging_and_metrics_startup(self) -> None:
        settings: OnlyFansScraperSettings = OnlyFansScraperSettings(
            _env_file=None, _cli_parse_args=[], username='example',
            proxies_env=None, proxy_files=None, metrics_port=9920,
            log_file='/dev/stdout', log_level='DEBUG', log_format='text',
        )
        with (
            patch.object(scraper, 'OnlyFansScraperSettings',
                         return_value=settings),
            patch.object(scraper, 'configure_logging') as configure,
            patch.object(scraper, 'start_metrics_server') as server,
            patch.object(scraper, 'scrape_creators',
                         AsyncMock(return_value=0)),
        ):
            scraper.main()
        configure.assert_called_once_with(
            level='DEBUG', filename='/dev/stdout', log_format='text',
        )
        server.assert_called_once_with(9920)
        self.assertEqual(sample(
            'scraper_num_processes', platform='onlyfans',
            scraper='onlyfans_creator', role='worker',
        ), 1)

    def test_onlyfans_logging_environment_overrides_shared_values(self) -> None:
        with patch.dict('os.environ', {
            'ONLYFANS_CREATOR_LOG_LEVEL': 'debug', 'LOG_LEVEL': 'ERROR',
            'ONLYFANS_CREATOR_LOG_FILE': '/dev/stdout', 'LOG_FILE': '/dev/null',
        }):
            settings: OnlyFansScraperSettings = OnlyFansScraperSettings(
                _env_file=None, _cli_parse_args=[],
                proxies_env=None, proxy_files=None,
            )
        self.assertEqual(settings.log_level, 'DEBUG')
        self.assertEqual(settings.log_file, '/dev/stdout')


class TestOnlyFansWorkerMetrics(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.fixture: queue_tests.TestOnlyFansQueue = (
            queue_tests.TestOnlyFansQueue()
        )
        await self.fixture.asyncSetUp()

    async def asyncTearDown(self) -> None:
        await self.fixture.asyncTearDown()

    async def test_worker_outcomes_and_sleep_are_measured(self) -> None:
        labels: dict[str, str] = {
            'platform': 'onlyfans', 'scraper': 'onlyfans_creator',
            'worker_id': str(self.fixture.settings.worker_id),
        }
        before: float = sample('scrapes_completed_total', **labels)
        await (
            self.fixture
            .test_worker_saves_due_creator_then_waits_for_future_work()
        )
        self.assertEqual(
            sample('scrapes_completed_total', **labels) - before, 1,
        )
        self.assertGreater(sample('scrape_duration_seconds_count',
                                  **labels), 0)
        self.assertEqual(sample('worker_sleep_seconds', **labels), 0)

    async def test_batch_counts_attempts_and_redacts_proxy_credentials(
        self,
    ) -> None:
        labels: dict[str, str] = {
            'platform': 'onlyfans', 'scraper': 'onlyfans_creator',
        }
        completed: float = sample('scrapes_completed_total', **labels)
        failed: float = sample('scrape_failures_total', **labels)
        written: float = sample('scrape_records_written_total', **labels)
        proxy: str = 'http://test-user:private-test-token@localhost:8888'
        object.__setattr__(self.fixture.settings, 'proxies', [proxy])

        @asynccontextmanager
        async def browser(*args: object) -> AsyncIterator[MagicMock]:
            yield self.fixture.context

        with (
            patch.object(scraper, 'anonymous_browser', browser),
            patch.object(scraper, 'OnlyFansRateLimiter') as limiter,
            patch.object(scraper, 'fetch_profile', AsyncMock(side_effect=[
                extract_profile(public_profile(), 'example'),
                ProfileBlockedError('blocked'),
            ])),
        ):
            limiter.return_value.aclose = AsyncMock()
            result: int = await scraper.scrape_creators(
                ['example', 'blocked', 'skipped'], self.fixture.settings,
            )
        self.assertEqual(result, 2)
        self.assertEqual(
            sample('scrapes_completed_total', **labels) - completed, 1,
        )
        self.assertEqual(sample('scrape_failures_total', **labels) - failed, 1)
        self.assertEqual(
            sample('scrape_records_written_total', **labels) - written, 1,
        )
        exposed: str = str([
            item.labels
            for metric in REGISTRY.collect() for item in metric.samples
            if item.labels.get('platform') == 'onlyfans'
        ])
        self.assertNotIn('private-test-token', exposed)
        self.assertNotIn('test-user', exposed)

    async def test_browser_failure_records_one_failure_and_retry(self) -> None:
        labels: dict[str, str] = {
            'platform': 'onlyfans', 'scraper': 'onlyfans_creator',
        }
        failed: float = sample('scrape_failures_total', **labels)
        retries: float = sample('scrape_retry_total', **labels)
        await (
            self.fixture
            .test_browser_startup_failure_retains_creator_for_retry()
        )
        self.assertEqual(sample('scrape_failures_total', **labels) - failed, 1)
        self.assertEqual(sample('scrape_retry_total', **labels) - retries, 1)

    async def test_idle_sleep_gauge_is_visible_and_reset_on_cancellation(
        self,
    ) -> None:
        labels: dict[str, str] = {
            'platform': 'onlyfans', 'scraper': 'onlyfans_creator',
        }

        async def sleep(delay: float) -> None:
            self.assertEqual(sample('worker_sleep_seconds', **labels), 60)
            raise asyncio.CancelledError

        with (
            patch.object(scraper, 'sleep', sleep),
            self.assertRaises(asyncio.CancelledError),
        ):
            await scraper.queue_worker(
                [None], self.fixture.queue, self.fixture.fm,
                self.fixture.settings, self.fixture.limiter, self.fixture.owner,
            )
        self.assertEqual(sample('worker_sleep_seconds', **labels), 0)
