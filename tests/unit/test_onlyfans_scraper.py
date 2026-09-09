'''Browser boundaries, proxy configuration and compressed output.'''

import asyncio
import json
import tempfile
import unittest
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.onlyfans.endpoints import PROFILE_BASE_URL
from scrape_exchange.onlyfans.onlyfans_browser import (
    ProfileBlockedError,
    ProfileUnavailableError,
    anonymous_browser,
    browser_proxy,
    fetch_profile,
)
from scrape_exchange.onlyfans.onlyfans_creator import OnlyFansCreator
from scrape_exchange.onlyfans.onlyfans_rate_limiter import (
    OnlyFansCallType,
    OnlyFansRateLimiter,
)
from scrape_exchange.onlyfans.settings import OnlyFansScraperSettings
from tests.unit.test_onlyfans_creator import public_profile
from tools.of_creator_scrape import load_creators, save_creator, scrape_creators


def settings(**overrides: Any) -> OnlyFansScraperSettings:
    return OnlyFansScraperSettings(
        _env_file=None, _cli_parse_args=[], redis_dsn='',
        rate_limiter_state_dir='', proxy_files=None, proxies_env=None,
        **overrides,
    )


class TestOnlyFansSettings(unittest.TestCase):
    def test_existing_proxy_file_loader_and_cli(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path: Path = Path(directory) / 'proxies.txt'
            path.write_text('http://localhost:8888\nhttp://localhost:8889\n')
            with patch.dict('os.environ', {}, clear=True):
                config: OnlyFansScraperSettings = OnlyFansScraperSettings(
                    _env_file=None, _cli_parse_args=[
                        '--username', '@Example', '--proxy-files', str(path),
                        '--creator-rpm', '3',
                    ],
                )
            self.assertEqual(list(config.proxies), [
                'http://localhost:8888', 'http://localhost:8889',
            ])
            self.assertEqual(load_creators(config), ['example'])
            self.assertEqual(config.creator_rpm, 3)

    def test_proxy_credentials_go_to_browser_not_server_url(self) -> None:
        self.assertEqual(browser_proxy('http://u:p%40ss@localhost:8888'), {
            'server': 'http://localhost:8888',
            'username': 'u', 'password': 'p@ss',
        })
        self.assertIsNone(browser_proxy(None))
        with self.assertRaises(ValueError):
            browser_proxy('local://localhost')

    def test_creator_file_deduplicates_and_preserves_order(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path: Path = Path(directory) / 'creators.txt'
            path.write_text('# creators\n@Example\nexample\nsecond\n')
            self.assertEqual(load_creators(settings(creator_file=path)), [
                'example', 'second',
            ])
        with self.assertRaises(ValueError):
            load_creators(settings())


class TestOnlyFansBrowser(unittest.IsolatedAsyncioTestCase):
    async def test_browser_launch_uses_configured_proxy(self) -> None:
        driver: MagicMock = MagicMock()
        browser: MagicMock = MagicMock()
        browser.new_context = AsyncMock(return_value=self.context)
        browser.close = AsyncMock()
        driver.firefox.launch = AsyncMock(return_value=browser)
        manager: MagicMock = MagicMock()
        manager.__aenter__ = AsyncMock(return_value=driver)
        manager.__aexit__ = AsyncMock(return_value=False)
        with (
            patch(
                'scrape_exchange.onlyfans.onlyfans_browser.async_playwright',
                return_value=manager,
            ),
            patch(
                'scrape_exchange.onlyfans.onlyfans_browser.'
                'camoufox_launch_options',
                return_value={'executable_path': '/tmp/test-browser'},
            ),
        ):
            async with anonymous_browser(
                'http://u:p%40ss@localhost:8888', settings(),
            ) as context:
                self.assertIs(context, self.context)
        self.assertEqual(driver.firefox.launch.await_args.kwargs['proxy'], {
            'server': 'http://localhost:8888',
            'username': 'u', 'password': 'p@ss',
        })
        self.assertNotIn('storage_state', browser.new_context.await_args.kwargs)
        self.assertTrue(browser.close.await_count)

    def setUp(self) -> None:
        self.proxy: str = 'http://localhost:8888'
        self.limiter: MagicMock = MagicMock()
        self.limiter.acquire = AsyncMock()
        self.limiter.penalise = AsyncMock()
        self.page: MagicMock = MagicMock()
        self.page.url = f'{PROFILE_BASE_URL}/example'
        self.page.main_frame = object()
        self.page.route = AsyncMock()
        self.page.unroute = AsyncMock()
        self.page.close = AsyncMock()
        self.context: MagicMock = MagicMock()
        self.context.new_page = AsyncMock(return_value=self.page)
        self.status: int = 200
        self.payload: object = public_profile()
        self.routes: list[MagicMock] = []
        self.page.goto = AsyncMock(side_effect=self.navigate)

    async def navigate(self, url: str, **kwargs: Any) -> SimpleNamespace:
        gate: Any = self.page.route.call_args.args[1]
        path: str
        for path, kind in ((url, 'document'), (
            f'{PROFILE_BASE_URL}/api2/v2/users/example', 'fetch',
        )):
            route: MagicMock = MagicMock()
            route.request.url = path
            route.request.resource_type = kind
            route.request.frame = self.page.main_frame
            route.continue_ = AsyncMock()
            route.abort = AsyncMock()
            self.routes.append(route)
            await gate(route)
            if route.abort.called:
                return SimpleNamespace(status=200)
        response: MagicMock = MagicMock()
        response.url = f'{PROFILE_BASE_URL}/api2/v2/users/example'
        response.request.method = 'GET'
        response.status = self.status
        response.headers = {}
        response.body = AsyncMock(
            return_value=json.dumps(self.payload).encode(),
        )
        listener: Any = self.page.on.call_args.args[1]
        listener(response)
        return SimpleNamespace(status=200)

    async def test_profile_requests_use_bound_proxy_and_save_round_trip(
        self,
    ) -> None:
        creator: OnlyFansCreator = await fetch_profile(
            self.context, 'example', self.limiter, self.proxy, settings(),
        )
        self.assertEqual(creator.like_count, 500)
        self.assertEqual(self.limiter.acquire.await_args_list[0].args, (
            OnlyFansCallType.CREATOR,
        ))
        self.assertEqual(self.limiter.acquire.await_args_list[1].args, (
            OnlyFansCallType.DATA,
        ))
        for call in self.limiter.acquire.await_args_list:
            self.assertEqual(call.kwargs['proxy'], self.proxy)
        self.assertTrue(self.page.close.await_count)
        with tempfile.TemporaryDirectory() as directory:
            fm: AssetFileManagement = AssetFileManagement(directory)
            await save_creator(creator, fm)
            record: dict[str, Any] = await fm.read_file(
                'onlyfans-creator-example.json.br',
            )
            self.assertEqual(record['like_count'], 500)
            self.assertEqual(record['subscription_status'], 'paid')

    async def test_rate_limiter_failure_aborts_requests_and_closes_page(
        self,
    ) -> None:
        self.limiter.acquire.side_effect = ConnectionError('backend failed')
        with self.assertRaises(ConnectionError):
            await fetch_profile(
                self.context, 'example', self.limiter, self.proxy, settings(),
            )
        self.assertFalse(self.routes[0].continue_.called)
        self.assertTrue(self.page.close.await_count)

    async def test_blocked_response_penalises_proxy_and_never_emits_profile(
        self,
    ) -> None:
        self.status = 429
        with self.assertRaises(ProfileBlockedError):
            await fetch_profile(
                self.context, 'example', self.limiter, self.proxy, settings(),
            )
        self.assertTrue(self.limiter.penalise.await_count)
        for call in self.limiter.penalise.await_args_list:
            self.assertEqual(call.kwargs['proxy'], self.proxy)

    async def test_missing_profile_is_distinct_from_blocked(self) -> None:
        self.status = 404
        with self.assertRaises(ProfileUnavailableError):
            await fetch_profile(
                self.context, 'example', self.limiter, self.proxy, settings(),
            )
        self.assertFalse(self.limiter.penalise.called)

    async def test_timeout_closes_page_without_a_profile(self) -> None:
        self.page.goto.side_effect = None
        self.page.goto.return_value = SimpleNamespace(status=200)
        with self.assertRaises(TimeoutError):
            await fetch_profile(
                self.context, 'example', self.limiter, self.proxy,
                settings(profile_timeout_seconds=0.02),
            )
        self.assertTrue(self.page.close.await_count)

    async def test_wrong_profile_and_login_redirect_are_not_saved(self) -> None:
        self.payload = public_profile(username='other')
        with self.assertRaises(ValueError):
            await fetch_profile(
                self.context, 'example', self.limiter, self.proxy, settings(),
            )
        self.payload = public_profile()
        self.page.url = f'{PROFILE_BASE_URL}/login'
        with self.assertRaises(ProfileBlockedError):
            await fetch_profile(
                self.context, 'example', self.limiter, self.proxy, settings(),
            )


class TestOnlyFansLimiter(unittest.IsolatedAsyncioTestCase):
    async def test_metrics_do_not_expose_proxy_credentials(self) -> None:
        from prometheus_client import REGISTRY
        limiter: OnlyFansRateLimiter = OnlyFansRateLimiter(settings())
        await limiter.acquire(
            OnlyFansCallType.DATA,
            'http://test-user:private-test-token@localhost:8888',
        )
        labels: list[str] = [
            str(sample.labels)
            for metric in REGISTRY.collect() for sample in metric.samples
            if sample.labels.get('platform') == 'onlyfans'
        ]
        self.assertTrue(labels)
        self.assertFalse(any('private-test-token' in label for label in labels))

    async def test_file_state_does_not_penalise_other_platforms(self) -> None:
        from scrape_exchange.twitch.settings import TwitchScraperSettings
        from scrape_exchange.twitch.twitch_rate_limiter import (
            TwitchCallType,
            TwitchRateLimiter,
        )
        with tempfile.TemporaryDirectory() as directory:
            config: OnlyFansScraperSettings = settings()
            config.rate_limiter_state_dir = directory
            onlyfans: OnlyFansRateLimiter = OnlyFansRateLimiter(config)
            twitch: TwitchRateLimiter = TwitchRateLimiter(TwitchScraperSettings(
                _env_file=None, _cli_parse_args=[], redis_dsn='',
                rate_limiter_state_dir=directory,
                creator_disable_proxies=True,
            ))
            await onlyfans.penalise(OnlyFansCallType.CREATOR, None, 60)
            async with asyncio.timeout(2):
                await twitch.acquire(TwitchCallType.CREATOR)

    async def test_redis_backend_shares_penalties_and_does_not_fall_back(
        self,
    ) -> None:
        import fakeredis.aioredis

        client: fakeredis.aioredis.FakeRedis = fakeredis.aioredis.FakeRedis(
            decode_responses=True,
        )
        config: OnlyFansScraperSettings = settings()
        config.redis_dsn = 'redis://localhost:6379/0'
        with patch('scrape_exchange.rate_limiter.redis_from_url',
                   return_value=client):
            first: OnlyFansRateLimiter = OnlyFansRateLimiter(config)
            second: OnlyFansRateLimiter = OnlyFansRateLimiter(config)
        try:
            await first.penalise(OnlyFansCallType.DATA, None, 60)
            with self.assertRaises(TimeoutError):
                async with asyncio.timeout(0.03):
                    await second.acquire(OnlyFansCallType.DATA)
            self.assertTrue(await client.keys('rl:onlyfans:*'))
            with (
                patch.object(client, 'evalsha', side_effect=ConnectionError),
                self.assertLogs('scrape_exchange.rate_limiter', level='WARNING'),
                self.assertRaises(ConnectionError),
            ):
                await second.acquire(OnlyFansCallType.DATA)
        finally:
            await client.aclose()

    async def test_file_backend_shares_proxy_penalty_between_instances(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config: OnlyFansScraperSettings = settings()
            config.rate_limiter_state_dir = directory
            first: OnlyFansRateLimiter = OnlyFansRateLimiter(config)
            second: OnlyFansRateLimiter = OnlyFansRateLimiter(config)
            await first.penalise(
                OnlyFansCallType.CREATOR, 'http://localhost:8888', 30,
            )
            with self.assertRaises(TimeoutError):
                async with asyncio.timeout(0.03):
                    await second.acquire(
                        OnlyFansCallType.CREATOR, 'http://localhost:8888',
                    )
            async with asyncio.timeout(2):
                await second.acquire(
                    OnlyFansCallType.CREATOR, 'http://localhost:8889',
                )


class TestOnlyFansBatch(unittest.IsolatedAsyncioTestCase):
    async def test_all_proxies_are_used_and_failures_do_not_write_files(
        self,
    ) -> None:
        calls: list[tuple[str, str | None]] = []
        active: int = 0
        peak: int = 0

        @asynccontextmanager
        async def browser(
            proxy: str | None, config: OnlyFansScraperSettings,
        ) -> AsyncIterator[object]:
            nonlocal active, peak
            active += 1
            peak = max(active, peak)
            try:
                yield proxy
            finally:
                active -= 1

        async def fetch(
            context: object, username: str, limiter: OnlyFansRateLimiter,
            proxy: str | None, config: OnlyFansScraperSettings,
        ) -> OnlyFansCreator:
            from scrape_exchange.onlyfans.onlyfans_creator import (
                extract_profile,
            )
            self.assertEqual(context, proxy)
            calls.append((username, proxy))
            if username == 'blocked':
                raise ProfileBlockedError('blocked')
            return extract_profile(public_profile(username=username), username)

        with tempfile.TemporaryDirectory() as directory:
            config: OnlyFansScraperSettings = settings(
                creator_data_directory=directory, concurrency=1,
            )
            object.__setattr__(config, 'proxies', [
                'http://localhost:8888', 'http://localhost:8889',
            ])
            with (
                patch('tools.of_creator_scrape.anonymous_browser', browser),
                patch('tools.of_creator_scrape.fetch_profile', fetch),
            ):
                failed: int = await scrape_creators(
                    ['first', 'blocked', 'third', 'skipped'], config,
                )
            self.assertEqual(failed, 2)
            self.assertEqual(peak, 1)
            self.assertEqual(calls, [
                ('first', 'http://localhost:8888'),
                ('third', 'http://localhost:8888'),
                ('blocked', 'http://localhost:8889'),
            ])
            self.assertEqual(sorted(
                path.name for path in Path(directory).glob('*.json.br')
            ), [
                'onlyfans-creator-first.json.br',
                'onlyfans-creator-third.json.br',
            ])
