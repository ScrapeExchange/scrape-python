'''
Unit tests for TikTokSessionPool. The Camoufox layer
(CamoufoxTikTokApi, camoufox_launch_options, apply_camoufox_env) is
mocked; the live browser path is covered by the gated integration
test.
'''

import asyncio
import contextlib
import tempfile
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.tiktok.tiktok_rate_limiter import (
    TikTokRateLimiter,
)
from scrape_exchange.tiktok.tiktok_session_jar import (
    TikTokSessionJar,
)
from scrape_exchange.browser_memory import BrowserMemory
from scrape_exchange.tiktok.tiktok_metrics import (
    BROWSER_MEMORY_BYTES,
    BROWSER_PROCESSES,
)
from scrape_exchange.tiktok.tiktok_session_pool import (
    DIRECT_SESSION_PROXY,
    FIREFOX_MEMORY_PREFS,
    TikTokSessionPool,
    SessionUnavailable,
    _await_with_watchdog,
    _ensure_camoufox_runtime_libs,
    _missing_camoufox_runtime_libs,
    _proxy_key,
    _to_playwright_proxy,
)
from scrape_exchange.tiktok.tiktok_types import TikTokCallType

_CAMOUFOX_MOD: str = 'scrape_exchange.tiktok.tiktok_camoufox_api'


def _make_api(token: str = 'tok', has_session: bool = True) -> MagicMock:
    api: MagicMock = MagicMock()
    api.create_sessions = AsyncMock()
    api.close_sessions = AsyncMock()
    api.stop_playwright = AsyncMock()
    api.set_rate_limiter = MagicMock()
    session: MagicMock = MagicMock()
    session.ms_token = token
    session.page = MagicMock()
    session.page.goto = AsyncMock()
    api.get_session_cookies = AsyncMock(
        return_value={'msToken': token},
    )
    api.sessions = [session] if has_session else []
    return api


def _refresh_count(outcome: str) -> float:
    from scrape_exchange.tiktok.tiktok_metrics import (
        MS_TOKEN_REFRESH_TOTAL,
    )
    return MS_TOKEN_REFRESH_TOTAL.labels(
        platform='tiktok', scraper='test',
        outcome=outcome, worker_id='0',
    )._value.get()


@contextlib.contextmanager
def _patch_camoufox(apis: list, launch_opts: dict | None = None):
    '''Patch the Camoufox layer so each CamoufoxTikTokApi() returns
    the next api from `apis` in order. Yields the apply_camoufox_env
    mock for env-lifecycle assertions.'''
    it = iter(apis)
    with patch(
        f'{_CAMOUFOX_MOD}.CamoufoxTikTokApi',
        side_effect=lambda *a, **k: next(it),
    ), patch(
        f'{_CAMOUFOX_MOD}.camoufox_launch_options',
        return_value=launch_opts or {
            'executable_path': '/bin/camoufox',
            'args': [],
            'env': {'CAMOU_CONFIG_1': 'x'},
        },
    ), patch(f'{_CAMOUFOX_MOD}.apply_camoufox_env') as env_mock:
        with patch(
            'scrape_exchange.tiktok.tiktok_session_pool.'
            '_ensure_camoufox_runtime_libs',
        ), patch(
            'scrape_exchange.tiktok.tiktok_session_pool._start_playwright',
            new=AsyncMock(side_effect=lambda: _make_driver()),
        ):
            yield env_mock


def _make_driver() -> MagicMock:
    driver: MagicMock = MagicMock()
    driver.stop = AsyncMock()
    return driver


class TestCamoufoxRuntimeDeps(unittest.TestCase):

    def test_missing_runtime_libs_returns_packages(self) -> None:
        def _cdll(lib: str) -> None:
            if lib == 'libgtk-3.so.0':
                raise OSError('missing')

        with patch(
            'scrape_exchange.tiktok.tiktok_session_pool.'
            'platform.system',
            return_value='Linux',
        ), patch(
            'scrape_exchange.tiktok.tiktok_session_pool.ctypes.CDLL',
            side_effect=_cdll,
        ):
            self.assertEqual(
                _missing_camoufox_runtime_libs(),
                ['libgtk-3-0t64'],
            )

    def test_missing_runtime_libs_raises_install_command(self) -> None:
        with patch(
            'scrape_exchange.tiktok.tiktok_session_pool.'
            '_missing_camoufox_runtime_libs',
            return_value=['libgtk-3-0t64', 'libnss3'],
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                r'sudo apt-get update && sudo apt-get install -y '
                r'libgtk-3-0t64 libnss3',
            ):
                _ensure_camoufox_runtime_libs()


class TestProxyKey(unittest.TestCase):

    def test_distinguishes_ports_without_exposing_password(self) -> None:
        first: str = _proxy_key(
            'http://user:secret@192.168.1.16:3128',
        )
        second: str = _proxy_key(
            'http://user:secret@192.168.1.16:3129',
        )
        self.assertNotEqual(first, second)
        self.assertIn('192.168.1.16:3128', first)
        self.assertNotIn('secret', first)


class TestAwaitWithWatchdog(unittest.IsolatedAsyncioTestCase):

    async def test_touches_work_while_awaitable_is_pending(
        self,
    ) -> None:
        release: asyncio.Event = asyncio.Event()

        async def _slow() -> str:
            await release.wait()
            return 'done'

        watchdog: MagicMock = MagicMock()

        async def _release_soon() -> None:
            await asyncio.sleep(0.025)
            release.set()

        with patch(
            'scrape_exchange.tiktok.tiktok_session_pool.'
            'Watchdog',
        ) as wd:
            wd.get.return_value = watchdog
            releaser: asyncio.Task = asyncio.create_task(
                _release_soon(),
            )
            result: str = await _await_with_watchdog(
                _slow(), interval_seconds=0.01,
            )
            await releaser
        self.assertEqual(result, 'done')
        self.assertGreaterEqual(
            watchdog.touch_work.call_count, 2,
        )


class TestBootstrap(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.state_dir: str = self._tmp.name
        TikTokRateLimiter.reset()
        self.rl: TikTokRateLimiter = TikTokRateLimiter()
        self.rl.acquire = AsyncMock()

    def tearDown(self) -> None:
        self._tmp.cleanup()
        TikTokRateLimiter.reset()

    def _pool(self, proxies: list[str]) -> TikTokSessionPool:
        return TikTokSessionPool(
            proxies=proxies,
            state_dir=self.state_dir,
            ms_token_ttl_seconds=3600,
            rate_limiter=self.rl,
            scraper_label='test',
            api_call_type=TikTokCallType.CREATOR_API,
            bootstrap_timeout_ms=123000,
        )

    async def test_one_api_per_proxy_all_ready(self) -> None:
        proxies: list[str] = [
            'http://p1.example:8080', 'http://p2.example:8080',
        ]
        apis: list = [_make_api(), _make_api()]
        with _patch_camoufox(apis):
            pool = self._pool(proxies)
            await pool.bootstrap()
        self.assertEqual(
            sorted(pool.ready_proxies()), sorted(proxies),
        )
        self.assertEqual(pool.failed_proxies(), [])
        for api in apis:
            api.create_sessions.assert_awaited_once()

    async def test_empty_proxy_list_bootstraps_direct_session(
        self,
    ) -> None:
        api = _make_api(token='tok')
        with _patch_camoufox([api]) as env_mock:
            pool = self._pool([])
            await pool.bootstrap()

        self.assertEqual(pool.ready_proxies(), [DIRECT_SESSION_PROXY])
        api.create_sessions.assert_awaited_once()
        self.assertEqual(
            api.create_sessions.call_args.kwargs['proxies'],
            [None],
        )
        api.set_rate_limiter.assert_called_once_with(
            self.rl, DIRECT_SESSION_PROXY, TikTokCallType.CREATOR_API,
        )
        env_mock.assert_called_once()

    async def test_bootstrap_logs_per_proxy_and_summary(self) -> None:
        proxy = 'http://user:pass@p1.example:8080'
        api = _make_api(token='tok')
        with _patch_camoufox([api]), patch(
            'scrape_exchange.tiktok.tiktok_session_pool._LOGGER',
        ) as logger:
            pool = self._pool([proxy])
            await pool.bootstrap()

        info_calls = logger.info.call_args_list
        messages = [call.args[0] for call in info_calls]
        self.assertIn(
            'TikTok session pool bootstrap starting',
            messages,
        )
        self.assertIn(
            'TikTok proxy bootstrap starting',
            messages,
        )
        self.assertIn(
            'TikTok proxy bootstrap succeeded',
            messages,
        )
        self.assertIn(
            'TikTok session pool bootstrap complete',
            messages,
        )
        proxy_success = next(
            call for call in info_calls
            if call.args[0] == 'TikTok proxy bootstrap succeeded'
        )
        endpoint = proxy_success.kwargs['extra']['proxy_endpoint']
        self.assertTrue(endpoint.startswith('p1.example:8080#'))
        self.assertNotIn('user', endpoint)
        self.assertNotIn('pass', endpoint)
        self.assertTrue(
            proxy_success.kwargs['extra']['has_ms_token'],
        )
        summary = next(
            call for call in info_calls
            if call.args[0] == 'TikTok session pool bootstrap complete'
        )
        self.assertEqual(summary.kwargs['extra']['requested'], 1)
        self.assertEqual(summary.kwargs['extra']['ready'], 1)
        self.assertEqual(summary.kwargs['extra']['failed'], 0)

    async def test_partial_failure_excludes_only_failed(self) -> None:
        proxies: list[str] = [
            'http://p1.example:8080', 'http://p2.example:8080',
        ]
        good: MagicMock = _make_api()
        bad: MagicMock = _make_api()
        bad.create_sessions = AsyncMock(
            side_effect=RuntimeError('chromium crash'),
        )
        with _patch_camoufox([good, bad]):
            pool = self._pool(proxies)
            await pool.bootstrap()
        self.assertEqual(pool.ready_proxies(), [proxies[0]])
        self.assertEqual(pool.failed_proxies(), [proxies[1]])
        # the failed proxy's partial api must be cleaned up
        bad.stop_playwright.assert_awaited()

    async def test_proxy_affine_tokens(self) -> None:
        proxies: list[str] = [
            'http://p1.example:8080', 'http://p2.example:8080',
        ]
        jar = TikTokSessionJar(self.state_dir, ttl_seconds=3600)
        jar.set_token('p1.example:8080', 'AFFINE1')
        jar.flush()
        apis: list = [_make_api(), _make_api()]
        with _patch_camoufox(apis):
            pool = self._pool(proxies)
            await pool.bootstrap()
        self.assertEqual(
            apis[0].create_sessions.call_args.kwargs['ms_tokens'],
            ['AFFINE1'],
        )
        self.assertIsNone(
            apis[1].create_sessions.call_args.kwargs['ms_tokens'],
        )

    async def test_same_host_different_ports_use_distinct_tokens(
        self,
    ) -> None:
        proxies: list[str] = [
            'http://192.168.1.16:3128',
            'http://192.168.1.16:3129',
        ]
        jar = TikTokSessionJar(self.state_dir, ttl_seconds=3600)
        jar.set_token('192.168.1.16:3128', 'PORT3128')
        jar.set_token('192.168.1.16:3129', 'PORT3129')
        jar.flush()
        apis: list = [_make_api(), _make_api()]
        with _patch_camoufox(apis):
            pool = self._pool(proxies)
            await pool.bootstrap()
        self.assertEqual(
            apis[0].create_sessions.call_args.kwargs['ms_tokens'],
            ['PORT3128'],
        )
        self.assertEqual(
            apis[1].create_sessions.call_args.kwargs['ms_tokens'],
            ['PORT3129'],
        )

    async def test_persists_captured_token_by_endpoint(self) -> None:
        proxies: list[str] = ['http://p1.example:8080']
        apis: list = [_make_api(token='CAPTURED')]
        with _patch_camoufox(apis):
            pool = self._pool(proxies)
            await pool.bootstrap()
            await pool.shutdown()
        reopened = TikTokSessionJar(self.state_dir, ttl_seconds=3600)
        rec = reopened.get('p1.example:8080')
        self.assertIsNotNone(rec)
        self.assertEqual(rec.ms_token, 'CAPTURED')

    async def test_shutdown_closes_all_apis(self) -> None:
        proxies: list[str] = [
            'http://p1.example:8080', 'http://p2.example:8080',
        ]
        apis: list = [_make_api(), _make_api()]
        with _patch_camoufox(apis):
            pool = self._pool(proxies)
            await pool.bootstrap()
            await pool.shutdown()
        for api in apis:
            api.close_sessions.assert_awaited()

    async def test_rebuild_discards_token_and_replaces_api(self) -> None:
        proxy = 'http://p1.example:8080'
        first = _make_api(token='BLOCKED')
        second = _make_api(token='FRESH')
        with _patch_camoufox([first, second]):
            pool = self._pool([proxy])
            await pool.bootstrap()
            rebuilt = await pool.rebuild(proxy)

        self.assertTrue(rebuilt)
        first.close_sessions.assert_awaited()
        self.assertIs(pool._sessions[proxy].api, second)
        self.assertIsNone(
            second.create_sessions.await_args.kwargs['ms_tokens'],
        )

    async def test_failed_rebuild_retires_proxy(self) -> None:
        proxy = 'http://p1.example:8080'
        first = _make_api(token='BLOCKED')
        second = _make_api()
        second.create_sessions.side_effect = RuntimeError('blocked')
        with _patch_camoufox([first, second]):
            pool = self._pool([proxy])
            await pool.bootstrap()
            rebuilt = await pool.rebuild(proxy)

        self.assertFalse(rebuilt)
        self.assertNotIn(proxy, pool.ready_proxies())
        self.assertIn(proxy, pool.failed_proxies())

    async def test_quarantine_closes_session_and_deletes_token(
        self,
    ) -> None:
        proxy = 'http://p1.example:8080'
        first = _make_api(token='BLOCKED')
        with _patch_camoufox([first]):
            pool = self._pool([proxy])
            await pool.bootstrap()
            await pool.quarantine(proxy)

        first.close_sessions.assert_awaited()
        self.assertNotIn(proxy, pool.ready_proxies())
        self.assertIsNone(pool._jar.get(_proxy_key(proxy)))

    async def test_bootstrap_passes_proxy_objects_to_camoufox(
        self,
    ) -> None:
        proxies: list[str] = ['http://user:pass@proxy.example:8080']
        apis: list = [_make_api()]
        with _patch_camoufox(apis) as env_mock:
            pool = self._pool(proxies)
            await pool.bootstrap()
        kwargs = apis[0].create_sessions.call_args.kwargs
        self.assertEqual(kwargs['proxies'], [{
            'server': 'http://proxy.example:8080',
            'username': 'user',
            'password': 'pass',
        }])
        apis[0].set_rate_limiter.assert_called_once_with(
            self.rl, proxies[0], TikTokCallType.CREATOR_API,
        )
        self.assertEqual(kwargs['browser'], 'firefox')
        self.assertEqual(kwargs['timeout'], 123000)
        env_mock.assert_called()

    async def test_bootstrap_omits_webgl_prefs_by_default(self) -> None:
        launch_opts: dict = {
            'executable_path': '/bin/camoufox',
            'args': [],
            'env': {},
            'firefox_user_prefs': {'webgl.force-enabled': True},
        }
        apis: list = [_make_api()]
        with _patch_camoufox(apis, launch_opts):
            await self._pool(['http://p1.example:8080']).bootstrap()
        self.assertEqual(
            apis[0].create_sessions.call_args.kwargs['firefox_user_prefs'],
            FIREFOX_MEMORY_PREFS,
        )

    async def test_bootstrap_merges_memory_prefs_over_camoufox(
        self,
    ) -> None:
        launch_opts: dict = {
            'executable_path': '/bin/camoufox',
            'args': [],
            'env': {},
            'firefox_user_prefs': {
                'webgl.force-enabled': True,
                'dom.ipc.processCount': 16,
            },
        }
        apis: list = [_make_api()]
        with _patch_camoufox(apis, launch_opts):
            await TikTokSessionPool(
                proxies=['http://p1.example:8080'],
                state_dir=self.state_dir,
                ms_token_ttl_seconds=3600,
                rate_limiter=self.rl,
                scraper_label='test',
                api_call_type=TikTokCallType.CREATOR_API,
                browser_webgl=True,
            ).bootstrap()
        prefs: dict = (
            apis[0].create_sessions.call_args.kwargs['firefox_user_prefs']
        )
        self.assertTrue(prefs['webgl.force-enabled'])
        self.assertEqual(prefs['dom.ipc.processCount'], 16)
        self.assertEqual(prefs['media.autoplay.default'], 5)
        self.assertLessEqual(FIREFOX_MEMORY_PREFS.items(), prefs.items())

    async def test_bootstrap_passes_suppressed_resource_types(
        self,
    ) -> None:
        apis: list = [_make_api()]
        with _patch_camoufox(apis):
            pool: TikTokSessionPool = TikTokSessionPool(
                proxies=['http://p1.example:8080'],
                state_dir=self.state_dir,
                ms_token_ttl_seconds=3600,
                rate_limiter=self.rl,
                scraper_label='test',
                api_call_type=TikTokCallType.CREATOR_API,
                suppress_resource_types=['media', 'font'],
            )
            await pool.bootstrap()
        self.assertEqual(
            apis[0].create_sessions.call_args.kwargs[
                'suppress_resource_load_types'
            ],
            ['media', 'font'],
        )

    async def test_no_suppressed_types_passes_none(self) -> None:
        apis: list = [_make_api()]
        with _patch_camoufox(apis):
            await self._pool(['http://p1.example:8080']).bootstrap()
        self.assertIsNone(
            apis[0].create_sessions.call_args.kwargs[
                'suppress_resource_load_types'
            ],
        )

    async def test_proxies_share_one_playwright_driver(self) -> None:
        apis: list = [_make_api(), _make_api()]
        with _patch_camoufox(apis):
            pool = self._pool([
                'http://p1.example:8080', 'http://p2.example:8080',
            ])
            await pool.bootstrap()
            first = apis[0].create_sessions.call_args.kwargs['playwright']
            second = apis[1].create_sessions.call_args.kwargs['playwright']
            self.assertIs(first, second)
            await pool.shutdown()
        first.stop.assert_awaited_once()

    async def test_driver_restarts_when_no_session_is_alive(self) -> None:
        bad: MagicMock = _make_api()
        bad.create_sessions = AsyncMock(side_effect=RuntimeError('dead'))
        apis: list = [bad, _make_api()]
        with _patch_camoufox(apis):
            await self._pool([
                'http://p1.example:8080', 'http://p2.example:8080',
            ]).bootstrap()
        first = bad.create_sessions.call_args.kwargs['playwright']
        second = apis[1].create_sessions.call_args.kwargs['playwright']
        self.assertIsNot(first, second)
        first.stop.assert_awaited_once()

    async def test_driver_kept_while_other_sessions_live(self) -> None:
        apis: list = [_make_api(), _make_api(), _make_api()]
        proxies: list[str] = [
            'http://p1.example:8080', 'http://p2.example:8080',
        ]
        with _patch_camoufox(apis):
            pool = self._pool(proxies)
            await pool.bootstrap()
            await pool.rebuild(proxies[0])
        driver = apis[0].create_sessions.call_args.kwargs['playwright']
        self.assertIs(
            apis[2].create_sessions.call_args.kwargs['playwright'], driver,
        )
        driver.stop.assert_not_awaited()

    def _aging_pool(
        self, proxies: list[str], max_age: float,
    ) -> TikTokSessionPool:
        return TikTokSessionPool(
            proxies=proxies,
            state_dir=self.state_dir,
            ms_token_ttl_seconds=3600,
            rate_limiter=self.rl,
            scraper_label='test',
            api_call_type=TikTokCallType.CREATOR_API,
            browser_max_age_seconds=max_age,
        )

    @staticmethod
    def _age(pool: TikTokSessionPool, proxy: str, seconds: float) -> None:
        pool._sessions[proxy].created_at -= seconds

    async def test_recycle_replaces_old_browser_keeping_token(self) -> None:
        proxy: str = 'http://p1.example:8080'
        old: MagicMock = _make_api(token='tok')
        new: MagicMock = _make_api(token='tok')
        with _patch_camoufox([old, new]):
            pool = self._aging_pool([proxy], max_age=100)
            await pool.bootstrap()
            self._age(pool, proxy, 1000)
            await pool.refresh_tokens()
        self.assertIs(pool._sessions[proxy].api, new)
        self.assertEqual(
            new.create_sessions.call_args.kwargs['ms_tokens'], ['tok'],
        )
        old.close_sessions.assert_awaited()
        new.close_sessions.assert_not_awaited()
        self.assertEqual(pool.ready_proxies(), [proxy])

    async def test_young_browser_is_not_recycled(self) -> None:
        apis: list = [_make_api(), _make_api()]
        with _patch_camoufox(apis):
            pool = self._aging_pool(['http://p1.example:8080'], max_age=100)
            await pool.bootstrap()
            await pool.refresh_tokens()
        apis[1].create_sessions.assert_not_awaited()

    async def test_one_age_recycle_per_pass_oldest_first(self) -> None:
        proxies: list[str] = [
            'http://p1.example:8080', 'http://p2.example:8080',
        ]
        apis: list = [_make_api(), _make_api(), _make_api()]
        with _patch_camoufox(apis):
            pool = self._aging_pool(proxies, max_age=100)
            await pool.bootstrap()
            self._age(pool, proxies[0], 1000)
            self._age(pool, proxies[1], 5000)
            await pool.refresh_tokens()
        self.assertIs(pool._sessions[proxies[1]].api, apis[2])
        self.assertIs(pool._sessions[proxies[0]].api, apis[0])

    async def test_disconnected_browser_is_recycled(self) -> None:
        proxy: str = 'http://p1.example:8080'
        dead: MagicMock = _make_api()
        dead.browser.is_connected = MagicMock(return_value=False)
        fresh: MagicMock = _make_api()
        with _patch_camoufox([dead, fresh]):
            pool = self._aging_pool([proxy], max_age=0)
            await pool.bootstrap()
            await pool.refresh_tokens()
        self.assertIs(pool._sessions[proxy].api, fresh)
        dead.close_sessions.assert_awaited()

    async def test_failed_recycle_keeps_old_browser(self) -> None:
        proxy: str = 'http://p1.example:8080'
        old: MagicMock = _make_api()
        bad: MagicMock = _make_api()
        bad.create_sessions = AsyncMock(side_effect=RuntimeError('boom'))
        with _patch_camoufox([old, bad]):
            pool = self._aging_pool([proxy], max_age=100)
            await pool.bootstrap()
            self._age(pool, proxy, 1000)
            await pool.refresh_tokens()
        self.assertIs(pool._sessions[proxy].api, old)
        old.close_sessions.assert_not_awaited()
        bad.close_sessions.assert_awaited()
        self.assertEqual(pool.failed_proxies(), [])

    async def test_quarantine_during_recycle_drops_new_browser(self) -> None:
        proxy: str = 'http://p1.example:8080'
        old: MagicMock = _make_api()
        new: MagicMock = _make_api()
        with _patch_camoufox([old, new]):
            pool = self._aging_pool([proxy], max_age=100)
            await pool.bootstrap()

            async def _quarantine_then_launch(**_: object) -> None:
                await pool.quarantine(proxy)

            new.create_sessions = AsyncMock(
                side_effect=_quarantine_then_launch,
            )
            self.assertFalse(await pool._recycle(proxy, 'age'))
        self.assertNotIn(proxy, pool.ready_proxies())
        new.close_sessions.assert_awaited()

    async def test_bootstrap_records_browser_memory(self) -> None:
        usage: BrowserMemory = BrowserMemory(
            browser_bytes=300 * 1024 ** 2, browser_processes=2,
            driver_bytes=60 * 1024 ** 2, driver_processes=1,
        )
        with _patch_camoufox([_make_api()]), patch(
            'scrape_exchange.tiktok.tiktok_session_pool.browser_memory',
            return_value=usage,
        ):
            await self._pool(['http://p1.example:8080']).bootstrap()
        labels: dict[str, str] = {
            'platform': 'tiktok', 'scraper': 'test', 'worker_id': '0',
        }
        self.assertEqual(
            BROWSER_MEMORY_BYTES.labels(
                kind='browser', **labels,
            )._value.get(),
            300 * 1024 ** 2,
        )
        self.assertEqual(
            BROWSER_MEMORY_BYTES.labels(
                kind='driver', **labels,
            )._value.get(),
            60 * 1024 ** 2,
        )
        self.assertEqual(
            BROWSER_PROCESSES.labels(kind='browser', **labels)._value.get(),
            2,
        )

    async def test_bootstrap_is_rate_limited(self) -> None:
        proxy = 'http://p1.example:8080'
        api = _make_api()
        self.rl.acquire = AsyncMock()
        with _patch_camoufox([api]):
            pool = self._pool([proxy])
            await pool.bootstrap()
        self.rl.acquire.assert_awaited_once_with(
            TikTokCallType.BOOTSTRAP, proxy=proxy,
        )

    async def test_session_lease_does_not_double_charge_api(self) -> None:
        proxy = 'http://p1.example:8080'
        api = _make_api()
        self.rl.acquire = AsyncMock()
        with _patch_camoufox([api]):
            pool = self._pool([proxy])
            await pool.bootstrap()
            self.rl.acquire.reset_mock()
            async with pool.session_for(proxy):
                pass
        self.rl.acquire.assert_not_awaited()

    async def test_direct_api_gate_uses_configured_call_type(self) -> None:
        proxy = 'http://p1.example:8080'
        pool = self._pool([proxy])

        await pool.gate_api_request(proxy)

        self.rl.acquire.assert_awaited_once_with(
            TikTokCallType.CREATOR_API, proxy=proxy,
        )

    async def test_session_for_yields_proxy_api(self) -> None:
        proxies: list[str] = ['http://p1.example:8080']
        apis: list = [_make_api()]
        with _patch_camoufox(apis):
            pool = self._pool(proxies)
            await pool.bootstrap()
            async with pool.session_for(proxies[0]) as handle:
                self.assertIs(handle, apis[0])

    async def test_session_for_unavailable_proxy_raises(self) -> None:
        proxies: list[str] = ['http://p1.example:8080']
        bad: MagicMock = _make_api()
        bad.create_sessions = AsyncMock(
            side_effect=RuntimeError('crash'),
        )
        with _patch_camoufox([bad]):
            pool = self._pool(proxies)
            await pool.bootstrap()
            with self.assertRaises(SessionUnavailable):
                async with pool.session_for(proxies[0]):
                    pass

    async def test_no_multiplex(self) -> None:
        '''Two concurrent acquires for the same proxy serialise.'''
        proxies: list[str] = ['http://p1.example:8080']
        apis: list = [_make_api()]
        order: list[str] = []

        async def hold(pool: TikTokSessionPool, tag: str) -> None:
            async with pool.session_for(proxies[0]):
                order.append(f'enter:{tag}')
                await asyncio.sleep(0.05)
                order.append(f'exit:{tag}')

        with _patch_camoufox(apis):
            pool = self._pool(proxies)
            await pool.bootstrap()
            await asyncio.gather(hold(pool, 'a'), hold(pool, 'b'))

        self.assertIn(order, [
            ['enter:a', 'exit:a', 'enter:b', 'exit:b'],
            ['enter:b', 'exit:b', 'enter:a', 'exit:a'],
        ])


class TestRefresh(unittest.IsolatedAsyncioTestCase):
    '''Tests for TikTokSessionPool.refresh_tokens / run_refresh_loop.'''

    _PROXY: str = 'http://p1.example:8080'
    _IP: str = 'p1.example:8080'

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.state_dir: str = self._tmp.name
        TikTokRateLimiter.reset()
        self.rl: TikTokRateLimiter = TikTokRateLimiter()
        self.rl.acquire = AsyncMock()

    def tearDown(self) -> None:
        self._tmp.cleanup()
        TikTokRateLimiter.reset()

    def _pool(
        self,
        proxies: list[str] | None = None,
        refresh_fraction: float = 0.75,
    ) -> TikTokSessionPool:
        return TikTokSessionPool(
            proxies=proxies or [self._PROXY],
            state_dir=self.state_dir,
            ms_token_ttl_seconds=3600,
            rate_limiter=self.rl,
            scraper_label='test',
            api_call_type=TikTokCallType.CREATOR_API,
            refresh_fraction=refresh_fraction,
        )

    async def test_passive_refresh_persists_rotated_token(
        self,
    ) -> None:
        '''Passive path: browser rotated the cookie; jar is updated.'''
        apis: list = [_make_api(token='BOOT')]
        with _patch_camoufox(apis):
            pool = self._pool()
            await pool.bootstrap()
        apis[0].get_session_cookies = AsyncMock(
            return_value={'msToken': 'ROTATED'},
        )
        before_ok: float = _refresh_count('success')
        await pool.refresh_tokens()
        rec = pool._jar.get(self._IP)
        self.assertIsNotNone(rec)
        self.assertEqual(rec.ms_token, 'ROTATED')
        bound = pool._sessions[self._PROXY]
        self.assertEqual(bound.session.ms_token, 'ROTATED')
        apis[0].sessions[0].page.goto.assert_not_awaited()
        self.assertEqual(
            _refresh_count('success'), before_ok + 1,
        )

    async def test_passive_unchanged_token_preserves_captured_at(
        self,
    ) -> None:
        '''
        Cookie returns same value as stored: set_token not called,
        captured_at unchanged, goto not called, success counted.
        '''
        apis: list = [_make_api(token='SAME')]
        with _patch_camoufox(apis):
            pool = self._pool()
            await pool.bootstrap()
        captured_before: float = (
            pool._jar.get(self._IP).captured_at
        )
        before_ok: float = _refresh_count('success')
        await pool.refresh_tokens()
        rec = pool._jar.get(self._IP)
        self.assertEqual(rec.captured_at, captured_before)
        apis[0].sessions[0].page.goto.assert_not_awaited()
        self.assertEqual(
            _refresh_count('success'), before_ok + 1,
        )

    async def test_active_refresh_when_stale_navigates(
        self,
    ) -> None:
        '''
        When the stored token is stale (age >= fraction*ttl), the pool
        navigates to a TikTok URL (active path) before reading the
        cookie.
        '''
        apis: list = [_make_api(token='BOOT')]
        with _patch_camoufox(apis):
            pool = self._pool()
            await pool.bootstrap()
        # Force staleness: set captured_at to 0 so age is huge
        pool._jar._records[self._IP].captured_at = 0.0
        apis[0].get_session_cookies = AsyncMock(
            return_value={'msToken': 'FRESH'},
        )
        before_ok: float = _refresh_count('success')
        await pool.refresh_tokens()
        apis[0].sessions[0].page.goto.assert_awaited_once()
        rec = pool._jar.get(self._IP)
        self.assertIsNotNone(rec)
        self.assertEqual(rec.ms_token, 'FRESH')
        self.assertGreater(rec.captured_at, 0.0)
        self.assertEqual(
            _refresh_count('success'), before_ok + 1,
        )

    async def test_refresh_failure_records_metric(
        self,
    ) -> None:
        '''
        A failure in get_session_cookies records a failure metric and
        does not propagate the exception.
        '''
        apis: list = [_make_api(token='BOOT')]
        with _patch_camoufox(apis):
            pool = self._pool()
            await pool.bootstrap()
        apis[0].get_session_cookies = AsyncMock(
            side_effect=RuntimeError('boom'),
        )
        before_fail: float = _refresh_count('failure')
        await pool.refresh_tokens()  # must NOT raise
        self.assertEqual(
            _refresh_count('failure'), before_fail + 1,
        )

    async def test_refresh_skips_when_no_sessions(
        self,
    ) -> None:
        '''
        A pool with no bootstrapped sessions treats refresh_tokens as a
        no-op and neither raises nor increments counters.
        '''
        pool = self._pool()
        # Do NOT bootstrap — _sessions is empty
        before_ok: float = _refresh_count('success')
        before_fail: float = _refresh_count('failure')
        await pool.refresh_tokens()
        self.assertEqual(_refresh_count('success'), before_ok)
        self.assertEqual(_refresh_count('failure'), before_fail)

    async def test_run_refresh_loop_runs_and_cancels(
        self,
    ) -> None:
        '''
        run_refresh_loop executes at least one refresh pass before being
        cancelled.
        '''
        apis: list = [_make_api(token='BOOT')]
        with _patch_camoufox(apis):
            pool = self._pool()
            await pool.bootstrap()
        task: asyncio.Task = asyncio.create_task(
            pool.run_refresh_loop(0.01),
        )
        await asyncio.sleep(0.05)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        apis[0].get_session_cookies.assert_awaited()


class TestToPlaywrightProxy(unittest.TestCase):
    '''The pool must hand Playwright proxy *objects*, not the
    canonical proxy URL strings, or ``launch(proxy=...)`` rejects
    them.'''

    def test_proxy_with_auth(self) -> None:
        obj: dict[str, str] = _to_playwright_proxy(
            'http://user:pass@host.example:8080',
        )
        self.assertEqual(obj, {
            'server': 'http://host.example:8080',
            'username': 'user',
            'password': 'pass',
        })

    def test_proxy_without_auth(self) -> None:
        obj: dict[str, str] = _to_playwright_proxy(
            'http://proxy.example:3128',
        )
        self.assertEqual(obj, {'server': 'http://proxy.example:3128'})
        self.assertNotIn('username', obj)
        self.assertNotIn('password', obj)


if __name__ == '__main__':
    unittest.main()
