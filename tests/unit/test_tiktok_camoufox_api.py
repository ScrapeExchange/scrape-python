'''
Unit tests for CamoufoxTikTokApi — the TikTokApi subclass that runs
signing and the data fetch in Camoufox's main world so webmssdk's
hooked fetch injects X-Gnarly. The page is mocked; the main-world
behaviour is exercised live in the gated integration test.
'''

import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from playwright.async_api import Error as PlaywrightError

from scrape_exchange.tiktok.tiktok_camoufox_api import (
    CamoufoxTikTokApi,
    apply_camoufox_env,
    camoufox_launch_options,
    _endpoint_label,
)
from scrape_exchange.tiktok.tiktok_metrics import (
    API_CALL_DURATION_SECONDS,
    API_CALL_TOTAL,
)
from scrape_exchange.tiktok.tiktok_types import TikTokCallType


def _bootstrap_api(goto_side_effect: object) -> tuple:
    '''Build a CamoufoxTikTokApi with a fully-mocked browser/context/
    page whose ``goto`` uses *goto_side_effect*.'''
    api: CamoufoxTikTokApi = CamoufoxTikTokApi()
    context: MagicMock = MagicMock()
    page: MagicMock = MagicMock()
    page.goto = AsyncMock(side_effect=goto_side_effect)
    page.wait_for_load_state = AsyncMock()
    page.close = AsyncMock()
    page.mouse.move = AsyncMock()
    page.once = MagicMock()
    page.set_default_navigation_timeout = MagicMock()
    context.new_page = AsyncMock(return_value=page)
    context.add_cookies = AsyncMock()
    context.close = AsyncMock()
    api.browser = MagicMock()
    api.browser.new_context = AsyncMock(return_value=context)
    api.get_session_cookies = AsyncMock(
        return_value={'msToken': 'BOOT'},
    )
    api._TikTokApi__set_session_params = AsyncMock()
    return api, page


def _api_with_page(evaluate_side_effect: list) -> tuple:
    api: CamoufoxTikTokApi = CamoufoxTikTokApi()
    session: MagicMock = MagicMock()
    session.page = MagicMock()
    session.page.evaluate = AsyncMock(side_effect=evaluate_side_effect)
    session.page.goto = AsyncMock()
    api.sessions = [session]
    return api, session


def _api_call_count(
    *,
    scraper: str,
    endpoint: str,
    outcome: str,
    worker_id: str = '0',
) -> float:
    return API_CALL_TOTAL.labels(
        platform='tiktok',
        scraper=scraper,
        endpoint=endpoint,
        outcome=outcome,
        worker_id=worker_id,
    )._value.get()


def _api_call_latency_count(
    *,
    scraper: str,
    endpoint: str,
    worker_id: str = '0',
) -> float:
    child = API_CALL_DURATION_SECONDS.labels(
        platform='tiktok',
        scraper=scraper,
        endpoint=endpoint,
        worker_id=worker_id,
    )
    for metric in child.collect():
        for sample in metric.samples:
            if sample.name.endswith('_count'):
                return float(sample.value)
    raise AssertionError('histogram count sample not found')


class TestCreateSession(unittest.IsolatedAsyncioTestCase):

    async def test_bootstrap_waits_for_domcontentloaded(self) -> None:
        api: CamoufoxTikTokApi = CamoufoxTikTokApi()
        context: MagicMock = MagicMock()
        page: MagicMock = MagicMock()
        page.goto = AsyncMock()
        page.wait_for_load_state = AsyncMock()
        page.close = AsyncMock()
        page.mouse.move = AsyncMock()
        page.once = MagicMock()
        page.set_default_navigation_timeout = MagicMock()
        context.new_page = AsyncMock(return_value=page)
        context.add_cookies = AsyncMock()
        context.close = AsyncMock()
        api.browser = MagicMock()
        api.browser.new_context = AsyncMock(return_value=context)
        api.get_session_cookies = AsyncMock(
            return_value={'msToken': 'BOOT'},
        )
        api._TikTokApi__set_session_params = AsyncMock()

        with patch(
            'scrape_exchange.tiktok.tiktok_camoufox_api.'
            'stealth_async',
            new=AsyncMock(),
        ), patch(
            'scrape_exchange.tiktok.tiktok_camoufox_api.'
            'asyncio.sleep',
            new=AsyncMock(),
        ):
            await api._TikTokApi__create_session(
                proxy={'server': 'http://p:8080'},
                timeout=123000,
            )

        page.wait_for_load_state.assert_awaited_once_with(
            'domcontentloaded',
        )
        page.set_default_navigation_timeout.assert_called_once_with(
            123000,
        )
        self.assertEqual(api.sessions[0].ms_token, 'BOOT')

    async def test_bootstrap_tolerates_ns_binding_aborted(
        self,
    ) -> None:
        '''A bootstrap goto that aborts with NS_BINDING_ABORTED
        (TikTok client-navigated and superseded our navigation) must
        not fail the session — the page is still heading to TikTok and
        wait_for_load_state gates readiness.'''
        api: CamoufoxTikTokApi
        api, _ = _bootstrap_api([
            PlaywrightError(
                'Page.goto: NS_BINDING_ABORTED; maybe frame was '
                'detached?',
            ),
            None,
        ])
        with patch(
            'scrape_exchange.tiktok.tiktok_camoufox_api.'
            'stealth_async', new=AsyncMock(),
        ), patch(
            'scrape_exchange.tiktok.tiktok_camoufox_api.'
            'asyncio.sleep', new=AsyncMock(),
        ):
            await api._TikTokApi__create_session(
                proxy={'server': 'http://p:8080'}, timeout=90000,
            )
        self.assertEqual(len(api.sessions), 1)
        self.assertEqual(api.sessions[0].ms_token, 'BOOT')

    async def test_bootstrap_propagates_non_abort_goto_error(
        self,
    ) -> None:
        '''A genuine navigation error (e.g. connection refused, or a
        navigation timeout) must still fail the session.'''
        api: CamoufoxTikTokApi
        api, _ = _bootstrap_api(
            PlaywrightError('Page.goto: NS_ERROR_CONNECTION_REFUSED'),
        )
        with patch(
            'scrape_exchange.tiktok.tiktok_camoufox_api.'
            'stealth_async', new=AsyncMock(),
        ), patch(
            'scrape_exchange.tiktok.tiktok_camoufox_api.'
            'asyncio.sleep', new=AsyncMock(),
        ):
            with self.assertRaises(PlaywrightError):
                await api._TikTokApi__create_session(
                    proxy={'server': 'http://p:8080'}, timeout=90000,
                )
        self.assertEqual(len(api.sessions), 0)


class TestEntityOwnership(unittest.TestCase):

    def test_entities_remain_bound_to_the_api_that_created_them(
        self,
    ) -> None:
        first: CamoufoxTikTokApi = CamoufoxTikTokApi()
        first_user = first.user(username='first')
        second: CamoufoxTikTokApi = CamoufoxTikTokApi()
        second_user = second.user(username='second')

        self.assertIs(first_user.parent, first)
        self.assertIs(second_user.parent, second)

    def test_nested_entity_factories_bind_to_the_same_api(self) -> None:
        first: CamoufoxTikTokApi = CamoufoxTikTokApi()
        second: CamoufoxTikTokApi = CamoufoxTikTokApi()

        video = first.video(id='123')
        playlist = first.playlist(id='456')

        self.assertIs(video.parent, first)
        self.assertIs(playlist.parent, first)
        self.assertIsNot(video.parent, second)


class TestGenerateXBogus(unittest.IsolatedAsyncioTestCase):

    async def test_returns_frontiersign_result_in_main_world(
        self,
    ) -> None:
        # readiness -> True, then frontierSign -> result.
        api, session = _api_with_page([
            True, {'X-Bogus': 'abc123'},
        ])
        result = await api.generate_x_bogus(
            'https://www.tiktok.com/api/user/detail/?x=1',
            session_index=0,
        )
        self.assertEqual(result, {'X-Bogus': 'abc123'})
        # Both evaluate calls must target the main world.
        for call in session.page.evaluate.await_args_list:
            self.assertTrue(call.args[0].startswith('mw:'))
        # The signing call must invoke frontierSign.
        self.assertIn(
            'frontierSign',
            session.page.evaluate.await_args_list[-1].args[0],
        )

    async def test_retries_after_navigation_then_succeeds(
        self,
    ) -> None:
        # not-ready -> goto -> ready -> frontierSign.
        api, session = _api_with_page([
            False, True, {'X-Bogus': 'z'},
        ])
        result = await api.generate_x_bogus(
            'https://www.tiktok.com/api/user/detail/', session_index=0,
        )
        self.assertEqual(result, {'X-Bogus': 'z'})
        session.page.goto.assert_awaited_once()

    async def test_raises_when_signer_never_appears(self) -> None:
        # readiness False for all 5 attempts.
        api, session = _api_with_page([False] * 5)
        with self.assertRaises(TimeoutError):
            await api.generate_x_bogus(
                'https://www.tiktok.com/api/user/detail/',
                session_index=0,
            )
        self.assertEqual(session.page.goto.await_count, 5)


class TestRunFetchScript(unittest.IsolatedAsyncioTestCase):

    def test_endpoint_label_classifies_common_tiktok_urls(self) -> None:
        self.assertEqual(
            _endpoint_label('https://www.tiktok.com/api/user/detail/'),
            'user_info',
        )
        self.assertEqual(
            _endpoint_label(
                'https://www.tiktok.com/api/post/item_list/',
            ),
            'user_videos',
        )
        self.assertEqual(
            _endpoint_label('https://www.tiktok.com/api/item/detail/'),
            'video_info',
        )
        self.assertEqual(
            _endpoint_label(
                'https://www.tiktok.com/api/challenge/detail/',
            ),
            'hashtag_info',
        )

    async def test_gates_every_fetch_attempt_by_proxy(self) -> None:
        limiter = AsyncMock()
        api, _ = _api_with_page([
            True, None, '{"itemList": []}',
            True, None, '{"itemList": []}',
        ])
        api.set_rate_limiter(
            limiter, 'http://proxy.example:8080',
            TikTokCallType.VIDEO_API,
        )

        await api.run_fetch_script(
            'https://www.tiktok.com/api/item/list/',
            headers={}, session_index=0,
        )
        await api.run_fetch_script(
            'https://www.tiktok.com/api/item/list/?cursor=1',
            headers={}, session_index=0,
        )

        self.assertEqual(limiter.acquire.await_count, 2)
        limiter.acquire.assert_awaited_with(
            TikTokCallType.VIDEO_API,
            proxy='http://proxy.example:8080',
        )

    async def test_kicks_off_then_polls_until_result(self) -> None:
        # kickoff -> (err None, result None) -> (err None, result OK).
        api, session = _api_with_page([
            True,        # kickoff returns truthy sentinel
            None, None,  # 1st poll: no error, no result yet
            None, '{"itemList": []}',  # 2nd poll: result ready
        ])
        with patch(
            'scrape_exchange.tiktok.tiktok_camoufox_api.asyncio.sleep',
            new=AsyncMock(),
        ):
            result = await api.run_fetch_script(
                'https://www.tiktok.com/api/user/detail/?x=1',
                headers={'a': 'b'},
                session_index=0,
            )
        self.assertEqual(result, '{"itemList": []}')
        # The fetch kickoff must run in the main world.
        self.assertTrue(
            session.page.evaluate.await_args_list[0]
            .args[0].startswith('mw:')
        )
        self.assertIn(
            'fetch',
            session.page.evaluate.await_args_list[0].args[0],
        )

    async def test_records_api_call_success_metrics(self) -> None:
        api, _ = _api_with_page([
            True,
            None, '{"userInfo":{"user":{"id":"1"},"stats":{}}}',
        ])
        api.set_metrics_context('tiktok_creator', 'worker-a')
        before_calls: float = _api_call_count(
            scraper='tiktok_creator',
            endpoint='user_info',
            outcome='success',
            worker_id='worker-a',
        )
        before_latency: float = _api_call_latency_count(
            scraper='tiktok_creator',
            endpoint='user_info',
            worker_id='worker-a',
        )

        result = await api.run_fetch_script(
            'https://www.tiktok.com/api/user/detail/?x=1',
            headers={},
            session_index=0,
        )

        self.assertIn('userInfo', result)
        self.assertEqual(
            _api_call_count(
                scraper='tiktok_creator',
                endpoint='user_info',
                outcome='success',
                worker_id='worker-a',
            ),
            before_calls + 1,
        )
        self.assertEqual(
            _api_call_latency_count(
                scraper='tiktok_creator',
                endpoint='user_info',
                worker_id='worker-a',
            ),
            before_latency + 1,
        )

    async def test_records_api_call_failure_metrics(self) -> None:
        api, _ = _api_with_page([
            True,
            None, '',
        ])
        api.set_metrics_context('tiktok_video', 'worker-b')
        before_calls: float = _api_call_count(
            scraper='tiktok_video',
            endpoint='video_info',
            outcome='rate_limit',
            worker_id='worker-b',
        )
        before_latency: float = _api_call_latency_count(
            scraper='tiktok_video',
            endpoint='video_info',
            worker_id='worker-b',
        )

        with self.assertRaisesRegex(
            RuntimeError, 'bot detection: empty response',
        ):
            await api.run_fetch_script(
                'https://www.tiktok.com/api/item/detail/?itemId=1',
                headers={},
                session_index=0,
            )

        self.assertEqual(
            _api_call_count(
                scraper='tiktok_video',
                endpoint='video_info',
                outcome='rate_limit',
                worker_id='worker-b',
            ),
            before_calls + 1,
        )
        self.assertEqual(
            _api_call_latency_count(
                scraper='tiktok_video',
                endpoint='video_info',
                worker_id='worker-b',
            ),
            before_latency + 1,
        )

    async def test_raises_on_main_world_fetch_error(self) -> None:
        api, _ = _api_with_page([
            True,                 # kickoff
            'TypeError: failed',  # poll: error set
        ])
        with patch(
            'scrape_exchange.tiktok.tiktok_camoufox_api.asyncio.sleep',
            new=AsyncMock(),
        ):
            with self.assertRaises(Exception) as ctx:
                await api.run_fetch_script(
                    'https://www.tiktok.com/api/user/detail/',
                    headers={}, session_index=0,
                )
        self.assertIn('failed', str(ctx.exception))

    async def test_times_out_when_result_never_arrives(self) -> None:
        # kickoff truthy, then every poll returns (None err, None res).
        api, _ = _api_with_page(
            [True] + [None] * 200,
        )
        with patch(
            'scrape_exchange.tiktok.tiktok_camoufox_api.asyncio.sleep',
            new=AsyncMock(),
        ):
            with self.assertRaises(TimeoutError):
                await api.run_fetch_script(
                    'https://www.tiktok.com/api/user/detail/',
                    headers={}, session_index=0,
                )

    async def test_returns_empty_string_result_verbatim(self) -> None:
        # Empty bodies are a TikTok bot-detection signature.
        api, _ = _api_with_page([
            True,      # kickoff
            None, '',  # poll: no error, empty-string result
        ])
        with patch(
            'scrape_exchange.tiktok.tiktok_camoufox_api.asyncio.sleep',
            new=AsyncMock(),
        ):
            with self.assertRaisesRegex(
                RuntimeError, 'bot detection: empty response',
            ):
                await api.run_fetch_script(
                    'https://www.tiktok.com/api/user/detail/',
                    headers={}, session_index=0,
                )

    async def test_rejects_empty_user_info_as_bot_detection(self) -> None:
        api, _ = _api_with_page([
            True,
            None,
            '{"userInfo":{"user":{},"stats":{}}}',
        ])
        with patch(
            'scrape_exchange.tiktok.tiktok_camoufox_api.asyncio.sleep',
            new=AsyncMock(),
        ):
            with self.assertRaisesRegex(
                RuntimeError, 'bot detection: empty userInfo',
            ):
                await api.run_fetch_script(
                    'https://www.tiktok.com/api/user/detail/',
                    headers={}, session_index=0,
                )

    async def test_rejects_html_without_logging_body(self) -> None:
        api, _ = _api_with_page([
            True, None, '<!DOCTYPE html><html>blocked</html>',
        ])
        with patch(
            'scrape_exchange.tiktok.tiktok_camoufox_api.asyncio.sleep',
            new=AsyncMock(),
        ):
            with self.assertRaisesRegex(
                RuntimeError, 'bot detection: HTML response',
            ):
                await api.run_fetch_script(
                    'https://www.tiktok.com/api/user/detail/',
                    headers={}, session_index=0,
                )


class TestApplyCamoufoxEnv(unittest.TestCase):
    '''CAMOU_CONFIG_* lives in process-global os.environ; per-proxy
    bootstraps must clear the previous proxy's config before setting
    the next, or geo/fingerprint leaks across browsers.'''

    def test_clears_stale_config_and_sets_new(self) -> None:
        base: dict = {
            'CAMOU_CONFIG_1': 'old1',
            'CAMOU_CONFIG_2': 'old2',
            'UNRELATED': 'keep',
        }
        with patch.dict(os.environ, base, clear=True):
            apply_camoufox_env({
                'CAMOU_CONFIG_1': 'new1',
                'FONTCONFIG_PATH': '/etc/fonts',
            })
            # stale CAMOU_CONFIG_2 must be gone
            self.assertNotIn('CAMOU_CONFIG_2', os.environ)
            self.assertEqual(os.environ['CAMOU_CONFIG_1'], 'new1')
            self.assertEqual(
                os.environ['FONTCONFIG_PATH'], '/etc/fonts',
            )
            # unrelated vars are untouched
            self.assertEqual(os.environ['UNRELATED'], 'keep')

    def test_stringifies_values(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            apply_camoufox_env({'CAMOU_CONFIG_1': 123})
            self.assertEqual(os.environ['CAMOU_CONFIG_1'], '123')


class TestCamoufoxLaunchOptions(unittest.TestCase):
    '''Centralises the validated anti-detect config so the pool and
    the gated integration test launch identically.'''

    def test_passes_validated_config_and_returns_opts(self) -> None:
        proxy: dict = {'server': 'http://proxy.example:3128'}
        fake = MagicMock(return_value={'executable_path': '/x'})
        with patch(
            'scrape_exchange.tiktok.tiktok_camoufox_api'
            '.launch_options', fake,
        ):
            opts: dict = camoufox_launch_options(proxy)
        self.assertEqual(opts, {'executable_path': '/x'})
        kwargs = fake.call_args.kwargs
        self.assertEqual(kwargs['proxy'], proxy)
        self.assertTrue(kwargs['geoip'])
        self.assertTrue(kwargs['main_world_eval'])
        self.assertFalse(kwargs['block_webgl'])
        self.assertTrue(kwargs['headless'])

    def test_proxy_optional(self) -> None:
        fake = MagicMock(return_value={})
        with patch(
            'scrape_exchange.tiktok.tiktok_camoufox_api'
            '.launch_options', fake,
        ):
            camoufox_launch_options(None)
        self.assertIsNone(fake.call_args.kwargs['proxy'])


if __name__ == '__main__':
    unittest.main()
