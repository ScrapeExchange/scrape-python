'''
Unit tests for the http fetch mode of tools/tt_creator_scrape.py:
profile pages fetched over httpx + curl-cffi instead of Camoufox.
'''

import asyncio
import json
import os
import tempfile
import unittest
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
from httpx_curl_cffi import AsyncCurlTransport

from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.tiktok import TikTokCallType, classify_tiktok_error
from scrape_exchange.tiktok.tiktok_metrics import API_CALL_TOTAL
from tests.unit.test_tt_creator_scrape import _raw_user, _settings
from tools import tt_creator_scrape as tool

PROXY: str = 'http://p1:8080'


def _profile_html(detail: dict) -> str:
    '''A profile page carrying *detail* as its user-detail scope.'''
    payload: dict[str, Any] = {
        '__DEFAULT_SCOPE__': {'webapp.user-detail': detail},
    }
    return (
        '<html><head><script id="__UNIVERSAL_DATA_FOR_REHYDRATION__" '
        f'type="application/json">{json.dumps(payload)}</script>'
        '</head></html>'
    )


def _api_call_value(outcome: str) -> float:
    return API_CALL_TOTAL.labels(
        platform='tiktok',
        scraper='tiktok_creator',
        endpoint='user_info',
        outcome=outcome,
        worker_id='0',
    )._value.get()


class TestHttpCreatorFetcher(unittest.IsolatedAsyncioTestCase):

    def _fetcher(
        self,
        response: httpx.Response | None = None,
        error: Exception | None = None,
    ) -> tuple[tool.HttpCreatorFetcher, MagicMock, MagicMock]:
        limiter: MagicMock = MagicMock()
        limiter.acquire = AsyncMock(return_value=PROXY)
        fetcher: tool.HttpCreatorFetcher = tool.HttpCreatorFetcher(
            limiter, '0',
        )
        client: MagicMock = MagicMock()
        client.get = AsyncMock(return_value=response, side_effect=error)
        client.aclose = AsyncMock()
        fetcher._clients[PROXY] = client
        return fetcher, limiter, client

    async def _scrape_error(
        self, fetcher: tool.HttpCreatorFetcher,
    ) -> str:
        settings: MagicMock = _settings()
        with self.assertRaises(RuntimeError) as ctx:
            await fetcher.scrape(PROXY, 'someone', settings)
        return classify_tiktok_error(ctx.exception)

    async def test_success_builds_creator_without_lists(self) -> None:
        html: str = _profile_html(
            _raw_user('someone', followers=42, video_count=7),
        )
        fetcher, limiter, client = self._fetcher(
            httpx.Response(200, text=html),
        )
        before: float = _api_call_value('success')

        creator = await fetcher.scrape(PROXY, 'someone', _settings())

        self.assertEqual(creator.username, 'someone')
        self.assertEqual(creator.follower_count, 42)
        self.assertEqual(creator.video_count, 7)
        self.assertEqual(creator.videos, [])
        self.assertEqual(creator.playlists, [])
        limiter.acquire.assert_awaited_once_with(
            TikTokCallType.CREATOR_API, proxy=PROXY,
        )
        client.get.assert_awaited_once_with(
            'https://www.tiktok.com/@someone',
        )
        self.assertEqual(_api_call_value('success'), before + 1)

    async def test_banned_account_is_unavailable(self) -> None:
        html: str = _profile_html(
            {'statusCode': 10221, 'statusMsg': 'user banned'},
        )
        fetcher, _, _ = self._fetcher(httpx.Response(200, text=html))
        self.assertEqual(await self._scrape_error(fetcher), 'unavailable')

    async def test_missing_rehydration_payload_is_rate_limit(
        self,
    ) -> None:
        fetcher, _, _ = self._fetcher(
            httpx.Response(200, text='<html>nothing</html>'),
        )
        self.assertEqual(await self._scrape_error(fetcher), 'rate_limit')

    async def test_bot_check_page_is_rate_limit(self) -> None:
        before: float = _api_call_value('rate_limit')
        fetcher, _, _ = self._fetcher(
            httpx.Response(
                200, text='<html>captcha-verify please wait</html>',
            ),
        )
        self.assertEqual(await self._scrape_error(fetcher), 'rate_limit')
        self.assertEqual(_api_call_value('rate_limit'), before + 1)

    async def test_blocking_statuses_are_rate_limit(self) -> None:
        for status in (403, 429):
            with self.subTest(status=status):
                fetcher, _, _ = self._fetcher(
                    httpx.Response(status, text=''),
                )
                self.assertEqual(
                    await self._scrape_error(fetcher), 'rate_limit',
                )

    async def test_server_error_is_transient(self) -> None:
        fetcher, _, _ = self._fetcher(httpx.Response(503, text=''))
        self.assertEqual(await self._scrape_error(fetcher), 'transient')

    async def test_transport_error_is_transient(self) -> None:
        fetcher, _, _ = self._fetcher(
            error=httpx.ConnectError('proxy refused'),
        )
        self.assertEqual(await self._scrape_error(fetcher), 'transient')

    async def test_mismatched_identity_is_transient(self) -> None:
        html: str = _profile_html(_raw_user('somebody_else'))
        fetcher, _, _ = self._fetcher(httpx.Response(200, text=html))
        self.assertEqual(await self._scrape_error(fetcher), 'transient')

    async def test_gate_uses_creator_api_bucket(self) -> None:
        fetcher, limiter, _ = self._fetcher()
        await fetcher.gate(PROXY)
        limiter.acquire.assert_awaited_once_with(
            TikTokCallType.CREATOR_API, proxy=PROXY,
        )

    async def test_quarantine_drops_client_and_rebuild_succeeds(
        self,
    ) -> None:
        fetcher, _, client = self._fetcher()
        await fetcher.quarantine(PROXY)
        client.aclose.assert_awaited_once()
        self.assertNotIn(PROXY, fetcher._clients)
        self.assertTrue(await fetcher.rebuild(PROXY))

    async def test_client_impersonates_chrome_through_proxy(
        self,
    ) -> None:
        fetcher: tool.HttpCreatorFetcher = tool.HttpCreatorFetcher(
            MagicMock(), '0',
        )
        with patch.object(
            tool, 'AsyncCurlTransport', wraps=AsyncCurlTransport,
        ) as transport_cls:
            client: httpx.AsyncClient = fetcher._client(PROXY)
        try:
            transport_cls.assert_called_once_with(
                impersonate='chrome', default_headers=True, proxy=PROXY,
            )
            self.assertIs(fetcher._client(PROXY), client)
            self.assertEqual(
                client.headers['Accept-Language'], 'en-US,en;q=0.9',
            )
        finally:
            await fetcher.close()
        self.assertEqual(fetcher._clients, {})


class TestHttpFetchModeSettings(unittest.TestCase):

    def test_default_fetch_mode_is_http(self) -> None:
        s = tool.CreatorSettings(_env_file=None, _cli_parse_args=[])
        self.assertEqual(s.creator_fetch_mode, 'http')

    def test_fetch_mode_from_env(self) -> None:
        with patch.dict(
            os.environ, {'TIKTOK_CREATOR_FETCH_MODE': 'browser'},
        ):
            s = tool.CreatorSettings(_env_file=None, _cli_parse_args=[])
        self.assertEqual(s.creator_fetch_mode, 'browser')

    def test_http_mode_ignores_max_browsers(self) -> None:
        s: MagicMock = _settings(creator_fetch_mode='http')
        self.assertEqual(
            tool._resolve_creator_concurrency(s, proxy_count=67), 67,
        )


class TestProcessCreatorHttp(unittest.IsolatedAsyncioTestCase):

    async def test_success_without_video_queue(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            fm: AssetFileManagement = AssetFileManagement(
                tmp,
                prefix_rankings={'creator': [tool.CREATOR_FILE_PREFIX]},
            )
            fetcher: MagicMock = MagicMock()
            fetcher.scrape = AsyncMock(
                return_value=tool.TikTokCreator.from_user_info(
                    _raw_user('charli', followers=42),
                ),
            )
            queue: MagicMock = MagicMock()
            queue.update_tier = AsyncMock()
            queue.release = AsyncMock()
            maps: MagicMock = MagicMock()
            maps.user_id_to_username.put = AsyncMock()
            maps.username_to_user_id.put = AsyncMock()
            maps.nickname_to_sec_uid.put = AsyncMock()

            outcome = await tool._process_creator(
                'charli', PROXY, fetcher, queue, None, fm, _settings(),
                '0', 'p1', maps,
            )

            self.assertIsNone(outcome)
            written: dict = await fm.read_file(
                tool._creator_filename('charli'),
            )
            self.assertEqual(written['follower_count'], 42)
            self.assertEqual(written['videos'], [])
            queue.update_tier.assert_awaited_once_with('charli', 42)
            queue.release.assert_awaited_once_with('charli')


class TestRunWorkerHttp(unittest.IsolatedAsyncioTestCase):

    async def test_no_browsers_or_video_queue(self) -> None:
        ctx: MagicMock = MagicMock()
        ctx.settings = _settings(creator_fetch_mode='http')
        ctx.proxies = [PROXY, 'local://10.0.0.1', 'http://p2:8080']
        ctx.rate_limiter = MagicMock()

        worker_calls: list[tuple[str, object, object]] = []

        async def _fake_worker(
            proxy: str, fetcher: object, queue: object,
            video_queue: object, *a: object,
        ) -> None:
            worker_calls.append((proxy, fetcher, video_queue))

        async def _noop_loop(*a: object) -> None:
            await asyncio.sleep(0)

        with patch.object(
            tool, 'TikTokSessionPool',
        ) as pool_cls, patch.object(
            tool, '_build_video_queue',
        ) as video_queue_builder, patch.object(
            tool, '_build_queue', return_value=MagicMock(),
        ), patch.object(
            tool, '_build_creator_identity_maps',
            return_value=MagicMock(),
        ), patch.object(
            tool, '_proxy_worker', _fake_worker,
        ), patch.object(
            tool, '_maintenance_loop', _noop_loop,
        ), patch.object(
            tool, 'AssetFileManagement',
        ), patch.object(
            tool.HttpCreatorFetcher, 'close', AsyncMock(),
        ) as close:
            await tool._run_worker(ctx)

        pool_cls.assert_not_called()
        video_queue_builder.assert_not_called()
        self.assertEqual(
            [proxy for proxy, _, _ in worker_calls],
            [PROXY, 'http://p2:8080'],
        )
        for _, fetcher, video_queue in worker_calls:
            self.assertIsInstance(fetcher, tool.HttpCreatorFetcher)
            self.assertIsNone(video_queue)
        close.assert_awaited_once()

    async def test_only_local_entries_exits_cleanly(self) -> None:
        ctx: MagicMock = MagicMock()
        ctx.settings = _settings(creator_fetch_mode='http')
        ctx.proxies = ['local://10.0.0.1']
        ctx.rate_limiter = MagicMock()

        with patch.object(
            tool, '_build_queue',
        ) as build_queue, patch.object(
            tool, '_build_creator_identity_maps',
            return_value=MagicMock(),
        ), patch.object(tool, 'AssetFileManagement'):
            await tool._run_worker(ctx)

        build_queue.assert_not_called()


if __name__ == '__main__':
    unittest.main()
