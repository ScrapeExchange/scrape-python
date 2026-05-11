#!/usr/bin/env python3
'''
Unit tests for the AsyncYouTubeClient class.

'''

import logging
import unittest
from types import SimpleNamespace
from unittest.mock import patch, AsyncMock

import httpx

from scrape_exchange.youtube.youtube_client import AsyncYouTubeClient


# Auth-failure tests deliberately drive the client into non-200 and
# network-error branches; the production code logs a WARNING on each.
# Silence the module's logger so clean runs don't emit those records.
_YT_CLIENT_LOGGER: logging.Logger = logging.getLogger(
    'scrape_exchange.youtube.youtube_client',
)
_YT_CLIENT_LOGGER_PRIOR_LEVEL: int = _YT_CLIENT_LOGGER.level


def setUpModule() -> None:
    _YT_CLIENT_LOGGER.setLevel(logging.ERROR)


def tearDownModule() -> None:
    _YT_CLIENT_LOGGER.setLevel(_YT_CLIENT_LOGGER_PRIOR_LEVEL)


class TestAuth(unittest.IsolatedAsyncioTestCase):
    async def test_create_cookie_header(self) -> None:
        async with AsyncYouTubeClient() as client:
            header: str = client.create_cookie_header({'a': '1', 'b': '2'})
            self.assertIn('a=1', header)
            self.assertIn('b=2', header)
            self.assertIn('; ', header)

    @patch('httpx.AsyncClient.get', new_callable=AsyncMock)
    async def test_get_readtimeout_raises(self, mock_get) -> None:
        mock_get.side_effect = httpx.ReadTimeout('to')

        async with AsyncYouTubeClient() as client:
            with self.assertRaises(RuntimeError):
                await client.get('https://example.com')

    @patch('httpx.AsyncClient.get', new_callable=AsyncMock)
    async def test_get_redirect_follow(self, mock_get) -> None:
        resp1 = SimpleNamespace(status_code=303,
                                headers={'Location': 'https://www.youtube.com/next'},
                                text='')
        resp2 = SimpleNamespace(status_code=200, headers={}, text='OK')
        mock_get.side_effect = [resp1, resp2]

        with patch.object(AsyncYouTubeClient, '_delay', new=AsyncMock()):
            async with AsyncYouTubeClient() as client:
                result: str | None = await client.get('https://start')

        self.assertEqual(result, 'OK')

    @patch('httpx.AsyncClient.get', new_callable=AsyncMock)
    async def test_get_non_200_returns_none(self, mock_get) -> None:
        mock_get.return_value = SimpleNamespace(
            status_code=500, headers={}, text='err'
        )

        async with AsyncYouTubeClient() as client:
            result: str | None = await client.get('https://start')

        self.assertIsNone(result)

    @patch('httpx.AsyncClient.get', new_callable=AsyncMock)
    async def test_get_404_raises_value_error(self, mock_get) -> None:
        mock_get.return_value = SimpleNamespace(
            status_code=404, headers={}, text='no'
        )

        async with AsyncYouTubeClient() as client:
            with self.assertRaises(ValueError):
                await client.get('https://start')

    @patch('httpx.AsyncClient.get', new_callable=AsyncMock)
    async def test_get_consent_cookies_request_error(self, mock_get) -> None:
        mock_get.side_effect = httpx.RequestError('net')

        # After removal of get_consent_cookies this case is no longer
        # applicable; ensure client still raises on network errors when
        # performing a normal get call that surfaces RequestError.
        mock_get.side_effect = httpx.RequestError('net')

        async with AsyncYouTubeClient() as client:
            with self.assertRaises(httpx.RequestError):
                # the underlying httpx AsyncClient.get is patched to raise
                await client.get('https://example.com')


class TestSessionWarmUp(unittest.IsolatedAsyncioTestCase):
    '''AsyncYouTubeClient.get() must call ``_warm_session`` exactly
    once over the lifetime of a client (not per-request) and must
    do so before acquiring an HTML rate-limit token. The warm-up
    fetches youtube.com so the cookie jar picks up real session
    cookies before the first channel-page request — without it
    the WAF rejected ~99.8% of HTML traffic.'''

    @patch('httpx.AsyncClient.get', new_callable=AsyncMock)
    async def test_warm_runs_once_across_calls(
        self, mock_get,
    ) -> None:
        mock_get.return_value = SimpleNamespace(
            status_code=200, headers={}, text='ok',
        )
        async with AsyncYouTubeClient() as client:
            await client.get('https://www.youtube.com/foo')
            await client.get('https://www.youtube.com/bar')
            await client.get('https://www.youtube.com/baz')

        # 3 user calls + 1 warm-up = 4 calls. The warm URL must
        # be the SCRAPE_URL (https://www.youtube.com).
        urls = [c.args[0] for c in mock_get.call_args_list]
        self.assertEqual(len(urls), 4)
        warm_calls = [u for u in urls if u == client.SCRAPE_URL]
        self.assertEqual(
            len(warm_calls), 1,
            f'expected 1 warm fetch, got urls={urls}',
        )

    @patch('httpx.AsyncClient.get', new_callable=AsyncMock)
    async def test_warm_failure_does_not_block_request(
        self, mock_get,
    ) -> None:
        '''If the warm-up request raises, the client must still
        attempt the user request — better to try than to hard-fail.
        The warm-up is best-effort.'''
        ok_resp = SimpleNamespace(
            status_code=200, headers={}, text='ok',
        )
        mock_get.side_effect = [
            httpx.ConnectError('warm fail'),  # warm-up fails
            ok_resp,                          # user call succeeds
        ]
        async with AsyncYouTubeClient() as client:
            result = await client.get('https://www.youtube.com/foo')
        self.assertEqual(result, 'ok')
        self.assertEqual(len(mock_get.call_args_list), 2)

    @patch('httpx.AsyncClient.get', new_callable=AsyncMock)
    async def test_reset_session_warm_re_warms(
        self, mock_get,
    ) -> None:
        mock_get.return_value = SimpleNamespace(
            status_code=200, headers={}, text='ok',
        )
        async with AsyncYouTubeClient() as client:
            await client.get('https://www.youtube.com/a')  # warm 1
            client.reset_session_warm()
            await client.get('https://www.youtube.com/b')  # warm 2
        urls = [c.args[0] for c in mock_get.call_args_list]
        warm_calls = [u for u in urls if u == client.SCRAPE_URL]
        self.assertEqual(len(warm_calls), 2)


class TestTransportConnectionReuse(unittest.IsolatedAsyncioTestCase):
    '''Pin the guarantee that the curl_cffi transport does NOT set
    ``CurlOpt.FRESH_CONNECT``. With per-proxy pooling in place,
    every AsyncYouTubeClient is long-lived and reused across calls;
    setting FRESH_CONNECT=True would force a fresh TCP+TLS
    handshake on every request, defeating the pool and producing
    SYN-burst pressure on YouTube. Regression discovered when the
    channel scraper hit 0% about-page success rate; restoring
    keepalive returned it to healthy.'''

    async def test_async_curl_transport_has_no_fresh_connect_option(
        self,
    ) -> None:
        captured: dict[str, object] = {}

        from httpx_curl_cffi import AsyncCurlTransport

        original_init = AsyncCurlTransport.__init__

        def _spy_init(
            self_: AsyncCurlTransport,
            *args: object,
            **kwargs: object,
        ) -> None:
            captured['curl_options'] = kwargs.get('curl_options')
            original_init(self_, *args, **kwargs)

        with patch.object(
            AsyncCurlTransport, '__init__', _spy_init,
        ):
            async with AsyncYouTubeClient():
                pass

        # Either ``curl_options`` is absent entirely or, if present,
        # it must not enable FRESH_CONNECT. Both shapes preserve
        # connection reuse.
        opts = captured.get('curl_options')
        if opts is not None:
            try:
                from httpx_curl_cffi import CurlOpt
                self.assertNotIn(CurlOpt.FRESH_CONNECT, opts)
            except ImportError:
                self.fail(
                    'CurlOpt no longer importable; '
                    'test needs updating',
                )


if __name__ == '__main__':
    unittest.main()
