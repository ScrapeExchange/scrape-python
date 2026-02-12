#!/usr/bin/env python3
'''
Unit tests for the AsyncYouTubeClient class.

'''

import unittest
from types import SimpleNamespace
from unittest.mock import patch, AsyncMock

import httpx

from scrape_exchange.youtube.youtube_client import AsyncYouTubeClient


class TestAuth(unittest.IsolatedAsyncioTestCase):


    async def test_get_headers_and_init_cookies(self) -> None:
        # Custom headers and consent cookies applied at init
        headers: dict[str, str] = {'X-Custom': 'Yes'}
        consent: dict[str, str] = {'FOO': 'BAR'}

        async with AsyncYouTubeClient(user_agent='MyAgent',
                                      headers=headers,
                                      consent_cookies=consent) as client:
            got: dict[str, str] = client.get_headers()
            # header keys may be normalized to lowercase by the client
            lower: dict[str, str] = {k.lower(): v for k, v in got.items()}
            self.assertIn('user-agent', lower)
            self.assertEqual(lower['user-agent'], 'MyAgent')
            self.assertIn('x-custom', lower)
            # cookie should be present
            self.assertEqual(client.cookies.get('FOO'), 'BAR')

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
            status_code=404, headers={}, text='no'
        )

        async with AsyncYouTubeClient() as client:
            result: str | None = await client.get('https://start')

        self.assertIsNone(result)

    @patch('httpx.AsyncClient.get', new_callable=AsyncMock)
    async def test_get_delay_calls_delay(self, mock_get) -> None:
        mock_get.return_value = SimpleNamespace(
            status_code=200, headers={}, text='ok'
        )
        delay_mock = AsyncMock()
        with patch.object(AsyncYouTubeClient, '_delay', new=delay_mock):
            async with AsyncYouTubeClient() as client:
                result: str | None = await client.get('https://start', delay=1)

        self.assertEqual(result, 'ok')
        delay_mock.assert_awaited()

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




if __name__ == '__main__':
    unittest.main()
