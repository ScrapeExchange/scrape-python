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


if __name__ == '__main__':
    unittest.main()
