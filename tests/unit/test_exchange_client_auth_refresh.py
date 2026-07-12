'''
Regression tests for ExchangeClient JWT refresh after a previously
working token starts being rejected.
'''

import asyncio
import unittest
from collections.abc import Callable
from unittest.mock import AsyncMock, patch

import httpx

from scrape_exchange.exchange_client import ExchangeClient


def _make_client(
    handler: Callable[[httpx.Request], httpx.Response],
) -> ExchangeClient:
    client: ExchangeClient = ExchangeClient.__new__(ExchangeClient)
    client.exchange_url = 'https://fake.scrape.exchange'
    client.jwt_header = 'Bearer stale'
    client._api_key_id = 'key-id'
    client._api_key_secret = 'key-secret'
    client._auth_refresh_lock = asyncio.Lock()
    client._auth_had_success = False
    client._upload_queue = None
    client._upload_tasks = []
    client._upload_shutdown = False
    client._upload_queue_depths = {}
    httpx.AsyncClient.__init__(
        client,
        transport=httpx.MockTransport(handler),
        trust_env=False,
    )
    client.headers['Authorization'] = client.jwt_header
    return client


class TestExchangeClientAuthRefresh(unittest.IsolatedAsyncioTestCase):

    async def test_post_refreshes_jwt_after_prior_success(
        self,
    ) -> None:
        seen_auth: list[str | None] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen_auth.append(request.headers.get('authorization'))
            if len(seen_auth) == 1:
                return httpx.Response(201, json={'ok': True})
            if len(seen_auth) == 2:
                return httpx.Response(401, text='Invalid JWT')
            return httpx.Response(201, json={'ok': True})

        client: ExchangeClient = _make_client(handler)
        get_token: AsyncMock = AsyncMock(return_value='Bearer fresh')
        try:
            with patch.object(
                ExchangeClient, 'get_jwt_token', get_token,
            ):
                first: httpx.Response = await client.post(
                    'https://fake.scrape.exchange/api/v1/data',
                    json={'first': True},
                )
                second: httpx.Response = await client.post(
                    'https://fake.scrape.exchange/api/v1/data',
                    json={'second': True},
                )

            self.assertEqual(first.status_code, 201)
            self.assertEqual(second.status_code, 201)
            self.assertEqual(
                seen_auth,
                ['Bearer stale', 'Bearer stale', 'Bearer fresh'],
            )
            get_token.assert_awaited_once_with(
                'key-id',
                'key-secret',
                'https://fake.scrape.exchange',
            )
            self.assertEqual(client.headers['Authorization'], 'Bearer fresh')
        finally:
            await httpx.AsyncClient.aclose(client)

    async def test_get_refreshes_jwt_after_prior_success(
        self,
    ) -> None:
        seen_auth: list[str | None] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen_auth.append(request.headers.get('authorization'))
            if len(seen_auth) == 1:
                return httpx.Response(200, json={'ok': True})
            if len(seen_auth) == 2:
                return httpx.Response(401, text='Invalid JWT')
            return httpx.Response(200, json={'ok': True})

        client: ExchangeClient = _make_client(handler)
        get_token: AsyncMock = AsyncMock(return_value='Bearer fresh')
        try:
            with patch.object(
                ExchangeClient, 'get_jwt_token', get_token,
            ):
                first: httpx.Response = await client.get(
                    'https://fake.scrape.exchange/api/v1/schema'
                )
                second: httpx.Response = await client.get(
                    'https://fake.scrape.exchange/api/v1/schema'
                )

            self.assertEqual(first.status_code, 200)
            self.assertEqual(second.status_code, 200)
            self.assertEqual(
                seen_auth,
                ['Bearer stale', 'Bearer stale', 'Bearer fresh'],
            )
            get_token.assert_awaited_once()
            self.assertEqual(client.headers['Authorization'], 'Bearer fresh')
        finally:
            await httpx.AsyncClient.aclose(client)

    async def test_does_not_refresh_before_prior_success(self) -> None:
        seen_auth: list[str | None] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen_auth.append(request.headers.get('authorization'))
            return httpx.Response(401, text='Invalid JWT')

        client: ExchangeClient = _make_client(handler)
        get_token: AsyncMock = AsyncMock(return_value='Bearer fresh')
        try:
            with patch.object(
                ExchangeClient, 'get_jwt_token', get_token,
            ):
                response: httpx.Response = await client.post(
                    'https://fake.scrape.exchange/api/v1/data',
                    json={'first': True},
                )

            self.assertEqual(response.status_code, 401)
            self.assertEqual(seen_auth, ['Bearer stale'])
            get_token.assert_not_awaited()
            self.assertEqual(client.headers['Authorization'], 'Bearer stale')
        finally:
            await httpx.AsyncClient.aclose(client)


if __name__ == '__main__':
    unittest.main()
