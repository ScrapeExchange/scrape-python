'''Unit tests for the pooled httpx.AsyncClient pool.'''

import unittest

import httpx

import scrape_exchange.proxy_loader as proxy_loader
from scrape_exchange.proxy_loader import (
    aclose_pooled_httpx_clients,
    pooled_httpx_client_for_entry,
)


class TestPooledHttpxClient(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        proxy_loader._reset_pool_for_tests()

    async def asyncTearDown(self) -> None:
        await aclose_pooled_httpx_clients()

    async def test_same_entry_returns_same_instance(self) -> None:
        a: httpx.AsyncClient = pooled_httpx_client_for_entry(
            'http://1.1.1.1:80',
        )
        b: httpx.AsyncClient = pooled_httpx_client_for_entry(
            'http://1.1.1.1:80',
        )
        self.assertIs(a, b)

    async def test_different_entries_return_different_instances(
        self,
    ) -> None:
        a: httpx.AsyncClient = pooled_httpx_client_for_entry(
            'http://1.1.1.1:80',
        )
        b: httpx.AsyncClient = pooled_httpx_client_for_entry(
            'http://2.2.2.2:80',
        )
        self.assertIsNot(a, b)

    async def test_none_entry_caches_separately(self) -> None:
        a: httpx.AsyncClient = pooled_httpx_client_for_entry(None)
        b: httpx.AsyncClient = pooled_httpx_client_for_entry(None)
        self.assertIs(a, b)

    async def test_local_entry_uses_local_address_transport(
        self,
    ) -> None:
        client: httpx.AsyncClient = pooled_httpx_client_for_entry(
            'local://192.0.2.5',
        )
        # httpx.AsyncHTTPTransport stores its config under
        # _pool._local_address.
        transport: object = client._transport
        self.assertEqual(
            transport._pool._local_address, '192.0.2.5',
        )

    async def test_default_timeout(self) -> None:
        client: httpx.AsyncClient = pooled_httpx_client_for_entry(
            'http://1.1.1.1:80',
        )
        self.assertEqual(
            client.timeout, httpx.Timeout(10.0, connect=5.0),
        )

    async def test_aclose_all_closes_and_clears(self) -> None:
        a: httpx.AsyncClient = pooled_httpx_client_for_entry(
            'http://1.1.1.1:80',
        )
        await aclose_pooled_httpx_clients()
        self.assertTrue(a.is_closed)
        # After aclose_all the cache is empty; new get() builds.
        b: httpx.AsyncClient = pooled_httpx_client_for_entry(
            'http://1.1.1.1:80',
        )
        self.assertIsNot(a, b)

    def test_reset_for_tests_does_not_close(self) -> None:
        a: httpx.AsyncClient = pooled_httpx_client_for_entry(
            'http://1.1.1.1:80',
        )
        proxy_loader._reset_pool_for_tests()
        self.assertFalse(a.is_closed)


if __name__ == '__main__':
    unittest.main()
