"""Unit tests for httpx_client_for_entry factory."""

import asyncio
import unittest

import httpx

from scrape_exchange.proxy_loader import httpx_client_for_entry


class TestHttpxClientForEntry(unittest.TestCase):

    def test_proxy_url_entry_returns_async_client(self) -> None:
        client: httpx.AsyncClient = httpx_client_for_entry(
            'http://user:pass@1.1.1.1:80',
        )
        try:
            self.assertIsInstance(client, httpx.AsyncClient)
            # Proxy URL entries set up _mounts; presence of any
            # non-default mount confirms the proxy was wired in.
            self.assertTrue(len(client._mounts) > 0)
        finally:
            asyncio.run(client.aclose())

    def test_local_entry_binds_local_address(self) -> None:
        client: httpx.AsyncClient = httpx_client_for_entry(
            'local://192.0.2.5',
        )
        try:
            self.assertIsInstance(client, httpx.AsyncClient)
            transport: object = client._transport
            # Touches httpx private internals
            # (_transport._pool._local_address); if a future httpx
            # version breaks this path, update the introspection —
            # the factory's contract (binding the local IP) is
            # unchanged.
            pool: object = transport._pool
            self.assertEqual(
                pool._local_address, '192.0.2.5',
            )
        finally:
            asyncio.run(client.aclose())

    def test_extra_kwargs_passed_through_for_local(self) -> None:
        client: httpx.AsyncClient = httpx_client_for_entry(
            'local://192.0.2.5',
            headers={'X-Test': 'yes'},
        )
        try:
            self.assertEqual(
                client.headers.get('X-Test'), 'yes',
            )
        finally:
            asyncio.run(client.aclose())

    def test_extra_kwargs_passed_through_for_proxy(self) -> None:
        client: httpx.AsyncClient = httpx_client_for_entry(
            'http://1.1.1.1:80',
            headers={'X-Test': 'yes'},
        )
        try:
            self.assertEqual(
                client.headers.get('X-Test'), 'yes',
            )
        finally:
            asyncio.run(client.aclose())


    def test_none_entry_returns_vanilla_client(self) -> None:
        client: httpx.AsyncClient = httpx_client_for_entry(None)
        try:
            self.assertIsInstance(client, httpx.AsyncClient)
            # No proxy mounts and no custom transport — vanilla
            # client. _mounts is empty for a default AsyncClient.
            self.assertEqual(len(client._mounts), 0)
        finally:
            asyncio.run(client.aclose())

    def test_none_entry_passes_extra_kwargs(self) -> None:
        client: httpx.AsyncClient = httpx_client_for_entry(
            None, headers={'X-Test': 'yes'},
        )
        try:
            self.assertEqual(
                client.headers.get('X-Test'), 'yes',
            )
        finally:
            asyncio.run(client.aclose())


if __name__ == '__main__':
    unittest.main()
