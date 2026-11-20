'''
Unit tests that HTTP clients built per proxy share one TLS context.

Each ``ssl.SSLContext`` loads the whole CA bundle (~1.4 MiB). With a
long-lived client per proxy, and hundreds of proxies per worker, one
context per client cost hundreds of MB per scraper process.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import ssl
import unittest
from typing import Any

from scrape_exchange import proxy_loader
from scrape_exchange.youtube import youtube_channel_tabs

PROXIES: list[str] = [
    'http://10.0.0.1:3128',
    'http://10.0.0.2:3128',
    'http://10.0.0.3:3128',
]


def _ssl_contexts(client: Any) -> set[int]:
    '''ids of every TLS context a client's transports use for the
    target connection: the default transport and each proxy mount.'''
    contexts: set[int] = set()
    transports: list[Any] = [client._transport, *client._mounts.values()]
    transport: Any
    for transport in transports:
        if transport is None:
            continue
        contexts.add(id(transport._pool._ssl_context))
    return contexts


class TestHttpx2ClientsShareSslContext(unittest.IsolatedAsyncioTestCase):

    async def test_pooled_clients_share_one_context(self) -> None:
        clients: list[Any] = [
            proxy_loader._make_pooled_httpx_client_for_entry(entry)
            for entry in [*PROXIES, None, 'local://127.0.0.1']
        ]
        try:
            shared: ssl.SSLContext = proxy_loader.shared_ssl_context()
            client: Any
            for client in clients:
                self.assertEqual(_ssl_contexts(client), {id(shared)})
        finally:
            for client in clients:
                await client.aclose()

    async def test_unpooled_clients_share_one_context(self) -> None:
        clients: list[Any] = [
            proxy_loader.httpx_client_for_entry(entry)
            for entry in [*PROXIES, None, 'local://127.0.0.1']
        ]
        try:
            shared: ssl.SSLContext = proxy_loader.shared_ssl_context()
            client: Any
            for client in clients:
                self.assertEqual(_ssl_contexts(client), {id(shared)})
        finally:
            for client in clients:
                await client.aclose()

    async def test_explicit_verify_is_respected(self) -> None:
        client: Any = proxy_loader.httpx_client_for_entry(
            PROXIES[0], verify=False,
        )
        try:
            self.assertNotIn(
                id(proxy_loader.shared_ssl_context()),
                _ssl_contexts(client),
            )
        finally:
            await client.aclose()

    def test_shared_context_verifies_certificates(self) -> None:
        context: ssl.SSLContext = proxy_loader.shared_ssl_context()
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
        self.assertTrue(context.check_hostname)


class TestInnertubeSessionsShareSslContext(unittest.TestCase):

    def test_sessions_share_one_context(self) -> None:
        clients: list[Any] = [
            youtube_channel_tabs.build_innertube_with_pool_limits(entry)
            for entry in [*PROXIES, None]
        ]
        try:
            shared: ssl.SSLContext = (
                youtube_channel_tabs._innertube_ssl_context()
            )
            self.assertEqual(shared.verify_mode, ssl.CERT_REQUIRED)
            self.assertTrue(shared.check_hostname)
            client: Any
            for client in clients:
                self.assertEqual(
                    _ssl_contexts(client.adaptor.session), {id(shared)},
                )
        finally:
            for client in clients:
                client.close()


if __name__ == '__main__':
    unittest.main()
