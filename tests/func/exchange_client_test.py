#!/usr/bin/env python3
'''

Integration tests for AsyncYouTubeClient that perform real HTTP calls to
YouTube.

These tests hit live YouTube endpoints and may be slow or flaky depending on
network conditions. They are intended to run separately from unit tests.
'''

import os
import unittest

from scrape_exchange.exchange_client import ExchangeClient


class TestIntegration(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.api_key_id: str = os.environ.get('API_KEY_ID')
        self.api_key_secret: str = os.environ.get('API_KEY_SECRET')
        self.exchange_url: str = os.environ.get('EXCHANGE_URL')

    async def test_api_keys(self) -> None:
        self.assertIsNotNone(self.api_key_id)
        self.assertIsNotNone(self.api_key_secret)
        self.assertIsNotNone(self.exchange_url)

        client: ExchangeClient = await ExchangeClient.setup(
            self.api_key_id, self.api_key_secret, self.exchange_url
        )

        self.assertIsNotNone(client.jwt_header)
        self.assertTrue(client.jwt_header.startswith('Bearer '))


if __name__ == '__main__':
    unittest.main()
