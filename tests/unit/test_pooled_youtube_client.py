'''Unit tests for the pooled AsyncYouTubeClient pool.'''

import unittest

import scrape_exchange.youtube.youtube_client as youtube_client
from scrape_exchange.youtube.youtube_client import (
    AsyncYouTubeClient,
    aclose_pooled_youtube_clients,
    pooled_youtube_client_for_entry,
)


class TestPooledYouTubeClient(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        youtube_client._reset_pool_for_tests()

    async def asyncTearDown(self) -> None:
        await aclose_pooled_youtube_clients()

    async def test_same_entry_returns_same_instance(self) -> None:
        a: AsyncYouTubeClient = pooled_youtube_client_for_entry(
            'http://1.1.1.1:80',
        )
        b: AsyncYouTubeClient = pooled_youtube_client_for_entry(
            'http://1.1.1.1:80',
        )
        self.assertIs(a, b)

    async def test_different_entries_return_different_instances(
        self,
    ) -> None:
        a: AsyncYouTubeClient = pooled_youtube_client_for_entry(
            'http://1.1.1.1:80',
        )
        b: AsyncYouTubeClient = pooled_youtube_client_for_entry(
            'http://2.2.2.2:80',
        )
        self.assertIsNot(a, b)

    async def test_proxy_pinned_on_cached_client(self) -> None:
        client: AsyncYouTubeClient = (
            pooled_youtube_client_for_entry('http://1.1.1.1:80')
        )
        self.assertEqual(client.proxy, 'http://1.1.1.1:80')

    async def test_none_entry_caches_separately(self) -> None:
        a: AsyncYouTubeClient = pooled_youtube_client_for_entry(
            None,
        )
        b: AsyncYouTubeClient = pooled_youtube_client_for_entry(
            None,
        )
        self.assertIs(a, b)
        self.assertIsNone(a.proxy)

    async def test_aclose_all_closes_and_clears(self) -> None:
        a: AsyncYouTubeClient = pooled_youtube_client_for_entry(
            'http://1.1.1.1:80',
        )
        await aclose_pooled_youtube_clients()
        self.assertTrue(a.is_closed)
        b: AsyncYouTubeClient = pooled_youtube_client_for_entry(
            'http://1.1.1.1:80',
        )
        self.assertIsNot(a, b)


if __name__ == '__main__':
    unittest.main()
