'''
Unit tests for ``channel_exists`` in tools/yt_channel_scrape.py.

The function POSTs to /api/v1/filter scoped to the authenticated
uploader (no ``version`` field, so all schema versions match) and
returns True iff at least one edge comes back. It fails-open
(returns False) on any HTTP / decode error and on a missing
``authenticated_username``.
'''

import unittest

from typing import Any
from unittest.mock import AsyncMock, MagicMock

from tools.yt_channel_scrape import channel_exists


_GET_FILTER_API: str = '/api/v1/filter'
_EXCHANGE_URL: str = 'https://scrape.exchange'
_UPLOADER: str = 'testuser'
_HANDLE: str = 'examplechannel'
_FILTER_URL: str = f'{_EXCHANGE_URL}{_GET_FILTER_API}'


def _make_response(
    status_code: int,
    json_data: Any = None,
    text: str = '',
) -> MagicMock:
    resp: MagicMock = MagicMock()
    resp.status_code = status_code
    resp.json = MagicMock(return_value=json_data)
    resp.text = text
    return resp


class TestChannelExists(unittest.IsolatedAsyncioTestCase):

    def _make_client(
        self, uploader: str | None = _UPLOADER,
    ) -> MagicMock:
        client: MagicMock = MagicMock()
        client.exchange_url = _EXCHANGE_URL
        client.post = AsyncMock()
        client.authenticated_username = uploader
        return client

    async def test_returns_true_when_uploader_has_record(
        self,
    ) -> None:
        client: MagicMock = self._make_client()
        client.post.return_value = _make_response(
            200, {'edges': [{'node': {'item_id': 'x'}}]},
        )

        result: bool = await channel_exists(client, _HANDLE)
        self.assertTrue(result)

        client.post.assert_awaited_once()
        args, kwargs = client.post.call_args
        self.assertEqual(args[0], _FILTER_URL)
        body: dict[str, Any] = kwargs['json']
        self.assertEqual(body['username'], _UPLOADER)
        self.assertEqual(body['platform'], 'youtube')
        self.assertEqual(body['entity'], 'channel')
        self.assertEqual(
            body['platform_content_id'], _HANDLE,
        )
        self.assertNotIn('version', body)

    async def test_returns_false_when_no_edges(
        self,
    ) -> None:
        client: MagicMock = self._make_client()
        client.post.return_value = _make_response(
            200, {'edges': []},
        )

        result: bool = await channel_exists(client, _HANDLE)
        self.assertFalse(result)

    async def test_returns_false_when_no_authenticated_username(
        self,
    ) -> None:
        '''Cannot scope the query => fail-open and skip the POST.'''
        client: MagicMock = self._make_client(uploader=None)

        result: bool = await channel_exists(client, _HANDLE)
        self.assertFalse(result)
        client.post.assert_not_awaited()

    async def test_returns_false_when_post_raises(
        self,
    ) -> None:
        client: MagicMock = self._make_client()
        client.post.side_effect = ConnectionError('boom')

        result: bool = await channel_exists(client, _HANDLE)
        self.assertFalse(result)

    async def test_returns_false_on_5xx(
        self,
    ) -> None:
        client: MagicMock = self._make_client()
        client.post.return_value = _make_response(
            503, text='upstream timeout',
        )

        result: bool = await channel_exists(client, _HANDLE)
        self.assertFalse(result)

    async def test_returns_false_when_edges_missing(
        self,
    ) -> None:
        '''Defensive: response without an `edges` key.'''
        client: MagicMock = self._make_client()
        client.post.return_value = _make_response(200, {})

        result: bool = await channel_exists(client, _HANDLE)
        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()
