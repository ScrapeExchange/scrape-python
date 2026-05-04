'''
Unit tests for _video_has_formats_on_server in tools/yt_video_scrape.py.

The function makes two HTTP calls:
  1. POST /api/v1/filter with username/platform/entity/
     platform_content_id, scoped to the authenticated uploader.
  2. A data GET to the data_url returned in the first edge's
     ``node`` JSON.

It returns True only when both succeed (HTTP 200) and the data
payload contains a non-empty 'formats' list.  Any exception or
unexpected shape returns False (fail-open).
'''

import unittest

from typing import Any
from unittest.mock import AsyncMock, MagicMock

from tools.yt_video_scrape import _video_has_formats_on_server

# Constants mirrored from ExchangeClient so tests are self-contained.
_GET_FILTER_API: str = '/api/v1/filter'
_EXCHANGE_URL: str = 'https://scrape.exchange'
_UPLOADER: str = 'testuser'
_VIDEO_ID: str = 'dQw4w9WgXcQ'
_DATA_URL: str = 'https://data.scrape.exchange/payload/abc123'

# Expected filter endpoint URL assembled from the above constants.
_FILTER_URL: str = f'{_EXCHANGE_URL}{_GET_FILTER_API}'


def _make_settings() -> MagicMock:
    '''Return a minimal mock of VideoSettings.'''
    settings: MagicMock = MagicMock()
    settings.exchange_url = _EXCHANGE_URL
    return settings


def _make_response(
    status_code: int,
    json_data: Any = None,
) -> MagicMock:
    '''
    Return a mock httpx-like Response with ``status_code`` and a
    ``json()`` method that returns ``json_data``.
    '''
    resp: MagicMock = MagicMock()
    resp.status_code = status_code
    resp.json = MagicMock(return_value=json_data)
    return resp


def _filter_payload(data_url: str | None) -> dict[str, Any]:
    '''Build a minimal filter response shape with one edge whose
    node contains the supplied ``data_url`` (or no data_url when
    ``None``).'''
    node: dict[str, Any] = {}
    if data_url is not None:
        node['data_url'] = data_url
    return {'edges': [{'node': node}]}


class TestVideoHasFormatsOnServer(unittest.IsolatedAsyncioTestCase):

    def _make_client(
        self, uploader: str | None = _UPLOADER,
    ) -> MagicMock:
        '''Return a mock ExchangeClient with AsyncMock get/post and
        the supplied authenticated_username.'''
        client: MagicMock = MagicMock()
        client.get = AsyncMock()
        client.post = AsyncMock()
        client.authenticated_username = uploader
        return client

    async def test_returns_true_when_formats_present(
        self,
    ) -> None:
        '''
        Happy path: filter 200 with one edge containing data_url,
        data 200 with a non-empty formats list => True.
        '''
        settings: MagicMock = _make_settings()
        client: MagicMock = self._make_client()

        client.post.return_value = _make_response(
            200, _filter_payload(_DATA_URL),
        )
        client.get.return_value = _make_response(
            200,
            {'formats': [{'ext': 'mp4', 'height': 720}]},
        )

        result: bool = await _video_has_formats_on_server(
            client, settings, _VIDEO_ID,
        )
        self.assertTrue(result)

        # Verify the filter call was scoped to the authenticated
        # uploader, no version pin, and the right content id.
        client.post.assert_awaited_once()
        args, kwargs = client.post.call_args
        self.assertEqual(args[0], _FILTER_URL)
        body: dict[str, Any] = kwargs['json']
        self.assertEqual(body['username'], _UPLOADER)
        self.assertEqual(body['platform'], 'youtube')
        self.assertEqual(body['entity'], 'video')
        self.assertEqual(
            body['platform_content_id'], _VIDEO_ID,
        )
        self.assertNotIn('version', body)

    async def test_returns_false_when_no_authenticated_username(
        self,
    ) -> None:
        '''No JWT => no uploader scope => bail before any HTTP.'''
        settings: MagicMock = _make_settings()
        client: MagicMock = self._make_client(uploader=None)

        result: bool = await _video_has_formats_on_server(
            client, settings, _VIDEO_ID,
        )
        self.assertFalse(result)
        client.post.assert_not_awaited()
        client.get.assert_not_awaited()

    async def test_returns_false_when_filter_not_200(
        self,
    ) -> None:
        '''Filter POST returns 503 => False.'''
        settings: MagicMock = _make_settings()
        client: MagicMock = self._make_client()
        client.post.return_value = _make_response(503)

        result: bool = await _video_has_formats_on_server(
            client, settings, _VIDEO_ID,
        )
        self.assertFalse(result)

    async def test_returns_false_when_filter_post_raises(
        self,
    ) -> None:
        '''Exception during filter POST => False (fail-open).'''
        settings: MagicMock = _make_settings()
        client: MagicMock = self._make_client()
        client.post.side_effect = ConnectionError('network error')

        result: bool = await _video_has_formats_on_server(
            client, settings, _VIDEO_ID,
        )
        self.assertFalse(result)

    async def test_returns_false_when_no_edges(
        self,
    ) -> None:
        '''Filter 200 with no edges => uploader has no record.'''
        settings: MagicMock = _make_settings()
        client: MagicMock = self._make_client()
        client.post.return_value = _make_response(
            200, {'edges': []},
        )

        result: bool = await _video_has_formats_on_server(
            client, settings, _VIDEO_ID,
        )
        self.assertFalse(result)
        client.get.assert_not_awaited()

    async def test_returns_false_when_data_url_missing(
        self,
    ) -> None:
        '''
        Filter 200 with edge but no 'data_url' key => False.
        No data GET should be made.
        '''
        settings: MagicMock = _make_settings()
        client: MagicMock = self._make_client()
        client.post.return_value = _make_response(
            200, _filter_payload(None),
        )

        result: bool = await _video_has_formats_on_server(
            client, settings, _VIDEO_ID,
        )
        self.assertFalse(result)
        client.get.assert_not_awaited()

    async def test_returns_false_when_data_url_get_not_200(
        self,
    ) -> None:
        '''data_url present but data GET returns 503 => False.'''
        settings: MagicMock = _make_settings()
        client: MagicMock = self._make_client()
        client.post.return_value = _make_response(
            200, _filter_payload(_DATA_URL),
        )
        client.get.return_value = _make_response(503)

        result: bool = await _video_has_formats_on_server(
            client, settings, _VIDEO_ID,
        )
        self.assertFalse(result)

    async def test_returns_false_when_data_url_get_raises(
        self,
    ) -> None:
        '''Exception during data_url GET => False (fail-open).'''
        settings: MagicMock = _make_settings()
        client: MagicMock = self._make_client()
        client.post.return_value = _make_response(
            200, _filter_payload(_DATA_URL),
        )
        client.get.side_effect = TimeoutError(
            'data fetch timed out',
        )

        result: bool = await _video_has_formats_on_server(
            client, settings, _VIDEO_ID,
        )
        self.assertFalse(result)

    async def test_returns_false_when_formats_key_absent(
        self,
    ) -> None:
        '''
        Both calls return 200 but data JSON has no 'formats' key
        => False.
        '''
        settings: MagicMock = _make_settings()
        client: MagicMock = self._make_client()
        client.post.return_value = _make_response(
            200, _filter_payload(_DATA_URL),
        )
        client.get.return_value = _make_response(
            200, {'title': 'Some Video'},
        )

        result: bool = await _video_has_formats_on_server(
            client, settings, _VIDEO_ID,
        )
        self.assertFalse(result)

    async def test_returns_false_when_formats_is_empty_list(
        self,
    ) -> None:
        '''formats present but empty => False.'''
        settings: MagicMock = _make_settings()
        client: MagicMock = self._make_client()
        client.post.return_value = _make_response(
            200, _filter_payload(_DATA_URL),
        )
        client.get.return_value = _make_response(
            200, {'formats': []},
        )

        result: bool = await _video_has_formats_on_server(
            client, settings, _VIDEO_ID,
        )
        self.assertFalse(result)

    async def test_returns_false_when_formats_is_not_a_list(
        self,
    ) -> None:
        '''
        formats present but not a list (e.g. None or a string)
        => False.
        '''
        settings: MagicMock = _make_settings()

        for bad_value in (None, 'mp4', 42, {}):
            with self.subTest(formats=bad_value):
                client: MagicMock = self._make_client()
                client.post.return_value = _make_response(
                    200, _filter_payload(_DATA_URL),
                )
                client.get.return_value = _make_response(
                    200, {'formats': bad_value},
                )

                result: bool = await _video_has_formats_on_server(
                    client, settings, _VIDEO_ID,
                )
                self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()
