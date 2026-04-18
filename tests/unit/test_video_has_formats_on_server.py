'''
Unit tests for _video_has_formats_on_server in tools/yt_video_scrape.py.

The function makes two HTTP calls via exchange_client.get():
  1. A metadata GET to the data-param endpoint.
  2. A data GET to the data_url returned in the metadata JSON.

It returns True only when both succeed (HTTP 200) and the data
payload contains a non-empty 'formats' list.  Any exception or
unexpected shape returns False (fail-open).
'''

import asyncio
import importlib.util
import unittest

from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import AsyncMock, MagicMock


def _load_yt_video_scrape() -> ModuleType:
    '''
    Load tools/yt_video_scrape.py directly from
    its file path. ``tools/`` is not a Python
    package so a normal import fails.

    Cached in ``sys.modules`` to avoid duplicate
    Prometheus metric registration when multiple
    test files load the same module in one process.
    '''
    import sys
    if 'yt_video_scrape' in sys.modules:
        return sys.modules['yt_video_scrape']

    repo_root: Path = (
        Path(__file__).resolve().parents[2]
    )
    module_path: Path = (
        repo_root / 'tools' / 'yt_video_scrape.py'
    )
    spec = importlib.util.spec_from_file_location(
        'yt_video_scrape', module_path,
    )
    assert (
        spec is not None
        and spec.loader is not None
    )
    module: ModuleType = (
        importlib.util.module_from_spec(spec)
    )
    sys.modules['yt_video_scrape'] = module
    spec.loader.exec_module(module)
    return module


yt_video_scrape: ModuleType = _load_yt_video_scrape()
_video_has_formats_on_server = (
    yt_video_scrape._video_has_formats_on_server
)

# Constants mirrored from ExchangeClient so tests are self-contained.
_GET_DATA_PARAM: str = '/api/data/v1/param'
_EXCHANGE_URL: str = 'https://scrape.exchange'
_SCHEMA_OWNER: str = 'testuser'
_SCHEMA_VERSION: str = '0.0.1'
_VIDEO_ID: str = 'dQw4w9WgXcQ'
_DATA_URL: str = 'https://data.scrape.exchange/payload/abc123'

# Expected metadata endpoint URL assembled from the above constants.
_METADATA_URL: str = (
    f'{_EXCHANGE_URL}'
    f'{_GET_DATA_PARAM}'
    f'/{_SCHEMA_OWNER}'
    f'/youtube/video'
    f'/{_SCHEMA_VERSION}'
    f'/{_VIDEO_ID}'
)


def _make_settings() -> MagicMock:
    '''Return a minimal mock of VideoSettings.'''
    settings: MagicMock = MagicMock()
    settings.exchange_url = _EXCHANGE_URL
    settings.schema_owner = _SCHEMA_OWNER
    settings.schema_version = _SCHEMA_VERSION
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


class TestVideoHasFormatsOnServer(unittest.IsolatedAsyncioTestCase):

    def _make_client(self) -> MagicMock:
        '''Return a mock ExchangeClient with an AsyncMock get().'''
        client: MagicMock = MagicMock()
        client.get = AsyncMock()
        return client

    async def test_returns_true_when_formats_present(
        self,
    ) -> None:
        '''
        Happy path: metadata 200 with data_url, data 200 with a
        non-empty formats list => True.
        '''
        settings: MagicMock = _make_settings()
        client: MagicMock = self._make_client()

        metadata_resp: MagicMock = _make_response(
            200, {'data_url': _DATA_URL},
        )
        data_resp: MagicMock = _make_response(
            200,
            {'formats': [{'ext': 'mp4', 'height': 720}]},
        )

        async def _get(url: str) -> MagicMock:
            if url == _METADATA_URL:
                return metadata_resp
            return data_resp

        client.get.side_effect = _get

        result: bool = await _video_has_formats_on_server(
            client, settings, _VIDEO_ID,
        )
        self.assertTrue(result)

    async def test_returns_false_when_metadata_not_200(
        self,
    ) -> None:
        '''Metadata GET returns 404 => False.'''
        settings: MagicMock = _make_settings()
        client: MagicMock = self._make_client()
        client.get.return_value = _make_response(404)

        result: bool = await _video_has_formats_on_server(
            client, settings, _VIDEO_ID,
        )
        self.assertFalse(result)

    async def test_returns_false_when_metadata_get_raises(
        self,
    ) -> None:
        '''Exception during metadata GET => False (fail-open).'''
        settings: MagicMock = _make_settings()
        client: MagicMock = self._make_client()
        client.get.side_effect = ConnectionError('network error')

        result: bool = await _video_has_formats_on_server(
            client, settings, _VIDEO_ID,
        )
        self.assertFalse(result)

    async def test_returns_false_when_data_url_missing(
        self,
    ) -> None:
        '''
        Metadata 200 but JSON has no 'data_url' key => False.
        No second HTTP call should be made.
        '''
        settings: MagicMock = _make_settings()
        client: MagicMock = self._make_client()
        client.get.return_value = _make_response(200, {})

        result: bool = await _video_has_formats_on_server(
            client, settings, _VIDEO_ID,
        )
        self.assertFalse(result)
        # Only one call: the metadata GET.
        self.assertEqual(client.get.call_count, 1)

    async def test_returns_false_when_data_url_get_not_200(
        self,
    ) -> None:
        '''data_url present but data GET returns 503 => False.'''
        settings: MagicMock = _make_settings()
        client: MagicMock = self._make_client()

        metadata_resp: MagicMock = _make_response(
            200, {'data_url': _DATA_URL},
        )
        data_resp: MagicMock = _make_response(503)

        async def _get(url: str) -> MagicMock:
            if url == _METADATA_URL:
                return metadata_resp
            return data_resp

        client.get.side_effect = _get

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

        metadata_resp: MagicMock = _make_response(
            200, {'data_url': _DATA_URL},
        )

        async def _get(url: str) -> MagicMock:
            if url == _METADATA_URL:
                return metadata_resp
            raise TimeoutError('data fetch timed out')

        client.get.side_effect = _get

        result: bool = await _video_has_formats_on_server(
            client, settings, _VIDEO_ID,
        )
        self.assertFalse(result)

    async def test_returns_false_when_formats_key_absent(
        self,
    ) -> None:
        '''
        Both GETs return 200 but data JSON has no 'formats' key
        => False.
        '''
        settings: MagicMock = _make_settings()
        client: MagicMock = self._make_client()

        metadata_resp: MagicMock = _make_response(
            200, {'data_url': _DATA_URL},
        )
        data_resp: MagicMock = _make_response(
            200, {'title': 'Some Video'},
        )

        async def _get(url: str) -> MagicMock:
            if url == _METADATA_URL:
                return metadata_resp
            return data_resp

        client.get.side_effect = _get

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

        metadata_resp: MagicMock = _make_response(
            200, {'data_url': _DATA_URL},
        )
        data_resp: MagicMock = _make_response(
            200, {'formats': []},
        )

        async def _get(url: str) -> MagicMock:
            if url == _METADATA_URL:
                return metadata_resp
            return data_resp

        client.get.side_effect = _get

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

                metadata_resp: MagicMock = _make_response(
                    200, {'data_url': _DATA_URL},
                )
                data_resp: MagicMock = _make_response(
                    200, {'formats': bad_value},
                )

                async def _get(
                    url: str,
                    _meta: MagicMock = metadata_resp,
                    _data: MagicMock = data_resp,
                ) -> MagicMock:
                    if url == _METADATA_URL:
                        return _meta
                    return _data

                client.get.side_effect = _get

                result: bool = await _video_has_formats_on_server(
                    client, settings, _VIDEO_ID,
                )
                self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()
