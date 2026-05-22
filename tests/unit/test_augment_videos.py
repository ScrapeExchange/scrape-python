'''Tests for tools/augment_videos.py.'''

import importlib.util
import os
import tempfile
import unittest
import uuid

from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import brotli
import orjson

from scrape_exchange.scrape_api import (
    GetDataResponseModel,
    QueryResponseModel,
)


def _load_tool() -> ModuleType:
    import sys
    if 'augment_videos' in sys.modules:
        return sys.modules['augment_videos']
    repo_root: Path = Path(__file__).resolve().parents[2]
    module_path: Path = repo_root / 'tools' / 'augment_videos.py'
    spec = importlib.util.spec_from_file_location(
        'augment_videos', module_path,
    )
    assert spec is not None and spec.loader is not None
    module: ModuleType = importlib.util.module_from_spec(spec)
    sys.modules['augment_videos'] = module
    spec.loader.exec_module(module)
    return module


tool: ModuleType = _load_tool()


class _EmptyMapping:

    def get(self, video_id: str) -> None:
        return None


class _MutableMapping:

    def __init__(self) -> None:
        self.data: dict[str, dict] = {}

    def get(self, video_id: str) -> dict | None:
        return self.data.get(video_id)

    def __setitem__(self, video_id: str, info: dict) -> None:
        self.data[video_id] = info


def _make_node(**fields: Any) -> dict:
    '''Build a valid GetDataResponseModel-shaped dict for tests.

    Required GetDataResponseModel fields all get reasonable
    defaults; callers override only what's relevant.
    '''
    base: dict = {
        'item_id': str(uuid.uuid4()),
        'username': 'mock',
        'schema_username': 'mock',
        'platform': 'youtube',
        'entity': 'channel',
        'version': '1.0.0',
        'platform_content_id': 'UCmock',
        'source_url': 'https://www.youtube.com/@mock',
        'last_modified_timestamp': '2026-05-19T00:00:00Z',
        'created_timestamp': '2026-05-19T00:00:00Z',
        'data_url': '/api/v1/data/mock',
    }
    base.update(fields)
    return base


def _make_filter_response(
    *nodes: dict,
) -> QueryResponseModel[GetDataResponseModel]:
    return QueryResponseModel[
        GetDataResponseModel
    ].model_validate({
        'total_count': len(nodes),
        'edges': [
            {'cursor': f'c{i}', 'node': n}
            for i, n in enumerate(nodes)
        ],
        'page_info': {
            'has_next_page': False, 'end_cursor': None,
        },
    })


class TestSyncHelpers(unittest.TestCase):

    def test_video_name_parser_accepts_min_and_dlp_files(self) -> None:
        self.assertEqual(
            tool._video_id_from_name('video-min-abc123.json.br'),
            'abc123',
        )
        self.assertEqual(
            tool._video_id_from_name('video-dlp-abc123.json.br'),
            'abc123',
        )
        self.assertIsNone(
            tool._video_id_from_name('video-dlp-abc123.json.br.failed'),
        )
        self.assertFalse(tool._is_video_file('channel-abc123.json.br'))

    def test_read_plain_json_marks_file_for_rewrite(self) -> None:
        payload: dict = {'video_id': 'plain', 'title': 'ok'}
        with tempfile.TemporaryDirectory() as td:
            path: Path = Path(td) / 'video-min-plain.json.br'
            path.write_bytes(orjson.dumps(payload))

            parsed: dict | None
            needs_rewrite: bool
            parsed, needs_rewrite = tool._read_json_br(path)

        self.assertEqual(parsed, payload)
        self.assertTrue(needs_rewrite)


class TestProcessOneVideo(unittest.IsolatedAsyncioTestCase):
    '''_process_one_video became async after the migration to
    scrape_exchange.scrape_api; every test calls it via await
    and passes an ExchangeClient mock.'''

    def _client(self) -> MagicMock:
        '''Build a stand-in for ExchangeClient. Tests that
        exercise the API path additionally patch
        ``tool.filter_data`` / ``tool.fetch_dict_url``; tests
        that don't go through the API leave the mock untouched.'''
        client: MagicMock = MagicMock(name='ExchangeClient')
        return client

    async def test_process_video_rewrites_plain_json_file(
        self,
    ) -> None:
        payload: dict = {
            'video_id': 'plain',
            'channel_id': 'UCx',
            'channel_handle': '@plain',
            'channel_url': 'https://www.youtube.com/@plain',
            'channel_is_verified': False,
            'channel_follower_count': 1,
        }
        with tempfile.TemporaryDirectory() as td:
            path: Path = Path(td) / 'video-min-plain.json.br'
            path.write_bytes(orjson.dumps(payload))
            with os.scandir(td) as entries:
                entry: os.DirEntry = next(entries)

                result = await tool._process_one_video(
                    entry, _EmptyMapping(), dry_run=False,
                    client=self._client(),
                )

            decoded: dict = orjson.loads(
                brotli.decompress(path.read_bytes()),
            )

        self.assertTrue(result.rewritten)
        self.assertFalse(result.needed_augmentation)
        self.assertEqual(decoded['video_id'], payload['video_id'])
        self.assertEqual(decoded['channel_id'], payload['channel_id'])
        self.assertEqual(
            decoded['channel_handle'], payload['channel_handle'],
        )
        self.assertEqual(decoded['channel_url'], payload['channel_url'])
        self.assertEqual(
            decoded['channel_follower_count'],
            payload['channel_follower_count'],
        )

    async def test_process_video_rewrites_brotli_with_trailing_garbage(
        self,
    ) -> None:
        payload: dict = {
            'video_id': 'garbage',
            'channel_id': 'UCx',
            'channel_handle': '@garbage',
            'channel_url': 'https://www.youtube.com/@garbage',
            'channel_is_verified': False,
            'channel_follower_count': 1,
        }
        encoded: bytes = orjson.dumps(payload)
        with tempfile.TemporaryDirectory() as td:
            path: Path = Path(td) / 'video-min-garbage.json.br'
            path.write_bytes(brotli.compress(encoded) + b'junk')
            with os.scandir(td) as entries:
                entry: os.DirEntry = next(entries)

                result = await tool._process_one_video(
                    entry, _EmptyMapping(), dry_run=False,
                    client=self._client(),
                )

            decoded: dict = orjson.loads(
                brotli.decompress(path.read_bytes()),
            )

        self.assertTrue(result.rewritten)
        self.assertFalse(result.needed_augmentation)
        self.assertEqual(decoded['video_id'], payload['video_id'])
        self.assertEqual(decoded['channel_id'], payload['channel_id'])
        self.assertEqual(
            decoded['channel_handle'], payload['channel_handle'],
        )
        self.assertEqual(decoded['channel_url'], payload['channel_url'])
        self.assertEqual(
            decoded['channel_follower_count'],
            payload['channel_follower_count'],
        )

    async def test_process_video_augments_from_local_mapping(
        self,
    ) -> None:
        video: dict = {
            'video_id': 'local',
            'channel_id': 'UClocal',
            'channel_handle': None,
            'channel_url': None,
            'channel_is_verified': None,
            'channel_follower_count': None,
        }
        mapping = _MutableMapping()
        mapping['local'] = {
            'channel_id': 'UClocal',
            'channel_handle': 'localhandle',
            'channel_url': 'https://www.youtube.com/@localhandle',
            'channel_is_verified': True,
            'subscriber_count': 44,
        }
        with tempfile.TemporaryDirectory() as td:
            path: Path = Path(td) / 'video-min-local.json.br'
            path.write_bytes(brotli.compress(orjson.dumps(video)))
            with os.scandir(td) as entries:
                entry: os.DirEntry = next(entries)
                result = await tool._process_one_video(
                    entry, mapping, dry_run=False,
                    client=self._client(),
                )

            decoded: dict = orjson.loads(
                brotli.decompress(path.read_bytes()),
            )

        self.assertTrue(result.needed_augmentation)
        self.assertTrue(result.augmented_from_local)
        self.assertFalse(result.augmented_from_api)
        self.assertFalse(result.unresolved)
        self.assertEqual(decoded['channel_handle'], 'localhandle')
        self.assertEqual(decoded['channel_follower_count'], 44)

    async def test_process_video_dlp_file_augments_from_local_mapping(
        self,
    ) -> None:
        video: dict = {
            'video_id': 'dlp',
            'channel_id': 'UCdlp',
            'channel_handle': None,
            'channel_url': None,
            'channel_is_verified': None,
            'channel_follower_count': None,
        }
        mapping = _MutableMapping()
        mapping['dlp'] = {
            'channel_id': 'UCdlp',
            'channel_handle': 'dlphandle',
            'channel_url': 'https://www.youtube.com/@dlphandle',
            'channel_is_verified': False,
            'subscriber_count': 55,
        }
        with tempfile.TemporaryDirectory() as td:
            path: Path = Path(td) / 'video-dlp-dlp.json.br'
            path.write_bytes(brotli.compress(orjson.dumps(video)))
            with os.scandir(td) as entries:
                entry: os.DirEntry = next(entries)
                self.assertTrue(tool._is_video_file(entry.name))
                result = await tool._process_one_video(
                    entry, mapping, dry_run=False,
                    client=self._client(),
                )

            decoded: dict = orjson.loads(
                brotli.decompress(path.read_bytes()),
            )

        self.assertTrue(result.needed_augmentation)
        self.assertTrue(result.augmented_from_local)
        self.assertEqual(decoded['channel_handle'], 'dlphandle')
        self.assertEqual(decoded['channel_follower_count'], 55)

    async def test_process_video_does_not_need_nice_to_have_fields(
        self,
    ) -> None:
        payload: dict = {
            'video_id': 'has-critical-fields',
            'channel_id': 'UCcritical',
            'channel_handle': 'critical',
            'channel_url': None,
            'channel_is_verified': None,
            'channel_follower_count': None,
        }
        with tempfile.TemporaryDirectory() as td:
            path: Path = (
                Path(td) / 'video-min-has-critical-fields.json.br'
            )
            path.write_bytes(brotli.compress(orjson.dumps(payload)))
            with os.scandir(td) as entries:
                entry: os.DirEntry = next(entries)
                with patch.object(
                    tool, '_fetch_channel_info_from_exchange',
                    new_callable=AsyncMock,
                ) as fetch:
                    result = await tool._process_one_video(
                        entry, _EmptyMapping(), dry_run=False,
                        client=self._client(),
                    )

        self.assertFalse(result.needed_augmentation)
        self.assertFalse(result.unresolved)
        fetch.assert_not_called()

    async def test_process_video_fetches_missing_mapping_from_exchange(
        self,
    ) -> None:
        video: dict = {
            'video_id': 'needs-channel',
            'channel_id': 'UCpreferred',
            'channel_handle': None,
            'channel_url': None,
            'channel_is_verified': None,
            'channel_follower_count': None,
        }
        channel: dict = {
            'channel_id': 'UCpreferred',
            'channel_handle': 'preferred',
            'verified': True,
            'subscriber_count': 1234,
        }
        usernames: list[str | None] = []
        fetched_urls: list[str] = []

        async def fake_filter_data(
            client: Any, filters: Any,
        ) -> QueryResponseModel[GetDataResponseModel]:
            usernames.append(filters.username)
            if filters.username == 'nikkie':
                return _make_filter_response(_make_node(
                    platform_content_id='UCpreferred',
                ))
            return _make_filter_response()

        async def fake_fetch_dict_url(
            client: Any, url: str,
        ) -> dict:
            fetched_urls.append(url)
            return channel

        mapping = _MutableMapping()
        with tempfile.TemporaryDirectory() as td:
            path: Path = Path(td) / 'video-min-needs-channel.json.br'
            path.write_bytes(brotli.compress(orjson.dumps(video)))
            with os.scandir(td) as entries:
                entry: os.DirEntry = next(entries)
                with patch.object(
                    tool, 'filter_data',
                    side_effect=fake_filter_data,
                ), patch.object(
                    tool, 'fetch_dict_url',
                    side_effect=fake_fetch_dict_url,
                ):
                    result = await tool._process_one_video(
                        entry, mapping, dry_run=False,
                        client=self._client(),
                    )

            decoded: dict = orjson.loads(
                brotli.decompress(path.read_bytes()),
            )

        self.assertTrue(result.needed_augmentation)
        self.assertFalse(result.augmented_from_local)
        self.assertTrue(result.augmented_from_api)
        self.assertFalse(result.unresolved)
        # The preferred uploaders are tried in order; the first
        # one (drand) returns no edges, the second (nikkie) hits.
        self.assertEqual(usernames, ['drand', 'nikkie'])
        self.assertEqual(fetched_urls, ['/api/v1/data/mock'])
        self.assertEqual(decoded['channel_handle'], 'preferred')
        self.assertEqual(
            decoded['channel_url'],
            'https://www.youtube.com/@preferred',
        )
        self.assertTrue(decoded['channel_is_verified'])
        self.assertEqual(decoded['channel_follower_count'], 1234)
        self.assertEqual(
            mapping.data['needs-channel']['channel_handle'],
            'preferred',
        )

    async def test_process_video_reuses_cached_exchange_channel_info(
        self,
    ) -> None:
        video: dict = {
            'video_id': 'needs-channel',
            'channel_id': 'UCpreferred',
            'channel_handle': None,
            'channel_url': None,
            'channel_is_verified': None,
            'channel_follower_count': None,
        }
        channel: dict = {
            'channel_id': 'UCpreferred',
            'channel_handle': 'preferred',
            'verified': False,
            'subscriber_count': 7,
        }
        filter_calls: list[str | None] = []

        async def fake_filter_data(
            client: Any, filters: Any,
        ) -> QueryResponseModel[GetDataResponseModel]:
            filter_calls.append(filters.username)
            return _make_filter_response(_make_node(
                platform_content_id='UCpreferred',
            ))

        async def fake_fetch_dict_url(
            client: Any, url: str,
        ) -> dict:
            return channel

        cache: dict[tuple[str, str], dict | None] = {}
        with tempfile.TemporaryDirectory() as td:
            for name in ('one', 'two'):
                path: Path = Path(td) / f'video-min-{name}.json.br'
                body: dict = video | {'video_id': name}
                path.write_bytes(brotli.compress(orjson.dumps(body)))

            with patch.object(
                tool, 'filter_data',
                side_effect=fake_filter_data,
            ), patch.object(
                tool, 'fetch_dict_url',
                side_effect=fake_fetch_dict_url,
            ):
                for entry in os.scandir(td):
                    result = await tool._process_one_video(
                        entry, _MutableMapping(), dry_run=False,
                        client=self._client(),
                        channel_api_cache=cache,
                    )
                    self.assertTrue(result.augmented_from_api)

        # First video hits the API (one filter call for the
        # first preferred uploader); the second video reuses the
        # cache entry without another call.
        self.assertEqual(len(filter_calls), 1)
        self.assertEqual(
            cache[('platform_content_id', 'UCpreferred')][
                'channel_handle'
            ],
            'preferred',
        )

    async def test_process_video_reuses_cached_exchange_miss(
        self,
    ) -> None:
        video: dict = {
            'video_id': 'needs-channel',
            'channel_id': 'UCmissing',
            'channel_handle': None,
            'channel_url': None,
            'channel_is_verified': None,
            'channel_follower_count': None,
        }
        usernames: list[str | None] = []

        async def fake_filter_data(
            client: Any, filters: Any,
        ) -> QueryResponseModel[GetDataResponseModel]:
            usernames.append(filters.username)
            return _make_filter_response()

        cache: dict[tuple[str, str], dict | None] = {}
        with tempfile.TemporaryDirectory() as td:
            for name in ('one', 'two'):
                path: Path = Path(td) / f'video-min-{name}.json.br'
                body: dict = video | {'video_id': name}
                path.write_bytes(brotli.compress(orjson.dumps(body)))

            with patch.object(
                tool, 'filter_data',
                side_effect=fake_filter_data,
            ):
                for entry in os.scandir(td):
                    result = await tool._process_one_video(
                        entry, _MutableMapping(), dry_run=False,
                        client=self._client(),
                        channel_api_cache=cache,
                    )
                    self.assertTrue(result.needed_augmentation)
                    self.assertTrue(result.unresolved)

        self.assertEqual(
            usernames,
            ['drand', 'nikkie', 'boinko', 'leady', None],
        )
        self.assertIsNone(
            cache[('platform_content_id', 'UCmissing')],
        )

    async def test_process_video_reports_unmitigated_augmentation_need(
        self,
    ) -> None:
        payload: dict = {
            'video_id': 'missing-fields',
            'channel_id': 'UCmissing',
            'channel_handle': None,
            'channel_url': None,
            'channel_is_verified': None,
            'channel_follower_count': None,
        }
        with tempfile.TemporaryDirectory() as td:
            path: Path = Path(td) / 'video-min-missing-fields.json.br'
            path.write_bytes(brotli.compress(orjson.dumps(payload)))
            with os.scandir(td) as entries:
                entry: os.DirEntry = next(entries)
                with patch.object(
                    tool, '_fetch_channel_info_from_exchange',
                    new_callable=AsyncMock,
                    return_value={'channel_id': 'UCmissing'},
                ):
                    result = await tool._process_one_video(
                        entry, _EmptyMapping(), dry_run=False,
                        client=self._client(),
                    )

        self.assertTrue(result.needed_augmentation)
        self.assertTrue(result.unresolved)


if __name__ == '__main__':
    unittest.main()
