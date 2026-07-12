'''Smoke tests for scrape_exchange.scrape_api.'''

from __future__ import annotations

import json
import unittest
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import brotli
import orjson

from scrape_exchange.datatypes import Platform
from scrape_exchange.scrape_api import (
    EdgeResponse,
    GetDataResponseModel,
    PageInfoResponse,
    PostFilterRequestModel,
    QueryResponseModel,
    filter_data,
    get_data_by_param,
    get_data_by_item_id,
    iter_filter_data,
)


def _make_entry(item_id: str | None = None) -> dict:
    return {
        'item_id': item_id or str(uuid4()),
        'username': 'boinko',
        'schema_username': 'boinko',
        'platform': 'youtube',
        'entity': 'video',
        'version': '1.0.0',
        'platform_content_id': 'dQw4w9WgXcQ',
        'platform_creator_id': None,
        'platform_topic_id': None,
        'source_url': 'https://www.youtube.com/watch?v=dQw4w9WgXcQ',
        'last_modified_timestamp': (
            datetime.now(UTC).isoformat()
        ),
        'created_timestamp': datetime.now(UTC).isoformat(),
        'data_url': 'https://files.scrape.exchange/item.json.br',
    }


def _make_mock_response(payload: dict | bytes) -> MagicMock:
    resp: MagicMock = MagicMock()
    resp.raise_for_status = MagicMock()
    if isinstance(payload, (bytes, bytearray)):
        resp.content = bytes(payload)
        resp.json = MagicMock(side_effect=ValueError('binary'))
    else:
        resp.json = MagicMock(return_value=payload)
        resp.content = orjson.dumps(payload)
    return resp


class TestPaginationModels(unittest.TestCase):

    def test_edge_response_round_trip(self) -> None:
        edge: EdgeResponse[dict] = EdgeResponse.model_validate({
            'cursor': 'abc',
            'node': {'foo': 'bar'},
        })
        self.assertEqual(edge.cursor, 'abc')
        self.assertEqual(edge.node, {'foo': 'bar'})

    def test_query_response_round_trip(self) -> None:
        entry: dict = _make_entry()
        page: QueryResponseModel = (
            QueryResponseModel[GetDataResponseModel].model_validate({
                'total_count': 1,
                'edges': [
                    {'cursor': 'x', 'node': entry},
                ],
                'page_info': {
                    'has_next_page': False,
                    'end_cursor': None,
                },
            })
        )
        self.assertEqual(page.total_count, 1)
        self.assertEqual(len(page.edges), 1)
        node: GetDataResponseModel = page.edges[0].node
        self.assertEqual(node.platform, Platform.YOUTUBE)
        self.assertEqual(node.entity, 'video')


class TestPostFilterRequest(unittest.TestCase):

    def test_serialise_excludes_none(self) -> None:
        req: PostFilterRequestModel = PostFilterRequestModel(
            platform=Platform.YOUTUBE, entity='channel',
        )
        payload: dict = req.model_dump(
            mode='json', exclude_none=True,
        )
        self.assertNotIn('username', payload)
        self.assertNotIn('after', payload)
        self.assertEqual(payload['platform'], 'youtube')
        self.assertEqual(payload['entity'], 'channel')
        self.assertEqual(payload['first'], 100)


class TestEndpointWrappers(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_get_data_by_item_id_calls_path(self) -> None:
        entry: dict = _make_entry()
        client: MagicMock = MagicMock()
        client.base_url = 'https://scrape.exchange'
        client.get = AsyncMock(
            return_value=_make_mock_response(entry),
        )
        result: GetDataResponseModel = await get_data_by_item_id(
            client, entry['item_id'],
        )
        client.get.assert_awaited_once()
        called_url: str = client.get.await_args.args[0]
        self.assertIn(
            f'/api/v1/data/item_id/{entry["item_id"]}',
            called_url,
        )
        self.assertEqual(str(result.item_id), entry['item_id'])

    async def test_get_data_by_param_serialises_platform_enum(
        self,
    ) -> None:
        entry: dict = _make_entry()
        client: MagicMock = MagicMock()
        client.exchange_url = 'https://scrape.exchange'
        client.get = AsyncMock(
            return_value=_make_mock_response(entry),
        )

        await get_data_by_param(
            client,
            username='drand',
            platform=Platform.YOUTUBE,
            entity='channel',
            version='0.0.2',
            platform_content_id='UC0RqFH_a-Ha43-Q2ZYVhX_A',
        )

        called_url: str = client.get.await_args.args[0]
        self.assertIn('/drand/youtube/channel/0.0.2/', called_url)
        self.assertNotIn('Platform.YOUTUBE', called_url)

    async def test_filter_data_posts_json(self) -> None:
        entry: dict = _make_entry()
        page_payload: dict = {
            'total_count': 1,
            'edges': [{'cursor': 'c', 'node': entry}],
            'page_info': {
                'has_next_page': False, 'end_cursor': None,
            },
        }
        client: MagicMock = MagicMock()
        client.base_url = 'https://scrape.exchange'
        client.post = AsyncMock(
            return_value=_make_mock_response(page_payload),
        )
        page: QueryResponseModel = await filter_data(
            client,
            PostFilterRequestModel(
                platform=Platform.YOUTUBE, entity='video',
            ),
        )
        client.post.assert_awaited_once()
        called_kwargs: dict = client.post.await_args.kwargs
        self.assertIn('json', called_kwargs)
        self.assertEqual(
            called_kwargs['json']['platform'], 'youtube',
        )
        self.assertEqual(len(page.edges), 1)


class TestIterFilterData(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_walks_pages_until_end(self) -> None:
        entries: list[dict] = [
            _make_entry() for _ in range(3)
        ]
        page1: dict = {
            'total_count': 3,
            'edges': [
                {'cursor': 'c1', 'node': entries[0]},
                {'cursor': 'c2', 'node': entries[1]},
            ],
            'page_info': {
                'has_next_page': True, 'end_cursor': 'c2',
            },
        }
        page2: dict = {
            'total_count': 3,
            'edges': [{'cursor': 'c3', 'node': entries[2]}],
            'page_info': {
                'has_next_page': False, 'end_cursor': 'c3',
            },
        }
        responses: list = [
            _make_mock_response(page1),
            _make_mock_response(page2),
        ]
        client: MagicMock = MagicMock()
        client.base_url = 'https://scrape.exchange'
        client.post = AsyncMock(side_effect=responses)
        seen: list = []
        async for entry in iter_filter_data(
            client, PostFilterRequestModel(),
        ):
            seen.append(entry)
        self.assertEqual(len(seen), 3)
        self.assertEqual(client.post.await_count, 2)
        # Second call must carry the cursor from page 1.
        second_kwargs: dict = (
            client.post.await_args_list[1].kwargs
        )
        self.assertEqual(
            second_kwargs['json']['after'], 'c2',
        )


class TestDataDictFetch(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_brotli_payload_round_trips(self) -> None:
        from scrape_exchange.scrape_api import _fetch_data_dict

        record: dict = {'channel_handle': 'a', 'video_ids': ['x']}
        compressed: bytes = brotli.compress(orjson.dumps(record))
        entry: GetDataResponseModel = (
            GetDataResponseModel.model_validate(_make_entry())
        )
        client: MagicMock = MagicMock()
        client.get = AsyncMock(
            return_value=_make_mock_response(compressed),
        )
        result: dict = await _fetch_data_dict(client, entry)
        self.assertEqual(result, record)

    async def test_plain_json_payload_fallback(self) -> None:
        from scrape_exchange.scrape_api import _fetch_data_dict

        record: dict = {'video_id': 'plain'}
        entry: GetDataResponseModel = (
            GetDataResponseModel.model_validate(_make_entry())
        )
        client: MagicMock = MagicMock()
        client.get = AsyncMock(
            return_value=_make_mock_response(
                json.dumps(record).encode(),
            ),
        )
        result: dict = await _fetch_data_dict(client, entry)
        self.assertEqual(result, record)


if __name__ == '__main__':
    unittest.main()
