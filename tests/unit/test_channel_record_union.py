'''
An incomplete channel enumeration keeps every video ID the channel's
record already holds: the newly scraped IDs are unioned with the
previous record (local, uploaded or on the exchange) and the
exchange's video IDs before the record is written.
'''

import tempfile
import unittest
from typing import Any
from unittest.mock import AsyncMock, patch

import brotli
import httpx2 as httpx
import orjson

from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.youtube.channel_video_refresh import (
    EXCHANGE_DATA_NOT_FOUND,
    ExchangeRecordError,
    FilterQueryError,
    fetch_exchange_channel_video_ids,
)
from scrape_exchange.youtube.youtube_channel import YouTubeChannel
from tools import yt_channel_scrape as scraper


EXCHANGE_URL: str = 'https://scrape.exchange'
CHANNEL_ID: str = 'UCexample'
CONTENT_PATH: str = f'/api/v1/data/content/youtube/channel/{CHANNEL_ID}'
FILENAME: str = f'channel-{CHANNEL_ID}.json.br'


def _node(data_url: str) -> dict[str, Any]:
    return {
        'item_id': '00000000-0000-0000-0000-000000000001',
        'username': 'uploader',
        'schema_username': 'boinko',
        'platform': 'youtube',
        'entity': 'channel',
        'version': '0.0.2',
        'platform_content_id': CHANNEL_ID,
        'source_url': f'https://www.youtube.com/channel/{CHANNEL_ID}',
        'last_modified_timestamp': '2026-10-01T00:00:00Z',
        'created_timestamp': '2026-01-01T00:00:00Z',
        'data_url': data_url,
    }


def _records(*data_urls: str) -> httpx.Response:
    return httpx.Response(200, json={
        'total_count': len(data_urls),
        'edges': [
            {'cursor': str(index), 'node': _node(url)}
            for index, url in enumerate(data_urls)
        ],
        'page_info': {'has_next_page': False, 'end_cursor': None},
    })


def _not_found() -> httpx.Response:
    return httpx.Response(404, json={'detail': EXCHANGE_DATA_NOT_FOUND})


def _payload(video_ids: list[str]) -> httpx.Response:
    return httpx.Response(200, content=brotli.compress(
        orjson.dumps({'channel_id': CHANNEL_ID, 'video_ids': video_ids}),
    ))


class TestFetchExchangeChannelVideoIds(unittest.IsolatedAsyncioTestCase):

    async def _fetch(self, respond) -> set[str]:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(respond),
        ) as client:
            return await fetch_exchange_channel_video_ids(
                client, EXCHANGE_URL, CHANNEL_ID,
            )

    async def test_no_record_returns_empty(self) -> None:
        self.assertEqual(await self._fetch(lambda _r: _not_found()), set())

    async def test_unions_video_ids_of_every_record(self) -> None:
        def respond(request: httpx.Request) -> httpx.Response:
            if request.url.path == CONTENT_PATH:
                return _records(
                    'https://files.scrape.exchange/a.json.br',
                    '/files/b.json.br',
                )
            if request.url.path == '/a.json.br':
                return _payload(['v1', 'v2'])
            if request.url.path == '/files/b.json.br':
                return _payload(['v3'])
            return httpx.Response(500)

        self.assertEqual(await self._fetch(respond), {'v1', 'v2', 'v3'})

    async def test_payload_failure_raises(self) -> None:
        def respond(request: httpx.Request) -> httpx.Response:
            if request.url.path == CONTENT_PATH:
                return _records('https://files.scrape.exchange/a.json.br')
            return httpx.Response(503)

        with self.assertRaises(ExchangeRecordError):
            await self._fetch(respond)

    async def test_other_404_raises(self) -> None:
        with self.assertRaises(ExchangeRecordError):
            await self._fetch(lambda _r: httpx.Response(404))

    async def test_transport_error_raises(self) -> None:
        def respond(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError('down', request=request)

        with self.assertRaises(ExchangeRecordError):
            await self._fetch(respond)


class TestUnionPreviousVideoIds(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self.directory: tempfile.TemporaryDirectory = (
            tempfile.TemporaryDirectory()
        )
        self.addCleanup(self.directory.cleanup)
        self.fm: AssetFileManagement = AssetFileManagement(
            self.directory.name,
        )
        self.channel: YouTubeChannel = YouTubeChannel(
            'example', channel_id=CHANNEL_ID, with_download_client=False,
        )
        self.channel.video_ids = {'new1'}
        self.record_ids: AsyncMock = AsyncMock(return_value={'rec1'})
        self.video_ids: AsyncMock = AsyncMock(return_value={'exch1'})
        for name, mock in (
            ('fetch_exchange_channel_video_ids', self.record_ids),
            ('fetch_exchange_video_ids', self.video_ids),
        ):
            patcher = patch.object(scraper, name, mock)
            patcher.start()
            self.addCleanup(patcher.stop)

    async def _union(self, exchange_ids: set[str] | None = None) -> None:
        await scraper._union_previous_video_ids(
            self.fm, FILENAME, self.channel,
            http_client=AsyncMock(), exchange_url=EXCHANGE_URL,
            exchange_ids=exchange_ids,
        )

    async def test_prefers_local_file(self) -> None:
        await self.fm.write_file(FILENAME, {'video_ids': ['old1']})
        await self._union()
        self.assertEqual(self.channel.video_ids, {'new1', 'old1', 'exch1'})
        self.record_ids.assert_not_awaited()

    async def test_falls_back_to_uploaded_copy(self) -> None:
        await self.fm.write_file(FILENAME, {'video_ids': ['up1']})
        await self.fm.mark_uploaded(FILENAME)
        await self._union()
        self.assertEqual(self.channel.video_ids, {'new1', 'up1', 'exch1'})
        self.record_ids.assert_not_awaited()

    async def test_falls_back_to_exchange_record(self) -> None:
        await self._union()
        self.assertEqual(self.channel.video_ids, {'new1', 'rec1', 'exch1'})

    async def test_new_channel_keeps_new_ids(self) -> None:
        self.record_ids.return_value = set()
        self.video_ids.return_value = set()
        await self._union()
        self.assertEqual(self.channel.video_ids, {'new1'})

    async def test_uses_given_exchange_ids(self) -> None:
        await self._union(exchange_ids={'given1'})
        self.assertEqual(
            self.channel.video_ids, {'new1', 'rec1', 'given1'},
        )
        self.video_ids.assert_not_awaited()

    async def test_record_error_propagates(self) -> None:
        self.record_ids.side_effect = ExchangeRecordError('down')
        with self.assertRaises(ExchangeRecordError):
            await self._union()

    async def test_filter_error_propagates(self) -> None:
        self.video_ids.side_effect = FilterQueryError('down')
        with self.assertRaises(FilterQueryError):
            await self._union()



class TestLegacyPathRejectsPartial(unittest.IsolatedAsyncioTestCase):

    async def test_partial_scrape_is_a_failure(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(
            'example', channel_id=CHANNEL_ID, with_download_client=False,
        )

        async def scrape(chan: YouTubeChannel, *_args) -> str:
            chan.partial_error = 'Failed channel tabs: playlists'
            return 'proxy'

        with patch.object(
            scraper, '_try_scrape_channel_typed', side_effect=scrape,
        ):
            ok, proxy = await scraper._try_scrape_channel(
                channel, AsyncMock(), AsyncMock(), CHANNEL_ID, {},
            )
        self.assertFalse(ok)
        self.assertIsNone(proxy)

if __name__ == '__main__':
    unittest.main()
