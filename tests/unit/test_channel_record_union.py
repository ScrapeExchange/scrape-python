'''
An incomplete channel enumeration keeps every video ID the channel's
local record already holds: the newly scraped IDs are unioned with
the previous local record and the exchange's video IDs before the
record is written.
'''

import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.youtube.channel_video_refresh import FilterQueryError
from scrape_exchange.youtube.youtube_channel import YouTubeChannel
from tools import yt_channel_scrape as scraper


EXCHANGE_URL: str = 'https://scrape.exchange'
CHANNEL_ID: str = 'UCexample'
FILENAME: str = f'channel-{CHANNEL_ID}.json.br'


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
        self.video_ids: AsyncMock = AsyncMock(return_value={'exch1'})
        patcher = patch.object(
            scraper, 'fetch_exchange_video_ids', self.video_ids,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    async def _union(self, exchange_ids: set[str] | None = None) -> None:
        await scraper._union_previous_video_ids(
            self.fm, FILENAME, self.channel,
            scraper.ExchangeVideoIds(
                AsyncMock(), EXCHANGE_URL, CHANNEL_ID, known=exchange_ids,
            ),
        )

    async def test_prefers_local_file(self) -> None:
        await self.fm.write_file(FILENAME, {'video_ids': ['old1']})
        await self._union()
        self.assertEqual(self.channel.video_ids, {'new1', 'old1', 'exch1'})

    async def test_falls_back_to_uploaded_copy(self) -> None:
        await self.fm.write_file(FILENAME, {'video_ids': ['up1']})
        await self.fm.mark_uploaded(FILENAME)
        await self._union()
        self.assertEqual(self.channel.video_ids, {'new1', 'up1', 'exch1'})

    async def test_no_local_record_keeps_new_and_exchange_ids(
        self,
    ) -> None:
        await self._union()
        self.assertEqual(self.channel.video_ids, {'new1', 'exch1'})

    async def test_uses_given_exchange_ids(self) -> None:
        await self._union(exchange_ids={'given1'})
        self.assertEqual(self.channel.video_ids, {'new1', 'given1'})
        self.video_ids.assert_not_awaited()

    async def test_filter_error_keeps_local_union(self) -> None:
        # An exchange outage must not fail the channel's scrape.
        await self.fm.write_file(FILENAME, {'video_ids': ['old1']})
        self.video_ids.side_effect = FilterQueryError('down')
        await self._union()
        self.assertEqual(self.channel.video_ids, {'new1', 'old1'})


class TestExchangeVideoIds(unittest.IsolatedAsyncioTestCase):

    async def test_fetches_once(self) -> None:
        fetch: AsyncMock = AsyncMock(return_value={'v1'})
        ids: scraper.ExchangeVideoIds = scraper.ExchangeVideoIds(
            AsyncMock(), EXCHANGE_URL, CHANNEL_ID,
        )
        self.assertIsNone(ids.fetched)
        with patch.object(scraper, 'fetch_exchange_video_ids', fetch):
            self.assertEqual(await ids.get(), {'v1'})
            self.assertEqual(await ids.get(), {'v1'})
        fetch.assert_awaited_once()
        self.assertEqual(ids.fetched, {'v1'})

    async def test_known_ids_are_not_fetched(self) -> None:
        fetch: AsyncMock = AsyncMock()
        ids: scraper.ExchangeVideoIds = scraper.ExchangeVideoIds(
            AsyncMock(), EXCHANGE_URL, CHANNEL_ID, known={'k1'},
        )
        with patch.object(scraper, 'fetch_exchange_video_ids', fetch):
            self.assertEqual(await ids.get(), {'k1'})
        fetch.assert_not_awaited()

    async def test_failure_is_retried(self) -> None:
        fetch: AsyncMock = AsyncMock(
            side_effect=[FilterQueryError('down'), {'v1'}],
        )
        ids: scraper.ExchangeVideoIds = scraper.ExchangeVideoIds(
            AsyncMock(), EXCHANGE_URL, CHANNEL_ID,
        )
        with patch.object(scraper, 'fetch_exchange_video_ids', fetch):
            with self.assertRaises(FilterQueryError):
                await ids.get()
            self.assertEqual(await ids.get(), {'v1'})


class TestLegacyPathRejectsPartial(unittest.IsolatedAsyncioTestCase):

    async def test_partial_scrape_is_a_failure(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(
            'example', channel_id=CHANNEL_ID, with_download_client=False,
        )

        async def scrape(chan: YouTubeChannel, *_args) -> str:
            chan.partial_error = 'Failed channel tabs: playlists'
            chan.content_partial = True
            return 'proxy'

        with patch.object(
            scraper, '_try_scrape_channel_typed', side_effect=scrape,
        ):
            ok, proxy = await scraper._try_scrape_channel(
                channel, AsyncMock(), AsyncMock(), CHANNEL_ID, {},
            )
        self.assertFalse(ok)
        self.assertIsNone(proxy)

    async def test_about_only_partial_is_a_success(self) -> None:
        # /about is best-effort on this path, as it always was.
        channel: YouTubeChannel = YouTubeChannel(
            'example', channel_id=CHANNEL_ID, with_download_client=False,
        )

        async def scrape(chan: YouTubeChannel, *_args) -> str:
            chan.partial_error = 'About page scrape failed'
            return 'proxy'

        with patch.object(
            scraper, '_try_scrape_channel_typed', side_effect=scrape,
        ):
            ok, proxy = await scraper._try_scrape_channel(
                channel, AsyncMock(), AsyncMock(), CHANNEL_ID, {},
            )
        self.assertTrue(ok)
        self.assertEqual(proxy, 'proxy')


if __name__ == '__main__':
    unittest.main()
