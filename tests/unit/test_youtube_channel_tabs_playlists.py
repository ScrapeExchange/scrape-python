'''
Unit tests for paging a channel's Playlists tab to the end.
'''

import logging
import unittest
from typing import Any
from unittest.mock import AsyncMock

from scrape_exchange.youtube.youtube_channel_tabs import YouTubeChannelTabs


CHANNEL_ID: str = 'UCtest123'

METRIC_BASE: dict[str, str] = {
    'platform': 'youtube',
    'scraper': 'test',
    'entity': 'channel',
    'tab': 'playlists',
    'worker_id': 'test',
}

_TABS_LOGGER: logging.Logger = logging.getLogger(
    'scrape_exchange.youtube.youtube_channel_tabs',
)
_TABS_LOGGER_PRIOR_LEVEL: int = _TABS_LOGGER.level


def setUpModule() -> None:
    _TABS_LOGGER.setLevel(logging.CRITICAL)


def tearDownModule() -> None:
    _TABS_LOGGER.setLevel(_TABS_LOGGER_PRIOR_LEVEL)


def _playlist(playlist_id: str) -> dict[str, Any]:
    return {
        'lockupViewModel': {
            'contentType': 'LOCKUP_CONTENT_TYPE_PLAYLIST',
            'contentId': playlist_id,
            'metadata': {
                'lockupMetadataViewModel': {
                    'title': {'content': f'Playlist {playlist_id}'},
                },
            },
        },
    }


def _more(token: str) -> dict[str, Any]:
    return {
        'continuationItemRenderer': {
            'continuationEndpoint': {
                'continuationCommand': {'token': token},
            },
        },
    }


def _continuation(
    playlist_ids: list[str], token: str = '',
) -> dict[str, Any]:
    items: list[dict[str, Any]] = [
        _playlist(playlist_id) for playlist_id in playlist_ids
    ]
    if token:
        items.append(_more(token))
    return {
        'onResponseReceivedActions': [{
            'appendContinuationItemsAction': {
                'continuationItems': items,
            },
        }],
    }


class TestPagePlaylists(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.tabs: YouTubeChannelTabs = YouTubeChannelTabs(CHANNEL_ID)

    async def _page(
        self, first_items: list[dict[str, Any]],
    ) -> Any:
        return await self.tabs._page_playlists(
            first_items, METRIC_BASE, 'playlists',
        )

    async def test_playlists_paged_to_end(self) -> None:
        first_items: list[dict[str, Any]] = [
            _playlist('PL1'), _playlist('PL2'), _more('t1'),
        ]
        self.tabs._browse = AsyncMock(
            return_value=_continuation(['PL3']),
        )
        playlists = await self._page(first_items)

        self.assertEqual(
            {p.playlist_id for p in playlists}, {'PL1', 'PL2', 'PL3'})
        self.tabs._browse.assert_awaited_once_with(
            continuation_token='t1',
        )
        self.assertEqual(self.tabs.failed_tabs, set())

    async def test_failed_playlist_page_keeps_first_page(self) -> None:
        first_items: list[dict[str, Any]] = [
            _playlist('PL1'), _playlist('PL2'), _more('t1'),
        ]
        self.tabs._browse = AsyncMock(side_effect=RuntimeError('boom'))
        playlists = await self._page(first_items)

        self.assertEqual(
            {p.playlist_id for p in playlists}, {'PL1', 'PL2'})
        self.assertIn('playlists', self.tabs.failed_tabs)

    async def test_repeated_token_stops(self) -> None:
        first_items: list[dict[str, Any]] = [
            _playlist('PL1'), _more('t1'),
        ]
        self.tabs._browse = AsyncMock(
            return_value=_continuation(['PL2'], token='t1'),
        )
        playlists = await self._page(first_items)

        self.assertEqual(
            {p.playlist_id for p in playlists}, {'PL1', 'PL2'})
        self.assertEqual(self.tabs._browse.await_count, 1)
        self.assertEqual(self.tabs.failed_tabs, set())

    async def test_first_page_without_token_needs_no_browse(self) -> None:
        first_items: list[dict[str, Any]] = [_playlist('PL1')]
        self.tabs._browse = AsyncMock()
        playlists = await self._page(first_items)

        self.assertEqual({p.playlist_id for p in playlists}, {'PL1'})
        self.tabs._browse.assert_not_awaited()


if __name__ == '__main__':
    unittest.main()
