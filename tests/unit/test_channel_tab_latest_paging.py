'''
Unit tests for 'Latest'-ordered paging of the videos, shorts and live
channel tabs, with an early stop once a page holds only known IDs.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import logging
import unittest
from unittest.mock import AsyncMock

from scrape_exchange.youtube.youtube_channel_tabs import YouTubeChannelTabs


CHANNEL_ID: str = 'UCtest123'

_TABS_LOGGER: logging.Logger = logging.getLogger(
    'scrape_exchange.youtube.youtube_channel_tabs',
)
_TABS_LOGGER_PRIOR_LEVEL: int = _TABS_LOGGER.level


def setUpModule() -> None:
    _TABS_LOGGER.setLevel(logging.ERROR)


def tearDownModule() -> None:
    _TABS_LOGGER.setLevel(_TABS_LOGGER_PRIOR_LEVEL)


def _item(video_id: str) -> dict:
    '''Item parseable on every tab: lockupViewModel for videos/live,
    shortsLockupViewModel for shorts.'''
    return {
        'richItemRenderer': {
            'content': {
                'lockupViewModel': {'contentId': video_id},
                'shortsLockupViewModel': {
                    'onTap': {
                        'innertubeCommand': {
                            'commandMetadata': {
                                'webCommandMetadata': {
                                    'url': f'/shorts/{video_id}',
                                },
                            },
                        },
                    },
                },
            },
        },
    }


def _more(token: str) -> dict:
    return {
        'continuationItemRenderer': {
            'continuationEndpoint': {
                'continuationCommand': {'token': token},
            },
        },
    }


def _chip(text: str, selected: bool, token: str) -> dict:
    return {
        'chipViewModel': {
            'text': text,
            'selected': selected,
            'tapCommand': {
                'innertubeCommand': {
                    'continuationCommand': {'token': token},
                },
            },
        },
    }


def _tab_page(
    title: str, items: list[dict], chips: list[dict] | None,
) -> dict:
    grid: dict = {'contents': items}
    if chips is not None:
        grid['header'] = {'chipBarViewModel': {'chips': chips}}
    return {
        'contents': {
            'twoColumnBrowseResultsRenderer': {
                'tabs': [{
                    'tabRenderer': {
                        'title': title,
                        'content': {'richGridRenderer': grid},
                    },
                }],
            },
        },
    }


def _continuation(items: list[dict]) -> dict:
    return {
        'onResponseReceivedActions': [{
            'appendContinuationItemsAction': {
                'continuationItems': items,
            },
        }],
    }


def _chip_reload(items: list[dict]) -> dict:
    return {
        'onResponseReceivedActions': [
            {
                'reloadContinuationItemsCommand': {
                    'continuationItems': [
                        {'chipBarViewModel': {'chips': []}},
                    ],
                },
            },
            {
                'reloadContinuationItemsCommand': {
                    'continuationItems': items,
                },
            },
        ],
    }


LATEST_SELECTED: list[dict] = [
    _chip('Latest', True, 'chip-latest'),
    _chip('Popular', False, 'chip-popular'),
    _chip('Oldest', False, 'chip-oldest'),
]


OLDEST_AVAILABLE: list[dict] = [
    _chip('Latest', True, 'chip-latest'),
    _chip('Popular', False, 'chip-popular'),
    _chip('Oldest', False, 'chip-oldest'),
]


class _KnownIds:
    def __init__(self, known: set[str]) -> None:
        self.known: set[str] = known
        self.calls: list[list[str]] = []

    async def __call__(self, video_ids: list[str]) -> set[str]:
        self.calls.append(list(video_ids))
        return {v for v in video_ids if v in self.known}


class TestLatestPaging(unittest.IsolatedAsyncioTestCase):

    def _tabs(
        self, responses: dict[str, dict], known: _KnownIds | None,
        oldest_first_limit: int = 0,
    ) -> tuple[YouTubeChannelTabs, list[str]]:
        tabs: YouTubeChannelTabs = YouTubeChannelTabs(
            CHANNEL_ID, known_video_ids=known,
            oldest_first_limit=oldest_first_limit,
        )
        requested: list[str] = []

        async def browse(
            params: str = '', continuation_token: str = '',
            **kwargs: object,
        ) -> dict:
            key: str = params or continuation_token
            requested.append(key)
            return responses[key]

        tabs._browse = AsyncMock(side_effect=browse)
        return tabs, requested

    async def _scrape(
        self, tabs: YouTubeChannelTabs, title: str,
    ) -> set[str]:
        renderer: dict = {
            'title': title.capitalize(),
            'endpoint': {'browseEndpoint': {'params': 'tab'}},
        }
        result: tuple = await tabs._scrape_tab(renderer, title)
        return result[0]

    async def test_stops_after_all_known_page(self) -> None:
        title: str
        for title in ('videos', 'shorts', 'live'):
            with self.subTest(title=title):
                responses: dict[str, dict] = {
                    'tab': _tab_page(
                        title.capitalize(),
                        [_item('new1'), _item('old1'), _more('p2')],
                        LATEST_SELECTED,
                    ),
                    'p2': _continuation(
                        [_item('old2'), _item('old3'), _more('p3')],
                    ),
                    'p3': _continuation([_item('old4')]),
                }
                known: _KnownIds = _KnownIds({'old1', 'old2', 'old3'})
                tabs: YouTubeChannelTabs
                requested: list[str]
                tabs, requested = self._tabs(responses, known)
                ids: set[str] = await self._scrape(tabs, title)
                self.assertEqual(ids, {'new1', 'old1', 'old2', 'old3'})
                self.assertEqual(requested, ['tab', 'p2'])
                self.assertFalse(tabs.enumeration_complete)

    async def test_selects_latest_chip_when_not_default(self) -> None:
        chips: list[dict] = [
            _chip('Latest', False, 'chip-latest'),
            _chip('Popular', True, 'chip-popular'),
        ]
        responses: dict[str, dict] = {
            'tab': _tab_page(
                'Videos', [_item('popular1'), _more('pop-p2')], chips,
            ),
            'chip-latest': _chip_reload(
                [_item('new1'), _item('old1'), _more('p2')],
            ),
            'p2': _continuation([_item('old2'), _more('p3')]),
        }
        known: _KnownIds = _KnownIds({'old1', 'old2'})
        tabs: YouTubeChannelTabs
        requested: list[str]
        tabs, requested = self._tabs(responses, known)
        ids: set[str] = await self._scrape(tabs, 'videos')
        self.assertEqual(ids, {'new1', 'old1', 'old2'})
        self.assertEqual(requested, ['tab', 'chip-latest', 'p2'])
        self.assertNotIn('popular1', ids)

    async def test_no_early_stop_without_known_fn(self) -> None:
        responses: dict[str, dict] = {
            'tab': _tab_page(
                'Videos', [_item('a'), _more('p2')], LATEST_SELECTED,
            ),
            'p2': _continuation([_item('b')]),
        }
        tabs: YouTubeChannelTabs
        requested: list[str]
        tabs, requested = self._tabs(responses, None)
        ids: set[str] = await self._scrape(tabs, 'videos')
        self.assertEqual(ids, {'a', 'b'})
        self.assertEqual(requested, ['tab', 'p2'])
        self.assertTrue(tabs.enumeration_complete)

    async def test_no_early_stop_without_latest_chip(self) -> None:
        responses: dict[str, dict] = {
            'tab': _tab_page(
                'Videos', [_item('a'), _more('p2')], None,
            ),
            'p2': _continuation([_item('b')]),
        }
        known: _KnownIds = _KnownIds({'a', 'b'})
        tabs: YouTubeChannelTabs
        requested: list[str]
        tabs, requested = self._tabs(responses, known)
        ids: set[str] = await self._scrape(tabs, 'videos')
        self.assertEqual(ids, {'a', 'b'})
        self.assertEqual(requested, ['tab', 'p2'])
        self.assertEqual(known.calls, [])
        self.assertTrue(tabs.enumeration_complete)

    async def test_all_known_last_page_is_complete(self) -> None:
        responses: dict[str, dict] = {
            'tab': _tab_page(
                'Videos', [_item('a'), _item('b')], LATEST_SELECTED,
            ),
        }
        known: _KnownIds = _KnownIds({'a', 'b'})
        tabs: YouTubeChannelTabs
        requested: list[str]
        tabs, requested = self._tabs(responses, known)
        ids: set[str] = await self._scrape(tabs, 'videos')
        self.assertEqual(ids, {'a', 'b'})
        self.assertTrue(tabs.enumeration_complete)

    async def test_mixed_pages_page_through(self) -> None:
        responses: dict[str, dict] = {
            'tab': _tab_page(
                'Videos', [_item('a'), _more('p2')], LATEST_SELECTED,
            ),
            'p2': _continuation([_item('b'), _item('x'), _more('p3')]),
            'p3': _continuation([_item('c')]),
        }
        known: _KnownIds = _KnownIds({'b'})
        tabs: YouTubeChannelTabs
        requested: list[str]
        tabs, requested = self._tabs(responses, known)
        ids: set[str] = await self._scrape(tabs, 'videos')
        self.assertEqual(ids, {'a', 'b', 'x', 'c'})
        self.assertEqual(requested, ['tab', 'p2', 'p3'])
        self.assertTrue(tabs.enumeration_complete)


    async def test_oldest_first_stops_at_limit(self) -> None:
        title: str
        for title in ('videos', 'shorts', 'live'):
            with self.subTest(title=title):
                responses: dict[str, dict] = {
                    'tab': _tab_page(
                        title.capitalize(),
                        [_item('new1'), _more('latest-p2')],
                        OLDEST_AVAILABLE,
                    ),
                    'chip-oldest': _chip_reload(
                        [_item('o1'), _item('o2'), _more('p2')],
                    ),
                    'p2': _continuation(
                        [_item('o3'), _item('o4'), _more('p3')],
                    ),
                    'p3': _continuation([_item('o5')]),
                }
                tabs: YouTubeChannelTabs
                requested: list[str]
                tabs, requested = self._tabs(
                    responses, None, oldest_first_limit=3,
                )
                ids: set[str] = await self._scrape(tabs, title)
                self.assertEqual(ids, {'o1', 'o2', 'o3'})
                self.assertEqual(
                    requested, ['tab', 'chip-oldest', 'p2'],
                )
                self.assertFalse(tabs.enumeration_complete)

    async def test_oldest_first_small_tab_is_complete(self) -> None:
        responses: dict[str, dict] = {
            'tab': _tab_page(
                'Videos', [_item('new1'), _more('latest-p2')],
                OLDEST_AVAILABLE,
            ),
            'chip-oldest': _chip_reload(
                [_item('o1'), _item('o2'), _more('p2')],
            ),
            'p2': _continuation([_item('o3')]),
        }
        tabs: YouTubeChannelTabs
        requested: list[str]
        tabs, requested = self._tabs(
            responses, None, oldest_first_limit=3,
        )
        ids: set[str] = await self._scrape(tabs, 'videos')
        self.assertEqual(ids, {'o1', 'o2', 'o3'})
        self.assertTrue(tabs.enumeration_complete)

    async def test_oldest_first_without_chip_limits_default_order(
        self,
    ) -> None:
        responses: dict[str, dict] = {
            'tab': _tab_page(
                'Videos', [_item('a'), _item('b'), _more('p2')], None,
            ),
            'p2': _continuation([_item('c'), _more('p3')]),
        }
        tabs: YouTubeChannelTabs
        requested: list[str]
        tabs, requested = self._tabs(
            responses, None, oldest_first_limit=2,
        )
        ids: set[str] = await self._scrape(tabs, 'videos')
        self.assertEqual(ids, {'a', 'b'})
        self.assertEqual(requested, ['tab'])
        self.assertFalse(tabs.enumeration_complete)


if __name__ == '__main__':
    unittest.main()
