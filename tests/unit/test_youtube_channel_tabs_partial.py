'''
Unit tests for partial channel tab scrapes: a failing tab stops
instead of failing the whole scrape, keeps the IDs it collected
before the failing page, and is reported in ``failed_tabs``.
'''

import asyncio
import logging
import unittest
from typing import Any
from unittest.mock import AsyncMock

from scrape_exchange.youtube.channel_errors import (
    ChannelNotFoundError,
    ChannelRedirectedError,
    ChannelTerminatedError,
)
from scrape_exchange.youtube.youtube_channel_tabs import (
    LISTING_TAB_DONE,
    YouTubeChannelTabs,
)


CHANNEL_ID: str = 'UCtest123'
VIDEOS_TAB: dict[str, Any] = {
    'title': 'Videos',
    'endpoint': {'browseEndpoint': {'params': 'videos_p'}},
}
PLAYLISTS_TAB: dict[str, Any] = {
    'title': 'Playlists',
    'endpoint': {'browseEndpoint': {'params': 'playlists_p'}},
}

_TABS_LOGGER: logging.Logger = logging.getLogger(
    'scrape_exchange.youtube.youtube_channel_tabs',
)
_TABS_LOGGER_PRIOR_LEVEL: int = _TABS_LOGGER.level


def setUpModule() -> None:
    _TABS_LOGGER.setLevel(logging.CRITICAL)


def tearDownModule() -> None:
    _TABS_LOGGER.setLevel(_TABS_LOGGER_PRIOR_LEVEL)


def _video(video_id: str) -> dict[str, Any]:
    return {
        'richItemRenderer': {
            'content': {'videoRenderer': {'videoId': video_id}},
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


def _oldest_chip(token: str) -> dict[str, Any]:
    return {
        'chipViewModel': {
            'text': 'Oldest',
            'tapCommand': {
                'innertubeCommand': {
                    'continuationCommand': {'token': token},
                },
            },
        },
    }


def _page(
    video_ids: list[str], token: str = '', title: str = 'Videos',
    params: str = 'videos_p',
    chips: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    contents: list[dict[str, Any]] = [_video(v) for v in video_ids]
    if token:
        contents.append(_more(token))
    rich_grid: dict[str, Any] = {'contents': contents}
    if chips is not None:
        rich_grid['header'] = {'chipBarViewModel': {'chips': chips}}
    return {
        'contents': {
            'twoColumnBrowseResultsRenderer': {
                'tabs': [{
                    'tabRenderer': {
                        'title': title,
                        'endpoint': {'browseEndpoint': {'params': params}},
                        'content': {'richGridRenderer': rich_grid},
                    },
                }],
            },
        },
    }


def _continuation(video_id: str, token: str) -> dict[str, Any]:
    items: list[dict[str, Any]] = [_video(video_id)]
    if token:
        items.append(_more(token))
    return {
        'onResponseReceivedActions': [{
            'appendContinuationItemsAction': {
                'continuationItems': items,
            },
        }],
    }


def _tabs(pages: dict[str, Any], **kwargs: Any) -> YouTubeChannelTabs:
    '''Tabs whose _browse answers from *pages*, keyed by params or
    continuation token; an exception value is raised.'''
    tabs: YouTubeChannelTabs = YouTubeChannelTabs(CHANNEL_ID, **kwargs)

    async def browse(
        params: str = '', continuation_token: str = '',
    ) -> dict[str, Any]:
        page: Any = pages[params or continuation_token]
        if isinstance(page, BaseException):
            raise page
        return page

    tabs._browse = AsyncMock(side_effect=browse)
    return tabs


def _loaded_tabs(
    tabs: YouTubeChannelTabs, renderers: list[dict[str, Any]],
) -> None:
    tabs.tabs = [
        {'tabRenderer': renderer} for renderer in renderers
    ]


class TestFailedContinuation(unittest.IsolatedAsyncioTestCase):
    async def test_failed_continuation_keeps_ids_in_every_mode(self) -> None:
        modes: dict[str, dict[str, Any]] = {
            'full listing': {},
            'known video ids': {
                'known_video_ids': AsyncMock(return_value=set()),
            },
            'oldest first': {'oldest_first_limit': 100},
        }
        name: str
        kwargs: dict[str, Any]
        for name, kwargs in modes.items():
            with self.subTest(mode=name):
                tabs: YouTubeChannelTabs = _tabs({
                    'videos_p': _page(['a', 'b'], 't1'),
                    't1': RuntimeError('boom'),
                }, **kwargs)
                video_ids, *_ = await tabs._scrape_tab(
                    VIDEOS_TAB, 'videos',
                )
                self.assertEqual(video_ids, {'a', 'b'})
                self.assertEqual(tabs.failed_tabs, {'videos'})
                self.assertFalse(tabs.enumeration_complete)

    async def test_resumable_failure_saves_token_and_fails_tab(self) -> None:
        tabs: YouTubeChannelTabs = _tabs({
            'videos_p': _page(['a'], 't1'),
            't1': RuntimeError('boom'),
        }, listing_resume={})
        video_ids, *_ = await tabs._scrape_tab(VIDEOS_TAB, 'videos')
        self.assertEqual(video_ids, {'a'})
        self.assertEqual(tabs.listing_state, {'videos': 't1'})
        self.assertEqual(tabs.failed_tabs, {'videos'})
        self.assertFalse(tabs.enumeration_complete)

    async def test_finished_tab_is_not_failed(self) -> None:
        tabs: YouTubeChannelTabs = _tabs({
            'videos_p': _page(['a'], 't1'),
            't1': _continuation('b', ''),
        }, listing_resume={})
        video_ids, *_ = await tabs._scrape_tab(VIDEOS_TAB, 'videos')
        self.assertEqual(video_ids, {'a', 'b'})
        self.assertEqual(tabs.failed_tabs, set())
        self.assertEqual(tabs.listing_state, {'videos': LISTING_TAB_DONE})
        self.assertTrue(tabs.enumeration_complete)


class TestFailedTab(unittest.IsolatedAsyncioTestCase):
    async def test_failed_initial_page_fails_only_that_tab(self) -> None:
        tabs: YouTubeChannelTabs = _tabs({
            'videos_p': _page(['a']),
            'playlists_p': RuntimeError('boom'),
        })
        _loaded_tabs(tabs, [VIDEOS_TAB, PLAYLISTS_TAB])
        video_ids, _podcast_ids, playlists, *_rest = (
            await tabs.scrape_loaded_tabs()
        )
        self.assertEqual(video_ids, {'a'})
        self.assertEqual(playlists, set())
        self.assertEqual(tabs.failed_tabs, {'playlists'})
        self.assertFalse(tabs.enumeration_complete)

    async def test_failed_sort_page_fails_tab(self) -> None:
        tabs: YouTubeChannelTabs = _tabs({
            'videos_p': _page(
                ['a'], 't1', chips=[_oldest_chip('sort_t')],
            ),
            'sort_t': RuntimeError('boom'),
        }, oldest_first_limit=100)
        video_ids, *_ = await tabs._scrape_tab(VIDEOS_TAB, 'videos')
        self.assertEqual(video_ids, set())
        self.assertEqual(tabs.failed_tabs, {'videos'})

    async def test_cancelled_tab_propagates(self) -> None:
        tabs: YouTubeChannelTabs = _tabs({
            'videos_p': asyncio.CancelledError(),
        })
        _loaded_tabs(tabs, [VIDEOS_TAB])
        with self.assertRaises(asyncio.CancelledError):
            await tabs.scrape_loaded_tabs()

    async def test_classification_errors_propagate(self) -> None:
        errors: dict[str, Exception] = {
            'terminated': ChannelTerminatedError('gone'),
            'redirected': ChannelRedirectedError(CHANNEL_ID, 'UCnew'),
            'not found': ChannelNotFoundError('missing'),
        }
        name: str
        error: Exception
        for name, error in errors.items():
            with self.subTest(error=name):
                tabs: YouTubeChannelTabs = _tabs({
                    'videos_p': _page(['a']),
                    'playlists_p': error,
                })
                _loaded_tabs(tabs, [VIDEOS_TAB, PLAYLISTS_TAB])
                with self.assertRaises(type(error)):
                    await tabs.scrape_loaded_tabs()
                self.assertEqual(tabs.failed_tabs, set())


if __name__ == '__main__':
    unittest.main()
