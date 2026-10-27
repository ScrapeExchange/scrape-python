'''
Unit tests for topic-channel detection and the synthetic uploads listing
tab: a topic channel's tracks come from its uploads playlist, paged with
the same resume and checkpoint machinery as the video tabs.
'''

import logging
import unittest
from typing import Any, Callable
from unittest.mock import AsyncMock

from scrape_exchange.youtube.youtube_channel_tabs import (
    LISTING_TAB_DONE,
    YouTubeChannelTabs,
)


CHANNEL_ID: str = 'UCEPMVbUzImPl4p8k4LkGevA'
UPLOADS_BROWSE_ID: str = 'VLUUEPMVbUzImPl4p8k4LkGevA'

_TABS_LOGGER: logging.Logger = logging.getLogger(
    'scrape_exchange.youtube.youtube_channel_tabs',
)
_TABS_LOGGER_PRIOR_LEVEL: int = _TABS_LOGGER.level


def setUpModule() -> None:
    _TABS_LOGGER.setLevel(logging.CRITICAL)


def tearDownModule() -> None:
    _TABS_LOGGER.setLevel(_TABS_LOGGER_PRIOR_LEVEL)


HOME_TAB: dict[str, Any] = {
    'tabRenderer': {'title': 'Home', 'content': {}},
}


def _videos_tab(video_id: str) -> dict[str, Any]:
    return {
        'tabRenderer': {
            'title': 'Videos',
            'endpoint': {'browseEndpoint': {'params': 'videos_p'}},
            'content': {
                'richGridRenderer': {
                    'contents': [{
                        'richItemRenderer': {
                            'content': {
                                'videoRenderer': {'videoId': video_id},
                            },
                        },
                    }],
                },
            },
        },
    }


def _channel_page(title: str, tabs: list[dict[str, Any]]) -> dict:
    return {
        'contents': {'twoColumnBrowseResultsRenderer': {'tabs': tabs}},
        'metadata': {'channelMetadataRenderer': {'title': title}},
    }


def _lockup_video(video_id: str) -> dict[str, Any]:
    return {
        'lockupViewModel': {
            'contentId': video_id,
            'contentType': 'LOCKUP_CONTENT_TYPE_VIDEO',
        },
    }


def _more_vm(token: str) -> dict[str, Any]:
    return {
        'continuationItemViewModel': {
            'continuationCommand': {
                'innertubeCommand': {
                    'continuationCommand': {'token': token},
                },
            },
        },
    }


def _uploads_page(
    video_ids: list[str], token: str = '',
) -> dict[str, Any]:
    items: list[dict[str, Any]] = [
        _lockup_video(video_id) for video_id in video_ids
    ]
    if token:
        items.append(_more_vm(token))
    return {
        'contents': {
            'twoColumnBrowseResultsRenderer': {
                'tabs': [{
                    'tabRenderer': {
                        'content': {
                            'sectionListRenderer': {
                                'contents': [{
                                    'itemSectionRenderer': {
                                        'contents': items,
                                    },
                                }],
                            },
                        },
                    },
                }],
            },
        },
    }


def _uploads_continuation(
    video_ids: list[str], token: str = '',
) -> dict[str, Any]:
    items: list[dict[str, Any]] = [
        _lockup_video(video_id) for video_id in video_ids
    ]
    if token:
        items.append(_more_vm(token))
    return {
        'onResponseReceivedActions': [{
            'appendContinuationItemsAction': {
                'continuationItems': items,
            },
        }],
    }


def _album_lockup(album_id: str) -> dict[str, Any]:
    return {
        'lockupViewModel': {
            'contentId': album_id,
            'contentType': 'LOCKUP_CONTENT_TYPE_ALBUM',
            'metadata': {
                'lockupMetadataViewModel': {
                    'title': {'content': f'Album {album_id}'},
                },
            },
            'contentImage': {
                'collectionThumbnailViewModel': {
                    'primaryThumbnail': {
                        'thumbnailViewModel': {
                            'overlays': [{
                                'thumbnailOverlayBadgeViewModel': {
                                    'thumbnailBadges': [{
                                        'thumbnailBadgeViewModel': {
                                            'text': '27 songs',
                                        },
                                    }],
                                },
                            }],
                        },
                    },
                },
            },
        },
    }


def _grid_album(album_id: str) -> dict[str, Any]:
    return {
        'gridPlaylistRenderer': {
            'playlistId': album_id,
            'title': {'runs': [{'text': f'Album {album_id}'}]},
            'videoCountShortText': {'simpleText': '11'},
        },
    }


def _panel_token(token: str) -> dict[str, Any]:
    return {
        'continuationItemRenderer': {
            'continuationEndpoint': {
                'continuationCommand': {'token': token},
            },
        },
    }


def _shelf(
    album_ids: list[str], panel_token: str = '',
) -> dict[str, Any]:
    shelf: dict[str, Any] = {
        'content': {
            'horizontalListRenderer': {
                'items': [
                    _album_lockup(album_id) for album_id in album_ids
                ],
            },
        },
    }
    if panel_token:
        shelf['endpoint'] = {
            'showEngagementPanelEndpoint': {
                'engagementPanel': {
                    'engagementPanelSectionListRenderer': {
                        'content': {
                            'sectionListRenderer': {
                                'contents': [{
                                    'itemSectionRenderer': {
                                        'contents': [{
                                            'continuationItemRenderer': {
                                                'continuationEndpoint': {
                                                    'continuationCommand': {
                                                        'token': panel_token,
                                                    },
                                                },
                                            },
                                        }],
                                    },
                                }],
                            },
                        },
                    },
                },
            },
        }
    return {'shelfRenderer': shelf}


def _home_tab(*shelves: dict[str, Any]) -> dict[str, Any]:
    return {
        'tabRenderer': {
            'title': 'Home',
            'content': {
                'sectionListRenderer': {
                    'contents': [{
                        'itemSectionRenderer': {
                            'contents': list(shelves),
                        },
                    }],
                },
            },
        },
    }


def _panel_page(
    albums: list[str], token: str = '', wrapped: bool = False,
) -> dict[str, Any]:
    items: list[dict[str, Any]] = [
        _grid_album(album_id) for album_id in albums
    ]
    if token:
        items.append(_panel_token(token))
    if wrapped:
        items = [{'gridRenderer': {'items': items}}]
    return {
        'onResponseReceivedEndpoints': [{
            'appendContinuationItemsAction': {
                'continuationItems': items,
            },
        }],
    }


class _Browser:
    '''Records every _browse call and answers from the fixture.'''

    def __init__(
        self,
        page: dict[str, Any],
        uploads: Callable[[str | None, str], dict[str, Any]] | None = None,
    ) -> None:
        self.page: dict[str, Any] = page
        self.uploads: Callable[[str | None, str], dict] | None = uploads
        self.calls: list[dict[str, Any]] = []

    async def __call__(
        self, params: str = '', continuation_token: str = '',
        max_retries: int = 4, browse_id: str | None = None,
    ) -> dict[str, Any]:
        return self.route(params, continuation_token, browse_id)

    def route(
        self, params: str = '', continuation_token: str = '',
        browse_id: str | None = None,
    ) -> dict[str, Any]:
        self.calls.append({
            'params': params,
            'continuation_token': continuation_token,
            'browse_id': browse_id,
        })
        if browse_id is not None:
            if self.uploads is None:
                raise RuntimeError(f'unexpected uploads browse {browse_id}')
            return self.uploads(browse_id, '')
        if continuation_token and self.uploads is not None:
            return self.uploads(None, continuation_token)
        return self.page

    def attrs(self, key: str) -> list[Any]:
        return [call[key] for call in self.calls]


def _tabs(
    page: dict[str, Any], browser: _Browser, **kwargs: Any,
) -> YouTubeChannelTabs:
    tabs: YouTubeChannelTabs = YouTubeChannelTabs(CHANNEL_ID, **kwargs)
    tabs._browse = AsyncMock(side_effect=browser.route)
    return tabs


class TestTopicDetection(unittest.IsolatedAsyncioTestCase):

    async def test_topic_detected_from_browse_title(self) -> None:
        browser: _Browser = _Browser(
            _channel_page('Queen - Topic', [HOME_TAB]),
        )
        tabs: YouTubeChannelTabs = _tabs(_channel_page(
            'Queen - Topic', [HOME_TAB]), browser)
        await tabs.browse_channel()
        self.assertTrue(tabs.is_topic)

        browser = _Browser(_channel_page('Queen', [HOME_TAB]))
        tabs = _tabs(_channel_page('Queen', [HOME_TAB]), browser)
        await tabs.browse_channel()
        self.assertFalse(tabs.is_topic)

    async def test_non_topic_channel_never_browses_uploads(self) -> None:
        page: dict[str, Any] = _channel_page(
            'Queen', [_videos_tab('vid1')],
        )
        browser: _Browser = _Browser(page)
        tabs: YouTubeChannelTabs = _tabs(page, browser)
        await tabs.browse_channel()
        video_ids, *_ = await tabs.scrape_loaded_tabs()

        self.assertEqual(video_ids, {'vid1'})
        # Only the channel browse: no uploads-playlist browse.
        self.assertEqual(set(browser.attrs('browse_id')), {None})


class TestUploadsListing(unittest.IsolatedAsyncioTestCase):

    async def test_uploads_listed_across_pages(self) -> None:
        first: dict[str, Any] = _uploads_page(['v1', 'v2'], token='t1')

        def uploads(
            browse_id: str | None, token: str,
        ) -> dict[str, Any]:
            if token == 't1':
                return _uploads_continuation(['v3'])
            return first

        page: dict[str, Any] = _channel_page(
            'Queen - Topic', [HOME_TAB],
        )
        browser: _Browser = _Browser(page, uploads)
        tabs: YouTubeChannelTabs = _tabs(page, browser)
        await tabs.browse_channel()
        video_ids, *_ = await tabs.scrape_loaded_tabs()

        self.assertEqual(video_ids, {'v1', 'v2', 'v3'})
        self.assertIn(UPLOADS_BROWSE_ID, browser.attrs('browse_id'))
        self.assertIn('t1', browser.attrs('continuation_token'))
        self.assertEqual(tabs.failed_tabs, set())

    async def test_uploads_resumable_checkpoint_and_done(self) -> None:
        def uploads(
            browse_id: str | None, token: str,
        ) -> dict[str, Any]:
            if token:
                return _uploads_continuation(['v3'])
            return _uploads_page(['v1', 'v2'], token='t1')

        page: dict[str, Any] = _channel_page(
            'Queen - Topic', [HOME_TAB],
        )
        browser: _Browser = _Browser(page, uploads)
        tabs: YouTubeChannelTabs = _tabs(page, browser, listing_resume={})
        await tabs.browse_channel()
        video_ids, *_ = await tabs.scrape_loaded_tabs()

        self.assertEqual(video_ids, {'v1', 'v2', 'v3'})
        self.assertEqual(
            tabs.listing_state, {'uploads': LISTING_TAB_DONE})
        self.assertTrue(tabs.listing_complete)

    async def test_uploads_resume_from_saved_token(self) -> None:
        def uploads(
            browse_id: str | None, token: str,
        ) -> dict[str, Any]:
            if token == 'saved':
                return _uploads_continuation(['v9'])
            return _uploads_page(['v1', 'v2'], token='t1')

        page: dict[str, Any] = _channel_page(
            'Queen - Topic', [HOME_TAB],
        )
        browser: _Browser = _Browser(page, uploads)
        tabs: YouTubeChannelTabs = _tabs(
            page, browser, listing_resume={'uploads': 'saved'})
        await tabs.browse_channel()
        video_ids, *_ = await tabs.scrape_loaded_tabs()

        self.assertIn('saved', browser.attrs('continuation_token'))
        self.assertIn('v9', video_ids)

    async def test_uploads_incremental_stop(self) -> None:
        def uploads(
            browse_id: str | None, token: str,
        ) -> dict[str, Any]:
            if token:
                return _uploads_continuation(['v3'])
            return _uploads_page(['v1', 'v2'], token='t1')

        page: dict[str, Any] = _channel_page(
            'Queen - Topic', [HOME_TAB],
        )
        browser: _Browser = _Browser(page, uploads)
        tabs: YouTubeChannelTabs = _tabs(
            page, browser,
            known_video_ids=AsyncMock(return_value={'v1', 'v2'}),
        )
        await tabs.browse_channel()
        video_ids, *_ = await tabs.scrape_loaded_tabs()

        self.assertEqual(video_ids, {'v1', 'v2'})
        # The all-known first page ends the listing: no continuation.
        self.assertEqual(set(browser.attrs('continuation_token')), {''})
        # No 'Latest'/'Oldest' chip browse: the uploads playlist has
        # none.
        self.assertEqual(set(browser.attrs('params')), {''})

    async def test_uploads_failure_marks_partial(self) -> None:
        def uploads(
            browse_id: str | None, token: str,
        ) -> dict[str, Any]:
            raise RuntimeError('uploads browse failed')

        page: dict[str, Any] = _channel_page(
            'Queen - Topic', [_videos_tab('vid1')],
        )
        browser: _Browser = _Browser(page, uploads)
        tabs: YouTubeChannelTabs = _tabs(page, browser)
        await tabs.browse_channel()
        video_ids, *_ = await tabs.scrape_loaded_tabs()

        self.assertIn('uploads', tabs.failed_tabs)
        self.assertEqual(video_ids, {'vid1'})


class TestHomeShelves(unittest.IsolatedAsyncioTestCase):

    async def test_home_shelf_albums_collected(self) -> None:
        home: dict[str, Any] = _home_tab(_shelf(['a1', 'a2']))
        page: dict[str, Any] = _channel_page('Queen - Topic', [home])
        browser: _Browser = _Browser(page)
        tabs: YouTubeChannelTabs = _tabs(page, browser)

        playlists = await tabs._scrape_home_playlists(home['tabRenderer'])

        self.assertEqual(
            {playlist.playlist_id for playlist in playlists},
            {'a1', 'a2'},
        )
        # The Home tab is already loaded: no extra browse.
        tabs._browse.assert_not_awaited()
        self.assertEqual(tabs.failed_tabs, set())

    async def test_album_panel_paged(self) -> None:
        home: dict[str, Any] = _home_tab(
            _shelf([f'a{i}' for i in range(1, 13)], panel_token='p1'),
        )
        page: dict[str, Any] = _channel_page('Queen - Topic', [home])
        browser: _Browser = _Browser(page)
        tabs: YouTubeChannelTabs = _tabs(page, browser)

        async def panel(
            params: str = '', continuation_token: str = '',
            max_retries: int = 4, browse_id: str | None = None,
        ) -> dict[str, Any]:
            if continuation_token == 'p1':
                return _panel_page(
                    ['a13', 'a14', 'a15'], token='p2', wrapped=True)
            return _panel_page(['a16'])

        tabs._browse = AsyncMock(side_effect=panel)
        playlists = await tabs._scrape_home_playlists(home['tabRenderer'])

        self.assertEqual(
            {playlist.playlist_id for playlist in playlists},
            {f'a{i}' for i in range(1, 17)},
        )
        self.assertEqual(tabs._browse.await_count, 2)
        self.assertEqual(tabs.failed_tabs, set())

    async def test_album_panel_failure_keeps_shelf(self) -> None:
        home: dict[str, Any] = _home_tab(
            _shelf(['a1', 'a2'], panel_token='p1'),
        )
        page: dict[str, Any] = _channel_page('Queen - Topic', [home])
        browser: _Browser = _Browser(page)
        tabs: YouTubeChannelTabs = _tabs(page, browser)
        tabs._browse = AsyncMock(side_effect=RuntimeError('panel failed'))

        playlists = await tabs._scrape_home_playlists(home['tabRenderer'])

        self.assertEqual(
            {playlist.playlist_id for playlist in playlists},
            {'a1', 'a2'},
        )
        self.assertIn('home', tabs.failed_tabs)

    async def test_album_panel_repeated_token_stops(self) -> None:
        home: dict[str, Any] = _home_tab(
            _shelf([], panel_token='p1'),
        )
        page: dict[str, Any] = _channel_page('Queen - Topic', [home])
        browser: _Browser = _Browser(page)
        tabs: YouTubeChannelTabs = _tabs(page, browser)
        tabs._browse = AsyncMock(
            return_value=_panel_page(['a1'], token='p1'))

        playlists = await tabs._scrape_home_playlists(home['tabRenderer'])

        self.assertEqual(
            {playlist.playlist_id for playlist in playlists}, {'a1'})
        self.assertEqual(tabs._browse.await_count, 1)

    async def test_non_topic_home_not_scraped(self) -> None:
        home: dict[str, Any] = _home_tab(_shelf(['a1']))
        page: dict[str, Any] = _channel_page(
            'Queen', [home, _videos_tab('vid1')])

        def uploads(
            browse_id: str | None, token: str,
        ) -> dict[str, Any]:
            return _uploads_page([])

        browser: _Browser = _Browser(page, uploads)
        tabs: YouTubeChannelTabs = _tabs(page, browser)
        await tabs.browse_channel()
        _video_ids, _podcasts, playlists, *_ = (
            await tabs.scrape_loaded_tabs()
        )

        self.assertEqual(playlists, set())
        self.assertNotIn('home', tabs.failed_tabs)


if __name__ == '__main__':
    unittest.main()
