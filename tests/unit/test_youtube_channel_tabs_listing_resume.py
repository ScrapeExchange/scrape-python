'''
Unit tests for resumable full listings in YouTubeChannelTabs: resume
from a saved continuation token, stop instead of raising on a failed
page, skip tabs listed by an earlier scrape, and checkpoint.
'''

import logging
import unittest
from typing import Any, Callable
from unittest.mock import AsyncMock, MagicMock

from innertube.errors import RequestError as InnerTubeRequestError

from scrape_exchange.youtube.youtube_channel_tabs import (
    LISTING_CHECKPOINT_PAGES,
    LISTING_TAB_DONE,
    YouTubeChannelTabs,
    _token_rejected,
)


CHANNEL_ID: str = 'UCtest123'
TAB_RENDERER: dict[str, Any] = {
    'endpoint': {'browseEndpoint': {'params': 'videos_p'}},
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


def _first_page(video_id: str, token: str) -> dict[str, Any]:
    contents: list[dict[str, Any]] = [_video(video_id)]
    if token:
        contents.append(_more(token))
    return {
        'contents': {
            'twoColumnBrowseResultsRenderer': {
                'tabs': [{
                    'tabRenderer': {
                        'title': 'Videos',
                        'content': {
                            'richGridRenderer': {'contents': contents},
                        },
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


def _rejected() -> RuntimeError:
    '''What _browse raises after its retries when InnerTube keeps
    answering HTTP 400.'''
    error: MagicMock = MagicMock()
    error.code = 400
    try:
        raise RuntimeError('Failed') from InnerTubeRequestError(error)
    except RuntimeError as exc:
        return exc


def _tabs(
    pages: dict[str, Any],
    listing_resume: dict[str, str] | None = None,
    on_listing_checkpoint: Callable | None = None,
) -> YouTubeChannelTabs:
    '''Tabs whose _browse answers from *pages*, keyed by params or
    continuation token; an exception value is raised.'''
    tabs: YouTubeChannelTabs = YouTubeChannelTabs(
        CHANNEL_ID, listing_resume=listing_resume,
        on_listing_checkpoint=on_listing_checkpoint,
    )

    async def browse(
        params: str = '', continuation_token: str = '',
    ) -> dict[str, Any]:
        page: Any = pages[params or continuation_token]
        if isinstance(page, Exception):
            raise page
        return page

    tabs._browse = AsyncMock(side_effect=browse)
    return tabs


def _browsed_tokens(tabs: YouTubeChannelTabs) -> list[str]:
    return [
        call.kwargs.get('continuation_token', '')
        for call in tabs._browse.call_args_list
        if call.kwargs.get('continuation_token')
    ]


class TestResumableListingFailure(unittest.IsolatedAsyncioTestCase):
    async def test_failed_page_keeps_ids_and_token(self) -> None:
        tabs: YouTubeChannelTabs = _tabs({
            'videos_p': _first_page('v1', 't1'),
            't1': _continuation('v2', 't2'),
            't2': RuntimeError('boom'),
        }, listing_resume={})
        video_ids, *_ = await tabs._scrape_tab(TAB_RENDERER, 'videos')
        self.assertEqual(video_ids, {'v1', 'v2'})
        self.assertEqual(tabs.listing_state, {'videos': 't2'})
        self.assertFalse(tabs.enumeration_complete)
        self.assertFalse(tabs.listing_complete)

    async def test_failed_page_raises_when_not_resumable(self) -> None:
        tabs: YouTubeChannelTabs = _tabs({
            'videos_p': _first_page('v1', 't1'),
            't1': RuntimeError('boom'),
        })
        with self.assertRaises(RuntimeError):
            await tabs._scrape_tab(TAB_RENDERER, 'videos')

    async def test_known_id_listing_is_not_resumable(self) -> None:
        tabs: YouTubeChannelTabs = YouTubeChannelTabs(
            CHANNEL_ID, known_video_ids=AsyncMock(return_value=set()),
            listing_resume={},
        )
        self.assertFalse(tabs.resumable_listing)

    async def test_completed_tab_is_done(self) -> None:
        tabs: YouTubeChannelTabs = _tabs({
            'videos_p': _first_page('v1', 't1'),
            't1': _continuation('v2', ''),
        }, listing_resume={})
        video_ids, *_ = await tabs._scrape_tab(TAB_RENDERER, 'videos')
        self.assertEqual(video_ids, {'v1', 'v2'})
        self.assertEqual(tabs.listing_state, {'videos': LISTING_TAB_DONE})
        self.assertTrue(tabs.enumeration_complete)
        self.assertTrue(tabs.listing_complete)


class TestResumableListingResume(unittest.IsolatedAsyncioTestCase):
    async def test_resumes_from_saved_token(self) -> None:
        tabs: YouTubeChannelTabs = _tabs({
            'videos_p': _first_page('v1', 't1'),
            'saved': _continuation('v9', ''),
        }, listing_resume={'videos': 'saved'})
        video_ids, *_ = await tabs._scrape_tab(TAB_RENDERER, 'videos')
        self.assertEqual(video_ids, {'v1', 'v9'})
        self.assertEqual(_browsed_tokens(tabs), ['saved'])
        self.assertEqual(tabs.listing_state, {'videos': LISTING_TAB_DONE})
        # The IDs before the resume point are not in this scrape.
        self.assertFalse(tabs.enumeration_complete)
        self.assertTrue(tabs.listing_complete)

    async def test_rejected_token_restarts_tab(self) -> None:
        tabs: YouTubeChannelTabs = _tabs({
            'videos_p': _first_page('v1', 't1'),
            'saved': _rejected(),
            't1': _continuation('v2', ''),
        }, listing_resume={'videos': 'saved'})
        video_ids, *_ = await tabs._scrape_tab(TAB_RENDERER, 'videos')
        self.assertEqual(video_ids, {'v1', 'v2'})
        self.assertEqual(_browsed_tokens(tabs), ['saved', 't1'])
        self.assertTrue(tabs.enumeration_complete)
        self.assertTrue(tabs.listing_complete)

    async def test_empty_response_to_token_restarts_tab(self) -> None:
        tabs: YouTubeChannelTabs = _tabs({
            'videos_p': _first_page('v1', 't1'),
            'saved': {},
            't1': _continuation('v2', ''),
        }, listing_resume={'videos': 'saved'})
        video_ids, *_ = await tabs._scrape_tab(TAB_RENDERER, 'videos')
        self.assertEqual(video_ids, {'v1', 'v2'})
        self.assertEqual(_browsed_tokens(tabs), ['saved', 't1'])

    async def test_transient_failure_keeps_saved_token(self) -> None:
        tabs: YouTubeChannelTabs = _tabs({
            'videos_p': _first_page('v1', 't1'),
            'saved': RuntimeError('timeout'),
        }, listing_resume={'videos': 'saved'})
        video_ids, *_ = await tabs._scrape_tab(TAB_RENDERER, 'videos')
        self.assertEqual(video_ids, {'v1'})
        self.assertEqual(tabs.listing_state, {'videos': 'saved'})

    async def test_single_page_tab_ignores_saved_token(self) -> None:
        tabs: YouTubeChannelTabs = _tabs({
            'videos_p': _first_page('v1', ''),
        }, listing_resume={'videos': 'saved'})
        video_ids, *_ = await tabs._scrape_tab(TAB_RENDERER, 'videos')
        self.assertEqual(video_ids, {'v1'})
        self.assertEqual(_browsed_tokens(tabs), [])

    async def test_listed_tab_is_skipped(self) -> None:
        tabs: YouTubeChannelTabs = _tabs(
            {}, listing_resume={'videos': LISTING_TAB_DONE},
        )
        tabs.tabs = [{
            'tabRenderer': {'title': 'Videos', **TAB_RENDERER},
        }]
        video_ids, *_ = await tabs.scrape_loaded_tabs()
        self.assertEqual(video_ids, set())
        tabs._browse.assert_not_called()
        self.assertEqual(tabs.listing_state, {'videos': LISTING_TAB_DONE})
        self.assertFalse(tabs.enumeration_complete)
        self.assertTrue(tabs.listing_complete)


class TestResumableListingCheckpoint(unittest.IsolatedAsyncioTestCase):
    def _pages(self, count: int) -> dict[str, Any]:
        pages: dict[str, Any] = {'videos_p': _first_page('v0', 't1')}
        index: int
        for index in range(1, count + 1):
            pages[f't{index}'] = _continuation(
                f'v{index}', f't{index + 1}' if index < count else '',
            )
        return pages

    async def test_checkpoints_every_n_pages(self) -> None:
        checkpoint: AsyncMock = AsyncMock()
        pages: int = LISTING_CHECKPOINT_PAGES * 2 + 5
        tabs: YouTubeChannelTabs = _tabs(
            self._pages(pages), listing_resume={},
            on_listing_checkpoint=checkpoint,
        )
        video_ids, *_ = await tabs._scrape_tab(TAB_RENDERER, 'videos')
        self.assertEqual(len(video_ids), pages + 1)
        self.assertEqual(checkpoint.await_count, 2)
        first, second = checkpoint.await_args_list
        n: int = LISTING_CHECKPOINT_PAGES
        self.assertEqual(first.args[:2], ('videos', f't{n + 1}'))
        self.assertEqual(
            first.args[2], {f'v{index}' for index in range(n + 1)},
        )
        self.assertEqual(second.args[:2], ('videos', f't{2 * n + 1}'))
        self.assertEqual(
            second.args[2],
            {f'v{index}' for index in range(n + 1, 2 * n + 1)},
        )

    async def test_failing_checkpoint_does_not_stop_listing(self) -> None:
        checkpoint: AsyncMock = AsyncMock(side_effect=RuntimeError('db'))
        pages: int = LISTING_CHECKPOINT_PAGES + 1
        tabs: YouTubeChannelTabs = _tabs(
            self._pages(pages), listing_resume={},
            on_listing_checkpoint=checkpoint,
        )
        video_ids, *_ = await tabs._scrape_tab(TAB_RENDERER, 'videos')
        self.assertEqual(len(video_ids), pages + 1)
        self.assertEqual(tabs.listing_state, {'videos': LISTING_TAB_DONE})


class TestTokenRejected(unittest.TestCase):
    def test_chained_400_is_rejected(self) -> None:
        self.assertTrue(_token_rejected(_rejected()))

    def test_other_errors_are_not_rejected(self) -> None:
        error: MagicMock = MagicMock()
        error.code = 500
        self.assertFalse(_token_rejected(InnerTubeRequestError(error)))
        self.assertFalse(_token_rejected(RuntimeError('boom')))


if __name__ == '__main__':
    unittest.main()
