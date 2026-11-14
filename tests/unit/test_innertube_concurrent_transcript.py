'''The transcript is fetched concurrently with NEXT when the player
response lists caption tracks, and after NEXT otherwise.'''

import asyncio
import unittest
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.rate_limiter import RateLimitWaitExceeded
from scrape_exchange.youtube import youtube_video_innertube as mod
from scrape_exchange.youtube.innertube_calls import calls_label, track_calls
from scrape_exchange.youtube.youtube_video import YouTubeVideo

CAPTIONS: dict[str, Any] = {'playerCaptionsTracklistRenderer': {
    'captionTracks': [{'languageCode': 'en', 'kind': 'asr'}],
}}


def _player(with_captions: bool) -> dict[str, Any]:
    data: dict[str, Any] = {
        'playabilityStatus': {'status': 'OK'},
        'videoDetails': {'title': 'Title'},
    }
    if with_captions:
        data['captions'] = CAPTIONS
    return data


@contextmanager
def _patched(call_api: Any, limiter: MagicMock) -> Iterator[None]:
    with ExitStack() as stack:
        for name, value in (
            ('borrow_pooled_player_innertube_for_entry', MagicMock()),
            ('borrow_pooled_innertube_for_entry', MagicMock()),
        ):
            stack.enter_context(
                patch.object(mod, name, return_value=value),
            )
        for name in (
            'release_pooled_player_innertube', 'release_pooled_innertube',
        ):
            stack.enter_context(patch.object(mod, name, new=AsyncMock()))
        stack.enter_context(patch.object(mod, '_call_innertube', call_api))
        stack.enter_context(patch.object(
            mod.YouTubeRateLimiter, 'get', return_value=limiter,
        ))
        yield


def _limiter() -> MagicMock:
    limiter: MagicMock = MagicMock()
    limiter.acquire = AsyncMock(return_value=None)
    limiter.report_proxy_result = AsyncMock()
    limiter.penalise = AsyncMock()
    return limiter


class TestConcurrentTranscript(unittest.IsolatedAsyncioTestCase):
    async def test_captions_fetch_transcript_while_next_runs(self) -> None:
        panel_sent: asyncio.Event = asyncio.Event()
        calls: list[str] = []

        async def call_api(client: Any, method: Any, *args: Any) -> Any:
            if method is client.player:
                calls.append('player')
                return _player(True)
            if method is client.next:
                calls.append('next')
                # Completes only if get_panel was sent meanwhile; a
                # sequential implementation times out here.
                await asyncio.wait_for(panel_sent.wait(), timeout=2)
                return {}
            calls.append('get_panel')
            panel_sent.set()
            return {}

        with _patched(call_api, _limiter()), track_calls() as sent:
            await mod.InnerTubeVideoParser.scrape(
                YouTubeVideo(video_id='vid123'), proxy=None, max_retries=1,
            )
        self.assertEqual(sorted(calls), ['get_panel', 'next', 'player'])
        self.assertEqual(calls_label(sent), 'player_next_transcript')

    async def test_no_captions_and_no_panel_skips_transcript(self) -> None:
        call_api: AsyncMock = AsyncMock(side_effect=[_player(False), {}])
        with _patched(call_api, _limiter()), track_calls() as sent:
            await mod.InnerTubeVideoParser.scrape(
                YouTubeVideo(video_id='vid123'), proxy=None, max_retries=1,
            )
        self.assertEqual(call_api.await_count, 2)
        self.assertEqual(calls_label(sent), 'player_next')

    async def test_no_captions_but_panel_fetches_after_next(self) -> None:
        order: list[str] = []

        async def call_api(client: Any, method: Any, *args: Any) -> Any:
            if method is client.player:
                order.append('player')
                return _player(False)
            if method is client.next:
                order.append('next')
                return {}
            order.append('get_panel')
            return {}

        with (
            _patched(call_api, _limiter()),
            patch.object(mod, 'has_transcript_panel', return_value=True),
        ):
            await mod.InnerTubeVideoParser.scrape(
                YouTubeVideo(video_id='vid123'), proxy=None, max_retries=1,
            )
        self.assertEqual(order, ['player', 'next', 'get_panel'])

    async def test_next_failure_cancels_transcript(self) -> None:
        panel_started: asyncio.Event = asyncio.Event()
        cancelled: list[bool] = []
        limiter: MagicMock = _limiter()

        async def acquire(call_type: Any, **kwargs: Any) -> None:
            if call_type == mod.YouTubeCallType.NEXT:
                # Bounded so the old sequential code fails, not hangs.
                await asyncio.wait_for(panel_started.wait(), timeout=2)
                raise RateLimitWaitExceeded('next', None, 1.0, 1.0)

        limiter.acquire = AsyncMock(side_effect=acquire)

        async def call_api(client: Any, method: Any, *args: Any) -> Any:
            if method is client.player:
                return _player(True)
            panel_started.set()
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                cancelled.append(True)
                raise
            return {}

        with _patched(call_api, limiter):
            with self.assertRaises(RateLimitWaitExceeded):
                await mod.InnerTubeVideoParser.scrape(
                    YouTubeVideo(video_id='vid123'), proxy=None,
                    max_retries=1,
                )
            await asyncio.sleep(0)
        self.assertEqual(cancelled, [True])
