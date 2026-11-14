'''Tracking of the YouTube calls made by one video scrape attempt.'''

import asyncio
import unittest

from scrape_exchange.youtube.innertube_calls import (
    calls_label,
    record_call,
    track_calls,
)


class TestInnertubeCalls(unittest.IsolatedAsyncioTestCase):
    def test_label_uses_fixed_order(self) -> None:
        self.assertEqual(calls_label(set()), 'none')
        self.assertEqual(
            calls_label({'channel_browse', 'transcript', 'next', 'player'}),
            'player_next_transcript_channel_browse',
        )
        self.assertEqual(calls_label({'next', 'player'}), 'player_next')

    def test_record_outside_tracking_is_ignored(self) -> None:
        record_call('player')
        with track_calls() as calls:
            self.assertEqual(calls, set())

    async def test_tracks_across_awaits_and_isolates_tasks(self) -> None:
        async def attempt(names: list[str]) -> str:
            with track_calls() as calls:
                for name in names:
                    await asyncio.sleep(0)
                    record_call(name)
                return calls_label(calls)

        labels: list[str] = await asyncio.gather(
            attempt(['player', 'next']),
            attempt(['player', 'next', 'transcript']),
        )
        self.assertEqual(labels, ['player_next', 'player_next_transcript'])


class TestParserRecordsCalls(unittest.IsolatedAsyncioTestCase):
    async def _scrape(self, has_transcript: bool) -> str:
        from unittest.mock import AsyncMock, MagicMock, patch

        from scrape_exchange.youtube import youtube_video_innertube as mod
        from scrape_exchange.youtube.youtube_video import YouTubeVideo

        limiter: MagicMock = MagicMock()
        limiter.acquire = AsyncMock(return_value=None)
        limiter.report_proxy_result = AsyncMock()
        player_data: dict = {
            'playabilityStatus': {'status': 'OK'},
            'videoDetails': {'title': 'Title'},
        }
        with (
            patch.object(
                mod, 'borrow_pooled_player_innertube_for_entry',
                return_value=MagicMock(),
            ),
            patch.object(
                mod, 'release_pooled_player_innertube', new=AsyncMock(),
            ),
            patch.object(
                mod, 'borrow_pooled_innertube_for_entry',
                return_value=MagicMock(),
            ),
            patch.object(mod, 'release_pooled_innertube', new=AsyncMock()),
            patch.object(
                mod, '_call_innertube',
                new=AsyncMock(side_effect=[player_data, {}, {}]),
            ),
            patch.object(
                mod, 'has_transcript_panel', return_value=has_transcript,
            ),
            patch.object(mod.YouTubeRateLimiter, 'get', return_value=limiter),
            track_calls() as calls,
        ):
            await mod.InnerTubeVideoParser.scrape(
                YouTubeVideo(video_id='vid123'), proxy=None, max_retries=1,
            )
        return calls_label(calls)

    async def test_without_transcript(self) -> None:
        self.assertEqual(await self._scrape(False), 'player_next')

    async def test_with_transcript(self) -> None:
        self.assertEqual(await self._scrape(True), 'player_next_transcript')
