'''_call_innertube separates queueing from the HTTP request, and the
transcript request does not wait for NEXT on a shared client.'''

import asyncio
import time
import unittest
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.youtube import youtube_video_innertube as mod
from scrape_exchange.youtube.youtube_video import YouTubeVideo

CAPTIONS: dict[str, Any] = {'playerCaptionsTracklistRenderer': {
    'captionTracks': [{'languageCode': 'en', 'kind': 'asr'}],
}}


def _waits(metric: MagicMock) -> dict[str, list[float]]:
    waits: dict[str, list[float]] = {}
    for labels, observe in zip(
        metric.labels.call_args_list,
        metric.labels.return_value.observe.call_args_list,
    ):
        waits.setdefault(labels.kwargs['phase'], []).append(
            observe.args[0],
        )
    return waits


class TestCallInnertubeTiming(unittest.IsolatedAsyncioTestCase):
    async def test_timing_starts_when_request_runs(self) -> None:
        client: MagicMock = MagicMock()

        def slow(*args: Any) -> dict[str, Any]:
            time.sleep(0.2)
            return {'ok': True}

        metric: MagicMock = MagicMock()
        first: mod._CallTiming = mod._CallTiming()
        second: mod._CallTiming = mod._CallTiming()
        with patch.object(mod, 'METRIC_INNERTUBE_CALL_WAIT', metric):
            queued: float = time.monotonic()
            results: list[dict[str, Any]] = await asyncio.gather(
                mod._call_innertube(
                    client, slow, call='next', timing=first,
                ),
                mod._call_innertube(
                    client, slow, call='next', timing=second,
                ),
            )
        self.assertEqual(results, [{'ok': True}, {'ok': True}])
        # Same client: the second call waits for the first one's lock.
        self.assertGreaterEqual(second.started - queued, 0.15)
        # The request alone (~0.2s), without the ~0.2s lock wait.
        self.assertGreaterEqual(time.monotonic() - queued, 0.35)
        self.assertLess(second.elapsed(queued), 0.3)
        waits: dict[str, list[float]] = _waits(metric)
        self.assertEqual(len(waits['lock']), 2)
        self.assertEqual(len(waits['executor']), 2)
        self.assertGreaterEqual(max(waits['lock']), 0.15)
        self.assertEqual(
            {c.kwargs['call'] for c in metric.labels.call_args_list},
            {'next'},
        )

    def test_elapsed_falls_back_when_never_started(self) -> None:
        timing: mod._CallTiming = mod._CallTiming()
        self.assertGreaterEqual(
            timing.elapsed(time.monotonic() - 1.0), 1.0,
        )


class TestTranscriptOwnClient(unittest.IsolatedAsyncioTestCase):
    async def test_next_and_transcript_overlap(self) -> None:
        player_client: MagicMock = MagicMock()
        web_client: MagicMock = MagicMock()
        transcript_client: MagicMock = MagicMock()
        player_client.player = MagicMock(return_value={
            'playabilityStatus': {'status': 'OK'},
            'videoDetails': {'title': 'Title'},
            'captions': CAPTIONS,
        })

        def blocking(*args: Any) -> dict[str, Any]:
            time.sleep(0.3)
            return {}

        web_client.next = MagicMock(side_effect=blocking)
        transcript_client.adaptor.dispatch = MagicMock(
            side_effect=blocking,
        )
        limiter: MagicMock = MagicMock()
        limiter.acquire = AsyncMock(return_value=None)
        limiter.report_proxy_result = AsyncMock()

        with (
            patch.object(
                mod, 'borrow_pooled_player_innertube_for_entry',
                return_value=player_client,
            ),
            patch.object(
                mod, 'release_pooled_player_innertube', new=AsyncMock(),
            ),
            patch.object(
                mod, 'borrow_pooled_innertube_for_entry',
                return_value=web_client,
            ),
            patch.object(mod, 'release_pooled_innertube', new=AsyncMock()),
            patch.object(
                mod, 'borrow_pooled_transcript_innertube_for_entry',
                return_value=transcript_client,
            ),
            patch.object(
                mod, 'release_pooled_transcript_innertube',
                new=AsyncMock(),
            ),
            patch.object(mod.YouTubeRateLimiter, 'get', return_value=limiter),
        ):
            start: float = time.monotonic()
            await mod.InnerTubeVideoParser.scrape(
                YouTubeVideo(video_id='vid123'), proxy=None, max_retries=1,
            )
            took: float = time.monotonic() - start

        web_client.next.assert_called_once()
        transcript_client.adaptor.dispatch.assert_called_once()
        # Sequential would take >= 0.6s.
        self.assertLess(took, 0.5)
