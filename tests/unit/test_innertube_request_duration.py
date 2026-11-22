'''api_request_duration_seconds carries the innertube call name, so
player, next and get_panel latency can be told apart.'''

import unittest
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.youtube import youtube_client
from scrape_exchange.youtube import youtube_video_innertube as mod
from scrape_exchange.youtube.youtube_video import YouTubeVideo

CAPTIONS: dict[str, Any] = {'playerCaptionsTracklistRenderer': {
    'captionTracks': [{'languageCode': 'en', 'kind': 'asr'}],
}}


class TestRequestDurationCallLabel(unittest.IsolatedAsyncioTestCase):

    def test_metric_has_call_label(self) -> None:
        self.assertIn(
            'call',
            youtube_client.METRIC_YT_REQUEST_DURATION._labelnames,
        )

    async def test_video_scrape_labels_each_call(self) -> None:
        player_client: MagicMock = MagicMock()
        web_client: MagicMock = MagicMock()
        transcript_client: MagicMock = MagicMock()
        player_client.player = MagicMock(return_value={
            'playabilityStatus': {'status': 'OK'},
            'videoDetails': {'title': 'Title'},
            'captions': CAPTIONS,
        })
        web_client.next = MagicMock(return_value={})
        transcript_client.adaptor.dispatch = MagicMock(return_value={})
        limiter: MagicMock = MagicMock()
        limiter.acquire = AsyncMock(return_value=None)
        limiter.report_proxy_result = AsyncMock()
        metric: MagicMock = MagicMock()

        with (
            patch.object(mod, 'METRIC_YT_REQUEST_DURATION', metric),
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
            await mod.InnerTubeVideoParser.scrape(
                YouTubeVideo(video_id='vid123'), proxy=None, max_retries=1,
            )

        calls: list[str] = [
            c.kwargs['call'] for c in metric.labels.call_args_list
        ]
        self.assertEqual(sorted(calls), ['get_panel', 'next', 'player'])
        self.assertTrue(all(
            c.kwargs['api'] == 'innertube'
            for c in metric.labels.call_args_list
        ))


if __name__ == '__main__':
    unittest.main()
