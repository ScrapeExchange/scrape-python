'''_scrape_to_disk observes scrape_duration_seconds per attempt.'''

import unittest
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.creator_map import InMemoryCreatorMap
from scrape_exchange.youtube.innertube_calls import record_call


def _settings() -> MagicMock:
    settings: MagicMock = MagicMock()
    settings.ytdlp_cache_dir = '/tmp/ytdlp-cache'
    settings.video_data_directory = '/tmp/videos'
    settings.log_level = 'INFO'
    settings.video_use_yt_dlp = False
    return settings


def _video() -> MagicMock:
    video: MagicMock = MagicMock()
    video.to_file = AsyncMock()
    video.channel_id = 'UC1234567890abcdefghij'
    video.channel_country = 'NL'
    video.category = None
    return video


class TestScrapeDuration(unittest.IsolatedAsyncioTestCase):
    async def _run(
        self, calls: list[str], *, fail: bool = False,
        creator_map: InMemoryCreatorMap | None = None,
    ) -> dict[str, Any]:
        from tools import yt_video_scrape

        async def fake_scrape(*args: Any, **kwargs: Any) -> MagicMock:
            for call in calls:
                record_call(call)
            if fail:
                raise RuntimeError('boom')
            return _video()

        metric: MagicMock = MagicMock()
        with (
            patch.object(yt_video_scrape, 'METRIC_SCRAPE_DURATION', metric),
            patch.object(
                yt_video_scrape.YouTubeVideo, 'scrape',
                new=AsyncMock(side_effect=fake_scrape),
            ),
            patch.object(
                yt_video_scrape.YouTubeChannel, 'resolve_channel_id',
                new=AsyncMock(return_value='Handle'),
            ),
        ):
            try:
                await yt_video_scrape._scrape_to_disk(
                    'dQw4w9WgXcQ', settings=_settings(), proxy=None,
                    download_client=None,
                    creator_map_backend=creator_map,
                )
            except RuntimeError:
                pass
        metric.labels.assert_called_once()
        metric.labels.return_value.observe.assert_called_once()
        return metric.labels.call_args.kwargs

    async def test_labels_success_with_transcript(self) -> None:
        labels: dict[str, Any] = await self._run(
            ['player', 'next', 'transcript'],
        )
        self.assertEqual(labels['innertube_calls'], 'player_next_transcript')
        self.assertEqual(labels['outcome'], 'success')
        self.assertEqual(labels['scraper'], 'video_scraper')
        self.assertEqual(labels['api'], 'innertube')

    async def test_labels_without_transcript(self) -> None:
        labels: dict[str, Any] = await self._run(['player', 'next'])
        self.assertEqual(labels['innertube_calls'], 'player_next')

    async def test_channel_browse_on_creator_map_miss(self) -> None:
        labels: dict[str, Any] = await self._run(
            ['player', 'next'], creator_map=InMemoryCreatorMap(),
        )
        self.assertEqual(
            labels['innertube_calls'], 'player_next_channel_browse',
        )

    async def test_failure_is_observed_with_calls_so_far(self) -> None:
        labels: dict[str, Any] = await self._run(['player'], fail=True)
        self.assertEqual(labels['outcome'], 'failure')
        self.assertEqual(labels['innertube_calls'], 'player')
