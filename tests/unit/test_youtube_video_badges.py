'''Video disclosures observed on the watch page, not channel badges.'''

import asyncio
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import orjson
from jsonschema import Draft202012Validator
from yt_dlp import YoutubeDL
from yt_dlp.extractor.youtube import YoutubeIE

from scrape_exchange.youtube.youtube_video import YouTubeVideo
from scrape_exchange.youtube.youtube_video_innertube import InnerTubeVideoParser


def watch_data() -> dict[str, Any]:
    # Sanitised from video 0FmGgMewBSk's ytInitialData. Tracking omitted.
    return {'contents': {'twoColumnWatchNextResults': {'results': {
        'results': {'contents': [{'videoPrimaryInfoRenderer': {'badges': [
            {'metadataBadgeRenderer': {
                'icon': {'iconType': 'INFO'},
                'style': 'BADGE_STYLE_TYPE_SIMPLE',
                'label': 'AI',
                'accessibilityData': {
                    'label': 'AI: Content was made with AI',
                },
            }},
        ]}}]},
    }}}}


EXPECTED: list[dict[str, str]] = [{
    'label': 'AI', 'style': 'BADGE_STYLE_TYPE_SIMPLE', 'icon': 'INFO',
    'accessibility_label': 'AI: Content was made with AI',
}]


class TestVideoBadges(unittest.TestCase):
    def test_innertube_persists_observed_ai_badge(self) -> None:
        video: YouTubeVideo = YouTubeVideo(video_id='0FmGgMewBSk')
        parser: InnerTubeVideoParser = InnerTubeVideoParser(video)
        parser._parse_next_data(watch_data())
        data: dict[str, Any] = video.to_dict()
        self.assertEqual(data['badges'], EXPECTED)
        self.assertEqual(
            YouTubeVideo.from_dict(data).to_dict()['badges'], EXPECTED,
        )

    def test_filters_duplicates_malformed_and_unrelated_badges(self) -> None:
        from scrape_exchange.youtube.youtube_video_badges import (
            extract_video_badges,
        )
        data: dict[str, Any] = watch_data()
        contents: list = data['contents']['twoColumnWatchNextResults'][
            'results']['results']['contents']
        badges: list = contents[0]['videoPrimaryInfoRenderer']['badges']
        badges.extend([badges[0], None, {'metadataBadgeRenderer': None}, {
            'metadataBadgeRenderer': {'label': 42},
        }, {'metadataBadgeRenderer': {'label': 'Future label'}}])
        contents.append({'videoSecondaryInfoRenderer': {'owner': {
            'badges': [{'metadataBadgeRenderer': {'label': 'Verified'}}],
        }}})
        self.assertEqual(
            extract_video_badges(data), EXPECTED + [{'label': 'Future label'}],
        )
        for missing in ({}, None, {'contents': None}):
            self.assertEqual(extract_video_badges(missing), [])

    def test_legacy_records_default_to_empty_badges(self) -> None:
        video: YouTubeVideo = YouTubeVideo.from_dict({'video_id': 'example'})
        self.assertEqual(video.to_dict()['badges'], [])
        imported: YouTubeVideo = YouTubeVideo.from_yt_dlp({'id': 'example'})
        self.assertEqual(imported.to_dict()['badges'], [])

    def test_yt_dlp_adapter_retains_data_and_resets_between_videos(self) -> None:
        from scrape_exchange.youtube.youtube_video_badges import (
            BadgeYoutubeIE,
        )

        def upstream_extract(
            extractor: YoutubeIE, url: str,
        ) -> dict[str, Any]:
            extractor._download_initial_data('example', '', 'web', {})
            return {'id': 'example', 'title': 'Example'}

        extractor: BadgeYoutubeIE = BadgeYoutubeIE()
        with (
            patch.object(YoutubeIE, '_real_extract', upstream_extract),
            patch.object(YoutubeIE, '_download_initial_data',
                         side_effect=[watch_data(), {}]),
        ):
            info: dict[str, Any] = extractor._real_extract('unused')
            video: YouTubeVideo = YouTubeVideo.from_yt_dlp(info)
            self.assertEqual(video.to_dict()['badges'], EXPECTED)
            self.assertEqual(video.title, 'Example')
            self.assertEqual(extractor._real_extract('unused')['badges'], [])

    def test_schema_validates_badges_and_legacy_records(self) -> None:
        schema: dict = orjson.loads(Path(
            'tests/collateral/boinko-youtube-video-schema.json',
        ).read_bytes())
        Draft202012Validator.check_schema(schema)
        validator: Draft202012Validator = Draft202012Validator(schema)
        data: dict = YouTubeVideo(video_id='example').to_dict()
        data['badges'] = EXPECTED
        self.assertEqual(list(validator.iter_errors(data)), [])
        data['badges'] = [{'label': 42}]
        self.assertTrue(list(validator.iter_errors(data)))
        data.pop('badges')
        self.assertEqual(list(validator.iter_errors(data)), [])

    def test_configured_extractor_reads_badges_from_embedded_html(self) -> None:
        from scrape_exchange.youtube.youtube_video_badges import (
            BadgeYoutubeIE,
        )
        with patch(
            'scrape_exchange.youtube.youtube_video._resolve_deno_path',
            return_value='/usr/bin/true',
        ):
            client: YoutubeDL = YouTubeVideo._setup_download_client(
                '/usr/bin/true', 'http://localhost:4416',
            )
        try:
            extractor: BadgeYoutubeIE = client.get_info_extractor('Youtube')
            self.assertIsInstance(extractor, BadgeYoutubeIE)
            html: str = (
                f'var ytInitialData = {orjson.dumps(watch_data()).decode()};'
            )

            def upstream_extract(
                instance: YoutubeIE, url: str,
            ) -> dict[str, Any]:
                instance._download_initial_data('example', html, 'web', {})
                return {'id': 'example'}

            with patch.object(YoutubeIE, '_real_extract', upstream_extract):
                self.assertEqual(
                    extractor._real_extract('unused')['badges'], EXPECTED,
                )
        finally:
            client.close()


class TestScrapeVideoBadges(unittest.IsolatedAsyncioTestCase):
    async def test_scrape_video_copies_badges_from_extractor_result(self) -> None:
        client: MagicMock = MagicMock()
        client.extract_info.return_value = {
            'id': 'example', 'badges': EXPECTED,
            'upload_date': '20260906', 'formats': [],
        }
        limiter: MagicMock = MagicMock()
        limiter.acquire = AsyncMock(return_value=None)
        limiter.get_cookie_file_cached.return_value = None
        video: YouTubeVideo = YouTubeVideo(
            video_id='example', download_client=client,
        )
        with (
            patch(
                'scrape_exchange.youtube.youtube_video.YouTubeRateLimiter.get',
                return_value=limiter,
            ),
            # Isolate metrics: the existing gauge's mostrecent mode does
            # not support inc()/dec() in this installed Prometheus client.
            patch(
                'scrape_exchange.youtube.youtube_video.'
                'METRIC_EXTRACT_INFO_ACTIVE',
            ),
            patch.object(
                asyncio.get_running_loop(), 'run_in_executor',
                new=AsyncMock(return_value=client.extract_info.return_value),
            ),
        ):
            await video._scrape_video()
        self.assertEqual(video.to_dict()['badges'], EXPECTED)
