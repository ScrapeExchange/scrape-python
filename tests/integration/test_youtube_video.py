#!/usr/bin/env python3
'''

Integration tests for AsyncYouTubeClient that perform real HTTP calls to
YouTube.

These tests hit live YouTube endpoints and may be slow or flaky depending on
network conditions. They are intended to run separately from unit tests.
'''

import os
import json
import shutil
import unittest
import tempfile
import datetime

from datetime import UTC

import orjson
import aiofiles
from jsonschema import Draft202012Validator

import brotli

from scrape_exchange.youtube.youtube_client import AsyncYouTubeClient
from scrape_exchange.youtube.youtube_video import YouTubeVideo
from scrape_exchange.youtube.youtube_video import YouTubeMediaType

YOUTUBE_VIDEO_ID: str = 'dQw4w9WgXcQ'
YOUTUBE_SHORT_ID: str = 'Bdl3DiNrIEA'

SCHEMA_PATH: str = 'tests/collateral/boinko-youtube-video-schema.json'

DENO_PATH: str = os.environ.get('HOME', '') + '/.deno/bin/deno'
PO_TOKEN_URL: str = 'http://localhost:4416'
PROXIES_FILE: str = 'tests/collateral/local/proxies.lst'


class TestIntegration(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp_dir: str = tempfile.mkdtemp()
        with open(SCHEMA_PATH) as f:
            self.schema: dict[str, any] = json.load(f)
        self.validator = Draft202012Validator(self.schema)

        self.proxies: list[str] = []
        if os.path.exists(PROXIES_FILE):
            with open(PROXIES_FILE) as f:
                self.proxies: list[str] = [
                    line.strip() for line in f if line.strip()
                ]

    async def asyncTearDown(self) -> None:
        shutil.rmtree(self.temp_dir)

    def _validate_schema(self, data: dict) -> None:
        errors: list[any] = list(self.validator.iter_errors(data))
        if errors:
            messages: str = '\n'.join(e.message for e in errors)
            self.fail(f'Schema validation failed:\n{messages}')

    async def test_innertube_video_scrape(self) -> None:
        video: YouTubeVideo = YouTubeVideo(YOUTUBE_VIDEO_ID)
        video.browse_client = AsyncYouTubeClient()
        await video.from_innertube()
        self.assertEqual(video.video_id, YOUTUBE_VIDEO_ID)
        self.assertIsNone(video.media_type, None)
        self.assertEqual(video.duration, 213)
        self.assertGreaterEqual(video.view_count, 1759729070)
        self.assertGreaterEqual(video.like_count, 18917330)
        self.assertGreaterEqual(video.comment_count, 2400000)
        self.assertFalse(video.age_restricted)
        self.assertTrue(video.is_family_safe)
        self.assertFalse(video.is_tv_film_video)
        self.assertEqual(video.age_limit, 0)
        self.assertEqual(video.category, 'Music')
        self.assertGreaterEqual(len(video.available_country_codes), 40)
        self.assertEqual(video.privacy_status, 'public')
        self.assertIn('rick astley', video.keywords)
        self.assertIn('never gonna give you up', video.keywords)
        self.assertFalse(video.is_live)
        self.assertFalse(video.is_tv_film_video)
        self.assertGreater(len(video.thumbnails), 2)
        self.assertGreater(
            video.published_timestamp,
            datetime.datetime(2009, 10, 25, tzinfo=UTC)
        )
        self.assertEqual(
            video.channel_url, 'http://www.youtube.com/@RickAstleyYT'
        )
        self.assertEqual(video.channel_handle, 'Rick Astley')
        self.assertEqual(video.privacy_status, 'public')

        self._validate_schema(video.to_dict())

    async def test_youtube_video(self) -> None:
        video: YouTubeVideo = await YouTubeVideo.scrape(
            video_id=YOUTUBE_VIDEO_ID,
            channel_handle='Rick Astley', channel_thumbnail=None,
            deno_path=DENO_PATH, po_token_url=PO_TOKEN_URL,
            ytdlp_cache_dir=self.temp_dir, debug=True,
            save_dir=self.temp_dir, proxies=self.proxies
        )
        self.assertEqual(video.video_id, YOUTUBE_VIDEO_ID)
        self.assertEqual(video.media_type, YouTubeMediaType.VIDEO)
        self._validate_schema(video.to_dict())

        async with aiofiles.open(
                f'{self.temp_dir}/{YOUTUBE_VIDEO_ID}.json.br', 'rb') as f:
            video_data: str = orjson.loads(brotli.decompress(await f.read()))
            self._validate_schema(video_data)
            loaded_video: YouTubeVideo = YouTubeVideo.from_dict(
                video_data
            )
            self.assertEqual(video, loaded_video)

    async def test_youtube_short(self) -> None:
        short: YouTubeVideo = await YouTubeVideo.scrape(
            video_id=YOUTUBE_SHORT_ID,
            channel_handle='byjacobward', channel_thumbnail=None,
            ytdlp_cache_dir=self.temp_dir, deno_path=DENO_PATH,
            po_token_url=PO_TOKEN_URL, debug=True, save_dir=self.temp_dir
        )
        self.assertEqual(short.video_id, YOUTUBE_SHORT_ID)
        self.assertEqual(short.media_type, YouTubeMediaType.SHORT)
        self._validate_schema(short.to_dict())

        short_from_file: YouTubeVideo = await YouTubeVideo.from_file(
            video_id=YOUTUBE_SHORT_ID, load_dir=self.temp_dir
        )
        self.assertEqual(short, short_from_file)


if __name__ == '__main__':
    unittest.main()
