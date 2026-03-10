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

import orjson
import aiofiles
from jsonschema import Draft202012Validator

import brotli

from scrape_exchange.youtube.youtube_video import YouTubeVideo
from scrape_exchange.youtube.youtube_video import YouTubeMediaType

YOUTUBE_VIDEO_ID: str = 'dQw4w9WgXcQ'
YOUTUBE_SHORT_ID: str = 'Bdl3DiNrIEA'

SCHEMA_PATH: str = 'tests/collateral/boinko-youtube-video-schema.json'

DENO_PATH: str = os.environ.get('HOME', '') + '/.deno/bin/deno'
PO_TOKEN_URL: str = 'http://localhost:4416'


class TestIntegration(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp_dir: str = tempfile.mkdtemp()
        with open(SCHEMA_PATH) as f:
            self.schema: dict[str, any] = json.load(f)
        self.validator = Draft202012Validator(self.schema)

    async def asyncTearDown(self) -> None:
        shutil.rmtree(self.temp_dir)

    def _validate_schema(self, data: dict) -> None:
        errors: list[any] = list(self.validator.iter_errors(data))
        if errors:
            messages: str = '\n'.join(e.message for e in errors)
            self.fail(f'Schema validation failed:\n{messages}')

    async def test_youtube_video(self) -> None:
        video: YouTubeVideo = await YouTubeVideo.scrape(
            video_id=YOUTUBE_VIDEO_ID,
            channel_name='Rick Astley', channel_thumbnail=None,
            deno_path=DENO_PATH, po_token_url=PO_TOKEN_URL, debug=True,
            save_dir=self.temp_dir,
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
            channel_name='byjacobward', channel_thumbnail=None,
            deno_path=DENO_PATH, po_token_url=PO_TOKEN_URL, debug=True,
            save_dir=self.temp_dir
        )
        self.assertEqual(short.video_id, YOUTUBE_SHORT_ID)
        self.assertEqual(short.media_type, YouTubeMediaType.SHORT)
        self._validate_schema(short.to_dict())

        short_from_file: YouTubeVideo = await YouTubeVideo.from_file(
            video_id=YOUTUBE_SHORT_ID, load_dir=self.temp_dir
        )
        self.assertEqual(short, short_from_file)

    async def test_innertube(self) -> None:
        video = YouTubeVideo('N3jdUSEYzdk')
        await video.from_innertube()
        self.assertEqual(video.video_id, 'N3jdUSEYzdk')


if __name__ == '__main__':
    unittest.main()
