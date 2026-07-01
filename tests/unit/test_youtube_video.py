import unittest
from pathlib import Path

import orjson
from jsonschema import Draft202012Validator

from scrape_exchange.youtube.youtube_thumbnail import YouTubeThumbnail
from scrape_exchange.youtube.youtube_video import YouTubeVideo


class TestYouTubeVideo(unittest.TestCase):
    def test_to_dict_round_trips_channel_country(self) -> None:
        video: YouTubeVideo = YouTubeVideo(video_id='dQw4w9WgXcQ')
        video.channel_id = 'UCaaaaaaaaaaaaaaaaaaaaaa'
        video.channel_country = 'US'

        restored: YouTubeVideo = YouTubeVideo.from_dict(video.to_dict())

        self.assertEqual(restored.channel_country, 'US')

    def test_from_dict_restores_channel_thumbnail(self) -> None:
        video: YouTubeVideo = YouTubeVideo(
            video_id='dQw4w9WgXcQ',
            channel_thumbnail=YouTubeThumbnail({
                'url': 'https://yt3.ggpht.com/example=s88-c-k-c0x00ffffff',
                'width': 88,
                'height': 88,
                'id': 'avatar',
                'preference': 0,
            }),
        )

        restored: YouTubeVideo = YouTubeVideo.from_dict(video.to_dict())

        self.assertIsNotNone(restored.channel_thumbnail_asset)
        self.assertEqual(
            restored.channel_thumbnail_asset.url,
            'https://yt3.ggpht.com/example=s88-c-k-c0x00ffffff',
        )
        self.assertEqual(restored.channel_thumbnail_url,
                         restored.channel_thumbnail_asset.url)
        self.assertEqual(restored.channel_thumbnail_asset.width, 88)
        self.assertEqual(restored.channel_thumbnail_asset.height, 88)

    def test_from_yt_dlp_uses_uploader_id_for_channel_handle(
        self,
    ) -> None:
        video: YouTubeVideo = YouTubeVideo.from_yt_dlp({
            'id': 'C7TICwKPG5g',
            'uploader_id': '@CanonicalHandle',
            'channel': '',
            'formats': [],
        })

        self.assertEqual(video.channel_handle, 'CanonicalHandle')

    def test_yt_dlp_channel_handle_prefers_uploader_id_over_channel(
        self,
    ) -> None:
        handle: str | None = YouTubeVideo._yt_dlp_channel_handle({
            'uploader_id': '@CanonicalHandle',
            'channel': 'Display Name',
        })

        self.assertEqual(handle, 'CanonicalHandle')


class TestYouTubeVideoSchema(unittest.TestCase):
    def test_schema_allows_channel_country(self) -> None:
        schema_path: Path = Path(
            'tests/collateral/boinko-youtube-video-schema.json'
        )
        schema: dict = orjson.loads(schema_path.read_bytes())
        validator = Draft202012Validator(schema)
        video: YouTubeVideo = YouTubeVideo(video_id='dQw4w9WgXcQ')
        video.channel_id = 'UCaaaaaaaaaaaaaaaaaaaaaa'
        video.channel_country = 'US'

        errors: list = list(validator.iter_errors(video.to_dict()))

        self.assertEqual(errors, [])


if __name__ == '__main__':
    unittest.main()
