import unittest

from scrape_exchange.youtube.youtube_thumbnail import YouTubeThumbnail
from scrape_exchange.youtube.youtube_video import YouTubeVideo


class TestYouTubeVideo(unittest.TestCase):
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


if __name__ == '__main__':
    unittest.main()
