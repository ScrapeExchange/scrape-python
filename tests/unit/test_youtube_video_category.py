import unittest

from scrape_exchange.youtube.youtube_video import YouTubeVideo


def _player_response(
    microformat_extra: dict, details_extra: dict | None = None,
) -> dict:
    video_details: dict = {
        'videoId': 'dQw4w9WgXcQ',
        'title': 'Test video',
        'lengthSeconds': '212',
        'viewCount': '100',
    }
    video_details.update(details_extra or {})
    microformat: dict = {
        'lengthSeconds': '212',
        'publishDate': '2009-10-24T23:57:33-07:00',
        'uploadDate': '2009-10-24T23:57:33-07:00',
    }
    microformat.update(microformat_extra)
    return {
        'videoDetails': video_details,
        'microformat': {'playerMicroformatRenderer': microformat},
        'playabilityStatus': {'status': 'OK'},
    }


class TestParseVideoHtmlCategory(unittest.TestCase):
    def test_category_read_from_microformat(self) -> None:
        video: YouTubeVideo = YouTubeVideo(video_id='dQw4w9WgXcQ')
        video._parse_video_html(_player_response({'category': 'Music'}))

        self.assertEqual(video.category, 'Music')
        self.assertEqual(video.to_dict()['category'], 'Music')

    def test_category_falls_back_to_video_details(self) -> None:
        video: YouTubeVideo = YouTubeVideo(video_id='dQw4w9WgXcQ')
        video._parse_video_html(
            _player_response({}, {'category': 'Gaming'})
        )

        self.assertEqual(video.category, 'Gaming')

    def test_existing_category_not_overwritten(self) -> None:
        video: YouTubeVideo = YouTubeVideo(video_id='dQw4w9WgXcQ')
        video.category = 'Education'
        video._parse_video_html(_player_response({'category': 'Music'}))

        self.assertEqual(video.category, 'Education')

    def test_missing_category_stays_none(self) -> None:
        video: YouTubeVideo = YouTubeVideo(video_id='dQw4w9WgXcQ')
        video._parse_video_html(_player_response({}))

        self.assertIsNone(video.category)


if __name__ == '__main__':
    unittest.main()
