'''
Unit tests for TikTokVideo.from_api() including photo-post
branch and hashtag/mention extraction from desc.
'''

import json
import unittest
from datetime import datetime, timezone
from pathlib import Path

from jsonschema import Draft202012Validator
from pydantic import ValidationError

from scrape_exchange.tiktok.tiktok_video import (
    TikTokVideo,
    extract_hashtags,
    extract_mentions,
)


_COLLATERAL: Path = (
    Path(__file__).parent.parent / 'collateral'
)
_SCHEMA_PATH: Path = (
    _COLLATERAL / 'boinko-tiktok-video-schema.json'
)


def _load(name: str) -> dict:
    return json.loads(
        (_COLLATERAL / 'tiktok' / name).read_text(),
    )


class TestTikTokVideo(unittest.TestCase):

    def test_from_api_video(self) -> None:
        payload: dict = _load('video_info_sample.json')
        v: TikTokVideo = TikTokVideo.from_api(
            payload,
            scraped_timestamp=datetime(
                2026, 4, 30, tzinfo=timezone.utc,
            ),
        )
        self.assertEqual(v.video_id, '7000000000000000001')
        self.assertEqual(v.username, 'charlidamelio')
        self.assertFalse(v.is_photo_post)
        self.assertEqual(v.duration, 28)
        self.assertEqual(v.view_count, 12_000_000)
        self.assertEqual(v.sound.id, '7100000000000000001')
        self.assertIn('cover', v.thumbnails)

    def test_from_api_photo_post(self) -> None:
        payload: dict = _load('video_info_photo_post.json')
        v: TikTokVideo = TikTokVideo.from_api(
            payload,
            scraped_timestamp=datetime(
                2026, 4, 30, tzinfo=timezone.utc,
            ),
        )
        self.assertTrue(v.is_photo_post)
        self.assertEqual(v.image_count, 3)
        self.assertEqual(v.duration, 0)

    def test_hashtag_extraction(self) -> None:
        self.assertEqual(
            extract_hashtags(
                'loving #fyp content with friends @bestie #foryou',
            ),
            ['fyp', 'foryou'],
        )

    def test_mention_extraction(self) -> None:
        self.assertEqual(
            extract_mentions(
                'loving #fyp content with friends @bestie #foryou',
            ),
            ['bestie'],
        )

    def test_to_dict_validates_against_schema(self) -> None:
        payload: dict = _load('video_info_sample.json')
        v: TikTokVideo = TikTokVideo.from_api(
            payload,
            scraped_timestamp=datetime(
                2026, 4, 30, tzinfo=timezone.utc,
            ),
        )
        schema: dict = json.loads(_SCHEMA_PATH.read_text())
        Draft202012Validator(schema).validate(v.to_dict())

    def test_to_dict_omits_sound_when_missing(self) -> None:
        '''When the API omits music, the sound key must be
        ABSENT from the output (not present-as-null) so the
        schema's $ref-typed property doesn't reject it.'''
        payload: dict = {
            'id': '999',
            'desc': 'no sound here',
            'createTime': 1700000000,
            'author': {
                'uniqueId': 'someuser',
                'secUid': 'sec_x',
            },
            'video': {
                'duration': 10,
                'cover': 'https://example/c.jpg',
                'subtitleInfos': [],
            },
            'stats': {
                'playCount': 1, 'diggCount': 0,
                'commentCount': 0, 'shareCount': 0,
                'collectCount': 0,
            },
        }
        v: TikTokVideo = TikTokVideo.from_api(
            payload,
            scraped_timestamp=datetime(
                2026, 4, 30, tzinfo=timezone.utc,
            ),
        )
        record: dict = v.to_dict()
        self.assertNotIn('sound', record)
        schema: dict = json.loads(_SCHEMA_PATH.read_text())
        Draft202012Validator(schema).validate(record)

    def test_required_fields_enforced(self) -> None:
        with self.assertRaises(ValidationError):
            TikTokVideo.from_api({}, scraped_timestamp=None)


if __name__ == '__main__':
    unittest.main()
