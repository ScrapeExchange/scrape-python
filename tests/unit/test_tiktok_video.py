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

from scrape_exchange.tiktok.tiktok_format import TikTokFormat
from scrape_exchange.tiktok.tiktok_video import (
    TikTokVideo,
    extract_hashtags,
    extract_mentions,
)


_COLLATERAL: Path = (
    Path(__file__).parent.parent / 'collateral'
)
_SCHEMA_PATH: Path = (
    _COLLATERAL / 'drand-tiktok-video-schema.json'
)


def _load(name: str) -> dict:
    return json.loads(
        (_COLLATERAL / 'tiktok' / name).read_text(),
    )


def _stream_payload() -> dict:
    '''Minimal payload with stream fields for format tests.'''
    return {
        'id': '7000000000000000002',
        'desc': 'stream fields',
        'createTime': 1700000000,
        'author': {
            'uniqueId': 'someuser',
            'secUid': 'sec_x',
        },
        'video': {
            'duration': 12,
            'width': 720,
            'height': 1280,
            'definition': '540p',
            'videoQuality': 'normal',
            'format': 'mp4',
            'codecType': 'h264',
            'bitrate': 1132103,
            'encodedType': 'normal',
            'playAddr': (
                'https://v16-webapp-prime.tiktok.com/video/'
                'tos/sample-play/?mime_type=video_mp4'
                '&expire=1767139200&signature=0000'
            ),
            'downloadAddr': (
                'https://v16-webapp-prime.tiktok.com/video/'
                'tos/sample-dl/?mime_type=video_mp4'
                '&expire=1767139200&signature=0000'
            ),
            'volumeInfo': {'Loudness': -14.2, 'Peak': 0.79433},
            'bitrateInfo': [
                {
                    'GearName': 'normal_540_0',
                    'Bitrate': 1132103,
                    'QualityType': 20,
                    'CodecType': 'h264',
                    'PlayAddr': {
                        'DataSize': 3962360,
                        'Width': 576,
                        'Height': 1024,
                        'Uri': 'v09044g40000sample0',
                        'UrlKey': (
                            'v09044g40000sample0_h264_540p'
                            '_1132103'
                        ),
                        'FileHash': (
                            'abc0123456789abcdef0123456789abc'
                        ),
                        'UrlList': [
                            'https://v16-webapp-prime.tiktok'
                            '.com/video/tos/sample0/'
                            '?mime_type=video_mp4'
                            '&expire=1767139200'
                            '&signature=0000',
                        ],
                    },
                },
                {
                    'GearName': 'adapt_540_1',
                    'Bitrate': 789012,
                    'QualityType': 28,
                    'CodecType': 'bytevc1',
                    'PlayAddr': {
                        'DataSize': 2761542,
                        'Width': 576,
                        'Height': 1024,
                        'Uri': 'v09044g40000sample1',
                        'UrlKey': (
                            'v09044g40000sample1_bytevc1'
                            '_540p_789012'
                        ),
                        'FileHash': (
                            'def0123456789abcdef0123456789def'
                        ),
                        'UrlList': [
                            'https://v16-webapp-prime.tiktok'
                            '.com/video/tos/sample1/'
                            '?mime_type=video_mp4'
                            '&expire=1767139200'
                            '&signature=0000',
                        ],
                    },
                },
            ],
            'subtitleInfos': [],
        },
        'stats': {
            'playCount': 1, 'diggCount': 0,
            'commentCount': 0, 'shareCount': 0,
            'collectCount': 0,
        },
    }


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
        self.assertEqual(v.author_id, '1234567890')
        self.assertEqual(v.author_nickname, 'Charli')
        self.assertTrue(v.author_verified)
        self.assertEqual(
            [a.model_dump() for a in v.author_avatar_urls],
            [
                {'name': 'thumb', 'url': 'https://example/avatar-thumb.jpeg'},
                {
                    'name': 'medium',
                    'url': 'https://example/avatar-medium.jpeg',
                },
                {'name': 'large', 'url': 'https://example/avatar-large.jpeg'},
            ],
        )
        self.assertEqual(v.author_stats['followerCount'], 155_000_000)
        self.assertTrue(v.is_hd_bitrate)
        self.assertEqual(v.repost_count, 1234)
        self.assertEqual(v.text_language, 'en')
        self.assertTrue(v.share_enabled)
        self.assertTrue(v.can_repost)
        self.assertEqual(
            v.creator_ai_comment['notEligibleReason'], 0,
        )
        self.assertEqual(
            v.sound.cover_large_url,
            'https://example/music-large.jpeg',
        )
        self.assertFalse(v.sound.copyrighted)
        self.assertEqual(v.raw_item['id'], '7000000000000000001')
        self.assertEqual(
            sorted(v.formats.keys()),
            ['adapt_540_1', 'adapt_720_1', 'normal_540_0'],
        )
        self.assertEqual(
            v.formats['normal_540_0'].codec, 'h264',
        )
        self.assertEqual(
            v.formats['adapt_720_1'].codec, 'bytevc1',
        )
        self.assertEqual(
            v.formats['adapt_720_1'].width, 720,
        )
        self.assertEqual(
            v.formats['adapt_720_1'].height, 1280,
        )
        self.assertEqual(v.definition, '540p')
        self.assertEqual(v.container_format, 'mp4')
        self.assertEqual(v.bitrate, 1132103)
        self.assertIsNotNone(v.play_url)
        self.assertIsNotNone(v.download_url)

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
        self.assertEqual(
            v.image_urls,
            [
                'https://example/1.jpg',
                'https://example/2.jpg',
                'https://example/3.jpg',
            ],
        )
        self.assertEqual(v.duration, 0)

    def test_from_api_formats(self) -> None:
        v: TikTokVideo = TikTokVideo.from_api(
            _stream_payload(),
            scraped_timestamp=datetime(
                2026, 4, 30, tzinfo=timezone.utc,
            ),
        )
        self.assertEqual(
            sorted(v.formats.keys()),
            ['adapt_540_1', 'normal_540_0'],
        )
        fmt: TikTokFormat = v.formats['adapt_540_1']
        self.assertEqual(fmt.bitrate, 789012)
        self.assertEqual(fmt.codec, 'bytevc1')
        self.assertEqual(fmt.width, 576)
        self.assertEqual(fmt.height, 1024)
        self.assertEqual(fmt.data_size, 2761542)
        self.assertEqual(len(fmt.urls), 1)

    def test_from_api_default_stream_fields(self) -> None:
        v: TikTokVideo = TikTokVideo.from_api(
            _stream_payload(),
            scraped_timestamp=datetime(
                2026, 4, 30, tzinfo=timezone.utc,
            ),
        )
        self.assertTrue(
            v.play_url.startswith(
                'https://v16-webapp-prime.tiktok.com/',
            ),
        )
        self.assertIn('sample-dl', v.download_url)
        self.assertEqual(v.definition, '540p')
        self.assertEqual(v.video_quality, 'normal')
        self.assertEqual(v.container_format, 'mp4')
        self.assertEqual(v.codec, 'h264')
        self.assertEqual(v.bitrate, 1132103)
        self.assertEqual(v.encoded_type, 'normal')

    def test_from_api_no_stream_data(self) -> None:
        '''Payloads without bitrateInfo (e.g. photo posts or
        trimmed responses) yield an empty formats dict and
        None scalars — never an exception.'''
        payload: dict = _stream_payload()
        payload['video'] = {
            'duration': 12, 'subtitleInfos': [],
        }
        v: TikTokVideo = TikTokVideo.from_api(
            payload,
            scraped_timestamp=datetime(
                2026, 4, 30, tzinfo=timezone.utc,
            ),
        )
        self.assertEqual(v.formats, {})
        self.assertIsNone(v.play_url)
        self.assertIsNone(v.definition)
        self.assertIsNone(v.bitrate)

    def test_from_api_malformed_bitrate_info(self) -> None:
        payload: dict = _stream_payload()
        payload['video']['bitrateInfo'] = 'bogus'
        v: TikTokVideo = TikTokVideo.from_api(
            payload,
            scraped_timestamp=datetime(
                2026, 4, 30, tzinfo=timezone.utc,
            ),
        )
        self.assertEqual(v.formats, {})

    def test_stream_payload_to_dict_validates(self) -> None:
        v: TikTokVideo = TikTokVideo.from_api(
            _stream_payload(),
            scraped_timestamp=datetime(
                2026, 4, 30, tzinfo=timezone.utc,
            ),
        )
        record: dict = v.to_dict()
        self.assertNotIn('codec', record)
        self.assertNotIn('bitrate', record)
        self.assertIn('codec', record['formats']['normal_540_0'])
        self.assertIn('bitrate', record['formats']['normal_540_0'])
        schema: dict = json.loads(_SCHEMA_PATH.read_text())
        Draft202012Validator(schema).validate(record)

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
        record: dict = v.to_dict()
        self.assertEqual(
            record['author_avatar_url'],
            'https://example/avatar-medium.jpeg',
        )
        schema: dict = json.loads(_SCHEMA_PATH.read_text())
        Draft202012Validator(schema).validate(record)

    def test_to_dict_round_trips_through_model(self) -> None:
        payload: dict = _load('video_info_sample.json')
        v: TikTokVideo = TikTokVideo.from_api(
            payload,
            scraped_timestamp=datetime(
                2026, 4, 30, tzinfo=timezone.utc,
            ),
        )

        record: dict = v.to_dict()
        restored: TikTokVideo = TikTokVideo.model_validate(record)

        self.assertEqual(
            restored.to_dict()['author_avatar_url'],
            'https://example/avatar-medium.jpeg',
        )

    def test_to_dict_author_avatar_url_fallback_order(self) -> None:
        payload: dict = _load('video_info_sample.json')
        payload['author'].pop('avatarMedium')
        v: TikTokVideo = TikTokVideo.from_api(
            payload,
            scraped_timestamp=datetime(
                2026, 4, 30, tzinfo=timezone.utc,
            ),
        )
        record: dict = v.to_dict()
        self.assertEqual(
            record['author_avatar_url'],
            'https://example/avatar-thumb.jpeg',
        )

        payload: dict = _load('video_info_sample.json')
        payload['author'].pop('avatarMedium')
        payload['author'].pop('avatarThumb')
        v: TikTokVideo = TikTokVideo.from_api(
            payload,
            scraped_timestamp=datetime(
                2026, 4, 30, tzinfo=timezone.utc,
            ),
        )
        record: dict = v.to_dict()
        self.assertEqual(
            record['author_avatar_url'],
            'https://example/avatar-large.jpeg',
        )

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

    def test_music_without_id_raises_validation_error(
        self,
    ) -> None:
        '''A music block present but with no id should raise
        ValidationError, not KeyError, per the from_api
        contract.'''
        payload: dict = {
            'id': '999',
            'desc': 'no music id',
            'createTime': 1700000000,
            'author': {
                'uniqueId': 'someuser',
                'secUid': 'sec_x',
            },
            'video': {'duration': 10, 'subtitleInfos': []},
            'music': {'title': 'no id here'},
            'stats': {
                'playCount': 1, 'diggCount': 0,
                'commentCount': 0, 'shareCount': 0,
                'collectCount': 0,
            },
        }
        with self.assertRaises(ValidationError):
            TikTokVideo.from_api(
                payload,
                scraped_timestamp=datetime(
                    2026, 4, 30, tzinfo=timezone.utc,
                ),
            )


if __name__ == '__main__':
    unittest.main()
