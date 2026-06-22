'''
Unit tests for TikTokCreator.from_api() and JSON-Schema
validation.
'''

import json
import unittest
from datetime import datetime, timezone
from pathlib import Path

from jsonschema import Draft202012Validator

from scrape_exchange.tiktok.tiktok_creator import TikTokCreator
from scrape_exchange.tiktok.tiktok_creator import TikTokPlaylistRef
from scrape_exchange.tiktok.tiktok_creator import TikTokVideoRef


_SCHEMA_PATH: Path = (
    Path(__file__).parent.parent
    / 'collateral'
    / 'drand-tiktok-creator-schema.json'
)
_FIXTURE_PATH: Path = (
    Path(__file__).parent.parent
    / 'collateral'
    / 'tiktok'
    / 'user_info_charlidamelio.json'
)


class TestTikTokCreator(unittest.TestCase):

    def test_from_api_maps_fields(self) -> None:
        payload: dict = json.loads(_FIXTURE_PATH.read_text())
        c: TikTokCreator = TikTokCreator.from_api(
            payload,
            scraped_timestamp=datetime(
                2026, 4, 30, 12, 0, tzinfo=timezone.utc,
            ),
        )
        self.assertEqual(c.username, 'charlidamelio')
        self.assertEqual(c.sec_uid, payload['secUid'])
        self.assertEqual(c.user_id, '5831967')
        self.assertEqual(c.follower_count, 155_400_000)
        self.assertTrue(c.verified)
        self.assertEqual(
            [a.name for a in c.avatar_urls],
            ['thumb', 'medium', 'large'],
        )
        self.assertEqual(
            c.url, 'https://www.tiktok.com/@charlidamelio',
        )

    def test_to_dict_validates_against_schema(self) -> None:
        payload: dict = json.loads(_FIXTURE_PATH.read_text())
        c: TikTokCreator = TikTokCreator.from_api(
            payload,
            scraped_timestamp=datetime(
                2026, 4, 30, 12, 0, tzinfo=timezone.utc,
            ),
        )
        c.videos = [
            TikTokVideoRef(
                video_id='123',
                username='charlidamelio',
                url='https://www.tiktok.com/@charlidamelio/video/123',
            ),
        ]
        c.playlists = [
            TikTokPlaylistRef(
                playlist_id='456',
                name='Dance series',
                video_count=12,
                cover_url='https://example.com/cover.jpg',
            ),
        ]
        record: dict = c.to_dict()
        self.assertEqual(record['videos'][0]['video_id'], '123')
        self.assertEqual(
            record['videos'][0]['username'], 'charlidamelio',
        )
        self.assertEqual(record['reposts'], [])
        self.assertEqual(record['liked'], [])
        self.assertEqual(record['playlists'][0]['playlist_id'], '456')

        schema: dict = json.loads(_SCHEMA_PATH.read_text())
        Draft202012Validator(schema).validate(record)

    def test_avatar_urls_preserve_variant_names(self) -> None:
        '''avatar_urls stores each public TikTok avatar variant
        with its source key name preserved. avatar_thumbnail keeps
        the large URL for compatibility.'''
        c: TikTokCreator = TikTokCreator.from_api(
            {
                'uniqueId': 'u',
                'secUid': 's',
                'id': '1',
                'avatarThumb': 'https://cdn.example/thumb.jpg',
                'avatarMedium': 'https://cdn.example/medium.jpg',
                'avatarLarger': 'https://cdn.example/large.jpg',
                'stats': {},
            },
            scraped_timestamp=datetime(
                2026, 4, 30, tzinfo=timezone.utc,
            ),
        )
        self.assertEqual(
            c.avatar_thumbnail, 'https://cdn.example/large.jpg',
        )
        record: dict = c.to_dict()
        self.assertEqual(
            record['avatar_thumbnail'], 'https://cdn.example/large.jpg',
        )
        self.assertEqual(
            record['avatar_urls'],
            [
                {'name': 'thumb', 'url': 'https://cdn.example/thumb.jpg'},
                {
                    'name': 'medium',
                    'url': 'https://cdn.example/medium.jpg',
                },
                {'name': 'large', 'url': 'https://cdn.example/large.jpg'},
            ],
        )
        schema: dict = json.loads(_SCHEMA_PATH.read_text())
        Draft202012Validator(schema).validate(record)

    def test_to_dict_omits_avatar_when_missing(self) -> None:
        '''When the API response has no avatar URL, the
        avatar_thumbnail key must be ABSENT from the output
        (not present-as-null), since the field is an optional
        string and excluded when None.'''
        payload: dict = {
            'uniqueId': 'noavatar',
            'secUid': 'sec_x',
            'id': '999',
            'stats': {},
        }
        c: TikTokCreator = TikTokCreator.from_api(
            payload,
            scraped_timestamp=datetime(
                2026, 4, 30, tzinfo=timezone.utc,
            ),
        )
        record: dict = c.to_dict()
        self.assertNotIn('avatar_thumbnail', record)
        self.assertEqual(record['avatar_urls'], [])

        # And it should still validate against the schema
        # (the field is not in `required`).
        schema: dict = json.loads(_SCHEMA_PATH.read_text())
        Draft202012Validator(schema).validate(record)

    def test_required_fields_enforced(self) -> None:
        from pydantic import ValidationError
        with self.assertRaises(ValidationError):
            TikTokCreator.from_api({}, scraped_timestamp=None)


class TestTikTokCreatorFromUserInfo(unittest.TestCase):
    '''from_user_info() consumes the raw User.info() response and
    must prefer statsV2 (string counts) over the legacy 32-bit
    stats object, which overflows into negatives for huge
    accounts.'''

    def _raw(self, stats: dict, stats_v2: dict) -> dict:
        return {
            'userInfo': {
                'user': {
                    'uniqueId': 'bigcreator',
                    'secUid': 'sec_big',
                    'id': '42',
                },
                'stats': stats,
                'statsV2': stats_v2,
            },
        }

    def test_prefers_statsv2_over_overflowed_stats(self) -> None:
        # stats.heartCount has overflowed int32 into a negative;
        # statsV2 carries the true value as a string.
        resp: dict = self._raw(
            stats={'heartCount': -712940121, 'followerCount': 100},
            stats_v2={
                'heartCount': '11800000000',
                'followerCount': '155400000',
            },
        )
        c: TikTokCreator = TikTokCreator.from_user_info(
            resp,
            scraped_timestamp=datetime(
                2026, 4, 30, tzinfo=timezone.utc,
            ),
        )
        self.assertEqual(c.username, 'bigcreator')
        self.assertEqual(c.heart_count, 11_800_000_000)
        self.assertEqual(c.follower_count, 155_400_000)

        # And the positive counts must validate against the schema
        # (heart_count has minimum: 0).
        schema: dict = json.loads(_SCHEMA_PATH.read_text())
        Draft202012Validator(schema).validate(c.to_dict())

    def test_falls_back_to_stats_when_no_statsv2(self) -> None:
        resp: dict = self._raw(
            stats={'heartCount': 5, 'followerCount': 9},
            stats_v2={},
        )
        c: TikTokCreator = TikTokCreator.from_user_info(
            resp,
            scraped_timestamp=datetime(
                2026, 4, 30, tzinfo=timezone.utc,
            ),
        )
        self.assertEqual(c.heart_count, 5)
        self.assertEqual(c.follower_count, 9)


if __name__ == '__main__':
    unittest.main()
