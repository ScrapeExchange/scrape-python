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


_SCHEMA_PATH: Path = (
    Path(__file__).parent.parent
    / 'collateral'
    / 'boinko-tiktok-creator-schema.json'
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
        record: dict = c.to_dict()

        schema: dict = json.loads(_SCHEMA_PATH.read_text())
        Draft202012Validator(schema).validate(record)

    def test_to_dict_omits_avatar_when_missing(self) -> None:
        '''When the API response has no avatar URL, the
        avatar_thumbnail key must be ABSENT from the output
        (not present-as-null), so the schema's $ref-typed
        property doesn't reject the record.'''
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

        # And it should still validate against the schema
        # (the field is not in `required`).
        schema: dict = json.loads(_SCHEMA_PATH.read_text())
        Draft202012Validator(schema).validate(record)

    def test_required_fields_enforced(self) -> None:
        from pydantic import ValidationError
        with self.assertRaises(ValidationError):
            TikTokCreator.from_api({}, scraped_timestamp=None)


if __name__ == '__main__':
    unittest.main()
