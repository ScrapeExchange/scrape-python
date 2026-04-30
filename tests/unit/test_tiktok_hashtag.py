'''
Unit tests for TikTokHashtag.from_api() and schema validation.
'''

import json
import unittest
from datetime import datetime, timezone
from pathlib import Path

from jsonschema import Draft202012Validator
from pydantic import ValidationError

from scrape_exchange.tiktok.tiktok_hashtag import (
    TikTokHashtag,
)


_COLLATERAL: Path = (
    Path(__file__).parent.parent / 'collateral'
)
_SCHEMA_PATH: Path = (
    _COLLATERAL / 'boinko-tiktok-hashtag-schema.json'
)
_FIXTURE_PATH: Path = (
    _COLLATERAL / 'tiktok' / 'hashtag_info_fyp.json'
)


class TestTikTokHashtag(unittest.TestCase):

    def test_from_api_maps_fields(self) -> None:
        payload: dict = json.loads(_FIXTURE_PATH.read_text())
        h: TikTokHashtag = TikTokHashtag.from_api(
            payload,
            scraped_timestamp=datetime(
                2026, 4, 30, tzinfo=timezone.utc,
            ),
        )
        self.assertEqual(h.name, 'fyp')
        self.assertEqual(h.hashtag_id, '229207')
        self.assertEqual(h.video_count, 130_000_000)
        self.assertFalse(h.is_commerce)
        self.assertEqual(h.url, 'https://www.tiktok.com/tag/fyp')

    def test_to_dict_validates_against_schema(self) -> None:
        payload: dict = json.loads(_FIXTURE_PATH.read_text())
        h: TikTokHashtag = TikTokHashtag.from_api(
            payload,
            scraped_timestamp=datetime(
                2026, 4, 30, tzinfo=timezone.utc,
            ),
        )
        schema: dict = json.loads(_SCHEMA_PATH.read_text())
        Draft202012Validator(schema).validate(h.to_dict())

    def test_required_fields_enforced(self) -> None:
        with self.assertRaises(ValidationError):
            TikTokHashtag.from_api({}, scraped_timestamp=None)


if __name__ == '__main__':
    unittest.main()
