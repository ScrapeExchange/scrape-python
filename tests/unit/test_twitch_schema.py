'''Validate the output schema against extraction and invalid records.'''

import json
import unittest
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

from scrape_exchange.twitch.twitch_profile_extractor import extract_profile


class TestTwitchSchema(unittest.TestCase):
    def setUp(self) -> None:
        path: Path = (
            Path(__file__).resolve().parents[1] / 'collateral'
            / 'drand-twitch-creator-schema.json'
        )
        schema: dict[str, Any] = json.loads(path.read_text())
        Draft202012Validator.check_schema(schema)
        self.validator: Draft202012Validator = Draft202012Validator(
            schema, format_checker=FormatChecker(),
        )
        self.record: dict[str, Any] = extract_profile([
            {'data': {'user': {
                'login': 'example', 'id': '123', 'displayName': 'Example',
                'description': '', 'profileImageURL': 'https://localhost/a',
                'followers': {'totalCount': 0},
            }}},
        ], '', 'example', 'https://localhost').to_dict()

    def test_url_annotations_have_server_required_uri_format(self) -> None:
        properties: dict[str, Any] = self.validator.schema['properties']
        for field, target in (
            ('url', 'source_url'),
            ('avatar_url', 'platform_creator_thumbnail_url'),
        ):
            with self.subTest(field=field):
                self.assertEqual(properties[field]['x-scrape-field'], target)
                self.assertEqual(properties[field]['type'], 'string')
                self.assertEqual(properties[field].get('format'), 'uri')

    def test_complete_record_requires_core_fields(self) -> None:
        self.assertEqual(self.record['completeness'], 'complete')
        self.validator.validate(self.record)
        for field in (
            'user_id', 'display_name', 'biography', 'avatar_url',
            'follower_count',
        ):
            with self.subTest(field=field):
                record: dict[str, Any] = dict(self.record)
                del record[field]
                self.assertFalse(self.validator.is_valid(record))

    def test_html_fallback_without_id_remains_valid(self) -> None:
        record: dict[str, Any] = extract_profile([], '''
            <link rel="canonical" href="https://localhost/example">
            <h1 data-a-target="user-display-name">Example</h1>
            <span data-a-target="followers-count">1.2K followers</span>
        ''', 'example', 'https://localhost').to_dict()
        self.assertNotIn('user_id', record)
        self.validator.validate(record)
        del record['follower_count']
        self.assertFalse(self.validator.is_valid(record))

    def test_rejects_invalid_values_and_duplicates(self) -> None:
        for field, value in (
            ('username', 'Example'), ('username', 'bad/name'),
            ('username', ''), ('username', 'a' * 26),
            ('sources', []), ('sources', ['unknown']),
            ('sources', ['html', 'html']),
            ('panels', [{}]), ('panels', [{'title': 'a'}] * 2),
            ('social_links', [{'url': ''}]),
            ('social_links', [{'url': 'https://localhost'}] * 2),
            ('extractor_version', 'unknown'),
            ('follower_count', -1), ('follower_count', None),
        ):
            with self.subTest(field=field, value=value):
                self.assertFalse(self.validator.is_valid({
                    **self.record, field: value,
                }))
