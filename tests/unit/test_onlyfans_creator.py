'''Profile semantics and schema contracts for anonymous creator records.'''

import json
import unittest
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker
from pydantic import ValidationError

from scrape_exchange.onlyfans.onlyfans_creator import (
    OnlyFansCreator,
    extract_profile,
    normalize_creator,
)


def public_profile(**overrides: Any) -> dict[str, Any]:
    '''Synthetic profile with unrelated fields to catch wrong mappings.'''
    return {
        'id': 123, 'username': 'example', 'name': 'Example Creator',
        'isVerified': True, 'favoritedCount': 500, 'favoritesCount': 9,
        'photosCount': 20, 'videosCount': 4, 'audiosCount': 0,
        'header': 'https://localhost/banner.jpg',
        'avatar': 'https://localhost/avatar.jpg',
        'about': 'A public biography', 'showSubscribersCount': True,
        'subscribersCount': 100, 'subscribePrice': 12.50,
        'currentSubscribePrice': 0, 'showMediaCount': True,
        **overrides,
    }


class TestOnlyFansCreator(unittest.TestCase):
    def test_requested_fields_and_regular_price(self) -> None:
        creator: OnlyFansCreator = extract_profile(
            public_profile(), 'example',
        )
        self.assertEqual(creator.username, 'example')
        self.assertEqual(creator.handle, '@example')
        self.assertEqual(creator.display_name, 'Example Creator')
        self.assertTrue(creator.verified)
        self.assertEqual(creator.like_count, 500)
        self.assertEqual(creator.photo_count, 20)
        self.assertEqual(creator.video_count, 4)
        self.assertEqual(creator.audio_count, 0)
        self.assertEqual(creator.fan_count, 100)
        self.assertEqual(creator.biography, 'A public biography')
        self.assertEqual(creator.banner_url, 'https://localhost/banner.jpg')
        self.assertEqual(creator.avatar_url, 'https://localhost/avatar.jpg')
        self.assertEqual(creator.subscription_price, 12.5)
        self.assertEqual(creator.subscription_status, 'paid')
        self.assertEqual(creator.subscription_currency, 'USD')

    def test_fans_require_explicit_visibility_opt_in(self) -> None:
        visibility: object
        for visibility in (False, None, 'true', 1):
            with self.subTest(visibility=visibility):
                creator: OnlyFansCreator = extract_profile(
                    public_profile(showSubscribersCount=visibility),
                    'example',
                )
                self.assertIsNone(creator.fan_count)
        payload: dict[str, Any] = public_profile()
        del payload['showSubscribersCount']
        self.assertIsNone(extract_profile(payload, 'example').fan_count)

    def test_missing_is_unknown_and_zero_is_preserved(self) -> None:
        creator: OnlyFansCreator = extract_profile(
            {'id': 123, 'username': 'example'}, 'example',
        )
        self.assertIsNone(creator.verified)
        self.assertIsNone(creator.like_count)
        self.assertIsNone(creator.subscription_status)
        self.assertIsNone(creator.subscription_price)
        self.assertIsNone(creator.to_dict()['fan_count'])
        creator = extract_profile(public_profile(
            subscribePrice=0, subscribersCount=0, isVerified=False,
            favoritedCount=0, about='',
        ), 'example')
        self.assertEqual(creator.subscription_status, 'free')
        self.assertEqual(creator.fan_count, 0)
        self.assertEqual(creator.like_count, 0)
        self.assertFalse(creator.verified)
        self.assertEqual(creator.biography, '')

    def test_hidden_media_counts_are_unknown(self) -> None:
        creator: OnlyFansCreator = extract_profile(
            public_profile(showMediaCount=False), 'example',
        )
        self.assertIsNone(creator.photo_count)
        self.assertIsNone(creator.video_count)
        self.assertIsNone(creator.audio_count)

    def test_rejects_wrong_identity_or_non_profile(self) -> None:
        payload: object
        for payload in ({}, [], {'error': {'code': 401}}, public_profile(
            username='someone_else',
        )):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                extract_profile(payload, 'example')

    def test_rejects_invalid_counts_prices_and_verification(self) -> None:
        field: str
        value: object
        for field, value in (
            ('favoritedCount', -1), ('photosCount', True),
            ('videosCount', 1.5), ('subscribePrice', -1),
            ('subscribePrice', float('nan')), ('subscribePrice', True),
            ('isVerified', 'false'),
        ):
            with (
                self.subTest(field=field, value=value),
                self.assertRaises(ValueError),
            ):
                extract_profile(public_profile(**{field: value}), 'example')

    def test_normalization_rejects_paths_and_unrelated_urls(self) -> None:
        self.assertEqual(normalize_creator(' @Example '), 'example')
        self.assertEqual(normalize_creator(
            f'{extract_profile(public_profile(), "example").url}?ref=x',
        ), 'example')
        value: str
        for value in ('../x', 'a/b', '', '@@example', 'has space',
                      'https://localhost/example', 'example#fragment'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                normalize_creator(value)

    def test_schema_validates_output_and_rejects_bad_data(self) -> None:
        path: Path = Path('tests/collateral/drand-onlyfans-creator-schema.json')
        schema: dict[str, Any] = json.loads(path.read_text())
        Draft202012Validator.check_schema(schema)
        validator: Draft202012Validator = Draft202012Validator(
            schema, format_checker=FormatChecker(),
        )
        record: dict[str, Any] = extract_profile(
            public_profile(), 'example',
        ).to_dict()
        validator.validate(record)
        validator.validate(extract_profile(
            {'id': 123, 'username': 'example'}, 'example',
        ).to_dict())
        field: str
        value: object
        for field, value in (
            ('like_count', -1), ('photo_count', True),
            ('subscription_status', 'trial'), ('unexpected', 1),
            ('scraped_timestamp', 'yesterday'),
            ('avatar_url', 'not a URL'),
        ):
            with self.subTest(field=field):
                self.assertFalse(validator.is_valid({**record, field: value}))
        with self.assertRaises(ValidationError):
            OnlyFansCreator.model_validate({**record, 'photo_count': -1})
        for changes in (
            {'fan_count_visible': False},
            {'subscription_price': 0},
            {'subscription_price': None},
            {'subscription_status': 'free'},
        ):
            with self.subTest(changes=changes):
                self.assertFalse(validator.is_valid({**record, **changes}))

    def test_nullable_annotations_use_api_compatible_property_types(
        self,
    ) -> None:
        path: Path = Path('tests/collateral/drand-onlyfans-creator-schema.json')
        stored: dict[str, Any] = json.loads(path.read_text())
        generated: dict[str, Any] = OnlyFansCreator.model_json_schema(
            mode='serialization',
        )
        self.assertEqual(stored['properties'], generated['properties'])
        schema: dict[str, Any]
        for schema in (stored, generated):
            avatar: dict[str, Any] = schema['properties']['avatar_url']
            self.assertEqual(avatar.get('type'), ['string', 'null'])
            self.assertEqual(avatar.get('format'), 'uri')
            self.assertEqual(avatar['pattern'], r'^https?://')
            prop: dict[str, Any]
            for prop in schema['properties'].values():
                if prop.get('x-scrape-gauge'):
                    self.assertEqual(prop.get('type'), ['integer', 'null'])
                    self.assertEqual(prop.get('minimum'), 0)
