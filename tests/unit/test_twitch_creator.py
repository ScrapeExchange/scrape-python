'''Twitch identity and extraction contracts.'''

import unittest

from scrape_exchange.twitch.normalization import normalize_creator
from scrape_exchange.twitch.twitch_creator import TwitchCreator
from scrape_exchange.twitch.twitch_error_classification import (
    ProfileExtractionError,
    ProfileIdentityError,
)
from scrape_exchange.twitch.twitch_profile_extractor import extract_profile

BASE: str = 'https://localhost'


class TestTwitchCreator(unittest.TestCase):
    def test_normalizes_only_profile_routes(self) -> None:
        for value in ('@Example', 'Example', f'{BASE}/Example/about?x=1'):
            with self.subTest(value=value):
                self.assertEqual(normalize_creator(value, BASE), 'example')
        for value in (
            'bad/name', 'videos', f'{BASE}/videos/123',
            f'{BASE}/example/clips', 'https://scrape.exchange/example',
            f'{BASE}@scrape.exchange/example', '', '../escape',
        ):
            with self.subTest(value=value):
                self.assertIsNone(normalize_creator(value, BASE))

    def test_selects_requested_user_and_keeps_zero_followers(self) -> None:
        payloads: list[object] = [
            {'data': {'recommended': {'login': 'other', 'id': '999'}}},
            {'data': {'user': {
                'id': '123', 'login': 'example', 'displayName': 'Example',
                'description': '', 'followers': {'totalCount': 0},
                'profileImageURL': 'https://files.scrape.exchange/a.png',
            }}},
        ]
        creator: TwitchCreator = extract_profile(
            payloads, '', 'example', BASE,
        )
        self.assertEqual(creator.user_id, '123')
        self.assertEqual(creator.follower_count, 0)
        self.assertEqual(creator.biography, '')
        self.assertFalse(creator.follower_count_is_approximate)

    def test_unknown_counts_and_badges_remain_unknown(self) -> None:
        creator: TwitchCreator = extract_profile(
            [{'data': {'user': {'login': 'example', 'id': '123'}}}],
            '', 'example', BASE,
        )
        self.assertIsNone(creator.follower_count)
        self.assertIsNone(creator.partner)
        self.assertEqual(creator.completeness, 'partial')
        self.assertNotIn('follower_count', creator.to_dict())

    def test_combines_split_user_responses(self) -> None:
        creator: TwitchCreator = extract_profile([
            {'data': {'user': {'login': 'example', 'id': '123'}}},
            {'data': {'user': {'id': '123', 'followers': {
                'totalCount': 42,
            }, 'roles': {'isPartner': False}}}},
        ], '', 'example', BASE)
        self.assertEqual(creator.follower_count, 42)
        self.assertFalse(creator.partner)

    def test_conflicting_identity_is_not_merged(self) -> None:
        with self.assertRaises(ProfileIdentityError):
            extract_profile([
                {'data': {'user': {'login': 'example', 'id': '123'}}},
                {'data': {'user': {'login': 'example', 'id': '456'}}},
            ], '', 'example', BASE)

    def test_nested_channel_and_non_user_ids_do_not_pollute_profile(
        self,
    ) -> None:
        creator: TwitchCreator = extract_profile([
            {'data': {'user': {
                'id': '123', 'login': 'example', '__typename': 'User',
                'channel': {'id': '123', '__typename': 'Channel',
                    'socialMedias': [{
                        'url': 'https://scrape.exchange', 'title': 'Site',
                    }]},
            }}},
            {'data': {'video': {
                'id': '123', '__typename': 'Video',
                'description': 'Not the biography',
            }}},
        ], '', 'example', BASE)
        self.assertIsNone(creator.biography)
        self.assertEqual(creator.social_links[0].title, 'Site')

    def test_unrelated_user_and_generic_page_are_not_success(self) -> None:
        with self.assertRaises(ProfileExtractionError):
            extract_profile([
                {'data': {'user': {'login': 'other', 'id': '999'}}},
            ], '<title>Welcome</title>', 'example', BASE)

    def test_meta_requires_matching_canonical_identity(self) -> None:
        html: str = f'''
        <meta property="og:url" content="{BASE}/example">
        <meta property="og:title" content="Example - Twitch">
        <meta property="og:description" content="Hello world">
        <meta property="og:image"
              content="https://files.scrape.exchange/a.png">
        '''
        creator: TwitchCreator = extract_profile([], html, 'example', BASE)
        self.assertEqual(creator.display_name, 'Example')
        self.assertEqual(creator.biography, 'Hello world')
        self.assertIsNone(creator.follower_count)
        with self.assertRaises(ProfileExtractionError):
            extract_profile([], html, 'different', BASE)

    def test_dom_counts_are_explicitly_approximate(self) -> None:
        html: str = f'''
        <link rel="canonical" href="{BASE}/example">
        <h1 data-a-target="user-display-name">Example</h1>
        <span data-a-target="followers-count">1.2K followers</span>
        '''
        creator: TwitchCreator = extract_profile([], html, 'example', BASE)
        self.assertEqual(creator.follower_count, 1200)
        self.assertTrue(creator.follower_count_is_approximate)

    def test_about_panel_dom_matches_observed_website_layout(self) -> None:
        creator: TwitchCreator = extract_profile([], f'''
            <link rel="canonical" href="{BASE}/example/about">
            <div data-a-target="about-panel">
                <h3>About Example</h3>
                <div><span>2.5M</span> followers</div>
                <p dir="auto">A public biography</p>
                <a class="social-media-link__link"
                   href="https://scrape.exchange">Website</a>
            </div>
        ''', 'example', BASE)
        self.assertEqual(creator.display_name, 'Example')
        self.assertEqual(creator.follower_count, 2500000)
        self.assertTrue(creator.follower_count_is_approximate)


    def test_serialization_round_trip(self) -> None:
        creator: TwitchCreator = extract_profile([
            {'data': {'user': {'login': 'example', 'id': '123'}}},
        ], '', 'example', BASE)
        self.assertEqual(
            TwitchCreator.model_validate(creator.to_dict()), creator,
        )
