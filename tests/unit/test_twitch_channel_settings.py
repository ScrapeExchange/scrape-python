'''Channel configuration must remain distinct from broadcast metadata.'''

import unittest

from scrape_exchange.twitch.twitch_browser import select_profile_responses
from scrape_exchange.twitch.twitch_creator import TwitchCreator
from scrape_exchange.twitch.twitch_profile_extractor import extract_profile


class TestChannelSettings(unittest.TestCase):
    def test_merge_settings_from_matching_users(self) -> None:
        creator: TwitchCreator = extract_profile([
            {'data': {'userOrError': {
                'id': '123', 'login': 'example',
                'primaryColorHex': 'Ab12Cd', '__typename': 'User',
            }}},
            {'data': {'user': {
                'id': '123', '__typename': 'User',
                'broadcastSettings': {'id': '123', 'title': ''},
            }}},
            {'data': {'user': {
                'id': '123', '__typename': 'User',
                'broadcastSettings': {
                    'id': '123', 'language': 'EN',
                    'game': {'id': '42', 'name': 'Example category'},
                    'isMature': True,
                    'contentClassificationLabels': ['Gambling'],
                    'isBrandedContent': True,
                },
                'stream': {'title': 'Wrong title', 'language': 'FR'},
                'lastBroadcast': {'game': {'id': '99', 'name': 'Old'}},
            }}},
            {'data': {'user': {
                'id': '999', 'login': 'other',
                'broadcastSettings': {'title': 'Other creator'},
            }}},
        ], '', 'example', 'https://localhost')
        self.assertEqual(creator.to_dict()['channel_settings'], {
            'title': '', 'category_id': '42',
            'category_name': 'Example category', 'language': 'en',
            'primary_color_hex': 'AB12CD',
        })
        self.assertEqual(TwitchCreator.model_validate(creator.to_dict()),
                         creator)

    def test_broadcast_metadata_does_not_become_channel_settings(self) -> None:
        creator: TwitchCreator = extract_profile([
            {'data': {'user': {
                'id': '123', 'login': 'example',
                'stream': {'title': 'Live', 'language': 'EN',
                           'tags': ['LiveTag']},
                'lastBroadcast': {'title': 'Past'},
                'videos': [{'id': '123', '__typename': 'Video',
                            'broadcastSettings': {'title': 'Recorded'}}],
            }}},
        ], '', 'example', 'https://localhost')
        self.assertNotIn('channel_settings', creator.to_dict())

    def test_malformed_or_mismatched_settings_are_omitted(self) -> None:
        for settings in (
            {'id': '999', 'title': 'Wrong owner'},
            {'id': '123', 'title': 7, 'language': None, 'game': []},
        ):
            with self.subTest(settings=settings):
                creator: TwitchCreator = extract_profile([
                    {'data': {'user': {
                        'id': '123', 'login': 'example',
                        'primaryColorHex': 'invalid',
                        'broadcastSettings': settings,
                    }}},
                ], '', 'example', 'https://localhost')
                self.assertNotIn('channel_settings', creator.to_dict())

    def test_website_settings_operations_are_selected_by_owner(self) -> None:
        for operation, variable in (
            ('HomeTrackQuery', 'channelLogin'),
            ('VideoPlayerMediaSessionManager', 'channel'),
            ('PlayerTrackingContextQuery', 'channel'),
        ):
            request: dict = {
                'operationName': operation, 'variables': {variable: 'example'},
            }
            response: dict = {'data': {'user': {'id': '123'}}}
            with self.subTest(operation=operation):
                self.assertEqual(select_profile_responses(
                    request, response, 'example',
                ), [response])
                self.assertEqual(select_profile_responses(
                    request, response, 'other',
                ), [])
                request['variables']['hasVideo'] = True
                self.assertEqual(select_profile_responses(
                    request, response, 'example',
                ), [])
