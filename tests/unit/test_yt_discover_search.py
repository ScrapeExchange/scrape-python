import unittest

from tools.yt_discover_search import (
    DiscoverSearchSettings,
    DiscoveredChannel,
    _dedupe_channels,
    _get_continuation_token,
    _extract_words_from_random_payload,
    _normalise_handle,
    extract_channels,
)


class TestDiscoverSearchSettings(unittest.TestCase):
    def test_cli_positional_search_terms(self) -> None:
        settings = DiscoverSearchSettings(
            _cli_parse_args=['special', 'topic'],
        )

        self.assertEqual(
            settings.search_terms,
            ['special', 'topic'],
        )

    def test_cli_settings_flags(self) -> None:
        settings = DiscoverSearchSettings(
            _cli_parse_args=[
                '--youtube-search-continuations', '3',
                '--random-word-language', 'de',
                '--keyword-count', '4',
            ],
        )

        self.assertEqual(settings.youtube_search_continuations, 3)
        self.assertEqual(settings.random_word_language, 'de')
        self.assertEqual(settings.keyword_count, 4)

    def test_keyword_count_defaults_to_one(self) -> None:
        settings = DiscoverSearchSettings(_cli_parse_args=[])

        self.assertEqual(settings.keyword_count, 1)


class TestRandomWordPayload(unittest.TestCase):
    def test_extracts_list_of_string_words(self) -> None:
        self.assertEqual(
            _extract_words_from_random_payload(['alpha', 'bravo']),
            ['alpha', 'bravo'],
        )

    def test_extracts_dict_word_values(self) -> None:
        self.assertEqual(
            _extract_words_from_random_payload([
                {'word': 'alpha'},
                {'word': 'bravo'},
            ]),
            ['alpha', 'bravo'],
        )


class TestNormaliseHandle(unittest.TestCase):
    def test_keeps_at_handle(self) -> None:
        self.assertEqual(_normalise_handle('@HistoryMatters'), '@HistoryMatters')

    def test_extracts_handle_from_path(self) -> None:
        self.assertEqual(
            _normalise_handle('/@HistoryMatters/videos'),
            '@HistoryMatters',
        )

    def test_rejects_channel_path(self) -> None:
        self.assertIsNone(
            _normalise_handle('/channel/UC22BdTgxefuvUivrjesETjg'),
        )


class TestExtractChannels(unittest.TestCase):
    def test_extracts_channel_renderer_id_and_handle(self) -> None:
        payload = {
            'contents': {
                'twoColumnSearchResultsRenderer': {
                    'primaryContents': {
                        'sectionListRenderer': {
                            'contents': [
                                {
                                    'itemSectionRenderer': {
                                        'contents': [
                                            {
                                                'channelRenderer': {
                                                    'channelId': (
                                                        'UC22BdTgxefuvUivrjesETjg'
                                                    ),
                                                    'navigationEndpoint': {
                                                        'browseEndpoint': {
                                                            'browseId': (
                                                                'UC22BdTgxefuvUivrjesETjg'
                                                            ),
                                                            'canonicalBaseUrl': (
                                                                '/@HistoryMatters'
                                                            ),
                                                        }
                                                    },
                                                }
                                            }
                                        ]
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        }

        self.assertEqual(
            extract_channels(payload),
            [
                DiscoveredChannel(
                    'UC22BdTgxefuvUivrjesETjg',
                    '@HistoryMatters',
                )
            ],
        )

    def test_extracts_owner_browse_endpoint_from_video_result(self) -> None:
        payload = {
            'videoRenderer': {
                'ownerText': {
                    'runs': [
                        {
                            'navigationEndpoint': {
                                'browseEndpoint': {
                                    'browseId': (
                                        'UCuAXFkgsw1L7xaCfnd5JJOw'
                                    ),
                                    'canonicalBaseUrl': (
                                        '/@RickAstleyYT'
                                    ),
                                }
                            }
                        }
                    ]
                }
            }
        }

        self.assertEqual(
            extract_channels(payload),
            [
                DiscoveredChannel(
                    'UCuAXFkgsw1L7xaCfnd5JJOw',
                    '@RickAstleyYT',
                )
            ],
        )

    def test_extracts_continuation_token(self) -> None:
        payload = {
            'continuationItemRenderer': {
                'continuationEndpoint': {
                    'continuationCommand': {'token': 'tok123'}
                }
            }
        }

        self.assertEqual(_get_continuation_token(payload), 'tok123')


class TestDedupeChannels(unittest.TestCase):
    def test_id_record_replaces_handle_only_record(self) -> None:
        channels = _dedupe_channels([
            DiscoveredChannel(None, '@HistoryMatters'),
            DiscoveredChannel(
                'UC22BdTgxefuvUivrjesETjg',
                '@HistoryMatters',
            ),
        ])

        self.assertEqual(
            channels,
            [
                DiscoveredChannel(
                    'UC22BdTgxefuvUivrjesETjg',
                    '@HistoryMatters',
                )
            ],
        )


if __name__ == '__main__':
    unittest.main()
