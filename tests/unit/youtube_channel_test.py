import unittest

from pathlib import Path

import orjson
import jsonschema

from scrape_exchange.youtube.youtube_channel import (
    YouTubeChannel,
    YouTubeThumbnail,
    YouTubeExternalLink,
    YouTubeChannelLink,
)


class TestYouTubeChannel(unittest.TestCase):

    def test_extract_channel_id_success_and_failure(self):
        html = 'some preface "externalId":"UCABC123" and more'
        self.assertEqual(YouTubeChannel.extract_channel_id(html), 'UCABC123')

        with self.assertRaises(ValueError):
            YouTubeChannel.extract_channel_id('')

    def test_extract_verified_status(self) -> None:
        self.assertTrue(
            YouTubeChannel.extract_verified_status('... "tooltip":"Verified" ...')
        )
        self.assertFalse(YouTubeChannel.extract_verified_status('no verified here'))

    def test_parse_nested_dicts(self) -> None:
        data: dict[str, dict[str, dict[str, list[int]]]] = \
            {'a': {'b': {'c': [1, 2, 3]}}}
        res: object | list[object] | None = YouTubeChannel.parse_nested_dicts(
            ['a', 'b', 'c'], data, list
        )
        self.assertEqual(res, [1, 2, 3])
        self.assertIsNone(
            YouTubeChannel.parse_nested_dicts(['x', 'y'], data, dict)
        )

    def test_parse_video_and_subscriber_count(self) -> None:
        # video count
        video_structure: dict = {
            'header': {
                'pageHeaderRenderer': {
                    'content': {
                        'pageHeaderViewModel': {
                            'metadata': {
                                'contentMetadataViewModel': {
                                    'metadataRows': [
                                        {
                                            'metadataParts': [
                                                {
                                                    'text': {
                                                        'content': '123 videos'
                                                    }
                                                }
                                            ]
                                        }
                                    ]
                                }
                            }
                        }
                    }
                }
            }
        }
        self.assertEqual(YouTubeChannel.parse_video_count(video_structure), 123)

        # subscriber count
        subs_structure: dict = {
            'header': {
                'pageHeaderRenderer': {
                    'content': {
                        'pageHeaderViewModel': {
                            'metadata': {
                                'contentMetadataViewModel': {
                                    'metadataRows': [
                                        {
                                            'metadataParts': [
                                                {
                                                    'text': {
                                                        'content': '1.2K subscribers'
                                                    }
                                                }
                                            ]
                                        }
                                    ]
                                }
                            }
                        }
                    }
                }
            }
        }
        self.assertEqual(YouTubeChannel.parse_subscriber_count(subs_structure), 1200)

    def test_parse_view_count_returns_none_for_missing(self) -> None:
        self.assertIsNone(YouTubeChannel.parse_view_count({}))

    def test_parse_thumbnails_https_prefixing(self) -> None:
        data: dict = {
            'header': {
                'pageHeaderRenderer': {
                    'content': {
                        'pageHeaderViewModel': {
                            'image': {
                                'decoratedAvatarViewModel': {
                                    'avatar': {
                                        'avatarViewModel': {
                                            'image': {
                                                'sources': [
                                                    {
                                                        'url': '//example.com/pic.jpg',
                                                        'width': 120,
                                                        'height': 90,
                                                    }
                                                ]
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        thumbnails: set[YouTubeThumbnail] = YouTubeChannel.parse_thumbnails(
            data
        )
        self.assertEqual(len(thumbnails), 1)
        thumb: YouTubeThumbnail = next(iter(thumbnails))
        self.assertTrue(thumb.url.startswith('https://'))

    def test_parse_banners_returns_display_hint(self) -> None:
        data: dict = {
            'header': {
                'pageHeaderRenderer': {
                    'content': {
                        'pageHeaderViewModel': {
                            'banner': {
                                'imageBannerViewModel': {
                                    'image': {
                                        'sources': [
                                            {
                                                'url': 'http://banner.jpg',
                                                'width': 300,
                                                'height': 100,
                                            }
                                        ]
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        banners: set[YouTubeThumbnail] = YouTubeChannel.parse_banners(data)
        self.assertEqual(len(banners), 1)
        banner: YouTubeThumbnail = next(iter(banners))
        self.assertEqual(banner.display_hint, 'banner')

    def test_generate_external_link_and_parse_external_urls(self) -> None:
        # Provide explicit title to avoid external SocialNetworks dependency
        link: YouTubeExternalLink | None = \
            YouTubeChannel._generate_external_link(
                'http://example.com/path', 5, title='Example'
            )
        self.assertIsInstance(link, YouTubeExternalLink)
        self.assertEqual(link.url, 'https://example.com/path')
        self.assertEqual(link.name, 'Example')

        data: list[dict[str, dict[str, dict[str, str]]]] = [
            {
                'channelExternalLinkViewModel': {
                    'title': {'content': 'Site'},
                    'link': {'content': 'http://foo.bar/path'},
                }
            }
        ]
        ext: set[YouTubeExternalLink] = YouTubeChannel.parse_external_urls(
            data
        )
        self.assertEqual(len(ext), 1)
        el: YouTubeExternalLink = next(iter(ext))
        self.assertEqual(el.url, 'https://foo.bar/path')

    def test_extract_linked_channels_parses_grid(self) -> None:
        page_data: dict = {
            'contents': {
                'twoColumnBrowseResultsRenderer': {
                    'tabs': [
                        {},
                    ]
                }
            }
        }

        # construct a tabs[0] with appropriate structure
        page_data['contents']['twoColumnBrowseResultsRenderer']['tabs'] = [
            {
                'tabRenderer': {
                    'content': {
                        'sectionListRenderer': {
                            'contents': [
                                {
                                    'itemSectionRenderer': {
                                        'contents': [
                                            {
                                                'shelfRenderer': {
                                                    'content': {
                                                        'horizontalListRenderer': {
                                                            'items': [
                                                                {
                                                                    'gridChannelRenderer': {
                                                                        'navigationEndpoint': {
                                                                            'commandMetadata': {
                                                                                'webCommandMetadata': {
                                                                                    'url': '/@FeaturedChannel'
                                                                                }
                                                                            }
                                                                        },
                                                                        'subscriberCountText': {
                                                                            'simpleText': '1.2K subscribers'
                                                                        }
                                                                    }
                                                                }
                                                            ]
                                                        }
                                                    }
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
        ]

        links: set[YouTubeChannelLink] = \
            YouTubeChannel.extract_linked_channels(page_data)
        self.assertEqual(len(links), 1)
        link: YouTubeChannelLink = next(iter(links))
        self.assertIsInstance(link, YouTubeChannelLink)
        self.assertEqual(link.channel_name, 'FeaturedChannel')
        self.assertEqual(link.subscriber_count, 1200)

    def test_historymatters_json_matches_schema(self) -> None:
        schema_path = Path(
            'tests/collateral/boinko-youtube-channel-schema.json'
        )
        sample_path = Path(
            'tests/collateral/youtube_channels/HistoryMatters.json'
        )

        schema: dict[str, any] = orjson.loads(schema_path.read_text())
        sample: dict[str, any] = orjson.loads(sample_path.read_text())

        # Will raise jsonschema.ValidationError on failure
        jsonschema.validate(instance=sample, schema=schema)


if __name__ == '__main__':
    unittest.main()
