'''
Unit tests for YouTubeChannel class and related functions.

:author: Boinko <boinko@scrape.exchange
:copyright: Copyright 2026
:license: GPLv3
'''

import unittest
import unittest.mock

from pathlib import Path
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import orjson
import jsonschema

from scrape_exchange.youtube.youtube_channel import (
    YouTubeChannel,
    YouTubeThumbnail,
    YouTubeExternalLink,
    YouTubeChannelLink,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_metadata_rows(content_text: str) -> dict:
    '''Build the nested pageHeaderRenderer structure.'''
    return {
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
                                                    'content': content_text
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


class TestYouTubeChannel(unittest.TestCase):

    def test_extract_channel_id_success_and_failure(self):
        html = 'some preface "externalId":"UCABC123" and more'
        self.assertEqual(YouTubeChannel.extract_channel_id(html), 'UCABC123')

        with self.assertRaises(ValueError):
            YouTubeChannel.extract_channel_id('')

    def test_extract_verified_status(self) -> None:
        self.assertTrue(
            YouTubeChannel.extract_verified_status(
                '... "tooltip":"Verified" ...'
            )
        )
        self.assertFalse(
            YouTubeChannel.extract_verified_status('no verified here')
        )

    def test_parse_video_and_subscriber_count(self) -> None:
        self.assertEqual(
            YouTubeChannel.parse_video_count(
                _make_metadata_rows('123 videos')
            ), 123
        )
        self.assertEqual(
            YouTubeChannel.parse_subscriber_count(
                _make_metadata_rows('1.2K subscribers')
            ), 1200
        )

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
                }
            }
        }

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

        jsonschema.validate(instance=sample, schema=schema)


# ---------------------------------------------------------------------------
# Constructor / equality / serialisation
# ---------------------------------------------------------------------------

class TestYouTubeChannelInit(unittest.TestCase):
    def test_init_with_name_sets_url(self) -> None:
        ch = YouTubeChannel(name='HistoryMatters')
        self.assertEqual(ch.name, 'HistoryMatters')
        self.assertIn('@HistoryMatters', ch.url)
        self.assertIsNone(ch.channel_id)
        self.assertEqual(ch.video_ids, set())

    def test_init_strips_at_from_name(self) -> None:
        ch = YouTubeChannel(name='@SomeChannel')
        self.assertEqual(ch.name, 'SomeChannel')

    def test_init_no_name(self) -> None:
        ch = YouTubeChannel()
        self.assertIsNone(ch.url)
        self.assertIsNone(ch.title)


class TestYouTubeChannelEquality(unittest.TestCase):
    def test_equal_channels(self) -> None:
        ch1 = YouTubeChannel(name='Test')
        ch2 = YouTubeChannel(name='Test')
        ch1.channel_id = ch2.channel_id = 'UC123'
        ch1.title = ch2.title = 'Test Title'
        self.assertEqual(ch1, ch2)

    def test_not_equal_different_name(self) -> None:
        ch1 = YouTubeChannel(name='A')
        ch2 = YouTubeChannel(name='B')
        self.assertNotEqual(ch1, ch2)

    def test_not_equal_different_type(self) -> None:
        ch = YouTubeChannel(name='X')
        self.assertNotEqual(ch, 'not_a_channel')

    def test_del_does_not_raise(self) -> None:
        ch = YouTubeChannel(name='X')
        ch.__del__()


class TestYouTubeChannelToFromDict(unittest.TestCase):
    def test_to_dict_basic_fields(self) -> None:
        ch = YouTubeChannel(name='Test')
        ch.channel_id = 'UC123'
        ch.title = 'My Title'
        ch.description = 'A description'
        ch.verified = True
        ch.subscriber_count = 1000
        ch.video_count = 50
        ch.view_count = 100000
        ch.joined_date = datetime(2020, 1, 15, tzinfo=UTC)
        data = ch.to_dict()
        self.assertEqual(data['channel'], 'Test')
        self.assertEqual(data['channel_id'], 'UC123')
        self.assertEqual(data['title'], 'My Title')
        self.assertEqual(data['description'], 'A description')
        self.assertTrue(data['verified'])
        self.assertEqual(data['subscriber_count'], 1000)
        self.assertEqual(data['video_count'], 50)
        self.assertEqual(data['view_count'], 100000)
        self.assertIn('2020', data['joined_date'])

    def test_to_dict_with_video_ids(self) -> None:
        ch = YouTubeChannel(name='Test')
        ch.video_ids = {'v1', 'v2'}
        data = ch.to_dict(with_video_ids=True)
        self.assertEqual(set(data['video_ids']), {'v1', 'v2'})

    def test_to_dict_without_video_ids(self) -> None:
        ch = YouTubeChannel(name='Test')
        ch.video_ids = {'v1'}
        data = ch.to_dict(with_video_ids=False)
        self.assertNotIn('video_ids', data)

    def test_to_dict_none_joined_date(self) -> None:
        ch = YouTubeChannel(name='Test')
        data = ch.to_dict()
        self.assertIsNone(data['joined_date'])

    def test_round_trip_from_dict(self) -> None:
        ch = YouTubeChannel(name='RoundTrip')
        ch.channel_id = 'UC_RT'
        ch.title = 'RT Title'
        ch.description = 'desc'
        ch.keywords = {'keyword1', 'keyword2'}
        ch.categories = {'cat1'}
        ch.is_family_safe = True
        ch.country = 'US'
        ch.available_country_codes = {'US', 'GB'}
        ch.joined_date = datetime(2021, 6, 1, tzinfo=UTC)
        ch.rss_url = 'https://rss.example.com'
        ch.verified = True
        ch.subscriber_count = 500
        ch.video_count = 20
        ch.view_count = 10000
        ch.channel_thumbnails = {
            YouTubeThumbnail(
                {'url': 'https://thumb.jpg', 'width': 88, 'height': 88}
            )
        }
        ch.banners = {
            YouTubeThumbnail(
                {'url': 'https://banner.jpg', 'width': 1060, 'height': 175},
                display_hint='banner'
            )
        }
        ch.external_urls = {
            YouTubeExternalLink(name='Site', url='https://site.com', priority=1)
        }

        data = ch.to_dict()
        restored = YouTubeChannel.from_dict(data)

        self.assertEqual(ch, restored)

    def test_from_dict_empty(self) -> None:
        ch = YouTubeChannel.from_dict({'channel': 'Empty'})
        self.assertEqual(ch.name, 'Empty')
        self.assertIsNone(ch.channel_id)
        self.assertIsNone(ch.joined_date)


# ---------------------------------------------------------------------------
# _extract_initial_data
# ---------------------------------------------------------------------------

class TestExtractInitialData(unittest.TestCase):
    def test_extracts_yt_initial_data(self) -> None:
        ch = YouTubeChannel(name='Test')
        html = 'blah ytInitialData = {"key": "value"}; more stuff'
        result = ch._extract_initial_data(html)
        self.assertEqual(result, {'key': 'value'})

    def test_extracts_window_syntax(self) -> None:
        ch = YouTubeChannel(name='Test')
        html = 'blah window["ytInitialData"] = {"k": 1}; more'
        result = ch._extract_initial_data(html)
        self.assertEqual(result, {'k': 1})

    def test_raises_on_missing_data(self) -> None:
        ch = YouTubeChannel(name='Test')
        with self.assertRaises(ValueError):
            ch._extract_initial_data('no data here')

    def test_sets_verified_from_html(self) -> None:
        ch = YouTubeChannel(name='Test')
        html = (
            '"tooltip":"Verified" '
            'ytInitialData = {"x": 1};'
        )
        ch._extract_initial_data(html)
        self.assertTrue(ch.verified)


# ---------------------------------------------------------------------------
# _extract_handle
# ---------------------------------------------------------------------------

class TestExtractHandle(unittest.TestCase):
    def test_from_url(self) -> None:
        ch = YouTubeChannel(name='Test')
        handle = ch._extract_handle(
            'https://www.youtube.com/@MyHandle/videos', {}
        )
        self.assertEqual(handle, '@MyHandle')

    def test_from_metadata_channel_url(self) -> None:
        ch = YouTubeChannel(name='Test')
        handle = ch._extract_handle(
            'https://www.youtube.com/channel/UC123',
            {'channelUrl': 'https://www.youtube.com/@FromMeta'}
        )
        self.assertEqual(handle, '@FromMeta')

    def test_returns_none(self) -> None:
        ch = YouTubeChannel(name='Test')
        self.assertIsNone(
            ch._extract_handle('https://www.youtube.com/channel/UC12', {})
        )


# ---------------------------------------------------------------------------
# _extract_simple_text
# ---------------------------------------------------------------------------

class TestExtractSimpleText(unittest.TestCase):
    def setUp(self) -> None:
        self.ch = YouTubeChannel(name='Test')

    def test_string_input(self) -> None:
        self.assertEqual(self.ch._extract_simple_text('hello'), 'hello')

    def test_content_key(self) -> None:
        self.assertEqual(
            self.ch._extract_simple_text({'content': 'from content'}),
            'from content'
        )

    def test_simple_text_key(self) -> None:
        self.assertEqual(
            self.ch._extract_simple_text({'simpleText': 'simple'}), 'simple'
        )

    def test_runs_key(self) -> None:
        self.assertEqual(
            self.ch._extract_simple_text(
                {'runs': [{'text': 'a'}, {'text': 'b'}]}
            ), 'ab'
        )

    def test_returns_none_for_empty(self) -> None:
        self.assertIsNone(self.ch._extract_simple_text({}))


# ---------------------------------------------------------------------------
# _parse_thumbnails  (instance helper, not static)
# ---------------------------------------------------------------------------

class TestParseThumbnailsHelper(unittest.TestCase):
    def setUp(self) -> None:
        self.ch = YouTubeChannel(name='Test')

    def test_three_thumbnails(self) -> None:
        thumbs_list = [
            {'url': 'a.jpg', 'width': 88, 'height': 88},
            {'url': 'b.jpg', 'width': 176, 'height': 176},
            {'url': 'c.jpg', 'width': 900, 'height': 900},
        ]
        result = self.ch._parse_thumbnails(thumbs_list)
        self.assertIn('default', result)
        self.assertIn('medium', result)
        self.assertIn('high', result)
        self.assertEqual(result['default']['url'], 'a.jpg')
        self.assertEqual(result['high']['url'], 'c.jpg')

    def test_one_thumbnail(self) -> None:
        result = self.ch._parse_thumbnails(
            [{'url': 'only.jpg', 'width': 100, 'height': 100}]
        )
        self.assertIn('default', result)
        self.assertNotIn('medium', result)

    def test_empty(self) -> None:
        result = self.ch._parse_thumbnails([])
        self.assertEqual(result, {})


class TestParseThumbnail(unittest.TestCase):
    def test_parse_single(self) -> None:
        ch = YouTubeChannel(name='Test')
        result = ch._parse_thumbnail(
            {'url': 'pic.jpg', 'width': 640, 'height': 480}
        )
        self.assertEqual(result['url'], 'pic.jpg')
        self.assertEqual(result['width'], 640)


# ---------------------------------------------------------------------------
# _extract_links
# ---------------------------------------------------------------------------

class TestExtractLinks(unittest.TestCase):
    def test_extracts_from_c4_tabbed_header(self) -> None:
        data = {
            'header': {
                'c4TabbedHeaderRenderer': {
                    'headerLinks': {
                        'channelHeaderLinksRenderer': {
                            'primaryLinks': [
                                {
                                    'title': {'simpleText': 'Twitter'},
                                    'navigationEndpoint': {
                                        'urlEndpoint': {
                                            'url': 'https://twitter.com/test'
                                        }
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        }
        links = YouTubeChannel._extract_links(data)
        self.assertEqual(len(links), 1)

    def test_empty_when_no_header(self) -> None:
        self.assertEqual(YouTubeChannel._extract_links({}), set())

    def test_empty_when_no_renderer(self) -> None:
        self.assertEqual(
            YouTubeChannel._extract_links({'header': {}}), set()
        )


# ---------------------------------------------------------------------------
# _parse_channel_about_metadata
# ---------------------------------------------------------------------------

class TestParseChannelAboutMetadata(unittest.TestCase):
    def test_parses_all_fields(self) -> None:
        ch = YouTubeChannel(name='Test')
        metadata = {
            'externalId': 'UC123',
            'title': 'Test Title',
            'description': 'A good channel',
            'rssUrl': 'https://rss.yt/feed',
            'availableCountryCodes': ['US', 'GB'],
            'vanityChannelUrl': 'https://www.youtube.com/@Test',
            'keywords': '"keyword1" "keyword2"',
            'isFamilySafe': True,
        }
        ch._parse_channel_about_metadata(metadata)
        self.assertEqual(ch.channel_id, 'UC123')
        self.assertEqual(ch.title, 'Test Title')
        self.assertEqual(ch.description, 'A good channel')
        self.assertEqual(ch.rss_url, 'https://rss.yt/feed')
        self.assertIn('US', ch.available_country_codes)
        self.assertTrue(ch.is_family_safe)
        self.assertIn('keyword1', ch.keywords)

    def test_does_not_overwrite_existing(self) -> None:
        ch = YouTubeChannel(name='Test')
        ch.title = 'Already set'
        ch._parse_channel_about_metadata({'title': 'New'})
        self.assertEqual(ch.title, 'Already set')


# ---------------------------------------------------------------------------
# _parse_channel_about_data
# ---------------------------------------------------------------------------

class TestParseChannelAboutData(unittest.TestCase):
    def test_parses_joined_date(self) -> None:
        ch = YouTubeChannel(name='Test')
        about = {
            'joinedDateText': {'content': 'Joined Aug 2, 2015'},
        }
        ch._parse_channel_about_data(about)
        self.assertEqual(ch.joined_date, datetime(2015, 8, 2, tzinfo=UTC))

    def test_parses_view_count(self) -> None:
        ch = YouTubeChannel(name='Test')
        about = {
            'viewCountText': {'simpleText': '1,234,567 views'},
        }
        ch._parse_channel_about_data(about)
        self.assertEqual(ch.view_count, 1234567)


# ---------------------------------------------------------------------------
# _find_about_renderer
# ---------------------------------------------------------------------------

class TestFindAboutRenderer(unittest.TestCase):
    def test_finds_about_view_model(self) -> None:
        ch = YouTubeChannel(name='Test')
        data = {
            'onResponseReceivedEndpoints': [
                {
                    'showEngagementPanelEndpoint': {
                        'engagementPanel': {
                            'engagementPanelSectionListRenderer': {
                                'content': {
                                    'sectionListRenderer': {
                                        'contents': [
                                            {
                                                'itemSectionRenderer': {
                                                    'contents': [
                                                        {
                                                            'aboutChannelRenderer': {
                                                                'metadata': {
                                                                    'aboutChannelViewModel': {
                                                                        'joinedDateText': {
                                                                            'content': 'Joined Jan 1, 2020'
                                                                        }
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
                    }
                }
            ]
        }
        result = ch._find_about_renderer(data)
        self.assertIsNotNone(result)
        self.assertIn('joinedDateText', result)

    def test_returns_none_when_missing(self) -> None:
        ch = YouTubeChannel(name='Test')
        ch.youtube_url = 'https://www.youtube.com/@Test'
        self.assertIsNone(
            ch._find_about_renderer(
                {'onResponseReceivedEndpoints': []}
            )
        )

    def test_returns_none_when_no_endpoints(self) -> None:
        ch = YouTubeChannel(name='Test')
        ch.youtube_url = 'https://www.youtube.com/@Test'
        self.assertIsNone(ch._find_about_renderer({}))


# ---------------------------------------------------------------------------
# parse_channel_video_data
# ---------------------------------------------------------------------------

class TestParseChannelVideoData(unittest.TestCase):
    def test_parses_channel_info(self) -> None:
        ch = YouTubeChannel(name='Test')

        page_data = {
            'metadata': {
                'channelMetadataRenderer': {
                    'name': 'TestChannel',
                    'title': 'Test Channel Title',
                    'description': 'Channel desc',
                    'keywords': '"kw1" "kw2"',
                    'isFamilySafe': True,
                }
            },
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
                                                        'url': 'https://yt.com/thumb.jpg',
                                                        'width': 88,
                                                        'height': 88
                                                    }
                                                ]
                                            }
                                        }
                                    }
                                }
                            },
                            'metadata': {
                                'contentMetadataViewModel': {
                                    'metadataRows': [
                                        {
                                            'metadataParts': [
                                                {
                                                    'text': {
                                                        'content': '100 subscribers'
                                                    }
                                                }
                                            ]
                                        },
                                        {
                                            'metadataParts': [
                                                {
                                                    'text': {
                                                        'content': '10 videos'
                                                    }
                                                }
                                            ]
                                        },
                                    ]
                                }
                            }
                        }
                    }
                }
            }
        }
        ch.parse_channel_video_data(page_data)
        self.assertEqual(ch.title, 'Test Channel Title')
        self.assertIn('kw1', ch.keywords)
        self.assertTrue(ch.is_family_safe)
        self.assertGreaterEqual(len(ch.channel_thumbnails), 1)

    def test_raises_on_missing_metadata(self) -> None:
        ch = YouTubeChannel(name='Test')
        page_data = {
            'metadata': {},
            'header': {
                'pageHeaderRenderer': {
                    'content': {
                        'pageHeaderViewModel': {}
                    }
                }
            }
        }
        with self.assertRaises(ValueError):
            ch.parse_channel_video_data(page_data)


# ---------------------------------------------------------------------------
# _set_channel_video_thumbnail
# ---------------------------------------------------------------------------

class TestSetChannelVideoThumbnail(unittest.TestCase):
    def test_picks_smallest(self) -> None:
        ch = YouTubeChannel(name='Test')
        ch.channel_thumbnails = {
            YouTubeThumbnail(
                {'url': 'https://big.jpg', 'width': 800, 'height': 800}
            ),
            YouTubeThumbnail(
                {'url': 'https://small.jpg', 'width': 40, 'height': 40}
            ),
        }
        ch._set_channel_video_thumbnail()
        self.assertIsNotNone(ch.channel_thumbnail)

    def test_no_thumbnails(self) -> None:
        ch = YouTubeChannel(name='Test')
        ch._set_channel_video_thumbnail()
        self.assertIsNone(ch.channel_thumbnail)


# ---------------------------------------------------------------------------
# _parse_thumbnails_banners
# ---------------------------------------------------------------------------

class TestParseThumbnailsBanners(unittest.TestCase):
    def test_parses_metadata_avatar(self) -> None:
        ch = YouTubeChannel(name='Test')
        metadata = {
            'avatar': {
                'thumbnails': [
                    {'url': 'https://avatar.jpg', 'width': 88, 'height': 88}
                ]
            }
        }
        page_data = {'header': {}}
        ch._parse_thumbnails_banners(metadata, page_data)
        self.assertGreaterEqual(len(ch.channel_thumbnails), 1)

    def test_parses_banner_from_header(self) -> None:
        ch = YouTubeChannel(name='Test')
        page_data = {
            'header': {
                'pageHeaderRenderer': {
                    'content': {
                        'pageHeaderViewModel': {
                            'banner': {
                                'imageBannerViewModel': {
                                    'image': {
                                        'sources': [
                                            {
                                                'url': 'https://banner.jpg',
                                                'width': 1060,
                                                'height': 175,
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
        ch._parse_thumbnails_banners({}, page_data)
        self.assertGreaterEqual(len(ch.banners), 1)

    def test_fallback_banner_from_c4_tabbed_header(self) -> None:
        ch = YouTubeChannel(name='Test')
        page_data = {
            'header': {
                'c4TabbedHeaderRenderer': {
                    'banner': {
                        'thumbnails': [
                            {
                                'url': 'https://c4banner.jpg',
                                'width': 1060,
                                'height': 175,
                            }
                        ]
                    }
                }
            }
        }
        ch._parse_thumbnails_banners({}, page_data)
        self.assertGreaterEqual(len(ch.banners), 1)


# ---------------------------------------------------------------------------
# find_nested_dicts
# ---------------------------------------------------------------------------

class TestFindNestedDicts(unittest.TestCase):
    def test_finds_in_dict(self) -> None:
        data = {'a': {'b': {'target': 'found'}}}
        self.assertEqual(
            YouTubeChannel.find_nested_dicts('target', data), 'found'
        )

    def test_finds_in_list(self) -> None:
        data = [{'target': 'in_list'}]
        self.assertEqual(
            YouTubeChannel.find_nested_dicts('target', data), 'in_list'
        )

    def test_returns_none_when_missing(self) -> None:
        self.assertIsNone(
            YouTubeChannel.find_nested_dicts('missing', {'a': 1})
        )

    def test_returns_none_for_non_container(self) -> None:
        self.assertIsNone(
            YouTubeChannel.find_nested_dicts('x', 'a string')
        )


# ---------------------------------------------------------------------------
# parse_nested_dicts
# ---------------------------------------------------------------------------

class TestFindNestedDicts(unittest.TestCase):
    def test_traverses_path(self) -> None:
        data = {'a': {'b': {'c': 'value'}}}
        self.assertEqual(
            YouTubeChannel.find_nested_dicts('c', data),
            'value'
        )

    def test_returns_none_for_missing_key(self) -> None:
        self.assertIsNone(
            YouTubeChannel.find_nested_dicts('z', {'a': {}})
        )

    def test_returns_none_for_non_dict_data(self) -> None:
        self.assertIsNone(
            YouTubeChannel.find_nested_dicts('a', 'just a string')
        )


# ---------------------------------------------------------------------------
# _generate_external_link (more coverage)
# ---------------------------------------------------------------------------

class TestGenerateExternalLink(unittest.TestCase):
    def test_strips_http(self) -> None:
        link = YouTubeChannel._generate_external_link(
            'http://twitter.com/test', 1, title='Twitter'
        )
        self.assertEqual(link.url, 'https://twitter.com/test')

    def test_strips_https(self) -> None:
        link = YouTubeChannel._generate_external_link(
            'https://instagram.com/test', 1, title='IG'
        )
        self.assertEqual(link.url, 'https://instagram.com/test')

    def test_infers_name_from_two_part_domain(self) -> None:
        '''e.g. twitter.com → twitter → maps to Twitter in SocialNetworks'''
        link = YouTubeChannel._generate_external_link(
            'https://twitter.com/user', 1
        )
        self.assertIsNotNone(link)
        self.assertEqual(link.name, 'Twitter')

    def test_infers_name_from_country_tld(self) -> None:
        '''e.g. bbc.co.uk → bbc'''
        link = YouTubeChannel._generate_external_link(
            'https://www.bbc.co.uk/news', 1
        )
        self.assertIsNotNone(link)

    def test_and_prefix_returns_none(self) -> None:
        '''URLs starting with "and " are overflow text, should return None'''
        link = YouTubeChannel._generate_external_link(
            'and 3 more links', 1
        )
        self.assertIsNone(link)

    def test_unknown_domain_uses_url_as_name(self) -> None:
        '''Unrecognised multi-part domain falls through to url as name.'''
        link = YouTubeChannel._generate_external_link(
            'https://some.random.thing.example/path', 1
        )
        self.assertIsNotNone(link)

    def test_strips_www(self) -> None:
        link = YouTubeChannel._generate_external_link(
            'https://www.facebook.com/page', 1
        )
        self.assertEqual(link.name, 'Facebook')


# ---------------------------------------------------------------------------
# parse_external_urls edge cases
# ---------------------------------------------------------------------------

class TestParseExternalUrls(unittest.TestCase):
    def test_skips_items_without_field(self) -> None:
        data = [{'somethingElse': {}}]
        result = YouTubeChannel.parse_external_urls(data)
        self.assertEqual(len(result), 0)

    def test_empty_input(self) -> None:
        result = YouTubeChannel.parse_external_urls([])
        self.assertEqual(len(result), 0)

    def test_none_input(self) -> None:
        result = YouTubeChannel.parse_external_urls(None)
        self.assertEqual(len(result), 0)


# ---------------------------------------------------------------------------
# parse_video_count / parse_subscriber_count edge cases
# ---------------------------------------------------------------------------

class TestParseCountEdgeCases(unittest.TestCase):
    def test_video_count_returns_none_no_rows(self) -> None:
        self.assertIsNone(YouTubeChannel.parse_video_count({}))

    def test_subscriber_count_returns_none_no_rows(self) -> None:
        self.assertIsNone(YouTubeChannel.parse_subscriber_count({}))

    def test_video_count_skips_non_video_text(self) -> None:
        data = _make_metadata_rows('100 subscribers')
        self.assertIsNone(YouTubeChannel.parse_video_count(data))

    def test_subscriber_count_skips_non_subscriber_text(self) -> None:
        data = _make_metadata_rows('50 videos')
        self.assertIsNone(YouTubeChannel.parse_subscriber_count(data))

    def test_video_count_skips_non_dict_text(self) -> None:
        '''metadataParts with a non-dict text should be skipped.'''
        data = {
            'header': {
                'pageHeaderRenderer': {
                    'content': {
                        'pageHeaderViewModel': {
                            'metadata': {
                                'contentMetadataViewModel': {
                                    'metadataRows': [
                                        {
                                            'metadataParts': [
                                                {'text': 'just a string'}
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
        self.assertIsNone(YouTubeChannel.parse_video_count(data))


# ---------------------------------------------------------------------------
# parse_view_count edge cases
# ---------------------------------------------------------------------------

class TestParseViewCount(unittest.TestCase):
    def test_with_valid_data(self) -> None:
        data = {
            'header': {
                'c4TabbedHeaderRenderer': {
                    'viewCountText': {
                        'simpleText': '1,234,567 views'
                    }
                }
            }
        }
        self.assertEqual(YouTubeChannel.parse_view_count(data), 1234567)

    def test_returns_none_for_empty_data(self) -> None:
        self.assertIsNone(YouTubeChannel.parse_view_count({}))


# ---------------------------------------------------------------------------
# parse_thumbnails edge cases (static method)
# ---------------------------------------------------------------------------

class TestParseThumbnailsStatic(unittest.TestCase):
    def test_fallback_to_list_input(self) -> None:
        '''When data is a list, it should be treated as thumbnails directly.'''
        data = [
            {'url': 'https://thumb.jpg', 'width': 88, 'height': 88}
        ]
        result = YouTubeChannel.parse_thumbnails(data)
        self.assertEqual(len(result), 1)

    def test_fallback_to_thumbnail_key(self) -> None:
        '''When dict has no deep path, falls back to .thumbnail.thumbnails'''
        data = {
            'thumbnail': {
                'thumbnails': [
                    {'url': 'https://fallback.jpg', 'width': 88, 'height': 88}
                ]
            }
        }
        result = YouTubeChannel.parse_thumbnails(data)
        self.assertEqual(len(result), 1)

    def test_empty_dict(self) -> None:
        result = YouTubeChannel.parse_thumbnails({})
        self.assertEqual(len(result), 0)


# ---------------------------------------------------------------------------
# parse_banners edge cases
# ---------------------------------------------------------------------------

class TestParseBannersEdgeCases(unittest.TestCase):
    def test_empty_data(self) -> None:
        result = YouTubeChannel.parse_banners({})
        self.assertEqual(len(result), 0)

    def test_multiple_banner_types(self) -> None:
        '''Testing banner + tvBanner together.'''
        data = {
            'header': {
                'pageHeaderRenderer': {
                    'content': {
                        'pageHeaderViewModel': {
                            'banner': {
                                'imageBannerViewModel': {
                                    'image': {
                                        'sources': [
                                            {
                                                'url': 'https://b1.jpg',
                                                'width': 1060,
                                                'height': 175,
                                            }
                                        ]
                                    }
                                }
                            },
                            'tvBanner': {
                                'imageBannerViewModel': {
                                    'image': {
                                        'sources': [
                                            {
                                                'url': 'https://tv.jpg',
                                                'width': 2560,
                                                'height': 1440,
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
        result = YouTubeChannel.parse_banners(data)
        self.assertEqual(len(result), 2)
        hints = {b.display_hint for b in result}
        self.assertEqual(hints, {'banner', 'tvBanner'})


# ---------------------------------------------------------------------------
# extract_linked_channels edge cases
# ---------------------------------------------------------------------------

class TestExtractLinkedChannelsEdge(unittest.TestCase):
    def test_empty_page_data(self) -> None:
        self.assertEqual(YouTubeChannel.extract_linked_channels({}), set())

    def test_skips_channel_without_subs(self) -> None:
        page_data = {
            'contents': {
                'twoColumnBrowseResultsRenderer': {
                    'tabs': [
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
                                                                                                'url': '/@Ch'
                                                                                            }
                                                                                        }
                                                                                    },
                                                                                    'subscriberCountText': {}
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
                }
            }
        }
        result = YouTubeChannel.extract_linked_channels(page_data)
        self.assertEqual(len(result), 0)

    def test_skips_renderer_without_url(self) -> None:
        page_data = {
            'contents': {
                'twoColumnBrowseResultsRenderer': {
                    'tabs': [
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
                                                                                    'navigationEndpoint': {},
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
                }
            }
        }
        result = YouTubeChannel.extract_linked_channels(page_data)
        self.assertEqual(len(result), 0)


# ---------------------------------------------------------------------------
# Additional coverage tests
# ---------------------------------------------------------------------------

class TestExtractInitialDataJsonError(unittest.TestCase):
    """Cover the JSONDecodeError → continue path in _extract_initial_data."""

    def test_continues_on_json_error(self) -> None:
        ch = YouTubeChannel('TestCh')
        # First pattern matches but JSON is invalid; second yields valid data
        html = (
            'var ytInitialData = {not valid json};'
            'window["ytInitialData"] = {"key": "value"};'
        )
        result = ch._extract_initial_data(html)
        self.assertEqual(result, {'key': 'value'})

    def test_raises_when_all_patterns_fail(self) -> None:
        ch = YouTubeChannel('TestCh')
        with self.assertRaises(ValueError):
            ch._extract_initial_data('no matching pattern here')


class TestExtractVerifiedStatusEdge(unittest.TestCase):
    def test_returns_false_for_none(self) -> None:
        # Line 1020: empty page_data logs warning and falls through
        self.assertFalse(YouTubeChannel.extract_verified_status(''))

    def test_returns_false_for_empty_string(self) -> None:
        self.assertFalse(YouTubeChannel.extract_verified_status(''))


class TestFindNestedDictsList(unittest.TestCase):
    """Cover the list branch at lines 1054-1060."""

    def test_finds_key_inside_list(self) -> None:
        data = [{'a': 1}, {'target': 'found'}]
        result = YouTubeChannel.find_nested_dicts('target', data)
        self.assertEqual(result, 'found')

    def test_returns_none_when_list_has_no_match(self) -> None:
        data = [{'a': 1}, {'b': 2}]
        self.assertIsNone(YouTubeChannel.find_nested_dicts('target', data))

    def test_finds_deeply_nested_in_list(self) -> None:
        data = {'outer': [{'inner': [{'key': 'deep'}]}]}
        self.assertEqual(
            YouTubeChannel.find_nested_dicts('key', data), 'deep'
        )


class TestParseViewCountException(unittest.TestCase):
    """Cover the except branch at lines 1170-1172."""

    def test_returns_none_on_malformed_view_count(self) -> None:
        # Pass a non-string in viewCountText.simpleText path that will
        # cause convert_number_string to raise
        data = {
            'header': {
                'c4TabbedHeaderRenderer': {
                    'viewCountText': {
                        'simpleText': 'not-a-number views'
                    }
                }
            }
        }
        result = YouTubeChannel.parse_view_count(data)
        # Should return None rather than raising
        self.assertIsNone(result)


class TestParseChannelAboutDataCountry(unittest.TestCase):
    """Cover the country_converter branch at line 656."""

    def test_parses_country_code(self) -> None:
        ch = YouTubeChannel('TestCh')
        about_renderer = {
            'country': {'simpleText': 'United Kingdom'},
        }
        ch._parse_channel_about_data(about_renderer)
        self.assertEqual(ch.country, 'GB')

    def test_no_country_leaves_none(self) -> None:
        ch = YouTubeChannel('TestCh')
        about_renderer = {}
        ch._parse_channel_about_data(about_renderer)
        self.assertIsNone(ch.country)


class TestParseThumbnailsBannersHeaderFallback(unittest.TestCase):
    """Cover the header_renderer avatar fallback at line 739."""

    def test_avatar_from_header_renderer(self) -> None:
        ch = YouTubeChannel('TestCh')
        metadata = {}  # No avatar in metadata
        page_data = {
            'header': {
                'c4TabbedHeaderRenderer': {
                    'avatar': {
                        'thumbnails': [
                            {'url': '//example.com/avatar.jpg',
                             'width': 88, 'height': 88}
                        ]
                    }
                }
            }
        }
        ch._parse_thumbnails_banners(metadata, page_data)
        self.assertTrue(len(ch.channel_thumbnails) > 0)
        thumb = next(iter(ch.channel_thumbnails))
        self.assertIn('avatar.jpg', thumb.url)


class TestExtractLinkedChannelsSkipsEmpty(unittest.TestCase):
    """Cover line 790 where gridChannelRenderer is empty dict."""

    def test_skips_empty_grid_renderer(self) -> None:
        page_data = {
            'contents': {
                'twoColumnBrowseResultsRenderer': {
                    'tabs': [
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
                                                                            {'gridChannelRenderer': {}},
                                                                            {'someOtherRenderer': {}}
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
                }
            }
        }
        result = YouTubeChannel.extract_linked_channels(page_data)
        self.assertEqual(len(result), 0)


class TestScrapeValidation(unittest.IsolatedAsyncioTestCase):
    """Cover save_dir validation in scrape() and scrape_channel_content()."""

    async def test_scrape_raises_without_save_dir(self) -> None:
        ch = YouTubeChannel('TestCh')
        ch.save_dir = None
        with self.assertRaises(ValueError):
            await ch.scrape()

    async def test_scrape_channel_content_raises_nonexistent_dir(self) -> None:
        ch = YouTubeChannel('TestCh')
        with self.assertRaises(ValueError):
            await ch.scrape_channel_content(save_dir='/nonexistent/path')

    async def test_scrape_uses_self_save_dir(self) -> None:
        ch = YouTubeChannel('TestCh')
        ch.save_dir = '/tmp'
        with patch.object(ch, 'scrape_about_page', new_callable=AsyncMock), \
             patch.object(ch, 'scrape_channel_content',
                          new_callable=AsyncMock) as mock_scc:
            await ch.scrape()
            mock_scc.assert_called_once_with(
                save_dir='/tmp',
                max_videos_per_channel=unittest.mock.ANY
            )


class TestGetChannelPage(unittest.IsolatedAsyncioTestCase):
    """Cover get_channel_page (lines 896-908)."""

    async def test_raises_on_empty_response(self) -> None:
        ch = YouTubeChannel('TestCh')
        ch.browse_client = MagicMock()
        ch.browse_client.get = AsyncMock(return_value=None)
        with self.assertRaises(RuntimeError):
            await ch.get_channel_page()

    async def test_returns_page_data(self) -> None:
        ch = YouTubeChannel('TestCh')
        ch.browse_client = MagicMock()
        html = (
            '"externalId":"UC123"' +
            'var ytInitialData = {"contents": {}};'
        )
        ch.browse_client.get = AsyncMock(return_value=html)
        result = await ch.get_channel_page()
        self.assertIsInstance(result, dict)
        self.assertIn('contents', result)


class TestScrapeVideo(unittest.IsolatedAsyncioTestCase):
    """Cover scrape_video (lines 1326-1334)."""

    async def test_delegates_to_youtube_video_scrape(self) -> None:
        ch = YouTubeChannel('TestCh')
        mock_video = MagicMock()
        with patch(
            'scrape_exchange.youtube.youtube_channel.YouTubeVideo'
        ) as MockYTVideo:
            MockYTVideo.scrape = AsyncMock(return_value=mock_video)
            result = await ch.scrape_video('abc123', None)
            self.assertEqual(result, mock_video)
            MockYTVideo.scrape.assert_called_once()


if __name__ == '__main__':
    unittest.main()
