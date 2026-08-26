'''
Unit tests for YouTubeChannel class and related functions.

:author: Boinko <boinko@scrape.exchange
:copyright: Copyright 2026
:license: GPLv3
'''

import asyncio
import logging
import inspect
import unittest
import unittest.mock


# A handful of tests exercise parse/extract branches that log a
# WARNING when input is malformed. Silence the module logger for the
# duration of the file to keep the test output clean.
_YC_LOGGER: logging.Logger = logging.getLogger(
    'scrape_exchange.youtube.youtube_channel',
)
_YC_LOGGER_PRIOR_LEVEL: int = _YC_LOGGER.level


def setUpModule() -> None:
    _YC_LOGGER.setLevel(logging.ERROR)


def tearDownModule() -> None:
    _YC_LOGGER.setLevel(_YC_LOGGER_PRIOR_LEVEL)

from pathlib import Path
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import orjson
import jsonschema
from jsonschema import Draft202012Validator

from scrape_exchange.youtube.youtube_channel import (
    YouTubeChannel,
    YouTubeThumbnail,
    YouTubeExternalLink,
    YouTubeChannelLink,
    terminal_channel_page_message,
)
from scrape_exchange.youtube.youtube_course import YouTubeCourse, YouTubeCourseVideo
from scrape_exchange.youtube.youtube_playlist import YouTubePlaylist
from scrape_exchange.youtube.youtube_post import YouTubePost
from scrape_exchange.youtube.youtube_product import YouTubeProduct


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

    def test_extract_channel_id_success_and_failure(self) -> None:
        html = 'some preface "externalId":"UCABC123" and more'
        self.assertEqual(YouTubeChannel.extract_channel_id(html), 'UCABC123')

        self.assertIsNone(YouTubeChannel.extract_channel_id(''))

        with self.assertRaises(ValueError):
            YouTubeChannel.extract_channel_id('blah')

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
        self.assertEqual(link.channel_handle, 'FeaturedChannel')
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
        ch = YouTubeChannel(channel_handle='HistoryMatters')
        self.assertEqual(ch.channel_handle, 'HistoryMatters')
        self.assertIn('@HistoryMatters', ch.url)
        self.assertIsNone(ch.channel_id)
        self.assertEqual(ch.video_ids, set())

    def test_init_strips_at_from_name(self) -> None:
        ch = YouTubeChannel(channel_handle='@SomeChannel')
        self.assertEqual(ch.channel_handle, 'SomeChannel')

    def test_init_no_name(self) -> None:
        ch = YouTubeChannel()
        self.assertIsNone(ch.url)
        self.assertIsNone(ch.title)

    def test_init_prefers_channel_id_url_when_both_given(self) -> None:
        # channel_id is preferred for the scrape URL (stable, no UTF-8),
        # but the handle is still retained as metadata.
        ch = YouTubeChannel(
            channel_handle='HistoryMatters',
            channel_id='UCaaaaaaaaaaaaaaaaaaaaaa',
        )
        self.assertIn('/channel/UCaaaaaaaaaaaaaaaaaaaaaa', ch.url)
        self.assertNotIn('@HistoryMatters', ch.url)
        self.assertEqual(ch.channel_handle, 'HistoryMatters')

    def test_init_id_only_uses_channel_url(self) -> None:
        ch = YouTubeChannel(channel_id='UCbbbbbbbbbbbbbbbbbbbbbb')
        self.assertIn('/channel/UCbbbbbbbbbbbbbbbbbbbbbb', ch.url)
        self.assertIsNone(ch.channel_handle)

    def test_init_handle_only_uses_at_url(self) -> None:
        ch = YouTubeChannel(channel_handle='HistoryMatters')
        self.assertIn('@HistoryMatters', ch.url)


class TestYouTubeChannelEquality(unittest.TestCase):
    def test_equal_channels(self) -> None:
        ch1 = YouTubeChannel(channel_handle='Test')
        ch2 = YouTubeChannel(channel_handle='Test')
        ch1.channel_id = ch2.channel_id = 'UC123'
        ch1.title = ch2.title = 'Test Title'
        self.assertEqual(ch1, ch2)

    def test_not_equal_different_name(self) -> None:
        ch1 = YouTubeChannel(channel_handle='A')
        ch2 = YouTubeChannel(channel_handle='B')
        self.assertNotEqual(ch1, ch2)

    def test_not_equal_different_type(self) -> None:
        ch = YouTubeChannel(channel_handle='X')
        self.assertNotEqual(ch, 'not_a_channel')


# ---------------------------------------------------------------------------
# Channel-id shape validation and normalisation
# ---------------------------------------------------------------------------

# A realistic mixed-case channel_id: prefix uppercase, body mixed-case
# base64url. Real YouTube IDs use the full case-sensitive alphabet.
_CANONICAL_ID: str = 'UC1ToEoUPkjz1qcLGX-u8YFA'


class TestIsChannelIdStrict(unittest.TestCase):
    '''``is_channel_id`` must reject lowercase ``uc`` prefixes.

    The pre-fix regex used ``re.IGNORECASE`` and so accepted
    ``'uc...'`` strings. Downstream consumers
    (``ChannelIdentityStore.bind``, ``promote_to_scheduled``,
    YouTube's own APIs) require the canonical ``UC`` prefix, so
    lowercase forms must fail validation here and be routed
    through ``normalise_channel_id`` instead.
    '''

    def test_accepts_canonical_uppercase_prefix(self) -> None:
        self.assertTrue(YouTubeChannel.is_channel_id(_CANONICAL_ID))

    def test_accepts_mixed_case_body(self) -> None:
        # Body chars are case-sensitive base64url; both cases occur
        # in real IDs.
        self.assertTrue(
            YouTubeChannel.is_channel_id('UCabcdefghijklmnopqrstuv')
        )
        self.assertTrue(
            YouTubeChannel.is_channel_id('UCABCDEFGHIJKLMNOPQRSTUV')
        )

    def test_rejects_lowercase_uc_prefix(self) -> None:
        self.assertFalse(
            YouTubeChannel.is_channel_id(
                'uc1toeoupkjz1qclgx-u8yfa'
            )
        )

    def test_rejects_mixed_case_prefix(self) -> None:
        self.assertFalse(
            YouTubeChannel.is_channel_id('Uc' + _CANONICAL_ID[2:])
        )
        self.assertFalse(
            YouTubeChannel.is_channel_id('uC' + _CANONICAL_ID[2:])
        )

    def test_rejects_wrong_length(self) -> None:
        self.assertFalse(YouTubeChannel.is_channel_id('UC123'))
        self.assertFalse(
            YouTubeChannel.is_channel_id(
                _CANONICAL_ID + 'x'
            )
        )

    def test_rejects_empty_and_none(self) -> None:
        self.assertFalse(YouTubeChannel.is_channel_id(''))
        self.assertFalse(YouTubeChannel.is_channel_id(None))


class TestNormaliseChannelId(unittest.TestCase):
    '''``normalise_channel_id`` recovers a canonical ID from a
    lowercase ``uc`` prefix while leaving the body case-sensitive.

    A 24-char input whose first two chars are ``uc``/``Uc``/``uC``
    and whose remaining 22 chars are valid base64url returns the
    same string with the first two chars uppercased to ``UC``.
    Already-canonical input round-trips unchanged. Anything else
    returns ``None``.
    '''

    def test_returns_canonical_unchanged(self) -> None:
        self.assertEqual(
            YouTubeChannel.normalise_channel_id(_CANONICAL_ID),
            _CANONICAL_ID,
        )

    def test_uppercases_lowercase_prefix(self) -> None:
        lowercased: str = 'uc' + _CANONICAL_ID[2:]
        self.assertEqual(
            YouTubeChannel.normalise_channel_id(lowercased),
            'UC' + _CANONICAL_ID[2:],
        )

    def test_uppercases_mixed_case_prefix(self) -> None:
        self.assertEqual(
            YouTubeChannel.normalise_channel_id(
                'Uc' + _CANONICAL_ID[2:]
            ),
            _CANONICAL_ID,
        )
        self.assertEqual(
            YouTubeChannel.normalise_channel_id(
                'uC' + _CANONICAL_ID[2:]
            ),
            _CANONICAL_ID,
        )

    def test_preserves_body_case(self) -> None:
        # Lowercased prefix must NOT lowercase the body — bodies
        # are case-sensitive base64url and case differences identify
        # distinct channels.
        result: str | None = YouTubeChannel.normalise_channel_id(
            'uc1ToEoUPkjz1qcLGX-u8YFA',
        )
        self.assertEqual(result, 'UC1ToEoUPkjz1qcLGX-u8YFA')

    def test_returns_none_for_empty(self) -> None:
        self.assertIsNone(YouTubeChannel.normalise_channel_id(''))

    def test_returns_none_for_none(self) -> None:
        self.assertIsNone(YouTubeChannel.normalise_channel_id(None))

    def test_returns_none_for_wrong_length(self) -> None:
        self.assertIsNone(
            YouTubeChannel.normalise_channel_id('uc123'),
        )
        self.assertIsNone(
            YouTubeChannel.normalise_channel_id(
                _CANONICAL_ID + 'x',
            ),
        )

    def test_returns_none_for_non_uc_prefix(self) -> None:
        # 24-char string that does not begin with 'uc' in any case.
        self.assertIsNone(
            YouTubeChannel.normalise_channel_id(
                'AB' + _CANONICAL_ID[2:],
            ),
        )

    def test_returns_none_for_invalid_body_chars(self) -> None:
        # '!' is not in the base64url alphabet.
        self.assertIsNone(
            YouTubeChannel.normalise_channel_id(
                'uc!' + _CANONICAL_ID[3:],
            ),
        )


class TestYouTubeChannelToFromDict(unittest.TestCase):
    def test_to_dict_basic_fields(self) -> None:
        ch = YouTubeChannel(channel_handle='Test')
        ch.channel_id = 'UC123'
        ch.title = 'My Title'
        ch.description = 'A description'
        ch.verified = True
        ch.subscriber_count = 1000
        ch.video_count = 50
        ch.view_count = 100000
        ch.joined_date = datetime(2020, 1, 15, tzinfo=UTC)
        data = ch.to_dict()
        self.assertEqual(data['channel_handle'], 'Test')
        self.assertEqual(data['channel_id'], 'UC123')
        self.assertEqual(data['title'], 'My Title')
        self.assertEqual(data['description'], 'A description')
        self.assertTrue(data['verified'])
        self.assertEqual(data['subscriber_count'], 1000)
        self.assertEqual(data['video_count'], 50)
        self.assertEqual(data['view_count'], 100000)
        self.assertIn('2020', data['joined_date'])

    def test_to_dict_with_video_ids(self) -> None:
        ch = YouTubeChannel(channel_handle='Test')
        ch.video_ids = {'v1', 'v2'}
        data = ch.to_dict(with_video_ids=True)
        self.assertEqual(set(data['video_ids']), {'v1', 'v2'})

    def test_to_dict_without_video_ids(self) -> None:
        ch = YouTubeChannel(channel_handle='Test')
        ch.video_ids = {'v1'}
        data = ch.to_dict(with_video_ids=False)
        self.assertNotIn('video_ids', data)

    def test_to_dict_none_joined_date(self) -> None:
        ch = YouTubeChannel(channel_handle='Test')
        data = ch.to_dict()
        self.assertIsNone(data['joined_date'])

    def test_round_trip_from_dict(self) -> None:
        ch = YouTubeChannel(channel_handle='RoundTrip')
        ch.channel_id = 'UC_RT'
        ch.title = 'RT Title'
        ch.description = 'desc'
        ch.category = 'Education'
        ch.keywords = {'keyword1', 'keyword2'}
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

        self.assertEqual(restored.category, 'Education')
        self.assertEqual(ch, restored)

    def test_from_dict_empty(self) -> None:
        ch = YouTubeChannel.from_dict({'channel_handle': 'Empty'})
        self.assertEqual(ch.channel_handle, 'Empty')
        self.assertIsNone(ch.channel_id)
        self.assertIsNone(ch.joined_date)


# ---------------------------------------------------------------------------
# _extract_initial_data
# ---------------------------------------------------------------------------

class TestExtractInitialData(unittest.TestCase):
    def test_extracts_yt_initial_data(self) -> None:
        ch = YouTubeChannel(channel_handle='Test')
        html = 'blah ytInitialData = {"key": "value"}; more stuff'
        result = ch._extract_initial_data(html)
        self.assertEqual(result, {'key': 'value'})

    def test_extracts_window_syntax(self) -> None:
        ch = YouTubeChannel(channel_handle='Test')
        html = 'blah window["ytInitialData"] = {"k": 1}; more'
        result = ch._extract_initial_data(html)
        self.assertEqual(result, {'k': 1})

    def test_raises_on_missing_data(self) -> None:
        ch = YouTubeChannel(channel_handle='Test')
        with self.assertRaises(ValueError):
            ch._extract_initial_data('no data here')

    def test_sets_verified_from_html(self) -> None:
        ch = YouTubeChannel(channel_handle='Test')
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
        ch = YouTubeChannel(channel_handle='Test')
        handle = ch._extract_handle(
            'https://www.youtube.com/@MyHandle/videos', {}
        )
        self.assertEqual(handle, '@MyHandle')

    def test_from_metadata_channel_url(self) -> None:
        ch = YouTubeChannel(channel_handle='Test')
        handle = ch._extract_handle(
            'https://www.youtube.com/channel/UC123',
            {'channelUrl': 'https://www.youtube.com/@FromMeta'}
        )
        self.assertEqual(handle, '@FromMeta')

    def test_returns_none(self) -> None:
        ch = YouTubeChannel(channel_handle='Test')
        self.assertIsNone(
            ch._extract_handle('https://www.youtube.com/channel/UC12', {})
        )


# ---------------------------------------------------------------------------
# _extract_simple_text
# ---------------------------------------------------------------------------

class TestExtractSimpleText(unittest.TestCase):
    def setUp(self) -> None:
        self.ch = YouTubeChannel(channel_handle='Test')

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
        self.ch = YouTubeChannel(channel_handle='Test')

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
        ch = YouTubeChannel(channel_handle='Test')
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
        ch = YouTubeChannel(channel_handle='Test')
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
        ch = YouTubeChannel(channel_handle='Test')
        ch.title = 'Already set'
        ch._parse_channel_about_metadata({'title': 'New'})
        self.assertEqual(ch.title, 'Already set')


# ---------------------------------------------------------------------------
# _parse_channel_about_data
# ---------------------------------------------------------------------------

class TestParseChannelAboutData(unittest.TestCase):
    def test_parses_joined_date(self) -> None:
        ch = YouTubeChannel(channel_handle='Test')
        about = {
            'joinedDateText': {'content': 'Joined Aug 2, 2015'},
        }
        ch._parse_channel_about_data(about)
        self.assertEqual(ch.joined_date, datetime(2015, 8, 2, tzinfo=UTC))

    def test_parses_view_count(self) -> None:
        ch = YouTubeChannel(channel_handle='Test')
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
        ch = YouTubeChannel(channel_handle='Test')
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
        ch = YouTubeChannel(channel_handle='Test')
        ch.url = 'https://www.youtube.com/@Test'
        self.assertIsNone(
            ch._find_about_renderer(
                {'onResponseReceivedEndpoints': []}
            )
        )

    def test_returns_none_when_no_endpoints(self) -> None:
        ch = YouTubeChannel(channel_handle='Test')
        ch.url = 'https://www.youtube.com/@Test'
        self.assertIsNone(ch._find_about_renderer({}))


# ---------------------------------------------------------------------------
# parse_channel_video_data
# ---------------------------------------------------------------------------

class TestParseChannelVideoData(unittest.TestCase):
    def test_parses_channel_info(self) -> None:
        ch = YouTubeChannel(channel_handle='Test')

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
        ch = YouTubeChannel(channel_handle='Test')
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

    def test_failed_video_count_parse_keeps_positive_count(self) -> None:
        ch = YouTubeChannel(channel_handle='Test')
        ch.video_count = 42
        page_data = {
            'metadata': {
                'channelMetadataRenderer': {
                    'name': 'TestChannel',
                }
            },
        }
        with patch.object(
            YouTubeChannel,
            'parse_video_count',
            return_value=None,
        ):
            ch.parse_channel_video_data(page_data)

        self.assertEqual(ch.video_count, 42)


# ---------------------------------------------------------------------------
# _set_channel_video_thumbnail
# ---------------------------------------------------------------------------

class TestSetChannelVideoThumbnail(unittest.TestCase):
    def test_picks_smallest(self) -> None:
        ch = YouTubeChannel(channel_handle='Test')
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
        ch = YouTubeChannel(channel_handle='Test')
        ch._set_channel_video_thumbnail()
        self.assertIsNone(ch.channel_thumbnail)


# ---------------------------------------------------------------------------
# _parse_thumbnails_banners
# ---------------------------------------------------------------------------

class TestParseThumbnailsBanners(unittest.TestCase):
    def test_parses_metadata_avatar(self) -> None:
        ch = YouTubeChannel(channel_handle='Test')
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
        ch = YouTubeChannel(channel_handle='Test')
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
        ch = YouTubeChannel(channel_handle='Test')
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
# find_nested_dicts — additional path-traversal cases
# ---------------------------------------------------------------------------

class TestFindNestedDictsTraversal(unittest.TestCase):
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


class TestReconcileDescriptionExternalLinks(unittest.TestCase):
    def test_adds_social_link_missing_from_external_urls(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(channel_handle='Test')
        channel.description = (
            'Follow us at https://www.instagram.com/example/. '
            'Read more at https://example.com/news.'
        )

        channel._reconcile_description_external_links()

        links: list[dict[str, str | int]] = [
            link.to_dict() for link in channel.external_urls
        ]
        self.assertEqual(
            links,
            [{
                'name': 'Instagram',
                'url': 'https://www.instagram.com/example/',
                'priority': 10,
            }],
        )

    def test_existing_network_suppresses_description_profile(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(channel_handle='Test')
        existing_link: YouTubeExternalLink = YouTubeExternalLink(
            name='Instagram',
            url='https://instagram.com/structured-profile',
            priority=10,
        )
        channel.external_urls = {existing_link}
        channel.description = (
            'Alternate profile: https://instagram.com/description-profile'
        )

        channel._reconcile_description_external_links()

        self.assertEqual(channel.external_urls, {existing_link})

    def test_twitter_and_x_are_the_same_network(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(channel_handle='Test')
        existing_link: YouTubeExternalLink = YouTubeExternalLink(
            name='Twitter',
            url='https://twitter.com/structured-profile',
            priority=10,
        )
        channel.external_urls = {existing_link}
        channel.description = 'Current profile: https://x.com/new-profile'

        channel._reconcile_description_external_links()

        self.assertEqual(channel.external_urls, {existing_link})

    def test_adds_first_profile_per_network_and_rejects_lookalike(
        self,
    ) -> None:
        channel: YouTubeChannel = YouTubeChannel(channel_handle='Test')
        channel.description = (
            'Facebook (https://m.facebook.com/first), then '
            'https://facebook.com/second. Ignore '
            'https://notinstagram.com/profile; use '
            'https://instagram.com/real-profile.'
        )

        channel._reconcile_description_external_links()

        links_by_name: dict[str, dict[str, str | int]] = {
            link.name: link.to_dict() for link in channel.external_urls
        }
        self.assertEqual(
            links_by_name,
            {
                'Facebook': {
                    'name': 'Facebook',
                    'url': 'https://m.facebook.com/first',
                    'priority': 10,
                },
                'Instagram': {
                    'name': 'Instagram',
                    'url': 'https://instagram.com/real-profile',
                    'priority': 20,
                },
            },
        )

    def test_adds_direct_email_address(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(channel_handle='Test')
        channel.description = 'Email hello@example.com for details.'

        channel._reconcile_description_external_links()

        self.assertEqual(
            [link.to_dict() for link in channel.external_urls],
            [{
                'name': 'email',
                'url': 'mailto:hello@example.com',
                'priority': 10,
            }],
        )

    def test_normalizes_whitespace_around_at_sign(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(channel_handle='Test')
        channel.description = 'Email hello   @   example.com for details.'

        channel._reconcile_description_external_links()

        self.assertEqual(
            {link.url for link in channel.external_urls},
            {'mailto:hello@example.com'},
        )

    def test_normalizes_whitespace_delimited_at_word(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(channel_handle='Test')
        channel.description = 'Email hello at example.com for details.'

        channel._reconcile_description_external_links()

        self.assertEqual(
            {link.url for link in channel.external_urls},
            {'mailto:hello@example.com'},
        )

    def test_normalizes_hyphen_delimited_at_word(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(channel_handle='Test')
        channel.description = 'Email hello-at-example.com for details.'

        channel._reconcile_description_external_links()

        self.assertEqual(
            {link.url for link in channel.external_urls},
            {'mailto:hello@example.com'},
        )

    def test_deduplicates_email_addresses_case_insensitively(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(channel_handle='Test')
        channel.description = (
            'Email Hello@example.com or hello @ EXAMPLE.COM.'
        )

        channel._reconcile_description_external_links()

        self.assertEqual(
            {link.url for link in channel.external_urls},
            {'mailto:Hello@example.com'},
        )

    def test_normalizes_parenthesized_at_word_with_optional_spaces(
        self,
    ) -> None:
        channel: YouTubeChannel = YouTubeChannel(channel_handle='Test')
        channel.description = (
            'Email first(at)example.com or second (at) example.org.'
        )

        channel._reconcile_description_external_links()

        self.assertEqual(
            {link.url for link in channel.external_urls},
            {
                'mailto:first@example.com',
                'mailto:second@example.org',
            },
        )

    def test_existing_mailto_link_suppresses_duplicate_address(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(channel_handle='Test')
        existing_link: YouTubeExternalLink = YouTubeExternalLink(
            name='email',
            url='mailto:HELLO@example.com',
            priority=10,
        )
        channel.external_urls = {existing_link}
        channel.description = 'Email hello at EXAMPLE.COM.'

        channel._reconcile_description_external_links()

        self.assertEqual(channel.external_urls, {existing_link})

    def test_rejects_malformed_email_addresses(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(channel_handle='Test')
        channel.description = (
            'Invalid hello..there@example.com, '
            'hello at -example.com, and hello@example..com.'
        )

        channel._reconcile_description_external_links()

        self.assertEqual(channel.external_urls, set())

    def test_does_not_extract_email_from_url_path(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(channel_handle='Test')
        channel.description = (
            'Read https://example.com/hello-at-example.org for details.'
        )

        channel._reconcile_description_external_links()

        self.assertEqual(
            {link.name for link in channel.external_urls},
            set(),
        )

    def test_missing_vanity_url_does_not_break_email_extraction(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(channel_handle='Test')
        channel._parse_channel_about_metadata({
            'description': 'Email hello@example.com',
        })

        channel._reconcile_description_external_links()

        self.assertEqual(
            {link.url for link in channel.external_urls},
            {'mailto:hello@example.com'},
        )

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


class TestResolveChannelIdSignature(unittest.TestCase):
    def test_yt_client_parameter_removed(self) -> None:
        params: list[str] = list(
            inspect.signature(
                YouTubeChannel.resolve_channel_id,
            ).parameters,
        )

        self.assertEqual(params, ['channel_id', 'proxy'])
        self.assertNotIn('yt_client', params)


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

    async def test_content_description_reconciles_when_about_fails(
        self,
    ) -> None:
        channel: YouTubeChannel = YouTubeChannel(
            'TestCh', channel_id='UCknown'
        )
        channel.save_dir = '/tmp'

        async def scrape_channel_content(
            *args: object,
            **kwargs: object,
        ) -> int:
            channel.description = (
                'Follow https://www.instagram.com/content-profile'
            )
            return 0

        with patch.object(
            channel,
            'scrape_about_page',
            new_callable=AsyncMock,
            side_effect=RuntimeError('about failed'),
        ), patch.object(
            channel,
            'scrape_channel_content',
            side_effect=scrape_channel_content,
        ):
            await channel.scrape(with_about_page=True)

        self.assertEqual(
            {link.url for link in channel.external_urls},
            {'https://www.instagram.com/content-profile'},
        )

    async def test_known_channel_scrapes_about_and_content_together(
        self,
    ) -> None:
        ch = YouTubeChannel('TestCh', channel_id='UCknown')
        ch.save_dir = '/tmp'
        about_started = asyncio.Event()
        content_started = asyncio.Event()
        unblock = asyncio.Event()

        async def scrape_about_page(*args, **kwargs) -> None:
            about_started.set()
            await unblock.wait()

        async def scrape_channel_content(*args, **kwargs) -> int:
            content_started.set()
            await unblock.wait()
            return 0

        with patch.object(
            ch, 'scrape_about_page',
            side_effect=scrape_about_page,
        ), patch.object(
            ch, 'scrape_channel_content',
            side_effect=scrape_channel_content,
        ):
            task = asyncio.create_task(
                ch.scrape(with_about_page=True),
            )
            await asyncio.wait_for(
                about_started.wait(), timeout=0.1,
            )
            await asyncio.wait_for(
                content_started.wait(), timeout=0.1,
            )
            unblock.set()
            await task


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


# ---------------------------------------------------------------------------
# to_dict() JSON-schema compliance
# ---------------------------------------------------------------------------

SCHEMA_PATH: str = 'tests/collateral/boinko-youtube-channel-schema.json'


class TestToDictSchemaValidation(unittest.TestCase):
    '''Validate that YouTubeChannel.to_dict() output complies with the
    boinko-youtube-channel JSON schema.
    '''

    @classmethod
    def setUpClass(cls) -> None:
        with open(SCHEMA_PATH) as f:
            cls.schema: dict[str, any] = orjson.loads(f.read())
        cls.validator = Draft202012Validator(cls.schema)

    def _validate(self, data: dict) -> None:
        errors: list[any] = list(self.validator.iter_errors(data))
        if errors:
            messages: str = '\n'.join(e.message for e in errors)
            self.fail(f'Schema validation failed:\n{messages}')

    # -- minimal channel (defaults only) -----------------------------------

    def test_minimal_channel_validates(self) -> None:
        '''A freshly-constructed channel with only a name should pass.'''
        ch = YouTubeChannel(channel_handle='TestChannel')
        self._validate(ch.to_dict())

    # -- channel with video_ids --------------------------------------------

    def test_to_dict_with_video_ids_validates(self) -> None:
        ch = YouTubeChannel(channel_handle='TestChannel')
        ch.video_ids = {'abc123', 'def456'}
        self._validate(ch.to_dict(with_video_ids=True))

    # -- fully-populated channel -------------------------------------------

    def test_fully_populated_channel_validates(self) -> None:
        '''A channel with every field populated should still pass.'''
        ch = YouTubeChannel(channel_handle='FullChannel')
        ch.channel_id = 'UC_FAKE_ID_1234'
        ch.title = 'Full Channel Title'
        ch.description = 'A test description.'
        ch.keywords = {'keyword1', 'keyword2'}
        ch.is_family_safe = True
        ch.country = 'US'
        ch.available_country_codes = {'US', 'GB', 'DE'}
        ch.joined_date = datetime(2020, 1, 15, tzinfo=UTC)
        ch.rss_url = 'https://www.youtube.com/feeds/videos.xml?channel_id=UC_FAKE'
        ch.verified = True
        ch.subscriber_count = 500000
        ch.video_count = 200
        ch.view_count = 100000000

        ch.channel_thumbnails = {
            YouTubeThumbnail({
                'id': 'thumb1',
                'url': 'https://yt3.example.com/thumb.jpg',
                'width': 900,
                'height': 900,
            }),
        }
        ch.banners = {
            YouTubeThumbnail({
                'id': 'banner1',
                'url': 'https://yt3.example.com/banner.jpg',
                'width': 2560,
                'height': 424,
            }, display_hint='banner'),
        }
        ch.external_urls = {
            YouTubeExternalLink(
                name='Twitter', url='https://twitter.com/test', priority=0
            ),
        }

        ch.playlists = {
            YouTubePlaylist(
                playlist_id='PL123',
                title='Test Playlist',
                video_count=5,
                thumbnail_url='https://i.ytimg.com/vi/abc/hqdefault.jpg',
                channel_id='UC_FAKE_ID_1234',
            ),
        }
        ch.courses = {
            YouTubeCourse(
                playlist_id='PLC123',
                title='Test Course',
                video_count=3,
                thumbnail_url='https://i.ytimg.com/vi/xyz/hqdefault.jpg',
                channel_id='UC_FAKE_ID_1234',
                videos=[
                    YouTubeCourseVideo(
                        video_id='v1', title='Lecture 1',
                        duration_label='10:30',
                    ),
                ],
            ),
        }
        ch.posts = {
            YouTubePost(
                post_id='Ugkx_test123',
                content_text='Hello community!',
                published_time_text='2 days ago',
                vote_count='42',
                channel_id='UC_FAKE_ID_1234',
            ),
        }
        ch.merch = {
            YouTubeProduct(
                title='T-Shirt',
                price='$25.00',
                merchant_name='Merch Store',
                thumbnail_url='https://example.com/shirt.jpg',
                product_url='https://example.com/buy',
                accessibility_title='Cool T-Shirt',
                channel_id='UC_FAKE_ID_1234',
            ),
        }
        ch.video_ids = {'vid1', 'vid2', 'vid3'}

        # Without video_ids
        self._validate(ch.to_dict())
        # With video_ids
        self._validate(ch.to_dict(with_video_ids=True))

    # -- round-trip: to_dict -> from_dict -> to_dict validates -------------

    def test_round_trip_validates(self) -> None:
        '''from_dict(to_dict(ch)).to_dict() should still validate.'''
        ch = YouTubeChannel(channel_handle='RoundTrip')
        ch.channel_id = 'UC_RT'
        ch.title = 'Round Trip Channel'
        ch.subscriber_count = 100
        ch.video_count = 10
        ch.view_count = 5000

        original: dict = ch.to_dict()
        restored: YouTubeChannel = YouTubeChannel.from_dict(original)
        self._validate(restored.to_dict())

    # -- collateral file validates -----------------------------------------

    def test_historymatters_collateral_validates(self) -> None:
        '''The saved HistoryMatters.json collateral should validate.'''
        sample_path = Path(
            'tests/collateral/youtube_channels/HistoryMatters.json'
        )
        sample: dict[str, any] = orjson.loads(sample_path.read_text())
        self._validate(sample)


class TestCanonicalHandleAttribute(unittest.IsolatedAsyncioTestCase):
    '''canonical_handle is populated by scrape_channel_content.'''

    async def test_canonical_handle_set_from_browse(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as save_dir:
            channel: YouTubeChannel = YouTubeChannel(
                channel_handle='input_casing',
            )
            channel.channel_id = 'UC1234567890abcdefghij'

            browse_response: dict = {
                'metadata': {
                    'channelMetadataRenderer': {
                        'vanityChannelUrl': (
                            'http://www.youtube.com/@HistoryMatters'
                        ),
                    },
                },
            }

            with patch(
                'scrape_exchange.youtube.youtube_channel'
                '.YouTubeChannelTabs',
            ) as tabs_cls:
                tabs_cls.return_value.browse_channel = AsyncMock(
                    return_value=browse_response,
                )
                tabs_cls.return_value.scrape_loaded_tabs = AsyncMock(
                    return_value=(
                        set(), set(), set(), set(), set(), set(),
                    ),
                )
                with patch.object(
                    channel, 'parse_channel_video_data',
                ):
                    await channel.scrape_channel_content(
                        save_dir=save_dir,
                    )

            self.assertEqual(
                channel.channel_handle, 'HistoryMatters',
            )

    async def test_channel_handle_kept_when_no_vanity(self) -> None:
        '''When InnerTube returns no vanity URL, the constructor-supplied
        handle is preserved (no canonical override).'''
        import tempfile

        with tempfile.TemporaryDirectory() as save_dir:
            channel: YouTubeChannel = YouTubeChannel(channel_handle='legacy')
            channel.channel_id = 'UC1234567890abcdefghij'

            with patch(
                'scrape_exchange.youtube.youtube_channel'
                '.YouTubeChannelTabs',
            ) as tabs_cls:
                tabs_cls.return_value.browse_channel = AsyncMock(
                    return_value={'metadata': {}},
                )
                tabs_cls.return_value.scrape_loaded_tabs = AsyncMock(
                    return_value=(
                        set(), set(), set(), set(), set(), set(),
                    ),
                )
                with patch.object(
                    channel, 'parse_channel_video_data',
                ):
                    await channel.scrape_channel_content(
                        save_dir=save_dir,
                    )

            self.assertEqual(channel.channel_handle, 'legacy')

    async def test_channel_content_reuses_browse_client_proxy(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as save_dir:
            channel: YouTubeChannel = YouTubeChannel(
                channel_handle='input',
            )
            channel.channel_id = 'UC1234567890abcdefghij'
            channel.browse_client = MagicMock()
            channel.browse_client.proxy = 'http://proxy-one:8080'

            with patch(
                'scrape_exchange.youtube.youtube_channel'
                '.YouTubeChannelTabs',
            ) as tabs_cls:
                tabs_cls.return_value.browse_channel = AsyncMock(
                    return_value={'metadata': {}},
                )
                tabs_cls.return_value.scrape_loaded_tabs = AsyncMock(
                    return_value=(
                        set(), set(), set(), set(), set(), set(),
                    ),
                )
                with patch.object(
                    channel, 'parse_channel_video_data',
                ):
                    await channel.scrape_channel_content(
                        save_dir=save_dir,
                    )

            tabs_cls.assert_called_once_with(
                'UC1234567890abcdefghij',
                'http://proxy-one:8080',
            )

    async def test_video_count_falls_back_to_parsed_video_ids(
        self,
    ) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as save_dir:
            channel: YouTubeChannel = YouTubeChannel(
                channel_handle='input',
            )
            channel.channel_id = 'UC1234567890abcdefghij'

            with patch(
                'scrape_exchange.youtube.youtube_channel'
                '.YouTubeChannelTabs',
            ) as tabs_cls:
                tabs_cls.return_value.browse_channel = AsyncMock(
                    return_value={'metadata': {}},
                )
                tabs_cls.return_value.scrape_loaded_tabs = AsyncMock(
                    return_value=(
                        {'video-one', 'short-one'},
                        set(), set(), set(), set(), set(),
                    ),
                )
                with patch.object(
                    channel, 'parse_channel_video_data',
                ):
                    await channel.scrape_channel_content(
                        save_dir=save_dir,
                    )

            self.assertEqual(channel.video_count, 2)

    async def test_zero_video_count_falls_back_to_parsed_video_ids(
        self,
    ) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as save_dir:
            channel: YouTubeChannel = YouTubeChannel(
                channel_handle='input',
            )
            channel.channel_id = 'UC1234567890abcdefghij'

            def set_zero_video_count(page_data: dict) -> None:
                channel.video_count = 0

            with patch(
                'scrape_exchange.youtube.youtube_channel'
                '.YouTubeChannelTabs',
            ) as tabs_cls:
                tabs_cls.return_value.browse_channel = AsyncMock(
                    return_value={'metadata': {}},
                )
                tabs_cls.return_value.scrape_loaded_tabs = AsyncMock(
                    return_value=(
                        {'video-one', 'video-two', 'short-one'},
                        set(), set(), set(), set(), set(),
                    ),
                )
                with patch.object(
                    channel,
                    'parse_channel_video_data',
                    side_effect=set_zero_video_count,
                ):
                    await channel.scrape_channel_content(
                        save_dir=save_dir,
                    )

            self.assertEqual(channel.video_count, 3)

    async def test_positive_video_count_ignores_parsed_id_fallback(
        self,
    ) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as save_dir:
            channel: YouTubeChannel = YouTubeChannel(
                channel_handle='input',
            )
            channel.channel_id = 'UC1234567890abcdefghij'

            def set_positive_video_count(page_data: dict) -> None:
                channel.video_count = 99

            with patch(
                'scrape_exchange.youtube.youtube_channel'
                '.YouTubeChannelTabs',
            ) as tabs_cls:
                tabs_cls.return_value.browse_channel = AsyncMock(
                    return_value={'metadata': {}},
                )
                tabs_cls.return_value.scrape_loaded_tabs = AsyncMock(
                    return_value=(
                        {'video-one', 'short-one'},
                        set(), set(), set(), set(), set(),
                    ),
                )
                with patch.object(
                    channel,
                    'parse_channel_video_data',
                    side_effect=set_positive_video_count,
                ):
                    await channel.scrape_channel_content(
                        save_dir=save_dir,
                    )

            self.assertEqual(channel.video_count, 99)


class TestScrapeAboutPageViewCountFallback(
    unittest.IsolatedAsyncioTestCase,
):
    '''When the AboutTab response lacks viewCountText, scrape_about_page
    must fall back to parse_view_count(page_data) so we don't silently
    persist 0 for channels whose view count lives in pageHeaderRenderer.
    '''

    async def test_view_count_falls_back_to_page_header(
        self,
    ) -> None:
        about_renderer: dict = {
            'videoCountText': {'simpleText': '100 videos'},
            # viewCountText deliberately omitted
        }
        page_data: dict = {
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
                                                        'content':
                                                            '1,234,567'
                                                            ' views',
                                                    },
                                                },
                                            ],
                                        },
                                    ],
                                },
                            },
                        },
                    },
                },
            },
        }

        ch: YouTubeChannel = YouTubeChannel(channel_handle='Test')
        ch.url = 'https://www.youtube.com/@Test'
        mock_client: AsyncMock = AsyncMock()
        mock_client.get = AsyncMock(return_value='<html/>')
        mock_client.proxy = None
        ch.browse_client = mock_client

        with patch.object(
            ch, '_extract_initial_data', return_value=page_data,
        ), patch.object(
            ch, '_find_about_renderer', return_value=about_renderer,
        ), patch.object(
            ch, '_parse_thumbnails_banners',
        ), patch.object(
            YouTubeChannel,
            'extract_linked_channels',
            return_value=set(),
        ), patch.object(
            YouTubeChannel,
            'extract_channel_id',
            return_value='UC1234567890abcdefghij',
        ):
            await ch.scrape_about_page()

        self.assertEqual(ch.view_count, 1234567)

    async def test_reconciles_description_after_structured_links(
        self,
    ) -> None:
        page_data: dict = {
            'metadata': {
                'channelMetadataRenderer': {
                    'description': (
                        'Instagram: https://instagram.com/from-description'
                    ),
                    'vanityChannelUrl': (
                        'https://www.youtube.com/@Test'
                    ),
                },
            },
        }
        about_renderer: dict = {
            'links': [{
                'channelExternalLinkViewModel': {
                    'title': {'content': 'Twitter'},
                    'link': {
                        'content': 'https://twitter.com/structured',
                    },
                },
            }],
        }
        channel: YouTubeChannel = YouTubeChannel(channel_handle='Test')
        channel.url = 'https://www.youtube.com/@Test'
        mock_client: AsyncMock = AsyncMock()
        mock_client.get = AsyncMock(return_value='<html/>')
        mock_client.proxy = None
        channel.browse_client = mock_client

        with patch.object(
            channel, '_extract_initial_data', return_value=page_data,
        ), patch.object(
            channel, '_find_about_renderer', return_value=about_renderer,
        ), patch.object(
            channel, '_parse_thumbnails_banners',
        ), patch.object(
            YouTubeChannel,
            'extract_linked_channels',
            return_value=set(),
        ), patch.object(
            YouTubeChannel,
            'extract_channel_id',
            return_value='UC1234567890abcdefghij',
        ):
            await channel.scrape_about_page()

        links_by_name: dict[str, str] = {
            link.name: link.url for link in channel.external_urls
        }
        self.assertEqual(
            links_by_name,
            {
                'YouTube': 'https://www.youtube.com/@Test',
                'Twitter': 'https://twitter.com/structured',
                'Instagram': (
                    'https://instagram.com/from-description'
                ),
            },
        )


class TestTerminalChannelPageMessage(
    unittest.IsolatedAsyncioTestCase,
):
    '''Terminal channel page messages should be detected before parsing.'''

    def test_detects_exact_terminal_message(self) -> None:
        message: str = (
            'This channel was removed because it violated our '
            'Community Guidelines.'
        )
        self.assertEqual(
            terminal_channel_page_message(f'<html>{message}</html>'),
            message,
        )

    def test_detects_terminal_message_across_whitespace(self) -> None:
        message: str = (
            'This account has been terminated because we received '
            'multiple third-party claims of copyright infringement '
            'regarding material the user posted.'
        )
        page_contents: str = (
            '<div>This account has been terminated because we received\n'
            'multiple third-party claims of copyright infringement\t'
            'regarding material the user posted.</div>'
        )
        self.assertEqual(
            terminal_channel_page_message(page_contents),
            message,
        )

    def test_non_terminal_page_returns_none(self) -> None:
        self.assertIsNone(
            terminal_channel_page_message(
                '<html><body>Welcome to this channel.</body></html>',
            ),
        )

    async def test_scrape_about_page_logs_and_raises_message(self) -> None:
        message: str = 'This channel is not available.'
        ch: YouTubeChannel = YouTubeChannel(channel_handle='Test')
        ch.url = 'https://www.youtube.com/@Test'
        mock_client: AsyncMock = AsyncMock()
        mock_client.get = AsyncMock(
            return_value=f'<html><body>{message}</body></html>',
        )
        mock_client.proxy = None
        ch.browse_client = mock_client

        with patch.object(
            ch, '_extract_initial_data',
        ) as extract_initial_data, self.assertLogs(
            'scrape_exchange.youtube.youtube_channel',
            level='INFO',
        ) as logs:
            with self.assertRaises(RuntimeError) as caught:
                await ch.scrape_about_page()

        self.assertEqual(str(caught.exception), message)
        extract_initial_data.assert_not_called()
        self.assertEqual(
            getattr(logs.records[0], 'terminal_message'),
            message,
        )


class TestResolveChannelIdViaInnerTube(unittest.IsolatedAsyncioTestCase):
    '''
    Tests for ``YouTubeChannel._resolve_channel_id_via_innertube``,
    the InnerTube-backed handle→channel_id fallback used when /about
    HTML cannot supply ``channel_id``.
    '''

    def _make_channel(
        self,
        *,
        channel_handle: str | None = 'MrBeast',
        channel_id: str | None = None,
    ) -> 'YouTubeChannel':
        from scrape_exchange.youtube.youtube_channel import (
            YouTubeChannel,
        )
        ch = YouTubeChannel(channel_handle=channel_handle)
        ch.channel_id = channel_id
        return ch

    def _outcome_count(self, strategy: str, outcome: str) -> float:
        from scrape_exchange.youtube.youtube_channel import (
            METRIC_CHANNEL_HANDLE_RESOLVER_OUTCOMES,
        )
        total: float = 0.0
        for metric in (
            METRIC_CHANNEL_HANDLE_RESOLVER_OUTCOMES.collect()
        ):
            for sample in metric.samples:
                if sample.name.endswith('_total') and (
                    sample.labels.get('strategy') == strategy
                    and sample.labels.get('outcome') == outcome
                ):
                    total += sample.value
        return total

    async def test_noop_when_channel_id_already_set(self) -> None:
        ch = self._make_channel(
            channel_id='UCX6OQ3DkcsbYNE6H8uQQuVA',
        )
        from unittest.mock import patch
        with patch(
            'scrape_exchange.youtube.youtube_channel.'
            'pooled_innertube_for_entry',
        ) as mock_pool:
            ok = await ch._resolve_channel_id_via_innertube()
        self.assertTrue(ok)
        mock_pool.assert_not_called()

    async def test_noop_when_channel_handle_missing(self) -> None:
        ch = self._make_channel(channel_handle=None)
        from unittest.mock import patch
        with patch(
            'scrape_exchange.youtube.youtube_channel.'
            'pooled_innertube_for_entry',
        ) as mock_pool:
            ok = await ch._resolve_channel_id_via_innertube()
        self.assertFalse(ok)
        mock_pool.assert_not_called()

    async def test_handle_is_already_channel_id(self) -> None:
        ch = self._make_channel(
            channel_handle='UCX6OQ3DkcsbYNE6H8uQQuVA',
        )
        from unittest.mock import patch
        with patch(
            'scrape_exchange.youtube.youtube_channel.'
            'pooled_innertube_for_entry',
        ) as mock_pool:
            ok = await ch._resolve_channel_id_via_innertube()
        self.assertTrue(ok)
        self.assertEqual(
            ch.channel_id, 'UCX6OQ3DkcsbYNE6H8uQQuVA',
        )
        mock_pool.assert_not_called()

    async def test_resolves_via_browse(self) -> None:
        '''Strategy 1 (browse with handle) succeeds.'''
        ch = self._make_channel()
        from unittest.mock import patch, MagicMock, AsyncMock
        fake_client = MagicMock()
        fake_client.browse.return_value = {
            'metadata': {
                'channelMetadataRenderer': {
                    'externalId': 'UCX6OQ3DkcsbYNE6H8uQQuVA',
                },
            },
        }
        fake_limiter = MagicMock()
        fake_limiter.acquire = AsyncMock()
        before_hit = self._outcome_count('browse', 'hit')
        before_resolve_hit = self._outcome_count('resolve_url', 'hit')
        with patch(
            'scrape_exchange.youtube.youtube_channel.'
            'pooled_innertube_for_entry',
            return_value=fake_client,
        ), patch(
            'scrape_exchange.youtube.youtube_channel.'
            'YouTubeRateLimiter.get',
            return_value=fake_limiter,
        ):
            ok = await ch._resolve_channel_id_via_innertube()

        self.assertTrue(ok)
        self.assertEqual(
            ch.channel_id, 'UCX6OQ3DkcsbYNE6H8uQQuVA',
        )
        fake_client.browse.assert_called_once_with('@MrBeast')
        # navigation/resolve_url should not be called when browse
        # already supplied a usable channel_id.
        fake_client.adaptor.dispatch.assert_not_called()
        # Metric: browse strategy ticked 'hit' once; resolve_url
        # never ticked.
        self.assertEqual(
            self._outcome_count('browse', 'hit') - before_hit,
            1.0,
        )
        self.assertEqual(
            self._outcome_count('resolve_url', 'hit')
            - before_resolve_hit,
            0.0,
        )

    async def test_falls_back_to_resolve_url(self) -> None:
        '''Strategy 1 fails or returns no externalId; strategy 2
        (navigation/resolve_url) succeeds.'''
        ch = self._make_channel()
        from unittest.mock import patch, MagicMock, AsyncMock
        fake_client = MagicMock()
        # browse returns an empty/unhelpful response.
        fake_client.browse.return_value = {}
        fake_client.adaptor.dispatch.return_value = {
            'endpoint': {
                'browseEndpoint': {
                    'browseId': 'UCX6OQ3DkcsbYNE6H8uQQuVA',
                },
            },
        }
        fake_limiter = MagicMock()
        fake_limiter.acquire = AsyncMock()
        before_browse_miss = self._outcome_count('browse', 'miss')
        before_resolve_hit = self._outcome_count(
            'resolve_url', 'hit',
        )
        with patch(
            'scrape_exchange.youtube.youtube_channel.'
            'pooled_innertube_for_entry',
            return_value=fake_client,
        ), patch(
            'scrape_exchange.youtube.youtube_channel.'
            'YouTubeRateLimiter.get',
            return_value=fake_limiter,
        ):
            ok = await ch._resolve_channel_id_via_innertube()

        self.assertTrue(ok)
        self.assertEqual(
            ch.channel_id, 'UCX6OQ3DkcsbYNE6H8uQQuVA',
        )
        fake_client.browse.assert_called_once()
        fake_client.adaptor.dispatch.assert_called_once_with(
            'navigation/resolve_url',
            body={'url': 'https://www.youtube.com/@MrBeast'},
        )
        # Metric: browse miss ticked once; resolve_url hit ticked
        # once.
        self.assertEqual(
            self._outcome_count('browse', 'miss')
            - before_browse_miss,
            1.0,
        )
        self.assertEqual(
            self._outcome_count('resolve_url', 'hit')
            - before_resolve_hit,
            1.0,
        )

    async def test_browse_raises_then_resolve_url_succeeds(self) -> None:
        ch = self._make_channel()
        from unittest.mock import patch, MagicMock, AsyncMock
        fake_client = MagicMock()
        fake_client.browse.side_effect = RuntimeError('browse boom')
        fake_client.adaptor.dispatch.return_value = {
            'endpoint': {
                'browseEndpoint': {
                    'browseId': 'UCX6OQ3DkcsbYNE6H8uQQuVA',
                },
            },
        }
        fake_limiter = MagicMock()
        fake_limiter.acquire = AsyncMock()
        before_browse_err = self._outcome_count('browse', 'error')
        before_resolve_hit = self._outcome_count(
            'resolve_url', 'hit',
        )
        with patch(
            'scrape_exchange.youtube.youtube_channel.'
            'pooled_innertube_for_entry',
            return_value=fake_client,
        ), patch(
            'scrape_exchange.youtube.youtube_channel.'
            'YouTubeRateLimiter.get',
            return_value=fake_limiter,
        ):
            ok = await ch._resolve_channel_id_via_innertube()

        self.assertTrue(ok)
        self.assertEqual(
            ch.channel_id, 'UCX6OQ3DkcsbYNE6H8uQQuVA',
        )
        # Metric: browse 'error' ticked, resolve_url 'hit' ticked.
        self.assertEqual(
            self._outcome_count('browse', 'error')
            - before_browse_err,
            1.0,
        )
        self.assertEqual(
            self._outcome_count('resolve_url', 'hit')
            - before_resolve_hit,
            1.0,
        )

    async def test_both_strategies_fail(self) -> None:
        ch = self._make_channel()
        from unittest.mock import patch, MagicMock, AsyncMock
        fake_client = MagicMock()
        fake_client.browse.return_value = {}
        fake_client.adaptor.dispatch.return_value = {}
        fake_limiter = MagicMock()
        fake_limiter.acquire = AsyncMock()
        before_browse_miss = self._outcome_count('browse', 'miss')
        before_resolve_miss = self._outcome_count(
            'resolve_url', 'miss',
        )
        with patch(
            'scrape_exchange.youtube.youtube_channel.'
            'pooled_innertube_for_entry',
            return_value=fake_client,
        ), patch(
            'scrape_exchange.youtube.youtube_channel.'
            'YouTubeRateLimiter.get',
            return_value=fake_limiter,
        ):
            ok = await ch._resolve_channel_id_via_innertube()

        self.assertFalse(ok)
        self.assertIsNone(ch.channel_id)
        # Metric: both strategies ticked 'miss' once.
        self.assertEqual(
            self._outcome_count('browse', 'miss')
            - before_browse_miss,
            1.0,
        )
        self.assertEqual(
            self._outcome_count('resolve_url', 'miss')
            - before_resolve_miss,
            1.0,
        )

    async def test_handle_with_at_prefix_normalized(self) -> None:
        '''Handles already prefixed with @ should not be doubled.'''
        ch = self._make_channel(channel_handle='@MrBeast')
        # YouTubeChannel.__init__ strips leading '@', so the
        # stored handle is 'MrBeast'; this is just a sanity check
        # that the resolver builds a single-@ URL/browse_id.
        self.assertEqual(ch.channel_handle, 'MrBeast')
        from unittest.mock import patch, MagicMock, AsyncMock
        fake_client = MagicMock()
        fake_client.browse.return_value = {
            'metadata': {
                'channelMetadataRenderer': {
                    'externalId': 'UCX6OQ3DkcsbYNE6H8uQQuVA',
                },
            },
        }
        fake_limiter = MagicMock()
        fake_limiter.acquire = AsyncMock()
        with patch(
            'scrape_exchange.youtube.youtube_channel.'
            'pooled_innertube_for_entry',
            return_value=fake_client,
        ), patch(
            'scrape_exchange.youtube.youtube_channel.'
            'YouTubeRateLimiter.get',
            return_value=fake_limiter,
        ):
            await ch._resolve_channel_id_via_innertube()
        fake_client.browse.assert_called_once_with('@MrBeast')


if __name__ == '__main__':
    unittest.main()
