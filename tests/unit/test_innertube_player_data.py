'''
Unit tests for ``InnerTubeVideoParser._apply_player_data``.

The mapping was previously inlined in ``scrape()``; extracting it as a
pure function lets us cover one specific class of bug without
mocking the InnerTube transport.

Bug class: identity fields (``channel_id``, ``channel_handle``,
``channel_url``, ``title``, ``description``, ``url``, ``embed_url``)
must NOT be clobbered when YouTube's ``player`` response includes the
key with an empty/None value — common for unavailable, deleted, or
region-blocked videos.  The previous code path used
``video_details.get('key', existing)``, which only honours the
default when the key is absent; this test set asserts the new
``response or existing`` semantics work for the full set of keys.
'''

import unittest

from scrape_exchange.youtube.youtube_video import YouTubeVideo
from scrape_exchange.youtube import youtube_video_innertube as innertube_mod
from scrape_exchange.youtube.youtube_video_innertube import (
    InnerTubeVideoParser,
    _classify_player_reason,
    _player_response_shape,
)


def _video_with_rss_data() -> YouTubeVideo:
    '''
    Build a YouTubeVideo populated as if ``from_rss_entry`` had just
    run on a feed whose ``<entry>`` carried the full set of identity
    fields.  Each test then runs ``_apply_player_data`` against a
    crafted ``player_data`` dict to verify the merge behaviour.
    '''
    video: YouTubeVideo = YouTubeVideo(
        video_id='vid12345', channel_handle='OriginalHandle',
    )
    video.channel_id = 'UCabcdefghijABCDEFGHIJ'
    video.channel_url = 'https://youtube.com/@OriginalHandle'
    video.title = 'Original RSS title'
    video.description = 'Original RSS description'
    video.url = 'https://youtu.be/vid12345'
    video.embed_url = 'https://www.youtube.com/embed/vid12345'
    return video


class TestApplyPlayerDataIdentityFieldsNotClobbered(
    unittest.TestCase,
):
    '''
    These cases reproduce the runtime crash that motivated the fix:
    the bulk uploader received video-min files where InnerTube had
    overwritten RSS-set channel_id/channel_handle with empty
    strings, causing ``resolve_video_upload_handle`` to call
    ``fallback_handle('')`` and abort the whole bulk batch.
    '''

    def test_empty_string_channel_id_does_not_clobber(self) -> None:
        video: YouTubeVideo = _video_with_rss_data()
        player_data: dict = {
            'videoDetails': {'channelId': ''},
            'microformat': {'playerMicroformatRenderer': {}},
        }
        InnerTubeVideoParser._apply_player_data(video, player_data)
        self.assertEqual(video.channel_id, 'UCabcdefghijABCDEFGHIJ')

    def test_none_channel_id_does_not_clobber(self) -> None:
        video: YouTubeVideo = _video_with_rss_data()
        player_data: dict = {
            'videoDetails': {'channelId': None},
            'microformat': {'playerMicroformatRenderer': {}},
        }
        InnerTubeVideoParser._apply_player_data(video, player_data)
        self.assertEqual(video.channel_id, 'UCabcdefghijABCDEFGHIJ')

    def test_empty_string_author_does_not_clobber_handle(
        self,
    ) -> None:
        video: YouTubeVideo = _video_with_rss_data()
        player_data: dict = {
            'videoDetails': {'author': ''},
            'microformat': {'playerMicroformatRenderer': {}},
        }
        InnerTubeVideoParser._apply_player_data(video, player_data)
        self.assertEqual(video.channel_handle, 'OriginalHandle')

    def test_missing_keys_preserve_existing_values(self) -> None:
        '''Sanity: when InnerTube returns no identity keys at all
        (the original "default" semantics), existing values are
        kept too — so the fix is strictly more permissive, not
        regressing the absent-key case.'''
        video: YouTubeVideo = _video_with_rss_data()
        player_data: dict = {
            'videoDetails': {},
            'microformat': {'playerMicroformatRenderer': {}},
        }
        InnerTubeVideoParser._apply_player_data(video, player_data)
        self.assertEqual(video.channel_id, 'UCabcdefghijABCDEFGHIJ')
        self.assertEqual(video.channel_handle, 'OriginalHandle')
        self.assertEqual(video.title, 'Original RSS title')
        self.assertEqual(video.description, 'Original RSS description')

    def test_present_values_overwrite_existing(self) -> None:
        '''Happy path: when InnerTube returns concrete identity
        values they replace the RSS-sourced placeholders.'''
        video: YouTubeVideo = _video_with_rss_data()
        player_data: dict = {
            'videoDetails': {
                'channelId': 'UCnewchannelidNEW123456',
                'author': 'CanonicalHandle',
                'title': 'Canonical title',
                'shortDescription': 'Canonical description',
            },
            'microformat': {
                'playerMicroformatRenderer': {
                    'canonicalUrl':
                        'https://www.youtube.com/watch?v=vid12345',
                    'ownerProfileUrl':
                        'https://youtube.com/@CanonicalHandle',
                },
            },
        }
        InnerTubeVideoParser._apply_player_data(video, player_data)
        self.assertEqual(video.channel_id, 'UCnewchannelidNEW123456')
        self.assertEqual(video.channel_handle, 'CanonicalHandle')
        self.assertEqual(video.title, 'Canonical title')
        self.assertEqual(video.description, 'Canonical description')
        self.assertEqual(
            video.url,
            'https://www.youtube.com/watch?v=vid12345',
        )
        self.assertEqual(
            video.channel_url,
            'https://youtube.com/@CanonicalHandle',
        )

    def test_empty_microformat_url_fields_do_not_clobber(
        self,
    ) -> None:
        '''Same fix applied to the microformat-sourced URL fields:
        an empty ``canonicalUrl``, ``ownerProfileUrl``, or embed
        ``iframeUrl`` must not blank an RSS-sourced value.'''
        video: YouTubeVideo = _video_with_rss_data()
        player_data: dict = {
            'videoDetails': {},
            'microformat': {
                'playerMicroformatRenderer': {
                    'canonicalUrl': '',
                    'ownerProfileUrl': '',
                    'embed': {'iframeUrl': ''},
                },
            },
        }
        InnerTubeVideoParser._apply_player_data(video, player_data)
        self.assertEqual(
            video.url, 'https://youtu.be/vid12345',
        )
        self.assertEqual(
            video.channel_url,
            'https://youtube.com/@OriginalHandle',
        )
        self.assertEqual(
            video.embed_url,
            'https://www.youtube.com/embed/vid12345',
        )

    def test_owner_profile_url_sets_handle_when_missing(self) -> None:
        video: YouTubeVideo = YouTubeVideo(
            video_id='vid12345', channel_handle=None,
        )
        player_data: dict = {
            'videoDetails': {},
            'microformat': {
                'playerMicroformatRenderer': {
                    'ownerProfileUrl':
                        'https://www.youtube.com/@CanonicalHandle',
                },
            },
        }
        InnerTubeVideoParser._apply_player_data(video, player_data)
        self.assertEqual(video.channel_handle, 'CanonicalHandle')

    def test_owner_profile_url_handle_wins_over_display_author(
        self,
    ) -> None:
        video: YouTubeVideo = YouTubeVideo(
            video_id='vid12345', channel_handle=None,
        )
        player_data: dict = {
            'videoDetails': {
                'author': 'Display Name With Spaces',
            },
            'microformat': {
                'playerMicroformatRenderer': {
                    'ownerProfileUrl':
                        'https://www.youtube.com/@CanonicalHandle/videos',
                },
            },
        }
        InnerTubeVideoParser._apply_player_data(video, player_data)
        self.assertEqual(video.channel_handle, 'CanonicalHandle')

    def test_empty_title_and_description_do_not_clobber(
        self,
    ) -> None:
        video: YouTubeVideo = _video_with_rss_data()
        player_data: dict = {
            'videoDetails': {'title': '', 'shortDescription': ''},
            'microformat': {'playerMicroformatRenderer': {}},
        }
        InnerTubeVideoParser._apply_player_data(video, player_data)
        self.assertEqual(video.title, 'Original RSS title')
        self.assertEqual(
            video.description, 'Original RSS description',
        )


class TestVideoThumbnailFiltering(unittest.TestCase):
    '''Bug #1: thumbnail entries from YouTube without a ``url``
    field were silently persisted with ``url=None``. The yt-dlp /
    RSS path filters them via YouTubeVideo._parse_thumbnails;
    the InnerTube path did not. Since innertube is the default
    backend, every video scrape was vulnerable.
    '''

    def test_thumbnails_without_url_are_skipped(self) -> None:
        video: YouTubeVideo = _video_with_rss_data()
        # Two valid + one missing-url thumbnail.
        player_data: dict = {
            'videoDetails': {
                'thumbnail': {
                    'thumbnails': [
                        {
                            'url': (
                                'https://i.ytimg.com/vi/'
                                'vid/default.jpg'
                            ),
                            'width': 120, 'height': 90,
                        },
                        {
                            'width': 320, 'height': 180,
                        },  # no url
                        {
                            'url': (
                                'https://i.ytimg.com/vi/'
                                'vid/hq.jpg'
                            ),
                            'width': 480, 'height': 360,
                        },
                    ],
                },
            },
            'microformat': {'playerMicroformatRenderer': {}},
        }
        InnerTubeVideoParser._apply_player_data(
            video, player_data,
        )
        # Only the two with a real url should land on the video.
        self.assertEqual(len(video.thumbnails), 2)
        for thumb in video.thumbnails.values():
            self.assertIsNotNone(thumb.url)


class TestPlayerResponseInstrumentation(unittest.TestCase):
    '''Classify sparse InnerTube ``player()`` responses without using
    raw YouTube reason text as a Prometheus label.
    '''

    def test_unplayable_status_is_unavailable(self) -> None:
        player_data: dict = {
            'playabilityStatus': {
                'status': 'UNPLAYABLE',
                'reason': 'This video is unavailable',
            },
            'videoDetails': {},
        }
        self.assertEqual(
            _classify_player_reason(player_data),
            'unavailable',
        )

    def test_login_required_age_gate_is_restricted(self) -> None:
        player_data: dict = {
            'playabilityStatus': {
                'status': 'LOGIN_REQUIRED',
                'reason': 'Sign in to confirm your age',
            },
            'videoDetails': {},
        }
        self.assertEqual(
            _classify_player_reason(player_data),
            'restricted',
        )

    def test_bot_pressure_reason_is_client_block(self) -> None:
        player_data: dict = {
            'playabilityStatus': {
                'status': 'LOGIN_REQUIRED',
                'reason': (
                    'Sign in to confirm you are not a bot. '
                    'This helps protect our community.'
                ),
            },
            'videoDetails': {},
        }
        self.assertEqual(
            _classify_player_reason(player_data),
            'client_block',
        )

    def test_response_shape_reports_useful_video_details(self) -> None:
        player_data: dict = {
            'videoDetails': {
                'title': 'A video',
                'channelId': 'UCabcdef',
                'thumbnail': {
                    'thumbnails': [
                        {'url': 'https://i.ytimg.com/vi/x/default.jpg'},
                    ],
                },
            },
        }
        self.assertEqual(
            _player_response_shape(player_data),
            {
                'has_title': True,
                'has_thumbnails': True,
                'has_channel_id': True,
            },
        )

    def test_record_player_response_uses_bounded_labels(self) -> None:
        calls: list[dict] = []

        class FakeCounter:
            def labels(self, **kwargs: object) -> 'FakeCounter':
                calls.append(kwargs)
                return self

            def inc(self) -> None:
                return None

        player_data: dict = {
            'playabilityStatus': {
                'status': 'LOGIN_REQUIRED',
                'reason': (
                    'Sign in to confirm you are not a bot. '
                    'This helps protect our community.'
                ),
            },
            'videoDetails': {},
        }

        original = innertube_mod.METRIC_INNERTUBE_PLAYER_RESPONSES
        try:
            innertube_mod.METRIC_INNERTUBE_PLAYER_RESPONSES = (
                FakeCounter()
            )
            with self.assertLogs(level='WARNING') as logs:
                innertube_mod._record_player_response(
                    'vid123',
                    player_data,
                    proxy_ip='203.0.113.10',
                    proxy_file='proxy-a',
                )
        finally:
            innertube_mod.METRIC_INNERTUBE_PLAYER_RESPONSES = original

        self.assertEqual(calls[0]['status'], 'login_required')
        self.assertEqual(calls[0]['reason_class'], 'client_block')
        self.assertEqual(calls[0]['has_title'], 'false')
        self.assertEqual(calls[0]['has_thumbnails'], 'false')
        self.assertIn(
            'InnerTube PLAYER returned sparse videoDetails',
            logs.output[0],
        )
        self.assertEqual(
            getattr(logs.records[0], 'proxy_ip'), '203.0.113.10',
        )


class TestNextDataSetsChannelThumbnail(unittest.TestCase):
    '''Bug #2: callers pass ``channel_thumbnail=None`` because they
    don't have it; the InnerTube parser must extract it from the
    ``next`` response so the persisted video has a populated
    channel avatar.'''

    def _build_parser_with_video(self) -> InnerTubeVideoParser:
        video: YouTubeVideo = YouTubeVideo(
            video_id='vid', channel_handle='OriginalHandle',
        )
        # Bypass the live-network constructor; the parser's
        # ``_parse_next_data`` is a method on the instance and
        # only needs ``self.video`` and proper subclass routing.
        parser: InnerTubeVideoParser = (
            InnerTubeVideoParser.__new__(InnerTubeVideoParser)
        )
        parser.video = video
        return parser

    def test_largest_owner_thumbnail_wins(self) -> None:
        parser = self._build_parser_with_video()
        next_data: dict = {
            'contents': {
                'twoColumnWatchNextResults': {
                    'results': {
                        'results': {
                            'contents': [
                                {'videoSecondaryInfoRenderer': {
                                    'owner': {
                                        'videoOwnerRenderer': {
                                            'thumbnail': {
                                                'thumbnails': [
                                                    {
                                                        'url': (
                                                            'https://x/'
                                                            's48'
                                                        ),
                                                        'width': 48,
                                                        'height': 48,
                                                    },
                                                    {
                                                        'url': (
                                                            'https://x/'
                                                            's176'
                                                        ),
                                                        'width': 176,
                                                        'height': 176,
                                                    },
                                                    {
                                                        'url': (
                                                            'https://x/'
                                                            's88'
                                                        ),
                                                        'width': 88,
                                                        'height': 88,
                                                    },
                                                ],
                                            },
                                        },
                                    },
                                }},
                            ],
                        },
                    },
                },
            },
        }
        parser._parse_next_data(next_data)
        self.assertIsNotNone(
            parser.video.channel_thumbnail_asset,
        )
        self.assertEqual(
            parser.video.channel_thumbnail_asset.width, 176,
        )
        self.assertEqual(
            parser.video.channel_thumbnail_url,
            'https://x/s176',
        )

    def test_owner_renderer_sets_channel_identity(self) -> None:
        parser = self._build_parser_with_video()
        parser.video.channel_id = None
        parser.video.channel_handle = ''
        parser.video.channel_url = None
        next_data: dict = {
            'contents': {
                'twoColumnWatchNextResults': {
                    'results': {
                        'results': {
                            'contents': [
                                {'videoSecondaryInfoRenderer': {
                                    'owner': {
                                        'videoOwnerRenderer': {
                                            'navigationEndpoint': {
                                                'commandMetadata': {
                                                    'webCommandMetadata': {
                                                        'url': (
                                                            '/@Canonical'
                                                        ),
                                                    },
                                                },
                                                'browseEndpoint': {
                                                    'browseId': (
                                                        'UC1234567890'
                                                        'abcdefghij'
                                                    ),
                                                    'canonicalBaseUrl': (
                                                        '/@Canonical'
                                                    ),
                                                },
                                            },
                                        },
                                    },
                                }},
                            ],
                        },
                    },
                },
            },
        }

        parser._parse_next_data(next_data)

        self.assertEqual(
            parser.video.channel_id,
            'UC1234567890abcdefghij',
        )
        self.assertEqual(parser.video.channel_handle, 'Canonical')
        self.assertEqual(
            parser.video.channel_url,
            'https://www.youtube.com/@Canonical',
        )

    def test_owner_title_run_endpoint_sets_channel_identity(
        self,
    ) -> None:
        parser = self._build_parser_with_video()
        parser.video.channel_id = None
        parser.video.channel_handle = ''
        parser.video.channel_url = None
        next_data: dict = {
            'contents': {
                'twoColumnWatchNextResults': {
                    'results': {
                        'results': {
                            'contents': [
                                {'videoSecondaryInfoRenderer': {
                                    'owner': {
                                        'videoOwnerRenderer': {
                                            'title': {
                                                'runs': [{
                                                    'text': (
                                                        'Display Name'
                                                    ),
                                                    'navigationEndpoint': {
                                                        'browseEndpoint': {
                                                            'browseId': (
                                                                'UCabc'
                                                            ),
                                                            'canonicalBaseUrl':
                                                                '/@FromTitle',
                                                        },
                                                    },
                                                }],
                                            },
                                        },
                                    },
                                }},
                            ],
                        },
                    },
                },
            },
        }

        parser._parse_next_data(next_data)

        self.assertEqual(parser.video.channel_id, 'UCabc')
        self.assertEqual(parser.video.channel_handle, 'FromTitle')
        self.assertEqual(
            parser.video.channel_url,
            'https://www.youtube.com/@FromTitle',
        )

    def test_url_less_owner_thumbnails_filtered(self) -> None:
        parser = self._build_parser_with_video()
        next_data: dict = {
            'contents': {
                'twoColumnWatchNextResults': {
                    'results': {
                        'results': {
                            'contents': [
                                {'videoSecondaryInfoRenderer': {
                                    'owner': {
                                        'videoOwnerRenderer': {
                                            'thumbnail': {
                                                'thumbnails': [
                                                    {
                                                        'width': 999,
                                                        'height': 999,
                                                    },  # no url
                                                    {
                                                        'url': (
                                                            'https://x/'
                                                            'real'
                                                        ),
                                                        'width': 88,
                                                        'height': 88,
                                                    },
                                                ],
                                            },
                                        },
                                    },
                                }},
                            ],
                        },
                    },
                },
            },
        }
        parser._parse_next_data(next_data)
        self.assertEqual(
            parser.video.channel_thumbnail_asset.width, 88,
        )
        self.assertEqual(
            parser.video.channel_thumbnail_url,
            'https://x/real',
        )

    def test_missing_owner_renderer_keeps_existing_value(
        self,
    ) -> None:
        parser = self._build_parser_with_video()
        # A pre-existing channel_thumbnail (e.g. passed by caller)
        # must not be wiped out when the InnerTube response lacks
        # videoOwnerRenderer.
        from scrape_exchange.youtube.youtube_thumbnail import (
            YouTubeThumbnail,
        )
        existing: YouTubeThumbnail = YouTubeThumbnail({
            'url': 'https://existing/thumb', 'width': 100,
            'height': 100,
        })
        parser.video.channel_thumbnail_asset = existing
        parser.video.channel_thumbnail_url = existing.url

        next_data: dict = {
            'contents': {
                'twoColumnWatchNextResults': {
                    'results': {
                        'results': {
                            'contents': [],  # nothing useful
                        },
                    },
                },
            },
        }
        parser._parse_next_data(next_data)
        self.assertIs(
            parser.video.channel_thumbnail_asset, existing,
        )


if __name__ == '__main__':
    unittest.main()
