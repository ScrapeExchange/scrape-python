'''
Unit tests for YouTubeChannelTabs.

These tests use mocked InnerTube responses to cover all code-paths
without making real network calls.
'''

import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.youtube.youtube_channel_tabs import YouTubeChannelTabs
from scrape_exchange.youtube.youtube_types import YouTubeChannelPageType


CHANNEL_ID: str = 'UCtest123'


class TestYouTubeChannelTabsInit(unittest.TestCase):
    def test_init_sets_channel_id(self) -> None:
        tabs = YouTubeChannelTabs(CHANNEL_ID)
        self.assertEqual(tabs.channel_id, CHANNEL_ID)
        self.assertIsNotNone(tabs.client)
        self.assertEqual(tabs.tabs, [])


class TestExtractVideoId(unittest.TestCase):
    def setUp(self) -> None:
        self.tabs = YouTubeChannelTabs(CHANNEL_ID)

    def test_regular_video(self) -> None:
        item = {
            'richItemRenderer': {
                'content': {
                    'videoRenderer': {'videoId': 'abc123'}
                }
            }
        }
        self.assertEqual(self.tabs._extract_video_id(item, 'videos'), 'abc123')

    def test_live_video(self) -> None:
        item = {
            'richItemRenderer': {
                'content': {
                    'videoRenderer': {'videoId': 'live456'}
                }
            }
        }
        self.assertEqual(self.tabs._extract_video_id(item, 'live'), 'live456')

    def test_shorts_video(self) -> None:
        item = {
            'richItemRenderer': {
                'content': {
                    'shortsLockupViewModel': {
                        'onTap': {
                            'innertubeCommand': {
                                'commandMetadata': {
                                    'webCommandMetadata': {
                                        'url': '/shorts/short789'
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        self.assertEqual(
            self.tabs._extract_video_id(item, 'shorts'), 'short789'
        )

    def test_shorts_missing_url(self) -> None:
        item = {'richItemRenderer': {'content': {}}}
        self.assertIsNone(self.tabs._extract_video_id(item, 'shorts'))

    def test_shorts_url_not_starting_with_shorts(self) -> None:
        item = {
            'richItemRenderer': {
                'content': {
                    'shortsLockupViewModel': {
                        'onTap': {
                            'innertubeCommand': {
                                'commandMetadata': {
                                    'webCommandMetadata': {
                                        'url': '/watch?v=xyz'
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        self.assertIsNone(self.tabs._extract_video_id(item, 'shorts'))

    def test_regular_video_missing(self) -> None:
        item = {'richItemRenderer': {'content': {}}}
        self.assertIsNone(self.tabs._extract_video_id(item, 'videos'))

    def test_empty_item(self) -> None:
        self.assertIsNone(self.tabs._extract_video_id({}, 'videos'))
        self.assertIsNone(self.tabs._extract_video_id({}, 'shorts'))


class TestGetPodcastIds(unittest.TestCase):
    def setUp(self) -> None:
        self.tabs = YouTubeChannelTabs(CHANNEL_ID)

    def test_extracts_podcast_ids(self) -> None:
        contents = [
            {
                'richItemRenderer': {
                    'content': {
                        'lockupViewModel': {'contentId': 'pod1'}
                    }
                }
            },
            {
                'richItemRenderer': {
                    'content': {
                        'lockupViewModel': {'contentId': 'pod2'}
                    }
                }
            },
        ]
        ids = self.tabs._get_podcast_ids(contents)
        self.assertEqual(ids, {'pod1', 'pod2'})

    def test_skips_missing_content_id(self) -> None:
        contents = [
            {
                'richItemRenderer': {
                    'content': {
                        'lockupViewModel': {}
                    }
                }
            },
        ]
        ids = self.tabs._get_podcast_ids(contents)
        self.assertEqual(ids, set())

    def test_empty_contents(self) -> None:
        self.assertEqual(self.tabs._get_podcast_ids([]), set())


class TestGetPlaylistItems(unittest.TestCase):
    def setUp(self) -> None:
        self.tabs = YouTubeChannelTabs(CHANNEL_ID)

    def test_parses_playlists(self) -> None:
        tab_renderer = {
            'content': {
                'sectionListRenderer': {
                    'contents': [
                        {
                            'itemSectionRenderer': {
                                'contents': [
                                    {
                                        'gridRenderer': {
                                            'items': [
                                                {
                                                    'lockupViewModel': {
                                                        'contentType': 'LOCKUP_CONTENT_TYPE_PLAYLIST',
                                                        'contentId': 'PLabc',
                                                        'metadata': {
                                                            'lockupMetadataViewModel': {
                                                                'title': {
                                                                    'content': 'My Playlist'
                                                                },
                                                                'metadata': {
                                                                    'contentMetadataViewModel': {
                                                                        'metadataRows': [
                                                                            {
                                                                                'metadataParts': [
                                                                                    {
                                                                                        'text': {
                                                                                            'content': '5 videos'
                                                                                        }
                                                                                    }
                                                                                ]
                                                                            }
                                                                        ]
                                                                    }
                                                                }
                                                            }
                                                        },
                                                        'contentImage': {
                                                            'collectionThumbnailViewModel': {
                                                                'primaryThumbnail': {
                                                                    'thumbnailViewModel': {
                                                                        'image': {
                                                                            'sources': [
                                                                                {
                                                                                    'url': 'https://img.jpg',
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
                                            ]
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                }
            }
        }
        playlists = self.tabs._get_playlist_items(tab_renderer, CHANNEL_ID)
        self.assertEqual(len(playlists), 1)
        pl = next(iter(playlists))
        self.assertEqual(pl.playlist_id, 'PLabc')
        self.assertEqual(pl.title, 'My Playlist')

    def test_empty_sections(self) -> None:
        tab_renderer = {'content': {'sectionListRenderer': {'contents': []}}}
        playlists = self.tabs._get_playlist_items(tab_renderer, CHANNEL_ID)
        self.assertEqual(len(playlists), 0)

    def test_missing_content(self) -> None:
        playlists = self.tabs._get_playlist_items({}, CHANNEL_ID)
        self.assertEqual(len(playlists), 0)


class TestGetCourseItems(unittest.TestCase):
    def setUp(self) -> None:
        self.tabs = YouTubeChannelTabs(CHANNEL_ID)

    def test_empty_contents(self) -> None:
        tab_renderer = {'content': {'richGridRenderer': {'contents': []}}}
        courses = self.tabs._get_course_items(tab_renderer, CHANNEL_ID)
        self.assertEqual(len(courses), 0)

    def test_missing_content(self) -> None:
        courses = self.tabs._get_course_items({}, CHANNEL_ID)
        self.assertEqual(len(courses), 0)

    def test_parses_valid_course(self) -> None:
        tab_renderer = {
            'content': {
                'richGridRenderer': {
                    'contents': [
                        {
                            'richItemRenderer': {
                                'content': {
                                    'playlistRenderer': {
                                        'playlistId': 'PLabc123',
                                        'title': {
                                            'simpleText': 'My Course'
                                        },
                                        'videoCount': '5',
                                    }
                                }
                            }
                        }
                    ]
                }
            }
        }
        courses = self.tabs._get_course_items(tab_renderer, CHANNEL_ID)
        self.assertEqual(len(courses), 1)


class TestGetPostItems(unittest.TestCase):
    def setUp(self) -> None:
        self.tabs = YouTubeChannelTabs(CHANNEL_ID)

    def test_empty_sections(self) -> None:
        tab_renderer = {'content': {'sectionListRenderer': {'contents': []}}}
        posts = self.tabs._get_post_items(tab_renderer, CHANNEL_ID)
        self.assertEqual(len(posts), 0)

    def test_missing_content(self) -> None:
        posts = self.tabs._get_post_items({}, CHANNEL_ID)
        self.assertEqual(len(posts), 0)

    def test_parses_valid_post(self) -> None:
        tab_renderer = {
            'content': {
                'sectionListRenderer': {
                    'contents': [
                        {
                            'itemSectionRenderer': {
                                'contents': [
                                    {
                                        'backstagePostThreadRenderer': {
                                            'post': {
                                                'backstagePostRenderer': {
                                                    'postId': 'post123',
                                                    'contentText': {
                                                        'runs': [
                                                            {
                                                                'text':
                                                                    'Hello'
                                                            }
                                                        ]
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
        posts = self.tabs._get_post_items(tab_renderer, CHANNEL_ID)
        self.assertEqual(len(posts), 1)


class TestGetProductItems(unittest.TestCase):
    def setUp(self) -> None:
        self.tabs = YouTubeChannelTabs(CHANNEL_ID)

    def test_parses_products(self) -> None:
        tab_renderer = {
            'content': {
                'sectionListRenderer': {
                    'contents': [
                        {
                            'itemSectionRenderer': {
                                'contents': [
                                    {
                                        'shelfRenderer': {
                                            'content': {
                                                'gridRenderer': {
                                                    'items': [
                                                        {
                                                            'verticalProductCardRenderer': {
                                                                'title': 'Mug',
                                                                'price': '$9.99',
                                                                'merchantName': 'Shop',
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
        products = self.tabs._get_product_items(tab_renderer, CHANNEL_ID)
        self.assertEqual(len(products), 1)
        product = next(iter(products))
        self.assertEqual(product.title, 'Mug')
        self.assertEqual(product.price, '$9.99')

    def test_empty_sections(self) -> None:
        tab_renderer = {'content': {'sectionListRenderer': {'contents': []}}}
        products = self.tabs._get_product_items(tab_renderer, CHANNEL_ID)
        self.assertEqual(len(products), 0)

    def test_missing_content(self) -> None:
        products = self.tabs._get_product_items({}, CHANNEL_ID)
        self.assertEqual(len(products), 0)


class TestGetContinuationToken(unittest.TestCase):
    def setUp(self) -> None:
        self.tabs = YouTubeChannelTabs(CHANNEL_ID)

    def test_extracts_token(self) -> None:
        last_item = {
            'continuationItemRenderer': {
                'continuationEndpoint': {
                    'continuationCommand': {
                        'token': 'abc_token_123'
                    }
                }
            }
        }
        self.assertEqual(
            self.tabs.get_continuation_token(last_item), 'abc_token_123'
        )

    def test_missing_renderer(self) -> None:
        self.assertEqual(self.tabs.get_continuation_token({}), '')

    def test_missing_token_key(self) -> None:
        last_item = {
            'continuationItemRenderer': {
                'continuationEndpoint': {
                    'continuationCommand': {}
                }
            }
        }
        self.assertEqual(self.tabs.get_continuation_token(last_item), '')


class TestGetTab(unittest.TestCase):
    def setUp(self) -> None:
        self.tabs = YouTubeChannelTabs(CHANNEL_ID)

    def test_finds_tab_by_title(self) -> None:
        page_data = {
            'contents': {
                'twoColumnBrowseResultsRenderer': {
                    'tabs': [
                        {'tabRenderer': {'title': 'Home'}},
                        {'tabRenderer': {'title': 'Videos', 'extra': True}},
                    ]
                }
            }
        }
        result = self.tabs.get_tab(page_data, 'videos')
        self.assertEqual(result['title'], 'Videos')
        self.assertTrue(result['extra'])

    def test_raises_when_tab_not_found(self) -> None:
        page_data = {
            'contents': {
                'twoColumnBrowseResultsRenderer': {
                    'tabs': [
                        {'tabRenderer': {'title': 'Home'}},
                    ]
                }
            }
        }
        with self.assertRaises(RuntimeError):
            self.tabs.get_tab(page_data, 'store')

    def test_raises_when_no_tabs(self) -> None:
        page_data = {'contents': {'twoColumnBrowseResultsRenderer': {}}}
        with self.assertRaises(RuntimeError):
            self.tabs.get_tab(page_data, 'videos')


class TestGetTabByType(unittest.IsolatedAsyncioTestCase):
    async def test_finds_existing_tab(self) -> None:
        tabs = YouTubeChannelTabs(CHANNEL_ID)
        tabs.tabs = [
            {'tabRenderer': {'title': 'Videos', 'data': 'v'}},
            {'tabRenderer': {'title': 'Shorts', 'data': 's'}},
        ]
        result = await tabs.get_tab_by_type(YouTubeChannelPageType.VIDEOS)
        self.assertEqual(result['data'], 'v')

    async def test_returns_none_when_missing(self) -> None:
        tabs = YouTubeChannelTabs(CHANNEL_ID)
        tabs.tabs = [
            {'tabRenderer': {'title': 'Home'}},
        ]
        result = await tabs.get_tab_by_type(YouTubeChannelPageType.LIVE)
        self.assertIsNone(result)

    async def test_fetches_tabs_if_empty(self) -> None:
        tabs = YouTubeChannelTabs(CHANNEL_ID)
        tabs.tabs = []

        async def fake_get_page_tabs():
            tabs.tabs = [
                {'tabRenderer': {'title': 'Videos', 'data': 'loaded'}},
            ]
            return tabs.tabs

        tabs.get_page_tabs = fake_get_page_tabs
        result = await tabs.get_tab_by_type(YouTubeChannelPageType.VIDEOS)
        self.assertEqual(result['data'], 'loaded')


class TestGetPageTabs(unittest.IsolatedAsyncioTestCase):
    async def test_returns_tabs(self) -> None:
        tabs = YouTubeChannelTabs(CHANNEL_ID)
        fake_response = {
            'contents': {
                'twoColumnBrowseResultsRenderer': {
                    'tabs': [
                        {'tabRenderer': {'title': 'Home'}},
                        {'tabRenderer': {'title': 'Videos'}},
                    ]
                }
            }
        }
        tabs._browse = AsyncMock(return_value=fake_response)
        result = await tabs.get_page_tabs()
        self.assertEqual(len(result), 2)
        self.assertEqual(tabs.tabs, result)

    async def test_raises_when_no_tabs(self) -> None:
        tabs = YouTubeChannelTabs(CHANNEL_ID)
        fake_response = {'contents': {'twoColumnBrowseResultsRenderer': {}}}
        tabs._browse = AsyncMock(return_value=fake_response)
        with self.assertRaises(RuntimeError):
            await tabs.get_page_tabs()


class TestBrowse(unittest.IsolatedAsyncioTestCase):
    async def test_browse_no_params(self) -> None:
        tabs = YouTubeChannelTabs(CHANNEL_ID)
        tabs.client = MagicMock()
        tabs.client.browse.return_value = {'data': 'ok'}
        result = await tabs._browse()
        tabs.client.browse.assert_called_once_with(CHANNEL_ID)
        self.assertEqual(result, {'data': 'ok'})

    async def test_browse_with_params(self) -> None:
        tabs = YouTubeChannelTabs(CHANNEL_ID)
        tabs.client = MagicMock()
        tabs.client.browse.return_value = {'data': 'params'}
        result = await tabs._browse(params='EgZ2aWRlb3M=')
        tabs.client.browse.assert_called_once_with(
            CHANNEL_ID, params='EgZ2aWRlb3M='
        )
        self.assertEqual(result, {'data': 'params'})

    async def test_browse_with_continuation(self) -> None:
        tabs = YouTubeChannelTabs(CHANNEL_ID)
        tabs.client = MagicMock()
        tabs.client.browse.return_value = {'data': 'cont'}
        result = await tabs._browse(continuation_token='tok123')
        tabs.client.browse.assert_called_once_with(
            CHANNEL_ID, continuation='tok123'
        )
        self.assertEqual(result, {'data': 'cont'})

    @patch(
        'scrape_exchange.youtube.youtube_channel_tabs.AsyncYouTubeClient._delay',
        new_callable=AsyncMock,
    )
    async def test_browse_retries_on_failure(self, mock_delay) -> None:
        tabs = YouTubeChannelTabs(CHANNEL_ID)
        tabs.client = MagicMock()
        tabs.client.browse.side_effect = [
            Exception('fail1'),
            Exception('fail2'),
            {'data': 'recovered'},
        ]
        result = await tabs._browse(max_retries=4)
        self.assertEqual(result, {'data': 'recovered'})
        self.assertEqual(tabs.client.browse.call_count, 3)

    @patch(
        'scrape_exchange.youtube.youtube_channel_tabs.AsyncYouTubeClient._delay',
        new_callable=AsyncMock,
    )
    async def test_browse_raises_after_max_retries(self, mock_delay) -> None:
        tabs = YouTubeChannelTabs(CHANNEL_ID)
        tabs.client = MagicMock()
        tabs.client.browse.side_effect = Exception('always fail')
        with self.assertRaises(RuntimeError) as ctx:
            await tabs._browse(max_retries=2)
        self.assertIn('Failed to fetch tabbed data', str(ctx.exception))


class TestScrapeContent(unittest.IsolatedAsyncioTestCase):
    '''
    Test the full scrape_content flow with mocked InnerTube responses.
    '''

    @patch(
        'scrape_exchange.youtube.youtube_channel_tabs.AsyncYouTubeClient._delay',
        new_callable=AsyncMock,
    )
    async def test_scrape_content_basic_flow(self, mock_delay) -> None:
        '''
        Simulate a channel with:
        - Home tab (skipped)
        - Videos tab (1 video, no continuation)
        - Playlists tab (empty)
        - Store tab (empty)
        '''

        # Build initial browse response with tab list
        initial_tabs = [
            {
                'tabRenderer': {
                    'title': 'Home',
                    'endpoint': {
                        'browseEndpoint': {'params': 'home_p'}
                    }
                }
            },
            {
                'tabRenderer': {
                    'title': 'Videos',
                    'endpoint': {
                        'browseEndpoint': {'params': 'videos_p'}
                    }
                }
            },
            {
                'tabRenderer': {
                    'title': 'Playlists',
                    'endpoint': {
                        'browseEndpoint': {'params': 'playlists_p'}
                    }
                }
            },
            {
                'tabRenderer': {
                    'title': 'Store',
                    'endpoint': {
                        'browseEndpoint': {'params': 'store_p'}
                    }
                }
            },
        ]

        initial_response = {
            'contents': {
                'twoColumnBrowseResultsRenderer': {
                    'tabs': initial_tabs
                }
            }
        }

        # Videos tab browse response
        videos_response = {
            'contents': {
                'twoColumnBrowseResultsRenderer': {
                    'tabs': [
                        {
                            'tabRenderer': {
                                'title': 'Videos',
                                'content': {
                                    'richGridRenderer': {
                                        'contents': [
                                            {
                                                'richItemRenderer': {
                                                    'content': {
                                                        'videoRenderer': {
                                                            'videoId': 'vid1'
                                                        }
                                                    }
                                                }
                                            },
                                        ]
                                    }
                                }
                            }
                        }
                    ]
                }
            }
        }

        # Playlists tab browse response (empty)
        playlists_response = {
            'contents': {
                'twoColumnBrowseResultsRenderer': {
                    'tabs': [
                        {
                            'tabRenderer': {
                                'title': 'Playlists',
                                'content': {
                                    'sectionListRenderer': {
                                        'contents': []
                                    }
                                }
                            }
                        }
                    ]
                }
            }
        }

        # Store tab browse response (empty)
        store_response = {
            'contents': {
                'twoColumnBrowseResultsRenderer': {
                    'tabs': [
                        {
                            'tabRenderer': {
                                'title': 'Store',
                                'content': {
                                    'sectionListRenderer': {
                                        'contents': []
                                    }
                                }
                            }
                        }
                    ]
                }
            }
        }

        # Map params to responses
        param_to_response = {
            'videos_p': videos_response,
            'playlists_p': playlists_response,
            'store_p': store_response,
        }

        original_init = YouTubeChannelTabs.__init__

        def mock_init(self_inner, channel_id):
            original_init(self_inner, channel_id)
            # Replace client with mock
            mock_client = MagicMock()

            call_count = [0]

            def browse_side_effect(*args, **kwargs):
                call_count[0] += 1
                if call_count[0] == 1:
                    # Initial browse
                    return initial_response
                params = kwargs.get('params', '')
                if params in param_to_response:
                    return param_to_response[params]
                return {'contents': {}}

            mock_client.browse.side_effect = browse_side_effect
            self_inner.client = mock_client

        with patch.object(YouTubeChannelTabs, '__init__', mock_init):
            video_ids, podcast_ids, playlists, courses, posts, products = \
                await YouTubeChannelTabs.scrape_content(CHANNEL_ID)

        self.assertIn('vid1', video_ids)
        self.assertEqual(podcast_ids, set())
        self.assertEqual(playlists, set())
        self.assertEqual(courses, set())
        self.assertEqual(products, set())

    @patch(
        'scrape_exchange.youtube.youtube_channel_tabs.AsyncYouTubeClient._delay',
        new_callable=AsyncMock,
    )
    async def test_scrape_content_skips_tab_without_renderer(
        self, mock_delay
    ) -> None:
        '''A tab without tabRenderer should be skipped.'''

        initial_response = {
            'contents': {
                'twoColumnBrowseResultsRenderer': {
                    'tabs': [
                        {'expandableTabRenderer': {'title': 'More'}},
                    ]
                }
            }
        }

        original_init = YouTubeChannelTabs.__init__

        def mock_init(self_inner, channel_id):
            original_init(self_inner, channel_id)
            self_inner.client = MagicMock()
            self_inner.client.browse.return_value = initial_response

        with patch.object(YouTubeChannelTabs, '__init__', mock_init):
            video_ids, podcast_ids, playlists, courses, posts, products = \
                await YouTubeChannelTabs.scrape_content(CHANNEL_ID)

        self.assertEqual(video_ids, set())
        self.assertEqual(podcast_ids, set())

    @patch(
        'scrape_exchange.youtube.youtube_channel_tabs.AsyncYouTubeClient._delay',
        new_callable=AsyncMock,
    )
    async def test_scrape_content_with_continuation(
        self, mock_delay
    ) -> None:
        '''Test the continuation token pagination flow.'''

        initial_tabs = [
            {
                'tabRenderer': {
                    'title': 'Videos',
                    'endpoint': {
                        'browseEndpoint': {'params': 'videos_p'}
                    }
                }
            },
        ]

        initial_response = {
            'contents': {
                'twoColumnBrowseResultsRenderer': {
                    'tabs': initial_tabs
                }
            }
        }

        # Videos tab: first page with continuation
        videos_first_page = {
            'contents': {
                'twoColumnBrowseResultsRenderer': {
                    'tabs': [
                        {
                            'tabRenderer': {
                                'title': 'Videos',
                                'content': {
                                    'richGridRenderer': {
                                        'contents': [
                                            {
                                                'richItemRenderer': {
                                                    'content': {
                                                        'videoRenderer': {
                                                            'videoId': 'vid1'
                                                        }
                                                    }
                                                }
                                            },
                                            {
                                                'continuationItemRenderer': {
                                                    'continuationEndpoint': {
                                                        'continuationCommand': {
                                                            'token': 'cont_tok'
                                                        }
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
        }

        # Continuation response (no more pages)
        continuation_response = {
            'onResponseReceivedActions': [
                {
                    'appendContinuationItemsAction': {
                        'continuationItems': [
                            {
                                'richItemRenderer': {
                                    'content': {
                                        'videoRenderer': {
                                            'videoId': 'vid2'
                                        }
                                    }
                                }
                            },
                        ]
                    }
                }
            ]
        }

        original_init = YouTubeChannelTabs.__init__

        def mock_init(self_inner, channel_id):
            original_init(self_inner, channel_id)
            mock_client = MagicMock()

            call_count = [0]

            def browse_side_effect(*args, **kwargs):
                call_count[0] += 1
                if call_count[0] == 1:
                    return initial_response
                params = kwargs.get('params', '')
                cont = kwargs.get('continuation', '')
                if params == 'videos_p':
                    return videos_first_page
                if cont == 'cont_tok':
                    return continuation_response
                return {}

            mock_client.browse.side_effect = browse_side_effect
            self_inner.client = mock_client

        with patch.object(YouTubeChannelTabs, '__init__', mock_init):
            video_ids, podcast_ids, playlists, courses, posts, products = \
                await YouTubeChannelTabs.scrape_content(CHANNEL_ID)

        self.assertEqual(video_ids, {'vid1', 'vid2'})

    @patch(
        'scrape_exchange.youtube.youtube_channel_tabs.AsyncYouTubeClient._delay',
        new_callable=AsyncMock,
    )
    async def test_scrape_content_podcasts(self, mock_delay) -> None:
        '''Podcasts tab should be extracted without continuation.'''

        initial_tabs = [
            {
                'tabRenderer': {
                    'title': 'Podcasts',
                    'endpoint': {
                        'browseEndpoint': {'params': 'podcasts_p'}
                    }
                }
            },
        ]

        initial_response = {
            'contents': {
                'twoColumnBrowseResultsRenderer': {
                    'tabs': initial_tabs
                }
            }
        }

        podcasts_response = {
            'contents': {
                'twoColumnBrowseResultsRenderer': {
                    'tabs': [
                        {
                            'tabRenderer': {
                                'title': 'Podcasts',
                                'content': {
                                    'richGridRenderer': {
                                        'contents': [
                                            {
                                                'richItemRenderer': {
                                                    'content': {
                                                        'lockupViewModel': {
                                                            'contentId': 'pod1'
                                                        }
                                                    }
                                                }
                                            },
                                        ]
                                    }
                                }
                            }
                        }
                    ]
                }
            }
        }

        original_init = YouTubeChannelTabs.__init__

        def mock_init(self_inner, channel_id):
            original_init(self_inner, channel_id)
            mock_client = MagicMock()
            call_count = [0]

            def browse_side_effect(*args, **kwargs):
                call_count[0] += 1
                if call_count[0] == 1:
                    return initial_response
                return podcasts_response

            mock_client.browse.side_effect = browse_side_effect
            self_inner.client = mock_client

        with patch.object(YouTubeChannelTabs, '__init__', mock_init):
            video_ids, podcast_ids, playlists, courses, posts, products = \
                await YouTubeChannelTabs.scrape_content(CHANNEL_ID)

        self.assertEqual(podcast_ids, {'pod1'})
        self.assertEqual(video_ids, set())

    @patch(
        'scrape_exchange.youtube.youtube_channel_tabs.AsyncYouTubeClient._delay',
        new_callable=AsyncMock,
    )
    async def test_scrape_content_empty_contents(self, mock_delay) -> None:
        '''Tab with empty richGridRenderer contents should be skipped.'''

        initial_tabs = [
            {
                'tabRenderer': {
                    'title': 'Live',
                    'endpoint': {
                        'browseEndpoint': {'params': 'live_p'}
                    }
                }
            },
        ]

        initial_response = {
            'contents': {
                'twoColumnBrowseResultsRenderer': {
                    'tabs': initial_tabs
                }
            }
        }

        # Live tab: empty contents
        live_response = {
            'contents': {
                'twoColumnBrowseResultsRenderer': {
                    'tabs': [
                        {
                            'tabRenderer': {
                                'title': 'Live',
                                'content': {
                                    'richGridRenderer': {
                                        'contents': []
                                    }
                                }
                            }
                        }
                    ]
                }
            }
        }

        original_init = YouTubeChannelTabs.__init__

        def mock_init(self_inner, channel_id):
            original_init(self_inner, channel_id)
            mock_client = MagicMock()
            call_count = [0]

            def browse_side_effect(*args, **kwargs):
                call_count[0] += 1
                if call_count[0] == 1:
                    return initial_response
                return live_response

            mock_client.browse.side_effect = browse_side_effect
            self_inner.client = mock_client

        with patch.object(YouTubeChannelTabs, '__init__', mock_init):
            video_ids, pod_ids, pls, courses, posts, products = \
                await YouTubeChannelTabs.scrape_content(CHANNEL_ID)

        self.assertEqual(video_ids, set())

    @patch(
        'scrape_exchange.youtube.youtube_channel_tabs.AsyncYouTubeClient._delay',
        new_callable=AsyncMock,
    )
    async def test_scrape_content_continuation_no_actions(
        self, mock_delay
    ) -> None:
        '''Continuation response without actions should stop pagination.'''

        initial_tabs = [
            {
                'tabRenderer': {
                    'title': 'Videos',
                    'endpoint': {
                        'browseEndpoint': {'params': 'videos_p'}
                    }
                }
            },
        ]

        initial_response = {
            'contents': {
                'twoColumnBrowseResultsRenderer': {
                    'tabs': initial_tabs
                }
            }
        }

        videos_first_page = {
            'contents': {
                'twoColumnBrowseResultsRenderer': {
                    'tabs': [
                        {
                            'tabRenderer': {
                                'title': 'Videos',
                                'content': {
                                    'richGridRenderer': {
                                        'contents': [
                                            {
                                                'richItemRenderer': {
                                                    'content': {
                                                        'videoRenderer': {
                                                            'videoId': 'vid1'
                                                        }
                                                    }
                                                }
                                            },
                                            {
                                                'continuationItemRenderer': {
                                                    'continuationEndpoint': {
                                                        'continuationCommand': {
                                                            'token': 'tok'
                                                        }
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
        }

        # Empty continued response
        empty_continuation = {}

        original_init = YouTubeChannelTabs.__init__

        def mock_init(self_inner, channel_id):
            original_init(self_inner, channel_id)
            mock_client = MagicMock()
            call_count = [0]

            def browse_side_effect(*args, **kwargs):
                call_count[0] += 1
                if call_count[0] == 1:
                    return initial_response
                if kwargs.get('params') == 'videos_p':
                    return videos_first_page
                return empty_continuation

            mock_client.browse.side_effect = browse_side_effect
            self_inner.client = mock_client

        with patch.object(YouTubeChannelTabs, '__init__', mock_init):
            video_ids, *_ = await YouTubeChannelTabs.scrape_content(
                CHANNEL_ID
            )

        self.assertIn('vid1', video_ids)

    @patch(
        'scrape_exchange.youtube.youtube_channel_tabs.AsyncYouTubeClient._delay',
        new_callable=AsyncMock,
    )
    async def test_scrape_content_courses_tab(self, mock_delay) -> None:
        """Cover lines 81-84: courses branch in scrape_content."""

        initial_response = {
            'contents': {
                'twoColumnBrowseResultsRenderer': {
                    'tabs': [
                        {
                            'tabRenderer': {
                                'title': 'Courses',
                                'endpoint': {
                                    'browseEndpoint': {'params': 'courses_p'}
                                }
                            }
                        }
                    ]
                }
            }
        }

        courses_page = {
            'contents': {
                'twoColumnBrowseResultsRenderer': {
                    'tabs': [
                        {
                            'tabRenderer': {
                                'title': 'Courses',
                                'content': {
                                    'richGridRenderer': {
                                        'contents': [
                                            {
                                                'richItemRenderer': {
                                                    'content': {
                                                        'playlistRenderer': {
                                                            'playlistId':
                                                                'PLcourse1',
                                                            'title': {
                                                                'simpleText':
                                                                    'Course 1'
                                                            },
                                                            'videoCount': '3',
                                                        }
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
        }

        original_init = YouTubeChannelTabs.__init__

        def mock_init(self_inner, channel_id):
            original_init(self_inner, channel_id)
            mock_client = MagicMock()
            call_count = [0]

            def browse_side_effect(*args, **kwargs):
                call_count[0] += 1
                if call_count[0] == 1:
                    return initial_response
                return courses_page

            mock_client.browse.side_effect = browse_side_effect
            self_inner.client = mock_client

        with patch.object(YouTubeChannelTabs, '__init__', mock_init):
            video_ids, podcast_ids, playlists, courses, posts, products = \
                await YouTubeChannelTabs.scrape_content(CHANNEL_ID)

        self.assertEqual(len(courses), 1)
        self.assertEqual(len(video_ids), 0)

    @patch(
        'scrape_exchange.youtube.youtube_channel_tabs.AsyncYouTubeClient._delay',
        new_callable=AsyncMock,
    )
    async def test_scrape_content_posts_tab(self, mock_delay) -> None:
        """Cover lines 86-89: posts branch in scrape_content."""

        initial_response = {
            'contents': {
                'twoColumnBrowseResultsRenderer': {
                    'tabs': [
                        {
                            'tabRenderer': {
                                'title': 'Posts',
                                'endpoint': {
                                    'browseEndpoint': {'params': 'posts_p'}
                                }
                            }
                        }
                    ]
                }
            }
        }

        posts_page = {
            'contents': {
                'twoColumnBrowseResultsRenderer': {
                    'tabs': [
                        {
                            'tabRenderer': {
                                'title': 'Posts',
                                'content': {
                                    'sectionListRenderer': {
                                        'contents': [
                                            {
                                                'itemSectionRenderer': {
                                                    'contents': [
                                                        {
                                                            'backstagePostThreadRenderer': {
                                                                'post': {
                                                                    'backstagePostRenderer': {
                                                                        'postId': 'post1',
                                                                        'contentText': {
                                                                            'runs': [{'text': 'Hello'}]
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
                    ]
                }
            }
        }

        original_init = YouTubeChannelTabs.__init__

        def mock_init(self_inner, channel_id):
            original_init(self_inner, channel_id)
            mock_client = MagicMock()
            call_count = [0]

            def browse_side_effect(*args, **kwargs):
                call_count[0] += 1
                if call_count[0] == 1:
                    return initial_response
                return posts_page

            mock_client.browse.side_effect = browse_side_effect
            self_inner.client = mock_client

        with patch.object(YouTubeChannelTabs, '__init__', mock_init):
            video_ids, podcast_ids, playlists, courses, posts, products = \
                await YouTubeChannelTabs.scrape_content(CHANNEL_ID)

        self.assertEqual(len(posts), 1)
        self.assertEqual(len(video_ids), 0)


if __name__ == '__main__':
    unittest.main()
