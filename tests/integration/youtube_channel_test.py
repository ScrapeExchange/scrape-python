#!/usr/bin/env python3
'''

Integration tests for AsyncYouTubeClient that perform real HTTP calls to
YouTube.

These tests hit live YouTube endpoints and may be slow or flaky depending on
network conditions. They are intended to run separately from unit tests.
'''


import os
import shutil
import unittest
import tempfile

from datetime import UTC, datetime

import orjson
import aiofiles

from scrape_exchange.youtube.youtube_course import YouTubeCourse
from scrape_exchange.youtube.youtube_post import YouTubePost
from scrape_exchange.youtube.youtube_product import YouTubeProduct
from scrape_exchange.youtube.youtube_channel import YouTubeChannel
from scrape_exchange.youtube.youtube_playlist import YouTubePlaylist
from scrape_exchange.youtube.youtube_channel_tabs import YouTubeChannelTabs
from scrape_exchange.youtube.youtube_types import YouTubeChannelPageType
from scrape_exchange.youtube.youtube_types import YouTubeChannelLink

from scrape_exchange.youtube.youtube_video import DENO_PATH, PO_TOKEN_URL

YOUTUBE_HISTORYMATTERS_CHANNEL: str = 'HistoryMatters'
YOUTUBE_HISTORYMATTERS_CHANNEL_ID: str = 'UC22BdTgxefuvUivrjesETjg'
YOUTUBE_SOCRATICA_CHANNEL: str = 'Socratica'
YOUTUBE_SOCRATICA_CHANNEL_ID: str = 'UCW6TXMZ5Pq6yL6_k5NZ2e0Q'

OUTPUT_DIR: str = 'tests/collateral/youtube_channels'


class TestIntegration(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.api_key_id: str = os.environ.get('API_KEY_ID', '')
        self.api_key_secret: str = os.environ.get('API_KEY_SECRET', '')
        self.api_base_url: str = os.environ.get(
            'API_BASE_URL', 'https://scrape.exchange'
        )
        self.temp_dir: str = tempfile.mkdtemp()
        os.mkdir(f'{self.temp_dir}/uploaded')

    async def asyncTearDown(self) -> None:
        if self.temp_dir and os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    async def test_youtube_channel(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(
            name=YOUTUBE_HISTORYMATTERS_CHANNEL, deno_path=DENO_PATH,
            po_token_url=PO_TOKEN_URL, debug=True, save_dir=OUTPUT_DIR
        )
        await channel.scrape_about_page()
        self.assertEqual(channel.name, YOUTUBE_HISTORYMATTERS_CHANNEL)
        self.assertEqual(channel.channel_id, YOUTUBE_HISTORYMATTERS_CHANNEL_ID)
        self.assertEqual(channel.title, 'History Matters')
        self.assertEqual(channel.joined_date, datetime(2015, 8, 2, tzinfo=UTC))
        self.assertEqual(
            channel.rss_url,
            'https://www.youtube.com/feeds/videos.xml?channel_id=UC22BdTgxefuvUivrjesETjg'  # noqa: E501
        )
        self.assertTrue(channel.verified)
        self.assertGreaterEqual(channel.subscriber_count, 1880000)
        self.assertGreaterEqual(channel.video_count, 384)
        self.assertGreaterEqual(channel.view_count, 755841320)

        output_file: str = f'{self.temp_dir}/{YOUTUBE_HISTORYMATTERS_CHANNEL}.json'
        async with aiofiles.open(output_file, 'w') as f:
            await f.write(
                orjson.dumps(
                    channel.to_dict(), option=orjson.OPT_INDENT_2
                ).decode('utf-8'))

        async with aiofiles.open(output_file, 'r') as f:
            content: str = await f.read()
            channel_data: dict[str, any] = orjson.loads(content)

            loaded_channel: YouTubeChannel = YouTubeChannel.from_dict(
                channel_data
            )
            self.assertEqual(loaded_channel, channel)

    async def test_scrape_channel(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(
            name=YOUTUBE_HISTORYMATTERS_CHANNEL, deno_path=DENO_PATH,
            po_token_url=PO_TOKEN_URL, debug=True, save_dir=self.temp_dir
        )
        await channel.scrape()
        self.assertEqual(channel.name, YOUTUBE_HISTORYMATTERS_CHANNEL)
        self.assertEqual(channel.channel_id, YOUTUBE_HISTORYMATTERS_CHANNEL_ID)
        self.assertEqual(channel.title, 'History Matters')
        self.assertEqual(channel.joined_date, datetime(2015, 8, 2, tzinfo=UTC))
        self.assertEqual(
            channel.rss_url,
            'https://www.youtube.com/feeds/videos.xml?channel_id=UC22BdTgxefuvUivrjesETjg'  # noqa: E501
        )
        self.assertTrue(channel.verified)
        self.assertGreaterEqual(channel.subscriber_count, 1880000)
        self.assertGreaterEqual(channel.video_count, 384)
        self.assertGreaterEqual(channel.view_count, 755841320)

    async def test_channel_links(self) -> None:
        channel: YouTubeChannel = YouTubeChannel(
            name='ComedyCentral', deno_path=DENO_PATH,
            po_token_url=PO_TOKEN_URL, debug=True, save_dir=OUTPUT_DIR
        )
        await channel.scrape_about_page()
        self.assertGreaterEqual(len(channel.channel_links), 10)
        link: YouTubeChannelLink
        for link in channel.channel_links:
            self.assertIsNotNone(link.channel_name)
            self.assertIsNotNone(link.subscriber_count)

    async def test_youtube_channel_tabs_scrape(self) -> None:
        channel_tabs: YouTubeChannelTabs = YouTubeChannelTabs(
            channel_id=YOUTUBE_HISTORYMATTERS_CHANNEL_ID
        )

        video_ids: set[str]
        podcast_ids: set[str]
        playlists: set[YouTubePlaylist]
        courses: set[YouTubeCourse]
        posts: set[YouTubePost]
        products: set[YouTubeProduct]
        video_ids, podcast_ids, playlists, courses, posts, products = \
            await channel_tabs.scrape_content(
                channel_id=YOUTUBE_HISTORYMATTERS_CHANNEL_ID
            )
        self.assertGreaterEqual(len(video_ids), 3)
        self.assertGreaterEqual(len(podcast_ids), 1)
        self.assertGreaterEqual(len(playlists), 1)

        # Validate playlist objects have required fields
        playlist: YouTubePlaylist = next(iter(playlists))
        self.assertIsNotNone(playlist.playlist_id)
        self.assertIsNotNone(playlist.title)
        self.assertGreaterEqual(playlist.video_count, 1)
        self.assertIsNotNone(playlist.url)
        self.assertEqual(
            playlist.channel_id, YOUTUBE_HISTORYMATTERS_CHANNEL_ID
        )

        # Validate round-trip serialisation
        data: dict = playlist.to_dict()
        restored: YouTubePlaylist = YouTubePlaylist.from_dict(data)
        self.assertEqual(playlist, restored)

    async def test_youtube_channel_courses_scrape(self) -> None:
        '''Scrape courses from the Socratica channel.'''
        channel_tabs: YouTubeChannelTabs = YouTubeChannelTabs(
            channel_id=YOUTUBE_SOCRATICA_CHANNEL_ID
        )

        video_ids: set[str]
        podcast_ids: set[str]
        playlists: set[YouTubePlaylist]
        courses: set[YouTubeCourse]
        posts: set[YouTubePost]
        products: set[YouTubeProduct]
        video_ids, podcast_ids, playlists, courses, posts, products = \
            await channel_tabs.scrape_content(
                channel_id=YOUTUBE_SOCRATICA_CHANNEL_ID
            )
        self.assertGreaterEqual(len(courses), 1)

        # Validate course objects have required fields
        course: YouTubeCourse = next(iter(courses))
        self.assertIsNotNone(course.playlist_id)
        self.assertIsNotNone(course.title)
        self.assertGreaterEqual(course.video_count, 1)
        self.assertIsNotNone(course.url)
        self.assertEqual(
            course.channel_id, YOUTUBE_SOCRATICA_CHANNEL_ID
        )
        self.assertGreater(len(course.videos), 0)
        self.assertIsNotNone(course.videos[0].video_id)
        self.assertIsNotNone(course.videos[0].title)

        # Validate round-trip serialisation
        data: dict = course.to_dict()
        restored: YouTubeCourse = YouTubeCourse.from_dict(data)
        self.assertEqual(course, restored)

    async def test_youtube_channel_posts_scrape(self) -> None:
        '''Scrape posts from the HistoryMatters channel.'''
        channel_tabs: YouTubeChannelTabs = YouTubeChannelTabs(
            channel_id=YOUTUBE_HISTORYMATTERS_CHANNEL_ID
        )

        video_ids: set[str]
        podcast_ids: set[str]
        playlists: set[YouTubePlaylist]
        courses: set[YouTubeCourse]
        posts: set[YouTubePost]
        products: set[YouTubeProduct]
        video_ids, podcast_ids, playlists, courses, posts, products = \
            await channel_tabs.scrape_content(
                channel_id=YOUTUBE_HISTORYMATTERS_CHANNEL_ID
            )
        self.assertGreaterEqual(len(posts), 1)

        # Validate post objects have required fields
        post: YouTubePost = next(iter(posts))
        self.assertIsNotNone(post.post_id)
        self.assertIsNotNone(post.content_text)
        self.assertIsNotNone(post.published_time_text)
        self.assertIsNotNone(post.vote_count)
        self.assertIsNotNone(post.url)
        self.assertEqual(
            post.channel_id, YOUTUBE_HISTORYMATTERS_CHANNEL_ID
        )

        # Validate round-trip serialisation
        data: dict = post.to_dict()
        restored: YouTubePost = YouTubePost.from_dict(data)
        self.assertEqual(post, restored)

    async def test_youtube_channel_products_scrape(self) -> None:
        '''Scrape products from the HistoryMatters channel store.'''
        channel_tabs: YouTubeChannelTabs = YouTubeChannelTabs(
            channel_id=YOUTUBE_HISTORYMATTERS_CHANNEL_ID
        )

        video_ids: set[str]
        podcast_ids: set[str]
        playlists: set[YouTubePlaylist]
        courses: set[YouTubeCourse]
        posts: set[YouTubePost]
        products: set[YouTubeProduct]
        video_ids, podcast_ids, playlists, courses, posts, products = \
            await channel_tabs.scrape_content(
                channel_id=YOUTUBE_HISTORYMATTERS_CHANNEL_ID
            )
        self.assertGreaterEqual(len(products), 1)

        # Validate product objects have required fields
        product: YouTubeProduct = next(iter(products))
        self.assertIsNotNone(product.title)
        self.assertIsNotNone(product.price)
        self.assertIsNotNone(product.merchant_name)
        self.assertIsNotNone(product.thumbnail_url)
        self.assertIsNotNone(product.product_url)
        self.assertEqual(
            product.channel_id, YOUTUBE_HISTORYMATTERS_CHANNEL_ID
        )

        # Validate round-trip serialisation
        data: dict = product.to_dict()
        restored: YouTubeProduct = YouTubeProduct.from_dict(data)
        self.assertEqual(product, restored)


if __name__ == '__main__':
    unittest.main()
