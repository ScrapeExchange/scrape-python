'''
Class to parse the data returned by the Innertube API for the channel tabs
(Videos, Shorts, Live, Courses, Podcasts, Playlists, Posts, Store) and extract
the video IDs, playlist data, course data, post data, and product data.

:maintainer: Boinko <boinko@scrape.exchange>
:copyright: Copyright 2026
:license: GPLv3
'''

from logging import Logger
from logging import getLogger

from innertube import InnerTube

from scrape_exchange.youtube.youtube_types import YouTubeChannelPageType

from .youtube_client import AsyncYouTubeClient
from .youtube_playlist import YouTubePlaylist
from .youtube_course import YouTubeCourse
from .youtube_post import YouTubePost
from .youtube_product import YouTubeProduct

_LOGGER: Logger = getLogger(__name__)


class YouTubeChannelTabs:
    def __init__(self, channel_id: str) -> None:
        self.channel_id: str = channel_id
        self.client: InnerTube = InnerTube('WEB', '2.20230728.00.00')
        self.tabs: list[dict[str, any]] = []

    @staticmethod
    async def scrape_content(
        channel_id: str
    ) -> tuple[
        set[str], set[str], set[YouTubePlaylist], set[YouTubeCourse],
        set[YouTubePost], set[YouTubeProduct]
    ]:
        self = YouTubeChannelTabs(channel_id)
        tabs: dict[str, dict[str, any]] = await self.get_page_tabs()

        podcast_ids: set[str] = set()
        video_ids: set[str] = set()
        playlists: set[YouTubePlaylist] = set()
        courses: set[YouTubeCourse] = set()
        posts: set[YouTubePost] = set()
        products: set[YouTubeProduct] = set()

        tab: dict[str, any]
        for tab in tabs:
            tab_renderer: dict[str, any] = tab.get('tabRenderer')

            if not tab_renderer:
                _LOGGER.debug(
                    f'Channel {channel_id} a tab without a tabRenderer, '
                    f'skipping tab: {tab.get("title", "")}'
                )
                continue

            title: str = tab_renderer.get('title', '').lower()
            if title == 'home':
                continue

            # Extract the browse params for the tab
            params: str = tab_renderer['endpoint']['browseEndpoint']['params']

            # Wait a bit so that we don't overload the YT server
            await AsyncYouTubeClient._delay(0.3, 0.8)

            # First page: use params to navigate to the tab
            page_data: dict = await self._browse(params=params)
            page_tab: dict[str, any] = self.get_tab(page_data, title)

            if title == 'playlists':
                playlists = self._get_playlist_items(
                    page_tab, channel_id
                )
                continue
            elif title == 'courses':
                courses = self._get_course_items(
                    page_tab, channel_id
                )
                continue
            elif title == 'posts':
                posts = self._get_post_items(
                    page_tab, channel_id
                )
                continue
            elif title == 'store':
                products = self._get_product_items(
                    page_tab, channel_id
                )
                continue

            contents: list = page_tab.get(
                'content', {}
            ).get(
                'richGridRenderer', {}
            ).get('contents', [])

            if not contents:
                _LOGGER.debug(
                    f'No contents found for channel {channel_id} '
                    f'tab {title}'
                )
                continue

            if title == 'podcasts':
                # Podcasts page doesn't have a continuation token, so we
                # can exit after the first page
                podcast_ids = self._get_podcast_ids(contents)
                continue

            continuation_token: str = self.get_continuation_token(contents[-1])
            if 'continuationItemRenderer' in contents[-1]:
                contents = contents[:-1]

            for content in contents:
                video_id = self._extract_video_id(content, title)
                if video_id:
                    video_ids.add(video_id)

            # Subsequent pages: use continuation token
            while continuation_token:
                await AsyncYouTubeClient._delay(0.3, 0.8)

                continued_data: dict = await self._browse(
                    continuation_token=continuation_token
                )

                actions: list = continued_data.get(
                    'onResponseReceivedActions'
                )
                if not actions:
                    break

                continuation_items: list = actions[0].get(
                    'appendContinuationItemsAction', {}
                ).get('continuationItems')

                if not continuation_items:
                    break

                continuation_token = self.get_continuation_token(
                    continuation_items[-1]
                )
                if 'continuationItemRenderer' in continuation_items[-1]:
                    continuation_items = continuation_items[:-1]

                for item in continuation_items:
                    video_id: str | None = self._extract_video_id(item, title)
                    if video_id:
                        video_ids.add(video_id)

        return video_ids, podcast_ids, playlists, courses, posts, products

    def _extract_video_id(self, item: dict[str, any], tab_title: str
                          ) -> str | None:
        '''
        Extract a video ID from a rich item renderer, handling both
        regular videos/live and shorts layouts.

        :param item: a single item from the contents list
        :param tab_title: the lowercase tab title (videos, shorts, live, etc.)
        :returns: the video ID or None
        '''

        if tab_title != 'shorts':
            video_id: str | None = item.get(
                'richItemRenderer', {}
            ).get(
                'content', {}
            ).get(
                'videoRenderer', {}
            ).get('videoId')

            return video_id

        # Special handling for shorts
        video_url: str | None = item.get(
            'richItemRenderer', {}
        ).get(
            'content', {}
        ).get(
            'shortsLockupViewModel', {}
        ).get(
            'onTap', {}
        ).get(
            'innertubeCommand', {}
        ).get(
            'commandMetadata', {}
        ).get(
            'webCommandMetadata', {}
        ).get('url')

        if video_url and video_url.startswith('/shorts/'):
            return video_url.split('/')[-1] or None

        return None

    def _get_podcast_ids(self, contents: list) -> set[str]:
        '''
        Gets the podcast IDs from the podcasts tab content
        '''
        podcast_ids: set[str] = set()
        for content in contents:
            podcast_id: str | None = content.get(
                'richItemRenderer', {}
            ).get(
                'content', {}
            ).get(
                'lockupViewModel', {}
            ).get(
                'contentId'
            )

            podcast_ids.add(podcast_id) if podcast_id else None

        return podcast_ids

    def _get_playlist_items(self, tab_renderer: dict[str, any],
                            channel_id: str) -> set[YouTubePlaylist]:
        '''
        Parses playlists from the playlists tab.  The playlists tab uses a
        ``sectionListRenderer`` → ``gridRenderer`` layout instead of the
        ``richGridRenderer`` used by the videos / shorts / live tabs.

        :param tab_renderer: the tabRenderer dict for the playlists tab
        :param channel_id: the channel ID owning the playlists
        :returns: a set of YouTubePlaylist instances
        '''

        playlists: set[YouTubePlaylist] = set()

        sections: list = tab_renderer.get(
            'content', {}
        ).get(
            'sectionListRenderer', {}
        ).get('contents', [])

        for section in sections:
            items: list = section.get(
                'itemSectionRenderer', {}
            ).get('contents', [{}])[0].get(
                'gridRenderer', {}
            ).get('items', [])

            for item in items:
                playlist: YouTubePlaylist | None = \
                    YouTubePlaylist.from_innertube(item, channel_id)
                if playlist:
                    playlists.add(playlist)

        return playlists

    def _get_course_items(
        self, tab_renderer: dict[str, any], channel_id: str
    ) -> set[YouTubeCourse]:
        '''
        Parses courses from the courses tab.  The courses tab uses
        ``richGridRenderer`` → ``richItemRenderer`` → ``playlistRenderer``.

        :param tab_renderer: the tabRenderer dict for the courses tab
        :param channel_id: the channel ID owning the courses
        :returns: a set of YouTubeCourse instances
        '''

        courses: set[YouTubeCourse] = set()

        contents: list = tab_renderer.get(
            'content', {}
        ).get(
            'richGridRenderer', {}
        ).get('contents', [])

        for item in contents:
            course: YouTubeCourse | None = \
                YouTubeCourse.from_innertube(item, channel_id)
            if course:
                courses.add(course)

        return courses

    def _get_post_items(self, tab_renderer: dict[str, any],
                        channel_id: str) -> set[YouTubePost]:
        '''
        Parses posts from the posts/community tab.  The posts tab uses
        ``sectionListRenderer`` → ``itemSectionRenderer`` with
        ``backstagePostThreadRenderer`` items.

        :param tab_renderer: the tabRenderer dict for the posts tab
        :param channel_id: the channel ID owning the posts
        :returns: a set of YouTubePost instances
        '''

        posts: set[YouTubePost] = set()

        sections: list = tab_renderer.get(
            'content', {}
        ).get(
            'sectionListRenderer', {}
        ).get('contents', [])

        for section in sections:
            items: list = section.get(
                'itemSectionRenderer', {}
            ).get('contents', [])

            for item in items:
                post: YouTubePost | None = YouTubePost.from_innertube(
                    item, channel_id
                )
                if post:
                    posts.add(post)

        return posts

    def _get_product_items(self, tab_renderer: dict[str, any],
                           channel_id: str) -> set[YouTubeProduct]:
        '''
        Parses products from the store tab.  The store tab uses
        ``sectionListRenderer`` → ``itemSectionRenderer`` →
        ``shelfRenderer`` → ``gridRenderer`` with
        ``verticalProductCardRenderer`` items.

        :param tab_renderer: the tabRenderer dict for the store tab
        :param channel_id: the channel ID owning the products
        :returns: a set of YouTubeProduct instances
        '''

        products: set[YouTubeProduct] = set()

        sections: list = tab_renderer.get(
            'content', {}
        ).get(
            'sectionListRenderer', {}
        ).get('contents', [])

        for section in sections:
            isr_contents: list = section.get(
                'itemSectionRenderer', {}
            ).get('contents', [])

            for shelf_item in isr_contents:
                grid_items: list = shelf_item.get(
                    'shelfRenderer', {}
                ).get(
                    'content', {}
                ).get(
                    'gridRenderer', {}
                ).get('items', [])

                for item in grid_items:
                    product: YouTubeProduct | None = \
                        YouTubeProduct.from_innertube(item, channel_id)
                    if product:
                        products.add(product)

        return products

    async def _browse(self, params: str = '', continuation_token: str = '',
                      max_retries: int = 4) -> dict:
        retries: int = 1
        delay_seconds: int = 1
        while retries <= max_retries:
            try:
                if not params:
                    if not continuation_token:
                        return self.client.browse(self.channel_id)
                    else:
                        return self.client.browse(
                            self.channel_id, continuation=continuation_token
                        )
                else:
                    # No need to sent continuation token and params
                    return self.client.browse(
                        self.channel_id, params=params
                    )
            except Exception as e:
                retries += 1
                _LOGGER.error(
                    f'Error fetching videos data (attempt {retries}): {e}'
                )
                await AsyncYouTubeClient._delay(
                    delay_seconds-1, delay_seconds
                )
                delay_seconds *= 2

        raise RuntimeError(
            f'Failed to fetch tabbed data after {max_retries} attempts'
        )

    async def get_page_tabs(self) -> list[dict[str, any]]:
        '''
        Gets the first page of videos data for the channel

        :returns: a list containing the data of the tabs.
        :raises: RuntimeError if the tabs cannot be extracted from the page
        '''

        # Fetch the browse data for the channel
        channel_data: dict = await self._browse()

        await AsyncYouTubeClient._delay(0.3, 0.8)

        # Extract the tabs of the channel
        self.tabs = channel_data.get(
            'contents', {}
        ).get(
            'twoColumnBrowseResultsRenderer', {}
        ).get(
            'tabs', []
        )

        if not self.tabs:
            raise RuntimeError(
                f'Failed to extract tabs for channel: {self.channel_id}'
            )

        return self.tabs

    async def get_tab_by_type(self, page_type: YouTubeChannelPageType
                              ) -> dict[str, any] | None:
        '''
        Gets the tab renderer for the given page type (Videos, Shorts,
        Live, Podcasts). The page tabs will be scraped if they haven't been
        already.

        :param page_tabs: the list of tabs to search through
        :param page_type: the type of the tab to find
        :returns: the tab renderer for the given page type or None if the
        channel doesn't have that tab
        :raises: (none)
        '''

        title: str = page_type.value.lower()

        # Get the list of channel tabs and find the requested one
        if not self.tabs:
            await self.get_page_tabs()

        for tab in self.tabs or []:
            tab_renderer: dict[str, any] = tab.get('tabRenderer', {})
            if tab_renderer.get('title', '').lower() == title:
                return tab_renderer

        return None

    def get_continuation_token(self, last_item: dict) -> str:
        '''
        Gets the continuation token from the last_item of the data collected
        by the Innertube client

        :param last_item: the last item of the data collected by the Innertube
        client
        :returns: the continuation token or an empty string if there are no
        more pages
        '''

        token: str = last_item.get(
            'continuationItemRenderer', {}
        ).get(
            'continuationEndpoint', {}
        ).get(
            'continuationCommand', {}
        ).get(
            'token', ''
        )
        return token

    def get_tab(self, page_data: dict[str, any], title: str
                ) -> dict[str, any]:
        '''
        Gets the tab renderer for the given page type (Videos, Shorts,
        Live, Podcasts)

        :param page_tabs: the list of tabs to search through
        :param title: the title of the tab to find (case-insensitive)
        :returns: the tab renderer for the given page type
        :raises: RuntimeError if the tab with the given title cannot be found
        '''

        page_tabs: list[dict[str, any]] = page_data.get(
            'contents', {}
        ).get('twoColumnBrowseResultsRenderer', {}).get('tabs')

        if not page_tabs:
            raise RuntimeError(
                f'Failed to extract tabs for channel: {self.channel_id}'
            )

        for tab in page_tabs or []:
            tab_renderer: dict[str, any] = tab.get('tabRenderer', {})
            if tab_renderer.get('title', '').lower() == title:
                return tab_renderer

        raise RuntimeError(
            f'Channel {self.channel_id} does not have a {title} tab'
        )

