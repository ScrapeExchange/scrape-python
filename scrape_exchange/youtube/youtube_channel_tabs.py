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
import logging
import time

from innertube import InnerTube
from innertube.errors import RequestError as InnerTubeRequestError

from scrape_exchange.datatypes import MAX_KEEPALIVE_REQUESTS
from scrape_exchange.youtube.youtube_types import YouTubeChannelPageType

from .youtube_client import (
    AsyncYouTubeClient,
    INNERTUBE_CLIENT_NAME,
    INNERTUBE_CLIENT_VERSION,
    METRIC_YT_REQUEST_DURATION,
    generate_visitor_info,
)
from scrape_exchange.worker_id import get_worker_id
from scrape_exchange.util import extract_proxy_ip, proxy_network_for
from .youtube_cookiejar import YouTubeCookieJar
from .youtube_rate_limiter import YouTubeRateLimiter, YouTubeCallType
from .youtube_playlist import YouTubePlaylist
from .youtube_course import YouTubeCourse
from .youtube_post import YouTubePost
from .youtube_product import YouTubeProduct

_LOGGER: Logger = getLogger(__name__)


class YouTubeChannelTabs:
    def __init__(self, channel_id: str, proxy: str | None = None
                 ) -> None:
        self.channel_id: str = channel_id
        self.proxy: str | None = (
            proxy
            or YouTubeRateLimiter.get().select_proxy(YouTubeCallType.BROWSE)
        )
        self.client: InnerTube = self.get_innertube_client()
        self.client_request_count: int = 0
        self.tabs: list[dict[str, any]] = []

    def get_innertube_client(self) -> InnerTube:
        '''
        Gets the Innertube client for browsing/scraping data.  The client is
        created lazily on the first call to this method.  Uses ``self.proxy``
        when set.
        ---
        :returns: An instance of the Innertube client.
        '''

        _LOGGER.debug('Creating new Innertube client')
        try:
            if self.client:
                self.client.adaptor.session.close()
        except AttributeError:
            # this happens when we are called by the constructor
            pass

        self.client_request_count = 0
        if not self.proxy:
            logging.warning(
                'No proxies configured, proceeding without proxies'
            )

        client: InnerTube = InnerTube(
            'WEB', INNERTUBE_CLIENT_VERSION, proxies=self.proxy
        )

        # Load cached session cookies (YSC, PREF, SOCS, etc.) from the
        # per-proxy cookie jar first, then overwrite VISITOR_INFO1_LIVE with a
        # freshly-generated value so every rotated client gets a new identity.
        YouTubeCookieJar.get().load_into_session(
            client.adaptor.session, self.proxy
        )
        self.visitor_id: str = generate_visitor_info()
        client.adaptor.session.cookies.set(
            'VISITOR_INFO1_LIVE', self.visitor_id,
            domain='.youtube.com', path='/'
        )

        # Align the X-YouTube-Client-Name / Version headers with what
        # the WEB client sends in a real browser session.
        client.adaptor.session.headers['X-YouTube-Client-Name'] = \
            INNERTUBE_CLIENT_NAME
        client.adaptor.session.headers['X-YouTube-Client-Version'] = \
            INNERTUBE_CLIENT_VERSION

        return client

    async def browse_channel(self) -> dict[str, any]:
        '''
        Browse the channel via InnerTube and return the full page data
        (header, metadata, tabs).  Also stores the tabs list on the instance
        for later use by ``scrape_loaded_tabs()``.

        :returns: the full InnerTube browse response for the channel
        :raises: RuntimeError if the tabs cannot be extracted
        '''

        channel_data: dict = await self._browse()

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

        return channel_data

    async def scrape_loaded_tabs(self) -> tuple[
        set[str], set[str], set[YouTubePlaylist], set[YouTubeCourse],
        set[YouTubePost], set[YouTubeProduct]
    ]:
        '''
        Iterate over previously loaded tabs and scrape their content.
        ``browse_channel()`` or ``get_page_tabs()`` must have been called
        first.

        :returns: (video_ids, podcast_ids, playlists, courses, posts, products)
        :raises: RuntimeError if tabs have not been loaded
        '''

        if not self.tabs:
            raise RuntimeError(
                'Tabs not loaded — call browse_channel() first'
            )

        return await self._scrape_tabs(self.tabs)

    @staticmethod
    async def scrape_content(
        channel_id: str, proxies: list[str] | None = None
    ) -> tuple[
        set[str], set[str], set[YouTubePlaylist], set[YouTubeCourse],
        set[YouTubePost], set[YouTubeProduct]
    ]:
        instance = YouTubeChannelTabs(channel_id, proxies)
        await instance.browse_channel()
        return await instance.scrape_loaded_tabs()

    async def _scrape_tabs(self, tabs: list[dict[str, any]]) -> tuple[
        set[str], set[str], set[YouTubePlaylist], set[YouTubeCourse],
        set[YouTubePost], set[YouTubeProduct]
    ]:
        podcast_ids: set[str] = set()
        video_ids: set[str] = set()
        playlists: set[YouTubePlaylist] = set()
        courses: set[YouTubeCourse] = set()
        posts: set[YouTubePost] = set()
        products: set[YouTubeProduct] = set()

        extra: dict[str, str] = {'channel_id': self.channel_id}
        tab: dict[str, any]
        for tab in tabs:
            tab_renderer: dict[str, any] = tab.get('tabRenderer')

            if not tab_renderer:
                _LOGGER.debug(
                    'Channel has a tab without a tabRenderer, '
                    'skipping tab',
                    extra=extra | {
                        'title': tab.get('title', ''),
                    }
                )
                continue

            title: str = tab_renderer.get('title', '').lower()
            if title == 'home':
                continue

            # Extract the browse params for the tab
            params: str = tab_renderer['endpoint']['browseEndpoint']['params']

            # First page: use params to navigate to the tab
            page_data: dict = await self._browse(params=params)
            page_tab: dict[str, any] = self.get_tab(page_data, title)

            if title == 'playlists':
                playlists = self._get_playlist_items(page_tab)
                _LOGGER.debug(
                    'Parsed playlists',
                    extra=extra | {'playlists_length': len(playlists)}
                )
                continue
            elif title == 'courses':
                courses = self._get_course_items(page_tab)
                _LOGGER.debug(
                    'Parsed courses',
                    extra=extra | {'courses_length': len(courses)}
                )
                continue
            elif title == 'posts':
                posts = self._get_post_items(page_tab)
                _LOGGER.debug(
                    'Parsed posts',
                    extra=extra | {'posts_length': len(posts)}
                )
                continue
            elif title == 'store':
                products = self._get_product_items(page_tab)
                _LOGGER.debug(
                    'Parsed merch products',
                    extra=extra | {'products_length': len(products)}
                )
                continue

            contents: list = page_tab.get(
                'content', {}
            ).get(
                'richGridRenderer', {}
            ).get('contents', [])

            if not contents:
                _LOGGER.debug(
                    'No contents found for channel tab',
                    extra=extra | {'title': title}
                )
                continue

            if title == 'podcasts':
                # Podcasts page doesn't have a continuation token, so we
                # can exit after the first page
                podcast_ids = self._get_podcast_ids(contents)
                _LOGGER.debug(
                    'Parsed podcasts',
                    extra=extra | {'podcast_ids_length': len(podcast_ids)}
                )
                continue

            continuation_token: str = self.get_continuation_token(contents[-1])
            if 'continuationItemRenderer' in contents[-1]:
                contents = contents[:-1]

            _LOGGER.debug(
                'Parsed videos or shorts',
                extra=extra | {'contents_length': len(contents)}
            )
            for content in contents:
                video_id = self._extract_video_id(content, title)
                if video_id:
                    video_ids.add(video_id)

            # Subsequent pages: use continuation token
            while continuation_token:
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

                _LOGGER.debug(
                    'Parsed videos or shorts',
                    extra=extra | {
                        'continuation_items_length': len(
                            continuation_items
                        )
                    }
                )
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

    def _get_playlist_items(self, tab_renderer: dict[str, any]
                            ) -> set[YouTubePlaylist]:
        '''
        Parses playlists from the playlists tab.  The playlists tab uses a
        ``sectionListRenderer`` → ``gridRenderer`` layout instead of the
        ``richGridRenderer`` used by the videos / shorts / live tabs.

        :param tab_renderer: the tabRenderer dict for the playlists tab
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
                    YouTubePlaylist.from_innertube(item, self.channel_id)
                if playlist:
                    playlists.add(playlist)

        return playlists

    def _get_course_items(self, tab_renderer: dict[str, any]
                          ) -> set[YouTubeCourse]:
        '''
        Parses courses from the courses tab.  The courses tab uses
        ``richGridRenderer`` → ``richItemRenderer`` → ``playlistRenderer``.

        :param tab_renderer: the tabRenderer dict for the courses tab
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
                YouTubeCourse.from_innertube(item, self.channel_id)
            if course:
                courses.add(course)

        return courses

    def _get_post_items(self, tab_renderer: dict[str, any]
                        ) -> set[YouTubePost]:
        '''
        Parses posts from the posts/community tab.  The posts tab uses
        ``sectionListRenderer`` → ``itemSectionRenderer`` with
        ``backstagePostThreadRenderer`` items.

        :param tab_renderer: the tabRenderer dict for the posts tab
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
                    item, self.channel_id
                )
                if post:
                    posts.add(post)

        return posts

    def _get_product_items(self, tab_renderer: dict[str, any]
                           ) -> set[YouTubeProduct]:
        '''
        Parses products from the store tab.  The store tab uses
        ``sectionListRenderer`` → ``itemSectionRenderer`` →
        ``shelfRenderer`` → ``gridRenderer`` with
        ``verticalProductCardRenderer`` items.

        :param tab_renderer: the tabRenderer dict for the store tab
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
                        YouTubeProduct.from_innertube(item, self.channel_id)
                    if product:
                        products.add(product)

        return products

    async def _browse(self, params: str = '', continuation_token: str = '',
                      max_retries: int = 4) -> dict:
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
        penalty: float = 4.0
        _PENALTY_MAX: float = 300.0

        proxy_ip: str = (
            extract_proxy_ip(self.proxy) if self.proxy else 'none'
        )
        proxy_network: str = proxy_network_for(proxy_ip)
        extra: dict[str, str] = {
            'channel_id': self.channel_id,
            'proxy': self.proxy or 'none',
            'proxy_ip': proxy_ip,
            'proxy_network': proxy_network,
        }
        for attempt in range(1, max_retries + 1):
            self.client_request_count += 1
            if self.client_request_count > MAX_KEEPALIVE_REQUESTS:
                _LOGGER.debug(
                    'Client request count exceeded threshold, '
                    'creating new client',
                    extra=extra | {
                        'client_request_count': (
                            self.client_request_count
                        ),
                    }
                )
                self.client = self.get_innertube_client()

            await limiter.acquire(YouTubeCallType.BROWSE, proxy=self.proxy)

            start: float = time.monotonic()
            try:
                result: dict
                if not params:
                    if not continuation_token:
                        result = self.client.browse(self.channel_id)
                    else:
                        result = self.client.browse(
                            self.channel_id, continuation=continuation_token
                        )
                else:
                    result = self.client.browse(self.channel_id, params=params)
                METRIC_YT_REQUEST_DURATION.labels(
                    kind='innertube',
                    status_class='2xx',
                    worker_id=get_worker_id(),
                    proxy_ip=proxy_ip,
                    proxy_network=proxy_network,
                ).observe(time.monotonic() - start)
                return result
            except InnerTubeRequestError as exc:
                METRIC_YT_REQUEST_DURATION.labels(
                    kind='innertube',
                    status_class=('4xx' if exc.error.code == 429 else 'error'),
                    worker_id=get_worker_id(),
                    proxy_ip=proxy_ip,
                    proxy_network=proxy_network,
                ).observe(time.monotonic() - start)
                if exc.error.code == 429:
                    await limiter.penalise(
                        YouTubeCallType.BROWSE, self.proxy, penalty
                    )
                    _LOGGER.warning(
                        'InnerTube BROWSE rate-limited',
                        extra=extra | {
                            'attempt': attempt,
                            'max_retries': max_retries,
                            'penalty_seconds': penalty,
                        },
                    )
                    penalty = min(penalty * 2, _PENALTY_MAX)
                    if attempt < max_retries:
                        await AsyncYouTubeClient._delay(penalty, penalty)
                else:
                    _LOGGER.error(
                        'InnerTube BROWSE error',
                        exc=exc,
                        extra=extra | {
                            'attempt': attempt, 'max_retries': max_retries,
                            'penalty_seconds': penalty,
                        },
                    )
                    penalty = min(penalty * 2, _PENALTY_MAX)
                    if attempt < max_retries:
                        await AsyncYouTubeClient._delay(penalty, penalty)
                    _LOGGER.error(
                        'InnerTube BROWSE error',
                        exc=exc, extra=extra | {
                            'attempt': attempt, 'max_retries': max_retries,
                        },
                    )
                    if attempt < max_retries:
                        await AsyncYouTubeClient._delay(
                            penalty - 1, penalty
                        )
                    penalty = min(penalty * 2, _PENALTY_MAX)
            except Exception as exc:
                METRIC_YT_REQUEST_DURATION.labels(
                    kind='innertube',
                    status_class='error',
                    worker_id=get_worker_id(),
                    proxy_ip=proxy_ip,
                    proxy_network=proxy_network,
                ).observe(time.monotonic() - start)
                _LOGGER.error(
                    'InnerTube BROWSE error',
                    exc=exc,
                    extra=extra | {
                        'attempt': attempt,
                        'max_retries': max_retries,
                    },
                )
                if attempt < max_retries:
                    await AsyncYouTubeClient._delay(penalty - 1, penalty)
                penalty = min(penalty * 2, _PENALTY_MAX)

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
