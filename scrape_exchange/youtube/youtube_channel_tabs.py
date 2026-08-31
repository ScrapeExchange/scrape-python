'''
Class to parse the data returned by the Innertube API for the channel tabs
(Videos, Shorts, Live, Courses, Podcasts, Playlists, Posts, Store) and extract
the video IDs, playlist data, course data, post data, and product data.

:maintainer: Boinko <boinko@scrape.exchange>
:copyright: Copyright 2026
:license: GPLv3
'''

import asyncio
import functools
import threading
from concurrent.futures import ThreadPoolExecutor
from logging import Logger
from logging import getLogger
import logging
import time
from typing import Any, Callable, Final

import httpx
from prometheus_client import Counter, Histogram

from innertube import InnerTube
from innertube.errors import RequestError as InnerTubeRequestError

from scrape_exchange._lazy_async_pool import _LazyAsyncPool
from scrape_exchange.datatypes import MAX_KEEPALIVE_REQUESTS
from scrape_exchange.proxy_loader import _POOLED_HTTPX_LIMITS
from scrape_exchange.youtube.youtube_types import YouTubeChannelPageType

from .youtube_client import (
    ANDROID_CLIENT_VERSION,
    ANDROID_USER_AGENT,
    AsyncYouTubeClient,
    INNERTUBE_CLIENT_NAME,
    INNERTUBE_CLIENT_VERSION,
    METRIC_YT_REQUEST_DURATION,
    _get_scraper,
    generate_visitor_info,
)
from scrape_exchange.worker_id import get_worker_id
from scrape_exchange.proxy_loader import proxy_file_label
from scrape_exchange.proxy_phase_metrics import (
    install_innertube_phase_tracing,
)
from scrape_exchange.util import (
    extract_proxy_ip,
    extract_proxy_port,
    proxy_network_for,
)
from scrape_exchange.watchdog import Watchdog
from .youtube_cookiejar import YouTubeCookieJar
from .youtube_rate_limiter import YouTubeRateLimiter, YouTubeCallType
from .youtube_playlist import YouTubePlaylist
from .youtube_course import YouTubeCourse
from .youtube_post import YouTubePost
from .youtube_product import YouTubeProduct

_LOGGER: Logger = getLogger(__name__)

CHANNEL_TAB_DURATION: Histogram = Histogram(
    'channel_tab_scrape_duration_seconds',
    'Duration of one YouTube channel tab scrape.',
    [
        'platform', 'scraper', 'entity', 'tab',
        'outcome', 'worker_id',
    ],
    buckets=(
        0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
        10.0, 30.0, 60.0, 120.0, 300.0,
    ),
)
CHANNEL_TAB_PAGES: Counter = Counter(
    'channel_tab_pages_total',
    'YouTube channel tab pages fetched by page type.',
    [
        'platform', 'scraper', 'entity', 'tab',
        'page_type', 'outcome', 'worker_id',
    ],
)
CHANNEL_TAB_ITEMS: Counter = Counter(
    'channel_tab_items_total',
    'Items parsed from YouTube channel tabs.',
    [
        'platform', 'scraper', 'entity', 'tab',
        'item_type', 'worker_id',
    ],
)

_KNOWN_TAB_LABELS: Final[set[str]] = {
    'courses',
    'live',
    'playlists',
    'podcasts',
    'posts',
    'shorts',
    'store',
    'videos',
}


def _tab_metric_label(title: str) -> str:
    '''Return a low-cardinality Prometheus label for a tab title.'''

    normalised: str = title.strip().lower().replace(' ', '_')
    if normalised in _KNOWN_TAB_LABELS:
        return normalised
    return 'other'


def _tab_metric_base(tab: str) -> dict[str, str]:
    return {
        'platform': 'youtube',
        'scraper': _get_scraper(),
        'entity': 'channel',
        'tab': tab,
        'worker_id': get_worker_id(),
    }


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
        Return the pooled Innertube client for ``self.proxy``.

        The pool factory in this module handles cookie-jar load and
        a single VISITOR_INFO1_LIVE per proxy at first creation, so
        repeated calls reuse the warmed-up TCP/TLS session — the
        keep-alive ``MAX_KEEPALIVE_REQUESTS`` rotation that this
        method previously performed is intentionally collapsed
        (see the design spec for the trade-off discussion).

        :returns: An instance of the Innertube client.
        '''

        self.client_request_count = 0
        if not self.proxy:
            _LOGGER.warning(
                'No proxies configured, proceeding without proxies'
            )
        return pooled_innertube_for_entry(self.proxy)

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
        proxy: str | None = (
            YouTubeRateLimiter.get().select_proxy(YouTubeCallType.BROWSE)
            or (random.choice(proxies) if proxies else None)
        )
        instance = YouTubeChannelTabs(channel_id, proxy)
        await instance.browse_channel()
        return await instance.scrape_loaded_tabs()

    async def _scrape_tabs(self, tabs: list[dict[str, any]]) -> tuple[
        set[str], set[str], set[YouTubePlaylist], set[YouTubeCourse],
        set[YouTubePost], set[YouTubeProduct]
    ]:
        extra: dict[str, str] = {'channel_id': self.channel_id}
        tab_tasks: list[asyncio.Task[
            tuple[
                set[str], set[str], set[YouTubePlaylist],
                set[YouTubeCourse], set[YouTubePost],
                set[YouTubeProduct],
            ]
        ]] = []
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

            tab_tasks.append(asyncio.create_task(
                self._scrape_tab(tab_renderer, title),
            ))

        video_ids: set[str] = set()
        podcast_ids: set[str] = set()
        playlists: set[YouTubePlaylist] = set()
        courses: set[YouTubeCourse] = set()
        posts: set[YouTubePost] = set()
        products: set[YouTubeProduct] = set()
        results = await asyncio.gather(*tab_tasks)
        for (
            tab_video_ids, tab_podcast_ids,
            tab_playlists, tab_courses, tab_posts,
            tab_products,
        ) in results:
            video_ids.update(tab_video_ids)
            podcast_ids.update(tab_podcast_ids)
            playlists.update(tab_playlists)
            courses.update(tab_courses)
            posts.update(tab_posts)
            products.update(tab_products)

        return video_ids, podcast_ids, playlists, courses, posts, products

    async def _scrape_tab(
        self, tab_renderer: dict[str, any], title: str,
    ) -> tuple[
        set[str], set[str], set[YouTubePlaylist],
        set[YouTubeCourse], set[YouTubePost],
        set[YouTubeProduct],
    ]:
        started_at: float = time.monotonic()
        tab_label: str = _tab_metric_label(title)
        metric_base: dict[str, str] = _tab_metric_base(tab_label)
        outcome: str = 'success'
        active_page_type: str | None = None
        video_ids: set[str] = set()
        extra: dict[str, str] = {'channel_id': self.channel_id}

        try:
            params: str = tab_renderer[
                'endpoint'
            ]['browseEndpoint']['params']
            active_page_type = 'initial'
            page_data: dict = await self._browse(params=params)
            active_page_type = None
            CHANNEL_TAB_PAGES.labels(
                **metric_base,
                page_type='initial',
                outcome='success',
            ).inc()
            page_tab: dict[str, any] = self.get_tab(page_data, title)

            if title == 'playlists':
                playlists: set[YouTubePlaylist] = (
                    self._get_playlist_items(page_tab)
                )
                CHANNEL_TAB_ITEMS.labels(
                    **metric_base,
                    item_type='playlist',
                ).inc(len(playlists))
                _LOGGER.debug(
                    'Parsed playlists',
                    extra=extra | {'playlists_length': len(playlists)}
                )
                return video_ids, set(), playlists, set(), set(), set()
            if title == 'courses':
                courses: set[YouTubeCourse] = (
                    self._get_course_items(page_tab)
                )
                CHANNEL_TAB_ITEMS.labels(
                    **metric_base,
                    item_type='course',
                ).inc(len(courses))
                _LOGGER.debug(
                    'Parsed courses',
                    extra=extra | {'courses_length': len(courses)}
                )
                return video_ids, set(), set(), courses, set(), set()
            if title == 'posts':
                posts: set[YouTubePost] = self._get_post_items(
                    page_tab,
                )
                CHANNEL_TAB_ITEMS.labels(
                    **metric_base,
                    item_type='post',
                ).inc(len(posts))
                _LOGGER.debug(
                    'Parsed posts',
                    extra=extra | {'posts_length': len(posts)}
                )
                return video_ids, set(), set(), set(), posts, set()
            if title == 'store':
                products: set[YouTubeProduct] = (
                    self._get_product_items(page_tab)
                )
                CHANNEL_TAB_ITEMS.labels(
                    **metric_base,
                    item_type='product',
                ).inc(len(products))
                _LOGGER.debug(
                    'Parsed merch products',
                    extra=extra | {'products_length': len(products)}
                )
                return video_ids, set(), set(), set(), set(), products

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
                return video_ids, set(), set(), set(), set(), set()

            if title == 'podcasts':
                podcast_ids: set[str] = self._get_podcast_ids(
                    contents,
                )
                CHANNEL_TAB_ITEMS.labels(
                    **metric_base,
                    item_type='podcast_id',
                ).inc(len(podcast_ids))
                _LOGGER.debug(
                    'Parsed podcasts',
                    extra=extra | {
                        'podcast_ids_length': len(podcast_ids)
                    }
                )
                return video_ids, podcast_ids, set(), set(), set(), set()

            continuation_token: str = self.get_continuation_token(
                contents[-1],
            )
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

            # Subsequent pages for one tab keep their tab-local order.
            while continuation_token:
                active_page_type = 'continuation'
                continued_data: dict = await self._browse(
                    continuation_token=continuation_token
                )
                active_page_type = None

                CHANNEL_TAB_PAGES.labels(
                    **metric_base,
                    page_type='continuation',
                    outcome='success',
                ).inc()

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
                    video_id: str | None = self._extract_video_id(
                        item, title,
                    )
                    if video_id:
                        video_ids.add(video_id)

            CHANNEL_TAB_ITEMS.labels(
                **metric_base,
                item_type='video_id',
            ).inc(len(video_ids))
            return video_ids, set(), set(), set(), set(), set()
        except Exception:
            outcome = 'failure'
            if active_page_type is not None:
                CHANNEL_TAB_PAGES.labels(
                    **metric_base,
                    page_type=active_page_type,
                    outcome='failure',
                ).inc()
            raise
        finally:
            CHANNEL_TAB_DURATION.labels(
                **metric_base,
                outcome=outcome,
            ).observe(time.monotonic() - started_at)

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

            if video_id is not None:
                return video_id

            lockup_video_id: str | None = item.get(
                'richItemRenderer', {}
            ).get(
                'content', {}
            ).get(
                'lockupViewModel', {}
            ).get('contentId')

            return lockup_video_id

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
        proxy_port: str = (
            extract_proxy_port(self.proxy) if self.proxy else 'none'
        )
        proxy_file: str = proxy_file_label(self.proxy or '')
        extra: dict[str, str] = {
            'channel_id': self.channel_id,
            'proxy_ip': proxy_ip,
            'proxy_port': proxy_port,
            'proxy_network': proxy_network,
            'proxy_file': proxy_file,
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
                        result = await _call_innertube_browse(
                            self.client.browse,
                            self.channel_id,
                        )
                    else:
                        result = await _call_innertube_browse(
                            functools.partial(
                                self.client.browse,
                                self.channel_id,
                                continuation=continuation_token,
                            ),
                        )
                else:
                    result = await _call_innertube_browse(
                        functools.partial(
                            self.client.browse,
                            self.channel_id,
                            params=params,
                        ),
                    )
                duration: float = time.monotonic() - start
                METRIC_YT_REQUEST_DURATION.labels(
                    platform='youtube',
                    scraper=_get_scraper(),
                    api='innertube',
                    status_class='2xx',
                    worker_id=get_worker_id(),
                    proxy_file=proxy_file,
                ).observe(duration)
                _LOGGER.debug(
                    'InnerTube request completed',
                    extra=extra | {
                        'api': 'browse',
                        'duration': duration,
                        'status_class': '2xx',
                    },
                )
                return result
            except InnerTubeRequestError as exc:
                Watchdog.get().touch_work()
                duration = time.monotonic() - start
                status_class: str = (
                    '4xx' if exc.error.code == 429 else 'error'
                )
                METRIC_YT_REQUEST_DURATION.labels(
                    platform='youtube',
                    scraper=_get_scraper(),
                    api='innertube',
                    status_class=status_class,
                    worker_id=get_worker_id(),
                    proxy_file=proxy_file,
                ).observe(duration)
                _LOGGER.debug(
                    'InnerTube request failed',
                    exc=exc,
                    extra=extra | {
                        'api': 'browse',
                        'duration': duration,
                        'status_class': status_class,
                    },
                )
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
                Watchdog.get().touch_work()
                duration = time.monotonic() - start
                METRIC_YT_REQUEST_DURATION.labels(
                    platform='youtube',
                    scraper=_get_scraper(),
                    api='innertube',
                    status_class='error',
                    worker_id=get_worker_id(),
                    proxy_file=proxy_file,
                ).observe(duration)
                _LOGGER.debug(
                    'InnerTube request failed',
                    exc=exc,
                    extra=extra | {
                        'api': 'browse',
                        'duration': duration,
                        'status_class': 'error',
                    },
                )
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


# InnerTube player/next POSTs return small JSON bodies (~1–50 KB);
# the healthy p99 sits well under 3 s. We therefore use a tighter,
# explicit-per-class timeout for the sync InnerTube session than the
# shared :data:`_POOLED_HTTPX_DEFAULT_TIMEOUT` (which keeps a 10 s
# read default for paths with larger, slower bodies like the channel
# /about HTML). On 2026-05-17 09:17 UTC a YouTube WAF event caused
# slow-stall responses through commercial proxy networks (Hype, Bart,
# ProxySeller); the previous implicit 10 s read default did not fail
# fast enough and workers were only unwedged by the 60 s outer
# wait_for in :func:`_call_innertube`. Failing the inner call inside
# 8 s lets the retry/penalty loop in
# :func:`InnerTubeVideoParser.scrape` rotate to a different proxy
# while the outer cap stays a safety net.
_INNERTUBE_SYNC_HTTPX_TIMEOUT: Final[httpx.Timeout] = httpx.Timeout(
    connect=5.0, read=8.0, write=5.0, pool=5.0,
)

# Wall-clock safety net for a single channel-tab InnerTube BROWSE call.
# The sync HTTP stack should time out first, but live proxy stalls can still
# wedge executor futures long enough for the scraper watchdog to kill the
# worker. Keep this below WATCHDOG_WORK_TIMEOUT_SECONDS.
_INNERTUBE_BROWSE_HARD_TIMEOUT_SECONDS: float = 60.0


# Dedicated thread pool for the *synchronous* InnerTube calls
# (player/next/browse run via run_in_executor). Kept separate from the
# default ThreadPoolExecutor so a wedged InnerTube HTTP thread cannot
# starve the cookie-file / brotli ``to_thread`` work that keeps a worker
# alive — the silent-hang failure mode from 2026-05-30. Size is set from
# settings at startup via :func:`configure_innertube_executor`.
_INNERTUBE_EXECUTOR_THREADS: int = 16
_INNERTUBE_EXECUTOR: ThreadPoolExecutor | None = None
_INNERTUBE_EXECUTOR_LOCK: threading.Lock = threading.Lock()


def configure_innertube_executor(threads: int) -> None:
    '''Set the dedicated InnerTube executor size. Must be called before
    the first :func:`innertube_executor` use (i.e. at scraper startup);
    it does not resize an already-created pool.'''
    global _INNERTUBE_EXECUTOR_THREADS
    _INNERTUBE_EXECUTOR_THREADS = max(1, threads)


def innertube_executor() -> ThreadPoolExecutor:
    '''Return the process-wide dedicated InnerTube executor, creating it
    lazily on first use.'''
    global _INNERTUBE_EXECUTOR
    if _INNERTUBE_EXECUTOR is None:
        with _INNERTUBE_EXECUTOR_LOCK:
            if _INNERTUBE_EXECUTOR is None:
                _INNERTUBE_EXECUTOR = ThreadPoolExecutor(
                    max_workers=_INNERTUBE_EXECUTOR_THREADS,
                    thread_name_prefix='innertube',
                )
    return _INNERTUBE_EXECUTOR


def run_on_innertube_executor(
    fn: Callable[..., Any], *args: object,
) -> asyncio.Future:
    '''Schedule the synchronous *fn* on the dedicated InnerTube executor
    and return the awaitable. Keyword-bearing calls pass a
    ``functools.partial``.'''
    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
    return loop.run_in_executor(innertube_executor(), fn, *args)


async def _call_innertube_browse(
    fn: Callable[..., dict[str, Any]], *args: object,
) -> dict[str, Any]:
    '''
    Run one synchronous InnerTube BROWSE call with a wall-clock cap.
    '''

    return await asyncio.wait_for(
        run_on_innertube_executor(fn, *args),
        timeout=_INNERTUBE_BROWSE_HARD_TIMEOUT_SECONDS,
    )


def shutdown_innertube_executor() -> None:
    '''Drop and shut down the dedicated InnerTube executor without
    waiting for running threads (a wedged InnerTube call would hang a
    join; the process exits via os._exit regardless).'''
    global _INNERTUBE_EXECUTOR
    executor: ThreadPoolExecutor | None = _INNERTUBE_EXECUTOR
    _INNERTUBE_EXECUTOR = None
    if executor is not None:
        executor.shutdown(wait=False, cancel_futures=True)


def build_innertube_with_pool_limits(
    entry: str | None,
    client_name: str = 'WEB',
    client_version: str = INNERTUBE_CLIENT_VERSION,
    user_agent: str | None = None,
) -> InnerTube:
    '''Construct an :class:`InnerTube` whose underlying
    ``httpx.Client`` carries our shared pool limits
    (:data:`_POOLED_HTTPX_LIMITS` — max_keepalive 100,
    keepalive_expiry 120s) rather than the third-party
    library's defaults (5s keepalive_expiry, which evicts
    connections between consecutive calls under our
    cadence). The session timeout is taken from
    :data:`_INNERTUBE_SYNC_HTTPX_TIMEOUT` — see that
    constant's docstring for why it is tighter than the
    shared default.

    Cookie loading, visitor_id, header setup, and phase-
    tracing are NOT applied here; callers do that
    themselves because the pooled and one-off lifecycles
    diverge after construction (pooled lives forever
    across requests; one-off lives one enrichment cycle
    then closes).
    '''

    if user_agent is None:
        client: InnerTube = InnerTube(
            client_name, client_version, proxies=entry,
        )
    else:
        client = InnerTube(
            client_name,
            client_version,
            user_agent=user_agent,
            proxies=entry,
        )
    old_session: httpx.Client = client.adaptor.session
    client.adaptor.session = httpx.Client(
        base_url=old_session.base_url,
        headers=old_session.headers.copy(),
        limits=_POOLED_HTTPX_LIMITS,
        timeout=_INNERTUBE_SYNC_HTTPX_TIMEOUT,
        proxies=entry,
    )
    setattr(client, 'close', client.adaptor.session.close)
    old_session.close()
    return client


def _make_pooled_player_innertube_for_entry(
    entry: str | None,
) -> InnerTube:
    '''Build a cookie-free Android client for player requests.'''

    client: InnerTube = build_innertube_with_pool_limits(
        entry,
        client_name='ANDROID',
        client_version=ANDROID_CLIENT_VERSION,
        user_agent=ANDROID_USER_AGENT,
    )
    install_innertube_phase_tracing(
        client.adaptor.session,
        proxy_file=proxy_file_label(entry or ''),
    )
    return client


def _make_pooled_innertube_for_entry(
    entry: str | None,
) -> InnerTube:
    '''Pool factory for the third-party InnerTube library. Cookie-
    jar warm-up and a single (stable) VISITOR_INFO1_LIVE happen
    once per entry. Subsequent calls reuse the cached session.

    Uses :func:`build_innertube_with_pool_limits` so the underlying
    session carries our keep-alive settings instead of the library
    defaults (5s expiry).
    '''

    client: InnerTube = build_innertube_with_pool_limits(entry)
    YouTubeCookieJar.get().load_into_session(
        client.adaptor.session, entry,
    )
    visitor_id: str = generate_visitor_info()
    client.adaptor.session.cookies.set(
        'VISITOR_INFO1_LIVE', visitor_id,
        domain='.youtube.com', path='/',
    )
    client.adaptor.session.headers[
        'X-YouTube-Client-Name'
    ] = INNERTUBE_CLIENT_NAME
    client.adaptor.session.headers[
        'X-YouTube-Client-Version'
    ] = INNERTUBE_CLIENT_VERSION
    install_innertube_phase_tracing(
        client.adaptor.session,
        proxy_file=proxy_file_label(entry or ''),
    )
    return client


_INNERTUBE_POOL: _LazyAsyncPool[
    str | None, InnerTube,
] = _LazyAsyncPool(
    factory=_make_pooled_innertube_for_entry,
    aclose_attr='close',
)

_PLAYER_INNERTUBE_POOL: _LazyAsyncPool[
    str | None, InnerTube,
] = _LazyAsyncPool(
    factory=_make_pooled_player_innertube_for_entry,
    aclose_attr='close',
)


def pooled_innertube_for_entry(entry: str | None) -> InnerTube:
    '''Return the long-lived, keep-alive-pooled
    :class:`innertube.InnerTube` for ``entry``. Same instance
    across calls for the same key. Cookie-jar and visitor_id
    setup happen once at first creation.'''

    return _INNERTUBE_POOL.get(entry)


def pooled_player_innertube_for_entry(
    entry: str | None,
) -> InnerTube:
    '''Return the pooled Android VR client for player requests.'''

    return _PLAYER_INNERTUBE_POOL.get(entry)


def borrow_pooled_innertube_for_entry(
    entry: str | None,
) -> InnerTube:
    '''Borrow the Web client used for one ``next`` request.'''

    return _INNERTUBE_POOL.borrow(entry)


async def release_pooled_innertube(client: InnerTube) -> None:
    '''Release a Web client borrowed for one request.'''

    await _INNERTUBE_POOL.release(client)


def borrow_pooled_player_innertube_for_entry(
    entry: str | None,
) -> InnerTube:
    '''Borrow the Android VR client used for one player request.'''

    return _PLAYER_INNERTUBE_POOL.borrow(entry)


async def release_pooled_player_innertube(
    client: InnerTube,
) -> None:
    '''Release an Android VR client borrowed for one request.'''

    await _PLAYER_INNERTUBE_POOL.release(client)


async def refresh_pooled_innertube_for_entry(
    entry: str | None,
    *,
    challenged: InnerTube,
) -> bool:
    '''Retire the exact player-client generation YouTube challenged.

    New calls immediately receive a replacement. Existing borrowers keep
    the challenged generation alive until their requests finish. A stale
    response cannot retire a replacement created by an earlier response.
    '''

    return await _PLAYER_INNERTUBE_POOL.retire_key(
        entry,
        expected=challenged,
    )


async def refresh_pooled_web_innertube_for_entry(
    entry: str | None,
    *,
    challenged: InnerTube,
) -> bool:
    '''Retire the exact Web-client generation that failed transport.'''

    return await _INNERTUBE_POOL.retire_key(
        entry,
        expected=challenged,
    )


async def aclose_pooled_innertube() -> None:
    '''Close every pooled InnerTube session and empty the pool.
    Called from the scraper shutdown drain.'''

    await _INNERTUBE_POOL.aclose_all()
    await _PLAYER_INNERTUBE_POOL.aclose_all()


def _reset_pool_for_tests() -> None:
    '''Drop cached pooled InnerTube sessions without calling
    aclose. Tests only.'''

    _INNERTUBE_POOL.reset_for_tests()
    _PLAYER_INNERTUBE_POOL.reset_for_tests()
