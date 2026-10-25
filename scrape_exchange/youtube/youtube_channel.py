'''
Model a Youtube channel

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import os
import re
import time

from typing import Awaitable, Callable, Self
from logging import Logger
from logging import getLogger
from datetime import UTC, datetime
from urllib.parse import urlsplit

from ..brotli import brotli_write_async
import orjson
import country_converter

import httpx
from innertube import InnerTube
from yt_dlp import YoutubeDL

from ..datatypes import IngestStatus
from ..datatypes import SocialNetworks

import random

from prometheus_client import Counter

from scrape_exchange.worker_id import get_worker_id
from scrape_exchange.proxy_loader import proxy_file_label
from scrape_exchange.util import (
    extract_proxy_ip as _extract_proxy_ip,
    extract_proxy_port as _extract_proxy_port,
)

from .youtube_client import (
    AsyncYouTubeClient,
    CONSENT_COOKIES,
    METRIC_YT_REQUEST_DURATION,
    _get_scraper,
    pooled_youtube_client_for_entry,
)
from .youtube_rate_limiter import (
    YouTubeCallType,
    YouTubeRateLimiter,
)

from .youtube_video import YouTubeVideo
from .youtube_video import DENO_PATH
from .youtube_video import PO_TOKEN_URL
from .youtube_video import YTDLP_CACHE_DIR
from .youtube_thumbnail import YouTubeThumbnail
from .youtube_playlist import YouTubePlaylist
from .youtube_course import YouTubeCourse
from .youtube_post import YouTubePost
from .youtube_types import YouTubeChannelLink
from .youtube_product import YouTubeProduct

from .youtube_external_link import YouTubeExternalLink
from .channel_errors import ChannelRedirectedError
from .channel_errors import ChannelTerminatedError
from .youtube_channel_tabs import KnownVideoIdsFn
from .youtube_channel_tabs import (
    ListingCheckpointFn as TabListingCheckpointFn,
)
from .youtube_channel_tabs import YouTubeChannelTabs
from .youtube_channel_tabs import borrow_pooled_innertube_for_entry
from .youtube_channel_tabs import refresh_pooled_web_innertube_for_entry
from .youtube_channel_tabs import release_pooled_innertube

from ..util import split_quoted_string, convert_number_string
from ..util import get_imported_assets
from ..file_management import VIDEO_FILE_PREFIX


_LOGGER: Logger = getLogger(__name__)

# Called with the channel once its first page is parsed; True stops the
# scrape before the video tabs are enumerated.
StopIfFn = Callable[['YouTubeChannel'], bool]

# Called during a resumable full listing (see :meth:`YouTubeChannel.scrape`)
# with the channel, the tab, the continuation token of the tab's next
# page and the video IDs found since the previous checkpoint.
ListingCheckpointFn = Callable[
    ['YouTubeChannel', str, str, set[str]], Awaitable[None]
]

# Limits the amount of videos imported for a channel
# per run
MAX_CHANNEL_VIDEOS_PER_RUN: int = 40

# Record keys a successful /about scrape fills (scrape_about_page and
# the parsers it calls).
ABOUT_FIELDS: frozenset[str] = frozenset({
    'title', 'description', 'rss_url', 'available_country_codes',
    'keywords', 'is_family_safe', 'joined_date', 'view_count',
    'video_count', 'subscriber_count', 'external_urls', 'country',
    'channel_thumbnails', 'banners', 'channel_links',
})
# Record keys a successful InnerTube browse fills
# (parse_channel_video_data).
INNERTUBE_FIELDS: frozenset[str] = frozenset({
    'title', 'description', 'keywords', 'is_family_safe',
    'channel_thumbnails', 'banners', 'subscriber_count', 'video_count',
})
# The record key each content tab fills. A tab the channel lacks
# leaves its key fetched (and empty); a tab that failed does not.
TAB_FIELDS: dict[str, str] = {
    'playlists': 'playlists',
    'courses': 'courses',
    'posts': 'posts',
    'store': 'merch',
    'videos': 'video_ids',
    'shorts': 'video_ids',
    'live': 'video_ids',
}
TAB_RECORD_FIELDS: frozenset[str] = frozenset(TAB_FIELDS.values())
# Always written: the keys the channel schema requires.
IDENTITY_FIELDS: frozenset[str] = frozenset(
    {'channel_id', 'channel_handle', 'url'},
)
_COUNT_FIELDS: frozenset[str] = frozenset(
    {'subscriber_count', 'video_count', 'view_count'},
)

HTTP_PREFIX: str = 'http://'
HTTPS_PREFIX: str = 'https://'

RESOLV_URL: str = 'navigation/resolve_url'

# Outcomes from
# :meth:`YouTubeChannel._resolve_channel_id_via_innertube`. Labels:
#
# - ``strategy`` is ``"resolve_url"`` (``adaptor.dispatch(RESOLV_URL,
#   ...)``). The former ``"browse"`` strategy was removed because
#   InnerTube browse rejects handles with HTTP 400.
# - ``outcome`` is ``"hit"`` (channel_id obtained), ``"miss"`` (call
#   succeeded but no usable browseId/externalId in response), or
#   ``"error"`` (call raised an exception).
METRIC_CHANNEL_HANDLE_RESOLVER_OUTCOMES: Counter = Counter(
    'channel_handle_resolver_outcomes_total',
    'Outcomes of the InnerTube handle->channel_id resolver, '
    'used as a fallback when /about HTML did not supply a '
    'channel_id.',
    [
        'platform', 'scraper', 'entity',
        'strategy', 'outcome', 'worker_id',
        'proxy_ip', 'proxy_port', 'proxy_file',
    ],
)

# Whether a subscriber count could be parsed from a scraped channel
# page. ``source`` is ``"innertube"`` (pageHeaderRenderer metadata
# rows of the InnerTube browse response) or ``"about_page"``
# (``subscriberCountText`` of the /about HTML page). ``outcome`` is
# ``"found"``, ``"absent"`` or ``"unparseable"``. ``is_retry`` marks
# a forced missing-subscriber retry scrape.
METRIC_CHANNEL_SUBSCRIBER_COUNT_PARSE: Counter = Counter(
    'channel_subscriber_count_parse_total',
    'Attempts to parse the subscriber count from a scraped '
    'YouTube channel page, by page source and outcome.',
    ['platform', 'scraper', 'source', 'outcome', 'is_retry',
     'worker_id'],
)


def record_subscriber_count_parse(
    source: str, outcome: str, is_retry: bool = False,
) -> None:
    '''
    Record the outcome of parsing the subscriber count from *source*.

    :param source: ``'innertube'`` or ``'about_page'``
    :param outcome: ``'found'``, ``'absent'`` or ``'unparseable'``
    :param is_retry: whether this scrape is a forced missing-subscriber
        retry
    :returns: (none)
    :raises: (none)
    '''

    METRIC_CHANNEL_SUBSCRIBER_COUNT_PARSE.labels(
        platform='youtube',
        scraper=_get_scraper(),
        source=source,
        outcome=outcome,
        is_retry='true' if is_retry else 'false',
        worker_id=get_worker_id(),
    ).inc()


async def _resolver_innertube_call(
    proxy: str | None, call: Callable[[InnerTube], dict],
) -> dict:
    '''
    Run one resolver InnerTube call on a borrowed pooled Web client.

    A transport error retires the client generation: httpcore 0.16
    leaves failed proxy tunnels in the pool until it is exhausted.

    :param proxy: the proxy entry whose pooled client to use
    :param call: the synchronous call to make with the client
    :returns: the InnerTube response
    :raises: whatever *call* raises
    '''

    limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()
    client: InnerTube = borrow_pooled_innertube_for_entry(proxy)
    try:
        result: dict = call(client)
    except httpx.TransportError as exc:
        await refresh_pooled_web_innertube_for_entry(
            proxy, challenged=client,
        )
        if not isinstance(exc, httpx.ReadTimeout):
            await limiter.report_proxy_result(proxy, False)
        raise
    finally:
        await release_pooled_innertube(client)
    await limiter.report_proxy_result(proxy, True)
    return result


TERMINAL_CHANNEL_PAGE_MESSAGES: tuple[str, ...] = (
    (
        'This channel was removed because it violated our '
        'Community Guidelines.'
    ),
    (
        'This account has been terminated because we received '
        'multiple third-party claims of copyright infringement '
        'regarding material the user posted.'
    ),
    'This channel is not available.',
)


def _normalise_page_text(value: str) -> str:
    return ' '.join(value.split())


def terminal_channel_page_message(page_contents: str) -> str | None:
    '''Return a terminal channel-page message found in raw HTML.'''

    normalised: str = _normalise_page_text(page_contents)
    for message in TERMINAL_CHANNEL_PAGE_MESSAGES:
        if _normalise_page_text(message) in normalised:
            return message
    return None


def canonical_handle_from_browse(channel_data: dict) -> str | None:
    '''
    Extract the canonical @-handle from an InnerTube browse_channel()
    response. YouTube normalises the casing in vanityChannelUrl, so
    this is the UI-faithful handle we use for platform_creator_id.

    :param channel_data: Return value of
        YouTubeChannelTabs.browse_channel().
    :returns: The handle without the leading '@' (e.g. 'HistoryMatters'),
        or None when vanityChannelUrl is missing, empty, or contains no
        '/@' segment (legacy handle-less channels).
    '''

    metadata: dict = channel_data.get(
        'metadata', {},
    ).get('channelMetadataRenderer', {})
    vanity_url: str | None = metadata.get('vanityChannelUrl')
    if not vanity_url or '/@' not in vanity_url:
        return None
    handle: str = vanity_url.split('/@', 1)[1]
    handle = handle.split('/', 1)[0].split('?', 1)[0]
    return handle or None


def fallback_handle(name: str) -> str:
    '''
    Deterministic fallback for channels with no canonical handle.
    Strips leading '@' and whitespace, lowercases, then makes
    the result safe to use as a filename: '/' becomes '_'
    (otherwise Path would treat the handle as a sub-path),
    NULs are removed (POSIX rejects them), and leading '.' is
    stripped (would create a hidden file). Used only when
    YouTube returns no vanityChannelUrl for a channel. Never
    used to paper over InnerTube failures.

    :param name: The channel name or title to normalise.
    :returns: Lowercased, stripped, filesystem-safe name.
    :raises ValueError: When the result would be empty.
    '''

    cleaned: str = name.replace('\x00', '')
    result: str = cleaned.strip().lstrip('@').strip().lower()
    result = result.replace('/', '_').lstrip('.')
    if not result:
        raise ValueError(
            f'fallback_handle produced empty result from {name!r}'
        )
    return result


class YouTubeChannel:
    CHANNEL_URL: str = AsyncYouTubeClient.SCRAPE_URL + '/{channel_handle}'
    CHANNEL_URL_WITH_AT: str = \
        AsyncYouTubeClient.SCRAPE_URL + '/@{channel_handle}'
    CHANNEL_URL_WITH_ID: str = \
        AsyncYouTubeClient.SCRAPE_URL + '/channel/{channel_id}'

    CHANNEL_HANDLE_REGEX: re.Pattern[str] = re.compile(
        r'"canonicalBaseUrl":"/@([^"]+)"'
    )
    CHANNEL_ID_REGEX_SCRAPE: re.Pattern[str] = \
        re.compile(r'"externalId":"(.*?)"')
    CHANNEL_SCRAPE_REGEX_SHORT: re.Pattern[str] = re.compile(
        r'var ytInitialData = (.*?);'
    )
    CHANNEL_SCRAPE_REGEX: re.Pattern[str] = re.compile(
        r'var ytInitialData = (.*?);$'
    )
    RX_SCRAPE_CHANNEL_ID: re.Pattern[str] = re.compile(
        r'"externalId":"(.*?)"'
    )
    # Prefix must be canonical uppercase ``UC``. Body is
    # case-sensitive base64url: distinct case identifies distinct
    # channels, so the body match is ``[A-Za-z0-9_-]`` rather than
    # case-insensitive ``[A-Z0-9_-]``. Lowercase ``uc`` inputs are
    # recoverable via :meth:`normalise_channel_id`.
    CHANNEL_ID_REGEX_MATCH: re.Pattern[str] = re.compile(
        r'^UC[A-Za-z0-9_-]{22}$',
    )

    def __init__(
        self, channel_handle: str | None = None,
        channel_id: str | None = None,
        deno_path: str = DENO_PATH, po_token_url: str = PO_TOKEN_URL,
        ytdlp_cache_dir: str = YTDLP_CACHE_DIR,
        debug: bool = False, save_dir: str = None,
        consent_cookies: dict[str, str] | None = CONSENT_COOKIES,
        with_download_client: bool = True,
    ) -> None:
        '''
        Models a YouTube channel.

        :param channel_handle: the channel's URL-slug handle as it appears
            in the vanity URL — e.g. for
            ``https://www.youtube.com/@HistoryMatters``
            the handle is ``'HistoryMatters'``. Distinct from
            :attr:`title` (the human-readable display name).
        :param channel_id: the opaque ``UC...`` identifier (immutable across
            renames, unlike the handle).
        :param deno_path: path to the Deno executable for running the
            download client.
        :param po_token_url: URL to get the PO token for the download client.
        :debug: whether to enable debug logging for the download client.
        :param save_dir: directory to save downloaded media assets to.
        :param consent_cookies: cookies to use to bypass consent pages.
        '''

        self.consent_cookies: dict[str, str] = consent_cookies
        self.save_dir: str | None = save_dir

        # Only used for scraping channel about page. Use with caution!
        self.browse_client: AsyncYouTubeClient | None = None

        self.browse_request_count: int = 0

        # Set True by ``scrape_about_page`` on success; consumed by
        # the scraper tool to label the ``api`` dimension on the
        # ``scrapes_completed_total`` metric (``html`` if /about
        # succeeded, ``innertube`` if only the InnerTube fallback
        # produced the data).
        self.about_page_succeeded: bool = False

        if with_download_client:
            self.download_client: YoutubeDL = \
                YouTubeVideo._setup_download_client(
                    deno_path=deno_path,
                    po_token_url=po_token_url,
                    ytdlp_cache_dir=ytdlp_cache_dir,
                    debug=debug,
                )

        self.url: str | None = None
        self.title: str | None = None
        self.channel_handle: str | None = None
        if channel_handle:
            self.channel_handle = channel_handle.lstrip('@')
        # channel_id is preferred for the scrape URL whenever available:
        # it is stable and free of UTF-8, unlike the @handle. The handle
        # (when supplied) is still kept above as metadata.
        if channel_id:
            self.url = YouTubeChannel.CHANNEL_URL_WITH_ID.format(
                channel_id=channel_id
            )
        elif self.channel_handle:
            self.url = YouTubeChannel.CHANNEL_URL_WITH_AT.format(
                channel_handle=self.channel_handle.replace(' ', '')
            )
        self.channel_id: str | None = channel_id
        self.description: str | None = None
        self.category: str | None = None
        self.keywords: set[str] = set()
        self.verified: bool = False
        self.is_family_safe: bool = False
        self.available_country_codes: set[str] = set()
        self.channel_thumbnails: set[YouTubeThumbnail] = set()
        self.country: str | None = None
        self.joined_date: datetime | None = None

        # This thumbnail is used for the YouTubeVideo.channel_thumbnail
        self.channel_thumbnail: YouTubeThumbnail | None = None

        self.banners: set[YouTubeThumbnail] = set()
        self.external_urls: set[YouTubeExternalLink] = set()

        # YouTube does not seem to keep these RSS feeds up to date
        self.rss_url: str | None = None

        # The number of subscribers and views are not always available
        self.subscriber_count: int | None = None
        self.subscriber_count_retry: bool = False
        self.video_count: int | None = None
        self.view_count: int | None = None
        # Set when a scrape stopped early because ``stop_if`` said the
        # channel is too small to enumerate its videos.
        self.scrape_stopped_early: bool = False

        self.channel_links: set[YouTubeChannelLink] = set()

        self.video_ids: set[str] = set()
        # False when tab paging stopped early at already-known IDs, so
        # video_ids holds only the channel's newest videos.
        self.video_ids_complete: bool = True
        # Per listed tab after a resumable full listing: LISTING_TAB_DONE
        # or the continuation token the next scrape resumes from.
        self.listing_state: dict[str, str] = {}
        # True when every video, short and live broadcast has been
        # listed, by this scrape alone or by a resumable full listing
        # over several scrapes.
        self.video_listing_complete: bool = True
        # Record keys filled by this scrape; see to_dict(only_fetched).
        self.fetched_fields: set[str] = set()
        # Set when part of the content fetch failed but the scrape
        # still collected useful data.
        self.partial_error: str | None = None
        # True when the partial part is channel content (a tab or the
        # InnerTube browse failed); False when only /about failed, so
        # the content collected is complete.
        self.content_partial: bool = False
        self.podcast_ids: set[str] = set()
        self.merch: set[YouTubeProduct] = set()
        self.courses: set[YouTubeCourse] = set()
        self.playlists: set[YouTubePlaylist] = set()
        self.posts: set[YouTubePost] = set()

        self.videos: set[YouTubeVideo] = set()

    def _create_browse_client(self, proxies: list[str] | str | None = None
                              ) -> None:
        '''
        Get the YouTube client for browsing/scraping data. This is separate
        from the download client used for downloading media assets.

        :returns: the YouTube client for browsing/scraping data
        '''

        selected_proxy: str | None = (
            YouTubeRateLimiter.get().select_proxy(
                YouTubeCallType.HTML,
            )
            or (random.choice(proxies) if proxies else None)
        )
        self.browse_client = pooled_youtube_client_for_entry(
            selected_proxy,
        )
        self.proxies: list[str] | None = proxies
        self.browse_request_count: int = 0

    def __eq__(self, other: Self) -> bool:
        if not isinstance(other, YouTubeChannel):
            return False

        equal: bool = (
            self.channel_handle == other.channel_handle and
            self.channel_id == other.channel_id and
            self.title == other.title and
            self.description == other.description and
            self.category == other.category and
            self.joined_date == other.joined_date and
            self.rss_url == other.rss_url and
            self.verified == other.verified and
            self.is_family_safe == other.is_family_safe and
            self.video_count == other.video_count and
            self.view_count == other.view_count and
            self.subscriber_count == other.subscriber_count and
            self.channel_links == other.channel_links and
            self.external_urls == other.external_urls and
            self.country == other.country and
            self.keywords == other.keywords and
            self.banners == other.banners and
            self.available_country_codes == other.available_country_codes and
            self.channel_thumbnails == other.channel_thumbnails and
            self.banners == other.banners
        )
        return equal

    def to_dict(
        self, with_video_ids: bool = False, only_fetched: bool = False,
    ) -> dict[str, any]:
        '''
        :param with_video_ids: include ``video_ids``
        :param only_fetched: emit only the identity keys and the keys in
        :attr:`fetched_fields`, so a partial scrape's record leaves out
        what it did not fetch instead of writing it as empty
        '''
        data: dict[str, any] = self._to_dict(with_video_ids)
        if not only_fetched:
            return data
        keep: set[str] = (
            IDENTITY_FIELDS | self.fetched_fields
            | ({'video_ids'} if with_video_ids else set())
        )
        return {key: value for key, value in data.items() if key in keep}

    def _to_dict(self, with_video_ids: bool = False) -> dict[str, any]:
        data: dict[str, any] = {
            'channel_id': self.channel_id,
            'channel_handle': (
                self.channel_handle.lstrip('@')
                if self.channel_handle else None
            ),
            'title': self.title,
            'description': self.description,
            'category': self.category,
            'keywords': list(self.keywords),
            'is_family_safe': self.is_family_safe,
            'country': self.country,
            'available_country_codes': list(self.available_country_codes),
            'channel_thumbnails': [
                t.to_dict() for t in self.channel_thumbnails or set()
            ],
            'banners': [b.to_dict() for b in self.banners or set()],
            'external_urls': [
                el.to_dict() for el in self.external_urls or set()
            ],
            'joined_date':
                self.joined_date.isoformat() if self.joined_date else None,
            'url': self.url,
            'rss_url': self.rss_url,
            'verified': self.verified,
            'channel_links': [
                cl.to_dict() for cl in self.channel_links or set()
            ],
            'courses': [c.to_dict() for c in self.courses or set()],
            'playlists': [p.to_dict() for p in self.playlists or set()],
            'posts': [p.to_dict() for p in self.posts or set()],
            'merch': [m.to_dict() for m in self.merch or set()],
        }

        if self.subscriber_count is not None:
            data['subscriber_count'] = self.subscriber_count
        if self.video_count is not None:
            data['video_count'] = self.video_count
        if self.view_count is not None:
            data['view_count'] = self.view_count

        if with_video_ids:
            data['video_ids'] = list(self.video_ids)

        return data

    @staticmethod
    def from_dict(
        data: dict[str, any],
        with_download_client: bool = False,
    ) -> Self:
        '''
        Reconstruct a :class:`YouTubeChannel` from a previously-
        serialised dict. Defaults to ``with_download_client=False``
        because reconstruction paths (bulk upload, creator-map
        rebuild, watcher upload, etc.) do not scrape; spinning up
        yt-dlp per record adds latency and resource cost without
        benefit. Callers that intend to scrape after reconstruction
        can opt in via ``with_download_client=True``.
        '''
        channel = YouTubeChannel(
            channel_handle=data.get('channel_handle'),
            channel_id=data.get('channel_id'),
            with_download_client=with_download_client,
        )
        channel.title = data.get('title')
        channel.description = data.get('description')
        channel.category = data.get('category')
        channel.keywords = set(data.get('keywords', []))
        channel.is_family_safe = data.get('is_family_safe', False)
        channel.country = data.get('country')
        channel.available_country_codes = set(
            data.get('available_country_codes', [])
        )
        channel.channel_thumbnails = {
            YouTubeThumbnail.from_dict(t) for t in data.get(
                'channel_thumbnails', []
            )
        }
        channel.banners = {
            YouTubeThumbnail.from_dict(b) for b in data.get('banners', [])
        }
        channel.external_urls = {
            YouTubeExternalLink.from_dict(el) for el in data.get(
                'external_urls', []
            )
        }
        joined_date_str: str | None = data.get('joined_date')
        if joined_date_str:
            channel.joined_date = datetime.fromisoformat(joined_date_str)
        channel.url = data.get('url') or channel.url
        channel.rss_url = data.get('rss_url')
        channel.verified = data.get('verified', False)
        channel.subscriber_count = data.get('subscriber_count')
        channel.video_count = data.get('video_count')
        channel.view_count = data.get('view_count')

        for cl in data.get('channel_links', []):
            try:
                channel.channel_links.add(YouTubeChannelLink.from_dict(cl))
            except Exception:
                continue

        channel.courses = {
            YouTubeCourse.from_dict(c) for c in data.get('courses', [])
        }
        channel.playlists = {
            YouTubePlaylist.from_dict(p) for p in data.get('playlists', [])
        }
        channel.posts = {
            YouTubePost.from_dict(p) for p in data.get('posts', [])
        }
        channel.merch = {
            YouTubeProduct.from_dict(m) for m in data.get('merch', [])
        }
        channel.video_ids = set(data.get('video_ids', []))
        return channel

    def _extract_initial_data(self, html_content: str) -> dict:
        '''
        Extract ytInitialData from the HTML page

        :param html_content: Raw HTML content from YouTube page
        :returns: Parsed ytInitialData dictionary or None if not found
        :raises: ValueError: If a consent page is detected (only when consent
        cookies are not set)
        '''

        # Verified badge is hard to find otherwise
        self.verified = (
            self.verified or YouTubeChannel.extract_verified_status(
                page_data=html_content
            )
        )

        # YouTube embeds data in a script tag as ytInitialData
        # Try multiple patterns as YouTube's format can vary
        patterns: list[str] = [
            r'ytInitialData\s*=\s*({.*?});',
            r'window\["ytInitialData"\] = ({.*?});',
        ]

        for pattern in patterns:
            match: re.Match[str] | None = re.search(
                pattern, html_content, re.DOTALL
            )
            if match:
                try:
                    return orjson.loads(match.group(1))
                except orjson.JSONDecodeError:
                    continue

        raise ValueError(
            f'Could not extract data for channel {self.channel_handle}'
        )

    def _extract_handle(self, url: str, metadata: dict) -> str | None:
        '''
        Extract channel handle from URL or metadata

        :param url:the URL of the channel page
        :param metadata: the channel metadata extracted from the page
        :returns: the channel handle (e.g. @HistoryMatters) or None if not
        found
        :raises: ValueError: If a consent page is detected (only when consent
        cookies are not set)
        '''

        # Try to extract from URL
        if '@' in url:
            parts: list[str] = url.split('@')
            if len(parts) > 1:
                handle: str = '@' + parts[1].split('/')[0].split('?')[0]
                return handle

        # Try from metadata
        if metadata.get('channelUrl'):
            channel_url: str = metadata['channelUrl']
            if '@' in channel_url:
                parts = channel_url.split('@')
                if len(parts) > 1:
                    return '@' + parts[1].split('/')[0]

        return None

    def _parse_thumbnails(self, thumbnails_list: list[dict]
                          ) -> dict[str, dict]:
        '''
        Parse thumbnail data into schema format
        '''

        thumbnails: dict = {}

        if len(thumbnails_list) >= 1:
            thumbnails['default'] = self._parse_thumbnail(thumbnails_list[0])
        if len(thumbnails_list) >= 2:
            thumbnails['medium'] = self._parse_thumbnail(thumbnails_list[1])
        if len(thumbnails_list) >= 3:
            thumbnails['high'] = self._parse_thumbnail(thumbnails_list[-1])

        return thumbnails

    def _parse_thumbnail(self, thumbnail: dict) -> dict[str, any]:
        '''
        Parse a single thumbnail
        '''

        return {
            'url': thumbnail.get('url'),
            'width': thumbnail.get('width'),
            'height': thumbnail.get('height'),
        }

    @staticmethod
    def _extract_links(initial_data: dict) -> set[YouTubeExternalLink]:
        '''
        Extract external links from the header dict of the channel 'about'
        data.
        '''

        links: set[YouTubeExternalLink] = set()

        # Links are typically in the header or about section
        header: dict[str, any] = initial_data.get('header', {})
        header_renderer: dict[str, any] = (
            header.get('c4TabbedHeaderRenderer') or
            header.get('pageHeaderRenderer') or
            {}
        )

        # Check for primary links
        if not header_renderer:
            return set()

        primary_links: list[dict[str, any]] = header_renderer.get(
            'headerLinks', {}
        ).get(
            'channelHeaderLinksRenderer', {}
        ).get(
            'primaryLinks', []
        )
        priority: int = 10
        for link in primary_links:
            url: str = link.get(
                'navigationEndpoint', {}
            ).get(
                'urlEndpoint', {}
            ).get('url')

            if url:
                links.add(
                    YouTubeExternalLink(
                        name=link.get(
                            'title', {}
                        ).get(
                            'simpleText', 'Link'
                        ),
                        url=url,
                        priority=priority,
                    )
                )
                priority += 10

        return links

    @staticmethod
    def parse_external_urls(data: dict[str, any]) -> set[YouTubeExternalLink]:
        '''
        Parses the external URLs out of the YouTube channel about page
        'about renderer'

        :param data: the YouTube channel page as a dict
        :returns: list of external URLs
        '''

        external_links: set[YouTubeExternalLink] = set()

        field_name: str = 'channelExternalLinkViewModel'
        priority: int = 10
        for item in data or []:
            item_data: dict[str, dict[str, any]] = item.get(field_name)
            if not item_data:
                continue

            title: str = item_data.get('title', {}).get('content', {})
            url: str = item_data.get('link', {}).get('content')
            external_link: YouTubeExternalLink | None = \
                YouTubeChannel._generate_external_link(url, priority, title)
            priority += 10
            if external_link:
                external_links.add(external_link)

        return external_links

    def _reconcile_description_external_links(self) -> None:
        '''Add missing social and email links from the description.'''

        if not self.description:
            return

        description_networks: set[str] = set()
        for external_link in self.external_urls:
            network_name: str | None = self._social_network_name(
                external_link.url
            )
            if network_name:
                description_networks.add(
                    self._social_network_identity(network_name)
                )

        priority: int = max(
            (link.priority for link in self.external_urls),
            default=0,
        ) + 10
        raw_url: str
        for raw_url in re.findall(
            r'https?://[^\s<>"\']+', self.description
        ):
            url: str = raw_url.rstrip('.,;:!?)]}')
            network_name: str | None = self._social_network_name(url)
            if not network_name:
                continue
            network_identity: str = self._social_network_identity(
                network_name
            )
            if network_identity in description_networks:
                continue

            external_link: YouTubeExternalLink | None = (
                self._generate_external_link(
                    url, priority, title=network_name,
                )
            )
            if external_link:
                self.external_urls.add(external_link)
                description_networks.add(network_identity)
                priority += 10

        local_atom: str = r"[A-Za-z0-9!#$%&'*+=?^_`{|}~-]+"
        local_part: str = rf'{local_atom}(?:\.{local_atom})*'
        domain_label: str = (
            r'[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?'
        )
        domain: str = rf'(?:{domain_label}\.)+[A-Za-z]{{2,63}}'
        separator: str = (
            r'(?:\s*@\s*|\s+at\s+|-at-|\s*\(\s*at\s*\)\s*)'
        )
        email_pattern: str = (
            rf'(?<![A-Za-z0-9._%+/~-])({local_part})'
            rf'{separator}({domain})(?![A-Za-z0-9-])'
        )
        existing_emails: set[str] = {
            link.url.split(':', 1)[1].lower()
            for link in self.external_urls
            if link.url.lower().startswith('mailto:')
        }
        for email_match in re.finditer(
            email_pattern,
            self.description,
            flags=re.IGNORECASE,
        ):
            email_address: str = (
                f'{email_match.group(1)}@{email_match.group(2)}'
            )
            email_identity: str = email_address.lower()
            if email_identity in existing_emails:
                continue
            self.external_urls.add(
                YouTubeExternalLink(
                    name='email',
                    url=f'mailto:{email_address}',
                    priority=priority,
                )
            )
            existing_emails.add(email_identity)
            priority += 10

    @staticmethod
    def _social_network_identity(network_name: str) -> str:
        '''Return a stable identity for renamed social networks.'''

        if network_name in ('Twitter', 'X'):
            return 'X'
        return network_name

    @staticmethod
    def _social_network_name(url: str) -> str | None:
        '''Return the configured social-network name for an HTTP(S) URL.'''

        try:
            hostname: str | None = urlsplit(url).hostname
        except ValueError:
            return None
        if not hostname:
            return None

        hostname = hostname.lower().removeprefix('www.')
        if hostname in SocialNetworks:
            return SocialNetworks[hostname]

        labels: list[str] = hostname.split('.')
        if len(labels) < 2:
            return None
        domain_name: str = labels[-2]
        if (
            labels[-1] in ('au', 'ng', 'nz', 'tt', 'uk')
            and len(labels) >= 3
        ):
            domain_name = labels[-3]
        return SocialNetworks.get(domain_name)

    async def scrape(
        self, save_dir: str | None = None,
        max_videos_per_channel: int = 0,
        with_about_page: bool = False,
        proxies: list[str] | str | None = None,
        with_video_ids: bool = True,
        require_complete_video_ids: bool = False,
        known_video_ids: KnownVideoIdsFn | None = None,
        oldest_first_limit: int = 0,
        stop_if: 'StopIfFn | None' = None,
        listing_resume: dict[str, str] | None = None,
        on_listing_checkpoint: ListingCheckpointFn | None = None,
    ) -> None:
        '''
        Scrapes the channel page for information. This does not include data
        about the videos for the channel as multiple requests are needed
        to get that data. Use get_videos_page() and parse_channel_video_data()
        to get the videos from the channel.

        :param save_dir: the directory to save media assets to. If not
        provided, the instance's save_dir will be used.
        :param max_videos_per_channel: maximum number of videos to scrape per
        channel
        :param with_about_page: whether to scrape the about page
        :param proxies: list of proxy URLs to use for scraping
        :param with_video_ids: when False, skip
        scrape_channel_content() so only metadata (about
        page, subscriber count, thumbnails, banners) is
        fetched. Use when the platform already has the
        channel's video list and a metadata-only refresh
        is sufficient.
        :param require_complete_video_ids: propagate enumeration failures
        so callers do not acknowledge an incomplete full scrape.
        :param known_video_ids: when set, page the videos, shorts and
        live tabs in 'Latest' order and stop at the first page whose
        IDs are all known; see :attr:`video_ids_complete`.
        :param oldest_first_limit: when > 0, page the videos, shorts and
        live tabs in 'Oldest' order and keep at most this many IDs per
        tab (a new channel's first scrape).
        :param stop_if: when set, called once the about page (or the
        first InnerTube page) has been parsed; when it returns True the
        video enumeration is cancelled or skipped and
        :attr:`scrape_stopped_early` is set.
        :param listing_resume: when not None, a full listing (neither
        ``known_video_ids`` nor ``oldest_first_limit``) is resumable: it
        continues from this saved :attr:`listing_state` and a page that
        fails after its retries ends the listing for this scrape instead
        of failing it; see :attr:`video_listing_complete`.
        :param on_listing_checkpoint: called every few pages of a
        resumable full listing.
        :returns: dict of the scraped data
        :raises: ValueError if no data could be scraped or parsed
        '''

        if not save_dir:
            if not self.save_dir:
                raise ValueError('Save directory must be provided')
            save_dir = self.save_dir

        if not self.browse_client:
            self._create_browse_client(proxies=proxies)

        async def scrape_about_best_effort() -> None:
            if not with_about_page:
                return
            try:
                await self.scrape_about_page(proxies=proxies)
                self.about_page_succeeded = True
                self.fetched_fields |= ABOUT_FIELDS
            except Exception as exc:
                # /about is best-effort: InnerTube populates most
                # channel metadata, so HTML failures keep going.
                _LOGGER.warning(
                    'About page scrape failed; '
                    'continuing with InnerTube',
                    extra={
                        'channel_handle': self.channel_handle,
                        'exc': str(exc),
                    },
                )

        content_task: asyncio.Task[int] | None = None
        if (
            with_video_ids
            and self.channel_id
        ):
            content_task = asyncio.create_task(
                self.scrape_channel_content(
                    save_dir=save_dir,
                    max_videos_per_channel=(
                        max_videos_per_channel
                    ),
                    known_video_ids=known_video_ids,
                    oldest_first_limit=oldest_first_limit,
                    stop_if=stop_if,
                    listing_resume=listing_resume,
                    on_listing_checkpoint=on_listing_checkpoint,
                ),
            )

        await scrape_about_best_effort()

        # When /about did not yield ``channel_id`` (most commonly
        # because /about failed), resolve it from ``channel_handle``
        # via InnerTube so :meth:`scrape_channel_content` can run
        # on the InnerTube path. No-op when ``channel_id`` is
        # already known.
        if not self.channel_id and self.channel_handle:
            await self._resolve_channel_id_via_innertube()

        if stop_if is not None and stop_if(self):
            await self._stop_content_task(content_task)
            self._reconcile_description_external_links()
            return

        if not with_video_ids:
            return  # caller wanted metadata only
        try:
            if content_task is not None:
                await content_task
            else:
                await self.scrape_channel_content(
                    save_dir=save_dir,
                    max_videos_per_channel=(
                        max_videos_per_channel
                    ),
                    known_video_ids=known_video_ids,
                    oldest_first_limit=oldest_first_limit,
                    stop_if=stop_if,
                    listing_resume=listing_resume,
                    on_listing_checkpoint=on_listing_checkpoint,
                )
        except (ValueError, RuntimeError) as exc:
            if require_complete_video_ids:
                if not self._keep_partial_result(exc):
                    # A bad continuation is not evidence of a missing
                    # channel.
                    raise RuntimeError(
                        f'Incomplete channel video enumeration: {exc}',
                    ) from exc
        if (
            with_about_page and not self.about_page_succeeded
            and self.partial_error is None and self.fetched_fields
        ):
            # Fields only /about provides were not fetched.
            self.partial_error = 'About page scrape failed'
        self._reconcile_description_external_links()

    def _keep_partial_result(self, exc: Exception) -> bool:
        '''Turn a failed content fetch into a partial result when the
        scrape already collected useful data; True when it did.

        A channel alert or redirect wrapped in *exc* is never a partial
        result.
        '''
        if not self.fetched_fields or isinstance(
            exc.__cause__,
            (ChannelTerminatedError, ChannelRedirectedError),
        ):
            return False
        self.partial_error = f'Incomplete channel content: {exc}'
        self.content_partial = True
        self.video_ids_complete = False
        self.video_listing_complete = False
        _LOGGER.warning(
            'Channel content fetch failed; keeping the partial result',
            extra={
                'channel_handle': self.channel_handle,
                'channel_id': self.channel_id,
                'exc': str(exc),
            },
        )
        return True

    async def scrape_about_page(self, proxies: list[str] | str | None = None
                                ) -> None:
        '''
        Scrape the About tab for information. This does not include data
        about the videos for the channel as multiple requests are needed
        to get that data. Use get_videos_page() and parse_channel_video_data()
        to get the videos from the channel.

        :returns: dict of the scraped data
        :raises: ValueError if page returns a 404, RuntimeError
        if no data could be parsed
        '''

        if not self.url:
            raise ValueError('Channel URL is not set')

        about_url: str = self.url.rstrip('/') + '/about'

        if not self.browse_client:
            self._create_browse_client(proxies=proxies)

        proxy: str | None = self.browse_client.proxy
        extra: dict[str, any] = {
            'channel_handle': self.channel_handle,
            'url': about_url,
            'proxy_ip': _extract_proxy_ip(proxy) if proxy else 'none',
            'proxy_port': _extract_proxy_port(proxy) if proxy else 'none',
        }
        try:
            page_contents: str | None = await self.browse_client.get(about_url)
        except ValueError:
            _LOGGER.warning('About page not found for channel', extra=extra)
            raise
        except Exception as exc:
            _LOGGER.warning(
                'Error fetching about page for channel', exc=exc, extra=extra
            )
            raise RuntimeError(
                'Could not retrieve about page for channel '
                f'{self.channel_handle}'
            ) from exc

        if not page_contents:
            raise RuntimeError(
                'Could not retrieve about page for channel '
                f'{self.channel_handle}'
            )

        terminal_message: str | None = terminal_channel_page_message(
            page_contents
        )
        if terminal_message:
            _LOGGER.info(
                'About page contains terminal channel message',
                extra={**extra, 'terminal_message': terminal_message},
            )
            raise RuntimeError(terminal_message)

        page_data: dict[str, any] = self._extract_initial_data(page_contents)

        # This parses the channel metadata
        metadata: dict[str, any] = page_data.get(
            'metadata', {}
        ).get('channelMetadataRenderer', {})
        if metadata:
            self._parse_channel_about_metadata(metadata)

        self.channel_id = (
            self.channel_id or YouTubeChannel.extract_channel_id(page_contents)
        )

        about_data: dict[str, any] | None = self._find_about_renderer(
            page_data
        )
        if not about_data:
            _LOGGER.warning('Could not find about tab renderer', extra=extra)
            raise RuntimeError(
                'Could not find about tab renderer for channel '
                f'{self.channel_handle}'
            )

        self._parse_channel_about_data(about_data)

        # AboutTab sometimes omits viewCountText for channels whose
        # view count lives in pageHeaderRenderer.
        if self.view_count is None:
            self.view_count = YouTubeChannel.parse_view_count(page_data)

        self._parse_thumbnails_banners(metadata, page_data)

        self.channel_links = YouTubeChannel.extract_linked_channels(page_data)
        self._reconcile_description_external_links()

    def _find_about_renderer(self, initial_data: dict) -> dict | None:
        '''
        Gets the channel data from the aboutChannelViewModel

        :param initial_data: the ytInitialData dict extracted from the page
        :returns: the aboutChannelViewModel dict or None if it cannot be found
        :raises: (none)
        '''

        endpoints: list[dict[str, str]] = initial_data.get(
            'onResponseReceivedEndpoints', []
        )
        endpoint: dict[str, str]
        for endpoint in endpoints:
            section_list: list = endpoint.get(
                'showEngagementPanelEndpoint', {}
            ).get(
                'engagementPanel', {}
            ).get(
                'engagementPanelSectionListRenderer', {}
            ).get(
                'content', {}
            ).get(
                'sectionListRenderer', {}
            ).get(
                'contents', []
            )

            item: dict
            for item in section_list:
                item_section: list[dict[str, any]] = item.get(
                    'itemSectionRenderer', {}
                ).get('contents', [])
                for content_item in item_section or []:
                    about_view_model: dict[str, dict[str, str]] | None = \
                        content_item.get(
                            'aboutChannelRenderer', {}
                        ).get(
                            'metadata', {}
                        ).get(
                            'aboutChannelViewModel', {}
                        )

                    if about_view_model:
                        return about_view_model

        _LOGGER.warning(
            'Could not find about tab renderer',
            extra={'channel_handle': self.channel_handle, 'url': self.url}
        )
        return None

    def _extract_simple_text(self, text_obj: dict | str | None) -> str | None:
        '''
        Extracts simple text from a YouTube object

        :return: extracted text or None
        '''

        if isinstance(text_obj, str):
            return text_obj

        if text_obj is None:
            return None

        if text_obj.get('content'):
            return text_obj['content']

        if text_obj.get('simpleText'):
            return text_obj['simpleText']

        if text_obj.get('runs'):
            return ''.join(run.get('text', '') for run in text_obj['runs'])

        return None

    def _parse_channel_about_metadata(self, metadata: dict) -> None:
        '''Parse channel data from ytInitialData channelMetadataRenderer'''

        self.channel_id = metadata.get(
            'externalId', self.channel_id
        )
        self.title = self.title or metadata.get('title')

        self.description = metadata.get('description', self.description)
        self.rss_url = metadata.get('rssUrl', self.rss_url)

        self.available_country_codes = self.available_country_codes | \
            set(metadata.get('availableCountryCodes', []))

        vanity_url: str = metadata.get('vanityChannelUrl')
        if (not vanity_url and isinstance(metadata.get('ownerUrls'), list)
                and len(metadata['ownerUrls']) > 0):
            vanity_url = metadata['ownerUrls'][0]

        if vanity_url:
            try:
                self.channel_handle = vanity_url.split('/@', 1)[1]
            except IndexError:
                pass

        if vanity_url:
            self.external_urls.add(
                YouTubeExternalLink(
                    name='YouTube', url=vanity_url, priority=0,
                )
            )

        self.keywords = self.keywords | split_quoted_string(
            metadata.get('keywords')
        )

        self.is_family_safe = metadata.get('isFamilySafe', False)

    def _parse_channel_about_data(self, about_renderer: dict) -> None:
        '''
        Parse channelAboutFullMetadataRenderer data
        '''

        joined_text: str = self._extract_simple_text(
            about_renderer.get('joinedDateText')
        )
        if joined_text:
            joined_text = joined_text[len('Joined '):]
            # YouTubeClient sets locale to en-US, so we parse accordingly
            self.joined_date = self.joined_date or datetime.strptime(
                joined_text, '%b %d, %Y'
            ).replace(tzinfo=UTC)

        self.view_count = self.view_count or convert_number_string(
            self._extract_simple_text(about_renderer.get('viewCountText'))
        )

        self.video_count = self.video_count or convert_number_string(
            self._extract_simple_text(about_renderer.get('videoCountText'))
        )

        about_subs_text: str | None = self._extract_simple_text(
            about_renderer.get('subscriberCountText'),
        )
        about_subscriber_count: int | None = None
        about_outcome: str
        if not about_subs_text:
            about_outcome = 'absent'
        else:
            try:
                about_subscriber_count = convert_number_string(
                    about_subs_text,
                )
            except (ValueError, IndexError, KeyError):
                about_outcome = 'unparseable'
                _LOGGER.warning(
                    'Unparseable subscriber count text on about page',
                    extra={
                        'channel_handle': self.channel_handle,
                        'subscriber_count_text': about_subs_text,
                    },
                )
            else:
                about_outcome = 'found'
        record_subscriber_count_parse(
            'about_page', about_outcome, self.subscriber_count_retry,
        )
        if about_subscriber_count is not None and \
                self.subscriber_count is None:
            self.subscriber_count = about_subscriber_count

        self.external_urls = self.external_urls | \
            YouTubeChannel.parse_external_urls(
                about_renderer.get('links', [])
            )

        # Redundant with metadata parsing, still kept as fallback if
        # YouTube changes metadata structure
        self.description = self.description or self._extract_simple_text(
            about_renderer.get('description', {})
        )

        # Redundant with metadata parsing, still kept as fallback if
        # YouTube changes metadata structure
        country_text: str | None = self._extract_simple_text(
            about_renderer.get('country')
        )
        if country_text:
            self.country = country_converter.convert(
                country_text, to='ISO2', not_found=None
            )

    def parse_channel_video_data(self, page_data: dict) -> None:
        '''
        Parses the info from the channel 'videos' page

        :param page_data: the parsed 'videos' page data for the
            channel
        :returns: (none)
        '''

        self.channel_thumbnails = self.channel_thumbnails | \
            YouTubeChannel.parse_thumbnails(page_data)

        self._set_channel_video_thumbnail()

        self.banners: set[YouTubeThumbnail] = self.banners | \
            YouTubeChannel.parse_banners(page_data)

        parsed_subscriber_count: int | None
        subscriber_outcome: str
        parsed_subscriber_count, subscriber_outcome = \
            YouTubeChannel.parse_subscriber_count_with_outcome(page_data)
        record_subscriber_count_parse(
            'innertube', subscriber_outcome,
            self.subscriber_count_retry,
        )
        if parsed_subscriber_count is not None:
            self.subscriber_count = parsed_subscriber_count

        parsed_video_count: int | None = (
            YouTubeChannel.parse_video_count(page_data)
        )
        if parsed_video_count is not None or not self.video_count:
            self.video_count = parsed_video_count

        channel_info: dict[str, any] | None = page_data.get(
            'metadata', {}
        ).get(
            'channelMetadataRenderer'
        )
        if not channel_info:
            raise ValueError(
                f'No channel metadata found for channel: {self.channel_handle}'
            )

        # We already get the channel name from the channel metadata but we
        # keep it here in case YouTube changes their metadata structure
        self.channel_handle: str = \
            self.channel_handle or channel_info.get('name', '').lstrip('@')
        self.title = self.title or channel_info.get('title')

        # We already get description from about metadata but we
        # keep it here in case YouTube changes their metadata structure
        self.description = channel_info.get('description', self.description)

        # We already get keywords from about metadata but we
        # keep it here in case YouTube changes their metadata structure
        keywords_data: str = channel_info.get('keywords')
        self.keywords = self.keywords | split_quoted_string(keywords_data)

        # We get the this data already from the about metadata but we
        # keep it here in case YouTube changes their metadata structure
        self.is_family_safe = channel_info.get(
            'isFamilySafe', self.is_family_safe
        )

    def _set_channel_video_thumbnail(self) -> None:
        # The channel thumbnail used for videos is the smallestavailable
        if self.channel_thumbnails:
            self.channel_thumbnail = min(self.channel_thumbnails)

    def _parse_thumbnails_banners(self, metadata: dict[str, any],
                                  page_data: dict[str, any]) -> None:

        self.external_urls = self.external_urls | \
            YouTubeChannel._extract_links(page_data)

        # Thumbnails
        header: dict[str, dict[str, any]] = page_data.get('header', {})
        # Try different header types
        header_renderer: dict[str, any] = (
            header.get('c4TabbedHeaderRenderer') or
            header.get('pageHeaderRenderer') or
            {}
        )

        if metadata.get('avatar', {}).get('thumbnails'):
            self.channel_thumbnails = self.channel_thumbnails | \
                YouTubeChannel.parse_thumbnails(
                    metadata['avatar']['thumbnails']
                )
        elif ('avatar' in header_renderer and
                'thumbnails' in header_renderer['avatar']):
            self.channel_thumbnails = self.channel_thumbnails | \
                YouTubeChannel.parse_thumbnails(
                    header_renderer['avatar']['thumbnails']
                )

        self._set_channel_video_thumbnail()

        # Banner
        banners: list[dict[str, str]] = page_data.get(
            'header', {}
        ).get(
            'pageHeaderRenderer', {}
        ).get(
            'content', {}
        ).get(
            'pageHeaderViewModel', {}
        ).get(
            'banner', {}
        ).get(
            'imageBannerViewModel', {}
        ).get(
            'image', {}
        ).get(
            'sources', []
        )
        if not banners:
            banners = header_renderer.get('banner', {}).get('thumbnails', [])

        for banner in banners:
            self.banners.add(
                YouTubeThumbnail(data=banner)
            )

    @staticmethod
    def extract_linked_channels(page_data: dict[str, any]
                                ) -> set[YouTubeChannelLink]:
        '''
        Extracts the linked channels from the channel home page data.
        These are the channels that are linked in the "Featured Channels"
        tab of the channel page.
        :param page_data: the parsed data from the channel home page
        :returns: a set of tuples containing the linked channel URL and'''

        def parse_list_items(list_items: list) -> set[YouTubeChannelLink]:
            page_links: set[YouTubeChannelLink] = set()
            list_item: dict[str, any]
            for list_item in list_items or []:
                channel_renderer: dict[str, any] = list_item.get(
                    'gridChannelRenderer', {}
                )
                if not channel_renderer:
                    continue

                channel_path: str | None = channel_renderer.get(
                    'navigationEndpoint', {}
                ).get(
                    'commandMetadata', {}
                ).get(
                    'webCommandMetadata', {}
                ).get('url')

                if not channel_path:
                    continue

                channel_handle: str = channel_path.lstrip('/@')
                subs_text: str | None = channel_renderer.get(
                    'subscriberCountText', {}
                ).get(
                    'simpleText'
                )
                if not subs_text:
                    continue

                subs: int = convert_number_string(subs_text)
                page_links.add(
                    YouTubeChannelLink(
                        channel_handle=channel_handle, subscriber_count=subs
                    )
                )
            return page_links

        def parse_section_item_contents(section_item_contents: list
                                        ) -> set[YouTubeChannelLink]:
            page_links: set[YouTubeChannelLink] = set()
            section_item_content: dict[str, any]
            for section_item_content in section_item_contents or []:
                list_items: list[dict[str, any]] = \
                    section_item_content.get(
                        'shelfRenderer', {}
                    ).get(
                        'content', {}
                    ).get(
                        'horizontalListRenderer', {}
                    ).get(
                        'items', []
                    )
                if list_items:
                    page_links |= parse_list_items(list_items)

            return page_links

        def parse_section_items(section_items: list
                                ) -> set[YouTubeChannelLink]:
            page_links: set[YouTubeChannelLink] = set()
            section_item: dict[str, any]
            for section_item in section_items or []:
                section_item_contents: list[dict[str, any]] = section_item.get(
                    'itemSectionRenderer', {}
                ).get(
                    'contents', []
                )
                if section_item_contents:
                    page_links |= parse_section_item_contents(
                        section_item_contents
                    )

            return page_links

        def parse_tabs(tabs: list[dict[str, any]]) -> set[YouTubeChannelLink]:
            page_links: set[YouTubeChannelLink] = set()
            tab: dict[str, any]
            for tab in tabs or []:
                section_items: list[dict[str, any]] = tab.get(
                    'tabRenderer', {}
                ).get(
                    'content', {}
                ).get(
                    'sectionListRenderer', {}
                ).get(
                    'contents', []
                )
                if section_items:
                    page_links = page_links | parse_section_items(
                        section_items
                    )
            return page_links

        # Nested functions add to the page-links set in the parent function
        # scope

        tabs: list[dict[str, any]] = page_data.get(
            'contents', {}
        ).get(
            'twoColumnBrowseResultsRenderer', {}
        ).get(
            'tabs', []
        )
        page_links: set[YouTubeChannelLink] = parse_tabs(tabs)

        return page_links

    @staticmethod
    def is_channel_id(name: str | None) -> bool:
        if not name:
            return False

        return bool(YouTubeChannel.CHANNEL_ID_REGEX_MATCH.match(name))

    @staticmethod
    def normalise_channel_id(name: str | None) -> str | None:
        '''Recover a canonical channel_id from a lowercase ``uc``
        prefix.

        YouTube channel_ids are 24 chars: a literal ``UC`` prefix
        followed by 22 case-sensitive base64url chars. Some
        upstream code paths (third-party imports, hand-typed URLs)
        produce lowercase ``uc...`` forms. The body case is
        irrecoverable once lost, but the prefix is fixed, so we
        canonicalise by uppercasing the first two chars when the
        rest of the string is a valid base64url body.

        :returns: the canonical form (``UC`` + 22 base64url chars)
            if *name* is already canonical or has a lowercase /
            mixed-case ``uc`` prefix with a valid body; ``None``
            otherwise.
        '''
        if not name or len(name) != 24:
            return None
        candidate: str = 'UC' + name[2:] if name[:2].upper() == 'UC' \
            else name
        if YouTubeChannel.CHANNEL_ID_REGEX_MATCH.match(candidate):
            return candidate
        return None

    async def _resolve_channel_id_via_innertube(self) -> bool:
        '''
        Resolve ``self.channel_id`` from ``self.channel_handle`` via
        InnerTube. Used as a fallback when /about HTML did not
        supply a ``channel_id`` (e.g. /about timed out or the page
        was blocked by the WAF) so that
        :meth:`scrape_channel_content` can still run via the
        InnerTube ``browse`` endpoint.

        Calls ``navigation/resolve_url`` via
        ``adaptor.dispatch(RESOLV_URL, body={'url': '...'})``,
        mirroring what the YouTube web client does on every
        ``/@handle`` navigation. InnerTube ``browse('@<handle>')``
        is not used: YouTube rejects handles as ``browseId`` with
        HTTP 400 INVALID_ARGUMENT.

        Reuses the per-proxy pooled InnerTube client tied to
        ``self.browse_client.proxy`` so the call is rate-limited
        and proxied identically to the rest of the channel scrape.

        :returns: ``True`` if ``self.channel_id`` was set,
            ``False`` otherwise.
        '''
        if self.channel_id:
            return True
        if not self.channel_handle:
            return False

        # If the handle is itself a channel_id (some callers pass
        # ``UC...`` as a handle), use it directly with no API call.
        # Lowercase ``uc`` prefixes are canonicalised — the lookup
        # would otherwise propagate a non-canonical id into
        # ``ChannelIdentityStore.bind`` and the scrape queue.
        normalised_id: str | None = YouTubeChannel.normalise_channel_id(
            self.channel_handle,
        )
        if normalised_id is not None:
            self.channel_id = normalised_id
            return True

        handle: str = self.channel_handle
        if not handle.startswith('@'):
            handle = f'@{handle}'

        proxy: str | None = getattr(self.browse_client, 'proxy', None)
        proxy_ip: str = (
            _extract_proxy_ip(proxy) if proxy else 'none'
        )
        proxy_port: str = (
            _extract_proxy_port(proxy) if proxy else 'none'
        )
        proxy_file: str = proxy_file_label(proxy or '')
        scraper: str = _get_scraper()
        worker_id: str = get_worker_id()
        metric_labels: dict[str, str] = {
            'platform': 'youtube',
            'scraper': scraper,
            'entity': 'channel',
            'worker_id': worker_id,
            'proxy_ip': proxy_ip,
            'proxy_port': proxy_port,
            'proxy_file': proxy_file,
        }
        extra: dict[str, str] = {
            'channel_handle': self.channel_handle,
            'proxy_ip': proxy_ip,
            'proxy_port': proxy_port,
        }

        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()

        # InnerTube browse rejects handles as browseId (HTTP 400
        # INVALID_ARGUMENT), so resolve via navigation/resolve_url.
        outcome: str
        duration: float
        start: float = time.monotonic()
        try:
            await limiter.acquire(
                YouTubeCallType.BROWSE, proxy=proxy,
            )
            result: dict = await _resolver_innertube_call(
                proxy,
                lambda client: client.adaptor.dispatch(
                    RESOLV_URL,
                    body={
                        'url': f'https://www.youtube.com/{handle}',
                    },
                ),
            )
            browse_id: str | None = (
                result.get('endpoint', {})
                      .get('browseEndpoint', {})
                      .get('browseId')
            )
            if browse_id and YouTubeChannel.is_channel_id(browse_id):
                duration = time.monotonic() - start
                self.channel_id = browse_id
                outcome = 'hit'
                _LOGGER.debug(
                    'Resolved channel_id via navigation/resolve_url',
                    extra=extra | {
                        'channel_id': browse_id,
                        'duration': duration,
                        'api': RESOLV_URL,
                        'status_class': '2xx',
                    },
                )
                METRIC_CHANNEL_HANDLE_RESOLVER_OUTCOMES.labels(
                    strategy='resolve_url', outcome=outcome,
                    **metric_labels,
                ).inc()
                METRIC_YT_REQUEST_DURATION.labels(
                    platform='youtube', scraper=scraper,
                    api='innertube_resolver',
                    status_class='2xx', worker_id=worker_id,
                    proxy_file=proxy_file,
                ).observe(duration)
                return True
            outcome = 'miss'
            duration = time.monotonic() - start
            METRIC_YT_REQUEST_DURATION.labels(
                platform='youtube', scraper=scraper,
                api='innertube_resolver',
                status_class='2xx', worker_id=worker_id,
                proxy_file=proxy_file,
            ).observe(duration)
            _LOGGER.warning(
                'InnerTube channel_id resolver returned '
                'no usable result',
                extra=extra | {
                    'api': RESOLV_URL,
                    'duration': duration,
                    'status_class': '2xx',
                    'outcome': outcome,
                },
            )
        except Exception as exc:
            outcome = 'error'
            duration = time.monotonic() - start
            METRIC_YT_REQUEST_DURATION.labels(
                platform='youtube', scraper=scraper,
                api='innertube_resolver',
                status_class='error', worker_id=worker_id,
                proxy_file=proxy_file,
            ).observe(duration)
            _LOGGER.warning(
                'InnerTube channel_id resolver failed',
                exc=exc,
                extra=extra | {
                    'api': RESOLV_URL,
                    'duration': duration,
                    'status_class': 'error',
                },
            )
        METRIC_CHANNEL_HANDLE_RESOLVER_OUTCOMES.labels(
            strategy='resolve_url', outcome=outcome,
            **metric_labels,
        ).inc()
        return False

    @staticmethod
    async def resolve_channel_id(
        channel_id: str,
        proxy: str | None = None,
    ) -> str | None:
        '''
        Resolves a YouTube channel ID to its handle/name using the InnerTube
        API.

        :param channel_id: YouTube channel ID (e.g. UCxxxxxxxxxxxxxxxxxxxxxx)
        :param proxy: Optional proxy URL for the InnerTube request.
        :returns: channel handle without leading '@', or None if not resolved
        '''

        try:
            tabs = YouTubeChannelTabs(channel_id, proxy)
            channel_data: dict = await tabs.browse_channel()
        except Exception as exc:
            _LOGGER.debug(
                'Failed to browse channel for ID',
                exc=exc,
                extra={
                    'channel_id': channel_id,
                    'proxy_ip': (
                        _extract_proxy_ip(proxy) if proxy else 'none'
                    ),
                    'proxy_port': (
                        _extract_proxy_port(proxy) if proxy else 'none'
                    ),
                },
            )
            return None

        # Extract canonicalBaseUrl from the channel metadata
        metadata: dict = channel_data.get(
            'metadata', {}
        ).get('channelMetadataRenderer', {})
        vanity_url: str | None = metadata.get('vanityChannelUrl')
        if vanity_url and '/@' in vanity_url:
            handle: str = vanity_url.split('/@')[-1].split('/')[0]
            _LOGGER.debug(
                'Resolved channel ID to handle',
                extra={'channel_id': channel_id, 'handle': handle}
            )
            return handle

        _LOGGER.debug(
            'Could not extract channel handle from InnerTube data for ID',
            extra={'channel_id': channel_id}
        )
        return None

    async def scrape_channel_content(
        self, save_dir: str, max_videos_per_channel: int = 0,
        known_video_ids: KnownVideoIdsFn | None = None,
        oldest_first_limit: int = 0,
        stop_if: 'StopIfFn | None' = None,
        listing_resume: dict[str, str] | None = None,
        on_listing_checkpoint: ListingCheckpointFn | None = None,
    ) -> int:
        '''
        Scrapes video_id's and videos from the YouTube InnerTube API.

        :param save_dir: the directory to save the scraped video metadata to
        as a brotli-compressed JSON file. Already-uploaded videos are read
        from ``{save_dir}/uploaded`` (created automatically by
        :class:`AssetFileManagement`).
        :param max_videos_per_channel: the maximum number of videos to ingest.
        If set to 0, no videos will be scraped, only the video IDs will be
        scraped.
        :param known_video_ids: see :meth:`scrape`.
        :param oldest_first_limit: see :meth:`scrape`.
        :param stop_if: see :meth:`scrape`; checked after the first
        InnerTube page is parsed, before the tabs are enumerated.
        :param listing_resume: see :meth:`scrape`.
        :param on_listing_checkpoint: see :meth:`scrape`.
        :returns: number of videos scraped
        :raises: RuntimeError, ValueError
        '''

        if not save_dir or not os.path.isdir(save_dir):
            raise ValueError(f'Invalid save directory: {save_dir}')

        if not self.channel_id:
            raise ValueError(
                'channel_id must be set before scraping content — '
                'call scrape_about_page() first'
            )

        proxy: str | None = getattr(self.browse_client, 'proxy', None)
        tab_checkpoint: TabListingCheckpointFn | None = None
        if on_listing_checkpoint is not None:
            async def tab_checkpoint(
                tab: str, token: str, video_ids: set[str],
            ) -> None:
                await on_listing_checkpoint(self, tab, token, video_ids)
        tabs = YouTubeChannelTabs(
            self.channel_id, proxy, known_video_ids=known_video_ids,
            oldest_first_limit=oldest_first_limit,
            listing_resume=listing_resume,
            on_listing_checkpoint=tab_checkpoint,
        )
        page_data: dict[str, any] = await tabs.browse_channel()

        # Prefer the canonical handle from InnerTube when available;
        # otherwise keep whatever was set during construction (URL slug
        # or fallback handle derived from the title).
        canonical: str | None = canonical_handle_from_browse(page_data)
        if canonical:
            self.channel_handle = canonical

        self.parse_channel_video_data(page_data)
        self.fetched_fields |= INNERTUBE_FIELDS

        if stop_if is not None and stop_if(self):
            self.scrape_stopped_early = True
            return 0

        try:
            self.video_ids, self.podcast_ids, self.playlists, self.courses, \
                self.posts, self.merch = \
                await tabs.scrape_loaded_tabs()
        except Exception as exc:
            raise RuntimeError(
                f'Failed to scrape channel content: {exc}'
            ) from exc
        self._record_tab_fields(tabs.failed_tabs)
        self.video_ids_complete = tabs.enumeration_complete
        self.listing_state = dict(tabs.listing_state)
        self.video_listing_complete = (
            tabs.enumeration_complete or tabs.listing_complete
        )
        if self.video_ids_complete:
            # A partial enumeration would undercount the videos.
            self._set_video_count_from_loaded_video_ids()

        videos_imported: int = 0
        if max_videos_per_channel:
            videos_imported = await self.scrape_videos(
                save_dir, max_videos_per_channel, self.video_ids
            )

        _LOGGER.debug(
            'Scraped videos from YouTube channel',
            extra={
                'videos_imported': videos_imported,
                'channel_handle': self.channel_handle,
            }
        )

        return videos_imported

    def _record_tab_fields(self, failed_tabs: set[str]) -> None:
        '''Mark the record keys of the content tabs fetched, except
        those of *failed_tabs*; a failed tab makes the scrape partial.

        ``video_ids`` stays fetched when a video tab failed: it holds
        the IDs collected before the failure, which callers union with
        the channel's earlier IDs.
        '''
        failed_fields: set[str] = {
            TAB_FIELDS[title] for title in failed_tabs
            if title in TAB_FIELDS
        } - {'video_ids'}
        self.fetched_fields |= TAB_RECORD_FIELDS - failed_fields
        if failed_tabs:
            self.content_partial = True
            self.partial_error = (
                f'Failed channel tabs: {", ".join(sorted(failed_tabs))}'
            )

    async def _stop_content_task(
        self, content_task: 'asyncio.Task[int] | None',
    ) -> None:
        '''Cancel a running video enumeration and mark the scrape as
        stopped early.'''
        self.scrape_stopped_early = True
        if content_task is None:
            return
        content_task.cancel()
        try:
            await content_task
        except asyncio.CancelledError:
            current: asyncio.Task | None = asyncio.current_task()
            if current is not None and current.cancelling():
                # This task was cancelled too: do not swallow that.
                raise
        except Exception as exc:
            # The enumeration failed before it was cancelled; its
            # result is not needed.
            _LOGGER.debug(
                'Discarded video enumeration failed',
                extra={
                    'channel_handle': self.channel_handle,
                    'exc': str(exc),
                },
            )

    def _set_video_count_from_loaded_video_ids(self) -> None:
        if self.video_count not in (None, 0):
            return
        parsed_video_count: int = len(self.video_ids)
        if parsed_video_count > 0:
            self.video_count = parsed_video_count

    async def scrape_videos(
        self, save_dir: str, max_videos_per_channel: int,
        video_ids: set[str]
    ) -> int:
        '''
        Scrapes video data for a set of video IDs

        :param video_ids: the video IDs to scrape
        :returns: a set of YouTubeVideo objects
        '''

        if not max_videos_per_channel:
            return 0

        already_ingested_videos: dict[str, tuple[IngestStatus, datetime]] = \
            get_imported_assets(save_dir)

        extra: dict[str, any] = {
            'channel_handle': self.channel_handle, 'video_ids': video_ids
        }
        videos_imported: int = 0
        for video_id in video_ids:
            if video_id in already_ingested_videos:
                _LOGGER.debug(
                    'Skipping already ingested video for channel', extra=extra
                )
                continue

            try:
                video: YouTubeVideo | None = await self.scrape_video(
                    video_id, self.channel_thumbnail
                )
                if not video:
                    continue

                file_path: str = os.path.join(
                    save_dir, f'{VIDEO_FILE_PREFIX}{video_id}.json.br'
                )
                await brotli_write_async(file_path, video.to_dict())

                videos_imported += 1
                already_ingested_videos[video_id] = (
                    IngestStatus.SCRAPED, video.published_timestamp
                )

                _LOGGER.debug('Scraped video for channel', extra=extra)
                self.videos.add(video)

                if videos_imported >= max_videos_per_channel:
                    break
            except RuntimeError:
                continue
            except Exception as exc:
                raise RuntimeError(f'Failed to scrape video: {exc}') from exc

        return videos_imported

    @staticmethod
    def extract_channel_id(page_data: str) -> str | None:
        '''
        Extracts the YouTube channel ID (ie. 'UC<22 characters>') from the
        channel page data

        :param page_data: channel description
        :returns: the YouTube channel ID
        '''

        if not page_data:
            _LOGGER.warning('No page data to extract channel ID from')
            return

        match: re.Match[str] | None = \
            YouTubeChannel.CHANNEL_ID_REGEX_SCRAPE.search(page_data)

        if match is None:
            raise ValueError('Channel ID not found')

        channel_id: str = match.group(1)

        return channel_id

    @staticmethod
    def extract_verified_status(page_data: str) -> bool:
        '''
        Extracts whether the channel is verified from the channel page data

        :param page_data: channel description
        :returns: whether the channel is verified
        '''
        if not page_data:
            _LOGGER.warning('No page data to extract verified status from')

        match: re.Match[str] | None = re.search(
            r'"tooltip"\s*:\s*"Verified"', page_data
        )
        return match is not None

    @staticmethod
    def find_nested_dicts(target: str, data: any, path: str = '<root>') -> any:
        '''
        Helper function to locate a wanted key in the nested dictionaries
        in the scraped data

        :param target: the key to search for
        :param data: the data to search in
        :param path: the path through the dict in the current data
        :returns: the value of the key
        '''

        if isinstance(data, dict):
            _LOGGER.debug(
                'Searching dict for target',
                extra={
                    'target': target,
                    'path': path,
                    'keys': ','.join(data.keys()),
                }
            )
            if target in data:
                return data[target]

            for key, value in data.items():
                result: any = YouTubeChannel.find_nested_dicts(
                    target, value, f'{path}:{key}'
                )
                if result:
                    return result

        if isinstance(data, list):
            _LOGGER.debug(
                'In list with items',
                extra={'data_length': len(data)}
            )
            for item in data:
                result: any = YouTubeChannel.find_nested_dicts(
                    target, item, f'{path}[]'
                )
                if result:
                    return result

        return None

    @staticmethod
    def _parse_video_count_from_metadata_rows(
        data: dict,
    ) -> int | None:
        '''Extract video count from the new
        ``pageHeaderRenderer`` metadata rows.'''

        metadata_rows: list[dict[str, any]] = data.get(
            'header', {}
        ).get(
            'pageHeaderRenderer', {}
        ).get(
            'content', {}
        ).get(
            'pageHeaderViewModel', {}
        ).get(
            'metadata', {}
        ).get(
            'contentMetadataViewModel', {}
        ).get(
            'metadataRows', []
        )

        for row in metadata_rows:
            parts: list[dict] = (
                row.get('metadataParts', []) or []
            )
            for part in parts:
                text: dict | None = part.get('text')
                if not isinstance(text, dict):
                    continue
                content: str = text.get('content', '')
                # The channel handle shares the row, and a handle such
                # as '@nomorevideos3' contains 'video' too.
                if (
                    not content
                    or content.lstrip().startswith('@')
                    or 'video' not in content
                ):
                    continue
                try:
                    count: int | None = (
                        convert_number_string(content)
                    )
                except (ValueError, KeyError):
                    continue
                if count is not None:
                    return count
        return None

    @staticmethod
    def parse_video_count(data: dict) -> int | None:
        '''
        Parse the video count from the scraped data.

        Tries the new ``pageHeaderRenderer`` path first,
        then falls back to the legacy
        ``c4TabbedHeaderRenderer``.

        :param data: the scraped data
        :returns: the video count, or None
        '''

        count: int | None = (
            YouTubeChannel
            ._parse_video_count_from_metadata_rows(data)
        )
        if count is not None:
            return count

        videos_text: str = data.get(
            'header', {}
        ).get(
            'c4TabbedHeaderRenderer', {}
        ).get(
            'videosCountText', {}
        ).get(
            'runs', [{}]
        )[0].get('text', '')
        if videos_text:
            count = convert_number_string(videos_text)
            if count is not None:
                return count

        _LOGGER.debug('Failed to parse videos count')
        return None

    @staticmethod
    def parse_subscriber_count(data: dict) -> int | None:
        '''
        Parse the subscriber count from the scraped data

        :param data: the scraped data
        :returns: the subscriber count
        '''

        return YouTubeChannel.parse_subscriber_count_with_outcome(data)[0]

    @staticmethod
    def parse_subscriber_count_with_outcome(
        data: dict,
    ) -> tuple[int | None, str]:
        '''
        Parse the subscriber count from the scraped data, reporting
        why parsing did or did not yield a value.

        :param data: the scraped data
        :returns: a tuple of the subscriber count (or ``None``) and
            the outcome ``'found'``, ``'absent'`` or ``'unparseable'``
        '''

        metadata_rows: list = data.get(
            'header', {}
        ).get(
            'pageHeaderRenderer', {}
        ).get(
            'content', {}
        ).get(
            'pageHeaderViewModel', {}
        ).get(
            'metadata', {}
        ).get(
            'contentMetadataViewModel', {}
        ).get(
            'metadataRows', []
        )

        for metadata_row in metadata_rows:
            metadata_parts: list[dict] = metadata_row.get(
                'metadataParts', []
            ) or []
            for metadata_part in metadata_parts:
                if isinstance(metadata_part.get('text'), dict):
                    content: str = metadata_part['text'].get('content', '')
                    if content and content.rstrip().endswith((
                        ' subscriber', ' subscribers',
                    )):
                        try:
                            youtube_subs_count: int | None = \
                                convert_number_string(content)
                        except (ValueError, IndexError, KeyError):
                            _LOGGER.warning(
                                'Unparseable subscriber count text '
                                'in InnerTube header',
                                extra={'subscriber_count_text': content},
                            )
                            return (None, 'unparseable')
                        return (youtube_subs_count, 'found')

        _LOGGER.debug('Failed to parse subscriber count')
        return (None, 'absent')

    @staticmethod
    def _parse_view_count_from_metadata_rows(
        data: dict,
    ) -> int | None:
        '''Extract view count from the new
        ``pageHeaderRenderer`` metadata rows.'''

        metadata_rows: list = data.get(
            'header', {}
        ).get(
            'pageHeaderRenderer', {}
        ).get(
            'content', {}
        ).get(
            'pageHeaderViewModel', {}
        ).get(
            'metadata', {}
        ).get(
            'contentMetadataViewModel', {}
        ).get(
            'metadataRows', []
        )

        for row in metadata_rows:
            parts: list[dict] = (
                row.get('metadataParts', []) or []
            )
            for part in parts:
                text: dict | None = part.get('text')
                if not isinstance(text, dict):
                    continue
                content: str = text.get('content', '')
                # Skip the channel handle: '@viewtube' contains 'view'.
                if (
                    not content
                    or content.lstrip().startswith('@')
                    or 'view' not in content
                ):
                    continue
                try:
                    count: int | None = (
                        convert_number_string(content)
                    )
                except (ValueError, KeyError):
                    continue
                if count is not None:
                    return count
        return None

    @staticmethod
    def parse_view_count(data: dict) -> int | None:
        '''
        Parse the total views count for the channel
        from the scraped data.

        Tries the new ``pageHeaderRenderer`` path first,
        then falls back to the legacy
        ``c4TabbedHeaderRenderer``.

        :param data: the scraped data
        :returns: the view count, or None
        '''

        try:
            count: int | None = (
                YouTubeChannel
                ._parse_view_count_from_metadata_rows(
                    data,
                )
            )
            if count is not None:
                return count

            views_data: str = data.get(
                'header', {}
            ).get(
                'c4TabbedHeaderRenderer', {}
            ).get(
                'viewCountText', {}
            ).get(
                'simpleText', ''
            )
            if views_data:
                return convert_number_string(views_data)

            return None
        except Exception as exc:
            _LOGGER.debug(
                'Failed to parse views count', exc=exc,
            )
            return None

    @staticmethod
    def parse_thumbnails(data: dict[str, any]) -> set[YouTubeThumbnail]:
        '''
        Parses the thumbnails out of the YouTube channel page either
        scraped or retrieved using the InnerTube API
        '''

        thumbnails_data: list[dict[str, str | int]] = []
        if isinstance(data, dict):
            thumbnails_data: list = data.get(
                'header', {}
            ).get(
                'pageHeaderRenderer', {}
            ).get(
                'content', {}
            ).get(
                'pageHeaderViewModel', {}
            ).get(
                'image', {}
            ).get(
                'decoratedAvatarViewModel', {}
            ).get(
                'avatar', {}
            ).get(
                'avatarViewModel', {}
            ).get(
                'image', {}
            ).get(
                'sources', []
            )

        if not thumbnails_data:
            # If we can't parse the data from the channel scrape,
            # we try to get the data from the InnerTube API
            _LOGGER.debug(
                'Falling back to Innertube for channel thumbnails',
            )
            if isinstance(data, list):
                thumbnails_data = data
            else:
                thumbnails_data = \
                    data.get('thumbnail', {}).get('thumbnails', [])
                _LOGGER.debug('Data is a low-depth list')

        channel_thumbnails: set[YouTubeThumbnail] = set()
        for thumbnail_data in thumbnails_data:
            url: str | None = thumbnail_data.get('url')
            if (url and (
                    not url.startswith(HTTPS_PREFIX) or url.startswith('//'))):
                thumbnail_data['url'] = f'https:{url}'
            thumbnail = YouTubeThumbnail(thumbnail_data)
            channel_thumbnails.add(thumbnail)

        return channel_thumbnails

    @staticmethod
    def _generate_external_link(url: str, priority: int,
                                title: str | None = None
                                ) -> YouTubeExternalLink | None:
        # Strip of the protocol from the url
        if url.startswith(HTTP_PREFIX):
            url = url[len(HTTP_PREFIX):]
        elif url.startswith(HTTPS_PREFIX):
            url = url[len(HTTPS_PREFIX):]

        if not title:
            # Figure out with social network the url is pointing to
            name: str = url.split('/')[0]
            domain_parts: list[str] = name.split('.')
            # Strip of 'www'
            if domain_parts and domain_parts[0] == 'www':
                domain_parts = domain_parts[1:]

            if len(domain_parts) == 2:
                name = domain_parts[0]
            elif domain_parts[-1] in ('tt', 'uk', 'au', 'nz', 'ng'):
                name = domain_parts[-3]
            else:
                if url.startswith('and '):
                    # This is text ' and <n> more link<s>'
                    _LOGGER.debug(
                        'TODO: call YT API to get the additional links',
                        extra={'url': url}
                    )
                    return None
                else:
                    _LOGGER.debug(
                        'Could not parse link name our of URL',
                        extra={'url': url}
                    )
                    name = url

            # Fall back to the inferred domain label so unknown
            # sites are self-describing instead of 'www'.
            title = SocialNetworks.get(name.lower(), name)
            _LOGGER.debug(
                'Parsed external link label ouf of URL',
                extra={'name': name, 'url': url}
            )

        return YouTubeExternalLink(
            name=title, url=f'https://{url}', priority=priority,
        )

    @staticmethod
    def parse_banners(data: dict[str, any]) -> set[YouTubeThumbnail]:
        '''
        Parses the banner images out of the YouTube channel page

        :param data: the YouTube channel page as a dict
        :returns: list of banners as YouTubeThumbnails
        '''

        banners: set[YouTubeThumbnail] = set()

        banner_type: str
        for banner_type in ['banner', 'tvBanner', 'mobileBanner']:
            banner_data: dict[str, str | int] | None = data.get(
                'header', {}
            ).get(
                'pageHeaderRenderer', {}
            ).get(
                'content', {}
            ).get(
                'pageHeaderViewModel', {}
            ).get(
                banner_type, {}
            ).get(
                'imageBannerViewModel', {}
            ).get(
                'image', {}
            ).get(
                'sources', []
            )

            thumbnail_data: dict[str, str | int]
            for thumbnail_data in banner_data:
                channel_banner: YouTubeThumbnail = YouTubeThumbnail(
                    thumbnail_data, display_hint=banner_type
                )
                _LOGGER.debug(
                    'Found banner',
                    extra={'url': channel_banner.url}
                )
                banners.add(channel_banner)

        return banners

    async def scrape_video(
        self, video_id: str, channel_thumbnail: YouTubeThumbnail | None
    ) -> YouTubeVideo | None:
        '''
        Find the videos in the by walking through the deserialized
        output of a scrape of a YouTube channel

        :param video_id: YouTube video ID
        :param video_table: Table to see if video has already been ingested
        where to store newly ingested videos
        :returns: the scraped YouTubeVideo
        :raises: RuntimeError: if scraping the video fails
        '''

        _LOGGER.debug(
            'Scraping video',
            extra={'video_id': video_id}
        )

        video: YouTubeVideo = await YouTubeVideo.scrape(
            video_id, self.channel_handle, channel_thumbnail,
            download_client=self.download_client,
        )

        return video
