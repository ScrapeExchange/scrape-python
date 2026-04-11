'''
Model a Youtube video

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import os
import re
import random
import tempfile

from enum import Enum
from typing import Self
from random import randrange
from logging import Logger
from logging import getLogger
from datetime import datetime
from datetime import timezone
import aiofiles
from dateutil import parser as dateutil_parser

import asyncio
from asyncio import sleep

import orjson
import brotli
import untangle
from innertube import InnerTube

from prometheus_client import Gauge

from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadError

from ..util import convert_number_string

from .youtube_format import YouTubeFormat
from .youtube_caption import YouTubeCaption
from .youtube_client import AsyncYouTubeClient
from .youtube_thumbnail import YouTubeThumbnail
from .youtube_videochapter import YouTubeVideoChapter
from .youtube_rate_limiter import YouTubeRateLimiter, YouTubeCallType
from .youtube_video_innertube import InnerTubeVideoParser

_LOGGER: Logger = getLogger(__name__)

DENO_PATH: str = os.environ.get('HOME', '') + '/.deno/bin/deno'
PO_TOKEN_URL: str = 'http://localhost:4416'
YTDLP_CACHE_DIR: str = '/var/tmp/yt_dlp_cache'

# Number of yt-dlp ``extract_info`` calls currently running in the
# default ThreadPoolExecutor, labelled by proxy.  Used to detect
# thread-pool starvation: if this gauge stays pinned at the executor
# size while ``yt_video_proxy_videos_scraped_total`` rate is low, the
# scraper is bottlenecked by the executor (or the GIL) rather than by
# the rate limiter or YouTube.
METRIC_EXTRACT_INFO_ACTIVE: Gauge = Gauge(
    'youtube_extract_info_active',
    'Number of yt-dlp extract_info calls currently running in the '
    'thread pool, labelled by proxy.',
    ['proxy'],
)


class YouTubeMediaType(str, Enum):
    VIDEO = 'video'
    SHORT = 'short'
    PLAYLIST = 'playlist'
    CHANNEL = 'channel'
    LIVE = 'live'
    MOVIE = 'movie'


class YouTubeVideo:
    VIDEO_URL: str = 'https://www.youtube.com/watch?v={video_id}'
    EMBED_URL: str = 'https://www.youtube.com/embed/{video_id}'

    def __init__(self,
                 video_id: str | None = None,
                 channel_name: str | None = None,
                 channel_thumbnail: YouTubeThumbnail | None = None,
                 browse_client: AsyncYouTubeClient | None = None,
                 download_client: YoutubeDL | None = None) -> None:

        # Consent cookies needed for YouTubeClient and yt-dlp
        self.browse_client: AsyncYouTubeClient | None = browse_client
        self.download_client: YoutubeDL | None = download_client

        self.video_id: str | None = video_id
        self.title: str | None = None
        self.long_title: str | None = None
        self.description: str | None = None

        # Info about the channel
        self.channel_id: str | None = None
        self.channel_name = channel_name
        self.channel_url: str | None = None
        self.channel_is_verified: bool | None = None
        self.channel_follower_count: int | None = None
        self.channel_thumbnail_url: str | None = \
            channel_thumbnail.url if channel_thumbnail else None
        self.channel_thumbnail_asset: YouTubeThumbnail | None = \
            channel_thumbnail

        self.created_timestamp: datetime = datetime.now(tz=timezone.utc)
        self.uploaded_timestamp: datetime | None = None
        self.published_timestamp: datetime | None = None
        self.availability: str | None = None
        self.available_country_codes: set[str] = set()

        self.view_count: int | None = None
        self.like_count: int | None = None
        self.dislike_count: int | None = None
        self.comment_count: int | None = None

        self.url: str | None = None
        self.embed_url: str | None = None
        self.thumbnails: dict[str, YouTubeThumbnail] = {}

        self.is_live: bool = False
        self.was_live: bool = False
        self.media_type: YouTubeMediaType | None = None
        self.embedable: bool | None = None

        self.age_limit: int = 0
        self.age_restricted: bool = False
        self.is_family_safe: bool | None = None
        self.is_tv_film_video: bool = False
        self.aspect_ratio: float = 0.0

        # Duration of the video in seconds
        self.duration: int | None = None

        self.chapters: list[YouTubeVideoChapter] = []
        self.subtitles: dict[str, YouTubeCaption] = {}
        self.automatic_captions: dict[str, YouTubeCaption] = {}
        self.heatmaps: list[dict[str, float]] = []

        # Data for the Byoda table with assets
        self.license: str | None = None

        # This is the default profile. If we ingest the asset from
        # YouTube then this will be overwritten with info about the
        # YouTube formats
        self.formats: dict[str, YouTubeFormat] = {}

        self.locale: str | None = None
        self.default_audio_language: str | None = None

        self.tags: set[str] = set()
        self.annotations: set[str] = set()
        self.categories: set[str] = set()
        self.keywords: set[str] = set()
        self.privacy_status: str = 'public'

    def __eq__(self, other: Self) -> bool:
        if not isinstance(other, YouTubeVideo):
            return False

        same: bool = (
            self.video_id == other.video_id
            and self.title == other.title
            and self.long_title == other.long_title
            and self.description == other.description
            and self.channel_id == other.channel_id
            and self.channel_name == other.channel_name
            and self.channel_url == other.channel_url
            and self.channel_is_verified == other.channel_is_verified
            and self.channel_thumbnail_url == other.channel_thumbnail_url
            and self.created_timestamp == other.created_timestamp
            and self.uploaded_timestamp == other.uploaded_timestamp
            and self.published_timestamp == other.published_timestamp
            and self.availability == other.availability
            and self.available_country_codes == other.available_country_codes
            and self.url == other.url
            and self.embed_url == other.embed_url
            and self.media_type == other.media_type
            and self.embedable == other.embedable
            and self.age_limit == other.age_limit
            and self.age_restricted == other.age_restricted
            and self.is_family_safe == other.is_family_safe
            and self.is_tv_film_video == other.is_tv_film_video
            and self.aspect_ratio == other.aspect_ratio
            and self.duration == other.duration
            and self.license == other.license
            and self.categories == other.categories
            and self.default_audio_language == other.default_audio_language
            and self.privacy_status == other.privacy_status
        )
        if not same:
            return False

        if self.thumbnails != other.thumbnails:
            return False

        if self.subtitles != other.subtitles:
            return False
        if self.automatic_captions != other.automatic_captions:
            return False
        if self.chapters != other.chapters:
            return False

        if len(self.formats) != len(other.formats):
            return False

        for key in self.formats:
            if key not in other.formats:
                return False
            if self.formats[key] != other.formats[key]:
                return False

        return True

    def to_dict(self) -> dict[str, any]:
        '''
        Returns a dict representation of the video
        '''

        data: dict[str, any] = {
            'video_id': self.video_id,
            'title': self.title,
            'long_title': self.long_title,
            'description': self.description,
            'channel_id': self.channel_id,
            'channel_name': self.channel_name,
            'channel_url': self.channel_url,
            'channel_is_verified': self.channel_is_verified,
            'channel_follower_count': self.channel_follower_count,
            'availability': self.availability,
            'view_count': self.view_count,
            'like_count': self.like_count,
            'dislike_count': self.dislike_count,
            'comment_count': self.comment_count,
            'available_country_codes': list(self.available_country_codes),
            'url': self.url,
            'is_live': self.is_live,
            'was_live': self.was_live,
            'media_type': self.media_type.value if self.media_type else None,
            'embedable': self.embedable,
            'age_limit': self.age_limit,
            'age_restricted': self.age_restricted,
            'is_family_safe': self.is_family_safe,
            'is_tv_film_video': self.is_tv_film_video,
            'aspect_ratio': self.aspect_ratio,
            'duration': self.duration,
            'heatmaps': self.heatmaps or [],
            'license': self.license,
            'locale': self.locale,
            'default_audio_language': self.default_audio_language,
            'tags': list(self.tags),
            'annotations': list(self.annotations),
            'keywords': list(self.keywords),
            'categories': list(self.categories),
            'privacy_status': self.privacy_status
        }

        if self.embed_url:
            data['embed_url'] = self.embed_url
        else:
            data['embed_url'] = self.EMBED_URL.format(video_id=self.video_id)

        if self.channel_thumbnail_asset:
            data['channel_thumbnail'] = self.channel_thumbnail_asset.to_dict()

        if self.created_timestamp:
            data['created_timestamp'] = self.created_timestamp.isoformat()

        if self.uploaded_timestamp:
            data['uploaded_timestamp'] = self.uploaded_timestamp.isoformat()

        if self.published_timestamp:
            data['published_timestamp'] = self.published_timestamp.isoformat()

        if self.thumbnails:
            data['thumbnails'] = {
                label: thumb.to_dict()
                for label, thumb in self.thumbnails.items()
            }

        if self.subtitles:
            data['subtitles'] = {
                lang: subtitle.to_dict()
                for lang, subtitle in self.subtitles.items()
            }
        if self.automatic_captions:
            data['automatic_captions'] = {
                lang: caption.to_dict()
                for lang, caption in self.automatic_captions.items()
            }

        if self.formats:
            data['formats'] = {
                format_id: fmt.to_dict()
                for format_id, fmt in self.formats.items()
            }

        return data

    @staticmethod
    def from_dict(data: dict[str, any]) -> Self:
        '''
        Factory for YouTubeVideo, parses data provided by to_dict
        '''

        video = YouTubeVideo(
            video_id=data.get('video_id'),
            channel_name=data.get('channel_name'),
            channel_thumbnail=YouTubeThumbnail.from_yt_dict(
                data['channel_thumbnail']
            ) if data.get('channel_thumbnail') else None
        )

        video.title = data.get('title')
        video.long_title = data.get('long_title')
        video.description = data.get('description')
        video.channel_id = data.get('channel_id')
        video.channel_url = data.get('channel_url')
        video.channel_is_verified = data.get('channel_is_verified')
        video.channel_follower_count = data.get('channel_follower_count')
        video.availability = data.get('availability')
        video.available_country_codes = set(
            data.get('available_country_codes', [])
        )
        video.view_count = data.get('view_count')
        video.like_count = data.get('like_count')
        video.dislike_count = data.get('dislike_count')
        video.comment_count = data.get('comment_count')
        video.url = data.get('url')
        video.embed_url = data.get('embed_url')
        video.is_live = data.get('is_live', False)
        video.was_live = data.get('was_live', False)
        media_type: str | None = data.get('media_type')
        video.media_type = YouTubeMediaType(media_type) if media_type else None
        video.embedable = data.get('embedable')
        video.age_limit = data.get('age_limit', 0)
        video.age_restricted = data.get('age_restricted', False)
        video.is_family_safe = data.get('is_family_safe')
        video.aspect_ratio = float(data.get('aspect_ratio', 0))
        video.duration = data.get('duration')
        video.heatmaps = data.get('heatmaps', [])
        video.license = data.get('license')
        video.locale = data.get('locale')
        video.default_audio_language = data.get('default_audio_language')
        video.categories = set(data.get('categories', []))
        video.tags = set(data.get('tags', []))
        video.annotations = set(data.get('annotations', []))
        video.keywords = set(data.get('keywords', []))
        video.privacy_status = data.get('privacy_status', 'public')
        if isinstance(video.privacy_status, bool):
            video.privacy_status = 'public' if video.privacy_status else 'private'

        if 'created_timestamp' in data:
            try:
                video.created_timestamp = dateutil_parser.parse(
                    data['created_timestamp']
                )
            except (ValueError, TypeError):
                pass

        if 'uploaded_timestamp' in data:
            try:
                video.uploaded_timestamp = dateutil_parser.parse(
                    data['uploaded_timestamp']
                )
            except (ValueError, TypeError):
                pass

        if 'published_timestamp' in data:
            try:
                video.published_timestamp = dateutil_parser.parse(
                    data['published_timestamp']
                )
            except (ValueError, TypeError):
                pass

        video.thumbnails = {
            label: YouTubeThumbnail.from_dict(thumb_data)
            for label, thumb_data in data.get('thumbnails', {}).items()
        }

        video.subtitles = {
            lang: YouTubeCaption.from_dict(subtitle_data)
            for lang, subtitle_data in data.get('subtitles', {}).items()
        }

        video.automatic_captions = {
            lang: YouTubeCaption.from_dict(caption_data)
            for lang, caption_data
            in data.get('automatic_captions', {}).items()
        }

        video.formats = {
            format_id: YouTubeFormat.from_dict(fmt_data)
            for format_id, fmt_data in data.get('formats', {}).items()
        }

        return video

    @staticmethod
    def from_rss_entry(entry: untangle.Element) -> Self:
        '''
        Factory for YouTubeVideo, populates the subset of fields available
        from a YouTube channel RSS feed entry. Fields not present in the
        RSS feed are left at their constructor defaults.

        We currently do not try to detect shorts from the RSS feed:
        'entry.link._attributes=
            {
                'rel': 'alternate',
                'href': 'https://www.youtube.com/shorts/QrAZfXiuF9E'
            }
        We're not using that because we don't have an is_short property in
        YouTubeVideo

        :param entry: An untangle Element for a single <entry> in a YouTube
        channel RSS feed (https://www.youtube.com/feeds/videos.xml).
        :returns: A YouTubeVideo instance with RSS-available fields set.
        :raises: AttributeError if the entry is missing the mandatory
        yt:videoId or title elements.
        '''

        video = YouTubeVideo(video_id=entry.yt_videoId.cdata)
        video.title = entry.title.cdata
        video.url = YouTubeVideo.VIDEO_URL.format(video_id=video.video_id)
        video.embed_url = YouTubeVideo.EMBED_URL.format(
            video_id=video.video_id
        )

        try:
            video.channel_id = entry.yt_channelId.cdata
        except AttributeError:
            pass

        try:
            video.channel_name = entry.author.name.cdata
            video.channel_url = entry.author.uri.cdata
        except AttributeError:
            pass

        try:
            video.published_timestamp = dateutil_parser.parse(
                entry.published.cdata
            )
        except (ValueError, AttributeError):
            pass

        try:
            video.uploaded_timestamp = dateutil_parser.parse(
                entry.updated.cdata
            )
        except (ValueError, AttributeError):
            pass

        try:
            video.description = entry.media_group.media_description.cdata
        except AttributeError:
            pass

        try:
            view_str: str = (
                entry.media_group.media_community.media_statistics['views']
            )
            video.view_count = int(view_str)
        except (AttributeError, KeyError, ValueError):
            pass

        try:
            thumb_data: dict = {
                'url': entry.media_group.media_thumbnail['url']
            }
            try:
                thumb_data['width'] = int(
                    entry.media_group.media_thumbnail['width']
                )
                thumb_data['height'] = int(
                    entry.media_group.media_thumbnail['height']
                )
            except (KeyError, ValueError):
                pass
            video.thumbnails = YouTubeVideo._parse_thumbnails(
                None, [thumb_data]
            )
        except (AttributeError, KeyError):
            pass

        return video

    async def update(self, browse_client: AsyncYouTubeClient | None = None,
                     delay: float = 1.0) -> None:
        '''
        Update the video metadata by scraping the video page. This can
        be used to update fields that may have changed since the initial
        scrape, such as view count, like count or thumbnails. This method does
        not update the formats, chapters, or captions - for that you would
        need to call scrape() again.

        Calling this function too frequently may lead to rate limiting or
        CAPTCHAs, so a delay is included between the request and the scrape.
        The default delay is 1 second, but it can be increased if you are
        making frequent calls to update() on many videos.

        :returns: (none)
        :raises: ValueError, RuntimeError
        '''

        if not self.video_id:
            raise ValueError('Cannot update video without video_id')

        if not browse_client:
            if not self.browse_client:
                raise ValueError('No browse client available')
            browse_client = self.browse_client

        if not self.url:
            self.url: str = self.VIDEO_URL.format(video_id=self.video_id)

        if not self.embed_url:
            self.embed_url = self.EMBED_URL.format(video_id=self.video_id)

        try:
            html_content: str | None = await browse_client.get(
                self.url, delay=delay
            )
        except ValueError:
            _LOGGER.warning(
                'Video page not found for video',
                extra={'video_id': self.video_id, 'url': self.url}
            )
            raise

        if html_content is None:
            raise RuntimeError(f'Failed to retrieve video page for {self.url}')

        player_response: dict[str, any] = self._extract_player_response(
            html_content
        )

        self._parse_video_html(player_response)

    @staticmethod
    async def scrape(
        video_id: str, channel_name: str | None,
        channel_thumbnail: YouTubeThumbnail | None,
        deno_path: str = DENO_PATH, po_token_url: str = PO_TOKEN_URL,
        ytdlp_cache_dir: str = YTDLP_CACHE_DIR, download_client: YoutubeDL | None = None,
        debug: bool = False, save_dir: str | None = None,
        filename_prefix: str = '', with_formats: bool = True,
        proxies: list[str] = []
    ) -> Self | None:
        '''
        Collects data about a video using InnerTube API and optionally yt-dlp
        for format extraction.

        :param video_id: YouTube video ID
        :param channel_name: Name of the channel that we are scraping the
        video for
        :param channel_thumbnail: Thumbnail for the creator of the video
        page
        '''

        if not proxies:
            proxy: str | None = YouTubeRateLimiter.get().select_proxy(
                YouTubeCallType.PLAYER
            )
        else:
            proxy = random.choice(proxies)

        instantiated_download_client: bool = False
        if not download_client:
            download_client = YouTubeVideo._setup_download_client(
                deno_path, po_token_url, ytdlp_cache_dir, debug, proxy=proxy,
            )
            instantiated_download_client = True

        self: YouTubeVideo = YouTubeVideo(
            video_id=video_id,
            channel_name=channel_name,
            channel_thumbnail=channel_thumbnail,
            download_client=download_client,
        )

        await self.from_innertube(proxy=proxy)

        if with_formats:
            await self._scrape_video(proxy=proxy)

        if save_dir:
            await self.to_file(save_dir, filename_prefix)

        if instantiated_download_client:
            download_client.close()

        return self

    async def to_file(self, save_dir: str, filename_prefix: str = '',
                      overwrite: bool = True, compressed: bool = True) -> str:
        '''
        Saves the video metadata to a JSON file in the specified directory.

        :param save_dir: Directory to save the JSON file in
        :returns: (none)
        :raises: (none)
        '''

        if not self.video_id:
            raise ValueError('Cannot save video without video_id')

        filename: str = YouTubeVideo.get_filepath(
            self.video_id, save_dir, filename_prefix, compressed
        )

        data: bytes = orjson.dumps(self.to_dict(), option=orjson.OPT_INDENT_2)

        if compressed:
            data = brotli.compress(data, quality=11, mode=brotli.MODE_TEXT)
        if not overwrite and os.path.exists(filename):
            raise FileExistsError(f'File {filename} already exists')

        async with aiofiles.open(filename, 'wb') as f:
            await f.write(data)

        return filename

    @staticmethod
    async def from_file(video_id: str, load_dir: str,
                        filename_prefix: str = '',
                        compressed: bool = True) -> Self:
        '''
        Loads video metadata from a JSON file in the specified directory.

        :param load_dir: Directory to load the JSON file from
        :returns: A YouTubeVideo instance with data loaded from the file
        :raises: FileNotFoundError if the file does not exist
        '''

        filename: str = YouTubeVideo.get_filepath(
            video_id, load_dir, filename_prefix, compressed
        )

        async with aiofiles.open(filename, 'rb') as f:
            data: bytes = await f.read()

        if compressed:
            data = brotli.decompress(data)

        video_data: dict[str, any] = orjson.loads(data)
        return YouTubeVideo.from_dict(video_data)

    @staticmethod
    def get_filepath(video_id: str, save_dir: str, filename_prefix: str = '',
                     compressed: bool = True) -> str:
        '''
        Get the file path for the video metadata JSON file in the specified
        directory.

        :param save_dir: Directory to save the JSON file in
        :returns: File path for the JSON file
        :raises: ValueError if video_id is not set
        '''

        if not video_id:
            raise ValueError('Cannot get file path for video without video_id')

        filename: str = f'{save_dir}/{filename_prefix}{video_id}.json'
        if compressed:
            filename += '.br'

        return filename

    @staticmethod
    def _setup_download_client(
        deno_path: str, po_token_url: str,
        ytdlp_cache_dir: str = YTDLP_CACHE_DIR,
        debug: bool = False, proxy: str | None = None,
        cookie_file: str | None = None
    ) -> YoutubeDL:
        '''
        Set up the yt-dlp download client with appropriate options for
        scraping YouTube videos, including consent cookies and Deno for
        JavaScript execution.

        :param deno_path: the path to the Deno executable for JavaScript
        execution
        :param po_token_url: the URL to fetch PO tokens for YouTube scraping
        :param debug: whether to enable debug logging
        :param proxy: optional proxy URL for yt-dlp requests
        :returns: configured YoutubeDL instance
        :raises: ValueError if required parameters are missing
        '''

        if not deno_path:
            raise ValueError('deno_path is required if no download_client')
        if not po_token_url:
            raise ValueError('po_token_url is required if no download_client')

        _LOGGER.debug(
            'Using deno and po-token-url',
            extra={
                'deno_path': deno_path,
                'po_token_url': po_token_url,
            }
        )

        from .youtube_client import CONSENT_COOKIES
        with tempfile.NamedTemporaryFile(mode='w') as temp_file:
            temp_file.write('# Netscape HTTP Cookie File\n')
            for name, value in CONSENT_COOKIES.items():
                temp_file.write(
                    f'.youtube.com\tTRUE\t/\tFALSE\t0\t{name}\t{value}\n'
                )
            temp_file.flush()
            cookie_file_path: str = temp_file.name
            ytdlp_opts: dict = {
                'quiet': not debug,
                'verbose': debug,
                'logger': _LOGGER,
                'noprogress': True,
                'no_color': True,
                'format': 'all',
                'proxy': proxy,
                'cookiefile': cookie_file_path,
                'cachedir': ytdlp_cache_dir,
                'js_runtimes': {'deno': {'path': deno_path}},
                'extractor_args': {
                    'youtube': {
                        'player-client': 'tv,mweb',
                        'youtubepot-bgutilhttp:base_url': po_token_url
                    }
                },
                'remote_components': ['ejs:github']
            }
            if cookie_file:
                ytdlp_opts['cookiefile'] = cookie_file

            download_client = YoutubeDL(ytdlp_opts)
        return download_client

    def _extract_initial_data(self, html_content: str) -> dict:
        '''
        Extract ytInitialData from the HTML page

        :param html_content: HTML content of the video page
        :returns: dict with player response data
        :raises: ByodaValueError if data could not be extracted
        '''

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
                    json_str: str = match.group(1).replace("\\'", "'")
                    return orjson.loads(json_str)
                except orjson.JSONDecodeError:
                    continue

        raise ValueError(
            f'Could not extract initial data from video page from {self.url}'
        )

    def _extract_player_response(self, html_content: str) -> dict:
        '''
        Extract ytInitialPlayerResponse from the HTML page

        :param html_content: HTML content of the video page
        :returns: dict with player response data
        :raises: ByodaValueError if data could not be extracted
        '''
        patterns: list[str] = [
            r'ytInitialPlayerResponse\s*=\s*({.*?});',
            r'var ytInitialPlayerResponse = ({.*?});',
        ]

        for pattern in patterns:
            match: re.Match[str] | None = re.search(
                pattern, html_content, re.DOTALL
            )
            if match:
                try:
                    json_str: str = match.group(1)
                    json_str = json_str.replace("\\'", "'")
                    return orjson.loads(json_str)
                except orjson.JSONDecodeError:
                    continue

        raise ValueError(
            f'Could not extract player response data from {self.url}'
        )

    def _parse_video_html(self, player_response: dict) -> None:
        '''
        Parse the metadata scraped from a YouTube video page

        :param initial_data:
        :param player_response:
        :returns: (none)
        :raises: (none)
        '''

        # Basic info from player response
        video_details: dict[str, any] = player_response.get('videoDetails', {})
        microformat: dict[str, any] = player_response.get(
            'microformat', {}
        ).get(
            'playerMicroformatRenderer'
        )
        if not video_details or not microformat:
            _LOGGER.debug(
                'Missing microformat data for video',
                extra={'video_id': self.video_id}
            )

            status: str = player_response.get(
                'playabilityStatus', {}
            ).get('status')
            if status == 'LOGIN_REQUIRED':
                raise RuntimeError('YouTube blocked the client')

            raise ValueError('Missing microformat data for video')

        # Video ID and URLs. These will be changed later if the video is a
        # short
        self.url = self.VIDEO_URL.format(video_id=self.video_id)
        self.embed_url = self.EMBED_URL.format(video_id=self.video_id)

        self.title = video_details.get('title')
        self.description = video_details.get('shortDescription')

        try:
            self.duration = int(
                microformat.get(
                    'lengthSeconds', video_details.get('lengthSeconds')
                )
            )
        except ValueError:
            pass

        self.view_count = convert_number_string(
            video_details.get('viewCount', 0)
        )

        if microformat.get('publishDate'):
            self.published_timestamp = dateutil_parser.parse(
                microformat['publishDate']
            )

        if microformat.get('uploadDate'):
            self.uploaded_timestamp = dateutil_parser.parse(
                microformat['uploadDate']
            )

        category: str | None = video_details.get('category')
        if category:
            if isinstance(category, str):
                self.categories.add(category)
            elif isinstance(category, list):
                self.categories |= set(category)

        # TODO: does this ever get a value?
        self.default_audio_language = microformat.get(
            'defaultAudioLanguage'
        )

        self.is_family_safe = microformat.get('isFamilySafe', False)

        if not self.privacy_status:
            self.privacy_status = microformat.get('privacyStatus', 'public')
            if isinstance(self.privacy_status, bool) and self.privacy_status:
                self.privacy_status = 'private'
            else:
                self.privacy_status = 'public'

        self.is_live = video_details.get(
            'isLive', video_details.get('isLiveContent', False)
        )

        self.keywords = self.keywords | set(video_details.get('keywords', []))

        self.thumbnails = self.thumbnails | YouTubeVideo._parse_thumbnails(
            None, video_details.get('thumbnail', {}).get('thumbnails', []) +
            microformat.get('thumbnail', {}).get('thumbnails', [])
        )

        self.age_restricted = self._check_age_restriction(
            player_response.get('playabilityStatus', {})
        )

        self.embedable = \
            microformat.get('embed', {}).get('iframeUrl') is not None

        if microformat.get('hasYpcMetadata'):
            self.license = 'youtube'

    @staticmethod
    def _parse_thumbnails(label: str | None,
                          thumbnail_data: list[dict[str, str | int]]
                          ) -> dict[str, YouTubeThumbnail]:
        '''
        Parse thumbnail URLs without downloading
        '''

        thumbnails: dict[str, YouTubeThumbnail] = {}
        thumb_list: list[YouTubeThumbnail] = []
        for thumbnail_d in thumbnail_data or []:
            thumbnail = YouTubeThumbnail(thumbnail_d)
            if thumbnail.url:
                # We only want to store thumbnails for which
                # we know the size and have a URL
                thumb_list.append(thumbnail)
            else:
                _LOGGER.debug(
                    'Not importing thumbnail without size or URL',
                    extra={
                        'size': thumbnail.size,
                        'url': thumbnail.url,
                    }
                )

        if not thumb_list:
            return {}

        # Sort by width and assign to standard sizes
        thumb_list = sorted(
            thumb_list, key=lambda x: (int(x.preference or 0), x.width or 0)
        )

        seen_urls: set[str] = set()

        thumb: YouTubeThumbnail
        for thumb in thumb_list:
            if thumb.url in seen_urls:
                continue
            seen_urls.add(thumb.url)

            if not label:
                if thumb.id:
                    label = thumb.id
                else:
                    label = f'{thumb.width}x{thumb.height}'

            if label not in thumbnails:
                thumbnails[label] = thumb

            label = None

        return thumbnails

    def _check_age_restriction(self, playability: dict) -> bool:
        '''
        Check if video is age-restricted

        :param playability: playabilityStatus section from the player response
        :returns: True if the video is age-restricted, False otherwise
        '''

        reason: str = playability.get('reason', '')
        if 'age' in reason.lower() or 'sign in' in reason.lower():
            return True

        # Check for age gate
        if playability.get('status') == 'LOGIN_REQUIRED':
            return True

        return False

    async def _scrape_video(self, proxy: str | None = None) -> None:
        '''
        Collects data about a video by scraping the webpage for the video.

        :param proxy: proxy to acquire the rate-limit token for; when None
            the limiter auto-selects from the registered pool.
        :returns: (none)
        :raises: (none)
        '''

        if not self.download_client:
            raise RuntimeError(
                'No download_client available for scraping video'
            )

        proxy, cookie_file = await YouTubeRateLimiter.get().acquire(
            YouTubeCallType.PLAYER, proxy=proxy,
        )
        self.download_client.params['proxy'] = proxy
        if cookie_file:
            self.download_client.params['cookiefile'] = cookie_file

        try:
            _LOGGER.debug(
                'Scraping YouTube video',
                extra={'video_id': self.video_id}
            )
            loop = asyncio.get_event_loop()
            proxy_label: str = proxy or 'none'
            METRIC_EXTRACT_INFO_ACTIVE.labels(proxy=proxy_label).inc()
            try:
                video_info: dict[str, any] = await loop.run_in_executor(
                    None,
                    lambda: self.download_client.extract_info(
                        self.url, download=False
                    )
                )
            finally:
                METRIC_EXTRACT_INFO_ACTIVE.labels(
                    proxy=proxy_label
                ).dec()
            if video_info:
                _LOGGER.debug(
                    'Collected info for video',
                    extra={'video_id': self.video_id}
                )
            else:
                sleepy_time: int = randrange(10, 30)
                await sleep(sleepy_time)
                raise RuntimeError(
                    f'Video scrape failed, no info returned: {self.video_id}'
                )
        except DownloadError as exc:
            sleepy_time: int = randrange(2, 5)
            await sleep(sleepy_time)
            raise RuntimeError(
                f'Failed to extract info for video: {self.video_id}: '
                f'{exc}, sleeping'
            ) from exc
        except Exception as exc:
            sleepy_time: int = randrange(10, 30)
            await sleep(sleepy_time)
            raise RuntimeError(
                f'Failed to extract info for video: {self.video_id}: '
                f'{exc}, sleeping'
            ) from exc

        self.channel_id = video_info.get('channel_id')
        self.channel_name: str = self.channel_name or video_info.get('channel')
        self.channel_url: str = video_info.get('channel_url')
        self.channel_is_verified: bool = video_info.get('channel_is_verified')
        self.channel_follower_count: int = video_info.get(
            'channel_follower_count'
        )
        self.embedable = video_info.get('playable_in_embed', True)
        try:
            self.media_type = YouTubeMediaType(
                video_info.get('media_type', '').lower()
            )
        except (KeyError, ValueError):
            self.media_type = None

        self.description = video_info.get('description')
        self.title = video_info.get('title')
        self.view_count = video_info.get('view_count')
        self.like_count = video_info.get('like_count')
        self.comment_count = video_info.get(
            'comment_count'
        )
        self.long_title = self.long_title or video_info.get('fulltitle')
        self.is_live = self.is_live or video_info.get('is_live')
        self.was_live = self.was_live or video_info.get('was_live')
        self.availability = video_info.get('availability')
        self.duration = self.duration or video_info.get('duration')
        self.tags = self.tags | set(video_info.get('tags', []))
        self.categories = \
            self.categories | set(video_info.get('categories', []))
        self.default_audio_language = video_info.get('language')
        self.age_limit = self.age_limit or video_info.get('age_limit', 0)
        self.heatmaps = video_info.get('heatmap', [])
        self.aspect_ratio: float = float(video_info.get('aspect_ratio') or 0)

        for language_code, captions in video_info.get(
                'automatic_captions', {}).items():
            for caption in captions:
                caption = YouTubeCaption(language_code, caption)
                self.automatic_captions[language_code] = caption

        for language_code, captions in video_info.get(
                'subtitles', {}).items():
            for caption in captions:
                caption = YouTubeCaption(language_code, caption)
                self.subtitles[language_code] = caption

        self.published_timestamp = dateutil_parser.parse(
            video_info.get('upload_date', self.published_timestamp)
        )

        self.thumbnails = YouTubeVideo._parse_thumbnails(
            None, video_info.get('thumbnails') or []
        )

        for chapter_data in video_info.get('chapters') or []:
            chapter = YouTubeVideoChapter(chapter_data)
            self.chapters.append(chapter)

        self.formats = self._parse_formats(video_info['formats'])

        _LOGGER.debug(
            'Parsed all available data for video',
            extra={'video_id': self.video_id}
        )

    @staticmethod
    def _parse_formats(profile_info: list[dict[str, any]]
                       ) -> dict[str, YouTubeFormat]:

        formats: dict[str, YouTubeFormat] = {}
        for format_data in profile_info or []:
            yt_format: YouTubeFormat = YouTubeFormat.from_dict(format_data)
            formats[yt_format.format_id] = yt_format

        return formats

    async def from_innertube(self, innertube: InnerTube | None = None,
                             proxy: str | None = None,
                             ) -> None:
        await InnerTubeVideoParser.scrape(
            self, innertube, proxy=proxy
        )
