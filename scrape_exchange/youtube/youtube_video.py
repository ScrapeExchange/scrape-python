'''
Model a Youtube video

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import os
import re

from typing import Self
from random import randrange
from logging import Logger
from logging import getLogger
from datetime import datetime
from datetime import timezone
import aiofiles
from dateutil import parser as dateutil_parser

import orjson

from asyncio import sleep

from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadError

from ..util import IngestStatus
from ..util import convert_number_string

from .youtube_format import YouTubeFormat
from .youtube_thumbnail import YouTubeThumbnail
from .youtube_client import AsyncYouTubeClient

_LOGGER: Logger = getLogger(__name__)

DENO_PATH: str = os.environ.get('HOME', '') + '/.deno/bin/deno'
PO_TOKEN_URL: str = 'http://localhost:4416'


class YouTubeVideoChapter:
    def __init__(self, chapter_info: dict[str, float | str]) -> None:
        self.start_time: float = chapter_info.get('start_time')
        self.end_time: float = chapter_info.get('end_time')
        self.title: str = chapter_info.get('title')

    def __eq__(self, other: Self) -> bool:
        if not isinstance(other, YouTubeVideoChapter):
            return False

        return (
            self.start_time == other.start_time
            and self.end_time == other.end_time
            and self.title == other.title
        )

    def to_dict(self) -> dict[str, str, float]:
        '''
        Returns a dict representation of the chapter
        '''

        return {
            'start': self.start_time,
            'end': self.end_time,
            'title': self.title
        }

    @staticmethod
    def from_dict(data: dict[str, str | int | float]) -> Self:
        '''
        Factory for YouTubeVideoChapter, parses data are provided
        by yt-dlp
        '''

        return YouTubeVideoChapter(
            {
                'start_time': data.get('start_time'),
                'end_time': data.get('end_time'),
                'title': data.get('title')
            }
        )


class YouTubeCaption:
    def __init__(self, language_code: str, caption_info: dict[str, str]
                 ) -> None:
        '''
        Describes a caption / subtitle track for a YouTube video

        :param language_code: BCP-47 language code for the caption
        :param is_auto_generated: whether the caption track is auto-generated
        :param caption_info: information about the caption track
        :returns: (none)
        :raises: (none)
        '''

        self.language_code: str = language_code
        self.url: str = caption_info.get('url')
        self.extension: str = caption_info.get('ext')
        self.protocol: str = caption_info.get('protocol')

    def __eq__(self, other: Self) -> bool:
        if not isinstance(other, YouTubeCaption):
            return False

        return (
            self.language_code == other.language_code
            and self.url == other.url
            and self.extension == other.extension
            and self.protocol == other.protocol
        )

    def to_dict(self) -> dict[str, str]:
        '''
        Returns a dict representation of the caption
        '''

        return {
            'language_code': self.language_code,
            'url': self.url,
            'extension': self.extension,
            'protocol': self.protocol
        }

    @staticmethod
    def from_dict(data: dict[str, str | int | bool]) -> Self:
        '''
        Factory for YouTubeCaption, parses data provided by yt-dlp
        '''

        return YouTubeCaption(
            language_code=data.get('language_code'),
            caption_info={
                'url': data.get('url'),
                'ext': data.get('extension'),
                'protocol': data.get('protocol')
            }
        )

class YouTubeShort:
    SHORTS_URL: str = 'https://www.youtube.com/shorts/{video_id}'

    def __init__(self, video_id: str, title: str) -> None:
        self.video_id: str = video_id
        self.title: str = title

    def __eq__(self, other: Self) -> bool:
        if not isinstance(other, YouTubeShort):
            return False

        return self.video_id == other.video_id

    def to_dict(self) -> dict[str, str]:
        '''
        Returns a dict representation of the short
        '''

        return {
            'video_id': self.video_id,
            'title': self.title
        }

    @staticmethod
    def from_dict(data: dict[str, str | int]) -> Self:
        '''
        Factory for YouTubeShort, parses data provided by yt-dlp
        '''

        return YouTubeShort(
            video_id=data.get('video_id'),
            title=data.get('title')
        )


class YouTubeVideo:
    VIDEO_URL: str = 'https://www.youtube.com/watch?v={video_id}'

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
        self.kind: str | None = None
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

        self.view_count: int | None = None
        self.like_count: int | None = None
        self.dislike_count: int | None = None
        self.comment_count: int | None = None

        self.url: str | None = None
        self.short_url: str | None = None
        self.embed_url: str | None = None
        self.thumbnails: dict[YouTubeThumbnail] = {}

        self.is_live: bool = False
        self.was_live: bool = False
        self.is_short: bool = False
        self.embedable: bool | None = None

        self.age_limit: int = 0
        self.age_restricted: bool = False
        self.is_family_safe: bool | None = None
        self.aspect_ratio: str | None = None

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
        self.default_audio_language: str | None = 'en'

        self.tags: set[str] = set()
        self.annotations: set[str] = set()
        self.categories: set[str] = set()
        self.keywords: set[str] = set()
        self.privacy_status: str = 'public'

        self.client: AsyncYouTubeClient = AsyncYouTubeClient()

        self.ingest_status: IngestStatus = IngestStatus.NONE

    def __eq__(self, other: Self) -> bool:
        if not isinstance(other, YouTubeVideo):
            return False

        same: bool = (
            self.video_id == other.video_id
            and self.title == other.title
            and self.channel_name == other.channel_name
            and self.kind == other.kind
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
            and self.url == other.url
            and self.short_url == other.short_url
            and self.embed_url == other.embed_url
            and self.is_short == other.is_short
            and self.embedable == other.embedable
            and self.age_limit == other.age_limit
            and self.age_restricted == other.age_restricted
            and self.is_family_safe == other.is_family_safe
            and self.aspect_ratio == other.aspect_ratio
            and self.duration == other.duration
            and self.license == other.license
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
            'kind': self.kind,
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
            'url': self.url,
            'short_url': self.short_url,
            'embed_url': self.embed_url,
            'is_live': self.is_live,
            'was_live': self.was_live,
            'is_short': self.is_short,
            'embedable': self.embedable,
            'age_limit': self.age_limit,
            'age_restricted': self.age_restricted,
            'is_family_safe': self.is_family_safe,
            'aspect_ratio': self.aspect_ratio,
            'duration': self.duration,
            'heatmaps': self.heatmaps,
            'license': self.license,
            'locale': self.locale,
            'default_audio_language': self.default_audio_language,
            'tags': list(self.tags),
            'annotations': list(self.annotations),
            'keywords': list(self.keywords),
            'privacy_status': self.privacy_status
        }

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
        video.kind = data.get('kind')
        video.long_title = data.get('long_title')
        video.description = data.get('description')
        video.channel_id = data.get('channel_id')
        video.channel_url = data.get('channel_url')
        video.channel_is_verified = data.get('channel_is_verified')
        video.channel_follower_count = data.get('channel_follower_count')
        video.availability = data.get('availability')
        video.view_count = data.get('view_count')
        video.like_count = data.get('like_count')
        video.dislike_count = data.get('dislike_count')
        video.comment_count = data.get('comment_count')
        video.url = data.get('url')
        video.short_url = data.get('short_url')
        video.embed_url = data.get('embed_url')
        video.is_live = data.get('is_live', False)
        video.was_live = data.get('was_live', False)
        video.is_short = data.get('is_short', False)
        video.embedable = data.get('embedable')
        video.age_limit = data.get('age_limit', 0)
        video.age_restricted = data.get('age_restricted', False)
        video.is_family_safe = data.get('is_family_safe')
        video.aspect_ratio = data.get('aspect_ratio')
        video.duration = data.get('duration')
        video.heatmaps = data.get('heatmaps', [])
        video.license = data.get('license')
        video.locale = data.get('locale')
        video.default_audio_language = data.get('default_audio_language')
        video.tags = set(data.get('tags', []))
        video.annotations = set(data.get('annotations', []))
        video.keywords = set(data.get('keywords', []))
        video.privacy_status = data.get('privacy_status', 'public')

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
    async def scrape(
        video_id: str, channel_name: str | None,
        channel_thumbnail: YouTubeThumbnail | None,
        deno_path: str = DENO_PATH, po_token_url: str = PO_TOKEN_URL,
        browse_client: AsyncYouTubeClient | None = None,
        download_client: YoutubeDL | None = None,
        debug: bool = False, save_dir: str | None = None
    ) -> Self | None:
        '''
        Collects data about a video by scraping the webpage for the video

        :param video_id: YouTube video ID
        :param channel_name: Name of the channel that we are scraping the
        video for
        :param channel_thumbnail: Thumbnail for the creator of the video
        page
        '''

        if not browse_client:
            browse_client = AsyncYouTubeClient()

        if not download_client:
            download_client = YouTubeVideo._setup_download_client(
                browse_client, deno_path, po_token_url, debug
            )

        self: YouTubeVideo = YouTubeVideo(
            video_id=video_id,
            channel_name=channel_name,
            channel_thumbnail=channel_thumbnail,
            browse_client=browse_client,
            download_client=download_client,
        )

        canonical_url: str = self.VIDEO_URL.format(video_id=video_id)
        html_content: str | None = await self.client.get(canonical_url)

        # Extract initial data
        initial_data: dict[str, any] = self._extract_initial_data(html_content)
        player_response: dict[str, any] = self._extract_player_response(
            html_content
        )

        self._parse_video_html(initial_data, player_response)

        await self._scrape_video()

        self.is_short = self._is_short(initial_data)

        if save_dir:
            await self.to_file(save_dir)

        return self

    async def to_file(self, save_dir: str) -> None:
        '''
        Saves the video metadata to a JSON file in the specified directory.

        :param save_dir: Directory to save the JSON file in
        :returns: (none)
        :raises: (none)
        '''

        if not self.video_id:
            raise ValueError('Cannot save video without video_id')

        filename: str = f'{save_dir}/{self.video_id}.json'
        async with aiofiles.open(filename, 'w') as f:
            await f.write(
                orjson.dumps(
                    self.to_dict(), option=orjson.OPT_INDENT_2
                ).decode('utf-8')
            )

    @staticmethod
    def _setup_download_client(browse_client: AsyncYouTubeClient,
                               deno_path: str, po_token_url: str,
                               debug: bool = False) -> YoutubeDL:
        '''
        Set up the yt-dlp download client with appropriate options for
        scraping YouTube videos, including consent cookies and Deno for
        JavaScript execution.

        :param deno_path: the path to the Deno executable for JavaScript
        execution
        :param po_token_url: the URL to fetch PO tokens for YouTube scraping
        :param debug: whether to enable debug logging
        :returns: configured YoutubeDL instance
        :raises: ValueError if required parameters are missing
        '''

        if not deno_path:
            raise ValueError('deno_path is required if no download_client')
        if not po_token_url:
            raise ValueError('po_token_url is required if no download_client')

        ytdlp_opts: dict = {
            'quiet': not debug,
            'verbose': debug,
            'logger': _LOGGER,
            'noprogress': True,
            'no_color': True,
            'format': 'all',
            'http_headers': dict(browse_client.headers) | {
                'Cookie': '; '.join(
                    f'{k}={v}' for k, v
                    in browse_client.consent_cookies.items()
                )
            },
            'js_runtimes': {'deno': {'path': deno_path}},
            'extractor_args': {
                'youtube': {
                    'player-client': 'default,mweb',
                    'youtubepot-bgutilhttp:base_url': po_token_url
                }
            }
        }
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

    def _parse_video_html(self, initial_data: dict,
                          player_response: dict) -> None:
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
            _LOGGER.info(
                'Missing microformat data for video',
                extra={'video_id': self.video_id}
            )
            raise ValueError('Missing microformat data for video')

        # Video ID and URLs
        self.url = f'https://www.youtube.com/watch?v={self.video_id}'
        self.short_url = f'https://youtu.be/{self.video_id}'
        self.embed_url = f'https://www.youtube.com/embed/{self.video_id}'

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

        if microformat.get('category'):
            self.categories | set(microformat.get('category', []))

        # TODO: does this ever get a value?
        self.default_audio_language = microformat.get(
            'defaultAudioLanguage'
        )

        self.is_family_safe = microformat.get('isFamilySafe', False)

        self.privacy_status = microformat.get(
            'isUnlisted', self.privacy_status or 'public')

        self.is_live = video_details.get(
            'isLive', video_details.get('isLiveContent', False)
        )

        results: str = initial_data.get('contents', {}).get(
                'twoColumnWatchNextResults', {}).get('secondaryResults', '')
        if (self.duration and self.duration < 60
                and microformat.get('isShortsEligible', False)
                and 'reelShelfRenderer' in results):
            self.is_short = True

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
                    f'Not importing thumbnail without size '
                    f'({thumbnail.size}) or URL ({thumbnail.url})'
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

    async def _scrape_video(self) -> None:
        '''
        Collects data about a video by scraping the webpage for the video.

        :returns: (none)
        :raises: (none)
        '''

        if not self.download_client:
            raise RuntimeError(
                'No download_client available for scraping video'
            )

        try:
            _LOGGER.debug(f'Scraping YouTube video: {self.video_id}')
            video_info: dict[str, any] = \
                self.download_client.extract_info(
                    self.url, download=False
                )
            if video_info:
                _LOGGER.debug(f'Collected info for video: {self.video_id}')
            else:
                sleepy_time: int = randrange(10, 30)
                await sleep(sleepy_time)
                raise RuntimeError(
                    f'Video scrape failed, no info returned: {self.video_id}'
                )
        except DownloadError as exc:
            sleepy_time: int = randrange(2, 5)
            self._transition_state(IngestStatus.UNAVAILABLE)
            await sleep(sleepy_time)
            raise RuntimeError(
                f'Failed to extract info for video: {self.video_id}, sleeping'
            ) from exc
        except Exception as exc:
            sleepy_time: int = randrange(10, 30)
            self._transition_state(IngestStatus.UNAVAILABLE)
            await sleep(sleepy_time)
            raise RuntimeError(
                f'Failed to extract info for video: {self.video_id}, sleeping'
            ) from exc

        self.channel_id = video_info.get('channel_id')
        self.channel_name: str = self.channel_name or video_info.get('channel')
        self.channel_url: str = video_info.get('channel_url')
        self.channel_is_verified: bool = video_info.get('channel_is_verified')
        self.channel_follower_count: int = video_info.get(
            'channel_follower_count'
        )
        self.embedable = video_info.get('playable_in_embed', True)

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
        self.categories = self.categories | set(video_info.get('categories', []))
        self.default_audio_language = video_info.get('language')
        self.age_limit = self.age_limit or video_info.get('age_limit', 0)
        self.heatmaps = video_info.get('heatmaps', [])
        self.aspect_ratio: str = video_info.get('aspect_ratio')

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
            f'Parsed all available data for video: {self.video_id}'
        )

    @staticmethod
    def _parse_formats(profile_info: list[dict[str, any]]
                       ) -> dict[str, YouTubeFormat]:

        formats: dict[str, YouTubeFormat] = {}
        for format_data in profile_info or []:
            yt_format: YouTubeFormat = YouTubeFormat.from_dict(format_data)
            formats[yt_format.format_id] = yt_format

        return formats

    def _is_short(self, initial_data: dict) -> bool:
        '''
        Determine if the video is a YouTube Short

        :param video_details: videoDetails section from the player response
        :param initial_data: ytInitialData extracted from the HTML page
        :returns: True if the video is a YouTube Short, False otherwise
        '''

        if self.duration and self.duration > 60:
            return False

        if self.aspect_ratio < 1:
            return True

        # Check for Shorts-specific markers in initial data
        try:
            # Look for reelWatchEndpoint or shorts indicators
            renderer: str = initial_data.get(
                'contents', {}
            ).get(
                'twoColumnWatchNextResults', {}
            ).get(
                'secondaryResults', {}
            ).get(
                'reelShelfRenderer'
            )
            return bool(renderer)
        except (KeyError, TypeError):
            return False

    def _transition_state(self, ingest_status: IngestStatus | str) -> None:
        '''
        Transition the ingest state of the video

        :param ingest_status: the new ingest state
        '''

        if isinstance(ingest_status, str):
            ingest_status = IngestStatus(ingest_status)

        _LOGGER.debug(
            f'Video {self.video_id} transitioned to {ingest_status}',
        )
        self.ingest_status = ingest_status
