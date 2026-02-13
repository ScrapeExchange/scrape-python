'''
Model a Youtube channel

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import re
import logging

from uuid import UUID
from typing import Self
from shutil import rmtree
from random import random
from tempfile import mkdtemp
from logging import Logger
from logging import getLogger
from dataclasses import dataclass
from datetime import UTC, datetime

import orjson
import country_converter


from innertube import InnerTube

from yt_dlp import YoutubeDL


from .youtube_client import (
    AsyncYouTubeClient,
    CONSENT_COOKIES,
    HEADERS,
    USER_AGENT
)

from .youtube_video import YouTubeVideo
from .youtube_video import DENO_PATH
from .youtube_video import PO_TOKEN_URL
from .youtube_thumbnail import YouTubeThumbnail
from .youtube_external_link import YouTubeExternalLink

from ..util import split_quoted_string, convert_number_string


_LOGGER: Logger = getLogger(__name__)

# Limits the amount of videos imported for a channel
# per run
MAX_CHANNEL_VIDEOS_PER_RUN: int = 40

HTTP_PREFIX: str = 'http://'
HTTPS_PREFIX: str = 'https://'


@dataclass
class YouTubeChannelLink:
    channel_name: str
    subscriber_count: int

    def __hash__(self) -> int:
        return hash(self.channel_name)

    def to_dict(self) -> dict[str, str | int]:
        return {
            'channel_name': self.channel_name,
            'subscriber_count': self.subscriber_count
        }

    @staticmethod
    def from_dict(data: dict[str, str | int]) -> Self:
        return YouTubeChannelLink(
            name=data['name'],
            subscriber_count=data['subscriber_count']
        )


class YouTubeChannel:
    CHANNEL_URL: str = AsyncYouTubeClient.SCRAPE_URL + '/{channel_name}'
    CHANNEL_URL_WITH_AT: str = \
        AsyncYouTubeClient.SCRAPE_URL + '/@{channel_name}'

    CHANNEL_ID_REGEX: re.Pattern[str] = re.compile(r'"externalId":"(.*?)"')
    CHANNEL_SCRAPE_REGEX_SHORT: re.Pattern[str] = re.compile(
        r'var ytInitialData = (.*?);'
    )
    CHANNEL_SCRAPE_REGEX: re.Pattern[str] = re.compile(
        r'var ytInitialData = (.*?);$'
    )
    RX_SCRAPE_CHANNEL_ID: re.Pattern[str] = re.compile(
        r'"externalId":"(.*?)"'
    )

    def __init__(
        self, name: str = None,
        deno_path: str = DENO_PATH, po_token_url: str = PO_TOKEN_URL,
        debug: bool = False, save_dir: str = None,
        consent_cookies: dict[str, str] | None = CONSENT_COOKIES,
        user_agent: str = USER_AGENT, headers: dict[str, str] = HEADERS,
    ) -> None:
        '''
        Models a YouTube channel

        :param name: the name of the channel as it appears in the vanity URL,
        i.e., for https://www.youtube.com/@HistoryMatters, name is
        'HistoryMatters'
        :param channel_id: The YouTube channel ID, i.e. the last part of:
        https://www.youtube.com/channel/UC22BdTgxefuvUivrjesETjg
        :param ingest: whether to ingest the A/V streams of the scraped assets
        :param consent_cookies: cookies to use to bypass consent pages
        :param user_agent: User-Agent string to use for HTTP requests
        :param lock_file: path to lock file to prevent concurrent runs
        :param storage_driver: storage driver to use for persisting media
        '''

        self.consent_cookies: dict[str, str] = consent_cookies
        self.headers: dict[str, str] = headers
        self._work_dir: str = mkdtemp(dir='/tmp')
        self.save_dir: str | None = save_dir

        self.browse_client = AsyncYouTubeClient(
            user_agent=user_agent, headers=headers,
            consent_cookies=consent_cookies
        )
        self.download_client: YoutubeDL = YouTubeVideo._setup_download_client(
            browse_client=self.browse_client, deno_path=deno_path,
            po_token_url=po_token_url, debug=debug
        )

        self.url: str | None = None
        self.title: str | None = None
        if name:
            self.name = name.lstrip('@')
            self.url: str = YouTubeChannel.CHANNEL_URL_WITH_AT.format(
                channel_name=self.name.replace(' ', '')
            )

        self.channel_id: UUID | None = None
        self.description: str | None = None
        self.keywords: set[str] = set()
        self.categories: set[str] = set()
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
        self.video_count: int | None = None
        self.view_count: int | None = None

        self.channel_links: set[YouTubeChannelLink] = set()

        self.videos: dict[str, YouTubeVideo] = {}

    def __del__(self) -> None:
        rmtree(self._work_dir, ignore_errors=True)

    def __eq__(self, other: Self) -> bool:
        if not isinstance(other, YouTubeChannel):
            return False

        equal: bool = (
            self.name == other.name and
            self.channel_id == other.channel_id and
            self.title == other.title and
            self.description == other.description and
            self.joined_date == other.joined_date and
            self.rss_url == other.rss_url and
            self.verified == other.verified and
            self.is_family_safe == other.is_family_safe and
            self.video_count == other.video_count and
            self.view_count == other.view_count and
            self.subscriber_count == other.subscriber_count and
            self.external_urls == other.external_urls and
            self.categories == other.categories and
            self.country == other.country and
            self.keywords == other.keywords and
            self.banners == other.banners and
            self.available_country_codes == other.available_country_codes and
            self.channel_thumbnails == other.channel_thumbnails and
            self.external_urls == other.external_urls and
            self.banners == other.banners
        )
        return equal

    def to_dict(self) -> dict[str, any]:
        data: dict[str, any] = {
            'channel_id': self.channel_id,
            'channel': self.name.lstrip('@'),
            'title': self.title,
            'description': self.description,
            'keywords': list(self.keywords),
            'categories': list(self.categories),
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
            'joined_date': str(self.joined_date) if self.joined_date else None,
            'rss_url': self.rss_url,
            'verified': self.verified,
            'subscriber_count': self.subscriber_count or 0,
            'video_count': self.video_count or 0,
            'view_count': self.view_count or 0,
        }

        return data

    @staticmethod
    def from_dict(data: dict[str, any]) -> Self:
        channel = YouTubeChannel(name=data.get('channel'))
        channel.channel_id = data.get('channel_id')
        channel.title = data.get('title')
        channel.description = data.get('description')
        channel.keywords = set(data.get('keywords', []))
        channel.categories = set(data.get('categories', []))
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
        channel.rss_url = data.get('rss_url')
        channel.verified = data.get('verified', False)
        channel.subscriber_count = data.get('subscriber_count')
        channel.video_count = data.get('video_count')
        channel.view_count = data.get('view_count')

        return channel

    def _extract_initial_data(self, html_content: str) -> dict | None:
        '''
        Extract ytInitialData from the HTML page

        :param html_content: Raw HTML content from YouTube page
        :returns: Parsed ytInitialData dictionary or None if not found
        :raises: ValueError: If a consent page is detected (only when consent
        cookies are not set)
        '''

        # Verified badge is hard to find otherwise
        self.verified = YouTubeChannel.extract_verified_status(
            page_data=html_content
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

        return None

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
    def _extract_links(initial_data: dict) -> set[dict[str, str]]:
        '''
        Extract external links from the header dict of the channel 'about'
        data.
        '''

        links: set = set()

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
        for link in primary_links:
            url: str = link.get(
                'navigationEndpoint', {}
            ).get(
                'urlEndpoint', {}
            ).get('url')

            if url:
                links.add(
                    {
                        'title': link.get(
                            'title', {}
                        ).get(
                            'simpleText', 'Link'
                        ),
                        'url': url
                    }
                )

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

    async def scrape(self) -> None:
        '''
        Scrape the About tab for information. This does not include data
        about the videos for the channel as multiple requests are needed
        to get that data. Use get_videos_page() and parse_channel_video_data()
        to get the videos from the channel.

        :returns: dict of the scraped data
        :raises: (none)
        '''

        about_url: str = self.url.rstrip('/') + '/about'

        page_contents: str | None = await self.browse_client.get(about_url)

        self.channel_id = YouTubeChannel.extract_channel_id(page_contents)

        page_data: dict | None = self._extract_initial_data(page_contents)

        if not page_data:
            _LOGGER.warning('Could not extract data from page')
            return

        # This parses the channel metadata
        metadata: dict[str, any] = page_data.get(
            'metadata', {}
        ).get('channelMetadataRenderer', {})
        if metadata:
            self._parse_channel_about_metadata(metadata)

        about_data: dict[str, any] | None = self._find_about_renderer(
            page_data
        )
        if not about_data:
            _LOGGER.warning('Could not find about tab renderer')
            return

        self._parse_channel_about_data(about_data)

        self._parse_thumbnails_banners(metadata, page_data)

        self.channel_links = YouTubeChannel.extract_linked_channels(page_data)

    def _find_about_renderer(self, initial_data: dict) -> dict | None:
        '''
        Gets the channel data from the aboutChannelViewModel
        '''

        endpoints: list[dict[str, str]] = initial_data.get(
            'onResponseReceivedEndpoints', []
        )
        endpoint: dict[str, str]
        for endpoint in endpoints:
            section_list: list = YouTubeChannel.parse_nested_dicts(
                [
                    'showEngagementPanelEndpoint', 'engagementPanel',
                    'engagementPanelSectionListRenderer',
                    'content', 'sectionListRenderer', 'contents'
                ], endpoint, list
            )

            item: dict
            for item in section_list or []:
                item_section: dict[str, any] = item.get(
                    'itemSectionRenderer', {}
                ).get('contents', {})
                for content_item in item_section or []:
                    about_view_model: dict[str, dict[str, str]] | None = \
                        YouTubeChannel.parse_nested_dicts(
                            [
                                'aboutChannelRenderer', 'metadata',
                                'aboutChannelViewModel'
                            ], content_item, dict
                        )
                    if about_view_model:
                        return about_view_model

        _LOGGER.warning(
            'Could not find about tab renderer',
            extra={'channel': self.name, 'url': self.youtube_url}
        )
        return None

    def _extract_simple_text(self, text_obj: dict | str | None) -> str | None:
        '''
        Extracts simple text from a YouTube object

        :return: extracted text or None
        '''

        if isinstance(text_obj, str):
            return text_obj

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

        self.subscriber_count = self.subscriber_count or convert_number_string(
            self._extract_simple_text(
                about_renderer.get('subscriberCountText')
            )
        )

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
        self.country = country_converter.convert(
            self._extract_simple_text(
                about_renderer.get('country')
            ), to='ISO2', not_found=None
        )

    def parse_channel_video_data(self, page_html: str) -> None:
        '''
        Parses the info from the channel 'videos' page

        :param page_data: the text of the 'videos' page for the channel
        :returns: (none)
        '''

        log_data: dict[str, str] = {'channel': self.name}

        if not page_html:
            _LOGGER.warning(
                'No page data to parse from channel info', extra=log_data
            )
            return None

        self.channel_id = self.channel_id or \
            YouTubeChannel.extract_channel_id(page_html)

        page_data: dict[str, any] = self._extract_initial_data(page_html)
        if not page_data:
            _LOGGER.warning(
                'No parsed data found for channel', extra=log_data
            )
            return None

        self.channel_thumbnails = self.channel_thumbnails | \
            YouTubeChannel.parse_thumbnails(page_data)

        self._set_channel_video_thumbnail()

        self.banners: set[YouTubeThumbnail] = self.banners | \
            YouTubeChannel.parse_banners(page_data)

        self.subscriber_count = \
            YouTubeChannel.parse_subscriber_count(page_data)

        self.video_count: int | None = \
            YouTubeChannel.parse_video_count(page_data)

        # We can't get total views for the channel from the videos page,
        # we can get it from the about page though so no worries here

        channel_info: dict[str, any] | None = page_data.get(
            'metadata', {}
        ).get(
            'channelMetadataRenderer'
        )
        if not channel_info:
            _LOGGER.info(
                'No channel metadata found for channel', extra=log_data
            )
            raise ValueError('No channel metadata found')

        # We already get the channel name from the channel metadata but we
        # keep it here in case YouTube changes their metadata structure
        self.name: str = self.name or channel_info.get('title', '').lstrip('@')
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
            self.channel_thumbnail = sorted(self.channel_thumbnails)[0]

    def _parse_thumbnails_banners(self, metadata: dict[str, any],
                                  page_data: dict[str, any]) -> None:

        metadata_rows: dict | None = YouTubeChannel.parse_nested_dicts(
            [
                'header', 'pageHeaderRenderer', 'content',
                'pageHeaderViewModel'
            ], page_data, dict
        )

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

        if 'avatar' in metadata and 'thumbnails' in metadata['avatar']:
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
        banners: list[dict[str, str]] = metadata_rows.get(
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

                channel_name: str = channel_path.lstrip('/@')
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
                        channel_name=channel_name, subscriber_count=subs
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

        def parse_section_items(section_items: list) -> set[YouTubeChannelLink]:
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

    async def get_videos_page(self) -> str:
        '''
        Gets the videos page HTML content

        :returns: HTML content of the videos page
        '''

        videos_url: str = self.youtube_url.rstrip('/') + '/videos'

        log_extra: dict[str, str] = {'channel': self.name, 'url': videos_url}

        page_html: str | None = await self.browse_client.get(videos_url)

        if not page_html:
            _LOGGER.warning(
                'No page data found for channel videos page',
                extra=log_extra
            )
            return ''

        return page_html

    async def scrape_videos(self, ingest_interval: int = 0,
                            max_videos_per_channel: int = 0) -> int:
        '''
        Scrapes videos from the YouTube website
        
        :param ingest_interval: the interval in seconds between ingests
        :param max_videos_per_channel: the maximum number of videos to ingest
        :param already_ingested_videos: dictionary of ingested assets with
        YouTube video IDs as keys and as values a dict with ingest_status
        and published_timestamp
        :returns: number of pages scraped
        :raises: ByodaRuntimeError, ByodaValueError, ByodaException
        '''

        log_extra: dict[str, str] = {'channel': self.name}

        if not self.name:
            raise ValueError('No channel name provided')

        page_html: str = await self.get_videos_page()

        if not page_html:
            raise RuntimeError(f'No page data found for channel: {self.name}')

        self.parse_channel_video_data(page_html)

        self.video_ids: list[str] = []
        try:
            self.video_ids = await self.get_video_ids()
            if not self.video_ids:
                raise ValueError('No video IDs extracted')
        except Exception as exc:
            raise RuntimeError(f'Failed to extract video IDs: {exc}') from exc

        videos_imported: int = 0
        for video_id in self.video_ids:
            try:
                video: YouTubeVideo | None = await self.scrape_video(
                    video_id, video_table, self.ingest_videos,
                    self.channel_thumbnail
                )
                if not video:
                    continue
            except RuntimeError:
                continue
            except Exception as exc:
                raise Exception(
                    f'Failed to scrape video: {exc}'
                ) from exc

            log_extra['video_id'] = video.video_id
            log_extra['ingest_status'] = video.ingest_status.value

            if self.lock_file:
                self.update_lock_file()

            _LOGGER.debug('Persisting video', extra=log_extra)
            try:
                result: bool | None = await video.persist(
                    member,
                    ingest_asset=self.ingest_videos,
                    video_table=video_table,
                    bento4_directory=bento4_directory,
                    moderate_request_url=moderate_request_url,
                    moderate_jwt_header=moderate_jwt_header,
                    moderate_claim_url=moderate_claim_url,
                    custom_domain=custom_domain
                )

                if result is None:
                    log_extra['ingest_status'] = video.ingest_status.value
                    _LOGGER.debug(
                        'Failed to persist video', extra=log_extra
                    )

                videos_imported += 1
                if (max_videos_per_channel
                        and videos_imported >= max_videos_per_channel):
                    break
            except RuntimeError:
                pass
            except Exception:
                raise
            except Exception as exc:
                raise Exception(
                    'Failed to persist video', extra=log_extra,
                    loglevel=logging.INFO
                ) from exc

            if ingest_interval:
                random_delay: float = \
                    random() * ingest_interval + ingest_interval / 2
                _LOGGER.debug(
                    'Sleeping between ingesting assets for a channel',
                    extra=log_extra | {'seconds': random_delay}
                )
                await asyncio.sleep(random_delay)

            video = None

        _LOGGER.debug(
            f'Scraped {len(self.videos)} videos from YouTube channel',
            extra=log_extra
        )

        return videos_imported

    @staticmethod
    def extract_channel_id(page_data: str) -> str:
        '''
        Extracts the YouTube channel ID (ie. 'gxefuvUivrjesETjg') from the
        channel page data

        :param page_data: channel description
        :returns: the YouTube channel ID
        '''
        if not page_data:
            _LOGGER.warning('No page data to extract channel ID from')

        match: re.Match[str] | None = \
            YouTubeChannel.CHANNEL_ID_REGEX.search(page_data)

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
                f'Target:{target} Path:{path} Keys:{','.join(data.keys())}'
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
            _LOGGER.debug(f'In list with {len(data)} items')
            for item in data:
                result: any = YouTubeChannel.find_nested_dicts(
                    target, item, f'{path}[]'
                )
                if result:
                    return result

        return None

    @staticmethod
    def parse_video_count(data: dict) -> int | None:
        '''
        Parse the video count from the scraped data

        :param data: the scraped data
        :returns: the subscriber count
        '''

        metadata_rows: str | any = YouTubeChannel.parse_nested_dicts(
            [
                'header', 'pageHeaderRenderer', 'content',
                'pageHeaderViewModel', 'metadata',
                'contentMetadataViewModel', 'metadataRows',
            ], data, list
        )

        for metadata_row in metadata_rows or []:
            metadata_parts: list[dict] = metadata_row.get(
                'metadataParts', []
            ) or []
            for metadata_part in metadata_parts:
                if isinstance(metadata_part.get('text'), dict):
                    content: str = metadata_part['text'].get('content', '')
                    if content and 'videos' in content:
                        youtube_video_count: int | None = \
                            convert_number_string(content)
                        return youtube_video_count

        _LOGGER.debug('Failed to parse videos count')
        return None

    @staticmethod
    def parse_subscriber_count(data: dict) -> int | None:
        '''
        Parse the subscriber count from the scraped data

        :param data: the scraped data
        :returns: the subscriber count
        '''

        metadata_rows: str | any = YouTubeChannel.parse_nested_dicts(
            [
                'header', 'pageHeaderRenderer', 'content',
                'pageHeaderViewModel', 'metadata',
                'contentMetadataViewModel', 'metadataRows',
            ], data, list
        )

        for metadata_row in metadata_rows or []:
            metadata_parts: list[dict] = metadata_row.get(
                'metadataParts', []
            ) or []
            for metadata_part in metadata_parts:
                if isinstance(metadata_part.get('text'), dict):
                    content: str = metadata_part['text'].get('content', '')
                    if content and 'subscribers' in content:
                        youtube_subs_count: int | None = \
                            convert_number_string(content)
                        return youtube_subs_count

        _LOGGER.debug('Failed to parse subscriber count')
        return None

    @staticmethod
    def parse_view_count(data: dict) -> int | None:
        '''
        Parse the total views count for the channel from the scraped data

        :param data: the scraped data
        :returns: the subscriber count
        '''

        try:
            views_data: dict | any = YouTubeChannel.parse_nested_dicts(
                [
                    'header', 'c4TabbedHeaderRenderer', 'viewCountText',
                    'simpleText'
                ], data, list
            )
            if not views_data:
                return None

            view_count: int | None = convert_number_string(views_data)

            return view_count
        except Exception as exc:
            _LOGGER.debug(f'Failed to parse views count: {exc}')
            return None

    @staticmethod
    def parse_thumbnails(data: dict[str, any]) -> set[YouTubeThumbnail]:
        '''
        Parses the thumbnails out of the YouTube channel page either
        scraped or retrieved using the InnerTube API
        '''

        thumbnails_data: list | None = YouTubeChannel.parse_nested_dicts(
            [
                'header', 'pageHeaderRenderer', 'content',
                'pageHeaderViewModel', 'image', 'decoratedAvatarViewModel',
                'avatar', 'avatarViewModel', 'image', 'sources'
            ], data, list
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
                        f'TODO: call YT API to get the additional links: {url}'
                    )
                    return None
                else:
                    _LOGGER.debug(
                        f'Could not parse link name our of URL: {url}'
                    )
                    name = url

            title = SocialNetworks.get(name.lower(), 'www')
            _LOGGER.debug(f'Parsed external link label {name} ouf of {url}')

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
            banner_data: dict[str, str | int] | None = \
                YouTubeChannel.parse_nested_dicts(
                    [
                        'header', 'pageHeaderRenderer', 'content',
                        'pageHeaderViewModel', banner_type,
                        'imageBannerViewModel', 'image', 'sources',
                    ], data, list
                )

            thumbnail_data: dict[str, str | int]
            for thumbnail_data in banner_data or []:
                channel_banner: YouTubeThumbnail = YouTubeThumbnail(
                    thumbnail_data, display_hint=banner_type
                )
                _LOGGER.debug(f'Found banner: {channel_banner.url}')
                banners.add(channel_banner)

        return banners

    @staticmethod
    def parse_nested_dicts(keys: list[str], data: dict[str, any],
                           final_type: callable
                           ) -> object | list[object] | None:
        for key in keys:
            if key in data:
                data = data[key]
            else:
                return None

        if not isinstance(data, final_type):
            _LOGGER.debug(
                f'Expected value of {final_type} but got {type(data)}: {data}'
            )
            return None

        return data

    async def get_video_ids(self) -> list[str]:
        # Client for YouTube (Web)
        client = InnerTube('WEB', '2.20230728.00.00')

        video_ids: list[str] = []

        first_run: bool = True
        continuation_token: str = ''
        while first_run or continuation_token:
            # If this is the first video listing, browse the 'Videos' page
            if not continuation_token:
                first_run = False
                # Fetch the browse data for the channel
                channel_data: dict = client.browse(self.youtube_channel_id)

                # Extract the tab renderer for the 'Videos' tab of the channel
                tabs: list = YouTubeChannel.parse_nested_dicts(
                    ['contents', 'twoColumnBrowseResultsRenderer', 'tabs'],
                    channel_data, list
                )
                if not tabs or len(tabs) < 2 or 'tabRenderer' not in tabs[1]:
                    _LOGGER.warning('Scraped video does not have 2 tabs')
                    return []

                videos_tab_renderer: dict = tabs[1]['tabRenderer']

                # Make sure this tab is the 'Videos' tab
                if videos_tab_renderer['title'] != 'Videos':
                    _LOGGER.warning(
                        'Scraped channel does not have a "Videos" tab'
                    )
                    return []

                # Extract the browse params for the 'Videos' tab of the channel
                videos_params: str = \
                    videos_tab_renderer['endpoint']['browseEndpoint']['params']

                # Wait a bit so that Google doesn't suspect us of being a bot
                await AsyncYouTubeClient._delay()

                # Fetch the browse data for the channel's videos
                videos_data: dict = client.browse(
                    self.youtube_channel_id, params=videos_params
                )

                # Extract the contents list
                tabs = YouTubeChannel.parse_nested_dicts(
                    ['contents', 'twoColumnBrowseResultsRenderer', 'tabs'],
                    videos_data, list
                )
                contents: list = YouTubeChannel.parse_nested_dicts(
                    ['tabRenderer', 'content', 'richGridRenderer', 'contents'],
                    tabs[1], list
                )
            else:
                # Fetch more videos by using the continuation token
                continued_videos_data: dict = client.browse(
                    continuation=continuation_token
                )
                # Wait a bit so that Google doesn't suspect us of being a bot
                await AsyncYouTubeClient._delay()

                contents: list = YouTubeChannel.parse_nested_dicts(
                    ['appendContinuationItemsAction', 'continuationItems'],
                    continued_videos_data['onResponseReceivedActions'][0],
                    list
                )

            # Extract the rich video items and the continuation item
            *rich_items, continuation_item = contents

            # Loop through each video and log out its details
            for rich_item in rich_items:
                video_renderer: dict | None = \
                    YouTubeChannel.parse_nested_dicts(
                        ['richItemRenderer', 'content', 'videoRenderer'],
                        rich_item, dict
                    )

                video_id = video_renderer.get('videoId')
                if video_id:
                    video_ids.append(video_id)

            cont_renderer = continuation_item.get('continuationItemRenderer')
            if not cont_renderer:
                return video_ids

            # Extract the continuation token
            item_data: dict | None = YouTubeChannel.parse_nested_dicts(
                [
                    'continuationItemRenderer',
                    'continuationEndpoint',
                    'continuationCommand'
                ], continuation_item, dict
            )
            continuation_token = item_data['token']

        return video_ids

    async def scrape_video(
        self, video_id: str, table: Table,
        ingest_videos: bool, channel_thumbnail: YouTubeThumbnail | None
    ) -> YouTubeVideo | None:
        '''
        Find the videos in the by walking through the deserialized
        output of a scrape of a YouTube channel

        :param video_id: YouTube video ID
        :param video_table: Table to see if video has already been ingested
        where to store newly ingested videos
        :param ingest_videos: whether to upload the A/V streams of the
        scraped assets to storage
        :returns: the scraped YouTubeVideo or None if the video should not be
        ingested
        :raises: ByodaRuntimeError: if scraping the video fails
        '''

        log_data: dict[str, str] = {
            'channel': self.name, 'video_id': video_id
        }
        _LOGGER.debug('Processing video', extra=log_data)

        # We scrape if either:
        # 1: We haven't processed the video before
        # 2: We have already ingested the asset with ingest_status
        # 'external' and we now want to ingest the AV streams for the
        # channel
        status = IngestStatus.NONE

        data_filter: DataFilterSet = DataFilterSet(
            {'publisher_asset_id': {'eq': video_id}}
        )
        result: list[QueryResult] | None = await table.query(data_filter)
        if result and isinstance(result, list) and len(result):
            video_data, _ = result[0]
            try:
                status: IngestStatus | None = \
                    video_data.get('ingest_status')

                if isinstance(status, str):
                    status = IngestStatus(status)
            except ValueError:
                status = IngestStatus.NONE

            if not ingest_videos and status == IngestStatus.EXTERNAL:
                _LOGGER.debug(
                    'Skipping video as it is already ingested and we are '
                    'not importing AV streams', extra=log_data
                )
                return
            elif status == IngestStatus.PUBLISHED:
                _LOGGER.debug(
                    'Skipping video that we already ingested earlier in this '
                    'run', extra=log_data
                )
                return

            _LOGGER.debug(
                f'Ingesting AV streams video with ingest status {status}',
                extra=log_data
            )
        else:
            if ingest_videos:
                status = IngestStatus.NONE

        video: YouTubeVideo = await YouTubeVideo.scrape(
            video_id, ingest_videos, self.name, channel_thumbnail,
            browse_client=self.browse_client,
            download_client=self.download_client,
            storage_driver=self.storage_driver,
        )

        if not video:
            # This can happen if we decide not to import the video
            return

        if video.ingest_status != IngestStatus.UNAVAILABLE:
            # Video IDs may appear multiple times in scraped data
            # so we set the ingest status for the class instance
            # AND for the dict of already ingested videos
            video._transition_state(IngestStatus.QUEUED_START)

        return video

    @staticmethod
    async def get_channel(title: str) -> Self:
        '''
        Gets the channel ID using the YouTube innertube API
        '''

        channel = YouTubeChannel(title=title)

        return channel
