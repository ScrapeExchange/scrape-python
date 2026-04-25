#!/usr/bin/env python3

'''
Discover new YouTube channels via BFS from a set of seed pages.

Reads known channels from channels.lst, scrapes the YOUTUBE_URLS seeds,
and recursively scrapes every new channel it finds. Persists discovered
channels (with subscriber counts) and failed scrapes to append-only
JSONL files so a crashed run can be resumed.

Depends on scrape_exchange.youtube.youtube_client.AsyncYouTubeClient from
the sibling repo ../scrape-python. Either `pip install -e ../scrape-python`
or put that repo on PYTHONPATH before running this tool.

:maintainer : Steven Hessing <steven@byoda.org>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import re
import sys
import asyncio
import logging

from typing import Self
from random import shuffle
from collections import deque

import orjson
import aiofiles

from bs4 import BeautifulSoup
from pydantic import AliasChoices, Field

from scrape_exchange.creator_map import (
    CreatorMap,
    FileCreatorMap,
    RedisCreatorMap,
)
from scrape_exchange.logging import configure_logging
from scrape_exchange.settings import ScraperSettings
from scrape_exchange.youtube.youtube_rate_limiter import (
    YouTubeRateLimiter,
)

from scrape_exchange.youtube.youtube_channel import (
    YouTubeChannel as ScrapeYouTubeChannel,
)
from scrape_exchange.youtube.youtube_client import AsyncYouTubeClient

# The underlying Redis hash name when the Redis backend is
# active. Kept here for reference; RedisCreatorMap derives
# the same key internally from ``platform='youtube'``.
REDIS_CHANNEL_MAP_KEY: str = 'youtube:creator_map'

_LOGGER: logging.Logger = logging.getLogger(__name__)

CHANNEL_PREFIX: str = 'channel/'

# Matches bare YouTube channel ids ("UC" + 22 id chars).
# Used to avoid writing UC-id → UC-id rows into the
# creator_map hash when a scrape target was given as a
# channel/UC... URL rather than as a handle.
_UC_ID_RE: re.Pattern = re.compile(
    r'^UC[A-Za-z0-9_-]{22}$',
)

CHANNEL_RX: re.Pattern = re.compile(r'\"canonicalBaseUrl\":\"\/@(.*?)\"')
CHANNEL_SCRAPE_REGEX_SHORT: re.Pattern[str] = re.compile(
    r'var ytInitialData = (.*?);'
)
CHANNEL_SCRAPE_REGEX: re.Pattern[str] = re.compile(
    r'var ytInitialData = (.*?);$'
)

YOUTUBE_URLS: list[str] = [
    'https://www.youtube.com/feed/trending',
    'https://www.youtube.com/channel/UCkYQyvc_i9hXEo4xic9Hh2g',  # shopping
    'https://www.youtube.com/gaming',
    'https://www.youtube.com/podcasts',
    'https://www.youtube.com/channel/UCrpQ4p1Ql_hG8rKXIKM1MOQ',  # Fashion
    'https://www.youtube.com/feed/guide_builder',
]

IGNORE_CHANNELS: set[str] = set(
    ['news', 'live', 'youtube', '360', 'gaming', 'music', 'movies', 'sports']
)


def parse_nested_dicts(keys: list[str], data: dict[str, any],
                       final_type: callable) -> object | None:
    for key in keys:
        if key in data:
            data = data.get(key)
        else:
            return None

    if not isinstance(data, final_type):
        _LOGGER.debug(
            'Unexpected data type',
            extra={
                'expected_type': repr(final_type),
                'actual_type': repr(type(data)),
                'data': repr(data),
            },
        )
        return None

    return data


def convert_number_string(number_text: str | int) -> int | None:
    '''Convert a number with optional k/m/b suffix to an integer.'''

    if not number_text or isinstance(number_text, int):
        return number_text

    words: list[str] = number_text.split(' ')
    number_text = words[0].strip()

    try:
        multiplier: str = number_text[-1].upper()
        if not multiplier.isnumeric():
            multipliers: dict[str, int] = {
                'K': 1000,
                'M': 1000000,
                'B': 1000000000,
            }
            count_pre: float = float(number_text[:-1])
            count = int(count_pre * multipliers[multiplier])
        else:
            count = int(number_text)
        return count
    except Exception as exc:
        _LOGGER.debug(
            'Could not convert text to a number',
            exc=exc,
            extra={'number_text': number_text},
        )
        return None


def parse_channel_id(data: dict) -> str | None:
    '''Parse the channel ID (UC...) from ytInitialData.'''

    channel_id: str | None = parse_nested_dicts(
        [
            'metadata', 'channelMetadataRenderer',
            'externalId',
        ], data, str,
    )
    return channel_id


def parse_subscriber_count(data: dict) -> int | None:
    '''Parse the subscriber count from the scraped ytInitialData.'''

    paths: list[list[str]] = [
        [
            'header', 'pageHeaderRenderer', 'content',
            'pageHeaderViewModel', 'metadata', 'contentMetadataViewModel',
            'metadataRows'
        ],
        [
            'header', 'pageHeaderRenderer', 'metadata',
            'contentMetadataViewModel', 'metadataRows',
        ],
    ]

    metadata_rows = None
    for path in paths:
        metadata_rows = parse_nested_dicts(path, data, list)
        if metadata_rows:
            break

    if not metadata_rows or not isinstance(metadata_rows, list):
        return None

    for metadata_row in metadata_rows:
        if 'metadataParts' not in metadata_row:
            continue
        for metadata_part in metadata_row['metadataParts']:
            metadatapart_text: dict[str, any] = metadata_part.get('text')
            if not metadatapart_text:
                continue
            subs_text = metadata_part['text'].get('content')
            if not subs_text or subs_text.startswith('@'):
                continue
            youtube_subs_count: int | None = convert_number_string(subs_text)
            if youtube_subs_count is not None:
                return youtube_subs_count

    return None


def parse_page_text(channel: str, page_text: str) -> dict[str, any]:
    '''Extract the ytInitialData JSON blob from a YouTube HTML page.'''

    soup = BeautifulSoup(page_text, 'html.parser')
    script = soup.find('script', string=CHANNEL_SCRAPE_REGEX_SHORT)
    if not script:
        _LOGGER.debug(
            'Did not find ytInitialData script for channel',
            extra={'channel': channel},
        )
        soup.decompose()
        return {}

    raw_data: str = CHANNEL_SCRAPE_REGEX.search(script.text).group(1)
    soup.decompose()
    soup = None
    script = None

    try:
        parsed_data: dict[str, any] = orjson.loads(raw_data)
    except orjson.JSONDecodeError as exc:
        _LOGGER.debug(
            'Failed parsing JSON data for channel',
            exc=exc,
            extra={'channel': channel},
        )
        return {}

    return parsed_data


def _collect_video_authors(node: any,
                           page_links: set[tuple[str, int | None]]) -> None:
    '''
    Recursively walk a ytInitialData tree. For every `videoRenderer`
    node found, add the author's channel (handle when available, else
    UC-id path) to `page_links` with subs=None.
    '''

    if isinstance(node, dict):
        vr: dict | None = node.get('videoRenderer')
        if isinstance(vr, dict):
            for field in ('ownerText', 'longBylineText', 'shortBylineText'):
                runs = (vr.get(field) or {}).get('runs')
                if not isinstance(runs, list):
                    continue
                for run in runs:
                    canonical: str | None = parse_nested_dicts(
                        ['navigationEndpoint', 'browseEndpoint',
                         'canonicalBaseUrl'],
                        run if isinstance(run, dict) else {}, str,
                    )
                    name: str | None = None
                    if canonical:
                        name = canonical.lstrip('/@')
                    else:
                        browse_id: str | None = parse_nested_dicts(
                            ['navigationEndpoint', 'browseEndpoint',
                             'browseId'],
                            run if isinstance(run, dict) else {}, str,
                        )
                        if browse_id and browse_id.startswith('UC'):
                            name = f'channel/{browse_id}'
                    if name:
                        page_links.add((name, None))
                        break
                else:
                    continue
                break
        for value in node.values():
            _collect_video_authors(value, page_links)
    elif isinstance(node, list):
        for item in node:
            _collect_video_authors(item, page_links)


class DiscoveredChannel:
    def __init__(
        self, channel_handle: str,
        page_links: set[tuple[str, int | None]],
        description: str | None,
        subs: int | None,
        channel_id: str | None,
    ) -> None:
        self.channel_handle: str = channel_handle
        self.page_links: set[tuple[str, int | None]] = (
            page_links
        )
        self.description: str | None = description
        self.subs: int | None = subs
        self.channel_id: str | None = channel_id

    @staticmethod
    def parse_channel_page(channel_handle: str,
                           page_data: dict[str, any]) -> Self:
        page_links: set[tuple[str, int | None]] = set()

        channel_id: str | None = parse_channel_id(page_data)
        subs_count: int | None = parse_subscriber_count(page_data)

        description: str | None = parse_nested_dicts(
            [
                'header', 'pageHeaderRenderer', 'content',
                'pageHeaderViewModel', 'description',
                'descriptionPreviewViewModel', 'description',
                'content'
            ], page_data, str
        )

        tabs: list[dict[str, any]] = parse_nested_dicts(
            ['contents', 'twoColumnBrowseResultsRenderer', 'tabs'],
            page_data, list
        )
        for tab in tabs or []:
            section_items: list[dict[str, any]] = parse_nested_dicts(
                ['tabRenderer', 'content', 'sectionListRenderer', 'contents'],
                tab, list
            )
            for section_item in section_items or []:
                item: dict[str, any] | None = section_item.get(
                    'itemSectionRenderer'
                )
                if not item:
                    continue

                section_item_contents: list[dict[str, any]] = item.get(
                    'contents'
                )
                if (not section_item_contents
                        or not isinstance(section_item_contents, list)):
                    continue

                for section_item_content in section_item_contents:
                    list_items: list[dict[str, any]] = parse_nested_dicts(
                        [
                            'shelfRenderer', 'content',
                            'horizontalListRenderer', 'items'
                        ], section_item_content, list
                    )
                    if not list_items:
                        list_items = parse_nested_dicts(
                            [
                                'shelfRenderer', 'content',
                                'gridRenderer', 'items'
                            ], section_item_content, list
                        )
                    if not list_items:
                        continue

                    for list_item in list_items:
                        channel_renderer: dict[str, any] = list_item.get(
                            'gridChannelRenderer'
                        )
                        if not channel_renderer:
                            continue

                        url: str | None = parse_nested_dicts(
                            [
                                'navigationEndpoint', 'commandMetadata',
                                'webCommandMetadata', 'url'
                            ], channel_renderer, str
                        )
                        if not url:
                            continue
                        url = url.lstrip('/@')

                        subs: int | None = None
                        subs_text: str | None = parse_nested_dicts(
                            ['subscriberCountText', 'simpleText'],
                            channel_renderer, str
                        )
                        if subs_text:
                            subs = convert_number_string(subs_text)
                        page_links.add((url, subs))

        _collect_video_authors(page_data, page_links)

        return DiscoveredChannel(
            channel_handle, page_links, description,
            subs_count, channel_id,
        )


def load_known_channels(filepath: str) -> set[str]:
    '''
    Read the known-channels list and return a set of normalized handles.

    File format: one channel per line. Lines may optionally start with '@'
    or 'https://www.youtube.com/'. Blank lines and '#' comments skipped.
    Tab-separated lines: use the second column if present, otherwise first.
    '''

    url_prefix: str = 'https://www.youtube.com/'
    known: set[str] = set()
    try:
        with open(filepath, 'r') as file_in:
            for line in file_in.readlines():
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if (line.startswith('{') and line.endswith('}')
                        and 'channel' in line):
                    try:
                        data: dict[str, str | int] = orjson.loads(line)
                        channel = data.get('channel', line)
                    except orjson.JSONDecodeError:
                        channel = line
                words: list[str] = line.split('\t')
                if not words:
                    continue
                channel: str
                if len(words) == 1:
                    channel = words[0].strip().lstrip('@')
                else:
                    channel = words[1].strip().lstrip('@')
                channel = channel.strip().lstrip('@').split(',')[0]
                if channel.startswith(url_prefix):
                    channel = channel[len(url_prefix):]
                channel = channel.lstrip('@')
                if '@' in channel:
                    _LOGGER.warning(
                        'Channel has @ mid-string, skipping',
                        extra={'channel': channel},
                    )
                    continue
                channel = channel.strip()
                if channel:
                    known.add(channel.lower())
    except FileNotFoundError:
        _LOGGER.warning(
            'Channel list not found, starting empty',
            extra={'filepath': filepath},
        )
    _LOGGER.info(
        'Loaded known channels',
        extra={'count': len(known), 'filepath': filepath},
    )
    return known


def load_discovered(filepath: str
                    ) -> tuple[dict[str, int | None], set[str]]:
    '''
    Load discovered.jsonl into (discovered_map, fully_scraped_set).

    Later records overwrite earlier ones. Channels with source='scrape'
    are added to the fully_scraped set so the BFS skips rescraping them.
    '''

    discovered: dict[str, int | None] = {}
    fully_scraped: set[str] = set()
    try:
        with open(filepath, 'r') as file_in:
            for lineno, line in enumerate(file_in, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    record: dict = orjson.loads(line)
                except orjson.JSONDecodeError as exc:
                    _LOGGER.warning(
                        'Skipping malformed JSON',
                        exc=exc,
                        extra={'filepath': filepath, 'lineno': lineno},
                    )
                    continue
                channel = record.get('channel')
                if not channel:
                    _LOGGER.warning(
                        'Missing channel field',
                        extra={'filepath': filepath, 'lineno': lineno},
                    )
                    continue
                discovered[channel] = record.get('subs')
                if record.get('source') == 'scrape':
                    fully_scraped.add(channel)
    except FileNotFoundError:
        _LOGGER.info(
            'Discovered-records file does not exist, starting fresh',
            extra={'filepath': filepath},
        )
    _LOGGER.info(
        'Resumed discovered records',
        extra={
            'discovered_count': len(discovered),
            'fully_scraped_count': len(fully_scraped),
            'filepath': filepath,
        },
    )
    return discovered, fully_scraped


def load_failed(filepath: str) -> dict[str, dict]:
    '''Load failed.jsonl into a channel -> record dict.'''

    failed: dict[str, dict] = {}
    try:
        with open(filepath, 'r') as file_in:
            for lineno, line in enumerate(file_in, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    record: dict = orjson.loads(line)
                except orjson.JSONDecodeError as exc:
                    _LOGGER.warning(
                        'Skipping malformed JSON',
                        exc=exc,
                        extra={'filepath': filepath, 'lineno': lineno},
                    )
                    continue
                channel = record.get('channel')
                if not channel:
                    continue
                failed[channel] = record
    except FileNotFoundError:
        _LOGGER.info(
            'Failed-records file does not exist, starting fresh',
            extra={'filepath': filepath},
        )
    _LOGGER.info(
        'Resumed failed records',
        extra={'count': len(failed), 'filepath': filepath},
    )
    return failed


def _handle_from_target(target: str) -> str | None:
    '''
    Extract the plain handle from a scrape target. Returns
    ``None`` when the target is a bare UC-id or an empty
    string, since those are not handles and should not be
    written to the creator_map.
    '''
    handle: str = target
    if handle.startswith(CHANNEL_PREFIX):
        handle = handle[len(CHANNEL_PREFIX):]
    handle = handle.lstrip('@')
    if not handle or _UC_ID_RE.match(handle):
        return None
    return handle


async def update_creator_map(
    creator_map: CreatorMap | None,
    channel: str,
    channel_id: str | None,
) -> None:
    '''
    Record ``channel_id → handle`` in the shared
    creator_map so that downstream scrapers can resolve
    UC-ids to handles without re-scraping the channel
    page. Backend-agnostic — works against both
    :class:`FileCreatorMap` and :class:`RedisCreatorMap`.

    Silent no-op when no creator_map is configured, the
    channel_id is missing, or *channel* does not resolve
    to a handle (e.g. it was a ``channel/UC...`` URL form).
    Backend errors are logged as a warning; they never
    propagate so a transient storage outage cannot derail
    the discovery BFS.
    '''
    if creator_map is None or not channel_id:
        return
    handle: str | None = _handle_from_target(channel)
    if handle is None:
        return
    try:
        await creator_map.put(channel_id, handle)
    except Exception as exc:
        _LOGGER.warning(
            'Failed to update creator_map',
            exc=exc,
            extra={
                'channel_id': channel_id,
                'handle': handle,
            },
        )


async def record_discovered(
    channel: str, subs: int | None, source: str,
    discovered: dict[str, int | None],
    out_fd,
    channel_id: str | None = None,
    creator_map: CreatorMap | None = None,
) -> None:
    '''Update in-memory dict and append one JSONL record to out_fd.

    When both *channel_id* and *creator_map* are supplied, the
    shared creator_map (file- or Redis-backed) is also updated
    via :func:`update_creator_map`.
    '''

    discovered[channel] = subs
    payload: dict = {
        'channel': channel, 'subs': subs, 'source': source,
    }
    if channel_id:
        payload['channel_id'] = channel_id
    try:
        await out_fd.write(orjson.dumps(payload).decode('utf-8') + '\n')
        await out_fd.flush()
    except OSError as exc:
        _LOGGER.warning(
            'Failed to persist discovered record',
            exc=exc,
            extra={'channel': channel},
        )
    await update_creator_map(
        creator_map, channel, channel_id,
    )


async def record_failed(channel: str, info: dict,
                        failed: dict[str, dict], out_fd) -> None:
    '''Update in-memory dict and append one JSONL record to out_fd.'''

    payload: dict = {'channel': channel}
    payload.update(info)
    failed[channel] = payload
    try:
        await out_fd.write(orjson.dumps(payload).decode('utf-8') + '\n')
        await out_fd.flush()
    except OSError as exc:
        _LOGGER.warning(
            'Failed to persist failed record',
            exc=exc,
            extra={'channel': channel},
        )


def _is_channel_target(target: str) -> bool:
    '''Return True if *target* is a channel handle or
    ``channel/UC...`` path, not a feed/full URL.'''

    if target.startswith(('http://', 'https://')):
        return False
    return True


async def _scrape_channel(
    target: str,
    proxies: list[str] | None,
) -> DiscoveredChannel:
    '''Scrape a channel page using
    :class:`ScrapeYouTubeChannel` and return a
    :class:`DiscoveredChannel`.'''

    name: str = target
    if target.startswith(CHANNEL_PREFIX):
        name = target[len(CHANNEL_PREFIX):]

    yt_channel: ScrapeYouTubeChannel = (
        ScrapeYouTubeChannel(
            name, with_download_client=False,
        )
    )
    await yt_channel.scrape_about_page(proxies=proxies)

    page_links: set[tuple[str, int | None]] = set()
    for link in yt_channel.channel_links:
        page_links.add((
            link.channel_handle,
            link.subscriber_count,
        ))

    return DiscoveredChannel(
        channel_handle=target,
        page_links=page_links,
        description=yt_channel.description,
        subs=yt_channel.subscriber_count,
        channel_id=yt_channel.channel_id,
    )


async def fetch(client: AsyncYouTubeClient, url: str
                ) -> tuple[str | None, dict | None]:
    '''
    Wrap client.get. Returns (page_text, None) on success, or
    (None, error_info) with a classified failure reason.
    '''

    try:
        text: str | None = await client.get(url)
    except ValueError:
        return None, {'reason': 'not_found', 'status_code': 404}
    except RuntimeError:
        return None, {'reason': 'retries_exhausted', 'status_code': None}
    except Exception as exc:
        return None, {
            'reason': 'exception',
            'status_code': None,
            'exception': repr(exc),
        }
    if text is None:
        return None, {'reason': 'http_non_200', 'status_code': None}
    return text, None


async def resolve_channel_paths(
        creator_map: CreatorMap | None,
        page_links: set[tuple[str, int | None]],
        ) -> set[tuple[str, int | None]]:
    '''
    Replace ``channel/UC...`` entries in *page_links* with their
    handle form by looking up the UC-id in the shared creator_map
    (file- or Redis-backed). Entries whose UC-id is not present in
    the map, and anything other than a ``channel/UC...`` path, are
    returned unchanged.

    Backend errors are logged once and the original *page_links*
    is returned unchanged — a transient outage must not break the
    discovery BFS.
    '''

    uc_ids: list[str] = []
    for name, _ in page_links:
        if name.startswith(CHANNEL_PREFIX):
            uc_id: str = name.removeprefix(CHANNEL_PREFIX)
            if uc_id.startswith('UC'):
                uc_ids.append(uc_id)
    if not uc_ids or creator_map is None:
        return page_links

    resolved_map: dict[str, str] = {}
    try:
        for uc_id in uc_ids:
            handle: str | None = await creator_map.get(uc_id)
            if handle:
                resolved_map[uc_id] = handle.lstrip('@')
    except Exception as exc:
        _LOGGER.warning(
            'creator_map lookup failed, skipping resolution',
            exc=exc,
            extra={'uc_ids_count': len(uc_ids)},
        )
        return page_links

    resolved: set[tuple[str, int | None]] = set()
    for name, subs in page_links:
        if name.startswith(CHANNEL_PREFIX):
            uc_id = name.removeprefix(CHANNEL_PREFIX)
            mapped: str | None = resolved_map.get(uc_id)
            resolved.add((mapped, subs) if mapped else (name, subs))
        else:
            resolved.add((name, subs))
    return resolved


async def _scrape_feed_page(
    client: AsyncYouTubeClient,
    creator_map: CreatorMap | None,
    target: str,
) -> DiscoveredChannel:
    '''Scrape a non-channel URL (feed, trending, etc.)
    and return a :class:`DiscoveredChannel`.

    :raises ValueError: on HTTP or parse failure.
    '''

    page_text, err = await fetch(client, target)
    if err is not None:
        raise ValueError(
            f'fetch failed: {err}'
        )

    page_data: dict = parse_page_text(
        target, page_text,
    )
    if not page_data:
        raise ValueError('parse_error')

    channel: DiscoveredChannel = (
        DiscoveredChannel.parse_channel_page(
            target, page_data,
        )
    )
    channel.page_links = await resolve_channel_paths(
        creator_map, channel.page_links,
    )
    return channel


async def _scrape_target(
    client: AsyncYouTubeClient,
    creator_map: CreatorMap | None,
    target: str,
    proxies: list[str] | None,
) -> DiscoveredChannel:
    '''Dispatch *target* to the right scraper.

    :raises ValueError, RuntimeError: on failure.
    '''

    if _is_channel_target(target):
        return await _scrape_channel(target, proxies)

    return await _scrape_feed_page(
        client, creator_map, target,
    )


async def _enqueue_links(
    channel: DiscoveredChannel,
    min_subs: int,
    known_channels: set[str],
    discovered: dict[str, int | None],
    fully_scraped: set[str],
    failed: dict[str, dict],
    in_queue: set[str],
    to_scrape: deque[str],
    discovered_fd,
) -> None:
    '''Record discovered links and enqueue new ones.'''

    for link_name, link_subs in channel.page_links:
        if not link_name:
            continue
        if link_name.lower() in known_channels:
            continue
        if link_name.lower() in IGNORE_CHANNELS:
            continue
        if link_subs is not None and link_subs < min_subs:
            continue
        await record_discovered(
            link_name, link_subs, 'link',
            discovered, discovered_fd,
        )
        if (link_name not in fully_scraped
                and link_name not in failed
                and link_name not in in_queue):
            to_scrape.append(link_name)
            in_queue.add(link_name)


def _build_initial_queue(
    discovered: dict[str, int | None],
    fully_scraped: set[str],
    failed: dict[str, dict],
    known_channels: set[str],
) -> deque[str]:
    '''Seed the BFS queue with YOUTUBE_URLS and
    orphan channels from a previous run.'''

    to_scrape: deque[str] = deque()
    seeds: list[str] = list(YOUTUBE_URLS)
    shuffle(seeds)
    to_scrape.extend(seeds)

    orphans: set[str] = (
        set(discovered.keys())
        - fully_scraped
        - set(failed.keys())
        - known_channels
        - set(seeds)
    )
    to_scrape.extend(orphans)
    if orphans:
        _LOGGER.info(
            'Re-enqueued orphan link-only channels from resume',
            extra={'orphan_count': len(orphans)},
        )

    return to_scrape


async def discover(client: AsyncYouTubeClient,
                   creator_map: CreatorMap | None,
                   known_channels: set[str],
                   discovered: dict[str, int | None],
                   fully_scraped: set[str],
                   failed: dict[str, dict],
                   discovered_fd,
                   failed_fd,
                   min_subs: int,
                   proxies: list[str] | None = None,
                   ) -> None:
    '''Drive the serial BFS until the queue is empty.'''

    to_scrape: deque[str] = _build_initial_queue(
        discovered, fully_scraped, failed,
        known_channels,
    )
    in_queue: set[str] = set(to_scrape)

    while to_scrape:
        target: str = to_scrape.popleft()
        in_queue.discard(target)

        _LOGGER.info(
            'Scraping target',
            extra={
                'target': target,
                'queue': len(to_scrape),
                'discovered': len(discovered),
                'fully_scraped': len(fully_scraped),
                'failed': len(failed),
            },
        )

        try:
            channel: DiscoveredChannel = (
                await _scrape_target(
                    client, creator_map,
                    target, proxies,
                )
            )
        except (ValueError, RuntimeError) as exc:
            _LOGGER.info(
                'Target scrape failed',
                exc=exc,
                extra={'target': target, 'proxies': proxies},
            )
            if target not in YOUTUBE_URLS:
                await record_failed(
                    target,
                    {
                        'reason': str(exc),
                        'status_code': None,
                    },
                    failed, failed_fd,
                )
            continue

        fully_scraped.add(target)

        if target not in YOUTUBE_URLS:
            if (channel.subs is None
                    or channel.subs >= min_subs):
                await record_discovered(
                    target, channel.subs, 'scrape',
                    discovered, discovered_fd,
                    channel_id=channel.channel_id,
                    creator_map=creator_map,
                )
            else:
                _LOGGER.info(
                    'Target below min-subs, not recorded',
                    extra={
                        'target': target,
                        'subs': channel.subs,
                    },
                )

        await _enqueue_links(
            channel, min_subs, known_channels,
            discovered, fully_scraped, failed,
            in_queue, to_scrape, discovered_fd,
        )

    _LOGGER.info('BFS queue empty, discovery complete')


async def build_creator_map(
    settings: 'DiscoverSettings',
) -> CreatorMap:
    '''
    Construct the creator_map backend from *settings*.

    * If ``REDIS_DSN`` is set, instantiate
      :class:`RedisCreatorMap` and probe connectivity with
      a single ``HLEN``. If the probe fails, log an error
      and ``sys.exit(1)`` — a misconfigured REDIS_DSN is a
      startup fault, not a silent degrade.
    * Otherwise, fall back to :class:`FileCreatorMap`
      rooted at ``settings.creator_map_file``.
    '''
    if settings.redis_dsn:
        cmap: RedisCreatorMap = RedisCreatorMap(
            settings.redis_dsn,
            platform='youtube',
        )
        try:
            await cmap.size()
        except Exception as exc:
            _LOGGER.error(
                'REDIS_DSN is set but Redis is '
                'unreachable; refusing to start',
                exc=exc,
                extra={'redis_dsn': settings.redis_dsn},
            )
            sys.exit(1)
        _LOGGER.info(
            'Using Redis creator_map',
            extra={'redis_dsn': settings.redis_dsn},
        )
        return cmap

    fmap: FileCreatorMap = FileCreatorMap(
        settings.creator_map_file,
    )
    _LOGGER.info(
        'Using file creator_map',
        extra={
            'creator_map_file': (
                settings.creator_map_file
            ),
        },
    )
    return fmap


class DiscoverSettings(ScraperSettings):
    '''Settings for the YouTube channel discovery tool.'''

    channel_list: str = Field(
        default='channels.lst',
        validation_alias=AliasChoices(
            'YOUTUBE_CHANNEL_LIST', 'channel_list',
        ),
        description=(
            'Known channels file (one handle per line)'
        ),
    )
    discovered_channels: str = Field(
        default='discovered-channels.jsonl',
        validation_alias=AliasChoices(
            'YOUTUBE_DISCOVERED_CHANNELS', 'youtube_discovered_channels',
        ),
        description=(
            'Output JSONL file for discovered channels'
        ),
    )
    failed_channels: str = Field(
        default='failed-channels.jsonl',
        validation_alias=AliasChoices(
            'YOUTUBE_FAILED_CHANNELS', 'youtube_failed_channels',
        ),
        description=(
            'Output JSONL file for failed scrapes'
        ),
    )
    creator_map_file: str = Field(
        default='channel_map.csv',
        validation_alias=AliasChoices(
            'YOUTUBE_CHANNEL_MAP_FILE',
            'youtube_channel_map_file',
            'creator_map_file',
        ),
        description=(
            'Path to the CSV file used by '
            'FileCreatorMap when REDIS_DSN is not '
            'set. One "creator_id,handle" line per '
            'channel. Ignored when Redis is '
            'configured, in which case the shared '
            'youtube:creator_map hash is used '
            'instead.'
        ),
    )
    min_subs: int = Field(
        default=100,
        validation_alias=AliasChoices(
            'MIN_SUBS', 'min_subs',
        ),
        description=(
            'Minimum subscriber count to keep a channel'
        ),
    )


async def main() -> None:
    settings: DiscoverSettings = DiscoverSettings()

    configure_logging(
        level=settings.log_level,
        filename=settings.log_file,
        log_format=settings.log_format,
    )

    known_channels: set[str] = load_known_channels(
        settings.channel_list,
    )
    discovered: dict[str, int | None]
    fully_scraped: set[str]
    discovered, fully_scraped = load_discovered(
        settings.discovered_channels,
    )
    failed: dict[str, str] = load_failed(settings.failed_channels)

    _LOGGER.info(
        'Startup state',
        extra={
            'known': len(known_channels),
            'discovered': len(discovered),
            'fully_scraped': len(fully_scraped),
            'failed': len(failed),
        },
    )

    creator_map: CreatorMap = await build_creator_map(
        settings,
    )

    rate_limiter: YouTubeRateLimiter = (
        YouTubeRateLimiter.get(
            state_dir=settings.rate_limiter_state_dir,
            redis_dsn=settings.redis_dsn,
        )
    )
    rate_limiter.set_proxies(settings.proxies)

    disc_channels: str = settings.discovered_channels
    fail_channels: str = settings.failed_channels
    async with (
        aiofiles.open(disc_channels, 'a') as discovered_fd,
        aiofiles.open(fail_channels, 'a') as failed_fd,
    ):
        async with AsyncYouTubeClient() as client:
            proxies: list[str] | None = (
                settings.proxies.split(',')
                if settings.proxies
                else None
            )
            await discover(
                client, creator_map,
                known_channels,
                discovered, fully_scraped, failed,
                discovered_fd, failed_fd,
                settings.min_subs,
                proxies=proxies,
            )


if __name__ == '__main__':
    asyncio.run(main())
