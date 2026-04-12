#!/usr/bin/env python3

'''
A worker tool that periodically reads YouTube channel RSS feeds and checks
whether the videos are already stored in Scrape Exchange.

Processes up to a configurable number of channels concurrently, then sleeps
until the next polling interval.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import os
import shutil
import signal
import sys
import heapq
import random
import asyncio
import logging

from pathlib import Path
from time import monotonic
from datetime import UTC
from datetime import datetime

import brotli
import orjson
import untangle
import aiofiles

import httpx
from httpx import Response

from prometheus_client import Counter, Gauge, start_http_server

from pydantic import AliasChoices, Field, field_validator

from innertube import InnerTube

from scrape_exchange.exchange_client import ExchangeClient
from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.logging import configure_logging
from scrape_exchange.scraper_supervisor import (
    SupervisorConfig,
    publish_config_metrics,
    run_supervisor,
)
from scrape_exchange.settings import normalize_log_level
from scrape_exchange.youtube.youtube_channel_tabs import YouTubeChannelTabs
from scrape_exchange.scrape_exchange_rate_limiter import (
    ScrapeExchangeRateLimiter,
)
from scrape_exchange.youtube.youtube_rate_limiter import (
    YouTubeRateLimiter, YouTubeCallType
)
from scrape_exchange.youtube.youtube_video import YouTubeVideo
from scrape_exchange.youtube.youtube_video_innertube import (
    _parse_count as parse_count
)
from scrape_exchange.youtube.settings import YouTubeScraperSettings


VIDEO_FILENAME_PREFIX: str = 'video-min-'
CHANNEL_FILENAME_PREFIX: str = 'channel-'
UPLOADED_DIR: str = '/uploaded'

MIN_CHANNEL_INTERVAL_SECONDS: int = 60 * 60 * 4     # 1 hour
RETRY_INTERVAL_SECONDS: int = 60 * 2                # 2 minutes
MAX_CONCURRENT_CHANNELS: int = 3

FILE_EXTENSION: str = '.json.br'

BACKUP_SUFFIX: str = 'bak'

YOUTUBE_RSS_URL: str = (
    'https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}'
)

FAILURE_DELAY: int = 60

CHANNEL_SCHEMA_OWNER: str = 'boinko'
CHANNEL_SCHEMA_VERSION: str = '0.0.1'
CHANNEL_SCHEMA_PLATFORM: str = 'youtube'
CHANNEL_SCHEMA_ENTITY: str = 'channel'

MIN_SLEEP_SECONDS: float = 1.8
MAX_SLEEP_SECONDS: float = 4.5

# Prometheus metrics
METRIC_CHANNEL_MAP_SIZE = Gauge(
    'yt_rss_channel_map_size',
    'Number of channels in the channel map',
)
METRIC_QUEUE_SIZE = Gauge(
    'yt_rss_queue_size',
    'Number of channels in the processing queue',
)
METRIC_VIDEOS_ENQUEUED = Counter(
    'yt_rss_videos_enqueued_total',
    'Total number of videos successfully enqueued for background '
    'upload. Actual delivery is tracked by '
    'exchange_client_background_uploads_total{entity="video"}.',
)
METRIC_API_CHANNEL_CALLS = Counter(
    'yt_rss_post_data_api_channel_calls_total',
    'Number of times the POST data API was called for a channel',
)
METRIC_API_VIDEO_CALLS = Counter(
    'yt_rss_post_data_api_video_calls_total',
    'Number of times the POST data API was called for a video',
)
METRIC_RSS_FAILURES = Counter(
    'yt_rss_feed_download_failures_total',
    'Number of times an RSS feed could not be downloaded',
)
METRIC_RSS_DOWNLOADED = Counter(
    'yt_rss_feeds_downloaded_total',
    'Number of RSS feeds successfully downloaded',
)
METRIC_SLEEP_SECONDS = Gauge(
    'yt_rss_sleep_seconds_before_next_channel',
    'Seconds the worker will sleep before processing the next channel batch',
)
METRIC_CONCURRENCY = Gauge(
    'yt_rss_concurrency_level',
    'Number of channels being processed concurrently in the current batch',
)
METRIC_INNERTUBE_FAILURES = Counter(
    'yt_rss_innertube_call_failures_total',
    'Number of times an Innertube API call failed',
)
METRIC_CHANNEL_SECONDS_SINCE_LAST_PROCESSED = Gauge(
    'yt_rss_channel_seconds_since_last_processed',
    'Seconds elapsed since the channel was last processed (only set for channels that have been processed before)',     # noqa: E501
)


# Track interval between RSS feed checks per channel
CHANNEL_LAST_CHECKED: dict[str, float] = {}


class RssSettings(YouTubeScraperSettings):
    '''
    Worker configuration loaded in priority order:
    CLI flags > environment variables > .env file > built-in defaults.

    Backward-compatible env var names (e.g. SCRAPE_EXCHANGE_URL,
    MIN_CHANNEL_INTERVAL_SECONDS) are accepted alongside the shorter
    field-name-based names (e.g. EXCHANGE_URL, MIN_INTERVAL).
    '''

    schema_version: str = Field(
        default='0.0.1',
        validation_alias=AliasChoices(
            'SCHEMA_VERSION', 'schema_version'
        ),
        description='Schema version to use for uploads',
    )
    schema_owner: str = Field(
        default='boinko',
        validation_alias=AliasChoices(
            'SCHEMA_USERNAME', 'schema_owner'
        ),
        description='Username of the schema owner used for data API calls',
    )
    queue_file: str = Field(
        default='yt-rss-reader-queue.json',
        validation_alias=AliasChoices(
            'RSS_QUEUE_FILE', 'rss_queue_file'
        ),
        description='Path to JSON file for persisting the channel queue',
    )
    no_feeds_file: str = Field(
        default='yt-rss-reader-no-feeds.txt',
        validation_alias=AliasChoices(
            'NO_FEEDS_FILE', 'no_feeds_file'
        ),
        description=(
            'Path to text file where channel names with missing RSS feeds are '
            'logged (one channel name per line)'
        ),
    )
    with_innertube: bool = Field(
        default=True,
        validation_alias=AliasChoices(
            'WITH_INNERTUBE', 'with_innertube'
        ),
        description=(
            'Whether to fetch additional video data from YouTube Innertube '
            'API (default: True). If enabled, the worker will attempt to '
            'fetch additional metadata for each video using the YouTube '
            'Innertube API. If an Innertube request fails, the worker will '
            'log the error and continue processing with the data obtained '
            'from the RSS feed. Disable this feature if you want to retrieve '
            'RSS feeds from many channels as it may get you rate-limited by '
            'YouTube.'
        ),
    )
    min_interval: int = Field(
        default=MIN_CHANNEL_INTERVAL_SECONDS,
        validation_alias=AliasChoices(
            'MIN_CHANNEL_INTERVAL_SECONDS', 'min_interval'
        ),
        description=(
            f'Minimum seconds between RSS polls per channel '
            f'(default: {MIN_CHANNEL_INTERVAL_SECONDS})'
        ),
    )
    retry_interval: int = Field(
        default=RETRY_INTERVAL_SECONDS,
        validation_alias=AliasChoices(
            'RETRY_INTERVAL_SECONDS', 'retry_interval'
        ),
        description=(
            f'Seconds before retrying a failed channel '
            f'(default: {RETRY_INTERVAL_SECONDS})'
        ),
    )
    metrics_port: int = Field(
        default=9500,
        validation_alias=AliasChoices('RSS_METRICS_PORT', 'rss_metrics_port'),
        description='Port for the Prometheus metrics HTTP server',
    )
    rss_concurrency: int = Field(
        default=3,
        validation_alias=AliasChoices(
            'RSS_CONCURRENCY', 'rss_concurrency'
        ),
        description=(
            'Number of channels to fetch in parallel inside one '
            'RSS scraper process. RSS-scraper-specific so the '
            'video scraper can keep its own CONCURRENCY setting '
            'independent.'
        ),
    )
    rss_num_processes: int = Field(
        default=1,
        validation_alias=AliasChoices(
            'RSS_NUM_PROCESSES', 'rss_num_processes'
        ),
        description=(
            'Number of child RSS scraper processes to spawn. When '
            '> 1 the invocation becomes a supervisor that splits '
            'the proxy pool into N disjoint chunks and spawns one '
            'child per chunk. Each child runs with '
            'RSS_NUM_PROCESSES=1, gets its own METRICS_PORT '
            '(base + worker_instance, with base reserved for the '
            'supervisor and worker_instance starting at 1) and '
            'log file, if specified. Use this to bypass the GIL '
            'when polling many RSS feeds in parallel and to '
            'distribute Innertube fetch load across processes.'
        ),
    )
    rss_log_level: str = Field(
        default='INFO',
        validation_alias=AliasChoices(
            'RSS_LOG_LEVEL', 'rss_log_level',
            'LOG_LEVEL', 'log_level',
        ),
        description=(
            'Logging level for the RSS scraper '
            '(DEBUG, INFO, WARNING, ERROR, CRITICAL). Honours '
            'RSS_LOG_LEVEL first so this scraper can be dialled '
            'up independently of the video and channel scrapers; '
            'falls back to LOG_LEVEL when the scraper-specific '
            'var is unset.'
        ),
    )
    rss_log_file: str = Field(
        default='/dev/stdout',
        validation_alias=AliasChoices(
            'RSS_LOG_FILE', 'rss_log_file',
            'LOG_FILE', 'log_file',
        ),
        description=(
            'Log file path for the RSS scraper. Honours '
            'RSS_LOG_FILE first so each scraper can write to its '
            'own file; falls back to LOG_FILE when the '
            'scraper-specific var is unset.'
        ),
    )

    @field_validator('rss_log_level', mode='before')
    @classmethod
    def _normalize_rss_log_level(cls, v: str) -> str:
        return normalize_log_level(v)


async def fetch_rss(rss_url: str) -> list[YouTubeVideo] | None:
    '''
    Fetches and parses the YouTube RSS feed for a channel.

    :param rss_url: The URL of the YouTube RSS feed.
    :param proxy: Optional proxy URL for the HTTP request.
    :returns: A list of YouTubeVideo instances populated from the RSS feed.
    :raises: httpx.HTTPStatusError on non-2xx HTTP responses.
    :raises: httpx.RequestError on network-level failures.
    '''

    logging.debug('Fetching RSS feed', extra={'rss_url': rss_url})
    proxy: str | None
    __cookie_file: str | None
    proxy, __cookie_file = await YouTubeRateLimiter.get().acquire(
        YouTubeCallType.RSS
    )

    try:
        async with httpx.AsyncClient(proxies=proxy) as http:
            response: Response = await http.get(rss_url, timeout=10)
            response.raise_for_status()
            data: str = response.text
    except httpx.HTTPStatusError as exc:
        METRIC_RSS_FAILURES.inc()
        if exc.response.status_code == 404:
            raise ValueError(f'RSS feed not found: {rss_url}') from exc
        if exc.response.status_code == 500:
            raise RuntimeError(
                f'Server error fetching RSS feed: {rss_url}'
            ) from exc
        logging.warning(
            'HTTP error fetching RSS feed',
            extra={
                'rss_url': rss_url,
                'status_code': exc.response.status_code,
                'response_text': exc.response.text,
            },
        )
        raise
    except Exception as exc:
        METRIC_RSS_FAILURES.inc()
        logging.debug('Getting RSS data failed', exc=exc)
        raise

    METRIC_RSS_DOWNLOADED.inc()

    if not data:
        raise RuntimeError(f'No data received from RSS feed {rss_url}')

    feed: untangle.Element = untangle.parse(data)
    raw_entries: list | object = getattr(feed.feed, 'entry', [])
    if not isinstance(raw_entries, list):
        raw_entries = [raw_entries]

    videos: list[YouTubeVideo] = []
    for entry in raw_entries:
        try:
            video: YouTubeVideo = YouTubeVideo.from_rss_entry(entry)
            videos.append(video)
        except AttributeError as exc:
            logging.warning('Skipping malformed RSS entry', exc=exc)

    return videos


async def check_video_exists(
    client: ExchangeClient, settings: RssSettings, video_id: str
) -> bool:
    '''
    Checks whether a video is already stored in Scrape Exchange.

    :param client: The authenticated Scrape Exchange API client.
    :param settings: Worker settings.
    :param video_id: The YouTube video ID to look up.
    :returns: True if the video is already stored, False if not found.
    :raises: RuntimeError on unexpected API response status codes.
    '''

    url: str = (
        f'{settings.exchange_url}{ExchangeClient.POST_DATA_API}'
        f'/content/youtube/video/{video_id}'
    )
    response: Response = await client.get(url)

    if response.status_code == 200:
        return True
    if response.status_code == 404:
        return False
    raise RuntimeError(
        f'Unexpected status {response.status_code} checking video '
        f'{video_id}: {response.text}'
    )


def enqueue_upload_video(
    client: ExchangeClient, settings: RssSettings,
    channel_name: str, video: YouTubeVideo,
) -> bool:
    '''
    Fire-and-forget upload of an RSS-discovered video to Scrape
    Exchange.

    RSS-sourced videos have no on-disk asset backing them (they come
    straight out of the RSS feed), so no ``file_manager`` is passed:
    the background worker just POSTs with retries and increments the
    ``exchange_client_background_uploads_total`` counter on success
    or failure. If the queue is full the job is dropped and will be
    re-enqueued the next time the RSS feed is polled.

    :returns: ``True`` if the job was enqueued, ``False`` if dropped.
    '''

    enqueued: bool = client.enqueue_upload(
        f'{settings.exchange_url}{ExchangeClient.POST_DATA_API}',
        json={
            'username': settings.schema_owner,
            'platform': 'youtube',
            'entity': 'video',
            'version': settings.schema_version,
            'source_url': video.url,
            'data': video.to_dict(),
            'platform_content_id': video.video_id,
            'platform_creator_id': channel_name,
            'platform_topic_id': None,
        },
        entity='video',
        log_extra={
            'platform_content_id': video.video_id,
            'platform_creator_id': channel_name,
        },
    )
    if enqueued:
        METRIC_API_VIDEO_CALLS.inc()
    return enqueued


async def read_no_feeds_file(filepath: str) -> dict[str, tuple[str, str, int]]:
    '''
    Reads the no-feeds file and returns a dict of channel_id -> (url,
    channel_name, count).

    :param filepath: Path to the no-feeds file.
    :returns: A dict mapping channel IDs to tuples of (url, channel_name,
    count).
    :raises: OSError if there is an error reading the file.
    '''

    no_feeds: dict[str, tuple[str, str, int]] = {}
    if not os.path.exists(filepath):
        return no_feeds

    async with aiofiles.open(filepath, 'r') as f:
        async for line in f:
            parts: list[str] = line.strip().split()
            if len(parts) < 3:
                continue
            channel_id: str = parts[0]
            url: str = parts[1]
            name: str = parts[2]
            count: int = 1
            if len(parts) >= 4:
                try:
                    count = int(parts[3])
                except ValueError:
                    pass
            if channel_id in no_feeds:
                existing_count: int = no_feeds[channel_id][2]
                no_feeds[channel_id] = (url, name, int(existing_count + count))
            else:
                no_feeds[channel_id] = (url, name, count)

    return no_feeds


async def update_no_feeds(
    channel_id: str, rss_url: str, channel_name: str,
    no_feeds: dict[str, tuple[str, str, int]], count: int = 0
) -> None:
    '''
    Updates the no-feeds file with a new entry or increments the count for an
    existing entry.

    :param channel_id: The YouTube channel ID.
    :param url: The URL of the RSS feed that was attempted to be fetched.
    :param channel_name: The human-readable name of the channel.
    :param no_feeds: The current dict of channel_id -> (url, channel_name,
    count) loaded from the no-feeds file.
    :param no_feeds_file: Path to the no-feeds file to update.
    :raises: OSError if there is an error writing to the file.
    '''

    if channel_id in no_feeds:
        if count == 0:
            no_feeds.pop(channel_id)
        else:
            fail_count: int
            rss_url, channel_name, fail_count = no_feeds[channel_id]
            fail_count += count
            no_feeds[channel_id] = (rss_url, channel_name, fail_count)
    else:
        no_feeds[channel_id] = (rss_url, channel_name, count)


async def process_channel(
    channel_name: str, channel_id: str, client: ExchangeClient,
    no_feeds: dict[str, tuple[str, str, int]], settings: RssSettings,
) -> bool:
    '''
    Fetches the RSS feed for one channel and checks or stores each video.

    Raises on RSS fetch failure or if any video could not be stored, so
    the worker loop can schedule a retry for the whole channel.

    :param channel_name: Human-readable channel name (for logging).
    :param channel_id: YouTube channel ID.
    :param client: The authenticated Scrape Exchange API client.
    :param no_feeds: Dict tracking channels with missing RSS feeds.
    :param settings: Worker settings.
    :returns: bool if the channel should be scheduled again
    :raises: httpx.HTTPError if the RSS feed cannot be retrieved.
    :raises: RuntimeError if one or more videos could not be stored.
    '''

    proxy: str | None = YouTubeRateLimiter.get().select_proxy(
        YouTubeCallType.BROWSE
    )

    if channel_id in CHANNEL_LAST_CHECKED:
        elapsed: float = monotonic() - CHANNEL_LAST_CHECKED[channel_id]
        METRIC_CHANNEL_SECONDS_SINCE_LAST_PROCESSED.set(elapsed)
        logging.info(
            'Processing channel',
            extra={
                'channel_name': channel_name,
                'elapsed': elapsed,
            },
        )
    else:
        logging.debug(
            'First time processing channel',
            extra={
                'channel_name': channel_name,
                'channel_id': channel_id,
            },
        )

    rss_url: str = YOUTUBE_RSS_URL.format(channel_id=channel_id)

    # Store failures in the 'no-feeds' file to avoid repeatedly
    # hitting the same missing feed
    fail_count: int = 0
    if channel_id in no_feeds:
        _: str
        rss_url, _, fail_count = no_feeds[channel_id]
        if fail_count >= 3:
            logging.info(
                'Channel has failed to fetch RSS feed too many times, '
                'skipping',
                extra={
                    'channel_name': channel_name,
                    'fail_count': fail_count,
                },
            )
            return False
        logging.info(
            'Channel had missing RSS feed',
            extra={
                'channel_name': channel_name,
                'rss_url': rss_url,
                'fail_count': fail_count,
            },
        )

    CHANNEL_LAST_CHECKED[channel_id] = monotonic()

    if not await update_channel(
        client, channel_name, channel_id, proxy,
    ):
        await update_no_feeds(
            channel_id, rss_url, channel_name, no_feeds, 1
        )
        return False

    innertube: InnerTube = InnerTube('WEB', proxies=proxy)
    try:
        videos: list[YouTubeVideo] | None = await fetch_rss(rss_url)
        if videos is None:
            logging.debug(
                'RSS feed not found for channel',
                extra={'channel_name': channel_name},
            )
            await update_no_feeds(
                channel_id, YOUTUBE_RSS_URL.format(channel_id=channel_id),
                channel_name, no_feeds, 1
            )
            return False
    except RuntimeError:
        logging.debug('Server error fetching RSS feed, will retry later')
        return True
    except Exception as exc:
        logging.debug(
            'Failed to fetch RSS feed for channel',
            exc=exc,
            extra={'channel_name': channel_name},
        )
        await update_no_feeds(
            channel_id, YOUTUBE_RSS_URL.format(channel_id=channel_id),
            channel_name, no_feeds, 1
        )
        return False

    if channel_id in no_feeds:
        await update_no_feeds(
                channel_id, YOUTUBE_RSS_URL.format(channel_id=channel_id),
                channel_name, no_feeds, 0
        )

    if not videos:
        logging.debug(
            'No videos found in RSS feed for channel',
            extra={'channel_name': channel_name},
        )
        return True

    logging.debug(
        'Videos found in RSS feed',
        extra={
            'channel_name': channel_name,
            'video_count': len(videos),
        },
    )

    videos_uploaded: int = 0
    videos_existing: int = 0
    videos_failed: int = 0
    for video in videos:
        file_path: str = YouTubeVideo.get_filepath(
            video.video_id, settings.video_data_directory,
            VIDEO_FILENAME_PREFIX
        )

        if os.path.exists(file_path):
            logging.debug(
                'Found existing file for video, skipping',
                extra={'video_id': video.video_id},
            )
            videos_existing += 1
            continue

        try:
            exists: bool = await check_video_exists(
                client, settings, video.video_id
            )
            if exists:
                logging.debug(
                    'Video already on scrape.exchange',
                    extra={
                        'channel_name': channel_name,
                        'video_id': video.video_id,
                        'title': video.title,
                    },
                )
                videos_existing += 1
                continue
        except Exception as exc:
            logging.warning(
                'Failed to check video existence on '
                'scrape.exchange, will upload anyway',
                exc=exc,
                extra={'video_id': video.video_id},
            )

        try:
            await video.from_innertube(
                innertube=innertube, proxy=proxy,
            )
            logging.debug('Updated video using InnerTube data')
        except Exception as exc:
            METRIC_INNERTUBE_FAILURES.inc()
            logging.warning(
                'Failed to get InnerTube video data, '
                'will continue with RSS data',
                exc=exc,
                extra={'video_id': video.video_id},
            )

        try:
            filename: str = await video.to_file(
                settings.video_data_directory,
                filename_prefix=VIDEO_FILENAME_PREFIX,
                overwrite=True,
            )
        except Exception as exc:
            logging.warning(
                'Failed to write video file to disk, '
                'skipping video',
                exc=exc,
                extra={
                    'video_id': video.video_id,
                    'channel_name': channel_name,
                },
            )
            videos_failed += 1
            continue

        logging.debug(
            'Stored the file, video not yet on '
            'scrape.exchange, uploading',
            extra={
                'filename': filename,
                'channel_name': channel_name,
                'video_id': video.video_id,
                'title': video.title,
            },
        )
        # Fire-and-forget: background worker handles the POST
        # with retries. videos_uploaded counts successful
        # enqueues; true delivery status is tracked by
        # exchange_client_background_uploads_total.
        if enqueue_upload_video(
            client, settings, channel_name, video
        ):
            METRIC_VIDEOS_ENQUEUED.inc()
            videos_uploaded += 1
            logging.debug(
                'Video enqueued for upload',
                extra={
                    'channel_name': channel_name,
                    'video_id': video.video_id,
                    'title': video.title,
                },
            )

    missed: int = (
        len(videos) - videos_uploaded
        - videos_existing - videos_failed
    )
    logging.info(
        'Processed videos for channel',
        extra={
            'channel_name': channel_name,
            'videos_uploaded': videos_uploaded,
            'videos_existing': videos_existing,
            'videos_failed': videos_failed,
            'video_count': len(videos),
        },
    )
    if missed > 0:
        raise RuntimeError(
            f'{missed} out of {len(videos)} videos '
            f'for channel {channel_name!r} could not '
            f'be processed'
        )

    videos = []
    return True


async def update_channel(
    client: ExchangeClient, channel_name: str,
    channel_id: str, proxy: str | None = None,
) -> bool:
    '''
    Fetches channel metadata via InnerTube and updates the channel
    data on Scrape Exchange via the data API.

    :param client: The authenticated Scrape Exchange API client.
    :param channel_name: The channel handle / vanity name.
    :param channel_id: The YouTube channel ID.
    :param proxy: Optional proxy URL for the InnerTube request.
    :returns: True if the channel data was fetched successfully,
              False on failure.
    :raises: (none)
    '''

    try:
        tabs: YouTubeChannelTabs = YouTubeChannelTabs(channel_id, proxy)
        channel_data: dict = await tabs.browse_channel()
    except Exception as exc:
        logging.debug(
            'Failed to browse channel via InnerTube',
            exc=exc,
            extra={'channel_name': channel_name},
        )
        return False

    metadata: dict = channel_data.get(
        'metadata', {}
    ).get('channelMetadataRenderer', {})

    header: dict = channel_data.get('header', {}).get(
        'c4TabbedHeaderRenderer', {}
    )

    title: str = metadata.get('title', channel_name)
    description: str = metadata.get('description', '')

    subscriber_text: str = header.get('subscriberCountText', {}).get(
        'simpleText', ''
    )
    subscriber_count: int = 0
    if subscriber_text:
        try:
            subscriber_count = parse_count(subscriber_text) or 0
        except Exception:
            pass

    # Fire-and-forget: background worker inside ExchangeClient handles
    # the POST with retries. No file_manager is passed because an RSS
    # channel update has no on-disk asset backing it; success is
    # tracked via exchange_client_background_uploads_total.
    enqueued: bool = client.enqueue_upload(
        f'{client.exchange_url}{ExchangeClient.POST_DATA_API}',
        json={
            'username': CHANNEL_SCHEMA_OWNER,
            'platform': CHANNEL_SCHEMA_PLATFORM,
            'entity': CHANNEL_SCHEMA_ENTITY,
            'version': CHANNEL_SCHEMA_VERSION,
            'source_url': (
                'https://www.youtube.com/channel/'
                f'{channel_id}'
            ),
            'data': {
                'channel': channel_name.lstrip('@'),
                'title': title,
                'subscriber_count': subscriber_count,
                'view_count': 0,
                'description': description,
            },
            'platform_content_id': channel_name,
            'platform_creator_id': channel_name,
            'platform_topic_id': None,
        },
        entity='channel',
        log_extra={'channel_name': channel_name},
    )
    if enqueued:
        METRIC_API_CHANNEL_CALLS.inc()

    return True


def read_channel_map_file(filepath: str) -> dict[str, str]:
    '''
    Reads the channel map CSV file and returns a dict of channel_id ->
    channel_name.

    :param filepath: Path to the channel map CSV file.
    :returns: A dict mapping channel IDs to channel names.
    :raises: OSError if there is an error reading the file.
    '''

    channel_map: dict[str, str] = {}
    if not os.path.exists(filepath):
        return channel_map

    logging.info(
        'Reading channel map',
        extra={'filepath': filepath},
    )
    with open(filepath, 'r') as file_desc:
        line: str
        for line in file_desc:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts: list[str] = line.split(',')
            if len(parts) < 2:
                logging.warning(
                    'Skipping malformed line in channel map file',
                    extra={'line': line},
                )
                continue
            channel_id: str
            channel_name: str
            channel_id, channel_name = parts[0].strip(), parts[1].strip()
            if channel_id and channel_name:
                channel_map[channel_id] = channel_name

    return channel_map


def get_channelmap(channel_map_file: str, channel_data_dir: str
                   ) -> dict[str, str]:
    '''
    Loads the wanted channels from the directory with known channel data files.

    :param channel_data_dir: Path to the channel data directory.
    :returns: A dict mapping channel IDs to channel names.
    '''

    channel_map: dict[str, str] = read_channel_map_file(channel_map_file)
    updated_channel_map: dict[str, str] = {}
    known_channels: list[str] = list(channel_map.values())
    for directory in [channel_data_dir, channel_data_dir + UPLOADED_DIR]:
        os.makedirs(directory, exist_ok=True)
        files: list[str] = [
            filename for filename in os.listdir(directory)
            if filename.startswith(CHANNEL_FILENAME_PREFIX)
            and filename.endswith(FILE_EXTENSION)
        ]
        filename: str
        for filename in files:
            channel_name: str = filename[
                len(CHANNEL_FILENAME_PREFIX):-1*len(FILE_EXTENSION)
            ]
            if channel_name in known_channels:
                continue

            file_path: str = os.path.join(directory, filename)
            try:
                data: dict[str, any] = read_channel_file(file_path)
                channel_map[data['channel_id']] = channel_name
                updated_channel_map[data['channel_id']] = channel_name
            except Exception as exc:
                try:
                    os.remove(file_path)
                except OSError:
                    pass
                logging.debug(
                    'Removed invalid channel file',
                    exc=exc,
                    extra={'file_path': file_path},
                )

    logging.info(
        'Writing channel map',
        extra={
            'entry_count': len(channel_map),
            'channel_map_file': channel_map_file,
        },
    )
    # We only append new entries as yt_channel_scraper.py also appends to this
    # file, so we want to avoid rewriting it and potentially causing conflicts.
    with open(channel_map_file, 'a') as f:
        for channel_id, channel_name in updated_channel_map.items():
            f.write(f'{channel_id},{channel_name}\n')

    return channel_map


def read_channel_file(filepath: str) -> dict[str, any]:
    '''
    Reads a channel data file, which may be compressed with Brotli.

    :param filepath: Path to the channel data file.
    :returns: The parsed channel data as a dictionary.
    :raises: OSError if there is an error reading the file.
             orjson.JSONDecodeError if the file contents cannot be parsed as
             JSON.
    '''

    if filepath.endswith(FILE_EXTENSION):
        with open(filepath, 'rb') as f:
            decompressed_data: bytes = brotli.decompress(f.read())
            return orjson.loads(decompressed_data)
    elif filepath.endswith('.json'):
        with open(filepath, 'r') as f:
            return orjson.loads(f.read())


def get_queue(settings, channel_fm: AssetFileManagement,
              channels: dict[str, str] = {}
              ) -> list[tuple[float, str, str]]:
    '''
    Initializes the channel queue with next-check timestamps,
    from the persisted queue file if available and augmented with the
    discovered channels

    :param settings: Worker settings.
    :param channel_fm: AssetFileManagement instance owning the channel data
        directory, used to test for skip-marker files.
    :param channels: Optional dict of channel_name -> channel_id to add to the
                     queue.
    :returns: A list of (next_check_timestamp, channel_name, channel_id)
              tuples, sorted by next_check_timestamp.
    :raises: (none)
    '''

    temp_queue: list[tuple[float, str, str]] = get_current_queue(settings)

    # Now create the actual queue with a list of tuples
    # ugh, (or)json does not support tuples but converts each tuple to a
    # list. Hopefully orjson keeps the list in the same order as the
    # original tuple, so we can just convert it back.
    queue: list[tuple[float, str, str]] = []
    seen_names: set[str] = set()
    seen_ids: set[str] = set()
    for timestamp, name, channel_id in temp_queue:
        if name.lower() in seen_names or channel_id.lower() in seen_ids:
            continue
        if should_skip_channel(name, channel_id, channel_fm):
            continue

        seen_names.add(name.lower())
        seen_ids.add(channel_id.lower())
        queue.append((timestamp, name, channel_id))

    channel_name: str
    channel_id: str
    channel_items: list[tuple[str, str]] = list(channels.items())
    random.shuffle(channel_items)
    now: float = datetime.now(UTC).timestamp()
    for channel_id, channel_name in channel_items:
        if (channel_name.lower() in seen_names
                or channel_id.lower() in seen_ids):
            continue

        if should_skip_channel(channel_name, channel_id, channel_fm):
            continue

        queue.append((now, channel_name, channel_id))
        seen_names.add(channel_name.lower())
        seen_ids.add(channel_id.lower())

    heapq.heapify(queue)

    return queue


def get_current_queue(settings: RssSettings) -> list[tuple[float, str, str]]:
    queue_file: str = settings.queue_file
    temp_queue: list[list[float, str, str]] = []
    try:
        temp_queue = load_queue(queue_file)
    except Exception as exc:
        logging.warning(
            'Failed to load queue file',
            exc=exc,
            extra={'queue_file': queue_file},
        )

    if temp_queue:
        try:
            shutil.copyfile(queue_file, f'{queue_file}.{BACKUP_SUFFIX}')
            logging.debug(
                'Backed up queue file',
                extra={
                    'queue_file': queue_file,
                    'backup_suffix': BACKUP_SUFFIX,
                },
            )
        except OSError as exc:
            logging.warning(
                'Failed to back up queue file',
                exc=exc,
                extra={'queue_file': queue_file},
            )
    else:
        try:
            backup_queue_file: str = f'{queue_file}.{BACKUP_SUFFIX}'
            shutil.copyfile(backup_queue_file, queue_file)
            temp_queue = load_queue(queue_file)
            logging.info(
                'Using backup queue file',
                extra={'backup_queue_file': backup_queue_file},
            )
        except Exception as exc:
            logging.warning(
                'Failed to load backup queue file',
                exc=exc,
                extra={'backup_queue_file': backup_queue_file},
            )

    if not temp_queue:
        logging.warning('Starting with an empty queue')

    return temp_queue


def should_skip_channel(channel_name: str, channel_id: str,
                        channel_fm: AssetFileManagement) -> bool:
    '''
    Checks if a channel should be skipped based on the presence of marker
    files in the channel data directory.

    :param channel_name: The name of the channel.
    :param channel_id: The ID of the channel.
    :param channel_fm: AssetFileManagement instance owning the channel data
        directory.
    :returns: True if the channel should be skipped, False otherwise.
    '''
    not_found: Path = (
        channel_fm.base_dir / f'{CHANNEL_FILENAME_PREFIX}{channel_name}'
                              '.not_found'
    )
    unresolved: Path = (
        channel_fm.base_dir / f'{CHANNEL_FILENAME_PREFIX}{channel_id}'
                              '.unresolved'
    )
    if not_found.exists() or unresolved.exists():
        logging.debug(
            'Skipping channel marked as not_found or unresolved',
            extra={
                'channel_name': channel_name,
                'channel_id': channel_id,
            },
        )
        return True
    return False


def load_queue(filepath: str) -> list[tuple[float, str, str]]:
    '''
    Loads the channel queue from a JSON file.

    :param filepath: Path to the JSON file containing the queue data.
    :returns: A list of (next_check_timestamp, channel_name, channel_id)
              tuples, sorted by next_check_timestamp.
    :raises: FileNotFoundError if the file does not exist.
             OSError if there is an error reading the file.
             orjson.JSONDecodeError if the file contents cannot be parsed as
             JSON.
    '''

    with open(filepath, 'rb') as fd:
        content: bytes = fd.read()

        # (next_check_timestamp, channel_name, channel_id)
        raw: list[list[float, str, str]] = orjson.loads(content)

    seen_names: set[str] = set()
    seen_ids: set[str] = set()
    queue: list[list[float, str, str]] = []
    _: float
    channel_name: str
    channel_id: str
    for entry in raw:
        _, channel_name, channel_id = entry
        if (channel_name.lower() in seen_names
                or channel_id.lower() in seen_ids):
            logging.debug(
                'Skipping duplicate queue entry',
                extra={
                    'channel_name': channel_name,
                    'channel_id': channel_id,
                },
            )
            continue
        seen_names.add(channel_name.lower())
        seen_ids.add(channel_id.lower())
        queue.append(entry)

    logging.info(
        'Loaded queue from file',
        extra={
            'filepath': filepath,
            'channel_count': len(queue),
        },
    )
    return queue


async def worker_loop(
    settings: RssSettings, client: ExchangeClient,
    channel_fm: AssetFileManagement,
) -> None:
    '''
    Runs indefinitely, processing channels in priority order.

    Each channel is assigned a next-check timestamp. The loop pops up to
    max_concurrent channels that are due, processes them concurrently, then
    re-schedules them: at now+min_interval on success, or now+retry_interval
    on failure.

    :param settings: Worker settings.
    :param client: The authenticated Scrape Exchange API client.
    :param channel_fm: AssetFileManagement instance owning the channel data
        directory.
    '''

    channel_map: dict[str, str] = get_channelmap(
        settings.channel_map_file, settings.channel_data_directory
    )
    queue: list[tuple[float, str, str]] = get_queue(
        settings, channel_fm, channel_map
    )
    METRIC_CHANNEL_MAP_SIZE.set(len(channel_map))
    METRIC_QUEUE_SIZE.set(len(queue))

    logging.info(
        'Worker started',
        extra={
            'channel_count': len(queue),
            'min_interval': settings.min_interval,
            'retry_interval': settings.retry_interval,
            'rss_concurrency': settings.rss_concurrency,
            'discovered_channels': len(channel_map),
        },
    )

    try:
        no_feeds: dict[str, tuple[str, str, int]] = (
            await read_no_feeds_file(settings.no_feeds_file)
        )
    except OSError as exc:
        logging.warning(
            'Failed to read no-feeds file, starting with '
            'empty set',
            exc=exc,
            extra={'no_feeds_file': settings.no_feeds_file},
        )
        no_feeds = {}

    while True:
        now: float = datetime.now(UTC).timestamp()

        next_time: float = queue[0][0]
        if next_time > now:
            sleep_secs: float = next_time - now
            METRIC_SLEEP_SECONDS.set(sleep_secs)
            logging.debug(
                'Sleeping until next batch',
                extra={'sleep_secs': sleep_secs},
            )
            await asyncio.sleep(sleep_secs)
        METRIC_SLEEP_SECONDS.set(0)

        now: float = datetime.now(UTC).timestamp()
        # Collect up to max_concurrent channels that are ready
        batch: list[tuple[float, str, str]] = []
        while (queue and queue[0][0] <= now
               and len(batch) < settings.rss_concurrency):
            batch.append(heapq.heappop(queue))

        METRIC_QUEUE_SIZE.set(len(queue))
        METRIC_CONCURRENCY.set(len(batch))
        logging.debug(
            'Batch prepared',
            extra={
                'batch_size': len(batch),
                'batch_names': ', '.join(
                    name for _, name, _ in batch
                ),
            },
        )

        tasks: list[asyncio.Task] = [
            process_channel(
                name, channel_id, client, no_feeds, settings,
            ) for _, name, channel_id in batch
        ]
        results: list = await asyncio.gather(*tasks, return_exceptions=True)

        now = datetime.now(UTC).timestamp()
        for i, (_, name, channel_id) in enumerate(batch):
            result = results[i]
            if (isinstance(result, ValueError)
                    or (isinstance(result, bool) and result is False)):
                logging.info(
                    'RSS feed not found for channel',
                    extra={
                        'channel_name': name,
                        'channel_id': channel_id,
                    },
                )
            elif (isinstance(result, BaseException)
                    and not isinstance(result, FileExistsError)):
                logging.warning(
                    'Channel failed',
                    exc_info=result,
                    extra={
                        'channel_name': name,
                        'error_type': type(result).__name__,
                        'error': result,
                    },
                )
            next_check: float = now + settings.min_interval
            heapq.heappush(queue, (next_check, name, channel_id))

        METRIC_QUEUE_SIZE.set(len(queue))
        METRIC_CONCURRENCY.set(0)

        try:
            # Now write the updated feeds info back to the file
            logging.debug(
                'Updating no-feeds file',
                extra={
                    'no_feeds_file': settings.no_feeds_file,
                    'entry_count': len(no_feeds),
                },
            )
            async with aiofiles.open(settings.no_feeds_file, 'w') as f:
                for cid, (url, name, count) in sorted(
                    no_feeds.items(), key=lambda x: x[1][2], reverse=True
                ):
                    await f.write(f'{cid}\t{url}\t{name}\t{count}\n')
        except OSError as exc:
            logging.warning(
                'Failed to write no-feeds file',
                exc=exc,
                extra={'no_feeds_file': settings.no_feeds_file},
            )

        try:
            async with aiofiles.open(settings.queue_file, 'wb') as fd:
                await fd.write(
                    orjson.dumps(queue, option=orjson.OPT_INDENT_2)
                )
        except OSError as exc:
            logging.warning(
                'Failed to write queue file',
                exc=exc,
                extra={'queue_file': settings.queue_file},
            )


async def _run_worker(settings: RssSettings) -> None:
    '''
    Run a single in-process RSS scraper worker (the leaf of the
    supervisor tree). Authenticates with Scrape Exchange, sets up
    the rate limiter, and enters :func:`worker_loop`.
    '''
    logging.info(
        'Starting YouTube RSS scrape tool',
        extra={'settings': settings.model_dump()},
    )
    worker_proxies: list[str] = [
        p.strip() for p in settings.proxies.split(',') if p.strip()
    ] if settings.proxies else []
    logging.info(
        'RSS scraper worker started',
        extra={
            'metrics_port': settings.metrics_port,
            'proxies_count': len(worker_proxies),
            'first_proxy': (
                worker_proxies[0] if worker_proxies else None
            ),
            'last_proxy': (
                worker_proxies[-1] if worker_proxies else None
            ),
        },
    )

    try:
        start_http_server(settings.metrics_port)
    except OSError as exc:
        logging.critical(
            'Failed to bind Prometheus metrics port',
            exc=exc,
            extra={'metrics_port': settings.metrics_port},
        )
        sys.exit(1)
    logging.info(
        'Prometheus metrics available',
        extra={'metrics_port': settings.metrics_port},
    )
    publish_config_metrics(
        role='worker', scraper_label='rss',
        num_processes=1,
        concurrency=settings.rss_concurrency,
    )
    # AssetFileManagement creates channel_data_directory and its
    # 'uploaded' subdirectory automatically. video_data_directory is
    # still managed by YouTubeVideo.to_file directly, so we create
    # it manually here.
    channel_fm: AssetFileManagement = AssetFileManagement(
        settings.channel_data_directory
    )
    os.makedirs(settings.video_data_directory, exist_ok=True)

    try:
        client: ExchangeClient = await ExchangeClient.setup(
            api_key_id=settings.api_key_id,
            api_key_secret=settings.api_key_secret,
            exchange_url=settings.exchange_url,
        )
    except Exception as exc:
        logging.critical(
            'Failed to connect to Scrape Exchange API',
            exc=exc,
            extra={
                'exchange_url': settings.exchange_url,
            },
        )
        sys.exit(1)

    YouTubeRateLimiter.get(
        state_dir=settings.rate_limiter_state_dir,
        redis_dsn=settings.redis_dsn,
    ).set_proxies(settings.proxies)

    post_rate: float = float(max(
        1,
        settings.rss_num_processes * settings.rss_concurrency,
    ))
    ScrapeExchangeRateLimiter.get(
        state_dir=settings.rate_limiter_state_dir,
        post_rate=post_rate,
        redis_dsn=settings.redis_dsn,
    )

    # Wire SIGINT/SIGTERM so they cancel the main task;
    # ExchangeClient's overridden aclose() (invoked by
    # ``async with client``) drains the background upload queue with
    # a 10-second timeout before the HTTP transport is torn down.
    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
    main_task: asyncio.Task = asyncio.current_task()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, main_task.cancel)
        except NotImplementedError:
            pass

    try:
        async with client:
            await worker_loop(settings, client, channel_fm)
    except asyncio.CancelledError:
        logging.info(
            'Shutdown signal received; upload queue drained'
        )
        raise


def main() -> None:
    '''
    Top-level entry point. Reads settings and dispatches to either
    the supervisor (when ``rss_num_processes > 1``) or the
    in-process scraper worker (when ``rss_num_processes == 1``).
    '''
    settings: RssSettings = RssSettings()

    if not settings.api_key_id or not settings.api_key_secret:
        print(
            'Error: API key ID and secret must be provided via '
            '--api-key-id/--api-key-secret, environment variables '
            'API_KEY_ID/API_KEY_SECRET, or a .env file'
        )
        sys.exit(1)

    configure_logging(
        level=settings.rss_log_level,
        filename=settings.rss_log_file,
        log_format=settings.log_format,
    )

    if settings.rss_num_processes > 1:
        sys.exit(run_supervisor(SupervisorConfig(
            scraper_label='rss',
            num_processes_env_var='RSS_NUM_PROCESSES',
            log_file_env_var='RSS_LOG_FILE',
            num_processes=settings.rss_num_processes,
            concurrency=settings.rss_concurrency,
            proxies=settings.proxies,
            metrics_port=settings.metrics_port,
            log_file=settings.rss_log_file or None,
            api_key_id=settings.api_key_id,
            api_key_secret=settings.api_key_secret,
            exchange_url=settings.exchange_url,
        )))

    asyncio.run(_run_worker(settings))


if __name__ == '__main__':
    main()
