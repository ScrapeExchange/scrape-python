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
from scrape_exchange.worker_id import get_worker_id
from scrape_exchange.youtube.settings import YouTubeScraperSettings
from scrape_exchange.channel_map import (
    ChannelMap,
    FileChannelMap,
    RedisChannelMap,
)
from scrape_exchange.creator_queue import (
    CreatorQueue,
    FileCreatorQueue,
    RedisCreatorQueue,
    TierConfig,
    parse_priority_queues,
)


VIDEO_FILENAME_PREFIX: str = 'video-min-'
CHANNEL_FILENAME_PREFIX: str = 'channel-'
UPLOADED_DIR: str = '/uploaded'

MIN_CHANNEL_INTERVAL_SECONDS: int = 30 * 60         # 30 minutes
RETRY_INTERVAL_SECONDS: int = 60 * 60 * 4           # 4 hours
DEFAULT_PRIORITY_QUEUES: str = '1:10000000,4:1000000,12:100000,24:10000,48:0'
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

MSG_NO_RSS_FEED: str = 'RSS feed not found for channel'

# Prometheus metrics
METRIC_CHANNEL_MAP_SIZE = Gauge(
    'yt_rss_channel_map_size',
    'Number of channels in the channel map',
    ['worker_id'],
)
METRIC_QUEUE_SIZE = Gauge(
    'yt_rss_queue_size',
    'Number of channels in the processing queue',
    ['tier', 'worker_id'],
)
METRIC_VIDEOS_ENQUEUED = Counter(
    'yt_rss_videos_enqueued_total',
    'Total number of videos successfully enqueued for background '
    'upload. Actual delivery is tracked by '
    'exchange_client_background_uploads_total{entity="video"}.',
    ['worker_id'],
)
METRIC_API_CHANNEL_CALLS = Counter(
    'yt_rss_post_data_api_channel_calls_total',
    'Number of times the POST data API was called for a channel',
    ['worker_id'],
)
METRIC_API_VIDEO_CALLS = Counter(
    'yt_rss_post_data_api_video_calls_total',
    'Number of times the POST data API was called for a video',
    ['worker_id'],
)
METRIC_RSS_FAILURES = Counter(
    'yt_rss_feed_download_failures_total',
    'Number of times an RSS feed could not be downloaded',
    ['worker_id'],
)
METRIC_RSS_DOWNLOADED = Counter(
    'yt_rss_feeds_downloaded_total',
    'Number of RSS feeds successfully downloaded',
    ['worker_id'],
)
METRIC_SLEEP_SECONDS = Gauge(
    'yt_rss_sleep_seconds_before_next_channel',
    'Seconds the worker will sleep before processing the next '
    'channel batch',
    ['worker_id'],
)
METRIC_CONCURRENCY = Gauge(
    'yt_rss_concurrency_level',
    'Number of channels being processed concurrently in the '
    'current batch',
    ['worker_id'],
)
METRIC_INNERTUBE_FAILURES = Counter(
    'yt_rss_innertube_call_failures_total',
    'Number of times an Innertube API call failed',
    ['worker_id'],
)
METRIC_CHANNEL_SECONDS_SINCE_LAST_PROCESSED = Gauge(
    'yt_rss_channel_seconds_since_last_processed',
    'Seconds elapsed since the channel was last processed '
    '(only set for channels that have been processed before)',
    ['worker_id'],
)
METRIC_TIER_ON_TIME = Counter(
    'yt_rss_tier_on_time_total',
    'Channels processed within the tier interval',
    ['tier', 'worker_id'],
)
METRIC_TIER_OVERDUE = Counter(
    'yt_rss_tier_overdue_total',
    'Channels processed after the tier interval expired',
    ['tier', 'worker_id'],
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
    rss_max_no_feed_failures: int = Field(
        default=3,
        validation_alias=AliasChoices(
            'RSS_MAX_NO_FEED_FAILURES',
            'rss_max_no_feed_failures',
        ),
        description=(
            'Number of consecutive RSS 404 failures '
            'before a channel is skipped. YouTube '
            'silently degrades RSS feeds for flagged '
            'IPs, so a higher threshold avoids '
            'permanently blacklisting channels that '
            'are only transiently unreachable.'
        ),
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
    priority_queues: str = Field(
        default='4:1000000,12:100000,24:10000,48:0',
        validation_alias=AliasChoices(
            'PRIORITY_QUEUES', 'priority_queues',
        ),
        description=(
            'Comma-separated interval_hours:min_subscribers '
            'pairs defining priority tiers, ordered by '
            'priority (first = highest).'
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
        METRIC_RSS_FAILURES.labels(worker_id=get_worker_id()).inc()
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
        METRIC_RSS_FAILURES.labels(worker_id=get_worker_id()).inc()
        logging.debug('Getting RSS data failed', exc=exc)
        raise

    METRIC_RSS_DOWNLOADED.labels(worker_id=get_worker_id()).inc()

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
        METRIC_API_VIDEO_CALLS.labels(worker_id=get_worker_id()).inc()
    return enqueued


MSG_PROCESSED_VIDEOS: str = 'Processed videos for channel'


async def _fetch_rss_safe(
    rss_url: str,
) -> list[YouTubeVideo] | None | Exception:
    '''
    Wrapper around :func:`fetch_rss` that captures
    exceptions so the result can be used inside
    ``asyncio.gather`` without aborting sibling tasks.
    '''
    try:
        return await fetch_rss(rss_url)
    except Exception as exc:
        return exc


async def _enrich_and_store_video(
    video: YouTubeVideo,
    innertube: InnerTube,
    proxy: str | None,
    client: ExchangeClient,
    channel_name: str,
    settings: RssSettings,
) -> str | None:
    '''
    Enrich a single video via InnerTube, write it to
    disk, and enqueue the upload.  Returns the filename
    on success, ``None`` on failure.
    '''
    try:
        await video.from_innertube(
            innertube=innertube, proxy=proxy,
        )
        logging.debug(
            'Updated video using InnerTube data',
            extra={'video_id': video.video_id},
        )
    except Exception as exc:
        METRIC_INNERTUBE_FAILURES.labels(
            worker_id=get_worker_id()
        ).inc()
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
        return None

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
    if enqueue_upload_video(
        client, settings, channel_name, video,
    ):
        METRIC_VIDEOS_ENQUEUED.labels(
            worker_id=get_worker_id()
        ).inc()
        logging.debug(
            'Video enqueued for upload',
            extra={
                'channel_name': channel_name,
                'video_id': video.video_id,
                'title': video.title,
            },
        )
    return filename


async def process_channel(
    channel_name: str, channel_id: str, client: ExchangeClient,
    creator_queue: CreatorQueue, settings: RssSettings,
) -> bool | None:
    '''
    Fetches the RSS feed for one channel and checks or stores each video.

    Returns ``True`` on success, ``False`` on transient failure (the
    channel stays in the queue for retry), or ``None`` when the
    channel should be permanently removed from the queue (e.g.
    too many consecutive no-feed failures).

    Raises on RSS fetch failure or if any video could not be stored, so
    the worker loop can schedule a retry for the whole channel.

    :param channel_name: Human-readable channel name (for logging).
    :param channel_id: YouTube channel ID.
    :param client: The authenticated Scrape Exchange API client.
    :param creator_queue: Queue backend (file or Redis).
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
        METRIC_CHANNEL_SECONDS_SINCE_LAST_PROCESSED.labels(
            worker_id=get_worker_id()
        ).set(elapsed)
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

    # Store failures in the no-feeds store to avoid repeatedly
    # hitting the same missing feed
    no_feed_entry: tuple[str, str, int] | None = (
        await creator_queue.get_no_feeds(channel_id)
    )
    fail_count: int = 0
    if no_feed_entry is not None:
        _: str
        rss_url, _, fail_count = no_feed_entry
        if fail_count >= settings.rss_max_no_feed_failures:
            logging.info(
                'Channel has exceeded no-feed failure '
                'threshold, removing from queue',
                extra={
                    'channel_name': channel_name,
                    'fail_count': fail_count,
                },
            )
            return None
        logging.info(
            'Channel had missing RSS feed',
            extra={
                'channel_name': channel_name,
                'rss_url': rss_url,
                'fail_count': fail_count,
            },
        )

    CHANNEL_LAST_CHECKED[channel_id] = monotonic()

    # --- Phase 1: channel update + RSS fetch in parallel ---
    update_result: tuple[bool, int]
    rss_result: list[YouTubeVideo] | None | Exception
    update_result, rss_result = await asyncio.gather(
        update_channel(
            client, channel_name, channel_id, proxy,
        ),
        _fetch_rss_safe(rss_url),
    )

    update_ok: bool = update_result[0]
    sub_count: int = update_result[1]

    if not update_ok:
        # Leave the tier unchanged — don't overwrite a
        # known subscriber count with 0 just because
        # InnerTube failed this time.
        await creator_queue.set_no_feeds(
            channel_id, rss_url, channel_name, 1,
        )
        return False
    await creator_queue.update_tier(channel_id, sub_count)

    # Handle RSS fetch errors.
    if isinstance(rss_result, RuntimeError):
        logging.debug(
            'Server error fetching RSS feed, '
            'will retry later',
        )
        return True
    if isinstance(rss_result, Exception):
        logging.debug(
            'Failed to fetch RSS feed for channel',
            exc=rss_result,
            extra={
                'channel_name': channel_name,
            },
        )
        await creator_queue.set_no_feeds(
            channel_id,
            YOUTUBE_RSS_URL.format(
                channel_id=channel_id,
            ),
            channel_name,
            1,
        )
        return False

    videos: list[YouTubeVideo] | None = rss_result
    if videos is None:
        logging.debug(
            MSG_NO_RSS_FEED,
            extra={
                'channel_name': channel_name,
            },
        )
        await creator_queue.set_no_feeds(
            channel_id,
            YOUTUBE_RSS_URL.format(
                channel_id=channel_id,
            ),
            channel_name,
            1,
        )
        return False

    no_feed_entry = await creator_queue.get_no_feeds(
        channel_id,
    )
    if no_feed_entry is not None:
        await creator_queue.clear_no_feeds(channel_id)

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

    # --- Phase 2: filter locally-known videos, then
    #     batch-check existence on scrape.exchange ---
    candidates: list[YouTubeVideo] = []
    videos_existing: int = 0
    for video in videos:
        file_path: str = YouTubeVideo.get_filepath(
            video.video_id,
            settings.video_data_directory,
            VIDEO_FILENAME_PREFIX,
        )
        if os.path.exists(file_path):
            logging.debug(
                'Found existing file for video, '
                'skipping',
                extra={'video_id': video.video_id},
            )
            videos_existing += 1
        else:
            candidates.append(video)

    if not candidates:
        logging.info(
            MSG_PROCESSED_VIDEOS,
            extra={
                'channel_name': channel_name,
                'videos_uploaded': 0,
                'videos_existing': videos_existing,
                'videos_failed': 0,
                'video_count': len(videos),
            },
        )
        return True

    # Batch-check existence concurrently.
    exist_results: list[bool | Exception] = (
        await asyncio.gather(
            *[
                check_video_exists(
                    client, settings, v.video_id,
                )
                for v in candidates
            ],
            return_exceptions=True,
        )
    )

    new_videos: list[YouTubeVideo] = []
    for video, exists in zip(
        candidates, exist_results,
    ):
        if isinstance(exists, Exception):
            logging.warning(
                'Failed to check video existence on '
                'scrape.exchange, will upload anyway',
                exc=exists,
                extra={
                    'video_id': video.video_id,
                },
            )
            new_videos.append(video)
        elif exists:
            logging.debug(
                'Video already on scrape.exchange',
                extra={
                    'channel_name': channel_name,
                    'video_id': video.video_id,
                    'title': video.title,
                },
            )
            videos_existing += 1
        else:
            new_videos.append(video)

    if not new_videos:
        logging.info(
            MSG_PROCESSED_VIDEOS,
            extra={
                'channel_name': channel_name,
                'videos_uploaded': 0,
                'videos_existing': videos_existing,
                'videos_failed': 0,
                'video_count': len(videos),
            },
        )
        return True

    # --- Phase 3: enrich + upload new videos in
    #     parallel ---
    innertube: InnerTube = InnerTube(
        'WEB', proxies=proxy,
    )

    def _close_innertube() -> None:
        '''Close the InnerTube httpx session.'''
        session: object = getattr(
            innertube.adaptor, 'session', None,
        )
        if session is not None and hasattr(
            session, 'close',
        ):
            session.close()

    enrich_results: list[str | None | Exception] = (
        await asyncio.gather(
            *[
                _enrich_and_store_video(
                    video, innertube, proxy,
                    client, channel_name, settings,
                )
                for video in new_videos
            ],
            return_exceptions=True,
        )
    )

    videos_uploaded: int = 0
    videos_failed: int = 0
    for video, result in zip(
        new_videos, enrich_results,
    ):
        if isinstance(result, Exception):
            logging.warning(
                'Failed to process video',
                exc=result,
                extra={
                    'video_id': video.video_id,
                    'channel_name': channel_name,
                },
            )
            videos_failed += 1
        elif result is None:
            videos_failed += 1
        else:
            videos_uploaded += 1

    missed: int = (
        len(new_videos) - videos_uploaded
        - videos_failed
    )
    logging.info(
        MSG_PROCESSED_VIDEOS,
        extra={
            'channel_name': channel_name,
            'videos_uploaded': videos_uploaded,
            'videos_existing': videos_existing,
            'videos_failed': videos_failed,
            'video_count': len(videos),
        },
    )
    if missed > 0:
        _close_innertube()
        raise RuntimeError(
            f'{missed} out of {len(videos)} videos '
            f'for channel {channel_name!r} could not '
            f'be processed'
        )

    _close_innertube()
    videos = []
    return True


async def update_channel(
    client: ExchangeClient, channel_name: str,
    channel_id: str, proxy: str | None = None,
) -> tuple[bool, int]:
    '''
    Fetches channel metadata via InnerTube and updates the channel
    data on Scrape Exchange via the data API.

    :param client: The authenticated Scrape Exchange API client.
    :param channel_name: The channel handle / vanity name.
    :param channel_id: The YouTube channel ID.
    :param proxy: Optional proxy URL for the InnerTube request.
    :returns: Tuple of (success, subscriber_count). success is True
              if the channel data was fetched successfully.
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
        return False, 0

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
        METRIC_API_CHANNEL_CALLS.labels(worker_id=get_worker_id()).inc()

    return True, subscriber_count


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


def _log_channel_result(
    result: object,
    name: str,
    cid: str,
) -> None:
    '''Log the outcome of a single process_channel call.'''

    if (isinstance(result, ValueError)
            or (
                isinstance(result, bool)
                and result is False
            )):
        logging.info(
            MSG_NO_RSS_FEED,
            extra={
                'channel_name': name,
                'channel_id': cid,
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


def _record_tier_sla(
    tier: int,
    scheduled_time: float,
    now: float,
) -> None:
    '''
    Increment the on-time or overdue SLA counter.

    Compares the current time against the queue score
    (the time the channel was scheduled to be fetched).
    If ``now <= scheduled_time`` the fetch is on-time;
    otherwise it is overdue.
    '''

    wid: str = get_worker_id()
    if now <= scheduled_time:
        METRIC_TIER_ON_TIME.labels(
            tier=str(tier), worker_id=wid,
        ).inc()
    else:
        METRIC_TIER_OVERDUE.labels(
            tier=str(tier), worker_id=wid,
        ).inc()


async def _publish_queue_sizes(
    creator_queue: CreatorQueue,
) -> None:
    '''Set the per-tier queue size gauges.'''

    wid: str = get_worker_id()
    sizes: dict[int, int] = (
        await creator_queue.queue_sizes_by_tier()
    )
    for tier, count in sizes.items():
        METRIC_QUEUE_SIZE.labels(
            tier=str(tier), worker_id=wid,
        ).set(count)


async def _enrich_subscriber_counts(
    client: ExchangeClient,
    channel_map: dict[str, str],
    subscriber_counts: dict[str, int],
    known_ids: set[str] | None = None,
) -> None:
    '''
    Fill in missing subscriber counts by querying the
    scrape.exchange content API.  Modifies
    *subscriber_counts* in place.

    Skips channels that already have a subscriber
    count in *subscriber_counts* or are already
    enqueued in *known_ids* (from Redis).  This
    avoids 100K+ API calls on startup when most
    channels are already in the queue from a
    previous run.
    '''

    skip: set[str] = set(subscriber_counts.keys())
    if known_ids:
        skip |= known_ids

    missing: list[str] = [
        cid for cid in channel_map
        if cid not in skip
    ]
    if not missing:
        return

    # Cap API lookups to avoid blocking startup
    # indefinitely.  Un-enriched channels default to
    # the lowest tier and get re-tiered after first
    # scrape.
    _MAX_LOOKUPS: int = 100_000
    if len(missing) > _MAX_LOOKUPS:
        logging.info(
            'Capping API enrichment to %d of %d '
            'channels (rest default to lowest tier)',
            _MAX_LOOKUPS, len(missing),
        )
        missing = missing[:_MAX_LOOKUPS]

    logging.info(
        'Fetching subscriber counts from API '
        'for channels without local data',
        extra={
            'count': len(missing),
            'skipped_known': (
                len(channel_map) - len(missing)
            ),
        },
    )
    fetched: int = 0
    for cid in missing:
        name: str = channel_map[cid]
        url: str = (
            f'{client.exchange_url}'
            f'{ExchangeClient.GET_CONTENT_API}'
            f'/youtube/channel/{name}'
        )
        try:
            resp: Response = await client.get(url)
        except Exception:
            continue
        if resp.status_code != 200:
            continue
        try:
            data: dict = resp.json()
        except Exception:
            continue
        sub_count: int = data.get(
            'subscriber_count', 0,
        )
        if sub_count:
            subscriber_counts[cid] = sub_count
            fetched += 1

    if fetched:
        logging.info(
            'Enriched subscriber counts from API',
            extra={'fetched': fetched},
        )


async def worker_loop(
    settings: RssSettings,
    client: ExchangeClient,
    channel_fm: AssetFileManagement,
    creator_queue: CreatorQueue,
    tiers: list[TierConfig],
    channel_map: ChannelMap,
) -> None:
    '''
    Runs indefinitely, processing channels in priority order.

    Each channel is assigned a next-check timestamp based on its
    subscriber-count tier. The loop pops up to rss_concurrency
    channels that are due, processes them concurrently, then
    re-schedules them via the queue's tier-aware release logic.

    :param settings: Worker settings.
    :param client: The authenticated Scrape Exchange API client.
    :param channel_fm: AssetFileManagement instance owning the
        channel data directory.
    :param creator_queue: Queue backend (file or Redis).
    :param tiers: Priority tier configuration.
    :param channel_map: Channel map backend (file or Redis).
    '''

    channel_map_data: dict[str, str] = (
        await channel_map.get_all()
    )
    subscriber_counts: dict[str, int] = {}
    known_ids: set[str] = (
        await creator_queue.known_creator_ids()
    )
    await _enrich_subscriber_counts(
        client, channel_map_data, subscriber_counts,
        known_ids=known_ids,
    )
    added: int = await creator_queue.populate(
        channel_map_data, channel_fm, tiers, subscriber_counts,
    )
    queue_size: int = await creator_queue.queue_size()
    METRIC_CHANNEL_MAP_SIZE.labels(
        worker_id=get_worker_id()
    ).set(len(channel_map_data))
    await _publish_queue_sizes(creator_queue)

    logging.info(
        'Worker started',
        extra={
            'channel_count': queue_size,
            'channels_added': added,
            'min_interval': settings.min_interval,
            'retry_interval': settings.retry_interval,
            'rss_concurrency': settings.rss_concurrency,
            'discovered_channels': len(channel_map_data),
        },
    )

    while True:
        queue_size = await creator_queue.queue_size()
        if queue_size == 0:
            logging.info(
                'Queue is empty, rebuilding after delay',
                extra={
                    'rebuild_delay': settings.min_interval,
                },
            )
            await _publish_queue_sizes(creator_queue)
            METRIC_SLEEP_SECONDS.labels(
                worker_id=get_worker_id()
            ).set(settings.min_interval)
            await asyncio.sleep(settings.min_interval)
            METRIC_SLEEP_SECONDS.labels(
                worker_id=get_worker_id()
            ).set(0)
            channel_map_data = (
                await channel_map.get_all()
            )
            subscriber_counts = {}
            known_ids = (
                await creator_queue.known_creator_ids()
            )
            await _enrich_subscriber_counts(
                client, channel_map_data,
                subscriber_counts,
                known_ids=known_ids,
            )
            await creator_queue.populate(
                channel_map_data, channel_fm,
                tiers, subscriber_counts,
            )
            METRIC_CHANNEL_MAP_SIZE.labels(
                worker_id=get_worker_id()
            ).set(len(channel_map_data))
            await _publish_queue_sizes(creator_queue)
            continue

        now: float = datetime.now(UTC).timestamp()

        next_time: float | None = (
            await creator_queue.next_due_time()
        )
        claim_cutoff: float | None = None
        if next_time is not None and next_time > now:
            sleep_secs: float = next_time - now
            if sleep_secs > settings.min_interval:
                # Nothing due within min_interval;
                # pull the next channel early instead
                # of idling.
                logging.debug(
                    'Next channel not due for longer '
                    'than min_interval, claiming early',
                    extra={
                        'sleep_secs': sleep_secs,
                        'min_interval': (
                            settings.min_interval
                        ),
                    },
                )
                claim_cutoff = next_time
            else:
                METRIC_SLEEP_SECONDS.labels(
                    worker_id=get_worker_id()
                ).set(sleep_secs)
                logging.debug(
                    'Sleeping until next batch',
                    extra={'sleep_secs': sleep_secs},
                )
                await asyncio.sleep(sleep_secs)
        METRIC_SLEEP_SECONDS.labels(
            worker_id=get_worker_id()
        ).set(0)

        batch: list[tuple[str, str]] = (
            await creator_queue.claim_batch(
                settings.rss_concurrency,
                get_worker_id(),
                cutoff=claim_cutoff,
            )
        )
        if not batch:
            await asyncio.sleep(MIN_SLEEP_SECONDS)
            continue

        await _publish_queue_sizes(creator_queue)
        METRIC_CONCURRENCY.labels(
            worker_id=get_worker_id()
        ).set(len(batch))
        logging.debug(
            'Batch prepared',
            extra={
                'batch_size': len(batch),
                'batch_names': ', '.join(
                    name for _, name, _ in batch
                ),
            },
        )

        process_now: float = (
            datetime.now(UTC).timestamp()
        )
        results: list = await asyncio.gather(
            *[
                process_channel(
                    name, cid, client,
                    creator_queue, settings,
                )
                for cid, name, _ in batch
            ],
            return_exceptions=True,
        )

        for i, (cid, name, sched) in enumerate(batch):
            result = results[i]
            _log_channel_result(result, name, cid)
            if result is None:
                await creator_queue.remove(cid)
            else:
                tier: int = (
                    await creator_queue.get_tier(cid)
                )
                await creator_queue.release(cid)
                _record_tier_sla(
                    tier, sched, process_now,
                )

        await _publish_queue_sizes(creator_queue)
        METRIC_CONCURRENCY.labels(
            worker_id=get_worker_id()
        ).set(0)


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
        logging.info(
            'Prometheus metrics available',
            extra={'metrics_port': settings.metrics_port},
        )
    except OSError as exc:
        logging.warning(
            'Failed to bind Prometheus metrics port; '
            'worker will run without metrics',
            exc=exc,
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
        logging.shutdown()
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

    creator_queue: CreatorQueue
    if settings.redis_dsn:
        creator_queue = RedisCreatorQueue(
            settings.redis_dsn,
            get_worker_id(),
            platform='youtube',
        )
    else:
        creator_queue = FileCreatorQueue(
            settings.queue_file,
            settings.no_feeds_file,
        )

    channel_map_backend: ChannelMap
    if settings.redis_dsn:
        channel_map_backend = RedisChannelMap(
            settings.redis_dsn,
        )
    else:
        channel_map_backend = FileChannelMap(
            settings.channel_map_file,
        )

    tiers: list[TierConfig] = parse_priority_queues(
        settings.priority_queues,
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
            await worker_loop(
                settings, client, channel_fm,
                creator_queue, tiers, channel_map_backend,
            )
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
