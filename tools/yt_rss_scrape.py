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
import sys
import heapq
import random
import asyncio
import logging

from pathlib import Path
from time import monotonic
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import brotli
import orjson
import untangle
import aiofiles

from httpx import Response

from prometheus_client import Counter, Gauge, start_http_server

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from innertube import InnerTube

from scrape_exchange.exchange_client import ExchangeClient
from scrape_exchange.youtube.youtube_channel import YouTubeChannel
from scrape_exchange.youtube.youtube_video import YouTubeVideo
from scrape_exchange.youtube.youtube_client import AsyncYouTubeClient

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

INNERTUBE_BLOCKED_TIMER: datetime = datetime.now(UTC) - timedelta(days=365)
FAILURE_DELAY: int = 60

CHANNEL_SCHEMA_OWNER: str = 'boinko'
CHANNEL_SCHEMA_VERSION: str = '0.0.1'
CHANNEL_SCHEMA_PLATFORM: str = 'youtube'
CHANNEL_SCHEMA_ENTITY: str = 'channel'

MIN_SLEEP_SECONDS: float = 0.8
MAX_SLEEP_SECONDS: float = 1.5

# Prometheus metrics
METRIC_CHANNEL_MAP_SIZE = Gauge(
    'yt_rss_channel_map_size',
    'Number of channels in the channel map',
)
METRIC_QUEUE_SIZE = Gauge(
    'yt_rss_queue_size',
    'Number of channels in the processing queue',
)
METRIC_VIDEOS_UPLOADED = Counter(
    'yt_rss_videos_uploaded_total',
    'Total number of videos successfully uploaded to the data API',
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
METRIC_CHANNEL_UPDATE_FAILURES = Counter(
    'yt_rss_channel_update_failures_total',
    'Number of times a channel update to the data API failed',
)
METRIC_VIDEO_UPLOAD_FAILURES = Counter(
    'yt_rss_video_upload_failures_total',
    'Number of times a video upload to the data API failed',
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


class Settings(BaseSettings):
    '''
    Worker configuration loaded in priority order:
    CLI flags > environment variables > .env file > built-in defaults.

    Backward-compatible env var names (e.g. SCRAPE_EXCHANGE_URL,
    MIN_CHANNEL_INTERVAL_SECONDS) are accepted alongside the shorter
    field-name-based names (e.g. EXCHANGE_URL, MIN_INTERVAL).
    '''

    model_config = SettingsConfigDict(
        env_file=str(Path(__file__).parent.parent / '.env'),
        env_file_encoding='utf-8',
        cli_parse_args=True,
        cli_kebab_case=True,
        populate_by_name=True,
        extra='ignore',
    )

    exchange_url: str = Field(
        default='https://scrape.exchange',
        validation_alias=AliasChoices('EXCHANGE_URL', 'exchange_url'),
        description='Base URL for the Scrape.Exchange API',
    )

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
    api_key_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'API_KEY_ID', 'api_key_id'
        ),
        description='API key ID for Scrape.Exchange authentication',
    )
    api_key_secret: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'API_KEY_SECRET', 'api_key_secret'
        ),
        description='API key secret for Scrape.Exchange authentication',
    )
    channel_map_file: str = Field(
        default='channel_map.csv',
        validation_alias=AliasChoices(
            'YOUTUBE_CHANNEL_MAP_FILE', 'channel_map_file'
        ),
        description=(
            'CSV file to save mapping of channel IDs to names for channels '
            'scraped during this run (format: channel_id,channel_name).'
        )
    )
    channel_data_directory: str = Field(
        default='channels',
        validation_alias=AliasChoices(
            'YOUTUBE_CHANNEL_DATA_DIR', 'channel_data_directory'
        ),
    )
    video_data_directory: str = Field(
        default='videos',
        validation_alias=AliasChoices(
            'YOUTUBE_VIDEO_DATA_DIR', 'video_data_directory'
        ),
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
    max_concurrent: int = Field(
        default=MAX_CONCURRENT_CHANNELS,
        validation_alias=AliasChoices(
            'MAX_CONCURRENT_CHANNELS', 'max_concurrent'
        ),
        description=(
            f'Maximum channels processed concurrently '
            f'(default: {MAX_CONCURRENT_CHANNELS})'
        ),
    )
    proxies: str | None = Field(
        default=None,
        validation_alias=AliasChoices('PROXIES', 'proxies'),
        description=(
            'Optional proxy URL to use for HTTP requests (e.g. '
            '"http://user:pass@host:port"). Multiple proxies can be specified '
            'separated by commas, in which case they will be used in a '
            'round-robin fashion.'
        )
    )
    log_level: str = Field(
        default='INFO',
        description='Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)',
        validation_alias=AliasChoices('LOG_LEVEL', 'log_level'),
    )
    log_file: str = Field(
        default='/tmp/yt-rss-reader.log',
        validation_alias=AliasChoices('LOG_FILE', 'log_file'),
        description='Log file path',
    )

    metrics_port: int = Field(
        default=9800,
        validation_alias=AliasChoices('METRICS_PORT', 'metrics_port'),
        description='Port for the Prometheus metrics HTTP server',
    )

    @field_validator('log_level', mode='before')
    @classmethod
    def uppercase_log_level(cls, v: str) -> str:
        upper: str = v.upper() if isinstance(v, str) else v
        valid: set[str] = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}
        if upper not in valid:
            raise ValueError(
                f'log_level must be one of {sorted(valid)}, got {v!r}'
            )
        return upper


async def fetch_rss(rss_url: str, yt_client: AsyncYouTubeClient
                    ) -> list[YouTubeVideo] | None:
    '''
    Fetches and parses the YouTube RSS feed for a channel.

    :param rss_url: The URL of the YouTube RSS feed.
    :returns: A list of YouTubeVideo instances populated from the RSS feed.
    :raises: httpx.HTTPStatusError on non-2xx HTTP responses.
    :raises: httpx.RequestError on network-level failures.
    '''

    logging.debug(f'Fetching RSS feed: {rss_url}')

    try:
        data: str | None = await yt_client.get(rss_url, timeout=1)
    except ValueError:
        METRIC_RSS_FAILURES.inc()
        await asyncio.sleep(random.random() + 1)
        raise
    except Exception:
        METRIC_RSS_FAILURES.inc()
        logging.debug(
            f'Getting RSS data failed, sleeping {FAILURE_DELAY} seconds'
        )
        await asyncio.sleep(FAILURE_DELAY)
        raise

    sleep: float = random.uniform(MIN_SLEEP_SECONDS, MAX_SLEEP_SECONDS)
    logging.debug('Sleeping for %.2f seconds after fetching RSS feed', sleep)
    await asyncio.sleep(sleep)
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
            logging.warning(f'Skipping malformed RSS entry: {exc}')

    return videos


async def check_video_exists(
    client: ExchangeClient, settings: Settings, video_id: str
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


async def upload_video(
    client: ExchangeClient, settings: Settings,
    channel_name: str, video: YouTubeVideo
) -> bool:
    '''
    Stores a video in Scrape Exchange via the data API.

    :param client: The authenticated Scrape Exchange API client.
    :param settings: Worker settings.
    :param channel_id: YouTube channel ID used as platform_creator_id.
    :param video: YouTubeVideo instance produced by fetch_rss().
    :returns: True if the video exists on the server, False otherwise.
    '''

    try:
        METRIC_API_VIDEO_CALLS.inc()
        response: Response = await client.post(
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
            }
        )
    except Exception as exc:
        METRIC_VIDEO_UPLOAD_FAILURES.inc()
        logging.info(f'Failed to upload video {video.video_id}: {exc}')
        return False

    if response.status_code == 201:
        METRIC_VIDEOS_UPLOADED.inc()
        return True

    METRIC_VIDEO_UPLOAD_FAILURES.inc()
    logging.warning(
        f'Failed to store video {video.video_id}: '
        f'HTTP {response.status_code} - {response.text}'
    )
    return False


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
    yt_client: AsyncYouTubeClient,
    no_feeds: dict[str, tuple[str, str, int]], settings: Settings
) -> bool:
    '''
    Fetches the RSS feed for one channel and checks or stores each video.

    Raises on RSS fetch failure or if any video could not be stored, so
    the worker loop can schedule a retry for the whole channel.

    :param channel_name: Human-readable channel name (for logging).
    :param channel_id: YouTube channel ID.
    :param client: The authenticated Scrape Exchange API client.
    :param settings: Worker settings.
    :returns: bool if the channel should be scheduled again
    :raises: httpx.HTTPError if the RSS feed cannot be retrieved.
    :raises: RuntimeError if one or more videos could not be stored.
    '''

    if channel_id in CHANNEL_LAST_CHECKED:
        elapsed: float = monotonic() - CHANNEL_LAST_CHECKED[channel_id]
        METRIC_CHANNEL_SECONDS_SINCE_LAST_PROCESSED.set(elapsed)
        logging.info(
            f'Processing channel {channel_name!r} last checked '
            f'{elapsed:.1f}s ago'
        )
    else:
        logging.debug(
            f'First time processing channel {channel_name!r} ({channel_id})'
        )

    rss_url: str = YOUTUBE_RSS_URL.format(channel_id=channel_id)
    fail_count: int = 0
    if channel_id in no_feeds:
        _: str
        rss_url, _, fail_count = no_feeds[channel_id]
        if fail_count >= 3:
            logging.info(
                f'Channel {channel_name!r} has failed to fetch RSS feed '
                f'{fail_count} times, skipping'
            )
            return False
        logging.info(
            f'Channel {channel_name!r} had missing RSS feed (attempted URL: '
            f'{rss_url}), failure count: {fail_count}'
        )

    CHANNEL_LAST_CHECKED[channel_id] = monotonic()

    channel = YouTubeChannel(
        name=channel_name, browse_client=yt_client, with_download_client=False
    )
    channel.channel_id = channel_id
    if not await update_channel(client, channel):
        await update_no_feeds(
            channel_id, rss_url, channel_name, no_feeds, 1
        )
        return False

    innertube: InnerTube = InnerTube('WEB', proxies=yt_client.proxy)
    try:
        videos: list[YouTubeVideo] | None = await fetch_rss(rss_url, yt_client)
        if videos is None:
            logging.debug(f'RSS feed not found for channel {channel_name!r}')
            await update_no_feeds(
                channel_id, YOUTUBE_RSS_URL.format(channel_id=channel_id),
                channel_name, no_feeds, 1
            )
            return False
    except Exception as exc:
        logging.debug(
            f'Failed to fetch RSS feed for channel {channel_name!r}: {exc}'
        )
        await update_no_feeds(
            channel_id, YOUTUBE_RSS_URL.format(channel_id=channel_id),
            channel_name, no_feeds, 1
        )
        return False

    # Scraping was succesfull, reset the fail counter
    if channel_id in no_feeds:
        await update_no_feeds(
                channel_id, YOUTUBE_RSS_URL.format(channel_id=channel_id),
                channel_name, no_feeds, 0
        )

    if not videos:
        await asyncio.sleep(
            random.uniform(MIN_SLEEP_SECONDS, MAX_SLEEP_SECONDS)
        )
        logging.debug(
            f'No videos found in RSS feed for channel {channel_name!r}'
        )
        return True

    logging.debug(
        f'[{channel_name}] {len(videos)} video(s) in RSS feed'
    )

    videos_uploaded: int = 0
    videos_existing: int = 0
    global INNERTUBE_BLOCKED_TIMER
    for video in videos:
        file_path: str = YouTubeVideo.get_filepath(
            video.video_id, settings.video_data_directory,
            VIDEO_FILENAME_PREFIX
        )

        if os.path.exists(file_path):
            logging.debug(
                f'Found existing file for video {video.video_id}, skipping'
            )
            videos_existing += 1
            continue

        try:
            exists: bool = await check_video_exists(
                client, settings, video.video_id
            )
            if exists:
                logging.debug(
                    f'[{channel_name}] {video.video_id} ({video.title!r}): '
                    f'already on scrape.exchange'
                )
                videos_existing += 1
                continue

            if settings.with_innertube:
                if INNERTUBE_BLOCKED_TIMER < datetime.now(UTC):
                    await video.from_innertube(innertube=innertube)
                    logging.debug('Updated video with data from YouTube')
                else:
                    logging.info(
                        f'Innertube blocked until {INNERTUBE_BLOCKED_TIMER}'
                    )
        except RuntimeError as exc:
            METRIC_INNERTUBE_FAILURES.inc()
            INNERTUBE_BLOCKED_TIMER = \
                datetime.now(UTC) + timedelta(seconds=3600)
            logging.debug(
                'Failed to get InnerTube video data, will continue with '
                f'what we have: {exc}'
            )
        except Exception as exc:
            logging.debug(
                'Failed to get InnerTube data, will continue with '
                f'what we have: {exc}'
            )

        try:
            filename: str = await video.to_file(
                settings.video_data_directory,
                filename_prefix=VIDEO_FILENAME_PREFIX,
                overwrite=True
            )
            logging.debug(
                f'Stored the file in {filename} -'
                f'[{channel_name}] {video.video_id} ({video.title!r}): '
                f'not yet on scrape.exchange, uploading'
            )
            stored: bool = await upload_video(
                client, settings, channel_name, video
            )
            if stored:
                logging.debug(
                    f'[{channel_name}] {video.video_id} '
                    f'({video.title!r}): uploaded'
                )
                videos_uploaded += 1
            else:
                logging.warning(
                    f'[{channel_name}] {video.video_id} '
                    f'({video.title!r}): upload failed'
                )
        except RuntimeError as exc:
            logging.warning(
                f'[{channel_name}] {video.video_id}: API error - {exc}'
            )

    missed: int = len(videos) - videos_uploaded - videos_existing
    logging.info(
        f'{channel_name}: uploaded {videos_uploaded} videos, '
        f'{videos_existing} were already on the server'
    )
    if missed > 0:
        raise RuntimeError(
            f'{missed} out of {len(videos)} videos '
            f'for channel {channel_name!r} could not be processed'
        )

    videos = []
    channel = None
    return True


async def update_channel(client: ExchangeClient, channel: YouTubeChannel
                         ) -> bool:
    '''
    Scrapes the about page of the channel for metadata and updates the channel
    data on Scrape Exchange via the data API.

    :param client: The authenticated Scrape Exchange API client.
    :param channel: The YouTubeChannel instance to update.
    :returns: True if the channel data was scraped successfully, False on
    failure to scrape.
    :raises: (none)
    '''

    try:
        await channel.scrape_about_page()
    except Exception as exc:
        logging.debug(
            f'Failed to scrape about page for channel {channel.name!r}: {exc}'
        )
        return False

    try:
        channel_id: str = channel.channel_id
        METRIC_API_CHANNEL_CALLS.inc()
        response: Response = await client.post(
            f'{client.exchange_url}{ExchangeClient.POST_DATA_API}',
            json={
                'username': CHANNEL_SCHEMA_OWNER,
                'platform': CHANNEL_SCHEMA_PLATFORM,
                'entity': CHANNEL_SCHEMA_ENTITY,
                'version': CHANNEL_SCHEMA_VERSION,
                'source_url': f'https://www.youtube.com/channel/{channel_id}',
                'data': {
                    'channel': channel.name.lstrip('@'),
                    'title': channel.title,
                    'subscriber_count': channel.subscriber_count or 0,
                    'view_count': channel.view_count or 0,
                    'video_count': channel.video_count or 0,
                    'description': channel.description,
                },
                'platform_content_id': channel.name,
                'platform_creator_id': channel.name,
                'platform_topic_id': None,
            }
        )
        if response.status_code == 201:
            logging.debug(f'Channel {channel.name!r} updated successfully')
        else:
            METRIC_CHANNEL_UPDATE_FAILURES.inc()
            logging.warning(
                f'Failed to store channel {channel.name!r}: '
                f'HTTP {response.status_code} - {response.text}'
            )
    except Exception as exc:
        METRIC_CHANNEL_UPDATE_FAILURES.inc()
        logging.info(f'Failed to update channel data: {exc}')

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

    logging.info(f'Reading channel map from {filepath!r}')
    with open(filepath, 'r') as file_desc:
        line: str
        for line in file_desc:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts: list[str] = line.split(',')
            if len(parts) < 2:
                logging.warning(
                    f'Skipping malformed line in channel map file: {line!r}'
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
            except Exception as exc:
                os.remove(file_path)
                logging.debug(
                    f'Removed invalid channel file {file_path!r}: {exc}'
                )

    logging.info(
        f'Writing channel map with {len(channel_map)} entries '
        f'to {channel_map_file!r}'
    )
    with open(channel_map_file, 'w') as f:
        for channel_id, channel_name in channel_map.items():
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


def get_queue(settings, channels: dict[str, str] = {}
              ) -> list[tuple[float, str, str]]:
    '''
    Initializes the channel queue with next-check timestamps,
    from the persisted queue file if available and augmented with the
    discovered channels

    :param settings: Worker settings.
    :param channels: Optional dict of channel_name -> channel_id to add to the
                     queue.
    :returns: A list of (next_check_timestamp, channel_name, channel_id)
              tuples, sorted by next_check_timestamp.
    :raises: (none)
    '''
    now: float = datetime.now(UTC).timestamp()
    queue_file: str = settings.queue_file
    temp_queue: list[list[float, str, str]] = []
    try:
        temp_queue = load_queue(queue_file)
    except Exception as exc:
        logging.warning(
            f'Failed to load queue file {queue_file!r}: {exc}'
        )

    if temp_queue:
        try:
            shutil.copyfile(queue_file, f'{queue_file}.{BACKUP_SUFFIX}')
            logging.debug(
                f'Backed up queue file to {queue_file!r}.{BACKUP_SUFFIX}'
            )
        except OSError as exc:
            logging.warning(
                f'Failed to back up queue file {queue_file!r}: {exc}'
            )
    else:
        try:
            backup_queue_file: str = f'{queue_file}.{BACKUP_SUFFIX}'
            shutil.copyfile(backup_queue_file, queue_file)
            temp_queue = load_queue(queue_file)
            logging.info(f'Using backup queue file {backup_queue_file!r}')
        except Exception as exc:
            logging.warning(
                f'Failed to load backup queue file {backup_queue_file!r}: '
                f'{exc}'
            )

    if not temp_queue:
        logging.warning('Starting with an empty queue')

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
        seen_names.add(name.lower())
        seen_ids.add(channel_id.lower())
        queue.append((timestamp, name, channel_id))

    channel_name: str
    channel_id: str
    channel_items: list[tuple[str, str]] = list(channels.items())
    random.shuffle(channel_items)
    for channel_id, channel_name in channel_items:
        if (channel_name.lower() not in seen_names
                and channel_id.lower() not in seen_ids):
            queue.append((now, channel_name, channel_id))
            seen_names.add(channel_name.lower())
            seen_ids.add(channel_id.lower())

    heapq.heapify(queue)

    return queue


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
                f'Skipping duplicate queue entry: {channel_name!r} '
                f'({channel_id})'
            )
            continue
        seen_names.add(channel_name.lower())
        seen_ids.add(channel_id.lower())
        queue.append(entry)

    logging.info(
        f'Loaded queue from file {filepath!r}: '
        f'{len(queue)} channel(s)'
    )
    return queue


async def worker_loop(
    settings: Settings, client: ExchangeClient
) -> None:
    '''
    Runs indefinitely, processing channels in priority order.

    Each channel is assigned a next-check timestamp. The loop pops up to
    max_concurrent channels that are due, processes them concurrently, then
    re-schedules them: at now+min_interval on success, or now+retry_interval
    on failure.

    :param settings: Worker settings.
    :param client: The authenticated Scrape Exchange API client.
    '''

    channel_map: dict[str, str] = get_channelmap(
        settings.channel_map_file, settings.channel_data_directory
    )
    queue: list[tuple[float, str, str]] = get_queue(
        settings, channel_map
    )
    METRIC_CHANNEL_MAP_SIZE.set(len(channel_map))
    METRIC_QUEUE_SIZE.set(len(queue))

    logging.info(
        f'Worker started: {len(queue)} channel(s), '
        f'min_interval={settings.min_interval}s, '
        f'retry_interval={settings.retry_interval}s, '
        f'max_concurrent={settings.max_concurrent}, '
        f'discovered_channels={len(channel_map)}'
    )

    yt_client = AsyncYouTubeClient(
        proxies=settings.proxies.split(',') if settings.proxies else None
    )

    no_feeds: dict[str, tuple[str, str, int]] = await read_no_feeds_file(
        settings.no_feeds_file
    )

    while True:
        now: float = datetime.now(UTC).timestamp()

        next_time: float = queue[0][0]
        if next_time > now:
            sleep_secs: float = next_time - now
            METRIC_SLEEP_SECONDS.set(sleep_secs)
            logging.debug(f'Sleeping {sleep_secs:.1f}s until next batch')
            await asyncio.sleep(sleep_secs)
        METRIC_SLEEP_SECONDS.set(0)

        now: float = datetime.now(UTC).timestamp()
        # Collect up to max_concurrent channels that are ready
        batch: list[tuple[float, str, str]] = []
        while (queue and queue[0][0] <= now
               and len(batch) < settings.max_concurrent):
            batch.append(heapq.heappop(queue))

        METRIC_QUEUE_SIZE.set(len(queue))
        METRIC_CONCURRENCY.set(len(batch))
        logging.debug(
            f'Batch size {len(batch)}: '
            f'{", ".join(name for _, name, _ in batch)}'
        )

        tasks: list[asyncio.Task] = [
            process_channel(
                name, channel_id, client, yt_client, no_feeds, settings
            ) for _, name, channel_id in batch
        ]
        results: list = await asyncio.gather(*tasks, return_exceptions=True)

        now = datetime.now(UTC).timestamp()
        for i, (_, name, channel_id) in enumerate(batch):
            result = results[i]
            if (isinstance(result, ValueError)
                    or (isinstance(result, bool) and result is False)):
                logging.info(
                    f'RSS feed not found for channel {name!r} - {channel_id}'
                )
            elif (isinstance(result, BaseException)
                    and not isinstance(result, FileExistsError)):
                logging.warning(
                    f'Channel {name!r} failed: {type(result).__name__}: '
                    f'{result}', exc_info=result,
                )
            next_check: float = now + settings.min_interval
            heapq.heappush(queue, (next_check, name, channel_id))

        METRIC_QUEUE_SIZE.set(len(queue))
        METRIC_CONCURRENCY.set(0)

        try:
            # Now write the updated feeds info back to the file
            logging.debug(
                f'Updating no-feeds file: {settings.no_feeds_file!r} '
                f'with {len(no_feeds)} entries'
            )
            async with aiofiles.open(settings.no_feeds_file, 'w') as f:
                for cid, (url, name, count) in sorted(
                    no_feeds.items(), key=lambda x: x[1][2], reverse=True
                ):
                    await f.write(f'{cid}\t{url}\t{name}\t{count}\n')
        except OSError as exc:
            logging.warning(
                f'Failed to write no-feeds file {settings.no_feeds_file!r}: '
                f'{exc}'
            )

        try:
            async with aiofiles.open(settings.queue_file, 'wb') as fd:
                await fd.write(
                    orjson.dumps(queue, option=orjson.OPT_INDENT_2)
                )
        except OSError as exc:
            logging.warning(
                f'Failed to write queue file {settings.queue_file!r}: {exc}'
            )


async def main() -> None:
    '''
    Entry point: loads settings, configures logging, authenticates with
    Scrape Exchange, then runs the worker loop indefinitely.

    Settings are loaded from (in priority order): CLI flags, environment
    variables, .env file, built-in defaults.

    :returns: (none)
    :raises: (none)
    '''

    settings: Settings = Settings()

    if not settings.api_key_id or not settings.api_key_secret:
        print(
            'Error: API key ID and secret must be provided via '
            '--api-key-id/--api-key-secret, environment variables '
            'API_KEY_ID/API_KEY_SECRET, or a .env file'
        )
        sys.exit(1)

    logging.basicConfig(
        level=settings.log_level,
        filename=settings.log_file,
        format='%(levelname)s:'
               '%(asctime)s:'
               '%(filename)s:'
               '%(funcName)s():'
               '%(lineno)d:'
               '%(message)s'
    )
    start_http_server(settings.metrics_port)
    logging.info(
        f'Prometheus metrics available on port {settings.metrics_port}'
    )
    os.makedirs(settings.channel_data_directory, exist_ok=True)
    os.makedirs(settings.video_data_directory, exist_ok=True)

    client: ExchangeClient = await ExchangeClient.setup(
        api_key_id=settings.api_key_id,
        api_key_secret=settings.api_key_secret,
        exchange_url=settings.exchange_url,
    )

    async with client:
        await worker_loop(settings, client)


if __name__ == '__main__':
    asyncio.run(main())
