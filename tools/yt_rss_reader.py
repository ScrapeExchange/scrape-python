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
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import httpx
import orjson
import untangle
import aiofiles

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from innertube import InnerTube

from scrape_exchange.exchange_client import ExchangeClient
from scrape_exchange.youtube.youtube_video import YouTubeVideo
from scrape_exchange.youtube.youtube_client import AsyncYouTubeClient

_LOGGER: logging.Logger = logging.getLogger(__name__)

VIDEO_FILENAME_PREFIX: str = 'video-min-'
CHANNEL_FILENAME_PREFIX: str = 'channel-'
UPLOADED_DIR: str = '/uploaded'

MIN_CHANNEL_INTERVAL_SECONDS: int = 60 * 240  # 4 hours
RETRY_INTERVAL_SECONDS: int = 60 * 2          # 2 minutes
MAX_CONCURRENT_CHANNELS: int = 3

BACKUP_SUFFIX: str = 'bak'

YOUTUBE_RSS_URL: str = (
    'https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}'
)

INNERTUBE_BLOCKED_TIMER: datetime = datetime.now(UTC) - timedelta(seconds=60)
FAILURE_DELAY: int = 60


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
    username: str = Field(
        default='boinko',
        validation_alias=AliasChoices('USERNAME', 'username'),
        description='Username for Scrape.Exchange uploads',
    )
    schema_version: str = Field(
        default='0.0.1',
        validation_alias=AliasChoices(
            'SCHEMA_VERSION', 'schema_version'
        ),
        description='Schema version to use for uploads',
    )
    schema_username: str = Field(
        default='boinko',
        validation_alias=AliasChoices(
            'SCHEMA_USERNAME', 'schema_username'
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
    directory: str = Field(
        default='/tmp/yt_rss_reader',
        validation_alias=AliasChoices(
            'YOUTUBE_SAVE_DIR', 'youtube_save_dir'
        ),
    )
    channel_db_file: str = Field(
        default='channels.csv',
        validation_alias=AliasChoices(
            'YOUTUBE_CHANNEL_DB_FILE', 'channel_db_file'
        ),
        description='CSV file mapping channel IDs to channel names for video uploads',  # noqa: E501
    )
    queue_file: str = Field(
        default='/tmp/yt-rss-reader-queue.json',
        validation_alias=AliasChoices(
            'RSS_QUEUE_FILE', 'rss_queue_file'
        ),
        description='Path to JSON file for persisting the channel queue',
    )
    with_innertube: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            'WITH_INNERTUBE', 'with_innertube'
        ),
        description=(
            'Whether to fetch additional video data from YouTube Innertube API '
            '(default: False). If enabled, the worker will attempt to fetch '
            'additional metadata for each video using the YouTube Innertube API. '
            'If an Innertube request fails, the worker will log the error and '
            'continue processing with the data obtained from the RSS feed.'
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


async def fetch_rss(channel_id: str) -> list[YouTubeVideo]:
    '''
    Fetches and parses the YouTube RSS feed for a channel.

    :param channel_id: The YouTube channel ID.
    :returns: A list of YouTubeVideo instances populated from the RSS feed.
    :raises: httpx.HTTPStatusError on non-2xx HTTP responses.
    :raises: httpx.RequestError on network-level failures.
    '''

    url: str = YOUTUBE_RSS_URL.format(channel_id=channel_id)
    _LOGGER.debug(f'Fetching RSS feed: {url}')

    async with AsyncYouTubeClient() as yt_client:
        try:
            data: str | None = await yt_client.get(url, timeout=1)
        except Exception:
            _LOGGER.debug(
                'Getting RSS data failed, sleeping {FAILURE_DELAY} seconds'
            )
            await asyncio.sleep(FAILURE_DELAY)
            raise

        await asyncio.sleep(random.uniform(0.2, 0.5))

    if not data:
        raise RuntimeError(f'No data received from RSS feed {url}')

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
            _LOGGER.warning(f'Skipping malformed RSS entry: {exc}')

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
        f'/param/{settings.schema_username}/youtube/video'
        f'/{settings.schema_version}/{video_id}'
    )
    response: httpx.Response = await client.get(url)

    if response.status_code == 200:
        return True
    if response.status_code == 404:
        return False
    raise RuntimeError(
        f'Unexpected status {response.status_code} checking video '
        f'{video_id}: {response.text}'
    )


async def store_video(
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
        response: httpx.Response = await client.post(
            f'{settings.exchange_url}{ExchangeClient.POST_DATA_API}',
            json={
                'username': settings.schema_username,
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
        _LOGGER.info(f'Failed to upload data: {exc}')
        return False

    if response.status_code == 201 or response.status_code == 500:
        # Status 500 is bug in server when video already exists
        return True

    _LOGGER.warning(
        f'Failed to store video {video.video_id}: '
        f'HTTP {response.status_code} - {response.text}'
    )
    return False


async def process_channel(
    channel_name: str, channel_id: str,
    client: ExchangeClient, settings: Settings
) -> None:
    '''
    Fetches the RSS feed for one channel and checks or stores each video.

    Raises on RSS fetch failure or if any video could not be stored, so
    the worker loop can schedule a retry for the whole channel.

    :param channel_name: Human-readable channel name (for logging).
    :param channel_id: YouTube channel ID.
    :param client: The authenticated Scrape Exchange API client.
    :param settings: Worker settings.
    :raises: httpx.HTTPError if the RSS feed cannot be retrieved.
    :raises: RuntimeError if one or more videos could not be stored.
    '''

    _LOGGER.info(f'Processing channel {channel_name!r} ({channel_id})')

    innertube: InnerTube = InnerTube('WEB')
    videos: list[YouTubeVideo] = await fetch_rss(channel_id)
    if not videos:
        await asyncio.sleep(random.uniform(0.2, 0.5))
        _LOGGER.info(f'No videos found in RSS feed for channel {channel_name!r}')
        return

    _LOGGER.debug(
        f'[{channel_name}] {len(videos)} video(s) in RSS feed'
    )

    videos_uploaded: int = 0
    videos_existing: int = 0
    global INNERTUBE_BLOCKED_TIMER
    for video in videos:
        try:
            if settings.with_innertube:
                if INNERTUBE_BLOCKED_TIMER < datetime.now(UTC):
                    await video.from_innertube(innertube=innertube)
                    _LOGGER.debug('Updated video with data from YouTube')
                else:
                    _LOGGER.debug(
                        f'Innertube blocked until {INNERTUBE_BLOCKED_TIMER}'
                    )
        except Exception as exc:
            INNERTUBE_BLOCKED_TIMER = \
                datetime.now(UTC) + timedelta(seconds=3600)
            _LOGGER.debug(
                'Failed to update video data, will continue with '
                f'what we have: {exc}'
            )

        try:
            filename: str = await video.to_file(
                settings.directory, filename_prefix=VIDEO_FILENAME_PREFIX,
                overwrite=True
            )
            _LOGGER.debug(f'Stored the file in {filename}')

            exists: bool = await check_video_exists(
                client, settings, video.video_id
            )
            if exists:
                _LOGGER.info(
                    f'[{channel_name}] {video.video_id} ({video.title!r}): '
                    f'already on scrape.exchange'
                )
                videos_existing += 1
            else:
                _LOGGER.debug(
                    f'[{channel_name}] {video.video_id} ({video.title!r}): '
                    f'not yet on scrape.exchange, storing'
                )
                stored: bool = await store_video(
                    client, settings, channel_name, video
                )
                if stored:
                    _LOGGER.debug(
                        f'[{channel_name}] {video.video_id} '
                        f'({video.title!r}): stored'
                    )
                    videos_uploaded += 1
                else:
                    _LOGGER.warning(
                        f'[{channel_name}] {video.video_id} '
                        f'({video.title!r}): store failed'
                    )
        except RuntimeError as exc:
            _LOGGER.warning(
                f'[{channel_name}] {video.video_id}: API error - {exc}'
            )

    missed: int = len(videos) - videos_uploaded - videos_existing
    _LOGGER.debug(
        f'{channel_name}: uploaded {videos_uploaded} videos, '
        f'{videos_existing} were already on the server'
    )
    if missed > 0:
        raise RuntimeError(
            f'{missed} out of {len(videos)} videos '
            f'for channel {channel_name!r} could not be processed'
        )


def get_channels(channel_db_file: str) -> dict[str, str]:
    '''
    Loads the wanted channels from the hardcoded wanted_channels dict.

    In a more advanced implementation, this could read from a config file or
    an API instead.

    :param directory: (not used in this implementation) Directory path for
                      potential future channel config files.
    :returns: A dict mapping channel names to YouTube channel IDs.
    '''

    channel_map: dict[str, str] = {}
    with open(channel_db_file) as f:
        for line in f:
            channel_id: str
            channel_name: str
            channel_id, channel_name = line.strip().split(',')
            if not channel_id or not channel_name:
                logging.warning(
                    f'Invalid channel mapping line: {line.strip()}')
                continue
            if channel_id in channel_map:
                logging.warning(
                    f'Duplicate channel ID {channel_id} in mapping file, '
                    f'overwriting previous name {channel_map[channel_id]}'
                )

            channel_map[channel_id] = channel_name
    return channel_map


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
    channels_seen: set[str] = set()
    queue_file: str = settings.queue_file
    temp_queue: list[list[float, str, str]] = []
    try:
        temp_queue = load_queue(queue_file)
    except Exception as exc:
        _LOGGER.warning(
            f'Failed to load queue file {queue_file!r}: {exc}'
        )

    if temp_queue:
        try:
            shutil.copyfile(queue_file, f'{queue_file}.{BACKUP_SUFFIX}')
            _LOGGER.debug(
                f'Backed up queue file to {queue_file!r}.{BACKUP_SUFFIX}'
            )
        except OSError as exc:
            _LOGGER.warning(
                f'Failed to back up queue file {queue_file!r}: {exc}'
            )
    else:
        try:
            backup_queue_file: str = f'{queue_file}.{BACKUP_SUFFIX}'
            shutil.copyfile(backup_queue_file, queue_file)
            temp_queue = load_queue(queue_file)
        except Exception as exc:
            _LOGGER.warning(
                f'Failed to load backup queue file {backup_queue_file!r}: {exc}'
            )

    if not temp_queue:
        _LOGGER.info('Starting with an empty queue')

    # Now create the actual queue with a list of tuples
    # ugh, (or)json does not support tuples but converts each tuple to a
    # list. Hopefully orjson keeps the list in the same order as the
    # original tuple, so we can just convert it back.
    queue: list[tuple[float, str, str]] = []
    for timestamp, name, channel_id in temp_queue:
        queue.append((timestamp, name, channel_id))

    channel_name: str
    channel_id: str
    channel_items = list(channels.items())
    random.shuffle(channel_items)
    for channel_id, channel_name in channel_items:
        if channel_name not in channels_seen:
            queue.append((now, channel_name, channel_id))
            channels_seen.add(channel_name)

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
        queue: list[list[float, str, str]] = orjson.loads(content)
        _LOGGER.info(
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

    channel_map: dict[str, str] = get_channels(settings.channel_db_file)
    queue: list[tuple[float, str, str]] = get_queue(
        settings, channel_map
    )

    _LOGGER.info(
        f'Worker started: {len(queue)} channel(s), '
        f'min_interval={settings.min_interval}s, '
        f'retry_interval={settings.retry_interval}s, '
        f'max_concurrent={settings.max_concurrent}, '
        f'discovered_channels={len(channel_map)}'
    )

    while True:
        now: float = datetime.now(UTC).timestamp()

        next_time: float = queue[0][0]
        if next_time > now:
            sleep_secs: float = next_time - now
            _LOGGER.debug(f'Sleeping {sleep_secs:.1f}s until next batch')
            await asyncio.sleep(sleep_secs)
            continue

        # Collect up to max_concurrent channels that are ready
        batch: list[tuple[float, str, str]] = []
        while (queue and queue[0][0] <= now
               and len(batch) < settings.max_concurrent):
            batch.append(heapq.heappop(queue))

        _LOGGER.info(
            'Batch: ' + ', '.join(name for _, name, _ in batch)
        )

        tasks: list[asyncio.Task] = [
            process_channel(name, channel_id, client, settings)
            for _, name, channel_id in batch
        ]
        results: list = await asyncio.gather(*tasks, return_exceptions=True)

        now = datetime.now(UTC).timestamp()
        for i, (_, name, channel_id) in enumerate(batch):
            result = results[i]
            if (isinstance(result, BaseException)
                    and not isinstance(result, FileExistsError)):
                _LOGGER.warning(f'Channel {name!r} failed ({result})')
                # next_check: float = now + settings.retry_interval
                next_check: float = now + settings.min_interval
            else:
                next_check: float = now + settings.min_interval
            heapq.heappush(queue, (next_check, name, channel_id))

        try:
            async with aiofiles.open(settings.queue_file, 'wb') as fd:
                await fd.write(
                    orjson.dumps(queue, option=orjson.OPT_INDENT_2)
                )
        except OSError as exc:
            _LOGGER.warning(
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
               '%(filename)s:'
               '%(funcName)s():'
               '%(lineno)d:'
               '%(message)s'
    )
    os.makedirs(settings.directory, exist_ok=True)

    client: ExchangeClient = await ExchangeClient.setup(
        api_key_id=settings.api_key_id,
        api_key_secret=settings.api_key_secret,
        exchange_url=settings.exchange_url,
    )

    async with client:
        await worker_loop(settings, client)


if __name__ == '__main__':
    asyncio.run(main())
