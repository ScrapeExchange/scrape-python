#!/usr/bin/env python3

'''
YouTube Channel Upload Tool. Reads YouTube channel names from .lst files in a
specified directory (one channel name per word on each line). For each channel,
checks whether it was already scraped; if not, scrapes it and saves to disk.
Then checks whether the scraped data was already uploaded; if not, uploads it
to Scrape Exchange and moves the file to an "uploaded" sub-directory.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import os
import sys
import asyncio
import logging

from random import random, shuffle, choice
from pathlib import Path

import orjson
import brotli
import aiofiles
import aiofiles.os

from httpx import Response

from prometheus_client import Counter, Gauge, start_http_server

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from scrape_exchange.exchange_client import ExchangeClient
from scrape_exchange.util import CHANNEL_FILE_PREFIX
from scrape_exchange.youtube.youtube_channel import YouTubeChannel
from scrape_exchange.youtube.youtube_client import AsyncYouTubeClient
from scrape_exchange.youtube.youtube_video import DENO_PATH, PO_TOKEN_URL

CHANNEL_FILE_POSTFIX = '.json.br'


class Settings(BaseSettings):
    '''
    Tool configuration loaded in priority order:
    CLI flags > environment variables > .env file > built-in defaults.
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
        validation_alias=AliasChoices(
            'EXCHANGE_URL', 'exchange_url'
        ),
        description='Base URL for the Scrape.Exchange API',
    )
    schema_owner: str = Field(
        default='boinko',
        validation_alias=AliasChoices('SCHEMA_OWNER', 'schema_owner'),
        description='Username of the owner of the YouTube channel schema'
    )
    schema_version: str = Field(
        default='0.0.1',
        validation_alias=AliasChoices('SCHEMA_VERSION', 'schema_version'),
        description='Schema version string sent with uploads',
    )
    upload_only: bool = Field(
        default=False,
        validation_alias=AliasChoices('UPLOAD_ONLY', 'upload_only'),
        description='Only perform the upload step, skipping data scraping',
    )
    no_upload: bool = Field(
        default=False,
        validation_alias=AliasChoices('#NO_UPLOAD', '#no_upload'),
        description='Only perform the scraping step, skipping data upload',
    )
    channel_list: str = Field(
        default='channels.lst',
        validation_alias=AliasChoices(
            'YOUTUBE_CHANNEL_LIST', 'channel_list'
        )
    )
    existing_channels_list: str = Field(
        default='existing_channels.csv',
        validation_alias=AliasChoices(
            'YOUTUBE_EXISTING_CHANNEL_LIST', 'existing_channels_list'
        ),
        description=(
            'CSV file containing existing channel IDs and names '
            'to skip scraping (format: channel_id,channel_name).'
        )
    )
    channel_data_directory: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'YOUTUBE_CHANNEL_DATA_DIR', 'channel_data_directory'
        ),
        description='Directory to save the scraped data',
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
    api_key_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices('API_KEY_ID', 'api_key_id'),
        description='API key ID for authenticating with the Scrape.Exchange API',       # noqa: E501
    )
    api_key_secret: str | None = Field(
        default=None,
        validation_alias=AliasChoices('API_KEY_SECRET', 'api_key_secret'),
        description='API key secret for authenticating with the Scrape.Exchange API',   # noqa: E501
    )
    log_level: str = Field(
        default='INFO',
        validation_alias=AliasChoices('LOG_LEVEL', 'log_level'),
        description='Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)',
    )
    log_file: str = Field(
        default='/dev/stdout',
        validation_alias=AliasChoices('LOG_FILE', 'log_file'),
        description='Log file path',
    )
    concurrency: int = Field(
        default=3,
        validation_alias=AliasChoices(
            'MAX_CONCURRENT_CHANNELS', 'max_concurrent_channels'
        ),
        description='Number of channels to scrape concurrently',
    )
    proxies: str | None = Field(
        default=None,
        validation_alias=AliasChoices('PROXIES', 'proxies'),
        description=(
            'Comma-separated list of proxy URLs to use for scraping (e.g. '
            '"http://proxy1:port,http://proxy2:port"). If not set, no '
            'proxy will be used.'
        )
    )

    metrics_port: int = Field(
        default=9801,
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


# Prometheus metrics
METRIC_CHANNEL_EXISTS_FOUND = Counter(
    'yt_channel_exists_found_total',
    'Number of times a channel was found to already exist on Scrape Exchange',
)
METRIC_CHANNEL_EXISTS_NOT_FOUND = Counter(
    'yt_channel_exists_not_found_total',
    'Number of times a channel was found to not exist on Scrape Exchange',
)
METRIC_CHANNEL_EXISTS_FAILURES = Counter(
    'yt_channel_exists_check_failures_total',
    'Number of times the channel existence check call failed',
)
METRIC_UNIQUE_CHANNELS_READ = Gauge(
    'yt_channel_unique_channels_read',
    'Number of unique channel names read from the channel list',
)
METRIC_FILES_PENDING_UPLOAD = Gauge(
    'yt_channel_files_pending_upload',
    'Number of channel files found that may need to be uploaded',
)
METRIC_UPLOADED_FILE_EXISTS = Counter(
    'yt_channel_uploaded_file_exists_total',
    'Number of channels skipped because an uploaded file already exists',
)
METRIC_CHANNELS_SCRAPED = Counter(
    'yt_channel_scraped_total',
    'Number of channels successfully scraped',
)
METRIC_SCRAPE_FAILURES = Counter(
    'yt_channel_scrape_failures_total',
    'Number of times channel scraping failed',
)
METRIC_CHANNEL_IDS_TO_RESOLVE = Gauge(
    'yt_channel_ids_to_resolve',
    'Number of channel IDs that needed to be resolved to channel names',
)
METRIC_CHANNEL_IDS_RESOLVED = Counter(
    'yt_channel_ids_resolved_total',
    'Number of channel IDs successfully resolved to channel names',
)
METRIC_CHANNEL_ID_RESOLUTION_FAILURES = Counter(
    'yt_channel_id_resolution_failures_total',
    'Number of channel IDs that failed to resolve to channel names',
)


async def main() -> None:
    '''
    Main function to execute the YouTube channel upload process.

    :returns: (none)
    :raises: (none)
    '''

    settings: Settings = Settings()
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

    if not settings.api_key_id or not settings.api_key_secret:
        print(
            'Error: API key ID and secret must be provided via '
            '--api-key-id/--api-key-secret, environment variables '
            'API_KEY_ID/API_KEY_SECRET, or a .env file'
        )
        sys.exit(1)
    if not settings.channel_list:
        print(
            'Error: file containing channels to scrape must be provided via '
            '--channel-list or environment variable YOUTUBE_CHANNEL_LIST'
        )
        sys.exit(1)
    if not settings.channel_data_directory:
        print(
            'Error: Directory for scraped channel data must be provided via '
            '--channel-data-directory or environment variable '
            'YOUTUBE_CHANNEL_DATA_DIR'
        )
        sys.exit(1)
    if not os.path.isdir(settings.channel_data_directory):
        print(
            f'Directory {settings.channel_data_directory} does not exist. '
            'It will be created.'
        )
        os.makedirs(
            os.path.join(settings.channel_data_directory, 'uploaded'),
            exist_ok=True
        )
    if not os.path.isfile(settings.channel_list):
        print(
            f'Error: Channel list file {settings.channel_list} does not exist'
        )
        sys.exit(1)

    logging.info(
        'Starting YouTube channel upload tool with settings: '
        f'{settings.model_dump()}'
    )

    client: ExchangeClient | None = await ExchangeClient.setup(
        api_key_id=settings.api_key_id,
        api_key_secret=settings.api_key_secret,
        exchange_url=settings.exchange_url
    )

    yt_clients: list[AsyncYouTubeClient] = []
    if settings.proxies:
        for proxy in set(settings.proxies.split(',')):
            yt_clients.append(
                AsyncYouTubeClient(proxies=[proxy.strip()])
            )
    else:
        yt_clients.append(AsyncYouTubeClient())
        logging.info(
            'No proxies configured, using direct connection for scraping'
        )
        settings.concurrency = 1

    if not settings.no_upload:
        await upload_channels(settings, client)

    if not settings.upload_only:
        await scrape_channels(settings, client, yt_clients)


async def channel_exists(client: ExchangeClient, channel_name: str) -> bool:
    '''
    Checks if a channel with the given name already exists on Scrape Exchange.

    :param client: The Scrape Exchange client instance.
    :param channel_name: The name of the YouTube channel to check.
    :returns: True if the channel exists, False otherwise.
    :raises: (none)
    '''

    resp: Response = await client.get(
        f'{client.exchange_url}{ExchangeClient.GET_CONTENT_API}'
        f'/youtube/channel/{channel_name}'
    )

    if resp.status_code == 200:
        data: dict = resp.json()
        exists: bool = data.get('exists', False)
        if exists:
            METRIC_CHANNEL_EXISTS_FOUND.inc()
        else:
            METRIC_CHANNEL_EXISTS_NOT_FOUND.inc()
        return exists
    elif resp.status_code == 404:
        METRIC_CHANNEL_EXISTS_NOT_FOUND.inc()
        return False
    else:
        METRIC_CHANNEL_EXISTS_FAILURES.inc()
        logging.warning(
            f'Failed to check existence of channel {channel_name}: '
            f'Status code {resp.status_code}, response: {resp.text}'
        )
        # Assume the channel does not exist if there was an error checking
        return False


async def scrape_channels(settings: Settings, client: ExchangeClient,
                          yt_clients: list[AsyncYouTubeClient]) -> None:

    new_channels: set[str] = await read_channels(
        settings.channel_list, settings.existing_channels_list,
        settings.channel_map_file, settings.channel_data_directory,
        yt_clients, settings.concurrency,
    )

    logging.info(
        f'Read {len(new_channels)} unique channel names from .lst files'
    )
    channel_list: list[str] = list(new_channels)
    shuffle(channel_list)

    semaphore: asyncio.Semaphore = asyncio.Semaphore(settings.concurrency)

    async def worker(name: str) -> bool:
        channel_name: str = normalize_channel_name(name)
        async with semaphore:
            yt_client: AsyncYouTubeClient = choice(yt_clients)
            return await scrape_channel(
                settings, client, channel_name, yt_client
            )

    tasks: list[asyncio.Task] = [
        asyncio.create_task(worker(name)) for name in channel_list if name
    ]
    errors: int = 0
    for coro in asyncio.as_completed(tasks):
        if errors > 100:
            for task in tasks:
                task.cancel()
            logging.critical('Too many errors encountered, aborting')
            raise RuntimeError('Too many errors encountered during scraping')
        failed: bool = await coro
        if failed:
            errors += 1


async def upload_channels(settings: Settings, client: ExchangeClient
                          ) -> None:
    saved_filepath: str
    uploaded_filepath: str

    files: list[str] = [
        f for f in os.listdir(settings.channel_data_directory)
        if f.startswith(CHANNEL_FILE_PREFIX)
        and f.endswith(CHANNEL_FILE_POSTFIX)
    ]
    METRIC_FILES_PENDING_UPLOAD.set(len(files))
    logging.info(
        f'Found {len(files)} already scraped channel files that may need to be '
        'uploaded'
    )
    for filename in files:
        if filename.endswith('failed'):
            logging.debug(
                f'Skipping previously failed upload file: {filename}'
            )
            continue
        channel_name: str = normalize_channel_name(
            filename[len(CHANNEL_FILE_PREFIX):-1*len(CHANNEL_FILE_POSTFIX)]
        )
        saved_filepath, uploaded_filepath = get_file_paths(
            channel_name, settings.channel_data_directory
        )
        if await aiofiles.os.path.exists(uploaded_filepath):
            METRIC_UPLOADED_FILE_EXISTS.inc()
            logging.debug(
                f'Found existing uploaded file for channel {channel_name}, '
                'checking timestamps'
            )
            upload_timestamp: float = await aiofiles.os.stat(
                uploaded_filepath
            )
            save_timestamp: float = await aiofiles.os.stat(
                saved_filepath
            )
            if upload_timestamp.st_mtime >= save_timestamp.st_mtime:
                logging.debug(
                    f'Channel {channel_name} already uploaded, skipping'
                )
                try:
                    await aiofiles.os.remove(saved_filepath)
                except OSError:
                    pass
                continue

        try:
            async with aiofiles.open(saved_filepath, 'rb') as f:
                channel_data: str = orjson.loads(
                    brotli.decompress(await f.read())
                )
            channel: YouTubeChannel = YouTubeChannel.from_dict(
                channel_data
            )
            if await channel_exists(client, channel.name):
                logging.debug(
                    f'Channel {channel_name} already exists on Scrape '
                    'Exchange, skipping upload'
                )
                try:
                    await aiofiles.os.remove(saved_filepath)
                except OSError:
                    pass
                continue
            success: bool = await upload_channel(settings, client, channel)
            if success:
                await aiofiles.os.rename(saved_filepath, uploaded_filepath)
                logging.debug(
                    f'Successfully uploaded channel {channel_name}'
                )
            else:
                logging.warning(f'Failed to upload channel {channel_name}')
                await aiofiles.os.rename(
                    saved_filepath, saved_filepath + '.failed'
                )
        except Exception as exc:
            logging.error(
                f'Error processing channel file {saved_filepath}: {exc}'
            )


def normalize_channel_name(channel_name: str) -> str:
    '''
    Normalizes a YouTube channel name by stripping whitespace and converting
    to lowercase.

    :param channel_name: The original channel name.
    :returns: The normalized channel name.
    '''

    name: str = channel_name.strip().lstrip('@')
    if name.startswith('https://'):
        name = name.split('/')[-1]
        logging.debug(
            f'Extracted channel name from URL: {channel_name} -> {name}'
        )
    # If the name is an email address
    if '@' in name:
        name = name.split('@')[0]
        logging.debug(
            f'Extracted channel name from email: {channel_name} -> {name}')

    return name


def get_file_paths(channel_name: str, channel_data_directory: str
                   ) -> tuple[str, str]:
    filename: str = get_channel_filename(channel_name)
    saved_filepath: str = os.path.join(
        channel_data_directory, filename
    )
    uploaded_filepath: str = os.path.join(
        channel_data_directory, 'uploaded', filename
    )
    return saved_filepath, uploaded_filepath


def get_channel_filename(channel_name: str) -> str:
    return f'{CHANNEL_FILE_PREFIX}{channel_name}{CHANNEL_FILE_POSTFIX}'


async def scrape_channel(settings: Settings, client: ExchangeClient,
                         channel_name: str, yt_client: AsyncYouTubeClient
                         ) -> bool:
    '''
    Scrapes a single YouTube channel and uploads it to the Scrape Exchange.

    :param settings: Tool settings.
    :param client: The Scrape Exchange client instance.
    :param channel_name: The name of the YouTube channel to scrape.
    :returns: whether channel scraping/uploading failed
    :raises: (none)
    '''

    logging.debug(f'Processing channel: {channel_name}')
    saved_filepath: str
    uploaded_filepath: str
    saved_filepath, uploaded_filepath = get_file_paths(
        channel_name, settings.channel_data_directory
    )
    upload_timestamp: float = 0
    if await aiofiles.os.path.exists(uploaded_filepath):
        upload_timestamp = await aiofiles.os.stat(
            uploaded_filepath
        )
        logging.debug(
            f'Found {uploaded_filepath} for channel {channel_name}'
        )
    saved_timestamp: float = 0
    if await aiofiles.os.path.exists(saved_filepath):
        saved_timestamp = await aiofiles.os.stat(saved_filepath)

        logging.debug(
            f'Found {saved_filepath} for channel {channel_name}'
        )
    if await aiofiles.os.path.exists(saved_filepath + '.failed'):
        logging.debug(
            f'Found previously failed upload file for channel {channel_name}, '
            'skipping'
        )
        saved_filepath += '.failed'
        saved_timestamp = await aiofiles.os.stat(saved_filepath)

    if upload_timestamp:
        if (saved_timestamp
                and upload_timestamp.st_mtime >= saved_timestamp.st_mtime):
            logging.debug(
                f'Channel {channel_name} already uploaded, skipping'
            )
            try:
                await aiofiles.os.remove(saved_filepath)
            except OSError:
                pass

        # If the channel was already uploaded then there is nothing to do
        return False

    if await channel_exists(client, channel_name):
        logging.debug(
            f'Channel {channel_name} already exists on Scrape '
            'Exchange, skipping upload'
        )
        try:
            await aiofiles.os.remove(saved_filepath)
        except OSError:
            pass
        return False

    if not saved_timestamp:
        logging.debug(f'Channel {channel_name} not scraped, scraping now')
        channel: YouTubeChannel = YouTubeChannel(
            name=channel_name, deno_path=DENO_PATH,
            po_token_url=PO_TOKEN_URL, debug=True,
            save_dir=settings.channel_data_directory,
            browse_client=yt_client
        )
        try:
            await channel.scrape(max_videos_per_channel=0)
            if not channel.video_ids:
                logging.debug(f'No videos found for channel {channel_name}')
                await asyncio.sleep(random() * 5)
                return False
            data: bytes = orjson.dumps(
                channel.to_dict(with_video_ids=True),
                option=orjson.OPT_INDENT_2
            )
            compressed: bytes = brotli.compress(
                data,
                mode=brotli.MODE_TEXT,
                quality=11,
                lgwin=22
            )
            async with aiofiles.open(saved_filepath, 'wb') as fd:
                await fd.write(compressed)
            METRIC_CHANNELS_SCRAPED.inc()
            logging.debug(f'Downloaded channel {channel_name}')
        except RuntimeError as exc:
            METRIC_SCRAPE_FAILURES.inc()
            logging.warning(
                f'Failed to scrape channel {channel_name}: {exc}'
            )
            # No need to fail because of network errors, we can just
            # keep downloading channels
            await asyncio.sleep(random() * 5)
            return False
        except Exception as exc:
            METRIC_SCRAPE_FAILURES.inc()
            logging.warning(
                f'Unexpected error while scraping channel {channel_name}: '
                f'{exc}'
            )
            await asyncio.sleep(random() * 5)
            return True

    if settings.no_upload:
        logging.debug(f'No-upload flag set, skipping upload for channel '
                      f'{channel_name}')
        return False

    logging.debug(f'Uploading channel {channel_name} to Scrape Exchange')
    try:
        success: bool = await upload_channel(settings, client, channel)
        if success:
            try:
                await aiofiles.os.rename(
                    saved_filepath, uploaded_filepath
                )
            except OSError:
                pass
            logging.debug(f'Successfully uploaded channel {channel_name}')
        return False
    except Exception as exc:
        logging.info(f'Error uploading channel {channel_name}: {exc}')

    return False


async def upload_channel(settings: Settings, client: ExchangeClient,
                         channel: YouTubeChannel) -> bool:
    resp: Response = await client.post(
        f'{settings.exchange_url}{client.POST_DATA_API}', json={
            'username': settings.schema_owner,
            'platform': 'youtube',
            'entity': 'channel',
            'version': settings.schema_version,
            'source_url': channel.url,
            'data': channel.to_dict(with_video_ids=False),
            'platform_content_id': channel.name,
            'platform_creator_id': channel.name,
            'platform_topic_id': None
        }
    )

    if resp.status_code == 201:
        return True
    else:
        logging.warning(
            f'Failed to upload channel {channel.name}: '
            f'Status code {resp.status_code}, response: {resp.text}'
        )
        # No need to fail, we can just keep downloading and trying to
        # upload again in the future
        return False


async def read_existing_channels(file_path: str) -> dict[str, str]:
    '''
    Reads existing channel files from the specified directory and extracts
    channel names.

    :param directory: The directory containing existing channel files.
    :returns: A dict with existing YouTube channel IDs as keys and names as
    values  .
    :raises: (none)
    '''

    logging.info(f'Reading existing channel names from: {file_path}')

    channels: dict[str, str] = {}
    if not os.path.isfile(file_path):
        logging.warning(f'File {file_path} does not exist, returning empty set')
        return channels

    line: str
    async with aiofiles.open(file_path, 'r') as file_desc:
        async for line in file_desc:
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            if ',' in line:
                channel_id: str
                channel_name: str
                channel_id, channel_name = line.split(',', 1)
                channels[channel_id] = channel_name
            else:
                channels[line] = line

    return channels


async def read_channels(file_path: str, existing_channel_file: str,
                        channel_map_file: str, data_dir: str,
                        yt_clients: list[AsyncYouTubeClient],
                        concurrency: int = 3) -> set[str]:
    '''
    Reads .lst files from the specified directory and extracts YouTube channel
    names. This function accepts:
    - Lines that start with 'UC' and are 24 characters long, which are treated
      as channel IDs (these will be resolved to channel names later).
    - Lines that contain a tab character, where the channel name is expected to
      be the second word (after the tab).
    - Lines that start with youtube URL

    :param directory: The directory containing .lst files with channel names.
    :param concurrency: Number of channel ID resolutions to run concurrently.
    :returns: A list of YouTube channel names.
    :raises: (none)
    '''

    logging.info(f'Reading channel names from: {file_path}')

    existing_channels: dict[str, str] = await read_existing_channels(
        existing_channel_file
    )
    channel_map: dict[str, str] = await read_existing_channels(
        channel_map_file
    )
    existing_channel_names: set[str] = set(existing_channels.values())
    new_channel_names: set[str] = set()
    # Channel IDs that need to be resolved to names
    unresolved_ids: list[str] = []
    line: str
    async with aiofiles.open(file_path, 'r') as file_desc:
        async for line in file_desc:
            line = line.strip()
            if not line:
                continue
            channel_name: str | None = None
            if ',' in line:
                _: str
                _, channel_name = line.split(',', 1)
                channel_name = channel_name.strip()
            if not channel_name and YouTubeChannel.is_channel_id(line):
                if line in existing_channels or line in existing_channel_names:
                    continue
                if line in channel_map:
                    channel_name = channel_map[line]
                else:
                    unresolved_ids.append(line)
                    continue
            elif line.startswith('https://www.youtube.com/@'):
                channel_name = line[len('https://www.youtube.com/@'):].strip()
            elif (line.startswith('handle') or line.startswith('custom')
                    or line.startswith('user')):
                parts: list[str] = line.split('\\')
                if len(parts) >= 2:
                    channel_name = parts[1].strip()
            elif '\t' in line:
                channel_name = line.split('\t')[1].strip()
            else:
                channel_name = line

            if channel_name and channel_name not in existing_channel_names:
                new_channel_names.add(channel_name)

    if unresolved_ids:
        METRIC_CHANNEL_IDS_TO_RESOLVE.set(len(unresolved_ids))
        semaphore: asyncio.Semaphore = asyncio.Semaphore(concurrency)
        map_lock: asyncio.Lock = asyncio.Lock()

        async def resolve(channel_id: str) -> str | None:
            async with semaphore:
                try:
                    name: str = await YouTubeChannel.resolve_channel_id(
                        channel_id, choice(yt_clients)
                    )
                    if not name:
                        unresolved_file_path: str = os.path.join(
                            data_dir, f'channel-{channel_id}.unresolved'
                        )
                        if os.path.exists(unresolved_file_path):
                            logging.debug(
                                'Channel ID previously failed to'
                                f' resolve, skipping: {channel_id}'
                            )
                        else:
                            logging.warning(
                                f'Failed to resolve channel ID {channel_id}, '
                                f'touching {unresolved_file_path}'
                            )
                            async with aiofiles.open(unresolved_file_path, 'w'
                                                     ) as f:
                                await f.write(f'{channel_id}\n')
                        await asyncio.sleep(random() * 5)
                        METRIC_CHANNEL_ID_RESOLUTION_FAILURES.inc()
                        return None

                    async with map_lock:
                        async with aiofiles.open(
                                channel_map_file, 'a') as f:
                            await f.write(f'{channel_id},{name}\n')
                    logging.debug(
                        f'Resolved channel ID {channel_id} to name {name}'
                    )
                    METRIC_CHANNEL_IDS_RESOLVED.inc()
                    await asyncio.sleep(random())
                    return name
                except Exception as e:
                    METRIC_CHANNEL_ID_RESOLUTION_FAILURES.inc()
                    logging.debug(
                        f'Error while resolving channel ID {channel_id}: {e}'
                    )
                    await asyncio.sleep(random() * 5)
                    return None
        ids: list[str] = list(unresolved_ids)
        shuffle(ids)

        results: list[str | None] = await asyncio.gather(
            *(resolve(cid) for cid in ids)
        )
        for name in results:
            if name and name not in existing_channel_names:
                new_channel_names.add(name)

    METRIC_UNIQUE_CHANNELS_READ.set(len(new_channel_names))
    logging.info(
        f'Read {len(new_channel_names)} unique channel names from {file_path}'
    )
    new_channel_names.discard('')  # Remove empty channel names if any
    new_channel_names.discard(None)
    return new_channel_names


if __name__ == '__main__':
    asyncio.run(main())
