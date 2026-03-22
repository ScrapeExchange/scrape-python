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
from random import shuffle
import sys
import asyncio
import logging

from pathlib import Path
import orjson
import brotli
import aiofiles
import aiofiles.os

from httpx import Response

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from scrape_exchange.exchange_client import ExchangeClient
from scrape_exchange.util import CHANNEL_FILE_PREFIX
from scrape_exchange.youtube.youtube_channel import YouTubeChannel
from scrape_exchange.youtube.youtube_client import AsyncYouTubeClient
from scrape_exchange.youtube.youtube_video import DENO_PATH, PO_TOKEN_URL


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
    channel_data_directory: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'YOUTUBE_CHANNEL_DATA_DIR', 'channel_data_directory'
        ),
        description='Directory to save the scraped data',
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

    if not settings.api_key_id or not settings.api_key_secret:
        print(
            'Error: API key ID and secret must be provided via '
            '--api-key-id/--api-key-secret, environment variables '
            'API_KEY_ID/API_KEY_SECRET, or a .env file'
        )
        sys.exit(1)
    if not settings.channel_list:
        print(
            'Error: file containing channels to scrapemust be provided via '
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

    yt_client = AsyncYouTubeClient()

    if not settings.no_upload:
        await upload_channels(settings, client)

    if not settings.upload_only:
        await scrape_channels(settings, client, yt_client)


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
        return data.get('exists', False)
    elif resp.status_code == 404:
        return False
    else:
        logging.warning(
            f'Failed to check existence of channel {channel_name}: '
            f'Status code {resp.status_code}, response: {resp.text}'
        )
        # Assume the channel does not exist if there was an error checking
        return False


async def scrape_channels(settings: Settings, client: ExchangeClient,
                          yt_client: AsyncYouTubeClient) -> None:
    channel_names: set[str] = await read_channels(settings.channel_list)
    channel_names.discard('')  # Remove empty channel names if any
    logging.debug(
        f'Read {len(channel_names)} unique channel names from .lst files'
    )
    name: str
    errors: int = 0
    channel_list: list[str] = list(channel_names)
    shuffle(channel_list)
    for name in channel_list:
        channel_name: str = normalize_channel_name(name)
        failed: bool = await scrape_channel(
            settings, client, channel_name, yt_client
        )
        if failed:
            errors += 1
            if errors > 5:
                logging.critical('Too many errors encountered, aborting')
                raise RuntimeError(
                    'Too many errors encountered during scraping'
                )


async def upload_channels(settings: Settings, client: ExchangeClient
                          ) -> None:
    saved_filepath: str
    uploaded_filepath: str

    files: list[str] = [
        f for f in os.listdir(settings.channel_data_directory)
        if f.startswith(CHANNEL_FILE_PREFIX)
    ]
    logging.debug(
        f'Found {len(files)} channel files that may need to be uploaded'
    )
    for filename in files:
        if filename.endswith('failed'):
            logging.debug(
                f'Skipping previously failed upload file: {filename}'
            )
            continue
        channel_name: str = normalize_channel_name(
            filename[len(CHANNEL_FILE_PREFIX):-1*len('.json.br')]
        )
        saved_filepath, uploaded_filepath = get_file_paths(
            channel_name, settings.channel_data_directory
        )
        if await aiofiles.os.path.exists(uploaded_filepath):
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
    return f'{CHANNEL_FILE_PREFIX}{channel_name}.json.br'


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
                logging.warning(f'No videos found for channel {channel_name}')
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
            logging.debug(f'Downloaded channel {channel_name}')
        except RuntimeError as exc:
            logging.warning(
                f'Failed to scrape channel {channel_name}: {exc}'
            )
            # No need to fail because of network errors, we can just
            # keep downloading channels
            return False
        except Exception as exc:
            logging.error(
                f'Unexpected error while scraping channel {channel_name}: '
                f'{exc}'
            )
            return True

    if settings.no_upload:
        logging.debug(f'No upload flag set, skipping upload for channel '
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


async def read_channels(file_path: str) -> set[str]:
    '''
    Reads .lst files from the specified directory and extracts YouTube channel
    names.

    :param directory: The directory containing .lst files with channel names.
    :returns: A list of YouTube channel names.
    :raises: (none)
    '''

    logging.debug(f'Reading channel names from: {file_path}')

    channels: set[str] = set()
    async with aiofiles.open(file_path, 'r') as file_desc:
        async for line in file_desc:
            channel_name: str = line.split(' ')[0].strip()
            channels.add(channel_name)
    return channels


if __name__ == '__main__':
    asyncio.run(main())
