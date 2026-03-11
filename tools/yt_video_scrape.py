#!/usr/bin/env python3

'''
YouTube Video Upload Tool. Reads YouTube video files from a
specified directory. For each channel, checks whether it was already scraped; if not, scrapes it and saves to disk.
Then checks whether the scraped data was already uploaded; if not, uploads it
to Scrape Exchange and moves the file to an "uploaded" sub-directory.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import os
import asyncio
import logging

from pathlib import Path
from random import randint, shuffle

from httpx import Response

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from yt_dlp.YoutubeDL import YoutubeDL

from scrape_exchange.exchange_client import ExchangeClient

from scrape_exchange.youtube.youtube_video import YouTubeVideo
from scrape_exchange.youtube.youtube_client import AsyncYouTubeClient
from scrape_exchange.youtube.youtube_video import DENO_PATH, PO_TOKEN_URL

VIDEO_MIN_PREFIX = 'video-min-'
VIDEO_YTDLP_PREFIX = 'video-dlp-'
UPLOADED_DIRNAME: str = '/uploaded'
SLEEP_MIN_INTERVAL: int = 8
SLEEP_MAX_INTERVAL: int = 15
FAILURE_SLEEP_INTERVAL: int = 3600


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
        validation_alias=AliasChoices('NO_UPLOAD', 'no_upload'),
        description='Only perform the scraping step, skipping data upload',
    )

    video_data_directory: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'YOUTUBE_VIDEO_DATA_DIR', 'video_data_directory'
        ),
        description='Directory to save the scraped video data',
    )

    deno_path: str = Field(
        default=DENO_PATH,
        validation_alias=AliasChoices('DENO_PATH', 'deno_path'),
        description='Path to the Deno executable used for scraping',
    )
    po_token_url: str = Field(
        default=PO_TOKEN_URL,
        validation_alias=AliasChoices('PO_TOKEN_URL', 'po_token_url'),
        description='URL for the PO token used for authentication',
    )

    api_key_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices('API_KEY_ID', 'api_key_id'),
        description='API key ID for authenticating with the Scrape.Exchange API',
    )
    api_key_secret: str | None = Field(
        default=None,
        validation_alias=AliasChoices('API_KEY_SECRET', 'api_key_secret'),
        description='API key secret for authenticating with the Scrape.Exchange API',
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
    Main function to execute the YouTube video upload process.

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
    os.makedirs(
        settings.video_data_directory + UPLOADED_DIRNAME, exist_ok=True
    )
    logging.info(
        'Starting YouTube video upload tool with settings: '
        f'{settings.model_dump_json(indent=2)}'
    )
    if not settings.upload_only:
        logging.info('Starting video scraping process')
        await scrape_and_upload_videos(settings)


def video_uploaded(settings: Settings, video_id: str) -> str | None:
    '''
    Checks whether a video with the given ID has already been uploaded.

    :param settings: Configuration settings for the tool
    :param video_id: YouTube video ID to check
    :returns: Filepath of the uploaded video if it exists, None otherwise
    :raises: (none)
    '''

    uploaded_filepath: str = (
        f'{settings.video_data_directory}{UPLOADED_DIRNAME}/'
        f'{VIDEO_YTDLP_PREFIX}{video_id}.json.br'
    )
    if os.path.isfile(uploaded_filepath):
        try:
            min_filepath: str = (
                f'{settings.video_data_directory}/'
                f'{VIDEO_MIN_PREFIX}{video_id}.json.br'
            )
            if os.path.isfile(min_filepath):
                os.remove(min_filepath)
            scraped_filepath: str = (
                f'{settings.video_data_directory}/'
                f'{VIDEO_YTDLP_PREFIX}{video_id}.json.br'
            )
            if os.path.isfile(scraped_filepath):
                os.remove(scraped_filepath)
        except OSError:
            pass

        return uploaded_filepath

    return None


def video_scraped_not_uploaded(settings: Settings, video_id: str
                               ) -> str | None:
    '''
    Checks whether a video with the given ID has already been scraped
    with YT-DLP but not yet uploaded.

    :param settings: Configuration settings for the tool
    :param video_id: YouTube video ID to check
    :returns: Filepath of the scraped video if it exists, None otherwise
    :raises: (none)
    '''

    scraped_filepath: str = (
        f'{settings.video_data_directory}/'
        f'{VIDEO_YTDLP_PREFIX}{video_id}.json.br'
    )
    if os.path.isfile(scraped_filepath):
        try:
            min_filepath: str = (
                f'{settings.video_data_directory}/'
                f'{VIDEO_MIN_PREFIX}{video_id}.json.br'
            )
            if os.path.isfile(min_filepath):
                os.remove(min_filepath)
        except OSError:
            pass

        return scraped_filepath

    return None


async def scrape_and_upload_videos(settings: Settings) -> None:
    '''
    Scrapes YouTube video data and uploads it to Scrape Exchange.

    :param settings: Configuration settings for the tool
    :returns: (none)
    :raises: (none)
    '''

    exchange_client: ExchangeClient = await ExchangeClient.setup(
        api_key_id=settings.api_key_id,
        api_key_secret=settings.api_key_secret,
        exchange_url=settings.exchange_url,
    )

    browse_client = AsyncYouTubeClient()
    download_client: YoutubeDL = YouTubeVideo._setup_download_client(
        browse_client, settings.deno_path, settings.po_token_url
    )
    files: list[str] = [
        entry for entry in os.listdir(settings.video_data_directory)
        if entry.endswith('.json.br') and (
            entry.startswith(VIDEO_MIN_PREFIX)
            or entry.startswith(VIDEO_YTDLP_PREFIX)
        )
    ]
    shuffle(files)
    for entry in files:
        video_needs_scraping: bool = False

        video: YouTubeVideo
        if entry.startswith(VIDEO_MIN_PREFIX):
            video_needs_scraping = True
            video_id: str = entry[len(VIDEO_MIN_PREFIX):-len('.json.br')]
            video = await YouTubeVideo.from_file(
                video_id, settings.video_data_directory, VIDEO_MIN_PREFIX
            )
        elif entry.startswith(VIDEO_YTDLP_PREFIX):
            video_id: str = entry[len(VIDEO_YTDLP_PREFIX):-len('.json.br')]
            video = await YouTubeVideo.from_file(
                video_id, settings.video_data_directory, VIDEO_YTDLP_PREFIX
            )
        else:
            continue

        if video_uploaded(settings, video_id):
            logging.debug(
                f'Video {video_id} already uploaded, skipping'
            )
            continue

        if video_needs_scraping:
            try:
                video = await YouTubeVideo.scrape(
                    video_id, channel_name=video.channel_name,
                    channel_thumbnail=None, browse_client=browse_client,
                    download_client=download_client,
                    save_dir=settings.video_data_directory,
                    filename_prefix=VIDEO_YTDLP_PREFIX,
                    debug=settings.log_level == 'DEBUG'
                )
                await asyncio.sleep(
                    randint(SLEEP_MIN_INTERVAL, SLEEP_MAX_INTERVAL)
                )
            except Exception as exc:
                sleep: int = FAILURE_SLEEP_INTERVAL + randint(0, 60)
                extension: str = '.failed'
                error_val: str = str(exc).lower()
                if ('uploader has not made this video available in your country' in error_val           # noqa: E501
                        or 'this live event will begin in' in error_val
                        or 'this live event has ended' in error_val
                        or 'live stream recording is not available' in error_val                        # noqa: E501
                        or 'video unavailable' in error_val
                        or 'video is not available' in error_val
                        or 'this video is private' in error_val
                        or 'this video has been removed' in error_val
                        or 'this video is age restricted and only available on youtube' in error_val):  # noqa: E501
                    sleep = randint(SLEEP_MIN_INTERVAL, SLEEP_MAX_INTERVAL)
                    extension = '.unavailable'
                else:
                    logging.info(f'Failed to scrape video {video_id}: {exc}')
                    try:
                        os.rename(
                            os.path.join(settings.video_data_directory, entry),
                            os.path.join(
                                settings.video_data_directory,
                                entry + extension
                            )
                        )
                    except OSError:
                        pass

                await asyncio.sleep(sleep)
                continue

            try:
                os.remove(os.path.join(settings.video_data_directory, entry))
            except OSError:
                pass

        try:
            if not settings.no_upload and await upload_video(
                exchange_client, settings, video.channel_name, video
            ):
                await video.to_file(
                    settings.video_data_directory + UPLOADED_DIRNAME,
                    VIDEO_YTDLP_PREFIX
                )
                os.remove(
                    settings.video_data_directory + '/' + VIDEO_YTDLP_PREFIX +
                    video_id + '.json.br'
                )
                logging.info(f'Uploaded video {video.video_id}')
        except OSError:
            pass
        except Exception as exc:
            logging.info(f'Failed to upload video {video.video_id}: {exc}')


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
        logging.info(f'Failed to upload data: {exc}')
        return False

    if response.status_code == 201 or response.status_code == 500:
        # Status 500 is bug in server when video already exists
        return True

    logging.warning(
        f'Failed to store video {video.video_id}: '
        f'HTTP {response.status_code} - {response.text}'
    )
    return False

if __name__ == '__main__':
    asyncio.run(main())
