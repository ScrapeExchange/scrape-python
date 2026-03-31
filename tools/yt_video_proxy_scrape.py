#!/usr/bin/env python3

'''
YouTube Video Upload Tool. Reads YouTube video files from a
specified directory. For each channel, checks whether it was already scraped;
if not, scrapes it and saves to disk.
Then checks whether the scraped data was already uploaded; if not, uploads it
to Scrape Exchange and moves the file to an "uploaded" sub-directory.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import os
import sys
import time
import asyncio
import logging

from asyncio import Task, Queue
from pathlib import Path
from random import randint, shuffle

import aiofiles
import brotli

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
UPLOADED_DIRNAME: str = 'uploaded'
SLEEP_MIN_INTERVAL: int = 12
SLEEP_MAX_INTERVAL: int = 18
FAILURE_SLEEP_INTERVAL_MIN: int = 300
FAILURE_SLEEP_INTERVAL_MAX: int = 3600

FILE_EXTENSION: str = '.json.br'

START_TIME: float = time.monotonic()


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
        description=(
            'API key ID for authenticating with the Scrape.Exchange API'
        )
    )
    api_key_secret: str | None = Field(
        default=None,
        validation_alias=AliasChoices('API_KEY_SECRET', 'api_key_secret'),
        description=(
            'API key secret for authenticating with the Scrape.Exchange API'
        )
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
    max_files: int | None = Field(
        default=None,
        description='Maximum number of files to process in one run'
    )
    min_sleep: int = Field(
        default=SLEEP_MIN_INTERVAL,
        validation_alias=AliasChoices('MIN_SLEEP', 'min_sleep'),
        description='Minimum number of seconds to sleep between scrapes'
    )
    max_sleep: int = Field(
        default=SLEEP_MAX_INTERVAL,
        validation_alias=AliasChoices('MAX_SLEEP', 'max_sleep'),
        description='Maximum number of seconds to sleep between scrapes'
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
    status_file: str | None = Field(
        default='/tmp/proxy_status.log',
        validation_alias=AliasChoices('STATUS_FILE', 'status_file'),
        description='File to write status updates to',
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

    if not settings.proxies:
        print(
            'No proxies provided, you should use yt_video_scrape.py instead '
            'of yt_video_proxy_scrape.py'
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
    os.makedirs(
        os.path.join(settings.video_data_directory, UPLOADED_DIRNAME),
        exist_ok=True
    )
    logging.info(
        'Starting YouTube video upload tool with settings: '
        f'{settings.model_dump_json(indent=2)}'
    )

    logging.info('Starting video scraping process')
    await worker_loop(settings)


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


def video_needs_uploading(settings: Settings, filename: str) -> bool:
    '''
    Checks whether a video with the given ID needs to be uploaded.

    :param settings: Configuration settings for the tool
    :param video_id: YouTube video ID to check
    :returns: Whether file still needs to be uploaded
    :raises: (none)
    '''

    video_id: str
    if filename.startswith(VIDEO_MIN_PREFIX):
        video_id = filename[len(VIDEO_MIN_PREFIX):-len(FILE_EXTENSION)]
    else:
        video_id = filename[len(VIDEO_YTDLP_PREFIX):-len(FILE_EXTENSION)]

    uploaded_filepath: str = os.path.join(
        settings.video_data_directory, UPLOADED_DIRNAME,
        f'{VIDEO_YTDLP_PREFIX}{video_id}.json.br'
    )
    if os.path.exists(uploaded_filepath):
        try:
            if filename.startswith(VIDEO_MIN_PREFIX):
                os.remove(
                    os.path.join(settings.video_data_directory, filename)
                )
                return False

            if (filename.startswith(VIDEO_YTDLP_PREFIX)
                and os.path.getmtime(uploaded_filepath) >= os.path.getmtime(
                os.path.join(settings.video_data_directory, filename)
            )):
                os.remove(os.path.join(settings.video_data_directory, filename))
                return False
            else:
                return True
        except OSError:
            pass

        return False

    return True


async def prepare_workload(settings: Settings) -> Queue:
    '''
    Assess the workload by listing video files in the data directory and
    categorizing them based on their prefixes.

    :param settings: Configuration settings for the tool
    :returns: A dictionary mapping proxy URLs to lists of video file entries
    :raises: (none)
    '''

    files: list[str] = [
        entry for entry in os.listdir(settings.video_data_directory)
        if entry.endswith(FILE_EXTENSION) and (
            entry.startswith(VIDEO_MIN_PREFIX)
            or entry.startswith(VIDEO_YTDLP_PREFIX)
        ) and video_needs_uploading(settings, entry)
    ]
    shuffle(files)
    queue: Queue = Queue()
    for file in files:
        await queue.put(file)

    logging.debug(
        f'Prepared workload with {len(files)} video files to process'
    )
    return queue


async def worker_loop(settings: Settings) -> None:
    '''
    Main worker loop to continuously scrape and upload videos.

    :param settings: Configuration settings for the tool
    :returns: (none)
    :raises: (none)
    '''

    proxies: list[str] = settings.proxies.split(',')
    queue: Queue = await prepare_workload(settings)

    tasks: list[Task] = []
    count: int = 0
    for proxy in proxies:
        task: Task = asyncio.create_task(
            worker(proxy, queue, settings, count)
        )
        tasks.append(task)
        count += 1
    started_at = time.monotonic()
    await queue.join()
    total_slept_for = time.monotonic() - started_at

    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)

    logging.info(
        f'{len(proxies)} workers worked in parallel '
        f'for {total_slept_for:.2f} seconds'
    )


async def worker(proxy: str, queue: Queue, settings: Settings, instance: int
                 ) -> None:
    '''
    Worker function to process video files from the queue using a specific
    proxy.

    :param proxy: Proxy URL to use for scraping
    :param queue: Queue containing video file entries to process
    :param settings: Configuration settings for the tool
    :returns: (none)
    :raises: (none)
    '''

    proxy_count: int = len(settings.proxies.split(','))
    browse_client = AsyncYouTubeClient(proxies=[proxy])
    download_client: YoutubeDL = YouTubeVideo._setup_download_client(
        browse_client=browse_client, deno_path=settings.deno_path,
        po_token_url=settings.po_token_url, proxies=[proxy]
    )

    exchange_client: ExchangeClient = await ExchangeClient.setup(
        api_key_id=settings.api_key_id,
        api_key_secret=settings.api_key_secret,
        exchange_url=settings.exchange_url,
    )

    logging.debug(f'{instance}: Worker started with proxy: {proxy}')
    sleep: int = settings.min_sleep
    files_scraped: int = 0
    files_uploaded: int = 0
    while True:
        async with aiofiles.open(settings.status_file, 'a') as f:
            await f.write(
                f'{int(time.monotonic() - START_TIME)}: {instance}: '
                f'{proxy_count}, scraped: {files_scraped}, '
                f'uploaded: {files_uploaded}\n'
            )
        entry: str = await queue.get()

        logging.debug(
            f'{instance}: Worker with proxy {proxy} processing file: {entry}'
        )
        video_id: str
        video_needs_scraping: bool
        prefix: str
        if entry.startswith(VIDEO_MIN_PREFIX):
            # Video hasn't been scraped with YT-DLP yet
            prefix = VIDEO_MIN_PREFIX
            video_id = entry[len(VIDEO_MIN_PREFIX):-len(FILE_EXTENSION)]
            video_needs_scraping = True
        elif entry.startswith(VIDEO_YTDLP_PREFIX):
            prefix = VIDEO_YTDLP_PREFIX
            video_id = entry[len(VIDEO_YTDLP_PREFIX):-len(FILE_EXTENSION)]
            video_needs_scraping = False
        else:
            queue.task_done()
            continue

        try:
            video: YouTubeVideo = await YouTubeVideo.from_file(
                video_id, settings.video_data_directory, prefix
            )
        except FileNotFoundError:
            logging.warning(
                f'Video file {entry} not found, skipping'
            )
            queue.task_done()
            continue
        except brotli.error as exc:
            logging.warning(
                f'{instance}: Failed to decompress video file {entry}, '
                f'skipping: {exc}'
            )
            try:
                os.remove(os.path.join(settings.video_data_directory, entry))
            except OSError:
                pass
            queue.task_done()
            continue
        except Exception as exc:
            logging.warning(
                f'{instance}: Failed to read video file {entry}, skipping: {exc}'
            )
            queue.task_done()
            continue

        if not video_needs_uploading(settings, video_id):
            logging.debug(
                f'{instance}:Video {video_id} already uploaded, skipping'
            )
            try:
                os.remove(os.path.join(settings.video_data_directory, entry))
            except OSError:
                pass

            queue.task_done()
            continue

        if video_needs_scraping:
            try:
                video, sleep = await _scrape(
                    entry, video_id, video.channel_name,
                    browse_client, download_client,
                    settings, proxy, sleep
                )
                if not video:
                    os.rename(
                        os.path.join(settings.video_data_directory, entry),
                        os.path.join(
                            settings.video_data_directory,
                            entry + '.unavailable'
                        )
                    )
                    logging.info(f'{instance}: sleeping for {sleep} seconds')
                    await asyncio.sleep(sleep)
                    queue.task_done()
                    continue
            except Exception as exc:
                logging.info(f'Failed to scrape video {video_id}: {exc}')
                try:
                    os.rename(
                        os.path.join(settings.video_data_directory, entry),
                        os.path.join(
                            settings.video_data_directory,
                            entry + '.failed'
                        )
                    )
                except OSError:
                    pass
                logging.info(f'{instance}: sleeping for {sleep} seconds')
                await asyncio.sleep(sleep)
                video = None
                queue.task_done()
                continue

            files_scraped += 1
            if video is not None:
                try:
                    await video.to_file(
                        settings.video_data_directory, VIDEO_YTDLP_PREFIX
                    )
                    os.remove(
                        os.path.join(settings.video_data_directory, entry)
                    )
                except OSError:
                    pass

        if video is not None:
            try:
                if await upload_video(
                    exchange_client, settings, video.channel_name, video
                ):
                    files_uploaded += 1
                    await video.to_file(
                        os.path.join(
                            settings.video_data_directory, UPLOADED_DIRNAME
                        ),
                        VIDEO_YTDLP_PREFIX
                    )
                    os.remove(
                        os.path.join(
                            settings.video_data_directory,
                            f'{VIDEO_YTDLP_PREFIX}{video_id}{FILE_EXTENSION}'
                        )
                    )
                    logging.info(f'Uploaded video {video.video_id}')
            except OSError:
                pass
            except Exception as exc:
                logging.info(f'Failed to upload video {video_id}: {exc}')

        logging.info(f'{instance}: sleeping for {sleep} seconds')
        if sleep <= settings.max_sleep:
            sleep = randint(settings.min_sleep, settings.max_sleep)
        await asyncio.sleep(sleep)
        logging.info(
            f'{instance}: {proxy} files scraped: {files_scraped}, '
            f'files uploaded: {files_uploaded}'
        )
        queue.task_done()


async def _scrape(entry: str, video_id: str, channel_name: str,
                  browse_client: AsyncYouTubeClient,
                  download_client: YoutubeDL, settings: Settings,
                  proxy: str, sleep: int | None = None
                  ) -> tuple[YouTubeVideo | None, int]:
    '''
    Scrapes video data for a given video ID using yt-dlp. If video scraping
    fails due to rate limiting or transient errors, returns an integer
    indicating how long to sleep before the next attempt.

    :param entry: Filename of the video data file to scrape
    :param video_id: YouTube video ID to scrape
    :param channel_name: YouTube channel name associated with the video
    :param browse_client: AsyncYouTubeClient instance for browsing YouTube
    :param download_client: YoutubeDL instance for downloading video data
    :param settings: Configuration settings for the tool
    :param proxy: Proxy URL to use for scraping
    :param sleep: Optional integer indicating how long to sleep before scraping
    :returns: YouTubeVideo instance if scraping is successful, sleep duration
    in seconds if scraping fails due to rate limiting or transient errors.
    The value of the sleep duration should be used the next time this function
    is called.
    '''

    if sleep is None:
        sleep: int = randint(SLEEP_MIN_INTERVAL, SLEEP_MAX_INTERVAL)

    video: YouTubeVideo | None = None
    try:
        video: YouTubeVideo = await YouTubeVideo.scrape(
            video_id, channel_name=channel_name,
            channel_thumbnail=None, browse_client=browse_client,
            download_client=download_client,
            save_dir=settings.video_data_directory,
            filename_prefix=VIDEO_YTDLP_PREFIX,
            debug=settings.log_level == 'DEBUG',
            proxies=[proxy]
        )
        sleep: int = randint(SLEEP_MIN_INTERVAL, SLEEP_MAX_INTERVAL)
    except Exception as exc:
        error_val: str = str(exc).lower()
        if ('rate-limited by youtube' in error_val
                or 'VPN/Proxy Detected' in error_val
                or 'YouTube blocked' in error_val
                or 'captcha' in error_val
                or 'try again later' in error_val
                or 'the page needs to be reloaded' in error_val
                or 'Missing microformat data' in error_val
                or '429' in error_val):
            sleep = max(sleep, FAILURE_SLEEP_INTERVAL_MIN)
            logging.info(f'Rate limited during scraping: {exc}')
        elif ('video available in your country' in error_val
                or 'this live event will begin in' in error_val
                or 'this live event has ended' in error_val
                or 'live stream recording is not available' in error_val
                or 'video unavailable' in error_val
                or 'inappropriate' in error_val
                or 'video is not available' in error_val
                or 'this video is private' in error_val
                or 'this video has been removed' in error_val
                or 'video is age restricted' in error_val
                or "available to this channel's members on level" in error_val
                or "members-only content" in error_val):
            extension = '.unavailable'
            sleep: int = randint(SLEEP_MIN_INTERVAL, SLEEP_MAX_INTERVAL)
            logging.info(f'Video {video_id} not available for scraping: {exc}')
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
        elif ('offline' in error_val
                or 'timed out' in error_val
                or 'sslerror' in error_val
                or 'ssl:' in error_val
                or 'unable to connect to proxy' in error_val):
            logging.info(f'Transient failure during scraping: {exc}')
            # We keep sleep to the same value here as this issue is caused by
            # a proxy failure, not because of YouTube rate limiting.
        else:
            logging.info(f'Failed to scrape video {video_id}: {exc}')
            sleep = max(sleep, FAILURE_SLEEP_INTERVAL_MIN)

    if sleep > SLEEP_MAX_INTERVAL:
        if sleep < FAILURE_SLEEP_INTERVAL_MIN:
            sleep = max(sleep, FAILURE_SLEEP_INTERVAL_MIN)
        else:
            sleep *= 2
            if sleep > FAILURE_SLEEP_INTERVAL_MAX:
                sleep = FAILURE_SLEEP_INTERVAL_MAX

    return video, sleep


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

    if response.status_code == 201:
        logging.debug(
            f'Video {video.video_id} upload response: '
            f'HTTP {response.status_code}'
        )
        return True

    logging.warning(
        f'Failed to store video {video.video_id}: '
        f'HTTP {response.status_code} - {response.text}'
    )
    return False

if __name__ == '__main__':
    asyncio.run(main())
