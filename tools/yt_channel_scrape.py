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
import signal
import sys
import asyncio
import logging
import resource

from random import shuffle
from pathlib import Path

import aiofiles

from httpx import Response

from prometheus_client import Counter, Gauge, start_http_server

from pydantic import AliasChoices, Field, field_validator

from scrape_exchange.exchange_client import ExchangeClient

from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.file_management import CHANNEL_FILE_PREFIX
from scrape_exchange.logging import configure_logging
from scrape_exchange.scraper_supervisor import (
    SupervisorConfig,
    publish_config_metrics,
    run_supervisor,
)
from scrape_exchange.settings import normalize_log_level

from scrape_exchange.youtube.youtube_channel import YouTubeChannel
from scrape_exchange.scrape_exchange_rate_limiter import (
    ScrapeExchangeRateLimiter,
)
from scrape_exchange.youtube.youtube_rate_limiter import YouTubeRateLimiter
from scrape_exchange.youtube.youtube_video import DENO_PATH, PO_TOKEN_URL
from scrape_exchange.worker_id import get_worker_id
from watchfiles import awatch, Change
from scrape_exchange.youtube.settings import YouTubeScraperSettings

CHANNEL_FILE_POSTFIX = '.json.br'

MAX_NEW_CHANNELS: int = 1000
MAX_RESOLVED_CHANNELS: int = 100


class ChannelSettings(YouTubeScraperSettings):
    '''
    Tool configuration loaded in priority order:
    CLI flags > environment variables > .env file > built-in defaults.
    '''

    channel_upload_only: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            'CHANNEL_UPLOAD_ONLY',
            'channel_upload_only',
        ),
        description=(
            'Only perform the upload step, skipping '
            'channel scraping'
        ),
    )
    channel_no_upload: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            'CHANNEL_NO_UPLOAD',
            'channel_no_upload',
        ),
        description=(
            'Only perform the scraping step, skipping '
            'data upload'
        ),
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
    max_new_channels: int = Field(
        default=MAX_NEW_CHANNELS,
        validation_alias=AliasChoices(
            'MAX_NEW_CHANNELS', 'max_new_channels'
        ),
        description=(
            'Maximum number of new channels to scrape in this run (channels '
            'that have already been scraped or marked as not found are not '
            'counted against this limit).'
        )
    )
    max_resolved_channels: int = Field(
        default=MAX_RESOLVED_CHANNELS,
        validation_alias=AliasChoices(
            'MAX_NEW_CHANNELS', 'max_new_channels'
        ),
        description=(
            'Maximum number of new channels to scrape in this run (channels '
            'that have already been scraped or marked as not found are not '
            'counted against this limit).'
        )
    )
    metrics_port: int = Field(
        default=9600,
        validation_alias=AliasChoices(
            'CHANNEL_METRICS_PORT', 'channel_metrics_port'
        ),
        description='Port for the Prometheus metrics HTTP server',
    )
    channel_concurrency: int = Field(
        default=3,
        validation_alias=AliasChoices(
            'CHANNEL_CONCURRENCY', 'channel_concurrency'
        ),
        description=(
            'Number of channels to scrape concurrently inside '
            'one channel scraper process. Channel-scraper-'
            'specific so the video and RSS scrapers can keep '
            'their own concurrency settings independent. '
            'Automatically clamped to 1 when no proxies are '
            'configured.'
        ),
    )
    channel_num_processes: int = Field(
        default=1,
        validation_alias=AliasChoices(
            'CHANNEL_NUM_PROCESSES', 'channel_num_processes'
        ),
        description=(
            'Number of child channel scraper processes to spawn. '
            'When > 1 the invocation becomes a supervisor that '
            'splits the proxy pool into N disjoint chunks and '
            'spawns one child per chunk. Each child runs with '
            'CHANNEL_NUM_PROCESSES=1, gets its own METRICS_PORT '
            '(base + worker_instance, with base reserved for the '
            'supervisor and worker_instance starting at 1) and '
            'log file, if specified.'
        ),
    )
    channel_log_level: str = Field(
        default='INFO',
        validation_alias=AliasChoices(
            'CHANNEL_LOG_LEVEL', 'channel_log_level',
            'LOG_LEVEL', 'log_level',
        ),
        description=(
            'Logging level for the channel scraper '
            '(DEBUG, INFO, WARNING, ERROR, CRITICAL). Honours '
            'CHANNEL_LOG_LEVEL first so this scraper can be '
            'dialled up independently of the video and RSS '
            'scrapers; falls back to LOG_LEVEL when the scraper-'
            'specific var is unset.'
        ),
    )
    channel_log_file: str = Field(
        default='/dev/stdout',
        validation_alias=AliasChoices(
            'CHANNEL_LOG_FILE', 'channel_log_file',
            'LOG_FILE', 'log_file',
        ),
        description=(
            'Log file path for the channel scraper. Honours '
            'CHANNEL_LOG_FILE first so each scraper can write to '
            'its own file; falls back to LOG_FILE when the '
            'scraper-specific var is unset.'
        ),
    )

    @field_validator('channel_log_level', mode='before')
    @classmethod
    def _normalize_channel_log_level(cls, v: str) -> str:
        return normalize_log_level(v)


# Prometheus metrics
METRIC_CHANNEL_EXISTS_FOUND = Counter(
    'yt_channel_exists_found_total',
    'Number of times a channel was found to already exist on Scrape Exchange',
    ['worker_id'],
)
METRIC_CHANNEL_EXISTS_NOT_FOUND = Counter(
    'yt_channel_exists_not_found_total',
    'Number of times a channel was found to not exist on Scrape Exchange',
    ['worker_id'],
)
METRIC_CHANNEL_EXISTS_FAILURES = Counter(
    'yt_channel_exists_check_failures_total',
    'Number of times the channel existence check call failed',
    ['worker_id'],
)
METRIC_UNIQUE_CHANNELS_READ = Gauge(
    'yt_channel_unique_channels_read',
    'Number of unique channel names read from the channel list',
    ['worker_id'],
)
METRIC_FILES_PENDING_UPLOAD = Gauge(
    'yt_channel_files_pending_upload',
    'Number of channel files found that may need to be uploaded',
    ['worker_id'],
)
METRIC_UPLOADED_FILE_EXISTS = Counter(
    'yt_channel_uploaded_file_exists_total',
    'Number of channels skipped because an uploaded file already exists',
    ['worker_id'],
)
METRIC_CHANNELS_SCRAPED = Counter(
    'yt_channel_scraped_total',
    'Number of channels successfully scraped',
    ['worker_id'],
)
METRIC_CHANNELS_ENQUEUED = Counter(
    'yt_channel_enqueued_total',
    'Number of channels successfully enqueued for background '
    'upload. Actual delivery is tracked by '
    'exchange_client_background_uploads_total{entity="channel"}.',
    ['worker_id'],
)
METRIC_SCRAPE_FAILURES = Counter(
    'yt_channel_scrape_failures_total',
    'Number of times channel scraping failed',
    ['worker_id'],
)
METRIC_CHANNEL_IDS_TO_RESOLVE = Gauge(
    'yt_channel_ids_to_resolve',
    'Number of channel IDs that needed to be resolved to channel names',
    ['worker_id'],
)
METRIC_CHANNEL_IDS_RESOLVED = Counter(
    'yt_channel_ids_resolved_total',
    'Number of channel IDs successfully resolved to channel names',
    ['worker_id'],
)
METRIC_CHANNEL_ID_RESOLUTION_FAILURES = Counter(
    'yt_channel_id_resolution_failures_total',
    'Number of channel IDs that failed to resolve to channel names',
    ['worker_id'],
)
METRIC_CHANNEL_NO_CONTENT_FOUND = Counter(
    'yt_channel_no_content_found_total',
    'Number of channels scraped that had no videos, playlists, courses, '
    'podcasts, or products',
    ['worker_id'],
)

# -- upload-only watcher metrics --
METRIC_WATCHER_FILES_DETECTED = Counter(
    'yt_channel_watcher_files_detected_total',
    'Files detected by the upload-only file watcher',
    ['worker_id'],
)
METRIC_WATCHER_FILES_SKIPPED = Counter(
    'yt_channel_watcher_files_skipped_total',
    'Files skipped by the watcher (already uploaded '
    'or superseded)',
    ['worker_id'],
)
METRIC_WATCHER_BATCHES = Counter(
    'yt_channel_watcher_batches_total',
    'Number of change batches yielded by the file '
    'watcher',
    ['worker_id'],
)


def _validate_settings(settings: ChannelSettings) -> None:
    '''
    Validate settings that are required for either the supervisor
    or the worker to run. Exits the process with code 1 and an
    error message on any violation.
    '''

    if not settings.api_key_id or not settings.api_key_secret:
        print(
            'Error: API key ID and secret must be provided via '
            '--api-key-id/--api-key-secret, environment variables '
            'API_KEY_ID/API_KEY_SECRET, or a .env file'
        )
        sys.exit(1)
    if not settings.channel_list:
        print(
            'Error: file containing channels to scrape must be '
            'provided via --channel-list or environment variable '
            'YOUTUBE_CHANNEL_LIST'
        )
        sys.exit(1)
    if not settings.channel_data_directory:
        print(
            'Error: Directory for scraped channel data must be '
            'provided via --channel-data-directory or environment '
            'variable YOUTUBE_CHANNEL_DATA_DIR'
        )
        sys.exit(1)
    if not os.path.isdir(settings.channel_data_directory):
        print(
            f'Directory {settings.channel_data_directory} does '
            'not exist. It will be created.'
        )
        os.makedirs(settings.channel_data_directory, exist_ok=True)
    if not os.path.isfile(settings.channel_list):
        print(
            f'Error: Channel list file {settings.channel_list} '
            'does not exist'
        )
        sys.exit(1)


async def _run_worker(settings: ChannelSettings) -> None:
    '''
    Run a single in-process channel scraper worker (the leaf of
    the supervisor tree). Binds the Prometheus HTTP server,
    publishes the configuration gauges for this process, wires up
    signal handlers, and runs the upload + scrape passes.
    '''

    _soft, _hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    _target = _hard if _hard != resource.RLIM_INFINITY else 1048576
    resource.setrlimit(resource.RLIMIT_NOFILE, (_target, _hard))

    logging.info(
        'Starting YouTube channel upload tool',
        extra={'settings': settings.model_dump()},
    )
    worker_proxies: list[str] = [
        p.strip() for p in settings.proxies.split(',') if p.strip()
    ] if settings.proxies else []
    logging.info(
        'Channel scraper worker started',
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
        role='worker', scraper_label='channel',
        num_processes=1,
        concurrency=settings.channel_concurrency,
    )

    # AssetFileManagement creates the 'uploaded' subdirectory
    # automatically and owns all read/write/marker operations
    # under channel_data_directory.
    fm: AssetFileManagement = AssetFileManagement(
        settings.channel_data_directory
    )

    try:
        client: ExchangeClient | None = (
            await ExchangeClient.setup(
                api_key_id=settings.api_key_id,
                api_key_secret=settings.api_key_secret,
                exchange_url=settings.exchange_url,
            )
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
        settings.channel_num_processes
        * settings.channel_concurrency,
    ))
    ScrapeExchangeRateLimiter.get(
        state_dir=settings.rate_limiter_state_dir,
        post_rate=post_rate,
        redis_dsn=settings.redis_dsn,
    )

    if not settings.proxies:
        logging.info(
            'No proxies configured, using direct connection '
            'for scraping'
        )
        settings.channel_concurrency = 1
        publish_config_metrics(
            role='worker', scraper_label='channel',
            num_processes=1,
            concurrency=settings.channel_concurrency,
        )

    # Wire SIGINT/SIGTERM to cancel the current task so the finally
    # block gets a chance to drain the ExchangeClient upload queue.
    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
    main_task: asyncio.Task = asyncio.current_task()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, main_task.cancel)
        except NotImplementedError:
            pass

    try:
        if not settings.channel_no_upload:
            await upload_channels(settings, client, fm)

        if settings.channel_upload_only:
            await _watch_and_upload_channels(
                settings, client, fm,
            )
        else:
            await scrape_channels(settings, client, fm)
    except asyncio.CancelledError:
        logging.info(
            'Shutdown signal received; '
            'draining background uploads'
        )
        raise
    finally:
        if client is not None:
            await client.drain_uploads(timeout=10.0)


def main() -> None:
    '''
    Top-level entry point. Reads settings and dispatches to
    either the shared supervisor (when
    ``channel_num_processes > 1``) or the in-process scraper
    worker (when ``channel_num_processes == 1``).
    '''

    settings: ChannelSettings = ChannelSettings()
    configure_logging(
        level=settings.channel_log_level,
        filename=settings.channel_log_file,
        log_format=settings.log_format,
    )
    _validate_settings(settings)

    if settings.channel_upload_only:
        settings.channel_num_processes = 1
        settings.metrics_port = settings.metrics_port - 1
        logging.info(
            'Upload-only mode: forcing single process '
            f'with metrics port {settings.metrics_port}'
        )

    if settings.channel_num_processes > 1:
        sys.exit(run_supervisor(SupervisorConfig(
            scraper_label='channel',
            num_processes_env_var='CHANNEL_NUM_PROCESSES',
            log_file_env_var='CHANNEL_LOG_FILE',
            num_processes=settings.channel_num_processes,
            concurrency=settings.channel_concurrency,
            proxies=settings.proxies,
            metrics_port=settings.metrics_port,
            log_file=settings.channel_log_file or None,
            api_key_id=settings.api_key_id,
            api_key_secret=settings.api_key_secret,
            exchange_url=settings.exchange_url,
        )))

    try:
        asyncio.run(_run_worker(settings))
    except asyncio.CancelledError:
        logging.info('Channel scraper shutdown complete')


async def channel_exists(client: ExchangeClient, channel_name: str) -> bool:
    '''
    Checks if a channel with the given name already exists on Scrape Exchange.

    :param client: The Scrape Exchange client instance.
    :param channel_name: The name of the YouTube channel to check.
    :returns: True if the channel exists, False otherwise.
    :raises: (none)
    '''

    try:
        resp: Response = await client.get(
            f'{client.exchange_url}{ExchangeClient.GET_CONTENT_API}'
            f'/youtube/channel/{channel_name}'
        )
    except Exception as exc:
        METRIC_CHANNEL_EXISTS_FAILURES.labels(
            worker_id=get_worker_id()
        ).inc()
        logging.warning(
            'Network error checking channel existence',
            exc=exc,
            extra={'channel_name': channel_name},
        )
        return False

    if resp.status_code == 200:
        data: dict = resp.json()
        exists: bool = data.get('exists', False)
        if exists:
            METRIC_CHANNEL_EXISTS_FOUND.labels(
                worker_id=get_worker_id()
            ).inc()
        else:
            METRIC_CHANNEL_EXISTS_NOT_FOUND.labels(
                worker_id=get_worker_id()
            ).inc()
        return exists
    elif resp.status_code == 404:
        METRIC_CHANNEL_EXISTS_NOT_FOUND.labels(
            worker_id=get_worker_id()
        ).inc()
        return False
    else:
        METRIC_CHANNEL_EXISTS_FAILURES.labels(
            worker_id=get_worker_id()
        ).inc()
        logging.warning(
            'Failed to check existence of channel',
            extra={
                'channel_name': channel_name,
                'status_code': resp.status_code,
                'response_text': resp.text,
            },
        )
        # Assume the channel does not exist if there was
        # an error checking
        return False


async def scrape_channels(
    settings: ChannelSettings,
    client: ExchangeClient,
    fm: AssetFileManagement,
) -> None:

    new_channels: set[str] = await read_channels(
        settings.channel_list,
        settings.channel_map_file, fm, client,
        settings.max_new_channels,
        settings.max_resolved_channels,
        settings.channel_concurrency,
    )

    logging.info(
        'Read unique channel names from .lst files not '
        'already scraped or marked as not found',
        extra={
            'new_channels_length': len(new_channels),
        },
    )
    # Only keep channel handles (no spaces)
    channel_list: list[str] = [
        ch for ch in new_channels
        if ' ' not in ch and ch
    ]
    shuffle(channel_list)

    # Feed channel names through a queue so only
    # ``channel_concurrency`` scrapes are live at any
    # time.  Previous approach created all tasks upfront,
    # holding ~1 000 coroutine frames (and their
    # YouTubeChannel objects) simultaneously — this
    # version lets completed work be GC'd immediately.
    queue: asyncio.Queue[str | None] = asyncio.Queue()
    for name in channel_list:
        queue.put_nowait(name)

    errors: int = 0
    abort: bool = False

    async def worker() -> None:
        nonlocal errors, abort
        while not abort:
            name: str | None = await queue.get()
            if name is None:
                queue.task_done()
                break
            try:
                channel_name: str = (
                    normalize_channel_name(name)
                )
                failed: bool = await scrape_channel(
                    settings, client, fm,
                    channel_name,
                )
                if failed:
                    errors += 1
                    if errors > 100:
                        abort = True
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                errors += 1
                logging.error(
                    'Unexpected error in channel '
                    'scrape worker',
                    exc=exc,
                    extra={'channel_name': name},
                )
                if errors > 100:
                    abort = True
            finally:
                queue.task_done()

    concurrency: int = settings.channel_concurrency
    workers: list[asyncio.Task] = [
        asyncio.create_task(
            worker(),
            name=f'channel-scrape-worker-{i}',
        )
        for i in range(concurrency)
    ]

    await queue.join()

    # Signal workers to exit
    for _ in workers:
        queue.put_nowait(None)
    await asyncio.gather(*workers, return_exceptions=True)

    if abort:
        logging.critical(
            'Too many errors encountered, aborting',
        )
        raise RuntimeError(
            'Too many errors encountered during '
            'scraping'
        )


async def upload_channels(
    settings: ChannelSettings,
    client: ExchangeClient,
    fm: AssetFileManagement,
) -> None:
    files: list[str] = [
        f for f in fm.list_base(
            prefix=CHANNEL_FILE_PREFIX,
            suffix=CHANNEL_FILE_POSTFIX,
        )
        if not f.endswith('failed')
    ]
    METRIC_FILES_PENDING_UPLOAD.labels(
        worker_id=get_worker_id()
    ).set(len(files))
    logging.info(
        'Found already scraped channel files that may '
        'need to be uploaded',
        extra={'files_length': len(files)},
    )
    for filename in files:
        await _upload_single_channel(
            filename, settings, client, fm,
        )


def _is_channel_file(filename: str) -> bool:
    '''Check if a filename is an uploadable channel file.'''
    return (
        filename.startswith(CHANNEL_FILE_PREFIX)
        and filename.endswith(CHANNEL_FILE_POSTFIX)
        and not filename.endswith('failed')
    )


async def _upload_single_channel(
    filename: str,
    settings: ChannelSettings,
    client: ExchangeClient,
    fm: AssetFileManagement,
) -> None:
    '''Upload a single channel file if it needs uploading.'''

    if fm.is_superseded(filename):
        METRIC_UPLOADED_FILE_EXISTS.labels(
            worker_id=get_worker_id(),
        ).inc()
        METRIC_WATCHER_FILES_SKIPPED.labels(
            worker_id=get_worker_id(),
        ).inc()
        await fm.delete(filename, fail_ok=False)
        return

    try:
        channel_data: dict = await fm.read_file(
            filename,
        )
        channel: YouTubeChannel = (
            YouTubeChannel.from_dict(channel_data)
        )
        if await channel_exists(client, channel.name):
            await fm.delete(filename, fail_ok=False)
            return

        if enqueue_upload_channel(
            settings, client, fm, filename, channel,
        ):
            METRIC_CHANNELS_ENQUEUED.labels(
                worker_id=get_worker_id(),
            ).inc()
    except Exception as exc:
        logging.error(
            'Error processing channel file',
            exc=exc,
            extra={'filename': filename},
        )


async def _watch_and_upload_channels(
    settings: ChannelSettings,
    client: ExchangeClient,
    fm: AssetFileManagement,
) -> None:
    '''
    Upload-only watcher loop.  Watches
    ``channel_data_directory`` for new or modified
    channel files and uploads them as they appear.

    Runs until cancelled by SIGINT / SIGTERM.
    '''

    base: Path = fm.base_dir
    logging.info(
        'Upload-only: watching for new channel files',
        extra={'watch_dir': str(base)},
    )

    wid: str = get_worker_id()
    async for changes in awatch(
        base,
        watch_filter=lambda change, path: (
            change in (Change.added, Change.modified)
            and _is_channel_file(Path(path).name)
        ),
    ):
        METRIC_WATCHER_BATCHES.labels(
            worker_id=wid,
        ).inc()
        for _change, path in changes:
            filename: str = Path(path).name
            METRIC_WATCHER_FILES_DETECTED.labels(
                worker_id=wid,
            ).inc()
            logging.info(
                'Upload-only: new channel file '
                'detected',
                extra={'filename': filename},
            )
            await _upload_single_channel(
                filename, settings, client, fm,
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
            'Extracted channel name from URL',
            extra={
                'original_channel_name': channel_name,
                'name': name,
            },
        )
    # If the name is an email address
    if '@' in name:
        name = name.split('@')[0]
        logging.debug(
            'Extracted channel name from email',
            extra={
                'original_channel_name': channel_name,
                'name': name,
            },
        )

    return name


def get_channel_filename(channel_name: str) -> str:
    return f'{CHANNEL_FILE_PREFIX}{channel_name}{CHANNEL_FILE_POSTFIX}'


async def scrape_channel(settings: ChannelSettings, client: ExchangeClient,
                         fm: AssetFileManagement, channel_name: str) -> bool:
    '''
    Scrapes a single YouTube channel and uploads it to the Scrape Exchange.

    :param settings: Tool settings.
    :param client: The Scrape Exchange client instance.
    :param fm: AssetFileManagement instance owning the channel data directory.
    :param channel_name: The name of the YouTube channel to scrape.
    :returns: whether channel scraping/uploading failed
    :raises: (none)
    '''

    logging.debug(
        'Processing channel', extra={'channel_name': channel_name}
    )
    channel: YouTubeChannel | None = None
    filename: str = get_channel_filename(channel_name)
    base_path: Path = fm.base_dir / filename
    uploaded_path: Path = fm.uploaded_dir / filename
    failed_path: Path = fm.base_dir / f'{filename}.failed'

    upload_mtime: float = 0
    if uploaded_path.exists():
        upload_mtime = uploaded_path.stat().st_mtime
        logging.debug(
            'Found uploaded path for channel',
            extra={
                'uploaded_path': uploaded_path,
                'channel_name': channel_name,
            },
        )

    saved_path: Path = base_path
    saved_mtime: float = 0
    if base_path.exists():
        saved_mtime = base_path.stat().st_mtime
        logging.debug(
            'Found base path for channel',
            extra={
                'base_path': base_path,
                'channel_name': channel_name,
            },
        )
    if failed_path.exists():
        logging.debug(
            'Found previously failed upload file for channel, '
            'skipping',
            extra={'channel_name': channel_name},
        )
        saved_path = failed_path
        saved_mtime = failed_path.stat().st_mtime

    if upload_mtime:
        if saved_mtime and upload_mtime >= saved_mtime:
            logging.debug(
                'Channel already uploaded, skipping',
                extra={'channel_name': channel_name},
            )
            await fm.delete(saved_path.name, fail_ok=False)

        # If the channel was already uploaded then there is nothing to do
        return False

    if not saved_mtime:
        logging.debug(
            'Channel not scraped, scraping now',
            extra={'channel_name': channel_name},
        )
        channel = YouTubeChannel(
            name=channel_name, deno_path=DENO_PATH,
            po_token_url=PO_TOKEN_URL, debug=True,
            save_dir=settings.channel_data_directory,
            with_download_client=False
        )
        try:
            logging.info(
                'Scraping channel',
                extra={'channel_name': channel_name},
            )
            await channel.scrape(
                with_about_page=True,
                max_videos_per_channel=0,
                proxies=settings.proxies,
            )
        except ValueError:
            logging.debug(
                'Channel not found, skipping',
                extra={'channel_name': channel_name},
            )
            try:
                await fm.mark_not_found(
                    f'{CHANNEL_FILE_PREFIX}'
                    f'{channel_name}',
                    content=f'{channel_name}\n',
                )
            except OSError:
                logging.warning(
                    'Failed to write not_found marker '
                    'for channel',
                    extra={
                        'channel_name': channel_name,
                    },
                )
            return False
        except asyncio.CancelledError:
            raise
        except RuntimeError as exc:
            METRIC_SCRAPE_FAILURES.labels(
                worker_id=get_worker_id()
            ).inc()
            logging.warning(
                'Failed to scrape channel',
                exc=exc,
                extra={'channel_name': channel_name},
            )
            return False
        except Exception as exc:
            METRIC_SCRAPE_FAILURES.labels(
                worker_id=get_worker_id()
            ).inc()
            logging.warning(
                'Unexpected error while scraping '
                'channel',
                exc=exc,
                extra={'channel_name': channel_name},
            )
            return False
        finally:
            # Close the browse client so its curl
            # transport releases sockets and buffers
            # immediately rather than waiting for GC.
            if (channel.browse_client is not None):
                await channel.browse_client.aclose()
                channel.browse_client = None

        if (not channel.video_ids and not channel.playlists
                and not channel.courses
                and not channel.podcast_ids
                and not channel.products):
            if channel.description:
                logging.info(
                    'Channel has description but no other '
                    'content, skipping upload',
                    extra={'channel_name': channel_name},
                )
            METRIC_CHANNEL_NO_CONTENT_FOUND.labels(
                worker_id=get_worker_id()
            ).inc()
            logging.info(
                'YouTube channel content counts',
                extra={
                    'channel_name': channel_name,
                    'playlists_length': (
                        len(channel.playlists)
                    ),
                    'courses_length': (
                        len(channel.courses)
                    ),
                    'podcast_ids_length': (
                        len(channel.podcast_ids)
                    ),
                    'products_length': (
                        len(channel.products)
                    ),
                },
            )
            return False

        try:
            await fm.write_file(
                filename,
                channel.to_dict(with_video_ids=True),
            )
        except Exception as exc:
            logging.error(
                'Failed to write channel file to disk',
                exc=exc,
                extra={
                    'channel_name': channel_name,
                    'filename': filename,
                },
            )
            return True
        METRIC_CHANNELS_SCRAPED.labels(
            worker_id=get_worker_id()
        ).inc()
        logging.info(
            'Downloaded channel',
            extra={'channel_name': channel_name},
        )

    if settings.channel_no_upload:
        logging.debug(
            'No-upload flag set, skipping upload for channel',
            extra={'channel_name': channel_name},
        )
        return False

    logging.debug(
        'Uploading channel to Scrape Exchange',
        extra={'channel_name': channel_name},
    )
    # If we reached here via the saved_mtime path (file existed on
    # disk but was never uploaded), ``channel`` was never assigned.
    # Load it from the persisted file before enqueueing.
    if channel is None:
        try:
            channel_data: dict = await fm.read_file(filename)
            channel = YouTubeChannel.from_dict(channel_data)
        except Exception as exc:
            logging.error(
                'Failed to load channel file for upload',
                exc=exc,
                extra={
                    'channel_name': channel_name,
                    'filename': filename,
                },
            )
            return True
    # Fire-and-forget: background worker moves the file on success;
    # on queue full the file stays in base_dir for the next retry.
    if enqueue_upload_channel(
        settings, client, fm, filename, channel,
    ):
        METRIC_CHANNELS_ENQUEUED.labels(
            worker_id=get_worker_id()
        ).inc()
    return False


def enqueue_upload_channel(
    settings: ChannelSettings, client: ExchangeClient,
    fm: AssetFileManagement, filename: str, channel: YouTubeChannel,
) -> bool:
    '''
    Fire-and-forget upload of a scraped channel to Scrape Exchange.

    Returns immediately; the background worker inside
    :class:`ExchangeClient` performs the POST with retries and, on
    HTTP 201, moves the channel file from ``base_dir`` to
    ``uploaded_dir`` via ``fm.mark_uploaded``. If the queue is full
    (API down, retries backing up) the enqueue is dropped and the
    file stays in ``base_dir`` for the next iteration to retry.

    :returns: ``True`` if the job was enqueued, ``False`` if dropped.
    '''

    logging.info(
        'Enqueuing channel for upload', extra={'channel_name': channel.name}
    )
    return client.enqueue_upload(
        f'{settings.exchange_url}{client.POST_DATA_API}',
        json={
            'username': settings.schema_owner,
            'platform': 'youtube',
            'entity': 'channel',
            'version': settings.schema_version,
            'source_url': channel.url,
            'data': channel.to_dict(with_video_ids=False),
            'platform_content_id': channel.name,
            'platform_creator_id': channel.name,
            'platform_topic_id': None,
        },
        file_manager=fm,
        filename=filename,
        entity='channel',
        log_extra={'channel_name': channel.name},
    )


async def read_channel_map(file_path: str) -> dict[str, str]:
    '''
    Reads existing channel files from the specified directory and extracts
    channel names.

    :param directory: The directory containing existing channel files.
    :returns: A dict with existing YouTube channel IDs as keys and names as
    values  .
    :raises: (none)
    '''

    logging.info(
        'Reading channel map', extra={'file_path': file_path}
    )

    channels: dict[str, str] = {}
    if not os.path.isfile(file_path):
        logging.warning(
            'File does not exist, returning empty set',
            extra={'file_path': file_path},
        )
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

    logging.info(
        'Finished reading channel map',
        extra={'file_path': file_path, 'entries': len(channels)},
    )
    return channels


async def read_channels(
    file_path: str, channel_map_file: str, fm: AssetFileManagement,
    exchange_client: ExchangeClient,
    max_new_channels: int, max_resolved_channels: int, concurrency: int = 3,
) -> set[str]:
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

    logging.info(
        'Reading channel names', extra={'file_path': file_path}
    )

    # These are known names but not necesarily will have been scraped
    channel_map: dict[str, str] = await read_channel_map(
        channel_map_file
    )
    new_channel_names: set[str] = set()

    # Channel IDs that need to be resolved to names
    unresolved_ids: set[str] = set()

    line: str
    async with aiofiles.open(file_path, 'r') as file_desc:
        async for line in file_desc:
            line = line.strip()
            if not line or line.startswith('#') or ' ' in line:
                continue
            if line.startswith('channel/'):
                line = line[len('channel/'):]
                if line.startswith('uc'):
                    line = line[0].upper() + line[1].upper() + line[2:]

            channel_name: str | None = None
            if ',' in line:
                _: str
                _, channel_name = line.split(',', 1)
                channel_name = channel_name.strip()
            if not channel_name and YouTubeChannel.is_channel_id(line):
                if line in channel_map:
                    channel_name = channel_map[line]
                elif (fm.base_dir
                      / f'{CHANNEL_FILE_PREFIX}{line}.unresolved').exists():
                    logging.debug(
                        'Channel ID previously failed to resolve, '
                        'skipping',
                        extra={'channel_id': line},
                    )
                else:
                    unresolved_ids.add(line)
            elif line.startswith('https://www.youtube.com/@'):
                channel_name = line[len('https://www.youtube.com/@'):].strip()
            elif (line.startswith('handle') or line.startswith('custom')
                    or line.startswith('user') or line.startswith('c/')):
                parts: list[str] = line.split('\\')
                if len(parts) >= 2:
                    channel_name = parts[1].strip()
            elif '\t' in line:
                channel_name = line.split('\t')[1].strip()
            else:
                channel_name = line

            if channel_name:
                new_channel_names.add(channel_name)

    logging.info(
        'Found unique channel names in file',
        extra={
            'new_channel_names_length': len(new_channel_names),
            'file_path': file_path,
        },
    )

    resolved_channels: set[str] = set()
    if unresolved_ids:
        logging.info(
            'Found unresolved channel IDs, will not '
            'resolve more than the configured maximum in this run',
            extra={
                'unresolved_ids_length': len(unresolved_ids),
                'max_resolved_channels': max_resolved_channels,
            },
        )
        METRIC_CHANNEL_IDS_TO_RESOLVE.labels(
            worker_id=get_worker_id()
        ).set(len(unresolved_ids))
        resolved_channels = await review_unresolved_ids(
            unresolved_ids, channel_map_file, fm,
            concurrency, max_resolved_channels
        )
        new_channel_names.update(resolved_channels)

    candidates: list[str] = []
    for channel_name in new_channel_names:
        if not channel_name:
            continue
        filename: str = get_channel_filename(channel_name)
        not_found_path: Path = (
            fm.base_dir / f'{CHANNEL_FILE_PREFIX}{channel_name}.not_found'
        )
        scraped_path: Path = fm.base_dir / filename
        uploaded_path: Path = fm.uploaded_dir / filename
        if (not_found_path.exists()
                or scraped_path.exists()
                or uploaded_path.exists()):
            logging.debug(
                'Skipping channel as we already have data for it',
                extra={'channel_name': channel_name},
            )
            continue
        candidates.append(channel_name)

    exists_semaphore: asyncio.Semaphore = asyncio.Semaphore(10)
    logging.info(
        'Checking existence of channel names on Scrape Exchange',
        extra={'max_new_channels': max_new_channels},
    )

    async def check_exists(name: str) -> tuple[str, bool]:
        async with exists_semaphore:
            return name, await channel_exists(exchange_client, name)

    existence_results: list[tuple[str, bool]] = await asyncio.gather(
        *(check_exists(name) for name in candidates[:max_new_channels])
    )

    checked_channel_names: set[str] = set()
    for channel_name, exists in existence_results:
        if exists:
            logging.debug(
                'Skipping channel name previously marked as not found '
                'or found to exist',
                extra={'channel_name': channel_name},
            )
            continue
        checked_channel_names.add(channel_name)
        if ((len(checked_channel_names) + len(resolved_channels))
                >= max_new_channels):
            logging.info(
                'Reached maximum new channels to scrape, stopping read',
                extra={'max_new_channels': max_new_channels},
            )
            break

    METRIC_UNIQUE_CHANNELS_READ.labels(
        worker_id=get_worker_id()
    ).set(len(checked_channel_names))
    logging.info(
        'Read unique channel names from file',
        extra={
            'checked_channel_names_length': len(checked_channel_names),
            'file_path': file_path,
        },
    )
    return checked_channel_names


async def review_unresolved_ids(
    unresolved_ids: set[str], channel_map_file: str, fm: AssetFileManagement,
    concurrency: int, max_resolved_channels: int
) -> set[str]:
    '''
    See if we can resolve a channel ID to a channel handle

    :param unresolved_ids: Set of channel IDs that need to be resolved to
    channel names.
    :param channel_map_file: Path to the CSV file where resolved
    channel ID-name pairs should be saved.
    :param fm: AssetFileManagement instance owning the channel data directory.
    :param concurrency: Number of channel ID resolutions to run concurrently.
    :returns: Set of resolved channel names corresponding to the input
    channel IDs.
    :raises: (none)
    '''

    semaphore: asyncio.Semaphore = asyncio.Semaphore(concurrency)
    map_lock: asyncio.Lock = asyncio.Lock()
    resolved_channel_names: set[str] = set()

    async def resolve(channel_id: str) -> str | None:
        async with semaphore:
            try:
                name: str = await YouTubeChannel.resolve_channel_id(
                    channel_id
                )
                if not name:
                    unresolved_file_path: Path = (
                        fm.base_dir
                        / f'{CHANNEL_FILE_PREFIX}{channel_id}.unresolved'
                    )
                    if unresolved_file_path.exists():
                        logging.debug(
                            'Channel ID previously failed to '
                            'resolve, skipping',
                            extra={'channel_id': channel_id},
                        )
                    else:
                        logging.info(
                            'Failed to resolve channel ID, '
                            'touching unresolved file',
                            extra={
                                'channel_id': channel_id,
                                'unresolved_file_path':
                                    unresolved_file_path,
                            },
                        )
                        await fm.mark_unresolved(
                            f'{CHANNEL_FILE_PREFIX}{channel_id}',
                            content=f'{channel_id}\n',
                        )
                    METRIC_CHANNEL_ID_RESOLUTION_FAILURES.labels(
                        worker_id=get_worker_id()
                    ).inc()
                    return None
                elif ' ' in name:
                    logging.info(
                        'Resolved channel ID to name with spaces',
                        extra={
                            'channel_id': channel_id,
                            'name': name,
                        },
                    )
                    return None
                async with map_lock:
                    async with aiofiles.open(
                            channel_map_file, 'a') as f:
                        await f.write(f'{channel_id},{name}\n')

                logging.debug(
                    'Resolved channel ID to name',
                    extra={
                        'channel_id': channel_id,
                        'name': name,
                    },
                )
                METRIC_CHANNEL_IDS_RESOLVED.labels(
                    worker_id=get_worker_id()
                ).inc()
                return name
            except Exception as e:
                METRIC_CHANNEL_ID_RESOLUTION_FAILURES.labels(
                    worker_id=get_worker_id()
                ).inc()
                logging.debug(
                    'Error while resolving channel ID',
                    exc=e,
                    extra={'channel_id': channel_id},
                )
                return None

    ids: list[str] = list(unresolved_ids)
    shuffle(ids)
    ids = ids[:max_resolved_channels]

    results: list[str | None] = await asyncio.gather(
        *(resolve(cid) for cid in ids)
    )
    for name in results:
        resolved_channel_names.add(name)

    logging.info(
        'Completed resolution of channel IDs',
        extra={
            'resolved_count': len(resolved_channel_names),
            'unresolved_count':
                len(unresolved_ids) - len(resolved_channel_names),
        },
    )
    return resolved_channel_names


if __name__ == '__main__':
    main()
