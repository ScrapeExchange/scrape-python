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
import json
import asyncio
import logging
import resource

from random import shuffle
from pathlib import Path

import aiofiles
import orjson

from httpx import Response
from watchfiles import awatch, Change
from prometheus_client import Counter, Gauge
from pydantic import AliasChoices, Field, field_validator

from scrape_exchange.creator_map import (
    CreatorMap,
    FileCreatorMap,
    RedisCreatorMap,
    CREATOR_MAP_RESOLUTION_TOTAL,
)
from scrape_exchange.name_map import (
    NameMap,
    NullNameMap,
    RedisNameMap,
)
from scrape_exchange.bulk_upload import (
    BulkBatchOutcome,
    upload_bulk_batch,
)
from scrape_exchange.exchange_client import ExchangeClient

from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.file_management import CHANNEL_FILE_PREFIX
from scrape_exchange.scraper_runner import (
    ScraperRunContext,
    ScraperRunner,
)
from scrape_exchange.settings import normalize_log_level
from scrape_exchange.util import extract_proxy_ip, proxy_network_for

from scrape_exchange.youtube.youtube_channel import YouTubeChannel
from scrape_exchange.youtube.youtube_rate_limiter import YouTubeRateLimiter
from scrape_exchange.youtube.youtube_video import DENO_PATH, PO_TOKEN_URL
from scrape_exchange.worker_id import get_worker_id

from scrape_exchange.schema_validator import (
    SchemaValidator,
    fetch_schema_dict,
)
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
        default='0.0.2',
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
            'MAX_RESOLVED_CHANNELS', 'max_resolved_channels'
        ),
        description=(
            'Maximum number of channels with channel-ids for which '
            'we do try to resolve the channel handle'
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
    # overrdide the base ScraperSettings proxies field with an RSS-specific one
    # pydantic-settings takes the value of the first matching alias
    proxies: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'CHANNEL_PROXIES', 'channel_proxies', 'PROXIES', 'proxies'
        ),
        description=(
            'Comma-separated list of proxy URLs to use for scraping (e.g. '
            '"http://proxy1:port,http://proxy2:port"). If not set, no '
            'proxy will be used.'
        )
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
    ['worker_id', 'proxy_ip', 'proxy_network'],
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
    ['worker_id', 'proxy_ip', 'proxy_network'],
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
    ['worker_id', 'proxy_ip', 'proxy_network'],
)

# -- scheduled bulk-upload metrics --
METRIC_CHANNELS_BULK_UPLOADED = Counter(
    'yt_channel_bulk_uploaded_total',
    'Channel records confirmed uploaded via the bulk API '
    '(per-record success in the job results).',
    ['worker_id'],
)
METRIC_CHANNELS_BULK_FAILED = Counter(
    'yt_channel_bulk_failed_total',
    'Channel records that the bulk worker reported as failed. '
    'Source files are left in base_dir for the next iteration.',
    ['worker_id'],
)
METRIC_CHANNELS_BULK_MISSING_RESULT = Counter(
    'yt_channel_bulk_missing_result_total',
    'Channel records that were submitted in a bulk batch but did '
    'not appear in the job results. Source files are left in '
    'base_dir for the next iteration.',
    ['worker_id'],
)
METRIC_BULK_BATCHES = Counter(
    'yt_channel_bulk_batches_total',
    'Bulk-upload batches dispatched by the scheduled upload sweep.',
    ['worker_id', 'outcome'],
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


async def _run_worker(
    ctx: ScraperRunContext,
) -> None:
    '''
    Run a single in-process channel scraper worker (the leaf of
    the supervisor tree). Runs the upload + scrape passes using
    the context provided by ScraperRunner.
    '''

    settings: ChannelSettings = ctx.settings

    _: int
    _hard: int
    _, _hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    _target: int = (
        _hard if _hard != resource.RLIM_INFINITY else 1048576
    )
    resource.setrlimit(
        resource.RLIMIT_NOFILE,
        (_target, _hard),
    )

    logging.info(
        'Starting YouTube channel upload tool',
        extra={'settings': settings.model_dump()}
    )

    # AssetFileManagement creates the 'uploaded' subdirectory
    # automatically and owns all read/write/marker operations
    # under channel_data_directory.
    fm: AssetFileManagement = AssetFileManagement(
        settings.channel_data_directory,
    )

    creator_map_backend: CreatorMap
    if settings.redis_dsn:
        creator_map_backend = RedisCreatorMap(
            settings.redis_dsn,
            platform='youtube',
        )
    else:
        creator_map_backend = FileCreatorMap(
            settings.channel_map_file,
        )

    name_map_backend: NameMap
    if settings.redis_dsn:
        name_map_backend = RedisNameMap(
            settings.redis_dsn, platform='youtube',
        )
    else:
        name_map_backend = NullNameMap()

    if not settings.proxies:
        logging.info(
            'No proxies configured, using direct '
            'connection for scraping',
        )
        settings.channel_concurrency = 1

    # Build the schema validator once at startup. Used by every
    # downstream upload site — bulk sweep, the upload-only watcher,
    # and the live-scrape worker — so that records that don't
    # conform to the channel JSON schema are rejected client-side
    # and the on-disk asset is renamed ``<filename>.invalid`` for
    # operator inspection.
    schema_dict: dict = await fetch_schema_dict(
        ctx.client,
        settings.exchange_url,
        settings.schema_owner,
        'youtube',
        'channel',
        settings.schema_version,
    )
    validator: SchemaValidator = SchemaValidator(schema_dict)

    if not settings.channel_no_upload:
        logging.info('Starting initial channel upload pass')
        await upload_channels(
            settings, ctx.client, fm,
            creator_map_backend, name_map_backend,
            validator,
        )

    if settings.channel_upload_only:
        logging.info('Starting watching for uploads')
        await _watch_and_upload_channels(
            settings, ctx.client, fm,
            creator_map_backend, name_map_backend,
            validator,
        )
    else:
        logging.info('Starting scraping channels')
        await scrape_channels(
            settings, ctx.client, fm,
            creator_map_backend, name_map_backend,
            validator,
        )


def _build_channel_rate_limiter(
    s: 'ChannelSettings',
) -> YouTubeRateLimiter:
    '''
    Construct (or fetch) the per-process YouTubeRateLimiter
    singleton. In ``--channel-upload-only`` mode the worker only
    reads existing files from disk and POSTs them to
    scrape.exchange — there is no YouTube traffic — so the
    proactive cookie warm-up + renewal loop is disabled. Outside
    that mode the warm-up runs as before.
    '''
    rl: YouTubeRateLimiter = YouTubeRateLimiter.get(
        state_dir=s.rate_limiter_state_dir,
        redis_dsn=s.redis_dsn,
    )
    rl.set_auto_warm_cookies(not s.channel_upload_only)
    return rl


def main() -> None:
    '''
    Top-level entry point. Reads settings and dispatches to
    either the shared supervisor (when
    ``channel_num_processes > 1``) or the in-process scraper
    worker (when ``channel_num_processes == 1``).
    '''

    settings: ChannelSettings = ChannelSettings()
    _validate_settings(settings)

    if settings.channel_upload_only:
        settings.channel_num_processes = 1
        settings.metrics_port = (
            settings.metrics_port - 1
        )

    runner: ScraperRunner = ScraperRunner(
        settings=settings,
        scraper_label='channel',
        platform='youtube',
        num_processes=(settings.channel_num_processes),
        concurrency=settings.channel_concurrency,
        metrics_port=settings.metrics_port,
        log_file=settings.channel_log_file,
        log_level=settings.channel_log_level,
        rate_limiter_factory=_build_channel_rate_limiter,
    )
    sys.exit(runner.run_sync(_run_worker))


async def channel_exists(client: ExchangeClient, channel_handle: str) -> bool:
    '''
    Checks if a channel with the given name already exists on Scrape Exchange.

    :param client: The Scrape Exchange client instance.
    :param channel_handle: The name of the YouTube channel to check.
    :returns: True if the channel exists, False otherwise.
    :raises: (none)
    '''

    try:
        resp: Response = await client.get(
            f'{client.exchange_url}{ExchangeClient.GET_CONTENT_API}'
            f'/youtube/channel/{channel_handle}'
        )
    except Exception as exc:
        METRIC_CHANNEL_EXISTS_FAILURES.labels(
            worker_id=get_worker_id()
        ).inc()
        logging.warning(
            'Network error checking channel existence',
            exc=exc,
            extra={'channel_handle': channel_handle},
        )
        return False

    if resp.status_code == 200:
        data: dict = resp.json()
        exists: bool = data.get('exists', False)
        if exists:
            METRIC_CHANNEL_EXISTS_FOUND.labels(worker_id=get_worker_id()).inc()
        else:
            METRIC_CHANNEL_EXISTS_NOT_FOUND.labels(
                worker_id=get_worker_id()
            ).inc()
        return exists
    elif resp.status_code == 404:
        METRIC_CHANNEL_EXISTS_NOT_FOUND.labels(worker_id=get_worker_id()).inc()
        return False
    else:
        METRIC_CHANNEL_EXISTS_FAILURES.labels(worker_id=get_worker_id()).inc()
        logging.warning(
            'Failed to check existence of channel',
            extra={
                'channel_handle': channel_handle,
                'status_code': resp.status_code,
                'response_text': resp.text,
            }
        )
        # Assume the channel does not exist if there was
        # an error checking
        return False


async def scrape_channels(
    settings: ChannelSettings,
    client: ExchangeClient,
    fm: AssetFileManagement,
    creator_map_backend: CreatorMap,
    name_map_backend: NameMap,
    validator: SchemaValidator,
) -> None:

    new_channels: set[str] = await read_channels(
        settings.channel_list,
        creator_map_backend, fm, client,
        settings.max_new_channels,
        settings.max_resolved_channels,
        settings.channel_concurrency,
    )

    logging.info(
        'Read unique channel names from .lst files not already scraped or '
        'marked as not found',
        extra={'new_channels_length': len(new_channels)},
    )
    # Only keep channel handles (no spaces)
    channel_list: list[str] = [
        ch for ch in new_channels if ' ' not in ch and ch
    ]
    shuffle(channel_list)

    # Feed channel names through a queue so only
    # ``channel_concurrency`` scrapes are live at any
    # time.
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
                channel_handle: str = (
                    normalize_channel_name(name)
                )
                failed: bool = await scrape_channel(
                    settings, client, fm,
                    channel_handle, creator_map_backend,
                    name_map_backend, validator,
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
                    extra={'channel_handle': name},
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


async def _collect_channel_record(
    filename: str,
    fm: AssetFileManagement,
    creator_map_backend: CreatorMap,
    name_map_backend: NameMap,
    validator: SchemaValidator,
) -> tuple[str, dict] | None:
    '''
    Read *filename* from base_dir and prepare a bulk-upload record.

    :returns: ``(channel_id, record_dict)`` on success, or ``None``
        when the file should be skipped (read error, missing
        ``channel_id``, or schema-validation failure — in the last
        case the file is renamed ``<filename>.invalid``).

    Side effects: updates the shared creator_map and name_map via
    :func:`resolve_channel_upload_handle`; on validation failure
    calls :meth:`AssetFileManagement.mark_invalid`.
    '''
    try:
        channel_data: dict = await fm.read_file(filename)
        channel: YouTubeChannel = (
            YouTubeChannel.from_dict(channel_data)
        )
    except Exception as exc:
        logging.error(
            'Error reading channel file for bulk upload',
            exc=exc,
            extra={'filename': filename},
        )
        return None

    handle: str = await resolve_channel_upload_handle(
        channel, creator_map_backend, name_map_backend,
    )
    channel.channel_handle = handle

    if not channel.channel_id:
        logging.warning(
            'Channel has no channel_id, skipping bulk upload',
            extra={
                'filename': filename,
                'channel_handle': handle,
            },
        )
        return None

    record_dict: dict = channel.to_dict(with_video_ids=False)
    err: str | None = validator.validate(record_dict)
    if err is not None:
        logging.warning(
            'Channel record failed schema validation, '
            'marking invalid and skipping upload',
            extra={
                'filename': filename,
                'channel_id': channel.channel_id,
                'channel_handle': handle,
                'validation_error': err,
            },
        )
        try:
            await fm.mark_invalid(filename)
        except OSError as exc:
            logging.warning(
                'Failed to mark channel file invalid',
                exc=exc,
                extra={'filename': filename},
            )
        return None

    logging.debug(
        'Collected channel record for bulk upload',
        extra={
            'filename': filename,
            'channel_id': channel.channel_id,
            'channel_handle': handle,
        },
    )
    return channel.channel_id, record_dict


async def _prepare_channel_line(
    filename: str,
    fm: AssetFileManagement,
    creator_map_backend: CreatorMap,
    name_map_backend: NameMap,
    validator: SchemaValidator,
) -> tuple[str, str, bytes] | None:
    '''
    Per-file work for the bulk-upload sweep, factored out so that
    :func:`upload_channels` can run ``channel_concurrency`` of
    these in flight at once via :func:`asyncio.gather`. Handles
    the superseded-file branch (delete + skip), the read /
    handle-resolution / serialise pipeline, and the byte-cap
    pre-check log.

    :returns: ``(channel_id, filename, line_bytes)`` on success,
        or ``None`` when the file should be skipped (superseded,
        read error, missing channel_id).
    '''
    logging.debug(
        'Considering channel file for bulk upload',
        extra={'filename': filename},
    )
    if fm.is_superseded(filename):
        logging.debug(
            'Channel file superseded, deleting',
            extra={'filename': filename},
        )
        METRIC_UPLOADED_FILE_EXISTS.labels(
            worker_id=get_worker_id(),
        ).inc()
        try:
            await fm.delete(filename, fail_ok=False)
        except Exception as exc:
            logging.warning(
                'Failed to delete superseded channel file',
                exc=exc,
                extra={'filename': filename},
            )
        return None

    record: tuple[str, dict] | None = (
        await _collect_channel_record(
            filename, fm,
            creator_map_backend, name_map_backend,
            validator,
        )
    )
    if record is None:
        return None
    channel_id: str
    record_dict: dict
    channel_id, record_dict = record

    line: bytes = orjson.dumps(record_dict) + b'\n'
    logging.debug(
        'Prepared channel record for bulk batch',
        extra={
            'filename': filename,
            'channel_id': channel_id,
            'record_bytes': len(line),
        },
    )
    return channel_id, filename, line


async def _upload_one_channel_batch(
    batch_buf: bytes,
    batch_records: list[tuple[str, str]],
    settings: ChannelSettings,
    client: ExchangeClient,
    fm: AssetFileManagement,
) -> None:
    '''
    Dispatch one prepared batch of channel records via the shared
    bulk-upload pipeline and route per-record outcomes into the
    channel-specific Prometheus metrics. The shared helper handles
    POST, progress streaming, and result reconciliation; this
    function only translates lifecycle outcomes into metric
    increments.
    '''
    if not batch_records:
        return

    outcome: BulkBatchOutcome = await upload_bulk_batch(
        batch_buf, batch_records,
        schema_owner=settings.schema_owner,
        schema_version=settings.schema_version,
        platform='youtube',
        entity='channel',
        exchange_url=settings.exchange_url,
        client=client,
        fm=fm,
        progress_timeout_seconds=(
            settings.bulk_progress_timeout_seconds
        ),
        filename_prefix='channels',
    )
    METRIC_BULK_BATCHES.labels(
        worker_id=get_worker_id(), outcome=outcome.status,
    ).inc()
    if outcome.success:
        METRIC_CHANNELS_BULK_UPLOADED.labels(
            worker_id=get_worker_id(),
        ).inc(outcome.success)
    if outcome.failed:
        METRIC_CHANNELS_BULK_FAILED.labels(
            worker_id=get_worker_id(),
        ).inc(outcome.failed)
    if outcome.missing:
        METRIC_CHANNELS_BULK_MISSING_RESULT.labels(
            worker_id=get_worker_id(),
        ).inc(outcome.missing)



async def upload_channels(
    settings: ChannelSettings,
    client: ExchangeClient,
    fm: AssetFileManagement,
    creator_map_backend: CreatorMap,
    name_map_backend: NameMap,
    validator: SchemaValidator,
) -> None:
    '''
    Sweep ``base_dir`` for pending channel files and upload them
    via the bulk API in batches of up to
    ``settings.bulk_batch_size`` records (or
    ``settings.bulk_max_batch_bytes`` bytes, whichever is hit
    first). Per-record success in the job results promotes the
    matching source file to ``uploaded_dir``; failed and missing
    records are left in ``base_dir`` for the next iteration.

    The watcher loop ``_watch_and_upload_channels`` keeps using
    the per-channel POST path because it processes files as they
    arrive and bulk batching offers no benefit for single-file
    arrivals.
    '''

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
        'Found channel files for bulk upload',
        extra={'files_length': len(files)},
    )
    if not files:
        return

    batch_buf: bytearray = bytearray()
    batch_records: list[tuple[str, str]] = []
    max_records: int = settings.bulk_batch_size
    max_bytes: int = settings.bulk_max_batch_bytes
    concurrency: int = max(settings.channel_concurrency, 1)

    # Process files in concurrent chunks of *concurrency* size.
    # Each task does the per-file I/O (read+decompress, creator_map
    # / name_map writes, JSON re-serialise) so the event loop has
    # ``concurrency`` files in flight at once. Sequential batching
    # then consumes the prepared lines in submission order, which
    # keeps the ``record_index`` fallback in
    # :func:`apply_bulk_results` consistent with the wire order.
    for start in range(0, len(files), concurrency):
        chunk: list[str] = files[start:start + concurrency]
        prepared: list[
            tuple[str, str, bytes] | None
        ] = await asyncio.gather(*(
            _prepare_channel_line(
                f, fm,
                creator_map_backend, name_map_backend,
                validator,
            )
            for f in chunk
        ))
        for entry in prepared:
            if entry is None:
                continue
            channel_id: str
            filename: str
            line: bytes
            channel_id, filename, line = entry
            logging.debug(
                'Adding channel record to bulk batch',
                extra={
                    'filename': filename,
                    'channel_id': channel_id,
                    'record_bytes': len(line),
                    'batch_records_count': len(batch_records),
                    'batch_bytes': len(batch_buf),
                },
            )

            if len(line) > max_bytes:
                logging.warning(
                    'Channel record exceeds bulk-batch byte cap, '
                    'skipping',
                    extra={
                        'filename': filename,
                        'channel_id': channel_id,
                        'record_bytes': len(line),
                        'max_bytes': max_bytes,
                    },
                )
                continue

            if (
                len(batch_records) >= max_records
                or len(batch_buf) + len(line) > max_bytes
            ):
                logging.debug(
                    'Channel bulk batch reached cap, flushing',
                    extra={
                        'records': len(batch_records),
                        'bytes': len(batch_buf),
                        'max_records': max_records,
                        'max_bytes': max_bytes,
                    },
                )
                await _upload_one_channel_batch(
                    bytes(batch_buf), batch_records,
                    settings, client, fm,
                )
                batch_buf = bytearray()
                batch_records = []

            batch_buf.extend(line)
            batch_records.append((channel_id, filename))

    if batch_records:
        logging.debug(
            'Flushing trailing channel bulk batch',
            extra={
                'records': len(batch_records),
                'bytes': len(batch_buf),
            },
        )
        await _upload_one_channel_batch(
            bytes(batch_buf), batch_records,
            settings, client, fm,
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
    creator_map_backend: CreatorMap,
    name_map_backend: NameMap,
    validator: SchemaValidator,
) -> None:
    '''
    Upload a single channel file if it needs uploading.
    '''

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
        if await channel_exists(client, channel.channel_handle):
            await fm.delete(filename, fail_ok=False)
            return

        if await enqueue_upload_channel(
            settings, client, fm, filename, channel,
            creator_map_backend, name_map_backend,
            validator,
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
    creator_map_backend: CreatorMap,
    name_map_backend: NameMap,
    validator: SchemaValidator,
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
                creator_map_backend, name_map_backend,
                validator,
            )


def normalize_channel_name(channel_handle: str) -> str:
    '''
    Normalises a YouTube channel name extracted from user input by
    stripping whitespace and a leading '@'. Also strips URL prefixes
    and anything after an '@' when the input looks like an email
    address.

    Case is preserved: input at this stage may not yet be the
    canonical handle. The canonical handle is resolved later by
    resolve_channel_upload_handle() using YouTube's vanityChannelUrl.

    :param channel_handle: The original channel name.
    :returns: The stripped channel name.
    '''

    name: str = channel_handle.strip().lstrip('@')
    if name.startswith('https://'):
        name = name.split('/')[-1]
        logging.debug(
            'Extracted channel name from URL',
            extra={
                'original_channel_name': channel_handle,
                'name': name,
            },
        )
    # If the name is an email address
    if '@' in name:
        name = name.split('@')[0]
        logging.debug(
            'Extracted channel name from email',
            extra={
                'original_channel_name': channel_handle,
                'name': name,
            },
        )

    return name


def get_channel_filename(channel_handle: str) -> str:
    return f'{CHANNEL_FILE_PREFIX}{channel_handle}{CHANNEL_FILE_POSTFIX}'


def _failed_marker_is_stale(
    fm: AssetFileManagement, filename: str,
    base_path: Path, failed_path: Path,
) -> bool:
    '''
    Return True if a ``.failed`` marker has been superseded by either
    an uploaded copy of the same channel or a newer (re-scraped) base
    file. In that case the caller should remove the marker and proceed
    rather than skip.
    '''
    if fm.was_uploaded(filename):
        return True
    try:
        base_mtime: float = base_path.stat().st_mtime
        failed_mtime: float = failed_path.stat().st_mtime
    except OSError:
        return False
    return base_mtime > failed_mtime


async def _skip_due_to_existing_state(
    fm: AssetFileManagement, filename: str,
    base_path: Path, extra: dict[str, str],
) -> bool:
    '''
    Decide whether the channel can be skipped without scraping or
    uploading based on what's already on disk.

    :returns: True if the channel should be skipped (caller returns
        ``False``); False if the caller should proceed.
    '''
    failed_path: Path = fm.marker_path(filename, '.failed')
    if failed_path.exists():
        if _failed_marker_is_stale(
            fm, filename, base_path, failed_path,
        ):
            await fm.delete(failed_path.name, fail_ok=True)
            logging.debug(
                'Removed stale .failed marker', extra=extra,
            )
        else:
            logging.debug(
                'Channel has .failed marker, skipping', extra=extra,
            )
            return True

    if not fm.was_uploaded(filename):
        return False

    if fm.is_superseded(filename):
        await fm.delete(filename, fail_ok=False)
        logging.debug(
            'Channel already uploaded, dropped stale base copy, '
            'skipping',
            extra=extra,
        )
        return True
    if not base_path.exists():
        logging.debug(
            'Channel already uploaded, no local base copy, skipping',
            extra=extra,
        )
        return True
    logging.debug(
        'Local base copy is newer than uploaded copy, re-uploading',
        extra=extra,
    )
    return False


def _record_scrape_failure(
    channel: YouTubeChannel, message: str, exc: BaseException,
    extra: dict[str, str],
) -> None:
    '''
    Bump the scrape-failure metric and log *message* with proxy labels.
    Reads the proxy off ``channel.browse_client`` if it's still open.
    '''
    proxy_used: str | None = getattr(
        channel.browse_client, 'proxy', None,
    )
    proxy_used_ip: str = (
        extract_proxy_ip(proxy_used) if proxy_used else 'none'
    )
    METRIC_SCRAPE_FAILURES.labels(
        worker_id=get_worker_id(),
        proxy_ip=proxy_used_ip,
        proxy_network=proxy_network_for(proxy_used_ip),
    ).inc()
    logging.warning(
        message, exc=exc, extra=extra | {
            'proxy': proxy_used, 'proxy_ip': proxy_used_ip,
        },
    )


async def _try_scrape_channel(
    channel: YouTubeChannel, settings: ChannelSettings,
    fm: AssetFileManagement, channel_handle: str,
    extra: dict[str, str],
) -> tuple[bool, str | None]:
    '''
    Run ``channel.scrape()``, handling expected failure modes (channel
    not found, runtime errors, generic exceptions) and always closing
    the browse client.

    :returns: ``(succeeded, scrape_proxy)``. On any failure mode that
        the caller should treat as a clean miss, ``succeeded`` is
        False and the caller returns ``False`` from
        :func:`scrape_channel`.
    '''
    try:
        logging.info('Scraping channel', extra=extra)
        await channel.scrape(
            with_about_page=True,
            max_videos_per_channel=0,
            proxies=settings.proxies,
        )
    except ValueError:
        logging.debug('Channel not found, skipping', extra=extra)
        try:
            await fm.mark_not_found(
                f'{CHANNEL_FILE_PREFIX}{channel_handle}',
                content=f'{channel_handle}\n',
            )
        except OSError:
            logging.warning(
                'Failed to write not_found marker for channel',
                extra=extra,
            )
        return False, None
    except asyncio.CancelledError:
        raise
    except RuntimeError as exc:
        _record_scrape_failure(
            channel, 'Failed to scrape channel', exc, extra,
        )
        return False, None
    except Exception as exc:
        _record_scrape_failure(
            channel, 'Unexpected error while scraping channel',
            exc, extra,
        )
        return False, None
    finally:
        # Capture the proxy used *before* we close the browse client
        # so downstream metric emissions (CHANNEL_NO_CONTENT_FOUND,
        # CHANNELS_SCRAPED) can still label by proxy_ip.
        scrape_proxy: str | None = getattr(
            channel.browse_client, 'proxy', None,
        )
        # Close the browse client so its curl transport releases
        # sockets and buffers immediately rather than waiting for GC.
        if channel.browse_client is not None:
            await channel.browse_client.aclose()
            channel.browse_client = None

    return True, scrape_proxy


def _channel_has_no_content(
    channel: YouTubeChannel, scrape_proxy_ip: str,
    scrape_proxy_network: str, channel_handle: str,
) -> bool:
    '''
    Return True (and emit the no-content metric) when *channel* has no
    videos, playlists, courses, podcasts, or products to upload.
    '''
    if (channel.video_ids or channel.playlists or channel.courses
            or channel.podcast_ids or channel.products):
        return False

    if channel.description:
        logging.info(
            'Channel has description but no other content, skipping '
            'upload',
            extra={'channel_handle': channel_handle},
        )
    METRIC_CHANNEL_NO_CONTENT_FOUND.labels(
        worker_id=get_worker_id(),
        proxy_ip=scrape_proxy_ip,
        proxy_network=scrape_proxy_network,
    ).inc()
    logging.info(
        'YouTube channel content counts',
        extra={
            'channel_handle': channel_handle,
            'proxy_ip': scrape_proxy_ip,
            'proxy_network': scrape_proxy_network,
            'playlists_length': len(channel.playlists),
            'courses_length': len(channel.courses),
            'podcast_ids_length': len(channel.podcast_ids),
            'products_length': len(channel.products),
        },
    )
    return True


async def _persist_scraped_channel(
    fm: AssetFileManagement, filename: str,
    channel: YouTubeChannel, channel_handle: str,
) -> bool:
    '''
    Write the freshly-scraped channel to disk. Returns True on success,
    False on a write failure (caller should propagate as "failed").
    '''
    try:
        await fm.write_file(
            filename, channel.to_dict(with_video_ids=True),
        )
    except Exception as exc:
        logging.error(
            'Failed to write channel file to disk',
            exc=exc,
            extra={
                'channel_handle': channel_handle, 'filename': filename,
            },
        )
        return False
    return True


async def _load_channel_from_disk(
    fm: AssetFileManagement, filename: str, channel_handle: str,
) -> YouTubeChannel | None:
    '''
    Load a previously-scraped channel from *fm*. Returns None on a read
    or deserialisation failure.
    '''
    try:
        channel_data: dict = await fm.read_file(filename)
        return YouTubeChannel.from_dict(channel_data)
    except Exception as exc:
        logging.error(
            'Failed to load channel file for upload',
            exc=exc,
            extra={
                'channel_handle': channel_handle, 'filename': filename,
            },
        )
        return None


async def scrape_channel(
    settings: ChannelSettings,
    client: ExchangeClient,
    fm: AssetFileManagement,
    channel_handle: str,
    creator_map_backend: CreatorMap,
    name_map_backend: NameMap,
    validator: SchemaValidator,
) -> bool:
    '''
    Scrapes a single YouTube channel and uploads it to the Scrape Exchange.

    :param settings: Tool settings.
    :param client: The Scrape Exchange client instance.
    :param fm: AssetFileManagement instance owning the channel data directory.
    :param channel_handle: The name of the YouTube channel to scrape.
    :param creator_map_backend: Shared CreatorMap for
        channel_id → handle persistence.
    :param name_map_backend: Shared NameMap for
        channel_title → channel_id persistence.
    :returns: whether channel scraping/uploading failed
    :raises: (none)
    '''

    extra: dict[str, str] = {'channel_handle': channel_handle}
    logging.debug('Processing channel', extra=extra)
    filename: str = get_channel_filename(channel_handle)
    extra['filename'] = filename
    base_path: Path = fm.base_dir / filename

    if await _skip_due_to_existing_state(fm, filename, base_path, extra):
        return False

    channel: YouTubeChannel | None = None

    if not base_path.exists():
        logging.debug(
            'Channel not scraped, scraping now', extra=extra,
        )
        channel = YouTubeChannel(
            channel_handle=channel_handle, deno_path=DENO_PATH,
            po_token_url=PO_TOKEN_URL, debug=True,
            save_dir=settings.channel_data_directory,
            with_download_client=False,
        )
        ok: bool
        scrape_proxy: str | None
        ok, scrape_proxy = await _try_scrape_channel(
            channel, settings, fm, channel_handle, extra,
        )
        if not ok:
            return False

        scrape_proxy_ip: str = (
            extract_proxy_ip(scrape_proxy) if scrape_proxy else 'none'
        )
        scrape_proxy_network: str = proxy_network_for(scrape_proxy_ip)

        if _channel_has_no_content(
            channel, scrape_proxy_ip, scrape_proxy_network, channel_handle,
        ):
            return False

        if not await _persist_scraped_channel(
            fm, filename, channel, channel_handle,
        ):
            return True

        METRIC_CHANNELS_SCRAPED.labels(
            worker_id=get_worker_id(),
            proxy_ip=scrape_proxy_ip,
            proxy_network=scrape_proxy_network,
        ).inc()
        logging.info(
            'Downloaded channel',
            extra={
                'channel_handle': channel_handle,
                'proxy_ip': scrape_proxy_ip,
                'proxy_network': scrape_proxy_network,
            },
        )

    if settings.channel_no_upload:
        logging.debug(
            'No-upload flag set, skipping upload for channel',
            extra={'channel_handle': channel_handle},
        )
        return False

    logging.debug(
        'Uploading channel to Scrape Exchange',
        extra={'channel_handle': channel_handle},
    )
    # If we reached here via the on-disk path (file existed but was
    # never uploaded, or local base is newer than uploaded copy),
    # ``channel`` was never assigned. Load it from disk before
    # enqueueing.
    if channel is None:
        channel = await _load_channel_from_disk(
            fm, filename, channel_handle,
        )
        if channel is None:
            return True

    # Fire-and-forget: background worker moves the file on success;
    # on queue full the file stays in base_dir for the next retry.
    if await enqueue_upload_channel(
        settings, client, fm, filename, channel,
        creator_map_backend, name_map_backend,
        validator,
    ):
        METRIC_CHANNELS_ENQUEUED.labels(
            worker_id=get_worker_id(),
        ).inc()
    return False


async def resolve_channel_upload_handle(
    channel: YouTubeChannel,
    creator_map_backend: CreatorMap,
    name_map_backend: NameMap,
) -> str:
    '''
    Resolve the handle to use for uploading *channel*.

    Returns ``channel.channel_handle``, which ``scrape_channel_content``
    has already populated with the canonical handle from YouTube's
    vanityChannelUrl (or left as the input handle when no canonical
    was returned). Writes the result to the creator map so RSS/video
    scrapers can read it. Also writes
    ``(channel.title, channel.channel_id)`` to the name map so
    re-ingest can recover ids from legacy display-name-only video
    records.

    :param channel: The scraped channel.
    :param creator_map_backend: Shared creator map backend.
    :param name_map_backend: Shared name map backend
        (channel_title → channel_id).
    :returns: The handle to use for the upload.
    '''

    handle: str = channel.channel_handle
    CREATOR_MAP_RESOLUTION_TOTAL.labels(
        scraper='channel', outcome='canonical',
    ).inc()

    # Issue the creator_map and name_map writes concurrently so the
    # bulk-upload sweep pays one round-trip of Redis latency per
    # channel instead of two.
    if channel.channel_id:
        writes: list = [
            creator_map_backend.put(channel.channel_id, handle),
        ]
        if channel.title:
            writes.append(
                name_map_backend.put(
                    asset_title=channel.title,
                    asset_id=channel.channel_id,
                )
            )
        await asyncio.gather(*writes)
    return handle


async def enqueue_upload_channel(
    settings: ChannelSettings, client: ExchangeClient,
    fm: AssetFileManagement, filename: str,
    channel: YouTubeChannel,
    creator_map_backend: CreatorMap,
    name_map_backend: NameMap,
    validator: SchemaValidator,
) -> bool:
    '''
    Fire-and-forget upload of a scraped channel to Scrape Exchange.

    Returns immediately; the background worker inside
    :class:`ExchangeClient` performs the POST with retries and, on
    HTTP 201, moves the channel file from ``base_dir`` to
    ``uploaded_dir`` via ``fm.mark_uploaded``. If the queue is full
    (API down, retries backing up) the enqueue is dropped and the
    file stays in ``base_dir`` for the next iteration to retry.

    :returns: ``True`` if the job was enqueued, ``False`` if dropped
        or if schema validation failed.
    '''

    handle: str = await resolve_channel_upload_handle(
        channel, creator_map_backend, name_map_backend,
    )
    channel.channel_handle = handle

    record_dict: dict = channel.to_dict(with_video_ids=False)
    err: str | None = validator.validate(record_dict)
    if err is not None:
        logging.warning(
            'Channel record failed schema validation, '
            'marking invalid and skipping upload',
            extra={
                'filename': filename,
                'channel_id': channel.channel_id,
                'channel_handle': handle,
                'validation_error': err,
            },
        )
        try:
            await fm.mark_invalid(filename)
        except OSError as exc:
            logging.warning(
                'Failed to mark channel file invalid',
                exc=exc,
                extra={'filename': filename},
            )
        return False

    logging.info(
        'Enqueuing channel for upload',
        extra={'channel_handle': channel.channel_handle},
    )
    # platform_content_id and platform_creator_id are intentionally
    # omitted: the server derives them from the channel schema's
    # ``x-scrape-field`` markers (``channel_id`` →
    # ``platform_content_id``, ``channel_handle`` →
    # ``platform_creator_id``) which are present in the data dict.
    return client.enqueue_upload(
        f'{settings.exchange_url}{client.POST_DATA_API}',
        json={
            'username': settings.schema_owner,
            'platform': 'youtube',
            'entity': 'channel',
            'version': settings.schema_version,
            'source_url': channel.url,
            'data': record_dict,
        },
        file_manager=fm,
        filename=filename,
        entity='channel',
        log_extra={
            'channel_handle': channel.channel_handle,
            'channel_id': channel.channel_id,
        },
    )


def _normalize_channel_line(raw_line: str) -> str | None:
    '''
    Strip and normalize one raw line from the channel list file.

    :returns: the normalized line, or ``None`` when the line should
        be skipped (blank, comment, or contains whitespace).
    '''

    line: str = raw_line.strip()
    if not line or line.startswith('#'):
        return None
    if ' ' in line:
        logging.info(
            'Skipping line with spaces, likely not a channel handle',
            extra={'line': line},
        )
        return None

    if line.startswith('channel/'):
        line = line[len('channel/'):]
    if line.startswith('uc'):
        line = line[0].upper() + line[1].upper() + line[2:]
    elif (line.startswith('{') and line.endswith('}')
            and ('channel' in line)):
        # JSONL, perhaps created with yt_discover_channels.py.
        # The authoritative map write happens post-scrape in
        # resolve_channel_upload_handle using YouTube's canonical
        # handle; the user-supplied casing here is not written.
        data: dict[str, str | int] = json.loads(line)
        line = data.get('channel', line)

    return line


def _resolve_known_channel_id(
    channel_id: str, channel_map_data: dict[str, str],
    fm: AssetFileManagement,
) -> tuple[str | None, str | None]:
    '''
    Look up a channel ID in the creator map and on the unresolved
    marker file. Returns ``(handle, unresolved_id)``: a handle if we
    already have a mapping, an unresolved id to queue for resolution,
    or both ``None`` if the id previously failed to resolve and
    should be ignored.
    '''

    if channel_id in channel_map_data:

        return channel_map_data[channel_id], None

    marker: Path = fm.marker_path(
        f'{CHANNEL_FILE_PREFIX}{channel_id}', '.unresolved',
    )
    if marker.exists():
        logging.debug(
            'Channel ID previously failed to resolve, skipping',
            extra={'channel_id': channel_id},
        )
        return None, None
    return None, channel_id


def _parse_channel_line(
    raw_line: str, channel_map_data: dict[str, str],
    fm: AssetFileManagement,
) -> tuple[str | None, str | None]:
    '''
    Parse one line from the channel list file.

    :returns: a ``(channel_handle, unresolved_channel_id)`` tuple.
        Either or both may be ``None``. An ``unresolved_channel_id``
        indicates a channel ID for which no mapping is known yet and
        which the caller should submit to the resolution step.
    '''

    line: str | None = _normalize_channel_line(raw_line)
    if line is None:
        return None, None

    channel_handle: str | None = None
    if ',' in line:
        _: str
        _, channel_handle = line.split(',', 1)
        channel_handle = channel_handle.strip()

    if not channel_handle and YouTubeChannel.is_channel_id(line):
        return _resolve_known_channel_id(line, channel_map_data, fm)
    elif line.startswith('https://www.youtube.com/@'):
        channel_handle = line[len('https://www.youtube.com/@'):].strip()
    elif line.startswith(('handle', 'custom', 'user', 'c/')):
        parts: list[str] = line.split('\\')
        if len(parts) >= 2:
            channel_handle = parts[1].strip()
    elif '\t' in line:
        channel_handle = line.split('\t')[1].strip()
    else:
        channel_handle = line

    return channel_handle, None


def _filter_unscraped_candidates(
    channel_handles: set[str], fm: AssetFileManagement,
) -> list[str]:
    '''
    Drop handles we already have local data for. A handle is skipped
    when any of these exist on disk: a ``.not_found`` marker, a
    scraped file in ``base_dir``, or an uploaded file in
    ``uploaded_dir``.
    '''

    candidates: list[str] = []
    for channel_handle in channel_handles:
        if not channel_handle:
            continue
        filename: str = get_channel_filename(channel_handle)
        not_found_path: Path = fm.marker_path(
            f'{CHANNEL_FILE_PREFIX}{channel_handle}', '.not_found',
        )
        scraped_path: Path = fm.base_dir / filename
        uploaded_path: Path = fm.uploaded_dir / filename
        if (not_found_path.exists()
                or scraped_path.exists()
                or uploaded_path.exists()):
            logging.debug(
                'Skipping channel as we already have data for it',
                extra={'channel_handle': channel_handle},
            )
            continue
        candidates.append(channel_handle)
    return candidates


async def _select_new_channels(
    candidates: list[str], exchange_client: ExchangeClient,
    max_new_channels: int, already_resolved_count: int,
) -> set[str]:
    '''
    Concurrently check up to ``max_new_channels`` candidates for
    existence on Scrape Exchange and return the ones that do not yet
    exist, stopping once the per-run budget (less the channels already
    resolved earlier in the same run) has been hit.
    '''

    exists_semaphore: asyncio.Semaphore = asyncio.Semaphore(10)

    async def check_exists(name: str) -> tuple[str, bool]:
        async with exists_semaphore:
            return name, await channel_exists(exchange_client, name)

    existence_results: list[tuple[str, bool]] = await asyncio.gather(
        *(check_exists(name) for name in candidates[:max_new_channels])
    )

    selected: set[str] = set()
    for channel_handle, exists in existence_results:
        if exists:
            logging.debug(
                'Skipping channel handle previously marked as not '
                'found or found to exist',
                extra={'channel_handle': channel_handle},
            )
            continue
        selected.add(channel_handle)
        if (len(selected) + already_resolved_count) >= max_new_channels:
            logging.info(
                'Reached maximum new channels to scrape, stopping read',
                extra={'max_new_channels': max_new_channels},
            )
            break
    return selected


async def read_channels(
    file_path: str, creator_map_backend: CreatorMap,
    fm: AssetFileManagement,
    exchange_client: ExchangeClient,
    max_new_channels: int, max_resolved_channels: int,
    concurrency: int = 3,
) -> set[str]:
    '''
    Reads .lst files from the specified directory and extracts YouTube channel
    handles. This function accepts:
    - Lines that start with 'UC' or 'uc' and are 24 characters long, which are
      treated as channel IDs (these will be resolved to channel names later).
    - Lines that contain a tab character, where the channel name is expected to
      be the second word (after the tab).
    - Lines that start with youtube URL
    - a JSON object on a line

    :param directory: The directory containing .lst files with channel names.
    :param concurrency: Number of channel ID resolutions to run concurrently.
    :returns: A list of YouTube channel names.
    :raises: (none)
    '''

    logging.info('Reading channel names', extra={'file_path': file_path})

    channel_map_data: dict[str, str] = await creator_map_backend.get_all()
    new_channel_handles: set[str] = set()
    unresolved_ids: set[str] = set()

    line: str
    async with aiofiles.open(file_path, 'r') as file_desc:
        async for line in file_desc:
            channel_handle: str | None
            unresolved_id: str | None
            channel_handle, unresolved_id = _parse_channel_line(
                line, channel_map_data, fm,
            )
            if channel_handle:
                new_channel_handles.add(channel_handle)
            if unresolved_id:
                unresolved_ids.add(unresolved_id)

    logging.info(
        'Found unique channel handles in file', extra={
            'new_channel_handles_length': len(new_channel_handles),
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
            unresolved_ids, creator_map_backend, fm,
            concurrency, max_resolved_channels
        )
        new_channel_handles.update(resolved_channels)

    candidates: list[str] = _filter_unscraped_candidates(
        new_channel_handles, fm,
    )

    logging.info(
        'Checking existence of channel handles on Scrape Exchange',
        extra={'max_new_channels': max_new_channels},
    )
    checked_channel_handles: set[str] = await _select_new_channels(
        candidates, exchange_client, max_new_channels,
        len(resolved_channels),
    )

    METRIC_UNIQUE_CHANNELS_READ.labels(
        worker_id=get_worker_id()
    ).set(len(checked_channel_handles))
    logging.info(
        'Read unique channel handles from file',
        extra={
            'checked_channel_handles_length': (
                len(checked_channel_handles)
            ),
            'file_path': file_path,
        },
    )
    return checked_channel_handles


async def review_unresolved_ids(
    unresolved_ids: set[str],
    creator_map_backend: CreatorMap,
    fm: AssetFileManagement,
    concurrency: int, max_resolved_channels: int,
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
    resolved_channel_names: set[str] = set()

    async def resolve(channel_id: str) -> str | None:
        async with semaphore:
            try:
                name: str = await YouTubeChannel.resolve_channel_id(
                    channel_id
                )
                if not name:
                    unresolved_file_path: Path = fm.marker_path(
                        f'{CHANNEL_FILE_PREFIX}{channel_id}',
                        '.unresolved',
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
                        'Resolved channel ID to name with spaces; '
                        'marking unresolved to avoid re-querying',
                        extra={
                            'channel_id': channel_id,
                            'name': name,
                        },
                    )
                    await fm.mark_unresolved(
                        f'{CHANNEL_FILE_PREFIX}{channel_id}',
                        content=f'{channel_id}\t{name}\n',
                    )
                    METRIC_CHANNEL_ID_RESOLUTION_FAILURES.labels(
                        worker_id=get_worker_id()
                    ).inc()
                    return None
                await creator_map_backend.put(
                    channel_id, name,
                )

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
    deferred: int = max(0, len(ids) - max_resolved_channels)
    ids = ids[:max_resolved_channels]
    if deferred:
        logging.info(
            'Deferred channel IDs over the per-run resolution cap',
            extra={
                'deferred_count': deferred,
                'max_resolved_channels': max_resolved_channels,
                'total_unresolved': len(unresolved_ids),
            },
        )

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
