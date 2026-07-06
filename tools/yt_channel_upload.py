#!/usr/bin/env python3

'''
YouTube Channel Upload Tool. Reads already-scraped YouTube channel files from
disk, uploads them to Scrape Exchange, and moves successfully uploaded files
to the uploaded sub-directory.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import contextlib
import logging
import os
import resource
import sys

from pathlib import Path

import brotli
import redis.asyncio as aioredis

from scrape_exchange.brotli import brotli_read

from pydantic import AliasChoices, Field, field_validator
from watchfiles import Change, awatch

from scrape_exchange.bulk_upload import (
    BulkBatchOutcome,
    reserve_bulk_upload_slot,
    resume_pending_bulk_uploads,
)
from scrape_exchange.channel_scrape_queue import (
    ChannelScrapeQueueSettings,
    RedisChannelScrapeQueue,
)
from scrape_exchange.creator_map import (
    CREATOR_MAP_RESOLUTION_TOTAL,
    CreatorMap,
    FileCreatorMap,
    RedisCreatorMap,
)
from scrape_exchange.exchange_client import ExchangeClient
from scrape_exchange.file_management import (
    AssetFileManagement,
    CHANNEL_FILE_PREFIX,
)
from scrape_exchange.name_map import (
    NameMap,
    NullNameMap,
    RedisNameMap,
)
from scrape_exchange.schema_validator import SchemaValidator, fetch_schema_dict
from scrape_exchange.scraper_runner import ScraperRunContext, ScraperRunner
from scrape_exchange.settings import normalize_log_level
from scrape_exchange.upload import (
    BulkUploadConfig,
    emit_bulk_batch_metrics,
    ndjson_line,
    upload_prepared_bulk_batch,
    validate_upload_record,
)
from scrape_exchange.worker_id import get_worker_id
from scrape_exchange.watchdog import Watchdog
from scrape_exchange.youtube.exchange_channels_set import (
    RedisExchangeChannelsSet,
)
from scrape_exchange.youtube.settings import YouTubeScraperSettings
from scrape_exchange.youtube.youtube_channel import YouTubeChannel
from scrape_exchange.youtube.youtube_rate_limiter import YouTubeRateLimiter

from scrape_exchange.scraper_metrics import (
    METRIC_UPLOADS_ENQUEUED as METRIC_CHANNELS_ENQUEUED,
    METRIC_UPLOADS_SKIPPED as METRIC_UPLOADED_FILE_EXISTS,
    METRIC_UPLOADS_FAILED as METRIC_CHANNELS_BULK_FAILED,
    METRIC_UPLOADS_MISSING_RESULT as METRIC_CHANNELS_BULK_MISSING_RESULT,
    METRIC_UPLOAD_BATCHES as METRIC_BULK_BATCHES,
    METRIC_FILES_PENDING_UPLOAD,
)
from scrape_exchange.exchange_client import (
    METRIC_BACKGROUND_UPLOADS as METRIC_CHANNELS_BULK_UPLOADED,
)


CHANNEL_FILE_POSTFIX = '.json.br'
CHANNEL_RSS_FILE_PREFIX = 'channel-rss-'
SCRAPER_LABEL: str = 'channel_uploader'
_WATCHDOG_TOUCH_INTERVAL: float = 30.0


FAIL_MARK_INVALID: str = 'Failed to mark channel file invalid'


class ChannelUploadSettings(YouTubeScraperSettings):
    '''Configuration for the channel upload worker.'''

    schema_owner: str = Field(
        default='boinko',
        validation_alias=AliasChoices('SCHEMA_OWNER', 'schema_owner'),
        description='Username of the owner of the YouTube channel schema',
    )
    schema_version: str = Field(
        default='0.0.2',
        validation_alias=AliasChoices('SCHEMA_VERSION', 'schema_version'),
        description='Schema version string sent with uploads',
    )
    channel_upload_watch: bool = Field(
        default=True,
        validation_alias=AliasChoices(
            'CHANNEL_UPLOAD_WATCH', 'channel_upload_watch',
        ),
        description=(
            'After the initial bulk sweep, keep watching '
            'YOUTUBE_CHANNEL_DATA_DIR for new channel files.'
        ),
    )
    channel_upload_concurrency: int = Field(
        default=3,
        validation_alias=AliasChoices(
            'CHANNEL_UPLOAD_CONCURRENCY', 'channel_upload_concurrency',
        ),
        description='Number of concurrent per-file upload prep workers.',
    )
    channel_upload_num_processes: int = Field(
        default=1,
        validation_alias=AliasChoices(
            'CHANNEL_UPLOAD_NUM_PROCESSES',
            'channel_upload_num_processes',
            'CHANNEL_NUM_PROCESSES',
            'channel_num_processes',
        ),
        description='Number of upload worker processes to spawn.',
    )
    metrics_port: int = Field(
        default=9599,
        validation_alias=AliasChoices(
            'CHANNEL_UPLOAD_METRICS_PORT', 'channel_upload_metrics_port',
            'CHANNEL_METRICS_PORT', 'channel_metrics_port',
        ),
        description='Port for the Prometheus metrics HTTP server',
    )
    channel_upload_log_level: str = Field(
        default='INFO',
        validation_alias=AliasChoices(
            'CHANNEL_UPLOAD_LOG_LEVEL', 'channel_upload_log_level',
            'CHANNEL_LOG_LEVEL', 'channel_log_level',
            'LOG_LEVEL', 'log_level',
        ),
        description='Logging level for the channel uploader.',
    )
    channel_upload_log_file: str = Field(
        default='/dev/stdout',
        validation_alias=AliasChoices(
            'CHANNEL_UPLOAD_LOG_FILE', 'channel_upload_log_file',
            'CHANNEL_LOG_FILE', 'channel_log_file',
            'LOG_FILE', 'log_file',
        ),
        description='Log file path for the channel uploader.',
    )
    max_active_bulk_jobs: int = Field(
        default=10,
        validation_alias=AliasChoices(
            'MAX_ACTIVE_BULK_JOBS', 'max_active_bulk_jobs',
        ),
        description=(
            'Maximum number of accepted bulk-upload jobs tracked '
            'in the local .bulk state directory that may be in '
            'flight at once. New batches block at this ceiling '
            'until one pending job reaches a terminal result and '
            'is reconciled.'
        ),
    )

    @field_validator('channel_upload_log_level', mode='before')
    @classmethod
    def _normalize_channel_upload_log_level(cls, v: str) -> str:
        return normalize_log_level(v)


def _validate_settings(settings: ChannelUploadSettings) -> None:
    if not settings.api_key_id or not settings.api_key_secret:
        print(
            'Error: API key ID and secret must be provided via '
            '--api-key-id/--api-key-secret, environment variables '
            'API_KEY_ID/API_KEY_SECRET, or a .env file'
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


def _channel_id_from_filename(filename: str) -> str:
    return filename.removeprefix(
        CHANNEL_FILE_PREFIX,
    ).removesuffix(CHANNEL_FILE_POSTFIX)


def _upload_concurrency(settings: ChannelUploadSettings) -> int:
    return max(
        getattr(
            settings,
            'channel_upload_concurrency',
            getattr(settings, 'channel_concurrency', 1),
        ),
        1,
    )


def _channel_bulk_config(settings: ChannelUploadSettings) -> BulkUploadConfig:
    return BulkUploadConfig(
        schema_owner=settings.schema_owner,
        schema_version=settings.schema_version,
        platform='youtube',
        entity='channel',
        exchange_url=settings.exchange_url,
        progress_timeout_seconds=settings.bulk_progress_timeout_seconds,
        filename_prefix='channels',
    )


async def resolve_channel_upload_handle(
    channel: YouTubeChannel,
    creator_map_backend: CreatorMap,
    name_map_backend: NameMap,
) -> str | None:
    '''Resolve and persist the canonical channel handle used on upload.'''

    handle: str | None = channel.channel_handle
    CREATOR_MAP_RESOLUTION_TOTAL.labels(
        platform='youtube',
        scraper=SCRAPER_LABEL,
        outcome='canonical',
    ).inc()

    if not channel.channel_id or handle is None:
        return handle

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


async def _collect_channel_record(
    filename: str,
    fm: AssetFileManagement,
    creator_map_backend: CreatorMap,
    name_map_backend: NameMap,
    validator: SchemaValidator,
    scrape_queue: RedisChannelScrapeQueue | None = None,
) -> tuple[str, dict] | None:
    try:
        channel_data: dict = await fm.read_file(filename)
        channel: YouTubeChannel = (
            YouTubeChannel.from_dict(channel_data)
        )
    except brotli.error as exc:
        await _reschedule_corrupt_channel_file(
            filename, fm, scrape_queue, exc,
        )
        return None
    except Exception as exc:
        logging.error(
            'Error reading channel file for bulk upload',
            exc=exc,
            extra={'filename': filename},
        )
        return None

    handle: str | None = await resolve_channel_upload_handle(
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
    if not await validate_upload_record(
        record_dict,
        validator,
        fm,
        filename,
        invalid_log_message=(
            'Channel record failed schema validation, '
            'marking invalid and skipping upload'
        ),
        mark_invalid_warning=FAIL_MARK_INVALID,
        log_extra={
            'channel_id': channel.channel_id,
            'channel_handle': handle,
        },
    ):
        return None

    return channel.channel_id, record_dict


async def _reschedule_corrupt_channel_file(
    filename: str,
    fm: AssetFileManagement,
    scrape_queue: RedisChannelScrapeQueue | None,
    exc: brotli.error,
) -> None:
    channel_id: str = _channel_id_from_filename(filename)
    logging.warning(
        'Failed to decompress channel file; rescheduling scrape',
        exc_info=exc,
        extra={
            'filename': filename,
            'channel_id': channel_id,
        },
    )
    if scrape_queue is None:
        raise RuntimeError(
            'channel scrape queue is required to recover from '
            'an unreadable Brotli file'
        ) from exc
    await scrape_queue.force_rescrape(
        f'i:{channel_id}',
        mode='full',
        source='channel_upload_corrupt_file',
    )
    await fm.delete(filename, fail_ok=False)


async def _prepare_channel_line(
    filename: str,
    fm: AssetFileManagement,
    creator_map_backend: CreatorMap,
    name_map_backend: NameMap,
    validator: SchemaValidator,
    scrape_queue: RedisChannelScrapeQueue | None = None,
) -> tuple[str, str, bytes] | None:
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
            platform='youtube',
            scraper=SCRAPER_LABEL,
            entity='channel',
            reason='already_uploaded',
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

    record: tuple[str, dict] | None = await _collect_channel_record(
        filename, fm, creator_map_backend, name_map_backend, validator,
        scrape_queue,
    )
    if record is None:
        return None
    channel_id, record_dict = record
    line: bytes = ndjson_line(record_dict)
    return channel_id, filename, line


async def _upload_one_channel_batch(
    batch_buf: bytes,
    batch_records: list[tuple[str, str]],
    settings: ChannelUploadSettings,
    client: ExchangeClient,
    fm: AssetFileManagement,
    exchange_set: RedisExchangeChannelsSet | None = None,
) -> None:
    if not batch_records:
        return

    outcome: BulkBatchOutcome = await upload_prepared_bulk_batch(
        batch_buf, batch_records,
        _channel_bulk_config(settings),
        client=client,
        fm=fm,
        exchange_set=exchange_set,
        id_from_filename=_channel_id_from_filename,
    )
    await emit_bulk_batch_metrics(
        outcome,
        platform='youtube',
        scraper=SCRAPER_LABEL,
        entity='channel',
        batches_counter=METRIC_BULK_BATCHES,
        uploaded_counter=METRIC_CHANNELS_BULK_UPLOADED,
        failed_counter=METRIC_CHANNELS_BULK_FAILED,
        missing_result_counter=METRIC_CHANNELS_BULK_MISSING_RESULT,
    )


async def upload_channels(
    settings: ChannelUploadSettings,
    client: ExchangeClient,
    fm: AssetFileManagement,
    creator_map_backend: CreatorMap,
    name_map_backend: NameMap,
    validator: SchemaValidator,
    scrape_queue: RedisChannelScrapeQueue | None = None,
) -> None:
    files: list[str] = await asyncio.to_thread(
        lambda: [
            f for f in fm.list_base(
                prefix=CHANNEL_FILE_PREFIX,
                suffix=CHANNEL_FILE_POSTFIX,
            )
            if not f.endswith('failed')
        ],
    )
    METRIC_FILES_PENDING_UPLOAD.labels(
        platform='youtube',
        scraper=SCRAPER_LABEL,
        entity='channel',
        worker_id=get_worker_id(),
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
    concurrency: int = _upload_concurrency(settings)
    redis_for_set: aioredis.Redis | None = (
        creator_map_backend.redis_client
    )
    exchange_set: RedisExchangeChannelsSet | None = (
        RedisExchangeChannelsSet(redis_for_set)
        if redis_for_set is not None else None
    )

    for start in range(0, len(files), concurrency):
        # Watchdog progress signal: the initial bulk drain of a large
        # backlog runs before the watch loop and must not look hung.
        Watchdog.get().touch_work()
        chunk: list[str] = files[start:start + concurrency]
        prepared: list[
            tuple[str, str, bytes] | None
        ] = await asyncio.gather(*(
            _prepare_channel_line(
                f, fm,
                creator_map_backend, name_map_backend,
                validator, scrape_queue,
            )
            for f in chunk
        ))
        for entry in prepared:
            if entry is None:
                continue
            channel_id, filename, line = entry
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
                async with reserve_bulk_upload_slot(
                    fm,
                    client,
                    settings.exchange_url,
                    max_active_jobs=settings.max_active_bulk_jobs,
                    poll_timeout_seconds=(
                        settings.bulk_progress_timeout_seconds
                    ),
                    exchange_set=exchange_set,
                    id_from_filename=_channel_id_from_filename,
                ):
                    await _upload_one_channel_batch(
                        bytes(batch_buf), batch_records,
                        settings, client, fm,
                        exchange_set=exchange_set,
                    )
                batch_buf = bytearray()
                batch_records = []

            batch_buf.extend(line)
            batch_records.append((channel_id, filename))

    if batch_records:
        async with reserve_bulk_upload_slot(
            fm,
            client,
            settings.exchange_url,
            max_active_jobs=settings.max_active_bulk_jobs,
            poll_timeout_seconds=(
                settings.bulk_progress_timeout_seconds
            ),
            exchange_set=exchange_set,
            id_from_filename=_channel_id_from_filename,
        ):
            await _upload_one_channel_batch(
                bytes(batch_buf), batch_records,
                settings, client, fm,
                exchange_set=exchange_set,
            )


async def enqueue_upload_channel(
    settings: ChannelUploadSettings, client: ExchangeClient,
    fm: AssetFileManagement, filename: str,
    channel: YouTubeChannel,
    creator_map_backend: CreatorMap,
    name_map_backend: NameMap,
    validator: SchemaValidator,
) -> bool:
    '''Fire-and-forget upload of a scraped channel to Scrape Exchange.'''

    handle: str | None = await resolve_channel_upload_handle(
        channel, creator_map_backend, name_map_backend,
    )
    channel.channel_handle = handle

    record_dict: dict = channel.to_dict(with_video_ids=False)
    if not await validate_upload_record(
        record_dict,
        validator,
        fm,
        filename,
        invalid_log_message=(
            'Channel record failed schema validation, '
            'marking invalid and skipping upload'
        ),
        mark_invalid_warning=FAIL_MARK_INVALID,
        log_extra={
            'channel_id': channel.channel_id,
            'channel_handle': handle,
        },
    ):
        return False

    ok: bool = client.enqueue_upload(
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
        platform='youtube',
        entity='channel',
        log_extra={
            'channel_handle': channel.channel_handle,
            'channel_id': channel.channel_id,
        },
    )
    if ok:
        METRIC_CHANNELS_ENQUEUED.labels(
            platform='youtube',
            scraper=SCRAPER_LABEL,
            entity='channel',
            mode='single',
            worker_id=get_worker_id(),
        ).inc()
    return ok


def _is_base_channel_upload_file(path: Path, base_dir: Path) -> bool:
    return (
        path.parent == base_dir
        and path.name.startswith(CHANNEL_FILE_PREFIX)
        and path.name.endswith(CHANNEL_FILE_POSTFIX)
        and not path.name.endswith('failed')
        and '.tmp' not in path.name
    )


async def _wait_for_channel_changes(base_dir: Path) -> None:
    '''Block until a channel upload candidate is added or modified.'''

    def _watch_filter(change: Change, raw_path: str) -> bool:
        if change not in (Change.added, Change.modified):
            return False
        return _is_base_channel_upload_file(Path(raw_path), base_dir)

    watcher = awatch(
        base_dir,
        watch_filter=_watch_filter,
        force_polling=False,
        recursive=False,
    )
    pending: asyncio.Task | None = None
    try:
        pending = asyncio.create_task(anext(watcher))
        while True:
            Watchdog.get().touch_work()
            done, _ = await asyncio.wait(
                {pending},
                timeout=_WATCHDOG_TOUCH_INTERVAL,
            )
            if not done:
                continue
            changes = pending.result()
            logging.debug(
                'Channel upload watcher detected file changes',
                extra={'changes': [(c.name, p) for c, p in changes]},
            )
            return
    finally:
        if pending is not None and not pending.done():
            pending.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await pending
        await watcher.aclose()


async def _run_channel_bulk_batch(
    settings: ChannelUploadSettings,
    client: ExchangeClient,
    fm: AssetFileManagement,
    batch_records: list[tuple[str, Path]],
    valid_records: list[dict],
    sleep_seconds: float,
) -> None:
    '''POST one bulk batch, stream progress, apply per-record
    results. Failures leave the files in ``base_dir`` for a
    future iteration to retry.'''
    batch_buf: bytes = await asyncio.to_thread(
        lambda: b''.join(ndjson_line(record) for record in valid_records),
    )
    await _upload_one_channel_batch(
        batch_buf,
        [
            (channel_id, path.name)
            for channel_id, path in batch_records
        ],
        settings,
        client,
        fm,
    )


def _pending_channel_paths(
    fm: AssetFileManagement,
    in_flight_filenames: set[str],
    batch_size: int,
) -> list[Path]:
    candidates: list[tuple[float, Path]] = []
    for name in fm.list_base(
        prefix=CHANNEL_FILE_PREFIX,
        suffix=CHANNEL_FILE_POSTFIX,
    ):
        if name.endswith('failed') or name in in_flight_filenames:
            continue
        path: Path = fm.base_dir / name
        try:
            mtime: float = path.stat().st_mtime
        except FileNotFoundError:
            # Another uploader/scraper may have moved or replaced the
            # file after list_base() saw it. Ignore this race and keep
            # draining the remaining backlog.
            continue
        candidates.append((mtime, path))

    candidates.sort(key=lambda item: item[0])
    return [path for _mtime, path in candidates[:batch_size]]


async def _unified_bulk_upload_loop(
    settings: ChannelUploadSettings,
    client: ExchangeClient,
    fm: AssetFileManagement,
    validator: SchemaValidator,
    sleep_seconds: float = 5.0,
    scrape_queue: RedisChannelScrapeQueue | None = None,
) -> None:
    in_flight: set[asyncio.Task] = set()
    in_flight_filenames: set[str] = set()
    max_active: int = max(settings.max_active_bulk_jobs, 1)

    while True:
        # Forward-progress signal for the liveness watchdog; without it
        # the watchdog terminates this uploader after its work timeout.
        Watchdog.get().touch_work()
        batch_size: int = settings.bulk_batch_size

        base_paths: list[Path] = await asyncio.to_thread(
            _pending_channel_paths,
            fm,
            in_flight_filenames,
            batch_size,
        )

        if not base_paths:
            if in_flight:
                await asyncio.wait(
                    in_flight,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                continue
            await _wait_for_channel_changes(fm.base_dir)
            continue

        valid_records: list[dict] = []
        batch_records: list[tuple[str, Path]] = []

        for path in base_paths:
            Watchdog.get().touch_work()
            await asyncio.sleep(0)
            try:
                record_dict: dict = await asyncio.to_thread(
                    brotli_read, path,
                )
            except brotli.error as exc:
                await _reschedule_corrupt_channel_file(
                    path.name, fm, scrape_queue, exc,
                )
                continue
            except Exception as exc:
                logging.warning(
                    'Failed to decode channel file',
                    extra={
                        'path': str(path),
                        'error': repr(exc),
                    },
                )
                continue

            if not await validate_upload_record(
                record_dict,
                validator,
                fm,
                path.name,
                invalid_log_message=(
                    'Channel file failed schema validation, '
                    'marking invalid and skipping upload'
                ),
                mark_invalid_warning=FAIL_MARK_INVALID,
                log_extra={'path': str(path)},
            ):
                continue

            channel_id: str = record_dict['channel_id']
            valid_records.append(record_dict)
            batch_records.append((channel_id, path))

        if not batch_records:
            if in_flight:
                await asyncio.wait(
                    in_flight,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                continue
            await _wait_for_channel_changes(fm.base_dir)
            continue

        while len(in_flight) >= max_active:
            await asyncio.wait(
                in_flight,
                return_when=asyncio.FIRST_COMPLETED,
            )
        batch_filenames: set[str] = {
            p.name for _cid, p in batch_records
        }
        in_flight_filenames.update(batch_filenames)

        async def _gated(
            records: list[tuple[str, Path]] = batch_records,
            valids: list[dict] = valid_records,
            filenames: set[str] = batch_filenames,
        ) -> None:
            try:
                async with reserve_bulk_upload_slot(
                    fm,
                    client,
                    settings.exchange_url,
                    max_active_jobs=max_active,
                    poll_timeout_seconds=(
                        settings.bulk_progress_timeout_seconds
                    ),
                    sleep_seconds=sleep_seconds,
                ):
                    await _run_channel_bulk_batch(
                        settings, client, fm,
                        records, valids, sleep_seconds,
                    )
            finally:
                in_flight_filenames.difference_update(filenames)

        task: asyncio.Task = asyncio.create_task(_gated())
        in_flight.add(task)
        task.add_done_callback(in_flight.discard)


async def _watch_and_upload_channels(
    settings: ChannelUploadSettings,
    client: ExchangeClient,
    fm: AssetFileManagement,
    validator: SchemaValidator,
    scrape_queue: RedisChannelScrapeQueue | None = None,
) -> None:
    logging.info(
        'Starting channel bulk upload loop',
        extra={'base_dir': str(fm.base_dir)},
    )
    await _unified_bulk_upload_loop(
        settings, client, fm, validator,
        scrape_queue=scrape_queue,
    )


def _build_channel_upload_rate_limiter(
    s: ChannelUploadSettings,
) -> YouTubeRateLimiter:
    rl: YouTubeRateLimiter = YouTubeRateLimiter.get(
        state_dir=s.rate_limiter_state_dir,
        redis_dsn=s.redis_dsn,
    )
    rl.set_auto_warm_cookies(False)
    return rl


async def _run_worker(ctx: ScraperRunContext) -> None:
    settings: ChannelUploadSettings = ctx.settings

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
        extra={'settings': settings.model_dump()},
    )

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

    schema_dict: dict = await fetch_schema_dict(
        ctx.client,
        settings.exchange_url,
        settings.schema_owner,
        'youtube',
        'channel',
        settings.schema_version,
    )
    validator: SchemaValidator = SchemaValidator(schema_dict)

    resume_redis: aioredis.Redis | None = (
        creator_map_backend.redis_client
    )
    scrape_queue: RedisChannelScrapeQueue | None = (
        RedisChannelScrapeQueue(
            resume_redis, ChannelScrapeQueueSettings(),
        )
        if resume_redis is not None else None
    )
    resume_exchange_set: RedisExchangeChannelsSet | None = (
        RedisExchangeChannelsSet(resume_redis)
        if resume_redis is not None else None
    )
    await resume_pending_bulk_uploads(
        fm, ctx.client, settings.exchange_url,
        exchange_set=resume_exchange_set,
        id_from_filename=_channel_id_from_filename,
    )
    await upload_channels(
        settings, ctx.client, fm,
        creator_map_backend, name_map_backend,
        validator, scrape_queue,
    )

    if settings.channel_upload_watch:
        await _watch_and_upload_channels(
            settings, ctx.client, fm, validator, scrape_queue,
        )


def main() -> None:
    settings: ChannelUploadSettings = ChannelUploadSettings()
    _validate_settings(settings)

    runner: ScraperRunner = ScraperRunner(
        settings=settings,
        scraper_label='channel_upload',
        platform='youtube',
        num_processes=(settings.channel_upload_num_processes),
        concurrency=max(settings.channel_upload_concurrency, 1),
        metrics_port=settings.metrics_port,
        log_file=settings.channel_upload_log_file,
        log_level=settings.channel_upload_log_level,
        rate_limiter_factory=_build_channel_upload_rate_limiter,
    )
    sys.exit(runner.run_sync(_run_worker))


if __name__ == '__main__':
    main()
