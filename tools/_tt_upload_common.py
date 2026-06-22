'''
Shared implementation for TikTok upload tools.

The public entrypoints live in ``tt_creator_upload.py`` and
``tt_video_upload.py``; this module keeps their bulk-upload drain/watch
logic identical while each tool supplies model parsing and requeueing.
'''

from __future__ import annotations

import asyncio
import logging
import os
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import brotli
from pydantic import AliasChoices, Field, field_validator, model_validator
from watchfiles import Change, awatch

from scrape_exchange.brotli import brotli_read
from scrape_exchange.bulk_upload import (
    BulkBatchOutcome,
    reserve_bulk_upload_slot,
    resume_pending_bulk_uploads,
)
from scrape_exchange.exchange_client import (
    ExchangeClient,
    METRIC_BACKGROUND_UPLOADS,
)
from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.proxy_loader import ProxyCatalog, set_active_catalog
from scrape_exchange.schema_validator import SchemaValidator, fetch_schema_dict
from scrape_exchange.scraper_metrics import (
    METRIC_FILES_PENDING_UPLOAD,
    METRIC_UPLOAD_BATCHES,
    METRIC_UPLOADS_FAILED,
    METRIC_UPLOADS_MISSING_RESULT,
)
from scrape_exchange.scraper_runner import ScraperRunContext, ScraperRunner
from scrape_exchange.settings import normalize_log_level
from scrape_exchange.tiktok import TikTokRateLimiter
from scrape_exchange.tiktok.settings import TikTokScraperSettings
from scrape_exchange.upload import (
    BulkUploadConfig,
    emit_bulk_batch_metrics,
    ndjson_line,
    upload_prepared_bulk_batch,
    validate_upload_record,
)
from scrape_exchange.watchdog import Watchdog
from scrape_exchange.worker_id import get_worker_id


PLATFORM: str = 'tiktok'
FILE_POSTFIX: str = '.json.br'
WATCHDOG_TOUCH_INTERVAL: float = 30.0


RecordLoader = Callable[[dict[str, Any]], dict[str, Any]]
RequeueCorrupt = Callable[[str], Awaitable[None]]


class TikTokUploadSettings(TikTokScraperSettings):
    tiktok_schema_owner: str = Field(
        default='drand',
        validation_alias=AliasChoices(
            'TIKTOK_SCHEMA_OWNER', 'tiktok_schema_owner',
        ),
        description='Username of the owner of the TikTok schema.',
    )
    tiktok_schema_version: str = Field(
        default='0.0.1',
        validation_alias=AliasChoices(
            'TIKTOK_SCHEMA_VERSION', 'tiktok_schema_version',
        ),
        description='Schema version string sent with uploads.',
    )
    max_active_bulk_jobs: int = Field(
        default=10,
        validation_alias=AliasChoices(
            'MAX_ACTIVE_BULK_JOBS', 'max_active_bulk_jobs',
        ),
        description='Maximum accepted bulk-upload jobs in flight.',
    )
    upload_log_level: str = Field(
        default='INFO',
        validation_alias=AliasChoices(
            'TIKTOK_UPLOAD_LOG_LEVEL', 'upload_log_level',
            'LOG_LEVEL', 'log_level',
        ),
        description='Logging level for TikTok upload tools.',
    )
    upload_log_file: str = Field(
        default='/dev/stdout',
        validation_alias=AliasChoices(
            'TIKTOK_UPLOAD_LOG_FILE', 'upload_log_file',
            'LOG_FILE', 'log_file',
        ),
        description='Log file path for TikTok upload tools.',
    )

    @field_validator('upload_log_level', mode='before')
    @classmethod
    def _normalize_upload_log_level(cls, value: str) -> str:
        return normalize_log_level(value)

    @model_validator(mode='after')
    def _load_proxy_catalog(self) -> 'TikTokUploadSettings':
        object.__setattr__(self, 'proxies', [])
        set_active_catalog(ProxyCatalog())
        return self

    @property
    def schema_owner(self) -> str:
        return self.tiktok_schema_owner

    @property
    def schema_version(self) -> str:
        return self.tiktok_schema_version


def validate_common_settings(
    settings: TikTokUploadSettings,
    *,
    data_directory: str | None,
    entity: str,
) -> None:
    if not settings.api_key_id or not settings.api_key_secret:
        print(
            'Error: API_KEY_ID/API_KEY_SECRET must be configured',
            file=sys.stderr,
        )
        raise SystemExit(1)
    if not data_directory:
        print(
            f'Error: TikTok {entity} data directory is not configured',
            file=sys.stderr,
        )
        raise SystemExit(1)
    if not os.path.isdir(data_directory):
        print(
            f'Directory {data_directory} does not exist. '
            'It will be created.',
            file=sys.stderr,
        )
        os.makedirs(data_directory, exist_ok=True)


def is_upload_file(filename: str, prefix: str) -> bool:
    return (
        filename.startswith(prefix)
        and filename.endswith(FILE_POSTFIX)
        and not filename.endswith('failed')
        and '.tmp' not in filename
    )


def iter_upload_files(fm: AssetFileManagement, prefix: str) -> list[str]:
    return [
        name
        for name in fm.list_base(prefix=prefix, suffix=FILE_POSTFIX)
        if is_upload_file(name, prefix)
    ]


def content_id_from_filename(
    filename: str,
    *,
    prefix: str,
) -> str:
    return filename.removeprefix(prefix).removesuffix(FILE_POSTFIX)


def bulk_config(
    settings: TikTokUploadSettings,
    *,
    entity: str,
) -> BulkUploadConfig:
    return BulkUploadConfig(
        schema_owner=settings.schema_owner,
        schema_version=settings.schema_version,
        platform=PLATFORM,
        entity=entity,
        exchange_url=settings.exchange_url,
        progress_timeout_seconds=settings.bulk_progress_timeout_seconds,
        filename_prefix=f'tiktok-{entity}s',
    )


async def prepare_line(
    filename: str,
    *,
    fm: AssetFileManagement,
    prefix: str,
    entity: str,
    validator: SchemaValidator,
    load_record: RecordLoader,
    requeue_corrupt: RequeueCorrupt | None,
) -> tuple[str, str, bytes] | None:
    path: Path = fm.base_dir / filename
    content_id: str = content_id_from_filename(filename, prefix=prefix)
    try:
        data: dict[str, Any] = await asyncio.to_thread(brotli_read, path)
    except brotli.error as exc:
        logging.warning(
            'Failed to decompress TikTok upload file; rescheduling scrape',
            exc_info=exc,
            extra={
                'filename': filename,
                'entity': entity,
                'content_id': content_id,
            },
        )
        if requeue_corrupt is not None:
            await requeue_corrupt(content_id)
            await fm.delete(filename, fail_ok=False)
        return None
    except Exception as exc:
        logging.warning(
            'Failed to read TikTok upload file',
            extra={
                'filename': filename,
                'entity': entity,
                'error': repr(exc),
            },
        )
        return None

    try:
        record: dict[str, Any] = load_record(data)
    except Exception as exc:
        logging.warning(
            'Failed to parse TikTok upload record',
            extra={
                'filename': filename,
                'entity': entity,
                'content_id': content_id,
                'error': repr(exc),
            },
        )
        await fm.mark_invalid(filename)
        return None

    if not await validate_upload_record(
        record,
        validator,
        fm,
        filename,
        invalid_log_message=(
            'TikTok record failed schema validation, '
            'marking invalid and skipping upload'
        ),
        mark_invalid_warning='Failed to mark TikTok file invalid',
        log_extra={'entity': entity, 'content_id': content_id},
    ):
        return None

    return content_id, filename, ndjson_line(record)


async def upload_batch(
    batch_buf: bytes,
    batch_records: list[tuple[str, str]],
    *,
    settings: TikTokUploadSettings,
    client: ExchangeClient,
    fm: AssetFileManagement,
    entity: str,
) -> None:
    if not batch_records:
        return
    outcome: BulkBatchOutcome = await upload_prepared_bulk_batch(
        batch_buf,
        batch_records,
        bulk_config(settings, entity=entity),
        client=client,
        fm=fm,
    )
    await emit_bulk_batch_metrics(
        outcome,
        platform=PLATFORM,
        scraper=f'tiktok_{entity}_uploader',
        entity=entity,
        batches_counter=METRIC_UPLOAD_BATCHES,
        uploaded_counter=METRIC_BACKGROUND_UPLOADS,
        failed_counter=METRIC_UPLOADS_FAILED,
        missing_result_counter=METRIC_UPLOADS_MISSING_RESULT,
    )


async def upload_existing(
    *,
    settings: TikTokUploadSettings,
    client: ExchangeClient,
    fm: AssetFileManagement,
    entity: str,
    prefix: str,
    validator: SchemaValidator,
    load_record: RecordLoader,
    requeue_corrupt: RequeueCorrupt | None,
) -> None:
    files: list[str] = await asyncio.to_thread(
        iter_upload_files, fm, prefix,
    )
    METRIC_FILES_PENDING_UPLOAD.labels(
        platform=PLATFORM,
        scraper=f'tiktok_{entity}_uploader',
        entity=entity,
        worker_id=get_worker_id(),
    ).set(len(files))
    logging.info(
        'Found TikTok files for bulk upload',
        extra={'entity': entity, 'files_length': len(files)},
    )
    if not files:
        return

    batch_buf: bytearray = bytearray()
    batch_records: list[tuple[str, str]] = []
    max_records: int = settings.bulk_batch_size
    max_bytes: int = settings.bulk_max_batch_bytes

    async def flush() -> None:
        nonlocal batch_buf, batch_records
        if not batch_records:
            return
        async with reserve_bulk_upload_slot(
            fm,
            client,
            settings.exchange_url,
            max_active_jobs=max(settings.max_active_bulk_jobs, 1),
            poll_timeout_seconds=settings.bulk_progress_timeout_seconds,
        ):
            await upload_batch(
                bytes(batch_buf),
                batch_records,
                settings=settings,
                client=client,
                fm=fm,
                entity=entity,
            )
        batch_buf = bytearray()
        batch_records = []

    for filename in files:
        Watchdog.get().touch_work()
        entry = await prepare_line(
            filename,
            fm=fm,
            prefix=prefix,
            entity=entity,
            validator=validator,
            load_record=load_record,
            requeue_corrupt=requeue_corrupt,
        )
        if entry is None:
            continue
        content_id, source_filename, line = entry
        if len(line) > max_bytes:
            logging.warning(
                'TikTok record exceeds bulk-batch byte cap, skipping',
                extra={
                    'filename': source_filename,
                    'entity': entity,
                    'content_id': content_id,
                    'record_bytes': len(line),
                    'max_bytes': max_bytes,
                },
            )
            continue
        if (
            len(batch_records) >= max_records
            or len(batch_buf) + len(line) > max_bytes
        ):
            await flush()
        batch_buf.extend(line)
        batch_records.append((content_id, source_filename))
    await flush()


async def watch_and_upload(
    *,
    settings: TikTokUploadSettings,
    client: ExchangeClient,
    fm: AssetFileManagement,
    entity: str,
    prefix: str,
    validator: SchemaValidator,
    load_record: RecordLoader,
    requeue_corrupt: RequeueCorrupt | None,
) -> None:
    base_dir: Path = fm.base_dir
    logging.info(
        'TikTok uploader watching for new files',
        extra={'entity': entity, 'watch_dir': str(base_dir)},
    )

    def _watch_filter(change: Change, raw_path: str) -> bool:
        if change not in (Change.added, Change.modified):
            return False
        path: Path = Path(raw_path)
        return path.parent == base_dir and is_upload_file(path.name, prefix)

    async for changes in awatch(
        base_dir,
        watch_filter=_watch_filter,
        recursive=False,
        yield_on_timeout=True,
    ):
        Watchdog.get().touch_work()
        if not changes:
            continue
        await upload_existing(
            settings=settings,
            client=client,
            fm=fm,
            entity=entity,
            prefix=prefix,
            validator=validator,
            load_record=load_record,
            requeue_corrupt=requeue_corrupt,
        )


async def run_upload_worker(
    ctx: ScraperRunContext,
    *,
    data_directory: str,
    entity: str,
    prefix: str,
    load_record: RecordLoader,
    requeue_corrupt_factory: Callable[
        [TikTokUploadSettings, AssetFileManagement],
        Awaitable[RequeueCorrupt | None],
    ],
    watch: bool,
) -> None:
    settings: TikTokUploadSettings = ctx.settings  # type: ignore[assignment]
    if ctx.client is None:
        raise RuntimeError('TikTok uploader requires ExchangeClient')
    fm: AssetFileManagement = AssetFileManagement(
        data_directory,
        prefix_rankings={entity: [prefix]},
    )
    schema_dict: dict = await fetch_schema_dict(
        ctx.client,
        settings.exchange_url,
        settings.schema_owner,
        PLATFORM,
        entity,
        settings.schema_version,
    )
    validator: SchemaValidator = SchemaValidator(schema_dict)
    requeue_corrupt = await requeue_corrupt_factory(settings, fm)
    await resume_pending_bulk_uploads(
        fm, ctx.client, settings.exchange_url,
    )
    await upload_existing(
        settings=settings,
        client=ctx.client,
        fm=fm,
        entity=entity,
        prefix=prefix,
        validator=validator,
        load_record=load_record,
        requeue_corrupt=requeue_corrupt,
    )
    if watch:
        await watch_and_upload(
            settings=settings,
            client=ctx.client,
            fm=fm,
            entity=entity,
            prefix=prefix,
            validator=validator,
            load_record=load_record,
            requeue_corrupt=requeue_corrupt,
        )


def build_rate_limiter(settings: TikTokUploadSettings) -> TikTokRateLimiter:
    return TikTokRateLimiter.get(
        redis_dsn=settings.redis_dsn,
        state_dir=settings.rate_limiter_state_dir,
    )


def run_tool(
    *,
    settings: TikTokUploadSettings,
    entity: str,
    concurrency: int,
    metrics_port: int,
    worker,
) -> None:
    runner: ScraperRunner = ScraperRunner(
        settings=settings,
        scraper_label=f'tiktok_{entity}_upload',
        platform=PLATFORM,
        num_processes=1,
        concurrency=max(concurrency, 1),
        metrics_port=metrics_port,
        log_file=settings.upload_log_file,
        log_level=settings.upload_log_level,
        rate_limiter_factory=build_rate_limiter,
        client_required=True,
    )
    sys.exit(runner.run_sync(worker))
