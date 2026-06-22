'''
Reusable upload-tool helpers.

This module sits above :mod:`scrape_exchange.bulk_upload`: it keeps the
HTTP bulk-upload protocol in ``bulk_upload.py`` and collects the small
operator-tool patterns shared by YouTube channels, YouTube videos, and
future TikTok uploaders.
'''

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass
from typing import Any

import orjson

from scrape_exchange.bulk_upload import (
    BulkBatchOutcome,
    finalize_bulk_batch,
    post_bulk_batch,
    upload_bulk_batch,
)
from scrape_exchange.exchange_client import ExchangeClient
from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.schema_validator import SchemaValidator
from scrape_exchange.worker_id import get_worker_id


@dataclass(frozen=True)
class BulkUploadConfig:
    schema_owner: str
    schema_version: str
    platform: str
    entity: str
    exchange_url: str
    progress_timeout_seconds: float
    filename_prefix: str


def next_filename_chunk(
    filenames: Iterator[str],
    chunk_size: int,
) -> list[str]:
    chunk: list[str] = []
    for _ in range(chunk_size):
        try:
            chunk.append(next(filenames))
        except StopIteration:
            break
    return chunk


def ndjson_line(record: dict) -> bytes:
    return orjson.dumps(record) + b'\n'


async def mark_invalid_file(
    fm: AssetFileManagement,
    filename: str,
    *,
    warning: str,
    log_extra: dict[str, Any] | None = None,
) -> None:
    try:
        await fm.mark_invalid(filename)
    except OSError as exc:
        extra: dict[str, Any] = {'filename': filename}
        if log_extra:
            extra.update(log_extra)
        logging.warning(warning, exc=exc, extra=extra)


async def validate_upload_record(
    record: dict,
    validator: SchemaValidator,
    fm: AssetFileManagement,
    filename: str,
    *,
    invalid_log_message: str,
    mark_invalid_warning: str,
    log_extra: dict[str, Any] | None = None,
) -> bool:
    err: str | None = validator.validate(record)
    if err is None:
        return True
    extra: dict[str, Any] = {
        'filename': filename,
        'validation_error': err,
    }
    if log_extra:
        extra.update(log_extra)
    logging.warning(invalid_log_message, extra=extra)
    await mark_invalid_file(
        fm,
        filename,
        warning=mark_invalid_warning,
        log_extra=extra,
    )
    return False


async def post_prepared_bulk_batch(
    batch_buf: bytes,
    batch_records: list[tuple[str, str]],
    config: BulkUploadConfig,
    client: ExchangeClient,
    fm: AssetFileManagement,
) -> tuple[str, str, BulkBatchOutcome | None]:
    return await post_bulk_batch(
        batch_buf,
        batch_records,
        schema_owner=config.schema_owner,
        schema_version=config.schema_version,
        platform=config.platform,
        entity=config.entity,
        exchange_url=config.exchange_url,
        client=client,
        fm=fm,
        filename_prefix=config.filename_prefix,
    )


async def finalize_prepared_bulk_batch(
    job_id: str,
    batch_id: str,
    err: BulkBatchOutcome | None,
    batch_records: list[tuple[str, str]],
    config: BulkUploadConfig,
    client: ExchangeClient,
    fm: AssetFileManagement,
    *,
    exchange_set: Any | None = None,
    id_from_filename: Callable[[str], str] | None = None,
) -> BulkBatchOutcome:
    if err is not None:
        return err
    return await finalize_bulk_batch(
        job_id,
        batch_id,
        batch_records,
        exchange_url=config.exchange_url,
        client=client,
        fm=fm,
        progress_timeout_seconds=config.progress_timeout_seconds,
        exchange_set=exchange_set,
        id_from_filename=id_from_filename,
    )


async def upload_prepared_bulk_batch(
    batch_buf: bytes,
    batch_records: list[tuple[str, str]],
    config: BulkUploadConfig,
    client: ExchangeClient,
    fm: AssetFileManagement,
    *,
    exchange_set: Any | None = None,
    id_from_filename: Callable[[str], str] | None = None,
) -> BulkBatchOutcome:
    return await upload_bulk_batch(
        batch_buf,
        batch_records,
        schema_owner=config.schema_owner,
        schema_version=config.schema_version,
        platform=config.platform,
        entity=config.entity,
        exchange_url=config.exchange_url,
        client=client,
        fm=fm,
        progress_timeout_seconds=config.progress_timeout_seconds,
        filename_prefix=config.filename_prefix,
        exchange_set=exchange_set,
        id_from_filename=id_from_filename,
    )


async def emit_bulk_batch_metrics(
    outcome: BulkBatchOutcome,
    *,
    platform: str,
    scraper: str,
    entity: str,
    batches_counter: Any,
    uploaded_counter: Any,
    failed_counter: Any,
    missing_result_counter: Any,
    on_success_id: Callable[[str], Awaitable[None]] | None = None,
    failed_ratio_threshold: float = 0.30,
) -> None:
    batches_counter.labels(
        platform=platform,
        scraper=scraper,
        entity=entity,
        mode='bulk',
        worker_id=get_worker_id(),
        outcome=outcome.status,
    ).inc()
    if outcome.success:
        if on_success_id is not None:
            for content_id in outcome.success_ids:
                await on_success_id(content_id)
        uploaded_counter.labels(
            platform=platform,
            scraper=scraper,
            entity=entity,
            mode='bulk',
            status='success',
            worker_id=get_worker_id(),
        ).inc(outcome.success)
    total: int = outcome.success + outcome.failed + outcome.missing
    if outcome.failed and total > 0 and (
        outcome.failed / total >= failed_ratio_threshold
    ):
        failed_counter.labels(
            platform=platform,
            scraper=scraper,
            entity=entity,
            mode='bulk',
            worker_id=get_worker_id(),
        ).inc(outcome.failed)
    if outcome.missing:
        missing_result_counter.labels(
            platform=platform,
            scraper=scraper,
            entity=entity,
            mode='bulk',
            worker_id=get_worker_id(),
        ).inc(outcome.missing)
