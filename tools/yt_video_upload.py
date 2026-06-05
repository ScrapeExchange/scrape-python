#!/usr/bin/env python3

'''
YouTube Video Upload Tool. Reads already-scraped YouTube video files from
disk, uploads them to Scrape Exchange, and moves successfully uploaded files
to the uploaded sub-directory.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import sys
import asyncio
import logging

from pathlib import Path
from asyncio import Queue, Task

import brotli
import orjson

from httpx import Response
from prometheus_client import Counter
from watchfiles import Change, awatch
from pydantic import AliasChoices, Field, field_validator

from scrape_exchange.bulk_upload import (
    BulkBatchOutcome,
    finalize_bulk_batch,
    post_bulk_batch,
    record_bulk_filter_skip,
    reserve_bulk_upload_slot,
    resume_pending_bulk_uploads,
    upload_bulk_batch,
)
from scrape_exchange.creator_map import (
    CREATOR_MAP_LOOKUP_TOTAL,
    CREATOR_MAP_RESOLUTION_TOTAL,
    CreatorMap,
    RedisCreatorMap,
)
from scrape_exchange.exchange_client import ExchangeClient
from scrape_exchange.file_management import (
    AssetFileManagement,
    VIDEO_ID_RE,
    VIDEO_MIN_FILE_PREFIX as VIDEO_MIN_PREFIX,
    VIDEO_YTDLP_FILE_PREFIX as VIDEO_YTDLP_PREFIX,
    COMPRESSED_JSON_SUFFIX as FILE_EXTENSION,
)
from scrape_exchange.schema_validator import SchemaValidator, fetch_schema_dict
from scrape_exchange.scraper_runner import ScraperRunContext, ScraperRunner
from scrape_exchange.settings import normalize_log_level
from scrape_exchange.youtube.settings import YouTubeScraperSettings
from scrape_exchange.youtube.youtube_channel import (
    YouTubeChannel,
    fallback_handle,
)
from scrape_exchange.youtube.youtube_rate_limiter import YouTubeRateLimiter
from scrape_exchange.youtube.youtube_video import YouTubeVideo
from scrape_exchange.youtube.uploaded_video_ids import UploadedVideoIds
from scrape_exchange.worker_id import get_worker_id
from scrape_exchange.watchdog import Watchdog


from scrape_exchange.scraper_metrics import (
    METRIC_UPLOADS_ENQUEUED as METRIC_VIDEOS_ENQUEUED,
    METRIC_UPLOADS_SKIPPED as METRIC_VIDEOS_ALREADY_UPLOADED,
    METRIC_UPLOADS_FAILED as METRIC_VIDEOS_BULK_FAILED,
    METRIC_UPLOADS_MISSING_RESULT as METRIC_VIDEOS_BULK_MISSING_RESULT,
    METRIC_UPLOAD_BATCHES as METRIC_VIDEO_BULK_BATCHES,
    METRIC_SCRAPE_QUEUE_SIZE as METRIC_QUEUE_SIZE,
    METRIC_WATCHER_BATCHES,
    METRIC_WATCHER_FILES_DETECTED,
    METRIC_WATCHER_FILES_SKIPPED,
    METRIC_UPLOADED_ADDS,
    METRIC_UPLOADED_LOOKUPS,
    METRIC_FILES_PENDING_UPLOAD,
)

from scrape_exchange.exchange_client import (
    METRIC_BACKGROUND_UPLOADS as METRIC_VIDEOS_BULK_UPLOADED,
)


METRIC_VIDEOS_SKIPPED_HAS_FORMATS: Counter = METRIC_VIDEOS_ALREADY_UPLOADED

SCRAPER_LABEL: str = 'video_uploader'
_QUEUE_MAXSIZE: int = 10_000
# How often an idle upload worker ticks the watchdog work signal while
# blocked waiting for files. Must be < WATCHDOG_WORK_TIMEOUT_SECONDS.
_WATCHDOG_TOUCH_INTERVAL: float = 30.0


async def _load_video_file(
    video_id: str,
    data_dir: str,
    prefix: str,
    entry: str,
    video_fm: AssetFileManagement,
) -> YouTubeVideo | None:
    '''
    Read and decompress a video JSON file from disk.

    Returns ``None`` (and logs) on missing files, corrupt
    Brotli payloads, or any other read error.  Corrupt
    files are deleted via *video_fm*.

    :param video_id: YouTube video ID.
    :param data_dir: Directory containing video files.
    :param prefix: Filename prefix (``video-min-`` or
        ``video-dlp-``).
    :param entry: Bare filename for logging / deletion.
    :param video_fm: File manager owning *data_dir*.
    :returns: Parsed video or ``None``.
    '''

    try:
        return await YouTubeVideo.from_file(
            video_id, data_dir, prefix,
        )
    except FileNotFoundError:
        logging.warning(
            'Video file not found, skipping',
            extra={'entry': entry},
        )
        return None
    except brotli.error as exc:
        logging.warning(
            'Failed to decompress video file, '
            'skipping',
            exc_info=exc,
            extra={'entry': entry},
        )
        await video_fm.delete(entry, fail_ok=False)
        return None
    except Exception as exc:
        logging.warning(
            'Failed to read video file, skipping',
            exc_info=exc,
            extra={'entry': entry},
        )
        return None


async def video_needs_uploading(
    video_fm: AssetFileManagement, filename: str,
) -> bool:
    '''
    Checks whether a video file in the base directory still needs to be
    uploaded, deleting it from disk if it has already been superseded by an
    uploaded copy.

    A video is considered superseded when an uploaded ``video-dlp-{id}``
    variant exists with a modification time greater than or equal to the
    local file (ties go to the uploaded copy).  This is checked uniformly
    for both ``video-min-`` and ``video-dlp-`` local files via
    :meth:`AssetFileManagement.is_superseded`.

    :param video_fm: AssetFileManagement instance owning the video data
        directory.
    :param filename: Bare filename to check.
    :returns: ``True`` if the file still needs to be uploaded, ``False`` if
        it was superseded (and removed).
    '''
    if not video_fm.is_superseded(filename):
        return True
    await video_fm.delete(filename, fail_ok=False)
    return False


def _is_bare_video_id(filename: str) -> bool:
    return VIDEO_ID_RE.fullmatch(filename) is not None


def _parse_entry(
    entry: str,
) -> tuple[str, str, bool] | None:
    '''
    Extract video ID, filename prefix, and scraping need
    from a queue entry filename.

    :param entry: Bare filename from the work queue.
    :returns: ``(video_id, prefix, needs_scraping)`` or
        ``None`` for unrecognised prefixes.
    '''

    if entry.startswith(VIDEO_MIN_PREFIX):
        video_id: str = entry[
            len(VIDEO_MIN_PREFIX):-len(FILE_EXTENSION)
        ]
        return video_id, VIDEO_MIN_PREFIX, True
    if entry.startswith(VIDEO_YTDLP_PREFIX):
        video_id = entry[
            len(VIDEO_YTDLP_PREFIX):-len(FILE_EXTENSION)
        ]
        return video_id, VIDEO_YTDLP_PREFIX, False
    if _is_bare_video_id(entry):
        return entry, '', True
    return None


def _record_bulk_filter_skip(reason: str) -> None:
    record_bulk_filter_skip(
        platform='youtube',
        scraper=SCRAPER_LABEL,
        entity='video',
        reason=reason,
    )


async def _add_uploaded_video_id(
    uploaded: UploadedVideoIds,
    video_id: str,
) -> None:
    try:
        await uploaded.add(video_id)
    except Exception:
        METRIC_UPLOADED_ADDS.labels(outcome='error').inc()
        raise
    METRIC_UPLOADED_ADDS.labels(outcome='ok').inc()


async def _contains_uploaded_video_id(
    uploaded: UploadedVideoIds,
    video_id: str,
) -> bool:
    try:
        found: bool = await uploaded.contains(video_id)
    except Exception:
        METRIC_UPLOADED_LOOKUPS.labels(outcome='error').inc()
        raise
    METRIC_UPLOADED_LOOKUPS.labels(
        outcome='hit' if found else 'miss',
    ).inc()
    return found


async def _contains_uploaded_video_ids(
    uploaded: UploadedVideoIds,
    video_ids: list[str],
) -> dict[str, bool]:
    if not video_ids:
        return {}
    try:
        found: dict[str, bool] = await uploaded.contains_many(
            video_ids,
        )
    except Exception:
        METRIC_UPLOADED_LOOKUPS.labels(outcome='error').inc(
            len(video_ids),
        )
        raise
    hits: int = sum(1 for value in found.values() if value)
    misses: int = len(video_ids) - hits
    if hits:
        METRIC_UPLOADED_LOOKUPS.labels(outcome='hit').inc(hits)
    if misses:
        METRIC_UPLOADED_LOOKUPS.labels(outcome='miss').inc(
            misses,
        )
    return found


async def _move_already_uploaded_to_uploaded(
    video_id: str,
    filename: str,
    uploaded: UploadedVideoIds,
    video_fm: AssetFileManagement,
    already_uploaded: bool | None = None,
) -> bool:
    if already_uploaded is None:
        already_uploaded = await _contains_uploaded_video_id(
            uploaded, video_id,
        )
    if not already_uploaded:
        return False
    logging.debug(
        'Video found in uploaded-video-ids set; moving file '
        'to uploaded',
        extra={'video_id': video_id, 'filename': filename},
    )
    await video_fm.mark_uploaded(filename)
    _record_bulk_filter_skip('uploaded_video_id')
    return True


class VideoUploadSettings(YouTubeScraperSettings):
    '''Configuration for the video upload worker.'''

    schema_owner: str = Field(
        default='boinko',
        validation_alias=AliasChoices('SCHEMA_OWNER', 'schema_owner'),
        description='Username of the owner of the YouTube video schema',
    )
    schema_version: str = Field(
        default='0.0.2',
        validation_alias=AliasChoices('SCHEMA_VERSION', 'schema_version'),
        description='Schema version string sent with uploads',
    )
    video_upload_watch: bool = Field(
        default=True,
        validation_alias=AliasChoices(
            'VIDEO_UPLOAD_WATCH', 'video_upload_watch',
        ),
        description=(
            'After the initial bulk sweep, keep watching '
            'YOUTUBE_VIDEO_DATA_DIR for new video files.'
        ),
    )
    video_upload_concurrency: int = Field(
        default=3,
        validation_alias=AliasChoices(
            'VIDEO_UPLOAD_CONCURRENCY', 'video_upload_concurrency',
            'VIDEO_CONCURRENCY', 'video_concurrency',
        ),
        description='Number of concurrent upload workers.',
    )
    metrics_port: int = Field(
        default=9399,
        validation_alias=AliasChoices(
            'VIDEO_UPLOAD_METRICS_PORT', 'video_upload_metrics_port',
            'VIDEO_METRICS_PORT', 'video_metrics_port',
        ),
        description='Port for the Prometheus metrics HTTP server',
    )
    video_upload_log_level: str = Field(
        default='INFO',
        validation_alias=AliasChoices(
            'VIDEO_UPLOAD_LOG_LEVEL', 'video_upload_log_level',
            'VIDEO_LOG_LEVEL', 'video_log_level',
            'LOG_LEVEL', 'log_level',
        ),
        description='Logging level for the video uploader.',
    )
    video_upload_log_file: str = Field(
        default='/dev/stdout',
        validation_alias=AliasChoices(
            'VIDEO_UPLOAD_LOG_FILE', 'video_upload_log_file',
            'VIDEO_LOG_FILE', 'video_log_file',
            'LOG_FILE', 'log_file',
        ),
        description='Log file path for the video uploader.',
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

    @field_validator('video_upload_log_level', mode='before')
    @classmethod
    def _normalize_video_upload_log_level(cls, v: str) -> str:
        return normalize_log_level(v)


def _build_video_upload_rate_limiter(
    s: VideoUploadSettings,
) -> YouTubeRateLimiter:
    rl: YouTubeRateLimiter = YouTubeRateLimiter.get(
        state_dir=s.rate_limiter_state_dir,
        redis_dsn=s.redis_dsn,
    )
    # Uploading existing files only needs lazy cookie acquisition for
    # occasional handle-resolution fallbacks, not proactive full-pool warmup.
    rl.set_auto_warm_cookies(False)
    return rl


async def resolve_video_upload_handle(
    video: YouTubeVideo,
    creator_map_backend: CreatorMap,
    proxy: str | None,
) -> str | None:
    '''Resolve the handle to use as platform_creator_id for *video*.'''

    def safe_fallback_handle(name: str) -> str | None:
        try:
            return fallback_handle(name)
        except ValueError as exc:
            logging.warning(
                'Video uploader: fallback handle was invalid; '
                'skipping upload',
                exc=exc,
                extra={
                    'video_id': video.video_id,
                    'channel_id': video.channel_id,
                    'channel_handle': video.channel_handle,
                },
            )
            CREATOR_MAP_RESOLUTION_TOTAL.labels(
                platform='youtube',
                scraper=SCRAPER_LABEL,
                outcome='error',
            ).inc()
            return None

    if not video.channel_id:
        if not video.channel_handle:
            logging.warning(
                'Video has neither channel_id nor channel_handle; '
                'cannot resolve upload handle, skipping',
                extra={'video_id': video.video_id},
            )
            CREATOR_MAP_RESOLUTION_TOTAL.labels(
                platform='youtube',
                scraper=SCRAPER_LABEL,
                outcome='error',
            ).inc()
            return None
        handle: str | None = safe_fallback_handle(video.channel_handle)
        if handle is None:
            return None
        CREATOR_MAP_RESOLUTION_TOTAL.labels(
            platform='youtube',
            scraper=SCRAPER_LABEL,
            outcome='fallback',
        ).inc()
        return handle

    cached: str | None = await creator_map_backend.get(video.channel_id)
    if cached:
        CREATOR_MAP_LOOKUP_TOTAL.labels(
            platform='youtube',
            scraper=SCRAPER_LABEL,
            outcome='hit',
        ).inc()
        return cached

    CREATOR_MAP_LOOKUP_TOTAL.labels(
        platform='youtube',
        scraper=SCRAPER_LABEL,
        outcome='miss',
    ).inc()

    try:
        resolved: str | None = await YouTubeChannel.resolve_channel_id(
            video.channel_id, proxy=proxy,
        )
    except Exception as exc:
        logging.warning(
            'Video uploader: InnerTube handle resolution failed; '
            'skipping upload, will retry next tick',
            exc=exc,
            extra={
                'video_id': video.video_id,
                'channel_id': video.channel_id,
                'proxy': proxy,
            },
        )
        CREATOR_MAP_RESOLUTION_TOTAL.labels(
            platform='youtube',
            scraper=SCRAPER_LABEL,
            outcome='error',
        ).inc()
        return None

    handle: str
    if resolved:
        handle = resolved
        outcome = 'canonical'
    else:
        handle = (
            safe_fallback_handle(video.channel_handle or video.channel_id)
        )
        if handle is None:
            return None
        outcome = 'fallback'
    CREATOR_MAP_RESOLUTION_TOTAL.labels(
        platform='youtube',
        scraper=SCRAPER_LABEL,
        outcome=outcome,
    ).inc()
    await creator_map_backend.put(video.channel_id, handle)
    return handle


async def enqueue_upload_video(
    client: ExchangeClient,
    settings: VideoUploadSettings,
    video_fm: AssetFileManagement,
    handle: str,
    video: YouTubeVideo,
    validator: SchemaValidator,
    *,
    filename_prefix: str = VIDEO_YTDLP_PREFIX,
    move_source_dir: Path | None = None,
    uploaded: UploadedVideoIds | None = None,
) -> bool:
    '''Enqueue a single video record for background upload.'''

    filename: str = f'{filename_prefix}{video.video_id}{FILE_EXTENSION}'
    video.channel_handle = handle
    record_dict: dict = video.to_dict()
    err: str | None = validator.validate(record_dict)
    if err is not None:
        logging.warning(
            'Video record failed schema validation, '
            'marking invalid and skipping upload',
            extra={
                'filename': filename,
                'video_id': video.video_id,
                'validation_error': err,
            },
        )
        try:
            await video_fm.mark_invalid(filename)
        except OSError as exc:
            logging.warning(
                'Failed to mark video file invalid',
                exc=exc,
                extra={'filename': filename},
            )
        return False
    return client.enqueue_upload(
        f'{settings.exchange_url}{ExchangeClient.POST_DATA_API}',
        json={
            'username': settings.schema_owner,
            'platform': 'youtube',
            'entity': 'video',
            'version': settings.schema_version,
            'source_url': video.url,
            'data': record_dict,
        },
        file_manager=video_fm,
        filename=filename,
        move_source_dir=move_source_dir,
        entity='video',
        log_extra={'video_id': video.video_id},
        on_success=(
            None if uploaded is None
            else lambda: _add_uploaded_video_id(
                uploaded,
                video.video_id,
            )
        ),
    )


def _is_video_upload_file(filename: str) -> bool:
    '''Check if a filename is an uploadable video file.'''
    return (
        (
            filename.startswith(VIDEO_YTDLP_PREFIX)
            or filename.startswith(VIDEO_MIN_PREFIX)
        )
        and filename.endswith(FILE_EXTENSION)
        and '.tmp' not in filename
    )


async def _mark_video_file_invalid(
    video_fm: AssetFileManagement,
    filename: str,
) -> None:
    try:
        await video_fm.mark_invalid(filename)
    except OSError as exc:
        logging.warning(
            'Failed to mark video file invalid',
            exc=exc,
            extra={'filename': filename},
        )


async def _collect_video_record(
    filename: str,
    settings: VideoUploadSettings,
    video_fm: AssetFileManagement,
    creator_map_backend: CreatorMap,
    proxy: str | None,
    validator: SchemaValidator,
) -> tuple[str, dict] | None:
    '''Read *filename* from base_dir and prepare a bulk-upload record.'''
    parsed: tuple[str, str, bool] | None = _parse_entry(filename)
    if parsed is None:
        logging.warning(
            'Skipping unrecognised video filename',
            extra={'filename': filename},
        )
        _record_bulk_filter_skip('unrecognised_filename')
        return None
    video_id_from_name: str
    prefix: str
    video_id_from_name, prefix, _ = parsed

    video: YouTubeVideo | None = await _load_video_file(
        video_id_from_name,
        settings.video_data_directory,
        prefix, filename, video_fm,
    )
    if video is None:
        _record_bulk_filter_skip('read_failed')
        return None

    handle: str | None = await resolve_video_upload_handle(
        video, creator_map_backend, proxy,
    )
    # We do additional data integrity checks until we've refactored the scrapers
    # to make sure this does not happen and we have updated the jsonschema to
    # catch these issues.
    if handle is None:
        logging.info(
            'Video bulk upload skipped: handle unresolved; '
            'marking invalid',
            extra={'filename': filename, 'video_id': video.video_id},
        )
        try:
            await video_fm.mark_invalid(filename)
        except OSError as exc:
            logging.warning(
                'Failed to mark video file invalid',
                exc=exc,
                extra={'filename': filename},
            )
        _record_bulk_filter_skip('no_handle')
        return None
    if any(char.isspace() for char in handle):
        logging.info(
            'Video bulk upload skipped: handle contains whitespace; '
            'marking invalid',
            extra={
                'filename': filename,
                'video_id': video.video_id,
                'channel_handle': handle,
            },
        )
        try:
            await video_fm.mark_invalid(filename)
        except OSError as exc:
            logging.warning(
                'Failed to mark video file invalid',
                exc=exc,
                extra={'filename': filename},
            )
        _record_bulk_filter_skip('invalid_handle')
        return None
    video.channel_handle = handle
    if not video.channel_url:
        video.channel_url = YouTubeChannel.CHANNEL_URL_WITH_AT.format(
            channel_handle=handle,
        )

    if not video.video_id:
        logging.warning(
            'Video has no video_id, skipping bulk upload',
            extra={'filename': filename},
        )
        _record_bulk_filter_skip('missing_video_id')
        return None

    thumbnails: dict = getattr(video, 'thumbnails', {})
    if not thumbnails:
        logging.info(
            'Video bulk upload skipped: no thumbnails; marking invalid',
            extra={'filename': filename, 'video_id': video.video_id},
        )
        await _mark_video_file_invalid(video_fm, filename)
        _record_bulk_filter_skip('no_thumbnails')
        return None

    for label, thumbnail in thumbnails.items():
        thumbnail_url: str | None
        if isinstance(thumbnail, dict):
            thumbnail_url = thumbnail.get('url')
        else:
            thumbnail_url = getattr(thumbnail, 'url', None)
        if not thumbnail_url:
            logging.info(
                'Video bulk upload skipped: thumbnail missing url; '
                'marking invalid',
                extra={
                    'filename': filename,
                    'video_id': video.video_id,
                    'thumbnail_label': label,
                },
            )
            await _mark_video_file_invalid(video_fm, filename)
            _record_bulk_filter_skip('thumbnail_missing_url')
            return None

    record_dict: dict = video.to_dict()
    err: str | None = validator.validate(record_dict)
    if err is not None:
        logging.warning(
            'Video record failed schema validation, '
            'marking invalid and skipping upload',
            extra={
                'filename': filename,
                'video_id': video.video_id,
                'validation_error': err,
            },
        )
        try:
            await video_fm.mark_invalid(filename)
        except OSError as exc:
            logging.warning(
                'Failed to mark video file invalid',
                exc=exc,
                extra={'filename': filename},
            )
        _record_bulk_filter_skip('schema_invalid')
        return None

    logging.debug(
        'Collected video record for bulk upload',
        extra={
            'filename': filename,
            'video_id': video.video_id,
            'channel_handle': handle,
            'channel_id': video.channel_id,
        },
    )
    return video.video_id, record_dict


async def _prepare_video_line(
    filename: str,
    settings: VideoUploadSettings,
    video_fm: AssetFileManagement,
    creator_map_backend: CreatorMap,
    proxy: str | None,
    validator: SchemaValidator,
    uploaded: UploadedVideoIds,
    already_uploaded: bool | None = None,
) -> tuple[str, str, bytes] | None:
    logging.debug(
        'Considering video file for bulk upload',
        extra={'filename': filename},
    )
    try:
        if not await video_needs_uploading(video_fm, filename):
            logging.debug(
                'Video file superseded, skipping',
                extra={'filename': filename},
            )
            _record_bulk_filter_skip('superseded')
            return None

        parsed: tuple[str, str, bool] | None = _parse_entry(filename)
        if parsed is None:
            return None
        video_id: str
        video_id, _, _ = parsed
        if await _move_already_uploaded_to_uploaded(
            video_id, filename, uploaded, video_fm,
            already_uploaded=already_uploaded,
        ):
            return None

        record: tuple[str, dict] | None = (
            await _collect_video_record(
                filename, settings, video_fm,
                creator_map_backend, proxy, validator,
            )
        )
        if record is None:
            return None
        record_dict: dict
        video_id, record_dict = record
        line: bytes = orjson.dumps(record_dict) + b'\n'
        return video_id, filename, line
    except OSError as exc:
        # A per-file filesystem error (e.g. ENOSPC from the ext4
        # single-directory entry limit when the data dir holds millions
        # of files) must not crash the whole bulk drain. Skip this file;
        # it is retried on the next pass and the directory keeps
        # draining via the files that do succeed.
        logging.warning(
            'Per-file OS error during upload prep; skipping file',
            extra={'filename': filename, 'error': repr(exc)},
        )
        _record_bulk_filter_skip('os_error')
        return None


async def _upload_one_video_batch(
    batch_buf: bytes,
    batch_records: list[tuple[str, str]],
    settings: VideoUploadSettings,
    client: ExchangeClient,
    video_fm: AssetFileManagement,
    uploaded: UploadedVideoIds,
) -> None:
    '''Backwards-compatible one-call upload helper.

    Production hot path (``_spawn_batch._gated``) calls
    :func:`_post_one_video_batch` and
    :func:`_finalize_one_video_batch` directly so it can release
    the batch bytes between phases. This wrapper is kept so
    existing one-shot callers and tests continue to work.
    '''
    if not batch_records:
        return
    outcome: BulkBatchOutcome = await upload_bulk_batch(
        batch_buf, batch_records,
        schema_owner=settings.schema_owner,
        schema_version=settings.schema_version,
        platform='youtube',
        entity='video',
        exchange_url=settings.exchange_url,
        client=client,
        fm=video_fm,
        progress_timeout_seconds=(
            settings.bulk_progress_timeout_seconds
        ),
        filename_prefix='videos',
    )
    await _emit_video_batch_metrics(outcome, uploaded)


async def _post_one_video_batch(
    batch_buf: bytes,
    batch_records: list[tuple[str, str]],
    settings: VideoUploadSettings,
    client: ExchangeClient,
    video_fm: AssetFileManagement,
) -> tuple[str, str, BulkBatchOutcome | None]:
    '''POST one batch and return ``(job_id, batch_id, error)``.

    Callers MUST drop their ``batch_buf`` reference once this
    returns — the remaining progress / results phases only need
    ``batch_records``.
    '''
    return await post_bulk_batch(
        batch_buf, batch_records,
        schema_owner=settings.schema_owner,
        schema_version=settings.schema_version,
        platform='youtube',
        entity='video',
        exchange_url=settings.exchange_url,
        client=client,
        fm=video_fm,
        filename_prefix='videos',
    )


async def _finalize_one_video_batch(
    job_id: str,
    batch_id: str,
    err: BulkBatchOutcome | None,
    batch_records: list[tuple[str, str]],
    settings: VideoUploadSettings,
    client: ExchangeClient,
    video_fm: AssetFileManagement,
    uploaded: UploadedVideoIds,
) -> None:
    '''Wait for the bulk job to finish, apply per-record results,
    and emit metrics. Does not touch the original batch bytes.'''
    outcome: BulkBatchOutcome
    if err is not None:
        outcome = err
    else:
        outcome = await finalize_bulk_batch(
            job_id, batch_id, batch_records,
            exchange_url=settings.exchange_url,
            client=client,
            fm=video_fm,
            progress_timeout_seconds=(
                settings.bulk_progress_timeout_seconds
            ),
        )
    await _emit_video_batch_metrics(outcome, uploaded)


async def _emit_video_batch_metrics(
    outcome: BulkBatchOutcome,
    uploaded: UploadedVideoIds,
) -> None:
    '''Record Prometheus counters from a bulk-batch outcome and
    add successful video IDs to the uploaded-IDs set.'''
    METRIC_VIDEO_BULK_BATCHES.labels(
        platform='youtube',
        scraper=SCRAPER_LABEL,
        entity='video',
        mode='bulk',
        worker_id=get_worker_id(),
        outcome=outcome.status,
    ).inc()
    if outcome.success:
        for video_id in outcome.success_ids:
            await _add_uploaded_video_id(uploaded, video_id)
        METRIC_VIDEOS_BULK_UPLOADED.labels(
            platform='youtube',
            scraper=SCRAPER_LABEL,
            entity='video',
            mode='bulk',
            status='success',
            worker_id=get_worker_id(),
        ).inc(outcome.success)
    total: int = outcome.success + outcome.failed + outcome.missing
    if outcome.failed and total > 0 and (
        outcome.failed / total >= 0.30
    ):
        METRIC_VIDEOS_BULK_FAILED.labels(
            platform='youtube',
            scraper=SCRAPER_LABEL,
            entity='video',
            mode='bulk',
            worker_id=get_worker_id(),
        ).inc(outcome.failed)
    if outcome.missing:
        METRIC_VIDEOS_BULK_MISSING_RESULT.labels(
            platform='youtube',
            scraper=SCRAPER_LABEL,
            entity='video',
            mode='bulk',
            worker_id=get_worker_id(),
        ).inc(outcome.missing)


async def upload_videos(
    settings: VideoUploadSettings,
    client: ExchangeClient,
    video_fm: AssetFileManagement,
    creator_map_backend: CreatorMap,
    validator: SchemaValidator,
    uploaded: UploadedVideoIds,
) -> None:
    '''Bulk-upload video-min and video-dlp files from base_dir.

    Up to ``settings.max_active_bulk_jobs`` accepted bulk jobs are
    kept in flight concurrently. Accepted jobs are counted by their
    durable ``.bulk`` state files, so a progress-channel failure
    does not free a slot while the server-side job is still pending.
    '''
    files: list[str] = video_fm.list_base(
        prefix=VIDEO_YTDLP_PREFIX, suffix=FILE_EXTENSION,
    ) + video_fm.list_base(
        prefix=VIDEO_MIN_PREFIX, suffix=FILE_EXTENSION,
    )
    files = [f for f in files if not f.endswith('failed')]
    METRIC_FILES_PENDING_UPLOAD.labels(
        platform='youtube',
        scraper=SCRAPER_LABEL,
        entity='video',
        worker_id=get_worker_id(),
    ).set(len(files))
    logging.info(
        'Found video files for bulk upload',
        extra={'files_length': len(files)},
    )
    if not files:
        return

    proxies: list[str] = settings.proxies
    proxy: str | None = proxies[0] if proxies else None

    batch_buf: bytearray = bytearray()
    batch_records: list[tuple[str, str]] = []
    max_records: int = settings.bulk_batch_size
    max_bytes: int = settings.bulk_max_batch_bytes
    concurrency: int = max(settings.video_upload_concurrency, 1)
    bulk_semaphore: asyncio.Semaphore = asyncio.Semaphore(
        max(settings.max_active_bulk_jobs, 1),
    )
    max_in_flight_batches: int = max(settings.max_active_bulk_jobs, 1)
    in_flight: set[Task] = set()

    async def _wait_for_one_batch() -> None:
        done: set[Task]
        done, _pending = await asyncio.wait(
            in_flight, return_when=asyncio.FIRST_COMPLETED,
        )
        in_flight.difference_update(done)
        await asyncio.gather(*done, return_exceptions=True)

    def _spawn_batch(
        buf: bytes,
        records: list[tuple[str, str]],
    ) -> None:
        async def _gated(_buf: bytes) -> None:
            # _buf is a parameter — NOT a closure capture — so
            # this frame can drop the bytes between the POST
            # and the long progress wait. Closure-capturing
            # buf would keep the batch alive through the
            # entire bulk lifecycle, blowing up memory.
            async with bulk_semaphore:
                async with reserve_bulk_upload_slot(
                    video_fm,
                    client,
                    settings.exchange_url,
                    max_active_jobs=settings.max_active_bulk_jobs,
                    poll_timeout_seconds=(
                        settings.bulk_progress_timeout_seconds
                    ),
                ):
                    job_id: str
                    batch_id: str
                    err: BulkBatchOutcome | None
                    (
                        job_id, batch_id, err,
                    ) = await _post_one_video_batch(
                        _buf, records,
                        settings, client, video_fm,
                    )
                    # Drop the batch bytes before the
                    # progress wait. Only batch_records is
                    # carried into the finalize phase.
                    _buf = b''  # noqa: F841
                    await _finalize_one_video_batch(
                        job_id, batch_id, err, records,
                        settings, client, video_fm, uploaded,
                    )
        task: Task = asyncio.create_task(_gated(buf))
        in_flight.add(task)

    try:
        for start in range(0, len(files), concurrency):
            # Watchdog progress signal: the initial bulk drain of a
            # large backlog runs before the watch loop and must not
            # look hung.
            Watchdog.get().touch_work()
            chunk: list[str] = files[start:start + concurrency]
            chunk_parsed: dict[str, tuple[str, str, bool]] = {}
            for filename in chunk:
                parsed: tuple[str, str, bool] | None = (
                    _parse_entry(filename)
                )
                if parsed is not None:
                    chunk_parsed[filename] = parsed
            already_uploaded: dict[str, bool] = (
                await _contains_uploaded_video_ids(
                    uploaded,
                    [
                        parsed[0]
                        for parsed in chunk_parsed.values()
                    ],
                )
            )
            prepared: list[
                tuple[str, str, bytes] | None
            ] = await asyncio.gather(*(
                _prepare_video_line(
                    f, settings, video_fm,
                    creator_map_backend, proxy, validator,
                    uploaded,
                    already_uploaded=(
                        already_uploaded.get(chunk_parsed[f][0])
                        if f in chunk_parsed else None
                    ),
                )
                for f in chunk
            ))
            for entry in prepared:
                if entry is None:
                    continue
                video_id: str
                filename: str
                line: bytes
                video_id, filename, line = entry
                if len(line) > max_bytes:
                    logging.warning(
                        'Video record exceeds bulk-batch byte cap, '
                        'skipping',
                        extra={
                            'filename': filename,
                            'video_id': video_id,
                            'record_bytes': len(line),
                            'max_bytes': max_bytes,
                        },
                    )
                    continue
                if (
                    len(batch_records) >= max_records
                    or len(batch_buf) + len(line) > max_bytes
                ):
                    _spawn_batch(
                        bytes(batch_buf), batch_records,
                    )
                    if len(in_flight) >= max_in_flight_batches:
                        await _wait_for_one_batch()
                    batch_buf = bytearray()
                    batch_records = []
                batch_buf.extend(line)
                batch_records.append((video_id, filename))

        if batch_records:
            _spawn_batch(bytes(batch_buf), batch_records)
            if len(in_flight) >= max_in_flight_batches:
                await _wait_for_one_batch()
    finally:
        if in_flight:
            await asyncio.gather(*in_flight, return_exceptions=True)


async def _process_upload_file(
    filename: str,
    settings: VideoUploadSettings,
    video_fm: AssetFileManagement,
    client: ExchangeClient,
    creator_map_backend: CreatorMap,
    validator: SchemaValidator,
    proxy: str | None,
    uploaded: UploadedVideoIds,
) -> bool:
    parsed: tuple[str, str, bool] | None = _parse_entry(filename)
    if parsed is None:
        return False
    video_id: str
    prefix: str
    video_id, prefix, _ = parsed
    try:
        if not await video_needs_uploading(video_fm, filename):
            METRIC_VIDEOS_ALREADY_UPLOADED.labels(
                platform='youtube',
                scraper=SCRAPER_LABEL,
                entity='video',
                reason='already_uploaded',
                worker_id=get_worker_id(),
            ).inc()
            return False
        if await _move_already_uploaded_to_uploaded(
            video_id, filename, uploaded, video_fm,
        ):
            return False
        video: YouTubeVideo | None = await _load_video_file(
            video_id, settings.video_data_directory,
            prefix, filename, video_fm,
        )
        if video is None:
            return False
        handle: str | None = await resolve_video_upload_handle(
            video, creator_map_backend, proxy,
        )
        if handle is None:
            return False
        uploaded_ok: bool = await enqueue_upload_video(
            client, settings, video_fm, handle, video, validator,
            filename_prefix=prefix,
            uploaded=uploaded,
        )
    except OSError as exc:
        # Per-file filesystem error (e.g. ENOSPC): skip this file rather
        # than killing the worker; it is retried on a later pass.
        logging.warning(
            'Per-file OS error during upload; skipping file',
            extra={'filename': filename, 'error': repr(exc)},
        )
        return False
    if uploaded_ok:
        METRIC_VIDEOS_ENQUEUED.labels(
            platform='youtube',
            scraper=SCRAPER_LABEL,
            entity='video',
            mode='single',
            worker_id=get_worker_id(),
        ).inc()
    return uploaded_ok


async def _upload_worker(
    proxy: str | None,
    queue: Queue,
    settings: VideoUploadSettings,
    video_fm: AssetFileManagement,
    client: ExchangeClient,
    creator_map_backend: CreatorMap,
    validator: SchemaValidator,
    uploaded: UploadedVideoIds,
) -> None:
    while True:
        # Forward-progress signal for the liveness watchdog: this loop
        # is the video uploader's unit of work. The bounded wait means
        # an idle worker (empty queue) still ticks the signal every
        # _WATCHDOG_TOUCH_INTERVAL seconds, while a worker wedged inside
        # _process_upload_file goes stale and is correctly terminated.
        Watchdog.get().touch_work()
        try:
            filename: str = await asyncio.wait_for(
                queue.get(), timeout=_WATCHDOG_TOUCH_INTERVAL,
            )
        except asyncio.TimeoutError:
            continue
        try:
            await _process_upload_file(
                filename, settings, video_fm, client,
                creator_map_backend, validator, proxy,
                uploaded,
            )
        finally:
            queue.task_done()


async def _watch_and_upload(
    queue: Queue,
    video_fm: AssetFileManagement,
    settings: VideoUploadSettings,
) -> None:
    await queue.join()
    base: Path = Path(settings.video_data_directory)
    logging.info(
        'Video uploader watching for new video files',
        extra={'watch_dir': str(base)},
    )
    wid: str = get_worker_id()
    async for changes in awatch(
        base,
        watch_filter=lambda change, path: (
            change in (Change.added, Change.modified)
            and _is_video_upload_file(Path(path).name)
        ),
    ):
        METRIC_WATCHER_BATCHES.labels(
            platform='youtube',
            scraper=SCRAPER_LABEL,
            entity='video',
            worker_id=wid,
        ).inc()
        for _change, path in changes:
            filename: str = Path(path).name
            METRIC_WATCHER_FILES_DETECTED.labels(
                platform='youtube',
                scraper=SCRAPER_LABEL,
                entity='video',
                worker_id=wid,
            ).inc()
            if not await video_needs_uploading(video_fm, filename):
                METRIC_WATCHER_FILES_SKIPPED.labels(
                    platform='youtube',
                    scraper=SCRAPER_LABEL,
                    entity='video',
                    worker_id=wid,
                ).inc()
                continue
            await queue.put(filename)
        await queue.join()


async def upload_worker_loop(
    settings: VideoUploadSettings,
    video_fm: AssetFileManagement,
    client: ExchangeClient,
    creator_map_backend: CreatorMap,
    validator: SchemaValidator,
    uploaded: UploadedVideoIds,
    enqueue_existing: bool = True,
) -> None:
    files: list[str] = []
    if enqueue_existing:
        files = video_fm.list_base(
            prefix=VIDEO_YTDLP_PREFIX, suffix=FILE_EXTENSION,
        ) + video_fm.list_base(
            prefix=VIDEO_MIN_PREFIX, suffix=FILE_EXTENSION,
        )
        files = [
            f for f in files
            if not f.endswith('failed') and _is_video_upload_file(f)
        ]
    queue: Queue = Queue(maxsize=_QUEUE_MAXSIZE)
    METRIC_QUEUE_SIZE.labels(
        platform='youtube',
        scraper=SCRAPER_LABEL,
        entity='video',
        tier='none',
        worker_id='',
    ).set(len(files))

    worker_count: int = max(
        settings.video_upload_concurrency,
        len(settings.proxies),
        1,
    )
    if not settings.proxies:
        worker_assignments: list[str | None] = [None] * worker_count
    else:
        worker_assignments = [
            settings.proxies[i % len(settings.proxies)]
            for i in range(worker_count)
        ]
    tasks: list[Task] = [
        asyncio.create_task(
            _upload_worker(
                proxy, queue, settings, video_fm,
                client, creator_map_backend, validator,
                uploaded,
            )
        )
        for proxy in worker_assignments
    ]
    try:
        for filename in files:
            await queue.put(filename)
        if settings.video_upload_watch:
            await _watch_and_upload(queue, video_fm, settings)
        else:
            await queue.join()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await client.drain_uploads(timeout=10.0)


async def _run_worker(ctx: ScraperRunContext) -> None:
    settings: VideoUploadSettings = ctx.settings
    if ctx.client is None:
        raise RuntimeError('video uploader requires ExchangeClient')
    if not settings.redis_dsn:
        raise RuntimeError(
            'redis_dsn is required for uploaded-video-id '
            'deduplication. Set REDIS_DSN in .env.',
        )
    video_fm = AssetFileManagement(settings.video_data_directory)
    creator_map_backend: CreatorMap = RedisCreatorMap(
        settings.redis_dsn,
        platform='youtube',
    )

    video_schema_dict: dict = await fetch_schema_dict(
        ctx.client, settings.exchange_url, settings.schema_owner,
        'youtube', 'video', settings.schema_version,
    )
    video_validator = SchemaValidator(video_schema_dict)
    uploaded: UploadedVideoIds = UploadedVideoIds(
        settings.redis_dsn,
    )

    await resume_pending_bulk_uploads(
        video_fm, ctx.client, settings.exchange_url,
    )
    await upload_videos(
        settings, ctx.client, video_fm,
        creator_map_backend, video_validator, uploaded,
    )
    await upload_worker_loop(
        settings, video_fm, ctx.client,
        creator_map_backend, video_validator, uploaded,
        enqueue_existing=False,
    )


def main() -> None:
    settings = VideoUploadSettings()
    runner = ScraperRunner(
        settings=settings,
        scraper_label='video_upload',
        platform='youtube',
        num_processes=1,
        concurrency=max(
            settings.video_upload_concurrency,
            len(settings.proxies),
            1,
        ),
        metrics_port=settings.metrics_port,
        log_file=settings.video_upload_log_file,
        log_level=settings.video_upload_log_level,
        rate_limiter_factory=_build_video_upload_rate_limiter,
        client_required=True,
    )
    sys.exit(runner.run_sync(_run_worker))


if __name__ == '__main__':
    main()
