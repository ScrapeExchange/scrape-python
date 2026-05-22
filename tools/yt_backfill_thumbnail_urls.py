#!/usr/bin/env python3
'''
One-shot operator tool for backfilling URL-less thumbnails on
already-scraped YouTube video records.

Background
----------
The InnerTube video parser used to keep thumbnail entries that
YouTube returned without a ``url`` field, so a chunk of records on
disk and on scrape.exchange now have ``thumbnails`` whose entries
have ``"url": null``. The on-disk and server-side data both fail
the boinko-youtube-video-0.0.2 schema's expectation that thumbnail
``url`` is a non-empty string. The live scraper has been fixed
(commit 33afe55) but the existing records need a backfill.

What it does
------------
1. Walks ``<video_data_directory>`` and its ``uploaded/``
   subdirectory.
2. For every ``video-min-<id>.json.br`` and ``video-dlp-<id>.json.br``
   file, brotli-decodes and inspects ``data['thumbnails']``. Files
   with one or more URL-less thumbnail entries are queued.
3. For each unique video_id (a video may have both a min- and a
   dlp-prefixed file in either of two directories) it issues a
   single InnerTube re-scrape via the shared YouTubeRateLimiter.
4. Patches **only** the ``thumbnails`` field of each on-disk file
   with the freshly-scraped thumbnail dict (so yt-dlp-only fields
   like ``formats`` are preserved). Validates the merged record
   against the live schema before writing.
5. Files that lived under ``<video_data_directory>/uploaded/``
   are moved back to the base directory so the regular upload
   worker (``yt_video_upload``) re-uploads the corrected record
   on its next sweep. Files already in the base directory are
   simply rewritten in place.
6. Appends the video_id to a JSONL "processed" file. Failures are
   appended to a separate "failed" JSONL with reason and message,
   AND added to the processed list so the tool does not re-attempt
   permanent failures (deleted/private/region-blocked videos) on
   restart.

The tool is resumable: on every start it reads the processed-ids
file into a set and skips already-handled videos.

Usage
-----
::

    uv run tools/yt_backfill_thumbnail_urls.py
    uv run tools/yt_backfill_thumbnail_urls.py \\
        --concurrency 4 --max-videos 200
    uv run tools/yt_backfill_thumbnail_urls.py \\
        --single-video-id HS1bUFtYj48
    uv run tools/yt_backfill_thumbnail_urls.py --dry-run

Environment / settings keys (all also accepted as CLI flags via
pydantic-settings):

* ``YOUTUBE_VIDEO_DATA_DIR`` (inherited from the live scraper).
* ``EXCHANGE_URL``, ``SCHEMA_OWNER``, ``SCHEMA_VERSION``.
* ``PROXIES`` / ``PROXY_FILES`` — same RSS-banned proxies that the
  video scraper uses are fine here; this tool calls InnerTube only.
* ``BACKFILL_PROCESSED_IDS_PATH`` (default
  ``<video_data_dir>/.thumb_backfill_processed.jsonl``).
* ``BACKFILL_FAILED_IDS_PATH`` (default
  ``<video_data_dir>/.thumb_backfill_failed.jsonl``).
* ``BACKFILL_CONCURRENCY`` (default 1).
* ``BACKFILL_MAX_VIDEOS`` (default unset → no cap).
* ``BACKFILL_DRY_RUN`` (default False).
* ``BACKFILL_SINGLE_VIDEO_ID`` (default unset → process everything
  the scan turns up).

:maintainer: Boinko <boinko@scrape.exchange>
:copyright : Copyright 2026
:license   : GPLv3
'''

from __future__ import annotations

import asyncio
import collections
import json
import logging
import os
import sys
import time

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator

import aiofiles
import brotli
import orjson

from pydantic import AliasChoices, Field

from scrape_exchange.brotli import brotli_write, brotli_write_async
from scrape_exchange.exchange_client import ExchangeClient
from scrape_exchange.scraper_runner import (
    ScraperRunContext,
    ScraperRunner,
)
from scrape_exchange.schema_validator import (
    SchemaValidator,
    fetch_schema_dict,
)
from scrape_exchange.youtube.settings import (
    YouTubeScraperSettings,
)
from scrape_exchange.youtube.youtube_rate_limiter import (
    YouTubeCallType,
    YouTubeRateLimiter,
)
from scrape_exchange.youtube.youtube_video import YouTubeVideo


_VIDEO_MIN_PREFIX: str = 'video-min-'
_VIDEO_DLP_PREFIX: str = 'video-dlp-'
_FILE_EXTENSION: str = '.json.br'
_VIDEO_PREFIXES: tuple[str, ...] = (
    _VIDEO_MIN_PREFIX, _VIDEO_DLP_PREFIX,
)


_LOGGER: logging.Logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


class BackfillSettings(YouTubeScraperSettings):
    '''
    One-shot operator-tool settings. Inherits the YouTube
    scraper's proxy / cookie / exchange-URL config so the tool
    can drive the same rate limiter and HTTP stack as the live
    scraper.
    '''

    schema_owner: str = Field(
        default='boinko',
        validation_alias=AliasChoices(
            'SCHEMA_OWNER', 'schema_owner',
        ),
        description='Schema owner username (POST body field).',
    )
    schema_version: str = Field(
        default='0.0.2',
        validation_alias=AliasChoices(
            'SCHEMA_VERSION', 'schema_version',
        ),
        description='Video schema version sent with uploads.',
    )

    backfill_processed_ids_path: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'BACKFILL_PROCESSED_IDS_PATH',
            'backfill_processed_ids_path',
        ),
        description=(
            'JSONL file (one video_id per line) tracking which '
            'videos have been processed in this or any prior '
            'run. Defaults to '
            '<video_data_directory>/.thumb_backfill_processed.jsonl.'
        ),
    )
    backfill_failed_ids_path: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'BACKFILL_FAILED_IDS_PATH',
            'backfill_failed_ids_path',
        ),
        description=(
            'JSONL file logging videos that could not be '
            'backfilled (deleted, private, region-blocked, '
            'schema-invalid, server-side error). Each line is a '
            'JSON object with video_id, reason, message, ts. '
            'Defaults to '
            '<video_data_directory>/.thumb_backfill_failed.jsonl.'
        ),
    )
    backfill_concurrency: int = Field(
        default=1,
        validation_alias=AliasChoices(
            'BACKFILL_CONCURRENCY', 'backfill_concurrency',
        ),
        description=(
            'Number of videos to process concurrently. The '
            'shared rate limiter still caps per-proxy traffic.'
        ),
    )
    backfill_max_videos: int | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'BACKFILL_MAX_VIDEOS', 'backfill_max_videos',
        ),
        description=(
            'Optional cap on the number of videos processed in '
            'this run; useful for staged backfill rollouts.'
        ),
    )
    backfill_dry_run: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            'BACKFILL_DRY_RUN', 'backfill_dry_run',
        ),
        description=(
            'Scan only: count URL-less videos, do not rescrape, '
            'write, or POST.'
        ),
    )
    backfill_single_video_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'BACKFILL_SINGLE_VIDEO_ID', 'backfill_single_video_id',
        ),
        description=(
            'Process exactly this video_id. Used for ad-hoc '
            'verification. Other videos are skipped even if '
            'their files match the URL-less filter.'
        ),
    )

    metrics_port: int = Field(
        default=9999,
        validation_alias=AliasChoices(
            'BACKFILL_METRICS_PORT', 'backfill_metrics_port',
        ),
        description=(
            'Prometheus port for the backfill tool. Distinct '
            'from the live scrapers so the tool can run on the '
            'same host without colliding.'
        ),
    )


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def _extract_video_id_from_filename(name: str) -> str | None:
    '''Return the video_id from a ``video-{min,dlp}-<id>.json.br``
    filename, or ``None`` if *name* does not match. Marker files
    (e.g. ``...invalid``) and unrelated content (channel files)
    return None.'''
    if not name.endswith(_FILE_EXTENSION):
        return None
    for prefix in _VIDEO_PREFIXES:
        if name.startswith(prefix):
            return name[len(prefix):-len(_FILE_EXTENSION)]
    return None


def _scan_dirs(base_dir: Path) -> Iterator[tuple[Path, str]]:
    '''Yield ``(file_path, video_id)`` pairs for every video file
    under *base_dir* and ``base_dir/uploaded``. The same video_id
    can be yielded up to four times (min/dlp x base/uploaded).'''
    for parent in (base_dir, base_dir / 'uploaded'):
        if not parent.is_dir():
            continue
        for entry in parent.iterdir():
            if not entry.is_file():
                continue
            vid: str | None = _extract_video_id_from_filename(
                entry.name,
            )
            if vid is None:
                continue
            yield entry, vid


def _has_urlless_thumbnail(data: dict[str, Any]) -> bool:
    '''Return True when *data* contains thumbnails with at least
    one entry whose ``url`` field is missing, ``None``, or
    empty-string. Records without any ``thumbnails`` field are
    not the bug we are fixing and return False.'''
    thumbs: Any = data.get('thumbnails')
    if not thumbs:
        return False

    iterable: list[Any]
    if isinstance(thumbs, dict):
        iterable = list(thumbs.values())
    elif isinstance(thumbs, list):
        iterable = list(thumbs)
    else:
        return False

    for entry in iterable:
        if not isinstance(entry, dict):
            # Corrupt/null entry — counts as URL-less.
            return True
        url: Any = entry.get('url')
        if not url:
            return True
    return False


def _apply_thumbnail_patch(
    existing: dict[str, Any],
    fresh_thumbnails: dict[str, Any] | list[Any],
) -> dict[str, Any]:
    '''Return a shallow copy of *existing* with its ``thumbnails``
    field replaced by *fresh_thumbnails*. Other fields are
    untouched (preserves yt-dlp ``formats`` etc. on
    ``video-dlp-`` records).'''
    patched: dict[str, Any] = dict(existing)
    patched['thumbnails'] = fresh_thumbnails
    return patched


def _load_processed_ids(path: Path) -> set[str]:
    '''Load a JSONL processed-ids file into a set. One bare
    video_id per line. Missing file → empty set.'''
    try:
        with open(path, 'r') as fh:
            return {
                line.strip()
                for line in fh
                if line.strip()
            }
    except FileNotFoundError:
        return set()


async def _append_processed_id(path: Path, video_id: str) -> None:
    '''Atomically append *video_id* + newline to the processed-ids
    file. Creates the file (and parent dir) on first write.'''
    path.parent.mkdir(parents=True, exist_ok=True)
    async with aiofiles.open(path, 'a') as fh:
        await fh.write(f'{video_id}\n')


async def _append_failed_id(
    path: Path, video_id: str, reason: str, message: str,
) -> None:
    '''Append a single failure record (one JSON object per line)
    to the failed-ids file.'''
    path.parent.mkdir(parents=True, exist_ok=True)
    record: dict[str, str] = {
        'video_id': video_id,
        'reason': reason,
        'message': message,
        'ts': datetime.now(UTC).isoformat(),
    }
    async with aiofiles.open(path, 'a') as fh:
        await fh.write(f'{json.dumps(record)}\n')


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------


def _read_record(
    path: Path,
) -> tuple[dict[str, Any] | None, bool]:
    '''Read an on-disk video record and return ``(record, needs_rewrite)``.

    The live scraper writes brotli-compressed JSON, but a long
    backfill run cannot afford to abort on every odd file:

    * a truncated brotli stream may still yield enough cleartext
      to parse a complete JSON document — try the streaming
      decoder before giving up;
    * a file may have been written by an older code path that
      did not brotli-compress — fall back to parsing the raw
      bytes as JSON;
    * only when neither path produces valid JSON do we log and
      return ``(None, False)`` so the caller can skip this
      record.

    The second element of the return tuple is ``True`` when a
    fallback was used to produce a parseable record. The caller
    is expected to rewrite the file in proper brotli-compressed
    JSON so future readers (live scraper, other tools) do not
    trip over the same corruption.
    '''
    try:
        raw: bytes = path.read_bytes()
    except OSError as exc:
        _LOGGER.warning(
            'Failed to read video file',
            extra={'path': str(path), 'exc': str(exc)},
        )
        return None, False
    if not raw:
        return None, False

    decompressed: bytes | None = None
    needs_rewrite: bool = False
    try:
        decompressed = brotli.decompress(raw)
    except brotli.error as exc:
        # Streaming fallback: keep whatever cleartext the decoder
        # produced before the corruption point. Short documents
        # often fit before the broken segment so the recovered
        # prefix is enough to parse the JSON.
        decoder = brotli.Decompressor()
        try:
            decompressed = decoder.process(raw)
        except brotli.error:
            decompressed = None
        if decompressed:
            needs_rewrite = True
            _LOGGER.warning(
                'Brotli decode failed; using partial output',
                extra={
                    'path': str(path),
                    'exc': str(exc),
                    'partial_bytes': len(decompressed),
                },
            )

    if decompressed:
        try:
            parsed: Any = orjson.loads(decompressed)
        except orjson.JSONDecodeError:
            parsed = None
        if isinstance(parsed, dict):
            return parsed, needs_rewrite

    # Last-resort: maybe the file is plain JSON (older write path
    # or corrupted compressed wrapper around intact JSON bytes).
    try:
        parsed = orjson.loads(raw)
    except orjson.JSONDecodeError as exc:
        _LOGGER.warning(
            'Video file is neither valid brotli nor plain JSON, deleting it',
            extra={'path': str(path), 'exc': str(exc)},
        )
        os.remove(path)
        return None, False
    if isinstance(parsed, dict):
        _LOGGER.warning(
            'Video file is plain JSON, not brotli-compressed',
            extra={'path': str(path)},
        )
        return parsed, True
    return None, False


def _repair_brotli_in_place(
    path: Path, record: dict[str, Any],
) -> None:
    '''Rewrite *path* with the strict brotli-compressed encoding
    of *record*. Used to repair files whose original wrapper was
    truncated, plain-JSON, or otherwise unable to decompress on
    the strict path. Best-effort: a write failure is logged and
    swallowed because the in-memory record is still usable.'''
    try:
        brotli_write(path, record)
    except OSError as exc:
        _LOGGER.warning(
            'Failed to repair brotli wrapper on disk',
            extra={'path': str(path), 'exc': str(exc)},
        )
        return
    _LOGGER.info(
        'Repaired brotli wrapper on disk',
        extra={'path': str(path)},
    )


# ---------------------------------------------------------------------------
# Main per-video processing
# ---------------------------------------------------------------------------


async def _rescrape_thumbnails(
    video_id: str,
    rate_limiter: YouTubeRateLimiter,
) -> tuple[YouTubeVideo, str | None]:
    '''Rescrape *video_id* via InnerTube using a proxy from the
    rate limiter. Returns ``(video, None)`` on success or
    ``(video, error_message)`` if the call raised. The video
    instance may still be partially populated on failure.

    Logs at DEBUG before the rate-limit acquire and at DEBUG
    after the InnerTube call returns, so a high concurrency
    setting that piles up on per-proxy PLAYER tokens is visible
    when DEBUG logging is enabled.
    '''
    proxy: str | None = rate_limiter.select_proxy(
        YouTubeCallType.PLAYER,
    )
    video: YouTubeVideo = YouTubeVideo(video_id=video_id)
    acquire_start: float = time.monotonic()
    try:
        _LOGGER.debug(
            'Awaiting PLAYER token',
            extra={'video_id': video_id, 'proxy': proxy},
        )
        await rate_limiter.acquire(
            YouTubeCallType.PLAYER, proxy=proxy,
        )
        token_wait: float = time.monotonic() - acquire_start
        _LOGGER.debug(
            'Got PLAYER token, calling from_innertube',
            extra={
                'video_id': video_id,
                'proxy': proxy,
                'token_wait_seconds': round(token_wait, 2),
            },
        )
        await video.from_innertube(proxy=proxy)
    except Exception as exc:
        return video, f'{type(exc).__name__}: {exc}'
    return video, None


async def _process_video(
    video_id: str,
    paths: list[Path],
    rate_limiter: YouTubeRateLimiter,
    validator: SchemaValidator,
    settings: BackfillSettings,
) -> tuple[str, str | None]:
    '''Run the full per-video pipeline. Returns
    ``(status, message)`` where status is ``'ok'`` or one of
    ``'innertube_error'``, ``'still_urlless'``,
    ``'schema_invalid'``, ``'write_error'``. The corrected
    record is written back to each on-disk path; files that
    lived under ``<base>/uploaded/`` are moved back to the base
    directory so the upload worker re-uploads them.'''
    video, exc_msg = await _rescrape_thumbnails(
        video_id, rate_limiter,
    )
    if exc_msg is not None:
        return 'innertube_error', exc_msg

    if not video.thumbnails:
        return (
            'still_urlless',
            'rescrape returned no thumbnails',
        )

    fresh_thumbnails: dict[str, Any] = {
        label: thumb.to_dict()
        for label, thumb in video.thumbnails.items()
    }
    if any(
        not (entry or {}).get('url')
        for entry in fresh_thumbnails.values()
    ):
        return (
            'still_urlless',
            'rescrape returned thumbnails without url',
        )

    # Patch each on-disk file individually so that
    # video-dlp-only fields (formats, etc.) are preserved.
    # Files that were in <base>/uploaded/ are moved back to the
    # base directory after a successful patch so the upload
    # worker re-uploads the corrected record on its next sweep.
    any_written: bool = False
    for path in paths:
        existing, _needs_rewrite = _read_record(path)
        if existing is None:
            continue
        patched: dict[str, Any] = _apply_thumbnail_patch(
            existing, fresh_thumbnails,
        )
        err: str | None = validator.validate(patched)
        if err is not None:
            return 'schema_invalid', f'{path.name}: {err}'

        if settings.backfill_dry_run:
            any_written = True
            continue

        try:
            await brotli_write_async(path, patched)
        except OSError as exc:
            return (
                'write_error',
                f'{path.name}: {exc}',
            )
        any_written = True

        # If the file lived under ``uploaded/`` it had already
        # been ingested by the API; the corrected version needs
        # to round-trip through the uploader again, so move it
        # back to the base directory. The uploader's normal sweep
        # (yt_video_upload) picks it up and re-POSTs / re-moves.
        if path.parent.name == 'uploaded':
            target: Path = path.parent.parent / path.name
            try:
                path.replace(target)
            except OSError as exc:
                return (
                    'write_error',
                    f'move-back failed {path} -> {target}: '
                    f'{exc}',
                )

    if not any_written:
        return (
            'still_urlless',
            'no on-disk record could be read after rescrape',
        )

    return 'ok', None


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


_SCAN_LOG_EVERY_FILES: int = 1000
_SCAN_LOG_EVERY_SECONDS: float = 30.0


def _build_workload(
    base_dir: Path,
    processed_ids: set[str],
    single_video_id: str | None,
    repair_in_place: bool,
) -> dict[str, list[Path]]:
    '''Return ``{video_id: [paths...]}`` for every video whose
    on-disk file has at least one URL-less thumbnail. Skips
    already-processed video_ids.

    When *repair_in_place* is True, any file whose strict brotli
    decode failed but whose JSON the resilient decoder could
    still recover is rewritten in proper brotli encoding before
    the rest of the run touches it. This protects future readers
    (the live scraper, other operator tools) from the same
    decode failure even when the file's thumbnails are already
    correct and it would otherwise be skipped here.

    Emits a heartbeat log every ``_SCAN_LOG_EVERY_FILES`` files
    or ``_SCAN_LOG_EVERY_SECONDS`` seconds (whichever comes
    first) so operators see the scan is alive on a multi-million-
    file directory.
    '''
    # Memory-tight: video_id -> list of paths. The list is at
    # most 4 entries (min/dlp x base/uploaded), so duplicate
    # detection via ``path not in paths`` is O(N) on a tiny N.
    # Avoiding a parallel ``seen_paths`` defaultdict(set) saves
    # ~30% of accumulator RAM on a multi-million-video catalog.
    work: dict[str, list[Path]] = collections.defaultdict(list)

    _LOGGER.info(
        'Scanning video data directory',
        extra={
            'base_dir': str(base_dir),
            'already_processed': len(processed_ids),
            'single_video_id': single_video_id,
            'repair_in_place': repair_in_place,
        },
    )

    if single_video_id:
        # When asked for a single video_id, pick up any matching
        # file we find (do not require URL-less entries — useful
        # for force-rescrape verification).
        for path, vid in _scan_dirs(base_dir):
            if vid != single_video_id:
                continue
            paths_for_vid: list[Path] = work[vid]
            if path not in paths_for_vid:
                paths_for_vid.append(path)
        return dict(work)

    files_scanned: int = 0
    files_skipped: int = 0
    files_repaired: int = 0
    last_log_at: float = time.monotonic()
    last_log_files: int = 0
    for path, vid in _scan_dirs(base_dir):
        files_scanned += 1
        if vid in processed_ids:
            files_skipped += 1
        else:
            record, needs_rewrite = _read_record(path)
            if record is not None:
                if needs_rewrite and repair_in_place:
                    brotli_write(path, record)
                    files_repaired += 1
                if _has_urlless_thumbnail(record):
                    paths_for_vid = work[vid]
                    if path not in paths_for_vid:
                        paths_for_vid.append(path)

        now: float = time.monotonic()
        if (
            files_scanned - last_log_files
            >= _SCAN_LOG_EVERY_FILES
            or now - last_log_at >= _SCAN_LOG_EVERY_SECONDS
        ):
            _LOGGER.info(
                'Scan progress',
                extra={
                    'files_scanned': files_scanned,
                    'files_skipped_processed': files_skipped,
                    'files_repaired': files_repaired,
                    'videos_queued': len(work),
                    'rate_files_per_sec': round(
                        (files_scanned - last_log_files)
                        / max(now - last_log_at, 1e-6),
                        1,
                    ),
                },
            )
            last_log_at = now
            last_log_files = files_scanned

    _LOGGER.info(
        'Scan complete',
        extra={
            'files_scanned': files_scanned,
            'files_skipped_processed': files_skipped,
            'files_repaired': files_repaired,
            'videos_queued': len(work),
        },
    )
    return dict(work)


# ---------------------------------------------------------------------------
# Worker loop / runner integration
# ---------------------------------------------------------------------------


async def _drain_queue(
    work: dict[str, list[Path]],
    settings: BackfillSettings,
    rate_limiter: YouTubeRateLimiter,
    validator: SchemaValidator,
    processed_path: Path,
    failed_path: Path,
) -> None:
    '''Process every video in *work* via a fixed-size worker pool.

    The previous implementation scheduled one ``asyncio.Task`` per
    video up front via ``asyncio.gather(*coros)``. With millions
    of videos that materialised millions of coroutine frames and
    Task objects (~2 kB each → 10+ GB on a large catalog) before
    a single one ran. The pool below keeps memory bounded to
    ``concurrency`` running tasks plus a small queue buffer
    regardless of the workload size.

    Items are popped from *work* as they are queued so the dict
    can drop the value reference and let Python reclaim each
    entry's path list once it has been handed to a worker.
    '''
    n_workers: int = max(1, settings.backfill_concurrency)
    cap: int | None = settings.backfill_max_videos
    total_planned: int = (
        len(work) if cap is None else min(len(work), cap)
    )
    queue: asyncio.Queue[tuple[str, list[Path]] | None] = (
        asyncio.Queue(maxsize=n_workers * 2)
    )

    started: int = 0
    succeeded: int = 0
    failed: int = 0
    counter_lock: asyncio.Lock = asyncio.Lock()

    async def _consume() -> None:
        nonlocal started, succeeded, failed
        while True:
            item: tuple[str, list[Path]] | None = (
                await queue.get()
            )
            if item is None:
                queue.task_done()
                return
            video_id, paths = item
            async with counter_lock:
                started += 1
                local_idx: int = started
            _LOGGER.info(
                'Processing video',
                extra={
                    'video_id': video_id,
                    'paths': [str(p) for p in paths],
                    'index': local_idx,
                    'total': total_planned,
                },
            )
            try:
                status, message = await _process_video(
                    video_id, paths, rate_limiter,
                    validator, settings,
                )
                if status == 'ok':
                    async with counter_lock:
                        succeeded += 1
                    await _append_processed_id(
                        processed_path, video_id,
                    )
                    _LOGGER.info(
                        'Backfill ok',
                        extra={
                            'video_id': video_id,
                            'index': local_idx,
                            'total': total_planned,
                        },
                    )
                else:
                    async with counter_lock:
                        failed += 1
                    await _append_failed_id(
                        failed_path, video_id, status,
                        message or '',
                    )
                    # Mark processed even on permanent failure so
                    # we don't retry deleted/private videos
                    # forever; operators inspect the failed file.
                    await _append_processed_id(
                        processed_path, video_id,
                    )
                    _LOGGER.warning(
                        'Backfill failed',
                        extra={
                            'video_id': video_id,
                            'reason': status,
                            'message': message,
                            'index': local_idx,
                            'total': total_planned,
                        },
                    )
            finally:
                queue.task_done()

    workers: list[asyncio.Task[None]] = [
        asyncio.create_task(_consume())
        for _ in range(n_workers)
    ]

    queued: int = 0
    try:
        # Drain *work* in-place so each (vid, paths) entry can be
        # garbage-collected once it has been handed to a worker.
        # ``popitem`` is O(1) and yields the most-recently-added
        # entry; for our purposes order is irrelevant.
        while work:
            if cap is not None and queued >= cap:
                break
            video_id, paths = work.popitem()
            queued += 1
            await queue.put((video_id, paths))
    finally:
        # Send one poison pill per worker so each cleanly exits
        # even if the producer raised mid-loop.
        for _ in range(n_workers):
            await queue.put(None)
        await asyncio.gather(*workers)

    _LOGGER.info(
        'Backfill run complete',
        extra={
            'total_planned': queued,
            'succeeded': succeeded,
            'failed': failed,
            'dry_run': settings.backfill_dry_run,
        },
    )


def _resolve_state_paths(
    settings: BackfillSettings,
) -> tuple[Path, Path]:
    base: Path = Path(settings.video_data_directory or '.')
    processed: Path = (
        Path(settings.backfill_processed_ids_path)
        if settings.backfill_processed_ids_path
        else base / '.thumb_backfill_processed.jsonl'
    )
    failed: Path = (
        Path(settings.backfill_failed_ids_path)
        if settings.backfill_failed_ids_path
        else base / '.thumb_backfill_failed.jsonl'
    )
    return processed, failed


async def _run_worker(ctx: ScraperRunContext) -> None:
    settings: BackfillSettings = ctx.settings  # type: ignore
    client: ExchangeClient | None = ctx.client
    rate_limiter: YouTubeRateLimiter = (
        ctx.rate_limiter  # type: ignore
    )
    if client is None:
        raise RuntimeError(
            'Backfill tool requires an ExchangeClient to '
            'fetch the schema for validation.'
        )

    base_dir: Path = Path(settings.video_data_directory or '.')
    if not base_dir.is_dir():
        raise FileNotFoundError(
            f'video_data_directory does not exist: {base_dir}'
        )

    processed_path, failed_path = _resolve_state_paths(settings)
    processed_ids: set[str] = _load_processed_ids(processed_path)
    _LOGGER.info(
        'Loaded processed-ids state',
        extra={
            'processed_path': str(processed_path),
            'failed_path': str(failed_path),
            'already_processed': len(processed_ids),
        },
    )

    schema_dict: dict[str, Any] = await fetch_schema_dict(
        client, settings.exchange_url, settings.schema_owner,
        'youtube', 'video', settings.schema_version,
    )
    validator: SchemaValidator = SchemaValidator(schema_dict)

    scan_start: float = time.monotonic()
    work: dict[str, list[Path]] = _build_workload(
        base_dir,
        processed_ids,
        settings.backfill_single_video_id,
        repair_in_place=not settings.backfill_dry_run,
    )
    _LOGGER.info(
        'Workload discovered',
        extra={
            'video_count': len(work),
            'scan_seconds': time.monotonic() - scan_start,
            'dry_run': settings.backfill_dry_run,
        },
    )
    if settings.backfill_dry_run:
        # Print a short summary to stdout for operator
        # convenience; logs may be JSON-formatted.
        sys.stdout.write(
            f'dry-run: {len(work)} videos with URL-less '
            f'thumbnails (after skipping '
            f'{len(processed_ids)} already processed)\n'
        )
        sys.stdout.flush()
        return

    if not work:
        _LOGGER.info('Nothing to do; exiting.')
        return

    await _drain_queue(
        work, settings, rate_limiter, validator,
        processed_path, failed_path,
    )


def _build_rate_limiter(
    s: BackfillSettings,
) -> YouTubeRateLimiter:
    '''Construct the per-process YouTubeRateLimiter for this
    one-shot tool. We are upload-only-ish (the InnerTube path
    does not need cookies) so cookie warm-up is disabled.'''
    rl: YouTubeRateLimiter = YouTubeRateLimiter.get(
        state_dir=s.rate_limiter_state_dir,
        redis_dsn=s.redis_dsn,
    )
    rl.set_auto_warm_cookies(False)
    return rl


def main() -> None:
    settings: BackfillSettings = BackfillSettings()
    runner: ScraperRunner = ScraperRunner(
        settings=settings,
        scraper_label='thumb_backfill',
        platform='youtube',
        num_processes=1,
        concurrency=settings.backfill_concurrency,
        metrics_port=settings.metrics_port,
        log_file=settings.log_file,
        log_level=settings.log_level,
        rate_limiter_factory=_build_rate_limiter,
        client_required=True,
    )
    sys.exit(runner.run_sync(_run_worker))


if __name__ == '__main__':
    main()
