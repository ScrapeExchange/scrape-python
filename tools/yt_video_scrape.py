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
import re
import sys
import time
import asyncio
import logging

from asyncio import Task, Queue
from pathlib import Path
from random import shuffle
from typing import NamedTuple

import brotli
import orjson

from httpx import Response

from prometheus_client import Counter

from pydantic import AliasChoices, Field, field_validator
from yt_dlp.YoutubeDL import YoutubeDL

from scrape_exchange.bulk_upload import (
    BulkBatchOutcome,
    record_bulk_filter_skip,
    resume_pending_bulk_uploads,
    upload_bulk_batch,
)
from scrape_exchange.exchange_client import ExchangeClient
from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.content_claim import (
    ContentClaim,
    FileContentClaim,
    NullContentClaim,
    RedisContentClaim,
)
from scrape_exchange.worker_id import get_worker_id
from scrape_exchange.util import extract_proxy_ip, proxy_network_for
from scrape_exchange.settings import normalize_log_level
from scrape_exchange.scraper_runner import (
    ScraperRunContext,
    ScraperRunner,
)
from scrape_exchange.youtube.youtube_rate_limiter import YouTubeRateLimiter
from scrape_exchange.youtube.youtube_video import YouTubeVideo
from scrape_exchange.youtube.youtube_video import (
    DENO_PATH, PO_TOKEN_URL, YTDLP_CACHE_DIR,
)
from scrape_exchange.youtube.youtube_channel import (
    YouTubeChannel,
    fallback_handle,
)
from scrape_exchange.creator_map import (
    CreatorMap,
    FileCreatorMap,
    NullCreatorMap,
    RedisCreatorMap,
    CREATOR_MAP_LOOKUP_TOTAL,
    CREATOR_MAP_RESOLUTION_TOTAL,
)
from watchfiles import awatch, Change
from scrape_exchange.schema_validator import (
    SchemaValidator,
    fetch_schema_dict,
)
from scrape_exchange.youtube.settings import YouTubeScraperSettings

VIDEO_MIN_PREFIX = 'video-min-'
VIDEO_YTDLP_PREFIX = 'video-dlp-'


class WorkItem(NamedTuple):
    '''
    Queue entry for the video scraper's worker pool.

    *filename* is the bare filename (no directory component) of a
    ``video-min-*`` or ``video-dlp-*`` file.

    Exactly one of ``from_uploaded`` and ``from_priority`` may be
    ``True``:

    * ``from_uploaded=True`` — file lives in ``uploaded_dir``
      (only ``video-min-*`` files that the bulk or watch uploader
      already pushed to scrape.exchange).
    * ``from_priority=True`` — file lives in
      ``video_priority_directory``; processed before any base-dir
      or uploaded-dir item, then moved straight into
      ``uploaded_dir`` once the upload returns 201.
    * both ``False`` — file lives in ``base_dir`` and follows the
      legacy code path.
    '''

    filename: str
    from_uploaded: bool = False
    from_priority: bool = False


# Exponential backoff window after a rate-limit-flavoured failure:
# the first failure produces a FAILURE_SLEEP_MIN-second sleep, each
# subsequent consecutive failure doubles the previous sleep, capped
# at FAILURE_SLEEP_MAX. Successful scrapes reset the sleep to 0.
FAILURE_SLEEP_MIN: int = 60
FAILURE_SLEEP_MAX: int = 300

FILE_EXTENSION: str = '.json.br'

START_TIME: float = time.monotonic()

# Maximum number of items buffered in the bounded work queue.
# The producer blocks when the queue is full, gating it to the
# worker pace and preventing unbounded memory growth.
_QUEUE_MAXSIZE: int = 10_000

# YouTube video IDs are 11 characters from the URL-safe base64
# alphabet. A bare file with this exact name (no extension) in
# the priority or base directory is treated as a request to
# scrape that video — see :func:`_promote_bare_id_files`.
_VIDEO_ID_RE: re.Pattern[str] = re.compile(r'^[A-Za-z0-9_-]{11}$')

# Single control byte prepended to a filename in the shuffled
# candidate buffer when the entry comes from ``uploaded_dir``.
# Plain (un-prefixed) entries are from ``base_dir``. The sentinel
# is used purely to keep that one bit alongside each filename
# without paying the ~80-byte ``WorkItem`` tuple overhead per
# entry; at this scraper's scale (~10M candidates) that saves
# ~800 MB of resident memory during ``prepare_workload``.
# ``\x01`` (SOH) is never produced by our filename builders, so
# the prefix is unambiguous.
_FROM_UPLOADED_SENTINEL: str = '\x01'


def _encode_shuffled_entry(filename: str, from_uploaded: bool) -> str:
    '''Encode a candidate for the compact shuffled buffer.'''
    if from_uploaded:
        return _FROM_UPLOADED_SENTINEL + filename
    return filename


def _decode_shuffled_entry(encoded: str) -> tuple[str, bool]:
    '''Inverse of :func:`_encode_shuffled_entry`.'''
    if encoded.startswith(_FROM_UPLOADED_SENTINEL):
        return encoded[len(_FROM_UPLOADED_SENTINEL):], True
    return encoded, False


# Ordered table of yt-dlp error classifications. Each entry is
# ``(reason, [substring, ...])``; the first reason whose substring
# list contains a substring of the *lowercased* error string wins.
# Anything that matches no entry is classified as ``other``.
#
# Every substring MUST already be lowercase — there's an assertion
# below this table to catch a regression. The original branch chain
# in ``_scrape`` had six dead substrings whose case made them
# impossible to match against ``str(exc).lower()``.
_ERROR_PATTERNS: list[tuple[str, list[str]]] = [
    ('rate_limit', [
        'rate-limited by youtube',
        'vpn/proxy detected',
        'youtube blocked',
        'captcha',
        'try again later',
        'the page needs to be reloaded',
        ' 429',
        'http 429',
        '429 too many',
        'expected string or bytes-like object',
    ]),
    ('missing_data', [
        'missing microformat data',
    ]),
    ('unavailable', [
        'this video is private',
        'this video has been removed',
        'video is age restricted',
        'sign in to confirm your age',
        "available to this channel's members on level",
        'members-only content',
        'this live event will begin in',
        'this live event has ended',
        'live stream recording is not available',
        'music premium',
        'video unavailable',
        'video is not available',
        'video available in your country',
        'not available in your country',
        'copyright',
        'inappropriate',
        'uploader',
        'offline.',
    ]),
    ('premiere', [
        'premieres',
        'premiere',
    ]),
    ('transient', [
        'offline',
        'timed out',
        'sslerror',
        'ssl:',
        'unable to connect to proxy',
    ]),
]

# Guard against future contributors adding an uppercase pattern that
# would silently never match.
for _reason, _patterns in _ERROR_PATTERNS:
    for _pat in _patterns:
        assert _pat == _pat.lower(), (
            f'_ERROR_PATTERNS contains non-lowercase pattern '
            f'{_pat!r} under reason {_reason!r}; the classifier '
            f'compares against str(exc).lower() so an uppercase '
            f'pattern is dead code'
        )


def _classify_yt_dlp_error(error_str: str) -> str:
    '''
    Classify a yt-dlp error message into one of the reason buckets
    used by ``scrape_failures_total``.

    Pure function: no I/O, no metric updates, no logging.
    Returns the matching reason name from :data:`_ERROR_PATTERNS`,
    or ``'other'`` when nothing matches.
    '''

    lowered: str = error_str.lower()
    for reason, patterns in _ERROR_PATTERNS:
        for pattern in patterns:
            if pattern in lowered:
                return reason
    return 'other'


def _proxy_network(proxy: str | None) -> str:
    '''
    Derive the proxy_network label (CIDR string, 'other',
    or 'none') from a proxy URL. Wrapping extract_proxy_ip
    catches the rare malformed-URL case so metric emission
    never raises.
    '''
    if not proxy:
        return 'none'
    try:
        return proxy_network_for(extract_proxy_ip(proxy))
    except ValueError:
        return 'other'


# Prometheus metrics — shared declarations live in scraper_metrics to
# avoid duplicate-registration errors when multiple tool modules are
# imported in the same process (e.g. test runners).
from scrape_exchange.scraper_metrics import (
    METRIC_SCRAPES_COMPLETED as METRIC_VIDEOS_SCRAPED,
    METRIC_SCRAPE_FAILURES,
    METRIC_SCRAPE_DURATION,
    METRIC_UPLOADS_ENQUEUED as METRIC_VIDEOS_ENQUEUED,
    METRIC_UPLOADS_SKIPPED as METRIC_VIDEOS_ALREADY_UPLOADED,
    METRIC_UPLOADS_FAILED as METRIC_VIDEOS_BULK_FAILED,
    METRIC_UPLOADS_MISSING_RESULT as METRIC_VIDEOS_BULK_MISSING_RESULT,
    METRIC_UPLOAD_BATCHES as METRIC_VIDEO_BULK_BATCHES,
    METRIC_SCRAPE_QUEUE_SIZE as METRIC_QUEUE_SIZE,
    METRIC_WORKER_SLEEP_SECONDS as METRIC_SLEEP_SECONDS,
    METRIC_WATCHER_FILES_DETECTED,
    METRIC_WATCHER_FILES_SKIPPED,
    METRIC_WATCHER_BATCHES,
)
# Alias so existing call sites for "has_formats" skips use the same
# underlying counter with reason="has_formats".
METRIC_VIDEOS_SKIPPED_HAS_FORMATS: Counter = METRIC_VIDEOS_ALREADY_UPLOADED


def _record_bulk_filter_skip(reason: str) -> None:
    '''
    Thin video-scoped wrapper around
    :func:`scrape_exchange.bulk_upload.record_bulk_filter_skip`.
    Pre-fills ``platform="youtube"``, ``scraper="video_scraper"``,
    ``entity="video"`` so the bulk-sweep skip sites stay terse;
    the underlying counter is shared across all scrapers, so
    Grafana panels aggregate fleet-wide by ``reason`` regardless
    of which scraper recorded the skip.
    '''
    record_bulk_filter_skip(
        platform='youtube',
        scraper='video_scraper',
        entity='video',
        reason=reason,
    )


METRIC_RATE_LIMIT_HITS = Counter(
    'rate_limit_hits_total',
    'Number of times a proxy was rate-limited by YouTube',
    ['platform', 'scraper', 'entity', 'api',
     'proxy_ip', 'proxy_network', 'worker_id'],
)

# -- scheduled bulk-upload metrics --
# Re-use the exchange_client counter (same metric name, registered
# once in that module) so Prometheus doesn't see a duplicate
# registration when exchange_client is imported in this process.
from scrape_exchange.exchange_client import (
    METRIC_BACKGROUND_UPLOADS as METRIC_VIDEOS_BULK_UPLOADED,
)


class VideoSettings(YouTubeScraperSettings):
    '''
    Tool configuration loaded in priority order:
    CLI flags > environment variables > .env file > built-in defaults.
    '''

    video_upload_only: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            'VIDEO_UPLOAD_ONLY',
            'video_upload_only',
        ),
        description=(
            'Only upload already-scraped video-dlp files, '
            'skipping the yt-dlp scrape step entirely. '
            'video-min files are excluded from the workload '
            'since they always require scraping.'
        ),
    )
    video_no_upload: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            'VIDEO_NO_UPLOAD',
            'video_no_upload',
        ),
        description=(
            'Only perform the scraping step, skipping '
            'data upload to Scrape Exchange'
        ),
    )
    video_use_yt_dlp: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            'VIDEO_USE_YT_DLP',
            'video_use_yt_dlp',
        ),
        description=(
            'When True, run yt-dlp after the InnerTube pass to '
            'fetch the metadata yt-dlp adds on top: formats, '
            'availability, media_type, aspect_ratio, heatmaps, '
            'embedable, license, default_audio_language. When '
            'False (the default) only the InnerTube pass runs '
            'and those properties are left unset; this trades '
            'completeness for throughput.'
        ),
    )
    video_priority_directory: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'VIDEO_PRIORITY_DIRECTORY',
            'video_priority_directory',
        ),
        description=(
            'Optional staging directory of '
            '``video-min-*.json.br`` files to process before '
            'any other queued video. After the InnerTube (or '
            'optionally yt-dlp) scrape and a successful upload '
            'to scrape.exchange the source file is moved from '
            'this directory into '
            '``<video_data_directory>/uploaded/``. On failure '
            'the source is left in place to be retried on the '
            'next run.'
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
    ytdlp_cache_dir: str = Field(
        default=YTDLP_CACHE_DIR,
        validation_alias=AliasChoices(
            'YTDLP_CACHE_DIR', 'ytdlp_cache_dir'
        ),
        description=(
            'Directory yt-dlp uses for its on-disk cache (player JS, '
            'signature decryption artefacts, etc.). Created at start '
            'if it does not exist.'
        ),
    )

    max_files: int | None = Field(
        default=None,
        description='Maximum number of files to process in one run'
    )
    metrics_port: int = Field(
        default=9400,
        validation_alias=AliasChoices(
            'VIDEO_METRICS_PORT', 'video_metrics_port'),
        description='Port for the Prometheus metrics HTTP server',
    )
    video_concurrency: int = Field(
        default=3,
        validation_alias=AliasChoices(
            'VIDEO_CONCURRENCY', 'video_concurrency'
        ),
        description=(
            'Number of videos to scrape concurrently inside one '
            'video scraper process. Video-scraper-specific so the '
            'RSS and channel scrapers can keep their own '
            'concurrency settings independent.'
        ),
    )
    video_log_level: str = Field(
        default='INFO',
        validation_alias=AliasChoices(
            'VIDEO_LOG_LEVEL', 'video_log_level',
            'LOG_LEVEL', 'log_level',
        ),
        description=(
            'Logging level for the video scraper '
            '(DEBUG, INFO, WARNING, ERROR, CRITICAL). Honours '
            'VIDEO_LOG_LEVEL first so this scraper can be dialled '
            'up independently of the RSS and channel scrapers; '
            'falls back to LOG_LEVEL when the scraper-specific '
            'var is unset.'
        ),
    )
    video_log_file: str = Field(
        default='/dev/stdout',
        validation_alias=AliasChoices(
            'VIDEO_LOG_FILE', 'video_log_file',
            'LOG_FILE', 'log_file',
        ),
        description=(
            'Log file path for the video scraper. Honours '
            'VIDEO_LOG_FILE first so each scraper can write to '
            'its own file; falls back to LOG_FILE when the '
            'scraper-specific var is unset.'
        ),
    )
    # overrdide the base ScraperSettings proxies field with an RSS-specific one
    # pydantic-settings takes the value of the first matching alias
    proxies: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'VIDEO_PROXIES', 'video_proxies', 'PROXIES', 'proxies'
        ),
        description=(
            'Comma-separated list of proxy URLs to use for scraping (e.g. '
            '"http://proxy1:port,http://proxy2:port"). If not set, no '
            'proxy will be used.'
        )
    )

    @field_validator('video_log_level', mode='before')
    @classmethod
    def _normalize_video_log_level(cls, v: str) -> str:
        return normalize_log_level(v)
    video_num_processes: int = Field(
        default=1,
        validation_alias=AliasChoices(
            'VIDEO_NUM_PROCESSES', 'video_num_processes'
        ),
        description=(
            'Number of child video scraper processes to spawn. '
            'When > 1 the invocation becomes a supervisor that '
            'splits the proxy pool into N disjoint chunks and '
            'spawns one child per chunk. Each child runs with '
            'VIDEO_NUM_PROCESSES=1, gets its own METRICS_PORT '
            '(base + worker_instance, with the base reserved for '
            'the supervisor and worker_instance starting at 1) '
            'and log file, if specified. Use this to bypass the '
            'GIL and the default ThreadPoolExecutor cap, both of '
            'which limit how many yt-dlp extract_info calls can '
            'run in parallel inside a single Python process.'
        ),
    )


def _build_video_rate_limiter(
    s: 'VideoSettings',
) -> YouTubeRateLimiter:
    '''
    Construct (or fetch) the per-process YouTubeRateLimiter
    singleton. In ``--video-upload-only`` mode the worker reads
    existing files from disk and POSTs them to scrape.exchange,
    so the proactive cookie warm-up + renewal loop is wasted
    work and is disabled here. The cookie file for any one
    proxy is still acquired lazily on demand for the InnerTube
    fallback inside ``resolve_video_upload_handle``.
    '''
    rl: YouTubeRateLimiter = YouTubeRateLimiter.get(
        state_dir=s.rate_limiter_state_dir,
        redis_dsn=s.redis_dsn,
    )
    rl.set_auto_warm_cookies(not s.video_upload_only)
    return rl


def main() -> None:
    '''
    Top-level entry point. Reads settings and
    dispatches to either the shared supervisor
    (when ``video_num_processes > 1``) or the
    in-process scraper worker.
    '''

    settings: VideoSettings = VideoSettings()
    os.makedirs(
        settings.ytdlp_cache_dir, exist_ok=True,
    )

    if settings.video_upload_only:
        settings.video_num_processes = 1
        settings.metrics_port = (
            settings.metrics_port - 1
        )

    runner: ScraperRunner = ScraperRunner(
        settings=settings,
        scraper_label='video',
        platform='youtube',
        num_processes=settings.video_num_processes,
        concurrency=settings.video_concurrency,
        metrics_port=settings.metrics_port,
        log_file=settings.video_log_file,
        log_level=settings.video_log_level,
        rate_limiter_factory=_build_video_rate_limiter,
        client_required=(
            not settings.video_no_upload
        ),
    )
    sys.exit(runner.run_sync(_run_worker))


async def _run_worker(
    ctx: ScraperRunContext,
) -> None:
    '''
    Run a single in-process scraper worker (the leaf of the
    supervisor tree). Spawns ``settings.video_concurrency`` async
    workers that share the proxy pool round-robin.
    '''

    settings: VideoSettings = ctx.settings

    video_fm: AssetFileManagement = (
        AssetFileManagement(
            settings.video_data_directory,
        )
    )
    logging.info(
        'Starting YouTube video scrape tool',
        extra={
            'settings': (
                settings.model_dump_json(indent=2)
            ),
        },
    )

    claim: ContentClaim
    if settings.video_num_processes > 1:
        if settings.redis_dsn:
            claim = RedisContentClaim(
                settings.redis_dsn,
                platform='youtube',
            )
        else:
            claim = FileContentClaim(
                settings.video_data_directory,
            )
        await claim.cleanup_stale()
    else:
        claim = NullContentClaim()

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

    # Build the schema validator once at startup. Used by both the
    # bulk-upload sweep and the live-scrape worker so that records
    # that don't conform to the video JSON schema are rejected
    # client-side and the on-disk asset is renamed
    # ``<filename>.invalid`` for operator inspection.
    video_schema_dict: dict = await fetch_schema_dict(
        ctx.client,
        settings.exchange_url,
        settings.schema_owner,
        'youtube',
        'video',
        settings.schema_version,
    )
    video_validator: SchemaValidator = SchemaValidator(
        video_schema_dict,
    )

    if (
        not settings.video_no_upload
        and ctx.client is not None
    ):
        # Reconcile any in-flight bulk jobs that the previous
        # process left behind in ``<base_dir>/.bulk/``. The
        # helper queries scrape.exchange for each persisted
        # job_id, applies the per-record results, and removes
        # the state file. Must run before ``upload_videos`` so
        # the same source files don't enter a fresh batch while
        # a prior batch is still pending server-side.
        await resume_pending_bulk_uploads(
            video_fm, ctx.client, settings.exchange_url,
        )
        await upload_videos(
            settings, ctx.client, video_fm, creator_map_backend,
            video_validator,
        )

    await worker_loop(
        settings, video_fm, claim, video_validator,
        ctx.client, creator_map_backend,
    )


async def video_needs_uploading(video_fm: AssetFileManagement,
                                filename: str) -> bool:
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


async def _video_min_superseded_by_dlp(
    video_fm: AssetFileManagement,
    item: 'WorkItem',
) -> bool:
    '''
    If a ``video-dlp-{id}`` variant exists in either base_dir or
    uploaded_dir, delete the ``video-min-{id}`` from its source
    directory (matching ``item.from_uploaded``) and return True so
    the caller skips enqueueing it. Otherwise return False.

    Replaces the upfront ``dlp_ids`` set + ``_dedup_video_min``
    pass that ``prepare_workload`` used to do; the trade is two
    extra ``stat()`` calls per video-min in exchange for not
    materializing every video-dlp filename in memory at startup.
    '''
    vid: str = item.filename[
        len(VIDEO_MIN_PREFIX):-len(FILE_EXTENSION)
    ]
    dlp_name: str = (
        f'{VIDEO_YTDLP_PREFIX}{vid}{FILE_EXTENSION}'
    )
    base_has_dlp: bool = (
        video_fm.base_dir / dlp_name
    ).exists()
    uploaded_has_dlp: bool = (
        video_fm.uploaded_dir / dlp_name
    ).exists()
    if not (base_has_dlp or uploaded_has_dlp):
        return False
    if item.from_uploaded:
        await video_fm.delete_uploaded(
            item.filename, fail_ok=False,
        )
        logging.debug(
            'Deleted uploaded video-min file '
            'superseded by video-dlp',
            extra={'video_id': vid},
        )
    else:
        await video_fm.delete(item.filename, fail_ok=False)
        logging.debug(
            'Deleted video-min file superseded by video-dlp',
            extra={'video_id': vid},
        )
    return True


def _scan_bare_id_markers(directory: Path) -> list[str]:
    '''
    Return the names of regular files in *directory* whose name is
    a YouTube video ID (matches :data:`_VIDEO_ID_RE`).
    '''
    markers: list[str] = []
    with os.scandir(directory) as it:
        for entry in it:
            if (
                entry.is_file()
                and _VIDEO_ID_RE.match(entry.name) is not None
            ):
                markers.append(entry.name)
    return markers


def _existing_record_for(
    video_fm: AssetFileManagement, vid: str,
) -> bool:
    '''
    True when a ``video-min-{vid}`` or ``video-dlp-{vid}`` record
    already exists in either ``base_dir`` or ``uploaded_dir``.
    '''
    min_name: str = f'{VIDEO_MIN_PREFIX}{vid}{FILE_EXTENSION}'
    dlp_name: str = f'{VIDEO_YTDLP_PREFIX}{vid}{FILE_EXTENSION}'
    return (
        (video_fm.base_dir / min_name).exists()
        or (video_fm.base_dir / dlp_name).exists()
        or (video_fm.uploaded_dir / min_name).exists()
        or (video_fm.uploaded_dir / dlp_name).exists()
    )


def _delete_marker(marker_path: Path, vid: str, reason: str) -> bool:
    '''Best-effort unlink of *marker_path*.  Returns True on success.'''
    try:
        marker_path.unlink()
        return True
    except OSError as exc:
        logging.warning(
            f'Failed to delete bare-id marker ({reason})',
            exc_info=exc,
            extra={'video_id': vid},
        )
        return False


async def _promote_one_marker(
    vid: str, directory: Path, video_fm: AssetFileManagement,
) -> str:
    '''
    Process a single bare-id marker.  Returns one of
    ``'promoted'``, ``'redundant'``, or ``'failed'`` describing
    what happened.
    '''
    marker_path: Path = directory / vid
    if _existing_record_for(video_fm, vid):
        if _delete_marker(
            marker_path, vid, 'existing record takes precedence',
        ):
            logging.info(
                'Deleted redundant bare-id marker; an '
                'existing record takes precedence',
                extra={'video_id': vid},
            )
        return 'redundant'
    video: YouTubeVideo = YouTubeVideo(video_id=vid)
    try:
        await video.to_file(
            str(directory),
            filename_prefix=VIDEO_MIN_PREFIX,
        )
    except OSError as exc:
        logging.warning(
            'Failed to promote bare-id marker; '
            'leaving marker in place',
            exc_info=exc,
            extra={'video_id': vid},
        )
        return 'failed'
    _delete_marker(marker_path, vid, 'after promotion')
    logging.info(
        'Promoted bare-id marker to video-min record',
        extra={
            'video_id': vid,
            'directory': str(directory),
        },
    )
    return 'promoted'


async def _promote_bare_id_files(
    directory: Path,
    video_fm: AssetFileManagement,
) -> None:
    '''
    Promote bare-id marker files into ``video-min-*.json.br``
    records.

    A bare-id marker is a file whose name matches
    :data:`_VIDEO_ID_RE` exactly (11 chars from the YouTube URL
    alphabet, no extension).  Operators drop these into the
    priority or base directory to request a scrape without having
    to construct a full ``video-min-*`` payload.

    For each marker found in *directory*:

    * If a ``video-min-{id}`` or ``video-dlp-{id}`` record
      already exists in either ``base_dir`` or ``uploaded_dir``,
      the marker is redundant — delete it.
    * Otherwise write a minimal ``video-min-{id}.json.br`` (just
      the video_id, all other fields default) into *directory*
      via :meth:`YouTubeVideo.to_file`, then delete the marker.
      The new file is picked up by the regular enumeration
      downstream.

    File contents of the marker itself are ignored.

    :param directory: Directory to scan for markers (priority or
        base).  Returns silently if the directory does not exist.
    :param video_fm: File manager used to look up existing
        records and the canonical base/uploaded directories.
    '''
    if not directory.is_dir():
        return
    markers: list[str] = _scan_bare_id_markers(directory)
    if not markers:
        return
    promoted: int = 0
    redundant: int = 0
    for vid in markers:
        outcome: str = await _promote_one_marker(
            vid, directory, video_fm,
        )
        if outcome == 'promoted':
            promoted += 1
        elif outcome == 'redundant':
            redundant += 1
    logging.info(
        'Bare-id marker sweep complete',
        extra={
            'directory': str(directory),
            'markers_seen': len(markers),
            'promoted': promoted,
            'redundant': redundant,
        },
    )


def _list_priority_items(
    settings: VideoSettings,
) -> list[WorkItem]:
    '''
    Return ``WorkItem(from_priority=True)`` entries for every
    ``video-min-*.json.br`` directly under
    ``settings.video_priority_directory``. Returns an empty list
    when the setting is unset or the directory does not exist;
    files are listed alphabetically so behaviour is reproducible
    across runs.
    '''
    dir_str: str | None = settings.video_priority_directory
    if not dir_str:
        return []
    pdir: Path = Path(dir_str)
    if not pdir.is_dir():
        logging.warning(
            'video_priority_directory is set but does not exist',
            extra={'video_priority_directory': str(pdir)},
        )
        return []
    items: list[WorkItem] = []
    for entry in sorted(pdir.iterdir()):
        if not entry.is_file():
            continue
        name: str = entry.name
        if not name.startswith(VIDEO_MIN_PREFIX):
            continue
        if not name.endswith(FILE_EXTENSION):
            continue
        items.append(
            WorkItem(
                filename=name,
                from_uploaded=False,
                from_priority=True,
            )
        )
    if items:
        logging.info(
            'Queued priority videos ahead of regular workload',
            extra={
                'count': len(items),
                'video_priority_directory': str(pdir),
            },
        )
    return items


async def prepare_workload(
    settings: VideoSettings,
    video_fm: AssetFileManagement,
) -> tuple[list[WorkItem], list[str]]:
    '''
    Enumerate the candidate workload and return it as two lists.

    Returns ``(priority_items, shuffled_items)``.

    * ``priority_items`` is a small list of :class:`WorkItem`
      that consume first (not shuffled).
    * ``shuffled_items`` is the global-shuffled candidate set
      encoded as plain ``str`` entries via
      :func:`_encode_shuffled_entry`: a bare filename means
      ``from_uploaded=False``; a filename prefixed with
      :data:`_FROM_UPLOADED_SENTINEL` means ``from_uploaded=True``.
      Storing strings instead of :class:`WorkItem` tuples saves
      ~80 bytes per entry, which at the scraper's scale (millions
      of candidates) translates to several hundred megabytes of
      resident memory during startup.  The producer
      (:func:`_produce_workload`) decodes each entry as it
      streams items onto the bounded work queue.

    The global ``shuffle`` call is intentional — different scraper
    hosts process the candidate set in different orders to reduce
    overlap; do not remove it.

    :param settings: Configuration settings for the tool
    :param video_fm: AssetFileManagement instance owning the video
        data directory.
    :returns: ``(priority_items, shuffled_items)``
    :raises: (none)
    '''

    # Promote any bare-id marker files (filename = 11-char video
    # ID, no extension) into proper video-min records before
    # enumeration so the rest of the pipeline sees them as
    # ordinary video-min entries.
    priority_dir_str: str | None = settings.video_priority_directory
    if priority_dir_str:
        await _promote_bare_id_files(
            Path(priority_dir_str), video_fm,
        )
    await _promote_bare_id_files(video_fm.base_dir, video_fm)

    dlp_base: list[str] = video_fm.list_base(
        prefix=VIDEO_YTDLP_PREFIX, suffix=FILE_EXTENSION,
    )
    min_base: list[str] = video_fm.list_base(
        prefix=VIDEO_MIN_PREFIX, suffix=FILE_EXTENSION,
    )

    items: list[str]
    if settings.video_upload_only:
        # Upload-only: candidates are every file in base_dir.
        # video-min files in uploaded_dir are not relevant here —
        # they would only be candidates for the yt-dlp pipeline.
        # All entries here are from base_dir, so the bare filename
        # encodes ``from_uploaded=False``.
        items = list(min_base) + list(dlp_base)
        logging.info(
            'Upload-only mode: queuing leftover files '
            'from base_dir',
            extra={
                'dlp_files_count': len(dlp_base),
                'min_files_count': len(min_base),
            },
        )
    else:
        min_uploaded: list[str] = video_fm.list_uploaded(
            prefix=VIDEO_MIN_PREFIX, suffix=FILE_EXTENSION,
        )
        items = (
            list(min_base)
            + [
                _FROM_UPLOADED_SENTINEL + f
                for f in min_uploaded
            ]
            + list(dlp_base)
        )

    shuffle(items)

    priority_items: list[WorkItem] = _list_priority_items(settings)
    logging.debug(
        'Prepared workload',
        extra={'files_length': len(items)},
    )
    return priority_items, items


def _log_producer_exception(producer_task: Task) -> None:
    '''
    If *producer_task* finished with an exception, log it at ERROR
    level.  Called after ``await producer_task`` so the task is
    guaranteed to be done.
    '''
    exc: BaseException | None = producer_task.exception()
    if exc is not None:
        logging.error(
            'Producer task raised an exception',
            exc_info=exc,
        )


async def _produce_workload(
    priority_items: list[WorkItem],
    shuffled_items: list[str],
    video_fm: AssetFileManagement,
    queue: Queue,
) -> None:
    '''
    Feed items onto the bounded work queue.

    Priority items are enqueued first without any filtering.
    Each entry in *shuffled_items* is decoded via
    :func:`_decode_shuffled_entry` (a bare filename means
    ``from_uploaded=False``; a sentinel-prefixed filename means
    ``from_uploaded=True``) into a fresh :class:`WorkItem`, which
    is then checked:

    * ``video-min-*`` items are dropped (with on-disk deletion) if
      a ``video-dlp-*`` counterpart already exists in either
      ``base_dir`` or ``uploaded_dir`` — the upgrade has happened
      and the min file is stale.
    * Non-uploaded items are dropped if
      :func:`video_needs_uploading` says the file is already
      superseded on disk.
    * ``from_uploaded`` items bypass the supersede check — the
      dedup against video-dlp above is the only filter that
      applies.

    Blocks when the queue is at :data:`_QUEUE_MAXSIZE`, so the
    producer is naturally gated to the worker pace.

    :param priority_items: Items to enqueue unconditionally first.
    :param shuffled_items: Compact, pre-shuffled candidate strings
        (bare filename or :data:`_FROM_UPLOADED_SENTINEL`-prefixed
        filename); see :func:`prepare_workload`.
    :param video_fm: AssetFileManagement for existence checks and
        deletion.
    :param queue: Bounded :class:`asyncio.Queue` to fill.
    '''
    for item in priority_items:
        await queue.put(item)
    for encoded in shuffled_items:
        filename: str
        from_uploaded: bool
        filename, from_uploaded = _decode_shuffled_entry(encoded)
        item: WorkItem = WorkItem(filename, from_uploaded)
        if (
            filename.startswith(VIDEO_MIN_PREFIX)
            and await _video_min_superseded_by_dlp(
                video_fm, item,
            )
        ):
            continue
        if (
            not from_uploaded
            and not await video_needs_uploading(
                video_fm, filename,
            )
        ):
            continue
        await queue.put(item)


async def worker_loop(
    settings: VideoSettings,
    video_fm: AssetFileManagement,
    claim: ContentClaim,
    validator: SchemaValidator,
    exchange_client: ExchangeClient | None = None,
    creator_map_backend: CreatorMap | None = None,
) -> None:
    '''
    Main worker loop to continuously scrape and upload videos.

    Uses the shared :class:`ExchangeClient` provided by
    :class:`ScraperRunner` for all proxy workers so that the
    background fire-and-forget upload queue is centralised.
    On shutdown (SIGINT/SIGTERM / all workers done), the
    client's upload queue is drained for up to 10 seconds in
    the ``finally`` block so in-flight POSTs get a chance to
    complete.

    :param settings: Configuration settings for the tool
    :param video_fm: AssetFileManagement instance owning the
        video data directory.
    :param claim: Cross-process claim lock backend.
    :param exchange_client: Shared Scrape.Exchange client,
        or ``None`` when upload is disabled.
    :returns: (none)
    :raises: (none)
    '''

    proxies: list[str] = (
        [p.strip() for p in settings.proxies.split(',') if p.strip()]
        if settings.proxies else []
    )
    priority_items: list[WorkItem]
    shuffled_items: list[str]
    priority_items, shuffled_items = await prepare_workload(
        settings, video_fm,
    )

    queue: Queue = Queue(maxsize=_QUEUE_MAXSIZE)

    METRIC_QUEUE_SIZE.labels(
        platform='youtube',
        scraper='video_scraper',
        entity='video',
        tier='none',
        worker_id=get_worker_id(),
    ).set(len(priority_items) + len(shuffled_items))

    producer_task: Task = asyncio.create_task(
        _produce_workload(
            priority_items, shuffled_items, video_fm, queue,
        )
    )

    # Honour VIDEO_CONCURRENCY: spawn exactly
    # settings.video_concurrency workers and distribute the proxy
    # pool round-robin across them. When video_concurrency <
    # len(proxies) the trailing proxies are intentionally left idle.
    # When video_concurrency > len(proxies) the workers wrap around
    # so each proxy gets ceil(video_concurrency / len(proxies))
    # workers.
    worker_count: int = max(settings.video_concurrency, 1)
    if not proxies:
        worker_assignments: list[str | None] = [None] * worker_count
    else:
        worker_assignments = [
            proxies[i % len(proxies)] for i in range(worker_count)
        ]

    effective_creator_map: CreatorMap = (
        creator_map_backend
        if creator_map_backend is not None
        else NullCreatorMap()
    )
    tasks: list[Task] = []
    try:
        for proxy in worker_assignments:
            task: Task = asyncio.create_task(
                worker(
                    proxy, queue, settings, video_fm,
                    exchange_client, claim,
                    effective_creator_map, validator,
                )
            )
            tasks.append(task)

        if settings.video_upload_only:
            # Wait for the producer to finish filling the initial
            # batch before handing off to the watch-uploader, which
            # does its own queue.join() internally.
            await producer_task
            _log_producer_exception(producer_task)
            await _watch_and_upload(
                queue, video_fm, settings,
            )
        else:
            # Await the producer before queue.join() so that
            # queue.join() cannot return prematurely when workers
            # drain the bounded queue faster than the producer
            # fills it.
            await producer_task
            _log_producer_exception(producer_task)
            started_at: float = time.monotonic()
            try:
                await queue.join()
            except asyncio.CancelledError:
                logging.info(
                    'worker_loop cancelled; '
                    'stopping workers'
                )
                raise
            elapsed: float = (
                time.monotonic() - started_at
            )

            logging.info(
                'Workers worked in parallel',
                extra={
                    'workers': worker_count,
                    'proxies_length': len(proxies),
                    'total_slept_for': elapsed,
                },
            )
    finally:
        if not producer_task.done():
            producer_task.cancel()
        await asyncio.gather(
            producer_task, return_exceptions=True,
        )
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if exchange_client is not None:
            await exchange_client.drain_uploads(timeout=10.0)


def _is_video_upload_file(filename: str) -> bool:
    '''Check if a filename is an uploadable video file.

    Both ``video-min-*`` (RSS-discovered) and ``video-dlp-*``
    (yt-dlp-enriched) variants are eligible for upload.
    '''
    return (
        (
            filename.startswith(VIDEO_YTDLP_PREFIX)
            or filename.startswith(VIDEO_MIN_PREFIX)
        )
        and filename.endswith(FILE_EXTENSION)
    )


async def _watch_and_upload(
    queue: Queue,
    video_fm: AssetFileManagement,
    settings: VideoSettings,
) -> None:
    '''
    Upload-only watcher loop.  Waits for the initial queue
    to drain, then watches ``video_data_directory`` for new
    or modified ``video-min-*.json.br`` and ``video-dlp-*.json.br``
    files and feeds them into *queue* so the existing workers
    can upload them.

    Runs until cancelled by SIGINT / SIGTERM.
    '''

    # Let workers finish the initial batch first.
    await queue.join()

    base: Path = Path(settings.video_data_directory)
    logging.info(
        'Upload-only: watching for new video files',
        extra={
            'watch_dir': str(base),
        },
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
            scraper='video_scraper',
            entity='video',
            worker_id=wid,
        ).inc()
        for _change, path in changes:
            filename: str = Path(path).name
            METRIC_WATCHER_FILES_DETECTED.labels(
                platform='youtube',
                scraper='video_scraper',
                entity='video',
                worker_id=wid,
            ).inc()
            if not await video_needs_uploading(
                video_fm, filename,
            ):
                METRIC_WATCHER_FILES_SKIPPED.labels(
                    platform='youtube',
                    scraper='video_scraper',
                    entity='video',
                    worker_id=wid,
                ).inc()
                continue
            logging.info(
                'Upload-only: new video file detected',
                extra={'filename': filename},
            )
            await queue.put(WorkItem(filename, False))
        # Wait for the batch to be consumed before
        # accepting more watcher events.
        await queue.join()


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
    return None


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
            exc=exc,
            extra={'entry': entry},
        )
        await video_fm.delete(entry, fail_ok=False)
        return None
    except Exception as exc:
        logging.warning(
            'Failed to read video file, skipping',
            exc=exc,
            extra={'entry': entry},
        )
        return None


async def _delete_source(
    video_fm: AssetFileManagement,
    entry: str,
    from_uploaded: bool,
    *,
    fail_ok: bool = False,
) -> None:
    '''
    Remove *entry* from whichever directory it was queued from
    (``uploaded_dir`` when *from_uploaded* is True, otherwise
    ``base_dir``).  Centralises the choice so callers don't have
    to spell it out at every deletion site.
    '''
    if from_uploaded:
        await video_fm.delete_uploaded(entry, fail_ok=fail_ok)
    else:
        await video_fm.delete(entry, fail_ok=fail_ok)


async def _retire_failed_source(
    video_fm: AssetFileManagement,
    entry: str,
    from_uploaded: bool,
    from_priority: bool,
) -> None:
    '''
    Apply the per-source failure-cleanup rule for *entry*:

    * priority sources stay in place so the next run retries
      them;
    * uploaded sources are silently deleted (no recovery path —
      the marker would poison the uploaded copy);
    * base-dir sources are renamed with the ``.failed`` suffix.

    All FS errors are swallowed — a failed cleanup is logged at
    a higher layer.
    '''
    if from_priority:
        return
    try:
        if from_uploaded:
            await video_fm.delete_uploaded(entry, fail_ok=True)
        else:
            await video_fm.mark_failed(entry)
    except OSError:
        pass


async def _scrape_and_save(
    entry: str,
    video_id: str,
    channel_handle: str,
    download_client: YoutubeDL | None,
    settings: VideoSettings,
    video_fm: AssetFileManagement,
    proxy: str,
    sleep: int,
    from_uploaded: bool = False,
    from_priority: bool = False,
) -> tuple[YouTubeVideo | None, int]:
    '''
    Scrape a video with yt-dlp, persist the result to
    disk, and remove the original ``video-min-`` file.

    On any failure the original file is left for a future
    retry (or marked as failed).  Returns the scraped
    video (or ``None``) together with the updated backoff
    sleep value.

    :param entry: Original queue filename.
    :param video_id: YouTube video ID.
    :param channel_handle: Channel name for the video.
    :param download_client: Configured YoutubeDL client (may be
        ``None`` when ``video_use_yt_dlp`` is False).
    :param settings: Scraper settings.
    :param video_fm: File manager owning the data dir.
    :param proxy: Proxy URL used for this worker.
    :param sleep: Current backoff sleep value.
    :param from_uploaded: When True the source ``video-min-`` file
        lives in ``uploaded_dir`` (already uploaded by the bulk or
        watch path); after a successful scrape it is deleted from
        there instead of from ``base_dir``.
    :param from_priority: When True the source file lives in
        ``settings.video_priority_directory`` and must be left in
        place — the post-upload move (handled by the bg upload
        worker via ``mark_uploaded_from``) is what retires it
        into ``uploaded_dir``. No ``video-dlp-`` artefact is
        produced for priority items.
    :returns: ``(video, sleep)`` — video is ``None`` on
        any failure.
    '''

    try:
        video: YouTubeVideo | None
        video, sleep = await _scrape(
            entry, video_id, channel_handle,
            download_client,
            settings, video_fm, proxy, sleep,
            from_priority=from_priority,
        )
    except Exception as exc:
        logging.info(
            'Failed to scrape video',
            exc=exc,
            extra={'video_id': video_id, 'proxy': proxy},
        )
        _proxy_ip: str = (
            extract_proxy_ip(proxy) if proxy else 'none'
        )
        METRIC_SCRAPE_FAILURES.labels(
            platform='youtube',
            scraper='video_scraper',
            entity='video',
            api='ytdlp',
            reason='other',
            worker_id=get_worker_id(),
            proxy_ip=_proxy_ip,
            proxy_network=_proxy_network(proxy),
        ).inc()
        await _retire_failed_source(
            video_fm, entry, from_uploaded, from_priority,
        )
        if sleep:
            METRIC_SLEEP_SECONDS.labels(
                platform='youtube',
                scraper='video_scraper',
                worker_id=get_worker_id(),
            ).set(sleep)
        return None, sleep

    if video is None:
        return None, sleep

    # Priority items skip the disk write + source delete step:
    # _scrape() already passed save_dir=None so no video-dlp-
    # was produced, and the source video-min- file must remain
    # in priority_dir until the bg upload worker moves it to
    # uploaded_dir on HTTP 201.
    if from_priority:
        return video, sleep

    try:
        await video.to_file(
            settings.video_data_directory,
            VIDEO_YTDLP_PREFIX,
        )
        # to_file uses model I/O so it bypasses FM
        # cleanup; explicitly remove the original
        # entry (e.g. the video-min-{id} file we
        # just upgraded to video-dlp-).
        await _delete_source(
            video_fm, entry, from_uploaded, fail_ok=False,
        )
    except OSError as exc:
        logging.warning(
            'Failed to write scraped video '
            'file to disk',
            exc=exc,
            extra={'video_id': video_id, 'proxy': proxy},
        )
        return None, sleep

    return video, sleep


async def _resolve_and_enqueue(
    video: YouTubeVideo,
    exchange_client: ExchangeClient,
    settings: VideoSettings,
    video_fm: AssetFileManagement,
    creator_map_backend: CreatorMap,
    validator: SchemaValidator,
    proxy: str,
    filename_prefix: str,
    move_source_dir: Path | None = None,
) -> bool:
    '''
    Resolve the canonical channel handle for *video* and hand the
    record to ``enqueue_upload_video``.  Returns ``True`` only when
    the upload was actually enqueued so the caller can keep its
    files-uploaded counter and Prometheus metric in step.

    When *move_source_dir* is provided, the post-upload
    ``mark_uploaded`` step moves the source file from there into
    ``uploaded_dir`` instead of from ``base_dir`` — used by the
    priority-directory pipeline.

    Pulled out of the worker loop to keep ``worker``'s control
    flow readable.
    '''
    handle: str | None = await resolve_video_upload_handle(
        video, creator_map_backend, proxy,
    )
    if handle is None:
        logging.info(
            'Video upload skipped: handle unresolved; '
            'file remains for retry',
            extra={
                'video_id': video.video_id,
                'channel_id': video.channel_id,
            },
        )
        return False
    enqueued: bool = await enqueue_upload_video(
        exchange_client, settings,
        video_fm, handle, video, validator,
        filename_prefix=filename_prefix,
        move_source_dir=move_source_dir,
    )
    if enqueued:
        METRIC_VIDEOS_ENQUEUED.labels(
            platform='youtube',
            scraper='video_scraper',
            entity='video',
            mode='single',
            worker_id=get_worker_id(),
        ).inc()
    return enqueued


async def _maybe_skip_already_scraped(
    video_id: str,
    entry: str,
    from_uploaded: bool,
    settings: VideoSettings,
    video_fm: AssetFileManagement,
    exchange_client: ExchangeClient | None,
) -> bool:
    '''
    Check whether the server already has a video-dlp record for
    *video_id*; if so, discard the local source file and return
    ``True`` so the caller can skip the scrape.  Only relevant for
    files that would otherwise trigger a yt-dlp scrape.
    '''
    if (
        settings.video_no_upload
        or exchange_client is None
        or not await _video_has_formats_on_server(
            exchange_client, settings, video_id,
        )
    ):
        return False
    logging.info(
        'Video already has formats on server, skipping scrape',
        extra={'video_id': video_id},
    )
    METRIC_VIDEOS_SKIPPED_HAS_FORMATS.labels(
        platform='youtube',
        scraper='video_scraper',
        entity='video',
        reason='has_formats',
        worker_id=get_worker_id(),
    ).inc()
    await _delete_source(
        video_fm, entry, from_uploaded, fail_ok=False,
    )
    return True


async def _scrape_with_claim(
    entry: str,
    video_id: str,
    channel_handle: str,
    download_client: YoutubeDL | None,
    settings: VideoSettings,
    video_fm: AssetFileManagement,
    proxy: str,
    sleep: int,
    from_uploaded: bool,
    claim: ContentClaim,
    from_priority: bool = False,
) -> tuple[YouTubeVideo | None, int, bool]:
    '''
    Acquire the per-video claim, run ``_scrape_and_save``, and
    release the claim.  Returns ``(video, sleep, scraped)`` where
    *scraped* is ``True`` when the yt-dlp pipeline produced a new
    file (so the worker can bump its ``files_scraped`` counter).
    Both ``video`` and ``scraped`` are ``False``-y when another
    worker holds the claim or the scrape failed.
    '''
    acquired: bool = await claim.acquire(video_id)
    if not acquired:
        logging.debug(
            'Video claimed by another process, skipping',
            extra={'video_id': video_id},
        )
        return None, sleep, False
    try:
        video, sleep = await _scrape_and_save(
            entry, video_id, channel_handle,
            download_client, settings,
            video_fm, proxy, sleep,
            from_uploaded=from_uploaded,
            from_priority=from_priority,
        )
    finally:
        await claim.release(video_id)
    if video is None:
        return None, sleep, False
    return video, sleep, True


class _ItemOutcome(NamedTuple):
    '''
    Result of processing one ``WorkItem`` inside :func:`worker`.

    *scraped* is True when yt-dlp produced a fresh ``video-dlp-``
    file; *uploaded* is True when ``enqueue_upload_video`` accepted
    the record into its background queue.  *sleep* carries the
    updated exponential-backoff value.
    '''

    sleep: int
    scraped: bool
    uploaded: bool


async def _load_for_processing(
    video_id: str,
    entry: str,
    prefix: str,
    from_uploaded: bool,
    settings: VideoSettings,
    video_fm: AssetFileManagement,
    from_priority: bool = False,
) -> YouTubeVideo | None:
    '''
    Load the on-disk record for processing.  Returns ``None`` when
    the file is missing/corrupt or when the defence-in-depth
    superseded check (base_dir entries only) decides the upload
    has already happened.
    '''
    if from_priority:
        data_dir: str = settings.video_priority_directory or ''
    elif from_uploaded:
        data_dir = str(video_fm.uploaded_dir)
    else:
        data_dir = settings.video_data_directory
    video: YouTubeVideo | None = await _load_video_file(
        video_id, data_dir, prefix, entry, video_fm,
    )
    if video is None:
        return None
    # is_superseded only operates on base_dir and the video-dlp
    # dedup in prepare_workload already rejected stale entries
    # for uploaded-dir sources, so the check is base-only.
    # Priority items are user-staged and bypass the supersede
    # check: by definition they're the freshest record.
    if (
        from_uploaded
        or from_priority
        or await video_needs_uploading(video_fm, entry)
    ):
        return video
    logging.debug(
        'Video was already uploaded, skipping',
        extra={'video_id': video_id},
    )
    METRIC_VIDEOS_ALREADY_UPLOADED.labels(
        platform='youtube',
        scraper='video_scraper',
        entity='video',
        reason='already_uploaded',
        worker_id=get_worker_id(),
    ).inc()
    return None


async def _scrape_and_track(
    entry: str,
    video_id: str,
    video: YouTubeVideo,
    from_uploaded: bool,
    download_client: YoutubeDL | None,
    settings: VideoSettings,
    video_fm: AssetFileManagement,
    proxy: str,
    sleep: int,
    claim: ContentClaim,
    from_priority: bool = False,
) -> tuple[YouTubeVideo | None, int, bool]:
    '''
    Run :func:`_scrape_with_claim`, sleep on failure if backoff was
    set, and emit the ``videos_scraped`` metric on success.  Returns
    ``(video, sleep, scraped)`` where *scraped* is ``True`` only when
    a fresh ``video-dlp-`` file was produced.
    '''
    fresh, sleep, scraped = await _scrape_with_claim(
        entry, video_id, video.channel_handle,
        download_client, settings,
        video_fm, proxy, sleep,
        from_uploaded, claim,
        from_priority=from_priority,
    )
    if not scraped:
        if fresh is None and sleep:
            logging.info(
                'Sleeping before next attempt',
                extra={'video_id': video_id, 'sleep': sleep},
            )
            await asyncio.sleep(sleep)
        return None, sleep, False
    METRIC_VIDEOS_SCRAPED.labels(
        platform='youtube',
        scraper='video_scraper',
        entity='video',
        api='ytdlp',
        worker_id=get_worker_id(),
        proxy_ip=extract_proxy_ip(proxy) if proxy else 'none',
        proxy_network=_proxy_network(proxy),
    ).inc()
    return fresh, sleep, True


async def _process_work_item(
    item: WorkItem,
    proxy: str,
    settings: VideoSettings,
    video_fm: AssetFileManagement,
    exchange_client: ExchangeClient | None,
    claim: ContentClaim,
    creator_map_backend: CreatorMap,
    validator: SchemaValidator,
    download_client: YoutubeDL | None,
    sleep: int,
) -> _ItemOutcome:
    '''
    Per-iteration body of :func:`worker`, factored out so the
    surrounding loop only deals with setup, counters, and the
    yt-dlp client refresh cadence.

    Returns an :class:`_ItemOutcome` describing what happened.
    '''
    entry: str = item.filename
    from_uploaded: bool = item.from_uploaded
    from_priority: bool = item.from_priority
    logging.debug(
        'Worker processing file',
        extra={
            'proxy': proxy,
            'entry': entry,
            'from_uploaded': from_uploaded,
            'from_priority': from_priority,
        },
    )

    parsed: tuple[str, str, bool] | None = _parse_entry(entry)
    if parsed is None:
        return _ItemOutcome(sleep, False, False)
    video_id: str
    prefix: str
    parsed_needs: bool
    video_id, prefix, parsed_needs = parsed
    # In upload-only mode never run yt-dlp; the file is uploaded
    # with whatever fidelity it currently has.
    needs_scraping: bool = (
        parsed_needs and not settings.video_upload_only
    )

    if (
        needs_scraping
        and not from_priority
        and await _maybe_skip_already_scraped(
            video_id, entry, from_uploaded,
            settings, video_fm, exchange_client,
        )
    ):
        return _ItemOutcome(sleep, False, False)

    video: YouTubeVideo | None = await _load_for_processing(
        video_id, entry, prefix, from_uploaded,
        settings, video_fm,
        from_priority=from_priority,
    )
    if video is None:
        return _ItemOutcome(sleep, False, False)

    upload_prefix: str = prefix
    scraped: bool = False
    if needs_scraping:
        video, sleep, scraped = await _scrape_and_track(
            entry, video_id, video, from_uploaded,
            download_client, settings, video_fm,
            proxy, sleep, claim,
            from_priority=from_priority,
        )
        if not scraped:
            return _ItemOutcome(sleep, False, False)
        # Priority items keep their original ``video-min-``
        # prefix because no ``video-dlp-`` artefact is written;
        # the source file in priority_dir is the asset that
        # eventually moves to ``uploaded_dir``.
        if not from_priority:
            upload_prefix = VIDEO_YTDLP_PREFIX

    move_source_dir: Path | None = (
        Path(settings.video_priority_directory)
        if from_priority and settings.video_priority_directory
        else None
    )
    uploaded: bool = (
        video is not None
        and not settings.video_no_upload
        and exchange_client is not None
        and await _resolve_and_enqueue(
            video, exchange_client, settings,
            video_fm, creator_map_backend,
            validator, proxy, upload_prefix,
            move_source_dir=move_source_dir,
        )
    )
    return _ItemOutcome(sleep, scraped, uploaded)


async def worker(
    proxy: str,
    queue: Queue,
    settings: VideoSettings,
    video_fm: AssetFileManagement,
    exchange_client: ExchangeClient | None,
    claim: ContentClaim,
    creator_map_backend: CreatorMap,
    validator: SchemaValidator,
) -> None:
    '''
    Worker function to process video files from the queue
    using a specific proxy.

    :param proxy: Proxy URL to use for scraping
    :param queue: Queue containing video file entries to
        process.
    :param settings: Configuration settings for the tool.
    :param video_fm: AssetFileManagement instance owning
        the video data directory.
    :param exchange_client: Shared Scrape.Exchange client
        (or ``None`` if setup failed; in that case the
        worker scrapes but does not upload).
    :param claim: Cross-process claim lock backend.
    '''

    cookie_file: str | None = (
        await YouTubeRateLimiter.get().get_cookie_file(
            proxy,
        )
    )
    logging.info(
        'Worker task bound to proxy',
        extra={
            'proxy': proxy,
            'cookie_file': cookie_file,
        },
    )
    download_client: YoutubeDL | None = None
    if settings.video_use_yt_dlp:
        try:
            download_client = (
                YouTubeVideo._setup_download_client(
                    deno_path=settings.deno_path,
                    po_token_url=settings.po_token_url,
                    ytdlp_cache_dir=settings.ytdlp_cache_dir,
                    proxy=proxy,
                    cookie_file=cookie_file,
                )
            )
        except Exception as exc:
            logging.critical(
                'Failed to set up yt-dlp download client; '
                'worker cannot proceed',
                exc=exc,
                extra={'proxy': proxy},
            )
            return
    else:
        logging.info(
            'video_use_yt_dlp is False; worker will scrape via '
            'InnerTube only and never construct a yt-dlp client',
            extra={'proxy': proxy},
        )

    # Recreate the yt-dlp client every N scrapes to shed
    # accumulated internal state (_printed_messages,
    # cached extractor instances, etc.). Only relevant when
    # the client was actually constructed; in InnerTube-only
    # mode there's nothing to refresh.
    _YTDLP_REFRESH_INTERVAL: int = 200

    sleep: int = 0
    files_scraped: int = 0
    files_uploaded: int = 0
    scrapes_since_refresh: int = 0
    while True:
        item: WorkItem = await queue.get()
        try:
            outcome: _ItemOutcome = await _process_work_item(
                item, proxy, settings, video_fm,
                exchange_client, claim, creator_map_backend,
                validator, download_client, sleep,
            )
            sleep = outcome.sleep
            if outcome.scraped:
                files_scraped += 1
                scrapes_since_refresh += 1
            if outcome.uploaded:
                files_uploaded += 1
            logging.debug(
                'Worker progress',
                extra={
                    'proxy': proxy,
                    'files_scraped': files_scraped,
                    'files_uploaded': files_uploaded,
                },
            )
        finally:
            queue.task_done()

            # Periodically recreate the yt-dlp client
            # to release accumulated internal state. Only
            # relevant when the client was actually constructed;
            # InnerTube-only workers have nothing to refresh.
            if (
                download_client is not None
                and scrapes_since_refresh
                    >= _YTDLP_REFRESH_INTERVAL
            ):
                scrapes_since_refresh = 0
                download_client.close()
                download_client = (
                    YouTubeVideo._setup_download_client(
                        deno_path=settings.deno_path,
                        po_token_url=(
                            settings.po_token_url
                        ),
                        ytdlp_cache_dir=(
                            settings.ytdlp_cache_dir
                        ),
                        proxy=proxy,
                        cookie_file=cookie_file,
                    )
                )
                logging.debug(
                    'Refreshed yt-dlp client',
                    extra={
                        'files_scraped': (
                            files_scraped
                        ),
                    },
                )


def _next_failure_sleep(current_sleep: int) -> int:
    '''
    Compute the next exponential-backoff interval after a
    rate-limit-flavoured failure.

    The first failure in a streak produces :data:`FAILURE_SLEEP_MIN`
    seconds of sleep. Each subsequent failure doubles the previous
    sleep, clamped at :data:`FAILURE_SLEEP_MAX`. Any call path that
    resets ``sleep`` to ``0`` (success, missing_data, unavailable,
    premiere) also resets the backoff: the next failure starts from
    :data:`FAILURE_SLEEP_MIN` again.

    Pure function: call it from test code directly.
    '''

    if current_sleep < FAILURE_SLEEP_MIN:
        return FAILURE_SLEEP_MIN
    return min(current_sleep * 2, FAILURE_SLEEP_MAX)


async def _handle_scrape_failure(
    exc: BaseException, proxy: str, video_id: str, entry: str,
    video_fm: AssetFileManagement, sleep: int,
) -> int:
    '''
    Classify a yt-dlp failure, update metrics, log it at the right
    level, and return the new ``sleep`` value the worker should use
    before its next attempt.

    The classification side-effects depend on the reason:

    * ``rate_limit`` — escalates ``sleep`` via
      :func:`_next_failure_sleep` (exponential backoff starting at
      ``FAILURE_SLEEP_MIN`` up to ``FAILURE_SLEEP_MAX``) and
      increments the rate-limit-hits counter.
    * ``missing_data`` / ``unavailable`` / ``premiere`` — resets
      ``sleep`` to 0; the worker should retry the next thing
      immediately. ``unavailable`` additionally marks the on-disk
      file via ``video_fm.mark_unavailable``.
    * ``transient`` — leaves ``sleep`` unchanged so a flaky proxy
      doesn't trigger a YouTube-style backoff.
    * ``other`` — escalates ``sleep`` via :func:`_next_failure_sleep`
      as a conservative default.
    '''

    reason: str = _classify_yt_dlp_error(str(exc))
    proxy_net: str = _proxy_network(proxy)
    proxy_ip_val: str = (
        extract_proxy_ip(proxy) if proxy else 'none'
    )
    METRIC_SCRAPE_FAILURES.labels(
        platform='youtube',
        scraper='video_scraper',
        entity='video',
        api='ytdlp',
        reason=reason,
        worker_id=get_worker_id(),
        proxy_ip=proxy_ip_val,
        proxy_network=proxy_net,
    ).inc()

    if reason == 'rate_limit':
        METRIC_RATE_LIMIT_HITS.labels(
            platform='youtube',
            scraper='video_scraper',
            entity='video',
            api='ytdlp',
            proxy_ip=proxy_ip_val,
            proxy_network=proxy_net,
            worker_id=get_worker_id(),
        ).inc()
        logging.warning(
            'Rate limited during scraping video',
            exc=exc,
            extra={'video_id': video_id, 'proxy': proxy},
        )
        return _next_failure_sleep(sleep)
    if reason == 'missing_data':
        logging.info(
            'Missing microformat data for video',
            exc=exc,
            extra={'video_id': video_id, 'proxy': proxy},
        )
        return 0
    if reason == 'unavailable':
        logging.info(
            'Video not available for scraping',
            exc=exc,
            extra={'video_id': video_id, 'proxy': proxy},
        )
        try:
            await video_fm.mark_unavailable(entry)
        except OSError:
            pass
        return 0
    if reason == 'premiere':
        logging.info(
            'Video is a Premiere, skipping for now',
            exc=exc,
            extra={'video_id': video_id, 'proxy': proxy},
        )
        return 0
    if reason == 'transient':
        # Sleep deliberately not bumped — a transient/proxy
        # failure shouldn't trigger a YouTube-style backoff.
        logging.info(
            'Transient failure during scraping',
            exc=exc,
            extra={'video_id': video_id, 'proxy': proxy},
        )
        return sleep
    logging.info(
        'Failed to scrape video',
        exc=exc,
        extra={'video_id': video_id, 'proxy': proxy},
    )
    return _next_failure_sleep(sleep)


async def _video_has_formats_on_server(
    exchange_client: ExchangeClient,
    settings: VideoSettings,
    video_id: str,
) -> bool:
    '''
    Check whether the video already exists on the
    scrape.exchange server with a non-empty formats list.

    Makes two HTTP calls: one to the data param endpoint
    for metadata, then fetches the data_url to inspect the
    actual scraped payload. Returns ``False`` on any error
    so the caller falls through to normal scraping.

    :param exchange_client: Authenticated API client.
    :param settings: Scraper settings (exchange_url,
        schema_owner, schema_version).
    :param video_id: YouTube video ID to look up.
    :returns: ``True`` if the server already holds data for
        this video with one or more format entries.
    '''

    url: str = (
        f'{settings.exchange_url}'
        f'{ExchangeClient.GET_DATA_PARAM}'
        f'/{settings.schema_owner}'
        f'/youtube/video'
        f'/{settings.schema_version}'
        f'/{video_id}'
    )
    try:
        resp: Response = await exchange_client.get(url)
    except Exception:
        return False
    if resp.status_code != 200:
        return False
    try:
        metadata: dict[str, any] = resp.json()
    except Exception:
        return False
    data_url: str | None = metadata.get('data_url')
    if not data_url:
        return False
    try:
        data_resp: Response = await exchange_client.get(
            data_url,
        )
    except Exception:
        return False
    if data_resp.status_code != 200:
        return False
    try:
        data: dict[str, any] = data_resp.json()
    except Exception:
        return False
    formats: list[any] | None = data.get('formats')
    return isinstance(formats, list) and len(formats) > 0


async def _scrape(entry: str, video_id: str, channel_handle: str,
                  download_client: YoutubeDL | None,
                  settings: VideoSettings,
                  video_fm: AssetFileManagement,
                  proxy: str, sleep: int = 0,
                  from_priority: bool = False,
                  ) -> tuple[YouTubeVideo | None, int]:
    '''
    Scrapes video data for a given video ID using InnerTube and yt-dlp. If
    video scraping fails due to rate limiting or transient errors, returns an
    integer indicating how long to sleep before the next attempt.

    :param entry: Filename of the video data file to scrape
    :param video_id: YouTube video ID to scrape
    :param channel_handle: YouTube channel name associated with the video
    :param download_client: YoutubeDL instance for downloading video data
    :param settings: Configuration settings for the tool
    :param video_fm: AssetFileManagement instance owning the video data
        directory.
    :param proxy: Proxy URL to use for scraping
    :param sleep: Optional integer indicating how long to sleep before scraping
    :returns: YouTubeVideo instance if scraping is successful, sleep duration
    in seconds if scraping fails due to rate limiting or transient errors.
    The value of the sleep duration should be used the next time this function
    is called.
    '''

    video: YouTubeVideo | None = None
    scrape_start: float = time.monotonic()
    # Priority items intentionally skip the on-disk
    # ``video-dlp-`` artifact: their source ``video-min-`` file
    # in the priority directory is what gets moved to
    # ``uploaded_dir`` after a successful upload, so writing a
    # second file would just litter ``base_dir``.
    save_dir: str | None = (
        None if from_priority else settings.video_data_directory
    )
    try:
        video: YouTubeVideo = await YouTubeVideo.scrape(
            video_id, channel_handle=channel_handle,
            channel_thumbnail=None,
            ytdlp_cache_dir=settings.ytdlp_cache_dir,
            download_client=download_client,
            save_dir=save_dir,
            filename_prefix=VIDEO_YTDLP_PREFIX,
            debug=settings.log_level == 'DEBUG',
            proxies=[proxy],
            with_formats=settings.video_use_yt_dlp,
        )
        METRIC_SCRAPE_DURATION.labels(
            platform='youtube',
            scraper='video_scraper',
            entity='video',
            api='ytdlp',
            outcome='success',
            worker_id=get_worker_id(),
        ).observe(time.monotonic() - scrape_start)
        sleep = 0
        logging.info(
            'Successfully scraped video',
            extra={'video_id': video_id, 'proxy': proxy},
        )
    except Exception as exc:
        METRIC_SCRAPE_DURATION.labels(
            platform='youtube',
            scraper='video_scraper',
            entity='video',
            api='ytdlp',
            outcome='failure',
            worker_id=get_worker_id(),
        ).observe(time.monotonic() - scrape_start)
        sleep = await _handle_scrape_failure(
            exc=exc, proxy=proxy, video_id=video_id,
            entry=entry, video_fm=video_fm, sleep=sleep,
        )

    return video, sleep


async def resolve_video_upload_handle(
    video: YouTubeVideo,
    creator_map_backend: CreatorMap,
    proxy: str | None,
) -> str | None:
    '''
    Resolve the handle to use as platform_creator_id for *video*.

    Read path:
        1. Try the creator map by channel_id (hit → return it).
        2. On miss, resolve via InnerTube. On success with a handle,
           write to the map and return it.
        3. On success without a handle (legacy channel), fall back to
           fallback_handle(video.channel_handle) and write to map.
        4. On InnerTube failure, return None — caller must skip the
           upload. Do NOT write to the map; the next tick retries.
        5. When neither channel_id nor channel_handle are populated
           (degenerate record from RSS or a corrupt write) there is
           nothing to fall back to, so return None and let the caller
           skip the upload.

    :returns: The handle to use, or None when the upload should be
        skipped.
    '''

    if not video.channel_id:
        if not video.channel_handle:
            logging.warning(
                'Video has neither channel_id nor channel_handle; '
                'cannot resolve upload handle, skipping',
                extra={'video_id': video.video_id},
            )
            CREATOR_MAP_RESOLUTION_TOTAL.labels(
                platform='youtube',
                scraper='video_scraper',
                outcome='error',
            ).inc()
            return None
        CREATOR_MAP_RESOLUTION_TOTAL.labels(
            platform='youtube',
            scraper='video_scraper',
            outcome='fallback',
        ).inc()
        return fallback_handle(video.channel_handle)

    cached: str | None = await creator_map_backend.get(
        video.channel_id,
    )
    if cached:
        CREATOR_MAP_LOOKUP_TOTAL.labels(
            platform='youtube',
            scraper='video_scraper',
            outcome='hit',
        ).inc()
        return cached

    CREATOR_MAP_LOOKUP_TOTAL.labels(
        platform='youtube',
        scraper='video_scraper',
        outcome='miss',
    ).inc()

    try:
        resolved: str | None = await YouTubeChannel.resolve_channel_id(
            video.channel_id, proxy=proxy,
        )
    except Exception as exc:
        logging.warning(
            'Video scraper: InnerTube handle resolution failed; '
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
            scraper='video_scraper',
            outcome='error',
        ).inc()
        return None

    handle: str
    if resolved:
        handle = resolved
        CREATOR_MAP_RESOLUTION_TOTAL.labels(
            platform='youtube',
            scraper='video_scraper',
            outcome='canonical',
        ).inc()
    else:
        handle = fallback_handle(
            video.channel_handle or video.channel_id,
        )
        CREATOR_MAP_RESOLUTION_TOTAL.labels(
            platform='youtube',
            scraper='video_scraper',
            outcome='fallback',
        ).inc()

    await creator_map_backend.put(video.channel_id, handle)
    return handle


async def enqueue_upload_video(
    client: ExchangeClient, settings: VideoSettings,
    video_fm: AssetFileManagement, handle: str,
    video: YouTubeVideo,
    validator: SchemaValidator,
    *,
    filename_prefix: str = VIDEO_YTDLP_PREFIX,
    move_source_dir: Path | None = None,
) -> bool:
    '''
    Fire-and-forget upload of a scraped video to Scrape Exchange,
    gated by client-side schema validation.

    Returns immediately: the background worker inside
    :class:`ExchangeClient` performs the POST with retries and, on
    HTTP 201, moves the on-disk asset from ``base_dir`` to
    ``uploaded_dir`` via ``video_fm.mark_uploaded``. If the queue is
    full (API down, retries backing up) the enqueue is dropped and
    the file stays in ``base_dir`` to be retried on the next
    iteration. On validation failure the on-disk asset is renamed
    ``<filename>.invalid`` and the function returns ``False``
    without enqueuing anything.

    :param handle: Canonical channel handle to use as
        platform_creator_id; must match the channel entity's handle.
    :param filename_prefix: On-disk filename prefix for the asset
        being uploaded.  Defaults to ``video-dlp-`` (the post-scrape
        artefact) but can be ``video-min-`` when upload-only mode
        ships RSS-discovered records straight to the API without
        running yt-dlp.
    :returns: ``True`` if the job was enqueued, ``False`` if
        dropped or if schema validation failed.
    '''

    filename: str = (
        f'{filename_prefix}{video.video_id}{FILE_EXTENSION}'
    )
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
    # platform_content_id and platform_creator_id are intentionally
    # omitted: the server derives them from the video schema's
    # ``x-scrape-field`` markers (``video_id`` →
    # ``platform_content_id``, ``channel_handle`` →
    # ``platform_creator_id``) which are present in the data dict.
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
    )


async def _collect_video_record(
    filename: str,
    settings: VideoSettings,
    video_fm: AssetFileManagement,
    creator_map_backend: CreatorMap,
    proxy: str | None,
    validator: SchemaValidator,
) -> tuple[str, dict] | None:
    '''
    Read *filename* from base_dir and prepare a bulk-upload record.

    :returns: ``(video_id, record_dict)`` on success, or ``None``
        when the file should be skipped (read error, unresolved
        handle, or missing ``video_id``).

    Side effects: updates the shared creator_map via
    :func:`resolve_video_upload_handle`.
    '''
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
    if handle is None:
        # Handle resolution failed (e.g. InnerTube transient
        # error). Leave the file for the next iteration.
        logging.info(
            'Video bulk upload skipped: handle unresolved; '
            'file remains for retry',
            extra={
                'filename': filename,
                'video_id': video.video_id,
            },
        )
        _record_bulk_filter_skip('no_handle')
        return None
    video.channel_handle = handle

    if not video.video_id:
        logging.warning(
            'Video has no video_id, skipping bulk upload',
            extra={'filename': filename},
        )
        _record_bulk_filter_skip('missing_video_id')
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
    settings: VideoSettings,
    video_fm: AssetFileManagement,
    creator_map_backend: CreatorMap,
    proxy: str | None,
    validator: SchemaValidator,
) -> tuple[str, str, bytes] | None:
    '''
    Per-file work for the video bulk-upload sweep, factored out
    so :func:`upload_videos` can run ``video_concurrency`` of
    these in flight at once via :func:`asyncio.gather`. Handles
    the superseded check, the read / handle-resolution /
    serialise pipeline, and emits the per-record debug logs.

    :returns: ``(video_id, filename, line_bytes)`` on success, or
        ``None`` when the file should be skipped (superseded,
        unresolved handle, missing video_id).
    '''
    logging.debug(
        'Considering video file for bulk upload',
        extra={'filename': filename},
    )
    if not await video_needs_uploading(video_fm, filename):
        logging.debug(
            'Video file superseded, skipping',
            extra={'filename': filename},
        )
        _record_bulk_filter_skip('superseded')
        return None

    record: tuple[str, dict] | None = (
        await _collect_video_record(
            filename, settings, video_fm,
            creator_map_backend, proxy, validator,
        )
    )
    if record is None:
        return None
    video_id: str
    record_dict: dict
    video_id, record_dict = record

    line: bytes = orjson.dumps(record_dict) + b'\n'
    logging.debug(
        'Prepared video record for bulk batch',
        extra={
            'filename': filename,
            'video_id': video_id,
            'record_bytes': len(line),
        },
    )
    return video_id, filename, line


async def _upload_one_video_batch(
    batch_buf: bytes,
    batch_records: list[tuple[str, str]],
    settings: VideoSettings,
    client: ExchangeClient,
    video_fm: AssetFileManagement,
) -> None:
    '''
    Dispatch one prepared batch of video records via the shared
    bulk-upload pipeline and route per-record outcomes into the
    video-specific Prometheus metrics.
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
    METRIC_VIDEO_BULK_BATCHES.labels(
        platform='youtube',
        scraper='video_scraper',
        entity='video',
        mode='bulk',
        worker_id=get_worker_id(),
        outcome=outcome.status,
    ).inc()
    if outcome.success:
        METRIC_VIDEOS_BULK_UPLOADED.labels(
            platform='youtube',
            scraper='video_scraper',
            entity='video',
            mode='bulk',
            status='success',
            worker_id=get_worker_id(),
        ).inc(outcome.success)
    if outcome.failed:
        METRIC_VIDEOS_BULK_FAILED.labels(
            platform='youtube',
            scraper='video_scraper',
            entity='video',
            mode='bulk',
            worker_id=get_worker_id(),
        ).inc(outcome.failed)
    if outcome.missing:
        METRIC_VIDEOS_BULK_MISSING_RESULT.labels(
            platform='youtube',
            scraper='video_scraper',
            entity='video',
            mode='bulk',
            worker_id=get_worker_id(),
        ).inc(outcome.missing)


async def upload_videos(
    settings: VideoSettings,
    client: ExchangeClient,
    video_fm: AssetFileManagement,
    creator_map_backend: CreatorMap,
    validator: SchemaValidator,
) -> None:
    '''
    Sweep ``base_dir`` for ``video-min-*`` (RSS-discovered) and
    ``video-dlp-*`` (yt-dlp-enriched) files and upload them via
    the bulk API in batches of up to ``settings.bulk_batch_size``
    records (or ``settings.bulk_max_batch_bytes`` bytes, whichever
    is hit first).  Both prefixes share the same
    ``boinko/youtube/video`` schema — the only required fields
    (video_id, url, channel_id|channel_handle) are populated by
    the RSS scraper before the file ever lands here, so a single
    bulk batch can carry mixed prefixes.  Per-record success in
    the job results promotes the matching source file to
    ``uploaded_dir``; failed and missing records are left in
    ``base_dir`` for the next iteration.

    The live-scrape worker keeps using the per-video POST path at
    :func:`enqueue_upload_video` because it processes videos as
    they are scraped and bulk batching offers no latency benefit
    for that path.
    '''
    files: list[str] = video_fm.list_base(
        prefix=VIDEO_YTDLP_PREFIX, suffix=FILE_EXTENSION,
    ) + video_fm.list_base(
        prefix=VIDEO_MIN_PREFIX, suffix=FILE_EXTENSION,
    )
    files = [f for f in files if not f.endswith('failed')]
    logging.info(
        'Found video files for bulk upload',
        extra={'files_length': len(files)},
    )
    if not files:
        return

    # Pick the first proxy for handle resolution on creator_map
    # misses. ``resolve_video_upload_handle`` only needs a proxy
    # for InnerTube fallback; cache hits don't touch the network.
    proxies: list[str] = (
        [p.strip() for p in settings.proxies.split(',') if p.strip()]
        if settings.proxies else []
    )
    proxy: str | None = proxies[0] if proxies else None

    batch_buf: bytearray = bytearray()
    batch_records: list[tuple[str, str]] = []
    max_records: int = settings.bulk_batch_size
    max_bytes: int = settings.bulk_max_batch_bytes
    concurrency: int = max(settings.video_concurrency, 1)

    # Run *concurrency* per-file pipelines in flight at once via
    # :func:`asyncio.gather`. Sequential batching consumes the
    # prepared lines in submission order so the ``record_index``
    # fallback in :func:`apply_bulk_results` matches the order the
    # server iterates the .jsonl.
    for start in range(0, len(files), concurrency):
        chunk: list[str] = files[start:start + concurrency]
        prepared: list[
            tuple[str, str, bytes] | None
        ] = await asyncio.gather(*(
            _prepare_video_line(
                f, settings, video_fm,
                creator_map_backend, proxy, validator,
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
                logging.debug(
                    'Video bulk batch reached cap, flushing',
                    extra={
                        'records': len(batch_records),
                        'bytes': len(batch_buf),
                        'max_records': max_records,
                        'max_bytes': max_bytes,
                    },
                )
                await _upload_one_video_batch(
                    bytes(batch_buf), batch_records,
                    settings, client, video_fm,
                )
                batch_buf = bytearray()
                batch_records = []

            batch_buf.extend(line)
            batch_records.append((video_id, filename))

    if batch_records:
        logging.debug(
            'Flushing trailing video bulk batch',
            extra={
                'records': len(batch_records),
                'bytes': len(batch_buf),
            },
        )
        await _upload_one_video_batch(
            bytes(batch_buf), batch_records,
            settings, client, video_fm,
        )


if __name__ == '__main__':
    main()
