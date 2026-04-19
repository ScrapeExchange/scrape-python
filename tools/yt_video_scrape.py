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
from random import shuffle

import brotli

from httpx import Response

from prometheus_client import Counter, Gauge, Histogram

from pydantic import AliasChoices, Field, field_validator
from yt_dlp.YoutubeDL import YoutubeDL

from scrape_exchange.exchange_client import ExchangeClient
from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.content_claim import (
    ContentClaim,
    FileContentClaim,
    NullContentClaim,
    RedisContentClaim,
)
from scrape_exchange.worker_id import get_worker_id
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
from scrape_exchange.youtube.settings import YouTubeScraperSettings

VIDEO_MIN_PREFIX = 'video-min-'
VIDEO_YTDLP_PREFIX = 'video-dlp-'
# Exponential backoff window after a rate-limit-flavoured failure:
# the first failure produces a FAILURE_SLEEP_MIN-second sleep, each
# subsequent consecutive failure doubles the previous sleep, capped
# at FAILURE_SLEEP_MAX. Successful scrapes reset the sleep to 0.
FAILURE_SLEEP_MIN: int = 60
FAILURE_SLEEP_MAX: int = 300

FILE_EXTENSION: str = '.json.br'

START_TIME: float = time.monotonic()


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
    used by ``yt_video_proxy_scrape_failures_total``.

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


# Prometheus metrics
METRIC_VIDEOS_SCRAPED = Counter(
    'yt_video_proxy_videos_scraped_total',
    'Number of videos successfully scraped with yt-dlp',
    ['proxy', 'worker_id'],
)
METRIC_SCRAPE_FAILURES = Counter(
    'yt_video_proxy_scrape_failures_total',
    'Number of times video scraping failed, labelled by reason '
    '(rate_limit, unavailable, premiere, transient, missing_data, '
    'other) and by proxy.',
    ['proxy', 'reason', 'worker_id'],
)
METRIC_SCRAPE_DURATION = Histogram(
    'yt_video_proxy_scrape_duration_seconds',
    'Duration of the yt-dlp / InnerTube scrape call for a single '
    'video, labelled by outcome (success/failure).',
    ['outcome', 'worker_id'],
    buckets=(0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0),
)
METRIC_RATE_LIMIT_HITS = Counter(
    'yt_video_proxy_rate_limit_hits_total',
    'Number of times a proxy was rate-limited by YouTube',
    ['proxy', 'worker_id'],
)
METRIC_VIDEOS_ENQUEUED = Counter(
    'yt_video_proxy_videos_enqueued_total',
    'Number of videos successfully enqueued for background upload. '
    'Actual delivery is tracked by '
    'exchange_client_background_uploads_total{entity="video"}.',
    ['worker_id'],
)
METRIC_VIDEOS_ALREADY_UPLOADED = Counter(
    'yt_video_proxy_videos_already_uploaded_total',
    'Number of videos skipped because they were already uploaded',
    ['worker_id'],
)
METRIC_QUEUE_SIZE = Gauge(
    'yt_video_proxy_queue_size',
    'Number of video files pending processing in the queue',
    ['worker_id'],
)
METRIC_SLEEP_SECONDS = Gauge(
    'yt_video_proxy_sleep_seconds',
    'Seconds the worker will sleep before processing the next video',
    ['proxy', 'worker_id'],
)
METRIC_VIDEOS_SKIPPED_HAS_FORMATS = Counter(
    'yt_video_proxy_videos_skipped_has_formats_total',
    'Number of videos skipped because the server already '
    'has data with a non-empty formats list',
    ['worker_id'],
)

# -- upload-only watcher metrics --
METRIC_WATCHER_FILES_DETECTED = Counter(
    'yt_video_watcher_files_detected_total',
    'Files detected by the upload-only file watcher',
    ['worker_id'],
)
METRIC_WATCHER_FILES_SKIPPED = Counter(
    'yt_video_watcher_files_skipped_total',
    'Files skipped by the watcher (already uploaded '
    'or superseded)',
    ['worker_id'],
)
METRIC_WATCHER_BATCHES = Counter(
    'yt_video_watcher_batches_total',
    'Number of change batches yielded by the file '
    'watcher',
    ['worker_id'],
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
        rate_limiter_factory=lambda s: (
            YouTubeRateLimiter.get(
                state_dir=s.rate_limiter_state_dir,
                redis_dsn=s.redis_dsn,
            )
        ),
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

    await worker_loop(
        settings, video_fm, claim,
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


async def prepare_workload(settings: VideoSettings,
                           video_fm: AssetFileManagement) -> Queue:
    '''
    Assess the workload by listing video files in the data directory and
    categorizing them based on their prefixes.

    :param settings: Configuration settings for the tool
    :param video_fm: AssetFileManagement instance owning the video data
        directory.
    :returns: A queue of bare filenames pending processing.
    :raises: (none)
    '''

    dlp_files: list[str] = video_fm.list_base(
        prefix=VIDEO_YTDLP_PREFIX, suffix=FILE_EXTENSION,
    )

    if settings.video_upload_only:
        # Upload-only mode: only process already-scraped
        # video-dlp files; video-min files are excluded
        # because they always require scraping.
        candidates: list[str] = dlp_files
        logging.info(
            'Upload-only mode: queuing video-dlp '
            'files, skipping video-min files',
            extra={'dlp_files_count': len(dlp_files)},
        )
    else:
        min_files: list[str] = video_fm.list_base(
            prefix=VIDEO_MIN_PREFIX,
            suffix=FILE_EXTENSION,
        )

        # Build a set of video IDs that already have a
        # video-dlp file.  When both video-min and
        # video-dlp exist for the same ID the video-min
        # file is stale (the video was already scraped)
        # — delete it and only queue the video-dlp entry
        # for upload.
        dlp_ids: set[str] = {
            f[len(VIDEO_YTDLP_PREFIX):-len(
                FILE_EXTENSION
            )]
            for f in dlp_files
        }
        deduped_min: list[str] = []
        for entry in min_files:
            vid: str = entry[
                len(VIDEO_MIN_PREFIX):-len(
                    FILE_EXTENSION
                )
            ]
            if vid in dlp_ids:
                await video_fm.delete(
                    entry, fail_ok=False,
                )
                logging.debug(
                    'Deleted video-min file superseded '
                    'by video-dlp',
                    extra={'video_id': vid},
                )
            else:
                deduped_min.append(entry)

        candidates = deduped_min + dlp_files
    files: list[str] = []
    for entry in candidates:
        if await video_needs_uploading(video_fm, entry):
            files.append(entry)
    shuffle(files)
    queue: Queue = Queue()
    for file in files:
        await queue.put(file)

    METRIC_QUEUE_SIZE.labels(
        worker_id=get_worker_id(),
    ).set(len(files))
    logging.debug(
        'Prepared workload',
        extra={'files_length': len(files)},
    )
    return queue


async def worker_loop(
    settings: VideoSettings,
    video_fm: AssetFileManagement,
    claim: ContentClaim,
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
    queue: Queue = await prepare_workload(settings, video_fm)

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
                    effective_creator_map,
                )
            )
            tasks.append(task)

        if settings.video_upload_only:
            await _watch_and_upload(
                queue, video_fm, settings,
            )
        else:
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
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if exchange_client is not None:
            await exchange_client.drain_uploads(timeout=10.0)


def _is_video_dlp_file(filename: str) -> bool:
    '''Check if a filename is an uploadable video-dlp file.'''
    return (
        filename.startswith(VIDEO_YTDLP_PREFIX)
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
    or modified ``video-dlp-*.json.br`` files and feeds them
    into *queue* so the existing workers can upload them.

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
            and _is_video_dlp_file(Path(path).name)
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
            if not await video_needs_uploading(
                video_fm, filename,
            ):
                METRIC_WATCHER_FILES_SKIPPED.labels(
                    worker_id=wid,
                ).inc()
                continue
            logging.info(
                'Upload-only: new video file detected',
                extra={'filename': filename},
            )
            await queue.put(filename)
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


async def _scrape_and_save(
    entry: str,
    video_id: str,
    channel_name: str,
    download_client: YoutubeDL,
    settings: VideoSettings,
    video_fm: AssetFileManagement,
    proxy: str,
    sleep: int,
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
    :param channel_name: Channel name for the video.
    :param download_client: Configured YoutubeDL client.
    :param settings: Scraper settings.
    :param video_fm: File manager owning the data dir.
    :param proxy: Proxy URL used for this worker.
    :param sleep: Current backoff sleep value.
    :returns: ``(video, sleep)`` — video is ``None`` on
        any failure.
    '''

    try:
        video: YouTubeVideo | None
        video, sleep = await _scrape(
            entry, video_id, channel_name,
            download_client,
            settings, video_fm, proxy, sleep,
        )
    except Exception as exc:
        logging.info(
            'Failed to scrape video',
            exc=exc,
            extra={'video_id': video_id, 'proxy': proxy},
        )
        METRIC_SCRAPE_FAILURES.labels(
            proxy=proxy, reason='other',
            worker_id=get_worker_id(),
        ).inc()
        try:
            await video_fm.mark_failed(entry)
        except OSError:
            pass
        if sleep:
            METRIC_SLEEP_SECONDS.labels(
                proxy=proxy,
                worker_id=get_worker_id(),
            ).set(sleep)
        return None, sleep

    if video is None:
        return None, sleep

    try:
        await video.to_file(
            settings.video_data_directory,
            VIDEO_YTDLP_PREFIX,
        )
        # to_file uses model I/O so it bypasses FM
        # cleanup; explicitly remove the original
        # entry (e.g. the video-min-{id} file we
        # just upgraded to video-dlp-).
        await video_fm.delete(
            entry, fail_ok=False,
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


async def worker(
    proxy: str,
    queue: Queue,
    settings: VideoSettings,
    video_fm: AssetFileManagement,
    exchange_client: ExchangeClient | None,
    claim: ContentClaim,
    creator_map_backend: CreatorMap,
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
    try:
        download_client: YoutubeDL = (
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

    # Recreate the yt-dlp client every N scrapes to shed
    # accumulated internal state (_printed_messages,
    # cached extractor instances, etc.).
    _YTDLP_REFRESH_INTERVAL: int = 200

    sleep: int = 0
    files_scraped: int = 0
    files_uploaded: int = 0
    scrapes_since_refresh: int = 0
    while True:
        entry: str = await queue.get()
        video: YouTubeVideo | None = None
        try:
            logging.debug(
                'Worker processing file',
                extra={
                    'proxy': proxy,
                    'entry': entry,
                },
            )

            parsed: tuple[str, str, bool] | None = (
                _parse_entry(entry)
            )
            if parsed is None:
                continue
            video_id: str = parsed[0]
            prefix: str = parsed[1]
            needs_scraping: bool = parsed[2]

            # For video-min files, check if the server
            # already has this video with formats before
            # spending a yt-dlp scrape slot.  Skipped in
            # no-upload mode since we won't upload anyway.
            if (needs_scraping
                    and not settings.video_no_upload
                    and exchange_client is not None):
                has_formats: bool = (
                    await _video_has_formats_on_server(
                        exchange_client, settings,
                        video_id,
                    )
                )
                if has_formats:
                    logging.info(
                        'Video already has formats '
                        'on server, skipping scrape',
                        extra={
                            'video_id': video_id,
                        },
                    )
                    METRIC_VIDEOS_SKIPPED_HAS_FORMATS.labels(
                        worker_id=get_worker_id(),
                    ).inc()
                    await video_fm.delete(
                        entry, fail_ok=False,
                    )
                    continue

            video = await _load_video_file(
                video_id,
                settings.video_data_directory,
                prefix, entry, video_fm,
            )
            if video is None:
                continue

            # Defence-in-depth: prepare_workload already
            # filtered superseded files, but state may
            # have changed between then and now.
            if not await video_needs_uploading(
                video_fm, entry,
            ):
                logging.debug(
                    'Video was already uploaded, '
                    'skipping',
                    extra={'video_id': video_id, 'proxy': proxy},
                )
                METRIC_VIDEOS_ALREADY_UPLOADED.labels(
                    worker_id=get_worker_id(),
                ).inc()
                continue

            if needs_scraping:
                acquired: bool = await claim.acquire(
                    video_id,
                )
                if not acquired:
                    logging.debug(
                        'Video claimed by another '
                        'process, skipping',
                        extra={
                            'video_id': video_id,
                        },
                    )
                    continue
                try:
                    video, sleep = (
                        await _scrape_and_save(
                            entry, video_id,
                            video.channel_name,
                            download_client, settings,
                            video_fm, proxy, sleep,
                        )
                    )
                finally:
                    await claim.release(video_id)
                if video is None:
                    if sleep:
                        logging.info(
                            'Sleeping before next '
                            'attempt',
                            extra={
                                'video_id': video_id,
                                'sleep': sleep,
                            },
                        )
                        await asyncio.sleep(sleep)
                    continue
                files_scraped += 1
                scrapes_since_refresh += 1
                METRIC_VIDEOS_SCRAPED.labels(
                    proxy=proxy,
                    worker_id=get_worker_id(),
                ).inc()

            if (video is not None
                    and not settings.video_no_upload
                    and exchange_client is not None):
                handle: str | None = (
                    await resolve_video_upload_handle(
                        video, creator_map_backend, proxy,
                    )
                )
                if handle is None:
                    logging.info(
                        'Video upload skipped: handle '
                        'unresolved; file remains for retry',
                        extra={
                            'video_id': video.video_id,
                            'channel_id': video.channel_id,
                        },
                    )
                else:
                    enqueued: bool = enqueue_upload_video(
                        exchange_client, settings,
                        video_fm, handle, video,
                    )
                    if enqueued:
                        files_uploaded += 1
                        METRIC_VIDEOS_ENQUEUED.labels(
                            worker_id=get_worker_id(),
                        ).inc()

            logging.debug(
                'Worker progress',
                extra={
                    'proxy': proxy,
                    'files_scraped': files_scraped,
                    'files_uploaded': files_uploaded,
                },
            )
        finally:
            # Free the YouTubeVideo (formats,
            # thumbnails, captions, etc.) immediately
            # rather than keeping it alive until the
            # next iteration's assignment.
            video = None
            queue.task_done()

            # Periodically recreate the yt-dlp client
            # to release accumulated internal state.
            if (scrapes_since_refresh
                    >= _YTDLP_REFRESH_INTERVAL):
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
    METRIC_SCRAPE_FAILURES.labels(
        proxy=proxy, reason=reason,
        worker_id=get_worker_id(),
    ).inc()

    if reason == 'rate_limit':
        METRIC_RATE_LIMIT_HITS.labels(
            proxy=proxy, worker_id=get_worker_id(),
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


async def _scrape(entry: str, video_id: str, channel_name: str,
                  download_client: YoutubeDL, settings: VideoSettings,
                  video_fm: AssetFileManagement,
                  proxy: str, sleep: int = 0
                  ) -> tuple[YouTubeVideo | None, int]:
    '''
    Scrapes video data for a given video ID using InnerTube and yt-dlp. If
    video scraping fails due to rate limiting or transient errors, returns an
    integer indicating how long to sleep before the next attempt.

    :param entry: Filename of the video data file to scrape
    :param video_id: YouTube video ID to scrape
    :param channel_name: YouTube channel name associated with the video
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
    try:
        video: YouTubeVideo = await YouTubeVideo.scrape(
            video_id, channel_name=channel_name,
            channel_thumbnail=None,
            ytdlp_cache_dir=settings.ytdlp_cache_dir,
            download_client=download_client,
            save_dir=settings.video_data_directory,
            filename_prefix=VIDEO_YTDLP_PREFIX,
            debug=settings.log_level == 'DEBUG',
            proxies=[proxy]
        )
        METRIC_SCRAPE_DURATION.labels(
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
           fallback_handle(video.channel_name) and write to map.
        4. On InnerTube failure, return None — caller must skip the
           upload. Do NOT write to the map; the next tick retries.

    :returns: The handle to use, or None when the upload should be
        skipped.
    '''

    if not video.channel_id:
        CREATOR_MAP_RESOLUTION_TOTAL.labels(
            scraper='video', outcome='fallback',
        ).inc()
        return fallback_handle(video.channel_name or '')

    cached: str | None = await creator_map_backend.get(
        video.channel_id,
    )
    if cached:
        CREATOR_MAP_LOOKUP_TOTAL.labels(
            scraper='video', outcome='hit',
        ).inc()
        return cached

    CREATOR_MAP_LOOKUP_TOTAL.labels(
        scraper='video', outcome='miss',
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
            scraper='video', outcome='error',
        ).inc()
        return None

    handle: str
    if resolved:
        handle = resolved
        CREATOR_MAP_RESOLUTION_TOTAL.labels(
            scraper='video', outcome='canonical',
        ).inc()
    else:
        handle = fallback_handle(
            video.channel_name or video.channel_id,
        )
        CREATOR_MAP_RESOLUTION_TOTAL.labels(
            scraper='video', outcome='fallback',
        ).inc()

    await creator_map_backend.put(video.channel_id, handle)
    return handle


def enqueue_upload_video(
    client: ExchangeClient, settings: VideoSettings,
    video_fm: AssetFileManagement, handle: str,
    video: YouTubeVideo,
) -> bool:
    '''
    Fire-and-forget upload of a scraped video to Scrape Exchange.

    Returns immediately: the background worker inside
    :class:`ExchangeClient` performs the POST with retries and, on
    HTTP 201, moves the on-disk asset from ``base_dir`` to
    ``uploaded_dir`` via ``video_fm.mark_uploaded``. If the queue is
    full (API down, retries backing up) the enqueue is dropped and
    the file stays in ``base_dir`` to be retried on the next
    iteration.

    :param handle: Canonical channel handle to use as
        platform_creator_id; must match the channel entity's handle.
    :returns: ``True`` if the job was enqueued, ``False`` if dropped.
    '''

    filename: str = (
        f'{VIDEO_YTDLP_PREFIX}{video.video_id}{FILE_EXTENSION}'
    )
    return client.enqueue_upload(
        f'{settings.exchange_url}{ExchangeClient.POST_DATA_API}',
        json={
            'username': settings.schema_owner,
            'platform': 'youtube',
            'entity': 'video',
            'version': settings.schema_version,
            'source_url': video.url,
            'data': video.to_dict(),
            'platform_content_id': video.video_id,
            'platform_creator_id': handle,
            'platform_topic_id': None,
        },
        file_manager=video_fm,
        filename=filename,
        entity='video',
        log_extra={'video_id': video.video_id},
    )


if __name__ == '__main__':
    main()
