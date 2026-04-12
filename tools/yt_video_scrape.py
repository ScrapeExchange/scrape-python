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
import signal
import sys
import time
import asyncio
import logging

from asyncio import Task, Queue
from random import shuffle

import brotli

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from pydantic import AliasChoices, Field, field_validator
from yt_dlp.YoutubeDL import YoutubeDL

from scrape_exchange.exchange_client import ExchangeClient
from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.logging import configure_logging
from scrape_exchange.scraper_supervisor import (
    SupervisorConfig,
    publish_config_metrics,
    run_supervisor,
)
from scrape_exchange.settings import normalize_log_level

from scrape_exchange.scrape_exchange_rate_limiter import (
    ScrapeExchangeRateLimiter,
)
from scrape_exchange.youtube.youtube_rate_limiter import YouTubeRateLimiter
from scrape_exchange.youtube.youtube_video import YouTubeVideo
from scrape_exchange.youtube.youtube_video import (
    DENO_PATH, PO_TOKEN_URL, YTDLP_CACHE_DIR,
)
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
    ['proxy'],
)
METRIC_SCRAPE_FAILURES = Counter(
    'yt_video_proxy_scrape_failures_total',
    'Number of times video scraping failed, labelled by reason '
    '(rate_limit, unavailable, premiere, transient, missing_data, '
    'other) and by proxy.',
    ['proxy', 'reason'],
)
METRIC_SCRAPE_DURATION = Histogram(
    'yt_video_proxy_scrape_duration_seconds',
    'Duration of the yt-dlp / InnerTube scrape call for a single '
    'video, labelled by outcome (success/failure).',
    ['outcome'],
    buckets=(0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0),
)
METRIC_RATE_LIMIT_HITS = Counter(
    'yt_video_proxy_rate_limit_hits_total',
    'Number of times a proxy was rate-limited by YouTube',
    ['proxy'],
)
METRIC_VIDEOS_ENQUEUED = Counter(
    'yt_video_proxy_videos_enqueued_total',
    'Number of videos successfully enqueued for background upload. '
    'Actual delivery is tracked by '
    'exchange_client_background_uploads_total{entity="video"}.',
)
METRIC_VIDEOS_ALREADY_UPLOADED = Counter(
    'yt_video_proxy_videos_already_uploaded_total',
    'Number of videos skipped because they were already uploaded',
)
METRIC_QUEUE_SIZE = Gauge(
    'yt_video_proxy_queue_size',
    'Number of video files pending processing in the queue',
)
METRIC_SLEEP_SECONDS = Gauge(
    'yt_video_proxy_sleep_seconds',
    'Seconds the worker will sleep before processing the next video',
    ['proxy'],
)


class VideoSettings(YouTubeScraperSettings):
    '''
    Tool configuration loaded in priority order:
    CLI flags > environment variables > .env file > built-in defaults.
    '''

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
    Top-level entry point. Reads settings and dispatches to either
    the shared supervisor (when ``video_num_processes > 1``) or the
    in-process scraper worker (when ``video_num_processes == 1``).
    '''

    settings: VideoSettings = VideoSettings()
    os.makedirs(settings.ytdlp_cache_dir, exist_ok=True)
    configure_logging(
        level=settings.video_log_level,
        filename=settings.video_log_file,
        log_format=settings.log_format,
    )

    if settings.video_num_processes > 1:
        sys.exit(run_supervisor(SupervisorConfig(
            scraper_label='video',
            num_processes_env_var='VIDEO_NUM_PROCESSES',
            log_file_env_var='VIDEO_LOG_FILE',
            num_processes=settings.video_num_processes,
            concurrency=settings.video_concurrency,
            proxies=settings.proxies,
            metrics_port=settings.metrics_port,
            log_file=settings.video_log_file or None,
            api_key_id=settings.api_key_id,
            api_key_secret=settings.api_key_secret,
            exchange_url=settings.exchange_url,
        )))

    asyncio.run(_run_worker(settings))


async def _run_worker(settings: VideoSettings) -> None:
    '''
    Run a single in-process scraper worker (the leaf of the
    supervisor tree). Spawns ``settings.video_concurrency`` async
    workers that share the proxy pool round-robin.
    '''

    # AssetFileManagement creates video_data_directory and its 'uploaded'
    # subdirectory automatically.
    video_fm: AssetFileManagement = AssetFileManagement(
        settings.video_data_directory
    )
    logging.info(
        'Starting YouTube video scrape tool',
        extra={'settings': settings.model_dump_json(indent=2)},
    )
    worker_proxies: list[str] = [
        p.strip() for p in settings.proxies.split(',') if p.strip()
    ] if settings.proxies else []
    logging.info(
        'Scraper worker started',
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
    except OSError as exc:
        logging.critical(
            'Failed to bind Prometheus metrics port',
            exc=exc,
            extra={'metrics_port': settings.metrics_port},
        )
        sys.exit(1)
    logging.info(
        'Prometheus metrics available',
        extra={'metrics_port': settings.metrics_port},
    )
    publish_config_metrics(
        role='worker', scraper_label='video',
        num_processes=1,
        concurrency=settings.video_concurrency,
    )

    YouTubeRateLimiter.get(
        state_dir=settings.rate_limiter_state_dir,
        redis_dsn=settings.redis_dsn,
    ).set_proxies(settings.proxies)

    post_rate: float = float(max(
        1,
        settings.video_num_processes
        * settings.video_concurrency,
    ))
    ScrapeExchangeRateLimiter.get(
        state_dir=settings.rate_limiter_state_dir,
        post_rate=post_rate,
        redis_dsn=settings.redis_dsn,
    )

    # Wire SIGINT/SIGTERM to cancel the worker loop so the finally
    # block gets a chance to drain the ExchangeClient upload queue.
    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
    main_task: Task = asyncio.current_task()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, main_task.cancel)
        except NotImplementedError:
            # add_signal_handler is not available on all platforms
            # (e.g. Windows). Fall through without signal wiring.
            pass

    try:
        await worker_loop(settings, video_fm)
    except asyncio.CancelledError:
        logging.info(
            'Shutdown signal received; upload queue drained'
        )
        raise


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

    candidates: list[str] = (
        video_fm.list_base(prefix=VIDEO_MIN_PREFIX, suffix=FILE_EXTENSION)
        + video_fm.list_base(prefix=VIDEO_YTDLP_PREFIX, suffix=FILE_EXTENSION)
    )
    files: list[str] = []
    for entry in candidates:
        if await video_needs_uploading(video_fm, entry):
            files.append(entry)
    shuffle(files)
    queue: Queue = Queue()
    for file in files:
        await queue.put(file)

    METRIC_QUEUE_SIZE.set(len(files))
    logging.debug(
        'Prepared workload',
        extra={'files_length': len(files)},
    )
    return queue


async def worker_loop(settings: VideoSettings,
                      video_fm: AssetFileManagement) -> None:
    '''
    Main worker loop to continuously scrape and upload videos.

    Creates a single shared :class:`ExchangeClient` for all proxy
    workers so that the background fire-and-forget upload queue is
    centralised. On shutdown (SIGINT/SIGTERM / all workers done),
    the client's upload queue is drained for up to 10 seconds in the
    ``finally`` block so in-flight POSTs get a chance to complete.

    :param settings: Configuration settings for the tool
    :param video_fm: AssetFileManagement instance owning the video data
        directory.
    :returns: (none)
    :raises: (none)
    '''

    proxies: list[str] = (
        [p.strip() for p in settings.proxies.split(',') if p.strip()]
        if settings.proxies else []
    )
    queue: Queue = await prepare_workload(settings, video_fm)

    exchange_client: ExchangeClient | None = None
    try:
        exchange_client = await ExchangeClient.setup(
            api_key_id=settings.api_key_id,
            api_key_secret=settings.api_key_secret,
            exchange_url=settings.exchange_url,
        )
    except Exception as exc:
        logging.warning(
            'ExchangeClient setup failed — workers will scrape but '
            'not upload',
            exc=exc,
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

    tasks: list[Task] = []
    try:
        for proxy in worker_assignments:
            task: Task = asyncio.create_task(
                worker(
                    proxy, queue, settings, video_fm, exchange_client
                )
            )
            tasks.append(task)

        started_at: float = time.monotonic()
        try:
            await queue.join()
        except asyncio.CancelledError:
            logging.info(
                'worker_loop cancelled; stopping workers'
            )
            raise
        total_slept_for: float = time.monotonic() - started_at

        logging.info(
            'Workers worked in parallel',
            extra={
                'workers': worker_count,
                'proxies_length': len(proxies),
                'total_slept_for': total_slept_for,
            },
        )
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if exchange_client is not None:
            await exchange_client.drain_uploads(timeout=10.0)


async def worker(proxy: str, queue: Queue, settings: VideoSettings,
                 video_fm: AssetFileManagement,
                 exchange_client: ExchangeClient | None) -> None:
    '''
    Worker function to process video files from the queue using a specific
    proxy.

    :param proxy: Proxy URL to use for scraping
    :param queue: Queue containing video file entries to process
    :param settings: Configuration settings for the tool
    :param video_fm: AssetFileManagement instance owning the video data
        directory.
    :param exchange_client: Shared Scrape.Exchange client (or ``None``
        if setup failed; in that case the worker scrapes but does not
        upload).
    :returns: (none)
    :raises: (none)
    '''

    cookie_file: str | None = await YouTubeRateLimiter.get().get_cookie_file(
        proxy
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

    sleep: int = 0
    files_scraped: int = 0
    files_uploaded: int = 0
    while True:
        entry: str = await queue.get()

        logging.debug(
            'Worker processing file',
            extra={'proxy': proxy, 'entry': entry},
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
                'Video file not found, skipping',
                extra={'entry': entry},
            )
            queue.task_done()
            continue
        except brotli.error as exc:
            logging.warning(
                'Failed to decompress video file, skipping',
                exc=exc,
                extra={'entry': entry},
            )
            await video_fm.delete(entry, fail_ok=False)
            queue.task_done()
            continue
        except Exception as exc:
            logging.warning(
                'Failed to read video file, skipping',
                exc=exc,
                extra={'entry': entry},
            )
            queue.task_done()
            continue

        # Defence-in-depth: prepare_workload already filtered superseded
        # files, but state may have changed between then and now.
        if not await video_needs_uploading(video_fm, entry):
            logging.debug(
                'Video was already uploaded, skipping',
                extra={'video_id': video_id},
            )
            METRIC_VIDEOS_ALREADY_UPLOADED.inc()
            queue.task_done()
            continue

        if video_needs_scraping:
            try:
                video, sleep = await _scrape(
                    entry, video_id, video.channel_name,
                    download_client,
                    settings, video_fm, proxy, sleep
                )
                if not video:
                    if sleep:
                        logging.info(
                            'Sleeping before next attempt',
                            extra={
                                'video_id': video_id,
                                'sleep': sleep,
                            },
                        )
                        await asyncio.sleep(sleep)
                    queue.task_done()
                    continue
            except Exception as exc:
                logging.info(
                    'Failed to scrape video',
                    exc=exc,
                    extra={'video_id': video_id},
                )
                METRIC_SCRAPE_FAILURES.labels(
                    proxy=proxy, reason='other'
                ).inc()
                try:
                    await video_fm.mark_failed(entry)
                except OSError:
                    pass
                if sleep:
                    logging.info(
                        'Sleeping before next attempt',
                        extra={
                            'video_id': video_id,
                            'sleep': sleep,
                        },
                    )
                    METRIC_SLEEP_SECONDS.labels(proxy=proxy).set(sleep)
                    await asyncio.sleep(sleep)
                video = None
                queue.task_done()
                continue

            files_scraped += 1
            METRIC_VIDEOS_SCRAPED.labels(proxy=proxy).inc()
            if video is not None:
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
                        extra={'video_id': video_id},
                    )
                    video = None

        if video is not None and exchange_client is not None:
            # Fire-and-forget: the background worker inside
            # ExchangeClient performs the POST and, on HTTP 201, moves
            # the file from base_dir to uploaded_dir. On queue-full
            # the file stays in base_dir and is picked up on the next
            # worker iteration.
            enqueued: bool = enqueue_upload_video(
                exchange_client, settings, video_fm,
                video.channel_name, video,
            )
            if enqueued:
                files_uploaded += 1
                METRIC_VIDEOS_ENQUEUED.inc()

        logging.debug(
            'Worker progress',
            extra={
                'proxy': proxy,
                'files_scraped': files_scraped,
                'files_uploaded': files_uploaded,
            },
        )
        queue.task_done()


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
    ).inc()

    if reason == 'rate_limit':
        METRIC_RATE_LIMIT_HITS.labels(proxy=proxy).inc()
        logging.warning(
            'Rate limited during scraping video',
            exc=exc,
            extra={'video_id': video_id},
        )
        return _next_failure_sleep(sleep)
    if reason == 'missing_data':
        logging.info(
            'Missing microformat data for video',
            exc=exc,
            extra={'video_id': video_id},
        )
        return 0
    if reason == 'unavailable':
        logging.info(
            'Video not available for scraping',
            exc=exc,
            extra={'video_id': video_id},
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
            extra={'video_id': video_id},
        )
        return 0
    if reason == 'transient':
        # Sleep deliberately not bumped — a transient/proxy
        # failure shouldn't trigger a YouTube-style backoff.
        logging.info(
            'Transient failure during scraping',
            exc=exc,
            extra={'video_id': video_id},
        )
        return sleep
    logging.info(
        'Failed to scrape video',
        exc=exc,
        extra={'video_id': video_id},
    )
    return _next_failure_sleep(sleep)


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
        METRIC_SCRAPE_DURATION.labels(outcome='success').observe(
            time.monotonic() - scrape_start
        )
        sleep = 0
        logging.info(
            'Successfully scraped video',
            extra={'video_id': video_id},
        )
    except Exception as exc:
        METRIC_SCRAPE_DURATION.labels(outcome='failure').observe(
            time.monotonic() - scrape_start
        )
        sleep = await _handle_scrape_failure(
            exc=exc, proxy=proxy, video_id=video_id,
            entry=entry, video_fm=video_fm, sleep=sleep,
        )

    return video, sleep


def enqueue_upload_video(
    client: ExchangeClient, settings: VideoSettings,
    video_fm: AssetFileManagement, channel_name: str,
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
            'platform_creator_id': channel_name,
            'platform_topic_id': None,
        },
        file_manager=video_fm,
        filename=filename,
        entity='video',
        log_extra={'video_id': video.video_id},
    )


if __name__ == '__main__':
    main()
