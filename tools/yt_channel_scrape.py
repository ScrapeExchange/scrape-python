#!/usr/bin/env python3

'''
YouTube Channel Scrape Tool. Reads YouTube channel names from a .lst file,
scrapes channels that are not already present on disk, and writes channel
records into the channel data directory for tools/yt_channel_upload.py.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import logging
import os
import re
import resource
import shutil
import sys
import time

from random import shuffle
from pathlib import Path
from typing import Any

import aiofiles
import httpx
import redis.asyncio as aioredis

from prometheus_client import Counter, Gauge
from pydantic import AliasChoices, Field, field_validator

from scrape_exchange.creator_map import (
    CreatorMap,
    FileCreatorMap,
    RedisCreatorMap,
)
from scrape_exchange.handle_map import NullHandleMap, RedisHandleMap
from scrape_exchange.youtube.channel_identity import (
    ChannelIdentityStore,
    ChannelNotFoundError,
    ChannelTerminatedError,
    InconsistentIdentityError,
    is_valid_channel_handle,
    resolve_channel_id,
    resolve_channel_handle,
)
from scrape_exchange.channel_list_parsing import (
    dedupe_preserving_case,
    parse_channel_handle,
)

from scrape_exchange.file_management import (
    AssetFileManagement,
    CHANNEL_FILE_PREFIX,
    atomic_write_bytes,
)
from scrape_exchange.scraper_runner import (
    ScraperRunContext,
    ScraperRunner,
)
from scrape_exchange.proxy_loader import proxy_file_label
from scrape_exchange.settings import normalize_log_level
from scrape_exchange.util import extract_proxy_ip, proxy_network_for

from scrape_exchange.redis_claim import RedisClaim
from scrape_exchange.worker_id import get_worker_id
from scrape_exchange.youtube.exchange_channels_set import (
    RedisExchangeChannelsSet,
)
from scrape_exchange.channel_scrape_queue import (
    ChannelScrapeQueueSettings,
    ChannelState,
    RedisChannelScrapeQueue,
)
from scrape_exchange.youtube.settings import YouTubeScraperSettings
from scrape_exchange.youtube.youtube_channel import YouTubeChannel
from scrape_exchange.youtube.youtube_rate_limiter import YouTubeRateLimiter
from scrape_exchange.youtube.youtube_video import DENO_PATH, PO_TOKEN_URL

from scrape_exchange.scraper_metrics import (
    METRIC_SCRAPES_COMPLETED as METRIC_CHANNELS_SCRAPED,
    METRIC_SCRAPE_DURATION,
    METRIC_SCRAPE_FAILURES,
)

CHANNEL_FILE_POSTFIX = '.json.br'

PRIORITY_MAX_RETRIES: int = 5

MAX_NEW_CHANNELS: int = 1000
MAX_RESOLVED_CHANNELS: int = 100


class ChannelSettings(YouTubeScraperSettings):
    '''
    Tool configuration loaded in priority order:
    CLI flags > environment variables > .env file > built-in defaults.
    '''

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
    uploaded_channels_list: str = Field(
        default='/data/uploaded_channels.lst',
        validation_alias=AliasChoices(
            'YOUTUBE_UPLOADED_CHANNELS_LIST',
            'uploaded_channels_list',
        ),
        description=(
            'Path to a newline-delimited list of channel handles '
            'already uploaded to Scrape Exchange. Channels in this '
            'list are skipped before they are scheduled for '
            'scraping.'
        ),
    )
    channel_priority_queues: str = Field(
        default=(
            '7:1000000,30:100000,'
            '90:10000,180:1000,365:0'
        ),
        validation_alias=AliasChoices(
            'CHANNEL_PRIORITY_QUEUES',
            'channel_priority_queues',
        ),
        description=(
            'Comma-separated tier spec '
            '"interval_days:min_subscribers", highest '
            'priority first. -1 in the interval means drop '
            'the channel from the queue after one '
            'successful scrape. The last tier must have '
            'min_subscribers=0 (catch-all). Mirrors the '
            'RSS scraper\'s RSS_PRIORITY_QUEUES, but in days.'
        ),
    )
    channel_queue_resolve_batch: int = Field(default=25)
    channel_queue_scrape_batch: int = Field(default=50)
    channel_queue_idle_poll_seconds: float = Field(
        default=2.0,
    )
    channel_resolve_max_attempts: int = Field(default=5)
    channel_resolve_backoff_seconds: int = Field(
        default=300,
    )
    channel_unavailable_hard_threshold: int = Field(
        default=3,
    )
    channel_unavailable_soft_retry_seconds: int = Field(
        default=86400,
    )
    channel_soft_reap_interval_seconds: int = Field(
        default=60,
    )
    channel_priority_drain_interval_seconds: int = Field(
        default=60,
        validation_alias=AliasChoices(
            'CHANNEL_PRIORITY_DRAIN_INTERVAL_SECONDS',
            'channel_priority_drain_interval_seconds',
        ),
        description=(
            'Seconds between priority-directory drain cycles in '
            'the Redis-driven channel scraper. Runs only on '
            'worker_id=1.'
        ),
    )

    @field_validator('channel_log_level', mode='before')
    @classmethod
    def _normalize_channel_log_level(cls, v: str) -> str:
        return normalize_log_level(v)


# Prometheus metrics — shared declarations live in scraper_metrics to
# avoid duplicate-registration errors when multiple tool modules are
# imported in the same process (e.g. test runners).
METRIC_UNIQUE_CHANNELS_READ = Gauge(
    'unique_channels_read',
    'Number of unique channel names read from the channel list',
    ['platform', 'scraper', 'entity', 'worker_id'],
    multiprocess_mode='livemostrecent',
)

METRIC_CHANNEL_IDS_TO_RESOLVE = Gauge(
    'pending_channel_id_resolutions',
    'Number of channel IDs that needed to be resolved to channel names',
    ['platform', 'scraper', 'entity', 'worker_id'],
    multiprocess_mode='livemostrecent',
)
# channel_id_resolutions_total — resolved/failed collapsed into one
# counter with outcome label per the new naming convention.
METRIC_CHANNEL_IDS_RESOLVED = Counter(
    'channel_id_resolutions_total',
    'Channel ID resolution outcomes, labelled by outcome '
    '(resolved / failed).',
    ['platform', 'scraper', 'entity', 'outcome', 'worker_id'],
)
METRIC_CHANNEL_ID_RESOLUTION_FAILURES = METRIC_CHANNEL_IDS_RESOLVED
METRIC_CHANNEL_NO_CONTENT_FOUND = Counter(
    'channel_no_content_found_total',
    'Number of channels scraped that had no videos, playlists, courses, '
    'podcasts, or products',
    ['platform', 'scraper', 'entity', 'worker_id', 'proxy_ip',
     'proxy_file'],
)
METRIC_CHANNEL_RESOLVE_CLAIM: Counter = Counter(
    'channel_resolve_claim_total',
    'Per-id resolution-claim outcomes for cross-fleet '
    'deduplication of channel_id resolutions',
    [
        'platform', 'scraper', 'entity', 'outcome',
        'worker_id',
    ],
)
METRIC_CHANNEL_EXCHANGE_SET_LOOKUP: Counter = Counter(
    'channel_exchange_set_lookups_total',
    'Per-handle lookups against youtube:exchange_channels',
    [
        'platform', 'scraper', 'entity', 'outcome',
        'worker_id',
    ],
)
METRIC_CHANNEL_SCRAPE_CLAIM: Counter = Counter(
    'channel_scrape_claim_total',
    'Per-handle scrape-claim outcomes for cross-host '
    'deduplication of channel scrapes',
    [
        'platform', 'scraper', 'entity', 'outcome',
        'worker_id',
    ],
)
CHANNEL_STATE_SIZE: Gauge = Gauge(
    'channel_state_size',
    'Number of channels currently in each '
    'workflow state.',
    ['state'],
)
CHANNEL_QUEUE_TIER_SIZE_NEW: Gauge = Gauge(
    'channel_queue_tier_size',
    'Number of channels in each scheduled tier.',
    ['tier'],
)
CHANNEL_RESOLVE_OUTCOMES: Counter = Counter(
    'channel_queue_resolve_outcomes_total',
    'Resolve-phase outcomes per attempt.',
    ['outcome'],
)
CHANNEL_SCRAPE_OUTCOMES: Counter = Counter(
    'channel_queue_scrape_outcomes_total',
    'Scrape-phase outcomes per attempt.',
    ['outcome'],
)
CHANNEL_SOFT_REAP: Counter = Counter(
    'channel_queue_soft_reap_total',
    'Soft-unavailable reaper outcomes.',
    ['outcome'],
)
CHANNEL_EXCHANGE_EXISTENCE_CHECK: Counter = Counter(
    'channel_exchange_existence_check_total',
    'Outcomes of scrape.exchange existence checks '
    'done before scraping a channel from the queue.',
    ['outcome'],
)


def _validate_settings(settings: ChannelSettings) -> None:
    '''
    Validate settings that are required for either the supervisor
    or the worker to run. Exits the process with code 1 and an
    error message on any violation.
    '''

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
    the supervisor tree). Runs the scrape pass using
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
        'Starting YouTube channel scrape tool',
        extra={'settings': settings.model_dump()}
    )

    # AssetFileManagement creates the 'uploaded' subdirectory
    # automatically and owns all read/write/marker operations
    # under channel_data_directory.
    fm: AssetFileManagement = AssetFileManagement(
        settings.channel_data_directory,
    )

    creator_map_backend: CreatorMap
    handle_map_backend: NullHandleMap | RedisHandleMap
    if settings.redis_dsn:
        creator_map_backend = RedisCreatorMap(
            settings.redis_dsn,
            platform='youtube',
        )
        redis_client: aioredis.Redis = (
            creator_map_backend.redis_client
        )
        handle_map_backend = RedisHandleMap(
            redis_client, platform='youtube',
        )
    else:
        creator_map_backend = FileCreatorMap(
            settings.channel_map_file,
        )
        handle_map_backend = NullHandleMap()

    identity_store: ChannelIdentityStore = ChannelIdentityStore(
        creator_map=creator_map_backend,
        handle_map=handle_map_backend,
    )

    if settings.redis_dsn:
        queue_settings: ChannelScrapeQueueSettings = (
            ChannelScrapeQueueSettings(
                channel_priority_queues=(
                    settings.channel_priority_queues
                ),
                channel_queue_resolve_batch=(
                    settings.channel_queue_resolve_batch
                ),
                channel_queue_scrape_batch=(
                    settings.channel_queue_scrape_batch
                ),
                channel_queue_idle_poll_seconds=(
                    settings.channel_queue_idle_poll_seconds
                ),
                channel_resolve_max_attempts=(
                    settings.channel_resolve_max_attempts
                ),
                channel_resolve_backoff_seconds=(
                    settings.channel_resolve_backoff_seconds
                ),
                channel_unavailable_hard_threshold=(
                    settings
                    .channel_unavailable_hard_threshold
                ),
                channel_unavailable_soft_retry_seconds=(
                    settings
                    .channel_unavailable_soft_retry_seconds
                ),
                channel_soft_reap_interval_seconds=(
                    settings
                    .channel_soft_reap_interval_seconds
                ),
            )
        )
        channel_queue: RedisChannelScrapeQueue = (
            RedisChannelScrapeQueue(
                redis_client, queue_settings
            )
        )

    if not settings.proxies:
        logging.info(
            'No proxies configured, using direct '
            'connection for scraping',
        )
        settings.channel_concurrency = 1

    logging.info('Starting scraping channels')
    if settings.redis_dsn:
        worker_id: str = get_worker_id()
        shutdown_event: asyncio.Event = asyncio.Event()
        # TODO(task-26): replace stub callables once
        # a standalone handle→channel_id resolver and
        # a channel_id-accepting scrape function exist.
        # resolve_fn: handle → channel_id (no analogue
        #   yet; _resolve_channel_id_via_innertube is a
        #   private YouTubeChannel instance method).
        # scrape_fn: channel_id → obj with .sub_count
        #   (no analogue yet; scrape_channel takes a
        #   handle and returns bool, not sub_count).
        http_client: httpx.AsyncClient = (
            httpx.AsyncClient(
                timeout=10.0,
                follow_redirects=True,
            )
        )
        try:
            # Supervisor assigns WORKER_ID='1' to the
            # first child (worker_instance = index + 1 in
            # scraper_supervisor.spawn_children). Standalone
            # processes default to '0'. RSS gates its
            # publisher on '1' (yt_rss_scrape.py:2254), so
            # the publisher does not run in standalone dev
            # mode — only in production under a supervisor.
            if worker_id == '1':
                loop_task: asyncio.Task[None] = (
                    asyncio.create_task(
                        _queue_driven_loop(
                            channel_queue,
                            identity_store,
                            settings,
                            fm,
                            creator_map_backend,
                            is_reap_worker=True,
                            shutdown_event=shutdown_event,
                            http_client=http_client,
                        ),
                    )
                )
                publisher_task: asyncio.Task[None] = (
                    asyncio.create_task(
                        _publish_channel_queue_sizes(
                            channel_queue,
                            shutdown_event=shutdown_event,
                        ),
                    )
                )
                drain_task: asyncio.Task[None] = (
                    asyncio.create_task(
                        _priority_drain_loop(
                            channel_queue,
                            identity_store,
                            settings,
                            shutdown_event,
                        ),
                    )
                )
                try:
                    await asyncio.gather(
                        loop_task, publisher_task, drain_task,
                    )
                finally:
                    for task in (
                        loop_task, publisher_task, drain_task,
                    ):
                        if not task.done():
                            task.cancel()
            else:
                await _queue_driven_loop(
                    channel_queue,
                    identity_store,
                    settings,
                    fm,
                    creator_map_backend,
                    is_reap_worker=False,
                    shutdown_event=shutdown_event,
                    http_client=http_client,
                )
        finally:
            await http_client.aclose()
    else:
        await scrape_channels(
            settings, fm, creator_map_backend,
            identity_store,
        )


def _build_channel_rate_limiter(
    s: 'ChannelSettings',
) -> YouTubeRateLimiter:
    '''
    Construct (or fetch) the per-process YouTubeRateLimiter
    singleton.
    '''
    rl: YouTubeRateLimiter = YouTubeRateLimiter.get(
        state_dir=s.rate_limiter_state_dir,
        redis_dsn=s.redis_dsn,
    )
    return rl


def main() -> None:
    '''
    Top-level entry point. Reads settings and dispatches to
    either the shared supervisor or the in-process scraper worker.
    '''

    settings: ChannelSettings = ChannelSettings()
    _validate_settings(settings)

    runner: ScraperRunner = ScraperRunner(
        settings=settings,
        scraper_label='channel',
        platform='youtube',
        num_processes=(settings.channel_num_processes),
        concurrency=max(
            settings.channel_concurrency,
            len(settings.proxies),
            1,
        ),
        metrics_port=settings.metrics_port,
        log_file=settings.channel_log_file,
        log_level=settings.channel_log_level,
        rate_limiter_factory=_build_channel_rate_limiter,
        client_required=False,
    )
    sys.exit(runner.run_sync(_run_worker))


# DEPRECATED: replaced by _queue_driven_loop
async def scrape_channels(
    settings: ChannelSettings,
    fm: AssetFileManagement,
    creator_map_backend: CreatorMap,
    identity_store: ChannelIdentityStore,
) -> None:

    uploaded_handles: set[str] = _load_uploaded_channel_handles(
        settings.uploaded_channels_list,
    )
    logging.info(
        'Read uploaded channels list',
        extra={
            'uploaded_channels_list': settings.uploaded_channels_list,
            'uploaded_handles_count': len(uploaded_handles),
        },
    )

    # Drain the priority directory first so that channels
    # flagged by yt_import_channel_export.py are scraped
    # ahead of the channels.lst backlog.
    priority_handles: dict[str, Path] = (
        await _drain_priority_directory(
            settings, identity_store,
        )
    )
    logging.info(
        'Priority directory drained',
        extra={
            'priority_handles_count': len(priority_handles),
        },
    )

    new_channels: set[str] = await read_channels(
        settings.channel_list,
        creator_map_backend, fm,
        settings.max_new_channels,
        settings.max_resolved_channels,
        uploaded_handles,
        settings.channel_concurrency,
        identity_store,
    )

    logging.info(
        'Read unique channel names from .lst files not '
        'already scraped or marked as not found',
        extra={'new_channels_length': len(new_channels)},
    )
    # Only keep channel handles (no spaces).  Priority
    # handles come first; the channels.lst tail is shuffled
    # independently so priority ordering is preserved.
    regular_list: list[str] = [
        ch for ch in new_channels
        if ' ' not in ch and ch
        and ch not in priority_handles
    ]
    shuffle(regular_list)
    channel_list: list[str] = (
        list(priority_handles.keys()) + regular_list
    )

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
                    settings, fm,
                    channel_handle, creator_map_backend,
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

    concurrency: int = max(
        settings.channel_concurrency,
        len(settings.proxies),
        1,
    )
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

    # Resolve the fate of every priority entry now that all
    # workers have finished: delete on success/sentinel,
    # rename to .failed otherwise.
    _priority_post_cleanup(priority_handles, fm)

    if abort:
        logging.critical(
            'Too many errors encountered, aborting',
        )
        raise RuntimeError(
            'Too many errors encountered during '
            'scraping'
        )


# ---------------------------------------------------------------------------
# Priority-directory drain helpers
# ---------------------------------------------------------------------------

_PRIORITY_CHANNEL_ID_RE: re.Pattern[str] = re.compile(
    r'^UC[A-Za-z0-9_-]{22}$', re.IGNORECASE,
)


def _rename_priority_to_failed(path: Path) -> None:
    '''Rename *path* to ``<path>.failed`` in-place.
    Logs a warning on OSError rather than propagating so
    that one bad rename does not abort the drain loop.
    '''
    failed: Path = path.with_name(path.name + '.failed')
    try:
        path.rename(failed)
    except OSError as exc:
        logging.warning(
            'Failed to rename priority entry to .failed',
            extra={
                'path': str(path),
                'error': str(exc),
            },
        )


async def _drain_priority_directory(
    settings: 'ChannelSettings',
    identity_store: ChannelIdentityStore,
    *,
    channel_queue: 'RedisChannelScrapeQueue | None' = None,
) -> 'dict[str, Path]':
    '''Drain bare channel-ID or channel-handle priority files.

    Legacy file mode returns resolved handles for
    :func:`_priority_post_cleanup`. Redis mode enqueues each resolved
    channel at priority and renames its source file to ``.processed``.
    '''
    priority_dir: Path = Path(
        settings.channel_priority_directory_path,
    )
    if not priority_dir.is_dir():
        return {}

    handles: dict[str, Path] = {}
    for path in sorted(priority_dir.iterdir()):
        if not path.is_file():
            continue
        if path.suffix in ('.failed', '.processed'):
            continue
        channel_id: str | None = None
        handle: str | None = None
        if _PRIORITY_CHANNEL_ID_RE.fullmatch(path.name):
            channel_id = path.name
            try:
                handle = await resolve_channel_id(channel_id)
            except Exception as exc:
                logging.warning(
                    'Priority drain: resolve_channel_id raised',
                    extra={
                        'channel_id': channel_id,
                        'error': str(exc),
                    },
                )
                _rename_priority_to_failed(path)
                continue
            if handle is None:
                logging.warning(
                    'Priority drain: channel_id unresolvable, '
                    'renaming to .failed',
                    extra={'channel_id': channel_id},
                )
                _rename_priority_to_failed(path)
                continue
        else:
            handle = path.name.removeprefix('@')
            if not is_valid_channel_handle(handle):
                continue
            try:
                channel_id = await identity_store.handle_map.get(
                    handle,
                )
                if channel_id is None:
                    channel_id = await resolve_channel_handle(
                        handle,
                    )
            except Exception as exc:
                logging.warning(
                    'Priority drain: channel handle resolution '
                    'raised',
                    extra={
                        'handle': handle,
                        'error': str(exc),
                    },
                )
                _rename_priority_to_failed(path)
                continue
            if channel_id is None:
                logging.warning(
                    'Priority drain: handle unresolvable, '
                    'renaming to .failed',
                    extra={'handle': handle},
                )
                _rename_priority_to_failed(path)
                continue

        try:
            await identity_store.bind(channel_id, handle)
        except InconsistentIdentityError as exc:
            logging.warning(
                'Priority drain: inconsistent identity bind',
                extra={
                    'channel_id': channel_id,
                    'handle': handle,
                    'error': str(exc),
                },
            )
            _rename_priority_to_failed(path)
            continue

        logging.info(
            'Priority drain: resolved priority entry',
            extra={
                'channel_id': channel_id,
                'handle': handle,
            },
        )
        if channel_queue is None:
            handles[handle] = path
            continue
        await channel_queue.enqueue_scheduled(
            channel_id,
            source='priority_directory',
            priority=True,
        )
        path.rename(path.with_name(path.name + '.processed'))

    return handles


def _priority_post_cleanup(
    priority_handles: 'dict[str, Path]',
    fm: AssetFileManagement,
) -> None:
    # Legacy file-based path only. Redis mode renames each priority
    # entry immediately after enqueueing it.
    '''After all workers finish, walk every priority entry
    and decide its fate:

    * **Delete** the priority file if a scraped
      ``channel-{handle}.json.br``, a ``.not_found``
      sentinel, or a ``.unresolved`` sentinel exists in
      either ``fm.base_dir`` or ``fm.uploaded_dir``.
    * **Rename to .failed** otherwise.
    '''
    for handle, path in priority_handles.items():
        scraped: str = (
            f'{CHANNEL_FILE_PREFIX}{handle}'
            f'{CHANNEL_FILE_POSTFIX}'
        )
        not_found: str = (
            f'{CHANNEL_FILE_PREFIX}{handle}.not_found'
        )
        unresolved: str = (
            f'{CHANNEL_FILE_PREFIX}{handle}.unresolved'
        )
        success_markers: list[str] = [
            scraped, not_found, unresolved,
        ]
        succeeded: bool = any(
            (fm.base_dir / name).exists()
            or (fm.uploaded_dir / name).exists()
            for name in success_markers
        )
        if succeeded:
            try:
                path.unlink(missing_ok=True)
            except OSError as exc:
                logging.warning(
                    'Failed to delete priority entry '
                    'after successful scrape',
                    extra={
                        'channel_id': path.name,
                        'error': str(exc),
                    },
                )
        else:
            _rename_priority_to_failed(path)


async def _priority_drain_loop(
    channel_queue: RedisChannelScrapeQueue,
    identity_store: ChannelIdentityStore,
    settings: ChannelSettings,
    shutdown_event: asyncio.Event,
) -> None:
    '''Drain priority-directory entries into Redis periodically.

    The Redis worker starts this only on ``WORKER_ID='1'`` so a
    single worker owns the local priority directory.
    '''
    while not shutdown_event.is_set():
        try:
            await _drain_priority_directory(
                settings,
                identity_store,
                channel_queue=channel_queue,
            )
        except Exception as exc:
            logging.warning(
                'Priority drain cycle raised; will retry next cycle',
                extra={'error': str(exc)},
            )
        try:
            await asyncio.wait_for(
                shutdown_event.wait(),
                timeout=(
                    settings
                    .channel_priority_drain_interval_seconds
                ),
            )
        except asyncio.TimeoutError:
            pass


async def resolve_and_bind_for_entry(
    *,
    channel_id: str,
    store: ChannelIdentityStore,
) -> str | None:
    '''Resolve *channel_id* to a handle via the shared channel
    identity library and persist both directions to Redis.

    Returns the resolved handle or ``None`` if YouTube did not
    return one (caller leaves the entry as unresolved).
    '''
    resolved: str | None = await resolve_channel_id(channel_id)
    if resolved:
        await store.bind(channel_id, resolved)
    return resolved


async def _publish_channel_queue_sizes(
    queue: RedisChannelScrapeQueue,
    *,
    interval: float = 30.0,
    shutdown_event: asyncio.Event,
) -> None:
    '''Publish gauges for channel_state_size and
    channel_queue_tier_size. Run on a single
    elected worker per host.'''
    while not shutdown_event.is_set():
        try:
            counts: dict[ChannelState, int] = (
                await queue.count_by_state()
            )
            for state, n in counts.items():
                CHANNEL_STATE_SIZE.labels(
                    state=state.value,
                ).set(n)
            tier_counts: dict[int, int] = (
                await queue.count_by_tier()
            )
            for tier, n in tier_counts.items():
                CHANNEL_QUEUE_TIER_SIZE_NEW.labels(
                    tier=str(tier),
                ).set(n)
        except Exception:
            logging.warning(
                'channel queue metrics publish '
                'failed',
                exc_info=True,
            )
        try:
            await asyncio.wait_for(
                shutdown_event.wait(),
                timeout=interval,
            )
        except asyncio.TimeoutError:
            pass


async def _queue_driven_loop(
    queue: 'RedisChannelScrapeQueue',
    identity: object,
    settings: 'ChannelSettings',
    fm: AssetFileManagement,
    creator_map_backend: 'CreatorMap',
    *,
    is_reap_worker: bool,
    shutdown_event: asyncio.Event,
    http_client: httpx.AsyncClient,
) -> None:
    '''Wave-driven main loop.

    Each iteration:
    1. Drains a batch from the unresolved queue
       → resolve.
    2. Drains a batch from the scheduled queue
       (across all tiers) → scrape.
    3. On the elected reap worker, runs the
       soft-unavailable reaper at the configured
       interval.
    4. Sleeps ``idle_poll_seconds`` if both queues
       were empty.
    '''
    last_reap: float = 0.0
    while not shutdown_event.is_set():
        handles: list[str] = (
            await queue.pop_unresolved(
                settings.channel_queue_resolve_batch,
            )
        )
        for handle in handles:
            await _resolve_one_queued(
                handle,
                queue=queue,
                identity=identity,
                settings=settings,
            )
        ids: list[str] = await queue.pop_scheduled(
            settings.channel_queue_scrape_batch,
            now=time.time(),
        )
        await _scrape_queued_batch(
            ids,
            queue=queue,
            settings=settings,
            fm=fm,
            creator_map_backend=creator_map_backend,
            http_client=http_client,
        )
        now: float = time.time()
        if (
            is_reap_worker
            and (
                now - last_reap
                >= (
                    settings
                    .channel_soft_reap_interval_seconds
                )
            )
        ):
            try:
                reaped: int = (
                    await queue.reap_soft_unavailable(
                        now=now,
                    )
                )
                if reaped:
                    CHANNEL_SOFT_REAP.labels(
                        outcome='reaped',
                    ).inc(reaped)
            except Exception:
                logging.warning(
                    'soft-unavailable reap failed',
                    exc_info=True,
                )
            last_reap = now
        if not handles and not ids:
            await asyncio.sleep(
                settings
                .channel_queue_idle_poll_seconds,
            )


async def _scrape_queued_batch(
    ids: list[str],
    *,
    queue: RedisChannelScrapeQueue,
    settings: ChannelSettings,
    fm: AssetFileManagement,
    creator_map_backend: CreatorMap,
    http_client: httpx.AsyncClient,
) -> None:
    '''Scrape one already-popped scheduled batch concurrently.'''
    if not ids:
        return
    concurrency: int = max(
        settings.channel_concurrency,
        len(settings.proxies),
        1,
    )
    semaphore: asyncio.Semaphore = asyncio.Semaphore(
        concurrency,
    )

    async def scrape_one(channel_id: str) -> None:
        async with semaphore:
            await _scrape_one_queued(
                channel_id,
                queue=queue,
                settings=settings,
                fm=fm,
                creator_map_backend=creator_map_backend,
                http_client=http_client,
            )

    await asyncio.gather(*(
        scrape_one(channel_id) for channel_id in ids
    ))


async def _resolve_one_queued(
    handle: str,
    *,
    queue: RedisChannelScrapeQueue,
    identity: ChannelIdentityStore,
    settings: ChannelSettings,
) -> None:
    '''Resolve a single handle popped from the
    unresolved queue via InnerTube, dispatching state
    transitions based on the outcome.

    Uses YouTubeChannel._resolve_channel_id_via_innertube
    directly so no external callable is needed.
    '''
    canonical: str = (
        handle.strip().removeprefix('@').strip()
    )
    member: str = f'h:{canonical}'
    channel: YouTubeChannel = YouTubeChannel(
        channel_handle=handle,
        deno_path=DENO_PATH,
        po_token_url=PO_TOKEN_URL,
        debug=False,
        save_dir=settings.channel_data_directory,
        with_download_client=False,
    )
    channel._create_browse_client(
        proxies=settings.proxies,
    )
    try:
        ok: bool = (
            await channel
            ._resolve_channel_id_via_innertube()
        )
    except OSError as exc:
        meta: dict[str, str] = (
            await queue.get_meta(member)
        )
        attempts: int = (
            int(meta.get('resolve_attempts', '0'))
            + 1
        )
        await queue.set_meta(
            member,
            resolve_attempts=str(attempts),
        )
        if (
            attempts
            >= settings.channel_resolve_max_attempts
        ):
            await queue.mark(
                member,
                state=ChannelState.UNRESOLVED,
                last_error=str(exc),
            )
            CHANNEL_RESOLVE_OUTCOMES.labels(
                outcome='unresolved',
            ).inc()
        else:
            await queue.requeue_with_backoff(
                handle,
                seconds=(
                    settings
                    .channel_resolve_backoff_seconds
                ),
                now=time.time(),
                unresolved=True,
            )
            CHANNEL_RESOLVE_OUTCOMES.labels(
                outcome='backoff',
            ).inc()
        return
    if not ok or not channel.channel_id:
        await queue.mark(
            member,
            state=ChannelState.NOT_FOUND,
            last_error='InnerTube returned no id',
        )
        CHANNEL_RESOLVE_OUTCOMES.labels(
            outcome='not_found',
        ).inc()
        return
    try:
        await identity.bind(
            channel.channel_id, canonical,
        )
    except InconsistentIdentityError as exc:
        await queue.mark(
            member,
            state=ChannelState.INCONSISTENT_IDENTITY,
            last_error=str(exc),
        )
        CHANNEL_RESOLVE_OUTCOMES.labels(
            outcome='inconsistent_identity',
        ).inc()
        return
    await queue.promote_to_scheduled(
        handle, channel.channel_id,
    )
    CHANNEL_RESOLVE_OUTCOMES.labels(
        outcome='promoted',
    ).inc()


async def _channel_exists_on_exchange(
    http_client: httpx.AsyncClient,
    exchange_url: str,
    channel_id: str,
) -> bool:
    '''Check whether scrape.exchange already has the
    channel record. Returns True if the platform has
    at least one record under
    /api/v1/data/content/youtube/channel/<channel_id>.

    On any error (network failure, non-200 response,
    malformed JSON), returns False and increments the
    error metric — the caller falls back to a full
    scrape, which is the safe default.
    '''
    url: str = (
        f'{exchange_url.rstrip("/")}'
        f'/api/v1/data/content/youtube/channel/'
        f'{channel_id}'
    )
    try:
        resp: httpx.Response = await http_client.get(
            url,
        )
    except Exception as exc:
        CHANNEL_EXCHANGE_EXISTENCE_CHECK.labels(
            outcome='error',
        ).inc()
        logging.debug(
            'channel existence check failed',
            exc_info=exc,
            extra={'channel_id': channel_id},
        )
        return False
    if resp.status_code != 200:
        CHANNEL_EXCHANGE_EXISTENCE_CHECK.labels(
            outcome='error',
        ).inc()
        return False
    try:
        body: dict[str, Any] = resp.json()
    except Exception:
        CHANNEL_EXCHANGE_EXISTENCE_CHECK.labels(
            outcome='error',
        ).inc()
        return False
    total_count: int = int(
        body.get('total_count', 0),
    )
    if total_count > 0:
        CHANNEL_EXCHANGE_EXISTENCE_CHECK.labels(
            outcome='exists',
        ).inc()
        return True
    CHANNEL_EXCHANGE_EXISTENCE_CHECK.labels(
        outcome='not_exists',
    ).inc()
    return False


async def _scrape_one_queued(
    channel_id: str,
    *,
    queue: RedisChannelScrapeQueue,
    settings: ChannelSettings,
    fm: AssetFileManagement,
    creator_map_backend: CreatorMap,
    http_client: httpx.AsyncClient,
) -> None:
    '''Scrape a single channel_id popped from the
    scheduled queue. Looks up the handle from
    creator_map_backend, runs
    _do_scrape_channel_to_disk_typed, and dispatches
    state transitions based on the outcome.
    '''
    member: str = f'i:{channel_id}'
    handle: str | None = (
        await creator_map_backend.get(channel_id)
    )
    if handle is None:
        await queue.mark(
            member,
            state=ChannelState.UNRESOLVED,
            last_error=(
                f'no handle in creator_map for '
                f'{channel_id!r}'
            ),
        )
        CHANNEL_SCRAPE_OUTCOMES.labels(
            outcome='unresolved',
        ).inc()
        return
    try:
        metadata_only: bool = (
            await _channel_exists_on_exchange(
                http_client,
                settings.exchange_url,
                channel_id,
            )
        )
    except Exception:
        logging.warning(
            'existence check raised unexpectedly; '
            'falling back to full scrape',
            exc_info=True,
            extra={'channel_id': channel_id},
        )
        CHANNEL_EXCHANGE_EXISTENCE_CHECK.labels(
            outcome='error',
        ).inc()
        metadata_only = False
    extra: dict[str, str] = {
        'channel_id': channel_id,
        'channel_handle': handle,
        'metadata_only': str(metadata_only),
    }
    filename: str = f'{CHANNEL_FILE_PREFIX}{handle}'
    try:
        channel: YouTubeChannel = (
            await _do_scrape_channel_to_disk_typed(
                settings, fm, handle, filename, extra,
                metadata_only=metadata_only,
            )
        )
    except ChannelNotFoundError as exc:
        await queue.mark(
            member,
            state=ChannelState.NOT_FOUND,
            last_error=str(exc),
        )
        CHANNEL_SCRAPE_OUTCOMES.labels(
            outcome='not_found',
        ).inc()
        return
    except ChannelTerminatedError as exc:
        await queue.mark(
            member,
            state=ChannelState.TERMINATED,
            last_error=str(exc),
        )
        CHANNEL_SCRAPE_OUTCOMES.labels(
            outcome='terminated',
        ).inc()
        return
    except (RuntimeError, OSError) as exc:
        await queue.mark_soft_unavailable(
            channel_id, last_error=str(exc),
        )
        CHANNEL_SCRAPE_OUTCOMES.labels(
            outcome='soft_unavailable',
        ).inc()
        return
    await queue.update_tier(
        channel_id,
        sub_count=channel.subscriber_count or 0,
        now=time.time(),
    )
    CHANNEL_SCRAPE_OUTCOMES.labels(
        outcome='scraped',
    ).inc()


def normalize_channel_name(channel_handle: str) -> str:
    '''
    Normalises a YouTube channel name extracted from user input by
    stripping whitespace and a leading '@'. Also strips URL prefixes
    and anything after an '@' when the input looks like an email
    address.

    Case is preserved: input at this stage may not yet be the
    canonical handle.

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


def _handle_from_channel_filename(filename: str) -> str:
    '''Strip ``channel-`` prefix and ``.json.br`` suffix to recover
    the bare handle, the inverse of :func:`get_channel_filename`.

    Used by the bulk-upload write-back into
    ``youtube:exchange_channels`` so apply_bulk_results doesn't need
    to know channel-specific filename conventions.'''
    return filename.removeprefix(
        CHANNEL_FILE_PREFIX,
    ).removesuffix(CHANNEL_FILE_POSTFIX)


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
        platform='youtube',
        scraper='channel_scraper',
        entity='channel',
        api='html',
        reason='other',
        worker_id=get_worker_id(),
        proxy_ip=proxy_used_ip,
        proxy_file=proxy_file_label(proxy_used or ''),
    ).inc()
    logging.warning(
        message, exc=exc, extra=extra | {
            'proxy': proxy_used, 'proxy_ip': proxy_used_ip,
        },
    )


async def _try_scrape_channel_typed(
    channel: YouTubeChannel,
    settings: ChannelSettings,
    extra: dict[str, str],
    *,
    metadata_only: bool = False,
) -> str | None:
    '''Run ``channel.scrape()`` and surface failures as typed
    exceptions for the queue-driven path.  Returns the proxy used
    by ``channel.browse_client`` on success (may be ``None`` for
    the direct path).

    :param metadata_only: when True, passes
        ``with_video_ids=False`` to ``channel.scrape()`` so
        only metadata (about page, subscriber count,
        thumbnails, banners) is fetched.

    Raises:
        ChannelNotFoundError: ``channel.scrape()`` raised
            ``ValueError`` indicating the handle does not resolve
            to a real channel.
        asyncio.CancelledError: propagated unchanged.
        RuntimeError, Exception: propagated unchanged for the
            caller to classify (transient vs permanent).
    '''
    logging.info('Scraping channel', extra=extra)
    try:
        await channel.scrape(
            with_about_page=True,
            max_videos_per_channel=0,
            proxies=settings.proxies,
            with_video_ids=not metadata_only,
        )
    except ValueError as exc:
        logging.debug(
            'Channel not found',
            extra=extra,
        )
        raise ChannelNotFoundError(str(exc)) from exc
    return getattr(
        channel.browse_client, 'proxy', None,
    )


async def _try_scrape_channel(
    channel: YouTubeChannel, settings: ChannelSettings,
    fm: AssetFileManagement, channel_handle: str,
    extra: dict[str, str],
) -> tuple[bool, str | None]:
    '''
    Run ``channel.scrape()``, trapping known failure modes and
    returning ``(False, None)`` for each.

    :returns: ``(succeeded, scrape_proxy)``. On any failure mode
        that the caller should treat as a clean miss,
        ``succeeded`` is False and the caller returns ``False``
        from :func:`scrape_channel`.
    '''
    try:
        proxy: str | None = await _try_scrape_channel_typed(
            channel, settings, extra,
        )
        return True, proxy
    except asyncio.CancelledError:
        raise
    except ChannelNotFoundError:
        try:
            await fm.mark_not_found(
                f'{CHANNEL_FILE_PREFIX}{channel_handle}',
                content=f'{channel_handle}\n',
            )
        except OSError:
            logging.warning(
                'Failed to write not_found marker '
                'for channel',
                extra=extra,
            )
        return False, None
    except RuntimeError as exc:
        _record_scrape_failure(
            channel, 'Failed to scrape channel', exc, extra,
        )
        return False, None
    except Exception as exc:
        _record_scrape_failure(
            channel,
            'Unexpected error while scraping channel',
            exc, extra,
        )
        return False, None


def _channel_has_no_content(
    channel: YouTubeChannel, scrape_proxy_ip: str,
    scrape_proxy_network: str, scrape_proxy: str | None,
    channel_handle: str,
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
        platform='youtube',
        scraper='channel_scraper',
        entity='channel',
        worker_id=get_worker_id(),
        proxy_ip=scrape_proxy_ip,
        proxy_file=proxy_file_label(scrape_proxy or ''),
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


async def _try_acquire_scrape_claim(
    channel_handle: str,
    creator_map_backend: CreatorMap,
) -> tuple['RedisClaim | None', str]:
    '''Try to acquire ``claim:youtube:channel:<channel_handle>`` on the
    Redis behind ``creator_map_backend``. The claim coordinates
    cross-host scrape work so two hosts running the same channel
    list don't both burn an InnerTube ``BROWSE`` budget on the
    same handle.

    Returns ``(claim, status)`` where ``status`` is one of:

    * ``'won'``: claim is held; caller must call
      ``RedisClaim.release(channel_handle)`` on terminal outcome.
    * ``'lost'``: a peer host holds the claim; caller should
      skip the scrape.
    * ``'no_redis'``: ``creator_map_backend`` has no Redis
      backend; ``claim`` is ``None`` and the caller should
      proceed without cross-host coordination.

    Emits ``channel_scrape_claim_total`` on won/lost so the
    caller doesn't have to repeat the labels. TTL is 60 minutes
    — matched to the ``YOUTUBE_UPLOADED_CHANNELS_LIST`` refresh
    cadence so a peer host hitting the same handle within a
    refresh window still sees the claim (and skips) even if our
    process crashed without calling ``release``. Channel scrapes
    typically finish in seconds, so the TTL is the
    crash-recovery floor, not the expected hold time.
    '''
    redis_for_claim: aioredis.Redis | None = (
        creator_map_backend.redis_client
    )
    if redis_for_claim is None:
        return None, 'no_redis'
    claim: RedisClaim = RedisClaim(
        redis_for_claim,
        key_prefix='claim:youtube:channel:',
        ttl_seconds=3600,
        owner=get_worker_id(),
    )
    won: bool = await claim.try_claim(channel_handle)
    METRIC_CHANNEL_SCRAPE_CLAIM.labels(
        platform='youtube',
        scraper='channel_scraper',
        entity='channel',
        outcome='won' if won else 'lost',
        worker_id=get_worker_id(),
    ).inc()
    if not won:
        return None, 'lost'
    return claim, 'won'


async def _scrape_channel_to_disk(
    settings: ChannelSettings,
    fm: AssetFileManagement,
    channel_handle: str,
    filename: str,
    creator_map_backend: CreatorMap,
    extra: dict[str, str],
) -> tuple[bool, bool | None, YouTubeChannel | None]:
    '''Acquire the cross-host scrape claim, run InnerTube, persist
    on success. Returns
    ``(should_continue, early_return, channel)``:

    * ``(True, None, channel)`` — file is on disk.
    * ``(False, False, None)`` — claim lost, scrape failed, or
      channel had no content; caller should ``return False``.
    * ``(False, True, None)`` — persist failed; caller should
      ``return True``.

    The claim is held only across the InnerTube scrape + persist.
    '''
    scrape_claim: RedisClaim | None
    claim_status: str
    scrape_claim, claim_status = (
        await _try_acquire_scrape_claim(
            channel_handle, creator_map_backend,
        )
    )
    if claim_status == 'lost':
        logging.debug(
            'Channel scrape claim held by peer host, skipping',
            extra=extra,
        )
        return False, False, None

    try:
        return await _do_scrape_channel_to_disk(
            settings, fm, channel_handle, filename, extra,
        )
    finally:
        if scrape_claim is not None:
            await scrape_claim.release(channel_handle)


async def _do_scrape_channel_to_disk(
    settings: ChannelSettings,
    fm: AssetFileManagement,
    channel_handle: str,
    filename: str,
    extra: dict[str, str],
) -> tuple[bool, bool | None, YouTubeChannel | None]:
    '''Inner half of :func:`_scrape_channel_to_disk` — runs while
    the cross-host scrape claim is held. Same return contract.'''
    scrape_start: float = time.monotonic()
    logging.debug(
        'Channel not scraped, scraping now', extra=extra,
    )
    channel: YouTubeChannel = YouTubeChannel(
        channel_handle=channel_handle, deno_path=DENO_PATH,
        po_token_url=PO_TOKEN_URL, debug=True,
        save_dir=settings.channel_data_directory,
        channel_id=extra.get('channel_id'),
        with_download_client=False,
    )
    ok: bool
    scrape_proxy: str | None
    ok, scrape_proxy = await _try_scrape_channel(
        channel, settings, fm, channel_handle, extra,
    )
    if not ok:
        METRIC_SCRAPE_DURATION.labels(
            platform='youtube',
            scraper='channel_scraper',
            entity='channel',
            api='html',
            outcome='failure',
            worker_id=get_worker_id(),
        ).observe(time.monotonic() - scrape_start)
        return False, False, None

    scrape_proxy_ip: str = (
        extract_proxy_ip(scrape_proxy)
        if scrape_proxy else 'none'
    )
    scrape_proxy_network: str = proxy_network_for(
        scrape_proxy_ip,
    )

    if _channel_has_no_content(
        channel, scrape_proxy_ip,
        scrape_proxy_network, scrape_proxy, channel_handle,
    ):
        return False, False, None

    if not await _persist_scraped_channel(
        fm, filename, channel, channel_handle,
    ):
        return False, True, None

    # ``api`` reflects which path produced the scrape:
    # - ``html`` if /about succeeded (the InnerTube call below it
    #   ran too, but /about supplied the channel-only fields)
    # - ``innertube`` if /about failed and only the InnerTube
    #   fallback produced data (Fix #2; quantifies its share)
    success_api: str = (
        'html' if channel.about_page_succeeded else 'innertube'
    )
    METRIC_CHANNELS_SCRAPED.labels(
        platform='youtube',
        scraper='channel_scraper',
        entity='channel',
        api=success_api,
        worker_id=get_worker_id(),
        proxy_ip=scrape_proxy_ip,
        proxy_file=proxy_file_label(scrape_proxy or ''),
    ).inc()
    METRIC_SCRAPE_DURATION.labels(
        platform='youtube',
        scraper='channel_scraper',
        entity='channel',
        api=success_api,
        outcome='success',
        worker_id=get_worker_id(),
    ).observe(time.monotonic() - scrape_start)
    logging.info(
        'Downloaded channel',
        extra={
            'channel_handle': channel_handle,
            'proxy_ip': scrape_proxy_ip,
            'proxy_network': scrape_proxy_network,
        },
    )
    return True, None, channel


async def _do_scrape_channel_to_disk_typed(
    settings: ChannelSettings,
    fm: AssetFileManagement,
    channel_handle: str,
    filename: str,
    extra: dict[str, str],
    *,
    metadata_only: bool = False,
) -> YouTubeChannel:
    '''Typed-exception variant of
    :func:`_do_scrape_channel_to_disk` for the
    queue-driven path. Performs the same scrape +
    no-content check + persistence sequence; on
    success returns the populated
    ``YouTubeChannel`` instance. The same Prometheus
    metric labels (``METRIC_SCRAPE_DURATION``,
    ``METRIC_CHANNELS_SCRAPED``) are emitted.

    :param metadata_only: when True, passes
        ``with_video_ids=False`` to the scrape and
        skips the no-content check (an existing
        channel without new video_ids is the expected
        outcome, not a failure).

    Raises:
        ChannelNotFoundError: channel returned 404,
            or the scrape produced no content (no
            video_ids, playlists, courses, podcasts,
            products) — only raised when
            ``metadata_only`` is False. From the
            queue's perspective both mean "terminal,
            nothing for us". ``last_error``
            distinguishes them.
        RuntimeError: scrape transient failure or
            persistence failure. Caller decides
            whether to soft-unavailable or retry.
        Other exceptions: propagated unchanged.
    '''
    scrape_start: float = time.monotonic()
    channel: YouTubeChannel = YouTubeChannel(
        channel_handle=channel_handle,
        channel_id=extra.get('channel_id'),
        deno_path=DENO_PATH,
        po_token_url=PO_TOKEN_URL, debug=True,
        save_dir=settings.channel_data_directory,
        with_download_client=False,
    )
    try:
        scrape_proxy: str | None = (
            await _try_scrape_channel_typed(
                channel, settings, extra,
                metadata_only=metadata_only,
            )
        )
    except ChannelNotFoundError:
        METRIC_SCRAPE_DURATION.labels(
            platform='youtube',
            scraper='channel_scraper',
            entity='channel',
            api='html',
            outcome='failure',
            worker_id=get_worker_id(),
        ).observe(time.monotonic() - scrape_start)
        raise

    scrape_proxy_ip: str = (
        extract_proxy_ip(scrape_proxy)
        if scrape_proxy else 'none'
    )
    scrape_proxy_network: str = proxy_network_for(
        scrape_proxy_ip,
    )

    if not metadata_only and _channel_has_no_content(
        channel, scrape_proxy_ip,
        scrape_proxy_network, scrape_proxy,
        channel_handle,
    ):
        raise ChannelNotFoundError(
            f'channel {channel_handle!r} scraped but '
            f'has no content',
        )

    if not await _persist_scraped_channel(
        fm, filename, channel, channel_handle,
    ):
        raise RuntimeError(
            f'failed to persist scraped channel '
            f'{channel_handle!r}',
        )

    success_api: str = (
        'html' if channel.about_page_succeeded
        else 'innertube'
    )
    METRIC_CHANNELS_SCRAPED.labels(
        platform='youtube',
        scraper='channel_scraper',
        entity='channel',
        api=success_api,
        worker_id=get_worker_id(),
        proxy_ip=scrape_proxy_ip,
        proxy_file=proxy_file_label(
            scrape_proxy or '',
        ),
    ).inc()
    METRIC_SCRAPE_DURATION.labels(
        platform='youtube',
        scraper='channel_scraper',
        entity='channel',
        api=success_api,
        outcome='success',
        worker_id=get_worker_id(),
    ).observe(time.monotonic() - scrape_start)
    logging.info(
        'Downloaded channel',
        extra={
            'channel_handle': channel_handle,
            'proxy_ip': scrape_proxy_ip,
            'proxy_network': scrape_proxy_network,
        },
    )
    return channel


async def scrape_channel(
    settings: ChannelSettings,
    fm: AssetFileManagement,
    channel_handle: str,
    creator_map_backend: CreatorMap,
) -> bool:
    '''
    Scrapes a single YouTube channel and stores it on disk.

    :param settings: Tool settings.
    :param fm: AssetFileManagement instance owning the channel data directory.
    :param channel_handle: The name of the YouTube channel to scrape.
    :param creator_map_backend: Shared CreatorMap for
        channel_id → handle persistence.
    :returns: whether channel scraping failed
    :raises: (none)
    '''

    extra: dict[str, str] = {'channel_handle': channel_handle}
    logging.debug('Processing channel', extra=extra)
    filename: str = get_channel_filename(channel_handle)
    extra['filename'] = filename
    base_path: Path = fm.base_dir / filename

    if await _skip_due_to_existing_state(fm, filename, base_path, extra):
        return False

    if not base_path.exists():
        proceed: bool
        early: bool | None
        proceed, early, _ = (
            await _scrape_channel_to_disk(
                settings, fm, channel_handle, filename,
                creator_map_backend, extra,
            )
        )
        if not proceed:
            return bool(early)
    return False


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
    Parse one line from the channel list file. Delegates form
    recognition to
    :func:`scrape_exchange.channel_list_parsing.parse_channel_handle`
    so the recognised URL forms, JSONL handling, comma-form
    direction, and case-insensitive dedup match
    ``cleanup_channel_list.py``. The only logic kept here is the
    cross-reference against this scraper's CreatorMap snapshot
    and ``.unresolved`` markers — :func:`_resolve_known_channel_id`
    decides whether a bare channel ID is already known, queued
    for resolution, or previously failed.

    :returns: a ``(channel_handle, unresolved_channel_id)`` tuple.
        Either or both may be ``None``. An ``unresolved_channel_id``
        indicates a channel ID for which no mapping is known yet and
        which the caller should submit to the resolution step.
    '''

    handle: str | None
    channel_id: str | None
    handle, channel_id = parse_channel_handle(raw_line)
    if handle is not None:
        return handle, None
    if channel_id is not None:
        return _resolve_known_channel_id(
            channel_id, channel_map_data, fm,
        )
    return None, None


def _load_uploaded_channel_handles(path: str | None) -> set[str]:
    '''Return uploaded channel handles from *path*, or empty set
    if the file is unset or missing.'''
    if not path:
        return set()
    list_path: Path = Path(path)
    if not list_path.exists():
        return set()
    try:
        with list_path.open('r', encoding='utf-8') as f:
            return {
                line.strip()
                for line in f
                if line.strip()
            }
    except OSError as exc:
        logging.warning(
            'Failed to read uploaded channels list; '
            'continuing with empty set',
            exc=exc,
            extra={'uploaded_channels_list': str(list_path)},
        )
        return set()


async def _filter_unscraped_candidates(
    channel_handles: set[str], fm: AssetFileManagement,
    uploaded_handles: set[str],
) -> list[str]:
    '''
    Drop handles we already have local data for. A handle is skipped
    when any of these exist on disk: a ``.not_found`` marker, a
    scraped file in ``base_dir``, or an uploaded file in
    ``uploaded_dir``. Handles present in *uploaded_handles* (loaded
    from ``YOUTUBE_UPLOADED_CHANNELS_LIST``) are also skipped so we
    do not re-scrape work that has already been uploaded; if such a
    handle still has a non-RSS scraped file sitting in ``base_dir``
    it is moved into ``uploaded_dir`` first so the uploader does not
    waste a round-trip on it.
    '''

    candidates: list[str] = []
    for channel_handle in channel_handles:
        if not channel_handle:
            continue
        filename: str = get_channel_filename(channel_handle)
        scraped_path: Path = fm.base_dir / filename
        if channel_handle in uploaded_handles:
            if (
                scraped_path.exists()
                and not filename.startswith('channel-rss-')
            ):
                try:
                    await fm.mark_uploaded(filename)
                    logging.info(
                        'Channel already uploaded; moved scraped '
                        'file to uploaded directory',
                        extra={
                            'channel_handle': channel_handle,
                            'filename': filename,
                        },
                    )
                except OSError as exc:
                    logging.warning(
                        'Failed to move scraped channel file to '
                        'uploaded directory',
                        exc=exc,
                        extra={
                            'channel_handle': channel_handle,
                            'filename': filename,
                        },
                    )
            else:
                logging.debug(
                    'Skipping channel; already in uploaded '
                    'channels list',
                    extra={'channel_handle': channel_handle},
                )
            continue
        not_found_path: Path = fm.marker_path(
            f'{CHANNEL_FILE_PREFIX}{channel_handle}', '.not_found',
        )
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


def _select_new_channels(
    candidates: list[str],
    max_new_channels: int, already_resolved_count: int,
) -> set[str]:
    '''
    Select up to the per-run scrape budget when no Redis
    ``youtube:exchange_channels`` set is available.
    '''

    remaining: int = max(max_new_channels - already_resolved_count, 0)
    selected: set[str] = set(candidates[:remaining])
    if len(selected) >= remaining:
        logging.info(
            'Reached maximum new channels to scrape, stopping read',
            extra={'max_new_channels': max_new_channels},
        )
    return selected


async def _select_new_channels_via_set(
    candidates: list[str],
    exchange_channels: RedisExchangeChannelsSet,
    max_new_channels: int,
    already_resolved_count: int,
) -> set[str]:
    '''Same contract as :func:`_select_new_channels` but uses
    a single batched ``SISMEMBER`` call.'''
    capped: list[str] = candidates[:max_new_channels]
    membership: dict[str, bool] = (
        await exchange_channels.contains_many(capped)
    )
    selected: set[str] = set()
    for handle in capped:
        if membership.get(handle, False):
            METRIC_CHANNEL_EXCHANGE_SET_LOOKUP.labels(
                platform='youtube',
                scraper='channel_scraper',
                entity='channel',
                outcome='hit',
                worker_id=get_worker_id(),
            ).inc()
            continue
        METRIC_CHANNEL_EXCHANGE_SET_LOOKUP.labels(
            platform='youtube',
            scraper='channel_scraper',
            entity='channel',
            outcome='miss',
            worker_id=get_worker_id(),
        ).inc()
        selected.add(handle)
        if (
            len(selected) + already_resolved_count
        ) >= max_new_channels:
            logging.info(
                'Reached maximum new channels to scrape, '
                'stopping read',
                extra={
                    'max_new_channels': max_new_channels,
                },
            )
            break
    return selected


async def _read_channel_list_file(
    file_path: str,
) -> tuple[list[str], list[str]]:
    '''
    Read the channel list file once and split it into ``(header,
    entries)`` where *header* is the run of leading comment /
    blank lines (preserved verbatim, without the trailing
    newline) and *entries* is every other non-blank, non-comment
    line stripped of whitespace.

    The header is preserved separately so the write-back step
    can re-emit it unchanged when the deduplicated entry list is
    persisted, matching the layout convention used by
    :mod:`tools.cleanup_channel_list`.
    '''
    header: list[str] = []
    entries: list[str] = []
    in_header: bool = True

    line: str
    async with aiofiles.open(file_path, 'r') as file_desc:
        async for line in file_desc:
            stripped: str = line.strip()
            if not stripped or stripped.startswith('#'):
                if in_header:
                    header.append(line.rstrip('\n'))
                continue
            in_header = False
            entries.append(stripped)

    return header, entries


async def _persist_deduped_channel_list(
    file_path: str,
    header: list[str],
    deduped: list[str],
) -> None:
    '''
    Write *deduped* back to *file_path*, preserving *header*
    verbatim. Backs up the original to ``<file_path>.bak``
    before writing so an operator can recover the prior content
    if the dedup ever drops something it shouldn't. All errors
    are logged at warning and swallowed — failure to write back
    must never abort the scraper, since the in-memory dedup is
    already correct.

    Pattern matches :mod:`tools.cleanup_channel_list`'s
    write-back so an operator running both gets consistent
    on-disk shape.
    '''
    list_path: Path = Path(file_path)
    backup: Path = list_path.with_suffix(
        list_path.suffix + '.bak',
    )
    try:
        shutil.copy2(list_path, backup)
    except OSError as exc:
        logging.warning(
            'Failed to back up channel list before dedup '
            'write; skipping write to avoid data loss',
            exc=exc,
            extra={'file_path': file_path},
        )
        return

    body: str = '\n'.join(deduped) + '\n'
    if header:
        body = '\n'.join(header) + '\n' + body
    payload: bytes = body.encode('utf-8')
    try:
        await atomic_write_bytes(list_path, payload)
    except OSError as exc:
        logging.warning(
            'Failed to write deduplicated channel list back to '
            'disk; in-memory dedup is still applied',
            exc=exc,
            extra={'file_path': file_path},
        )
        return
    logging.info(
        'Wrote deduplicated channel list back to disk',
        extra={
            'file_path': file_path,
            'entries': len(deduped),
            'backup': str(backup),
        },
    )


async def read_channels(
    file_path: str, creator_map_backend: CreatorMap,
    fm: AssetFileManagement,
    max_new_channels: int, max_resolved_channels: int,
    uploaded_handles: set[str],
    concurrency: int = 3,
    identity_store: ChannelIdentityStore | None = None,
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

    # Read every line first so we can run the same case-insensitive
    # dedup the cleanup tool uses (``dedupe_preserving_case``) before
    # per-line parsing. This keeps the scraper from queuing both
    # ``MyChannel`` and ``mychannel`` as separate candidates, and
    # makes URL / JSONL / comma-form variants for the same channel
    # collapse to one entry — matching ``cleanup_channel_list.py``.
    # The leading-comment header is captured separately so the
    # write-back step below preserves it verbatim.
    header: list[str]
    raw_entries: list[str]
    header, raw_entries = await _read_channel_list_file(file_path)

    deduped: list[str] = dedupe_preserving_case(raw_entries)
    duplicates_dropped: int = len(raw_entries) - len(deduped)
    if duplicates_dropped:
        logging.info(
            'Collapsed lower-case duplicates from channel list',
            extra={
                'raw_lines': len(raw_entries),
                'deduped_lines': len(deduped),
                'dropped': duplicates_dropped,
            },
        )
        await _persist_deduped_channel_list(
            file_path, header, deduped,
        )

    for entry in deduped:
        channel_handle: str | None
        unresolved_id: str | None
        channel_handle, unresolved_id = _parse_channel_line(
            entry, channel_map_data, fm,
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
            platform='youtube',
            scraper='channel_scraper',
            entity='channel',
            worker_id=get_worker_id(),
        ).set(len(unresolved_ids))
        resolve_claim: RedisClaim | None = None
        redis_client = creator_map_backend.redis_client
        if redis_client is not None:
            resolve_claim = RedisClaim(
                redis_client=redis_client,
                key_prefix='claim:youtube:channel:',
                ttl_seconds=60,
                owner=get_worker_id(),
            )
        resolved_channels = await review_unresolved_ids(
            unresolved_ids, creator_map_backend, fm,
            concurrency, max_resolved_channels,
            claim=resolve_claim,
            identity_store=identity_store,
        )
        new_channel_handles.update(resolved_channels)

    candidates: list[str] = await _filter_unscraped_candidates(
        new_channel_handles, fm, uploaded_handles,
    )

    logging.info(
        'Checking channel handles against local/Redis state',
        extra={'max_new_channels': max_new_channels},
    )
    checked_channel_handles: set[str]
    redis_client_for_set: aioredis.Redis | None = (
        creator_map_backend.redis_client
    )
    if redis_client_for_set is not None:
        exchange_set: RedisExchangeChannelsSet = (
            RedisExchangeChannelsSet(redis_client_for_set)
        )
        checked_channel_handles = (
            await _select_new_channels_via_set(
                candidates, exchange_set,
                max_new_channels, len(resolved_channels),
            )
        )
    else:
        checked_channel_handles = (
            _select_new_channels(
                candidates,
                max_new_channels,
                len(resolved_channels),
            )
        )

    METRIC_UNIQUE_CHANNELS_READ.labels(
        platform='youtube',
        scraper='channel_scraper',
        entity='channel',
        worker_id=get_worker_id(),
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


async def _innertube_resolve(
    channel_id: str,
    creator_map_backend: CreatorMap,
    fm: AssetFileManagement,
    identity_store: ChannelIdentityStore | None = None,
) -> str | None:
    '''Call YouTube InnerTube and update creator_map.

    Returns the resolved handle, or None on any failure.
    Owns all metric increments for the resolved/failed
    outcomes.
    '''
    try:
        name: str = (
            await YouTubeChannel.resolve_channel_id(channel_id)
        )
    except Exception as e:
        METRIC_CHANNEL_ID_RESOLUTION_FAILURES.labels(
            platform='youtube',
            scraper='channel_scraper',
            entity='channel',
            outcome='failed',
            worker_id=get_worker_id(),
        ).inc()
        logging.debug(
            'Error while resolving channel ID',
            exc=e,
            extra={'channel_id': channel_id},
        )
        return None

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
            platform='youtube',
            scraper='channel_scraper',
            entity='channel',
            outcome='failed',
            worker_id=get_worker_id(),
        ).inc()
        return None

    if ' ' in name:
        logging.info(
            'Resolved channel ID to name with spaces; '
            'marking unresolved to avoid re-querying',
            extra={'channel_id': channel_id, 'name': name},
        )
        await fm.mark_unresolved(
            f'{CHANNEL_FILE_PREFIX}{channel_id}',
            content=f'{channel_id}\t{name}\n',
        )
        METRIC_CHANNEL_ID_RESOLUTION_FAILURES.labels(
            platform='youtube',
            scraper='channel_scraper',
            entity='channel',
            outcome='failed',
            worker_id=get_worker_id(),
        ).inc()
        return None

    if identity_store is not None:
        await identity_store.bind(channel_id, name)
    else:
        await creator_map_backend.put(channel_id, name)
    logging.debug(
        'Resolved channel ID to name',
        extra={'channel_id': channel_id, 'name': name},
    )
    METRIC_CHANNEL_IDS_RESOLVED.labels(
        platform='youtube',
        scraper='channel_scraper',
        entity='channel',
        outcome='resolved',
        worker_id=get_worker_id(),
    ).inc()
    return name


async def review_unresolved_ids(
    unresolved_ids: set[str],
    creator_map_backend: CreatorMap,
    fm: AssetFileManagement,
    concurrency: int, max_resolved_channels: int,
    claim: RedisClaim | None = None,
    identity_store: ChannelIdentityStore | None = None,
) -> set[str]:
    '''
    See if we can resolve a channel ID to a channel handle

    :param unresolved_ids: Set of channel IDs that need to be
    resolved to channel names.
    :param creator_map_backend: Backend for storing resolved
    channel ID-name pairs.
    :param fm: AssetFileManagement instance owning the channel
    data directory.
    :param concurrency: Number of channel ID resolutions to run
    concurrently.
    :param max_resolved_channels: Cap on how many ids to resolve
    per call.
    :param claim: Optional RedisClaim for cross-fleet per-id
    deduplication. When set, only one worker per id fires an
    InnerTube call.
    :returns: Set of resolved channel names corresponding to the
    input channel IDs.
    :raises: (none)
    '''

    semaphore: asyncio.Semaphore = asyncio.Semaphore(concurrency)
    resolved_channel_names: set[str] = set()

    async def resolve(channel_id: str) -> str | None:
        async with semaphore:
            # Per-id recheck: a peer worker may have resolved
            # this id since the snapshot in read_channels was
            # taken. Cheap HGET avoids a redundant browse call.
            existing: str | None = (
                await creator_map_backend.get(channel_id)
            )
            if existing:
                METRIC_CHANNEL_IDS_RESOLVED.labels(
                    platform='youtube',
                    scraper='channel_scraper',
                    entity='channel',
                    outcome='resolved_by_peer',
                    worker_id=get_worker_id(),
                ).inc()
                return existing

            # Cross-fleet claim: if a peer wins, we skip and
            # rely on the next read_channels pass to pick up
            # the resolved handle from creator_map.
            if claim is not None:
                won: bool = await claim.try_claim(channel_id)
                if not won:
                    METRIC_CHANNEL_RESOLVE_CLAIM.labels(
                        platform='youtube',
                        scraper='channel_scraper',
                        entity='channel',
                        outcome='lost',
                        worker_id=get_worker_id(),
                    ).inc()
                    return None
                METRIC_CHANNEL_RESOLVE_CLAIM.labels(
                    platform='youtube',
                    scraper='channel_scraper',
                    entity='channel',
                    outcome='won',
                    worker_id=get_worker_id(),
                ).inc()

            try:
                return await _innertube_resolve(
                    channel_id,
                    creator_map_backend,
                    fm,
                    identity_store=identity_store,
                )
            finally:
                if claim is not None:
                    await claim.release(channel_id)

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
        if name is not None:
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
