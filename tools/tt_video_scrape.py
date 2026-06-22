#!/usr/bin/env python3

'''
TikTok video scrape daemon.

Consumes TikTok video URLs from the platform-scoped Redis video queue,
fetches one item payload per URL through the Camoufox-backed TikTok
session pool, maps it to ``TikTokVideo``, and writes Brotli JSON
records to disk.

Design: docs/superpowers/specs/
2026-06-10-tiktok-video-scrape-tool-design.md

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import logging
import os
import re
import resource
import sys
import time
from typing import Any, Awaitable, Callable, TypeVar

from pydantic import AliasChoices, Field, field_validator
from redis import exceptions as redis_exceptions

from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.proxy_loader import proxy_file_label
from scrape_exchange.redis_client import redis_from_url
from scrape_exchange.scraper_metrics import (
    METRIC_SCRAPE_DURATION,
    METRIC_SCRAPE_FAILURES,
    METRIC_SCRAPE_QUEUE_SIZE,
    METRIC_SCRAPE_RECORDS_WRITTEN,
    METRIC_SCRAPE_RETRIES,
    METRIC_SCRAPES_COMPLETED,
)
from scrape_exchange.scraper_runner import (
    ScraperRunContext,
    ScraperRunner,
)
from scrape_exchange.scraper_supervisor import (
    distribute_total_concurrency,
    random_proxy_subset,
)
from scrape_exchange.settings import (
    ScraperSettings,
    normalize_log_level,
)
from scrape_exchange.tiktok import (
    TikTokRateLimiter,
    TikTokSessionPool,
    TikTokVideo,
    TikTokCallType,
    classify_tiktok_error,
)
from scrape_exchange.tiktok.settings import TikTokScraperSettings
from scrape_exchange.tiktok.tiktok_session_pool import DIRECT_SESSION_PROXY
from scrape_exchange.util import extract_proxy_ip
from scrape_exchange.video_scrape_queue import (
    RedisVideoScrapeQueue,
    VideoScrapeQueueSettings,
    VideoState,
)
from scrape_exchange.watchdog import Watchdog
from scrape_exchange.worker_id import get_worker_id


_LOGGER: logging.Logger = logging.getLogger(__name__)

SCRAPER_LABEL: str = 'tiktok_video'
PLATFORM: str = 'tiktok'
ENTITY: str = 'video'
API_LABEL: str = 'tiktokapi'

VIDEO_FILE_PREFIX: str = 'tiktok-video-'
VIDEO_FILE_POSTFIX: str = '.json.br'
VIDEO_URL_RE: re.Pattern[str] = re.compile(
    r'https?://(?:www\.)?tiktok\.com/@[^/]+/video/([^/?#]+)'
)

_FD_TARGET: int = 1_048_576
_REDIS_RETRY_INITIAL_SECONDS: float = 1.0
_REDIS_RETRY_MAX_SECONDS: float = 60.0
_REDIS_RETRY_ERRORS: tuple[type[BaseException], ...] = (
    redis_exceptions.ConnectionError,
    redis_exceptions.TimeoutError,
)

_T = TypeVar('_T')


class VideoSettings(TikTokScraperSettings):
    '''Configuration for ``tt_video_scrape.py``.'''

    metrics_port: int = Field(
        default=9700,
        validation_alias=AliasChoices(
            'TIKTOK_VIDEO_METRICS_PORT', 'metrics_port',
        ),
        description='Prometheus metrics port.',
    )
    video_concurrency: int = Field(
        default=0,
        validation_alias=AliasChoices(
            'TIKTOK_VIDEO_CONCURRENCY',
            'TIKTOP_VIDEO_CONCURRENCY',
            'video_concurrency',
        ),
        description=(
            'Fleet-wide async worker upper bound. 0 means use proxy '
            'count. In multi-process mode this budget is split across '
            'child processes.'
        ),
    )
    video_num_processes: int = Field(
        default=1,
        validation_alias=AliasChoices(
            'TIKTOK_VIDEO_NUM_PROCESSES',
            'video_num_processes',
        ),
        description='Supervisor child count.',
    )
    video_log_file: str = Field(
        default='/dev/stdout',
        validation_alias=AliasChoices(
            'TIKTOK_VIDEO_LOG_FILE', 'video_log_file',
            'VIDEO_LOG_FILE', 'LOG_FILE', 'log_file',
        ),
        description='Log file path.',
    )
    video_log_level: str = Field(
        default='INFO',
        validation_alias=AliasChoices(
            'TIKTOK_VIDEO_LOG_LEVEL', 'video_log_level',
            'VIDEO_LOG_LEVEL', 'LOG_LEVEL', 'log_level',
        ),
        description='Logging level.',
    )
    video_queue_batch: int = Field(
        default=50,
        validation_alias=AliasChoices(
            'TIKTOK_VIDEO_QUEUE_BATCH', 'VIDEO_QUEUE_BATCH',
            'video_queue_batch',
        ),
        description='Number of video URLs popped per Redis batch.',
    )
    video_queue_idle_poll_seconds: float = Field(
        default=2.0,
        validation_alias=AliasChoices(
            'TIKTOK_VIDEO_QUEUE_IDLE_POLL_SECONDS',
            'VIDEO_QUEUE_IDLE_POLL_SECONDS',
            'video_queue_idle_poll_seconds',
        ),
        description='Sleep between empty queue polls.',
    )
    video_transient_max_attempts: int = Field(
        default=3,
        validation_alias=AliasChoices(
            'TIKTOK_VIDEO_TRANSIENT_MAX_ATTEMPTS',
            'VIDEO_TRANSIENT_MAX_ATTEMPTS',
            'video_transient_max_attempts',
        ),
        description='Transient attempts before FAILED.',
    )
    video_transient_backoff_seconds: int = Field(
        default=30,
        validation_alias=AliasChoices(
            'TIKTOK_VIDEO_TRANSIENT_BACKOFF_SECONDS',
            'VIDEO_TRANSIENT_BACKOFF_SECONDS',
            'video_transient_backoff_seconds',
        ),
        description='Backoff between transient attempts.',
    )

    @field_validator('video_log_level', mode='before')
    @classmethod
    def _normalize_video_log_level(cls, value: str) -> str:
        return normalize_log_level(value)


def _validate_settings(settings: VideoSettings) -> None:
    if not settings.video_data_directory:
        print(
            'Error: scraped video data directory must be set via '
            '--tiktok-video-data-dir or TIKTOK_VIDEO_DATA_DIR',
            file=sys.stderr,
        )
        raise SystemExit(1)
    if not settings.redis_dsn:
        print(
            'Error: Redis must be configured via --redis-dsn or '
            'REDIS_DSN',
            file=sys.stderr,
        )
        raise SystemExit(1)
    if not os.path.isdir(settings.video_data_directory):
        print(
            f'Directory {settings.video_data_directory} does not '
            'exist. It will be created.',
            file=sys.stderr,
        )
        os.makedirs(settings.video_data_directory, exist_ok=True)


def _video_filename(video_id: str) -> str:
    return f'{VIDEO_FILE_PREFIX}{video_id}{VIDEO_FILE_POSTFIX}'


def _proxy_label(proxy: str | None) -> str:
    if not proxy:
        return 'none'
    if proxy == DIRECT_SESSION_PROXY:
        return 'direct'
    try:
        return extract_proxy_ip(proxy)
    except ValueError:
        return 'unknown'


def _proxy_file_label(proxy: str | None) -> str:
    if not proxy:
        return 'none'
    if proxy == DIRECT_SESSION_PROXY:
        return 'direct'
    return proxy_file_label(proxy)


async def _redis_operation_with_backoff(
    operation: str,
    call: Callable[[], Awaitable[_T]],
) -> _T:
    sleep_seconds: float = _REDIS_RETRY_INITIAL_SECONDS
    while True:
        try:
            return await call()
        except _REDIS_RETRY_ERRORS:
            _LOGGER.warning(
                'Redis operation failed; retrying',
                extra={
                    'operation': operation,
                    'sleep_seconds': sleep_seconds,
                },
                exc_info=True,
            )
            await asyncio.sleep(sleep_seconds)
            sleep_seconds = min(
                sleep_seconds * 2.0,
                _REDIS_RETRY_MAX_SECONDS,
            )


def _unwrap_video_payload(resp: dict) -> dict:
    '''Return an item payload from common TikTok response wrappers.'''
    if isinstance(resp.get('itemInfo'), dict):
        item_struct: object = resp['itemInfo'].get('itemStruct')
        if isinstance(item_struct, dict):
            return item_struct
    item_struct = resp.get('itemStruct')
    if isinstance(item_struct, dict):
        return item_struct
    item = resp.get('item')
    if isinstance(item, dict):
        return item
    if (
        'id' in resp
        and isinstance(resp.get('author'), dict)
    ):
        return resp
    raise RuntimeError('TikTok item detail response has no item')


def _video_id_from_ref(video_ref: str) -> str:
    '''Return the item id from a queued TikTok video URL.'''
    match: re.Match[str] | None = VIDEO_URL_RE.match(video_ref)
    if not match:
        raise ValueError('TikTok video queue entries must be URLs')
    return match.group(1)


async def _fetch_video_payload(api: Any, video_ref: str) -> dict:
    '''Fetch one TikTok item payload by queued video URL.'''
    _video_id_from_ref(video_ref)
    video_obj: Any = api.video(url=video_ref)
    resp: dict | None = await video_obj.info()
    if resp is None:
        raise RuntimeError('TikTok returned an empty video info')
    return _unwrap_video_payload(resp)


def _build_queue(settings: VideoSettings) -> RedisVideoScrapeQueue:
    redis = redis_from_url(
        settings.redis_dsn,
        component='tiktok-video-queue',
        decode_responses=True,
    )
    queue_settings: VideoScrapeQueueSettings = (
        VideoScrapeQueueSettings(
            video_queue_batch=settings.video_queue_batch,
            video_queue_idle_poll_seconds=(
                settings.video_queue_idle_poll_seconds
            ),
            video_transient_max_attempts=(
                settings.video_transient_max_attempts
            ),
            video_transient_backoff_seconds=(
                settings.video_transient_backoff_seconds
            ),
        )
    )
    return RedisVideoScrapeQueue(
        redis, queue_settings, platform=PLATFORM,
    )


async def _scrape_to_disk(
    video_ref: str,
    *,
    api: Any,
    fm: AssetFileManagement,
) -> TikTokVideo:
    payload: dict = await _fetch_video_payload(api, video_ref)
    video: TikTokVideo = TikTokVideo.from_api(payload)
    await fm.write_file(_video_filename(video.video_id), video.to_dict())
    METRIC_SCRAPE_RECORDS_WRITTEN.labels(
        platform=PLATFORM,
        scraper=SCRAPER_LABEL,
        entity=ENTITY,
    ).inc()
    return video


_TRANSIENT_REASONS: frozenset[str] = frozenset({
    'transient', 'rate_limit', 'auth',
})
_UNAVAILABLE_REASONS: frozenset[str] = frozenset({
    'unavailable',
})


async def _scrape_one_queued(
    video_ref: str,
    *,
    queue: RedisVideoScrapeQueue,
    settings: VideoSettings,
    pool: TikTokSessionPool,
    proxy: str,
    fm: AssetFileManagement,
) -> None:
    attempts_left: int = settings.video_transient_max_attempts
    last_reason: str = 'other'
    worker_id: str = get_worker_id()
    proxy_ip: str = _proxy_label(proxy)
    proxy_file: str = _proxy_file_label(proxy)
    while attempts_left > 0:
        started: float = time.monotonic()
        try:
            async with pool.session_for(proxy) as api:
                await _scrape_to_disk(video_ref, api=api, fm=fm)
        except Exception as exc:
            reason: str = classify_tiktok_error(exc)
            last_reason = reason
            METRIC_SCRAPE_FAILURES.labels(
                platform=PLATFORM,
                scraper=SCRAPER_LABEL,
                entity=ENTITY,
                api=API_LABEL,
                reason=reason,
                worker_id=worker_id,
                proxy_ip=proxy_ip,
                proxy_file=proxy_file,
            ).inc()
            METRIC_SCRAPE_DURATION.labels(
                platform=PLATFORM,
                scraper=SCRAPER_LABEL,
                entity=ENTITY,
                api=API_LABEL,
                outcome='failure',
                worker_id=worker_id,
            ).observe(time.monotonic() - started)
            if reason in _UNAVAILABLE_REASONS:
                await queue.mark(
                    video_ref,
                    state=VideoState.UNAVAILABLE,
                    last_error=reason,
                )
                return
            if reason in _TRANSIENT_REASONS:
                attempts_left -= 1
                await queue.bump_attempts(
                    video_ref, last_error=reason,
                )
                METRIC_SCRAPE_RETRIES.labels(
                    platform=PLATFORM,
                    scraper=SCRAPER_LABEL,
                    entity=ENTITY,
                    api=API_LABEL,
                    reason=reason,
                ).inc()
                if attempts_left > 0:
                    await asyncio.sleep(
                        settings.video_transient_backoff_seconds,
                    )
                    continue
                break
            await queue.mark(
                video_ref,
                state=VideoState.FAILED,
                last_error=reason,
            )
            return
        else:
            await queue.complete(video_ref)
            METRIC_SCRAPES_COMPLETED.labels(
                platform=PLATFORM,
                scraper=SCRAPER_LABEL,
                entity=ENTITY,
                api=API_LABEL,
                worker_id=worker_id,
                proxy_ip=proxy_ip,
                proxy_file=proxy_file,
            ).inc()
            METRIC_SCRAPE_DURATION.labels(
                platform=PLATFORM,
                scraper=SCRAPER_LABEL,
                entity=ENTITY,
                api=API_LABEL,
                outcome='success',
                worker_id=worker_id,
            ).observe(time.monotonic() - started)
            return
    await queue.mark(
        video_ref,
        state=VideoState.FAILED,
        last_error=f'transient retries exhausted: {last_reason}',
    )


async def _queue_driven_loop(
    queue: RedisVideoScrapeQueue,
    settings: VideoSettings,
    pool: TikTokSessionPool,
    proxies: list[str],
    fm: AssetFileManagement,
    concurrency: int,
) -> None:
    if concurrency < 1:
        raise ValueError('concurrency must be >= 1')
    inflight: asyncio.Queue[str] = asyncio.Queue(
        maxsize=max(settings.video_queue_batch, concurrency),
    )

    async def producer() -> None:
        while True:
            video_refs: list[str] = await _redis_operation_with_backoff(
                'tiktok video queue pop',
                lambda: queue.pop(settings.video_queue_batch),
            )
            if not video_refs:
                Watchdog.get().touch_work()
                await asyncio.sleep(
                    settings.video_queue_idle_poll_seconds,
                )
                continue
            for video_ref in video_refs:
                await inflight.put(video_ref)

    async def consumer(index: int) -> None:
        proxy: str = proxies[index % len(proxies)]
        while True:
            video_ref: str = await inflight.get()
            Watchdog.get().touch_work()
            try:
                await _scrape_one_queued(
                    video_ref,
                    queue=queue,
                    settings=settings,
                    pool=pool,
                    proxy=proxy,
                    fm=fm,
                )
            except Exception:
                _LOGGER.exception(
                    'Unexpected error scraping TikTok video',
                    extra={'video_ref': video_ref},
                )
            finally:
                inflight.task_done()

    tasks: list[asyncio.Task[None]] = [
        asyncio.create_task(
            producer(), name='tiktok-video-queue-producer',
        ),
        *[
            asyncio.create_task(
                consumer(i),
                name=f'tiktok-video-queue-consumer-{i}',
            )
            for i in range(concurrency)
        ],
    ]
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def _publish_video_queue_sizes(
    queue: RedisVideoScrapeQueue,
    *,
    interval: float = 30.0,
) -> None:
    while True:
        try:
            counts: dict[VideoState, int] = (
                await queue.count_by_state()
            )
            for state, count in counts.items():
                METRIC_SCRAPE_QUEUE_SIZE.labels(
                    platform=PLATFORM,
                    scraper=SCRAPER_LABEL,
                    entity=ENTITY,
                    state=state.value,
                    worker_id='',
                ).set(count)
        except Exception:
            _LOGGER.warning(
                'TikTok video queue metrics publish failed',
                exc_info=True,
            )
        await asyncio.sleep(interval)


def _raise_fd_limit() -> None:
    hard: int
    _, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    target: int = (
        hard if hard != resource.RLIM_INFINITY else _FD_TARGET
    )
    resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))


async def _run_worker(ctx: ScraperRunContext) -> None:
    _raise_fd_limit()
    settings: VideoSettings = ctx.settings  # type: ignore[assignment]
    worker_id: str = settings.worker_id or get_worker_id()
    fm: AssetFileManagement = AssetFileManagement(
        settings.video_data_directory,
        prefix_rankings={'video': [VIDEO_FILE_PREFIX]},
    )
    proxy_limit: int = max(
        1,
        min(
            _resolve_video_concurrency(settings, len(ctx.proxies)),
            len(ctx.proxies),
        ),
    )
    pool_proxies: list[str] = ctx.proxies[:proxy_limit]
    pool: TikTokSessionPool = TikTokSessionPool(
        proxies=pool_proxies,
        state_dir=settings.session_state_dir,
        ms_token_ttl_seconds=settings.ms_token_ttl_seconds,
        rate_limiter=ctx.rate_limiter,
        scraper_label=SCRAPER_LABEL,
        api_call_type=TikTokCallType.VIDEO_API,
        worker_id=worker_id,
        refresh_fraction=settings.ms_token_refresh_fraction,
        bootstrap_timeout_ms=settings.session_bootstrap_timeout_ms,
    )
    await pool.bootstrap()
    ready: list[str] = pool.ready_proxies()
    if not ready:
        _LOGGER.error(
            'No proxies bootstrapped; nothing to scrape',
            extra={
                'requested': len(ctx.proxies),
                'selected': len(pool_proxies),
            },
        )
        await pool.shutdown()
        return

    queue: RedisVideoScrapeQueue = _build_queue(settings)
    configured: int = settings.video_concurrency
    concurrency: int = configured if configured > 0 else len(ready)
    concurrency = max(1, min(concurrency, len(ready)))

    refresh_task: asyncio.Task = asyncio.create_task(
        pool.run_refresh_loop(
            settings.ms_token_refresh_interval_seconds,
        ),
    )
    publisher_task: asyncio.Task | None = None
    if get_worker_id() == '1':
        publisher_task = asyncio.create_task(
            _publish_video_queue_sizes(queue),
        )
    try:
        await _queue_driven_loop(
            queue, settings, pool, ready, fm, concurrency,
        )
    finally:
        refresh_task.cancel()
        tasks: list[asyncio.Task] = [refresh_task]
        if publisher_task is not None:
            publisher_task.cancel()
            tasks.append(publisher_task)
        await asyncio.gather(*tasks, return_exceptions=True)
        await pool.shutdown()
        await queue._redis.aclose()


def _build_tiktok_rate_limiter(
    settings: ScraperSettings,
) -> TikTokRateLimiter:
    return TikTokRateLimiter(
        state_dir=settings.rate_limiter_state_dir,
        redis_dsn=settings.redis_dsn,
    )


def _resolve_video_concurrency(
    settings: VideoSettings,
    proxy_count: int,
) -> int:
    requested: int = int(settings.video_concurrency)
    if requested > 0:
        return requested
    if proxy_count > 0:
        return int(proxy_count)
    return 1


def _effective_process_count(
    requested_processes: int,
    total_concurrency: int,
    proxy_count: int,
) -> int:
    requested: int = max(int(requested_processes), 1)
    capacity: int = max(int(total_concurrency), 1)
    if proxy_count > 0:
        capacity = min(capacity, int(proxy_count))
    return max(1, min(requested, capacity))


def main() -> None:
    settings: VideoSettings = VideoSettings()
    _validate_settings(settings)
    proxy_count: int = len(settings.proxies)
    resolved_concurrency: int = _resolve_video_concurrency(
        settings, proxy_count,
    )
    if proxy_count > 0:
        resolved_concurrency = min(resolved_concurrency, proxy_count)
        object.__setattr__(
            settings, 'proxies',
            random_proxy_subset(
                list(settings.proxies), resolved_concurrency,
            ),
        )
        proxy_count = len(settings.proxies)
    process_count: int = _effective_process_count(
        settings.video_num_processes,
        resolved_concurrency,
        proxy_count,
    )
    child_concurrencies: list[int] | None = (
        distribute_total_concurrency(resolved_concurrency, process_count)
        if process_count > 1 else None
    )
    settings.video_num_processes = process_count
    settings.video_concurrency = (
        resolved_concurrency if child_concurrencies is None
        else max(child_concurrencies)
    )
    os.environ['TIKTOK_VIDEO_CONCURRENCY'] = str(resolved_concurrency)
    runner: ScraperRunner = ScraperRunner(
        settings=settings,
        scraper_label=SCRAPER_LABEL,
        platform=PLATFORM,
        num_processes=process_count,
        concurrency=resolved_concurrency,
        metrics_port=settings.metrics_port,
        log_file=settings.video_log_file,
        log_level=settings.video_log_level,
        rate_limiter_factory=_build_tiktok_rate_limiter,
        client_required=False,
        split_proxy_pool=True,
        concurrency_env_var='TIKTOK_VIDEO_CONCURRENCY',
        child_concurrencies=child_concurrencies,
    )
    sys.exit(runner.run_sync(_run_worker))


if __name__ == '__main__':
    main()
