#!/usr/bin/env python3
'''Scrape public OnlyFans creator metadata without logging in.'''

import asyncio
import logging
import signal
import time
from asyncio import sleep
from contextlib import AsyncExitStack
from typing import Any
from uuid import uuid4

from playwright.async_api import BrowserContext

from scrape_exchange.creator_queue import RedisCreatorQueue
from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.logging import configure_logging
from scrape_exchange.metrics_server import start_metrics_server
from scrape_exchange.onlyfans.onlyfans_browser import (
    ProfileBlockedError,
    anonymous_browser,
    fetch_profile,
)
from scrape_exchange.onlyfans.onlyfans_creator import (
    OnlyFansCreator,
    normalize_creator,
)
from scrape_exchange.onlyfans.onlyfans_rate_limiter import OnlyFansRateLimiter
from scrape_exchange.onlyfans.settings import (
    OnlyFansScraperSettings,
    parse_like_priority_queues,
)
from scrape_exchange.proxy_loader import proxy_file_label
from scrape_exchange.scraper_metrics import (
    METRIC_SCRAPE_DURATION,
    METRIC_SCRAPE_FAILURES,
    METRIC_SCRAPE_QUEUE_SIZE,
    METRIC_SCRAPE_RECORDS_WRITTEN,
    METRIC_SCRAPE_RETRIES,
    METRIC_SCRAPES_COMPLETED,
    METRIC_WORKER_SLEEP_SECONDS,
)
from scrape_exchange.scraper_supervisor import (
    METRIC_CONCURRENCY,
    METRIC_NUM_PROCESSES,
)
from scrape_exchange.util import extract_proxy_ip, extract_proxy_port

_LOGGER: logging.Logger = logging.getLogger(__name__)
PREFIX: str = 'onlyfans-creator-'
QUEUE_POLL_SECONDS: float = 60
LABELS: dict[str, str] = {
    'platform': 'onlyfans', 'scraper': 'onlyfans_creator', 'entity': 'creator',
}


def record_outcome(
    outcome: str, started: float, settings: OnlyFansScraperSettings,
    proxy: str | None,
) -> None:
    labels: dict[str, str] = {
        **LABELS, 'api': 'browser', 'worker_id': str(settings.worker_id),
    }
    proxy_labels: dict[str, str] = {
        'proxy_ip': extract_proxy_ip(proxy) if proxy else 'none',
        'proxy_port': extract_proxy_port(proxy) if proxy else 'none',
        'proxy_file': proxy_file_label(proxy or ''),
    }
    if outcome == 'scraped':
        METRIC_SCRAPES_COMPLETED.labels(
            **labels, **proxy_labels, channel_status='none',
        ).inc()
    elif outcome != 'claim_lost':
        METRIC_SCRAPE_FAILURES.labels(
            **labels, **proxy_labels, reason=outcome,
        ).inc()
    METRIC_SCRAPE_DURATION.labels(
        **labels,
        outcome=('success' if outcome == 'scraped' else
                 'claim_lost' if outcome == 'claim_lost' else 'failure'),
    ).observe(time.monotonic() - started)


async def worker_sleep(
    delay: float, settings: OnlyFansScraperSettings, owner: str,
) -> None:
    # Do not publish the UUID used for Redis claim ownership in labels.
    worker_id: str = f'{settings.worker_id}:{owner.rsplit(":", 1)[-1]}'
    METRIC_WORKER_SLEEP_SECONDS.labels(
        platform='onlyfans', scraper='onlyfans_creator', worker_id=worker_id,
    ).set(delay)
    try:
        await sleep(delay)
    finally:
        METRIC_WORKER_SLEEP_SECONDS.labels(
            platform='onlyfans', scraper='onlyfans_creator',
            worker_id=worker_id,
        ).set(0)


def build_queue(
    settings: OnlyFansScraperSettings, worker_id: str,
) -> RedisCreatorQueue:
    if not settings.redis_dsn:
        raise ValueError('Queue mode requires REDIS_DSN')
    queue: RedisCreatorQueue = RedisCreatorQueue(
        settings.redis_dsn, worker_id, 'onlyfans', key_namespace='scrape',
    )
    queue._tiers = parse_like_priority_queues(settings.creator_priority_queues)
    queue._key_queues = queue._build_queue_keys(queue._tiers)
    return queue


async def _owns_claim(
    queue: RedisCreatorQueue, username: str, owner: str,
) -> bool:
    return await queue._redis.get(
        f'{queue._claim_prefix}{username}',
    ) == owner


async def _retry_creator(
    username: str, queue: RedisCreatorQueue, settings: OnlyFansScraperSettings,
    owner: str, exc: Exception,
) -> str:
    if not await _owns_claim(queue, username, owner):
        return 'claim_lost'
    reason: str = (
        'rate_limit' if isinstance(exc, ProfileBlockedError) else 'failed'
    )
    await queue.record_scrape_failure(
        username, status=reason, error=type(exc).__name__, worker_id=owner,
    )
    delay: float = settings.creator_retry_interval_seconds
    if reason == 'rate_limit':
        delay = max(delay, settings.blocked_cooldown_seconds)
    if not await _owns_claim(queue, username, owner):
        return 'claim_lost'
    await queue.reschedule_in(username, delay)
    METRIC_SCRAPE_RETRIES.labels(**LABELS, api='browser', reason=reason).inc()
    _LOGGER.warning(
        f'Creator @{username}: {reason}; retry in {delay}s',
        extra={**LABELS, 'username': username, 'reason': reason,
               'retry_seconds': delay, 'error_type': type(exc).__name__,
               'worker_id': str(settings.worker_id)},
    )
    return reason


async def process_queued_creator(
    username: str, context: BrowserContext, queue: RedisCreatorQueue,
    fm: AssetFileManagement, settings: OnlyFansScraperSettings,
    limiter: OnlyFansRateLimiter, proxy: str | None, owner: str,
) -> str:
    if not await _owns_claim(queue, username, owner):
        return 'claim_lost'
    try:
        async with asyncio.timeout(settings.profile_timeout_seconds + 30):
            await queue.record_scrape_attempt(username, worker_id=owner)
            creator: OnlyFansCreator = await fetch_profile(
                context, username, limiter, proxy, settings,
            )
            if not await _owns_claim(queue, username, owner):
                return 'claim_lost'
            await save_creator(creator, fm)
            if not await _owns_claim(queue, username, owner):
                return 'claim_lost'
            evidence: dict[str, Any] = {'user_id': creator.user_id}
            if creator.like_count is not None:
                await queue.update_tier(username, creator.like_count)
                evidence['last_like_count'] = creator.like_count
            await queue.record_scrape_success(
                username, follower_count=None, worker_id=owner,
                evidence=evidence,
            )
            if not await _owns_claim(queue, username, owner):
                return 'claim_lost'
            await queue.release(username)
        _LOGGER.info(
            f'Saved and rescheduled creator @{username}',
            extra={**LABELS, 'username': username,
                   'worker_id': str(settings.worker_id)},
        )
        return 'scraped'
    except Exception as exc:  # noqa: BLE001 - retain failed queue work
        return await _retry_creator(username, queue, settings, owner, exc)


async def queue_worker(
    proxies: list[str | None], queue: RedisCreatorQueue,
    fm: AssetFileManagement, settings: OnlyFansScraperSettings,
    limiter: OnlyFansRateLimiter, owner: str,
) -> None:
    '''Poll due work; keep at most one anonymous browser per async task.'''
    index: int = 0
    context: BrowserContext | None = None
    current_proxy: str | None = None
    async with AsyncExitStack() as stack:
        while True:
            batch: list[tuple[str, str, float]] = await queue.claim_batch(
                1, owner, claim_ttl=settings.creator_claim_ttl_seconds,
            )
            if not batch:
                await worker_sleep(QUEUE_POLL_SECONDS, settings, owner)
                continue
            username: str = batch[0][0]
            proxy: str | None = proxies[index % len(proxies)]
            index += 1
            started: float = time.monotonic()
            try:
                if context is None or current_proxy != proxy:
                    async with asyncio.timeout(
                        settings.browser_timeout_seconds,
                    ):
                        await stack.aclose()
                        context = None
                        context = await stack.enter_async_context(
                            anonymous_browser(proxy, settings),
                        )
                    current_proxy = proxy
                outcome: str = await process_queued_creator(
                    username, context, queue, fm, settings, limiter,
                    proxy, owner,
                )
            except Exception as exc:  # noqa: BLE001 - browser startup failures
                outcome = await _retry_creator(
                    username, queue, settings, owner, exc,
                )
            record_outcome(outcome, started, settings, proxy)
            if outcome in ('failed', 'rate_limit'):
                await stack.aclose()
                context = None
                delay: float = settings.creator_retry_interval_seconds
                if outcome == 'rate_limit':
                    delay = max(delay, settings.blocked_cooldown_seconds)
                await worker_sleep(delay, settings, owner)


async def _queue_maintenance(queue: RedisCreatorQueue) -> None:
    while True:
        await queue.scan_and_recover_orphans_with_fleet_lock(
            lock_ttl_seconds=int(QUEUE_POLL_SECONDS),
        )
        METRIC_SCRAPE_QUEUE_SIZE.labels(
            platform='onlyfans', scraper='onlyfans_creator', entity='creator',
            state='queued', worker_id='',
        ).set(await queue.queue_size())
        await sleep(QUEUE_POLL_SECONDS)


async def run_daemon(settings: OnlyFansScraperSettings) -> None:
    instance: str = f'{settings.worker_id}:{uuid4().hex}'
    queue: RedisCreatorQueue = build_queue(settings, instance)
    limiter: OnlyFansRateLimiter | None = None
    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
    task: asyncio.Task[Any] | None = asyncio.current_task()
    if task is not None:
        loop.add_signal_handler(signal.SIGTERM, task.cancel)
    try:
        limiter = OnlyFansRateLimiter(settings)
        fm: AssetFileManagement = AssetFileManagement(
            str(settings.creator_data_directory),
            prefix_rankings={'creator': [PREFIX]},
        )
        proxies: list[str | None] = list(settings.proxies) or [None]
        count: int = min(settings.concurrency, len(proxies))
        async with asyncio.TaskGroup() as group:
            group.create_task(_queue_maintenance(queue))
            index: int
            for index in range(count):
                group.create_task(queue_worker(
                    proxies[index::count], queue, fm, settings, limiter,
                    f'{instance}:{index}',
                ))
    finally:
        loop.remove_signal_handler(signal.SIGTERM)
        try:
            if limiter is not None:
                await limiter.aclose()
        finally:
            await queue._redis.aclose()


def load_creators(settings: OnlyFansScraperSettings) -> list[str]:
    '''Validate the whole batch before opening a browser or writing files.'''
    if bool(settings.username) == bool(settings.creator_file):
        raise ValueError('Provide exactly one of --username or --creator-file')
    rows: list[str] = (
        [settings.username] if settings.username else
        settings.creator_file.read_text(encoding='utf-8').splitlines()
        if settings.creator_file else []
    )
    creators: list[str] = []
    seen: set[str] = set()
    number: int
    row: str
    for number, row in enumerate(rows, start=1):
        if not row.strip() or row.lstrip().startswith('#'):
            continue
        try:
            username: str = normalize_creator(row)
        except ValueError:
            raise ValueError(
                f'Invalid creator on input line {number}',
            ) from None
        if username not in seen:
            seen.add(username)
            creators.append(username)
    if not creators:
        raise ValueError('No creators in input')
    return creators


async def save_creator(
    creator: OnlyFansCreator, fm: AssetFileManagement,
) -> None:
    await fm.write_file(
        f'{PREFIX}{creator.username}.json.br', creator.to_dict(),
    )
    METRIC_SCRAPE_RECORDS_WRITTEN.labels(**LABELS).inc()


async def scrape_creators(
    creators: list[str], settings: OnlyFansScraperSettings,
) -> int:
    '''Use every configured proxy, with a bounded number of async tasks.'''
    proxies: list[str | None] = list(settings.proxies) or [None]
    limiter: OnlyFansRateLimiter = OnlyFansRateLimiter(settings)
    fm: AssetFileManagement = AssetFileManagement(
        str(settings.creator_data_directory),
        prefix_rankings={'creator': [PREFIX]},
    )
    semaphore: asyncio.Semaphore = asyncio.Semaphore(settings.concurrency)

    async def scrape_batch(proxy: str | None, batch: list[str]) -> int:
        failed: int = 0
        completed: int = 0
        if not batch:
            return 0
        async with semaphore:
            try:
                async with anonymous_browser(proxy, settings) as context:
                    username: str
                    for username in batch:
                        started: float = time.monotonic()
                        outcome: str = 'cancelled'
                        try:
                            creator: OnlyFansCreator = await fetch_profile(
                                context, username, limiter, proxy, settings,
                            )
                            await save_creator(creator, fm)
                            outcome = 'scraped'
                            _LOGGER.info(
                                f'Saved creator @{username}',
                                extra={**LABELS, 'username': username},
                            )
                        except ProfileBlockedError:
                            outcome = 'rate_limit'
                            _LOGGER.warning(
                                'Public access blocked; stopping this proxy '
                                'batch after applying the shared cooldown',
                                extra={**LABELS, 'username': username,
                                       'reason': 'rate_limit',
                                       'worker_id': str(settings.worker_id)},
                            )
                            return failed + len(batch) - completed
                        except Exception as exc:  # noqa: BLE001 - batch errors
                            outcome = 'failed'
                            # Raw browser errors can contain proxy credentials.
                            _LOGGER.warning(
                                f'Creator @{username} failed: '
                                f'{type(exc).__name__}',
                                extra={**LABELS, 'username': username,
                                       'error_type': type(exc).__name__,
                                       'worker_id': str(settings.worker_id)},
                            )
                            failed += 1
                        finally:
                            if outcome != 'cancelled':
                                record_outcome(
                                    outcome, started, settings, proxy,
                                )
                        completed += 1
            except Exception as exc:  # noqa: BLE001 - redact browser failures
                _LOGGER.warning(f'Browser batch failed: {type(exc).__name__}')
                return failed + len(batch) - completed
        return failed

    try:
        results: list[int] = await asyncio.gather(*(
            scrape_batch(proxy, creators[index::len(proxies)])
            for index, proxy in enumerate(proxies)
        ))
        return sum(results)
    finally:
        await limiter.aclose()


def main() -> None:
    try:
        settings: OnlyFansScraperSettings = OnlyFansScraperSettings()
    except Exception as exc:  # noqa: BLE001 - redact configuration secrets
        # Validation errors can echo secrets supplied through proxy settings.
        print(
            f'Invalid configuration ({type(exc).__name__}). '
            'Check input and proxy settings; use --help for options.',
        )
        raise SystemExit(2) from None
    try:
        creators: list[str] | None = (
            load_creators(settings)
            if settings.username or settings.creator_file else None
        )
        if creators is None and not settings.redis_dsn:
            raise ValueError('Set REDIS_DSN for the OnlyFans creator queue')
    except ValueError as exc:
        print(str(exc))
        raise SystemExit(2) from None
    except OSError:
        print('Could not read creator file; check --creator-file')
        raise SystemExit(2) from None
    configure_logging(
        level=settings.log_level, filename=settings.log_file,
        log_format=settings.log_format,
    )
    if settings.metrics_port is not None:
        start_metrics_server(settings.metrics_port)
        _LOGGER.info(
            'Prometheus metrics available',
            extra={**LABELS, 'metrics_port': settings.metrics_port},
        )
    config_labels: dict[str, str] = {
        'platform': 'onlyfans', 'scraper': 'onlyfans_creator',
        'role': 'worker', 'worker_id': str(settings.worker_id),
    }
    concurrency: int = min(settings.concurrency, len(settings.proxies) or 1)
    METRIC_NUM_PROCESSES.labels(**config_labels).set(1)
    METRIC_CONCURRENCY.labels(**config_labels).set(concurrency)
    _LOGGER.info(
        'Scraper worker started',
        extra={**config_labels, 'concurrency': concurrency,
               'proxies_count': len(settings.proxies),
               'metrics_port': settings.metrics_port,
               'mode': 'queue' if creators is None else 'batch'},
    )
    try:
        if creators is None:
            asyncio.run(run_daemon(settings))
            return
        failed: int = asyncio.run(scrape_creators(creators, settings))
    except asyncio.CancelledError:
        return
    except KeyboardInterrupt:
        raise SystemExit(130) from None
    except Exception as exc:  # noqa: BLE001 - safe CLI error boundary
        _LOGGER.error(f'Scraper failed: {type(exc).__name__}')
        raise SystemExit(1) from None
    _LOGGER.info(f'Scraped {len(creators) - failed}/{len(creators)} creators')
    if failed:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
