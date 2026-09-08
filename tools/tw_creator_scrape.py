#!/usr/bin/env python3
'''Scrape anonymous Twitch profiles to compressed JSON, once or as a daemon.'''

import asyncio
import contextlib
import logging
import math
import os
import random
import sys
import time
from pathlib import Path
from typing import cast
from uuid import uuid4

from pydantic import AliasChoices

from scrape_exchange.creator_queue import (
    RedisCreatorQueue,
    parse_priority_queues,
)
from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.scraper_metrics import (
    METRIC_CREATOR_SCRAPE_STATE_SIZE,
    METRIC_SCRAPE_DURATION,
    METRIC_SCRAPE_FAILURES,
    METRIC_SCRAPE_QUEUE_SIZE,
    METRIC_SCRAPE_RECORDS_WRITTEN,
    METRIC_SCRAPE_RETRIES,
    METRIC_SCRAPES_COMPLETED,
    METRIC_WORKER_SLEEP_SECONDS,
)
from scrape_exchange.scraper_runner import ScraperRunContext, ScraperRunner
from scrape_exchange.scraper_supervisor import distribute_total_concurrency
from scrape_exchange.twitch.normalization import normalize_creator
from scrape_exchange.twitch.settings import TwitchScraperSettings
from scrape_exchange.twitch.twitch_browser import fetch_profile
from scrape_exchange.twitch.twitch_creator import TwitchCreator
from scrape_exchange.twitch.twitch_error_classification import (
    ProfileExtractionError,
    ProfileIdentityError,
    classify_twitch_error,
)
from scrape_exchange.twitch.twitch_rate_limiter import (
    TwitchCallType,
    TwitchRateLimiter,
)
from scrape_exchange.twitch.twitch_session_pool import (
    DIRECT_PROXY,
    TwitchSessionPool,
)
from scrape_exchange.util import extract_proxy_ip, extract_proxy_port
from scrape_exchange.watchdog import Watchdog
from scrape_exchange.worker_id import get_worker_id

_LOGGER: logging.Logger = logging.getLogger(__name__)
PLATFORM: str = 'twitch'
SCRAPER: str = 'twitch_creator'
PREFIX: str = 'twitch-creator-'


def build_queue(
    settings: TwitchScraperSettings, worker_id: str,
) -> RedisCreatorQueue:
    if not settings.redis_dsn:
        raise ValueError('Daemon mode requires REDIS_DSN')
    queue: RedisCreatorQueue = RedisCreatorQueue(
        settings.redis_dsn, worker_id, PLATFORM, key_namespace='scrape',
    )
    queue._tiers = parse_priority_queues(settings.creator_priority_queues)
    queue._key_queues = queue._build_queue_keys(queue._tiers)
    return queue


def _check_identity(creator: TwitchCreator, previous_id: str | None) -> None:
    if previous_id and creator.user_id is None:
        raise ProfileExtractionError('Previously known account ID is missing')
    if previous_id and previous_id != creator.user_id:
        raise ProfileIdentityError('Username now resolves to a different ID')


async def save_creator(
    creator: TwitchCreator, fm: AssetFileManagement,
    previous_id: str | None = None,
) -> None:
    if normalize_creator(creator.username) != creator.username:
        raise ProfileIdentityError('Invalid filename identity')
    _check_identity(creator, previous_id)
    filename: str = f'{PREFIX}{creator.username}.json.br'
    for reader in (fm.read_file, fm.read_uploaded):
        with contextlib.suppress(FileNotFoundError):
            previous: dict = await reader(filename)
            _check_identity(creator, previous.get('user_id'))
    await fm.write_file(filename, creator.to_dict())


async def _owns_claim(
    queue: RedisCreatorQueue, username: str, claim_owner: str,
) -> bool:
    return await queue._redis.get(
        f'{queue._claim_prefix}{username}',
    ) == claim_owner


def _labels(proxy: str, worker_id: str) -> dict[str, str]:
    return {
        'platform': PLATFORM, 'scraper': SCRAPER, 'entity': 'creator',
        'api': 'browser', 'worker_id': worker_id,
        'proxy_ip': (
            extract_proxy_ip(proxy) if proxy != DIRECT_PROXY else 'none'
        ),
        'proxy_port': extract_proxy_port(proxy), 'proxy_file': '',
    }


async def process_creator(
    username: str, proxy: str, pool: TwitchSessionPool,
    queue: RedisCreatorQueue, fm: AssetFileManagement,
    settings: TwitchScraperSettings, worker_id: str, claim_owner: str,
) -> str | None:
    start: float = time.monotonic()
    labels: dict[str, str] = _labels(proxy, worker_id)
    outcome: str = 'failure'
    if not await _owns_claim(queue, username, claim_owner):
        return 'claim_lost'
    await queue.record_scrape_attempt(username, worker_id=worker_id)
    try:
        async with asyncio.timeout(settings.creator_profile_timeout_seconds):
            async with pool.session_for(proxy) as page:
                creator: TwitchCreator = await fetch_profile(
                    page, username, settings, pool.limiter, proxy,
                )
        if not await _owns_claim(queue, username, claim_owner):
            return 'claim_lost'
        state: dict = await queue.get_scrape_state(username)
        async with asyncio.timeout(30):
            await save_creator(creator, fm, state.get('user_id'))
        METRIC_SCRAPE_RECORDS_WRITTEN.labels(
            platform=PLATFORM, scraper=SCRAPER, entity='creator',
        ).inc()
        if creator.follower_count is not None:
            await queue.update_tier(username, creator.follower_count)
        await queue.record_scrape_success(
            username, follower_count=creator.follower_count,
            worker_id=worker_id, evidence={
                'user_id': creator.user_id,
                'extractor_version': creator.extractor_version,
                'completeness': creator.completeness,
                'sources': creator.sources,
                'follower_count_known': creator.follower_count is not None,
            },
        )
        await queue.release(username)
        METRIC_SCRAPES_COMPLETED.labels(
            **labels, channel_status='none',
        ).inc()
        outcome = 'success'
        return None
    except asyncio.CancelledError:
        # Claim expiry and orphan recovery safely resume interrupted work.
        raise
    except Exception as exc:  # noqa: BLE001 - classify and retain queue work
        reason: str = classify_twitch_error(exc)
        if not await _owns_claim(queue, username, claim_owner):
            return 'claim_lost'
        await queue.record_scrape_failure(
            username, status=reason, error=type(exc).__name__,
            worker_id=worker_id,
        )
        if reason in ('unavailable', 'identity_conflict'):
            await queue.exclude(username)
        else:
            delay: float = (
                settings.creator_bot_cooldown_seconds
                if reason == 'rate_limit'
                else settings.creator_retry_interval_seconds
            )
            await queue.reschedule_in(username, delay * random.uniform(1, 1.25))
            METRIC_SCRAPE_RETRIES.labels(
                platform=PLATFORM, scraper=SCRAPER, entity='creator',
                api='browser', reason=reason,
            ).inc()
        METRIC_SCRAPE_FAILURES.labels(**labels, reason=reason).inc()
        _LOGGER.warning(f'Twitch profile {username}: {reason}')
        return reason
    finally:
        METRIC_SCRAPE_DURATION.labels(
            platform=PLATFORM, scraper=SCRAPER, entity='creator',
            api='browser', outcome=outcome, worker_id=worker_id,
        ).observe(time.monotonic() - start)


async def _proxy_task(
    proxy: str, pool: TwitchSessionPool, queue: RedisCreatorQueue,
    fm: AssetFileManagement, settings: TwitchScraperSettings, worker_id: str,
) -> None:
    claim_owner: str = f'{worker_id}:{uuid4().hex}'
    cooldown: float = settings.creator_bot_cooldown_seconds
    while True:
        Watchdog.get().touch_work()
        # Waiting for a rate token must not consume the claim lifetime.
        await pool.limiter.acquire(TwitchCallType.CREATOR, proxy=proxy)
        batch: list[tuple[str, str, float]] = await queue.claim_batch(
            1, claim_owner, claim_ttl=settings.creator_claim_ttl_seconds,
        )
        if not batch:
            await asyncio.sleep(settings.creator_queue_idle_poll_seconds)
            continue
        result: str | None = await process_creator(
            batch[0][0], proxy, pool, queue, fm, settings,
            worker_id, claim_owner,
        )
        if result not in ('rate_limit', 'transient'):
            cooldown = settings.creator_bot_cooldown_seconds
            continue
        if result == 'rate_limit':
            await pool.limiter.penalise(TwitchCallType.CREATOR, proxy, cooldown)
        await pool.quarantine(proxy)
        delay: float = (
            cooldown if result == 'rate_limit'
            else settings.creator_retry_interval_seconds
        )
        METRIC_WORKER_SLEEP_SECONDS.labels(
            platform=PLATFORM, scraper=SCRAPER, worker_id=worker_id,
        ).set(delay)
        try:
            await asyncio.sleep(delay)
        finally:
            METRIC_WORKER_SLEEP_SECONDS.labels(
                platform=PLATFORM, scraper=SCRAPER, worker_id=worker_id,
            ).set(0)
        if not await pool.rebuild(proxy):
            raise RuntimeError('Twitch session rebuild failed')
        cooldown = min(cooldown * 2, settings.creator_bot_cooldown_max_seconds)


async def _maintenance(
    queue: RedisCreatorQueue, settings: TwitchScraperSettings, worker_id: str,
) -> None:
    next_recovery: float = 0
    statuses: set[str] = set()
    while True:
        Watchdog.get().touch_work()
        if time.monotonic() >= next_recovery:
            await queue.scan_and_recover_orphans_with_fleet_lock(
                recover=True,
                lock_ttl_seconds=max(1, math.ceil(
                    settings.creator_orphan_recovery_interval_seconds,
                )),
            )
            next_recovery = (
                time.monotonic()
                + settings.creator_orphan_recovery_interval_seconds
            )
        sizes: dict[int, int] = await queue.queue_sizes_by_tier()
        METRIC_SCRAPE_QUEUE_SIZE.labels(
            platform=PLATFORM, scraper=SCRAPER, entity='creator',
            state='queued', worker_id=worker_id,
        ).set(sum(sizes.values()))
        counts: dict[str, int] = await queue.count_by_scrape_status()
        statuses.update(counts)
        for status in statuses:
            METRIC_CREATOR_SCRAPE_STATE_SIZE.labels(
                platform=PLATFORM, scraper=SCRAPER, worker_id=worker_id,
                status=status,
            ).set(counts.get(status, 0))
        await asyncio.sleep(settings.creator_queue_idle_poll_seconds)


async def run_worker(ctx: ScraperRunContext) -> None:
    settings: TwitchScraperSettings = cast(TwitchScraperSettings, ctx.settings)
    if not settings.creator_data_directory:
        raise ValueError('Creator data directory must be configured')
    worker_id: str = settings.worker_id or get_worker_id()
    fm: AssetFileManagement = AssetFileManagement(
        settings.creator_data_directory,
        prefix_rankings={'creator': [PREFIX]},
    )
    pool: TwitchSessionPool = TwitchSessionPool(
        list(ctx.proxies)[:settings.creator_concurrency], settings,
        cast(TwitchRateLimiter, ctx.rate_limiter), worker_id,
    )
    queue: RedisCreatorQueue | None = None
    try:
        await pool.bootstrap()
        proxies: list[str] = pool.ready_proxies()
        if not proxies:
            raise RuntimeError('No Twitch browser sessions could start')
        if settings.username:
            proxy: str = proxies[0]
            await pool.limiter.acquire(TwitchCallType.CREATOR, proxy=proxy)
            async with pool.session_for(proxy) as page:
                creator: TwitchCreator = await fetch_profile(
                    page, settings.username, settings, pool.limiter, proxy,
                )
            await save_creator(creator, fm)
            _LOGGER.info(f'Saved {PREFIX}{creator.username}.json.br')
            return
        queue = build_queue(settings, worker_id)
        async with asyncio.TaskGroup() as group:
            group.create_task(_maintenance(queue, settings, worker_id))
            for proxy in proxies:
                group.create_task(_proxy_task(
                    proxy, pool, queue, fm, settings, worker_id,
                ))
    finally:
        try:
            await pool.shutdown()
        finally:
            if queue is not None:
                await queue._redis.aclose()


def _export_child_settings(settings: TwitchScraperSettings) -> None:
    '''The shared supervisor spawns children without CLI arguments.'''
    for name, field in type(settings).model_fields.items():
        value: object = getattr(settings, name)
        if value is None or name in (
            'api_key_id', 'api_key_secret', 'username',
        ):
            continue
        alias: object = field.validation_alias
        if isinstance(alias, AliasChoices):
            alias = alias.choices[0]
        key: str = str(alias) if alias else f'TWITCH_{name.upper()}'
        os.environ[key] = (
            str(value).lower() if isinstance(value, bool) else str(value)
        )
    # Parent already resolved proxy files. Children must use their slices.
    os.environ['PROXY_FILES'] = ''


def main() -> None:
    settings: TwitchScraperSettings = TwitchScraperSettings()
    if settings.username:
        username: str | None = normalize_creator(
            settings.username,
        )
        if username is None:
            raise SystemExit('Supply a valid profile username or URL')
        settings.username = username
    elif not settings.redis_dsn:
        raise SystemExit('Set REDIS_DSN for daemon mode, or use --username')
    if not settings.creator_data_directory:
        raise SystemExit('Configure TWITCH_CREATOR_DATA_DIR')
    Path(settings.creator_data_directory).mkdir(parents=True, exist_ok=True)
    capacity: int = len(settings.proxies) or 1
    total: int = min(settings.creator_concurrency, capacity)
    processes: int = min(settings.creator_num_processes, total)
    if settings.username:
        total = processes = 1
    child_concurrencies: list[int] | None = (
        distribute_total_concurrency(total, processes)
        if processes > 1 else None
    )
    settings.creator_concurrency = total
    settings.creator_num_processes = processes
    object.__setattr__(settings, 'proxies', list(settings.proxies)[:total])
    if processes > 1:
        _export_child_settings(settings)
    runner: ScraperRunner = ScraperRunner(
        settings=settings, scraper_label=SCRAPER, platform=PLATFORM,
        num_processes=processes, concurrency=total,
        metrics_port=settings.metrics_port,
        log_file=settings.creator_log_file,
        log_level=settings.creator_log_level,
        rate_limiter_factory=lambda value: TwitchRateLimiter(
            cast(TwitchScraperSettings, value),
        ),
        client_required=False, client_enabled=False, split_proxy_pool=True,
        concurrency_env_var='TWITCH_CREATOR_CONCURRENCY',
        child_concurrencies=child_concurrencies,
    )
    sys.exit(runner.run_sync(run_worker))


if __name__ == '__main__':
    main()
