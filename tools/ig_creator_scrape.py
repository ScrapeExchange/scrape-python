#!/usr/bin/env python3

'''
Instagram creator scrape daemon.

Scrape-to-disk only: records are written as Brotli ``.json.br`` via
``AssetFileManagement``. Upload/schema support is intentionally out of
scope for v1.

Design: docs/superpowers/specs/
2026-07-01-instagram-creator-scrape-tool-design.md

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import logging
import os
import random
import resource
import sys
import time

from typing import Any

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from pydantic import (
    AliasChoices,
    Field,
    field_validator,
    model_validator,
)

from scrape_exchange.creator_queue import (
    DEFAULT_ORPHAN_RECOVERY_INTERVAL_SECONDS,
    RedisCreatorQueue,
    TierConfig,
    parse_priority_queues,
)
from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.instagram import (
    InstagramCreator,
    InstagramProfileData,
    InstagramRateLimiter,
    InstagramSessionPool,
    classify_instagram_error,
)
from scrape_exchange.instagram.instagram_creator import (
    EXTRACTOR_VERSION,
    InstagramRateLimitError,
    extract_profile_data,
)
from scrape_exchange.instagram.settings import InstagramScraperSettings
from scrape_exchange.proxy_loader import ProxyCatalog, set_active_catalog
from scrape_exchange.scraper_metrics import (
    METRIC_INSTAGRAM_PROFILE_JSON_TIMEOUTS,
    METRIC_INSTAGRAM_PROFILE_TIMEOUT_HTML_BYTES,
    METRIC_CREATOR_SCRAPE_STATE_SIZE,
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
from scrape_exchange.util import extract_proxy_ip, extract_proxy_port
from scrape_exchange.watchdog import Watchdog
from scrape_exchange.worker_id import get_worker_id


_LOGGER: logging.Logger = logging.getLogger(__name__)

SCRAPER_LABEL: str = 'instagram_creator'
PLATFORM: str = 'instagram'
ENTITY: str = 'creator'
API_LABEL: str = 'browser'
CREATOR_FILE_PREFIX: str = 'instagram-creator-'
CREATOR_FILE_POSTFIX: str = '.json.br'
_FD_TARGET: int = 1_048_576
_PROFILE_MARKERS: tuple[tuple[str, str], ...] = (
    ('structured_profile', '"xig_user_by_username"'),
    ('biography', '"biography"'),
    ('edge_followed_by', '"edge_followed_by"'),
    ('follower_count', '"follower_count"'),
)
_TIMEOUT_MARKERS: tuple[tuple[str, str], ...] = (
    *_PROFILE_MARKERS,
    ('login', '/accounts/login/'),
    ('challenge', '/challenge/'),
    ('checkpoint', '/checkpoint/'),
    ('not_found', 'Sorry, this page'),
)


class InstagramProfileJsonTimeoutError(RuntimeError):
    '''Raised when profile JSON never hydrates, with page evidence.'''

    def __init__(self, timeout_ms: int, evidence: dict) -> None:
        super().__init__(
            f'instagram profile JSON timeout after {timeout_ms}ms',
        )
        self.evidence: dict = evidence


class CreatorSettings(InstagramScraperSettings):
    '''Configuration for ``ig_creator_scrape.py``.'''

    metrics_port: int = Field(
        default=9900,
        validation_alias=AliasChoices(
            'IG_CREATOR_METRICS_PORT',
            'INSTAGRAM_CREATOR_METRICS_PORT',
            'metrics_port',
        ),
        description='Prometheus metrics port.',
    )
    creator_concurrency: int = Field(
        default=0,
        validation_alias=AliasChoices(
            'IG_CREATOR_CONCURRENCY',
            'INSTAGRAM_CREATOR_CONCURRENCY',
            'creator_concurrency',
        ),
        description='Fleet-wide async worker upper bound.',
    )
    creator_num_processes: int = Field(
        default=1,
        validation_alias=AliasChoices(
            'IG_CREATOR_NUM_PROCESSES',
            'INSTAGRAM_CREATOR_NUM_PROCESSES',
            'creator_num_processes',
        ),
        description='Supervisor child count.',
    )
    creator_disable_proxies: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            'IG_CREATOR_DISABLE_PROXIES',
            'INSTAGRAM_CREATOR_DISABLE_PROXIES',
            'creator_disable_proxies',
        ),
        description=(
            'Ignore configured proxies and scrape through a direct '
            'browser session.'
        ),
    )
    creator_log_file: str = Field(
        default='/dev/stdout',
        validation_alias=AliasChoices(
            'IG_CREATOR_LOG_FILE',
            'INSTAGRAM_CREATOR_LOG_FILE',
            'LOG_FILE',
            'log_file',
        ),
        description='Log file path for the Instagram creator scraper.',
    )
    creator_log_level: str = Field(
        default='INFO',
        validation_alias=AliasChoices(
            'IG_CREATOR_LOG_LEVEL',
            'INSTAGRAM_CREATOR_LOG_LEVEL',
            'LOG_LEVEL',
            'log_level',
        ),
        description='Logging level for the Instagram creator scraper.',
    )
    creator_priority_queues: str = Field(
        default='72:10000000,168:1000000,336:100000,720:10000,4320:0',
        validation_alias=AliasChoices(
            'IG_CREATOR_PRIORITY_QUEUES',
            'INSTAGRAM_CREATOR_PRIORITY_QUEUES',
            'creator_priority_queues',
        ),
        description='Comma-separated interval_hours:min_followers tiers.',
    )
    creator_claim_ttl_seconds: int = Field(
        default=600,
        validation_alias=AliasChoices(
            'IG_CREATOR_CLAIM_TTL', 'creator_claim_ttl_seconds',
        ),
        description='Per-creator claim TTL.',
    )
    creator_queue_idle_poll_seconds: float = Field(
        default=30.0,
        validation_alias=AliasChoices(
            'IG_CREATOR_QUEUE_IDLE_POLL',
            'creator_queue_idle_poll_seconds',
        ),
        description='Sleep ceiling when nothing is due.',
    )
    creator_orphan_recovery_interval_seconds: float = Field(
        default=DEFAULT_ORPHAN_RECOVERY_INTERVAL_SECONDS,
        validation_alias=AliasChoices(
            'IG_CREATOR_ORPHAN_RECOVERY_INTERVAL',
            'creator_orphan_recovery_interval_seconds',
        ),
        description='Interval between queue orphan recovery scans.',
    )
    creator_retry_interval_seconds: float = Field(
        default=300.0,
        validation_alias=AliasChoices(
            'IG_CREATOR_RETRY_INTERVAL',
            'creator_retry_interval_seconds',
        ),
        description='Retry floor after transient failures.',
    )
    creator_rate_limit_retry_interval_seconds: float = Field(
        default=1800.0,
        validation_alias=AliasChoices(
            'IG_CREATOR_RATE_LIMIT_RETRY_INTERVAL',
            'creator_rate_limit_retry_interval_seconds',
        ),
        description='Retry floor after Instagram block/rate-limit pages.',
    )
    creator_unknown_followers_retry_interval_seconds: float = Field(
        default=86400.0,
        validation_alias=AliasChoices(
            'IG_CREATOR_UNKNOWN_FOLLOWERS_RETRY_INTERVAL',
            'creator_unknown_followers_retry_interval_seconds',
        ),
        description='Retry floor when follower_count is not available.',
    )
    creator_retry_jitter_fraction: float = Field(
        default=0.25,
        validation_alias=AliasChoices(
            'IG_CREATOR_RETRY_JITTER_FRACTION',
            'creator_retry_jitter_fraction',
        ),
        description='Random +/- fraction applied to retry delays.',
    )
    creator_bot_failure_threshold: int = Field(
        default=1,
        validation_alias=AliasChoices(
            'IG_CREATOR_BOT_FAILURE_THRESHOLD',
            'creator_bot_failure_threshold',
        ),
        description='Consecutive block signals before rebuilding.',
    )
    creator_bot_cooldown_seconds: float = Field(
        default=1800.0,
        validation_alias=AliasChoices(
            'IG_CREATOR_BOT_COOLDOWN',
            'creator_bot_cooldown_seconds',
        ),
        description='Cooldown after a bot-detected proxy session.',
    )
    creator_bot_cooldown_max_seconds: float = Field(
        default=3600.0,
        validation_alias=AliasChoices(
            'IG_CREATOR_BOT_COOLDOWN_MAX',
            'creator_bot_cooldown_max_seconds',
        ),
        description='Maximum exponential bot-detection cooldown.',
    )
    creator_profile_data_timeout_ms: int = Field(
        default=10_000,
        validation_alias=AliasChoices(
            'IG_CREATOR_PROFILE_DATA_TIMEOUT_MS',
            'creator_profile_data_timeout_ms',
        ),
        description='Time to wait for hydrated profile JSON.',
    )

    @field_validator('creator_log_level', mode='before')
    @classmethod
    def _normalize_creator_log_level(cls, v: str) -> str:
        return normalize_log_level(v)

    @model_validator(mode='after')
    def _load_proxy_catalog(self) -> 'CreatorSettings':
        if self.creator_disable_proxies:
            object.__setattr__(self, 'proxies', [])
            set_active_catalog(ProxyCatalog())
            return self
        return super()._load_proxy_catalog()


def _validate_settings(settings: CreatorSettings) -> None:
    if not settings.redis_dsn:
        print(
            'Error: Redis must be configured via --redis-dsn or REDIS_DSN',
        )
        sys.exit(1)
    if not settings.creator_data_directory:
        print(
            'Error: scraped creator data directory must be set via '
            '--creator-data-directory or IG_CREATOR_DATA_DIR',
        )
        sys.exit(1)
    if not os.path.isdir(settings.creator_data_directory):
        print(
            f'Directory {settings.creator_data_directory} does not '
            'exist. It will be created.',
        )
        os.makedirs(settings.creator_data_directory, exist_ok=True)


def _creator_filename(username: str) -> str:
    return f'{CREATOR_FILE_PREFIX}{username}{CREATOR_FILE_POSTFIX}'


def _jittered_retry_delay(
    base_seconds: float,
    jitter_fraction: float,
) -> float:
    base: float = max(float(base_seconds), 0.0)
    fraction: float = max(float(jitter_fraction), 0.0)
    if base == 0.0 or fraction == 0.0:
        return base
    spread: float = base * fraction
    return max(0.0, base + random.uniform(-spread, spread))


def _auto_creator_concurrency(proxy_count: int) -> int:
    return max(int(proxy_count), 1) if proxy_count > 0 else 1


def _resolve_creator_concurrency(
    settings: CreatorSettings,
    proxy_count: int,
) -> int:
    requested: int = int(settings.creator_concurrency)
    if requested > 0:
        return requested
    return _auto_creator_concurrency(proxy_count)


def _effective_creator_concurrency(
    settings: CreatorSettings,
    ready_proxy_count: int,
) -> int:
    if ready_proxy_count <= 0:
        return 1
    return max(
        1,
        min(
            _resolve_creator_concurrency(settings, ready_proxy_count),
            ready_proxy_count,
        ),
    )


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


def _sleep_until(due: float | None, idle_poll: float) -> float:
    if due is None:
        return idle_poll
    delta: float = due - time.time()
    return max(0.0, min(delta, idle_poll))


def _evidence_from_page(
    *,
    url: str | None,
    title: str | None,
    http_status: int | None,
    markers: list[str] | None = None,
) -> dict:
    return {
        'last_url': url,
        'page_title': title,
        'http_status': http_status,
        'detected_markers': markers or [],
        'extractor_version': EXTRACTOR_VERSION,
    }


def _detected_timeout_markers(html: str) -> list[str]:
    markers: list[str] = [
        marker for marker, needle in _TIMEOUT_MARKERS
        if needle in html
    ]
    return markers


def _title_kind(title: str | None) -> str:
    if not title:
        return 'empty'
    title_lower: str = title.lower()
    if 'login' in title_lower:
        return 'login'
    if 'challenge' in title_lower or 'checkpoint' in title_lower:
        return 'challenge'
    if 'instagram' in title_lower:
        return 'instagram'
    return 'other'


def _marker_summary(markers: list[str]) -> str:
    profile_markers: set[str] = {
        marker for marker, _needle in _PROFILE_MARKERS
    }
    if any(marker in profile_markers for marker in markers):
        return 'profile_marker'
    if 'login' in markers:
        return 'login'
    if 'challenge' in markers or 'checkpoint' in markers:
        return 'challenge'
    if 'not_found' in markers:
        return 'not_found'
    return 'none'


def _http_status_label(http_status: int | None) -> str:
    if http_status is None:
        return 'none'
    return str(http_status)


def _emit_profile_timeout_metrics(evidence: dict) -> None:
    http_status: str = _http_status_label(evidence.get('http_status'))
    title_kind: str = _title_kind(evidence.get('page_title'))
    markers: list[str] = list(evidence.get('detected_markers') or [])
    marker_summary: str = _marker_summary(markers)
    labels: dict[str, str] = {
        'platform': PLATFORM,
        'scraper': SCRAPER_LABEL,
        'entity': ENTITY,
        'http_status': http_status,
        'title_kind': title_kind,
        'marker_summary': marker_summary,
    }
    METRIC_INSTAGRAM_PROFILE_JSON_TIMEOUTS.labels(**labels).inc()
    METRIC_INSTAGRAM_PROFILE_TIMEOUT_HTML_BYTES.labels(
        **labels,
    ).set(float(evidence.get('html_length') or 0))


async def _profile_timeout_evidence(page: Any, evidence: dict) -> dict:
    timeout_evidence: dict = dict(evidence)
    try:
        html: str = await page.content()
    except Exception as exc:
        timeout_evidence['content_error'] = str(exc)
        timeout_evidence['html_length'] = 0
        timeout_evidence['detected_markers'] = []
    else:
        timeout_evidence['html_length'] = len(html)
        timeout_evidence['detected_markers'] = (
            _detected_timeout_markers(html)
        )
    timeout_evidence['last_url'] = getattr(page, 'url', None)
    try:
        timeout_evidence['page_title'] = await page.title()
    except Exception as exc:
        timeout_evidence['title_error'] = str(exc)
    return timeout_evidence


async def _wait_for_profile_data(
    page: Any,
    timeout_ms: int,
    evidence: dict,
) -> None:
    try:
        await page.wait_for_function(
            '''
            () => {
                const html = document.documentElement.innerHTML;
                return html.includes('"xig_user_by_username"')
                    || html.includes('"biography"')
                    || html.includes('"edge_followed_by"')
                    || html.includes('"follower_count"');
            }
            ''',
            timeout=timeout_ms,
        )
    except PlaywrightTimeoutError:
        timeout_evidence: dict = await _profile_timeout_evidence(
            page, evidence,
        )
        _emit_profile_timeout_metrics(timeout_evidence)
        raise InstagramProfileJsonTimeoutError(
            timeout_ms, timeout_evidence,
        )


async def _fetch_profile_data(
    pool: InstagramSessionPool,
    proxy: str,
    username: str,
    settings: CreatorSettings,
) -> tuple[InstagramProfileData, dict]:
    await pool.gate_profile_request(proxy)
    async with pool.session_for(proxy) as session:
        page = session.page
        url: str = f'https://www.instagram.com/{username}/'
        resp = await page.goto(
            url,
            wait_until='domcontentloaded',
            timeout=settings.session_bootstrap_timeout_ms,
        )
        status: int | None = (
            resp.status if resp is not None else None
        )
        title: str | None = await page.title()
        evidence: dict = _evidence_from_page(
            url=page.url,
            title=title,
            http_status=status,
        )
        if status in (403, 429):
            evidence['detected_markers'] = ['http_block']
            raise InstagramRateLimitError(f'http status {status}')
        await _wait_for_profile_data(
            page,
            settings.creator_profile_data_timeout_ms,
            evidence,
        )
        html: str = await page.content()
        profile: InstagramProfileData = extract_profile_data(
            html, username,
        )
        evidence['detected_markers'] = profile.detected_markers
        return profile, evidence


async def _handle_failure(
    exc: BaseException,
    username: str,
    queue: RedisCreatorQueue,
    settings: CreatorSettings,
    worker_id: str,
    proxy_ip: str,
    proxy_port: str,
    evidence: dict | None = None,
) -> str:
    reason: str = classify_instagram_error(exc)
    exception_evidence: dict | None = getattr(exc, 'evidence', None)
    if exception_evidence is not None:
        evidence = exception_evidence
    retry_interval: float | None = None
    if reason == 'unavailable':
        await queue.remove(username)
    elif reason == 'unknown_followers':
        retry_interval = (
            settings.creator_unknown_followers_retry_interval_seconds
        )
    elif reason == 'rate_limit':
        retry_interval = settings.creator_rate_limit_retry_interval_seconds
    elif reason == 'transient':
        retry_interval = settings.creator_retry_interval_seconds

    if retry_interval is not None:
        retry_interval = _jittered_retry_delay(
            retry_interval,
            settings.creator_retry_jitter_fraction,
        )
        await queue.reschedule_in(username, retry_interval)
        METRIC_SCRAPE_RETRIES.labels(
            platform=PLATFORM,
            scraper=SCRAPER_LABEL,
            entity=ENTITY,
            api=API_LABEL,
            reason=reason,
        ).inc()
    elif reason != 'unavailable':
        await queue.release(username)

    rec: dict | None = await queue.show_member(username)
    next_due_at: float | None = (
        rec.get('score') if rec is not None else None
    )
    failure_evidence: dict = dict(evidence or {})
    failure_evidence['next_due_at'] = next_due_at
    await queue.record_scrape_failure(
        username,
        status=reason,
        error=str(exc),
        worker_id=worker_id,
        proxy_ip=proxy_ip,
        evidence=failure_evidence,
    )
    _LOGGER.warning(
        'Instagram creator scrape failed',
        extra={
            'username': username,
            'reason': reason,
            'error': str(exc),
            'proxy_ip': proxy_ip,
            'proxy_port': proxy_port,
            'last_url': failure_evidence.get('last_url'),
            'page_title': failure_evidence.get('page_title'),
            'http_status': failure_evidence.get('http_status'),
            'detected_markers': failure_evidence.get(
                'detected_markers',
            ),
            'html_length': failure_evidence.get('html_length'),
            'content_error': failure_evidence.get('content_error'),
            'title_kind': _title_kind(
                failure_evidence.get('page_title'),
            ),
            'marker_summary': _marker_summary(
                list(failure_evidence.get('detected_markers') or []),
            ),
            'next_due_at': next_due_at,
        },
    )
    return reason


async def _process_creator(
    username: str,
    proxy: str,
    pool: InstagramSessionPool,
    queue: RedisCreatorQueue,
    fm: AssetFileManagement,
    settings: CreatorSettings,
    worker_id: str,
    proxy_ip: str,
) -> str | None:
    start: float = time.monotonic()
    proxy_port: str = extract_proxy_port(proxy)
    evidence: dict | None = None
    await queue.record_scrape_attempt(
        username, worker_id=worker_id, proxy_ip=proxy_ip,
    )
    try:
        profile, evidence = await _fetch_profile_data(
            pool, proxy, username, settings,
        )
        creator: InstagramCreator = InstagramCreator.from_profile_data(
            profile,
        )
        await fm.write_file(
            _creator_filename(creator.username), creator.to_dict(),
        )
        METRIC_SCRAPE_RECORDS_WRITTEN.labels(
            platform=PLATFORM, scraper=SCRAPER_LABEL, entity=ENTITY,
        ).inc()
        await queue.update_tier(username, creator.follower_count)
        await queue.release(username)
        rec: dict | None = await queue.show_member(username)
        success_evidence: dict = dict(evidence)
        success_evidence['next_due_at'] = (
            rec.get('score') if rec is not None else None
        )
        await queue.record_scrape_success(
            username,
            follower_count=creator.follower_count,
            worker_id=worker_id,
            proxy_ip=proxy_ip,
            evidence=success_evidence,
        )
        METRIC_SCRAPES_COMPLETED.labels(
            platform=PLATFORM,
            scraper=SCRAPER_LABEL,
            entity=ENTITY,
            api=API_LABEL,
            worker_id=worker_id,
            proxy_ip=proxy_ip,
            proxy_port=proxy_port,
            proxy_file='',
            channel_status='none',
        ).inc()
        METRIC_SCRAPE_DURATION.labels(
            platform=PLATFORM,
            scraper=SCRAPER_LABEL,
            entity=ENTITY,
            api=API_LABEL,
            outcome='success',
            worker_id=worker_id,
        ).observe(time.monotonic() - start)
        return None
    except Exception as exc:
        reason: str = await _handle_failure(
            exc, username, queue, settings, worker_id, proxy_ip,
            proxy_port, evidence,
        )
        METRIC_SCRAPE_FAILURES.labels(
            platform=PLATFORM,
            scraper=SCRAPER_LABEL,
            entity=ENTITY,
            api=API_LABEL,
            reason=reason,
            worker_id=worker_id,
            proxy_ip=proxy_ip,
            proxy_port=proxy_port,
            proxy_file='',
        ).inc()
        METRIC_SCRAPE_DURATION.labels(
            platform=PLATFORM,
            scraper=SCRAPER_LABEL,
            entity=ENTITY,
            api=API_LABEL,
            outcome='failure',
            worker_id=worker_id,
        ).observe(time.monotonic() - start)
        return reason


async def _proxy_worker(
    proxy: str,
    pool: InstagramSessionPool,
    queue: RedisCreatorQueue,
    fm: AssetFileManagement,
    settings: CreatorSettings,
    shutdown_event: asyncio.Event,
    worker_id: str,
) -> None:
    proxy_ip: str = extract_proxy_ip(proxy)
    proxy_port: str = extract_proxy_port(proxy)
    consecutive_bot_failures: int = 0
    circuit_events: int = 0
    while not shutdown_event.is_set():
        Watchdog.get().touch_work()
        try:
            batch: list[tuple[str, str, float]] = (
                await queue.claim_batch(
                    1,
                    worker_id,
                    claim_ttl=settings.creator_claim_ttl_seconds,
                )
            )
        except Exception as exc:
            _LOGGER.warning(
                'Instagram claim_batch failed',
                extra={
                    'proxy_ip': proxy_ip,
                    'proxy_port': proxy_port,
                    'error': str(exc),
                },
            )
            await asyncio.sleep(settings.creator_queue_idle_poll_seconds)
            continue
        if not batch:
            due: float | None = await queue.next_due_time()
            await asyncio.sleep(
                _sleep_until(
                    due, settings.creator_queue_idle_poll_seconds,
                ),
            )
            continue
        username: str = batch[0][0]
        outcome: str | None = await _process_creator(
            username, proxy, pool, queue, fm, settings,
            worker_id, proxy_ip,
        )
        if outcome != 'rate_limit':
            consecutive_bot_failures = 0
            continue
        consecutive_bot_failures += 1
        threshold: int = max(settings.creator_bot_failure_threshold, 1)
        if consecutive_bot_failures < threshold:
            continue
        cooldown_seconds: float = min(
            settings.creator_bot_cooldown_seconds * (2 ** circuit_events),
            settings.creator_bot_cooldown_max_seconds,
        )
        _LOGGER.warning(
            'Instagram proxy bot-detection circuit opened',
            extra={
                'proxy_ip': proxy_ip,
                'proxy_port': proxy_port,
                'consecutive_failures': consecutive_bot_failures,
                'cooldown_seconds': cooldown_seconds,
            },
        )
        await pool.quarantine(proxy)
        await asyncio.sleep(
            _jittered_retry_delay(
                cooldown_seconds, settings.creator_retry_jitter_fraction,
            ),
        )
        circuit_events += 1
        consecutive_bot_failures = 0
        rebuilt: bool = await pool.rebuild(proxy)
        if not rebuilt:
            _LOGGER.error(
                'Instagram proxy session rebuild failed; retiring worker',
                extra={
                    'proxy_ip': proxy_ip,
                    'proxy_port': proxy_port,
                },
            )
            return


async def _recover_creator_queue_orphans(
    queue: RedisCreatorQueue,
) -> int:
    breakdown: dict[int, dict[str, int]] | None = (
        await queue.scan_and_recover_orphans_with_fleet_lock(
            recover=True,
        )
    )
    if breakdown is None:
        return 0
    return sum(
        counts.get('orphan', 0) for counts in breakdown.values()
    )


async def _maintenance_loop(
    queue: RedisCreatorQueue,
    settings: CreatorSettings,
    worker_id: str,
    shutdown_event: asyncio.Event,
) -> None:
    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
    next_orphan_recovery_at: float = 0.0
    while not shutdown_event.is_set():
        Watchdog.get().touch_work()
        try:
            now: float = loop.time()
            if now >= next_orphan_recovery_at:
                await _recover_creator_queue_orphans(queue)
                next_orphan_recovery_at = (
                    now
                    + settings.creator_orphan_recovery_interval_seconds
                )
            sizes: dict[int, int] = await queue.queue_sizes_by_tier()
            METRIC_SCRAPE_QUEUE_SIZE.labels(
                platform=PLATFORM,
                scraper=SCRAPER_LABEL,
                entity=ENTITY,
                state='queued',
                worker_id=worker_id,
            ).set(sum(sizes.values()))
            for tier, count in sizes.items():
                METRIC_SCRAPE_QUEUE_SIZE.labels(
                    platform=PLATFORM,
                    scraper=SCRAPER_LABEL,
                    entity=ENTITY,
                    state=f'tier:{tier}',
                    worker_id=worker_id,
                ).set(count)
            state_counts: dict[str, int] = (
                await queue.count_by_scrape_status()
            )
            for status, count in state_counts.items():
                METRIC_CREATOR_SCRAPE_STATE_SIZE.labels(
                    platform=PLATFORM,
                    scraper=SCRAPER_LABEL,
                    status=status,
                    worker_id=worker_id,
                ).set(count)
        except Exception as exc:
            _LOGGER.warning(
                'Instagram maintenance loop pass failed',
                extra={'error': str(exc)},
            )
        await asyncio.sleep(settings.creator_queue_idle_poll_seconds)


def _build_queue(
    settings: CreatorSettings,
    worker_id: str,
    tiers: list[TierConfig],
) -> RedisCreatorQueue:
    queue: RedisCreatorQueue = RedisCreatorQueue(
        settings.redis_dsn,
        worker_id,
        PLATFORM,
        key_namespace='scrape',
    )
    queue._tiers = tiers
    queue._key_queues = queue._build_queue_keys(tiers)
    return queue


def _raise_fd_limit() -> None:
    hard: int
    _, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    target: int = (
        hard if hard != resource.RLIM_INFINITY else _FD_TARGET
    )
    resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))


async def _run_worker(ctx: ScraperRunContext) -> None:
    _raise_fd_limit()
    settings: CreatorSettings = ctx.settings  # type: ignore[assignment]
    worker_id: str = settings.worker_id or get_worker_id()
    fm: AssetFileManagement = AssetFileManagement(
        settings.creator_data_directory,
        prefix_rankings={'creator': [CREATOR_FILE_PREFIX]},
    )
    tiers: list[TierConfig] = parse_priority_queues(
        settings.creator_priority_queues,
    )
    proxy_limit: int = _effective_creator_concurrency(
        settings, len(ctx.proxies),
    )
    pool_proxies: list[str] = ctx.proxies[:proxy_limit]
    pool: InstagramSessionPool = InstagramSessionPool(
        proxies=pool_proxies,
        state_dir=settings.session_state_dir,
        rate_limiter=ctx.rate_limiter,
        scraper_label=SCRAPER_LABEL,
        worker_id=worker_id,
        bootstrap_timeout_ms=settings.session_bootstrap_timeout_ms,
    )
    await pool.bootstrap()
    ready: list[str] = pool.ready_proxies()
    if not ready:
        _LOGGER.error(
            'No Instagram proxies bootstrapped; nothing to scrape',
            extra={'requested': len(ctx.proxies)},
        )
        return
    queue: RedisCreatorQueue = _build_queue(settings, worker_id, tiers)
    shutdown_event: asyncio.Event = asyncio.Event()
    maintenance_task: asyncio.Task = asyncio.create_task(
        _maintenance_loop(queue, settings, worker_id, shutdown_event),
    )
    active_proxies: list[str] = ready[
        :_effective_creator_concurrency(settings, len(ready))
    ]
    worker_tasks: list[asyncio.Task] = [
        asyncio.create_task(
            _proxy_worker(
                proxy, pool, queue, fm, settings,
                shutdown_event, worker_id,
            ),
        )
        for proxy in active_proxies
    ]
    _LOGGER.info(
        'Instagram creator scrape daemon running',
        extra={'workers': len(worker_tasks), 'worker_id': worker_id},
    )
    try:
        await asyncio.gather(*worker_tasks)
    finally:
        shutdown_event.set()
        maintenance_task.cancel()
        await asyncio.gather(maintenance_task, return_exceptions=True)
        await queue._redis.aclose()
        await pool.shutdown()


def _build_instagram_rate_limiter(
    settings: ScraperSettings,
) -> InstagramRateLimiter:
    return InstagramRateLimiter(
        state_dir=settings.rate_limiter_state_dir,
        redis_dsn=settings.redis_dsn,
    )


def _apply_proxy_settings(settings: CreatorSettings) -> None:
    if settings.creator_disable_proxies:
        object.__setattr__(settings, 'proxies', [])


def main() -> None:
    settings: CreatorSettings = CreatorSettings()
    _validate_settings(settings)
    _apply_proxy_settings(settings)
    proxy_count: int = len(settings.proxies)
    resolved_concurrency: int = _resolve_creator_concurrency(
        settings, proxy_count,
    )
    if proxy_count > 0:
        resolved_concurrency = min(resolved_concurrency, proxy_count)
        object.__setattr__(
            settings,
            'proxies',
            random_proxy_subset(
                list(settings.proxies), resolved_concurrency,
            ),
        )
        proxy_count = len(settings.proxies)
    process_count: int = _effective_process_count(
        settings.creator_num_processes,
        resolved_concurrency,
        proxy_count,
    )
    child_concurrencies: list[int] | None = (
        distribute_total_concurrency(resolved_concurrency, process_count)
        if process_count > 1 else None
    )
    settings.creator_num_processes = process_count
    settings.creator_concurrency = (
        resolved_concurrency if child_concurrencies is None
        else max(child_concurrencies)
    )
    os.environ['IG_CREATOR_CONCURRENCY'] = str(resolved_concurrency)
    runner: ScraperRunner = ScraperRunner(
        settings=settings,
        scraper_label=SCRAPER_LABEL,
        platform=PLATFORM,
        num_processes=process_count,
        concurrency=resolved_concurrency,
        metrics_port=settings.metrics_port,
        log_file=settings.creator_log_file,
        log_level=settings.creator_log_level,
        rate_limiter_factory=_build_instagram_rate_limiter,
        client_required=False,
        split_proxy_pool=True,
        concurrency_env_var='IG_CREATOR_CONCURRENCY',
        child_concurrencies=child_concurrencies,
    )
    sys.exit(runner.run_sync(_run_worker))


if __name__ == '__main__':
    main()
