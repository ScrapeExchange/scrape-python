'''
TikTok creator scrape daemon.

Continuously scrapes TikTok creator/profile records to disk, driven
by a Redis-backed tiered priority queue. Each creator's re-scrape
cadence is set by its follower-count tier; higher-follower creators
are polled more often. This is the first of the deferred TikTok
scraper tools that consume the hardened ``TikTokSessionPool``
(one Camoufox browser per proxy, main-world signing/fetch, per-proxy
ms_token jar + refresh loop).

Scrape-to-disk only: records are written as Brotli ``.json.br`` via
``AssetFileManagement``; uploading them to scrape.exchange is a
separate tool (``tt_creator_upload.py``).

Design: docs/superpowers/specs/
2026-06-10-tiktok-creator-scrape-tool-design.md

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
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from pydantic import AliasChoices, Field, field_validator

from scrape_exchange.creator_map import (
    CreatorMap,
    RedisCreatorMap,
)
from scrape_exchange.creator_queue import (
    CreatorQueue,
    RedisCreatorQueue,
    TierConfig,
    parse_priority_queues,
)
from scrape_exchange.file_management import (
    AssetFileManagement,
)
from scrape_exchange.handle_map import (
    HandleMap,
    RedisHandleMap,
)
from scrape_exchange.name_map import (
    NameMap,
    RedisNameMap,
)
from scrape_exchange.redis_client import redis_from_url
from scrape_exchange.scraper_metrics import (
    METRIC_SCRAPE_DURATION,
    METRIC_SCRAPE_FAILURES,
    METRIC_SCRAPE_QUEUE_ENQUEUES,
    METRIC_SCRAPE_QUEUE_SIZE,
    METRIC_SCRAPE_RECORDS_WRITTEN,
    METRIC_SCRAPE_RETRIES,
    METRIC_SCRAPES_COMPLETED,
    METRIC_TIKTOK_SHORT_URL_RESOLUTIONS,
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
    TikTokCreator,
    TikTokCallType,
    TikTokPlaylistRef,
    TikTokRateLimiter,
    TikTokSessionPool,
    TikTokVideoRef,
    classify_tiktok_error,
)
from scrape_exchange.tiktok.settings import (
    TikTokScraperSettings,
)
from scrape_exchange.tiktok.short_url import (
    ShortUrlOutcome,
    is_tiktok_short_url,
    resolve_creator_short_url,
)
from scrape_exchange.util import extract_proxy_ip
from scrape_exchange.video_scrape_queue import (
    RedisVideoScrapeQueue,
    VideoScrapeQueueSettings,
)
from scrape_exchange.watchdog import Watchdog
from scrape_exchange.worker_id import get_worker_id


_LOGGER: logging.Logger = logging.getLogger(__name__)

SCRAPER_LABEL: str = 'tiktok_creator'
PLATFORM: str = 'tiktok'
ENTITY: str = 'creator'
API_LABEL: str = 'tiktokapi'

CREATOR_FILE_PREFIX: str = 'tiktok-creator-'
CREATOR_FILE_POSTFIX: str = '.json.br'
REPOST_ITEM_LIST_URL: str = (
    'https://www.tiktok.com/api/repost/item_list/'
)

# Default fd-heavy ceiling when the hard limit is unbounded.
_FD_TARGET: int = 1_048_576


@dataclass
class CreatorIdentityMaps:
    '''Redis identity maps maintained by successful creator scrapes.'''

    user_id_to_username: CreatorMap
    username_to_user_id: HandleMap
    nickname_to_sec_uid: NameMap


class TikTokProfileResponseError(RuntimeError):
    '''TikTok returned no usable user object for a profile request.'''

    def __init__(self, response: object, missing: object) -> None:
        status: object = None
        if isinstance(response, dict):
            status = response.get(
                'status_code', response.get('statusCode'),
            )
        super().__init__(
            'invalid TikTok user detail response: '
            f'status={status!r} missing={missing!r}',
        )
        self.response: object = response


class TikTokProfileIdentityError(RuntimeError):
    '''TikTok returned a different creator than the one requested.'''

    def __init__(self, requested: str, returned: str) -> None:
        super().__init__(
            'invalid TikTok profile identity response: '
            f'requested={requested!r} returned={returned!r}',
        )


class TikTokVideoListResponseError(RuntimeError):
    '''TikTok omitted videos that the public profile reports.'''

    def __init__(self, username: str, video_count: int) -> None:
        super().__init__(
            'invalid TikTok video list response: '
            f'username={username!r} profile_video_count={video_count}',
        )


def _proxy_endpoint(proxy: str) -> str:
    '''Credential-free proxy endpoint for logs.'''
    parsed = urlparse(proxy)
    host: str = parsed.hostname or proxy
    return (
        f'{host}:{parsed.port}'
        if parsed.port is not None else host
    )


class CreatorSettings(TikTokScraperSettings):
    '''
    Configuration for ``tt_creator_scrape.py``. Adds the tool's
    process/metrics/queue knobs on top of the shared TikTok scraper
    settings (proxies, redis_dsn, session_state_dir, ms_token_*,
    creator_data_directory).
    '''

    metrics_port: int = Field(
        default=9300,
        validation_alias=AliasChoices(
            'TIKTOK_CREATOR_METRICS_PORT', 'metrics_port',
        ),
        description='Prometheus metrics port (supervisor base).',
    )
    creator_concurrency: int = Field(
        default=0,
        validation_alias=AliasChoices(
            'TIKTOK_CREATOR_CONCURRENCY', 'creator_concurrency',
        ),
        description=(
            'Fleet-wide async worker upper bound. Set to 0 or leave '
            'unset to use the proxy count. In multi-process mode this '
            'budget is split across child processes; each proxy still '
            'allows at most one in-flight TikTok call.'
        ),
    )
    creator_num_processes: int = Field(
        default=1,
        validation_alias=AliasChoices(
            'TIKTOK_CREATOR_NUM_PROCESSES',
            'creator_num_processes',
        ),
        description=(
            'Supervisor child count. When > 1 the invocation '
            'becomes a supervisor that splits PROXIES into disjoint '
            'slices, one child per slice (own browsers + metrics '
            'port).'
        ),
    )
    creator_log_file: str = Field(
        default='/dev/stdout',
        validation_alias=AliasChoices(
            'TIKTOK_CREATOR_LOG_FILE', 'creator_log_file',
            'LOG_FILE', 'log_file',
        ),
        description=(
            'Log file path for the TikTok creator scraper. Honours '
            'TIKTOK_CREATOR_LOG_FILE first so this scraper can write '
            'to its own file; falls back to LOG_FILE when the scraper-'
            'specific var is unset.'
        ),
    )
    creator_log_level: str = Field(
        default='INFO',
        validation_alias=AliasChoices(
            'TIKTOK_CREATOR_LOG_LEVEL', 'creator_log_level',
            'LOG_LEVEL', 'log_level',
        ),
        description=(
            'Logging level for the TikTok creator scraper '
            '(DEBUG, INFO, WARNING, ERROR, CRITICAL). Honours '
            'TIKTOK_CREATOR_LOG_LEVEL first so this scraper can be '
            'dialled up independently; falls back to LOG_LEVEL when '
            'the scraper-specific var is unset.'
        ),
    )
    creator_priority_queues: str = Field(
        default='168:1000000,336:100000,720:10000,4320:0',
        validation_alias=AliasChoices(
            'TIKTOK_CREATOR_PRIORITY_QUEUES',
            'creator_priority_queues',
        ),
        description=(
            'Comma-separated interval_hours:min_followers tiers, '
            'highest priority first; last tier must be :0.'
        ),
    )
    creator_claim_ttl_seconds: int = Field(
        default=600,
        validation_alias=AliasChoices(
            'TIKTOK_CREATOR_CLAIM_TTL', 'creator_claim_ttl_seconds',
        ),
        description=(
            'Per-creator claim TTL; the claim of a crashed worker '
            'expires after this and the creator is recovered.'
        ),
    )
    creator_queue_idle_poll_seconds: float = Field(
        default=30.0,
        validation_alias=AliasChoices(
            'TIKTOK_CREATOR_QUEUE_IDLE_POLL',
            'creator_queue_idle_poll_seconds',
        ),
        description='Sleep ceiling when nothing is due.',
    )
    creator_retry_interval_seconds: float = Field(
        default=300.0,
        validation_alias=AliasChoices(
            'TIKTOK_CREATOR_RETRY_INTERVAL',
            'creator_retry_interval_seconds',
        ),
        description=(
            'Floor on the next-check delay after a transient '
            'failure.'
        ),
    )
    creator_rate_limit_retry_interval_seconds: float = Field(
        default=1800.0,
        validation_alias=AliasChoices(
            'TIKTOK_CREATOR_RATE_LIMIT_RETRY_INTERVAL',
            'creator_rate_limit_retry_interval_seconds',
        ),
        description=(
            'Floor on the next-check delay after TikTok bot-detection '
            'or rate-limit responses. Kept much longer than ordinary '
            'transient retry delay to avoid retry storms.'
        ),
    )
    creator_retry_jitter_fraction: float = Field(
        default=0.25,
        validation_alias=AliasChoices(
            'TIKTOK_CREATOR_RETRY_JITTER_FRACTION',
            'creator_retry_jitter_fraction',
        ),
        description=(
            'Random +/- fraction applied to retry delays so many '
            'failed creators do not become due at the same instant.'
        ),
    )
    creator_bot_failure_threshold: int = Field(
        default=1,
        validation_alias=AliasChoices(
            'TIKTOK_CREATOR_BOT_FAILURE_THRESHOLD',
            'creator_bot_failure_threshold',
        ),
        description=(
            'Consecutive bot-detection responses from one proxy '
            'before rebuilding its browser session.'
        ),
    )
    creator_bot_cooldown_seconds: float = Field(
        default=1800.0,
        validation_alias=AliasChoices(
            'TIKTOK_CREATOR_BOT_COOLDOWN',
            'creator_bot_cooldown_seconds',
        ),
        description=(
            'Delay after rebuilding a bot-detected proxy before it '
            'may claim another creator.'
        ),
    )
    creator_bot_cooldown_max_seconds: float = Field(
        default=3600.0,
        validation_alias=AliasChoices(
            'TIKTOK_CREATOR_BOT_COOLDOWN_MAX',
            'creator_bot_cooldown_max_seconds',
        ),
        description=(
            'Maximum exponential cooldown for a proxy repeatedly '
            'hitting TikTok bot detection.'
        ),
    )
    creator_video_ref_count: int = Field(
        default=0,
        validation_alias=AliasChoices(
            'TIKTOK_CREATOR_VIDEO_REF_COUNT',
            'creator_video_ref_count',
        ),
        description=(
            'Maximum number of video ids/URLs to collect from each '
            'profile collection: videos, reposts, and liked posts. '
            '0 means unlimited (paginate until TikTok has no more).'
        ),
    )
    creator_playlist_ref_count: int = Field(
        default=20,
        validation_alias=AliasChoices(
            'TIKTOK_CREATOR_PLAYLIST_REF_COUNT',
            'creator_playlist_ref_count',
        ),
        description=(
            'Maximum number of playlist references to collect from '
            'each creator profile.'
        ),
    )
    creator_short_url_resolve_timeout_seconds: float = Field(
        default=10.0,
        validation_alias=AliasChoices(
            'TIKTOK_CREATOR_SHORT_URL_RESOLVE_TIMEOUT',
            'creator_short_url_resolve_timeout_seconds',
        ),
        description=(
            'HTTP timeout when resolving a vm/vt.tiktok.com short '
            'link to a creator handle.'
        ),
    )
    creator_short_url_retry_interval_seconds: float = Field(
        default=300.0,
        validation_alias=AliasChoices(
            'TIKTOK_CREATOR_SHORT_URL_RETRY_INTERVAL',
            'creator_short_url_retry_interval_seconds',
        ),
        description=(
            'Fixed delay before retrying a transient short-URL '
            'resolution failure (tier-independent).'
        ),
    )
    creator_short_url_user_agent: str = Field(
        default=(
            'Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) '
            'AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 '
            'Mobile/15E148 Safari/604.1'
        ),
        validation_alias=AliasChoices(
            'TIKTOK_CREATOR_SHORT_URL_USER_AGENT',
            'creator_short_url_user_agent',
        ),
        description='User-Agent for short-URL resolution requests.',
    )

    @field_validator('creator_log_level', mode='before')
    @classmethod
    def _normalize_creator_log_level(cls, v: str) -> str:
        return normalize_log_level(v)


def _validate_settings(settings: CreatorSettings) -> None:
    '''
    Validate runtime prerequisites; exit 1 with a message on any
    violation.
    '''
    if not settings.redis_dsn:
        print(
            'Error: Redis must be configured via '
            '--redis-dsn or REDIS_DSN',
        )
        sys.exit(1)
    if not settings.creator_data_directory:
        print(
            'Error: scraped creator data directory must be set via '
            '--creator-data-directory or TIKTOK_CREATOR_DATA_DIR',
        )
        sys.exit(1)
    if not os.path.isdir(settings.creator_data_directory):
        print(
            f'Directory {settings.creator_data_directory} does not '
            'exist. It will be created.',
        )
        os.makedirs(
            settings.creator_data_directory, exist_ok=True,
        )


def _creator_filename(username: str) -> str:
    return f'{CREATOR_FILE_PREFIX}{username}{CREATOR_FILE_POSTFIX}'


def _effective_creator_concurrency(
    settings: CreatorSettings,
    ready_proxy_count: int,
) -> int:
    '''Return the hard per-process creator worker count.'''
    if ready_proxy_count <= 0:
        return 1
    requested: int = _resolve_creator_concurrency(
        settings, ready_proxy_count,
    )
    return max(1, min(requested, ready_proxy_count))


def _auto_creator_concurrency(
    proxy_count: int, num_processes: int,
) -> int:
    '''Derive fleet-wide concurrency from proxy count.'''
    if proxy_count <= 0:
        return 1
    return max(int(proxy_count), 1)


def _resolve_creator_concurrency(
    settings: CreatorSettings,
    proxy_count: int,
) -> int:
    '''Return explicit fleet-wide concurrency or the auto value.'''
    requested: int = int(settings.creator_concurrency)
    if requested > 0:
        return requested
    return _auto_creator_concurrency(
        proxy_count, settings.creator_num_processes,
    )


def _effective_process_count(
    requested_processes: int,
    total_concurrency: int,
    proxy_count: int,
) -> int:
    '''Return process count that cannot exceed worker/proxy capacity.'''
    requested: int = max(int(requested_processes), 1)
    capacity: int = max(int(total_concurrency), 1)
    if proxy_count > 0:
        capacity = min(capacity, int(proxy_count))
    return max(1, min(requested, capacity))


def _jittered_retry_delay(
    base_seconds: float,
    jitter_fraction: float,
) -> float:
    '''Apply bounded symmetric jitter to a retry/cooldown delay.'''
    base: float = max(float(base_seconds), 0.0)
    fraction: float = max(float(jitter_fraction), 0.0)
    if base == 0.0 or fraction == 0.0:
        return base
    spread: float = base * fraction
    return max(0.0, base + random.uniform(-spread, spread))


def _build_creator_identity_maps(
    settings: CreatorSettings,
) -> CreatorIdentityMaps:
    '''
    Build Redis-backed TikTok creator identity maps.

    Redis keys mirror the YouTube channel identity maps:
    ``tiktok:creator_map`` stores user_id -> username,
    ``tiktok:handle_map`` stores username -> user_id, and
    ``tiktok:name_map`` stores nickname -> sec_uid.
    '''
    user_id_to_username: RedisCreatorMap = RedisCreatorMap(
        settings.redis_dsn, platform=PLATFORM,
    )
    return CreatorIdentityMaps(
        user_id_to_username=user_id_to_username,
        username_to_user_id=RedisHandleMap(
            user_id_to_username.redis_client,
            platform=PLATFORM,
        ),
        nickname_to_sec_uid=RedisNameMap(
            settings.redis_dsn, platform=PLATFORM,
        ),
    )


async def _persist_creator_identity(
    creator: TikTokCreator,
    identity_maps: CreatorIdentityMaps,
) -> None:
    '''
    Persist identity lookups learned from a successful creator scrape.
    '''
    await identity_maps.user_id_to_username.put(
        creator.user_id, creator.username,
    )
    await identity_maps.username_to_user_id.put(
        creator.username, creator.user_id,
    )
    if creator.nickname:
        await identity_maps.nickname_to_sec_uid.put(
            creator.nickname, creator.sec_uid,
        )


def _creator_video_urls(creator: TikTokCreator) -> list[str]:
    '''Return unique video URLs discovered from a creator scrape.'''
    seen: set[str] = set()
    out: list[str] = []
    for refs in (creator.videos, creator.reposts, creator.liked):
        for ref in refs:
            video_url: str = ref.url
            if video_url and video_url not in seen:
                seen.add(video_url)
                out.append(video_url)
    return out


async def _enqueue_creator_videos(
    creator: TikTokCreator,
    video_queue: RedisVideoScrapeQueue,
) -> int:
    '''Queue videos discovered on a successfully scraped creator.'''
    enqueued: int = 0
    for video_url in _creator_video_urls(creator):
        try:
            if await video_queue.enqueue(
                video_url, source='tiktok_creator',
            ):
                enqueued += 1
                METRIC_SCRAPE_QUEUE_ENQUEUES.labels(
                    platform=PLATFORM,
                    scraper=SCRAPER_LABEL,
                    entity='video',
                    source='tiktok_creator',
                ).inc()
        except Exception as exc:
            _LOGGER.warning(
                'Failed to enqueue TikTok creator video',
                extra={
                    'username': creator.username,
                    'video_url': video_url,
                    'error': str(exc),
                },
            )
    return enqueued


async def _handle_failure(
    exc: BaseException,
    username: str,
    queue: CreatorQueue,
    settings: CreatorSettings,
    proxy_endpoint: str | None = None,
) -> str:
    '''
    Classify *exc* and act: unavailable creators are removed from the
    queue (so they stop re-enqueuing); transient/rate-limit/auth
    errors are released with a retry floor; everything else is
    released at the normal tier interval. Returns the reason.
    '''
    reason: str = classify_tiktok_error(exc)
    if reason == 'unavailable':
        await queue.remove(username)
    elif reason in ('transient', 'rate_limit', 'auth'):
        retry_interval: float = settings.creator_retry_interval_seconds
        if reason == 'rate_limit':
            retry_interval = (
                settings.creator_rate_limit_retry_interval_seconds
            )
        retry_interval = _jittered_retry_delay(
            retry_interval,
            settings.creator_retry_jitter_fraction,
        )
        await queue.release(
            username,
            retry_interval_seconds=retry_interval,
        )
        METRIC_SCRAPE_RETRIES.labels(
            platform=PLATFORM,
            scraper=SCRAPER_LABEL,
            entity=ENTITY,
            api=API_LABEL,
            reason=reason,
        ).inc()
    else:
        await queue.release(username)
    _LOGGER.warning(
        'Creator scrape failed',
        extra={
            'username': username,
            'reason': reason,
            'error': str(exc),
            'proxy_endpoint': proxy_endpoint,
        },
    )
    return reason


def _video_ref_from_payload(payload: dict) -> TikTokVideoRef | None:
    '''Build a compact video reference from a TikTok item payload.'''
    video_id_raw: object = payload.get('id')
    if video_id_raw is None:
        return None
    video_id: str = str(video_id_raw)
    if not video_id:
        return None
    author: object = payload.get('author')
    username: str | None = None
    if isinstance(author, dict):
        author_username: object = author.get('uniqueId')
        if isinstance(author_username, str) and author_username:
            username = author_username
    elif isinstance(author, str) and author:
        username = author
    if not username:
        return None
    return TikTokVideoRef(
        video_id=video_id,
        username=username,
        url=f'https://www.tiktok.com/@{username}/video/{video_id}',
    )


async def _collect_video_refs_from_iterator(
    iterator: Any,
    count: int,
) -> list[TikTokVideoRef]:
    '''Collect compact refs from a TikTokApi async video iterator.
    ``count <= 0`` means unlimited.'''
    limited: bool = count > 0
    refs: list[TikTokVideoRef] = []
    async for video in iterator:
        payload: dict | None = getattr(video, 'as_dict', None)
        if not isinstance(payload, dict):
            continue
        ref: TikTokVideoRef | None = _video_ref_from_payload(payload)
        if ref is not None:
            refs.append(ref)
        if limited and len(refs) >= count:
            break
    return refs


async def _collect_repost_refs(
    api: Any,
    sec_uid: str,
    count: int,
) -> list[TikTokVideoRef]:
    '''
    Collect reposted videos via TikTok's web item-list endpoint.
    TikTokApi 7.1.0 exposes User.videos()/liked(), but not reposts.
    ``count <= 0`` means unlimited.
    '''
    limited: bool = count > 0
    refs: list[TikTokVideoRef] = []
    cursor: int = 0
    while not limited or len(refs) < count:
        resp: dict | None = await api.make_request(
            url=REPOST_ITEM_LIST_URL,
            params={
                'secUid': sec_uid,
                'count': (
                    min(35, count - len(refs))
                    if limited else 35
                ),
                'cursor': cursor,
            },
        )
        if resp is None:
            raise RuntimeError('TikTok returned an empty repost list')
        items: list = resp.get('itemList', [])
        if not items:
            break
        for item in items:
            if not isinstance(item, dict):
                continue
            ref: TikTokVideoRef | None = _video_ref_from_payload(item)
            if ref is not None:
                refs.append(ref)
            if limited and len(refs) >= count:
                break
        if not resp.get('hasMore', False):
            break
        next_cursor: int = int(resp.get('cursor', cursor))
        if next_cursor == cursor:
            break
        cursor = next_cursor
    return refs


def _playlist_ref_from_object(playlist: Any) -> TikTokPlaylistRef | None:
    '''Build a compact playlist reference from a TikTokApi Playlist.'''
    playlist_id_raw: object = getattr(playlist, 'id', None)
    if playlist_id_raw is None:
        return None
    playlist_id: str = str(playlist_id_raw)
    if not playlist_id:
        return None
    video_count: object = getattr(playlist, 'video_count', None)
    return TikTokPlaylistRef(
        playlist_id=playlist_id,
        name=getattr(playlist, 'name', None),
        video_count=(
            int(video_count) if video_count is not None else None
        ),
        cover_url=getattr(playlist, 'cover_url', None),
    )


async def _collect_playlist_refs_from_iterator(
    iterator: Any,
    count: int,
) -> list[TikTokPlaylistRef]:
    '''Collect compact refs from a TikTokApi async playlist iterator.'''
    refs: list[TikTokPlaylistRef] = []
    async for playlist in iterator:
        ref: TikTokPlaylistRef | None = _playlist_ref_from_object(
            playlist,
        )
        if ref is not None:
            refs.append(ref)
        if len(refs) >= count:
            break
    return refs


async def _scrape_one(
    api: Any, username: str, settings: CreatorSettings,
) -> TikTokCreator:
    '''
    Issue the live User.info call, then collect compact video refs
    for public videos, reposts, and liked posts where available.
    '''
    user: Any = api.user(username=username)
    try:
        resp: dict = await user.info()
    except KeyError as exc:
        response: object = getattr(user, 'as_dict', None)
        raise TikTokProfileResponseError(
            response, exc.args[0] if exc.args else None,
        ) from exc
    creator: TikTokCreator = TikTokCreator.from_user_info(resp)
    if creator.username.casefold() != username.casefold():
        raise TikTokProfileIdentityError(
            username, creator.username,
        )
    if creator.private_account:
        return creator

    video_count: int = max(0, settings.creator_video_ref_count)
    api_video_count: int = (
        video_count if video_count > 0 else sys.maxsize
    )
    playlist_count: int = max(
        0, settings.creator_playlist_ref_count,
    )

    creator.videos = await _collect_video_refs_from_iterator(
        user.videos(count=api_video_count), video_count,
    )
    if creator.video_count > 0 and not creator.videos:
        raise TikTokVideoListResponseError(
            creator.username, creator.video_count,
        )
    creator.reposts = await _collect_repost_refs(
        api, creator.sec_uid, video_count,
    )
    try:
        creator.liked = await _collect_video_refs_from_iterator(
            user.liked(count=api_video_count), video_count,
        )
    except Exception as exc:
        _LOGGER.info(
            'Liked videos unavailable for creator',
            extra={'username': username, 'error': str(exc)},
        )
    if playlist_count > 0:
        creator.playlists = (
            await _collect_playlist_refs_from_iterator(
                user.playlists(count=playlist_count), playlist_count,
            )
        )
    return creator


async def _process_creator(
    username: str,
    proxy: str,
    pool: TikTokSessionPool,
    queue: CreatorQueue,
    video_queue: RedisVideoScrapeQueue,
    fm: AssetFileManagement,
    settings: CreatorSettings,
    worker_id: str,
    proxy_ip: str,
    identity_maps: CreatorIdentityMaps,
) -> str | None:
    '''
    Scrape one claimed creator through *proxy*, persist the record,
    re-tier by the freshly measured follower count, and release the
    claim. Failures are classified and the claim is released/removed
    accordingly — a bad creator never kills the worker.
    '''
    start: float = time.monotonic()
    try:
        async with pool.session_for(proxy) as api:
            creator: TikTokCreator = await _scrape_one(
                api, username, settings,
            )
        await fm.write_file(
            _creator_filename(creator.username), creator.to_dict(),
        )
        METRIC_SCRAPE_RECORDS_WRITTEN.labels(
            platform=PLATFORM,
            scraper=SCRAPER_LABEL,
            entity=ENTITY,
        ).inc()
        await _persist_creator_identity(creator, identity_maps)
        enqueued_videos: int = await _enqueue_creator_videos(
            creator, video_queue,
        )
        if enqueued_videos:
            _LOGGER.info(
                'Queued TikTok creator videos for scrape',
                extra={
                    'username': username,
                    'video_count': enqueued_videos,
                },
            )
        await queue.update_tier(username, creator.follower_count)
        await queue.release(username)
        METRIC_SCRAPES_COMPLETED.labels(
            platform=PLATFORM, scraper=SCRAPER_LABEL, entity=ENTITY,
            api=API_LABEL, worker_id=worker_id, proxy_ip=proxy_ip,
            proxy_file='',
        ).inc()
        METRIC_SCRAPE_DURATION.labels(
            platform=PLATFORM, scraper=SCRAPER_LABEL, entity=ENTITY,
            api=API_LABEL, outcome='success', worker_id=worker_id,
        ).observe(time.monotonic() - start)
        return None
    except Exception as exc:
        reason: str = await _handle_failure(
            exc, username, queue, settings,
            proxy_endpoint=_proxy_endpoint(proxy),
        )
        METRIC_SCRAPE_FAILURES.labels(
            platform=PLATFORM, scraper=SCRAPER_LABEL, entity=ENTITY,
            api=API_LABEL, reason=reason, worker_id=worker_id,
            proxy_ip=proxy_ip, proxy_file='',
        ).inc()
        METRIC_SCRAPE_DURATION.labels(
            platform=PLATFORM, scraper=SCRAPER_LABEL, entity=ENTITY,
            api=API_LABEL, outcome='failure', worker_id=worker_id,
        ).observe(time.monotonic() - start)
        return reason


def _sleep_until(due: float | None, idle_poll: float) -> float:
    '''Seconds to sleep: until the next due creator, bounded by
    *idle_poll*; *idle_poll* when nothing is queued.'''
    if due is None:
        return idle_poll
    delta: float = due - time.time()
    return max(0.0, min(delta, idle_poll))


def _short_url_graduation_weight(settings: CreatorSettings) -> int:
    '''Follower count routing a graduated handle to the next-to-lowest
    tier (matches the operator import default); re-tiered on first
    real scrape.'''
    tiers: list[TierConfig] = parse_priority_queues(
        settings.creator_priority_queues,
    )
    tier: TierConfig = tiers[-2] if len(tiers) >= 2 else tiers[-1]
    return max(tier.min_subscribers, 0)


async def _resolve_and_enqueue_short_url(
    short_url: str,
    proxy: str,
    queue: CreatorQueue,
    settings: CreatorSettings,
    worker_id: str,
    proxy_ip: str,
) -> None:
    '''Resolve a vm/vt short URL to a handle, graduate the handle into
    the queue, and discard the alias. Never raises — a bad short URL
    must not kill the worker.'''
    try:
        res = await resolve_creator_short_url(
            short_url,
            proxy=proxy,
            timeout=settings.creator_short_url_resolve_timeout_seconds,
            user_agent=settings.creator_short_url_user_agent,
        )
    except Exception as exc:  # defensive: resolver shouldn't raise
        _LOGGER.warning(
            'Short URL resolution crashed',
            extra={'short_url': short_url, 'error': str(exc)},
        )
        await queue.reschedule_in(
            short_url,
            settings.creator_short_url_retry_interval_seconds,
        )
        METRIC_TIKTOK_SHORT_URL_RESOLUTIONS.labels(
            platform=PLATFORM, scraper=SCRAPER_LABEL, entity=ENTITY,
            outcome=ShortUrlOutcome.TRANSIENT.value,
        ).inc()
        return

    if res.outcome is ShortUrlOutcome.RESOLVED and res.handle:
        await queue.schedule_if_absent(
            res.handle, res.handle,
            _short_url_graduation_weight(settings),
        )
        await queue.discard_member(short_url)
    elif res.outcome is ShortUrlOutcome.TRANSIENT:
        await queue.reschedule_in(
            short_url,
            settings.creator_short_url_retry_interval_seconds,
        )
    else:
        await queue.discard_member(short_url)

    METRIC_TIKTOK_SHORT_URL_RESOLUTIONS.labels(
        platform=PLATFORM, scraper=SCRAPER_LABEL, entity=ENTITY,
        outcome=res.outcome.value,
    ).inc()
    _LOGGER.info(
        'Resolved TikTok short URL',
        extra={
            'short_url': short_url,
            'outcome': res.outcome.value,
            'handle': res.handle,
            'proxy_ip': proxy_ip,
            'worker_id': worker_id,
        },
    )


async def _proxy_worker(
    proxy: str,
    pool: TikTokSessionPool,
    queue: CreatorQueue,
    video_queue: RedisVideoScrapeQueue,
    fm: AssetFileManagement,
    settings: CreatorSettings,
    shutdown_event: asyncio.Event,
    worker_id: str,
    identity_maps: CreatorIdentityMaps,
) -> None:
    '''
    One async worker bound to *proxy*. Claims one due creator at a
    time from the shared queue and scrapes it through this proxy's
    session, sleeping until the next is due when the queue is empty.
    Touches the watchdog every wave (incl. idle) so a healthy idle
    worker is never reaped.
    '''
    proxy_ip: str = extract_proxy_ip(proxy)
    consecutive_bot_failures: int = 0
    circuit_events: int = 0
    _LOGGER.info(
        'TikTok creator proxy worker started',
        extra={
            'proxy_endpoint': _proxy_endpoint(proxy),
            'worker_id': worker_id,
        },
    )
    while not shutdown_event.is_set():
        Watchdog.get().touch_work()
        try:
            batch: list[tuple[str, str, float]] = (
                await queue.claim_batch(
                    1, worker_id,
                    claim_ttl=settings.creator_claim_ttl_seconds,
                )
            )
        except Exception as exc:
            _LOGGER.warning(
                'claim_batch failed',
                extra={'proxy_ip': proxy_ip, 'error': str(exc)},
            )
            await asyncio.sleep(
                settings.creator_queue_idle_poll_seconds,
            )
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
        if is_tiktok_short_url(username):
            await pool.gate_api_request(proxy)
            await _resolve_and_enqueue_short_url(
                username, proxy, queue, settings, worker_id, proxy_ip,
            )
            continue
        outcome: str | None = await _process_creator(
            username, proxy, pool, queue, video_queue, fm, settings,
            worker_id, proxy_ip, identity_maps,
        )
        if outcome != 'rate_limit':
            consecutive_bot_failures = 0
            continue

        consecutive_bot_failures += 1
        threshold: int = max(
            settings.creator_bot_failure_threshold, 1,
        )
        if consecutive_bot_failures < threshold:
            continue

        _LOGGER.warning(
            'TikTok proxy bot-detection circuit opened; '
            'quarantining session',
            extra={
                'proxy_endpoint': _proxy_endpoint(proxy),
                'consecutive_failures': consecutive_bot_failures,
                'cooldown_seconds': (
                    min(
                        settings.creator_bot_cooldown_seconds
                        * (2 ** circuit_events),
                        settings.creator_bot_cooldown_max_seconds,
                    )
                ),
            },
        )
        await pool.quarantine(proxy)
        cooldown_seconds: float = _jittered_retry_delay(
            min(
                settings.creator_bot_cooldown_seconds
                * (2 ** circuit_events),
                settings.creator_bot_cooldown_max_seconds,
            ),
            settings.creator_retry_jitter_fraction,
        )
        circuit_events += 1
        consecutive_bot_failures = 0
        _LOGGER.info(
            'TikTok proxy bot-detection cooldown sleeping',
            extra={
                'proxy_endpoint': _proxy_endpoint(proxy),
                'cooldown_seconds': cooldown_seconds,
                'circuit_events': circuit_events,
            },
        )
        await asyncio.sleep(cooldown_seconds)
        rebuilt: bool = await pool.rebuild(proxy)
        if not rebuilt:
            _LOGGER.error(
                'TikTok proxy session rebuild failed; retiring worker',
                extra={'proxy_endpoint': _proxy_endpoint(proxy)},
            )
            return
        _LOGGER.info(
            'TikTok proxy session rebuild succeeded; worker resumed',
            extra={
                'proxy_endpoint': _proxy_endpoint(proxy),
                'circuit_events': circuit_events,
            },
        )


async def _maintenance_loop(
    queue: CreatorQueue,
    settings: CreatorSettings,
    worker_id: str,
    shutdown_event: asyncio.Event,
) -> None:
    '''
    Periodic queue housekeeping: recover claims orphaned by crashed
    workers and publish per-tier queue sizes. Touches the watchdog
    each pass.
    '''
    while not shutdown_event.is_set():
        Watchdog.get().touch_work()
        try:
            await queue.cleanup_stale_claims()
            sizes: dict[int, int] = (
                await queue.queue_sizes_by_tier()
            )
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
        except Exception as exc:
            _LOGGER.warning(
                'maintenance loop pass failed',
                extra={'error': str(exc)},
            )
        await asyncio.sleep(settings.creator_queue_idle_poll_seconds)


def _build_queue(
    settings: CreatorSettings,
    worker_id: str,
    tiers: list[TierConfig],
) -> CreatorQueue:
    '''Build the Redis-backed creator queue.'''
    queue: RedisCreatorQueue = RedisCreatorQueue(
        settings.redis_dsn, worker_id, PLATFORM,
        key_namespace='scrape',
    )
    queue._tiers = tiers
    queue._key_queues = queue._build_queue_keys(tiers)
    return queue


def _build_video_queue(
    settings: CreatorSettings,
) -> RedisVideoScrapeQueue:
    '''Build the Redis-backed TikTok video scrape queue.'''
    redis = redis_from_url(
        settings.redis_dsn,
        component='tiktok-creator-video-queue',
        decode_responses=True,
    )
    return RedisVideoScrapeQueue(
        redis, VideoScrapeQueueSettings(), platform=PLATFORM,
    )


def _raise_fd_limit() -> None:
    '''Raise the open-file soft limit to the hard limit (one Camoufox
    Firefox per proxy plus sockets is fd-heavy).'''
    hard: int
    _, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    target: int = (
        hard if hard != resource.RLIM_INFINITY else _FD_TARGET
    )
    resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))


async def _run_worker(ctx: ScraperRunContext) -> None:
    '''
    Leaf worker of the supervisor tree. Bootstraps one Camoufox
    browser per proxy in this child's slice, attaches to the Redis
    priority queue, then runs one worker per ready proxy plus the ms_token
    refresh and queue-maintenance background tasks until shutdown.
    '''
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
    identity_maps: CreatorIdentityMaps = _build_creator_identity_maps(
        settings,
    )
    proxy_limit: int = _effective_creator_concurrency(
        settings, len(ctx.proxies),
    )
    pool_proxies: list[str] = ctx.proxies[:proxy_limit]
    pool: TikTokSessionPool = TikTokSessionPool(
        proxies=pool_proxies,
        state_dir=settings.session_state_dir,
        ms_token_ttl_seconds=settings.ms_token_ttl_seconds,
        rate_limiter=ctx.rate_limiter,
        scraper_label=SCRAPER_LABEL,
        api_call_type=TikTokCallType.CREATOR_API,
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
        return

    queue: CreatorQueue = _build_queue(settings, worker_id, tiers)
    video_queue: RedisVideoScrapeQueue = _build_video_queue(settings)

    shutdown_event: asyncio.Event = asyncio.Event()
    refresh_task: asyncio.Task = asyncio.create_task(
        pool.run_refresh_loop(
            settings.ms_token_refresh_interval_seconds,
        ),
    )
    maintenance_task: asyncio.Task = asyncio.create_task(
        _maintenance_loop(
            queue, settings, worker_id, shutdown_event,
        ),
    )
    active_proxies: list[str] = ready[
        :_effective_creator_concurrency(settings, len(ready))
    ]
    _LOGGER.info(
        'TikTok creator active proxy selection complete',
        extra={
            'requested': len(ctx.proxies),
            'selected': len(pool_proxies),
            'ready': len(ready),
            'active': len(active_proxies),
            'configured_concurrency': settings.creator_concurrency,
            'active_proxy_endpoints': [
                _proxy_endpoint(proxy) for proxy in active_proxies
            ],
            'idle_ready_proxy_count': max(
                len(ready) - len(active_proxies), 0,
            ),
            'worker_id': worker_id,
        },
    )
    worker_tasks: list[asyncio.Task] = [
        asyncio.create_task(
            _proxy_worker(
                proxy, pool, queue, video_queue, fm, settings,
                shutdown_event, worker_id, identity_maps,
            ),
        )
        for proxy in active_proxies
    ]
    _LOGGER.info(
        'TikTok creator scrape daemon running',
        extra={'workers': len(worker_tasks), 'worker_id': worker_id},
    )
    try:
        await asyncio.gather(*worker_tasks)
    finally:
        shutdown_event.set()
        refresh_task.cancel()
        maintenance_task.cancel()
        await asyncio.gather(
            refresh_task, maintenance_task,
            return_exceptions=True,
        )
        await video_queue._redis.aclose()
        await pool.shutdown()


def _build_tiktok_rate_limiter(
    settings: ScraperSettings,
) -> TikTokRateLimiter:
    return TikTokRateLimiter(
        state_dir=settings.rate_limiter_state_dir,
        redis_dsn=settings.redis_dsn,
    )


def main() -> None:
    '''Entry point: validate, then dispatch through ScraperRunner
    (supervisor when ``creator_num_processes > 1``, else worker).'''
    settings: CreatorSettings = CreatorSettings()
    _validate_settings(settings)
    proxy_count: int = len(settings.proxies)
    resolved_concurrency: int = _resolve_creator_concurrency(
        settings, len(settings.proxies),
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
    os.environ['TIKTOK_CREATOR_CONCURRENCY'] = str(resolved_concurrency)

    runner: ScraperRunner = ScraperRunner(
        settings=settings,
        scraper_label=SCRAPER_LABEL,
        platform=PLATFORM,
        num_processes=process_count,
        concurrency=resolved_concurrency,
        metrics_port=settings.metrics_port,
        log_file=settings.creator_log_file,
        log_level=settings.creator_log_level,
        rate_limiter_factory=_build_tiktok_rate_limiter,
        client_required=False,
        split_proxy_pool=True,
        concurrency_env_var='TIKTOK_CREATOR_CONCURRENCY',
        child_concurrencies=child_concurrencies,
    )
    sys.exit(runner.run_sync(_run_worker))


if __name__ == '__main__':
    main()
