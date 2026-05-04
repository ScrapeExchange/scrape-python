'''YouTube channel-id pre-resolver.

Operator tool. Reads channel-id-bearing inputs (the same
.lst files the channel scraper consumes, or any .jsonl with
a ``channel_id`` field), resolves each id to a handle via
InnerTube (BROWSE bucket), and writes the result to the
shared creator_map in Redis. Coordinated across processes
and hosts via a SETNX claim per channel id.

Run once before kicking off the channel scraper on a fresh
backlog; the scraper then sees every id pre-resolved and
spends its budget on actual channel scrapes rather than
serial resolution.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Awaitable, Callable

import redis.asyncio as aioredis
from httpx import AsyncClient
from prometheus_client import Counter, start_http_server
from pydantic import AliasChoices, Field

from scrape_exchange.channel_list_parsing import (
    parse_channel_handle,
)
from scrape_exchange.creator_map import CreatorMap, RedisCreatorMap
from scrape_exchange.file_management import CHANNEL_FILE_PREFIX
from scrape_exchange.redis_claim import RedisClaim
from scrape_exchange.settings import ScraperSettings
from scrape_exchange.youtube.exchange_channels_set import (
    RedisExchangeChannelsSet,
)
from scrape_exchange.youtube.youtube_channel import YouTubeChannel
from scrape_exchange.youtube.youtube_rate_limiter import (
    YouTubeRateLimiter,
)


PageFn = Callable[
    [str | None],
    Awaitable[tuple[list[str], str | None]],
]


_FILTER_API_PATH: str = '/api/v1/filter'
_DEFAULT_PAGE_SIZE: int = 1000
_WARM_HTTP_TIMEOUT: float = 60.0


METRIC_RESOLVER_OUTCOMES: Counter = Counter(
    'channel_id_resolver_outcomes_total',
    'Per-id outcomes from the pre-resolver tool',
    ['platform', 'tool', 'outcome'],
)


class ResolveChannelIdsSettings(ScraperSettings):
    '''Settings for the channel-id pre-resolver tool.

    Inherits ``ScraperSettings`` for the standard
    ``EXCHANGE_URL`` / ``REDIS_DSN`` / ``PROXIES`` /
    ``RATE_LIMITER_STATE_DIR`` / ``WORKER_ID`` / ``LOG_*``
    fields and the project's ``.env`` + CLI parsing config.
    Tool-specific fields below.'''

    input_paths: list[str] = Field(
        default_factory=list,
        validation_alias=AliasChoices(
            'INPUT_PATHS', 'input_paths',
        ),
        description=(
            'Files to scan for channel ids. .lst (one per '
            'line), .jsonl (json object per line with a '
            '``channel_id`` field), or a directory of either.'
        ),
    )
    concurrency: int = Field(
        default=30,
        validation_alias=AliasChoices(
            'CONCURRENCY', 'concurrency',
        ),
        description='Async tasks per process',
    )
    channel_data_directory: str = Field(
        default='',
        validation_alias=AliasChoices(
            'CHANNEL_DATA_DIRECTORY',
            'channel_data_directory',
        ),
        description=(
            'Channel-asset directory (used only to read '
            '.unresolved markers, not written to)'
        ),
    )
    metrics_port: int = Field(
        default=9650,
        validation_alias=AliasChoices(
            'METRICS_PORT', 'metrics_port',
        ),
        description='Prometheus port',
    )
    num_processes: int = Field(
        default=1,
        validation_alias=AliasChoices(
            'NUM_PROCESSES', 'num_processes',
        ),
        description=(
            'Process count. >1 currently logs a warning '
            'and runs single-process; operator may run '
            'multiple instances manually since the SETNX '
            'claim already deduplicates work cross-process.'
        ),
    )
    mode: str = Field(
        default='resolve',
        validation_alias=AliasChoices('MODE', 'mode'),
        description=(
            "Operation mode: 'resolve' (default) drains "
            "channel_id resolutions; 'warm-exchange-set' "
            "paginates scrape.exchange and seeds the SET."
        ),
    )
    warmer_username: str = Field(
        default='boinko',
        validation_alias=AliasChoices(
            'WARMER_USERNAME', 'warmer_username',
        ),
        description=(
            'Username whose uploaded channels seed the SET '
            "in --mode=warm-exchange-set."
        ),
    )
    warmer_stop_after_no_new_pages: int = Field(
        default=3,
        validation_alias=AliasChoices(
            'WARMER_STOP_AFTER_NO_NEW_PAGES',
            'warmer_stop_after_no_new_pages',
        ),
        description=(
            'Halt the warmer after this many consecutive '
            'pages contribute zero new handles. Guards '
            'against the filter-API overcount bug.'
        ),
    )


def _iter_channel_ids_from_paths(
    paths: list[str],
) -> set[str]:
    '''Read channel ids from a mixed list of .lst / .jsonl
    paths. Returns a deduplicated set. Bare handles, comments,
    URLs without a recoverable id, and other shapes are
    silently skipped — this tool only resolves channel_ids.'''
    ids: set[str] = set()
    for raw in paths:
        path: Path = Path(raw)
        if not path.exists():
            continue
        if path.suffix == '.jsonl':
            _ingest_jsonl(path, ids)
        else:
            _ingest_lst(path, ids)
    return ids


def _ingest_jsonl(path: Path, ids: set[str]) -> None:
    with path.open('r', encoding='utf-8') as f:
        for line in f:
            stripped: str = line.strip()
            if not stripped:
                continue
            try:
                obj: dict = json.loads(stripped)
            except json.JSONDecodeError:
                continue
            cid: str | None = obj.get('channel_id')
            if cid:
                ids.add(cid)


def _ingest_lst(path: Path, ids: set[str]) -> None:
    with path.open('r', encoding='utf-8') as f:
        for line in f:
            cid_or_none: str | None = parse_channel_handle(line)[1]
            if cid_or_none:
                ids.add(cid_or_none)


async def _filter_unresolved(
    ids: set[str],
    creator_map_backend: CreatorMap,
) -> set[str]:
    '''Keep only ids that creator_map does not already know.

    Uses per-id ``get`` so the call works for both
    ``RedisCreatorMap`` and ``FileCreatorMap``. Pipelines
    via ``asyncio.gather`` so the wall-clock cost is
    one Redis round-trip total when the backend pipelines
    internally — fine for the typical 50k input.'''
    if not ids:
        return set()
    items: list[str] = list(ids)
    results: list[str | None] = await asyncio.gather(*(
        creator_map_backend.get(i) for i in items
    ))
    return {
        items[i]
        for i in range(len(items))
        if not results[i]
    }


async def _resolve_one(
    channel_id: str,
    creator_map_backend: CreatorMap,
    claim: RedisClaim,
    channel_data_directory: str,
) -> str:
    '''Resolve a single channel_id. Returns one of
    ``resolved``, ``already_resolved``, ``lost_claim``,
    ``unresolvable``.

    The recheck after the claim is acquired avoids a wasted
    InnerTube call when a peer worker resolved this id between
    our pre-filter and our claim.'''
    won: bool = await claim.try_claim(channel_id)
    if not won:
        outcome: str = 'lost_claim'
    else:
        try:
            outcome = await _resolve_one_held(
                channel_id,
                creator_map_backend,
                channel_data_directory,
            )
        finally:
            await claim.release(channel_id)
    METRIC_RESOLVER_OUTCOMES.labels(
        platform='youtube',
        tool='yt_resolve_channel_ids',
        outcome=outcome,
    ).inc()
    return outcome


async def _resolve_one_held(
    channel_id: str,
    creator_map_backend: CreatorMap,
    channel_data_directory: str,
) -> str:
    '''Body that runs while the SETNX claim is held — split out
    so :func:`_resolve_one` only handles claim/release/metrics.'''
    existing: str | None = (
        await creator_map_backend.get(channel_id)
    )
    if existing:
        return 'already_resolved'

    unresolved_marker: str = os.path.join(
        channel_data_directory,
        f'{CHANNEL_FILE_PREFIX}{channel_id}.unresolved',
    )
    if os.path.exists(unresolved_marker):
        return 'unresolvable'

    name: str | None = (
        await YouTubeChannel.resolve_channel_id(channel_id)
    )
    if not name or ' ' in name:
        with open(unresolved_marker, 'w') as f:
            f.write(f'{channel_id}\n')
        return 'unresolvable'

    await creator_map_backend.put(channel_id, name)
    return 'resolved'


async def run_resolver(
    settings: ResolveChannelIdsSettings,
    redis_client: aioredis.Redis,
    creator_map_backend: CreatorMap,
) -> dict[str, int]:
    '''Drive the resolution work and return a stats dict.

    Side effects: writes resolved (channel_id, handle) pairs
    to ``creator_map_backend``; writes ``.unresolved`` markers
    under ``settings.channel_data_directory`` for ids that
    InnerTube cannot resolve. Touches no other state.
    '''
    candidate_ids: set[str] = (
        _iter_channel_ids_from_paths(
            settings.input_paths,
        )
    )
    stats: dict[str, int] = {
        'processed': 0,
        'resolved': 0,
        'skipped': 0,
        'failed': 0,
    }
    if not candidate_ids:
        logging.info(
            'No candidate channel ids found in inputs',
            extra={'input_paths': settings.input_paths},
        )
        return stats

    unresolved: set[str] = await _filter_unresolved(
        candidate_ids, creator_map_backend,
    )
    if not unresolved:
        logging.info(
            'All candidate ids already resolved in creator_map',
            extra={'candidates': len(candidate_ids)},
        )
        return stats

    claim: RedisClaim = RedisClaim(
        redis_client=redis_client,
        key_prefix='youtube:resolving:',
        ttl_seconds=60,
        owner=settings.worker_id,
    )

    semaphore: asyncio.Semaphore = (
        asyncio.Semaphore(settings.concurrency)
    )

    async def worker(cid: str) -> str:
        async with semaphore:
            return await _resolve_one(
                cid,
                creator_map_backend=creator_map_backend,
                claim=claim,
                channel_data_directory=(
                    settings.channel_data_directory
                ),
            )

    outcomes: list[str] = await asyncio.gather(
        *(worker(cid) for cid in unresolved),
        return_exceptions=False,
    )

    for outcome in outcomes:
        stats['processed'] += 1
        if outcome == 'resolved':
            stats['resolved'] += 1
        elif outcome in {
            'already_resolved', 'lost_claim',
            'unresolvable',
        }:
            stats['skipped'] += 1
        else:
            stats['failed'] += 1

    logging.info(
        'Pre-resolver completed',
        extra={'stats': stats},
    )
    return stats


async def _warm_from_api(
    exchange_set: RedisExchangeChannelsSet,
    page_fn: PageFn,
    stop_after_no_new_pages: int = 3,
) -> int:
    '''Paginate through the supplied ``page_fn``, adding each
    page's handles to ``exchange_set``. Stops after
    ``stop_after_no_new_pages`` consecutive pages produce no new
    handles — guards against the filter-API overcount bug that
    never reports done. Returns the number of pages that
    contributed at least one new handle.'''
    seen_total: set[str] = set()
    no_new_streak: int = 0
    cursor: str | None = None
    pages_added: int = 0
    while no_new_streak < stop_after_no_new_pages:
        page: list[str]
        next_cursor: str | None
        page, next_cursor = await page_fn(cursor)
        if not page:
            break
        new: set[str] = set(page) - seen_total
        if new:
            await exchange_set.add_many(new)
            seen_total |= new
            no_new_streak = 0
            pages_added += 1
        else:
            no_new_streak += 1
        cursor = next_cursor
    return pages_added


def _make_filter_api_page_fn(
    client: AsyncClient,
    exchange_url: str,
    username: str,
    page_size: int = _DEFAULT_PAGE_SIZE,
) -> PageFn:
    '''Build a :data:`PageFn` over scrape.exchange's
    ``POST /api/v1/filter`` for ``(youtube, channel, username)``.
    Returns each page's ``platform_creator_id`` values (the
    channel handle, with any leading ``@`` stripped — for YouTube
    channels the schema markers map ``channel_handle`` →
    ``platform_creator_id`` server-side) and the next cursor.'''
    url: str = f'{exchange_url}{_FILTER_API_PATH}'

    async def _page(
        cursor: str | None,
    ) -> tuple[list[str], str | None]:
        body: dict[str, Any] = {
            'username': username,
            'platform': 'youtube',
            'entity': 'channel',
            'first': page_size,
        }
        if cursor is not None:
            body['after'] = cursor
        resp = await client.post(
            url, json=body, timeout=_WARM_HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        payload: dict[str, Any] = resp.json()
        edges: list[dict[str, Any]] = (
            payload.get('edges', [])
        )
        handles: list[str] = []
        for edge in edges:
            node: dict[str, Any] = edge.get('node') or {}
            raw: str | None = node.get('platform_creator_id')
            if not raw:
                continue
            handle: str = raw.lstrip('@')
            if handle:
                handles.append(handle)
        page_info: dict[str, Any] = (
            payload.get('page_info') or {}
        )
        next_cursor: str | None = (
            page_info.get('end_cursor')
            if page_info.get('has_next_page') else None
        )
        return handles, next_cursor

    return _page


async def _async_main(
    settings: ResolveChannelIdsSettings,
) -> int:
    '''Single-process entry: init Redis + creator_map + rate
    limiter, drive the configured mode, return an exit code.'''
    if not settings.redis_dsn:
        logging.error(
            'redis_dsn is required; pre-resolver is '
            'Redis-only by design',
        )
        return 2

    redis_client: aioredis.Redis = aioredis.from_url(
        settings.redis_dsn, decode_responses=True,
    )
    start_http_server(settings.metrics_port)
    try:
        if settings.mode == 'warm-exchange-set':
            return await _run_warmer_mode(
                settings, redis_client,
            )
        if settings.mode == 'resolve':
            return await _run_resolve_mode(
                settings, redis_client,
            )
        logging.error(
            'Unknown mode',
            extra={'mode': settings.mode},
        )
        return 2
    finally:
        await redis_client.aclose()


async def _run_resolve_mode(
    settings: ResolveChannelIdsSettings,
    redis_client: aioredis.Redis,
) -> int:
    # Bind the YouTubeRateLimiter singleton to the production
    # Redis-backed BROWSE bucket so the pre-resolver shares per-
    # proxy budget with the live scrapers running on this host
    # and on peers. First .get() with these args wins; subsequent
    # calls in this process inherit the same backend.
    YouTubeRateLimiter.get(
        state_dir=settings.rate_limiter_state_dir or None,
        redis_dsn=settings.redis_dsn,
    )
    creator_map_backend: RedisCreatorMap = (
        RedisCreatorMap(
            settings.redis_dsn, platform='youtube',
        )
    )
    stats: dict[str, int] = await run_resolver(
        settings,
        redis_client=redis_client,
        creator_map_backend=creator_map_backend,
    )
    logging.info(
        'Pre-resolver final stats',
        extra={'stats': stats},
    )
    return 0


async def _run_warmer_mode(
    settings: ResolveChannelIdsSettings,
    redis_client: aioredis.Redis,
) -> int:
    exchange_set: RedisExchangeChannelsSet = (
        RedisExchangeChannelsSet(redis_client)
    )
    async with AsyncClient() as client:
        page_fn: PageFn = _make_filter_api_page_fn(
            client,
            settings.exchange_url,
            settings.warmer_username,
        )
        pages_added: int = await _warm_from_api(
            exchange_set,
            page_fn=page_fn,
            stop_after_no_new_pages=(
                settings.warmer_stop_after_no_new_pages
            ),
        )
    set_size: int = await exchange_set.size()
    logging.info(
        'Warmer added handles to exchange_channels SET',
        extra={
            'pages_added': pages_added,
            'set_size_after': set_size,
        },
    )
    return 0


_WARMER_ONLY_FIELDS: frozenset[str] = frozenset({
    'warmer_username',
    'warmer_stop_after_no_new_pages',
})
_RESOLVE_ONLY_FIELDS: frozenset[str] = frozenset({
    'input_paths',
})


def _mode_setting_conflict(
    settings: ResolveChannelIdsSettings,
) -> str | None:
    '''Return a human-readable error if ``--mode`` and the
    explicitly set CLI/env flags don't match; ``None`` when the
    combination is consistent. Catches the case where the operator
    sets a warmer flag (e.g. ``--warmer-username``) but forgets to
    set ``--mode warm-exchange-set``, which would otherwise
    silently run the resolve mode with empty inputs.'''
    set_fields: set[str] = settings.model_fields_set
    if settings.mode == 'warm-exchange-set':
        misplaced: set[str] = _RESOLVE_ONLY_FIELDS & set_fields
        if misplaced:
            return (
                f'These flags only apply with the default '
                f'--mode resolve: {sorted(misplaced)}. Drop '
                f'them, or remove --mode warm-exchange-set.'
            )
        return None
    misplaced = _WARMER_ONLY_FIELDS & set_fields
    if misplaced:
        return (
            f'These flags only apply with --mode '
            f'warm-exchange-set: {sorted(misplaced)}. Re-run '
            f'with --mode warm-exchange-set, or remove the '
            f'flags.'
        )
    return None


def main() -> None:
    '''Top-level entry. Loads settings from env + CLI, configures
    logging, and dispatches to ``_async_main``. ``num_processes``
    > 1 currently logs a warning and runs single-process — the
    SETNX claim already deduplicates work cross-process, so the
    operator can run multiple instances by hand for now.'''
    settings: ResolveChannelIdsSettings = (
        ResolveChannelIdsSettings()
    )
    logging.basicConfig(
        level=settings.log_level.upper(),
        format='%(asctime)s %(levelname)s %(name)s %(message)s',
    )
    conflict: str | None = _mode_setting_conflict(settings)
    if conflict is not None:
        logging.error(conflict)
        sys.exit(2)
    if settings.num_processes > 1:
        logging.warning(
            'num_processes > 1 is not yet implemented as an '
            'in-tool supervisor; running single-process. The '
            'SETNX claim deduplicates cross-process work, so '
            'the operator can launch additional instances '
            'manually if BROWSE budget allows.',
            extra={'num_processes': settings.num_processes},
        )
    sys.exit(asyncio.run(_async_main(settings)))


if __name__ == '__main__':
    main()
