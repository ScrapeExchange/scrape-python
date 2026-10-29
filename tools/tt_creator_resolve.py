#!/usr/bin/env python3
'''Resolve TikTok video ids to creators and queue those creators.

Reads video ids (one per line) from ``--input-file`` or from stdin,
fetches ``https://www.tiktok.com/@_/video/<id>`` through the configured
proxies, and parses the page's embedded rehydration payload with
``TikTokVideo.from_api`` to recover the author's username, numeric user
id and sec_uid. No browser is used: traffic goes through ``httpx2``
(HTTP/2) with a full desktop-browser header set, gated by the shared
``TikTokRateLimiter`` so it competes fairly with the live scrapers for
per-proxy tokens.

Every username found is then added to the TikTok creator scrape queue
-- the same ``RedisCreatorQueue`` (namespace ``scrape``, platform
``tiktok``) that ``tt_creator_scrape.py`` consumes -- so the scraper
picks the creators up on its next poll.

Examples::

    uv run python tools/tt_creator_resolve.py \
        --input-file tt_video_ids-2026-10.lst

    cut -d, -f1 videos.csv | uv run python tools/tt_creator_resolve.py

    uv run python tools/tt_creator_resolve.py \
        --input-file ids.lst --dry-run

``--dry-run`` resolves and reports without touching the queue. Requests
that fail transiently (timeouts, 403/429, bot-check pages) are retried
with backoff; pages that carry no item payload (deleted or
region-blocked videos) are reported as ``unavailable``.
'''

from __future__ import annotations

import asyncio
import json
import logging
import random
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx2
from pydantic import AliasChoices, Field
from pydantic_settings import SettingsConfigDict

from scrape_exchange.creator_queue import (
    RedisCreatorQueue,
    TierConfig,
    parse_priority_queues,
)
from scrape_exchange.logging import configure_logging
from scrape_exchange.tiktok import (
    TikTokCallType,
    TikTokRateLimiter,
    TikTokVideo,
)
from scrape_exchange.tiktok.settings import TikTokScraperSettings

_LOGGER: logging.Logger = logging.getLogger(__name__)

VIDEO_URL_TEMPLATE: str = 'https://www.tiktok.com/@_/video/{video_id}'
TIKTOK_ORIGIN: str = 'https://www.tiktok.com'
QUEUE_PLATFORM: str = 'tiktok'
QUEUE_NAMESPACE: str = 'scrape'

_DEFAULT_USER_AGENT: str = (
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/124.0.0.0 Safari/537.36'
)

# Outcomes that are worth another attempt.
_TRANSIENT_STATUSES: frozenset[int] = frozenset({403, 408, 425, 429})
# Markers of an anti-bot interstitial rather than a real video page.
_BOT_MARKERS: tuple[str, ...] = (
    'verify.tiktok.com',
    'captcha-verify',
    'please wait',
)

STATUS_RESOLVED: str = 'resolved'
STATUS_UNAVAILABLE: str = 'unavailable'
STATUS_TRANSIENT: str = 'transient'
STATUS_ERROR: str = 'error'


class ResolveSettings(TikTokScraperSettings):
    '''CLI / env settings for the video-to-creator resolver.'''

    model_config = SettingsConfigDict(
        env_file=str(Path(__file__).parent.parent / '.env'),
        env_file_encoding='utf-8',
        cli_parse_args=True,
        cli_implicit_flags=True,
        cli_kebab_case=True,
        populate_by_name=True,
        extra='ignore',
    )

    input_file: Path | None = Field(
        default=None,
        validation_alias=AliasChoices('INPUT_FILE', 'input_file'),
        description='File of video ids, one per line. Omit for stdin.',
    )
    output_file: Path | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'TIKTOK_RESOLVE_OUTPUT', 'output_file',
        ),
        description='Optional JSONL file for one record per video id.',
    )
    concurrency: int = Field(
        default=4,
        ge=1,
        le=64,
        validation_alias=AliasChoices(
            'TIKTOK_RESOLVE_CONCURRENCY', 'concurrency',
        ),
        description='Video pages fetched in parallel.',
    )
    request_timeout_seconds: float = Field(
        default=30.0,
        gt=0.0,
        validation_alias=AliasChoices(
            'TIKTOK_RESOLVE_TIMEOUT', 'request_timeout_seconds',
        ),
        description='Per-request HTTP timeout.',
    )
    max_retries: int = Field(
        default=3,
        ge=0,
        validation_alias=AliasChoices(
            'TIKTOK_RESOLVE_RETRIES', 'max_retries',
        ),
        description='Retries per video id on transient failures.',
    )
    max_ids: int | None = Field(
        default=None,
        ge=1,
        validation_alias=AliasChoices(
            'TIKTOK_RESOLVE_MAX_IDS', 'max_ids',
        ),
        description='Stop after this many video ids (for testing).',
    )
    user_agent: str = Field(
        default=_DEFAULT_USER_AGENT,
        validation_alias=AliasChoices(
            'TIKTOK_RESOLVE_USER_AGENT', 'user_agent',
        ),
        description='User-Agent sent with every request.',
    )
    enqueue: bool = Field(
        default=True,
        validation_alias=AliasChoices('ENQUEUE', 'enqueue'),
        description='Add resolved usernames to the creator queue.',
    )
    creator_priority_queues: str = Field(
        default='168:1000000,336:100000,720:10000,4320:0',
        validation_alias=AliasChoices(
            'TIKTOK_CREATOR_PRIORITY_QUEUES',
            'creator_priority_queues',
        ),
        description=(
            'Comma-separated interval_hours:min_followers tiers; must '
            'match tt_creator_scrape.py so tier numbers line up.'
        ),
    )
    creator_weight: int | None = Field(
        default=None,
        ge=0,
        validation_alias=AliasChoices(
            'TIKTOK_RESOLVE_CREATOR_WEIGHT', 'creator_weight',
        ),
        description=(
            'Follower count used to pick the queue tier. Default: the '
            'next-to-lowest tier, matching how graduated short URLs '
            'and operator imports are queued.'
        ),
    )
    dry_run: bool = Field(
        default=False,
        validation_alias=AliasChoices('DRY_RUN', 'dry_run'),
        description='Resolve and report, but never touch the queue.',
    )


@dataclass(slots=True)
class Resolution:
    '''Outcome of resolving one video id.'''

    video_id: int
    status: str
    username: str | None = None
    user_id: str | None = None
    sec_uid: str | None = None
    nickname: str | None = None
    detail: str | None = None

    def as_record(self) -> dict[str, Any]:
        '''Serialise to a JSONL-friendly dict.'''
        return {
            'video_id': self.video_id,
            'status': self.status,
            'username': self.username,
            'user_id': self.user_id,
            'sec_uid': self.sec_uid,
            'nickname': self.nickname,
            'detail': self.detail,
        }


def browser_headers(user_agent: str) -> dict[str, str]:
    '''Desktop-Chrome request headers for the public video page.'''
    return {
        'User-Agent': user_agent,
        'Accept': (
            'text/html,application/xhtml+xml,application/xml;q=0.9,'
            'image/avif,image/webp,image/apng,*/*;q=0.8,'
            'application/signed-exchange;v=b3;q=0.7'
        ),
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Cache-Control': 'max-age=0',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Ch-Ua': (
            '"Chromium";v="124", "Google Chrome";v="124", '
            '"Not-A.Brand";v="99"'
        ),
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"macOS"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Referer': f'{TIKTOK_ORIGIN}/',
        'DNT': '1',
        'Connection': 'keep-alive',
    }


def read_video_ids(path: Path | None) -> list[int]:
    '''Read video ids from *path*, or stdin when it is ``None``/``-``.

    Blank lines and ``#`` comments are skipped, ids are de-duplicated
    while preserving order, and unparsable lines are logged and
    ignored.
    '''
    if path is None or str(path) == '-':
        text: str = sys.stdin.read()
        origin: str = '<stdin>'
    else:
        text = path.read_text(encoding='utf-8')
        origin = str(path)
    ids: list[int] = []
    seen: set[int] = set()
    for line_no, line in enumerate(text.splitlines(), start=1):
        value: str = line.strip()
        if not value or value.startswith('#'):
            continue
        try:
            video_id: int = int(value)
        except ValueError:
            _LOGGER.warning(
                'Ignoring unparsable video id',
                extra={
                    'source': origin,
                    'line': line_no,
                    'value': value[:40],
                },
            )
            continue
        if video_id in seen:
            continue
        seen.add(video_id)
        ids.append(video_id)
    return ids


def _rehydration_scope(html: str) -> dict[str, Any] | None:
    '''Return ``__DEFAULT_SCOPE__`` from the page's embedded JSON.'''
    marker: str = 'id="__UNIVERSAL_DATA_FOR_REHYDRATION__"'
    index: int = html.find(marker)
    if index < 0:
        return None
    start: int = html.find('>', index)
    end: int = html.find('</script>', start)
    if start < 0 or end < 0:
        return None
    try:
        data: Any = json.loads(html[start + 1:end])
    except json.JSONDecodeError:
        return None
    if not isinstance(data, dict):
        return None
    scope: Any = data.get('__DEFAULT_SCOPE__')
    return scope if isinstance(scope, dict) else None


def parse_author(html: str, video_id: int) -> Resolution:
    '''Extract the creator of *video_id* from a fetched video page.'''
    scope: dict[str, Any] | None = _rehydration_scope(html)
    if scope is None:
        blocked: bool = any(
            marker in html[:8000].lower() for marker in _BOT_MARKERS
        )
        return Resolution(
            video_id=video_id,
            status=STATUS_TRANSIENT if blocked else STATUS_UNAVAILABLE,
            detail='bot check' if blocked else 'no rehydration payload',
        )
    detail_block: Any = scope.get('webapp.video-detail') or {}
    item: Any = (detail_block.get('itemInfo') or {}).get('itemStruct')
    if not isinstance(item, dict):
        status_msg: str = str(detail_block.get('statusMsg') or 'no item')
        return Resolution(
            video_id=video_id,
            status=STATUS_UNAVAILABLE,
            detail=status_msg[:120],
        )
    try:
        video: TikTokVideo = TikTokVideo.from_api(item)
    except (ValueError, TypeError, KeyError) as exc:  # shape mismatch
        return Resolution(
            video_id=video_id,
            status=STATUS_ERROR,
            detail=f'{type(exc).__name__}: {str(exc)[:120]}',
        )
    if not video.username:
        return Resolution(
            video_id=video_id,
            status=STATUS_UNAVAILABLE,
            detail='item has no author username',
        )
    return Resolution(
        video_id=video_id,
        status=STATUS_RESOLVED,
        username=video.username,
        user_id=video.author_id,
        sec_uid=video.sec_uid,
        nickname=video.author_nickname,
    )


def _classify_status(status_code: int) -> str | None:
    '''Map a non-2xx HTTP status to an outcome, or ``None`` if retry.'''
    if status_code in _TRANSIENT_STATUSES or status_code >= 500:
        return STATUS_TRANSIENT
    if status_code >= 400:
        return STATUS_UNAVAILABLE
    return None


class Resolver:
    '''Fetches video pages through proxies under the shared limiter.'''

    def __init__(
        self,
        settings: ResolveSettings,
        limiter: TikTokRateLimiter,
        headers: dict[str, str],
    ) -> None:
        self._settings: ResolveSettings = settings
        self._limiter: TikTokRateLimiter = limiter
        self._headers: dict[str, str] = headers
        self._clients: dict[str | None, httpx2.AsyncClient] = {}
        self._proxies: list[str] = list(settings.proxies)

    def _client(self, proxy: str | None) -> httpx2.AsyncClient:
        '''Return (creating on first use) the client for *proxy*.'''
        client: httpx2.AsyncClient | None = self._clients.get(proxy)
        if client is not None:
            return client
        client = httpx2.AsyncClient(
            proxy=proxy,
            http2=True,
            follow_redirects=True,
            timeout=self._settings.request_timeout_seconds,
            headers=self._headers,
            limits=httpx2.Limits(
                max_connections=self._settings.concurrency + 2,
            ),
        )
        self._clients[proxy] = client
        return client

    async def resolve(self, video_id: int) -> Resolution:
        '''Resolve one video id, retrying transient outcomes.'''
        url: str = VIDEO_URL_TEMPLATE.format(video_id=video_id)
        attempt: int = 0
        last: Resolution | None = None
        while attempt <= self._settings.max_retries:
            proxy: str | None = await self._limiter.acquire(
                TikTokCallType.API,
            )
            try:
                response: httpx2.Response = await self._client(
                    proxy,
                ).get(url)
            except httpx2.HTTPError as exc:
                last = Resolution(
                    video_id=video_id,
                    status=STATUS_TRANSIENT,
                    detail=f'{type(exc).__name__}: {exc}'[:160],
                )
            else:
                status: str | None = _classify_status(
                    response.status_code,
                )
                if status is None:
                    last = parse_author(response.text, video_id)
                else:
                    last = Resolution(
                        video_id=video_id,
                        status=status,
                        detail=f'HTTP {response.status_code}',
                    )
            if last.status != STATUS_TRANSIENT:
                return last
            attempt += 1
            if attempt > self._settings.max_retries:
                break
            delay: float = min(30.0, 2.0 ** attempt) + random.random()
            _LOGGER.info(
                'Retrying video id',
                extra={
                    'video_id': video_id,
                    'attempt': attempt,
                    'delay_seconds': round(delay, 1),
                    'reason': last.detail,
                },
            )
            await asyncio.sleep(delay)
        assert last is not None
        return last

    async def close(self) -> None:
        '''Close every per-proxy client.'''
        for client in self._clients.values():
            await client.aclose()
        self._clients.clear()


def _fallback_weight(settings: ResolveSettings) -> int:
    '''Follower count placing a new creator in the next-to-lowest tier.'''
    tiers: list[TierConfig] = parse_priority_queues(
        settings.creator_priority_queues,
    )
    tier: TierConfig = tiers[-2] if len(tiers) >= 2 else tiers[-1]
    return max(tier.min_subscribers, 0)


async def enqueue_creators(
    settings: ResolveSettings,
    usernames: list[str],
) -> tuple[int, int]:
    '''Add *usernames* to the creator queue; return (new, existing).'''
    if not settings.redis_dsn:
        raise RuntimeError(
            'REDIS_DSN is required to enqueue creators; '
            'use --dry-run to resolve without queueing',
        )
    tiers: list[TierConfig] = parse_priority_queues(
        settings.creator_priority_queues,
    )
    queue: RedisCreatorQueue = RedisCreatorQueue(
        settings.redis_dsn,
        settings.worker_id,
        QUEUE_PLATFORM,
        key_namespace=QUEUE_NAMESPACE,
    )
    # add_member() needs the tier queues that populate() normally
    # derives; the operator adapters wire them the same way.
    queue._tiers = tiers
    queue._key_queues = queue._build_queue_keys(tiers)
    weight: int = (
        settings.creator_weight
        if settings.creator_weight is not None
        else _fallback_weight(settings)
    )
    added: int = 0
    existing: int = 0
    try:
        for username in usernames:
            if await queue.add_member(username, username, weight):
                added += 1
            else:
                existing += 1
    finally:
        await queue._redis.aclose()
    return added, existing


async def run(settings: ResolveSettings) -> int:
    '''Resolve every input id, then queue the creators it found.'''
    ids: list[int] = read_video_ids(settings.input_file)
    if settings.max_ids:
        ids = ids[: settings.max_ids]
    if not ids:
        print('no video ids to resolve', file=sys.stderr)
        return 1

    limiter: TikTokRateLimiter = TikTokRateLimiter.get(
        state_dir=settings.rate_limiter_state_dir,
        redis_dsn=settings.redis_dsn,
    )
    limiter.set_proxies(list(settings.proxies) or None)
    headers: dict[str, str] = browser_headers(settings.user_agent)
    resolver: Resolver = Resolver(settings, limiter, headers)
    concurrency: int = max(
        1, min(settings.concurrency, len(settings.proxies) or 1),
    )
    if not settings.proxies:
        _LOGGER.warning(
            'No proxies configured; resolving over the local egress',
        )
    _LOGGER.info(
        'Resolving video ids',
        extra={
            'video_ids': len(ids),
            'concurrency': concurrency,
            'proxies': len(settings.proxies),
        },
    )

    semaphore: asyncio.Semaphore = asyncio.Semaphore(concurrency)
    results: list[Resolution] = []

    async def one(video_id: int) -> Resolution:
        async with semaphore:
            return await resolver.resolve(video_id)

    try:
        results = list(await asyncio.gather(*(one(v) for v in ids)))
    finally:
        await resolver.close()

    counts: dict[str, int] = {}
    for result in results:
        counts[result.status] = counts.get(result.status, 0) + 1

    usernames: list[str] = []
    seen_names: set[str] = set()
    for result in results:
        if result.status != STATUS_RESOLVED or not result.username:
            continue
        if result.username in seen_names:
            continue
        seen_names.add(result.username)
        usernames.append(result.username)

    if settings.output_file:
        settings.output_file.parent.mkdir(parents=True, exist_ok=True)
        with settings.output_file.open('a', encoding='utf-8') as handle:
            for result in results:
                handle.write(json.dumps(result.as_record()) + '\n')

    added: int = 0
    existing: int = 0
    if settings.enqueue and not settings.dry_run and usernames:
        added, existing = await enqueue_creators(settings, usernames)

    summary: dict[str, Any] = {
        'video_ids': len(ids),
        'resolved': counts.get(STATUS_RESOLVED, 0),
        'unavailable': counts.get(STATUS_UNAVAILABLE, 0),
        'transient': counts.get(STATUS_TRANSIENT, 0),
        'errors': counts.get(STATUS_ERROR, 0),
        'unique_creators': len(usernames),
        'queued_new': added,
        'queue_existing': existing,
        'dry_run': settings.dry_run,
    }
    print(json.dumps(summary, indent=2))
    return 0 if counts.get(STATUS_RESOLVED, 0) else 1


def main() -> int:
    '''Entry point.'''
    settings: ResolveSettings = ResolveSettings()  # type: ignore[call-arg]
    configure_logging(
        settings.log_level, settings.log_file, settings.log_format,
    )
    return asyncio.run(run(settings))


if __name__ == '__main__':
    raise SystemExit(main())
