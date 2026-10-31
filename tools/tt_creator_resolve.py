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

Every username is added to the TikTok creator scrape queue -- the same
``RedisCreatorQueue`` (namespace ``scrape``, platform ``tiktok``) that
``tt_creator_scrape.py`` consumes -- as soon as it is found, so the
scraper picks creators up while the run is still going. A creator that
is already queued, being scraped or excluded is left alone.

Before fetching anything, ids the TikTok video scrape queue already
knows are skipped, whatever state it reports: ``queued`` (still to be
scraped), ``scraped`` (only the backlog-server backend remembers
completions -- the Redis backend forgets a completed video, so there it
can only skip what is queued), and the terminal ``failed`` /
``unavailable`` / ``removed``. Re-resolving a settled id cannot change
its outcome and the queue would refuse the video anyway. Ids the queue
has never seen are the only ones fetched.

Examples::

    uv run python tools/tt_creator_resolve.py \
        --input-file tt_video_ids-2026-10.lst

    cut -d, -f1 videos.csv | uv run python tools/tt_creator_resolve.py

    uv run python tools/tt_creator_resolve.py \
        --input-file ids.lst --dry-run

``--dry-run`` resolves and reports without touching the queue. Requests
that fail transiently (timeouts, 403/429, bot-check pages, redirects
away from the video page such as ``/hk/about`` or a login wall, and
TikTok server errors like ``ErrSysPanic``) are retried with backoff,
each attempt through the proxy the rate limiter hands out; pages that
carry no item payload (deleted videos) are reported as
``unavailable``. Outcome log lines name the proxy (``proxy_ip``,
``proxy_port``) so proxies TikTok geo-blocks can be found.

Ctrl-C is a clean stop: in-flight requests are cancelled, the summary
reports how far the run got (creators found so far are already queued)
and the exit code is 130. When ``--output-file`` is set each record is
appended as soon as it resolves, so nothing already done is lost.

Every id that reaches a terminal outcome is also appended to
``--processed-file`` (default ``~/tiktok-resolved-video-ids.lst``) as
it completes, and ids listed there are skipped on later runs, so a
large input file can be worked through in several passes.
``--reprocess`` ignores the log; ids whose page fetch stayed
``transient`` (timeouts, 403/429, bot checks, redirects, TikTok
server errors) are deliberately not
logged so a later run retries them.
'''

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import re
import signal
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx2
from pydantic import AliasChoices, Field
from pydantic_settings import SettingsConfigDict
from redis.exceptions import RedisError

from scrape_exchange.creator_queue import (
    RedisCreatorQueue,
    TierConfig,
    parse_priority_queues,
    tier_for_subscriber_count,
)
from scrape_exchange.logging import configure_logging
from scrape_exchange.redis_client import redis_from_url
from scrape_exchange.tiktok import (
    TikTokCallType,
    TikTokRateLimiter,
    TikTokVideo,
)
from scrape_exchange.tiktok.settings import TikTokScraperSettings
from scrape_exchange.util import extract_proxy_ip, extract_proxy_port
from scrape_exchange.video_queue_factory import (
    close_backlog_clients,
    make_video_scrape_queue,
)
from scrape_exchange.video_scrape_queue import (
    VideoScrapeQueue,
    VideoScrapeQueueSettings,
)

_LOGGER: logging.Logger = logging.getLogger(__name__)

VIDEO_URL_TEMPLATE: str = 'https://www.tiktok.com/@_/video/{video_id}'
TIKTOK_ORIGIN: str = 'https://www.tiktok.com'
# A followed redirect that leaves this path landed off the video page.
VIDEO_PATH_MARKER: str = '/video/'
QUEUE_PLATFORM: str = 'tiktok'
QUEUE_NAMESPACE: str = 'scrape'
# Video ids looked up per video scrape queue state request.
VIDEO_QUEUE_STATE_BATCH: int = 1000

# Video queue states that make an id pointless to resolve: the video is
# either waiting to be scraped or has already been scraped.
VIDEO_STATE_QUEUED: str = 'queued'
VIDEO_STATE_SCRAPED: str = 'scraped'

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
# Lowercased ``statusMsg`` fragments of TikTok-side failures: the
# video may well exist, so these are retried, not reported missing.
_SERVER_ERROR_MARKERS: tuple[str, ...] = (
    'errsyspanic',
    'rpcerror',
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
    processed_file: Path | None = Field(
        default=Path.home() / 'tiktok-resolved-video-ids.lst',
        validation_alias=AliasChoices(
            'TIKTOK_RESOLVE_PROCESSED_FILE', 'processed_file',
        ),
        description=(
            'Append-only log of the video ids this tool has processed, '
            'one per line. Ids listed there are skipped on later runs. '
            'Set to an empty value to disable the log.'
        ),
    )
    reprocess: bool = Field(
        default=False,
        validation_alias=AliasChoices('REPROCESS', 'reprocess'),
        description=(
            'Process every input id even if the processed log lists it.'
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


def _proxy_log_fields(proxy: str | None) -> dict[str, str]:
    '''Credential-free proxy fields for log records.'''
    if proxy is None:
        return {'proxy_ip': 'none', 'proxy_port': 'none'}
    return {
        'proxy_ip': extract_proxy_ip(proxy),
        'proxy_port': extract_proxy_port(proxy),
    }


def _no_username(
    video_id: int,
    status: str,
    detail: str,
    proxy: str | None = None,
) -> Resolution:
    '''Build and log an outcome where the username is unavailable.'''
    _LOGGER.warning(
        'Creator username not extracted',
        extra={
            'video_id': video_id,
            'status': status,
            'detail': detail,
            **_proxy_log_fields(proxy),
        },
    )
    return Resolution(
        video_id=video_id,
        status=status,
        detail=detail,
    )


def parse_author(
    html: str, video_id: int, proxy: str | None = None,
) -> Resolution:
    '''Extract the creator of *video_id* from a fetched video page.'''
    scope: dict[str, Any] | None = _rehydration_scope(html)
    if scope is None:
        blocked: bool = any(
            marker in html[:8000].lower() for marker in _BOT_MARKERS
        )
        return _no_username(
            video_id,
            STATUS_TRANSIENT if blocked else STATUS_UNAVAILABLE,
            'bot check' if blocked else 'no rehydration payload',
            proxy,
        )
    detail_block: Any = scope.get('webapp.video-detail') or {}
    item: Any = (detail_block.get('itemInfo') or {}).get('itemStruct')
    if not isinstance(item, dict):
        status_msg: str = str(detail_block.get('statusMsg') or 'no item')
        server_error: bool = any(
            marker in status_msg.lower()
            for marker in _SERVER_ERROR_MARKERS
        )
        return _no_username(
            video_id,
            STATUS_TRANSIENT if server_error else STATUS_UNAVAILABLE,
            status_msg[:120],
            proxy,
        )
    try:
        video: TikTokVideo = TikTokVideo.from_api(item)
    except (ValueError, TypeError, KeyError) as exc:  # shape mismatch
        return _no_username(
            video_id,
            STATUS_ERROR,
            f'{type(exc).__name__}: {str(exc)[:120]}',
            proxy,
        )
    if not video.username:
        return _no_username(
            video_id, STATUS_UNAVAILABLE, 'item has no author username',
            proxy,
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


_RECORD_ID_RE: re.Pattern[str] = re.compile(r'"video_id":\s*(\d+)')


def append_record(path: Path, result: Resolution) -> None:
    '''Append one JSONL record, flushed, so an interrupt keeps it.'''
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a', encoding='utf-8') as handle:
        handle.write(json.dumps(result.as_record()) + '\n')


def load_processed_ids(path: Path | None) -> set[int]:
    '''Video ids listed in the processed log (empty when absent).

    Unparsable lines are ignored so a truncated final line cannot stop
    a run from starting.
    '''
    if path is None or not path.exists():
        return set()
    done: set[int] = set()
    with path.open('r', encoding='utf-8') as handle:
        for line in handle:
            value: str = line.strip()
            if value.isdigit():
                done.add(int(value))
    return done


class ProcessedLog:
    '''Append-only log of processed video ids, flushed as it goes.'''

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path: Path = path
        self._handle = path.open('a', encoding='utf-8')
        self.recorded: int = 0

    def record(self, video_id: int) -> None:
        '''Append one id and flush so a kill cannot lose it.'''
        self._handle.write(f'{video_id}\n')
        self._handle.flush()
        self.recorded += 1

    def close(self) -> None:
        '''Close the log.'''
        self._handle.close()


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
                if status is not None:
                    last = Resolution(
                        video_id=video_id,
                        status=status,
                        detail=f'HTTP {response.status_code}',
                    )
                elif VIDEO_PATH_MARKER not in response.url.path:
                    # TikTok sends some proxies to a region page
                    # (/hk/about) or a login wall instead of the
                    # video; another proxy usually gets the page.
                    last = _no_username(
                        video_id,
                        STATUS_TRANSIENT,
                        f'redirected to {response.url.path}'[:120],
                        proxy,
                    )
                else:
                    last = parse_author(response.text, video_id, proxy)
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
                    **_proxy_log_fields(proxy),
                },
            )
            await asyncio.sleep(delay)
        assert last is not None
        return last

    async def close(self) -> None:
        '''Close every per-proxy client, ignoring teardown errors.'''
        clients: list[httpx2.AsyncClient] = list(
            self._clients.values(),
        )
        self._clients.clear()
        for client in clients:
            with contextlib.suppress(
                httpx2.HTTPError, RuntimeError, OSError,
            ):
                await client.aclose()


def _fallback_weight(settings: ResolveSettings) -> int:
    '''Follower count placing a new creator in the next-to-lowest tier.'''
    tiers: list[TierConfig] = parse_priority_queues(
        settings.creator_priority_queues,
    )
    tier: TierConfig = tiers[-2] if len(tiers) >= 2 else tiers[-1]
    return max(tier.min_subscribers, 0)


class CreatorEnqueuer:
    '''Adds resolved creators to the creator scrape queue as they are
    found, once per username per run.'''

    def __init__(self, settings: ResolveSettings) -> None:
        if not settings.redis_dsn:
            raise RuntimeError(
                'REDIS_DSN is required to enqueue creators; '
                'use --dry-run to resolve without queueing',
            )
        tiers: list[TierConfig] = parse_priority_queues(
            settings.creator_priority_queues,
        )
        self._queue: RedisCreatorQueue = RedisCreatorQueue(
            settings.redis_dsn,
            settings.worker_id,
            QUEUE_PLATFORM,
            key_namespace=QUEUE_NAMESPACE,
        )
        # schedule_if_absent() needs the tier queues that populate()
        # normally derives; the operator adapters wire them the same
        # way.
        self._queue._tiers = tiers
        self._queue._key_queues = self._queue._build_queue_keys(tiers)
        self._weight: int = (
            settings.creator_weight
            if settings.creator_weight is not None
            else _fallback_weight(settings)
        )
        self._tier: int = tier_for_subscriber_count(tiers, self._weight)
        self._done: set[str] = set()
        self.added: int = 0
        self.existing: int = 0
        self.failed: int = 0

    async def add(self, username: str) -> bool:
        '''Queue *username* unless it is already queued, claimed or
        excluded. Returns False only when Redis could not be reached,
        so the caller can leave the video id for a later run.'''
        if username in self._done:
            return True
        try:
            queued: bool = await self._queue.schedule_if_absent(
                username, username, self._weight,
            )
        except (RedisError, OSError, TimeoutError) as exc:
            self.failed += 1
            _LOGGER.warning(
                'Creator queue add failed',
                extra={'username': username, 'error': str(exc)},
            )
            return False
        self._done.add(username)
        if queued:
            self.added += 1
        else:
            self.existing += 1
        _LOGGER.info(
            'Creator queue add',
            extra={
                'username': username,
                'weight': self._weight,
                'tier': self._tier,
                'outcome': 'queued' if queued else 'already_queued',
            },
        )
        return True

    async def close(self) -> None:
        '''Close the queue's Redis connection pool.'''
        await self._queue._redis.aclose()


async def filter_known_video_ids(
    redis_dsn: str, video_ids: list[int],
) -> tuple[list[int], dict[str, int]]:
    '''Split *video_ids* into the ones worth fetching and the ones the
    TikTok video scrape queue already knows.

    Any id the queue reports a state for is dropped: ``queued`` (it is
    going to be scraped), ``scraped`` (only the backlog-server backend
    remembers completions), and the terminal ``failed`` /
    ``unavailable`` / ``removed``. Re-resolving an id the queue already
    settled cannot change its outcome, and the queue would reject the
    video anyway.

    :returns: (remaining ids in their original order, skipped counts
        keyed by queue state)
    '''
    redis: Any = redis_from_url(
        redis_dsn,
        component='tiktok-resolve-video-queue',
        decode_responses=True,
    )
    queue: VideoScrapeQueue = make_video_scrape_queue(
        platform=QUEUE_PLATFORM,
        redis=redis,
        settings=VideoScrapeQueueSettings(),
        component='tiktok-resolve-video-queue',
    )
    remaining: list[int] = []
    skipped: dict[str, int] = {}
    try:
        for start in range(0, len(video_ids), VIDEO_QUEUE_STATE_BATCH):
            chunk: list[int] = video_ids[
                start:start + VIDEO_QUEUE_STATE_BATCH
            ]
            states: dict[str, str | None] = await queue.get_state_names(
                [str(v) for v in chunk],
            )
            for video_id in chunk:
                state: str | None = states.get(str(video_id))
                if state is None:
                    remaining.append(video_id)
                    continue
                skipped[state] = skipped.get(state, 0) + 1
    finally:
        await redis.aclose()
        await close_backlog_clients()
    return remaining, skipped


async def run(settings: ResolveSettings) -> int:
    '''Resolve every input id, then queue the creators it found.'''
    ids: list[int] = read_video_ids(settings.input_file)
    if settings.max_ids:
        ids = ids[: settings.max_ids]
    input_ids: int = len(ids)
    already_done: int = 0
    if settings.processed_file is not None and not settings.reprocess:
        done_ids: set[int] = load_processed_ids(settings.processed_file)
        if done_ids:
            remaining: list[int] = [
                video_id for video_id in ids
                if video_id not in done_ids
            ]
            already_done = len(ids) - len(remaining)
            _LOGGER.info(
                'Skipping already processed video ids',
                extra={
                    'skipped': already_done,
                    'input_ids': input_ids,
                    'processed_file': str(settings.processed_file),
                },
            )
            ids = remaining
    skipped_queue: dict[str, int] = {}
    if settings.redis_dsn:
        remaining, skipped_queue = await filter_known_video_ids(
            settings.redis_dsn, ids,
        )
        skipped_total: int = sum(skipped_queue.values())
        _LOGGER.info(
            'Skipping video ids the video scrape queue already knows',
            extra={
                'skipped': skipped_total,
                'queued': skipped_queue.get(VIDEO_STATE_QUEUED, 0),
                'scraped': skipped_queue.get(VIDEO_STATE_SCRAPED, 0),
                'terminal': skipped_total - sum(
                    skipped_queue.get(state, 0)
                    for state in (
                        VIDEO_STATE_QUEUED, VIDEO_STATE_SCRAPED,
                    )
                ),
            },
        )
        ids = remaining
    else:
        _LOGGER.warning(
            'No REDIS_DSN; not checking the video scrape queue',
        )
    if not ids:
        print(
            f'no video ids to resolve '
            f'({already_done} already processed, '
            f'{sum(skipped_queue.values())} known to the '
            f'video scrape queue: '
            f'{skipped_queue or "none"})',
            file=sys.stderr,
        )
        return 1
    enqueuer: CreatorEnqueuer | None = None
    if settings.enqueue and not settings.dry_run:
        if not settings.redis_dsn:
            print(
                'REDIS_DSN is required to enqueue creators; '
                'use --dry-run to resolve without queueing',
                file=sys.stderr,
            )
            return 2
        enqueuer = CreatorEnqueuer(settings)

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
    interrupted: bool = False
    processed: ProcessedLog | None = (
        ProcessedLog(settings.processed_file)
        if settings.processed_file is not None else None
    )

    async def one(video_id: int) -> Resolution:
        async with semaphore:
            return await resolver.resolve(video_id)

    # Real tasks, not bare coroutines: an interrupt then cancels work
    # that is actually scheduled instead of leaving un-awaited
    # coroutine objects behind.
    tasks: list[asyncio.Task[Resolution]] = [
        asyncio.create_task(one(video_id), name=f'resolve-{video_id}')
        for video_id in ids
    ]
    # Turn Ctrl-C into a cancellation of this coroutine so the partial
    # results are summarised and the HTTP clients are closed; without
    # this the interrupt unwinds through asyncio.run's teardown and
    # kills the loop under the tasks.
    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
    main_task: asyncio.Task[Any] | None = asyncio.current_task()
    if main_task is not None:
        with contextlib.suppress(NotImplementedError, RuntimeError):
            loop.add_signal_handler(signal.SIGINT, main_task.cancel)
    try:
        for finished in asyncio.as_completed(tasks):
            result: Resolution = await finished
            results.append(result)
            if settings.output_file is not None:
                append_record(settings.output_file, result)
            queued: bool = True
            if (
                enqueuer is not None
                and result.status == STATUS_RESOLVED
                and result.username
            ):
                queued = await enqueuer.add(result.username)
            # Transient outcomes are not recorded: they exhausted their
            # retries but are worth another attempt on a later run.
            # Neither is a creator Redis would not take.
            if (
                processed is not None
                and result.status != STATUS_TRANSIENT
                and queued
            ):
                processed.record(result.video_id)
    except asyncio.CancelledError:
        interrupted = True
        for task in tasks:
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await resolver.close()
        if enqueuer is not None:
            await enqueuer.close()
        if processed is not None:
            processed.close()

    counts: dict[str, int] = {}
    for result in results:
        counts[result.status] = counts.get(result.status, 0) + 1

    usernames: set[str] = {
        result.username for result in results
        if result.status == STATUS_RESOLVED and result.username
    }

    summary: dict[str, Any] = {
        'video_ids': len(ids),
        'done': len(results),
        'resolved': counts.get(STATUS_RESOLVED, 0),
        'unavailable': counts.get(STATUS_UNAVAILABLE, 0),
        'transient': counts.get(STATUS_TRANSIENT, 0),
        'errors': counts.get(STATUS_ERROR, 0),
        'skipped_processed': already_done,
        'skipped_video_queue': sum(skipped_queue.values()),
        'skipped_queue_states': skipped_queue,
        'unique_creators': len(usernames),
        'queued_new': enqueuer.added if enqueuer is not None else 0,
        'queue_existing': (
            enqueuer.existing if enqueuer is not None else 0
        ),
        'queue_failures': enqueuer.failed if enqueuer is not None else 0,
        'interrupted': interrupted,
        'dry_run': settings.dry_run,
    }
    print(json.dumps(summary, indent=2))
    if interrupted:
        hint: str = (
            f'; {processed.recorded} id(s) logged to '
            f'{processed.path}, rerun to continue from there'
            if processed is not None
            else '; pass --processed-file next time to keep your place'
        )
        print(
            f'{len(results)} of {len(ids)} video id(s) done before '
            f'interrupt; creators found so far are queued{hint}',
            file=sys.stderr,
        )
        return 130
    return 0 if counts.get(STATUS_RESOLVED, 0) else 1


def main() -> int:
    '''Entry point.'''
    settings: ResolveSettings = ResolveSettings()  # type: ignore[call-arg]
    configure_logging(
        settings.log_level, settings.log_file, settings.log_format,
    )
    try:
        return asyncio.run(run(settings))
    except KeyboardInterrupt:
        print('interrupted', file=sys.stderr)
        return 130


if __name__ == '__main__':
    raise SystemExit(main())
