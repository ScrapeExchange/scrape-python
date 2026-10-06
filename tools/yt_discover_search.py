#!/usr/bin/env python3
'''Discover YouTube channels from InnerTube search results.

Search terms come from the command line or stdin (one pass, then exit),
or are random words. With ``--keyword-count N`` the tool searches N
random words once and exits; without it, it runs indefinitely, searching
a fresh batch of random words every round.
'''

from __future__ import annotations

import asyncio
import base64
import errno
import functools
import json
import logging
import os
import random
import signal
import stat
import sys
import time
import unicodedata
from collections import OrderedDict, deque
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
    Iterator,
    TypeVar,
)

import httpx  # InnerTube still runs on original httpx: its exception
# classes must be caught as httpx.*, not httpx2.*
import httpx2  # word-source fetches migrated to httpx2
from innertube.errors import RequestError as InnerTubeRequestError
from innertube.errors import ResponseError as InnerTubeResponseError
from prometheus_client import Counter, Gauge, Histogram
from pydantic import AliasChoices, Field
from pydantic_settings import CliPositionalArg, SettingsConfigDict

from scrape_exchange.channel_scrape_queue import (
    ChannelScrapeQueueSettings,
    RedisChannelScrapeQueue,
)
from scrape_exchange.creator_map import CreatorMap, RedisCreatorMap
from scrape_exchange.logging import configure_logging
from scrape_exchange.metrics_server import start_metrics_server
from scrape_exchange.redis_client import redis_from_url
from scrape_exchange.settings import ScraperSettings
from scrape_exchange.util import extract_proxy_ip, extract_proxy_port
from scrape_exchange.youtube.youtube_channel import YouTubeChannel
from scrape_exchange.youtube.youtube_channel_tabs import (
    aclose_pooled_innertube,
    configure_innertube_executor,
    pooled_innertube_localized_for_entry,
    run_on_innertube_executor,
    shutdown_innertube_executor,
)
from scrape_exchange.youtube.youtube_rate_limiter import (
    YouTubeCallType,
    YouTubeRateLimiter,
)


_LOGGER: logging.Logger = logging.getLogger(__name__)

_RANDOM_WORD_API_LANGUAGES: tuple[str, ...] = (
    'en', 'es', 'it', 'de', 'fr', 'zh', 'pt-br', 'ro',
)
# Additional languages backed by each language's Wikipedia edition.
_WIKIMEDIA_RANDOM_WORD_LANGUAGES: tuple[str, ...] = (
    'af', 'ar', 'az', 'be', 'bg', 'bn', 'bs', 'ca', 'cs', 'cy',
    'da', 'el', 'eo', 'et', 'eu', 'fa', 'fi', 'ga', 'gl', 'he',
    'hi', 'hr', 'hu', 'hy', 'id', 'is', 'ja', 'ka', 'kk', 'ko',
    'lt', 'lv', 'mk', 'ms', 'nl', 'no', 'pl', 'ru', 'sk', 'sl',
    'sq', 'sr', 'sv', 'sw', 'ta', 'th', 'tr', 'uk', 'ur', 'vi',
)
_DEFAULT_RANDOM_WORD_LANGUAGES: tuple[str, ...] = (
    _RANDOM_WORD_API_LANGUAGES + _WIKIMEDIA_RANDOM_WORD_LANGUAGES
)
_WIKIMEDIA_RANDOM_WORD_URL_TEMPLATE: str = (
    'https://{language}.wikipedia.org/w/api.php'
)
_WIKIMEDIA_USER_AGENT: str = (
    'scrape-python-yt-discover-search/1.0 '
    '(https://scrape.exchange)'
)
_OFFLINE_RANDOM_TERMS: tuple[str, ...] = (
    'water', 'house', 'music', 'historia', 'cocina',
    'viaggio', 'wissenschaft', 'jardin', 'cidade',
    'tecnologia', 'familia', 'natureza',
)

# Transient errors from an InnerTube search call that should not
# crash the run: a timed-out / failed page yields no continuation
# token, so the term simply ends and the next term continues.
# Mirrors the set caught in
# ``scrape_exchange/youtube/youtube_client.py``. ``InnerTubeRequestError``
# covers HTTP 4xx/5xx responses from YouTube. ``InnerTubeResponseError``
# covers non-JSON responses, such as HTML interstitials.
_TRANSIENT_SEARCH_ERRORS: tuple[type[BaseException], ...] = (
    # Base class for all httpx transport-level failures: timeouts,
    # connect errors, network errors, proxy errors (e.g. a proxy
    # returning 503), and protocol errors. A flapping proxy is
    # transient, so the whole family is retried/skipped rather than
    # crashing the run.
    httpx.TransportError,
    ConnectionResetError,
    ConnectionRefusedError,
    InnerTubeRequestError,
    InnerTubeResponseError,
)
_PROXY_CONNECTION_ERRORS: tuple[type[BaseException], ...] = (
    # Original httpx: the InnerTube client raises these.
    httpx.TransportError,
    ConnectionResetError,
    ConnectionRefusedError,
)
_SEARCH_RETRY_BACKOFF_SECONDS: float = 2.0

# InnerTube SEARCH result sort/filter params. Verified against the
# live search UI: the sort is the protobuf field-1 enum (3 == the
# 'Popularity' / view-count option) and the result-type filter is the
# nested field-2 message (2 == channels). ``_encode_search_params``
# reproduces exactly what the web client emits, so the tool never
# hardcodes an opaque base64 literal.
_SEARCH_SORT_POPULARITY: int = 3
# Random words searched per round when running without --keyword-count.
# Each round re-picks the random-word language, so batches rotate
# languages; with 30 markets, 2 facets and up to 6 pages a word costs up
# to ~360 search requests.
UNLIMITED_KEYWORD_BATCH: int = 10
_SEARCH_TYPE_VIDEO: int = 1
_SEARCH_TYPE_CHANNEL: int = 2

_SUBS_MULTIPLIERS: dict[str, int] = {
    'K': 1_000,
    'M': 1_000_000,
    'B': 1_000_000_000,
}

# Prioritised markets (densest 10M-100M channel populations first),
# as ``GL:hl`` pairs. Overridable via DISCOVER_MARKETS.
_DEFAULT_MARKETS: str = (
    'IN:hi, US:en, BR:pt, ID:id, MX:es, RU:ru, TR:tr, EG:ar, '
    'SA:ar, KR:ko, JP:ja, PH:fil, VN:vi, TH:th, PK:ur, BD:bn, '
    'NG:en, DE:de, GB:en, FR:fr, IT:it, ES:es, AR:es, CO:es, '
    'PL:pl, UA:uk, DZ:ar, MA:ar, IQ:ar, TW:zh-TW'
)


def _encode_varint(value: int) -> bytes:
    '''Encode a protobuf base-128 varint.'''

    out: bytearray = bytearray()
    while True:
        byte: int = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def _encode_varint_field(field: int, value: int) -> bytes:
    '''Encode a protobuf ``varint`` field (wire type 0).'''

    return _encode_varint(field << 3) + _encode_varint(value)


def _encode_len_field(field: int, payload: bytes) -> bytes:
    '''Encode a protobuf length-delimited field (wire type 2).'''

    return (
        _encode_varint((field << 3) | 2)
        + _encode_varint(len(payload))
        + payload
    )


def _encode_search_params(
    *,
    sort: int | None = None,
    upload_date: int | None = None,
    media_type: int | None = None,
    duration: int | None = None,
) -> str:
    '''Build the base64 InnerTube SEARCH ``params`` blob.

    ``sort`` is the outer field-1 enum; ``upload_date``,
    ``media_type`` and ``duration`` are fields of the nested
    field-2 filter message. Returns ``''`` when no option is
    given, so callers can omit ``params`` entirely.
    '''

    nested: bytes = b''
    if upload_date is not None:
        nested += _encode_varint_field(1, upload_date)
    if media_type is not None:
        nested += _encode_varint_field(2, media_type)
    if duration is not None:
        nested += _encode_varint_field(3, duration)
    outer: bytes = b''
    if sort is not None:
        outer += _encode_varint_field(1, sort)
    if nested:
        outer += _encode_len_field(2, nested)
    if not outer:
        return ''
    return base64.b64encode(outer).decode('ascii')


# Sort-by-view-count search params ('CAM=' and 'CAMSAhAC' today).
POPULARITY_VIDEO_PARAMS: str = _encode_search_params(
    sort=_SEARCH_SORT_POPULARITY,
)
POPULARITY_CHANNEL_PARAMS: str = _encode_search_params(
    sort=_SEARCH_SORT_POPULARITY,
    media_type=_SEARCH_TYPE_CHANNEL,
)


METRIC_SEARCH_PAGES: Counter = Counter(
    'discover_search_pages_total',
    'Search result pages requested, by market (gl), facet '
    '(channel/video search), page type and outcome (failed = no '
    'page after retries).',
    ['platform', 'market', 'facet', 'page_type', 'outcome'],
)
METRIC_SEARCH_ERRORS: Counter = Counter(
    'discover_search_errors_total',
    'Failed search request attempts, by exception type.',
    ['platform', 'error_type'],
)
METRIC_SEARCH_HTTP_ERRORS: Counter = Counter(
    'discover_search_http_errors_total',
    'InnerTube search calls answered with an HTTP error, by status '
    '(429 = YouTube rate limiting).',
    ['platform', 'status'],
)
METRIC_SEARCH_DURATION: Histogram = Histogram(
    'discover_search_duration_seconds',
    'Duration of one InnerTube search call, excluding the rate '
    'limiter wait.',
    ['platform', 'page_type'],
    buckets=(0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 15.0, 30.0, 60.0),
)
METRIC_CHANNELS_FOUND: Counter = Counter(
    'discover_channels_found_total',
    'Channels extracted from search result pages (before dedupe).',
    ['platform', 'facet'],
)
METRIC_ENQUEUE_OUTCOMES: Counter = Counter(
    'discover_enqueue_outcomes_total',
    'Outcome of offering a discovered channel to the channel scrape '
    'queue (enqueued, already_scraped, on_exchange, '
    'below_min_subscribers, ...).',
    ['platform', 'outcome'],
)
METRIC_JOBS_PLANNED: Gauge = Gauge(
    'discover_jobs_planned',
    'Market x term x facet search jobs in the current pass.',
    ['platform'],
)
METRIC_JOBS_COMPLETED: Counter = Counter(
    'discover_jobs_completed_total',
    'Market x term x facet search jobs finished in this process.',
    ['platform'],
)


def _facet_label(params: str | None) -> str:
    '''Metric label for the search facet encoded in *params*.'''
    if params == POPULARITY_CHANNEL_PARAMS:
        return 'channel'
    if params == POPULARITY_VIDEO_PARAMS:
        return 'video'
    return 'other'


def _record_enqueue_outcomes(counts: dict[str, int]) -> None:
    '''Add one job's enqueue outcome counts to the metric.'''
    outcome: str
    value: int
    for outcome, value in counts.items():
        if value:
            METRIC_ENQUEUE_OUTCOMES.labels(
                platform='youtube', outcome=outcome,
            ).inc(value)


def _start_metrics(port: int) -> None:
    '''Start the Prometheus endpoint; a busy port is logged, not
    fatal, so discovery still runs.'''
    try:
        start_metrics_server(port)
    except OSError as exc:
        _LOGGER.warning(
            'Could not start metrics server; continuing without',
            exc=exc, extra={'metrics_port': port},
        )
        return
    _LOGGER.info(
        'Metrics server started', extra={'metrics_port': port},
    )


def _parse_subscriber_text(text: str | None) -> int | None:
    '''Parse a subscriber label such as ``12.3M subscribers`` into
    an integer; ``None`` when it has no leading number.'''

    if not text:
        return None
    token: str = text.strip().split(' ')[0].replace(',', '')
    if not token:
        return None
    suffix: str = token[-1].upper()
    try:
        if suffix in _SUBS_MULTIPLIERS:
            return int(float(token[:-1]) * _SUBS_MULTIPLIERS[suffix])
        return int(token)
    except ValueError:
        return None


def _parse_markets(raw: str) -> list[tuple[str, str]]:
    '''Parse a ``GL:hl`` comma-separated market list into
    ``(gl, hl)`` pairs, preserving order and dropping invalid or
    duplicate entries. An empty language defaults to ``en``.'''

    markets: list[tuple[str, str]] = []
    seen: set[str] = set()
    for part in raw.split(','):
        token: str = part.strip()
        if not token:
            continue
        gl, _, hl = token.partition(':')
        gl = gl.strip().upper()
        hl = hl.strip() or 'en'
        if len(gl) != 2 or not gl.isalpha() or gl in seen:
            continue
        seen.add(gl)
        markets.append((gl, hl))
    return markets


@dataclass(frozen=True)
class DiscoveredChannel:
    channel_id: str | None
    channel_handle: str | None
    subscriber_count: int | None = None


class _SearchProxyPool:
    '''Unused proxies available to search workers for failover.'''

    def __init__(self, proxies: Iterable[str]) -> None:
        self._available: deque[str] = deque(proxies)

    def take(self) -> str | None:
        if not self._available:
            return None
        return self._available.popleft()

    def release(self, proxy: str) -> None:
        self._available.append(proxy)


class _SearchProxyLease:
    '''One worker's proxy plus access to shared unused proxies.'''

    def __init__(
        self,
        proxy: str | None,
        pool: _SearchProxyPool,
    ) -> None:
        self.proxy: str | None = proxy
        self._pool: _SearchProxyPool = pool
        self._reusable: bool = proxy is not None

    def replace_failed(self) -> bool:
        if self.proxy is None:
            return False
        self._reusable = False
        replacement: str | None = self._pool.take()
        if replacement is None:
            return False
        self.proxy = replacement
        self._reusable = True
        return True

    def mark_success(self) -> None:
        if self.proxy is not None:
            self._reusable = True

    def release(self) -> None:
        if self.proxy is not None and self._reusable:
            self._pool.release(self.proxy)
        self.proxy = None
        self._reusable = False


class DiscoverSearchSettings(ScraperSettings):
    '''Settings for ``yt_discover_search.py``.'''

    model_config = SettingsConfigDict(
        env_file=(
            str(Path(__file__).parent.parent / '.env'),
            '.env',
        ),
        env_file_encoding='utf-8',
        cli_parse_args=True,
        cli_kebab_case=True,
        populate_by_name=True,
        extra='ignore',
    )

    search_terms: CliPositionalArg[list[str]] = Field(
        default_factory=list,
        description=(
            'Search words/terms. Reads one per stdin line when omitted.'
        ),
    )
    log_file: str = Field(
        default='/dev/stderr',
        validation_alias=AliasChoices('LOG_FILE', 'log_file'),
        description='Log file path',
    )
    output_file: str | None = Field(
        default=None,
        validation_alias=AliasChoices('OUTPUT_FILE', 'output_file'),
        description=(
            'Optional JSONL file receiving discovered channels; use '
            '"-" for stdout. Not set (default): channels only go to '
            'the channel scrape queue in Redis.'
        ),
    )
    metrics_port: int = Field(
        default=9550,
        validation_alias=AliasChoices(
            'DISCOVER_METRICS_PORT', 'metrics_port',
        ),
        description='Port for the Prometheus metrics endpoint.',
    )
    pid_file: str = Field(
        default='/var/tmp/yt_discover_search.pid',
        validation_alias=AliasChoices('PID_FILE', 'pid_file'),
        description='File containing the running process ID.',
    )
    youtube_search_concurrency: int = Field(
        default=1,
        ge=1,
        validation_alias=AliasChoices(
            'YOUTUBE_SEARCH_CONCURRENCY',
            'youtube_search_concurrency',
        ),
        description=(
            'Maximum number of market searches processed '
            'concurrently, each worker using its own proxy. '
            'Limited to the proxy count when proxies are '
            'configured.'
        ),
    )
    random_word_url: str = Field(
        default='https://random-word-api.herokuapp.com/word',
        validation_alias=AliasChoices(
            'RANDOM_WORD_URL', 'random_word_url',
        ),
        description=(
            'Random Word API endpoint used for its eight native '
            'languages.'
        ),
    )
    keyword_count: int | None = Field(
        default=None,
        ge=1,
        validation_alias=AliasChoices(
            'KEYWORD_COUNT', 'keyword_count',
        ),
        description=(
            'Number of random words to search, once, when no keywords '
            'are supplied on stdin or the command line; the tool then '
            'exits. When not set, it runs indefinitely, searching '
            'batches of random words.'
        ),
    )
    random_word_languages: str = Field(
        default=','.join(_DEFAULT_RANDOM_WORD_LANGUAGES),
        validation_alias=AliasChoices(
            'RANDOM_WORD_LANGUAGES', 'random_word_languages',
        ),
        description=(
            'Comma-separated random-word language codes. Additional '
            'languages use Wikimedia.'
        ),
    )
    random_word_language: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'RANDOM_WORD_LANGUAGE', 'random_word_language',
        ),
        description=(
            'Optional random-word language code. When omitted, one '
            'configured language is chosen.'
        ),
    )
    discover_markets: str = Field(
        default=_DEFAULT_MARKETS,
        validation_alias=AliasChoices(
            'DISCOVER_MARKETS', 'discover_markets',
        ),
        description=(
            'Comma-separated GL:hl market pairs for the '
            'popularity-scoped pass, ordered by priority.'
        ),
    )
    discover_popular_continuations: int = Field(
        default=5,
        ge=0,
        validation_alias=AliasChoices(
            'DISCOVER_POPULAR_CONTINUATIONS',
            'discover_popular_continuations',
        ),
        description=(
            'Continuation pages fetched per term per market in the '
            'popularity-scoped pass.'
        ),
    )
    discover_min_subscribers: int = Field(
        default=4000,
        ge=0,
        validation_alias=AliasChoices(
            'DISCOVER_MIN_SUBSCRIBERS', 'discover_min_subscribers',
        ),
        description=(
            'Skip discovered channels whose known subscriber count '
            'is below this. Channels without a known count are '
            'still enqueued. 0 enqueues every discovered channel '
            '(the scrape records the real count and tier).'
        ),
    )
    discover_source: str = Field(
        default='discovered_popular',
        validation_alias=AliasChoices(
            'DISCOVER_SOURCE', 'discover_source',
        ),
        description=(
            'Queue source label set on channels enqueued by the '
            'popularity-scoped pass.'
        ),
    )


def _stdin_terms() -> list[str]:
    if sys.stdin.isatty():
        return []
    return [
        line.strip()
        for line in sys.stdin.read().splitlines()
        if line.strip()
    ]


def _normalise_languages(raw: str) -> tuple[str, ...]:
    langs = tuple(
        part.strip()
        for part in raw.split(',')
        if part.strip()
    )
    return langs or _DEFAULT_RANDOM_WORD_LANGUAGES


def _extract_word_from_random_payload(payload: Any) -> str | None:
    words = _extract_words_from_random_payload(payload)
    return words[0] if words else None


def _extract_words_from_random_payload(payload: Any) -> list[str]:
    if isinstance(payload, list):
        words: list[str] = []
        for item in payload:
            words.extend(_extract_words_from_random_payload(item))
        return words
    if isinstance(payload, dict):
        value = payload.get('word')
        return [value] if isinstance(value, str) else []
    return [payload] if isinstance(payload, str) else []


def _extract_words_from_wikimedia_payload(payload: Any) -> list[str]:
    if not isinstance(payload, dict):
        return []
    query: Any = payload.get('query')
    if not isinstance(query, dict):
        return []
    pages: Any = query.get('random')
    if not isinstance(pages, list):
        return []
    words: list[str] = []
    seen: set[str] = set()
    for page in pages:
        if not isinstance(page, dict):
            continue
        title: Any = page.get('title')
        if not isinstance(title, str):
            continue
        current: list[str] = []
        for character in title:
            is_mark: bool = unicodedata.category(character).startswith('M')
            if character.isalpha() or (current and is_mark):
                current.append(character)
                continue
            if current:
                word: str = ''.join(current)
                key: str = word.casefold()
                if key not in seen:
                    words.append(word)
                    seen.add(key)
                current = []
        if current:
            word = ''.join(current)
            key = word.casefold()
            if key not in seen:
                words.append(word)
                seen.add(key)
    return words


async def choose_random_search_term(
    settings: DiscoverSearchSettings,
    *,
    random_word_url: str | None = None,
    random_word_language: str | None = None,
) -> str:
    '''Return one random search term, falling back offline on failure.'''

    return (
        await choose_random_search_terms(
            settings,
            count=1,
            random_word_url=random_word_url,
            random_word_language=random_word_language,
        )
    )[0]


async def choose_random_search_terms(
    settings: DiscoverSearchSettings,
    *,
    count: int,
    random_word_url: str | None = None,
    random_word_language: str | None = None,
) -> list[str]:
    '''Return *count* random search terms, falling back offline.'''

    count = max(1, count)
    languages: tuple[str, ...] = _normalise_languages(
        settings.random_word_languages
    )
    lang: str = random_word_language or random.choice(languages)
    url: str = random_word_url or settings.random_word_url
    params: dict[str, str | int] = {
        'number': count,
        'length': random.randint(5, 12),
    }
    if lang != 'en':
        params['lang'] = lang
    try:
        async with httpx2.AsyncClient(
            timeout=10.0, http2=True,
        ) as client:
            if lang in _WIKIMEDIA_RANDOM_WORD_LANGUAGES:
                url = _WIKIMEDIA_RANDOM_WORD_URL_TEMPLATE.format(
                    language=lang,
                )
                params = {
                    'action': 'query',
                    'list': 'random',
                    'rnnamespace': 0,
                    'rnlimit': max(10, min(500, count * 5)),
                    'format': 'json',
                    'formatversion': 2,
                }
                response: httpx2.Response = await client.get(
                    url,
                    params=params,
                    headers={'User-Agent': _WIKIMEDIA_USER_AGENT},
                )
                response.raise_for_status()
                words = _extract_words_from_wikimedia_payload(
                    response.json(),
                )
            else:
                response = await client.get(url, params=params)
                response.raise_for_status()
                words = _extract_words_from_random_payload(
                    response.json(),
                )
            usable: list[str] = [word for word in words if len(word) >= 5]
            if usable:
                if len(usable) < count:
                    usable.extend(
                        random.choice(_OFFLINE_RANDOM_TERMS)
                        for _ in range(count - len(usable))
                    )
                return usable[:count]
            raise ValueError('random-word API returned no usable word')
    except Exception as exc:
        fallback: list[str] = [
            random.choice(_OFFLINE_RANDOM_TERMS)
            for _ in range(count)
        ]
        _LOGGER.warning(
            'Random-word lookup failed; using offline fallback',
            exc=exc,
            extra={'fallback': fallback, 'language': lang},
        )
        return fallback


def _is_channel_id(value: object) -> bool:
    return (
        isinstance(value, str)
        and YouTubeChannel.is_channel_id(value)
    )


def _normalise_handle(value: object) -> str | None:
    if not isinstance(value, str):
        return None

    text: str = value.strip()
    if not text:
        return None
    if text.startswith('https://www.youtube.com/'):
        text = text.removeprefix('https://www.youtube.com')
    if text.startswith('http://www.youtube.com/'):
        text = text.removeprefix('http://www.youtube.com')
    if text.startswith('/'):
        text = text[1:]
    if text.startswith('@'):
        handle: str = text.split('/', 1)[0]
    elif text.startswith('channel/'):
        return None
    else:
        return None

    if ' ' in handle or '/' in handle or len(handle) <= 1:
        return None
    return handle


def _candidate_from_browse_endpoint(
    endpoint: dict[str, Any],
    parent: dict[str, Any],
) -> DiscoveredChannel | None:
    channel_id: str | None = None
    if _is_channel_id(endpoint.get('browseId')):
        channel_id = str(endpoint['browseId'])
    elif _is_channel_id(parent.get('channelId')):
        channel_id = str(parent['channelId'])

    channel_handle: str | None = (
        _normalise_handle(endpoint.get('canonicalBaseUrl'))
        or _normalise_handle(
            endpoint.get('commandMetadata', {})
            .get('webCommandMetadata', {})
            .get('url')
        )
        or _normalise_handle(
            parent.get('commandMetadata', {})
            .get('webCommandMetadata', {})
            .get('url')
        )
    )
    if channel_id is None and channel_handle is None:
        return None
    return DiscoveredChannel(channel_id, channel_handle)


def _walk_json(value: Any) -> Iterable[tuple[Any, dict[str, Any] | None]]:
    stack: list[tuple[Any, dict[str, Any] | None]] = [(value, None)]
    while stack:
        current, parent = stack.pop()
        yield current, parent
        if isinstance(current, dict):
            for child in current.values():
                stack.append((child, current))
        elif isinstance(current, list):
            for child in current:
                stack.append((child, parent))


def extract_channels(payload: dict[str, Any]) -> list[DiscoveredChannel]:
    '''Extract discovered channel identities from an InnerTube payload.'''

    candidates: list[DiscoveredChannel] = []
    for value, parent in _walk_json(payload):
        if not isinstance(value, dict):
            continue
        if 'channelRenderer' in value:
            renderer = value['channelRenderer']
            if isinstance(renderer, dict):
                cid = (
                    str(renderer['channelId'])
                    if _is_channel_id(renderer.get('channelId'))
                    else None
                )
                handle = _normalise_handle(
                    renderer.get('navigationEndpoint', {})
                    .get('browseEndpoint', {})
                    .get('canonicalBaseUrl')
                )
                subs = _parse_subscriber_text(
                    (renderer.get('subscriberCountText') or {})
                    .get('simpleText')
                )
                if cid or handle:
                    candidates.append(
                        DiscoveredChannel(cid, handle, subs),
                    )
        if 'browseEndpoint' in value and isinstance(
            value['browseEndpoint'], dict,
        ):
            candidate: DiscoveredChannel | None = \
                _candidate_from_browse_endpoint(
                    value['browseEndpoint'],
                    parent if isinstance(parent, dict) else value,
                )
            if candidate is not None:
                candidates.append(candidate)
    return _dedupe_channels(candidates)


def _dedupe_channels(
    channels: Iterable[DiscoveredChannel],
) -> list[DiscoveredChannel]:
    by_key: OrderedDict[str, DiscoveredChannel] = OrderedDict()
    handle_to_key: dict[str, str] = {}

    for channel in channels:
        if channel.channel_id is None and channel.channel_handle is None:
            continue
        key: str = (
            f'id:{channel.channel_id}'
            if channel.channel_id is not None
            else f'handle:{channel.channel_handle}'
        )
        if (
            channel.channel_id is not None
            and channel.channel_handle is not None
            and channel.channel_handle in handle_to_key
        ):
            old_key: str = handle_to_key[channel.channel_handle]
            if old_key.startswith('handle:'):
                by_key.pop(old_key, None)
        existing: DiscoveredChannel | None = by_key.get(key)
        if existing is not None:
            channel = DiscoveredChannel(
                existing.channel_id or channel.channel_id,
                existing.channel_handle or channel.channel_handle,
                existing.subscriber_count or channel.subscriber_count,
            )
        by_key[key] = channel
        if channel.channel_handle is not None:
            handle_to_key[channel.channel_handle] = key

    return list(by_key.values())


def _get_continuation_token(payload: dict[str, Any]) -> str | None:
    for value, _ in _walk_json(payload):
        if not isinstance(value, dict):
            continue
        token = (
            value.get('continuationItemRenderer', {})
            .get('continuationEndpoint', {})
            .get('continuationCommand', {})
            .get('token')
        )
        if isinstance(token, str) and token:
            return token
    return None


async def _fetch_page_with_retry(
    fetch: Callable[[str | None], Awaitable[dict[str, Any]]],
    *,
    proxy: str | None,
    proxy_lease: _SearchProxyLease | None,
    log_extra: dict[str, Any],
) -> dict[str, Any] | None:
    '''Call ``fetch(proxy)`` for one search page with transient
    retries.

    A connection failure rotates through every unused proxy in the
    worker pool. Other transient errors are retried once. Returns
    ``None`` after retries are exhausted so the caller moves to the
    next term; non-transient errors propagate.
    '''

    attempt: int = 0
    retry_available: bool = True
    while True:
        active_proxy: str | None = (
            proxy_lease.proxy
            if proxy_lease is not None
            else proxy
        )
        try:
            result: dict[str, Any] = await fetch(active_proxy)
            if proxy_lease is not None:
                proxy_lease.mark_success()
            await YouTubeRateLimiter.get().report_proxy_result(
                active_proxy, True,
            )
            return result
        except _TRANSIENT_SEARCH_ERRORS as exc:
            METRIC_SEARCH_ERRORS.labels(
                platform='youtube', error_type=type(exc).__name__,
            ).inc()
            if isinstance(exc, InnerTubeRequestError):
                METRIC_SEARCH_HTTP_ERRORS.labels(
                    platform='youtube',
                    status=str(getattr(exc.error, 'code', 'unknown')),
                ).inc()
            if isinstance(exc, _PROXY_CONNECTION_ERRORS):
                await YouTubeRateLimiter.get().report_proxy_result(
                    active_proxy, False,
                )
            replaced_proxy: bool = (
                isinstance(exc, _PROXY_CONNECTION_ERRORS)
                and proxy_lease is not None
                and proxy_lease.replace_failed()
            )
            should_retry: bool = replaced_proxy or retry_available
            if replaced_proxy:
                action: str = 'retrying with different proxy'
            elif retry_available:
                action = 'retrying'
                retry_available = False
            else:
                action = 'giving up on term'
            _LOGGER.warning(
                f'InnerTube search page failed; {action}',
                exc=exc,
                extra={
                    **log_extra,
                    'error_type': type(exc).__name__,
                    'attempt': attempt,
                    'proxy_ip': (
                        extract_proxy_ip(active_proxy)
                        if active_proxy else 'none'
                    ),
                    'proxy_port': (
                        extract_proxy_port(active_proxy)
                        if active_proxy else 'none'
                    ),
                },
            )
            if not should_retry:
                return None
            attempt += 1
            await asyncio.sleep(_SEARCH_RETRY_BACKOFF_SECONDS)


_Job = TypeVar('_Job')


async def _run_search_workers(
    jobs: Iterable[_Job],
    *,
    concurrency: int,
    proxies: Iterable[str],
    run_job: Callable[
        [_Job, _SearchProxyLease], AsyncIterator[DiscoveredChannel]
    ],
    worker_name: str,
) -> AsyncIterator[DiscoveredChannel]:
    '''Run *jobs* from one shared queue across concurrent workers.

    Each worker receives a distinct proxy lease and calls
    ``run_job(job, lease)`` for every job it takes, forwarding the
    yielded channels. Concurrency is capped by the number of
    proxies, or set to one direct worker when no proxies are
    configured. Connection failures rotate through the shared pool
    of proxies not currently leased by another worker. A
    non-transient exception in any worker stops the whole run.
    '''

    if concurrency < 1:
        raise ValueError(
            f'concurrency must be >= 1, got {concurrency!r}',
        )

    proxy_list: list[str] = list(dict.fromkeys(proxies))
    worker_count: int
    if proxy_list:
        worker_count = min(concurrency, len(proxy_list))
    else:
        worker_count = 1

    proxy_pool: _SearchProxyPool = _SearchProxyPool(proxy_list)
    proxy_leases: list[_SearchProxyLease] = [
        _SearchProxyLease(proxy_pool.take(), proxy_pool)
        for _ in range(worker_count)
    ]

    job_queue: asyncio.Queue[_Job | None] = asyncio.Queue()
    result_queue: asyncio.Queue[
        DiscoveredChannel | Exception | None
    ] = asyncio.Queue()
    for job in jobs:
        job_queue.put_nowait(job)
    for _ in proxy_leases:
        job_queue.put_nowait(None)

    async def worker(proxy_lease: _SearchProxyLease) -> None:
        try:
            while True:
                job: _Job | None = await job_queue.get()
                try:
                    if job is None:
                        return
                    async for channel in run_job(job, proxy_lease):
                        await result_queue.put(channel)
                except Exception as exc:
                    await result_queue.put(exc)
                    return
                finally:
                    job_queue.task_done()
        finally:
            proxy_lease.release()
            await result_queue.put(None)

    tasks: list[asyncio.Task[None]] = [
        asyncio.create_task(
            worker(proxy_lease),
            name=f'{worker_name}-{index}',
        )
        for index, proxy_lease in enumerate(proxy_leases)
    ]
    finished: int = 0
    try:
        while finished < len(tasks):
            result: DiscoveredChannel | Exception | None = (
                await result_queue.get()
            )
            if result is None:
                finished += 1
            elif isinstance(result, Exception):
                raise result
            else:
                yield result
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def _innertube_search_localized(
    term: str,
    *,
    params: str | None,
    continuation: str | None,
    proxy: str | None,
    gl: str,
    hl: str,
    limiter: YouTubeRateLimiter,
) -> dict[str, Any]:
    '''One market-scoped InnerTube SEARCH call.'''

    await limiter.acquire(YouTubeCallType.SEARCH, proxy=proxy)
    client = pooled_innertube_localized_for_entry(proxy, gl, hl)
    if continuation:
        fn = functools.partial(client.search, continuation=continuation)
    else:
        fn = functools.partial(client.search, query=term, params=params)
    with METRIC_SEARCH_DURATION.labels(
        platform='youtube',
        page_type='continuation' if continuation else 'initial',
    ).time():
        return await run_on_innertube_executor(fn)


async def _search_page_localized_with_retry(
    term: str,
    *,
    params: str | None,
    continuation: str | None,
    proxy: str | None,
    gl: str,
    hl: str,
    limiter: YouTubeRateLimiter,
    proxy_lease: _SearchProxyLease | None = None,
) -> dict[str, Any] | None:
    '''Fetch one market-scoped search page with transient retries;
    see :func:`_fetch_page_with_retry`.'''

    async def fetch(active_proxy: str | None) -> dict[str, Any]:
        return await _innertube_search_localized(
            term,
            params=params,
            continuation=continuation,
            proxy=active_proxy,
            gl=gl,
            hl=hl,
            limiter=limiter,
        )

    return await _fetch_page_with_retry(
        fetch,
        proxy=proxy,
        proxy_lease=proxy_lease,
        log_extra={
            'search_term': term,
            'gl': gl,
            'hl': hl,
            'has_continuation': bool(continuation),
        },
    )


async def discover_popular_for_market(
    term: str,
    *,
    params: str,
    gl: str,
    hl: str,
    continuations: int,
    limiter: YouTubeRateLimiter,
    proxy: str | None = None,
    proxy_lease: _SearchProxyLease | None = None,
) -> AsyncIterator[DiscoveredChannel]:
    '''Search *term* in the ``(gl, hl)`` market with *params* (a
    sort/filter blob such as :data:`POPULARITY_CHANNEL_PARAMS`),
    yielding discovered channels per page.

    This is the market-scoped, popularity-ordered enumeration that
    replaces the retired trending feed: YouTube's trending/explore
    browseIds now return HTTP 400 and ``/feed/trending`` redirects
    to Home, so the only surface that still orders results by
    popularity per country is search with the sort param.

    Channels are yielded in discovery order; cross-page and
    cross-term deduplication is the caller's responsibility (see
    :class:`_ChannelEmitter`).
    '''

    if proxy_lease is not None:
        proxy = proxy_lease.proxy
    elif proxy is None:
        proxy = limiter.select_proxy(YouTubeCallType.SEARCH)
    continuation: str | None = None
    pages: int = max(0, continuations) + 1
    facet: str = _facet_label(params)
    for page in range(pages):
        start: float = time.monotonic()
        payload: dict[str, Any] | None = (
            await _search_page_localized_with_retry(
                term,
                params=params,
                continuation=continuation,
                proxy=proxy,
                gl=gl,
                hl=hl,
                limiter=limiter,
                proxy_lease=proxy_lease,
            )
        )
        METRIC_SEARCH_PAGES.labels(
            platform='youtube', market=gl, facet=facet,
            page_type='continuation' if page else 'initial',
            outcome='failed' if payload is None else 'success',
        ).inc()
        if payload is None:
            break
        page_channels: list[DiscoveredChannel] = extract_channels(payload)
        METRIC_CHANNELS_FOUND.labels(
            platform='youtube', facet=facet,
        ).inc(len(page_channels))
        for channel in page_channels:
            yield channel
        continuation = _get_continuation_token(payload)
        _LOGGER.info(
            'Market search page processed',
            extra={
                'search_term': term,
                'gl': gl,
                'hl': hl,
                'page': page,
                'duration': time.monotonic() - start,
                'has_continuation': bool(continuation),
            },
        )
        if not continuation:
            break


async def _channel_exists_on_exchange(
    http_client: Any,
    exchange_url: str,
    channel_id: str,
) -> bool | None:
    '''Return True/False when the exchange answers 200/404 for the
    channel record, ``None`` on any other outcome.'''

    url: str = (
        f'{exchange_url.rstrip("/")}'
        f'/api/v1/data/content/youtube/channel/{channel_id}'
    )
    try:
        response = await http_client.get(url)
    except Exception as exc:
        _LOGGER.debug(
            'exchange channel existence check failed',
            exc=exc,
            extra={'channel_id': channel_id},
        )
        return None
    if response.status_code == 404:
        return False
    if response.status_code == 200:
        return True
    return None


async def enqueue_discovered_channels(
    channels: Iterable[DiscoveredChannel],
    *,
    creator_map: CreatorMap,
    queue: RedisChannelScrapeQueue,
    http_client: Any,
    exchange_url: str,
    source: str,
    min_subscribers: int = 0,
) -> dict[str, int]:
    '''Enqueue discovered channels that are neither already scraped
    nor already present on scrape.exchange.

    Dedupe mirrors the channel scraper's link discovery: a channel
    in the ``creator_map`` (already scraped) or on the exchange is
    skipped, and a failed existence check skips the channel rather
    than enqueuing blind. ``enqueue_new`` atomically leaves alone any
    channel the channel queue already knows: terminal ones are
    counted as ``terminal_skipped``; queued, in-flight or pending
    ones as ``already_queued``.
    Returns per-outcome counts.
    '''

    counts: dict[str, int] = {
        'enqueued': 0,
        'already_scraped': 0,
        'on_exchange': 0,
        'check_failed': 0,
        'below_min_subscribers': 0,
        'no_channel_id': 0,
        'already_queued': 0,
        'terminal_skipped': 0,
    }
    for channel in channels:
        channel_id: str | None = channel.channel_id
        if not channel_id or not YouTubeChannel.is_channel_id(channel_id):
            counts['no_channel_id'] += 1
            continue
        subs: int | None = channel.subscriber_count
        if subs is not None and subs < min_subscribers:
            counts['below_min_subscribers'] += 1
            continue
        try:
            known: str | None = await creator_map.get(channel_id)
        except Exception:
            known = None
        if known:
            counts['already_scraped'] += 1
            continue
        exists: bool | None = await _channel_exists_on_exchange(
            http_client, exchange_url, channel_id,
        )
        if exists is True:
            counts['on_exchange'] += 1
            continue
        if exists is None:
            counts['check_failed'] += 1
            continue
        try:
            # Only channels the channel queue has never seen: queued,
            # in-flight, pending and terminal channels are left alone.
            result: str = await queue.enqueue_new(
                channel_id, source=source,
            )
        except Exception as exc:
            _LOGGER.warning(
                'failed to enqueue discovered channel',
                exc=exc,
                extra={'channel_id': channel_id},
            )
            continue
        if result == 'enqueued':
            counts['enqueued'] += 1
        elif result == 'terminal':
            counts['terminal_skipped'] += 1
        else:
            counts['already_queued'] += 1
    return counts


async def _build_queue_backends(
    settings: DiscoverSearchSettings,
) -> tuple[
    RedisCreatorMap | None,
    RedisChannelScrapeQueue | None,
    Any,
]:
    '''Build the creator_map, channel queue, and Redis client when
    ``REDIS_DSN`` is set; otherwise return ``(None, None, None)`` so
    discovery still runs but nothing is enqueued.'''

    if not settings.redis_dsn:
        _LOGGER.info(
            'REDIS_DSN not set; discovered channels will not be '
            'enqueued',
        )
        return None, None, None
    redis = redis_from_url(
        settings.redis_dsn,
        component='yt_discover_search',
        decode_responses=True,
    )
    creator_map: RedisCreatorMap = RedisCreatorMap(
        settings.redis_dsn, platform='youtube',
    )
    queue: RedisChannelScrapeQueue = RedisChannelScrapeQueue(
        redis, ChannelScrapeQueueSettings(),
    )
    return creator_map, queue, redis


@dataclass(frozen=True)
class _PopularSearchJob:
    '''One market x term x facet search of the popularity pass.'''

    gl: str
    hl: str
    term: str
    params: str


async def _run_popular_discovery(
    settings: DiscoverSearchSettings,
    limiter: YouTubeRateLimiter,
    terms: list[str],
) -> int:
    '''Run the market x term popularity-scoped discovery pass.

    Every market x term x facet (channel and video) search is a job
    on one shared queue, processed by up to
    ``YOUTUBE_SEARCH_CONCURRENCY`` workers, each holding its own
    proxy (see :func:`_run_search_workers`). Jobs are queued in
    market priority order. When Redis is configured, each job's
    channels are enqueued on the channel scrape queue after the
    known/on-exchange dedupe. Channels are also written to
    ``--output-file`` only when it is set; SIGHUP reopens that file.
    '''

    markets: list[tuple[str, str]] = _parse_markets(
        settings.discover_markets,
    )
    creator_map, queue, redis = await _build_queue_backends(settings)
    continuations: int = settings.discover_popular_continuations
    jobs: list[_PopularSearchJob] = [
        _PopularSearchJob(gl, hl, term, params)
        for gl, hl in markets
        for term in terms
        for params in (
            POPULARITY_CHANNEL_PARAMS,
            POPULARITY_VIDEO_PARAMS,
        )
    ]
    METRIC_JOBS_PLANNED.labels(platform='youtube').set(len(jobs))
    totals: dict[str, int] = {}
    async with httpx2.AsyncClient(
        timeout=30.0, http2=True,
    ) as http_client:

        async def run_job(
            job: _PopularSearchJob,
            proxy_lease: _SearchProxyLease,
        ) -> AsyncIterator[DiscoveredChannel]:
            batch: list[DiscoveredChannel] = []
            async for channel in discover_popular_for_market(
                job.term,
                params=job.params,
                gl=job.gl,
                hl=job.hl,
                continuations=continuations,
                limiter=limiter,
                proxy_lease=proxy_lease,
            ):
                batch.append(channel)
                yield channel
            METRIC_JOBS_COMPLETED.labels(platform='youtube').inc()
            if queue is None or creator_map is None:
                return
            counts: dict[str, int] = (
                await enqueue_discovered_channels(
                    batch,
                    creator_map=creator_map,
                    queue=queue,
                    http_client=http_client,
                    exchange_url=settings.exchange_url,
                    source=settings.discover_source,
                    min_subscribers=settings.discover_min_subscribers,
                )
            )
            _record_enqueue_outcomes(counts)
            for key, value in counts.items():
                totals[key] = totals.get(key, 0) + value

        channels: AsyncIterator[DiscoveredChannel] = _run_search_workers(
            jobs,
            concurrency=settings.youtube_search_concurrency,
            proxies=settings.proxies,
            run_job=run_job,
            worker_name='youtube-popular-search-worker',
        )
        if not settings.output_file:
            async for _ in channels:
                pass
        else:
            with _channel_output_stream(
                settings.output_file,
            ) as stream, _reopen_on_sighup(stream, settings.output_file):
                emitter: _ChannelEmitter = _ChannelEmitter(stream)
                async for channel in channels:
                    emitter.emit(channel)
    _LOGGER.info(
        'Popular discovery pass complete',
        extra={
            'markets': len(markets),
            'terms': len(terms),
            'totals': totals,
        },
    )
    if redis is not None:
        await redis.aclose()
    return 0


def _channel_to_json(channel: DiscoveredChannel) -> str:
    return json.dumps(
        {
            'channel_id': channel.channel_id,
            'channel_handle': channel.channel_handle,
        },
        ensure_ascii=False,
        separators=(',', ':'),
    )


class _ChannelEmitter:
    '''Stream discovered channels to *stream*, one JSON line each,
    deduplicating across the whole run.

    A channel is written the first time its ``channel_id`` is
    seen, or — when it carries no ``channel_id`` — the first time
    its ``channel_handle`` is seen. A handle that was emitted
    without an id is re-emitted once a record carrying both the
    handle and a new id arrives, so the id is not lost (the
    "upgrade" case). Each line is flushed immediately so a
    downstream pipe sees channels as they are discovered and a
    mid-run crash leaves valid partial output.
    '''

    def __init__(self, stream: Any) -> None:
        self._stream: Any = stream
        self._seen_ids: set[str] = set()
        self._seen_handles: set[str] = set()

    def emit(self, channel: DiscoveredChannel) -> bool:
        '''Write *channel* if it is new; return whether it was
        written.'''

        cid: str | None = channel.channel_id
        handle: str | None = channel.channel_handle
        if cid is not None:
            if cid in self._seen_ids:
                return False
        elif handle is None or handle in self._seen_handles:
            return False

        if cid is not None:
            self._seen_ids.add(cid)
        if handle is not None:
            self._seen_handles.add(handle)
        self._stream.write(f'{_channel_to_json(channel)}\n')
        self._stream.flush()
        return True


class _ChannelOutputStream:
    '''Append-only output stream that can reopen its pathname.'''

    def __init__(self, output_file: str) -> None:
        self._path: Path | None = (
            None
            if output_file == '-'
            else Path(output_file).expanduser()
        )
        self._stream: Any = None

    def __enter__(self) -> _ChannelOutputStream:
        if self._path is None:
            self._stream = sys.stdout
        else:
            self.reopen()
        return self

    def __exit__(self, *args: object) -> None:
        del args
        if self._path is not None and self._stream is not None:
            self._stream.close()
        self._stream = None

    def reopen(self) -> None:
        if self._path is None:
            return
        self._path.parent.mkdir(parents=True, exist_ok=True)
        replacement: Any = self._path.open(
            'a', encoding='utf-8',
        )
        previous: Any = self._stream
        self._stream = replacement
        if previous is not None:
            previous.close()

    def write(self, value: str) -> int:
        return self._stream.write(value)

    def flush(self) -> None:
        self._stream.flush()


def _channel_output_stream(
    output_file: str,
) -> _ChannelOutputStream:
    return _ChannelOutputStream(output_file)


@contextmanager
def _reopen_on_sighup(
    stream: _ChannelOutputStream,
    output_file: str,
) -> Iterator[None]:
    '''Reopen *stream* on SIGHUP so logrotate can move the output
    file; a no-op when writing to stdout (``-``).'''

    if output_file == '-':
        yield
        return
    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGHUP, stream.reopen)
    try:
        yield
    finally:
        loop.remove_signal_handler(signal.SIGHUP)


class _PidFileError(RuntimeError):
    pass


class _PidFile:
    '''Own one process-ID file for the current process lifetime.'''

    def __init__(self, path: str) -> None:
        self._path: Path = Path(path).expanduser()
        self._identity: tuple[int, int] | None = None

    def acquire(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        while True:
            try:
                fd: int = os.open(
                    self._path,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    0o600,
                )
            except FileExistsError:
                self._remove_stale_file()
                continue
            try:
                file_stat: os.stat_result = os.fstat(fd)
                self._identity = (
                    file_stat.st_dev,
                    file_stat.st_ino,
                )
                with os.fdopen(
                    fd, 'w', encoding='utf-8',
                ) as stream:
                    fd = -1
                    stream.write(f'{os.getpid()}\n')
            finally:
                if fd >= 0:
                    os.close(fd)
            return

    @staticmethod
    def _process_is_running(pid: int) -> bool:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True

    def _remove_stale_file(self) -> None:
        try:
            path_stat: os.stat_result = os.lstat(self._path)
        except FileNotFoundError:
            return
        if not stat.S_ISREG(path_stat.st_mode):
            raise _PidFileError(
                f'{self._path} is not a regular file',
            )
        if path_stat.st_uid != os.geteuid():
            raise _PidFileError(
                f'{self._path} is not owned by the current user',
            )

        flags: int = os.O_RDONLY
        if hasattr(os, 'O_NOFOLLOW'):
            flags |= os.O_NOFOLLOW
        try:
            fd: int = os.open(self._path, flags)
        except FileNotFoundError:
            return
        except OSError as exc:
            if exc.errno == errno.ELOOP:
                raise _PidFileError(
                    f'{self._path} is not a regular file',
                ) from exc
            raise
        try:
            file_stat: os.stat_result = os.fstat(fd)
            if not stat.S_ISREG(file_stat.st_mode):
                raise _PidFileError(
                    f'{self._path} is not a regular file',
                )
            if file_stat.st_uid != os.geteuid():
                raise _PidFileError(
                    f'{self._path} is not owned by the current user',
                )
            with os.fdopen(
                fd, 'r', encoding='utf-8',
            ) as stream:
                fd = -1
                raw_pid: str = stream.read(128).strip()
        finally:
            if fd >= 0:
                os.close(fd)

        try:
            pid: int = int(raw_pid)
        except ValueError:
            pid = -1
        if pid > 0 and self._process_is_running(pid):
            raise _PidFileError(
                f'process {pid} from {self._path} is still running',
            )

        try:
            current: os.stat_result = os.lstat(self._path)
        except FileNotFoundError:
            return
        if (
            current.st_dev == file_stat.st_dev
            and current.st_ino == file_stat.st_ino
        ):
            self._path.unlink()

    def release(self) -> None:
        if self._identity is None:
            return
        try:
            file_stat: os.stat_result = self._path.stat()
            contents: str = self._path.read_text(
                encoding='utf-8',
            )
        except FileNotFoundError:
            return
        identity: tuple[int, int] = (
            file_stat.st_dev,
            file_stat.st_ino,
        )
        if (
            identity == self._identity
            and contents.strip() == str(os.getpid())
        ):
            self._path.unlink()


async def _run_discovery(settings: DiscoverSearchSettings) -> int:
    configure_logging(
        level=settings.log_level,
        filename=settings.log_file,
        log_format=settings.log_format,
    )
    configure_innertube_executor(settings.innertube_executor_threads)
    _start_metrics(settings.metrics_port)
    limiter = YouTubeRateLimiter.get(
        state_dir=settings.rate_limiter_state_dir,
        redis_dsn=settings.redis_dsn,
    )
    limiter.set_proxies(list(settings.proxies) or None)

    terms: list[str] = list(settings.search_terms) or _stdin_terms()
    try:
        if terms:
            return await _run_popular_discovery(settings, limiter, terms)
        if settings.keyword_count is not None:
            terms = await choose_random_search_terms(
                settings,
                count=settings.keyword_count,
                random_word_language=settings.random_word_language,
            )
            return await _run_popular_discovery(settings, limiter, terms)
        discovery_round: int = 0
        while True:
            discovery_round += 1
            terms = await choose_random_search_terms(
                settings,
                count=UNLIMITED_KEYWORD_BATCH,
                random_word_language=settings.random_word_language,
            )
            _LOGGER.info(
                'Starting discovery round',
                extra={
                    'discovery_round': discovery_round,
                    'terms': len(terms),
                },
            )
            await _run_popular_discovery(settings, limiter, terms)
    finally:
        await aclose_pooled_innertube()
        shutdown_innertube_executor()


async def main_async(argv: list[str] | None = None) -> int:
    settings = DiscoverSearchSettings(_cli_parse_args=argv)
    pid_file = _PidFile(settings.pid_file)
    try:
        pid_file.acquire()
    except _PidFileError as exc:
        sys.stderr.write(f'pid file error: {exc}\n')
        return 1
    try:
        return await _run_discovery(settings)
    finally:
        pid_file.release()


def main() -> None:
    raise SystemExit(asyncio.run(main_async()))


if __name__ == '__main__':
    main()
