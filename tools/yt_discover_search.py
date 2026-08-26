#!/usr/bin/env python3
'''Discover YouTube channels from InnerTube search results.'''

from __future__ import annotations

import asyncio
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
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator, Iterable

import httpx
from innertube.errors import RequestError as InnerTubeRequestError
from innertube.errors import ResponseError as InnerTubeResponseError
from pydantic import AliasChoices, Field
from pydantic_settings import CliPositionalArg, SettingsConfigDict

from scrape_exchange.logging import configure_logging
from scrape_exchange.settings import ScraperSettings
from scrape_exchange.util import extract_proxy_ip, extract_proxy_port
from scrape_exchange.youtube.youtube_channel import YouTubeChannel
from scrape_exchange.youtube.youtube_channel_tabs import (
    aclose_pooled_innertube,
    configure_innertube_executor,
    pooled_innertube_for_entry,
    run_on_innertube_executor,
    shutdown_innertube_executor,
)
from scrape_exchange.youtube.youtube_rate_limiter import (
    YouTubeCallType,
    YouTubeRateLimiter,
)


_LOGGER: logging.Logger = logging.getLogger(__name__)

_DEFAULT_OUTPUT_FILE: str = 'data/searched_channels.jsonl'
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
    httpx.TransportError,
    ConnectionResetError,
    ConnectionRefusedError,
)
_SEARCH_RETRY_BACKOFF_SECONDS: float = 2.0


@dataclass(frozen=True)
class DiscoveredChannel:
    channel_id: str | None
    channel_handle: str | None


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
    output_file: str = Field(
        default=_DEFAULT_OUTPUT_FILE,
        validation_alias=AliasChoices('OUTPUT_FILE', 'output_file'),
        description=(
            'JSONL file receiving discovered channels. Use "-" to '
            'write channels to stdout.'
        ),
    )
    pid_file: str = Field(
        default='/var/tmp/yt_discover_search.pid',
        validation_alias=AliasChoices('PID_FILE', 'pid_file'),
        description='File containing the running process ID.',
    )
    youtube_search_continuations: int = Field(
        default=10,
        validation_alias=AliasChoices(
            'YOUTUBE_SEARCH_CONTINUATIONS',
            'youtube_search_continuations',
        ),
        description=(
            'Maximum InnerTube search continuation pages per term.'
        ),
    )
    youtube_search_concurrency: int = Field(
        default=1,
        ge=1,
        validation_alias=AliasChoices(
            'YOUTUBE_SEARCH_CONCURRENCY',
            'youtube_search_concurrency',
        ),
        description=(
            'Maximum number of search terms processed concurrently. '
            'Limited to the proxy count when proxies are configured.'
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
    keyword_count: int = Field(
        default=1,
        ge=1,
        validation_alias=AliasChoices(
            'KEYWORD_COUNT', 'keyword_count',
        ),
        description=(
            'Number of random words to request and search when no '
            'keywords are supplied on stdin or the command line.'
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
        async with httpx.AsyncClient(timeout=10.0) as client:
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
                response: httpx.Response = await client.get(
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
                if cid or handle:
                    candidates.append(DiscoveredChannel(cid, handle))
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


async def _innertube_search(
    term: str,
    *,
    continuation: str | None,
    proxy: str | None,
    limiter: YouTubeRateLimiter,
) -> dict[str, Any]:
    await limiter.acquire(YouTubeCallType.BROWSE, proxy=proxy)
    client = pooled_innertube_for_entry(proxy)
    if continuation:
        fn = functools.partial(client.search, continuation=continuation)
    else:
        fn = functools.partial(client.search, query=term)
    result = await run_on_innertube_executor(fn)
    return result


async def _search_page_with_retry(
    term: str,
    *,
    continuation: str | None,
    proxy: str | None,
    limiter: YouTubeRateLimiter,
    proxy_lease: _SearchProxyLease | None = None,
) -> dict[str, Any] | None:
    '''Fetch one InnerTube search page with transient retries.

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
            result: dict[str, Any] = await _innertube_search(
                term,
                continuation=continuation,
                proxy=active_proxy,
                limiter=limiter,
            )
            if proxy_lease is not None:
                proxy_lease.mark_success()
            return result
        except _TRANSIENT_SEARCH_ERRORS as exc:
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
                    'search_term': term,
                    'error_type': type(exc).__name__,
                    'attempt': attempt,
                    'has_continuation': bool(continuation),
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


async def discover_for_term(
    term: str,
    *,
    continuations: int,
    limiter: YouTubeRateLimiter,
    proxy: str | None = None,
    proxy_lease: _SearchProxyLease | None = None,
) -> AsyncIterator[DiscoveredChannel]:
    '''Search one term, yielding discovered channels per page.

    Each search page is parsed as soon as it arrives and its
    channels (deduped within the page by :func:`extract_channels`)
    are yielded in discovery order. Cross-page and cross-term
    deduplication is the caller's responsibility (see
    :class:`_ChannelEmitter`).

    A connection failure rotates through unused proxies. Other
    transient failures are retried once. If retries are exhausted,
    the term stops paginating and the caller continues with the next
    term. Channels yielded from earlier pages are kept.
    '''

    if proxy_lease is not None:
        proxy = proxy_lease.proxy
    elif proxy is None:
        proxy = limiter.select_proxy(YouTubeCallType.BROWSE)
    continuation: str | None = None
    pages = max(0, continuations) + 1

    for page in range(pages):
        start = time.monotonic()
        payload = await _search_page_with_retry(
            term,
            continuation=continuation,
            proxy=proxy,
            limiter=limiter,
            proxy_lease=proxy_lease,
        )
        if payload is None:
            break
        for channel in extract_channels(payload):
            yield channel
        continuation = _get_continuation_token(payload)
        _LOGGER.info(
            'InnerTube search page processed',
            extra={
                'search_term': term,
                'page': page,
                'duration': time.monotonic() - start,
                'has_continuation': bool(continuation),
            },
        )
        if not continuation:
            break


async def discover_for_terms(
    terms: Iterable[str],
    *,
    continuations: int,
    concurrency: int,
    proxies: Iterable[str],
    limiter: YouTubeRateLimiter,
) -> AsyncIterator[DiscoveredChannel]:
    '''Discover channels from one shared queue of search terms.

    Each consumer receives a distinct proxy. Concurrency is capped
    by the number of proxies, or set to one direct consumer when no
    proxies are configured. Connection failures rotate through the
    shared pool of proxies not currently leased by another worker.
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

    term_queue: asyncio.Queue[str | None] = asyncio.Queue()
    result_queue: asyncio.Queue[
        DiscoveredChannel | Exception | None
    ] = asyncio.Queue()
    for term in terms:
        term_queue.put_nowait(term)
    for _ in proxy_leases:
        term_queue.put_nowait(None)

    async def worker(proxy_lease: _SearchProxyLease) -> None:
        try:
            while True:
                term: str | None = await term_queue.get()
                try:
                    if term is None:
                        return
                    async for channel in discover_for_term(
                        term,
                        continuations=continuations,
                        limiter=limiter,
                        proxy_lease=proxy_lease,
                    ):
                        await result_queue.put(channel)
                except Exception as exc:
                    await result_queue.put(exc)
                    return
                finally:
                    term_queue.task_done()
        finally:
            proxy_lease.release()
            await result_queue.put(None)

    tasks: list[asyncio.Task[None]] = [
        asyncio.create_task(
            worker(proxy_lease),
            name=f'youtube-search-worker-{index}',
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
    limiter = YouTubeRateLimiter.get(
        state_dir=settings.rate_limiter_state_dir,
        redis_dsn=settings.redis_dsn,
    )
    limiter.set_proxies(list(settings.proxies) or None)

    terms: list[str] = list(settings.search_terms) or _stdin_terms()
    if not terms:
        terms = await choose_random_search_terms(
            settings,
            count=settings.keyword_count,
            random_word_language=settings.random_word_language,
        )

    try:
        with _channel_output_stream(settings.output_file) as stream:
            loop: asyncio.AbstractEventLoop = (
                asyncio.get_running_loop()
            )
            sighup_installed: bool = False
            if settings.output_file != '-':
                loop.add_signal_handler(
                    signal.SIGHUP, stream.reopen,
                )
                sighup_installed = True
            try:
                emitter = _ChannelEmitter(stream)
                async for channel in discover_for_terms(
                    terms,
                    continuations=(
                        settings.youtube_search_continuations
                    ),
                    concurrency=settings.youtube_search_concurrency,
                    proxies=settings.proxies,
                    limiter=limiter,
                ):
                    emitter.emit(channel)
            finally:
                if sighup_installed:
                    loop.remove_signal_handler(signal.SIGHUP)
        return 0
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
