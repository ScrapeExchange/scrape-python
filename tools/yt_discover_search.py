#!/usr/bin/env python3
'''Discover YouTube channels from InnerTube search results.'''

from __future__ import annotations

import asyncio
import contextlib
import functools
import json
import logging
import random
import sys
import time
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator, Iterable, Iterator

import httpx
from innertube.errors import RequestError as InnerTubeRequestError
from innertube.errors import ResponseError as InnerTubeResponseError
from pydantic import AliasChoices, Field
from pydantic_settings import CliPositionalArg, SettingsConfigDict

from scrape_exchange.logging import configure_logging
from scrape_exchange.settings import ScraperSettings
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
_DEFAULT_RANDOM_WORD_LANGUAGES: tuple[str, ...] = (
    'en', 'es', 'it', 'de', 'fr', 'zh', 'pt-br', 'ro',
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
_SEARCH_RETRY_BACKOFF_SECONDS: float = 2.0


@dataclass(frozen=True)
class DiscoveredChannel:
    channel_id: str | None
    channel_handle: str | None


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
    random_word_url: str = Field(
        default='https://random-word-api.herokuapp.com/word',
        validation_alias=AliasChoices(
            'RANDOM_WORD_URL', 'random_word_url',
        ),
        description='Random-word API endpoint used when no term is given.',
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
            'Comma-separated random-word API language codes.'
        ),
    )
    random_word_language: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'RANDOM_WORD_LANGUAGE', 'random_word_language',
        ),
        description=(
            'Optional random-word API language code for fallback term '
            'selection. When omitted, one configured language is chosen.'
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
            response: httpx.Response = await client.get(url, params=params)
            response.raise_for_status()
            words: list[str] = _extract_words_from_random_payload(
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
) -> dict[str, Any] | None:
    '''Fetch one InnerTube search page, retrying once on a
    transient error.

    Returns the search payload, or ``None`` when both the initial
    attempt and the single retry failed with a transient error
    (timeout / connection reset / InnerTube request error). A
    ``None`` return tells the caller to stop paginating the
    current term and move on; non-transient errors propagate.
    '''

    for attempt in range(2):  # initial attempt + one retry
        try:
            return await _innertube_search(
                term,
                continuation=continuation,
                proxy=proxy,
                limiter=limiter,
            )
        except _TRANSIENT_SEARCH_ERRORS as exc:
            action: str = (
                'retrying' if attempt == 0 else 'giving up on term'
            )
            _LOGGER.warning(
                f'InnerTube search page failed; {action}',
                exc=exc,
                extra={
                    'search_term': term,
                    'error_type': type(exc).__name__,
                    'attempt': attempt,
                    'has_continuation': bool(continuation),
                    'proxy': proxy,
                },
            )
            if attempt == 0:
                await asyncio.sleep(_SEARCH_RETRY_BACKOFF_SECONDS)
    return None


async def discover_for_term(
    term: str,
    *,
    continuations: int,
    limiter: YouTubeRateLimiter,
) -> AsyncIterator[DiscoveredChannel]:
    '''Search one term, yielding discovered channels per page.

    Each search page is parsed as soon as it arrives and its
    channels (deduped within the page by :func:`extract_channels`)
    are yielded in discovery order. Cross-page and cross-term
    deduplication is the caller's responsibility (see
    :class:`_ChannelEmitter`).

    A page that fails transiently (timeout / connection error /
    InnerTube request error) is retried once; if it still fails
    the term stops paginating and the caller continues with the
    next term. Channels yielded from earlier pages are kept.
    '''

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


@contextlib.contextmanager
def _channel_output_stream(output_file: str) -> Iterator[Any]:
    if output_file == '-':
        yield sys.stdout
        return

    path: Path = Path(output_file).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a', encoding='utf-8') as stream:
        yield stream


async def main_async(argv: list[str] | None = None) -> int:
    settings = DiscoverSearchSettings(_cli_parse_args=argv)
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
            emitter = _ChannelEmitter(stream)
            for term in terms:
                async for channel in discover_for_term(
                    term,
                    continuations=settings.youtube_search_continuations,
                    limiter=limiter,
                ):
                    emitter.emit(channel)
        return 0
    finally:
        await aclose_pooled_innertube()
        shutdown_innertube_executor()


def main() -> None:
    raise SystemExit(asyncio.run(main_async()))


if __name__ == '__main__':
    main()
