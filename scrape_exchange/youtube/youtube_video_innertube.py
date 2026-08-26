'''
Uses InnerTube to parse data about a video. It is separate from YouTubeVideo
class to keep the source files to a managable length

:author     : boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import logging
import re
import time

import httpx

from collections.abc import Callable
from datetime import datetime
from typing import Any

from innertube import InnerTube
from innertube.errors import RequestError as InnerTubeRequestError
from prometheus_client import Counter

from scrape_exchange.youtube.youtube_channel_tabs import (
    borrow_pooled_innertube_for_entry,
    borrow_pooled_player_innertube_for_entry,
    refresh_pooled_innertube_for_entry,
    refresh_pooled_web_innertube_for_entry,
    release_pooled_innertube,
    release_pooled_player_innertube,
    run_on_innertube_executor,
)

from dateutil import parser as dateutil_parser

from .youtube_caption import YouTubeCaption
from scrape_exchange.worker_id import get_worker_id
from scrape_exchange.proxy_loader import proxy_file_label
from scrape_exchange.util import extract_proxy_ip, extract_proxy_port
from .youtube_client import METRIC_YT_REQUEST_DURATION, _get_scraper
from .youtube_format import YouTubeFormat
from .youtube_rate_limiter import YouTubeRateLimiter, YouTubeCallType
from .youtube_thumbnail import YouTubeThumbnail
from .youtube_videochapter import YouTubeVideoChapter

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .youtube_video import YouTubeVideo


_INNERTUBE_CALL_LOCKS: dict[int, asyncio.Lock] = {}

_BOT_DETECTION_PENALTY_SECONDS: float = 300.0


class YouTubeBotDetectionError(RuntimeError):
    '''YouTube rejected an automated client instead of returning
    usable player metadata. This is transient and should be retried
    through a different proxy.'''


# Wall-clock safety net for a single innertube player/next call. The
# inner sync HTTP stack (httpx + curl_cffi) is supposed to enforce its
# own timeouts, but on 2026-05-17 both video scrapers (scraper001 +
# scraper002) went silent within the same 30-second window at 09:17 UTC
# with workers stuck inside the to_thread call, neither raising nor
# returning. Without this outer cap the worker can hang indefinitely
# and the supervisor's queue.join() blocks forever. 60s is far above
# p99 so this only fires on a genuine hang, not on a slow-but-
# progressing call.
_INNERTUBE_CALL_HARD_TIMEOUT_SECONDS: float = 60.0

METRIC_INNERTUBE_PLAYER_RESPONSES: Counter = Counter(
    'innertube_player_responses_total',
    'InnerTube player() response quality by playability status and '
    'bounded reason class. Non-OK or sparse 2xx responses can explain '
    'video-min files that later fail upload validation.',
    [
        'platform', 'scraper', 'status', 'reason_class',
        'has_title', 'has_thumbnails', 'has_channel_id',
        'worker_id', 'proxy_file',
    ],
)

_CONTENT_UNAVAILABLE_STATUSES: frozenset[str] = frozenset({
    'age_verification_required',
    'live_stream_offline',
    'unplayable',
})

_CLIENT_BLOCK_PATTERNS: tuple[str, ...] = (
    'automated',
    'bot',
    'client',
    'protect our community',
    'robot',
    'traffic',
    'unusual',
)

_RESTRICTED_PATTERNS: tuple[str, ...] = (
    'age',
    'members-only',
    'members only',
    'premium',
    'private',
    'sign in',
)

_UNAVAILABLE_PATTERNS: tuple[str, ...] = (
    'copyright',
    'deleted',
    'not available',
    'removed',
    'unavailable',
)


async def _call_innertube(
    client: InnerTube,
    fn: Callable[..., dict[str, Any]],
    *args: object,
) -> dict[str, Any]:
    '''
    Run the synchronous innertube client call in a worker thread.

    The innertube package exposes blocking methods. Calling them
    directly from async scraper tasks serialises all concurrent
    video workers on the event loop. Keep calls for the same pooled
    client serialised, but allow different proxy/client pairs to
    run concurrently in the executor.

    The call is bounded by :data:`_INNERTUBE_CALL_HARD_TIMEOUT_SECONDS`
    so that a hung sync HTTP request cannot wedge the worker
    indefinitely; on timeout :class:`asyncio.TimeoutError` is raised
    and caught by the generic exception handler in
    :func:`InnerTubeVideoParser.scrape`.
    '''
    lock: asyncio.Lock = _INNERTUBE_CALL_LOCKS.setdefault(
        id(client), asyncio.Lock(),
    )
    async with lock:
        return await asyncio.wait_for(
            run_on_innertube_executor(fn, *args),
            timeout=_INNERTUBE_CALL_HARD_TIMEOUT_SECONDS,
        )


def _safe_int(value: str) -> int | None:
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _safe_timestamp(value: str) -> datetime | None:
    try:
        return dateutil_parser.parse(value)
    except (ValueError, TypeError):
        return None


def _text(obj: dict | str | None) -> str | None:
    '''
    Extract plain text from a YouTube API text object.
    '''
    if obj is None:
        return None
    if isinstance(obj, str):
        return obj
    if 'simpleText' in obj:
        return obj['simpleText']
    if 'runs' in obj:
        return ''.join(r.get('text', '') for r in obj['runs'])
    return None


def _parse_count(text: str | None) -> int | None:
    '''
    Parse a YouTube count string to int.
    Handles "1.2M", "42,069 likes", "1.5K views", plain integers.
    '''
    if not text:
        return None
    token: str = text.strip().split()[0].replace(',', '').upper()
    try:
        if token.endswith('K'):
            return int(float(token[:-1]) * 1_000)
        if token.endswith('M'):
            return int(float(token[:-1]) * 1_000_000)
        if token.endswith('B'):
            return int(float(token[:-1]) * 1_000_000_000)
        digits: str = re.sub(r'\D.*$', '', token)
        return int(digits) if digits else None
    except (ValueError, TypeError):
        return None


def _channel_handle_from_url(url: str | None) -> str | None:
    '''
    Extract a canonical YouTube handle from a channel URL.

    InnerTube's ``videoDetails.author`` is the display name, not
    necessarily the ``@handle``.  The watch microformat often carries
    ``ownerProfileUrl`` as ``https://www.youtube.com/@Handle``; when it
    does, prefer that value for ``channel_handle``.
    '''
    if not url or '/@' not in url:
        return None
    handle: str = url.split('/@', 1)[1].split('/', 1)[0]
    handle = handle.split('?', 1)[0].split('#', 1)[0].strip()
    return handle or None


def _youtube_url(path_or_url: str | None) -> str | None:
    if not path_or_url:
        return None
    if path_or_url.startswith(('http://', 'https://')):
        return path_or_url
    if path_or_url.startswith('/'):
        return f'https://www.youtube.com{path_or_url}'
    return None


def _normalise_status(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        return 'missing'
    return value.strip().lower()


def _reason_contains(reason: str, patterns: tuple[str, ...]) -> bool:
    return any(pattern in reason for pattern in patterns)


def _classify_player_reason(player_data: dict[str, Any]) -> str:
    '''
    Return a bounded class explaining why ``player()`` is not useful.

    Raw ``playabilityStatus.reason`` strings can contain video-specific
    text and should not become metric labels. Keep this classification
    intentionally coarse; the raw reason is emitted in structured logs.
    '''

    playability: dict = player_data.get('playabilityStatus', {})
    status: str = _normalise_status(playability.get('status'))
    reason: str = str(playability.get('reason') or '').lower()

    if status == 'ok':
        return 'ok'
    if _reason_contains(reason, _CLIENT_BLOCK_PATTERNS):
        return 'client_block'
    if status in _CONTENT_UNAVAILABLE_STATUSES:
        return 'unavailable'
    if _reason_contains(reason, _UNAVAILABLE_PATTERNS):
        return 'unavailable'
    if status == 'login_required':
        return 'restricted'
    if _reason_contains(reason, _RESTRICTED_PATTERNS):
        return 'restricted'
    if status == 'missing':
        return 'missing_playability'
    return 'other'


def _player_response_shape(player_data: dict[str, Any]) -> dict[str, bool]:
    video_details: dict = player_data.get('videoDetails', {})
    thumbnails: list = (
        video_details.get('thumbnail', {}).get('thumbnails', [])
    )
    return {
        'has_title': bool(video_details.get('title')),
        'has_thumbnails': any(
            bool(thumbnail.get('url')) for thumbnail in thumbnails
            if isinstance(thumbnail, dict)
        ),
        'has_channel_id': bool(video_details.get('channelId')),
    }


def _record_player_response(
    video_id: str,
    player_data: dict[str, Any],
    *,
    proxy_ip: str,
    proxy_port: str,
    proxy_file: str,
) -> None:
    playability: dict = player_data.get('playabilityStatus', {})
    status: str = _normalise_status(playability.get('status'))
    reason_class: str = _classify_player_reason(player_data)
    shape: dict[str, bool] = _player_response_shape(player_data)

    METRIC_INNERTUBE_PLAYER_RESPONSES.labels(
        platform='youtube',
        scraper=_get_scraper(),
        status=status,
        reason_class=reason_class,
        has_title=str(shape['has_title']).lower(),
        has_thumbnails=str(shape['has_thumbnails']).lower(),
        has_channel_id=str(shape['has_channel_id']).lower(),
        worker_id=get_worker_id(),
        proxy_file=proxy_file,
    ).inc()

    if (
        status != 'ok'
        or not shape['has_title']
        or not shape['has_thumbnails']
    ):
        logging.warning(
            'InnerTube PLAYER returned sparse videoDetails',
            extra={
                'video_id': video_id,
                'proxy_ip': proxy_ip,
                'proxy_port': proxy_port,
                'playability_status': status,
                'playability_reason': playability.get('reason'),
                'playability_reason_class': reason_class,
                **shape,
            },
        )


def _find(contents: list[dict], key: str) -> dict | None:
    '''
    Return the value for key in the first item of contents that has it.
    '''
    for item in contents:
        if key in item:
            return item[key]
    return None


def _extract_comment_count(next_data: dict) -> int | None:
    '''
    Try to extract the comment count from engagementPanels.
    '''

    for panel in next_data.get('engagementPanels', []):
        renderer = panel.get('engagementPanelSectionListRenderer', {})
        if 'comments' not in renderer.get('panelIdentifier', '').lower():
            continue
        header = renderer.get('header', {})
        for header_key in (
            'engagementPanelTitleHeaderRenderer',
            'commentsEntryPointHeaderRenderer',
        ):
            count_text: str | None = _text(
                header.get(header_key, {}).get('contextualInfo')
            )
            if count_text:
                return _parse_count(count_text)
    return None


class InnerTubeVideoParser:
    def __init__(self, video: YouTubeVideo, innertube: InnerTube | None = None,
                 proxy: str | None = None) -> None:
        self.video: YouTubeVideo | None = video

        self.innertube: InnerTube | None = innertube
        self.player_innertube: InnerTube | None = innertube
        self.next_innertube: InnerTube | None = innertube
        if innertube is None and proxy is None:
            logging.warning(
                'No proxy configured, proceeding without proxy'
            )

    @staticmethod
    async def scrape(video: YouTubeVideo, innertube: InnerTube | None = None,
                     proxy: str | None = None,
                     max_retries: int = 4) -> None:
        self = InnerTubeVideoParser(video, innertube, proxy)
        limiter: YouTubeRateLimiter = YouTubeRateLimiter.get()

        _PLAYER_PENALTY_INITIAL: float = 4.0
        _PLAYER_PENALTY_MAX: float = 300.0
        penalty: float = _PLAYER_PENALTY_INITIAL

        player_data: dict | None = None
        player_client: InnerTube | None = None
        for attempt in range(1, max_retries + 1):
            proxy = await limiter.acquire(
                YouTubeCallType.PLAYER, proxy=proxy
            )
            proxy_ip: str = (
                extract_proxy_ip(proxy) if proxy else 'none'
            )
            proxy_port: str = (
                extract_proxy_port(proxy) if proxy else 'none'
            )
            proxy_file: str = proxy_file_label(proxy or '')
            start: float = time.monotonic()
            borrowed_player: bool = self.player_innertube is None
            player_client = (
                borrow_pooled_player_innertube_for_entry(proxy)
                if borrowed_player
                else self.player_innertube
            )
            try:
                try:
                    player_data = await _call_innertube(
                        player_client,
                        player_client.player,
                        video.video_id,
                    )
                finally:
                    if borrowed_player:
                        await release_pooled_player_innertube(
                            player_client,
                        )
                duration: float = time.monotonic() - start
                METRIC_YT_REQUEST_DURATION.labels(
                    platform='youtube',
                    scraper=_get_scraper(),
                    api='innertube',
                    status_class='2xx',
                    worker_id=get_worker_id(),
                    proxy_file=proxy_file,
                ).observe(duration)
                logging.debug(
                    'InnerTube request completed',
                    extra={
                        'api': 'player',
                        'video_id': video.video_id,
                        'duration': duration,
                        'proxy_ip': proxy_ip,
                        'proxy_port': proxy_port,
                        'proxy_file': proxy_file,
                        'status_class': '2xx',
                    },
                )
                break
            except InnerTubeRequestError as exc:
                duration = time.monotonic() - start
                status_class: str = (
                    '4xx'
                    if exc.error.code == 429
                    else 'error'
                )
                METRIC_YT_REQUEST_DURATION.labels(
                    platform='youtube',
                    scraper=_get_scraper(),
                    api='innertube',
                    status_class=status_class,
                    worker_id=get_worker_id(),
                    proxy_file=proxy_file,
                ).observe(duration)
                logging.debug(
                    'InnerTube request failed',
                    exc=exc,
                    extra={
                        'api': 'player',
                        'video_id': video.video_id,
                        'duration': duration,
                        'proxy_ip': proxy_ip,
                        'proxy_port': proxy_port,
                        'proxy_file': proxy_file,
                        'status_class': status_class,
                    },
                )
                if exc.error.code == 429:
                    await limiter.penalise(
                        YouTubeCallType.PLAYER, proxy, penalty
                    )
                    await limiter.penalise(
                        YouTubeCallType.NEXT, proxy, penalty
                    )
                    logging.warning(
                        'InnerTube PLAYER rate-limited',
                        extra={
                            'video_id': video.video_id,
                            'attempt': attempt,
                            'max_retries': max_retries,
                            'penalty_seconds': penalty,
                            'proxy_ip': proxy_ip,
                            'proxy_port': proxy_port,
                        },
                    )
                    penalty = min(penalty * 2, _PLAYER_PENALTY_MAX)
                    if attempt == max_retries:
                        raise RuntimeError(
                            f'InnerTube PLAYER rate-limited after '
                            f'{max_retries} attempts: {exc}'
                        )
                    await asyncio.sleep(penalty)
                else:
                    raise RuntimeError(
                        f'InnerTube API call failed: {exc}'
                    )
            except Exception as exc:
                if borrowed_player and isinstance(
                    exc, httpx.TransportError
                ):
                    await refresh_pooled_innertube_for_entry(
                        proxy,
                        challenged=player_client,
                    )
                duration = time.monotonic() - start
                METRIC_YT_REQUEST_DURATION.labels(
                    platform='youtube',
                    scraper=_get_scraper(),
                    api='innertube',
                    status_class='error',
                    worker_id=get_worker_id(),
                    proxy_file=proxy_file,
                ).observe(duration)
                logging.debug(
                    'InnerTube request failed',
                    exc=exc,
                    extra={
                        'api': 'player',
                        'video_id': video.video_id,
                        'duration': duration,
                        'proxy_ip': proxy_ip,
                        'proxy_port': proxy_port,
                        'proxy_file': proxy_file,
                        'status_class': 'error',
                    },
                )
                raise RuntimeError(
                    f'InnerTube API call failed: '
                    f'{type(exc).__name__}: {exc}'
                ) from exc

        _record_player_response(
            video.video_id,
            player_data,
            proxy_ip=proxy_ip,
            proxy_port=proxy_port,
            proxy_file=proxy_file,
        )
        if _classify_player_reason(player_data) == 'client_block':
            await limiter.penalise(
                YouTubeCallType.PLAYER,
                proxy,
                _BOT_DETECTION_PENALTY_SECONDS,
            )
            await limiter.penalise(
                YouTubeCallType.NEXT,
                proxy,
                _BOT_DETECTION_PENALTY_SECONDS,
            )
            if player_client is not None:
                await refresh_pooled_innertube_for_entry(
                    proxy,
                    challenged=player_client,
                )
            raise YouTubeBotDetectionError(
                f'YouTube bot detection triggered for '
                f'video_id={video.video_id}'
            )
        InnerTubeVideoParser._apply_player_data(video, player_data)

        _next_penalty: float = _PLAYER_PENALTY_INITIAL
        for attempt in range(1, max_retries + 1):
            await limiter.acquire(YouTubeCallType.NEXT, proxy=proxy)
            next_start: float = time.monotonic()
            borrowed_next: bool = self.next_innertube is None
            next_client: InnerTube = (
                borrow_pooled_innertube_for_entry(proxy)
                if borrowed_next
                else self.next_innertube
            )
            try:
                try:
                    next_data: dict[str, Any] = await _call_innertube(
                        next_client,
                        next_client.next,
                        video.video_id,
                    )
                finally:
                    if borrowed_next:
                        await release_pooled_innertube(next_client)
                duration = time.monotonic() - next_start
                METRIC_YT_REQUEST_DURATION.labels(
                    platform='youtube',
                    scraper=_get_scraper(),
                    api='innertube',
                    status_class='2xx',
                    worker_id=get_worker_id(),
                    proxy_file=proxy_file,
                ).observe(duration)
                logging.debug(
                    'InnerTube request completed',
                    extra={
                        'api': 'next',
                        'video_id': video.video_id,
                        'duration': duration,
                        'proxy_ip': proxy_ip,
                        'proxy_port': proxy_port,
                        'proxy_file': proxy_file,
                        'status_class': '2xx',
                    },
                )
                self._parse_next_data(next_data)
                break
            except InnerTubeRequestError as exc:
                duration = time.monotonic() - next_start
                status_class = (
                    '4xx'
                    if exc.error.code == 429
                    else 'error'
                )
                METRIC_YT_REQUEST_DURATION.labels(
                    platform='youtube',
                    scraper=_get_scraper(),
                    api='innertube',
                    status_class=status_class,
                    worker_id=get_worker_id(),
                    proxy_file=proxy_file,
                ).observe(duration)
                logging.debug(
                    'InnerTube request failed',
                    exc=exc,
                    extra={
                        'api': 'next',
                        'video_id': video.video_id,
                        'duration': duration,
                        'proxy_ip': proxy_ip,
                        'proxy_port': proxy_port,
                        'proxy_file': proxy_file,
                        'status_class': status_class,
                    },
                )
                if exc.error.code == 429:
                    await limiter.penalise(
                        YouTubeCallType.NEXT, proxy, _next_penalty
                    )
                    await limiter.penalise(
                        YouTubeCallType.PLAYER, proxy, _next_penalty
                    )
                    logging.warning(
                        'InnerTube NEXT rate-limited',
                        extra={
                            'video_id': video.video_id,
                            'attempt': attempt,
                            'max_retries': max_retries,
                            'penalty_seconds': _next_penalty,
                            'proxy_ip': proxy_ip,
                            'proxy_port': proxy_port,
                        },
                    )
                    _next_penalty = min(
                        _next_penalty * 2, _PLAYER_PENALTY_MAX
                    )
                    if attempt < max_retries:
                        await asyncio.sleep(_next_penalty)
                else:
                    break  # non-429 error on NEXT: skip silently
            except Exception as exc:
                if borrowed_next and isinstance(
                    exc, httpx.TransportError
                ):
                    await refresh_pooled_web_innertube_for_entry(
                        proxy,
                        challenged=next_client,
                    )
                duration = time.monotonic() - next_start
                METRIC_YT_REQUEST_DURATION.labels(
                    platform='youtube',
                    scraper=_get_scraper(),
                    api='innertube',
                    status_class='error',
                    worker_id=get_worker_id(),
                    proxy_file=proxy_file,
                ).observe(duration)
                logging.debug(
                    'InnerTube request failed',
                    exc=exc,
                    extra={
                        'api': 'next',
                        'video_id': video.video_id,
                        'duration': duration,
                        'proxy_ip': proxy_ip,
                        'proxy_port': proxy_port,
                        'proxy_file': proxy_file,
                        'status_class': 'error',
                    },
                )
                break  # NEXT is best-effort; never fail the whole scrape

        return video

    @staticmethod
    def _apply_player_data(
        video: YouTubeVideo, player_data: dict,
    ) -> None:
        '''
        Apply InnerTube ``player`` response fields onto *video*.

        Pure parsing — no I/O, no rate-limiter calls — so the field
        mapping can be unit-tested in isolation.

        Identity fields (``channel_id``, ``channel_handle``,
        ``channel_url``, ``title``, ``description``, ``url``,
        ``embed_url``) use the ``response or existing`` pattern
        rather than ``response.get(key, existing)``: a
        present-but-empty value from YouTube — common for
        unavailable, deleted, or region-blocked videos — must NOT
        clobber data already set by an earlier source (e.g. the
        RSS feed entry).  ``dict.get(k, default)`` only honours the
        default when the key is absent, which is too narrow.
        '''
        video_details: dict = player_data.get('videoDetails', {})
        captions_data: dict = player_data.get('captions', {})
        microformat: dict = player_data.get(
            'microformat', {}
        ).get(
            'playerMicroformatRenderer', {}
        )

        video.title = (
            video_details.get('title') or video.title
        )
        video.description = (
            video_details.get('shortDescription') or video.description
        )
        video.url = microformat.get('canonicalUrl') or video.url
        video.embed_url = (
            microformat.get('embed', {}).get('iframeUrl')
            or video.embed_url
        )

        video.duration = _safe_int(video_details.get('lengthSeconds'))

        if microformat.get('category') and not video.category:
            video.category = microformat['category']

        video.published_timestamp = _safe_timestamp(
            microformat.get('publishDate')
        )
        video.uploaded_timestamp = _safe_timestamp(
            microformat.get('uploadDate')
        )

        video.channel_id = (
            video_details.get('channelId') or video.channel_id
        )
        owner_profile_url: str | None = microformat.get('ownerProfileUrl')
        video.channel_url = owner_profile_url or video.channel_url
        video.channel_handle = (
            _channel_handle_from_url(owner_profile_url)
            or video_details.get('author')
            or video.channel_handle
        )
        video.is_live = bool(
            video_details.get('isLiveContent', video.is_live)
        )
        video.was_live = bool(
            video_details.get('isLiveContent', video.was_live)
        )
        if microformat.get('isUnlisted', False):
            video.privacy_status = 'private'
        else:
            video.privacy_status = 'public'

        video.keywords |= set(video_details.get('keywords', {}))
        video.tags |= set(video_details.get('tags', {}))
        video.available_country_codes |= set(
            microformat.get('availableCountries', [])
        )
        video.is_family_safe = microformat.get(
            'isFamilySafe', video.is_family_safe
        )

        video.view_count = _safe_int(video_details.get('viewCount'))
        video.like_count = _safe_int(microformat.get('likeCount'))
        playability: dict = player_data.get('playabilityStatus', {})
        video.age_restricted = (
            playability.get('status') == 'LOGIN_REQUIRED'
        )
        video.is_tv_film_video = video_details.get(
            'isTvFilmVideo', video.is_tv_film_video
        )
        thumbnails_data = video_details.get(
            'thumbnail', {}
        ).get('thumbnails', [])
        for thumbnail_data in thumbnails_data:
            thumbnail = YouTubeThumbnail(thumbnail_data)
            # YouTube occasionally returns a thumbnail entry with
            # width/height but no url; without this guard the
            # video gets persisted with ``url: null`` in the
            # output JSON. Mirrors the same guard already present
            # in YouTubeVideo._parse_thumbnails (the yt-dlp/RSS
            # path) which is why the bug only surfaced once
            # innertube became the default backend.
            if not thumbnail.url:
                continue
            label: str = (
                thumbnail.id or f'{thumbnail.width}x{thumbnail.height}'
            )
            if label and label not in video.thumbnails:
                video.thumbnails[label] = thumbnail

        # Have not seen values for chapters in InnerTube output.
        # This is untested
        for chapter_data in video_details.get('chapters') or []:
            chapter = YouTubeVideoChapter(chapter_data)
            video.chapters.append(chapter)

        # Have not seen formats in InnerTube output. This is untested
        for format_data in video_details.get('formats') or []:
            video.formats.add(YouTubeFormat(format_data))

        if captions_data:
            parsed_captions: dict = InnerTubeVideoParser.parse_captions(
                captions_data
            )
            for entry in parsed_captions.get('subtitles', []):
                lang: str = entry.get('language_code', 'unknown')
                video.subtitles[lang] = YouTubeCaption(lang, entry)
            for entry in parsed_captions.get('automatic_captions', []):
                lang = entry.get('language_code', 'unknown')
                video.automatic_captions[lang] = YouTubeCaption(
                    lang, entry,
                )

    @staticmethod
    def parse_captions(captions_data: dict) -> dict[str, list[dict]]:
        '''
        Parse caption / subtitle tracks from the captions dict.

        :param captions_data: the captions dict from a player() response.
        :returns: dict with keys 'subtitles' and 'automatic_captions', each a
                list of {language_code, language_name, url, vss_id} dicts.
        '''
        subtitles: list[dict] = []
        auto_captions: list[dict] = []

        renderer = captions_data.get('playerCaptionsTracklistRenderer', {})
        for track in renderer.get('captionTracks', []):
            entry: dict[str, any] = {
                'language_code': track.get('languageCode', 'unknown'),
                'language_name': _text(track.get('name')),
                'url': track.get('baseUrl'),
                'vss_id': track.get('vssId'),
            }
            if track.get('kind') == 'asr':
                auto_captions.append(entry)
            else:
                subtitles.append(entry)

        return {
            'subtitles': subtitles,
            'automatic_captions': auto_captions,
        }

    @staticmethod
    def parse_innertube_chapters(player_data: dict
                                 ) -> list[YouTubeVideoChapter]:
        markers_map: list[dict[str, any]] = (
            player_data
            .get('playerOverlays', {})
            .get('playerOverlayRenderer', {})
            .get('decoratedPlayerBarRenderer', {})
            .get('decoratedPlayerBarRenderer', {})
            .get('playerBar', {})
            .get('multiMarkersPlayerBarRenderer', {})
            .get('markersMap', [])
        )

        chapters: list[YouTubeVideoChapter] = []

        for marker in markers_map:
            for chapter in marker.get('value', {}).get('chapters', []):
                r: dict = chapter.get('chapterRenderer', {})
                title_runs: list = r.get('title', {}).get('runs', [])
                title: str = \
                    title_runs[0].get('text', '') if title_runs else ''
                start_ms: int = r.get('timeRangeStartMillis', 0)
                thumbs: list = r.get('thumbnail', {}).get('thumbnails', [])
                best_thumb_url: str = (
                    max(
                        thumbs, key=lambda t: t.get('width', 0), default={}
                    ).get('url')
                )
                if not (title and start_ms):
                    continue

                chapter = YouTubeVideoChapter(
                    {
                        'title': title,
                        'start_time': start_ms/1000,
                        'thumbnail_url': best_thumb_url
                    }
                )
                chapters.append(chapter)

        return chapters

    @staticmethod
    def _extract_like_count(primary: dict) -> int | None:
        '''
        Try multiple known YouTube API paths to extract the like count from
        videoPrimaryInfoRenderer. YouTube reorganises this path frequently.
        '''
        buttons = primary.get(
            'videoActions', {}
        ).get(
            'menuRenderer', {}
        ).get(
            'topLevelButtons', []
        )
        for btn in buttons:
            # Older path: segmentedLikeDislikeButtonRenderer
            like_toggle: str | None = _text(
                btn.get(
                    'segmentedLikeDislikeButtonRenderer', {}
                ).get(
                    'likeButton', {}
                ).get(
                    'toggleButtonRenderer', {}
                ).get(
                    'defaultText'
                )
            )
            if like_toggle:
                count: int | None = _parse_count(like_toggle)
                if count is not None:
                    return count

            # Newer path: segmentedLikeDislikeButtonViewModel
            label: str = btn.get(
                'segmentedLikeDislikeButtonViewModel', {}
            ).get(
                'likeButtonViewModel', {}
            ).get(
                'likeButtonViewModel', {}
            ).get(
                'toggleButtonViewModel', {}
            ).get(
                'toggleButtonViewModel', {}
            ).get(
                'defaultButtonViewModel', {}
            ).get(
                'buttonViewModel', {}
            ).get(
                'accessibilityText', ''
            )
            if label:
                prefix: str = 'like this video along with '
                if label.startswith(prefix):
                    return _parse_count(label[len(prefix):])

        return None

    def _parse_next_data(self, next_data: dict) -> None:
        '''
        Parse the next() API response for supplementary metadata.

        :returns: dict with like_count, comment_count, chapters, categories.
        '''

        def _find(contents: list[dict], key: str) -> dict | None:
            '''
            Return the value for key in the first item of contents that has it.
            '''

            for item in contents:
                if key in item:
                    return item[key]
            return None

        contents: dict | None = next_data.get(
            'contents', {}
        ).get(
            'twoColumnWatchNextResults', {}
        ).get(
            'results', {}
        ).get(
            'results', {}
        ).get(
            'contents'
        )
        if not contents:
            return
        primary: dict | None = _find(contents, 'videoPrimaryInfoRenderer')
        if primary:
            self.video.like_count = InnerTubeVideoParser._extract_like_count(
                primary
            )

        secondary = _find(contents, 'videoSecondaryInfoRenderer')
        if secondary:
            owner_renderer: dict = (
                secondary
                .get('owner', {})
                .get('videoOwnerRenderer', {})
            )
            self._apply_owner_renderer(owner_renderer)

            rows = (
                secondary
                .get('metadataRowContainer', {})
                .get('metadataRowContainerRenderer', {})
                .get('rows', [])
            )
            for row in rows:
                r = row.get('metadataRowRenderer', {})
                if 'categor' in (_text(r.get('title')) or '').lower():
                    for content in r.get('contents', []):
                        cat: str | None = _text(content)
                        if cat and not self.video.category:
                            self.video.category = cat
                            break

            # Channel thumbnail lives on the owner renderer of the
            # secondary info block — the same place the web client
            # reads the channel avatar from. Take the largest
            # thumbnail with a real url; the unfiltered list can
            # contain ones without a url, see Bug #1 guard above.
            owner_thumbs: list[dict] = (
                owner_renderer
                .get('thumbnail', {})
                .get('thumbnails', [])
            )
            best: YouTubeThumbnail | None = None
            for t in owner_thumbs:
                if not t.get('url'):
                    continue
                cand = YouTubeThumbnail(t)
                if best is None or (
                    (cand.width or 0) > (best.width or 0)
                ):
                    best = cand
            if best is not None:
                self.video.channel_thumbnail_asset = best
                self.video.channel_thumbnail_url = best.url
        self.video.comment_count = \
            _extract_comment_count(next_data) or self.video.comment_count
        self.video.chapters = InnerTubeVideoParser._parse_chapters_from_next(
            next_data
        )

    def _apply_owner_renderer(self, owner_renderer: dict) -> None:
        '''
        Extract channel identity from the owner block of the
        InnerTube ``next`` response.

        Age-restricted or otherwise partial ``player`` responses can
        omit ``videoDetails.channelId`` and microformat owner URLs, while
        ``next`` still includes the owner renderer used by the watch
        page sidebar.  That renderer is the same source we already use
        for the channel thumbnail.
        '''
        if not owner_renderer:
            return

        endpoint: dict = (
            owner_renderer
            .get('navigationEndpoint', {})
        )
        if not endpoint:
            title_runs: list[dict] = (
                owner_renderer
                .get('title', {})
                .get('runs', [])
            )
            if title_runs:
                endpoint = title_runs[0].get(
                    'navigationEndpoint', {}
                )

        browse_endpoint: dict = endpoint.get(
            'browseEndpoint', {},
        )
        channel_id: str | None = browse_endpoint.get('browseId')
        if channel_id:
            self.video.channel_id = (
                self.video.channel_id or channel_id
            )

        url: str | None = (
            browse_endpoint.get('canonicalBaseUrl')
            or endpoint
            .get('commandMetadata', {})
            .get('webCommandMetadata', {})
            .get('url')
        )
        channel_url: str | None = _youtube_url(url)
        if channel_url:
            self.video.channel_url = (
                self.video.channel_url or channel_url
            )
        handle: str | None = _channel_handle_from_url(channel_url)
        if handle:
            self.video.channel_handle = handle

    @staticmethod
    def _parse_chapters_from_next(next_data: dict) -> list[dict]:
        '''
        Extract chapters from engagementPanels in a next() response.

        :returns: list of {title, time_text, start_seconds} dicts.
        '''
        chapters: list[dict] = []
        for panel in next_data.get('engagementPanels', []):
            renderer = panel.get('engagementPanelSectionListRenderer', {})
            panel_id = renderer.get('panelIdentifier', '')
            if 'chapters' not in panel_id and 'macro-markers' not in panel_id:
                continue
            contents = (
                renderer
                .get('content', {})
                .get('sectionListRenderer', {})
                .get('contents', [])
            )

            for section in contents:
                section_data: list[dict] = section.get(
                    'itemSectionRenderer', {}
                ).get('contents', [])
                for item in section_data:
                    r = item.get('macroMarkersListItemRenderer', {})
                    if not r:
                        continue
                    start_secs = (
                        r.get('onTap', {})
                        .get('watchEndpoint', {})
                        .get('startTimeSeconds', 0)
                    )
                    chapters.append({
                        'title': _text(r.get('title')),
                        'time_text': _text(r.get('timeDescription')),
                        'start_seconds': float(start_secs),
                    })
        return chapters
