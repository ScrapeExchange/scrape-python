#!/usr/bin/env python3

'''
YouTube Video Scrape Tool. Reads bare YouTube video-id request files from a
specified directory, scrapes metadata, and saves enriched records back to disk.
Uploading those records is handled by
``tools/yt_video_upload.py``.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import errno
import os
import resource
import sys
import asyncio
import logging

from pathlib import Path
import random

from prometheus_client import Counter, Gauge

from pydantic import AliasChoices, Field, field_validator
from yt_dlp.YoutubeDL import YoutubeDL

from scrape_exchange.file_management import (
    AssetFileManagement,
    VIDEO_MIN_FILE_PREFIX as VIDEO_MIN_PREFIX,
    VIDEO_YTDLP_FILE_PREFIX as VIDEO_YTDLP_PREFIX,
)
from scrape_exchange.content_claim import (
    ContentClaim,
    FileContentClaim,
    NullContentClaim,
    RedisContentClaim,
)
from scrape_exchange.creator_map import (
    CreatorMap,
    RedisCreatorMap,
)
from scrape_exchange.proxy_loader import proxy_file_label
from scrape_exchange.redis_client import redis_from_url
from scrape_exchange.util import extract_proxy_ip, proxy_network_for
from scrape_exchange.worker_id import get_worker_id
from scrape_exchange.watchdog import Watchdog
from scrape_exchange.settings import normalize_log_level
from scrape_exchange.scraper_runner import (
    ScraperRunContext,
    ScraperRunner,
)
from scrape_exchange.youtube.youtube_rate_limiter import YouTubeRateLimiter
from scrape_exchange.youtube.youtube_channel import YouTubeChannel
from scrape_exchange.youtube.youtube_video import YouTubeVideo
from scrape_exchange.youtube.youtube_video import (
    DENO_PATH, PO_TOKEN_URL, YTDLP_CACHE_DIR,
)
from scrape_exchange.youtube.settings import YouTubeScraperSettings
from scrape_exchange.youtube.uploaded_video_ids import UploadedVideoIds
from scrape_exchange.video_scrape_queue import (
    RedisVideoScrapeQueue,
    VideoQueueChannelContext,
    VideoScrapeQueueEntry,
    VideoScrapeQueueSettings,
    VideoState,
)

import redis.asyncio as aioredis

# Prometheus metrics — shared declarations live in scraper_metrics to
# avoid duplicate-registration errors when multiple tool modules are
# imported in the same process (e.g. test runners).
from scrape_exchange.scraper_metrics import (
    METRIC_UPLOADS_SKIPPED as METRIC_VIDEOS_ALREADY_UPLOADED,
    METRIC_SCRAPES_COMPLETED,
    METRIC_SCRAPE_FAILURES,
    METRIC_UPLOADED_LOOKUPS,
)


# Ordered table of yt-dlp error classifications. Each entry is
# ``(reason, [substring, ...])``; the first reason whose substring
# list contains a substring of the *lowercased* error string wins.
# Anything that matches no entry is classified as ``other``.
#
# Every substring MUST already be lowercase — there's an assertion
# below this table to catch a regression. The original branch chain
# in ``_scrape`` had six dead substrings whose case made them
# impossible to match against ``str(exc).lower()``.
_ERROR_PATTERNS: list[tuple[str, list[str]]] = [
    ('rate_limit', [
        'rate-limited by youtube',
        'vpn/proxy detected',
        'youtube blocked',
        'captcha',
        'try again later',
        'the page needs to be reloaded',
        ' 429',
        'http 429',
        '429 too many',
        'expected string or bytes-like object',
    ]),
    ('missing_data', [
        'missing microformat data',
    ]),
    ('unavailable', [
        'this video is private',
        'this video has been removed',
        'video is age restricted',
        'sign in to confirm your age',
        "available to this channel's members on level",
        'members-only content',
        'this live event will begin in',
        'this live event has ended',
        'live stream recording is not available',
        'music premium',
        'video unavailable',
        'video is not available',
        'video available in your country',
        'not available in your country',
        'copyright',
        'inappropriate',
        'uploader',
        'offline.',
    ]),
    ('premiere', [
        'premieres',
        'premiere',
    ]),
    ('transient', [
        'offline',
        'timed out',
        'sslerror',
        'ssl:',
        'unable to connect to proxy',
    ]),
]

# Guard against future contributors adding an uppercase pattern that
# would silently never match.
for _reason, _patterns in _ERROR_PATTERNS:
    for _pat in _patterns:
        assert _pat == _pat.lower(), (
            f'_ERROR_PATTERNS contains non-lowercase pattern '
            f'{_pat!r} under reason {_reason!r}; the classifier '
            f'compares against str(exc).lower() so an uppercase '
            f'pattern is dead code'
        )


def _classify_yt_dlp_error(error_str: str) -> str:
    '''
    Classify a yt-dlp error message into one of the reason buckets
    used by ``scrape_failures_total``.

    Pure function: no I/O, no metric updates, no logging.
    Returns the matching reason name from :data:`_ERROR_PATTERNS`,
    or ``'other'`` when nothing matches.
    '''

    lowered: str = error_str.lower()
    for reason, patterns in _ERROR_PATTERNS:
        for pattern in patterns:
            if pattern in lowered:
                return reason
    return 'other'


def _is_bind_failure(exc: BaseException) -> bool:
    '''
    True when the exception (or anything in its cause/context chain)
    is an OSError(EADDRNOTAVAIL). Signals the kernel refused a
    local_address bind for a ``local://`` egress entry.
    '''

    seen: set[int] = set()
    cur: BaseException | None = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if (
            isinstance(cur, OSError)
            and cur.errno == errno.EADDRNOTAVAIL
        ):
            return True
        cur = cur.__cause__ or cur.__context__
    return False


def _classify_scrape_error(exc: BaseException) -> str:
    '''
    Top-level classifier for video scrape failures. Returns
    ``'bind_failed'`` for an OSError(EADDRNOTAVAIL) anywhere in the
    cause chain (a local:// IP that isn't bound on this host),
    otherwise delegates to :func:`_classify_yt_dlp_error` on the
    message string.
    '''

    if _is_bind_failure(exc):
        return 'bind_failed'
    return _classify_yt_dlp_error(str(exc))


_TRANSIENT_REASONS: frozenset[str] = frozenset({
    'rate_limit', 'transient', 'bind_failed',
})
_UNAVAILABLE_REASONS: frozenset[str] = frozenset({
    'unavailable', 'premiere',
})


async def _scrape_to_disk(
    video_id: str,
    *,
    settings: 'VideoSettings',
    proxy: str | None,
    download_client: YoutubeDL | None,
    creator_map_backend: CreatorMap | None = None,
    channel_context: VideoQueueChannelContext | None = None,
) -> None:
    '''Single-attempt scrape that raises on any failure.

    Streamlined queue-driven variant of :func:`_scrape_and_save`:
    builds the :class:`YouTubeVideo` via the InnerTube/yt-dlp
    pipeline and writes the output file to
    ``settings.video_data_directory``. Does not manage filesystem
    source files (queue-driven items have no on-disk source).
    Errors propagate so :func:`_scrape_one_queued` can classify
    them.

    ``proxy=None`` selects direct egress: ``YouTubeVideo.scrape``
    receives an empty ``proxies`` list and falls through to the
    rate limiter's ``select_proxy``, which returns ``None`` on
    hosts with no proxies configured (e.g. scraper003).
    '''
    video: YouTubeVideo = await YouTubeVideo.scrape(
        video_id, channel_handle=None,
        channel_thumbnail=None,
        ytdlp_cache_dir=settings.ytdlp_cache_dir,
        download_client=download_client,
        save_dir=None,
        filename_prefix=VIDEO_YTDLP_PREFIX,
        debug=settings.log_level == 'DEBUG',
        proxies=[proxy] if proxy else [],
        with_formats=settings.video_use_yt_dlp,
    )
    if channel_context is not None:
        _apply_queue_channel_context(video, channel_context)
    if creator_map_backend is not None:
        await _resolve_video_channel_handle(
            video,
            creator_map_backend=creator_map_backend,
            proxy=proxy,
        )
    output_prefix: str = (
        VIDEO_YTDLP_PREFIX
        if settings.video_use_yt_dlp else VIDEO_MIN_PREFIX
    )
    await video.to_file(
        settings.video_data_directory,
        output_prefix,
    )


def _apply_queue_channel_context(
    video: YouTubeVideo,
    channel_context: VideoQueueChannelContext,
) -> None:
    '''
    Fill missing channel fields from queue metadata. Every field is
    optional; absence must never block the scrape.
    '''
    if channel_context.channel_id and not video.channel_id:
        video.channel_id = channel_context.channel_id
    if channel_context.channel_handle and not video.channel_handle:
        video.channel_handle = channel_context.channel_handle
    if channel_context.channel_url and not video.channel_url:
        video.channel_url = channel_context.channel_url
    if (
        channel_context.channel_is_verified is not None
        and video.channel_is_verified is None
    ):
        video.channel_is_verified = (
            channel_context.channel_is_verified
        )


async def _resolve_video_channel_handle(
    video: YouTubeVideo,
    *,
    creator_map_backend: CreatorMap,
    proxy: str | None,
) -> None:
    '''
    Stamp a canonical channel handle onto a freshly scraped video
    before it is written to disk.

    InnerTube player data can omit ``ownerProfileUrl`` and its
    ``author`` field is a display title, not necessarily the
    canonical ``@handle``.  Prefer the shared channel_id -> handle
    creator map, then resolve via the channel browse endpoint and
    cache the result.
    '''
    if not video.channel_id:
        return

    cached: str | None = await creator_map_backend.get(
        video.channel_id,
    )
    if cached:
        video.channel_handle = cached
        return

    resolved: str | None = await YouTubeChannel.resolve_channel_id(
        video.channel_id,
        proxy=proxy,
    )
    if not resolved:
        return

    video.channel_handle = resolved
    await creator_map_backend.put(video.channel_id, resolved)


async def _scrape_one_queued(
    video_id: str,
    *,
    queue: RedisVideoScrapeQueue,
    settings: 'VideoSettings',
    proxies: list[str],
    uploaded: UploadedVideoIds,
    creator_map_backend: CreatorMap,
    channel_context: VideoQueueChannelContext | None = None,
) -> None:
    '''Scrape one queued video with the hybrid retry contract.

    - Success: ``queue.complete(video_id)``.
    - Transient reasons (rate_limit / transient / bind_failed)
      retry up to ``video_transient_max_attempts`` with
      ``bump_attempts`` between each; exhaustion marks FAILED.
    - Unavailable reasons (unavailable / premiere) mark
      UNAVAILABLE immediately.
    - Anything else marks FAILED immediately.

    A fresh proxy is selected per attempt when ``proxies`` is
    non-empty so that retries don't repeatedly hit the same
    egress IP (precisely what YouTube's rate limiter throttles
    on a 429). An empty ``proxies`` list means direct egress
    (e.g. scraper003) — each attempt passes ``proxy=None`` and
    the inner scrape falls through to the rate limiter's
    ``select_proxy``.
    '''
    try:
        is_uploaded: bool = await uploaded.contains(video_id)
    except Exception:
        METRIC_UPLOADED_LOOKUPS.labels(outcome='error').inc()
        raise
    if is_uploaded:
        # A forced re-scrape (via yt_video_queue.py add/import
        # --force) sets a one-shot ``force`` meta flag.
        # consume_force clears it on read so the force is honoured
        # exactly once and never leaks into a later scrape.
        forced: bool = await queue.consume_force(video_id)
        if not forced:
            METRIC_UPLOADED_LOOKUPS.labels(outcome='hit').inc()
            await queue.complete(video_id)
            VIDEO_QUEUE_OUTCOMES.labels(outcome='scraped').inc()
            return
        METRIC_UPLOADED_LOOKUPS.labels(outcome='forced').inc()
    else:
        METRIC_UPLOADED_LOOKUPS.labels(outcome='miss').inc()
    attempts_left: int = (
        settings.video_transient_max_attempts
    )
    last_reason: str = 'other'
    api: str = (
        'ytdlp' if settings.video_use_yt_dlp else 'innertube'
    )
    while attempts_left > 0:
        proxy: str | None = (
            random.choice(proxies) if proxies else None
        )
        proxy_ip: str = (
            extract_proxy_ip(proxy) if proxy else 'none'
        )
        proxy_file: str = proxy_file_label(proxy or '')
        try:
            await _scrape_to_disk(
                video_id,
                settings=settings,
                proxy=proxy,
                download_client=None,
                creator_map_backend=creator_map_backend,
                channel_context=channel_context,
            )
        except Exception as exc:
            reason: str = _classify_scrape_error(exc)
            last_reason = reason
            METRIC_SCRAPE_FAILURES.labels(
                platform='youtube',
                scraper='video_scraper',
                entity='video',
                api=api,
                reason=reason,
                worker_id=get_worker_id(),
                proxy_ip=proxy_ip,
                proxy_file=proxy_file,
            ).inc()
            if reason in _TRANSIENT_REASONS:
                attempts_left -= 1
                await queue.bump_attempts(
                    video_id, last_error=reason,
                )
                VIDEO_QUEUE_OUTCOMES.labels(
                    outcome='retried',
                ).inc()
                if attempts_left > 0:
                    await asyncio.sleep(
                        settings
                        .video_transient_backoff_seconds,
                    )
                continue
            if reason in _UNAVAILABLE_REASONS:
                await queue.mark(
                    video_id,
                    state=VideoState.UNAVAILABLE,
                    last_error=reason,
                )
                VIDEO_QUEUE_OUTCOMES.labels(
                    outcome='unavailable',
                ).inc()
                return
            await queue.mark(
                video_id,
                state=VideoState.FAILED,
                last_error=reason,
            )
            VIDEO_QUEUE_OUTCOMES.labels(
                outcome='failed',
            ).inc()
            return
        else:
            await queue.complete(video_id)
            METRIC_SCRAPES_COMPLETED.labels(
                platform='youtube',
                scraper='video_scraper',
                entity='video',
                api=api,
                worker_id=get_worker_id(),
                proxy_ip=proxy_ip,
                proxy_file=proxy_file,
            ).inc()
            VIDEO_QUEUE_OUTCOMES.labels(
                outcome='scraped',
            ).inc()
            return
    await queue.mark(
        video_id, state=VideoState.FAILED,
        last_error=(
            f'transient retries exhausted: '
            f'{last_reason}'
        ),
    )
    VIDEO_QUEUE_OUTCOMES.labels(outcome='failed').inc()


async def _queue_driven_loop(
    queue: RedisVideoScrapeQueue,
    settings: 'VideoSettings',
    proxies: list[str],
    concurrency: int,
    uploaded: UploadedVideoIds,
    creator_map_backend: CreatorMap,
) -> None:
    '''Streaming producer/consumer loop for the queue-driven
    video scraper.

    One producer task pops batches from Redis into an in-process
    :class:`asyncio.Queue`; ``concurrency`` consumer tasks pull
    from that queue and run :func:`_scrape_one_queued`
    independently. The bounded in-process queue provides natural
    backpressure: when consumers are saturated the producer
    blocks on ``put`` and stops pulling from Redis.

    The streaming shape was chosen over a batched
    ``asyncio.gather`` because gather waits for the slowest item
    in each batch before popping the next — long-tail latency
    leaves consumer slots idle at the batch boundary. Streaming
    lets fast items recycle their consumer immediately while
    slow items finish in parallel, so steady-state in-flight
    count tracks ``concurrency`` rather than the batch size.

    Cancellation propagates from the surrounding
    ``ScraperRunner``; on cancel every task is cancelled
    cooperatively and any consumer mid-scrape just abandons the
    item (same orphan window as the previous batched gather).
    '''
    if concurrency < 1:
        raise ValueError(
            f'concurrency must be >= 1, got {concurrency!r}',
        )
    # Buffer one batch ahead of the consumers so they never
    # idle on a Redis round-trip, but cap it so a long
    # consumer stall does not leak items out of Redis state
    # for longer than necessary.
    buffer_size: int = max(
        settings.video_queue_batch, concurrency,
    )
    inflight: asyncio.Queue[VideoScrapeQueueEntry] = asyncio.Queue(
        maxsize=buffer_size,
    )

    async def producer() -> None:
        while True:
            entries = await queue.pop_entries(
                settings.video_queue_batch,
            )
            if not entries:
                # Empty queue is "alive but idle", not a hang: keep the
                # watchdog work signal fresh while we poll.
                Watchdog.get().touch_work()
                await asyncio.sleep(
                    settings.video_queue_idle_poll_seconds,
                )
                continue
            for entry in entries:
                # ``put`` blocks when the buffer is full;
                # that's the backpressure path — we stop
                # popping from Redis until consumers drain.
                await inflight.put(entry)

    async def consumer() -> None:
        while True:
            entry = await inflight.get()
            Watchdog.get().touch_work()
            try:
                await _scrape_one_queued(
                    entry.video_id,
                    queue=queue,
                    settings=settings,
                    proxies=proxies,
                    uploaded=uploaded,
                    creator_map_backend=creator_map_backend,
                    channel_context=entry.channel,
                )
            except Exception:
                # Defensive: per-video classification +
                # queue-state transitions live inside
                # :func:`_scrape_one_queued`; this catch is
                # a safety net against bugs there so one
                # bad item cannot tear down the consumer.
                logging.exception(
                    'Unexpected error scraping video',
                    extra={'video_id': entry.video_id},
                )
            finally:
                inflight.task_done()

    tasks: list[asyncio.Task[None]] = [
        asyncio.create_task(
            producer(), name='video-queue-producer',
        ),
        *[
            asyncio.create_task(
                consumer(), name=f'video-queue-consumer-{i}',
            )
            for i in range(concurrency)
        ],
    ]
    try:
        await asyncio.gather(*tasks)
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def _publish_video_queue_sizes(
    queue: RedisVideoScrapeQueue,
    *,
    interval: float = 30.0,
) -> None:
    '''Periodically refresh :data:`VIDEO_STATE_SIZE` from Redis.

    Mirrors yt_channel_scrape._publish_queue_sizes; should be
    started on the elected ``WORKER_ID == '1'`` worker so the same
    gauges aren't published N times per host. Cancellation
    propagates from :func:`_run_worker`'s try/finally.
    '''
    while True:
        try:
            counts: dict[VideoState, int] = (
                await queue.count_by_state()
            )
            for state, n in counts.items():
                VIDEO_STATE_SIZE.labels(
                    state=state.value,
                ).set(n)
        except Exception:
            logging.warning(
                'video queue metrics publish failed',
                exc_info=True,
            )
        await asyncio.sleep(interval)


def _proxy_network(proxy: str | None) -> str:
    '''
    Derive the proxy_network label (CIDR string, 'other',
    or 'none') from a proxy URL. Wrapping extract_proxy_ip
    catches the rare malformed-URL case so metric emission
    never raises.
    '''
    if not proxy:
        return 'none'
    try:
        return proxy_network_for(extract_proxy_ip(proxy))
    except ValueError:
        return 'other'


# Alias so existing call sites for "has_formats" skips use the same
# underlying counter with reason="has_formats".
METRIC_VIDEOS_SKIPPED_HAS_FORMATS: Counter = METRIC_VIDEOS_ALREADY_UPLOADED


METRIC_RATE_LIMIT_HITS = Counter(
    'rate_limit_hits_total',
    'Number of times a proxy was rate-limited by YouTube',
    ['platform', 'scraper', 'entity', 'api',
     'proxy_ip', 'worker_id'],
)

VIDEO_STATE_SIZE: Gauge = Gauge(
    'video_state_size',
    'Number of videos currently in each workflow state.',
    ['state'],
    multiprocess_mode='mostrecent',
)
VIDEO_QUEUE_OUTCOMES: Counter = Counter(
    'video_queue_outcomes_total',
    'Outcomes per scrape attempt.',
    ['outcome'],
)


class VideoSettings(YouTubeScraperSettings):
    '''
    Tool configuration loaded in priority order:
    CLI flags > environment variables > .env file > built-in defaults.
    '''

    video_use_yt_dlp: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            'VIDEO_USE_YT_DLP',
            'video_use_yt_dlp',
        ),
        description=(
            'When True, run yt-dlp after the InnerTube pass to '
            'fetch the metadata yt-dlp adds on top: formats, '
            'availability, media_type, aspect_ratio, heatmaps, '
            'embedable, license, default_audio_language. When '
            'False (the default) only the InnerTube pass runs '
            'and those properties are left unset; this trades '
            'completeness for throughput.'
        ),
    )
    deno_path: str = Field(
        default=DENO_PATH,
        validation_alias=AliasChoices('DENO_PATH', 'deno_path'),
        description='Path to the Deno executable used for scraping',
    )
    po_token_url: str = Field(
        default=PO_TOKEN_URL,
        validation_alias=AliasChoices('PO_TOKEN_URL', 'po_token_url'),
        description='URL for the PO token used for authentication',
    )
    ytdlp_cache_dir: str = Field(
        default=YTDLP_CACHE_DIR,
        validation_alias=AliasChoices(
            'YTDLP_CACHE_DIR', 'ytdlp_cache_dir'
        ),
        description=(
            'Directory yt-dlp uses for its on-disk cache (player JS, '
            'signature decryption artefacts, etc.). Created at start '
            'if it does not exist.'
        ),
    )
    max_files: int | None = Field(
        default=None,
        description='Maximum number of files to process in one run'
    )
    metrics_port: int = Field(
        default=9400,
        validation_alias=AliasChoices(
            'VIDEO_METRICS_PORT', 'video_metrics_port'),
        description='Port for the Prometheus metrics HTTP server',
    )
    video_concurrency: int = Field(
        default=3,
        validation_alias=AliasChoices(
            'VIDEO_CONCURRENCY', 'video_concurrency'
        ),
        description=(
            'Number of videos to scrape concurrently inside one '
            'video scraper process. Video-scraper-specific so the '
            'RSS and channel scrapers can keep their own '
            'concurrency settings independent.'
        ),
    )
    video_log_level: str = Field(
        default='INFO',
        validation_alias=AliasChoices(
            'VIDEO_LOG_LEVEL', 'video_log_level',
            'LOG_LEVEL', 'log_level',
        ),
        description=(
            'Logging level for the video scraper '
            '(DEBUG, INFO, WARNING, ERROR, CRITICAL). Honours '
            'VIDEO_LOG_LEVEL first so this scraper can be dialled '
            'up independently of the RSS and channel scrapers; '
            'falls back to LOG_LEVEL when the scraper-specific '
            'var is unset.'
        ),
    )
    video_log_file: str = Field(
        default='/dev/stdout',
        validation_alias=AliasChoices(
            'VIDEO_LOG_FILE', 'video_log_file',
            'LOG_FILE', 'log_file',
        ),
        description=(
            'Log file path for the video scraper. Honours '
            'VIDEO_LOG_FILE first so each scraper can write to '
            'its own file; falls back to LOG_FILE when the '
            'scraper-specific var is unset.'
        ),
    )

    @field_validator('video_log_level', mode='before')
    @classmethod
    def _normalize_video_log_level(cls, v: str) -> str:
        return normalize_log_level(v)
    video_queue_batch: int = Field(
        default=50,
        validation_alias=AliasChoices(
            'VIDEO_QUEUE_BATCH', 'video_queue_batch',
        ),
        description=(
            'Number of video ids the worker pops from the Redis '
            'video scrape queue per batch.'
        ),
    )
    video_queue_idle_poll_seconds: float = Field(
        default=2.0,
        validation_alias=AliasChoices(
            'VIDEO_QUEUE_IDLE_POLL_SECONDS',
            'video_queue_idle_poll_seconds',
        ),
        description=(
            'Seconds the worker sleeps between empty-queue polls '
            'before retrying pop().'
        ),
    )
    video_transient_max_attempts: int = Field(
        default=3,
        validation_alias=AliasChoices(
            'VIDEO_TRANSIENT_MAX_ATTEMPTS',
            'video_transient_max_attempts',
        ),
        description=(
            'Maximum number of attempts for a video that hits a '
            'transient failure before it is marked FAILED.'
        ),
    )
    video_transient_backoff_seconds: int = Field(
        default=30,
        validation_alias=AliasChoices(
            'VIDEO_TRANSIENT_BACKOFF_SECONDS',
            'video_transient_backoff_seconds',
        ),
        description=(
            'Seconds the worker waits before retrying a video '
            'that hit a transient failure.'
        ),
    )
    video_num_processes: int = Field(
        default=1,
        validation_alias=AliasChoices(
            'VIDEO_NUM_PROCESSES', 'video_num_processes'
        ),
        description=(
            'Number of child video scraper processes to spawn. '
            'When > 1 the invocation becomes a supervisor that '
            'splits the proxy pool into N disjoint chunks and '
            'spawns one child per chunk. Each child runs with '
            'VIDEO_NUM_PROCESSES=1, gets its own METRICS_PORT '
            '(base + worker_instance, with the base reserved for '
            'the supervisor and worker_instance starting at 1) '
            'and log file, if specified. Use this to bypass the '
            'GIL and the default ThreadPoolExecutor cap, both of '
            'which limit how many yt-dlp extract_info calls can '
            'run in parallel inside a single Python process.'
        ),
    )


def _build_video_rate_limiter(
    s: 'VideoSettings',
) -> YouTubeRateLimiter:
    '''
    Construct (or fetch) the per-process YouTubeRateLimiter
    singleton.
    '''
    rl: YouTubeRateLimiter = YouTubeRateLimiter.get(
        state_dir=s.rate_limiter_state_dir,
        redis_dsn=s.redis_dsn,
    )
    return rl


def main() -> None:
    '''
    Top-level entry point. Reads settings and
    dispatches to either the shared supervisor
    (when ``video_num_processes > 1``) or the
    in-process scraper worker.
    '''

    settings: VideoSettings = VideoSettings()
    os.makedirs(
        settings.ytdlp_cache_dir, exist_ok=True,
    )

    runner: ScraperRunner = ScraperRunner(
        settings=settings,
        scraper_label='video',
        platform='youtube',
        num_processes=settings.video_num_processes,
        concurrency=max(
            settings.video_concurrency,
            len(settings.proxies),
            1,
        ),
        metrics_port=settings.metrics_port,
        log_file=settings.video_log_file,
        log_level=settings.video_log_level,
        rate_limiter_factory=_build_video_rate_limiter,
        client_required=False,
    )
    sys.exit(runner.run_sync(_run_worker))


async def _run_worker(
    ctx: ScraperRunContext,
) -> None:
    '''
    Run a single in-process scraper worker (the leaf of the
    supervisor tree). Spawns ``settings.video_concurrency`` async
    workers that share the proxy pool round-robin.
    '''

    settings: VideoSettings = ctx.settings

    # Raise the open-file soft limit to the hard limit so
    # high-concurrency scraping does not hit EMFILE on
    # output writes (video-min-*.json.br.tmp etc.) and
    # silently drop scraped data. Default container soft
    # limit is 1024; the hard limit is typically 524288.
    # Matches yt_channel_scrape.py.
    _: int
    _hard: int
    _, _hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    _target: int = (
        _hard if _hard != resource.RLIM_INFINITY
        else 1048576
    )
    resource.setrlimit(
        resource.RLIMIT_NOFILE,
        (_target, _hard),
    )

    video_fm: AssetFileManagement = (
        AssetFileManagement(
            settings.video_data_directory,
        )
    )
    logging.info(
        'Starting YouTube video scrape tool',
        extra={
            'settings': (
                settings.model_dump_json(indent=2)
            ),
        },
    )

    claim: ContentClaim
    if settings.redis_dsn:
        claim = RedisContentClaim(
            settings.redis_dsn,
            platform='youtube',
        )
    elif settings.video_num_processes > 1:
        claim = FileContentClaim(
            settings.video_data_directory,
        )
        await claim.cleanup_stale()
    else:
        claim = NullContentClaim()

    # The video scraper now consumes from the Redis-backed video
    # scrape queue (see ADR + Phase 4 plan). Subsequent tasks wire
    # the queue into the main loop; the queue is constructed here
    # so settings and lifetime are owned by _run_worker.
    video_queue: RedisVideoScrapeQueue | None = None
    if settings.redis_dsn:
        video_queue_redis: aioredis.Redis = redis_from_url(
            settings.redis_dsn,
            component='youtube-video-queue',
            decode_responses=True,
        )
        video_queue_settings: VideoScrapeQueueSettings = (
            VideoScrapeQueueSettings(
                video_queue_batch=(
                    settings.video_queue_batch
                ),
                video_queue_idle_poll_seconds=(
                    settings.video_queue_idle_poll_seconds
                ),
                video_transient_max_attempts=(
                    settings.video_transient_max_attempts
                ),
                video_transient_backoff_seconds=(
                    settings.video_transient_backoff_seconds
                ),
            )
        )
        video_queue = RedisVideoScrapeQueue(
            video_queue_redis, video_queue_settings,
        )

    if video_queue is None:
        raise RuntimeError(
            'redis_dsn is required: the video scraper now '
            'consumes from the Redis-backed video scrape queue '
            'and has no filesystem fallback. Set REDIS_DSN in '
            '.env.',
        )
    uploaded_video_ids: UploadedVideoIds = UploadedVideoIds(
        settings.redis_dsn,
    )
    creator_map_backend: CreatorMap = RedisCreatorMap(
        settings.redis_dsn,
        platform='youtube',
    )
    # ``claim`` and ``video_fm`` constructed above are no longer
    # used by the queue-driven loop. They remain in place for
    # _run_worker startup-side effects (the FileContentClaim
    # constructor seeds its directory; AssetFileManagement
    # ensures the data dir exists). A later cleanup can prune
    # those once cross-process dedup at queue level is verified.
    del claim, video_fm

    # Per-host singleton: only WORKER_ID '1' publishes queue-size
    # gauges so the same labels aren't written N times per scrape
    # interval. Supervisor children start at '1' (see
    # worker-id-1-is-primary memory).
    publisher_task: asyncio.Task[None] | None = None
    if get_worker_id() == '1':
        publisher_task = asyncio.create_task(
            _publish_video_queue_sizes(video_queue),
        )
    # Streaming concurrency: one consumer per proxy when the
    # configured per-process concurrency is lower, so the
    # streaming loop saturates the proxy pool. Mirrors the
    # ``concurrency`` value passed to ``ScraperRunner`` and
    # published as ``scraper_concurrency``.
    streaming_concurrency: int = max(
        settings.video_concurrency,
        len(settings.proxies),
        1,
    )
    try:
        await _queue_driven_loop(
            video_queue,
            settings,
            list(settings.proxies),
            streaming_concurrency,
            uploaded_video_ids,
            creator_map_backend,
        )
    finally:
        if publisher_task is not None:
            publisher_task.cancel()
            try:
                await publisher_task
            except (asyncio.CancelledError, Exception):
                pass


if __name__ == '__main__':
    main()
