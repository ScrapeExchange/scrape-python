#!/usr/bin/env python3

'''
A worker tool that periodically reads YouTube channel RSS feeds and checks
whether the videos are already stored in Scrape Exchange.

Processes up to a configurable number of channels concurrently, then sleeps
until the next polling interval.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import errno
import os
import sys
import asyncio
import logging
from time import monotonic
from datetime import UTC
from datetime import datetime
from typing import Awaitable, TypeVar

import brotli
import orjson
import untangle

import httpx
from httpx import Response

from prometheus_client import Counter, Gauge, Histogram

from pathlib import Path

from pydantic import (
    AliasChoices, Field, field_validator, model_validator,
)

from innertube import InnerTube

from scrape_exchange.exchange_client import ExchangeClient
from scrape_exchange.file_management import (
    AssetFileManagement,
    atomic_write_bytes,
)
from scrape_exchange.schema_validator import (
    SchemaValidator,
    fetch_schema_dict,
)
from scrape_exchange.scraper_runner import (
    ScraperRunContext,
    ScraperRunner,
)
from scrape_exchange.settings import normalize_log_level
from scrape_exchange.proxy_loader import (
    httpx_client_for_entry,
    jitter_pool_warmup,
    load_proxy_catalog,
    pooled_httpx_client_for_entry,
    proxy_file_label,
    set_active_catalog,
)
from scrape_exchange.util import extract_proxy_ip, proxy_network_for
from scrape_exchange.youtube.youtube_channel import (
    YouTubeChannel,
    canonical_handle_from_browse,
    fallback_handle,
)
from scrape_exchange.youtube.youtube_channel_tabs import YouTubeChannelTabs
from scrape_exchange.youtube.youtube_rate_limiter import (
    YouTubeRateLimiter, YouTubeCallType
)
from scrape_exchange.youtube.youtube_video import YouTubeVideo

from scrape_exchange.worker_id import get_worker_id
from scrape_exchange.youtube.settings import YouTubeScraperSettings
from scrape_exchange.creator_map import (
    CreatorMap,
    FileCreatorMap,
    RedisCreatorMap,
    CREATOR_HANDLE_MISMATCH_TOTAL,
    CREATOR_MAP_RESOLUTION_TOTAL,
)
from scrape_exchange.name_map import (
    NameMap,
    NullNameMap,
    RedisNameMap,
)
from scrape_exchange.creator_queue import (
    CreatorQueue,
    FileCreatorQueue,
    RedisCreatorQueue,
    TierConfig,
    parse_priority_queues,
)


VIDEO_FILENAME_PREFIX: str = 'video-min-'
CHANNEL_FILENAME_PREFIX: str = 'channel-'
UPLOADED_DIR: str = '/uploaded'

MIN_CHANNEL_INTERVAL_SECONDS: int = 30 * 60         # 30 minutes
RETRY_INTERVAL_SECONDS: int = 60 * 60 * 4           # 4 hours
DEFAULT_PRIORITY_QUEUES: str = '1:10000000,4:1000000,12:100000,24:10000,48:0'
MAX_CONCURRENT_CHANNELS: int = 3

FILE_EXTENSION: str = '.json.br'

BACKUP_SUFFIX: str = 'bak'

YOUTUBE_RSS_URL: str = (
    'https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}'
)

FAILURE_DELAY: int = 60

CHANNEL_SCHEMA_OWNER: str = 'boinko'
CHANNEL_SCHEMA_VERSION: str = '0.0.2'
CHANNEL_SCHEMA_PLATFORM: str = 'youtube'
CHANNEL_SCHEMA_ENTITY: str = 'channel'

T_Phase = TypeVar('T_Phase')

MIN_SLEEP_SECONDS: float = 1.8
MAX_SLEEP_SECONDS: float = 4.5

MSG_NO_RSS_FEED: str = 'RSS feed not found for channel'

# Prometheus metrics
# Shared metric declarations (avoids duplicate-registration when
# multiple tool modules are imported in the same process).
from scrape_exchange.scraper_metrics import (
    METRIC_CHANNEL_PRIORITY_WRITES,
    METRIC_SCRAPES_COMPLETED as METRIC_RSS_DOWNLOADED,
    METRIC_SCRAPE_DURATION,
    METRIC_SCRAPE_FAILURES as METRIC_RSS_FAILURES,
    METRIC_SCRAPE_QUEUE_SIZE as METRIC_QUEUE_SIZE,
)
METRIC_CHANNEL_MAP_SIZE: Gauge = Gauge(
    'channel_map_size',
    'Number of channels in the channel map',
    ['platform', 'scraper', 'worker_id'],
)
METRIC_NO_FEED_LIMIT_HIT: Counter = Counter(
    'rss_no_feed_limit_hit_total',
    'Number of times a channel hit the no-feed failure limit',
    ['platform', 'scraper', 'worker_id', 'limit'],
)
# api_calls_total — exchange API calls for channel and video
# entities, and InnerTube API calls (success/failed), all
# collapsed into one counter with entity, api, and status labels.
METRIC_API_CALLS: Counter = Counter(
    'api_calls_total',
    'Number of API calls made by the RSS scraper, labelled '
    'by entity, api endpoint, and status.',
    ['platform', 'scraper', 'entity', 'api', 'status',
     'worker_id', 'proxy_ip', 'proxy_network', 'proxy_file'],
)
METRIC_API_CHANNEL_CALLS: Counter = METRIC_API_CALLS
METRIC_INNERTUBE_SUCCESS: Counter = METRIC_API_CALLS
METRIC_INNERTUBE_FAILURES: Counter = METRIC_API_CALLS
METRIC_CONCURRENCY: Gauge = Gauge(
    'worker_concurrency',
    'Number of channels being processed concurrently in the '
    'current batch',
    ['platform', 'scraper', 'worker_id'],
)
METRIC_CHANNEL_SECONDS_SINCE_LAST_PROCESSED: Gauge = Gauge(
    'channel_seconds_since_last_processed',
    'Seconds elapsed since the channel was last processed '
    '(only set for channels that have been processed before)',
    ['platform', 'scraper', 'tier', 'worker_id'],
)
METRIC_TIER_ON_TIME: Counter = Counter(
    'channel_tier_on_time_total',
    'Channels processed within the tier interval',
    ['platform', 'scraper', 'tier', 'worker_id'],
)
METRIC_TIER_OVERDUE: Counter = Counter(
    'channel_tier_overdue_total',
    'Channels processed after the tier interval expired',
    ['platform', 'scraper', 'tier', 'worker_id'],
)
METRIC_PHASE_DURATION: Histogram = Histogram(
    'rss_phase_duration_seconds',
    'Per-phase time spent inside the RSS scrape pipeline.\n'
    'Streamer-level phases (one observation per cid):\n'
    '  claim, process, release, idle.\n'
    'process_channel sub-phases (one observation per cid '
    'unless the path is short-circuited):\n'
    '  fetch_rss      — the /feeds/videos.xml HTTP GET\n'
    '  innertube      — sum of InnerTube hot-path calls for '
    'newly-discovered videos\n'
    '  exchange_post  — exchange API POSTs for discovered '
    'videos\n'
    '  creator_map    — creator_map / canonicalisation '
    'updates triggered by this scrape.\n'
    'Buckets extend to 377 s so a saturated p95 actually '
    'tells us the upper bound rather than just the histogram '
    'ceiling.',
    ['platform', 'scraper', 'phase', 'worker_id'],
    buckets=(
        0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 3.0, 5.0,
        8.0, 13.0, 21.0, 34.0, 55.0,
        89.0, 144.0, 233.0, 377.0,
    ),
)
METRIC_TIER_POPULATION: Gauge = Gauge(
    'channel_tier_population',
    'Total cids in rss:<platform>:tiers HASH bucketed by '
    'their assigned tier. Includes queued, claimed, no_feeds '
    'and orphan cids alike — for the queued subset see '
    'scrape_queue_size{tier=N}. Populated by the dedicated '
    'queue-metrics loop on a single worker per host; '
    'aggregate across instances with max by (tier), not sum.',
    ['platform', 'scraper', 'tier'],
)
METRIC_ORPHANS_RECOVERED: Counter = Counter(
    'channel_orphans_recovered_total',
    'Creator ids re-enqueued by '
    'scan_and_recover_orphans because they were in '
    'the tiers hash but absent from every queue, not '
    'claimed, and not flagged no_feeds.',
    ['platform', 'scraper', 'tier'],
)


# Track interval between RSS feed checks per channel
CHANNEL_LAST_CHECKED: dict[str, float] = {}


class RssSettings(YouTubeScraperSettings):
    '''
    Worker configuration loaded in priority order:
    CLI flags > environment variables > .env file > built-in defaults.

    Backward-compatible env var names (e.g. SCRAPE_EXCHANGE_URL,
    MIN_CHANNEL_INTERVAL_SECONDS) are accepted alongside the shorter
    field-name-based names (e.g. EXCHANGE_URL, MIN_INTERVAL).
    '''

    schema_version: str = Field(
        default='0.0.1',
        validation_alias=AliasChoices(
            'SCHEMA_VERSION', 'schema_version'
        ),
        description='Schema version to use for uploads',
    )
    schema_owner: str = Field(
        default='boinko',
        validation_alias=AliasChoices(
            'SCHEMA_USERNAME', 'schema_owner'
        ),
        description='Username of the schema owner used for data API calls',
    )
    queue_file: str = Field(
        default='yt-rss-reader-queue.json',
        validation_alias=AliasChoices(
            'RSS_QUEUE_FILE', 'rss_queue_file'
        ),
        description='Path to JSON file for persisting the channel queue',
    )
    rss_enrich_subscriber_counts: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            'RSS_ENRICH_SUBSCRIBER_COUNTS',
            'rss_enrich_subscriber_counts',
        ),
        description=(
            'Run the startup pass that GETs subscriber counts '
            'from scrape.exchange for every channel not already '
            'in the Redis "known_creator_ids" set. Useful only '
            'for first-time setup or post-recovery; routine '
            'restarts should leave this False because the pass '
            'serializes ~80k API GETs and blocks the scrape loop '
            'for many minutes. With this False, un-enriched '
            'channels default to the lowest tier and get '
            're-tiered after their first successful scrape '
            '(which always re-records the subscriber_count).'
        ),
    )
    rss_seed_queue_from_uploaded: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            'RSS_SEED_QUEUE_FROM_UPLOADED',
            'rss_seed_queue_from_uploaded',
        ),
        description=(
            'Walk the uploaded-channel directory at startup and '
            'enqueue any channels not already in the Redis queue. '
            'Useful only for first-time setup, post-flush '
            'recovery, or fleet rebuilds. The scan reads + '
            'brotli-decodes every channel file before checking '
            'whether it is already queued, which on a populated '
            'archive (~hundreds of thousands of files) is '
            'multi-tens-of-minutes of work per worker on every '
            'restart. Default False; flip to True only for '
            'recovery scenarios.'
        ),
    )
    rss_max_no_feed_failures: int = Field(
        default=10,
        validation_alias=AliasChoices(
            'RSS_MAX_NO_FEED_FAILURES',
            'rss_max_no_feed_failures',
        ),
        description=(
            'Number of consecutive RSS 404 failures '
            'before a channel is skipped. YouTube '
            'silently degrades RSS feeds for flagged '
            'IPs, so a higher threshold avoids '
            'permanently blacklisting channels that '
            'are only transiently unreachable. A single '
            'successful RSS fetch clears the counter via '
            'clear_no_feeds().'
        ),
    )
    rss_max_no_feed_failures_had_feed: int = Field(
        default=50,
        validation_alias=AliasChoices(
            'RSS_MAX_NO_FEED_FAILURES_HAD_FEED',
            'rss_max_no_feed_failures_had_feed',
        ),
        description=(
            'Threshold (consecutive RSS 404 '
            'failures) before a channel is dropped '
            'from the queue, when that channel has '
            'previously served at least one '
            'successful RSS feed. Intended to be '
            'larger than rss_max_no_feed_failures so '
            'that established channels are not lost '
            'to transient YouTube soft-bans.'
        ),
    )
    had_feed_file: str = Field(
        default='yt-rss-reader-had-feed.txt',
        validation_alias=AliasChoices(
            'HAD_FEED_FILE', 'had_feed_file',
        ),
        description=(
            'Path to file used by FileCreatorQueue '
            'to persist the set of channel ids that '
            'have ever successfully served an RSS '
            'feed. Used to decide which threshold '
            '(rss_max_no_feed_failures vs. '
            'multiplier * rss_max_no_feed_failures) '
            'applies to a given channel.'
        ),
    )
    eligibility_fraction: float = Field(
        default=0.5,
        validation_alias=AliasChoices(
            'RSS_ELIGIBILITY_FRACTION',
            'eligibility_fraction',
        ),
        description=(
            'Fraction of the tier interval after '
            'which a channel becomes eligible to be '
            'RSS-fetched again. 0.5 means a channel '
            're-enters the queue once half its tier '
            'interval has elapsed since the last run. '
            'Setting this < 1.0 introduces headroom '
            'so the SLA panel can report on-time '
            'fetches.'
        ),
    )
    rss_proxy_files: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'RSS_PROXY_FILES', 'rss_proxy_files',
        ),
        description=(
            'RSS-only override for PROXY_FILES. When set, the RSS '
            'scraper loads its proxy catalog from this comma-'
            'separated list of files instead of PROXY_FILES, while '
            'video and channel scrapers keep the inherited list. '
            'Use this to exclude networks that YouTube has RSS-'
            'banned (returns 404 on /feeds/videos.xml) but still '
            'serves for HTML and InnerTube. Same line format as '
            'PROXY_FILES.'
        ),
    )

    @model_validator(mode='after')
    def _apply_rss_proxy_files_override(
        self,
    ) -> 'RssSettings':
        '''When ``rss_proxy_files`` is set, replace the proxy
        catalog loaded by the parent ``_load_proxy_catalog``
        validator with one built from the RSS-specific list.
        Runs after the parent validator (MRO-ordered) so the
        override always wins.'''
        if not self.rss_proxy_files:
            return self
        paths: list[Path] = [
            Path(p.strip())
            for p in self.rss_proxy_files.split(',')
            if p.strip()
        ]
        catalog = load_proxy_catalog(paths)
        object.__setattr__(self, 'proxies', catalog.entries)
        set_active_catalog(catalog)
        return self

    @field_validator('eligibility_fraction')
    @classmethod
    def _validate_eligibility_fraction(
        cls, v: float,
    ) -> float:
        if not (0.0 < v <= 1.0):
            raise ValueError(
                'eligibility_fraction must be in '
                f'(0.0, 1.0]; got {v!r}'
            )
        return v

    no_feeds_file: str = Field(
        default='yt-rss-reader-no-feeds.txt',
        validation_alias=AliasChoices(
            'NO_FEEDS_FILE', 'no_feeds_file'
        ),
        description=(
            'Path to text file where channel names with missing RSS feeds are '
            'logged (one channel name per line)'
        ),
    )
    min_interval: int = Field(
        default=MIN_CHANNEL_INTERVAL_SECONDS,
        validation_alias=AliasChoices(
            'MIN_CHANNEL_INTERVAL_SECONDS', 'min_interval'
        ),
        description=(
            f'Minimum seconds between RSS polls per channel '
            f'(default: {MIN_CHANNEL_INTERVAL_SECONDS})'
        ),
    )
    priority_queues: str = Field(
        default='1:10_000_000,4:1_000_000,12:100_000,72:10_000,168:0',
        validation_alias=AliasChoices(
            'PRIORITY_QUEUES', 'priority_queues',
        ),
        description=(
            'Comma-separated interval_hours:min_subscribers '
            'pairs defining priority tiers, ordered by '
            'priority (first = highest).'
        ),
    )
    retry_interval: int = Field(
        default=RETRY_INTERVAL_SECONDS,
        validation_alias=AliasChoices(
            'RETRY_INTERVAL_SECONDS', 'retry_interval'
        ),
        description=(
            f'Minimum seconds before a failed channel '
            f'is polled again. Applied as a floor on '
            f'top of the tier interval: effective '
            f'delay is '
            f'max(tier.interval_hours*3600, '
            f'retry_interval) after any failed '
            f'process_channel attempt. Successful '
            f'attempts ignore this and use the tier '
            f'interval directly. Default: '
            f'{RETRY_INTERVAL_SECONDS}.'
        ),
    )
    metrics_port: int = Field(
        default=9500,
        validation_alias=AliasChoices('RSS_METRICS_PORT', 'rss_metrics_port'),
        description='Port for the Prometheus metrics HTTP server',
    )
    rss_concurrency: int = Field(
        default=3,
        validation_alias=AliasChoices(
            'RSS_CONCURRENCY', 'rss_concurrency'
        ),
        description=(
            'Number of channels to fetch in parallel inside one '
            'RSS scraper process. RSS-scraper-specific so the '
            'video scraper can keep its own CONCURRENCY setting '
            'independent.'
        ),
    )
    rss_num_processes: int = Field(
        default=1,
        validation_alias=AliasChoices(
            'RSS_NUM_PROCESSES', 'rss_num_processes'
        ),
        description=(
            'Number of child RSS scraper processes to spawn. When '
            '> 1 the invocation becomes a supervisor that splits '
            'the proxy pool into N disjoint chunks and spawns one '
            'child per chunk. Each child runs with '
            'RSS_NUM_PROCESSES=1, gets its own METRICS_PORT '
            '(base + worker_instance, with base reserved for the '
            'supervisor and worker_instance starting at 1) and '
            'log file, if specified. Use this to bypass the GIL '
            'when polling many RSS feeds in parallel and to '
            'distribute Innertube fetch load across processes.'
        ),
    )
    rss_log_level: str = Field(
        default='INFO',
        validation_alias=AliasChoices(
            'RSS_LOG_LEVEL', 'rss_log_level',
            'LOG_LEVEL', 'log_level',
        ),
        description=(
            'Logging level for the RSS scraper '
            '(DEBUG, INFO, WARNING, ERROR, CRITICAL). Honours '
            'RSS_LOG_LEVEL first so this scraper can be dialled '
            'up independently of the video and channel scrapers; '
            'falls back to LOG_LEVEL when the scraper-specific '
            'var is unset.'
        ),
    )
    rss_log_file: str = Field(
        default='/dev/stdout',
        validation_alias=AliasChoices(
            'RSS_LOG_FILE', 'rss_log_file',
            'LOG_FILE', 'log_file',
        ),
        description=(
            'Log file path for the RSS scraper. Honours '
            'RSS_LOG_FILE first so each scraper can write to its '
            'own file; falls back to LOG_FILE when the '
            'scraper-specific var is unset.'
        ),
    )

    @field_validator('rss_log_level', mode='before')
    @classmethod
    def _normalize_rss_log_level(cls, v: str) -> str:
        return normalize_log_level(v)


# Re-export under the old private name so existing references in this
# module (and in its tests) keep working. Canonical definition lives
# in scrape_exchange.util.extract_proxy_ip so the channel and video
# scrapers can reuse it without a cross-tool import.
_extract_proxy_ip = extract_proxy_ip


def _record_rss_failure(
    reason: str,
    proxy_ip: str | None,
    proxy_network: str,
    proxy_file: str,
) -> None:
    METRIC_RSS_FAILURES.labels(
        platform='youtube',
        scraper='rss_scraper',
        entity='rss_feed',
        api='rss',
        reason=reason,
        worker_id=get_worker_id(),
        proxy_ip=proxy_ip or 'none',
        proxy_network=proxy_network,
        proxy_file=proxy_file,
    ).inc()


def _handle_http_status_error(
    exc: httpx.HTTPStatusError,
    rss_url: str,
    proxy: str | None,
    proxy_ip: str | None,
    proxy_network: str,
    extra: dict[str, str],
) -> None:
    '''
    Classify an HTTP status error, bump the appropriate failure metric,
    and raise the caller-facing exception. Always raises.
    '''
    logging.warning(
        'HTTP error fetching RSS feed',
        extra=extra | {
            'status_code': exc.response.status_code,
            'response_text': exc.response.text,
        },
    )
    pf: str = proxy_file_label(proxy or '')
    status_code: int = exc.response.status_code
    if status_code == 404:
        _record_rss_failure('not_found', proxy_ip, proxy_network, pf)
        YouTubeRateLimiter.get().report_rss_failure(
            proxy, is_circuit_tripping=True,
        )
        raise ValueError(
            f'RSS feed not found: {rss_url}'
        ) from exc
    if 500 <= status_code < 600:
        # YouTube returns 5xx under per-IP rate-limit pressure
        # (alongside 404s) when an aggressive scraper is hammering
        # one of its endpoints. Treat as a soft-ban signal so the
        # circuit breaker yanks the proxy off the rotation, the
        # same as for 404. Without this the limiter keeps issuing
        # tokens at full rate while every fetch from the IP is
        # rejected.
        _record_rss_failure(
            'server_error', proxy_ip, proxy_network, pf,
        )
        YouTubeRateLimiter.get().report_rss_failure(
            proxy, is_circuit_tripping=True,
        )
        raise RuntimeError(
            f'Server error fetching RSS feed: {rss_url}'
        ) from exc
    raise exc


async def fetch_rss(
    rss_url: str,
    channel_handle: str,
) -> list[YouTubeVideo] | None:
    '''
    Fetches and parses the YouTube RSS feed for a channel.

    :param rss_url: The URL of the YouTube RSS feed.
    :param channel_handle: Canonical channel handle (typically from
        the creator map, falling back to the queue's stored handle)
        to stamp onto every YouTubeVideo parsed from the feed.
    :param proxy: Optional proxy URL for the HTTP request.
    :returns: A list of YouTubeVideo instances populated from the RSS feed.
    :raises: httpx.HTTPStatusError on non-2xx HTTP responses.
    :raises: httpx.RequestError on network-level failures.
    '''

    scrape_start: float = monotonic()
    try:
        extra: dict[str, str] = {'rss_url': rss_url}

        logging.debug('Fetching RSS feed', extra=extra)
        proxy: str | None = (
            await YouTubeRateLimiter.get().acquire(
                YouTubeCallType.RSS
            )
        )
        try:
            proxy_ip: str | None = _extract_proxy_ip(proxy) if proxy else None
        except ValueError as exc:
            logging.warning(
                'Failed to parse proxy URL to get proxy_ip for metrics labeling',
                exc=exc,
                extra={'proxy': proxy},
            )
            proxy_ip = None
        proxy_network: str = proxy_network_for(proxy_ip)
        extra['proxy_ip'] = proxy_ip or 'none'
        extra['proxy_network'] = proxy_network
        extra['proxy'] = proxy or 'none'
        pfl: str = proxy_file_label(proxy or '')
        try:
            http: httpx.AsyncClient = pooled_httpx_client_for_entry(
                proxy,
            )
            # Stagger the first CONNECT tunnel from this worker
            # to *proxy* by a random 0-3s. Cold-start
            # coincidence across N worker processes was the
            # dominant timeout_connect source.
            await jitter_pool_warmup(proxy)
            response: Response = await http.get(
                rss_url,
                timeout=httpx.Timeout(5.0, connect=3.0),
            )
            response.raise_for_status()
            data: str = response.text
            logging.debug(
                'Fetched RSS feed successfully', extra=extra,
            )
            YouTubeRateLimiter.get().report_rss_success(proxy)
        except httpx.HTTPStatusError as exc:
            _handle_http_status_error(
                exc, rss_url, proxy, proxy_ip, proxy_network, extra,
            )
        except httpx.TimeoutException as exc:
            # Split connect vs read so SYN-drop pressure (the
            # dominant signal in production) is visible in
            # Prometheus instead of bundled into a single
            # ``timeout`` reason.
            if isinstance(exc, httpx.ConnectTimeout):
                timeout_kind: str = 'timeout_connect'
                YouTubeRateLimiter.get().report_rss_timeout(proxy)
            elif isinstance(exc, httpx.ReadTimeout):
                timeout_kind = 'timeout_read'
            elif isinstance(exc, httpx.PoolTimeout):
                timeout_kind = 'timeout_pool'
            else:
                timeout_kind = 'timeout_other'
            _record_rss_failure(
                timeout_kind, proxy_ip, proxy_network, pfl,
            )
            logging.warning(
                'Timeout fetching RSS feed',
                exc=exc,
                extra=extra | {'timeout_kind': timeout_kind},
            )
            raise
        except (httpx.NetworkError, httpx.ProxyError) as exc:
            cause: BaseException | None = (
                exc.__cause__ or exc.__context__
            )
            if (
                isinstance(cause, OSError)
                and cause.errno == errno.EADDRNOTAVAIL
            ):
                _record_rss_failure(
                    'bind_failed', proxy_ip, proxy_network, pfl,
                )
                logging.warning(
                    'Local IP not bound on this host',
                    exc=exc, extra=extra,
                )
            else:
                _record_rss_failure(
                    'network', proxy_ip, proxy_network, pfl,
                )
                logging.warning(
                    'Network error fetching RSS feed',
                    exc=exc, extra=extra,
                )
            raise
        except Exception as exc:
            _record_rss_failure(
                'unknown', proxy_ip, proxy_network, pfl,
            )
            logging.warning(
                'Getting RSS data failed', exc=exc, extra=extra,
            )
            raise

        if not data:
            _record_rss_failure(
                'no_data', proxy_ip, proxy_network, pfl,
            )
            logging.warning('No data received from RSS feed', extra=extra)
            raise RuntimeError(
                f'No data received from RSS feed {rss_url}'
            )

        METRIC_RSS_DOWNLOADED.labels(
            platform='youtube',
            scraper='rss_scraper',
            entity='rss_feed',
            api='rss',
            worker_id=get_worker_id(),
            proxy_ip=proxy_ip or 'none',
            proxy_network=proxy_network,
            proxy_file=pfl,
        ).inc()

        feed: untangle.Element = untangle.parse(data)
        raw_entries: list | object = getattr(feed.feed, 'entry', [])
        if not isinstance(raw_entries, list):
            raw_entries = [raw_entries]

        videos: list[YouTubeVideo] = []
        for entry in raw_entries:
            try:
                video: YouTubeVideo = YouTubeVideo.from_rss_entry(
                    entry,
                    channel_handle=channel_handle,
                )
                videos.append(video)
            except AttributeError as exc:
                logging.warning(
                    'Skipping malformed RSS entry', exc=exc, extra=extra
                )

        METRIC_SCRAPE_DURATION.labels(
            platform='youtube',
            scraper='rss_scraper',
            entity='rss_feed',
            api='rss',
            outcome='success',
            worker_id=get_worker_id(),
        ).observe(monotonic() - scrape_start)
        return videos
    except Exception:
        METRIC_SCRAPE_DURATION.labels(
            platform='youtube',
            scraper='rss_scraper',
            entity='rss_feed',
            api='rss',
            outcome='failure',
            worker_id=get_worker_id(),
        ).observe(monotonic() - scrape_start)
        raise


async def check_video_exists(
    client: ExchangeClient, settings: RssSettings, video_id: str
) -> bool:
    '''
    Checks whether a video is already stored in Scrape Exchange.

    :param client: The authenticated Scrape Exchange API client.
    :param settings: Worker settings.
    :param video_id: The YouTube video ID to look up.
    :returns: True if the video is already stored, False if not found.
    :raises: RuntimeError on unexpected API response status codes.
    '''

    url: str = (
        f'{settings.exchange_url}{ExchangeClient.POST_DATA_API}'
        f'/content/youtube/video/{video_id}'
    )
    response: Response = await client.get(url)

    if response.status_code == 200:
        return True
    if response.status_code == 404:
        return False
    raise RuntimeError(
        f'Unexpected status {response.status_code} checking video '
        f'{video_id}: {response.text}'
    )


MSG_PROCESSED_VIDEOS: str = 'Processed videos for channel'


async def _fetch_rss_safe(
    rss_url: str,
    channel_handle: str,
) -> list[YouTubeVideo] | None | Exception:
    '''
    Wrapper around :func:`fetch_rss` that captures
    exceptions so the result can be used inside
    ``asyncio.gather`` without aborting sibling tasks.
    '''
    try:
        return await fetch_rss(
            rss_url,
            channel_handle=channel_handle,
        )
    except Exception as exc:
        return exc


async def _enrich_and_store_video(
    video: YouTubeVideo,
    innertube: InnerTube,
    proxy: str | None,
    channel_handle: str,
    settings: RssSettings,
) -> str | None:
    '''
    Enrich a single video via InnerTube and write the resulting
    ``video-min-`` file to ``settings.video_data_directory``.
    Upload is the video scraper's responsibility — it sweeps the
    same directory for both ``video-min-*`` and ``video-dlp-*``
    files and pushes them through its bulk and watch upload
    pipelines.  Returns the bare filename on success, ``None`` on
    failure.

    :param channel_handle: Display name of the channel, used only
        for logging.
    '''

    proxy_ip: str = _extract_proxy_ip(proxy) if proxy else 'none'
    proxy_network: str = proxy_network_for(proxy_ip)
    extra: dict[str, str] = {
        'video_id': video.video_id,
        'channel_handle': channel_handle,
        'proxy_ip': proxy_ip,
        'proxy_network': proxy_network,
    }
    try:
        await video.from_innertube(
            innertube=innertube, proxy=proxy,
        )
        logging.debug(
            'Updated video using InnerTube data', extra=extra,
        )
        METRIC_INNERTUBE_SUCCESS.labels(
            platform='youtube',
            scraper='rss_scraper',
            entity='video',
            api='innertube',
            status='success',
            worker_id=get_worker_id(),
            proxy_ip=proxy_ip,
            proxy_network=proxy_network,
            proxy_file=proxy_file_label(proxy or ''),
        ).inc()
    except Exception as exc:
        METRIC_INNERTUBE_FAILURES.labels(
            platform='youtube',
            scraper='rss_scraper',
            entity='video',
            api='innertube',
            status='failed',
            worker_id=get_worker_id(),
            proxy_ip=proxy_ip,
            proxy_network=proxy_network,
            proxy_file=proxy_file_label(proxy or ''),
        ).inc()
        logging.warning(
            'Failed to get InnerTube video data, '
            'will continue with RSS data',
            exc=exc, extra=extra,
        )

    try:
        # Prefer the priority directory if configured so the
        # --video-upload-only container uploads RSS-discovered
        # videos ahead of the bulk-archive backlog. Fall back
        # to video_data_directory for the legacy code path.
        target_dir: str = (
            settings.video_priority_directory
            or settings.video_data_directory
        )
        filename: str = await video.to_file(
            target_dir,
            filename_prefix=VIDEO_FILENAME_PREFIX,
            overwrite=True,
        )
        extra['filename'] = filename
    except Exception as exc:
        logging.warning(
            'Failed to write video file to disk, '
            'skipping video',
            exc=exc, extra=extra,
        )
        return None

    # Upload is the video scraper's responsibility: its bulk and
    # watch uploaders sweep ``video-min-*`` and ``video-dlp-*``
    # files together (sharing the same ``boinko/youtube/video``
    # schema) and move successfully-uploaded copies to
    # ``uploaded_dir``.  Doing the upload here would race with
    # that sweep and cause duplicate POSTs to the API.
    logging.debug(
        'Stored video-min file; upload deferred to the '
        'video scraper',
        extra=extra,
    )
    return filename


async def process_channel(
    channel_handle: str, channel_id: str, client: ExchangeClient,
    creator_queue: CreatorQueue, settings: RssSettings,
    creator_map_backend: CreatorMap,
    name_map_backend: NameMap,
    channel_validator: SchemaValidator,
    tier: int,
) -> bool | None:
    '''
    Fetches the RSS feed for one channel and checks or stores each video.

    Returns ``True`` on success, ``False`` on transient failure (the
    channel stays in the queue for retry), or ``None`` when the
    channel should be permanently removed from the queue (e.g.
    too many consecutive no-feed failures).

    Raises on RSS fetch failure or if any video could not be stored, so
    the worker loop can schedule a retry for the whole channel.

    :param channel_handle: Human-readable channel name (for logging).
    :param channel_id: YouTube channel ID.
    :param client: The authenticated Scrape Exchange API client.
    :param creator_queue: Queue backend (file or Redis).
    :param settings: Worker settings.
    :param tier: Priority tier the channel was claimed from; used
        as a Prometheus label on the
        ``channel_seconds_since_last_processed`` gauge.
    :returns: bool if the channel should be scheduled again
    :raises: httpx.HTTPError if the RSS feed cannot be retrieved.
    :raises: RuntimeError if one or more videos could not be stored.
    '''

    proxy: str | None = YouTubeRateLimiter.get().select_proxy(
        YouTubeCallType.BROWSE
    )
    proxy_ip: str = _extract_proxy_ip(proxy) if proxy else 'none'
    extra: dict[str, str] = {
        'channel_handle': channel_handle,
        'channel_id': channel_id,
        'proxy_ip': proxy_ip,
    }
    if channel_id in CHANNEL_LAST_CHECKED:
        elapsed: float = monotonic() - CHANNEL_LAST_CHECKED[channel_id]
        METRIC_CHANNEL_SECONDS_SINCE_LAST_PROCESSED.labels(
            platform='youtube',
            scraper='rss_scraper',
            tier=str(tier),
            worker_id=get_worker_id(),
        ).set(elapsed)
        logging.debug(
            'Processing channel', extra=extra | {'elapsed': elapsed},
        )
    else:
        logging.debug(
            'First time processing channel', extra=extra,
        )

    rss_url: str = YOUTUBE_RSS_URL.format(channel_id=channel_id)
    extra['rss_url'] = rss_url

    # Store failures in the no-feeds store to avoid repeatedly
    # hitting the same missing feed
    no_feed_entry: tuple[str, str, int] | None = (
        await creator_queue.get_no_feeds(channel_id)
    )
    fail_count: int = 0
    if no_feed_entry is not None:
        _: str
        rss_url, _, fail_count = no_feed_entry
        threshold: int = settings.rss_max_no_feed_failures
        if await creator_queue.has_had_feed(channel_id):
            threshold = (
                settings.rss_max_no_feed_failures_had_feed
            )
        if fail_count >= threshold:
            logging.debug(
                'Channel has exceeded no-feed failure '
                'threshold, removing from queue',
                extra=extra | {
                    'fail_count': fail_count,
                    'threshold': threshold,
                },
            )
            METRIC_NO_FEED_LIMIT_HIT.labels(
                platform='youtube',
                scraper='rss_scraper',
                worker_id=get_worker_id(),
                limit='all',
            ).inc()
            return None
        logging.debug(
            'Channel had missing RSS feed',
            extra=extra | {
                'fail_count': fail_count,
                'threshold': threshold,
            },
        )

    CHANNEL_LAST_CHECKED[channel_id] = monotonic()

    # Resolve the canonical channel handle from the creator
    # map so fetch_rss can stamp it onto every YouTubeVideo
    # it parses. If the channel is not yet mapped, fall back
    # to the queue's stored handle
    mapped_handle: str | None = (
        await creator_map_backend.get(channel_id)
    )
    rss_channel_handle: str = mapped_handle or channel_handle

    # --- Phase 1: channel update + RSS fetch in parallel ---
    # Each branch is wrapped in ``_timed`` independently so the
    # phase histogram can attribute the cost of update_channel
    # (an InnerTube call) separately from the RSS feed fetch
    # (an HTTP GET to /feeds/videos.xml). Both run concurrently
    # via asyncio.gather; the wall-clock duration of the gather
    # is dominated by the slower of the two.
    update_result: tuple[bool, int, str | None]
    rss_result: list[YouTubeVideo] | None | Exception
    update_result, rss_result = await asyncio.gather(
        _timed(
            'update_channel',
            update_channel(
                client, channel_handle, channel_id,
                creator_map_backend, name_map_backend,
                channel_validator, proxy,
                settings=settings,
            ),
        ),
        _timed(
            'fetch_rss',
            _fetch_rss_safe(
                rss_url,
                channel_handle=rss_channel_handle,
            ),
        ),
    )
    update_ok: bool = update_result[0]
    sub_count: int = update_result[1]
    resolved_handle: str | None = update_result[2]

    if not update_ok:
        # Leave the tier unchanged — don't overwrite a
        # known subscriber count with 0 just because
        # InnerTube failed this time.
        await creator_queue.set_no_feeds(
            channel_id, rss_url, channel_handle, 1,
        )
        return False
    await creator_queue.update_tier(channel_id, sub_count)

    # Handle RSS fetch errors.
    if isinstance(rss_result, RuntimeError):
        logging.debug(
            'Server error fetching RSS feed',
            extra=extra | {'error': str(rss_result)}
        )
        return True
    if isinstance(rss_result, Exception):
        logging.debug(
            'Failed to fetch RSS feed for channel',
            exc=rss_result,
            extra=extra | {'error': str(rss_result)},
        )
        await creator_queue.set_no_feeds(
            channel_id, rss_url, channel_handle, 1
        )
        return False

    videos: list[YouTubeVideo] | None = rss_result
    if videos is None:
        logging.debug(MSG_NO_RSS_FEED, extra=extra)
        await creator_queue.set_no_feeds(
            channel_id, rss_url, channel_handle, 1
        )
        return False

    await creator_queue.mark_had_feed(channel_id)

    no_feed_entry = await creator_queue.get_no_feeds(
        channel_id,
    )
    if no_feed_entry is not None:
        await creator_queue.clear_no_feeds(channel_id)

    if not videos:
        logging.debug(
            'No videos found in RSS feed for channel', extra=extra
        )
        return True

    logging.debug(
        'Videos found in RSS feed',
        extra=extra | {'video_count': len(videos)},
    )

    # --- Phase 2: filter locally-known videos, then
    #     batch-check existence on scrape.exchange ---
    candidates: list[YouTubeVideo] = []
    videos_existing: int = 0
    for video in videos:
        file_path: str = YouTubeVideo.get_filepath(
            video.video_id,
            settings.video_data_directory,
            VIDEO_FILENAME_PREFIX,
        )
        if os.path.exists(file_path):
            logging.debug(
                'Found existing file for video, '
                'skipping',
                extra=extra | {'video_id': video.video_id},
            )
            videos_existing += 1
        else:
            candidates.append(video)

    if not candidates:
        logging.info(
            MSG_PROCESSED_VIDEOS,
            extra=extra | {
                'videos_uploaded': 0,
                'videos_existing': videos_existing,
                'videos_failed': 0,
                'video_count': len(videos),
            },
        )
        return True

    # Batch-check existence concurrently. The whole gather
    # is timed under ``check_existence`` — when there are many
    # candidates the slowest exchange API call dominates, which
    # is the right thing to measure for capacity planning.
    exist_started: float = monotonic()
    exist_results: list[bool | Exception] = (
        await asyncio.gather(
            *[
                check_video_exists(
                    client, settings, v.video_id,
                )
                for v in candidates
            ],
            return_exceptions=True,
        )
    )
    _observe_phase('check_existence', exist_started)

    new_videos: list[YouTubeVideo] = []
    for video, exists in zip(
        candidates, exist_results,
    ):
        extra['video_id'] = video.video_id
        extra['title'] = video.title
        if isinstance(exists, Exception):
            logging.warning(
                'Failed to check video existence on '
                'scrape.exchange, will upload anyway',
                exc=exists, extra=extra,
            )
            new_videos.append(video)
        elif exists:
            logging.debug('Video already on scrape.exchange', extra=extra)
            videos_existing += 1
        else:
            new_videos.append(video)

    if not new_videos:
        logging.info(
            MSG_PROCESSED_VIDEOS,
            extra=extra | {
                'videos_uploaded': 0,
                'videos_existing': videos_existing,
                'videos_failed': 0,
                'video_count': len(videos),
            },
        )
        return True

    # --- Phase 3: enrich + upload new videos in
    #     parallel ---
    innertube: InnerTube = InnerTube('WEB', proxies=proxy,)

    def _close_innertube() -> None:
        '''Close the InnerTube httpx session.'''
        session: object = getattr(
            innertube.adaptor, 'session', None,
        )
        if session is not None and hasattr(
            session, 'close',
        ):
            session.close()

    # update_ok was True above, so resolved_handle is set; assert for
    # the type-checker.
    assert resolved_handle is not None
    enrich_started: float = monotonic()
    enrich_results: list[str | None | Exception] = (
        await asyncio.gather(
            *[
                _enrich_and_store_video(
                    video, innertube, proxy,
                    channel_handle, settings,
                )
                for video in new_videos
            ],
            return_exceptions=True,
        )
    )
    _observe_phase('enrich_videos', enrich_started)

    videos_uploaded: int = 0
    videos_failed: int = 0
    for video, result in zip(
        new_videos, enrich_results,
    ):
        if isinstance(result, Exception):
            logging.warning(
                'Failed to process video',
                exc=result, extra=extra
            )
            videos_failed += 1
        elif result is None:
            videos_failed += 1
        else:
            videos_uploaded += 1

    missed: int = (
        len(new_videos) - videos_uploaded - videos_failed
    )
    logging.info(
        MSG_PROCESSED_VIDEOS, extra={
            'channel_handle': channel_handle,
            'videos_uploaded': videos_uploaded,
            'videos_existing': videos_existing,
            'videos_failed': videos_failed,
            'video_count': len(videos),
        },
    )
    if missed > 0:
        _close_innertube()
        raise RuntimeError(
            f'{missed} out of {len(videos)} videos '
            f'for channel {channel_handle!r} could not '
            f'be processed'
        )

    _close_innertube()
    videos = []
    return True


async def _ensure_priority_directory(
    settings: RssSettings,
) -> None:
    '''Create the channel priority directory if missing
    and verify it is writable. Called once at RSS scraper
    startup before any worker loop runs so a
    misconfigured mount surfaces immediately.

    :raises OSError: directory cannot be created or a
        temp-file write fails. Propagated so the
        supervisor exits non-zero and operators see the
        failure in container logs.
    '''
    path: str = settings.channel_priority_directory_path
    os.makedirs(path, exist_ok=True)
    probe: Path = (
        Path(path) / f'.write_probe.{os.getpid()}'
    )
    try:
        await asyncio.to_thread(
            probe.write_bytes, b'ok',
        )
    finally:
        try:
            await asyncio.to_thread(probe.unlink)
        except FileNotFoundError:
            pass


async def _write_channel_priority(
    record_dict: dict, settings: RssSettings,
) -> None:
    '''Persist the RSS-derived channel-stat record under
    ``settings.channel_priority_directory_path``. The
    ``--channel-upload-only`` container sweeps this
    directory ahead of base_dir and POSTs each file to
    scrape.exchange.

    The file is written via ``atomic_write_bytes`` so the
    consumer never sees a torn file. Concurrent writers
    for the same handle are last-writer-wins; both
    records carry the same RSS-derived stats so the lost
    write is functionally identical.

    :raises OSError: filesystem write failed; propagated
        so the streamer's outer exception handler logs it
        and the supervisor respawns the worker. The
        operator gets a ``Child crashed; will respawn``
        signal that something on the filesystem side is
        wrong (disk full, permission, missing mount).
    '''
    priority_dir: str = (
        settings.channel_priority_directory_path
    )
    handle: str = record_dict['channel_handle']
    target: Path = (
        Path(priority_dir)
        / f'channel-rss-{handle}.json.br'
    )
    data: bytes = brotli.compress(
        orjson.dumps(
            record_dict, option=orjson.OPT_INDENT_2,
        ),
        quality=11, mode=brotli.MODE_TEXT,
    )
    await atomic_write_bytes(target, data)


async def update_channel(
    client: ExchangeClient, channel_handle: str,
    channel_id: str,
    creator_map_backend: CreatorMap,
    name_map_backend: NameMap,
    validator: SchemaValidator,
    proxy: str | None = None,
    *,
    settings: RssSettings,
) -> tuple[bool, int, str | None]:
    '''
    Fetches channel metadata via InnerTube and updates the channel
    data on Scrape Exchange via the data API.

    :param client: The authenticated Scrape Exchange API client.
    :param channel_handle: The channel handle / vanity name as known
        to the caller (may be mis-cased; canonicalised here).
    :param channel_id: The YouTube channel ID.
    :param creator_map_backend: CreatorMap to persist the resolved
        handle for reads by other scrapers.
    :param name_map_backend: NameMap to persist the
        ``(channel_title, channel_id)`` pair so re-ingest can
        recover ids from legacy display-name-only records.
    :param proxy: Optional proxy URL for the InnerTube request.
    :returns: Tuple of (success, subscriber_count, resolved_handle).
        ``success`` is True if the channel data was fetched and
        uploaded. ``resolved_handle`` is None when fetch failed.
    :raises: (none)
    '''

    try:
        proxy_ip: str = _extract_proxy_ip(proxy) if proxy else 'none'
        proxy_network: str = proxy_network_for(proxy_ip)
        tabs: YouTubeChannelTabs = YouTubeChannelTabs(channel_id, proxy)
        channel_data: dict = await tabs.browse_channel()
    except Exception as exc:
        logging.debug(
            'Failed to browse channel via InnerTube',
            exc=exc,
            extra={'channel_handle': channel_handle, 'proxy': proxy},
        )
        METRIC_INNERTUBE_FAILURES.labels(
            platform='youtube',
            scraper='rss_scraper',
            entity='channel',
            api='innertube',
            status='failed',
            worker_id=get_worker_id(),
            proxy_ip=proxy_ip,
            proxy_network=proxy_network,
            proxy_file=proxy_file_label(proxy or ''),
        ).inc()
        return False, 0, None

    METRIC_INNERTUBE_SUCCESS.labels(
        platform='youtube',
        scraper='rss_scraper',
        entity='channel',
        api='innertube',
        status='success',
        worker_id=get_worker_id(),
        proxy_ip=proxy_ip,
        proxy_network=proxy_network,
        proxy_file=proxy_file_label(proxy or ''),
    ).inc()

    canonical: str | None = canonical_handle_from_browse(channel_data)
    resolved_handle: str
    if canonical:
        resolved_handle = canonical
        CREATOR_MAP_RESOLUTION_TOTAL.labels(
            platform='youtube',
            scraper='rss_scraper',
            outcome='canonical',
        ).inc()
    else:
        resolved_handle = fallback_handle(channel_handle)
        CREATOR_MAP_RESOLUTION_TOTAL.labels(
            platform='youtube',
            scraper='rss_scraper',
            outcome='fallback',
        ).inc()

    if channel_handle != resolved_handle:
        CREATOR_HANDLE_MISMATCH_TOTAL.labels(
            platform='youtube', scraper='rss_scraper',
        ).inc()
        logging.info(
            'RSS update_channel: canonicalising handle',
            extra={
                'channel_id': channel_id,
                'input_name': channel_handle,
                'canonical_handle': resolved_handle,
            },
        )

    await creator_map_backend.put(channel_id, resolved_handle)

    metadata: dict = channel_data.get(
        'metadata', {}
    ).get('channelMetadataRenderer', {})

    title: str = metadata.get('title', channel_handle)
    if title:
        await name_map_backend.put(
            asset_title=title, asset_id=channel_id,
        )
    description: str = metadata.get('description', '')

    subscriber_count: int = (
        YouTubeChannel.parse_subscriber_count(
            channel_data,
        ) or 0
    )
    # YouTube no longer surfaces lifetime views on the channel
    # header — they only appear on the About tab, which the
    # RSS scraper does not fetch. ``parse_view_count`` returns
    # ``None`` in that case; omit the field rather than write
    # ``0`` (indistinguishable from a real zero-view channel)
    # so the server keeps whatever value the full channel
    # scraper last wrote.
    view_count: int | None = (
        YouTubeChannel.parse_view_count(channel_data)
    )
    video_count: int = (
        YouTubeChannel.parse_video_count(
            channel_data,
        ) or 0
    )

    # Fire-and-forget: background worker inside ExchangeClient handles
    # the POST with retries. No file_manager is passed because an RSS
    # channel update has no on-disk asset backing it; success is
    # tracked via uploads_completed_total.
    #
    # platform_content_id and platform_creator_id are intentionally
    # omitted: the server derives them from the channel schema's
    # ``x-scrape-field`` markers (``channel_id`` →
    # ``platform_content_id``, ``channel_handle`` →
    # ``platform_creator_id``) which are now present in the data
    # dict under the schema-declared field names.
    channel_url: str = (
        f'https://www.youtube.com/channel/{channel_id}'
    )
    record_dict: dict = {
        'channel_id': channel_id,
        'channel_handle': resolved_handle,
        'url': channel_url,
        'title': title,
        'subscriber_count': subscriber_count,
        'video_count': video_count,
        'description': description,
    }
    if view_count is not None:
        record_dict['view_count'] = view_count
    err: str | None = validator.validate(record_dict)
    if err is not None:
        logging.warning(
            'RSS channel update failed schema validation, '
            'skipping upload',
            extra={
                'channel_id': channel_id,
                'channel_handle': resolved_handle,
                'validation_error': err,
            },
        )
        return False, subscriber_count, resolved_handle

    await _write_channel_priority(record_dict, settings)
    METRIC_CHANNEL_PRIORITY_WRITES.labels(
        platform='youtube',
        scraper='rss_scraper',
        worker_id=get_worker_id(),
    ).inc()

    return True, subscriber_count, resolved_handle


def read_channel_file(filepath: str) -> dict[str, any]:
    '''
    Reads a channel data file, which may be compressed with Brotli.

    :param filepath: Path to the channel data file.
    :returns: The parsed channel data as a dictionary.
    :raises: OSError if there is an error reading the file.
             orjson.JSONDecodeError if the file contents cannot be parsed as
             JSON.
    '''

    if filepath.endswith(FILE_EXTENSION):
        with open(filepath, 'rb') as f:
            decompressed_data: bytes = brotli.decompress(f.read())
            return orjson.loads(decompressed_data)
    elif filepath.endswith('.json'):
        with open(filepath, 'r') as f:
            return orjson.loads(f.read())


def _log_channel_result(
    result: object,
    name: str,
    cid: str,
) -> None:
    '''Log the outcome of a single process_channel call.'''

    if (isinstance(result, ValueError)
            or (
                isinstance(result, bool)
                and result is False
            )):
        logging.info(
            MSG_NO_RSS_FEED,
            extra={
                'channel_handle': name,
                'channel_id': cid,
            },
        )
    elif (isinstance(result, BaseException)
            and not isinstance(result, FileExistsError)):
        logging.warning(
            'Channel failed',
            exc_info=result,
            extra={
                'channel_handle': name,
                'error_type': type(result).__name__,
                'error': result,
            },
        )


def _is_on_time(
    scheduled_time: float,
    now: float,
    interval_seconds: float,
    eligibility_fraction: float,
) -> bool:
    '''Return True iff *now* is within the tier's
    full target interval.

    The queue score (*scheduled_time*) is set to
    ``last_run + eligibility_fraction * interval``
    by ``release()``, so the deadline for an on-time
    fetch is::

        scheduled + (1 - eligibility_fraction)
                  * interval

    which is equivalent to ``last_run + interval``.

    With ``eligibility_fraction = 1.0`` the deadline
    collapses to *scheduled_time* itself; combined
    with the claim cutoff (``score <= now``) every
    fetch is then overdue. That is the historical
    behaviour and is mathematically correct: with no
    scheduling headroom there is no room to be
    on-time.
    '''

    deadline: float = (
        scheduled_time
        + (1.0 - eligibility_fraction) * interval_seconds
    )
    return now <= deadline


def _record_tier_sla(
    tier: int,
    scheduled_time: float,
    now: float,
    interval_seconds: float,
    eligibility_fraction: float,
) -> None:
    '''Increment the on-time or overdue SLA counter.'''

    wid: str = get_worker_id()
    if _is_on_time(scheduled_time, now,
                   interval_seconds, eligibility_fraction):
        METRIC_TIER_ON_TIME.labels(
            platform='youtube',
            scraper='rss_scraper',
            tier=str(tier),
            worker_id=wid,
        ).inc()
    else:
        METRIC_TIER_OVERDUE.labels(
            platform='youtube',
            scraper='rss_scraper',
            tier=str(tier),
            worker_id=wid,
        ).inc()


async def _publish_queue_sizes(
    creator_queue: CreatorQueue,
) -> None:
    '''Set the per-tier queue size gauges from Redis ZCARD.

    Queue size is shared state, not per-worker. The metric has
    no ``worker_id`` label; publication is gated to a single
    worker per host (``WORKER_ID == '1'``) by the caller. Two
    publishers fleet-wide (one per supervisor host) → use
    ``max by (tier)`` in PromQL when aggregating across
    instances; ``sum`` would double-count.
    '''

    sizes: dict[int, int] = (
        await creator_queue.queue_sizes_by_tier()
    )
    for tier, count in sizes.items():
        METRIC_QUEUE_SIZE.labels(
            platform='youtube',
            scraper='rss_scraper',
            entity='rss_feed',
            tier=str(tier),
        ).set(count)


async def _publish_tier_population(
    creator_queue: CreatorQueue,
) -> None:
    '''Set the ``channel_tier_population`` gauge to the count of
    cids per tier from the ``rss:youtube:tiers`` HASH.

    Uses a cheap HSCAN-and-count (no per-cid pipelined ZSCORE
    fan-out). The previous implementation invoked
    ``scan_and_recover_orphans``, which fired ~7 Redis ops per
    cid in pipelines of 500 — at ~3M ops on the full hash it
    routinely failed to complete under fleet load and the gauge
    converged on partial counts. The new code performs a single
    HSCAN traversal of the tiers hash and counts values.
    '''
    counts: dict[int, int] = (
        await creator_queue.tier_population_summary()
    )
    for tier, count in counts.items():
        METRIC_TIER_POPULATION.labels(
            platform='youtube',
            scraper='rss_scraper',
            tier=str(tier),
        ).set(count)


async def _publish_queue_metrics_loop(
    creator_queue: CreatorQueue,
    interval_seconds: float = 30.0,
) -> None:
    '''Lightweight periodic publisher for the queue-size and
    tier-population gauges. Cheap (5 ZCARDs + one HSCAN scan
    per tick); safe to run alongside other workers.

    Gating to a single worker per host is the caller's
    responsibility.
    '''
    while True:
        try:
            await _publish_queue_sizes(creator_queue)
            await _publish_tier_population(creator_queue)
        except Exception:
            logging.warning(
                'queue metrics publish failed',
                exc_info=True,
            )
        await asyncio.sleep(interval_seconds)


async def _scan_and_recover_loop(
    creator_queue: CreatorQueue,
    interval_seconds: float = 300.0,
) -> None:
    '''Periodic orphan recovery loop.

    Runs forever until the task is cancelled. Failures are
    logged at WARNING and the loop continues on the next tick.

    Metric publishing has been moved to
    :func:`_publish_queue_metrics_loop` because the per-cid
    pipelined HSCAN here was not completing in production. This
    loop now only re-enqueues orphans (cids in the tiers hash
    absent from every queue) and increments the
    ``channel_orphans_recovered_total`` counter.

    Gating to a single worker is the caller's responsibility.
    '''
    while True:
        try:
            breakdown: dict[int, dict[str, int]] = (
                await creator_queue
                .scan_and_recover_orphans(recover=True)
            )
            for tier, counts in breakdown.items():
                METRIC_ORPHANS_RECOVERED.labels(
                    platform='youtube',
                    scraper='rss_scraper',
                    tier=str(tier),
                ).inc(counts.get('orphan', 0))
        except Exception:
            logging.warning(
                'orphan recovery scan failed',
                exc_info=True,
            )
        await asyncio.sleep(interval_seconds)


async def _enrich_subscriber_counts(
    client: ExchangeClient,
    channel_map: dict[str, str],
    subscriber_counts: dict[str, int],
    known_ids: set[str] | None = None,
) -> None:
    '''
    Fill in missing subscriber counts by querying the
    scrape.exchange content API.  Modifies
    *subscriber_counts* in place.

    Skips channels that already have a subscriber
    count in *subscriber_counts* or are already
    enqueued in *known_ids* (from Redis).  This
    avoids 100K+ API calls on startup when most
    channels are already in the queue from a
    previous run.
    '''

    skip: set[str] = set(subscriber_counts.keys())
    if known_ids:
        skip |= known_ids

    missing: list[str] = [
        cid for cid in channel_map
        if cid not in skip
    ]
    if not missing:
        return

    # Cap API lookups to avoid blocking startup
    # indefinitely.  Un-enriched channels default to
    # the lowest tier and get re-tiered after first
    # scrape.
    _MAX_LOOKUPS: int = 100_000
    if len(missing) > _MAX_LOOKUPS:
        logging.info(
            'Capping API enrichment (rest default to '
            'lowest tier)',
            extra={
                'enrichment_cap': _MAX_LOOKUPS,
                'missing_count': len(missing),
            },
        )
        missing = missing[:_MAX_LOOKUPS]

    logging.info(
        'Fetching subscriber counts from API '
        'for channels without local data',
        extra={
            'count': len(missing),
            'skipped_known': (
                len(channel_map) - len(missing)
            ),
        },
    )
    fetched: int = 0
    for cid in missing:
        name: str = channel_map[cid]
        url: str = (
            f'{client.exchange_url}'
            f'{ExchangeClient.GET_CONTENT_API}'
            f'/youtube/channel/{name}'
        )
        try:
            resp: Response = await client.get(url)
        except Exception:
            continue
        if resp.status_code != 200:
            continue
        try:
            data: dict = resp.json()
        except Exception:
            continue
        sub_count: int = data.get(
            'subscriber_count', 0,
        )
        if sub_count:
            subscriber_counts[cid] = sub_count
            fetched += 1

    if fetched:
        logging.info(
            'Enriched subscriber counts from API',
            extra={'fetched': fetched},
        )


async def _seed_queue_from_uploaded_channels(
    creator_queue: CreatorQueue,
    channel_fm: AssetFileManagement,
    tiers: list[TierConfig],
) -> int:
    '''
    Walk ``channel_fm.uploaded_dir`` for previously-uploaded channel
    files and enqueue any whose ``channel_id`` isn't already in the
    priority queue. Channels in the queue are left alone so this
    helper is safe to call on every startup.

    Tier selection per file:

    * ``subscriber_count`` is present and > 0 → standard
      :func:`tier_for_subscriber_count` routing.
    * ``subscriber_count`` is missing or zero → the **next-to-last**
      tier (one above the lowest-priority catch-all). Treating
      "unknown" as second-from-last avoids the populate path's
      default behaviour of promoting unknown counts to tier 1, which
      is wrong when bulk-seeding from on-disk archives where most
      records are simply missing the field.

    Implementation note: tier targeting is achieved by passing a
    synthetic ``subscriber_count`` equal to the target tier's
    ``min_subscribers``. This relies on tiers being ordered with
    monotonically descending min_subscribers (the documented
    convention in :func:`tier_for_subscriber_count`).

    :returns: Number of channels added to the queue.
    '''

    if not tiers:
        return 0

    fallback_tier: TierConfig = (
        tiers[-2] if len(tiers) >= 2 else tiers[-1]
    )
    fallback_min: int = max(fallback_tier.min_subscribers, 0)

    known: set[str] = await creator_queue.known_creator_ids()

    creators: dict[str, str] = {}
    subscriber_counts: dict[str, int] = {}
    fallback_count: int = 0

    files: list[str] = channel_fm.list_uploaded(
        prefix=CHANNEL_FILENAME_PREFIX, suffix='.json.br',
    )

    for filename in files:
        try:
            data: dict = await channel_fm.read_uploaded(filename)
        except Exception as exc:
            logging.warning(
                'Failed to read uploaded channel file during seed',
                exc=exc, extra={'filename': filename},
            )
            continue

        cid: str | None = data.get('channel_id')
        handle: str | None = data.get('channel_handle')
        if not cid or not handle:
            continue
        if cid in known:
            continue

        creators[cid] = handle
        sub_count: int | None = data.get('subscriber_count')
        if isinstance(sub_count, int) and sub_count > 0:
            subscriber_counts[cid] = sub_count
        else:
            subscriber_counts[cid] = fallback_min
            fallback_count += 1

    if not creators:
        logging.info(
            'No new channels to seed from uploaded directory',
            extra={
                'scanned_files': len(files),
                'already_in_queue': len(files),
            },
        )
        return 0

    added: int = await creator_queue.populate(
        creators, channel_fm, tiers, subscriber_counts,
    )
    logging.info(
        'Seeded queue from uploaded channel directory',
        extra={
            'scanned_files': len(files),
            'added': added,
            'unknown_subscriber_count': fallback_count,
            'fallback_tier': fallback_tier.tier,
        },
    )
    return added


def _observe_phase(phase: str, started: float) -> None:
    '''Record a duration observation against
    ``METRIC_PHASE_DURATION`` for *phase*. ``started`` is the
    ``time.monotonic()`` value captured at the start of the
    measured interval.'''
    METRIC_PHASE_DURATION.labels(
        platform='youtube',
        scraper='rss_scraper',
        phase=phase,
        worker_id=get_worker_id(),
    ).observe(monotonic() - started)


async def _timed(
    phase: str, coro: Awaitable[T_Phase],
) -> T_Phase:
    '''Await *coro* while recording its wall-clock duration into
    ``METRIC_PHASE_DURATION`` under the given *phase* label.
    The observation fires on both the success and exception
    paths so a wrapped failure is still attributed to its
    phase.'''
    started: float = monotonic()
    try:
        return await coro
    finally:
        _observe_phase(phase, started)


async def _stream_processor(
    streamer_id: int,
    creator_queue: CreatorQueue,
    client: ExchangeClient,
    creator_map_backend: CreatorMap,
    name_map_backend: NameMap,
    channel_validator: SchemaValidator,
    settings: RssSettings,
) -> None:
    '''Single-channel streaming processor.

    Replaces the prior ``claim_batch(N) → asyncio.gather(N)``
    pattern which suffered from gather tail-latency dominance:
    one slow channel held up the entire batch's worker, leaving
    most of the fleet's task budget idle. With one channel per
    streamer task, a slow channel only delays its own streamer;
    sibling streamers continue at the rate-limiter's natural
    pace.

    Each iteration records four phase timings into
    ``METRIC_PHASE_DURATION`` so the next bottleneck is
    diagnosable from Prometheus alone:

    * ``claim``   — time inside ``creator_queue.claim_batch(1)``
    * ``process`` — total time inside ``process_channel``
    * ``release`` — Redis release / remove + SLA bookkeeping
    * ``idle``    — backoff sleep after an empty claim batch

    :param streamer_id: 0-indexed identifier within the worker
        process. Used only as a logging attribution; the
        Prometheus ``worker_id`` label still groups all
        streamers within a process under a single series.
    '''
    worker_id: str = get_worker_id()
    while True:
        try:
            t0: float = monotonic()
            batch: list[tuple[str, str, float]] = (
                await creator_queue.claim_batch(1, worker_id)
            )
            _observe_phase('claim', t0)

            if not batch:
                t_idle: float = monotonic()
                await asyncio.sleep(MIN_SLEEP_SECONDS)
                _observe_phase('idle', t_idle)
                continue

            cid, name, sched = batch[0]
            claim_tier: int = (
                await creator_queue.get_tier(cid)
            )

            t_proc: float = monotonic()
            try:
                result: object = await process_channel(
                    name, cid, client,
                    creator_queue, settings,
                    creator_map_backend, name_map_backend,
                    channel_validator,
                    claim_tier,
                )
            except Exception as exc:
                result = exc
            _observe_phase('process', t_proc)

            _log_channel_result(result, name, cid)

            t_rel: float = monotonic()
            if result is None:
                await creator_queue.remove(cid)
            else:
                tier: int = (
                    await creator_queue.get_tier(cid)
                )
                interval_seconds: float = (
                    creator_queue.get_tier_interval(tier)
                    * 3600
                )
                failed: bool = (
                    result is False
                    or isinstance(result, Exception)
                )
                retry_s: float | None = (
                    float(settings.retry_interval)
                    if failed else None
                )
                await creator_queue.release(
                    cid,
                    retry_interval_seconds=retry_s,
                )
                _record_tier_sla(
                    tier, sched,
                    datetime.now(UTC).timestamp(),
                    interval_seconds,
                    settings.eligibility_fraction,
                )
            _observe_phase('release', t_rel)

        except asyncio.CancelledError:
            raise
        except Exception:
            logging.exception(
                'Stream processor uncaught error',
                extra={'streamer_id': streamer_id},
            )
            # Don't let an unexpected error kill the streamer;
            # back off briefly and retry to avoid hot-looping
            # against a transient problem.
            await asyncio.sleep(MIN_SLEEP_SECONDS)


async def worker_loop(
    settings: RssSettings,
    client: ExchangeClient,
    channel_fm: AssetFileManagement,
    creator_queue: CreatorQueue,
    tiers: list[TierConfig],
    creator_map_backend: CreatorMap,
    name_map_backend: NameMap,
    channel_validator: SchemaValidator,
) -> None:
    '''
    Runs indefinitely, processing channels in priority order.

    Each channel is assigned a next-check timestamp based on its
    subscriber-count tier. The loop pops up to rss_concurrency
    channels that are due, processes them concurrently, then
    re-schedules them via the queue's tier-aware release logic.

    :param settings: Worker settings.
    :param client: The authenticated Scrape Exchange API client.
    :param channel_fm: AssetFileManagement instance owning the
        channel data directory.
    :param creator_queue: Queue backend (file or Redis).
    :param tiers: Priority tier configuration.
    :param creator_map_backend: Creator map backend (file or Redis).
    '''

    channel_map_data: dict[str, str] = (
        await creator_map_backend.get_all()
    )
    subscriber_counts: dict[str, int] = {}
    known_ids: set[str] = (
        await creator_queue.known_creator_ids()
    )
    if settings.rss_enrich_subscriber_counts:
        await _enrich_subscriber_counts(
            client, channel_map_data, subscriber_counts,
            known_ids=known_ids,
        )
    else:
        logging.info(
            'Skipping subscriber-count enrichment '
            '(RSS_ENRICH_SUBSCRIBER_COUNTS=false); '
            'un-enriched channels default to lowest tier',
        )
    added: int = await creator_queue.populate(
        channel_map_data, channel_fm, tiers, subscriber_counts,
    )
    # Pull in any channels that exist as uploaded files on disk but
    # are missing from the creator_map (e.g. after a fleet rebuild
    # or DB wipe where the local archive is the source of truth).
    # Opt-in: the scan brotli-decodes every uploaded channel file
    # before checking whether it is already queued, which on a
    # populated archive is multi-tens-of-minutes per worker. Set
    # ``RSS_SEED_QUEUE_FROM_UPLOADED=true`` only for first-time
    # setup or recovery scenarios.
    if settings.rss_seed_queue_from_uploaded:
        seeded: int = await _seed_queue_from_uploaded_channels(
            creator_queue, channel_fm, tiers,
        )
        if seeded:
            added += seeded
    else:
        logging.info(
            'Skipping seed-from-uploaded scan '
            '(RSS_SEED_QUEUE_FROM_UPLOADED=false); '
            'queue assumed already populated in Redis',
        )
    # Re-enqueue creators whose tier hash entry exists but
    # which are missing from every tier zset (abandoned
    # claims from worker crashes, stale state from older
    # schema versions, etc.). Runs once at startup after
    # populate so repopulation and recovery complete before
    # workers start claiming.
    recovered: int = await creator_queue.cleanup_stale_claims()
    logging.info(
        'Recovered orphan creators at startup',
        extra={'recovered_count': recovered},
    )
    # Hold strong references to the long-running background
    # tasks so the asyncio event loop's weak-ref tracking does
    # not garbage-collect them mid-execution. Without this, a
    # pause in the worker_loop's inner scope at the wrong
    # moment can let the GC reap an in-flight scan.
    _bg_tasks: list[asyncio.Task[None]] = []
    if get_worker_id() == '1':
        _bg_tasks.append(asyncio.create_task(
            _scan_and_recover_loop(creator_queue),
        ))
        _bg_tasks.append(asyncio.create_task(
            _publish_queue_metrics_loop(creator_queue),
        ))
        logging.info(
            'Started orphan-recovery and queue-metrics loops',
            extra={
                'recovery_interval_seconds': 300,
                'metrics_interval_seconds': 30,
            },
        )
    queue_size: int = await creator_queue.queue_size()
    METRIC_CHANNEL_MAP_SIZE.labels(
        platform='youtube',
        scraper='rss_scraper',
        worker_id=get_worker_id(),
    ).set(len(channel_map_data))
    # Initial publish so the gauge is non-zero before the
    # first metric-loop tick; the loop takes over after that.
    if get_worker_id() == '1':
        await _publish_queue_sizes(creator_queue)
        await _publish_tier_population(creator_queue)

    logging.info(
        'Worker started',
        extra={
            'channel_count': queue_size,
            'channels_added': added,
            'min_interval': settings.min_interval,
            'retry_interval': settings.retry_interval,
            'rss_concurrency': settings.rss_concurrency,
            'discovered_channels': len(channel_map_data),
        },
    )

    # Spawn ``rss_concurrency`` independent stream processors.
    # Replaces the prior ``claim_batch(N) → asyncio.gather(N)``
    # pattern: with one channel per streamer task, a slow
    # channel only delays its own streamer; sibling streamers
    # keep running at the rate-limiter's natural pace and the
    # full task budget stays utilised. The startup-time
    # populate / cleanup_stale_claims above seeded the queue;
    # the queue-empty rebuild path (formerly an inline branch)
    # is no longer needed because the queue has 414 K entries
    # in production and would not naturally drain — empty
    # claims are absorbed by each streamer's own backoff
    # without any worker-level coordination.
    METRIC_CONCURRENCY.labels(
        platform='youtube',
        scraper='rss_scraper',
        worker_id=get_worker_id(),
    ).set(settings.rss_concurrency)
    streamers: list[asyncio.Task[None]] = [
        asyncio.create_task(
            _stream_processor(
                streamer_id=i,
                creator_queue=creator_queue,
                client=client,
                creator_map_backend=creator_map_backend,
                name_map_backend=name_map_backend,
                channel_validator=channel_validator,
                settings=settings,
            ),
        )
        for i in range(settings.rss_concurrency)
    ]
    try:
        await asyncio.gather(*streamers)
    except asyncio.CancelledError:
        for s in streamers:
            s.cancel()
        raise


async def _run_worker(
    ctx: ScraperRunContext,
) -> None:
    '''
    Run a single in-process RSS scraper worker (the leaf of the
    supervisor tree). Receives an already-configured
    :class:`ScraperRunContext` from :class:`ScraperRunner` and
    enters :func:`worker_loop`.
    '''
    settings: RssSettings = ctx.settings

    # Fail fast on a misconfigured priority directory so
    # the supervisor exits non-zero instead of silently
    # losing channel updates on every scrape.
    await _ensure_priority_directory(settings)

    # AssetFileManagement creates channel_data_directory and its
    # 'uploaded' subdirectory automatically. video_data_directory is
    # still managed by YouTubeVideo.to_file directly, so we create
    # it manually here.
    channel_fm: AssetFileManagement = AssetFileManagement(
        settings.channel_data_directory
    )
    os.makedirs(settings.video_data_directory, exist_ok=True)

    creator_queue: CreatorQueue
    if settings.redis_dsn:
        creator_queue = RedisCreatorQueue(
            settings.redis_dsn,
            get_worker_id(),
            platform='youtube',
            eligibility_fraction=settings.eligibility_fraction,
        )
    else:
        creator_queue = FileCreatorQueue(
            settings.queue_file,
            settings.no_feeds_file,
            eligibility_fraction=settings.eligibility_fraction,
            had_feed_file=settings.had_feed_file,
        )

    creator_map_backend: CreatorMap
    if settings.redis_dsn:
        creator_map_backend = RedisCreatorMap(
            settings.redis_dsn,
            platform='youtube',
        )
    else:
        creator_map_backend = FileCreatorMap(
            settings.channel_map_file,
        )

    name_map_backend: NameMap
    if settings.redis_dsn:
        name_map_backend = RedisNameMap(
            settings.redis_dsn, platform='youtube',
        )
    else:
        name_map_backend = NullNameMap()

    tiers: list[TierConfig] = parse_priority_queues(
        settings.priority_queues,
    )

    # Build the schema validators once at startup. The RSS scraper
    # uploads only channel-stat updates — video-min files are
    # written to disk but uploaded by the video scraper, which
    # owns the boinko/youtube/video schema.  Records that don't
    # conform to the channel schema are logged at WARNING and
    # dropped (RSS records have no on-disk asset to mark
    # ``.invalid``).
    channel_schema_dict: dict = await fetch_schema_dict(
        ctx.client,
        settings.exchange_url,
        CHANNEL_SCHEMA_OWNER,
        CHANNEL_SCHEMA_PLATFORM,
        CHANNEL_SCHEMA_ENTITY,
        CHANNEL_SCHEMA_VERSION,
    )
    channel_validator: SchemaValidator = SchemaValidator(
        channel_schema_dict,
    )

    await worker_loop(
        settings, ctx.client, channel_fm,
        creator_queue, tiers,
        creator_map_backend, name_map_backend,
        channel_validator,
    )


def main() -> None:
    '''
    Top-level entry point. Reads settings and dispatches to either
    the supervisor (when ``rss_num_processes > 1``) or the
    in-process scraper worker (when ``rss_num_processes == 1``).
    '''
    settings: RssSettings = RssSettings()

    if not settings.api_key_id or (
        not settings.api_key_secret
    ):
        print(
            'Error: API key ID and secret must be '
            'provided via --api-key-id/--api-key-'
            'secret, environment variables '
            'API_KEY_ID/API_KEY_SECRET, or a .env '
            'file'
        )
        sys.exit(1)

    runner: ScraperRunner = ScraperRunner(
        settings=settings,
        scraper_label='rss',
        platform='youtube',
        num_processes=settings.rss_num_processes,
        concurrency=settings.rss_concurrency,
        metrics_port=settings.metrics_port,
        log_file=settings.rss_log_file,
        log_level=settings.rss_log_level,
        rate_limiter_factory=lambda s: (
            YouTubeRateLimiter.get(
                state_dir=s.rate_limiter_state_dir,
                redis_dsn=s.redis_dsn,
            )
        ),
    )
    sys.exit(runner.run_sync(_run_worker))


if __name__ == '__main__':
    main()
