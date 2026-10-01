'''Queue missing videos from complete channel enumerations.'''

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

import httpx2 as httpx  # exchange API traffic runs on httpx2 (HTTP/2)
import redis.asyncio as aioredis
from prometheus_client import Counter, Histogram

from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.video_scrape_queue import (
    RedisVideoScrapeQueue,
    VideoScrapeQueueSettings,
)
from scrape_exchange.youtube.uploaded_video_ids import UploadedVideoIds
from scrape_exchange.youtube.youtube_channel import YouTubeChannel

_LOGGER: logging.Logger = logging.getLogger(__name__)

BATCH_SIZE: int = 100
DELIVER_ATTEMPTS: int = 3
DELIVER_BACKOFF_SECONDS: tuple[float, ...] = (0.5, 1.0, 2.0)
# Transient upstream statuses worth retrying.
RETRYABLE_STATUSES: frozenset[int] = frozenset({502, 503, 504})
# Page size for POST /api/v1/filter. The server caps pages at 1000
# records; a channel's full video inventory is typically 1-3 pages.
FILTER_PAGE_SIZE: int = 1000


FULL_SCRAPES: Counter = Counter(
    'channel_full_scrapes_total',
    'Full channel scrape outcomes including video queue delivery',
    ['outcome'],
)
VIDEO_IDS: Counter = Counter(
    'channel_full_scrape_video_ids_total',
    'Video IDs processed by full channel scrapes',
    ['outcome'],
)
ADDED_PER_SCRAPE: Histogram = Histogram(
    'channel_full_scrape_video_ids_added',
    'Video IDs added to the queue per full channel scrape attempt',
    buckets=(0, 1, 10, 100, 1000, 10000, 100000),
)


@dataclass
class FullScrapeSummary:
    '''One summary per full attempt; IDs belong in logs, not labels.'''

    channel_id: str
    video_ids_found: int = 0
    video_ids_existing: int = 0
    video_ids_added: int = 0
    video_ids_queue_known: int = 0
    video_ids_failed: int = 0

    def report(self, outcome: str) -> None:
        FULL_SCRAPES.labels(outcome=outcome).inc()
        counts: dict[str, int] = {
            'found': self.video_ids_found,
            'existing': self.video_ids_existing,
            'added': self.video_ids_added,
            'queue_known': self.video_ids_queue_known,
            'failed': self.video_ids_failed,
        }
        name: str
        count: int
        for name, count in counts.items():
            VIDEO_IDS.labels(outcome=name).inc(count)
        ADDED_PER_SCRAPE.observe(self.video_ids_added)
        _LOGGER.info(
            f'Full channel scrape {self.channel_id} {outcome}: '
            f'added {self.video_ids_added} video IDs to the queue',
            extra={
                'channel_id': self.channel_id,
                'outcome': outcome,
                **{f'video_ids_{name}': count
                   for name, count in counts.items()},
            },
        )


class FilterQueryError(RuntimeError):
    '''The /api/v1/filter query failed after all retries.'''


async def fetch_exchange_video_ids(
    http_client: httpx.AsyncClient,
    exchange_url: str,
    channel_id: str,
) -> set[str]:
    '''Return the platform_content_ids the exchange already holds
    for *channel_id*, via paginated ``POST /api/v1/filter``.

    One filter query per 1000-record page replaces the old one-GET-
    per-video existence sweep. Retries transient failures (timeouts,
    transport errors, 502/503/504) with backoff; other non-200
    statuses and exhausted retries raise :class:`FilterQueryError`
    so the caller keeps the refresh due.

    Caveat: records uploaded without a ``platform_creator_id`` do
    not match the filter and are treated as missing — harmless,
    since the video consumer rechecks uploaded membership before
    scraping and the uploaded set prevents re-upload.
    '''
    url: str = f'{exchange_url.rstrip("/")}/api/v1/filter'
    base_body: dict[str, Any] = {
        'platform': 'youtube',
        'entity': 'video',
        'platform_creator_id': channel_id,
        'first': FILTER_PAGE_SIZE,
    }
    existing: set[str] = set()
    attempt: int
    for attempt in range(1, DELIVER_ATTEMPTS + 1):
        after: str | None = None
        while True:
            body: dict[str, Any] = dict(base_body)
            if after is not None:
                body['after'] = after
            try:
                response: httpx.Response | None = (
                    await http_client.post(url, json=body)
                )
            except (httpx.TimeoutException, httpx.TransportError):
                response = None
            if response is not None:
                if response.status_code == 200:
                    data: dict[str, Any] = response.json()
                    edge: dict[str, Any]
                    for edge in data.get('edges', []):
                        node: dict[str, Any] = edge.get('node') or {}
                        content_id: str | None = (
                            node.get('platform_content_id')
                        )
                        if content_id:
                            existing.add(content_id)
                    page_info: dict[str, Any] = (
                        data.get('page_info') or {}
                    )
                    if not page_info.get('has_next_page'):
                        return existing
                    after = page_info.get('end_cursor')
                    if not after:
                        return existing
                    continue
                if response.status_code not in RETRYABLE_STATUSES:
                    raise FilterQueryError(
                        f'Video filter query returned '
                        f'{response.status_code} for {channel_id}: '
                        f'{response.text[:200]}',
                    )
            # Transient: fall through to the retry backoff.
            if attempt < DELIVER_ATTEMPTS:
                await asyncio.sleep(
                    DELIVER_BACKOFF_SECONDS[
                        min(attempt - 1, len(DELIVER_BACKOFF_SECONDS) - 1)
                    ],
                )
            break
    raise FilterQueryError(
        f'Video filter query failed after {DELIVER_ATTEMPTS} '
        f'attempts for {channel_id}',
    )


async def queue_channel_videos(
    channel: YouTubeChannel,
    *,
    redis: aioredis.Redis,
    http_client: httpx.AsyncClient,
    exchange_url: str,
    video_fm: AssetFileManagement,
    summary: FullScrapeSummary,
) -> None:
    '''Queue the channel's video IDs the exchange does not have.

    The exchange's per-channel video inventory is fetched with ONE
    paginated ``POST /api/v1/filter`` (filter:
    ``platform_creator_id=<channel_id>``) instead of one existence
    GET per video, so a full scrape costs a couple of API requests
    regardless of channel size. Locally-known IDs (uploaded set,
    scrape output files) are filtered first and never hit the API.

    Normal enqueue preserves existing queue entries and tombstones,
    so IDs that are neither on the exchange nor locally known are
    deduped by the queue itself; the video consumer rechecks
    uploaded membership to cover races with another host's upload.
    A filter-query failure raises and keeps the refresh due.
    '''
    uploaded: UploadedVideoIds = UploadedVideoIds('', redis_client=redis)
    queue: RedisVideoScrapeQueue = RedisVideoScrapeQueue(
        redis, VideoScrapeQueueSettings(),
    )
    if not channel.channel_id:
        raise ValueError(
            'queue_channel_videos requires a channel with a channel_id',
        )
    ids: list[str] = sorted(channel.video_ids)
    summary.video_ids_found = len(ids)

    # Local filtering: the uploaded set and scrape output files
    # already answer "handled" for these IDs without any API call.
    known: set[str] = set()
    start: int
    for start in range(0, len(ids), BATCH_SIZE):
        batch: list[str] = ids[start:start + BATCH_SIZE]
        flags: dict[str, bool] = await uploaded.contains_many(batch)
        video_id: str
        for video_id in batch:
            if flags.get(video_id) or (
                video_fm.video_scrape_output_exists(video_id)
            ):
                known.add(video_id)
    summary.video_ids_existing = len(known)
    candidates: list[str] = [
        video_id for video_id in ids if video_id not in known
    ]
    if not candidates:
        return

    exchange_ids: set[str] = await fetch_exchange_video_ids(
        http_client, exchange_url, channel.channel_id,
    )

    for video_id in candidates:
        if video_id in exchange_ids:
            summary.video_ids_existing += 1
            continue
        added: bool = await queue.enqueue(
            video_id, source='channel',
            channel_id=channel.channel_id,
            channel_handle=channel.channel_handle,
        )
        if added:
            summary.video_ids_added += 1
        else:
            summary.video_ids_queue_known += 1