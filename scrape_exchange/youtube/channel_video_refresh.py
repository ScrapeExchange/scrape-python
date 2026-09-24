'''Queue missing videos from complete channel enumerations.'''

import asyncio
import logging
from dataclasses import dataclass

import httpx
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
DELIVER_CONCURRENCY: int = 10
# Process-wide cap on concurrent exchange existence GETs. Without this,
# worker_count scrapes × DELIVER_CONCURRENCY each oversubscribe the
# shared httpx client's default 100-connection pool and every surplus
# request dies with httpx.PoolTimeout after the 10s pool deadline.
EXISTENCE_CONCURRENCY: int = 64
DELIVER_ATTEMPTS: int = 3
DELIVER_BACKOFF_SECONDS: tuple[float, ...] = (0.5, 1.0, 2.0)
# Transient upstream statuses worth retrying.
RETRYABLE_STATUSES: frozenset[int] = frozenset({502, 503, 504})
# A batch only aborts the scrape when failures look systemic: at least
# this many failures AND more than half the batch's candidates failed.
# Isolated timeouts are logged and tolerated so a single 10s timeout
# does not discard an otherwise successful full channel scrape.
SYSTEMIC_FAILURE_MINIMUM: int = 5

_EXISTENCE_GATES: dict[tuple[int, int], asyncio.Semaphore] = {}


def _get_existence_gate(
    limit: int = EXISTENCE_CONCURRENCY,
) -> asyncio.Semaphore:
    '''Return the per-event-loop gate for exchange existence GETs.'''

    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
    key: tuple[int, int] = (id(loop), limit)
    gate: asyncio.Semaphore | None = _EXISTENCE_GATES.get(key)
    if gate is None:
        gate = asyncio.Semaphore(limit)
        _EXISTENCE_GATES[key] = gate
    return gate


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


async def queue_channel_videos(
    channel: YouTubeChannel,
    *,
    redis: aioredis.Redis,
    http_client: httpx.AsyncClient,
    exchange_url: str,
    video_fm: AssetFileManagement,
    summary: FullScrapeSummary,
) -> None:
    '''Deliver missing IDs in bounded batches; systemic errors keep
    refresh due.

    Normal enqueue preserves existing queue entries and tombstones. The
    video consumer also rechecks uploaded membership to cover races with
    another host's upload after this producer's existence checks.

    Transient failures (httpx timeouts/transport errors, 502/503/504)
    are retried with backoff. Isolated failures are tolerated so a
    single 10s timeout does not discard an otherwise successful scrape;
    only systemic batch failures (most candidates failing) re-raise and
    keep the refresh due.
    '''
    uploaded: UploadedVideoIds = UploadedVideoIds('', redis_client=redis)
    queue: RedisVideoScrapeQueue = RedisVideoScrapeQueue(
        redis, VideoScrapeQueueSettings(),
    )
    ids: list[str] = sorted(channel.video_ids)
    summary.video_ids_found = len(ids)
    gate: asyncio.Semaphore = asyncio.Semaphore(DELIVER_CONCURRENCY)
    existence_gate: asyncio.Semaphore = _get_existence_gate()
    exchange_prefix: str = exchange_url.rstrip('/')

    async def deliver(video_id: str) -> None:
        try:
            url: str = (
                f'{exchange_prefix}/api/v1/data/content/youtube/video/'
                f'{video_id}'
            )
            attempt: int
            for attempt in range(1, DELIVER_ATTEMPTS + 1):
                try:
                    async with gate:
                        async with existence_gate:
                            response: httpx.Response = (
                                await http_client.get(url)
                            )
                    if response.status_code == 200:
                        summary.video_ids_existing += 1
                        return
                    if response.status_code == 404:
                        added: bool = await queue.enqueue(
                            video_id, source='channel',
                            channel_id=channel.channel_id,
                            channel_handle=channel.channel_handle,
                        )
                        if added:
                            summary.video_ids_added += 1
                        else:
                            summary.video_ids_queue_known += 1
                        return
                    if response.status_code not in RETRYABLE_STATUSES:
                        raise RuntimeError(
                            f'Video existence check returned '
                            f'{response.status_code} for {video_id}',
                        )
                except (httpx.TimeoutException, httpx.TransportError):
                    pass
                if attempt < DELIVER_ATTEMPTS:
                    await asyncio.sleep(
                        DELIVER_BACKOFF_SECONDS[
                            min(attempt - 1, len(DELIVER_BACKOFF_SECONDS) - 1)
                        ],
                    )
            raise RuntimeError(
                f'Video existence check failed after {DELIVER_ATTEMPTS} '
                f'attempts for {video_id}',
            )
        except Exception:
            summary.video_ids_failed += 1
            raise

    start: int
    video_id: str
    result: None | BaseException
    for start in range(0, len(ids), BATCH_SIZE):
        batch: list[str] = ids[start:start + BATCH_SIZE]
        known: dict[str, bool] = await uploaded.contains_many(batch)
        candidates: list[str] = []
        for video_id in batch:
            if known[video_id] or video_fm.video_scrape_output_exists(
                video_id,
            ):
                summary.video_ids_existing += 1
            else:
                candidates.append(video_id)
        results: list[None | BaseException] = await asyncio.gather(
            *(deliver(video_id) for video_id in candidates),
            return_exceptions=True,
        )
        failures: list[BaseException] = [
            result for result in results
            if isinstance(result, BaseException)
        ]
        if not failures:
            continue
        if (len(failures) >= SYSTEMIC_FAILURE_MINIMUM
                and len(failures) * 2 > len(candidates)):
            raise failures[0]
        _LOGGER.warning(
            f'{len(failures)} of {len(candidates)} video existence '
            f'checks failed for {channel.channel_id} after retries; '
            'skipping those IDs until the next full scrape',
            extra={
                'channel_id': channel.channel_id,
                'video_ids_failed': len(failures),
            },
        )