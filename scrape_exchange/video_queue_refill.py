'''Keep the Redis hot window of the video scrape queue filled from the
MongoDB backlog.

The video scrapers pop from the Redis queue. When it holds fewer than
``low_watermark`` videos, :meth:`VideoQueueRefill.refill_once` copies
the oldest queued backlog documents into Redis until it holds
``high_watermark``. Each batch is written to Redis first and only then
marked ``hot`` in MongoDB, so a crash in between at worst copies a
video twice (both writes are idempotent) and never loses one.

:meth:`VideoQueueRefill.reconcile` returns ``hot`` documents that are
missing from Redis (for example after a Redis restart lost the most
recent writes) to the backlog.
'''

import logging
from dataclasses import dataclass
from typing import Any

import redis.asyncio as aioredis
from prometheus_client import Counter, Gauge

from scrape_exchange.video_backlog import (
    STATE_HOT,
    STATE_QUEUED,
    MongoVideoBacklog,
)
from scrape_exchange.video_scrape_queue import pack_qmeta, qmeta_bucket

_LOGGER: logging.Logger = logging.getLogger(__name__)

METRIC_MOVED: Counter = Counter(
    'video_queue_refill_moved_total',
    'Videos copied from the MongoDB backlog into the Redis hot window',
    ['platform'],
)
METRIC_RECONCILED: Counter = Counter(
    'video_queue_refill_reconciled_total',
    'Hot backlog documents missing from Redis returned to the backlog',
    ['platform'],
)
METRIC_ERRORS: Counter = Counter(
    'video_queue_refill_errors_total',
    'Refill iterations that failed',
    ['platform'],
)
METRIC_HOT_SIZE: Gauge = Gauge(
    'video_queue_hot_size',
    'Videos waiting in the Redis hot window',
    ['platform'],
)
METRIC_BACKLOG_SIZE: Gauge = Gauge(
    'video_queue_backlog_size',
    'Known videos per backlog state (queued includes hot)',
    ['platform', 'state'],
)


@dataclass
class ReconcileStats:
    '''Counts from one :meth:`VideoQueueRefill.reconcile` pass.'''

    checked: int = 0
    requeued: int = 0


class VideoQueueRefill:
    '''Moves backlog documents into the Redis hot window.

    :param redis: Redis client holding the video queue
    :param backlog: the platform's MongoDB backlog
    :param platform: queue namespace (``youtube`` / ``tiktok``)
    :param low_watermark: refill when the hot window drops below this
    :param high_watermark: refill up to this many waiting videos
    :param batch_size: documents per MongoDB read / Redis pipeline
    '''

    def __init__(
        self,
        redis: aioredis.Redis,
        backlog: MongoVideoBacklog,
        *,
        platform: str = 'youtube',
        low_watermark: int = 10_000_000,
        high_watermark: int = 20_000_000,
        batch_size: int = 10_000,
    ) -> None:
        if not 0 < low_watermark <= high_watermark:
            raise ValueError('need 0 < low_watermark <= high_watermark')
        if batch_size < 1:
            raise ValueError('batch_size must be positive')
        self._redis: aioredis.Redis = redis
        self._backlog: MongoVideoBacklog = backlog
        self._platform: str = platform
        self._prefix: str = f'{platform}:video'
        self._low: int = low_watermark
        self._high: int = high_watermark
        self._batch: int = batch_size

    async def hot_size(self) -> int:
        '''Videos waiting in the Redis queue.'''
        return int(await self._redis.zcard(f'{self._prefix}:queue'))

    async def refill_once(self) -> int:
        '''Top the hot window up to the high watermark when it is
        below the low watermark.

        :returns: number of videos copied into Redis.
        '''
        size: int = await self.hot_size()
        METRIC_HOT_SIZE.labels(platform=self._platform).set(size)
        if size >= self._low:
            return 0
        wanted: int = self._high - size
        moved: int = 0
        while moved < wanted:
            docs: list[dict[str, Any]] = await self._backlog.oldest_queued(
                min(self._batch, wanted - moved),
            )
            if not docs:
                break
            await self._copy_to_redis(docs)
            ids: list[str] = [doc['_id'] for doc in docs]
            await self._backlog.set_state(
                ids, from_state=STATE_QUEUED, to_state=STATE_HOT,
            )
            moved += len(docs)
            METRIC_MOVED.labels(platform=self._platform).inc(len(docs))
        if moved:
            _LOGGER.info(
                'Refilled video queue hot window',
                extra={
                    'platform': self._platform, 'moved': moved,
                    'hot_size_before': size,
                },
            )
        return moved

    async def _copy_to_redis(self, docs: list[dict[str, Any]]) -> None:
        pipe: aioredis.client.Pipeline = self._redis.pipeline(
            transaction=False,
        )
        doc: dict[str, Any]
        for doc in docs:
            video_id: str = doc['_id']
            pipe.zadd(
                f'{self._prefix}:queue',
                {video_id: float(doc.get('enqueued_at') or 0)},
                nx=True,
            )
            pipe.hsetnx(
                f'{self._prefix}:qmeta:{qmeta_bucket(video_id)}',
                video_id,
                pack_qmeta(
                    source=doc.get('source') or '',
                    channel_id=doc.get('channel_id'),
                    channel_handle=doc.get('channel_handle'),
                    channel_is_verified=doc.get('channel_is_verified'),
                ),
            )
        await pipe.execute()

    async def reconcile(self, *, page_size: int = 10_000) -> ReconcileStats:
        '''Return hot documents whose Redis entry is gone to the
        backlog, so the refill copies them again.'''
        stats: ReconcileStats = ReconcileStats()
        after: str = ''
        while True:
            ids: list[str] = await self._backlog.hot_page(after, page_size)
            if not ids:
                break
            after = ids[-1]
            pipe: aioredis.client.Pipeline = self._redis.pipeline(
                transaction=False,
            )
            video_id: str
            for video_id in ids:
                pipe.hexists(
                    f'{self._prefix}:qmeta:{qmeta_bucket(video_id)}',
                    video_id,
                )
            present: list[bool] = await pipe.execute()
            missing: list[str] = [
                video_id for video_id, ok in zip(ids, present) if not ok
            ]
            stats.checked += len(ids)
            if missing:
                stats.requeued += await self._backlog.set_state(
                    missing, from_state=STATE_HOT, to_state=STATE_QUEUED,
                )
        METRIC_RECONCILED.labels(platform=self._platform).inc(
            stats.requeued,
        )
        return stats

    async def publish_backlog_sizes(self) -> None:
        '''Refresh the backlog-size gauges (cached counts).'''
        counts: dict[str, int] = await self._backlog.counts()
        state: str
        count: int
        for state, count in counts.items():
            METRIC_BACKLOG_SIZE.labels(
                platform=self._platform, state=state,
            ).set(count)
