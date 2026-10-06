'''One-off migration of the Redis video scrape queue into the MongoDB
backlog (see :mod:`scrape_exchange.video_backlog`).

Run the steps in this order with every producer and video scraper
stopped:

1. ``terminal``: copy the Redis ``unavailable`` / ``failed`` /
   ``removed`` tombstone hashes into backlog documents; optionally
   delete the hashes afterwards.
2. ``backlog``: move every queued video beyond the oldest *keep* from
   Redis into MongoDB as ``queued``, deleting it from Redis as it
   goes so Redis memory falls during the run.
3. ``hot``: record the videos left in Redis (the hot window) as
   ``hot`` backlog documents.

Every step is idempotent: re-running skips work already done.
'''

import asyncio
import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import redis.asyncio as aioredis

from scrape_exchange.video_backlog import (
    STATE_HOT,
    STATE_QUEUED,
    TERMINAL_STATES,
    MongoVideoBacklog,
    backlog_document,
)
from scrape_exchange.video_scrape_queue import qmeta_bucket, unpack_qmeta


@dataclass
class TerminalStats:
    '''Counts; in dry-run mode they describe proposed changes.'''

    scanned: int = 0
    inserted: int = 0
    invalid: int = 0
    hashes_deleted: int = 0


@dataclass
class OffloadStats:
    '''Counts; in dry-run mode they describe proposed changes.'''

    scanned: int = 0
    inserted: int = 0
    removed_from_redis: int = 0


@dataclass
class HotStats:
    '''Counts; in dry-run mode they describe proposed changes.'''

    scanned: int = 0
    inserted: int = 0


def _context(packed: str | None) -> dict[str, Any]:
    '''Backlog document fields from a packed qmeta entry.'''
    if not packed:
        return {}
    meta: dict[str, str] = unpack_qmeta(packed)
    verified: str | None = meta.get('channel_is_verified')
    return {
        'source': meta.get('source'),
        'channel_id': meta.get('channel_id'),
        'channel_handle': meta.get('channel_handle'),
        'channel_is_verified': (
            None if verified is None else verified == '1'
        ),
    }


def _parse_verified(value: Any) -> bool | None:
    if value is None or value == '':
        return None
    if isinstance(value, bool):
        return value
    return str(value) == '1'


def terminal_document(
    video_id: str, state: str, raw: str,
) -> dict[str, Any] | None:
    '''Backlog document for a Redis tombstone record; None when the
    record is not a JSON object.'''
    try:
        record: Any = json.loads(raw)
    except (TypeError, ValueError):
        return None
    if not isinstance(record, dict):
        return None
    ts: Any = record.get('ts')
    doc: dict[str, Any] = backlog_document(
        video_id, state=state,
        enqueued_at=ts if isinstance(ts, int) and ts > 0 else time.time(),
        source=record.get('source'),
        channel_id=record.get('channel_id'),
        channel_handle=record.get('channel_handle'),
        channel_is_verified=_parse_verified(
            record.get('channel_is_verified'),
        ),
    )
    doc['record'] = {
        key: record.get(key) for key in ('ts', 'last_error', 'note')
        if record.get(key) is not None
    }
    return doc


async def migrate_terminal(
    redis: aioredis.Redis,
    backlog: MongoVideoBacklog,
    *,
    platform: str = 'youtube',
    apply: bool = False,
    delete_legacy: bool = False,
    batch_size: int = 5000,
    limit: int = 0,
    progress: Callable[[TerminalStats], None] | None = None,
) -> TerminalStats:
    '''Copy the Redis terminal hashes into the backlog. A hash is
    only deleted after a complete, unlimited copy.'''
    stats: TerminalStats = TerminalStats()
    state: str
    for state in TERMINAL_STATES:
        key: str = f'{platform}:video:{state}'
        cursor: int = 0
        while True:
            records: dict[str, str]
            cursor, records = await redis.hscan(
                key, cursor=cursor, count=batch_size,
            )
            items: list[tuple[str, str]] = list(records.items())
            if limit:
                items = items[:max(limit - stats.scanned, 0)]
            docs: list[dict[str, Any]] = []
            video_id: str
            raw: str
            for video_id, raw in items:
                stats.scanned += 1
                doc: dict[str, Any] | None = terminal_document(
                    video_id, state, raw,
                )
                if doc is None:
                    stats.invalid += 1
                else:
                    docs.append(doc)
            if apply:
                stats.inserted += await backlog.add_many(docs)
            else:
                stats.inserted += len(docs)
            if progress is not None:
                progress(stats)
            if cursor == 0 or (limit and stats.scanned >= limit):
                break
        if limit and stats.scanned >= limit:
            return stats
        if apply and delete_legacy and cursor == 0:
            stats.hashes_deleted += int(await redis.unlink(key))
    return stats


async def offload_backlog(
    redis: aioredis.Redis,
    backlog: MongoVideoBacklog,
    *,
    platform: str = 'youtube',
    keep: int = 20_000_000,
    apply: bool = False,
    batch_size: int = 5000,
    pause_seconds: float = 0.0,
    limit: int = 0,
    progress: Callable[[OffloadStats], None] | None = None,
) -> OffloadStats:
    '''Move the newest queued videos beyond the oldest *keep* from
    Redis into MongoDB, newest first, deleting each batch from Redis
    once MongoDB has it. A dry run reads only the queue size and
    reports how many videos would move.'''
    queue_key: str = f'{platform}:video:queue'
    prefix: str = f'{platform}:video:qmeta'
    stats: OffloadStats = OffloadStats()
    excess: int = max(int(await redis.zcard(queue_key)) - keep, 0)
    if limit:
        excess = min(excess, limit)
    if not apply:
        stats.scanned = stats.inserted = stats.removed_from_redis = excess
        return stats
    while stats.scanned < excess:
        size: int = min(batch_size, excess - stats.scanned)
        members: list[tuple[str, float]] = await redis.zrange(
            queue_key, -size, -1, withscores=True,
        )
        if not members:
            break
        pipe: aioredis.client.Pipeline = redis.pipeline(transaction=False)
        video_id: str
        for video_id, _ in members:
            pipe.hget(f'{prefix}:{qmeta_bucket(video_id)}', video_id)
        packed: list[str | None] = await pipe.execute()
        docs: list[dict[str, Any]] = [
            backlog_document(
                video_id, state=STATE_QUEUED, enqueued_at=score,
                **_context(entry),
            )
            for (video_id, score), entry in zip(members, packed)
        ]
        stats.inserted += await backlog.add_many(docs)
        ids: list[str] = [video_id for video_id, _ in members]
        # A premature 'hot' step may have recorded these as hot.
        await backlog.set_state(
            ids, from_state=STATE_HOT, to_state=STATE_QUEUED,
        )
        pipe = redis.pipeline(transaction=False)
        pipe.zrem(queue_key, *ids)
        for video_id in ids:
            pipe.hdel(f'{prefix}:{qmeta_bucket(video_id)}', video_id)
        await pipe.execute()
        stats.scanned += len(members)
        stats.removed_from_redis += len(members)
        if progress is not None:
            progress(stats)
        if pause_seconds:
            await asyncio.sleep(pause_seconds)
    return stats


async def record_hot_window(
    redis: aioredis.Redis,
    backlog: MongoVideoBacklog,
    *,
    platform: str = 'youtube',
    apply: bool = False,
    batch_size: int = 5000,
    limit: int = 0,
    progress: Callable[[HotStats], None] | None = None,
) -> HotStats:
    '''Create ``hot`` backlog documents for the videos still in the
    Redis queue; existing documents are left alone.'''
    queue_key: str = f'{platform}:video:queue'
    prefix: str = f'{platform}:video:qmeta'
    stats: HotStats = HotStats()
    total: int = int(await redis.zcard(queue_key))
    if limit:
        total = min(total, limit)
    start: int = 0
    while start < total:
        stop: int = min(start + batch_size, total) - 1
        members: list[tuple[str, float]] = await redis.zrange(
            queue_key, start, stop, withscores=True,
        )
        if not members:
            break
        pipe: aioredis.client.Pipeline = redis.pipeline(transaction=False)
        video_id: str
        for video_id, _ in members:
            pipe.hget(f'{prefix}:{qmeta_bucket(video_id)}', video_id)
        packed: list[str | None] = await pipe.execute()
        docs: list[dict[str, Any]] = [
            backlog_document(
                video_id, state=STATE_HOT, enqueued_at=score,
                **_context(entry),
            )
            for (video_id, score), entry in zip(members, packed)
        ]
        stats.scanned += len(members)
        if apply:
            stats.inserted += await backlog.add_many(docs)
        else:
            stats.inserted += len(docs)
        if progress is not None:
            progress(stats)
        start += len(members)
    return stats
