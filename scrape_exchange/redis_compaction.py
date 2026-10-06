'''One-off migration of the scrapers' largest Redis structures to
their compact formats.

- ``video-meta``: convert legacy per-video ``<platform>:video:meta:<id>``
  hashes of queued videos into packed fields in the
  ``<platform>:video:qmeta:<bucket>`` hashes. Fields that the compact
  format does not hold (force, attempts, last_error, ...) stay in the
  now-sparse meta hash; a hash left empty disappears.
- ``uploaded`` / ``exchange-channels``: copy the legacy SETs into the
  Bloom filters that replace them, optionally deleting the SET.

Run with every scraper and uploader stopped: the migration does not
guard against concurrent writers.
'''

import asyncio
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass

import redis.asyncio as aioredis

from scrape_exchange.video_scrape_queue import (
    VideoState,
    pack_qmeta,
    qmeta_bucket,
)

LEGACY_UPLOADED_KEY: str = 'youtube:video:uploaded'
LEGACY_EXCHANGE_CHANNELS_KEY: str = 'youtube:exchange_channels'

# Meta-hash fields the packed queue entry replaces (or drops:
# state and created_at are implied by bucket and queue score,
# channel_url is derivable).
_PACKED_FIELDS: tuple[str, ...] = (
    'source', 'created_at', 'state', 'channel_id',
    'channel_handle', 'channel_url', 'channel_is_verified',
)


@dataclass
class VideoMetaStats:
    '''Counts; in dry-run mode they describe proposed changes.'''

    scanned: int = 0
    migrated: int = 0
    stranded: int = 0
    requeued: int = 0
    meta_deleted: int = 0
    meta_kept: int = 0
    skipped: int = 0


@dataclass
class SetCopyStats:
    '''Counts; in dry-run mode they describe proposed changes.'''

    scanned: int = 0
    copied: int = 0
    legacy_deleted: bool = False


async def migrate_video_meta(
    redis: aioredis.Redis,
    *,
    platform: str = 'youtube',
    apply: bool = False,
    requeue_stranded: bool = False,
    batch_size: int = 1000,
    pause_seconds: float = 0.0,
    limit: int = 0,
    progress: Callable[[VideoMetaStats], None] | None = None,
) -> VideoMetaStats:
    '''Convert queued videos' meta hashes into bucket entries.

    A queued video missing from the queue sorted set was popped by a
    scraper that died mid-scrape; both layouts treat it as known and
    never scrape it. It is counted as ``stranded`` and, with
    *requeue_stranded*, put back with its original ``created_at``
    as score. Meta hashes with a terminal or no state are left
    untouched.
    '''
    prefix: str = f'{platform}:video'
    stats: VideoMetaStats = VideoMetaStats()
    cursor: int = 0
    while True:
        keys: list[str]
        cursor, keys = await redis.scan(
            cursor=cursor, match=f'{prefix}:meta:*', count=batch_size,
        )
        if limit:
            keys = keys[:max(limit - stats.scanned, 0)]
        if keys:
            await _migrate_meta_batch(
                redis, prefix, keys, stats, apply, requeue_stranded,
            )
            if progress is not None:
                progress(stats)
            if pause_seconds:
                await asyncio.sleep(pause_seconds)
        if cursor == 0 or (limit and stats.scanned >= limit):
            break
    return stats


async def _migrate_meta_batch(
    redis: aioredis.Redis,
    prefix: str,
    keys: list[str],
    stats: VideoMetaStats,
    apply: bool,
    requeue_stranded: bool,
) -> None:
    read: aioredis.client.Pipeline = redis.pipeline(transaction=False)
    key: str
    for key in keys:
        read.hgetall(key)
        read.zscore(f'{prefix}:queue', key.split(':meta:', 1)[1])
    raw: list = await read.execute()
    write: aioredis.client.Pipeline = redis.pipeline(transaction=False)
    i: int
    for i, key in enumerate(keys):
        stats.scanned += 1
        meta: dict[str, str] = raw[2 * i]
        score: float | None = raw[2 * i + 1]
        if meta.get('state') != VideoState.QUEUED.value:
            stats.skipped += 1
            continue
        video_id: str = key.split(':meta:', 1)[1]
        stats.migrated += 1
        write.hset(
            f'{prefix}:qmeta:{qmeta_bucket(video_id)}', video_id,
            pack_qmeta(
                source=meta.get('source') or '',
                channel_id=meta.get('channel_id'),
                channel_handle=meta.get('channel_handle'),
                channel_is_verified=_parse_bool(
                    meta.get('channel_is_verified'),
                ),
            ),
        )
        if score is None:
            stats.stranded += 1
        if score is None and requeue_stranded:
            stats.requeued += 1
            write.zadd(
                f'{prefix}:queue',
                {video_id: _created_at(meta.get('created_at'))},
                nx=True,
            )
        if set(meta) <= set(_PACKED_FIELDS):
            stats.meta_deleted += 1
            write.delete(key)
        else:
            stats.meta_kept += 1
            write.hdel(key, *_PACKED_FIELDS)
    if apply and len(write):
        await write.execute()


def _parse_bool(value: str | None) -> bool | None:
    if value is None or value == '':
        return None
    return value == '1'


def _created_at(value: str | None) -> float:
    try:
        return float(value) if value else 0.0
    except ValueError:
        return 0.0


async def copy_set_to_bloom(
    redis: aioredis.Redis,
    *,
    legacy_key: str,
    add_many: Callable[[Iterable[str]], Awaitable[None]],
    apply: bool = False,
    delete_legacy: bool = False,
    batch_size: int = 10000,
    pause_seconds: float = 0.0,
    limit: int = 0,
    progress: Callable[[SetCopyStats], None] | None = None,
) -> SetCopyStats:
    '''Copy every member of *legacy_key* into a Bloom filter via
    *add_many* (idempotent), then optionally ``UNLINK`` the SET.
    The SET is only deleted after a complete, unlimited copy.'''
    stats: SetCopyStats = SetCopyStats()
    cursor: int = 0
    while True:
        members: list[str]
        cursor, members = await redis.sscan(
            legacy_key, cursor=cursor, count=batch_size,
        )
        if limit:
            members = members[:max(limit - stats.scanned, 0)]
        stats.scanned += len(members)
        if members:
            if apply:
                await add_many(members)
            stats.copied += len(members)
            if progress is not None:
                progress(stats)
            if pause_seconds:
                await asyncio.sleep(pause_seconds)
        if cursor == 0 or (limit and stats.scanned >= limit):
            break
    complete: bool = cursor == 0 and not limit
    if apply and delete_legacy and complete:
        await redis.unlink(legacy_key)
        stats.legacy_deleted = True
    return stats

