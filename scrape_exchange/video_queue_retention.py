'''Bound diagnostic retention without forgetting terminal video IDs.

Run after every producer has the tombstone-aware video queue implementation.
Use HSCAN and short, conditional Lua updates instead of a Redis-wide script.
'''

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any

import redis.asyncio as aioredis

from scrape_exchange.video_scrape_queue import TERMINAL_META_TTL_SECONDS

_STATES: tuple[str, ...] = ('unavailable', 'failed', 'removed')

_RETAIN_LUA: str = '''
-- KEYS: terminal hash, metadata, queue
-- ARGV: id, expected JSON, state, compact JSON, deadline, now, apply
-- Return: matched, records compacted, metadata deleted, TTLs added.
local vid = ARGV[1]
if redis.call('HGET', KEYS[1], vid) ~= ARGV[2] then
    return {0, 0, 0, 0}
end
if redis.call('ZSCORE', KEYS[3], vid) then return {0, 0, 0, 0} end
local exists = redis.call('EXISTS', KEYS[2])
local state = redis.call('HGET', KEYS[2], 'state')
if exists == 1 and state ~= ARGV[3] then return {0, 0, 0, 0} end
local compact, deleted, ttl_added = 0, 0, 0
if tonumber(ARGV[5]) <= tonumber(ARGV[6]) then
    if ARGV[2] ~= ARGV[4] then compact = 1 end
    deleted = exists
    if ARGV[7] == '1' then
        if compact == 1 then redis.call('HSET', KEYS[1], vid, ARGV[4]) end
        if deleted == 1 then redis.call('DEL', KEYS[2]) end
    end
elseif exists == 1 and redis.call('TTL', KEYS[2]) == -1 then
    ttl_added = 1
    if ARGV[7] == '1' then redis.call('EXPIREAT', KEYS[2], ARGV[5]) end
end
return {1, compact, deleted, ttl_added}
'''


@dataclass
class RetentionStats:
    '''In dry-run mode, action counts describe proposed changes.'''

    scanned: int = 0
    invalid: int = 0
    skipped: int = 0
    records_compacted: int = 0
    metadata_deleted: int = 0
    ttls_added: int = 0


class VideoQueueRetention:
    def __init__(
        self, redis: aioredis.Redis, *, platform: str = 'youtube',
    ) -> None:
        if platform not in ('youtube', 'tiktok'):
            raise ValueError('platform must be youtube or tiktok')
        self._redis: aioredis.Redis = redis
        self._prefix: str = f'{platform}:video'

    async def process_record(
        self, state: str, video_id: str, raw: str, *,
        now: int, apply: bool = False,
    ) -> list[int] | None:
        '''Compare and compact one snapshot; None means invalid data.

        Missing, malformed and future timestamps are deliberately left alone.
        A concurrent mark/unmark/force operation invalidates the snapshot.
        '''
        if state not in _STATES:
            raise ValueError('state must be terminal')
        record: Any
        try:
            record = json.loads(raw)
        except (TypeError, ValueError):
            return None
        if not isinstance(record, dict):
            return None
        timestamp: Any = record.get('ts')
        if type(timestamp) is not int or not 0 < timestamp <= now:
            return None
        compact: str = json.dumps({'ts': timestamp}, separators=(',', ':'))
        result: list[int] = await self._redis.eval(
            _RETAIN_LUA, 3,
            f'{self._prefix}:{state}',
            f'{self._prefix}:meta:{video_id}',
            f'{self._prefix}:queue',
            video_id, raw, state, compact,
            str(timestamp + TERMINAL_META_TTL_SECONDS), str(now),
            '1' if apply else '0',
        )
        return result

    async def run(
        self, *, apply: bool = False, batch_size: int = 200,
        pause_seconds: float = 0.05, limit: int = 0,
        now: int | None = None,
    ) -> RetentionStats:
        '''Scan terminal hashes with bounded writes and an optional limit.

        HSCAN's count is a hint; pause after each batch_size records even
        when Redis returns a larger page. Counts can include duplicates
        if the hashes change during the scan; updates are idempotent.
        '''
        if batch_size < 1 or pause_seconds < 0 or limit < 0:
            raise ValueError('invalid scan limits')
        current_time: int = int(time.time()) if now is None else now
        stats: RetentionStats = RetentionStats()
        state: str
        for state in _STATES:
            cursor: int = 0
            while True:
                records: dict[str, str]
                cursor, records = await self._redis.hscan(
                    f'{self._prefix}:{state}', cursor=cursor,
                    count=batch_size,
                )
                video_id: str
                raw: str
                for video_id, raw in records.items():
                    stats.scanned += 1
                    result: list[int] | None = await self.process_record(
                        state, video_id, raw, now=current_time, apply=apply,
                    )
                    if result is None:
                        stats.invalid += 1
                    elif result[0] == 0:
                        stats.skipped += 1
                    else:
                        stats.records_compacted += result[1]
                        stats.metadata_deleted += result[2]
                        stats.ttls_added += result[3]
                    if limit and stats.scanned >= limit:
                        return stats
                    if stats.scanned % batch_size == 0:
                        await asyncio.sleep(pause_seconds)
                if cursor == 0:
                    break
        return stats
