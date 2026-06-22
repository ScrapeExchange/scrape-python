'''Redis-backed work queue for the video scraper.

Implements the VideoScrapeQueue(ABC) interface described in
docs/superpowers/specs/2026-05-18-video-scrape-queue-design.md.
v1 ships RedisVideoScrapeQueue only; a FileVideoScrapeQueue is
planned for v2.
'''

import enum
import json
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, AsyncIterator

import redis.asyncio as aioredis
from pydantic import Field
from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
)


KEY_PREFIX: str = 'youtube:video'

_MARK_LUA: str = '''
-- KEYS[1] = meta hash
-- KEYS[2] = target state hash
-- KEYS[3] = queue key
-- KEYS[4] = unavailable hash
-- KEYS[5] = failed hash
-- KEYS[6] = removed hash
-- ARGV[1] = video_id
-- ARGV[2] = state value
-- ARGV[3] = record JSON
local vid = ARGV[1]
local state = ARGV[2]
local record = ARGV[3]
redis.call('ZREM', KEYS[3], vid)
for i = 4, 6 do
    redis.call('HDEL', KEYS[i], vid)
end
redis.call('HSET', KEYS[2], vid, record)
redis.call('HSET', KEYS[1], 'state', state)
return 1
'''

_UNMARK_LUA: str = '''
-- KEYS[1] = meta hash
-- KEYS[2] = queue key
-- KEYS[3] = unavailable hash
-- KEYS[4] = failed hash
-- KEYS[5] = removed hash
-- ARGV[1] = video_id
-- ARGV[2] = enqueue_time (score)
local vid = ARGV[1]
for i = 3, 5 do
    redis.call('HDEL', KEYS[i], vid)
end
redis.call(
    'ZADD', KEYS[2],
    ARGV[2], vid
)
redis.call('HSET', KEYS[1], 'state', 'queued')
-- Clear any force tag so reviving a terminal record never
-- re-arms a stale force into a later non-force scrape.
redis.call('HDEL', KEYS[1], 'force')
return 1
'''

# Force re-enqueue with explicit per-state behavior. The
# ``force`` meta flag tells the scraper to scrape this id once
# even when it is already in the fleet-wide uploaded set.
_FORCE_ENQUEUE_LUA: str = '''
-- KEYS[1] = queue_key
-- KEYS[2] = meta_key
-- KEYS[3] = unavailable hash
-- KEYS[4] = failed hash
-- KEYS[5] = removed hash
-- ARGV[1] = video_id, ARGV[2] = source
-- ARGV[3] = now (string), ARGV[4] = queued_state
-- ARGV[5] = channel_id
-- ARGV[6] = channel_handle
-- ARGV[7] = channel_url
-- ARGV[8] = channel_is_verified
local vid = ARGV[1]
local state = redis.call('HGET', KEYS[2], 'state')
local is_terminal = (
    state == 'unavailable'
    or state == 'failed'
    or state == 'removed'
)
if is_terminal then
    redis.call('HDEL', KEYS[3], vid)
    redis.call('HDEL', KEYS[4], vid)
    redis.call('HDEL', KEYS[5], vid)
    redis.call('ZADD', KEYS[1], ARGV[3], vid)
    redis.call('HSET', KEYS[2], 'state', ARGV[4])
    redis.call('HSET', KEYS[2], 'force', '1')
    if ARGV[5] ~= '' then redis.call('HSET', KEYS[2], 'channel_id', ARGV[5]) end
    if ARGV[6] ~= '' then redis.call('HSET', KEYS[2], 'channel_handle', ARGV[6]) end
    if ARGV[7] ~= '' then redis.call('HSET', KEYS[2], 'channel_url', ARGV[7]) end
    if ARGV[8] ~= '' then redis.call('HSET', KEYS[2], 'channel_is_verified', ARGV[8]) end
    return 'revived'
elseif state == ARGV[4] then
    -- queued (waiting in zset, or popped and mid-scrape -
    -- indistinguishable). Re-arm force and ensure it is
    -- queued without disturbing an existing waiting score.
    redis.call('HSET', KEYS[2], 'force', '1')
    redis.call('ZADD', KEYS[1], 'NX', ARGV[3], vid)
    if ARGV[5] ~= '' then redis.call('HSET', KEYS[2], 'channel_id', ARGV[5]) end
    if ARGV[6] ~= '' then redis.call('HSET', KEYS[2], 'channel_handle', ARGV[6]) end
    if ARGV[7] ~= '' then redis.call('HSET', KEYS[2], 'channel_url', ARGV[7]) end
    if ARGV[8] ~= '' then redis.call('HSET', KEYS[2], 'channel_is_verified', ARGV[8]) end
    return 'forced_pending'
else
    -- absent (no meta) or unknown -> add fresh with force.
    redis.call('ZADD', KEYS[1], ARGV[3], vid)
    redis.call('HSET', KEYS[2], 'state', ARGV[4])
    redis.call('HSETNX', KEYS[2], 'source', ARGV[2])
    redis.call('HSETNX', KEYS[2], 'created_at', ARGV[3])
    redis.call('HSET', KEYS[2], 'force', '1')
    if ARGV[5] ~= '' then redis.call('HSET', KEYS[2], 'channel_id', ARGV[5]) end
    if ARGV[6] ~= '' then redis.call('HSET', KEYS[2], 'channel_handle', ARGV[6]) end
    if ARGV[7] ~= '' then redis.call('HSET', KEYS[2], 'channel_url', ARGV[7]) end
    if ARGV[8] ~= '' then redis.call('HSET', KEYS[2], 'channel_is_verified', ARGV[8]) end
    return 'added'
end
'''

# Consume-on-read of the force flag: return 1 and clear the
# flag if it was set, else 0. Ensures a force is honoured at
# most once and never leaks into a later scrape.
_CONSUME_FORCE_LUA: str = '''
-- KEYS[1] = meta_key
local f = redis.call('HGET', KEYS[1], 'force')
if f then
    redis.call('HDEL', KEYS[1], 'force')
    return 1
end
return 0
'''

_ENQUEUE_LUA: str = '''
-- KEYS[1] = queue_key
-- KEYS[2] = meta_key
-- ARGV[1] = video_id, ARGV[2] = source
-- ARGV[3] = now (string), ARGV[4] = queued_state
-- ARGV[5] = channel_id
-- ARGV[6] = channel_handle
-- ARGV[7] = channel_url
-- ARGV[8] = channel_is_verified
local existing_state = redis.call(
    'HGET', KEYS[2], 'state'
)
-- If the video already has Redis metadata, treat it as
-- known and report a duplicate. Producers can safely call
-- enqueue() repeatedly without bypassing tombstones; only
-- `unmark` returns terminal records to the queue.
if existing_state ~= false then
    return 0
end
local added = redis.call('ZADD', KEYS[1], 'NX', ARGV[3], ARGV[1])
if added == 0 then
    return 0
end
redis.call(
    'HSETNX', KEYS[2], 'source', ARGV[2]
)
redis.call(
    'HSETNX', KEYS[2], 'created_at', ARGV[3]
)
redis.call(
    'HSETNX', KEYS[2], 'state', ARGV[4]
)
if ARGV[5] ~= '' then redis.call('HSET', KEYS[2], 'channel_id', ARGV[5]) end
if ARGV[6] ~= '' then redis.call('HSET', KEYS[2], 'channel_handle', ARGV[6]) end
if ARGV[7] ~= '' then redis.call('HSET', KEYS[2], 'channel_url', ARGV[7]) end
if ARGV[8] ~= '' then redis.call('HSET', KEYS[2], 'channel_is_verified', ARGV[8]) end
return added
'''

_POP_LUA: str = '''
-- KEYS: queue_key
-- ARGV: batch
local members = redis.call(
    'ZRANGE', KEYS[1], 0, ARGV[1] - 1
)
if #members > 0 then
    redis.call('ZREM', KEYS[1], unpack(members))
end
return members
'''


class VideoState(str, enum.Enum):
    QUEUED = 'queued'
    UNAVAILABLE = 'unavailable'
    FAILED = 'failed'
    REMOVED = 'removed'

    @classmethod
    def terminal_states(
        cls,
    ) -> frozenset['VideoState']:
        return frozenset({
            cls.UNAVAILABLE,
            cls.FAILED,
            cls.REMOVED,
        })


@dataclass(frozen=True)
class VideoQueueChannelContext:
    channel_id: str | None = None
    channel_handle: str | None = None
    channel_url: str | None = None
    channel_is_verified: bool | None = None


@dataclass(frozen=True)
class VideoScrapeQueueEntry:
    video_id: str
    channel: VideoQueueChannelContext
    source: str | None
    meta: dict[str, str]


class VideoScrapeQueueSettings(BaseSettings):
    # Standalone settings — not inherited from
    # ScraperSettings because the queue is also
    # consumed by tools/yt_video_queue.py (CLI) and
    # tools/yt_rss_scrape.py (producer), neither of
    # which needs scraper-level config.
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore',
    )

    video_queue_batch: int = Field(default=50)
    video_queue_idle_poll_seconds: float = Field(
        default=2.0,
    )
    video_transient_max_attempts: int = Field(
        default=3,
    )
    video_transient_backoff_seconds: int = Field(
        default=30,
    )


class VideoScrapeQueue(ABC):
    '''Abstract work queue for the video scraper.

    v1 scaffold: subclasses add concrete methods in
    later tasks (see plan:
    docs/superpowers/plans/2026-05-18-video-scrape-queue.md).
    '''

    @abstractmethod
    async def enqueue(
        self,
        video_id: str,
        *,
        source: str,
        channel_id: str | None = None,
        channel_handle: str | None = None,
        channel_url: str | None = None,
        channel_is_verified: bool | None = None,
    ) -> bool: ...

    @abstractmethod
    async def pop(self, batch: int) -> list[str]: ...

    @abstractmethod
    async def pop_entries(
        self, batch: int,
    ) -> list[VideoScrapeQueueEntry]: ...

    @abstractmethod
    async def complete(self, video_id: str) -> None: ...

    @abstractmethod
    async def mark(
        self, video_id: str, *,
        state: VideoState,
        last_error: str | None = None,
        note: str | None = None,
    ) -> None: ...

    @abstractmethod
    async def unmark(self, video_id: str) -> None: ...

    @abstractmethod
    async def bump_attempts(
        self, video_id: str, *, last_error: str,
    ) -> int: ...

    @abstractmethod
    async def get_state(
        self, video_id: str,
    ) -> VideoState | None: ...

    @abstractmethod
    async def get_meta(
        self, video_id: str,
    ) -> dict[str, str]: ...

    @abstractmethod
    async def set_meta(
        self, video_id: str, **fields: str,
    ) -> None: ...

    @abstractmethod
    async def count_by_state(
        self,
    ) -> dict['VideoState', int]: ...

    @abstractmethod
    async def search_meta(
        self,
        pattern: str,
        fields: tuple[str, ...] = (
            'last_error', 'source',
        ),
    ) -> list[str]: ...


class RedisVideoScrapeQueue(VideoScrapeQueue):
    '''Redis-backed implementation of
    VideoScrapeQueue. All keys live under the configured
    ``<platform>:video:*`` namespace. State transitions
    crossing multiple keys are atomic via Lua.
    '''

    def __init__(
        self,
        redis: aioredis.Redis,
        settings: VideoScrapeQueueSettings,
        platform: str = 'youtube',
    ) -> None:
        if not platform:
            raise ValueError('empty platform')
        self._redis: aioredis.Redis = redis
        self._settings: VideoScrapeQueueSettings = (
            settings
        )
        self._key_prefix: str = f'{platform}:video'

    def _k_queue(self) -> str:
        return f'{self._key_prefix}:queue'

    def _k_meta(self, video_id: str) -> str:
        return f'{self._key_prefix}:meta:{video_id}'

    def _k_state(self, state: VideoState) -> str:
        return f'{self._key_prefix}:{state.value}'

    @staticmethod
    def _redis_bool(value: bool | None) -> str:
        if value is None:
            return ''
        return '1' if value else '0'

    @staticmethod
    def _parse_redis_bool(value: str | None) -> bool | None:
        if value is None or value == '':
            return None
        return value == '1'

    @staticmethod
    def _entry_from_meta(
        video_id: str, meta: dict[str, str],
    ) -> VideoScrapeQueueEntry:
        return VideoScrapeQueueEntry(
            video_id=video_id,
            channel=VideoQueueChannelContext(
                channel_id=meta.get('channel_id') or None,
                channel_handle=meta.get('channel_handle') or None,
                channel_url=meta.get('channel_url') or None,
                channel_is_verified=(
                    RedisVideoScrapeQueue._parse_redis_bool(
                        meta.get('channel_is_verified'),
                    )
                ),
            ),
            source=meta.get('source') or None,
            meta=meta,
        )

    async def enqueue(
        self,
        video_id: str,
        *,
        source: str,
        channel_id: str | None = None,
        channel_handle: str | None = None,
        channel_url: str | None = None,
        channel_is_verified: bool | None = None,
    ) -> bool:
        if not video_id:
            raise ValueError('empty video_id')
        now: float = time.time()
        added: int = int(await self._redis.eval(
            _ENQUEUE_LUA, 2,
            self._k_queue(),
            self._k_meta(video_id),
            video_id, source,
            str(int(now)),
            VideoState.QUEUED.value,
            channel_id or '',
            channel_handle or '',
            channel_url or '',
            self._redis_bool(channel_is_verified),
        ))
        return added == 1

    async def force_enqueue(
        self,
        video_id: str,
        *,
        source: str,
        channel_id: str | None = None,
        channel_handle: str | None = None,
        channel_url: str | None = None,
        channel_is_verified: bool | None = None,
    ) -> str:
        '''Force a (re-)scrape of *video_id* regardless of prior
        state, tagging it so the scraper bypasses the uploaded-set
        skip.

        Returns the outcome:
        - ``'revived'``  — was terminal; tombstone cleared, re-queued.
        - ``'forced_pending'`` — was already queued / mid-scrape;
          force re-armed (see the in-flight race note in the design).
        - ``'added'``    — had no record; added fresh.
        '''
        if not video_id:
            raise ValueError('empty video_id')
        now: int = int(time.time())
        result: Any = await self._redis.eval(
            _FORCE_ENQUEUE_LUA, 5,
            self._k_queue(),
            self._k_meta(video_id),
            self._k_state(VideoState.UNAVAILABLE),
            self._k_state(VideoState.FAILED),
            self._k_state(VideoState.REMOVED),
            video_id, source,
            str(now),
            VideoState.QUEUED.value,
            channel_id or '',
            channel_handle or '',
            channel_url or '',
            self._redis_bool(channel_is_verified),
        )
        if isinstance(result, bytes):
            return result.decode()
        return str(result)

    async def consume_force(self, video_id: str) -> bool:
        '''Atomically read-and-clear the ``force`` meta flag.

        Returns ``True`` (and clears the flag) when it was set, so a
        force is honoured at most once and never leaks into a later
        scrape; ``False`` otherwise.
        '''
        result: Any = await self._redis.eval(
            _CONSUME_FORCE_LUA, 1,
            self._k_meta(video_id),
        )
        return int(result) == 1

    async def pop(self, batch: int) -> list[str]:
        members: list[str] = await self._redis.eval(
            _POP_LUA, 1, self._k_queue(), str(batch),
        )
        return members

    async def pop_entries(
        self, batch: int,
    ) -> list[VideoScrapeQueueEntry]:
        video_ids: list[str] = await self.pop(batch)
        if not video_ids:
            return []
        pipe: aioredis.client.Pipeline = (
            self._redis.pipeline(transaction=False)
        )
        for video_id in video_ids:
            pipe.hgetall(self._k_meta(video_id))
        metas: list[dict[str, str]] = await pipe.execute()
        return [
            self._entry_from_meta(video_id, meta)
            for video_id, meta in zip(video_ids, metas)
        ]

    async def complete(self, video_id: str) -> None:
        pipe: aioredis.client.Pipeline = (
            self._redis.pipeline(transaction=True)
        )
        pipe.zrem(self._k_queue(), video_id)
        pipe.delete(self._k_meta(video_id))
        for s in VideoState.terminal_states():
            pipe.hdel(self._k_state(s), video_id)
        await pipe.execute()

    async def mark(
        self, video_id: str, *,
        state: VideoState,
        last_error: str | None = None,
        note: str | None = None,
    ) -> None:
        if state not in VideoState.terminal_states():
            raise ValueError(
                f'mark target must be terminal, got '
                f'{state.value!r}'
            )
        now: float = time.time()
        source: str | None = await self._redis.hget(
            self._k_meta(video_id), 'source',
        )
        meta: dict[str, str] = await self.get_meta(video_id)
        record: dict[str, Any] = {
            'ts': int(now),
            'last_error': last_error,
            'note': note,
            'source': source,
        }
        for field in (
            'channel_id',
            'channel_handle',
            'channel_url',
            'channel_is_verified',
        ):
            if meta.get(field):
                record[field] = meta[field]
        await self._redis.eval(
            _MARK_LUA, 6,
            self._k_meta(video_id),
            self._k_state(state),
            self._k_queue(),
            self._k_state(VideoState.UNAVAILABLE),
            self._k_state(VideoState.FAILED),
            self._k_state(VideoState.REMOVED),
            video_id, state.value, json.dumps(record),
        )

    async def unmark(self, video_id: str) -> None:
        await self._redis.eval(
            _UNMARK_LUA, 5,
            self._k_meta(video_id),
            self._k_queue(),
            self._k_state(VideoState.UNAVAILABLE),
            self._k_state(VideoState.FAILED),
            self._k_state(VideoState.REMOVED),
            video_id, str(time.time()),
        )

    async def bump_attempts(
        self, video_id: str, *, last_error: str,
    ) -> int:
        meta_key: str = self._k_meta(video_id)
        pipe: aioredis.client.Pipeline = (
            self._redis.pipeline(transaction=True)
        )
        pipe.hincrby(meta_key, 'attempts', 1)
        pipe.hset(
            meta_key, 'last_error', last_error,
        )
        pipe.hset(
            meta_key,
            'last_attempt_at', str(int(time.time())),
        )
        results: list[Any] = await pipe.execute()
        return int(results[0])

    async def get_state(
        self, video_id: str,
    ) -> VideoState | None:
        raw: str | None = await self._redis.hget(
            self._k_meta(video_id), 'state',
        )
        if raw is None:
            return None
        try:
            return VideoState(raw)
        except ValueError:
            return None

    async def get_meta(
        self, video_id: str,
    ) -> dict[str, str]:
        return await self._redis.hgetall(
            self._k_meta(video_id),
        )

    async def set_meta(
        self, video_id: str, **fields: str,
    ) -> None:
        if fields:
            await self._redis.hset(
                self._k_meta(video_id),
                mapping=fields,
            )

    async def count_by_state(
        self,
    ) -> dict[VideoState, int]:
        pipe: aioredis.client.Pipeline = (
            self._redis.pipeline(transaction=False)
        )
        pipe.zcard(self._k_queue())
        terminal: list[VideoState] = sorted(
            VideoState.terminal_states(),
            key=lambda s: s.value,
        )
        for s in terminal:
            pipe.hlen(self._k_state(s))
        results: list[int] = await pipe.execute()
        out: dict[VideoState, int] = {
            VideoState.QUEUED: results[0],
        }
        for s, n in zip(terminal, results[1:]):
            out[s] = n
        return out

    async def search_meta(
        self,
        pattern: str,
        fields: tuple[str, ...] = (
            'last_error', 'source',
        ),
    ) -> list[str]:
        import fnmatch

        def _matches(
            vals: list[str | None],
        ) -> bool:
            return any(
                v is not None
                and fnmatch.fnmatchcase(v, pattern)
                for v in vals
            )

        out: list[str] = []
        cursor: int = 0
        while True:
            cursor, keys = await self._redis.scan(
                cursor=cursor,
                match=f'{self._key_prefix}:meta:*',
                count=500,
            )
            if keys:
                pipe: aioredis.client.Pipeline = (
                    self._redis.pipeline(
                        transaction=False,
                    )
                )
                for k in keys:
                    pipe.hmget(k, *fields)
                values: list[list[str | None]] = (
                    await pipe.execute()
                )
                for key, vals in zip(keys, values):
                    if _matches(vals):
                        vid: str = key.split(
                            ':meta:', 1,
                        )[1]
                        out.append(vid)
            if cursor == 0:
                break
        return out

    async def iter_members(self) -> AsyncIterator[dict]:
        '''Yield ``{'video_id': ..., **meta}`` for every member that
        still has a meta hash (terminal/removed members keep theirs;
        only ``complete()`` deletes it). Streams via ``SCAN``.'''
        cursor: int = 0
        while True:
            cursor, keys = await self._redis.scan(
                cursor=cursor,
                match=f'{self._key_prefix}:meta:*',
                count=500,
            )
            for key in keys:
                vid: str = key.split(':meta:', 1)[1]
                meta: dict[str, str] = await self._redis.hgetall(key)
                if meta:
                    yield {'video_id': vid, **meta}
            if cursor == 0:
                break
