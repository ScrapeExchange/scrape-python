'''Redis-backed work queue for the video scraper.

Implements the VideoScrapeQueue(ABC) interface described in
docs/superpowers/specs/2026-05-18-video-scrape-queue-design.md.
v1 ships RedisVideoScrapeQueue only; a FileVideoScrapeQueue is
planned for v2.
'''

import enum
import json
import time
import zlib
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

import redis.asyncio as aioredis
from pydantic import AliasChoices, Field
from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
)

KEY_PREFIX: str = 'youtube:video'

# TTL applied to a video's sparse meta hash when it is marked with
# a terminal state (unavailable/failed/removed). The state-hash
# entry remains as a compact tombstone, so enqueue() keeps
# reporting the video as known; only the per-key meta hash
# expires. The TTL bounds diagnostic retention; terminal
# membership continues to prevent automatic re-enqueueing.
TERMINAL_META_TTL_SECONDS: int = 30 * 24 * 3600

# Queued videos keep their enqueue context (source + channel) as a
# packed field in one of QMETA_BUCKETS bucket hashes instead of a
# per-video hash: ~60 bytes per video instead of ~240, and ~100M
# fewer top-level keys. Each bucket holds ~100 fields at 100M
# queued videos, well inside Redis' compact listpack encoding
# (hash-max-listpack-entries, default 512). The bucket count is
# part of the storage format: changing it needs a migration.
QMETA_BUCKETS: int = 1 << 20
QMETA_SEP: str = '|'

# The per-video ``meta`` hash is now sparse: it only exists for
# videos with a force flag, retry diagnostics (attempts,
# last_error, last_attempt_at) or retained terminal diagnostics.
# Its ``state`` field is only ever a terminal state; a queued
# video is one with a bucket entry.

_MARK_LUA: str = '''
-- KEYS[1] = qmeta bucket
-- KEYS[2] = meta hash
-- KEYS[3] = target state hash
-- KEYS[4] = queue key
-- KEYS[5] = unavailable hash
-- KEYS[6] = failed hash
-- KEYS[7] = removed hash
-- ARGV[1] = video_id
-- ARGV[2] = state value
-- ARGV[3] = record JSON
-- ARGV[4] = terminal meta TTL seconds (0 disables expiry)
local vid = ARGV[1]
redis.call('ZREM', KEYS[4], vid)
redis.call('HDEL', KEYS[1], vid)
for i = 5, 7 do
    redis.call('HDEL', KEYS[i], vid)
end
redis.call('HSET', KEYS[3], vid, ARGV[3])
-- Only retained diagnostics (attempts, last_error) get a state
-- and a TTL; the terminal hash entry is the durable record.
if redis.call('EXISTS', KEYS[2]) == 1 then
    redis.call('HSET', KEYS[2], 'state', ARGV[2])
    local ttl = tonumber(ARGV[4])
    if ttl ~= nil and ttl > 0 then
        redis.call('EXPIRE', KEYS[2], ttl)
    end
end
return 1
'''

_UNMARK_LUA: str = '''
-- KEYS[1] = qmeta bucket
-- KEYS[2] = meta hash
-- KEYS[3] = queue key
-- KEYS[4..6] = terminal state hashes
-- ARGV[1] = video_id
-- ARGV[2] = enqueue_time (score)
-- ARGV[3] = packed queue entry
local vid = ARGV[1]
for i = 4, 6 do
    redis.call('HDEL', KEYS[i], vid)
end
redis.call('ZADD', KEYS[3], ARGV[2], vid)
redis.call('HSET', KEYS[1], vid, ARGV[3])
-- Reviving a terminal record must clear the terminal TTL and
-- state that mark() armed, and any force tag so it never
-- re-arms a stale force into a later non-force scrape.
redis.call('HDEL', KEYS[2], 'state', 'force')
redis.call('PERSIST', KEYS[2])
return 1
'''

# Lua helper: merge a packed entry with new values. The existing
# source wins (first producer keeps attribution); non-empty
# channel values replace existing ones.
_LUA_MERGE: str = '''
local function merge(cur, src, cid, handle, ver)
    if cur then
        local s, c, h, v = string.match(
            cur, '^([^|]*)|([^|]*)|([^|]*)|([^|]*)$'
        )
        if s then
            if s ~= '' then src = s end
            if cid == '' then cid = c end
            if handle == '' then handle = h end
            if ver == '' then ver = v end
        end
    end
    return src .. '|' .. cid .. '|' .. handle .. '|' .. ver
end
'''

# Force re-enqueue with explicit per-state behavior. The
# ``force`` meta flag tells the scraper to scrape this id once
# even when it is already in the fleet-wide uploaded set.
_FORCE_ENQUEUE_LUA: str = _LUA_MERGE + '''
-- KEYS[1] = queue_key
-- KEYS[2] = qmeta bucket
-- KEYS[3] = meta hash
-- KEYS[4..6] = terminal state hashes
-- ARGV[1] = video_id, ARGV[2] = source, ARGV[3] = now
-- ARGV[4] = channel_id, ARGV[5] = channel_handle
-- ARGV[6] = channel_is_verified
-- ARGV[7] = fallback packed entry from the terminal record
local vid = ARGV[1]
local cur = redis.call('HGET', KEYS[2], vid)
if cur then
    -- queued (waiting in zset, or popped and mid-scrape -
    -- indistinguishable). Re-arm force and ensure it is
    -- queued without disturbing an existing waiting score.
    redis.call('HSET', KEYS[3], 'force', '1')
    redis.call('ZADD', KEYS[1], 'NX', ARGV[3], vid)
    redis.call('HSET', KEYS[2], vid, merge(
        cur, ARGV[2], ARGV[4], ARGV[5], ARGV[6]
    ))
    return 'forced_pending'
end
local state = redis.call('HGET', KEYS[3], 'state')
local is_terminal = (
    state == 'unavailable'
    or state == 'failed'
    or state == 'removed'
)
for i = 4, 6 do
    if redis.call('HDEL', KEYS[i], vid) == 1 then
        is_terminal = true
    end
end
-- A revived or re-armed record must not carry a terminal TTL.
redis.call('PERSIST', KEYS[3])
redis.call('HDEL', KEYS[3], 'state')
redis.call('HSET', KEYS[3], 'force', '1')
redis.call('ZADD', KEYS[1], ARGV[3], vid)
local base = nil
if ARGV[7] ~= '' then base = ARGV[7] end
redis.call('HSET', KEYS[2], vid, merge(
    base, ARGV[2], ARGV[4], ARGV[5], ARGV[6]
))
if is_terminal then return 'revived' end
return 'added'
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
-- KEYS[2] = qmeta bucket
-- KEYS[3] = meta hash
-- KEYS[4..6] = terminal state hashes
-- ARGV[1] = video_id, ARGV[2] = packed entry, ARGV[3] = now
local vid = ARGV[1]
-- A queued, in-flight or terminal video is known: report a
-- duplicate. Producers can safely call enqueue() repeatedly
-- without bypassing tombstones; only `unmark` and
-- `force_enqueue` return terminal records to the queue.
if redis.call('HEXISTS', KEYS[2], vid) == 1 then
    return 0
end
if redis.call('HEXISTS', KEYS[3], 'state') == 1 then
    return 0
end
for i = 4, 6 do
    if redis.call('HEXISTS', KEYS[i], vid) == 1 then
        return 0
    end
end
local added = redis.call('ZADD', KEYS[1], 'NX', ARGV[3], vid)
if added == 0 then
    return 0
end
redis.call('HSET', KEYS[2], vid, ARGV[2])
return added
'''

_GET_STATE_LUA: str = '''
-- KEYS: qmeta bucket, meta, unavailable, failed, removed
-- ARGV[1]: video ID
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then
    return 'queued'
end
local state = redis.call('HGET', KEYS[2], 'state')
if state then return state end
local states = {'unavailable', 'failed', 'removed'}
for i = 3, 5 do
    if redis.call('HEXISTS', KEYS[i], ARGV[1]) == 1 then
        return states[i - 2]
    end
end
return false
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
    ) -> frozenset[VideoState]:
        return frozenset({
            cls.UNAVAILABLE,
            cls.FAILED,
            cls.REMOVED,
        })

    @classmethod
    def terminal_states_values(cls) -> frozenset[str]:
        return frozenset(s.value for s in cls.terminal_states())


@dataclass(frozen=True)
class VideoQueueChannelContext:
    channel_id: str | None = None
    channel_handle: str | None = None
    channel_url: str | None = None
    channel_is_verified: bool | None = None


# Videos per backlog-server batch in enqueue_many.
ENQUEUE_MANY_CHUNK_SIZE: int = 1_000


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
    backlog_server_dsn: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'BACKLOG_SERVER_DSN', 'backlog_server_dsn',
        ),
        description=(
            'Redis-protocol URL of backlog-server, e.g. '
            'redis://:<password>@<host>:6390/0. When set, the YouTube '
            'and TikTok video queues live in backlog-server instead '
            'of Redis.'
        ),
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
    async def get_states(
        self, video_ids: list[str],
    ) -> dict[str, VideoState | None]: ...

    async def get_state_names(
        self, video_ids: list[str],
    ) -> dict[str, str | None]:
        '''State name per id, as the backend reports it.

        :meth:`get_states` collapses the backlog server's
        ``scraped`` state to ``None`` so every backend looks the
        same (Redis forgets a completed video). Callers that must
        tell a finished video from an unknown one use this instead:
        the default derives the name from :meth:`get_states`, and
        a backend that remembers completions overrides it.
        '''
        states: dict[str, VideoState | None] = await self.get_states(
            video_ids,
        )
        return {
            video_id: None if state is None else state.value
            for video_id, state in states.items()
        }

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
    ) -> dict[VideoState, int]: ...

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

    Key layout:

    - ``queue``: sorted set of waiting video IDs, scored by
      enqueue time.
    - ``qmeta:<bucket>``: hash per bucket; field = video ID,
      value = ``source|channel_id|channel_handle|is_verified``.
      Present while the video is queued or being scraped.
    - ``meta:<video_id>``: sparse hash for force, retry and
      retained terminal diagnostics.
    - ``unavailable`` / ``failed`` / ``removed``: terminal
      tombstone hashes.
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

    def _k_qmeta(self, video_id: str) -> str:
        return (
            f'{self._key_prefix}:qmeta:'
            f'{qmeta_bucket(video_id)}'
        )

    def _k_state(self, state: VideoState) -> str:
        return f'{self._key_prefix}:{state.value}'

    def _k_terminal(self) -> list[str]:
        return [
            self._k_state(VideoState.UNAVAILABLE),
            self._k_state(VideoState.FAILED),
            self._k_state(VideoState.REMOVED),
        ]

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

    @staticmethod
    def _merge_meta(
        packed: str | None, sparse: dict[str, str],
    ) -> dict[str, str]:
        '''Combine a bucket entry and the sparse meta hash into
        the flat meta view callers have always seen.'''
        meta: dict[str, str] = dict(sparse)
        if packed is not None:
            meta.update(unpack_qmeta(packed))
            meta['state'] = VideoState.QUEUED.value
        return meta

    async def _terminal_base(self, video_id: str) -> str:
        '''Packed entry rebuilt from the video's terminal record,
        so a revived video keeps its source and channel context.
        Empty when there is no (parseable) record.'''
        pipe: aioredis.client.Pipeline = (
            self._redis.pipeline(transaction=False)
        )
        key: str
        for key in self._k_terminal():
            pipe.hget(key, video_id)
        raws: list[str | None] = await pipe.execute()
        raw: str | None
        for raw in raws:
            if not raw:
                continue
            try:
                record: Any = json.loads(raw)
            except ValueError:
                continue
            if not isinstance(record, dict):
                continue
            return pack_qmeta(
                source=record.get('source') or '',
                channel_id=record.get('channel_id'),
                channel_handle=record.get('channel_handle'),
                channel_is_verified=self._parse_redis_bool(
                    record.get('channel_is_verified'),
                ),
            )
        return ''

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
        '''Queue *video_id* unless it is already known. The
        ``channel_url`` argument is accepted for compatibility but
        not stored; it is derivable from the handle or ID.'''
        if not video_id:
            raise ValueError('empty video_id')
        now: float = time.time()
        added: int = int(await self._redis.eval(
            _ENQUEUE_LUA, 6,
            self._k_queue(),
            self._k_qmeta(video_id),
            self._k_meta(video_id),
            *self._k_terminal(),
            video_id,
            pack_qmeta(
                source=source,
                channel_id=channel_id,
                channel_handle=channel_handle,
                channel_is_verified=channel_is_verified,
            ),
            str(int(now)),
        ))
        return added == 1

    async def enqueue_many(
        self,
        video_ids: list[str],
        *,
        source: str,
        channel_id: str | None = None,
        channel_handle: str | None = None,
        channel_is_verified: bool | None = None,
        chunk_size: int = ENQUEUE_MANY_CHUNK_SIZE,
    ) -> int:
        '''Queue every ID in *video_ids* that is not already known,
        with the same semantics as :meth:`enqueue` per ID.
        *chunk_size* is validated for parity with the backlog-server
        queue; Redis queues each ID with its own :meth:`enqueue`.

        :returns: number of IDs newly queued.
        '''
        if any(not video_id for video_id in video_ids):
            raise ValueError('empty video_id')
        if chunk_size < 1:
            raise ValueError('chunk_size must be positive')
        added: int = 0
        video_id: str
        for video_id in video_ids:
            if await self.enqueue(
                video_id, source=source, channel_id=channel_id,
                channel_handle=channel_handle,
                channel_is_verified=channel_is_verified,
            ):
                added += 1
        return added

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
        skip. ``channel_url`` is accepted but not stored.

        Returns the outcome:
        - ``'revived'``  — was terminal; tombstone cleared, re-queued.
        - ``'forced_pending'`` — was already queued / mid-scrape;
          force re-armed (see the in-flight race note in the design).
        - ``'added'``    — had no record; added fresh.
        '''
        if not video_id:
            raise ValueError('empty video_id')
        now: int = int(time.time())
        base: str = await self._terminal_base(video_id)
        result: Any = await self._redis.eval(
            _FORCE_ENQUEUE_LUA, 6,
            self._k_queue(),
            self._k_qmeta(video_id),
            self._k_meta(video_id),
            *self._k_terminal(),
            video_id,
            _clean(source),
            str(now),
            _clean(channel_id),
            _clean(channel_handle),
            self._redis_bool(channel_is_verified),
            base,
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
        metas: dict[str, dict[str, str]] = (
            await self._get_metas(video_ids)
        )
        return [
            self._entry_from_meta(video_id, metas[video_id])
            for video_id in video_ids
        ]

    async def _get_metas(
        self, video_ids: list[str],
    ) -> dict[str, dict[str, str]]:
        '''Merged meta view for each video, one round-trip.'''
        pipe: aioredis.client.Pipeline = (
            self._redis.pipeline(transaction=False)
        )
        video_id: str
        for video_id in video_ids:
            pipe.hget(self._k_qmeta(video_id), video_id)
            pipe.hgetall(self._k_meta(video_id))
        raw: list[Any] = await pipe.execute()
        return {
            video_id: self._merge_meta(raw[2 * i], raw[2 * i + 1])
            for i, video_id in enumerate(video_ids)
        }

    async def complete(self, video_id: str) -> None:
        pipe: aioredis.client.Pipeline = (
            self._redis.pipeline(transaction=True)
        )
        pipe.zrem(self._k_queue(), video_id)
        pipe.hdel(self._k_qmeta(video_id), video_id)
        pipe.delete(self._k_meta(video_id))
        for s in VideoState.terminal_states():
            pipe.hdel(self._k_state(s), video_id)
        await pipe.execute()

    async def flush(self) -> None:
        '''Nothing is buffered in Redis; kept for parity with the
        backlog-server queue, which callers flush on shutdown.'''

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
        meta: dict[str, str] = await self.get_meta(video_id)
        record: dict[str, Any] = {
            'ts': int(now),
            'last_error': last_error,
            'note': note,
            'source': meta.get('source'),
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
            _MARK_LUA, 7,
            self._k_qmeta(video_id),
            self._k_meta(video_id),
            self._k_state(state),
            self._k_queue(),
            *self._k_terminal(),
            video_id, state.value, json.dumps(record),
            str(TERMINAL_META_TTL_SECONDS),
        )

    async def unmark(self, video_id: str) -> None:
        base: str = await self._terminal_base(video_id)
        await self._redis.eval(
            _UNMARK_LUA, 6,
            self._k_qmeta(video_id),
            self._k_meta(video_id),
            self._k_queue(),
            *self._k_terminal(),
            video_id, str(time.time()),
            base or pack_qmeta(source=''),
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
        return (await self.get_states([video_id]))[video_id]

    async def get_states(
        self, video_ids: list[str],
    ) -> dict[str, VideoState | None]:
        '''Read states atomically per video, including tombstones.

        Pipeline lookups so one round-trip covers a candidate batch.
        '''
        out: dict[str, VideoState | None] = {}
        if not video_ids:
            return out
        pipe: aioredis.client.Pipeline = (
            self._redis.pipeline(transaction=False)
        )
        for video_id in video_ids:
            pipe.eval(
                _GET_STATE_LUA, 5,
                self._k_qmeta(video_id),
                self._k_meta(video_id),
                *self._k_terminal(),
                video_id,
            )
        raw: list[str | None] = await pipe.execute()
        for video_id, state in zip(video_ids, raw):
            if state is None:
                out[video_id] = None
                continue
            try:
                out[video_id] = VideoState(state)
            except ValueError:
                out[video_id] = None
        return out

    async def get_meta(
        self, video_id: str,
    ) -> dict[str, str]:
        '''Return the queue entry merged with retained diagnostics,
        or the surviving terminal state.'''
        meta: dict[str, str] = (
            await self._get_metas([video_id])
        )[video_id]
        if meta:
            if meta.get('state') == VideoState.QUEUED.value:
                score: float | None = await self._redis.zscore(
                    self._k_queue(), video_id,
                )
                if score is not None:
                    meta['created_at'] = str(int(score))
            return meta
        state: VideoState | None = await self.get_state(video_id)
        return {'state': state.value} if state is not None else {}

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
        '''Video IDs whose merged meta matches *pattern* (fnmatch)
        in any of *fields*. Scans every bucket and meta hash;
        operator use only.'''
        import fnmatch

        out: list[str] = []
        rec: dict
        async for rec in self.iter_members():
            if any(
                rec.get(f) is not None
                and fnmatch.fnmatchcase(rec[f], pattern)
                for f in fields
            ):
                out.append(rec['video_id'])
        return out

    async def iter_members(self) -> AsyncIterator[dict]:
        '''Stream queued members (bucket entries merged with their
        sparse meta) and then terminal members that still have a
        retained meta hash, via SCAN.

        Expired or compacted terminal records are absent from this view;
        count_by_state and direct state lookups still include them.
        '''
        cursor: int = 0
        while True:
            keys: list[str]
            cursor, keys = await self._redis.scan(
                cursor=cursor,
                match=f'{self._key_prefix}:qmeta:*',
                count=500,
            )
            key: str
            for key in keys:
                entries: dict[str, str] = await self._redis.hgetall(key)
                if not entries:
                    continue
                pipe: aioredis.client.Pipeline = (
                    self._redis.pipeline(transaction=False)
                )
                vid: str
                for vid in entries:
                    pipe.hgetall(self._k_meta(vid))
                sparse: list[dict[str, str]] = await pipe.execute()
                for vid, extra in zip(entries, sparse):
                    yield {
                        'video_id': vid,
                        **self._merge_meta(entries[vid], extra),
                    }
            if cursor == 0:
                break
        cursor = 0
        while True:
            cursor, keys = await self._redis.scan(
                cursor=cursor,
                match=f'{self._key_prefix}:meta:*',
                count=500,
            )
            for key in keys:
                vid = key.split(':meta:', 1)[1]
                if await self._redis.hexists(self._k_qmeta(vid), vid):
                    continue
                meta: dict[str, str] = await self._redis.hgetall(key)
                if meta:
                    yield {'video_id': vid, **meta}
            if cursor == 0:
                break


def qmeta_bucket(video_id: str) -> int:
    '''Bucket number of *video_id*'s packed queue entry.'''
    return zlib.crc32(video_id.encode('utf-8')) & (QMETA_BUCKETS - 1)


def _clean(value: str | None) -> str:
    '''Packed fields are ``|``-separated; drop the separator
    from values (it never occurs in IDs or handles).'''
    return (value or '').replace(QMETA_SEP, '')


def pack_qmeta(
    *,
    source: str,
    channel_id: str | None = None,
    channel_handle: str | None = None,
    channel_is_verified: bool | None = None,
) -> str:
    '''Encode a queued video's context as one bucket field value.'''
    verified: str = RedisVideoScrapeQueue._redis_bool(
        channel_is_verified,
    )
    return QMETA_SEP.join((
        _clean(source), _clean(channel_id),
        _clean(channel_handle), verified,
    ))


def unpack_qmeta(packed: str) -> dict[str, str]:
    '''Decode a bucket field value into meta fields, omitting
    empty ones.'''
    parts: list[str] = packed.split(QMETA_SEP, 3)
    if len(parts) != 4:
        return {'source': packed} if packed else {}
    names: tuple[str, ...] = (
        'source', 'channel_id', 'channel_handle',
        'channel_is_verified',
    )
    return {
        name: value for name, value in zip(names, parts) if value
    }

