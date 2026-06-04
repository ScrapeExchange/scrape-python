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
from typing import Any

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
-- ARGV[1] = video_id
-- ARGV[2] = state value
-- ARGV[3] = record JSON
local vid = ARGV[1]
local state = ARGV[2]
local record = ARGV[3]
redis.call('ZREM', 'youtube:video:queue', vid)
local states = {
    'unavailable', 'failed', 'removed',
}
for _, s in ipairs(states) do
    redis.call('HDEL', 'youtube:video:' .. s, vid)
end
redis.call('HSET', KEYS[2], vid, record)
redis.call('HSET', KEYS[1], 'state', state)
return 1
'''

_UNMARK_LUA: str = '''
-- KEYS[1] = meta hash
-- ARGV[1] = video_id
-- ARGV[2] = enqueue_time (score)
local vid = ARGV[1]
local states = {
    'unavailable', 'failed', 'removed',
}
for _, s in ipairs(states) do
    redis.call('HDEL', 'youtube:video:' .. s, vid)
end
redis.call(
    'ZADD', 'youtube:video:queue',
    ARGV[2], vid
)
redis.call('HSET', KEYS[1], 'state', 'queued')
return 1
'''

_ENQUEUE_LUA: str = '''
-- KEYS[1] = queue_key
-- KEYS[2] = meta_key
-- ARGV[1] = video_id, ARGV[2] = source
-- ARGV[3] = now (string), ARGV[4] = queued_state
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
        self, video_id: str, *, source: str,
    ) -> bool: ...

    @abstractmethod
    async def pop(self, batch: int) -> list[str]: ...

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
    VideoScrapeQueue. All keys live under the
    ``youtube:video:*`` namespace. State transitions
    crossing multiple keys are atomic via Lua.
    '''

    def __init__(
        self,
        redis: aioredis.Redis,
        settings: VideoScrapeQueueSettings,
    ) -> None:
        self._redis: aioredis.Redis = redis
        self._settings: VideoScrapeQueueSettings = (
            settings
        )

    def _k_queue(self) -> str:
        return f'{KEY_PREFIX}:queue'

    def _k_meta(self, video_id: str) -> str:
        return f'{KEY_PREFIX}:meta:{video_id}'

    def _k_state(self, state: VideoState) -> str:
        return f'{KEY_PREFIX}:{state.value}'

    async def enqueue(
        self, video_id: str, *, source: str,
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
        ))
        return added == 1

    async def pop(self, batch: int) -> list[str]:
        members: list[str] = await self._redis.eval(
            _POP_LUA, 1, self._k_queue(), str(batch),
        )
        return members

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
        record: dict[str, Any] = {
            'ts': int(now),
            'last_error': last_error,
            'note': note,
            'source': source,
        }
        await self._redis.eval(
            _MARK_LUA, 2,
            self._k_meta(video_id),
            self._k_state(state),
            video_id, state.value, json.dumps(record),
        )

    async def unmark(self, video_id: str) -> None:
        await self._redis.eval(
            _UNMARK_LUA, 1,
            self._k_meta(video_id),
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
                match=f'{KEY_PREFIX}:meta:*',
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
