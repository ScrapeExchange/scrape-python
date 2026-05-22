'''
Redis-backed map from ``channel_handle`` to ``channel_id``.

Inverse of :mod:`scrape_exchange.creator_map`. Kept as a
denormalised reverse so that handle→id lookups stay O(1) as
both maps grow into the millions of entries. See ``CONTEXT.md``
for the consistency invariant with creator_map and the
rationale.

Three interchangeable backends:

* :class:`RedisHandleMap` — Redis hash. Atomic operations,
  works across hosts. Key pattern: ``{platform}:handle_map``.
* :class:`NullHandleMap` — in-process dict for tests and
  offline operator runs.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

from abc import ABC, abstractmethod

import redis.asyncio as aioredis

# Per-HMGET cap. Keeps a single Redis command bounded so a
# large handle list completes in a handful of round-trips
# instead of one giant command.
_GET_MANY_CHUNK_SIZE: int = 10000


class HandleMap(ABC):
    '''Abstract async map from channel_handle to channel_id.'''

    @abstractmethod
    async def get(self, handle: str) -> str | None:
        '''Return the channel_id for *handle*, or None.'''

    @abstractmethod
    async def get_many(
        self, handles: list[str],
    ) -> dict[str, str | None]:
        '''Return handle -> channel_id (or None) for many
        handles in one batched call. Implementations must
        avoid fan-out over the connection pool — callers may
        pass large handle lists.'''

    @abstractmethod
    async def get_all(self) -> dict[str, str]:
        '''Return all handle -> channel_id mappings.'''

    @abstractmethod
    async def put(
        self, handle: str, channel_id: str,
    ) -> None:
        '''Store a single mapping.'''

    @abstractmethod
    async def put_many(
        self, mapping: dict[str, str],
    ) -> None:
        '''Store multiple mappings at once.'''

    @abstractmethod
    async def contains(self, handle: str) -> bool:
        '''Return True if *handle* is in the map.'''

    @abstractmethod
    async def size(self) -> int:
        '''Return the number of entries.'''

    @property
    def redis_client(self) -> 'aioredis.Redis | None':
        '''Underlying Redis client, or None if this backend
        is not Redis-backed. Used by callers that need to
        construct adjacent Redis-backed primitives tied to
        the same Redis.'''
        return None


class NullHandleMap(HandleMap):
    '''In-process map used by tests and offline operator
    runs. Stores entries in a plain dict.'''

    def __init__(self) -> None:
        self._store: dict[str, str] = {}

    async def get(self, handle: str) -> str | None:
        return self._store.get(handle)

    async def get_many(
        self, handles: list[str],
    ) -> dict[str, str | None]:
        if not handles:
            return {}
        return {h: self._store.get(h) for h in handles}

    async def get_all(self) -> dict[str, str]:
        return dict(self._store)

    async def put(
        self, handle: str, channel_id: str,
    ) -> None:
        self._store[handle] = channel_id

    async def put_many(
        self, mapping: dict[str, str],
    ) -> None:
        self._store.update(mapping)

    async def contains(self, handle: str) -> bool:
        return handle in self._store

    async def size(self) -> int:
        return len(self._store)


class RedisHandleMap(HandleMap):
    '''Production backend backed by a Redis hash keyed as
    ``{platform}:handle_map`` (default ``youtube:handle_map``).
    Field = channel_handle, value = channel_id.

    Accepts a pre-constructed :class:`redis.asyncio.Redis`
    client so that callers sharing a single Redis connection
    across multiple maps pay no extra connection overhead.
    '''

    def __init__(
        self,
        client: aioredis.Redis,
        platform: str = 'youtube',
    ) -> None:
        self._redis: aioredis.Redis = client
        self._key: str = f'{platform}:handle_map'

    @property
    def redis_client(self) -> aioredis.Redis:
        return self._redis

    async def get(self, handle: str) -> str | None:
        result: str | None = await self._redis.hget(
            self._key, handle,
        )
        return result

    async def get_many(
        self, handles: list[str],
    ) -> dict[str, str | None]:
        if not handles:
            return {}
        out: dict[str, str | None] = {}
        for start in range(0, len(handles), _GET_MANY_CHUNK_SIZE):
            chunk: list[str] = (
                handles[start:start + _GET_MANY_CHUNK_SIZE]
            )
            values: list[str | None] = (
                await self._redis.hmget(self._key, chunk)
            )
            for hdl, val in zip(chunk, values):
                out[hdl] = val
        return out

    async def get_all(self) -> dict[str, str]:
        result: dict[str, str] = (
            await self._redis.hgetall(self._key)
        )
        return result

    async def put(
        self, handle: str, channel_id: str,
    ) -> None:
        await self._redis.hset(
            self._key, handle, channel_id,
        )

    async def put_many(
        self, mapping: dict[str, str],
    ) -> None:
        if not mapping:
            return
        await self._redis.hset(self._key, mapping=mapping)

    async def contains(self, handle: str) -> bool:
        return await self._redis.hexists(self._key, handle)

    async def size(self) -> int:
        return await self._redis.hlen(self._key)
