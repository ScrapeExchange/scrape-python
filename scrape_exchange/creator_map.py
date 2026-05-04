'''
Creator map abstraction for creator_id to creator_handle
mappings.

Two interchangeable backends:

* :class:`FileCreatorMap` — CSV file with append-only writes.
  Uses an ``asyncio.Lock`` for intra-process safety.
* :class:`RedisCreatorMap` — Redis hash. Atomic operations,
  works across hosts.
* :class:`NullCreatorMap` — no-op for contexts that do not
  need the creator map.

The creator scraper is the authoritative writer. The RSS
scraper reads only.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import logging
import os

from abc import ABC, abstractmethod

import aiofiles
import redis.asyncio as aioredis

from prometheus_client import Counter

_LOGGER: logging.Logger = logging.getLogger(__name__)


CREATOR_MAP_LOOKUP_TOTAL: Counter = Counter(
    'creator_map_lookup_total',
    'CreatorMap lookups at upload time.',
    labelnames=('platform', 'scraper', 'outcome'),
)

CREATOR_MAP_RESOLUTION_TOTAL: Counter = Counter(
    'creator_map_resolution_total',
    'Outcomes of handle resolution on CreatorMap miss.',
    labelnames=('platform', 'scraper', 'outcome'),
)

CREATOR_HANDLE_MISMATCH_TOTAL: Counter = Counter(
    'creator_handle_mismatch_total',
    'Input channel name differed from canonical handle.',
    labelnames=('platform', 'scraper'),
)


class CreatorMap(ABC):
    '''Abstract base for creator_id to creator_handle mappings.'''

    @abstractmethod
    async def get(
        self, creator_id: str,
    ) -> str | None:
        '''Return the creator_handle for *creator_id*, or None.'''

    @abstractmethod
    async def get_all(self) -> dict[str, str]:
        '''Return all creator_id to creator_handle mappings.'''

    @abstractmethod
    async def put(
        self, creator_id: str, creator_handle: str,
    ) -> None:
        '''Store a single mapping.'''

    @abstractmethod
    async def put_many(
        self, mapping: dict[str, str],
    ) -> None:
        '''Store multiple mappings at once.'''

    @abstractmethod
    async def contains(
        self, creator_id: str,
    ) -> bool:
        '''Return True if *creator_id* is in the map.'''

    @abstractmethod
    async def size(self) -> int:
        '''Return the number of entries.'''

    @property
    def redis_client(self) -> 'aioredis.Redis | None':
        '''Underlying Redis client, or None if this backend
        is not Redis-backed. Used by callers that need to
        construct adjacent Redis-backed primitives (claims,
        sets) tied to the same Redis.'''
        return None


class FileCreatorMap(CreatorMap):
    '''
    CSV-backed creator map. Reads the full file on
    :meth:`get_all` and caches in memory. Writes append
    to the file under an ``asyncio.Lock``.
    '''

    def __init__(self, file_path: str) -> None:
        self._file_path: str = file_path
        self._lock: asyncio.Lock = asyncio.Lock()
        self._cache: dict[str, str] = {}

    async def get(
        self, creator_id: str,
    ) -> str | None:
        if not self._cache:
            await self.get_all()
        return self._cache.get(creator_id)

    async def get_all(self) -> dict[str, str]:
        self._cache = {}
        if not os.path.isfile(self._file_path):
            return self._cache

        line: str
        async with aiofiles.open(self._file_path, 'r') as f:
            async for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if ',' in line:
                    cid: str
                    creator_handle: str
                    cid, creator_handle = line.split(',', 1)
                    self._cache[cid] = creator_handle
                else:
                    self._cache[line] = line

        _LOGGER.info(
            'Read creator map from file',
            extra={
                'file_path': self._file_path,
                'entries': len(self._cache),
            },
        )
        return dict(self._cache)

    async def put(
        self, creator_id: str, creator_handle: str,
    ) -> None:
        async with self._lock:
            async with aiofiles.open(
                self._file_path, 'a',
            ) as f:
                await f.write(
                    f'{creator_id},{creator_handle}\n'
                )
        self._cache[creator_id] = creator_handle

    async def put_many(
        self, mapping: dict[str, str],
    ) -> None:
        if not mapping:
            return
        async with self._lock:
            async with aiofiles.open(
                self._file_path, 'a',
            ) as f:
                for cid, creator_handle in mapping.items():
                    await f.write(
                        f'{cid},{creator_handle}\n'
                    )
        self._cache.update(mapping)

    async def contains(
        self, creator_id: str,
    ) -> bool:
        if not self._cache:
            await self.get_all()
        return creator_id in self._cache

    async def size(self) -> int:
        if not self._cache:
            await self.get_all()
        return len(self._cache)


class RedisCreatorMap(CreatorMap):
    '''
    Redis hash-backed creator map. Key ``{platform}:creator_map``
    with field=creator_id, value=creator_handle.
    '''

    def __init__(
        self, redis_dsn: str, platform: str,
    ) -> None:
        self._redis: aioredis.Redis = (
            aioredis.from_url(
                redis_dsn, decode_responses=True,
            )
        )
        self._key: str = f'{platform}:creator_map'

    @property
    def redis_client(self) -> aioredis.Redis:
        return self._redis

    async def get(
        self, creator_id: str,
    ) -> str | None:
        result: str | None = await self._redis.hget(
            self._key, creator_id,
        )
        return result

    async def get_all(self) -> dict[str, str]:
        result: dict[str, str] = (
            await self._redis.hgetall(self._key)
        )
        return result

    async def put(
        self, creator_id: str, creator_handle: str,
    ) -> None:
        await self._redis.hset(
            self._key, creator_id, creator_handle,
        )

    async def put_many(
        self, mapping: dict[str, str],
    ) -> None:
        if not mapping:
            return
        await self._redis.hset(
            self._key, mapping=mapping,
        )

    async def contains(
        self, creator_id: str,
    ) -> bool:
        return await self._redis.hexists(
            self._key, creator_id,
        )

    async def size(self) -> int:
        return await self._redis.hlen(self._key)


class NullCreatorMap(CreatorMap):
    '''No-op creator map. Discards writes, returns empty.'''

    async def get(
        self, creator_id: str,
    ) -> str | None:
        return None

    async def get_all(self) -> dict[str, str]:
        return {}

    async def put(
        self, creator_id: str, creator_handle: str,
    ) -> None:
        pass

    async def put_many(
        self, mapping: dict[str, str],
    ) -> None:
        pass

    async def contains(
        self, creator_id: str,
    ) -> bool:
        return False

    async def size(self) -> int:
        return 0
