'''
Channel map abstraction for channel_id to channel_handle
mappings.

Two interchangeable backends:

* :class:`FileChannelMap` — CSV file with append-only writes.
  Uses an ``asyncio.Lock`` for intra-process safety.
* :class:`RedisChannelMap` — Redis hash. Atomic operations,
  works across hosts.
* :class:`NullChannelMap` — no-op for contexts that do not
  need the channel map.

The channel scraper is the authoritative writer. The RSS
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

_LOGGER: logging.Logger = logging.getLogger(__name__)

_REDIS_KEY: str = 'yt:channel_map'


class ChannelMap(ABC):
    '''Abstract base for channel_id to handle mappings.'''

    @abstractmethod
    async def get(
        self, channel_id: str,
    ) -> str | None:
        '''Return the handle for *channel_id*, or None.'''

    @abstractmethod
    async def get_all(self) -> dict[str, str]:
        '''Return all channel_id to handle mappings.'''

    @abstractmethod
    async def put(
        self, channel_id: str, handle: str,
    ) -> None:
        '''Store a single mapping.'''

    @abstractmethod
    async def put_many(
        self, mapping: dict[str, str],
    ) -> None:
        '''Store multiple mappings at once.'''

    @abstractmethod
    async def contains(
        self, channel_id: str,
    ) -> bool:
        '''Return True if *channel_id* is in the map.'''

    @abstractmethod
    async def size(self) -> int:
        '''Return the number of entries.'''


class FileChannelMap(ChannelMap):
    '''
    CSV-backed channel map. Reads the full file on
    :meth:`get_all` and caches in memory. Writes append
    to the file under an ``asyncio.Lock``.
    '''

    def __init__(self, file_path: str) -> None:
        self._file_path: str = file_path
        self._lock: asyncio.Lock = asyncio.Lock()
        self._cache: dict[str, str] = {}

    async def get(
        self, channel_id: str,
    ) -> str | None:
        if not self._cache:
            await self.get_all()
        return self._cache.get(channel_id)

    async def get_all(self) -> dict[str, str]:
        self._cache = {}
        if not os.path.isfile(self._file_path):
            return self._cache

        line: str
        async with aiofiles.open(
            self._file_path, 'r',
        ) as f:
            async for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if ',' in line:
                    cid: str
                    handle: str
                    cid, handle = line.split(',', 1)
                    self._cache[cid] = handle
                else:
                    self._cache[line] = line

        _LOGGER.info(
            'Read channel map from file',
            extra={
                'file_path': self._file_path,
                'entries': len(self._cache),
            },
        )
        return dict(self._cache)

    async def put(
        self, channel_id: str, handle: str,
    ) -> None:
        async with self._lock:
            async with aiofiles.open(
                self._file_path, 'a',
            ) as f:
                await f.write(
                    f'{channel_id},{handle}\n'
                )
        self._cache[channel_id] = handle

    async def put_many(
        self, mapping: dict[str, str],
    ) -> None:
        if not mapping:
            return
        async with self._lock:
            async with aiofiles.open(
                self._file_path, 'a',
            ) as f:
                for cid, handle in mapping.items():
                    await f.write(
                        f'{cid},{handle}\n'
                    )
        self._cache.update(mapping)

    async def contains(
        self, channel_id: str,
    ) -> bool:
        if not self._cache:
            await self.get_all()
        return channel_id in self._cache

    async def size(self) -> int:
        if not self._cache:
            await self.get_all()
        return len(self._cache)


class RedisChannelMap(ChannelMap):
    '''
    Redis hash-backed channel map. Key ``yt:channel_map``
    with field=channel_id, value=handle.
    '''

    def __init__(self, redis_dsn: str) -> None:
        import redis.asyncio as aioredis
        self._redis: aioredis.Redis = (
            aioredis.from_url(
                redis_dsn, decode_responses=True,
            )
        )
        self._key: str = _REDIS_KEY

    async def get(
        self, channel_id: str,
    ) -> str | None:
        result: str | None = await self._redis.hget(
            self._key, channel_id,
        )
        return result

    async def get_all(self) -> dict[str, str]:
        result: dict[str, str] = (
            await self._redis.hgetall(self._key)
        )
        return result

    async def put(
        self, channel_id: str, handle: str,
    ) -> None:
        await self._redis.hset(
            self._key, channel_id, handle,
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
        self, channel_id: str,
    ) -> bool:
        return await self._redis.hexists(
            self._key, channel_id,
        )

    async def size(self) -> int:
        return await self._redis.hlen(self._key)


class NullChannelMap(ChannelMap):
    '''No-op channel map. Discards writes, returns empty.'''

    async def get(
        self, channel_id: str,
    ) -> str | None:
        return None

    async def get_all(self) -> dict[str, str]:
        return {}

    async def put(
        self, channel_id: str, handle: str,
    ) -> None:
        pass

    async def put_many(
        self, mapping: dict[str, str],
    ) -> None:
        pass

    async def contains(
        self, channel_id: str,
    ) -> bool:
        return False

    async def size(self) -> int:
        return 0
