'''
Name map abstraction for channel_title to channel_id mappings.

Distinct from :class:`CreatorMap` (which keys on ``channel_id``):
the name map is keyed on the *display name* of a channel and is
used by the re-ingest tool to recover a ``channel_id`` from
legacy video files that only carry a display-name in the channel
slot. Once the id is known the existing CreatorMap can supply
the canonical handle.

Last-write-wins semantics: a later write to the same display
name silently replaces an earlier one. Display names can be
shared across unrelated channels in practice; the re-ingest
resolver treats a name-map hit as "best effort" rather than
authoritative.

The channel scraper and RSS scraper are the writers (every time
they successfully extract a ``(channel_title, channel_id)`` pair
for a canonical channel). The re-ingest tool reads only.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import logging

from abc import ABC, abstractmethod


_LOGGER: logging.Logger = logging.getLogger(__name__)


class NameMap(ABC):
    '''Abstract base for channel_title to channel_id mappings.'''

    @abstractmethod
    async def get(self, channel_title: str) -> str | None:
        '''Return the channel_id for *channel_title*, or None.'''

    @abstractmethod
    async def get_all(self) -> dict[str, str]:
        '''Return all channel_title to channel_id mappings.'''

    @abstractmethod
    async def put(
        self, channel_title: str, channel_id: str,
    ) -> None:
        '''Store a single mapping (last-write-wins).'''

    @abstractmethod
    async def put_many(
        self, mapping: dict[str, str],
    ) -> None:
        '''Store multiple mappings at once (last-write-wins).'''

    @abstractmethod
    async def contains(self, channel_title: str) -> bool:
        '''Return True if *channel_title* is in the map.'''

    @abstractmethod
    async def size(self) -> int:
        '''Return the number of entries.'''


class RedisNameMap(NameMap):
    '''
    Redis hash-backed name map. Key ``{platform}:name_map`` with
    ``field=channel_title``, ``value=channel_id``.
    '''

    def __init__(
        self, redis_dsn: str, platform: str,
    ) -> None:
        import redis.asyncio as aioredis
        self._redis: aioredis.Redis = aioredis.from_url(
            redis_dsn, decode_responses=True,
        )
        self._key: str = f'{platform}:name_map'

    async def get(self, channel_title: str) -> str | None:
        result: str | None = await self._redis.hget(
            self._key, channel_title,
        )
        return result

    async def get_all(self) -> dict[str, str]:
        result: dict[str, str] = (
            await self._redis.hgetall(self._key)
        )
        return result

    async def put(
        self, channel_title: str, channel_id: str,
    ) -> None:
        await self._redis.hset(
            self._key, channel_title, channel_id,
        )

    async def put_many(
        self, mapping: dict[str, str],
    ) -> None:
        if not mapping:
            return
        await self._redis.hset(self._key, mapping=mapping)

    async def contains(self, channel_title: str) -> bool:
        return await self._redis.hexists(
            self._key, channel_title,
        )

    async def size(self) -> int:
        return await self._redis.hlen(self._key)


class NullNameMap(NameMap):
    '''No-op name map. Discards writes, returns empty.'''

    async def get(self, channel_title: str) -> str | None:
        return None

    async def get_all(self) -> dict[str, str]:
        return {}

    async def put(
        self, channel_title: str, channel_id: str,
    ) -> None:
        pass

    async def put_many(
        self, mapping: dict[str, str],
    ) -> None:
        pass

    async def contains(self, channel_title: str) -> bool:
        return False

    async def size(self) -> int:
        return 0
