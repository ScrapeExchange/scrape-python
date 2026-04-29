'''
Name map abstraction for asset_title to asset_id mappings.

Distinct from :class:`CreatorMap` (which keys on ``asset_id``):
the name map is keyed on the *display name* of an asset (a
channel, a video, or any future entity that has a human-readable
title and a stable platform-side id) and is used by the
re-ingest tool to recover an ``asset_id`` from legacy files
that only carry a display-name in the asset slot. Once the id
is known the existing CreatorMap can supply the canonical
handle.

Last-write-wins semantics: a later write to the same display
name silently replaces an earlier one. Display names can be
shared across unrelated assets in practice; the re-ingest
resolver treats a name-map hit as "best effort" rather than
authoritative.

The channel scraper and RSS scraper are the writers (every time
they successfully extract an ``(asset_title, asset_id)`` pair
for a canonical asset). The re-ingest tool reads only.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import logging

from abc import ABC, abstractmethod


_LOGGER: logging.Logger = logging.getLogger(__name__)


class NameMap(ABC):
    '''Abstract base for asset_title to asset_id mappings.'''

    @abstractmethod
    async def get(self, asset_title: str) -> str | None:
        '''Return the asset_id for *asset_title*, or None.'''

    @abstractmethod
    async def get_all(self) -> dict[str, str]:
        '''Return all asset_title to asset_id mappings.'''

    @abstractmethod
    async def put(
        self, asset_title: str, asset_id: str,
    ) -> None:
        '''Store a single mapping (last-write-wins).'''

    @abstractmethod
    async def put_many(
        self, mapping: dict[str, str],
    ) -> None:
        '''Store multiple mappings at once (last-write-wins).
        *mapping* maps asset_title to asset_id.'''

    @abstractmethod
    async def contains(self, asset_title: str) -> bool:
        '''Return True if *asset_title* is in the map.'''

    @abstractmethod
    async def size(self) -> int:
        '''Return the number of entries.'''


class RedisNameMap(NameMap):
    '''
    Redis hash-backed name map. Key ``{platform}:name_map`` with
    ``field=asset_title``, ``value=asset_id``.
    '''

    def __init__(
        self, redis_dsn: str, platform: str,
    ) -> None:
        import redis.asyncio as aioredis
        self._redis: aioredis.Redis = aioredis.from_url(
            redis_dsn, decode_responses=True,
        )
        self._key: str = f'{platform}:name_map'

    async def get(self, asset_title: str) -> str | None:
        result: str | None = await self._redis.hget(
            self._key, asset_title,
        )
        return result

    async def get_all(self) -> dict[str, str]:
        result: dict[str, str] = (
            await self._redis.hgetall(self._key)
        )
        return result

    async def put(
        self, asset_title: str, asset_id: str,
    ) -> None:
        await self._redis.hset(
            self._key, asset_title, asset_id,
        )

    async def put_many(
        self, mapping: dict[str, str],
    ) -> None:
        if not mapping:
            return
        await self._redis.hset(self._key, mapping=mapping)

    async def contains(self, asset_title: str) -> bool:
        return await self._redis.hexists(
            self._key, asset_title,
        )

    async def size(self) -> int:
        return await self._redis.hlen(self._key)


class NullNameMap(NameMap):
    '''No-op name map. Discards writes, returns empty.'''

    async def get(self, asset_title: str) -> str | None:
        return None

    async def get_all(self) -> dict[str, str]:
        return {}

    async def put(
        self, asset_title: str, asset_id: str,
    ) -> None:
        pass

    async def put_many(
        self, mapping: dict[str, str],
    ) -> None:
        pass

    async def contains(self, asset_title: str) -> bool:
        return False

    async def size(self) -> int:
        return 0
