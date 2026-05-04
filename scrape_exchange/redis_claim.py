'''Generic Redis-backed SETNX claim helper.

Used by the channel scraper resolution path and the pre-resolver
operator tool to ensure a given resource (channel_id, handle) is
worked on by exactly one process across the fleet.
'''

from contextlib import asynccontextmanager
from typing import AsyncIterator

import redis.asyncio as aioredis


class RedisClaim:
    '''Per-key SETNX claim with TTL.

    :param redis_client: shared async Redis client.
    :param key_prefix: prefix applied to every key
        (e.g. ``youtube:resolving:``).
    :param ttl_seconds: claim lifetime; recovers from crashed
        owners.
    :param owner: identifier stored as the key value, useful for
        debugging and for safe ``release`` semantics.
    '''

    def __init__(
        self,
        redis_client: aioredis.Redis,
        key_prefix: str,
        ttl_seconds: int,
        owner: str,
    ) -> None:
        self._redis: aioredis.Redis = redis_client
        self._prefix: str = key_prefix
        self._ttl: int = ttl_seconds
        self._owner: str = owner

    def _key(self, resource: str) -> str:
        return f'{self._prefix}{resource}'

    async def try_claim(self, resource: str) -> bool:
        '''Return True if the caller acquired the claim.'''
        result: bool | None = await self._redis.set(
            self._key(resource),
            self._owner,
            nx=True,
            ex=self._ttl,
        )
        return bool(result)

    async def release(self, resource: str) -> None:
        '''Release the claim regardless of owner. Safe because
        each resource is only worked on by one owner at a time
        and the TTL bounds any race window.'''
        await self._redis.delete(self._key(resource))

    @asynccontextmanager
    async def acquire(
        self, resource: str,
    ) -> AsyncIterator[bool]:
        '''Async-context wrapper. Yields True when the claim
        was won; the caller should treat False as "skip this
        resource". Releases on exit only when won.'''
        won: bool = await self.try_claim(resource)
        try:
            yield won
        finally:
            if won:
                await self.release(resource)
