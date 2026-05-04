'''Redis SET wrapper recording handles known to exist on
scrape.exchange. Replaces per-candidate ``channel_exists``
HTTP calls with sub-millisecond ``SISMEMBER`` lookups.

The set is keyed by handle (not channel_id) because that is
the identifier _select_new_channels operates on.
'''

from typing import Iterable

import redis.asyncio as aioredis
from redis.asyncio.client import Pipeline


_KEY: str = 'youtube:exchange_channels'


class RedisExchangeChannelsSet:
    '''Async wrapper around the YouTube exchange-channels SET.

    :param redis_client: shared async Redis client.
    '''

    def __init__(
        self, redis_client: aioredis.Redis,
    ) -> None:
        self._redis: aioredis.Redis = redis_client

    async def add_many(
        self, handles: Iterable[str],
    ) -> None:
        '''Add zero or more handles atomically.'''
        items: list[str] = [h for h in handles if h]
        if not items:
            return
        await self._redis.sadd(_KEY, *items)

    async def contains_many(
        self, handles: list[str],
    ) -> dict[str, bool]:
        '''Return a dict ``{handle: bool}`` reporting set
        membership. Uses pipelined SISMEMBER so one network
        round-trip covers the full batch. Duplicate handles
        in the input list collapse to a single dict entry —
        the SISMEMBER result is deterministic so the value
        is identical for both copies.'''
        if not handles:
            return {}
        pipeline: Pipeline = (
            self._redis.pipeline(transaction=False)
        )
        for handle in handles:
            pipeline.sismember(_KEY, handle)
        results: list[int] = await pipeline.execute()
        return {
            handles[i]: bool(results[i])
            for i in range(len(handles))
        }

    async def size(self) -> int:
        '''Return the number of handles in the set.'''
        return int(await self._redis.scard(_KEY))
