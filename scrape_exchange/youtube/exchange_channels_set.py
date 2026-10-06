'''Redis Bloom filter recording channels known to exist on
scrape.exchange. Replaces per-candidate ``channel_exists`` HTTP
calls with sub-millisecond ``BF.MEXISTS`` lookups.

A false positive (about 1 in 100,000) makes a channel that is not
on the exchange look present, so it is skipped as a new-channel
candidate; there are no false negatives.
'''

from collections.abc import Iterable

import redis.asyncio as aioredis

_KEY: str = 'youtube:exchange_channels_bf'

# ~24 bits per member at a 1e-5 error rate: ~90 MB at capacity,
# versus ~56 bytes per member for the SET it replaces. Past the
# capacity Redis adds sub-filters instead of degrading the rate.
EXCHANGE_CHANNELS_BF_CAPACITY: int = 30_000_000
EXCHANGE_CHANNELS_BF_ERROR_RATE: float = 0.00001
EXCHANGE_CHANNELS_BF_EXPANSION: int = 2


class RedisExchangeChannelsSet:
    '''Async wrapper around the YouTube exchange-channels Bloom
    filter.

    :param redis_client: shared async Redis client.
    '''

    def __init__(
        self, redis_client: aioredis.Redis,
    ) -> None:
        self._redis: aioredis.Redis = redis_client

    async def add_many(
        self, handles: Iterable[str],
    ) -> None:
        '''Add zero or more members. The first write creates the
        filter with the configured sizing.'''
        items: list[str] = [h for h in handles if h]
        if not items:
            return
        await self._redis.execute_command(
            'BF.INSERT', _KEY,
            'CAPACITY', EXCHANGE_CHANNELS_BF_CAPACITY,
            'ERROR', f'{EXCHANGE_CHANNELS_BF_ERROR_RATE:f}',
            'EXPANSION', EXCHANGE_CHANNELS_BF_EXPANSION,
            'ITEMS', *items,
        )

    async def contains_many(
        self, handles: list[str],
    ) -> dict[str, bool]:
        '''Return a dict ``{handle: bool}`` reporting (probable)
        membership in one round-trip. Duplicate handles in the
        input list collapse to a single dict entry.'''
        if not handles:
            return {}
        results: list[int] = await self._redis.execute_command(
            'BF.MEXISTS', _KEY, *handles,
        )
        return {
            handles[i]: bool(results[i])
            for i in range(len(handles))
        }

    async def size(self) -> int:
        '''Return the (approximate) number of members.'''
        return int(await self._redis.execute_command('BF.CARD', _KEY))
