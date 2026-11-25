'''
Add items to a Bloom filter using only commands that both Redis 8 and
Dragonfly support.

``BF.INSERT`` (create with sizing and add in one call) does not exist
in Dragonfly. :func:`bloom_add_many` sends ``BF.RESERVE`` with the
configured sizing and ``BF.MADD`` in one round trip instead. The
reserve fails with "item exists" once the filter exists, which is
ignored. It must come first: ``BF.MADD`` on a missing key creates a
filter with the server's small default sizing.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

from collections.abc import Sequence
from typing import Any

import redis.asyncio as aioredis
from redis.exceptions import ResponseError


def _is_exists_error(result: Any) -> bool:
    '''Whether *result* is the error BF.RESERVE returns for an
    existing filter.'''
    return (
        isinstance(result, ResponseError)
        and 'item exists' in str(result).lower()
    )


async def bloom_add_many(
    client: aioredis.Redis,
    key: str,
    items: Sequence[str],
    *,
    error_rate: float,
    capacity: int,
    expansion: int,
) -> None:
    '''Add *items* to the Bloom filter at *key*, creating the filter
    with *error_rate*, *capacity* and *expansion* if it does not exist
    yet. A no-op for an empty *items*.

    :raises ResponseError: the server rejected the reserve for a reason
        other than an existing filter, or rejected the add.
    '''
    if not items:
        return
    pipe: aioredis.client.Pipeline = client.pipeline(transaction=False)
    pipe.execute_command(
        'BF.RESERVE', key, f'{error_rate:f}', capacity,
        'EXPANSION', expansion,
    )
    pipe.execute_command('BF.MADD', key, *items)
    reserved: Any
    added: Any
    reserved, added = await pipe.execute(raise_on_error=False)
    if isinstance(reserved, Exception) and not _is_exists_error(reserved):
        raise reserved
    if isinstance(added, Exception):
        raise added
