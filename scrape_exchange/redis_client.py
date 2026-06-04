'''Shared Redis client construction helpers.'''

import os
from typing import Any

from scrape_exchange.worker_id import get_worker_id


DEFAULT_MAX_CONNECTIONS: int = 4


def _redis_max_connections() -> int:
    raw: str | None = os.environ.get('REDIS_MAX_CONNECTIONS')
    if not raw:
        return DEFAULT_MAX_CONNECTIONS
    try:
        value: int = int(raw)
    except ValueError:
        return DEFAULT_MAX_CONNECTIONS
    return max(value, 1)


def redis_client_name(component: str) -> str:
    safe_component: str = (
        component.replace(' ', '-').replace(':', '-')
    )
    return (
        f'scrape-python:{safe_component}:'
        f'w{get_worker_id()}:pid{os.getpid()}'
    )


def redis_from_url(
    redis_dsn: str,
    *,
    component: str,
    max_connections: int | None = None,
    **kwargs: Any,
):
    import redis.asyncio as aioredis

    kwargs.setdefault(
        'max_connections',
        (
            _redis_max_connections()
            if max_connections is None
            else max(max_connections, 1)
        ),
    )
    kwargs.setdefault('client_name', redis_client_name(component))
    pool = aioredis.BlockingConnectionPool.from_url(
        redis_dsn, **kwargs,
    )
    return aioredis.Redis(connection_pool=pool)
