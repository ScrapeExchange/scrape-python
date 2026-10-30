'''Pick the video scrape queue backend for a platform.

The YouTube video queue lives in backlog-server when
``BACKLOG_SERVER_DSN`` is set; every other case uses the Redis
implementation.
'''

import asyncio
import logging
import weakref
from typing import Any

import redis.asyncio as aioredis

from scrape_exchange.backlog_server_queue import (
    BacklogServerVideoScrapeQueue,
)
from scrape_exchange.redis_client import backlog_redis_from_url
from scrape_exchange.video_scrape_queue import (
    RedisVideoScrapeQueue,
    VideoScrapeQueue,
    VideoScrapeQueueSettings,
)

# One backlog-server client (and so one connection pool) per DSN and
# event loop: callers such as the channel scraper build a queue per
# scrape, and a pool per queue would churn connections. Keyed by the
# loop object itself (weakly) rather than by ``id(loop)``: ids are
# reused once a loop is collected, and a client built on a dead loop
# can never establish a connection.
_BY_LOOP: weakref.WeakKeyDictionary[Any, dict[str, aioredis.Redis]] = (
    weakref.WeakKeyDictionary()
)
# Callers outside any running loop (sync setup code, tests) share one
# client per DSN.
_NO_LOOP: dict[str, aioredis.Redis] = {}


def _backlog_client(dsn: str, component: str) -> aioredis.Redis:
    '''Return the shared backlog-server client for *dsn*.'''
    loop: Any
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    clients: dict[str, aioredis.Redis] = (
        _NO_LOOP if loop is None else _BY_LOOP.setdefault(loop, {})
    )
    client: aioredis.Redis | None = clients.get(dsn)
    if client is None:
        created: aioredis.Redis = backlog_redis_from_url(
            dsn, component=component, decode_responses=True,
        )
        clients[dsn] = created
        return created
    return client


def make_video_scrape_queue(
    *,
    platform: str,
    redis: aioredis.Redis,
    settings: VideoScrapeQueueSettings,
    component: str,
) -> VideoScrapeQueue:
    '''Build the video scrape queue for *platform*.

    :param redis: the caller's Redis client, used by the Redis backend
    :param settings: queue settings; ``backlog_server_dsn`` selects
        backlog-server for YouTube
    :param component: client name for the backlog-server connection
    '''
    if platform == 'youtube' and settings.backlog_server_dsn:
        server: aioredis.Redis = _backlog_client(
            settings.backlog_server_dsn, component,
        )
        return BacklogServerVideoScrapeQueue(server, settings)
    return RedisVideoScrapeQueue(redis, settings, platform=platform)


async def close_backlog_clients() -> None:
    '''Close every backlog-server client bound to the running loop.

    The client (and so its connection pool) is cached per event loop
    because redis-py's async connections are loop-bound. A loop that
    ends without this leaves its sockets open: the next``asyncio.run()``
    builds a fresh pool, and the server keeps counting descriptors for
    connections no client will ever use again. Measured on the fleet
    before this existed: 19-43 sockets to backlog-server per yt-channel
    worker, 64 per yt-rss worker, and a server that eventually hit its
    ``RLIMIT_NOFILE`` and stopped accepting.

    Call it from the ``finally`` of whatever coroutine owns the loop —
    :func:`scrape_exchange.async_scope.run_async` does that for every
    tool that uses it.
    '''
    try:
        loop: Any = asyncio.get_running_loop()
    except RuntimeError:
        # No loop to close them on: dropping the references is all that
        # is left, and letting the GC get to them beats keeping a
        # client that can never be awaited again.
        _NO_LOOP.clear()
        return
    clients: dict[str, Any] = _BY_LOOP.pop(loop, {})
    for dsn, client in clients.items():
        try:
            await client.aclose()
        except Exception:
            logging.warning(
                f'closing backlog-server client for {dsn} failed',
                exc_info=True,
            )
