'''Pick the video scrape queue backend for a platform.

The YouTube video queue lives in backlog-server when
``BACKLOG_SERVER_DSN`` is set; every other case keeps the Redis
(and optional MongoDB backlog) implementation.
'''

import asyncio
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
