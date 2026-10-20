'''Pick the video scrape queue backend for a platform.

The YouTube video queue lives in backlog-server when
``BACKLOG_SERVER_DSN`` is set; every other case keeps the Redis
(and optional MongoDB backlog) implementation.
'''

import asyncio

import redis.asyncio as aioredis

from scrape_exchange.backlog_server_queue import (
    BacklogServerVideoScrapeQueue,
)
from scrape_exchange.redis_client import redis_from_url
from scrape_exchange.video_scrape_queue import (
    RedisVideoScrapeQueue,
    VideoScrapeQueue,
    VideoScrapeQueueSettings,
)

# One backlog-server client (and so one connection pool) per DSN and
# event loop: callers such as the channel scraper build a queue per
# scrape, and a pool per queue would churn connections.
_SHARED: dict[tuple[str, int], aioredis.Redis] = {}


def _backlog_client(dsn: str, component: str) -> aioredis.Redis:
    loop_id: int
    try:
        loop_id = id(asyncio.get_running_loop())
    except RuntimeError:
        loop_id = 0
    key: tuple[str, int] = (dsn, loop_id)
    client: aioredis.Redis | None = _SHARED.get(key)
    if client is None:
        client = redis_from_url(
            dsn, component=component, decode_responses=True,
        )
        _SHARED[key] = client
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
