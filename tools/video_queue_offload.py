'''Move the video scrape queue's backlog from Redis into MongoDB.

Stop every video producer (yt-channel, yt-rss, yt-discover-search,
tt-creator, uploaders) and the video scrapers first. Run the steps in
order:

1. --step terminal   Redis tombstone hashes -> MongoDB documents
2. --step backlog    queued videos beyond the oldest --keep -> MongoDB,
                     deleted from Redis as they are copied
3. --step hot        videos left in Redis -> 'hot' MongoDB documents

Then start tools/yt_video_queue_refill.py and the scrapers with
MONGO_DSN set. Defaults to a dry run; pass --apply to write. Every step
is safe to re-run.
'''

import asyncio
import json
import sys
from dataclasses import asdict
from typing import Literal

import redis.asyncio as aioredis
from pydantic import AliasChoices, Field, SecretStr, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict
from pymongo.errors import PyMongoError
from redis.exceptions import RedisError

from scrape_exchange.status_bar import StatusBar
from scrape_exchange.video_backlog import TERMINAL_STATES, MongoVideoBacklog
from scrape_exchange.video_backlog_migration import (
    HotStats,
    OffloadStats,
    TerminalStats,
    migrate_terminal,
    offload_backlog,
    record_hot_window,
)


class OffloadSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file='.env', extra='ignore', cli_parse_args=True,
        cli_kebab_case=True, cli_implicit_flags=True,
        hide_input_in_errors=True, populate_by_name=True,
    )

    redis_dsn: SecretStr = Field(
        validation_alias=AliasChoices('redis_dsn', 'REDIS_DSN'),
        description='Redis connection URL; prefer the REDIS_DSN environment.',
    )
    mongo_dsn: SecretStr = Field(
        validation_alias=AliasChoices('mongo_dsn', 'MONGO_DSN'),
        description='MongoDB connection URL; prefer the MONGO_DSN environment.',
    )
    step: Literal['terminal', 'backlog', 'hot'] = Field(
        description='Which migration step to run.',
    )
    platform: Literal['youtube', 'tiktok'] = Field(
        default='youtube', description='Video queue namespace.',
    )
    keep: int = Field(
        default=20_000_000, ge=0,
        description=(
            'backlog step: oldest videos to keep in Redis as the hot '
            'window (match the refill high watermark).'
        ),
    )
    apply: bool = Field(
        default=False,
        description='Apply changes; omitted means a read-only dry run.',
    )
    delete_legacy: bool = Field(
        default=False,
        description='terminal step: UNLINK each Redis hash after a full copy.',
    )
    limit: int = Field(
        default=0, ge=0,
        description='Maximum videos to process; 0 means all.',
    )
    batch_size: int = Field(
        default=5000, ge=1, le=50_000,
        description='Videos per Redis read and MongoDB insert.',
    )
    pause_seconds: float = Field(
        default=0.0, ge=0, allow_inf_nan=False,
        description='backlog step: pause between batches.',
    )
    status: Literal['auto', 'inplace', 'lines'] = Field(
        default='auto',
        description=(
            'Status display: inplace redraws one line every second, '
            'lines prints a new line every 30 s, auto picks inplace '
            'when stderr is a terminal.'
        ),
    )


async def _total(redis: aioredis.Redis, settings: OffloadSettings) -> int:
    queue_size: int = int(
        await redis.zcard(f'{settings.platform}:video:queue'),
    )
    total: int
    if settings.step == 'terminal':
        total = 0
        state: str
        for state in TERMINAL_STATES:
            total += int(
                await redis.hlen(f'{settings.platform}:video:{state}'),
            )
    elif settings.step == 'backlog':
        total = max(queue_size - settings.keep, 0)
    else:
        total = queue_size
    return min(total, settings.limit) if settings.limit else total


async def run(settings: OffloadSettings) -> None:
    redis: aioredis.Redis = aioredis.Redis.from_url(
        settings.redis_dsn.get_secret_value(), decode_responses=True,
        socket_connect_timeout=5, socket_timeout=60,
    )
    backlog: MongoVideoBacklog = MongoVideoBacklog.from_dsn(
        settings.mongo_dsn.get_secret_value(), platform=settings.platform,
    )
    mode: str = 'apply' if settings.apply else 'dry-run'
    try:
        status: StatusBar = StatusBar(
            f'{settings.step} ({mode})', await _total(redis, settings),
            inplace=(
                None if settings.status == 'auto'
                else settings.status == 'inplace'
            ),
        )
        stats: TerminalStats | OffloadStats | HotStats
        if settings.step == 'terminal':
            stats = await migrate_terminal(
                redis, backlog, platform=settings.platform,
                apply=settings.apply,
                delete_legacy=settings.delete_legacy,
                batch_size=settings.batch_size, limit=settings.limit,
                progress=status.update,
            )
        elif settings.step == 'backlog':
            stats = await offload_backlog(
                redis, backlog, platform=settings.platform,
                keep=settings.keep, apply=settings.apply,
                batch_size=settings.batch_size,
                pause_seconds=settings.pause_seconds,
                limit=settings.limit, progress=status.update,
            )
        else:
            stats = await record_hot_window(
                redis, backlog, platform=settings.platform,
                apply=settings.apply, batch_size=settings.batch_size,
                limit=settings.limit, progress=status.update,
            )
        status.finish(stats)
        print(json.dumps({
            'mode': mode, 'step': settings.step,
            'platform': settings.platform, **asdict(stats),
        }, sort_keys=True))
    finally:
        await redis.aclose()


def main() -> int:
    try:
        asyncio.run(run(OffloadSettings()))
    except ValidationError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except (RedisError, PyMongoError) as exc:
        # Connection errors may embed credentials or private endpoints.
        print(f'Database operation failed: {type(exc).__name__}',
              file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print('Interrupted; re-running the step is safe.', file=sys.stderr)
        return 130
    return 0


if __name__ == '__main__':
    sys.exit(main())
