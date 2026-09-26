'''Remove video diagnostics after 30 days, retaining terminal membership.

Defaults to dry run. Deploy the tombstone-aware queue to every producer
before using --apply. Run daily to maintain the retention window.
'''

import asyncio
import json
import sys
from dataclasses import asdict
from typing import Literal

import redis.asyncio as aioredis
from pydantic import Field, SecretStr, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict
from redis.exceptions import RedisError

from scrape_exchange.video_queue_retention import (
    RetentionStats,
    VideoQueueRetention,
)


class RetentionSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file='.env', extra='ignore', cli_parse_args=True,
        cli_kebab_case=True, cli_implicit_flags=True,
        hide_input_in_errors=True,
    )

    redis_dsn: SecretStr = Field(
        description='Redis connection URL; prefer the REDIS_DSN environment.',
    )
    platform: Literal['youtube', 'tiktok'] = Field(
        default='youtube', description='Video queue namespace to maintain.',
    )
    apply: bool = Field(
        default=False,
        description='Apply changes; omitted means a read-only dry run.',
    )
    limit: int = Field(
        default=0, ge=0,
        description='Maximum records to inspect; 0 scans all terminal IDs.',
    )
    batch_size: int = Field(
        default=200, ge=1, le=1000,
        description='HSCAN count hint and records between pauses.',
    )
    pause_seconds: float = Field(
        default=0.05, ge=0, allow_inf_nan=False,
        description='Pause between batches to limit server load.',
    )


async def run(settings: RetentionSettings) -> None:
    redis: aioredis.Redis = aioredis.Redis.from_url(
        settings.redis_dsn.get_secret_value(), decode_responses=True,
        socket_connect_timeout=5, socket_timeout=10,
    )
    try:
        retention: VideoQueueRetention = VideoQueueRetention(
            redis, platform=settings.platform,
        )
        stats: RetentionStats = await retention.run(
            apply=settings.apply, limit=settings.limit,
            batch_size=settings.batch_size,
            pause_seconds=settings.pause_seconds,
        )
        print(json.dumps({
            'mode': 'apply' if settings.apply else 'dry-run',
            'platform': settings.platform,
            **asdict(stats),
        }, sort_keys=True))
    finally:
        await redis.aclose()


def main() -> int:
    try:
        asyncio.run(run(RetentionSettings()))
    except ValidationError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except RedisError as exc:
        # Connection exceptions may embed credentials or private endpoints.
        print(f'Redis operation failed: {type(exc).__name__}', file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print('Interrupted; rerunning cleanup is safe.', file=sys.stderr)
        return 130
    return 0


if __name__ == '__main__':
    sys.exit(main())
