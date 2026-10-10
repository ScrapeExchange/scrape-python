'''Keep the Redis hot window of the video scrape queue filled from the
MongoDB backlog.

With MONGO_DSN set, producers add videos to MongoDB and the video
scrapers pop from Redis. This service copies the oldest queued videos
from MongoDB into Redis whenever the Redis queue drops below the low
watermark, up to the high watermark. Run exactly one instance per
platform; a second one is harmless (writes are idempotent) but wasted.

Example:

    REDIS_DSN=redis://... MONGO_DSN=mongodb://... \\
        tools/yt_video_queue_refill.py --platform youtube
'''

import asyncio
import logging
import signal
import sys
from typing import Literal

from pydantic import AliasChoices, Field, SecretStr, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

from scrape_exchange.logging import configure_logging
from scrape_exchange.metrics_server import start_metrics_server
from scrape_exchange.redis_client import redis_from_url
from scrape_exchange.video_backlog import MongoVideoBacklog
from scrape_exchange.video_queue_refill import (
    METRIC_ERRORS,
    ReconcileStats,
    VideoQueueRefill,
)

_LOGGER: logging.Logger = logging.getLogger('yt_video_queue_refill')


class RefillSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file='.env', extra='ignore', cli_parse_args=True,
        cli_kebab_case=True, cli_implicit_flags=True,
        hide_input_in_errors=True, populate_by_name=True,
    )

    redis_dsn: SecretStr = Field(
        validation_alias=AliasChoices('redis_dsn', 'REDIS_DSN'),
        description='Redis connection URL of the video queue.',
    )
    mongo_dsn: SecretStr = Field(
        validation_alias=AliasChoices('mongo_dsn', 'MONGO_DSN'),
        description='MongoDB connection URL of the video backlog.',
    )
    platform: Literal['youtube', 'tiktok'] = Field(
        default='youtube',
        description='Video queue namespace to keep filled.',
    )
    low_watermark: int = Field(
        default=10_000_000, ge=1,
        validation_alias=AliasChoices(
            'low_watermark', 'VIDEO_QUEUE_LOW_WATERMARK',
        ),
        description='Refill when Redis holds fewer waiting videos.',
    )
    high_watermark: int = Field(
        default=20_000_000, ge=1,
        validation_alias=AliasChoices(
            'high_watermark', 'VIDEO_QUEUE_HIGH_WATERMARK',
        ),
        description='Refill up to this many waiting videos.',
    )
    batch_size: int = Field(
        default=10_000, ge=1, le=100_000,
        description='Videos per MongoDB read and Redis pipeline.',
    )
    mark_hot_chunk_size: int = Field(
        default=1_000, ge=1, le=100_000,
        validation_alias=AliasChoices(
            'mark_hot_chunk_size', 'VIDEO_QUEUE_MARK_HOT_CHUNK_SIZE',
        ),
        description='Videos per MongoDB update when marking a batch hot.',
    )
    mark_hot_concurrency: int = Field(
        default=8, ge=1, le=64,
        validation_alias=AliasChoices(
            'mark_hot_concurrency', 'VIDEO_QUEUE_MARK_HOT_CONCURRENCY',
        ),
        description='MongoDB mark-hot updates in flight at once.',
    )
    interval_seconds: float = Field(
        default=30.0, gt=0,
        description='Seconds between hot-window checks.',
    )
    reconcile_on_start: bool = Field(
        default=True,
        description=(
            'On start, return hot backlog documents missing from '
            'Redis (e.g. lost in a Redis restart) to the backlog.'
        ),
    )
    metrics_port: int = Field(
        default=9560,
        validation_alias=AliasChoices(
            'metrics_port', 'REFILL_METRICS_PORT',
        ),
        description='Port for the Prometheus metrics endpoint.',
    )
    log_level: str = Field(
        default='INFO',
        validation_alias=AliasChoices('log_level', 'LOG_LEVEL'),
        description='Logging level.',
    )
    log_file: str = Field(
        default='/dev/stdout',
        validation_alias=AliasChoices('log_file', 'LOG_FILE'),
        description='Log file path; /dev/stdout logs to the console.',
    )
    log_format: Literal['json', 'text'] = Field(
        default='json',
        validation_alias=AliasChoices('log_format', 'LOG_FORMAT'),
        description='Log record format.',
    )


async def run(settings: RefillSettings) -> None:
    if settings.low_watermark > settings.high_watermark:
        raise ValueError('low watermark exceeds high watermark')
    configure_logging(
        level=settings.log_level, filename=settings.log_file,
        log_format=settings.log_format,
    )
    try:
        start_metrics_server(settings.metrics_port)
    except OSError:
        _LOGGER.warning(
            'Could not start metrics server; continuing without',
            extra={'metrics_port': settings.metrics_port},
        )
    redis = redis_from_url(
        settings.redis_dsn.get_secret_value(),
        component=f'{settings.platform}-video-queue-refill',
        decode_responses=True,
    )
    backlog: MongoVideoBacklog = MongoVideoBacklog.from_dsn(
        settings.mongo_dsn.get_secret_value(), platform=settings.platform,
    )
    refill: VideoQueueRefill = VideoQueueRefill(
        redis, backlog, platform=settings.platform,
        low_watermark=settings.low_watermark,
        high_watermark=settings.high_watermark,
        batch_size=settings.batch_size,
        mark_hot_chunk_size=settings.mark_hot_chunk_size,
        mark_hot_concurrency=settings.mark_hot_concurrency,
    )
    stop: asyncio.Event = asyncio.Event()
    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
    sig: signal.Signals
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    _LOGGER.info(
        'Video queue refill started',
        extra={
            'platform': settings.platform,
            'low_watermark': settings.low_watermark,
            'high_watermark': settings.high_watermark,
        },
    )
    try:
        if settings.reconcile_on_start:
            stats: ReconcileStats = await refill.reconcile()
            _LOGGER.info(
                'Reconciled hot backlog documents with Redis',
                extra={
                    'platform': settings.platform,
                    'checked': stats.checked,
                    'requeued': stats.requeued,
                },
            )
        while not stop.is_set():
            try:
                await refill.refill_once()
                await refill.publish_backlog_sizes()
            except Exception:
                METRIC_ERRORS.labels(platform=settings.platform).inc()
                _LOGGER.exception(
                    'Video queue refill iteration failed',
                    extra={'platform': settings.platform},
                )
            try:
                await asyncio.wait_for(
                    stop.wait(), timeout=settings.interval_seconds,
                )
            except TimeoutError:
                pass
    finally:
        await redis.aclose()
    _LOGGER.info('Video queue refill stopped')


def main() -> int:
    try:
        asyncio.run(run(RefillSettings()))
    except ValidationError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    return 0


if __name__ == '__main__':
    sys.exit(main())
