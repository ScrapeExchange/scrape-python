#!/usr/bin/env python3
'''Enqueue all video_ids from a channel file into the Redis
video scrape queue.

The channel file is a brotli-compressed JSON document produced by
``yt_channel_scrape.py`` (or any tool that writes
``YouTubeChannel.to_dict(with_video_ids=True)``). Every entry in
the channel's ``video_ids`` field is added to the back of
``youtube:video:queue`` via
:meth:`RedisVideoScrapeQueue.enqueue`.

The underlying ``enqueue`` is idempotent (ZADD NX): video_ids
already queued or in any terminal state are skipped without
overwriting their record.
'''

from __future__ import annotations

import asyncio
import logging
import re
import sys
from pathlib import Path

import redis.asyncio as aioredis
from pydantic import AliasChoices, Field
from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
)

from scrape_exchange.brotli import brotli_read
from scrape_exchange.redis_client import redis_from_url
from scrape_exchange.video_scrape_queue import (
    RedisVideoScrapeQueue,
    VideoScrapeQueueSettings,
)


_VIDEO_ID_RE: re.Pattern[str] = re.compile(
    r'^[A-Za-z0-9_-]{11}$',
)

_SOURCE_LABEL: str = 'channel-file-import'


class ImportChannelVideosSettings(BaseSettings):
    '''CLI / env / .env settings for the import script.'''

    model_config = SettingsConfigDict(
        env_file=str(
            Path(__file__).parent.parent / '.env',
        ),
        env_file_encoding='utf-8',
        cli_parse_args=True,
        cli_kebab_case=True,
        populate_by_name=True,
        extra='ignore',
    )

    channel_file: str = Field(
        validation_alias=AliasChoices(
            'CHANNEL_FILE', 'channel_file',
        ),
        description=(
            'Path to a brotli-compressed JSON channel file '
            '(e.g. /data/channels/channel-<handle>.json.br) '
            'whose ``video_ids`` field should be enqueued.'
        ),
    )
    redis_dsn: str = Field(
        default='redis://localhost:6379/0',
        validation_alias=AliasChoices(
            'REDIS_DSN', 'redis_dsn',
        ),
        description=(
            'Redis DSN for the video scrape queue. '
            'Production uses redis://localhost:6379/0 '
            'via .env.'
        ),
    )


def _read_video_ids(path: Path) -> list[str]:
    '''Return the channel file's ``video_ids`` as a list.

    Delegates the brotli-decompress + JSON-parse + corruption-
    recovery cycle to :func:`scrape_exchange.brotli.brotli_read`;
    that helper atomically rewrites the file when recovery
    succeeds so subsequent readers take the fast path. We just
    pluck ``video_ids`` from the parsed dict.

    Order in the returned list is not significant; the queue's
    own ZADD score (current time) determines pop order, so all
    imported ids appear behind the existing queue.
    '''
    data: dict = brotli_read(path)
    video_ids: list = list(data.get('video_ids', []))
    return [str(v) for v in video_ids]


async def _enqueue_all(
    video_ids: list[str],
    queue: RedisVideoScrapeQueue,
) -> tuple[int, int, int]:
    '''Enqueue each video id and return counters.

    :returns: ``(enqueued, skipped_invalid, errors)``.
        ``enqueued`` counts successful ``enqueue`` calls.
        Already-queued / terminal-state ids count as enqueued too
        — ZADD NX semantics inside the queue's Lua script make
        the call a no-op for those, but it doesn't raise.
    '''

    enqueued: int = 0
    skipped_invalid: int = 0
    errors: int = 0
    for video_id in video_ids:
        if not _VIDEO_ID_RE.match(video_id):
            logging.warning(
                'Skipping invalid video_id',
                extra={'video_id': video_id},
            )
            skipped_invalid += 1
            continue
        try:
            await queue.enqueue(
                video_id, source=_SOURCE_LABEL,
            )
            enqueued += 1
        except Exception as exc:
            logging.warning(
                'Failed to enqueue video',
                exc_info=exc,
                extra={'video_id': video_id},
            )
            errors += 1
    return enqueued, skipped_invalid, errors


async def main_async() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
    )
    settings: ImportChannelVideosSettings = (
        ImportChannelVideosSettings()
    )

    path: Path = Path(settings.channel_file)
    if not path.is_file():
        sys.stderr.write(
            f'channel file not found: {path}\n',
        )
        return 2

    video_ids: list[str] = _read_video_ids(path)
    logging.info(
        'Read video_ids from channel file',
        extra={
            'channel_file': str(path),
            'video_count': len(video_ids),
        },
    )
    if not video_ids:
        logging.info('Nothing to enqueue; exiting')
        return 0

    redis: aioredis.Redis = redis_from_url(
        settings.redis_dsn,
        component='yt-import-channel-videos',
        decode_responses=True,
    )
    queue: RedisVideoScrapeQueue = (
        RedisVideoScrapeQueue(
            redis, VideoScrapeQueueSettings(),
        )
    )
    try:
        enqueued, skipped, errors = (
            await _enqueue_all(video_ids, queue)
        )
    finally:
        await redis.aclose()

    sys.stdout.write(
        f'enqueued: {enqueued}, '
        f'skipped_invalid: {skipped}, '
        f'errors: {errors}\n',
    )
    return 0 if errors == 0 else 1


def main() -> None:
    raise SystemExit(asyncio.run(main_async()))


if __name__ == '__main__':
    main()
