#!/usr/bin/env python3

'''Seed the Redis uploaded-video-ID SET from a newline list.'''

from __future__ import annotations

import asyncio
import logging
import sys

from pathlib import Path
from typing import ClassVar

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from scrape_exchange.youtube.uploaded_video_ids import UploadedVideoIds


_LOGGER: logging.Logger = logging.getLogger(__name__)


class UploadedIdsMigrateSettings(BaseSettings):
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        env_file='.env',
        extra='ignore',
        cli_parse_args=True,
        cli_kebab_case=True,
    )

    uploaded_video_list: str = Field(
        default='data/uploaded_videos.lst',
        description='Newline-delimited uploaded video ID source file.',
    )
    redis_dsn: str | None = Field(
        default=None,
        description='Redis DSN for the uploaded video ID SET.',
    )
    batch_size: int = Field(
        default=10000,
        gt=0,
        description='IDs added to Redis per SADD batch.',
    )


def _count_lines(path: Path) -> int:
    with path.open('r', encoding='utf-8') as fh:
        return sum(1 for _ in fh)


async def migrate_uploaded_ids(
    settings: UploadedIdsMigrateSettings,
) -> tuple[int, int]:
    '''Seed Redis and return ``(file_lines, redis_cardinality)``.'''
    if not settings.redis_dsn:
        raise ValueError('redis_dsn is required')
    path: Path = Path(settings.uploaded_video_list)
    file_lines: int = _count_lines(path)
    uploaded: UploadedVideoIds = UploadedVideoIds(
        settings.redis_dsn,
    )
    initial_redis_count: int = int(
        await uploaded._client.scard(uploaded._KEY),
    )
    seeded: int = 0
    added: int = 0
    batch: list[str] = []
    with path.open('r', encoding='utf-8') as fh:
        for line in fh:
            value: str = line.strip()
            if value:
                batch.append(value)
            if len(batch) >= settings.batch_size:
                added += await _sadd_batch(uploaded, batch)
                seeded += len(batch)
                batch.clear()
                _log_progress(seeded, file_lines)
    if batch:
        added += await _sadd_batch(uploaded, batch)
        seeded += len(batch)
        _log_progress(seeded, file_lines)

    redis_count: int = int(
        await uploaded._client.scard(uploaded._KEY),
    )
    _LOGGER.info(
        'migration complete file_lines=%s seeded=%s added=%s '
        'initial_redis_scard=%s redis_scard=%s',
        file_lines,
        seeded,
        added,
        initial_redis_count,
        redis_count,
    )
    expected_minimum: int = initial_redis_count + added
    if redis_count < expected_minimum:
        raise RuntimeError(
            'uploaded-video-ID Redis cardinality fell below the '
            'migration adds: '
            f'initial_redis_scard={initial_redis_count} '
            f'added={added} redis_scard={redis_count}',
        )
    return file_lines, redis_count


async def _sadd_batch(
    uploaded: UploadedVideoIds,
    batch: list[str],
) -> int:
    pipeline = uploaded._client.pipeline(transaction=False)
    pipeline.sadd(uploaded._KEY, *batch)
    return int((await pipeline.execute())[0])


def _log_progress(seeded: int, file_lines: int) -> None:
    fraction: float = 1.0 if file_lines == 0 else seeded / file_lines
    _LOGGER.info(
        'seeded %s/%s (%.0f%%)',
        seeded,
        file_lines,
        fraction * 100,
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings: UploadedIdsMigrateSettings = UploadedIdsMigrateSettings()
    try:
        asyncio.run(migrate_uploaded_ids(settings))
    except Exception:
        _LOGGER.exception('uploaded-video-ID migration failed')
        sys.exit(1)


if __name__ == '__main__':
    main()
