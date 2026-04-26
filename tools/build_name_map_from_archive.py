#!/usr/bin/env python3

'''
One-shot tool that walks a channel archive directory, extracts
``(channel_title, channel_id)`` pairs from every readable
``channel-*.json.br`` file, and writes them into the YouTube
NameMap (``youtube:name_map`` Redis hash, last-write-wins).

The NameMap is consumed by ``tools/reingest_from_archive.py`` to
recover a ``channel_id`` from legacy video records that only
carry a display-name in the channel slot. Production scrapers
(``yt_channel_scrape.py``, ``yt_rss_scrape.py``) also write
to the same map at scrape time; this tool exists to backfill the
map from an existing archive.

Defaults to dry-run; pass ``--dry-run false`` to actually write
to Redis. Reads brotli-compressed JSON the same way the re-ingest
tool does, including the lenient streaming decoder for files
with corrupted brotli tails.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import logging
import sys

from pathlib import Path

from pydantic import AliasChoices, Field

from scrape_exchange.logging import configure_logging
from scrape_exchange.name_map import RedisNameMap
from scrape_exchange.youtube.settings import YouTubeScraperSettings

from tools.reingest_from_archive import (
    _enumerate_archive,
    _read_brotli_json,
    _valid_channel_id,
)


_LOGGER: logging.Logger = logging.getLogger(__name__)
_CHANNEL_FILE_PREFIX: str = 'channel-'
_FLUSH_BATCH_SIZE: int = 1000


class BuildNameMapSettings(YouTubeScraperSettings):
    channel_archive_dir: str = Field(
        default='scraped-channels',
        validation_alias=AliasChoices(
            'CHANNEL_ARCHIVE_DIR', 'channel_archive_dir',
        ),
        description=(
            'Directory containing channel-*.json.br files to '
            'mine for (channel_title, channel_id) pairs.'
        ),
    )
    dry_run: bool = Field(
        default=True,
        validation_alias=AliasChoices('DRY_RUN', 'dry_run'),
        description=(
            'When true (the default), parse every file and count '
            'what would be written but do not modify Redis. Use '
            '--dry-run false to actually populate the NameMap.'
        ),
    )


async def _process_one_file(
    path: Path, pending: dict[str, str],
    counters: dict[str, int],
) -> None:
    '''Read *path*, pull the ``(channel_title, channel_id)``
    pair if both are present and the id is valid, and stash it in
    *pending* for the next batch flush.'''

    try:
        data: dict = await _read_brotli_json(path)
    except Exception as exc:
        _LOGGER.warning(
            'Failed to read channel archive file',
            exc=exc, extra={'path': str(path)},
        )
        counters['errors'] += 1
        return

    title: str = (data.get('title') or '').strip()
    channel_id: str | None = _valid_channel_id(
        data.get('channel_id'),
    )
    if not title or not channel_id:
        counters['skipped_missing_field'] += 1
        return

    pending[title] = channel_id
    counters['pairs_seen'] += 1


async def _flush(
    nm: RedisNameMap | None,
    pending: dict[str, str],
    counters: dict[str, int],
    dry_run: bool,
) -> None:
    if not pending:
        return
    if not dry_run and nm is not None:
        await nm.put_many(pending)
    counters['written'] += len(pending)
    pending.clear()


async def _run_async(settings: BuildNameMapSettings) -> int:
    archive: Path = Path(settings.channel_archive_dir)
    if not archive.is_dir():
        _LOGGER.error(
            'Channel archive directory does not exist',
            extra={'channel_archive_dir': str(archive)},
        )
        return 1

    nm: RedisNameMap | None
    if settings.redis_dsn and not settings.dry_run:
        nm = RedisNameMap(
            redis_dsn=settings.redis_dsn, platform='youtube',
        )
    elif settings.redis_dsn and settings.dry_run:
        # In dry-run we still want to surface the count of pairs
        # that would be written; constructing the client is cheap.
        nm = RedisNameMap(
            redis_dsn=settings.redis_dsn, platform='youtube',
        )
    else:
        nm = None

    if nm is None and not settings.dry_run:
        _LOGGER.error(
            'redis_dsn must be set to write the name_map '
            '(or pass --dry-run to count pairs only)',
        )
        return 1

    _LOGGER.info(
        'Enumerating channel archive',
        extra={'channel_archive_dir': str(archive)},
    )
    work: list[str] = _enumerate_archive(
        archive, _CHANNEL_FILE_PREFIX,
    )
    _LOGGER.info(
        'Channel archive enumerated',
        extra={'total_files': len(work)},
    )

    counters: dict[str, int] = {
        'read': 0,
        'pairs_seen': 0,
        'written': 0,
        'skipped_missing_field': 0,
        'errors': 0,
    }
    pending: dict[str, str] = {}

    for path_str in work:
        counters['read'] += 1
        await _process_one_file(
            Path(path_str), pending, counters,
        )
        if len(pending) >= _FLUSH_BATCH_SIZE:
            await _flush(nm, pending, counters, settings.dry_run)

    await _flush(nm, pending, counters, settings.dry_run)

    print(
        f'Channels: read={counters["read"]} '
        f'pairs_seen={counters["pairs_seen"]} '
        f'written={counters["written"]} '
        f'skipped_missing_field={counters["skipped_missing_field"]} '
        f'errors={counters["errors"]}'
    )
    if settings.dry_run:
        print('(dry-run: NameMap was not modified)')
    return 0


def main() -> None:
    settings: BuildNameMapSettings = BuildNameMapSettings()
    configure_logging(
        level=settings.log_level,
        filename=settings.log_file,
        log_format=settings.log_format,
    )
    sys.exit(asyncio.run(_run_async(settings)))


if __name__ == '__main__':
    main()
