#!/usr/bin/env python3

'''
One-shot tool that re-ingests previously-scraped channel and video files
from local archive directories into the new schema vocabulary
(``channel_handle`` for the URL slug, ``title`` for the display name)
so they can be re-uploaded to a freshly-wiped Scrape Exchange server
through the normal ``yt_channel_scrape`` / ``yt_video_scrape``
watch-and-upload pipelines.

For every old-format ``channel-*.json.br`` under
``--channel-archive-dir`` and every ``video-*-*.json.br`` under
``--video-archive-dir`` the tool:

1. Reads the brotli-compressed JSON file.
2. Translates field names — ``channel`` (or ``channel_name``) →
   ``channel_handle``, ``canonical_handle`` collapsed into
   ``channel_handle`` (canonical wins when both are present),
   ``channel_links[].channel_name`` → ``channel_handle``,
   per-video ``channel_name`` → ``channel_handle``,
   per-video ``categories`` (list) → ``category`` (single string).
   ``title`` passes through unchanged.
3. Writes the translated record to the active scraper's base directory
   (``--youtube-channel-data-dir`` / ``--youtube-video-data-dir``) using
   :class:`AssetFileManagement` so collision and prefix-rank handling
   stays consistent with steady-state scraping.
4. Logs counters at the end.

The work is split across ``--num-processes`` worker processes, each
handling a disjoint round-robin slice of the file list. Single-process
mode (``--num-processes 1``) skips the pool entirely so the tool is
just as cheap to run for a small archive as for ten million files.

Defaults to dry-run; pass ``--dry-run false`` to actually write output
files.

Local archive entries are preferred over per-channel/per-video records
on the Scrape Exchange server (the local archives carry ``video_ids``
which the server stripped when serving). Server-side fetch is out of
scope for this tool; pull from there separately if needed.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import concurrent.futures
import logging
import re
import sys

from pathlib import Path

import aiofiles
import brotli
import orjson
from pydantic import AliasChoices, Field

from scrape_exchange.file_management import (
    AssetFileManagement,
    CHANNEL_FILE_PREFIX,
    VIDEO_FILE_PREFIX,
)
from scrape_exchange.logging import configure_logging
from scrape_exchange.youtube.settings import YouTubeScraperSettings


_LOGGER: logging.Logger = logging.getLogger(__name__)

# YouTube's documented handle format: 3–30 characters from the Latin
# alphabet, digits, underscore, hyphen, and period. Anything else
# (whitespace, slashes, &, etc.) means the legacy slot held a display
# name or junk rather than an actual handle, and the record is dropped.
_HANDLE_PATTERN: re.Pattern[str] = re.compile(r'^[A-Za-z0-9_.\-]{3,30}$')


class ReingestSettings(YouTubeScraperSettings):
    channel_archive_dir: str = Field(
        default='scraped-channels',
        validation_alias=AliasChoices(
            'CHANNEL_ARCHIVE_DIR', 'channel_archive_dir',
        ),
        description=(
            'Directory containing the old-format channel-*.json.br '
            'files to re-ingest.'
        ),
    )
    video_archive_dir: str | None = Field(
        default='scraped-videos',
        validation_alias=AliasChoices(
            'VIDEO_ARCHIVE_DIR', 'video_archive_dir',
        ),
        description=(
            'Directory containing the old-format video-*-*.json.br '
            'files to re-ingest. Set empty string to skip video '
            're-ingest.'
        ),
    )
    dry_run: bool = Field(
        default=True,
        validation_alias=AliasChoices('DRY_RUN', 'dry_run'),
        description=(
            'When true (the default), parse and translate every file '
            'but do not write any output. Use --dry-run false to '
            'actually re-ingest.'
        ),
    )
    num_processes: int = Field(
        default=1,
        validation_alias=AliasChoices(
            'NUM_PROCESSES', 'num_processes',
        ),
        description=(
            'Number of worker processes to split the archive across. '
            'Each worker takes a disjoint round-robin slice of the '
            'file list. Defaults to 1 (single-process). For ~10M '
            'files use roughly the host CPU count.'
        ),
    )


# ---------------------------------------------------------------------------
# Translators (pure functions; behaviour pinned by unit tests in
# tests/unit/test_reingest_translators.py)
# ---------------------------------------------------------------------------


def translate_channel(old: dict) -> dict | None:
    '''
    Translate an old-format channel record to the new schema.

    Field-mapping decisions:

    * ``channel_handle`` is set from the first non-empty value of
      ``canonical_handle`` (most authoritative — derived from
      InnerTube's ``vanityChannelUrl``), then ``channel`` (the legacy
      vanity slot, possibly a lowercased fallback), then
      ``channel_name`` (older still). No fallback is derived from
      ``title``: we only re-ingest a channel when we know its
      authoritative handle.
    * The resolved handle must match the YouTube handle format
      (3–30 chars, ``[A-Za-z0-9_.-]``); anything containing
      whitespace or other URL-incompatible characters means the
      slot held a display name or junk and the record is dropped.
    * ``channel_id`` must be present; without both ``channel_handle``
      and ``channel_id`` the record is dropped.
    * ``title`` (the channel display name) passes through unchanged.
    * The obsolete ``canonical_handle`` slot is dropped because the
      collapsed ``channel_handle`` already carries it.
    * ``channel_links[].channel_name`` is renamed to
      ``channel_handle``.

    :returns: The translated dict, or ``None`` when either the
        channel_handle or channel_id could not be determined (caller
        should skip the record and log).
    '''

    handle: str | None = (
        old.get('canonical_handle')
        or old.get('channel')
        or old.get('channel_name')
    )
    if not handle:
        return None
    if not old.get('channel_id'):
        return None

    handle = handle.lstrip('@')
    if not _HANDLE_PATTERN.match(handle):
        return None

    new: dict = dict(old)
    new['channel_handle'] = handle
    # ``title`` now also lives at the same key it always has, so the
    # value already passes through via ``dict(old)`` above. Just drop
    # the legacy handle slots.
    new.pop('canonical_handle', None)
    new.pop('channel', None)
    new.pop('channel_name', None)

    fixed_links: list[dict] = []
    for link in old.get('channel_links', []):
        link_new: dict = dict(link)
        if 'channel_name' in link_new:
            link_new['channel_handle'] = link_new.pop('channel_name')
        fixed_links.append(link_new)
    new['channel_links'] = fixed_links

    return new


def translate_video(old: dict) -> dict | None:
    '''
    Translate an old-format video record to the new schema:

    * ``channel_name`` → ``channel_handle``.
    * ``categories`` (list) → ``category`` (single string). YouTube
      assigns each video a single category; the list-shaped slot was
      over-modelled. The first list entry is taken; an empty list
      becomes ``None``.

    :returns: The translated dict, or ``None`` when either
        ``channel_handle`` or ``channel_id`` is missing — we only
        re-ingest videos whose owning channel is fully identified.
    '''

    new: dict = dict(old)
    if 'channel_name' in new:
        new['channel_handle'] = new.pop('channel_name')
    if 'categories' in new:
        cats = new.pop('categories')
        new['category'] = (
            cats[0] if isinstance(cats, list) and cats else None
        )
    if not new.get('channel_handle') or not new.get('channel_id'):
        return None
    return new


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------


async def _read_brotli_json(path: Path) -> dict:
    async with aiofiles.open(path, 'rb') as f:
        data: bytes = await f.read()
    return orjson.loads(brotli.decompress(data))


# ---------------------------------------------------------------------------
# Per-chunk translators (run inside worker processes)
# ---------------------------------------------------------------------------


async def _translate_channel_chunk(
    work_items: list[str], out_dir: str, dry_run: bool,
) -> dict[str, int]:
    fm: AssetFileManagement = AssetFileManagement(out_dir)
    counters: dict[str, int] = {
        'read': 0, 'written': 0,
        'skipped_no_handle_or_id': 0, 'errors': 0,
    }

    for path_str in work_items:
        src: Path = Path(path_str)
        counters['read'] += 1

        try:
            old: dict = await _read_brotli_json(src)
        except Exception as exc:
            _LOGGER.warning(
                'Failed to read channel archive file',
                exc=exc, extra={'path': path_str},
            )
            counters['errors'] += 1
            continue

        new: dict | None = translate_channel(old)
        if new is None:
            _LOGGER.info(
                'Channel archive file missing channel_handle or '
                'channel_id, skipping',
                extra={'path': path_str},
            )
            counters['skipped_no_handle_or_id'] += 1
            continue

        filename: str = (
            f'{CHANNEL_FILE_PREFIX}{new["channel_handle"]}.json.br'
        )

        if dry_run:
            counters['written'] += 1
            continue

        try:
            await fm.write_file(filename, new)
        except Exception as exc:
            _LOGGER.warning(
                'Failed to write translated channel file',
                exc=exc,
                extra={'path': filename, 'src': path_str},
            )
            counters['errors'] += 1
            continue
        counters['written'] += 1

    return counters


async def _translate_video_chunk(
    work_items: list[str], out_dir: str, dry_run: bool,
) -> dict[str, int]:
    fm: AssetFileManagement = AssetFileManagement(out_dir)
    counters: dict[str, int] = {
        'read': 0, 'written': 0,
        'skipped_no_handle_or_id': 0, 'errors': 0,
    }

    for path_str in work_items:
        src: Path = Path(path_str)
        counters['read'] += 1

        try:
            old: dict = await _read_brotli_json(src)
        except Exception as exc:
            _LOGGER.warning(
                'Failed to read video archive file',
                exc=exc, extra={'path': path_str},
            )
            counters['errors'] += 1
            continue

        new: dict | None = translate_video(old)
        if new is None:
            _LOGGER.info(
                'Video archive file missing channel_handle or '
                'channel_id, skipping',
                extra={'path': path_str},
            )
            counters['skipped_no_handle_or_id'] += 1
            continue
        filename: str = src.name

        if dry_run:
            counters['written'] += 1
            continue

        try:
            await fm.write_file(filename, new)
        except Exception as exc:
            _LOGGER.warning(
                'Failed to write translated video file',
                exc=exc,
                extra={'path': filename, 'src': path_str},
            )
            counters['errors'] += 1
            continue
        counters['written'] += 1

    return counters


# ---------------------------------------------------------------------------
# Worker entrypoints (top-level / pickleable for ProcessPoolExecutor)
# ---------------------------------------------------------------------------


def _install_worker_id_filter(worker_id: int) -> None:
    '''
    Tag every log record emitted in this process with ``worker_id``
    so JSON output can be correlated across the pool.
    '''

    class _WorkerIdFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            record.worker_id = f'w{worker_id}'
            return True

    worker_filter: logging.Filter = _WorkerIdFilter()
    for handler in logging.getLogger().handlers:
        handler.addFilter(worker_filter)


def _channel_worker_entrypoint(
    worker_id: int,
    work_items: list[str],
    out_dir: str,
    dry_run: bool,
    log_level: str,
    log_file: str,
    log_format: str,
) -> dict[str, int]:
    '''
    Top-level (pickleable) channel-chunk worker. Configures logging
    afresh in the child, tags records with ``worker_id``, and
    delegates to :func:`_translate_channel_chunk`.
    '''
    configure_logging(
        level=log_level, filename=log_file, log_format=log_format,
    )
    _install_worker_id_filter(worker_id)
    return asyncio.run(
        _translate_channel_chunk(work_items, out_dir, dry_run),
    )


def _video_worker_entrypoint(
    worker_id: int,
    work_items: list[str],
    out_dir: str,
    dry_run: bool,
    log_level: str,
    log_file: str,
    log_format: str,
) -> dict[str, int]:
    '''
    Top-level (pickleable) video-chunk worker. Configures logging
    afresh in the child, tags records with ``worker_id``, and
    delegates to :func:`_translate_video_chunk`.
    '''
    configure_logging(
        level=log_level, filename=log_file, log_format=log_format,
    )
    _install_worker_id_filter(worker_id)
    return asyncio.run(
        _translate_video_chunk(work_items, out_dir, dry_run),
    )


# ---------------------------------------------------------------------------
# Walk + chunk + dispatch
# ---------------------------------------------------------------------------


def _enumerate_archive(root: Path, prefix: str) -> list[str]:
    '''
    Recursively find every ``<prefix>*.json.br`` file under *root*.
    Strings pickle cheaper than ``Path`` objects across processes.
    '''
    return [
        str(p) for p in root.rglob(f'{prefix}*.json.br') if p.is_file()
    ]


def _merge_counters(
    aggregate: dict[str, int], part: dict[str, int],
) -> None:
    for key, value in part.items():
        aggregate[key] = aggregate.get(key, 0) + value


def _dispatch(
    settings: ReingestSettings,
    label: str,
    work: list[str],
    out_dir: str,
    worker_fn,
) -> dict[str, int]:
    '''
    Split *work* into ``settings.num_processes`` round-robin chunks
    and run *worker_fn* over each. Single-process mode runs inline
    to avoid fork overhead. Returns the aggregated counter dict.
    '''
    if not work:
        _LOGGER.info(
            f'No {label} files to re-ingest', extra={
                'archive': out_dir,
            },
        )
        return {}

    if settings.num_processes <= 1:
        _LOGGER.info(
            f'Translating {label} archive (single process)',
            extra={'total_files': len(work), 'out_dir': out_dir},
        )
        return worker_fn(
            0, work, out_dir, settings.dry_run,
            settings.log_level, settings.log_file,
            settings.log_format,
        )

    chunks: list[list[str]] = [
        work[i::settings.num_processes]
        for i in range(settings.num_processes)
    ]
    chunks = [c for c in chunks if c]
    num_workers: int = len(chunks)

    _LOGGER.info(
        f'Dispatching {label} workers',
        extra={
            'num_workers': num_workers,
            'total_files': len(work),
            'out_dir': out_dir,
            'dry_run': settings.dry_run,
        },
    )

    aggregated: dict[str, int] = {}
    with concurrent.futures.ProcessPoolExecutor(
        max_workers=num_workers,
    ) as pool:
        futures: list[concurrent.futures.Future] = [
            pool.submit(
                worker_fn,
                idx,
                chunk,
                out_dir,
                settings.dry_run,
                settings.log_level,
                settings.log_file,
                settings.log_format,
            )
            for idx, chunk in enumerate(chunks)
        ]
        for fut in concurrent.futures.as_completed(futures):
            _merge_counters(aggregated, fut.result())

    return aggregated


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def _validate(settings: ReingestSettings) -> int:
    if not settings.channel_data_directory:
        _LOGGER.error(
            'channel_data_directory must be configured to write '
            'translated channel files',
        )
        return 1
    if (
        settings.video_archive_dir
        and not settings.video_data_directory
    ):
        _LOGGER.error(
            'video_data_directory must be configured to write '
            'translated video files (or set --video-archive-dir to '
            'an empty string to skip videos)',
        )
        return 1
    if settings.num_processes < 1:
        _LOGGER.error(
            'num_processes must be at least 1',
            extra={'num_processes': settings.num_processes},
        )
        return 1
    return 0


def _run(settings: ReingestSettings) -> int:
    rc: int = _validate(settings)
    if rc != 0:
        return rc

    channel_archive: Path = Path(settings.channel_archive_dir)
    if not channel_archive.is_dir():
        _LOGGER.error(
            'Channel archive directory does not exist',
            extra={'channel_archive_dir': str(channel_archive)},
        )
        return 1

    _LOGGER.info(
        'Enumerating channel archive',
        extra={
            'channel_archive_dir': settings.channel_archive_dir,
        },
    )
    channel_work: list[str] = _enumerate_archive(
        channel_archive, CHANNEL_FILE_PREFIX,
    )
    _LOGGER.info(
        'Channel archive enumerated',
        extra={'total_files': len(channel_work)},
    )

    channel_counters: dict[str, int] = _dispatch(
        settings,
        'channel',
        channel_work,
        settings.channel_data_directory,
        _channel_worker_entrypoint,
    )
    _LOGGER.info(
        'Channel re-ingest complete',
        extra={'counters': channel_counters},
    )

    video_counters: dict[str, int] = {}
    if settings.video_archive_dir:
        video_archive: Path = Path(settings.video_archive_dir)
        if not video_archive.is_dir():
            _LOGGER.error(
                'Video archive directory does not exist',
                extra={'video_archive_dir': str(video_archive)},
            )
            return 1

        _LOGGER.info(
            'Enumerating video archive',
            extra={
                'video_archive_dir': settings.video_archive_dir,
            },
        )
        video_work: list[str] = _enumerate_archive(
            video_archive, VIDEO_FILE_PREFIX,
        )
        _LOGGER.info(
            'Video archive enumerated',
            extra={'total_files': len(video_work)},
        )

        video_counters = _dispatch(
            settings,
            'video',
            video_work,
            settings.video_data_directory,
            _video_worker_entrypoint,
        )
        _LOGGER.info(
            'Video re-ingest complete',
            extra={'counters': video_counters},
        )

    print(
        f'Channels: read={channel_counters.get("read", 0)} '
        f'written={channel_counters.get("written", 0)} '
        f'skipped_no_handle_or_id='
        f'{channel_counters.get("skipped_no_handle_or_id", 0)} '
        f'errors={channel_counters.get("errors", 0)}'
    )
    if video_counters:
        print(
            f'Videos:   read={video_counters.get("read", 0)} '
            f'written={video_counters.get("written", 0)} '
            f'skipped_no_handle_or_id='
            f'{video_counters.get("skipped_no_handle_or_id", 0)} '
            f'errors={video_counters.get("errors", 0)}'
        )
    if settings.dry_run:
        print('(dry-run: no files were written)')

    return 0


def main() -> None:
    settings: ReingestSettings = ReingestSettings()
    configure_logging(
        level=settings.log_level,
        filename=settings.log_file,
        log_format=settings.log_format,
    )
    sys.exit(_run(settings))


if __name__ == '__main__':
    main()
