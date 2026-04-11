#!/usr/bin/env python3

'''
Channel List Cleanup Tool. Reads a YouTube channel list file (one
channel ID or handle per line) and a channel map CSV
(``channel_id,channel_handle``) and produces a new channel list
containing only entries that have not yet been scraped.

For each line in the channel list:

* If the line is a channel handle, the file
  ``channel-<handle>.json.br`` is checked in both the base and
  ``uploaded/`` directories owned by
  :class:`AssetFileManagement`.  When neither copy exists, the handle
  is added to the new list.
* If the line is a channel ID, it is looked up in the channel map
  loaded from ``channel_map_file``.  When present the mapped handle is
  added to the new list; otherwise the channel ID itself is added.

The original channel list file is rotated to
``<channel_list>-YYYYMMDD`` (in the same directory) and the cleaned,
deduplicated list is written back to the path specified by the
``channel_list`` setting.

The channel map CSV is also deduplicated: a backup copy is written
to ``<channel_map>-<YYYYMMDD>`` and, when duplicate channel IDs
exist, the source file is rewritten with a single handle per ID.
The retained handle is the first one in file order that is not all
lowercase, falling back to the first handle if every candidate is
lowercase.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import logging
import os
import shutil
import sys

from datetime import date

import aiofiles

from scrape_exchange.file_management import (
    AssetFileManagement,
    CHANNEL_FILE_PREFIX,
)
from scrape_exchange.logging import configure_logging
from scrape_exchange.youtube.settings import YouTubeScraperSettings
from scrape_exchange.youtube.youtube_channel import YouTubeChannel

CHANNEL_FILE_POSTFIX: str = '.json.br'


def _select_handle(candidates: list[str]) -> str:
    '''
    Pick the best handle from *candidates* (line order preserved).

    Rule: the first handle that is **not** all lowercase wins.  If
    every candidate is all lowercase, the first one is returned.  A
    handle with no cased characters (e.g. ``@123``) counts as "not all
    lowercase" because ``str.islower()`` returns ``False``, which
    matches the intent of preferring handles with preserved casing.
    '''

    for candidate in candidates:
        if not candidate.islower():
            return candidate
    return candidates[0]


async def _load_raw_channel_map(
    file_path: str,
) -> dict[str, list[str]]:
    '''
    Read *file_path* and return a dict mapping channel ID to the list
    of handles seen for that ID, in file order.  Blank and ``#``
    comment lines are skipped.  Lines without a comma use the line
    itself as both channel ID and handle.
    '''

    raw: dict[str, list[str]] = {}
    line: str
    async with aiofiles.open(file_path, 'r') as file_desc:
        async for line in file_desc:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            channel_id: str
            channel_handle: str
            if ',' in line:
                channel_id, channel_handle = line.split(',', 1)
                channel_id = channel_id.strip()
                channel_handle = channel_handle.strip()
            else:
                channel_id = line
                channel_handle = line
            raw.setdefault(channel_id, []).append(channel_handle)
    return raw


def _dedup_raw_channel_map(
    raw: dict[str, list[str]],
) -> tuple[dict[str, str], int]:
    '''
    Collapse *raw* to one handle per channel ID via
    :func:`_select_handle`.  Returns the cleaned map and the number of
    channel IDs that had more than one candidate handle.
    '''

    cleaned: dict[str, str] = {
        cid: _select_handle(handles) for cid, handles in raw.items()
    }
    duplicate_ids: int = sum(
        1 for handles in raw.values() if len(handles) > 1
    )
    return cleaned, duplicate_ids


async def read_channel_map(file_path: str) -> dict[str, str]:
    '''
    Load a channel map CSV mapping channel IDs to channel handles.

    Lines are expected to have the form ``channel_id,channel_handle``.
    Blank lines and ``#`` comments are skipped.  Lines without a comma
    are treated as both the key and the value.

    When the file contains multiple rows for the same channel ID, the
    handle is chosen via :func:`_select_handle`: the first handle that
    is not all lowercase wins, falling back to the first handle when
    every candidate is lowercase.

    :param file_path: Path to the channel map file.
    :returns: Dict mapping channel IDs to channel handles.  Empty dict
        when *file_path* does not exist.
    '''

    if not os.path.isfile(file_path):
        logging.warning(
            'Channel map file does not exist; treating as empty',
            extra={'file_path': file_path},
        )
        return {}

    raw: dict[str, list[str]] = await _load_raw_channel_map(file_path)
    channel_map: dict[str, str]
    duplicate_ids: int
    channel_map, duplicate_ids = _dedup_raw_channel_map(raw)

    logging.info(
        'Loaded channel map',
        extra={
            'file_path': file_path,
            'entries': len(channel_map),
            'duplicate_ids': duplicate_ids,
        },
    )
    return channel_map


async def clean_channel_map(file_path: str) -> dict[str, str]:
    '''
    Back up, deduplicate, and rewrite the channel map at *file_path*.

    When *file_path* does not exist, nothing is written and an empty
    dict is returned.  When it exists a copy is written to
    ``<file_path>-<YYYYMMDD>`` first; only if duplicates were actually
    collapsed is the source file then overwritten with the cleaned
    map.  The returned dict is the cleaned, in-memory view callers
    should use for the rest of the run.
    '''

    if not os.path.isfile(file_path):
        logging.warning(
            'Channel map file does not exist; skipping cleanup',
            extra={'file_path': file_path},
        )
        return {}

    raw: dict[str, list[str]] = await _load_raw_channel_map(file_path)
    channel_map: dict[str, str]
    duplicate_ids: int
    channel_map, duplicate_ids = _dedup_raw_channel_map(raw)
    logging.info(
        'Loaded channel map for cleanup',
        extra={
            'file_path': file_path,
            'entries': len(channel_map),
            'duplicate_ids': duplicate_ids,
        },
    )

    backup_channel_map(file_path)
    if duplicate_ids > 0:
        await write_channel_map(file_path, channel_map)
    else:
        logging.info(
            'Channel map has no duplicates; skipping rewrite',
            extra={'file_path': file_path},
        )
    return channel_map


def backup_channel_map(file_path: str) -> str | None:
    '''
    Copy *file_path* to ``<file_path>-<YYYYMMDD>`` so the original map
    is preserved before the cleanup rewrites it.  Returns the backup
    path, or ``None`` when *file_path* does not exist.  An existing
    backup at today's date is overwritten.
    '''

    if not os.path.isfile(file_path):
        logging.warning(
            'Channel map file does not exist; skipping backup',
            extra={'file_path': file_path},
        )
        return None

    suffix: str = date.today().strftime('%Y%m%d')
    backup_path: str = f'{file_path}-{suffix}'
    if os.path.exists(backup_path):
        logging.warning(
            'Channel map backup already exists; overwriting',
            extra={'backup_path': backup_path},
        )
    shutil.copy2(file_path, backup_path)
    logging.info(
        'Backed up channel map',
        extra={
            'source_path': file_path,
            'backup_path': backup_path,
        },
    )
    return backup_path


async def write_channel_map(
    file_path: str, channel_map: dict[str, str],
) -> None:
    '''
    Write *channel_map* to *file_path* as ``channel_id,channel_handle``
    rows, preserving insertion order of the dict.
    '''

    async with aiofiles.open(file_path, 'w') as file_desc:
        for channel_id, channel_handle in channel_map.items():
            await file_desc.write(
                f'{channel_id},{channel_handle}\n'
            )
    logging.info(
        'Wrote cleaned channel map',
        extra={
            'file_path': file_path,
            'entries': len(channel_map),
        },
    )


def channel_file_exists(fm: AssetFileManagement, handle: str) -> bool:
    '''
    Return ``True`` if *handle* should be considered already handled:
    either a scraped data file ``channel-<handle>.json.br`` exists in
    the base or ``uploaded/`` directory, or a ``.not_found`` /
    ``.unresolved`` marker file exists in the base directory.
    '''

    filename: str = f'{CHANNEL_FILE_PREFIX}{handle}{CHANNEL_FILE_POSTFIX}'
    if (fm.base_dir / filename).exists():
        return True
    if (fm.uploaded_dir / filename).exists():
        return True

    marker_stem: str = f'{CHANNEL_FILE_PREFIX}{handle}'
    for marker_suffix in ('.not_found', '.unresolved'):
        if (fm.base_dir / f'{marker_stem}{marker_suffix}').exists():
            return True
    return False


def classify_line(
    line: str, channel_map: dict[str, str], fm: AssetFileManagement,
) -> str | None:
    '''
    Resolve a single channel-list line to the entry that should be
    carried forward, or ``None`` when the line should be dropped.
    '''

    if YouTubeChannel.is_channel_id(line):
        if line in channel_map:
            entry: str = channel_map[line]
            logging.debug(
                'Resolved channel ID via channel map',
                extra={'channel_id': line, 'channel_handle': entry},
            )
            return entry
        logging.debug(
            'Channel ID not in channel map; keeping ID',
            extra={'channel_id': line},
        )
        return line

    if channel_file_exists(fm, line):
        logging.debug(
            'Channel handle already scraped; dropping',
            extra={'channel_handle': line},
        )
        return None
    return line


async def build_new_channel_list(
    channel_list_path: str, channel_map: dict[str, str],
    fm: AssetFileManagement,
) -> list[str]:
    '''
    Read *channel_list_path* and return the deduplicated list of
    channel handles / IDs that should be carried forward into the new
    channel list.

    Order is preserved relative to the first occurrence in the source
    file.
    '''

    new_channels: list[str] = []
    seen: set[str] = set()

    line: str
    async with aiofiles.open(channel_list_path, 'r') as file_desc:
        async for line in file_desc:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            entry: str | None = classify_line(line, channel_map, fm)
            if entry and entry not in seen:
                seen.add(entry)
                new_channels.append(entry)

    logging.info(
        'Built new channel list',
        extra={
            'source_path': channel_list_path,
            'unique_entries': len(new_channels),
        },
    )
    return new_channels


async def rotate_and_write(
    channel_list_path: str, new_channels: list[str],
) -> None:
    '''
    Rotate *channel_list_path* to ``<channel_list_path>-YYYYMMDD`` and
    write *new_channels* (one entry per line) to a freshly created
    file at *channel_list_path*.
    '''

    suffix: str = date.today().strftime('%Y%m%d')
    rotated_path: str = f'{channel_list_path}-{suffix}'
    if os.path.exists(rotated_path):
        logging.warning(
            'Rotated path already exists; overwriting',
            extra={'rotated_path': rotated_path},
        )
    os.replace(channel_list_path, rotated_path)
    logging.info(
        'Rotated channel list',
        extra={
            'source_path': channel_list_path,
            'rotated_path': rotated_path,
        },
    )

    async with aiofiles.open(channel_list_path, 'w') as file_desc:
        for entry in new_channels:
            await file_desc.write(f'{entry}\n')

    logging.info(
        'Wrote new channel list',
        extra={
            'channel_list_path': channel_list_path,
            'entries': len(new_channels),
        },
    )


async def main() -> None:
    '''
    Entry point for the channel cleanup tool.
    '''

    settings: YouTubeScraperSettings = YouTubeScraperSettings()
    configure_logging(
        level=settings.log_level,
        filename=settings.log_file,
        log_format=settings.log_format,
    )

    if not settings.channel_list:
        print(
            'Error: channel list file must be provided via '
            '--channel-list or environment variable YOUTUBE_CHANNEL_LIST'
        )
        sys.exit(1)
    if not settings.channel_data_directory:
        print(
            'Error: channel data directory must be provided via '
            '--channel-data-directory or environment variable '
            'YOUTUBE_CHANNEL_DATA_DIR'
        )
        sys.exit(1)
    if not os.path.isfile(settings.channel_list):
        print(
            f'Error: channel list file {settings.channel_list} '
            'does not exist'
        )
        sys.exit(1)
    if not os.path.isdir(settings.channel_data_directory):
        print(
            f'Error: channel data directory '
            f'{settings.channel_data_directory} does not exist'
        )
        sys.exit(1)

    logging.info(
        'Starting channel cleanup tool',
        extra={'settings': settings.model_dump()},
    )

    fm: AssetFileManagement = AssetFileManagement(
        settings.channel_data_directory
    )

    channel_map: dict[str, str] = await clean_channel_map(
        settings.channel_map_file
    )
    new_channels: list[str] = await build_new_channel_list(
        settings.channel_list, channel_map, fm
    )
    await rotate_and_write(settings.channel_list, new_channels)


if __name__ == '__main__':
    asyncio.run(main())
