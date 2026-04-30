#!/usr/bin/env python3

'''
One-shot tool that cleans up the YouTube channel list file
referenced by the ``YOUTUBE_CHANNEL_LIST`` setting.

Cleanup rules:

1. **Lower-case duplicates** — when two or more entries share
   the same value after :py:meth:`str.lower`, keep the one that
   is *not* entirely lower-cased. If every variant in the group
   is already lower-case, the first one is kept.
2. **Entries containing whitespace** — looked up in the YouTube
   :class:`scrape_exchange.name_map.NameMap` (Redis hash
   ``youtube:name_map``). When the display name is unknown the
   entry is removed; otherwise the resolved ``channel_id`` is
   looked up in the YouTube :class:`CreatorMap` (Redis hash
   ``youtube:creator_map``) and replaced by the canonical handle
   when known, or by the bare ``channel_id`` when not.
3. **JSON entries** (``{...}`` or ``[...]``) — kept verbatim.
4. **Bare channel ids** (matching ``UC[A-Z0-9_-]{22}``) — looked
   up in the CreatorMap and replaced by the handle when known,
   kept as the channel id otherwise.
5. **YouTube channel URLs** (``https://www.youtube.com/@h``,
   ``/c/h``, ``/user/h``, ``/channel/UC...`` and the ``m.``,
   ``music.``, http variants) — replaced by the bare handle or
   channel id from the URL path. ``/channel/UC...`` URLs go
   through the same CreatorMap lookup as bare channel ids. The
   extracted form is also used as the dedup key so a URL line
   collapses with the bare-handle line for the same channel.

The original file is copied to ``<path>.bak`` (overwriting any
prior backup) before the cleaned list is written back to the
original path. Comment lines (starting with ``#``) and blank
lines that appear before the first data entry are preserved as a
file header; comments and blank lines elsewhere are dropped.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import logging
import shutil
import sys

from pathlib import Path

from scrape_exchange.channel_list_parsing import (
    dedupe_preserving_case,
    extract_url_canonical,
    is_json_entry,
    looks_like_display_name,
)
from scrape_exchange.creator_map import (
    CreatorMap,
    FileCreatorMap,
    RedisCreatorMap,
)
from scrape_exchange.logging import configure_logging
from scrape_exchange.name_map import (
    NameMap,
    NullNameMap,
    RedisNameMap,
)
from scrape_exchange.youtube.settings import YouTubeScraperSettings
from scrape_exchange.youtube.youtube_channel import YouTubeChannel


_LOGGER: logging.Logger = logging.getLogger(__name__)


def _resolve_named_entry(
    entry: str, name_map: dict[str, str],
    creator_map: dict[str, str],
) -> str | None:
    '''
    Resolve a free-text entry (one containing whitespace) via the
    in-memory NameMap and CreatorMap snapshots. Returns the
    replacement string, or ``None`` when the entry should be
    dropped.
    '''

    channel_id: str | None = name_map.get(entry)
    if channel_id is None:
        return None
    handle: str | None = creator_map.get(channel_id)
    return handle if handle else channel_id


def _classify_named_entry(
    entry: str, name_map: dict[str, str],
    creator_map: dict[str, str],
) -> tuple[str | None, str]:
    '''
    Resolve a display-name entry. Returns
    ``(replacement, count_key)`` where *replacement* is ``None``
    when the entry should be dropped.
    '''

    new: str | None = _resolve_named_entry(
        entry, name_map, creator_map,
    )
    if new is None:
        return None, 'spaces_dropped'
    if YouTubeChannel.is_channel_id(new):
        return new, 'spaces_resolved_channel_id'
    return new, 'spaces_resolved_handle'


def _classify_channel_id_entry(
    entry: str, creator_map: dict[str, str],
) -> tuple[str, str]:
    '''Resolve a bare channel id via the CreatorMap snapshot.'''

    handle: str | None = creator_map.get(entry)
    if handle:
        return handle, 'channel_id_resolved'
    return entry, 'channel_id_kept'


def _classify_url_entry(
    entry: str, creator_map: dict[str, str],
) -> tuple[str, str]:
    '''
    Resolve a YouTube channel URL to its bare canonical form.
    For ``/channel/UC...`` URLs the channel_id is fed through the
    CreatorMap so it gets replaced by the canonical handle when
    known. ``/@h``, ``/c/h``, and ``/user/h`` URLs are returned
    as the bare handle (the CreatorMap is keyed on channel_id, so
    handle-form URLs cannot be resolved further here).
    '''

    canonical: str | None = extract_url_canonical(entry)
    assert canonical is not None  # caller guarantees URL match
    if YouTubeChannel.is_channel_id(canonical):
        handle: str | None = creator_map.get(canonical)
        if handle:
            return handle, 'url_resolved_handle'
        return canonical, 'url_kept_channel_id'
    return canonical, 'url_resolved_handle'


def _classify_entry(
    entry: str, name_map: dict[str, str],
    creator_map: dict[str, str],
) -> tuple[str | None, str]:
    '''
    Apply the per-entry transformation rules. Returns
    ``(replacement, count_key)``; *replacement* is ``None`` when
    the entry should be dropped.
    '''

    if is_json_entry(entry):
        return entry, 'kept_json'
    if extract_url_canonical(entry) is not None:
        return _classify_url_entry(entry, creator_map)
    if looks_like_display_name(entry):
        return _classify_named_entry(entry, name_map, creator_map)
    if YouTubeChannel.is_channel_id(entry):
        return _classify_channel_id_entry(entry, creator_map)
    return entry, 'kept_other'


def _process_entries(
    entries: list[str], name_map: dict[str, str],
    creator_map: dict[str, str],
) -> tuple[list[str], list[str]]:
    '''
    Apply the per-entry transformation rules using in-memory
    snapshots of the NameMap and CreatorMap.

    :returns: a ``(kept, dropped)`` tuple. ``dropped`` contains the
        display-name entries that could not be resolved via the
        NameMap and have been removed from the channel list.
    '''

    out: list[str] = []
    dropped: list[str] = []
    counts: dict[str, int] = {
        'kept_json': 0,
        'kept_other': 0,
        'spaces_resolved_handle': 0,
        'spaces_resolved_channel_id': 0,
        'spaces_dropped': 0,
        'channel_id_resolved': 0,
        'channel_id_kept': 0,
        'url_resolved_handle': 0,
        'url_kept_channel_id': 0,
    }

    for entry in entries:
        replacement: str | None
        count_key: str
        replacement, count_key = _classify_entry(
            entry, name_map, creator_map,
        )
        counts[count_key] += 1
        if replacement is None:
            dropped.append(entry)
            _LOGGER.info(
                'Dropping entry: name not in NameMap',
                extra={'entry': entry},
            )
            continue
        out.append(replacement)

    _LOGGER.info(
        'Per-entry transformation summary', extra=counts,
    )
    return out, dropped


def _append_dropped_entries(
    dropped: list[str], target: Path,
) -> None:
    '''
    Append *dropped* entries to *target*, one per line. The file
    is created if it does not exist. A single trailing newline is
    always present after the appended block.
    '''

    if not dropped:
        return
    needs_leading_newline: bool = False
    if target.exists() and target.stat().st_size > 0:
        with target.open('rb') as fh:
            fh.seek(-1, 2)
            needs_leading_newline = fh.read(1) != b'\n'
    with target.open('a', encoding='utf-8') as fh:
        if needs_leading_newline:
            fh.write('\n')
        fh.write('\n'.join(dropped) + '\n')
    _LOGGER.info(
        'Appended dropped entries to channel_titles file',
        extra={
            'target': str(target),
            'appended_count': len(dropped),
        },
    )


def _split_header_and_entries(
    raw_lines: list[str],
) -> tuple[list[str], list[str]]:
    '''
    Split *raw_lines* into a header (leading comment and blank
    lines, preserved verbatim) and the list of stripped data
    entries that should be processed.
    '''

    header: list[str] = []
    entries: list[str] = []
    in_header: bool = True

    for raw in raw_lines:
        stripped: str = raw.strip()
        if not stripped or stripped.startswith('#'):
            if in_header:
                header.append(raw.rstrip('\n'))
            continue
        in_header = False
        entries.append(stripped)

    return header, entries


async def _run_async(settings: YouTubeScraperSettings) -> int:
    list_path: Path = Path(settings.channel_list)
    if not list_path.is_file():
        _LOGGER.error(
            'Channel list file not found',
            extra={'channel_list': str(list_path)},
        )
        return 1

    name_map_backend: NameMap
    creator_map_backend: CreatorMap
    if settings.redis_dsn:
        name_map_backend = RedisNameMap(
            settings.redis_dsn, platform='youtube',
        )
        creator_map_backend = RedisCreatorMap(
            settings.redis_dsn, platform='youtube',
        )
    else:
        _LOGGER.warning(
            'redis_dsn not set: NameMap is empty (all whitespace '
            'entries will be dropped) and CreatorMap will be read '
            'from channel_map_file',
            extra={
                'channel_map_file': settings.channel_map_file,
            },
        )
        name_map_backend = NullNameMap()
        creator_map_backend = FileCreatorMap(
            settings.channel_map_file,
        )

    name_map: dict[str, str] = await name_map_backend.get_all()
    creator_map: dict[str, str] = (
        await creator_map_backend.get_all()
    )
    _LOGGER.info(
        'Loaded NameMap and CreatorMap into memory',
        extra={
            'name_map_size': len(name_map),
            'creator_map_size': len(creator_map),
        },
    )

    raw_lines: list[str] = list_path.read_text().splitlines()
    header: list[str]
    entries: list[str]
    header, entries = _split_header_and_entries(raw_lines)
    original_count: int = len(entries)

    deduped: list[str] = dedupe_preserving_case(entries)
    transformed: list[str]
    dropped: list[str]
    transformed, dropped = _process_entries(
        deduped, name_map, creator_map,
    )
    final_entries: list[str] = dedupe_preserving_case(transformed)

    backup: Path = list_path.with_suffix(
        list_path.suffix + '.bak',
    )
    shutil.copy2(list_path, backup)
    _LOGGER.info(
        'Wrote backup of original channel list',
        extra={'backup_path': str(backup)},
    )

    output_lines: list[str] = list(header) + final_entries
    tmp_path: Path = list_path.with_suffix(
        list_path.suffix + '.tmp',
    )
    tmp_path.write_text('\n'.join(output_lines) + '\n')
    tmp_path.replace(list_path)

    titles_path: Path = list_path.parent / 'channel_titles.lst'
    _append_dropped_entries(dropped, titles_path)

    print(
        f'Original entries: {original_count}. '
        f'After dedup: {len(deduped)}. '
        f'After transform: {len(transformed)}. '
        f'Final: {len(final_entries)}. '
        f'Dropped (appended to channel_titles.lst): {len(dropped)}.'
    )
    print(f'Backup: {backup}')
    if dropped:
        print(f'Dropped names appended to: {titles_path}')
    return 0


def main() -> None:
    settings: YouTubeScraperSettings = YouTubeScraperSettings()
    configure_logging(
        level=settings.log_level,
        filename=settings.log_file,
        log_format=settings.log_format,
    )
    sys.exit(asyncio.run(_run_async(settings)))


if __name__ == '__main__':
    main()
