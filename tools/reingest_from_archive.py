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
from urllib.parse import unquote

import aiofiles
import brotli
import orjson
from pydantic import AliasChoices, Field

from scrape_exchange.creator_map import RedisCreatorMap
from scrape_exchange.file_management import (
    AssetFileManagement,
    CHANNEL_FILE_PREFIX,
    VIDEO_FILE_PREFIX,
)
from scrape_exchange.logging import configure_logging
from scrape_exchange.youtube.settings import YouTubeScraperSettings


_LOGGER: logging.Logger = logging.getLogger(__name__)

# YouTube handle format: 3–30 characters. YouTube's documented
# allowlist (Latin letters, digits, ``_.-``) is far too strict in
# practice — confirmed accepted handles include ``@ГеоргиГеоргиев-ь4ч``
# (Cyrillic), ``@ÁlexMontoya`` (accented Latin), Greek scripts, and
# even symbols like ``@THENvsNOW™``. So we use a denylist instead:
# whitespace and URL-structural characters (``/``, ``?``, ``#``,
# ``\``, ``@``) are the only definitive rejects. A legacy slot
# holding ``"My Channel Name"`` (whitespace) or ``"foo/bar"`` (slash)
# still gets dropped as junk; pretty much anything else is accepted
# and the file is migrated.
_HANDLE_PATTERN: re.Pattern[str] = re.compile(
    r'^[^\s/?#\\@]{3,30}$',
)

# YouTube channel IDs are always ``UC`` followed by 22 characters from
# the URL-safe base64 alphabet. Mirrors
# ``YouTubeChannel.CHANNEL_ID_REGEX_MATCH``. Records carrying a
# malformed channel_id (e.g. truncated, a display name, or a video
# id) are dropped — re-ingesting them would pollute the data dir
# with files keyed by junk.
_CHANNEL_ID_PATTERN: re.Pattern[str] = re.compile(
    r'^UC[A-Z0-9_-]{22}$', re.IGNORECASE,
)

# YouTube video IDs are exactly 11 characters from the URL-safe
# base64 alphabet. A record with a missing or malformed video_id
# can't be uniquely keyed and is dropped.
_VIDEO_ID_PATTERN: re.Pattern[str] = re.compile(
    r'^[A-Za-z0-9_-]{11}$',
)


class ReingestSettings(YouTubeScraperSettings):
    channel_archive_dir: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'CHANNEL_ARCHIVE_DIR', 'channel_archive_dir',
        ),
        description=(
            'Directory containing the old-format channel-*.json.br '
            'files to re-ingest. Leave unset (or set to an empty '
            'string) to skip channel re-ingest.'
        ),
    )
    video_archive_dir: str | None = Field(
        default=None,
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


def _valid_handle(value: str | None) -> str | None:
    '''Return *value* when it matches ``_HANDLE_PATTERN``, else None.'''

    if not value:
        return None
    return value if _HANDLE_PATTERN.match(value) else None


def _valid_channel_id(value: str | None) -> str | None:
    '''Return *value* when it matches ``_CHANNEL_ID_PATTERN``, else
    None.'''

    if not value:
        return None
    return value if _CHANNEL_ID_PATTERN.match(value) else None


def _valid_video_id(value: str | None) -> str | None:
    '''Return *value* when it matches ``_VIDEO_ID_PATTERN``, else
    None.'''

    if not value:
        return None
    return value if _VIDEO_ID_PATTERN.match(value) else None


def _resolve_handle_and_id(
    raw_handle: str | None,
    raw_channel_id: str | None,
    id_to_handle: dict[str, str],
    handle_to_id: dict[str, str],
) -> tuple[str | None, str | None]:
    '''
    Resolve a ``(channel_handle, channel_id)`` pair from the raw
    file values, falling back to the CreatorMap when either side
    is missing or the in-file handle fails ``_HANDLE_PATTERN``.

    Resolution rules:

    * The candidate handle is taken from *raw_handle*, ``@``-stripped,
      and accepted only if it matches ``_HANDLE_PATTERN``. A junk
      in-file handle is treated as missing so the CreatorMap can
      override it (per project policy: when both are present and the
      file handle is junk, the map wins).
    * When the handle is missing/junk and a *raw_channel_id* is
      present, look up the handle via *id_to_handle*. The mapped
      handle is also held to ``_HANDLE_PATTERN``: a non-conforming
      mapped handle is rejected and the pair stays unresolved.
    * When *raw_channel_id* is missing and a valid handle is in hand,
      look up the channel_id via *handle_to_id*.

    :returns: ``(handle, channel_id)`` with either side ``None`` when
        unresolved. Caller decides whether to drop the record.
    '''

    # Some legacy archive files store the handle URL-encoded
    # (e.g. ``%ce%95%ce%ba%ce%b4...`` for the Greek handle
    # ``Εκδόσεις...``). ``unquote`` is a no-op on plain strings so
    # it's safe to apply unconditionally; this only affects file
    # values, not CreatorMap-fetched handles which are written in
    # canonical form by the creator scraper.
    handle: str | None = _valid_handle(
        unquote(raw_handle.lstrip('@')) if raw_handle else None,
    )
    channel_id: str | None = _valid_channel_id(raw_channel_id)

    if not handle and channel_id:
        handle = _valid_handle(id_to_handle.get(channel_id))

    if not channel_id and handle:
        channel_id = _valid_channel_id(handle_to_id.get(handle))

    return handle, channel_id


def translate_channel(
    old: dict,
    id_to_handle: dict[str, str] | None = None,
    handle_to_id: dict[str, str] | None = None,
) -> dict | None:
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
      slot held a display name or junk and the record is dropped
      unless the CreatorMap can supply a valid handle for the
      ``channel_id``.
    * When either ``channel_handle`` or ``channel_id`` is missing,
      the *id_to_handle* / *handle_to_id* maps are consulted to fill
      the gap. Pass empty maps (the default) to disable lookup and
      fall back to file-only resolution.
    * ``title`` (the channel display name) passes through unchanged.
    * The obsolete ``canonical_handle`` slot is dropped because the
      collapsed ``channel_handle`` already carries it.
    * ``channel_links[].channel_name`` is renamed to
      ``channel_handle``.

    :returns: The translated dict, or ``None`` when either the
        channel_handle or channel_id could not be determined (caller
        should skip the record and log).
    '''

    id_to_handle = id_to_handle or {}
    handle_to_id = handle_to_id or {}

    raw_handle: str | None = (
        old.get('canonical_handle')
        or old.get('channel')
        or old.get('channel_name')
    )
    handle: str | None
    channel_id: str | None
    handle, channel_id = _resolve_handle_and_id(
        raw_handle, old.get('channel_id'),
        id_to_handle, handle_to_id,
    )
    if not handle or not channel_id:
        return None

    new: dict = dict(old)
    new['channel_handle'] = handle
    new['channel_id'] = channel_id
    # ``title`` now also lives at the same key it always has, so the
    # value already passes through via ``dict(old)`` above. Just drop
    # the legacy handle slots.
    new.pop('canonical_handle', None)
    new.pop('channel', None)
    new.pop('channel_name', None)
    # Channels carry an optional ``category`` (single string) in the
    # new schema. Some legacy records used a ``categories`` list slot
    # over-modelled from the video shape; collapse to the first entry
    # (or ``None`` when the list is empty).
    if 'categories' in new:
        cats = new.pop('categories')
        new['category'] = (
            cats[0] if isinstance(cats, list) and cats else None
        )

    fixed_links: list[dict] = []
    for link in old.get('channel_links', []):
        link_new: dict = dict(link)
        if 'channel_name' in link_new:
            link_new['channel_handle'] = link_new.pop('channel_name')
        fixed_links.append(link_new)
    new['channel_links'] = fixed_links

    return new


def translate_video(
    old: dict,
    id_to_handle: dict[str, str] | None = None,
    handle_to_id: dict[str, str] | None = None,
) -> dict | None:
    '''
    Translate an old-format video record to the new schema:

    * ``channel_name`` → ``channel_handle``.
    * ``categories`` (list) → ``category`` (single string). YouTube
      assigns each video a single category; the list-shaped slot was
      over-modelled. The first list entry is taken; an empty list
      becomes ``None``.
    * The resolved handle must match ``_HANDLE_PATTERN``. When
      either ``channel_handle`` or ``channel_id`` is missing or the
      handle is junk, the *id_to_handle* / *handle_to_id* maps
      (typically the YouTube CreatorMap) are consulted to fill the
      gap.

    Required fields for the record to be kept:

    * ``video_id`` (must match ``_VIDEO_ID_PATTERN``)
    * ``channel_id`` (must match ``_CHANNEL_ID_PATTERN``, either
      from the file or recovered via reverse handle lookup)

    ``channel_handle`` is *not* required: a record with a valid
    ``channel_id`` but an invalid file handle and no CreatorMap
    entry is still kept, with ``channel_handle`` stripped from
    the output. The video data is useful even when the canonical
    handle isn't yet known; downstream consumers can fill it in
    later once the channel is scraped.

    :returns: The translated dict, or ``None`` when ``video_id``
        or ``channel_id`` is missing/invalid.
    '''

    id_to_handle = id_to_handle or {}
    handle_to_id = handle_to_id or {}

    new: dict = dict(old)
    if 'channel_name' in new:
        new['channel_handle'] = new.pop('channel_name')
    if 'categories' in new:
        cats = new.pop('categories')
        new['category'] = (
            cats[0] if isinstance(cats, list) and cats else None
        )

    video_id: str | None = _valid_video_id(new.get('video_id'))
    if not video_id:
        return None

    handle: str | None
    channel_id: str | None
    handle, channel_id = _resolve_handle_and_id(
        new.get('channel_handle'), new.get('channel_id'),
        id_to_handle, handle_to_id,
    )
    if not channel_id:
        return None
    new['video_id'] = video_id
    new['channel_id'] = channel_id
    if handle:
        new['channel_handle'] = handle
    else:
        new.pop('channel_handle', None)
    # Fill in the canonical watch URL when the source record didn't
    # carry one — every YouTube video has the same URL shape and
    # downstream consumers expect the field to be present.
    if not new.get('url'):
        new['url'] = f'https://www.youtube.com/watch?v={video_id}'
    return new


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------


# Marker keys that indicate a record came from the scrape.exchange
# server: the actual scraped payload sits inside ``data`` and the
# envelope wraps it with platform/source metadata. At least one of
# these must be present at the top level for auto-unwrap to fire,
# so a scraper-format file that happens to carry a ``data`` field
# is left alone.
_SERVER_ENVELOPE_KEYS: frozenset[str] = frozenset({
    'platform_content_id',
    'platform_creator_id',
    'source_url',
})


def _unwrap_server_envelope(parsed: dict) -> dict:
    '''
    Return the inner record when *parsed* looks like a
    scrape.exchange server response, else *parsed* unchanged.

    Detection requires both a ``data`` key whose value is a dict
    and at least one of the known envelope marker keys at the top
    level. Auto-detection (rather than a CLI flag) lets a single
    archive directory mix old-format scraper files and server-
    format files without operator intervention.
    '''

    inner: object = parsed.get('data')
    if not isinstance(inner, dict):
        return parsed
    if not _SERVER_ENVELOPE_KEYS.intersection(parsed):
        return parsed
    return inner


async def _read_brotli_json(path: Path) -> dict:
    async with aiofiles.open(path, 'rb') as f:
        data: bytes = await f.read()
    parsed: dict = orjson.loads(brotli.decompress(data))
    return _unwrap_server_envelope(parsed)


async def _load_creator_maps(
    redis_dsn: str | None,
) -> tuple[dict[str, str], dict[str, str], RedisCreatorMap | None]:
    '''
    Load the YouTube CreatorMap from Redis once per worker and return
    the forward (channel_id → channel_handle) and reverse
    (channel_handle → channel_id) lookup tables alongside the
    :class:`RedisCreatorMap` instance itself. The translators use the
    in-memory tables to fill in records that carry only one of the
    two identifiers; the chunk loops use the instance to back-write
    newly-discovered ``(channel_id, channel_handle)`` pairs that
    weren't already in the map.

    With ~150k entries the full map fits comfortably in memory and a
    single ``HGETALL`` is dramatically cheaper than per-record
    round-trips against millions of archive files.

    :param redis_dsn: Redis DSN. When ``None`` or empty (e.g. when
        ``REDIS_DSN`` is unset in the environment), returns empty
        maps and ``None`` for the instance; the translators then
        behave as before and no back-write is attempted.
    '''

    if not redis_dsn:
        _LOGGER.info(
            'No redis_dsn configured; CreatorMap lookup disabled',
        )
        return {}, {}, None

    cm: RedisCreatorMap = RedisCreatorMap(
        redis_dsn=redis_dsn, platform='youtube',
    )
    id_to_handle: dict[str, str] = await cm.get_all()
    # Reverse map. Handles are unique to a channel in practice; on
    # the rare collision last-wins is acceptable for re-ingest.
    handle_to_id: dict[str, str] = {
        handle: cid for cid, handle in id_to_handle.items()
    }
    _LOGGER.info(
        'Loaded creator_map for re-ingest',
        extra={'entries': len(id_to_handle)},
    )
    return id_to_handle, handle_to_id, cm


# ---------------------------------------------------------------------------
# Per-chunk translators (run inside worker processes)
# ---------------------------------------------------------------------------


def _has_non_empty_file(path: Path) -> bool:
    '''
    True when *path* exists, is a regular file, and has non-zero
    length. Used by the chunk loops to skip archive entries whose
    migrated counterpart already lives in the data directory.
    '''

    try:
        return path.is_file() and path.stat().st_size > 0
    except OSError:
        return False


def _already_migrated(out_dir: str, filename: str) -> bool:
    '''
    True when *filename* already exists under *out_dir* with
    non-zero length, either at the top level (pending upload) or
    under ``uploaded/`` (already shipped). Either case means the
    re-ingest pipeline has handled this record before, so the
    archive file can be skipped without re-reading or re-writing.
    '''

    base: Path = Path(out_dir) / filename
    uploaded: Path = Path(out_dir) / 'uploaded' / filename
    return _has_non_empty_file(base) or _has_non_empty_file(uploaded)


def _was_file_handle_invalid(old: dict) -> bool:
    '''
    True when the in-file handle (canonical_handle / channel /
    channel_name) was missing or failed ``_HANDLE_PATTERN``. Used to
    increment the ``recovered_handle_from_map`` counter when the
    translator nevertheless produced a record.
    '''

    raw: str = (
        old.get('canonical_handle')
        or old.get('channel')
        or old.get('channel_name')
        or ''
    )
    handle: str = raw.lstrip('@') if raw else ''
    return not bool(_HANDLE_PATTERN.match(handle))


def _record_drop_reason(
    counters: dict[str, int],
    path_str: str,
    raw_channel_id: str | None,
    label: str,
) -> None:
    '''
    Bump the appropriate ``skipped_*`` counter when the translator
    returned ``None``. ``invalid_channel_id`` wins when the file
    actually had a channel_id that didn't match the YouTube
    UC-prefix pattern; otherwise the record is missing one of the
    required identifiers.
    '''

    if raw_channel_id and not _CHANNEL_ID_PATTERN.match(raw_channel_id):
        _LOGGER.info(
            f'{label} archive file has invalid channel_id, skipping',
            extra={
                'path': path_str, 'channel_id': raw_channel_id,
            },
        )
        counters['skipped_invalid_channel_id'] += 1
    else:
        _LOGGER.info(
            f'{label} archive file missing channel_handle or '
            'channel_id, skipping',
            extra={'path': path_str},
        )
        counters['skipped_no_handle_or_id'] += 1


async def _write_translated(
    fm: AssetFileManagement,
    filename: str,
    data: dict,
    counters: dict[str, int],
    src_path_str: str,
    label: str,
) -> None:
    '''
    Write *data* to ``<out_dir>/<filename>`` via *fm* and bump the
    appropriate counter. ``written`` on success, ``errors`` (with a
    log line) when ``write_file`` raises. Pulling this branch out
    of the per-record loop keeps the chunk functions under the
    cognitive-complexity ceiling.
    '''

    try:
        await fm.write_file(filename, data)
    except Exception as exc:
        _LOGGER.warning(
            f'Failed to write translated {label} file',
            exc=exc,
            extra={'path': filename, 'src': src_path_str},
        )
        counters['errors'] += 1
        return
    counters['written'] += 1


def _maybe_queue_creator_update(
    cm: RedisCreatorMap | None,
    pending: dict[str, str],
    id_to_handle: dict[str, str],
    handle_to_id: dict[str, str],
    channel_id: str,
    channel_handle: str,
) -> None:
    '''
    When *channel_id* is not already in the in-memory CreatorMap
    cache, stage it for back-write at chunk end and update the
    in-memory cache so subsequent records in the same chunk don't
    re-queue the same pair. No-op when *cm* is ``None`` (CreatorMap
    lookup disabled) or the id is already present.
    '''

    if cm is None:
        return
    if channel_id in id_to_handle:
        return
    pending[channel_id] = channel_handle
    id_to_handle[channel_id] = channel_handle
    handle_to_id[channel_handle] = channel_id


def _record_video_drop_reason(
    counters: dict[str, int],
    path_str: str,
    old: dict,
) -> None:
    '''
    Video drop-reason precedence: video_id problems win over
    channel_handle/channel_id problems, since a record without a
    valid video_id can't be uniquely keyed at all. Missing and
    malformed video_ids both increment the same counter — both mean
    "no usable video_id".
    '''

    raw_video_id: str | None = old.get('video_id')
    if not _valid_video_id(raw_video_id):
        _LOGGER.info(
            'Video archive file has missing or invalid video_id, '
            'skipping',
            extra={
                'path': path_str, 'video_id': raw_video_id,
            },
        )
        counters['skipped_invalid_video_id'] += 1
        return
    _record_drop_reason(
        counters, path_str, old.get('channel_id'), 'Video',
    )


_VIDEO_UNKNOWN_PREFIX: str = 'video-unknown-'


def _unwrap_unknown_video(parsed: dict) -> dict:
    '''
    Additional unwrap step for ``video-unknown-*`` archive files:
    if the parsed payload still has a ``data`` key whose value is a
    dict, take that as the payload. Distinct from
    :func:`_unwrap_server_envelope` because it doesn't require any
    envelope marker keys — ``video-unknown-`` files are produced by
    pipelines that may wrap the scraped payload in ``{"data": {…}}``
    without the platform/source metadata.
    '''

    inner: object = parsed.get('data')
    if isinstance(inner, dict):
        return inner
    return parsed


def _video_target_prefix(payload: dict) -> str:
    '''
    Decide the output prefix for a re-ingested ``video-unknown-*``
    file based on payload content. Presence of a non-empty
    ``formats`` list (the yt-dlp signature) means we have full DLP
    data and the file should be written as ``video-dlp-``;
    otherwise it's a minimal record and the prefix is ``video-min-``.
    '''

    formats: object = payload.get('formats')
    if isinstance(formats, list) and formats:
        return 'video-dlp-'
    return 'video-min-'


def _video_dest_filename(payload: dict) -> str:
    '''
    Output filename for a migrated video record. Always
    ``{video-dlp-|video-min-}{video_id}.json.br`` regardless of
    what the source filename looked like — the prefix comes from
    payload content (presence of a non-empty ``formats`` list)
    and the identifier is the validated ``video_id``.

    This collapses the legacy ``video-{channel_id}-{video_id}``
    naming and the ``video-unknown-`` shape into the canonical
    ``{prefix}{video_id}`` form that ``AssetFileManagement``
    expects.
    '''

    return f'{_video_target_prefix(payload)}{payload["video_id"]}.json.br'


def _extract_video_id_from_name(src_name: str) -> str | None:
    '''
    Best-effort extraction of the video_id from a source filename.
    Used by :func:`_video_already_migrated` to decide whether the
    canonical ``{prefix}{video_id}.json.br`` destination already
    exists *before* opening the source file.

    Takes the last ``-``-separated segment of the basename (so it
    works for ``video-X-Y``, ``video-min-Y``, ``video-dlp-Y``, and
    ``video-unknown-Y``) and only returns it when it matches
    ``_VIDEO_ID_PATTERN`` — a malformed candidate yields ``None``
    so the caller falls back to a conservative source-name check.
    '''

    if not src_name.endswith('.json.br'):
        return None
    stem: str = src_name[:-len('.json.br')]
    if '-' not in stem:
        return None
    candidate: str = stem.rsplit('-', 1)[1]
    return candidate if _VIDEO_ID_PATTERN.match(candidate) else None


async def _read_video_payload(
    src: Path,
    is_unknown: bool,
    counters: dict[str, int],
) -> dict | None:
    '''
    Read and pre-unwrap a video archive file. Returns the payload
    ready for ``translate_video``, or ``None`` when the file
    couldn't be read (caller should ``continue``). Bumps
    ``errors`` on failure. Pulled out of the chunk loop to keep
    its cognitive complexity below the linter ceiling.
    '''

    try:
        payload: dict = await _read_brotli_json(src)
    except Exception as exc:
        _LOGGER.warning(
            'Failed to read video archive file',
            exc=exc, extra={'path': str(src)},
        )
        counters['errors'] += 1
        return None
    if is_unknown:
        payload = _unwrap_unknown_video(payload)
    return payload


def _video_already_migrated(out_dir: str, src_name: str) -> bool:
    '''
    Check whether the canonical migrated counterpart of *src_name*
    already exists in *out_dir* (or its ``uploaded/`` subdir).
    The destination is always
    ``{video-dlp-|video-min-}{video_id}.json.br``, but the prefix
    can't be known until the file is read, so we check both
    candidates. Falls back to a source-name check when the
    video_id can't be derived from the filename.
    '''

    candidate: str | None = _extract_video_id_from_name(src_name)
    if candidate is None:
        return _already_migrated(out_dir, src_name)
    return (
        _already_migrated(out_dir, f'video-dlp-{candidate}.json.br')
        or _already_migrated(out_dir, f'video-min-{candidate}.json.br')
    )


def _was_video_file_handle_invalid(old: dict) -> bool:
    raw: str = (
        old.get('channel_handle')
        or old.get('channel_name')
        or ''
    )
    handle: str = raw.lstrip('@') if raw else ''
    return not bool(_HANDLE_PATTERN.match(handle))


async def _translate_channel_chunk(
    work_items: list[str],
    out_dir: str,
    dry_run: bool,
    id_to_handle: dict[str, str],
    handle_to_id: dict[str, str],
    cm: RedisCreatorMap | None,
) -> dict[str, int]:
    fm: AssetFileManagement = AssetFileManagement(out_dir)
    counters: dict[str, int] = {
        'read': 0, 'written': 0,
        'recovered_handle_from_map': 0,
        'recovered_id_from_map': 0,
        'added_to_creator_map': 0,
        'skipped_already_migrated': 0,
        'skipped_invalid_channel_id': 0,
        'skipped_no_handle_or_id': 0, 'errors': 0,
    }
    pending_creator_updates: dict[str, str] = {}

    for path_str in work_items:
        src: Path = Path(path_str)

        if _already_migrated(out_dir, src.name):
            counters['skipped_already_migrated'] += 1
            continue

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

        new: dict | None = translate_channel(
            old, id_to_handle, handle_to_id,
        )
        if new is None:
            _record_drop_reason(
                counters, path_str, old.get('channel_id'),
                'Channel',
            )
            continue

        if _was_file_handle_invalid(old):
            counters['recovered_handle_from_map'] += 1
        if not old.get('channel_id'):
            counters['recovered_id_from_map'] += 1
        _maybe_queue_creator_update(
            cm, pending_creator_updates,
            id_to_handle, handle_to_id,
            new['channel_id'], new['channel_handle'],
        )

        filename: str = (
            f'{CHANNEL_FILE_PREFIX}{new["channel_handle"]}.json.br'
        )

        if dry_run:
            counters['written'] += 1
            continue
        await _write_translated(
            fm, filename, new, counters, path_str, 'channel',
        )

    counters['added_to_creator_map'] = len(pending_creator_updates)
    if cm is not None and pending_creator_updates and not dry_run:
        await cm.put_many(pending_creator_updates)
    return counters


async def _translate_video_chunk(
    work_items: list[str],
    out_dir: str,
    dry_run: bool,
    id_to_handle: dict[str, str],
    handle_to_id: dict[str, str],
    cm: RedisCreatorMap | None,
) -> dict[str, int]:
    fm: AssetFileManagement = AssetFileManagement(out_dir)
    counters: dict[str, int] = {
        'read': 0, 'written': 0,
        'recovered_handle_from_map': 0,
        'recovered_id_from_map': 0,
        'kept_without_handle': 0,
        'added_to_creator_map': 0,
        'skipped_already_migrated': 0,
        'skipped_invalid_video_id': 0,
        'skipped_invalid_channel_id': 0,
        'skipped_no_handle_or_id': 0, 'errors': 0,
    }
    pending_creator_updates: dict[str, str] = {}

    for path_str in work_items:
        src: Path = Path(path_str)
        is_unknown: bool = src.name.startswith(_VIDEO_UNKNOWN_PREFIX)

        if _video_already_migrated(out_dir, src.name):
            counters['skipped_already_migrated'] += 1
            continue

        counters['read'] += 1

        old: dict | None = await _read_video_payload(
            src, is_unknown, counters,
        )
        if old is None:
            continue

        new: dict | None = translate_video(
            old, id_to_handle, handle_to_id,
        )
        if new is None:
            _record_video_drop_reason(counters, path_str, old)
            continue

        # ``channel_handle`` is optional in the output now: the
        # translator strips it when the file's handle is junk and
        # the CreatorMap can't supply a canonical one.
        handle_present: bool = 'channel_handle' in new
        if handle_present and _was_video_file_handle_invalid(old):
            counters['recovered_handle_from_map'] += 1
        if not handle_present:
            counters['kept_without_handle'] += 1
        if not old.get('channel_id'):
            counters['recovered_id_from_map'] += 1
        if handle_present:
            _maybe_queue_creator_update(
                cm, pending_creator_updates,
                id_to_handle, handle_to_id,
                new['channel_id'], new['channel_handle'],
            )

        filename: str = _video_dest_filename(new)

        if dry_run:
            counters['written'] += 1
            continue
        await _write_translated(
            fm, filename, new, counters, path_str, 'video',
        )

    counters['added_to_creator_map'] = len(pending_creator_updates)
    if cm is not None and pending_creator_updates and not dry_run:
        await cm.put_many(pending_creator_updates)
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


async def _run_channel_worker(
    work_items: list[str],
    out_dir: str,
    dry_run: bool,
    redis_dsn: str | None,
) -> dict[str, int]:
    id_to_handle: dict[str, str]
    handle_to_id: dict[str, str]
    cm: RedisCreatorMap | None
    id_to_handle, handle_to_id, cm = (
        await _load_creator_maps(redis_dsn)
    )
    return await _translate_channel_chunk(
        work_items, out_dir, dry_run,
        id_to_handle, handle_to_id, cm,
    )


async def _run_video_worker(
    work_items: list[str],
    out_dir: str,
    dry_run: bool,
    redis_dsn: str | None,
) -> dict[str, int]:
    id_to_handle: dict[str, str]
    handle_to_id: dict[str, str]
    cm: RedisCreatorMap | None
    id_to_handle, handle_to_id, cm = (
        await _load_creator_maps(redis_dsn)
    )
    return await _translate_video_chunk(
        work_items, out_dir, dry_run,
        id_to_handle, handle_to_id, cm,
    )


def _channel_worker_entrypoint(
    worker_id: int,
    work_items: list[str],
    out_dir: str,
    dry_run: bool,
    log_level: str,
    log_file: str,
    log_format: str,
    redis_dsn: str | None,
) -> dict[str, int]:
    '''
    Top-level (pickleable) channel-chunk worker. Configures logging
    afresh in the child, tags records with ``worker_id``, loads the
    CreatorMap once, and delegates to :func:`_translate_channel_chunk`.
    '''
    configure_logging(
        level=log_level, filename=log_file, log_format=log_format,
    )
    _install_worker_id_filter(worker_id)
    return asyncio.run(
        _run_channel_worker(work_items, out_dir, dry_run, redis_dsn),
    )


def _video_worker_entrypoint(
    worker_id: int,
    work_items: list[str],
    out_dir: str,
    dry_run: bool,
    log_level: str,
    log_file: str,
    log_format: str,
    redis_dsn: str | None,
) -> dict[str, int]:
    '''
    Top-level (pickleable) video-chunk worker. Configures logging
    afresh in the child, tags records with ``worker_id``, loads the
    CreatorMap once, and delegates to :func:`_translate_video_chunk`.
    '''
    configure_logging(
        level=log_level, filename=log_file, log_format=log_format,
    )
    _install_worker_id_filter(worker_id)
    return asyncio.run(
        _run_video_worker(work_items, out_dir, dry_run, redis_dsn),
    )


# ---------------------------------------------------------------------------
# Walk + chunk + dispatch
# ---------------------------------------------------------------------------


def _enumerate_archive(root: Path, prefix: str) -> list[str]:
    '''
    Recursively find every ``<prefix>*.json.br`` file under *root*.
    Strings pickle cheaper than ``Path`` objects across processes.

    Directory symlinks are followed (``recurse_symlinks=True``) so
    archives assembled from multiple disks via symlinked subtrees
    are walked end to end. Files reached via two different paths
    (e.g. the real path and a symlinked path) are processed twice,
    but the second pass hits the ``_already_migrated`` skip and is
    cheap. Symlink cycles would loop forever — Python's ``rglob``
    does not detect them — so the operator must avoid linking a
    subtree back into one of its ancestors.
    '''
    return [
        str(p) for p in root.rglob(
            f'{prefix}*.json.br', recurse_symlinks=True,
        ) if p.is_file()
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
            settings.log_format, settings.redis_dsn,
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
                settings.redis_dsn,
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
    if (
        not settings.channel_archive_dir
        and not settings.video_archive_dir
    ):
        _LOGGER.error(
            'At least one of --channel-archive-dir or '
            '--video-archive-dir must be set',
        )
        return 1
    if (
        settings.channel_archive_dir
        and not settings.channel_data_directory
    ):
        _LOGGER.error(
            'channel_data_directory must be configured to write '
            'translated channel files (or leave --channel-archive-dir '
            'unset to skip channels)',
        )
        return 1
    if (
        settings.video_archive_dir
        and not settings.video_data_directory
    ):
        _LOGGER.error(
            'video_data_directory must be configured to write '
            'translated video files (or leave --video-archive-dir '
            'unset to skip videos)',
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

    channel_counters: dict[str, int] = {}
    if settings.channel_archive_dir:
        channel_archive: Path = Path(settings.channel_archive_dir)
        if not channel_archive.is_dir():
            _LOGGER.error(
                'Channel archive directory does not exist',
                extra={
                    'channel_archive_dir': str(channel_archive),
                },
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

        channel_counters = _dispatch(
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

    if channel_counters:
        print(
            f'Channels: read={channel_counters.get("read", 0)} '
            f'written={channel_counters.get("written", 0)} '
            f'recovered_handle_from_map='
            f'{channel_counters.get("recovered_handle_from_map", 0)} '
            f'recovered_id_from_map='
            f'{channel_counters.get("recovered_id_from_map", 0)} '
            f'added_to_creator_map='
            f'{channel_counters.get("added_to_creator_map", 0)} '
            f'skipped_already_migrated='
            f'{channel_counters.get("skipped_already_migrated", 0)} '
            f'skipped_invalid_channel_id='
            f'{channel_counters.get("skipped_invalid_channel_id", 0)} '
            f'skipped_no_handle_or_id='
            f'{channel_counters.get("skipped_no_handle_or_id", 0)} '
            f'errors={channel_counters.get("errors", 0)}'
        )
    if video_counters:
        print(
            f'Videos:   read={video_counters.get("read", 0)} '
            f'written={video_counters.get("written", 0)} '
            f'recovered_handle_from_map='
            f'{video_counters.get("recovered_handle_from_map", 0)} '
            f'recovered_id_from_map='
            f'{video_counters.get("recovered_id_from_map", 0)} '
            f'kept_without_handle='
            f'{video_counters.get("kept_without_handle", 0)} '
            f'added_to_creator_map='
            f'{video_counters.get("added_to_creator_map", 0)} '
            f'skipped_already_migrated='
            f'{video_counters.get("skipped_already_migrated", 0)} '
            f'skipped_invalid_video_id='
            f'{video_counters.get("skipped_invalid_video_id", 0)} '
            f'skipped_invalid_channel_id='
            f'{video_counters.get("skipped_invalid_channel_id", 0)} '
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
