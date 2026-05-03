#!/usr/bin/env python3
'''
Augment ``video-min-*.json.br`` files in
``YOUTUBE_VIDEO_DATA_DIR`` with channel-level fields
(``channel_id``, ``channel_handle``, ``channel_url``,
``channel_is_verified``, ``channel_follower_count``) by reading
every channel file in ``YOUTUBE_CHANNEL_DATA_DIR/uploaded`` and
indexing each channel's ``video_ids`` list.

To keep the storage bounded, the script first enumerates the set
of video_ids that exist on disk as ``video-min-*.json.br`` files
and only stores mappings whose video_id is in that set. Without
this filter the mapping would grow to all video_ids referenced
by any channel, regardless of whether the corresponding video
file actually exists locally — which can be 100M+ entries for a
multi-hundred-thousand-channel corpus and OOM-kills the process.

The ``video_id -> channel info`` table is persisted as a sqlite3
database between runs (``augment_videos_table.sqlite3`` next to
the video data dir). Subsequent runs only re-read channel files
whose mtime is newer than the table's stored
``last_processed_at`` timestamp. A legacy
``augment_videos_table.json`` is auto-imported on first sqlite
run so existing tables don't have to be rebuilt from scratch.

Resilient against partially-written brotli files: when one-shot
decompression fails but a streaming pass recovers parseable
JSON, the file is rewritten on disk with cleanly-encoded bytes.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import argparse
import datetime
import json
import os
import secrets
import sqlite3
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path

import brotli
import orjson
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from scrape_exchange.youtube.youtube_channel import YouTubeChannel


# Mapping of video-side field name -> source field name on the
# ChannelInfo record. Used both when augmenting video files and
# when checking whether a video file has any augmentable hole.
_AUGMENT_FIELDS: dict[str, str] = {
    'channel_id': 'channel_id',
    'channel_handle': 'channel_handle',
    'channel_url': 'channel_url',
    'channel_is_verified': 'channel_is_verified',
    'channel_follower_count': 'subscriber_count',
}
_VIDEO_PREFIX: str = 'video-min-'
_VIDEO_SUFFIX: str = '.json.br'
_MARKER_SUFFIXES: tuple[str, ...] = (
    '.failed', '.exists', '.unavailable', '.invalid',
    '.unresolved', '.not_found',
)


@dataclass
class ChannelInfo:
    channel_id: str | None
    channel_handle: str
    channel_url: str
    channel_is_verified: bool
    subscriber_count: int | None


class MappingDB:
    '''
    sqlite-backed video_id -> channel-info mapping that exposes the
    subset of dict operations the rest of the script uses
    (``get(vid)`` -> dict | None, ``mapping[vid] = info_dict``,
    ``len(mapping)``).

    Writes accumulate inside a single sqlite transaction until
    :meth:`commit` is called. The script commits opportunistically
    every ``_CHANNEL_PROGRESS_INTERVAL`` channel files so the WAL
    cannot grow unboundedly during a long channel pass; this is
    crash-safety as a side effect — INSERT OR REPLACE makes
    every row idempotent, so a process kill mid-pass just causes
    the unsaved channels to be re-read on the next run.
    '''

    _SCHEMA: str = (
        'CREATE TABLE IF NOT EXISTS mappings ('
        ' video_id TEXT PRIMARY KEY,'
        ' channel_id TEXT,'
        ' channel_handle TEXT NOT NULL,'
        ' channel_url TEXT NOT NULL,'
        ' channel_is_verified INTEGER NOT NULL,'
        ' subscriber_count INTEGER'
        ');'
        'CREATE TABLE IF NOT EXISTS metadata ('
        ' key TEXT PRIMARY KEY,'
        ' value TEXT'
        ');'
    )

    def __init__(self, path: Path) -> None:
        self.path: Path = path
        self._conn: sqlite3.Connection = sqlite3.connect(str(path))
        self._conn.executescript(self._SCHEMA)
        self._conn.execute('PRAGMA journal_mode = WAL')
        self._conn.execute('PRAGMA synchronous = NORMAL')
        self._conn.execute('PRAGMA temp_store = MEMORY')
        self._conn.commit()

    def get(self, video_id: str) -> dict | None:
        row: tuple | None = self._conn.execute(
            'SELECT channel_id, channel_handle, channel_url, '
            'channel_is_verified, subscriber_count '
            'FROM mappings WHERE video_id = ?',
            (video_id,),
        ).fetchone()
        if row is None:
            return None
        return {
            'channel_id': row[0],
            'channel_handle': row[1],
            'channel_url': row[2],
            'channel_is_verified': bool(row[3]),
            'subscriber_count': row[4],
        }

    def __setitem__(self, video_id: str, info: dict) -> None:
        self._conn.execute(
            'INSERT OR REPLACE INTO mappings '
            '(video_id, channel_id, channel_handle, channel_url, '
            ' channel_is_verified, subscriber_count) '
            'VALUES (?, ?, ?, ?, ?, ?)',
            (
                video_id,
                info.get('channel_id'),
                info['channel_handle'],
                info['channel_url'],
                int(bool(info.get('channel_is_verified'))),
                info.get('subscriber_count'),
            ),
        )

    def __len__(self) -> int:
        row: tuple | None = self._conn.execute(
            'SELECT COUNT(*) FROM mappings',
        ).fetchone()
        return int(row[0]) if row else 0

    def get_last_processed_at(self) -> datetime.datetime | None:
        row: tuple | None = self._conn.execute(
            'SELECT value FROM metadata WHERE key = ?',
            ('last_processed_at',),
        ).fetchone()
        if not row or not row[0]:
            return None
        return datetime.datetime.fromisoformat(row[0])

    def set_last_processed_at(
        self, ts: datetime.datetime,
    ) -> None:
        self._conn.execute(
            'INSERT OR REPLACE INTO metadata (key, value) '
            'VALUES (?, ?)',
            ('last_processed_at', ts.isoformat()),
        )

    def bulk_load(self, rows: list[tuple]) -> None:
        '''
        Insert many ``(video_id, channel_id, channel_handle,
        channel_url, channel_is_verified, subscriber_count)``
        tuples in a single executemany. Used by the legacy-JSON
        importer; not called on the hot path.
        '''
        self._conn.executemany(
            'INSERT OR REPLACE INTO mappings '
            '(video_id, channel_id, channel_handle, channel_url, '
            ' channel_is_verified, subscriber_count) '
            'VALUES (?, ?, ?, ?, ?, ?)',
            rows,
        )

    def commit(self) -> None:
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()


def _migrate_legacy_json(db: MappingDB, json_path: Path) -> int:
    '''
    Bulk-import the legacy ``augment_videos_table.json`` into
    *db* the first time the sqlite store is opened. No-op if the
    JSON file is missing or the DB already has rows. Returns the
    number of rows imported.
    '''
    if not json_path.exists():
        return 0
    if len(db) > 0:
        return 0
    print(
        f'migrating legacy JSON table from {json_path}...',
        flush=True,
    )
    started: float = time.monotonic()
    with open(json_path, 'rb') as f:
        data: dict = orjson.loads(f.read()) or {}
    legacy_mappings: dict = data.get('mappings', {}) or {}
    rows: list[tuple] = []
    skipped: int = 0
    for vid, info in legacy_mappings.items():
        if not isinstance(vid, str) or not isinstance(info, dict):
            skipped += 1
            continue
        handle: str | None = info.get('channel_handle')
        url: str | None = info.get('channel_url')
        if not handle or not url:
            skipped += 1
            continue
        rows.append((
            vid,
            info.get('channel_id'),
            handle,
            url,
            int(bool(info.get('channel_is_verified'))),
            info.get('subscriber_count'),
        ))
    db.bulk_load(rows)
    ts_raw = data.get('last_processed_at')
    if isinstance(ts_raw, str):
        db.set_last_processed_at(
            datetime.datetime.fromisoformat(ts_raw),
        )
    db.commit()
    elapsed: float = time.monotonic() - started
    print(
        f'  migrated {len(rows):,} mappings in {elapsed:.1f}s '
        f'(skipped {skipped} malformed entries). '
        f'Legacy JSON file left in place — delete manually once '
        f'the sqlite table is verified.',
        flush=True,
    )
    return len(rows)


class AugmentSettings(BaseSettings):
    '''Pydantic-settings reader for the augment script.'''

    model_config = SettingsConfigDict(
        env_file='.env', env_file_encoding='utf-8', extra='ignore',
    )
    video_data_dir: str = Field(
        validation_alias='YOUTUBE_VIDEO_DATA_DIR',
    )
    channel_data_dir: str = Field(
        validation_alias='YOUTUBE_CHANNEL_DATA_DIR',
    )


def _safe_decompress(
    compressed: bytes,
) -> tuple[bytes | None, bool]:
    '''
    Decompress *compressed* with brotli, tolerating truncated
    streams.

    :returns: ``(decompressed_bytes, partial_recovery)``.
        ``partial_recovery`` is ``True`` when the one-shot
        ``brotli.decompress`` failed and the streaming
        decompressor was used to salvage a prefix; the caller
        should rewrite the source file with the canonical bytes
        in that case. ``decompressed_bytes`` is ``None`` if no
        usable output could be recovered.
    '''
    try:
        return brotli.decompress(compressed), False
    except brotli.error:
        pass

    decompressor: brotli.Decompressor = brotli.Decompressor()
    out: bytearray = bytearray()
    chunk_size: int = 65536
    for offset in range(0, len(compressed), chunk_size):
        try:
            out.extend(
                decompressor.process(
                    compressed[offset:offset + chunk_size]
                )
            )
        except brotli.error:
            break
    return (bytes(out) if out else None), bool(out)


_BROTLI_QUALITY: int = 9


def _atomic_write_brotli(path: Path, data: bytes) -> None:
    '''Write brotli-compressed *data* to *path* atomically.

    Uses quality 9 (vs the brotli library default of 11). At
    augment scale (millions of files) the wall-clock difference
    is roughly 3-5x in compression speed, with output size only
    1-2% larger.'''
    tmp: Path = path.with_name(
        f'.tmp-{secrets.token_hex(8)}{path.name}'
    )
    try:
        with open(tmp, 'wb') as f:
            f.write(brotli.compress(data, quality=_BROTLI_QUALITY))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            tmp.unlink()
        except OSError:
            pass
        raise


def _read_json_br(
    path: Path,
) -> tuple[dict | None, bool]:
    '''
    Read a brotli-compressed JSON file at *path*.

    :returns: ``(obj, needs_rewrite)`` where ``obj`` is the
        parsed dict (or ``None`` if unrecoverable) and
        ``needs_rewrite`` is ``True`` when one-shot decompression
        failed but parsing succeeded — caller should rewrite the
        file to clean up the on-disk bytes.
    '''
    try:
        compressed: bytes = path.read_bytes()
    except OSError as exc:
        print(f'read error {path}: {exc}', file=sys.stderr)
        return None, False
    if not compressed:
        print(f'empty file {path}', file=sys.stderr)
        return None, False
    raw: bytes | None
    partial: bool
    raw, partial = _safe_decompress(compressed)
    if raw is None:
        print(f'decompress failed {path}', file=sys.stderr)
        return None, False
    try:
        obj = json.loads(raw)
    except Exception as exc:
        print(f'json parse failed {path}: {exc}', file=sys.stderr)
        return None, False
    if not isinstance(obj, dict):
        print(
            f'not a JSON object (got {type(obj).__name__}) {path}',
            file=sys.stderr,
        )
        return None, False
    return obj, partial


def _write_json_br(path: Path, obj: dict) -> None:
    encoded: bytes = json.dumps(
        obj, ensure_ascii=False,
    ).encode('utf-8')
    _atomic_write_brotli(path, encoded)


def _channel_info_from_file(
    obj: dict,
) -> ChannelInfo | None:
    '''Extract a ChannelInfo from a parsed channel-file dict.

    Returns ``None`` if mandatory fields are missing.'''
    channel_handle: str | None = obj.get('channel_handle')
    if not channel_handle:
        return None
    channel_id: str | None = obj.get('channel_id')
    # Always store the canonical @handle URL — the channel file's
    # own `url` field may itself be the legacy /channel/UCxxx
    # form, and we want to upgrade videos to the @handle form
    # downstream.
    url: str = YouTubeChannel.CHANNEL_URL_WITH_AT.format(
        channel_handle=channel_handle,
    )
    sub_raw = obj.get('subscriber_count')
    subscriber_count: int | None = (
        int(sub_raw)
        if isinstance(sub_raw, (int, float)) and sub_raw >= 0
        else None
    )
    return ChannelInfo(
        channel_id=channel_id,
        channel_handle=channel_handle,
        channel_url=url,
        channel_is_verified=bool(obj.get('verified', False)),
        subscriber_count=subscriber_count,
    )


def _list_channel_files(channel_dir: Path) -> list[Path]:
    '''Return channel-*.json.br files in *channel_dir* sorted by
    mtime ascending so the newest write wins collisions.'''
    candidates: list[Path] = [
        p for p in channel_dir.iterdir()
        if p.is_file() and p.name.endswith('.json.br')
        and not any(
            p.name.endswith(s) for s in _MARKER_SUFFIXES
        )
    ]
    return sorted(candidates, key=lambda p: p.stat().st_mtime)


def _enumerate_video_ids(video_dir: Path) -> set[str]:
    '''Return the set of video_ids represented by top-level
    ``video-min-*.json.br`` files in *video_dir* (markers
    excluded). Parses the id from the filename rather than the
    file contents — at 8M+ files, opening each one would be
    impractical.'''
    ids: set[str] = set()
    prefix_len: int = len(_VIDEO_PREFIX)
    suffix_len: int = len(_VIDEO_SUFFIX)
    for entry in os.scandir(video_dir):
        if not entry.is_file(follow_symlinks=False):
            continue
        name: str = entry.name
        if not name.startswith(_VIDEO_PREFIX):
            continue
        if not name.endswith(_VIDEO_SUFFIX):
            continue
        if any(name.endswith(s) for s in _MARKER_SUFFIXES):
            continue
        ids.add(name[prefix_len:-suffix_len])
    return ids


def _try_rewrite(path: Path, obj: dict) -> None:
    '''Best-effort atomic rewrite; logs on failure.'''
    try:
        _write_json_br(path, obj)
    except OSError as exc:
        print(f'rewrite failed {path}: {exc}', file=sys.stderr)


def _resolve_collision(
    incoming: ChannelInfo, existing: dict,
) -> tuple[bool, str]:
    '''Apply the tie-breaker rules. Return ``(prefer_incoming,
    reason)`` — ``prefer_incoming`` is ``True`` if the
    *incoming* channel should replace *existing*.

    Rules in priority order:
      1. ``verified=True`` wins over ``verified=False``.
      2. handle ending with ``VEVO`` (case-insensitive) wins.
      3. higher ``subscriber_count`` wins.
      4. otherwise the existing entry is kept (deterministic).
    '''
    inc_v: bool = bool(incoming.channel_is_verified)
    ext_v: bool = bool(existing.get('channel_is_verified', False))
    if inc_v != ext_v:
        return inc_v, 'verified'
    inc_handle: str = (incoming.channel_handle or '').upper()
    ext_handle: str = (
        existing.get('channel_handle', '') or ''
    ).upper()
    inc_vevo: bool = inc_handle.endswith('VEVO')
    ext_vevo: bool = ext_handle.endswith('VEVO')
    if inc_vevo != ext_vevo:
        return inc_vevo, 'vevo_suffix'
    inc_sc: int = incoming.subscriber_count or 0
    ext_sc: int = existing.get('subscriber_count') or 0
    if inc_sc != ext_sc:
        return inc_sc > ext_sc, 'subscriber_count'
    return False, 'tie_kept_existing'


def _handle_collision(
    vid: str, info: ChannelInfo, info_dict: dict,
    existing: dict, source: Path,
    mapping: 'MappingDB',
) -> None:
    '''Resolve a single video_id collision, mutating *mapping*
    if the incoming channel wins, and logging the outcome.'''
    prefer_incoming: bool
    reason: str
    prefer_incoming, reason = _resolve_collision(info, existing)
    winner: dict = info_dict if prefer_incoming else existing
    loser: dict = existing if prefer_incoming else info_dict
    print(
        f'collision vid={vid}: kept '
        f'{winner.get("channel_handle")!r} (id='
        f'{winner.get("channel_id")}) over '
        f'{loser.get("channel_handle")!r} (id='
        f'{loser.get("channel_id")}) by {reason} '
        f'[from {source.name}]',
        file=sys.stderr,
    )
    if prefer_incoming:
        mapping[vid] = info_dict


def _merge_channel_into_mapping(
    obj: dict, source: Path, mapping: 'MappingDB',
    interesting: set[str],
) -> tuple[bool, int]:
    '''Merge one parsed channel dict into *mapping*. Only video_ids
    in *interesting* (the set of ids we have video files for) are
    considered — this caps memory usage at the size of the local
    video corpus rather than the union of all channels'
    video_ids lists.

    Returns ``(contributed, collisions)`` — *contributed* is
    ``True`` when at least one interesting video_id was added,
    refreshed, or resolved against an existing entry.
    Collisions on a video_id are resolved via
    :func:`_resolve_collision`.'''
    info: ChannelInfo | None = _channel_info_from_file(obj)
    if info is None:
        return False, 0
    video_ids = obj.get('video_ids')
    if not isinstance(video_ids, list) or not video_ids:
        return False, 0
    info_dict: dict = asdict(info)
    collisions: int = 0
    contributed: bool = False
    for vid in video_ids:
        if not isinstance(vid, str):
            continue
        if vid not in interesting:
            continue
        contributed = True
        existing: dict | None = mapping.get(vid)
        if existing is None:
            mapping[vid] = info_dict
            continue
        if existing.get('channel_id') == info.channel_id:
            mapping[vid] = info_dict  # refresh in case stats grew
            continue
        collisions += 1
        _handle_collision(
            vid, info, info_dict, existing, source, mapping,
        )
    return contributed, collisions


_CHANNEL_PROGRESS_INTERVAL: int = 1000
_CHANNEL_PROGRESS_SECONDS: float = 60.0


def update_mapping(
    mapping: 'MappingDB',
    channel_dir: Path,
    since: datetime.datetime | None,
    interesting: set[str],
) -> tuple[int, int, int]:
    '''
    Walk *channel_dir* and merge new entries into *mapping*,
    keeping only video_ids that appear in *interesting*.

    Channel files older than *since* are skipped. Returns
    ``(scanned, processed, collisions)``. Emits a progress
    line to stderr whichever comes first: every
    ``_CHANNEL_PROGRESS_INTERVAL`` scanned files OR
    ``_CHANNEL_PROGRESS_SECONDS`` of wall-clock time since
    the last progress line, and commits the open sqlite
    transaction at the same cadence so the WAL stays bounded.
    '''
    scanned: int = 0
    processed: int = 0
    collisions: int = 0
    cutoff: float = since.timestamp() if since else 0.0
    last_progress_at: float = time.monotonic()
    for path in _list_channel_files(channel_dir):
        scanned += 1
        now: float = time.monotonic()
        if (
            scanned % _CHANNEL_PROGRESS_INTERVAL == 0
            or now - last_progress_at
            >= _CHANNEL_PROGRESS_SECONDS
        ):
            mapping.commit()
            print(
                f'  channel progress: scanned={scanned} '
                f'processed={processed} collisions={collisions} '
                f'mapping_size={len(mapping)}',
                file=sys.stderr, flush=True,
            )
            last_progress_at = now
        if path.stat().st_mtime <= cutoff:
            continue
        obj: dict | None
        needs_rewrite: bool
        obj, needs_rewrite = _read_json_br(path)
        if obj is None:
            continue
        if needs_rewrite:
            _try_rewrite(path, obj)
        contributed: bool
        added: int
        contributed, added = _merge_channel_into_mapping(
            obj, path, mapping, interesting,
        )
        collisions += added
        if contributed:
            processed += 1
    return scanned, processed, collisions


def load_table(
    path: Path,
) -> tuple[MappingDB, datetime.datetime | None]:
    '''
    Open the sqlite mapping table at *path*, importing the
    legacy ``augment_videos_table.json`` next to it if present
    and the DB is empty. Returns the open ``MappingDB`` and the
    persisted ``last_processed_at`` (``None`` on first run).
    '''
    print(f'opening sqlite table at {path}...', flush=True)
    started: float = time.monotonic()
    db: MappingDB = MappingDB(path)
    legacy_json: Path = path.with_suffix('.json')
    _migrate_legacy_json(db, legacy_json)
    ts: datetime.datetime | None = db.get_last_processed_at()
    print(
        f'  table opened in {time.monotonic() - started:.1f}s '
        f'({len(db):,} entries)',
        flush=True,
    )
    return db, ts


def save_table(
    mapping: MappingDB, last_processed_at: datetime.datetime,
) -> None:
    '''
    Persist *last_processed_at* and commit any pending writes on
    the open sqlite transaction. The actual mapping rows have
    been written incrementally during the channel pass; this
    call just stamps the cutoff timestamp and forces a final
    fsync.
    '''
    print(
        f'committing sqlite table ({len(mapping):,} entries) '
        f'to {mapping.path}...',
        flush=True,
    )
    started: float = time.monotonic()
    mapping.set_last_processed_at(last_processed_at)
    mapping.commit()
    print(
        f'committed in {time.monotonic() - started:.1f}s',
        flush=True,
    )


def _is_video_min(name: str) -> bool:
    if not name.startswith(_VIDEO_PREFIX):
        return False
    if not name.endswith(_VIDEO_SUFFIX):
        return False
    return not any(name.endswith(s) for s in _MARKER_SUFFIXES)


def _apply_augmentation(
    obj: dict, info: dict | None,
) -> tuple[bool, bool]:
    '''Mutate *obj* with values from *info*.

    Returns ``(modified, lacks_mapping_for_missing_fields)``:
        * ``modified`` — True if any field was filled in.
        * ``lacks_mapping_for_missing_fields`` — True when *info*
          was ``None`` and *obj* still has at least one
          augmentable field empty (caller increments a counter).
    '''
    if info is None:
        return False, any(
            obj.get(f) in (None, '') for f in _AUGMENT_FIELDS
        )
    modified: bool = False
    for video_field, info_field in _AUGMENT_FIELDS.items():
        current = obj.get(video_field)
        if current not in (None, '') and not _is_stale(
            video_field, current,
        ):
            continue
        new_val = info.get(info_field)
        if new_val is None:
            continue
        obj[video_field] = new_val
        modified = True
    return modified, False


def _is_stale(field: str, value: object) -> bool:
    '''Return True for values that should be overwritten even
    though they are non-empty.

    Currently used to upgrade legacy ``/channel/UCxxx`` URLs to
    the canonical ``@handle`` form once the handle is known.
    '''
    if field == 'channel_url' and isinstance(value, str):
        return '/channel/' in value
    return False


def _save_augmented(
    path: Path, obj: dict, dry_run: bool,
) -> bool:
    '''Persist *obj* to *path*. Returns True on success.'''
    if dry_run:
        return True
    try:
        _write_json_br(path, obj)
    except OSError as exc:
        print(f'write failed {path}: {exc}', file=sys.stderr)
        return False
    return True


def _process_one_video(
    entry: os.DirEntry, mapping: 'MappingDB', dry_run: bool,
) -> str:
    '''
    Process a single video file entry.

    Returns one of: ``'updated'``, ``'no_mapping'``,
    ``'error'``, ``'noop'``.
    '''
    path: Path = Path(entry.path)
    obj: dict | None
    needs_rewrite: bool
    obj, needs_rewrite = _read_json_br(path)
    if obj is None:
        return 'error'
    video_id = obj.get('video_id')
    if not isinstance(video_id, str):
        return 'noop'
    info: dict | None = mapping.get(video_id)
    modified: bool
    no_mapping: bool
    modified, no_mapping = _apply_augmentation(obj, info)
    if modified or needs_rewrite:
        return 'updated' if _save_augmented(
            path, obj, dry_run,
        ) else 'error'
    return 'no_mapping' if no_mapping else 'noop'


_VIDEO_PROGRESS_INTERVAL: int = 10000
_VIDEO_PROGRESS_SECONDS: float = 60.0


def augment_videos(
    video_dir: Path,
    mapping: 'MappingDB',
    dry_run: bool,
    limit: int | None,
) -> tuple[int, int, int, int]:
    '''
    Walk *video_dir* and augment each ``video-min-*.json.br``
    in place when channel fields are missing.

    Emits a progress line to stderr whichever comes first:
    every ``_VIDEO_PROGRESS_INTERVAL`` scanned files OR
    ``_VIDEO_PROGRESS_SECONDS`` of wall-clock time since the
    last progress line. The time-based trigger ensures
    visibility even when reads are slow (e.g. cold disk
    cache, NFS hops) and the count-based trigger keeps the
    log compact when reads are fast. Returns
    ``(scanned, updated, no_mapping, errors)``.
    '''
    print(
        f'starting video augmentation pass over {video_dir} '
        f'(mapping has {len(mapping):,} entries)',
        flush=True,
    )
    scanned: int = 0
    updated: int = 0
    no_mapping: int = 0
    errors: int = 0
    last_progress_at: float = time.monotonic()
    for entry in os.scandir(video_dir):
        if limit is not None and scanned >= limit:
            break
        if not entry.is_file(follow_symlinks=False):
            continue
        if not _is_video_min(entry.name):
            continue
        scanned += 1
        if scanned == 1:
            print(
                f'  first matching video file: {entry.name}',
                file=sys.stderr, flush=True,
            )
        now: float = time.monotonic()
        if (
            scanned % _VIDEO_PROGRESS_INTERVAL == 0
            or now - last_progress_at >= _VIDEO_PROGRESS_SECONDS
        ):
            print(
                f'  video progress: scanned={scanned} '
                f'updated={updated} no_mapping={no_mapping} '
                f'errors={errors}',
                file=sys.stderr, flush=True,
            )
            last_progress_at = now
        outcome: str = _process_one_video(
            entry, mapping, dry_run,
        )
        if outcome == 'updated':
            updated += 1
        elif outcome == 'no_mapping':
            no_mapping += 1
        elif outcome == 'error':
            errors += 1
    return scanned, updated, no_mapping, errors


def main() -> int:
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description=(
            'Augment video-min files with channel data sourced '
            'from uploaded channel files.'
        ),
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='do not write any files',
    )
    parser.add_argument(
        '--limit', type=int, default=None,
        help='stop after processing this many video files',
    )
    args: argparse.Namespace = parser.parse_args()

    settings: AugmentSettings = AugmentSettings()
    video_dir: Path = Path(settings.video_data_dir)
    channel_dir: Path = Path(settings.channel_data_dir) / 'uploaded'
    table_path: Path = (
        video_dir.parent / 'augment_videos_table.sqlite3'
    )

    if not channel_dir.is_dir():
        print(
            f'channel dir missing: {channel_dir}',
            file=sys.stderr,
        )
        return 2
    if not video_dir.is_dir():
        print(
            f'video dir missing: {video_dir}',
            file=sys.stderr,
        )
        return 2
    print(
        f'starting augment_videos with settings: '
        f'channel_data_dir={channel_dir}, video_data_dir={video_dir}'
    )

    print('enumerating video files on disk...', flush=True)
    interesting: set[str] = _enumerate_video_ids(video_dir)
    print(f'found {len(interesting):,} unique video_ids on disk')

    mapping: MappingDB
    last_at: datetime.datetime | None
    mapping, last_at = load_table(table_path)
    print(
        f'loaded {len(mapping)} mappings; last_processed_at='
        f'{last_at.isoformat() if last_at else "(never)"}'
    )

    started_at: datetime.datetime = datetime.datetime.now(
        datetime.timezone.utc,
    )
    try:
        scanned: int
        processed: int
        collisions: int
        scanned, processed, collisions = update_mapping(
            mapping, channel_dir, last_at, interesting,
        )
        print(
            f'channel files: scanned={scanned} '
            f'newly_processed={processed} collisions={collisions}; '
            f'mapping size={len(mapping)}'
        )

        save_table(mapping, started_at)
        print(f'persisted table to {table_path}')

        # The video_id set is only needed during channel
        # processing to filter mappings. Drop it before the
        # video pass so the process doesn't hold it (≈1-2 GB
        # at 8M+ ids) alongside the per-file working set.
        del interesting

        v_scanned: int
        v_updated: int
        no_mapping: int
        v_errors: int
        v_scanned, v_updated, no_mapping, v_errors = (
            augment_videos(
                video_dir, mapping, args.dry_run, args.limit,
            )
        )
        print(
            f'video files: scanned={v_scanned} '
            f'updated={v_updated} no_mapping={no_mapping} '
            f'errors={v_errors}'
        )
        return 0 if v_errors == 0 else 1
    finally:
        mapping.close()


if __name__ == '__main__':
    sys.exit(main())
