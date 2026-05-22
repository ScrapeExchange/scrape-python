#!/usr/bin/env python3
'''
Augment ``video-min-*.json.br`` and ``video-dlp-*.json.br`` files in
``YOUTUBE_VIDEO_DATA_DIR`` with channel-level fields
(``channel_id``, ``channel_handle``, ``channel_url``,
``channel_is_verified``, ``channel_follower_count``) by looking
up the owning channel record from the scrape.exchange API.

For each local video file the script:

1. Reads the on-disk video record (with best-effort recovery of
   truncated brotli wrappers and a plain-JSON fallback).
2. Checks the sqlite cache
   (``augment_videos_table.sqlite3`` next to the video dir) for
   the channel info corresponding to the video's
   ``channel_id`` / ``channel_handle``.
3. On cache miss, POSTs to ``/api/v1/filter`` with
   ``platform=youtube, entity=channel`` and either
   ``platform_content_id={channel_id}`` or
   ``platform_creator_id={channel_handle}``, then GETs the
   resulting ``data_url`` to fetch the channel record.
4. Writes the augmented video record back to disk.

Past API hits are persisted to the sqlite cache so re-runs only
hit the API for videos whose channel never resolved.

Resilient against partially-written brotli files: when one-shot
decompression fails but a streaming pass recovers parseable
JSON, the file is rewritten on disk with cleanly-encoded bytes.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import argparse
import asyncio
import datetime
import json
import os
import sqlite3
import sys
import time

from pathlib import Path
from dataclasses import dataclass, asdict
from urllib.parse import ParseResult, urlparse


import brotli

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from scrape_exchange.brotli import (
    _best_effort_decompress,
    brotli_write,
)
from scrape_exchange.datatypes import Platform
from scrape_exchange.exchange_client import ExchangeClient
from scrape_exchange.scrape_api import (
    EdgeResponse,
    GetDataResponseModel,
    PostFilterRequestModel,
    QueryResponseModel,
    fetch_dict_url,
    filter_data,
)
from scrape_exchange.youtube.youtube_channel import YouTubeChannel
from scrape_exchange.youtube.youtube_video import YouTubeVideo

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
_CRITICAL_AUGMENT_FIELDS: tuple[str, ...] = (
    'channel_id', 'channel_handle',
)
_VIDEO_PREFIXES: tuple[str, ...] = ('video-min-', 'video-dlp-')
_VIDEO_SUFFIX: str = '.json.br'
_MARKER_SUFFIXES: tuple[str, ...] = (
    '.failed', '.exists', '.unavailable', '.invalid',
    '.unresolved', '.not_found',
)
_FILTER_API_PATH: str = '/api/v1/filter'
_DEFAULT_EXCHANGE_URL: str = 'https://scrape.exchange'
_PREFERRED_CHANNEL_UPLOADERS: tuple[str, ...] = (
    'drand', 'nikkie', 'boinko', 'leady',
)
_ChannelApiCache = dict[tuple[str, str], dict | None]


@dataclass
class ChannelInfo:
    channel_id: str | None
    channel_handle: str
    channel_url: str
    channel_is_verified: bool
    subscriber_count: int | None


@dataclass(frozen=True)
class VideoProcessResult:
    needed_augmentation: bool = False
    augmented_from_local: bool = False
    augmented_from_api: bool = False
    unresolved: bool = False
    rewritten: bool = False
    error: bool = False


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


class AugmentSettings(BaseSettings):
    '''Pydantic-settings reader for the augment script.'''

    model_config = SettingsConfigDict(
        env_file='.env', env_file_encoding='utf-8', extra='ignore',
    )
    video_data_dir: str = Field(
        validation_alias='YOUTUBE_VIDEO_DATA_DIR',
    )
    exchange_url: str = Field(
        default=_DEFAULT_EXCHANGE_URL,
        validation_alias=AliasChoices('EXCHANGE_URL', 'exchange_url'),
    )


def _channel_info_from_dict(
    obj: dict,
) -> ChannelInfo | None:
    '''Extract a ``ChannelInfo`` from a channel record dict.

    Accepts whatever shape ``YouTubeChannel.from_dict`` accepts
    (either the on-disk channel-*.json.br format or the
    deserialised payload of ``GET data_url`` from the
    scrape.exchange API). Returns ``None`` if the mandatory
    fields are missing.
    '''
    try:
        channel: YouTubeChannel = YouTubeChannel.from_dict(obj)
    except Exception:
        channel = YouTubeChannel(
            channel_handle=obj.get('channel_handle'),
            channel_id=obj.get('channel_id'),
            with_download_client=False,
        )
        channel.verified = bool(obj.get('verified', False))
        channel.subscriber_count = obj.get('subscriber_count')

    channel_handle: str | None = channel.channel_handle
    if not channel_handle:
        return None
    channel_handle = channel_handle.lstrip('@')
    channel_id: str | None = channel.channel_id
    # Always store the canonical @handle URL — the channel file's
    # own `url` field may itself be the legacy /channel/UCxxx
    # form, and we want to upgrade videos to the @handle form
    # downstream.
    url: str = YouTubeChannel.CHANNEL_URL_WITH_AT.format(
        channel_handle=channel_handle,
    )
    sub_raw = channel.subscriber_count
    subscriber_count: int | None = (
        int(sub_raw)
        if isinstance(sub_raw, (int, float)) and sub_raw >= 0
        else None
    )
    return ChannelInfo(
        channel_id=channel_id,
        channel_handle=channel_handle,
        channel_url=url,
        channel_is_verified=bool(channel.verified),
        subscriber_count=subscriber_count,
    )


def load_table(path: Path) -> tuple[MappingDB, datetime.datetime | None]:
    '''
    Open the sqlite mapping table at *path*, importing the
    legacy ``augment_videos_table.json`` next to it if present
    and the DB is empty. Returns the open ``MappingDB`` and the
    persisted ``last_processed_at`` (``None`` on first run).
    '''
    print(f'opening sqlite table at {path}...', flush=True)
    started: float = time.monotonic()
    db: MappingDB = MappingDB(path)
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


def _video_id_from_name(name: str) -> str | None:
    if not name.endswith(_VIDEO_SUFFIX):
        return None
    if any(name.endswith(s) for s in _MARKER_SUFFIXES):
        return None
    suffix_len: int = len(_VIDEO_SUFFIX)
    for prefix in _VIDEO_PREFIXES:
        if name.startswith(prefix):
            return name[len(prefix):-suffix_len]
    return None


def _is_video_file(name: str) -> bool:
    return _video_id_from_name(name) is not None


def _video_from_obj(obj: dict) -> YouTubeVideo | None:
    try:
        return YouTubeVideo.from_dict(obj)
    except Exception:
        video = YouTubeVideo(
            video_id=obj.get('video_id'),
            channel_handle=obj.get('channel_handle'),
        )
        video.channel_id = obj.get('channel_id')
        video.channel_url = obj.get('channel_url')
        video.channel_is_verified = obj.get('channel_is_verified')
        video.channel_follower_count = obj.get('channel_follower_count')
        return video


def _apply_augmentation(
    video: YouTubeVideo, info: dict | None,
) -> tuple[bool, bool]:
    '''Mutate *video* with values from *info*.

    Returns ``(modified, still_needs_augmentation)``:
        * ``modified`` — True if any field was filled in.
        * ``still_needs_augmentation`` — True when *video* still has
          at least one augmentable field empty or stale.
    '''
    if info is None:
        return False, _needs_augmentation(video)
    modified: bool = False
    for video_field, info_field in _AUGMENT_FIELDS.items():
        current = getattr(video, video_field)
        if current not in (None, '') and not _is_stale(
            video_field, current,
        ):
            continue
        new_val = info.get(info_field)
        if new_val is None:
            continue
        setattr(video, video_field, new_val)
        modified = True
    return modified, _needs_augmentation(video)


def _needs_augmentation(video: YouTubeVideo) -> bool:
    for field in _CRITICAL_AUGMENT_FIELDS:
        current = getattr(video, field)
        if current in (None, ''):
            return True
        if _is_stale(field, current):
            return True
    return False


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
        brotli_write(path, obj)
    except OSError as exc:
        print(f'write failed {path}: {exc}', file=sys.stderr)
        return False
    return True


def _normalize_channel_handle(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    handle: str = value.strip()
    if not handle:
        return None
    if '/@' in handle:
        parsed: ParseResult = urlparse(handle)
        path: str = parsed.path or handle
        handle = path.split('/@', 1)[1].split('/', 1)[0]
    return handle.lstrip('@') or None


def _channel_lookup_filters(video: YouTubeVideo) -> list[tuple[str, str]]:
    filters: list[tuple[str, str]] = []
    channel_id: str | None = video.channel_id
    if isinstance(channel_id, str) and channel_id.strip():
        filters.append(('platform_content_id', channel_id.strip()))
    else:
        handle: str | None = _normalize_channel_handle(
            video.channel_handle,
        )
        if handle is None:
            handle = _normalize_channel_handle(video.channel_url)
        if handle is not None:
            filters.append(('platform_creator_id', handle))
    return filters


def _cache_channel_api_result(
    cache: _ChannelApiCache,
    lookup_filters: list[tuple[str, str]],
    info: dict | None,
) -> None:
    for key in lookup_filters:
        cache[key] = info
    if info is None:
        return
    channel_id = info.get('channel_id')
    if isinstance(channel_id, str) and channel_id:
        cache[('platform_content_id', channel_id)] = info
    handle: str | None = _normalize_channel_handle(
        info.get('channel_handle'),
    )
    if handle is not None:
        cache[('platform_creator_id', handle)] = info


async def _channel_info_from_edge(
    client: ExchangeClient,
    edge: EdgeResponse[GetDataResponseModel],
) -> dict | None:
    '''Download an edge's ``data_url`` via the shared
    :func:`scrape_exchange.scrape_api.fetch_dict_url` helper and
    project it onto our :class:`ChannelInfo` shape.

    ``fetch_dict_url`` handles brotli + salvage + plain-JSON
    fallback uniformly, so a single call covers every payload
    encoding the server might serve.
    '''
    try:
        data: dict = await fetch_dict_url(
            client, edge.node.data_url,
        )
    except Exception as exc:
        print(
            f'channel data request failed: {exc}',
            file=sys.stderr,
        )
        return None
    info: ChannelInfo | None = _channel_info_from_dict(data)
    return asdict(info) if info is not None else None


async def _fetch_channel_info_from_exchange(
    video: YouTubeVideo,
    client: ExchangeClient,
    cache: _ChannelApiCache,
) -> dict | None:
    '''Look up channel info for *video* via the scrape.exchange
    Filter + Data APIs.

    Goes through :func:`scrape_exchange.scrape_api.filter_data`
    for the lookup and :func:`fetch_dict_url` for the channel
    payload -- no direct httpx use here. The lookup tries each
    ``(field, value)`` pair from :func:`_channel_lookup_filters`
    (channel_id first, handle next) and, for each pair, prefers
    known uploaders in :data:`_PREFERRED_CHANNEL_UPLOADERS`
    before falling back to any uploader. Negative results are
    cached.
    '''
    lookup_filters: list[tuple[str, str]] = (
        _channel_lookup_filters(video)
    )
    if not lookup_filters:
        return None
    for key in lookup_filters:
        if key in cache:
            return cache[key]
    for field_name, value in lookup_filters:
        base_kwargs: dict[str, any] = {
            'platform': Platform.YOUTUBE,
            'entity': 'channel',
            field_name: value,
            'first': 1,
        }
        for uploader in _PREFERRED_CHANNEL_UPLOADERS:
            filters: PostFilterRequestModel = (
                PostFilterRequestModel(
                    **base_kwargs, username=uploader,
                )
            )
            page: QueryResponseModel[GetDataResponseModel]
            try:
                page = await filter_data(client, filters)
            except Exception as exc:
                print(
                    f'filter request failed for '
                    f'{value!r} (uploader={uploader}): {exc}',
                    file=sys.stderr,
                )
                continue
            for edge in page.edges:
                info: dict | None = (
                    await _channel_info_from_edge(client, edge)
                )
                if info is not None:
                    _cache_channel_api_result(
                        cache, lookup_filters, info,
                    )
                    return info
        filters = PostFilterRequestModel(**base_kwargs)
        try:
            page = await filter_data(client, filters)
        except Exception as exc:
            print(
                f'filter request failed for {value!r}: {exc}',
                file=sys.stderr,
            )
            continue
        for edge in page.edges:
            info = await _channel_info_from_edge(client, edge)
            if info is not None:
                _cache_channel_api_result(
                    cache, lookup_filters, info,
                )
                return info
    _cache_channel_api_result(cache, lookup_filters, None)
    return None


def _cache_mapping(
    mapping: 'MappingDB', video_id: str, info: dict,
) -> None:
    try:
        mapping[video_id] = info
    except (TypeError, AttributeError, KeyError):
        pass


def _read_json_br(
    path: Path,
) -> tuple[dict | None, bool]:
    '''Read a brotli-compressed JSON file at *path*.

    :returns: ``(obj, needs_rewrite)`` where ``obj`` is the
        parsed dict (or ``None`` if unrecoverable) and
        ``needs_rewrite`` is ``True`` when one-shot
        decompression failed but parsing eventually succeeded
        from the salvaged stream or from a plain-JSON fallback.
        The caller is expected to rewrite the file with the
        canonical brotli wrapper in that case.

    Augment-specific (vs :func:`scrape_exchange.brotli.brotli_read`):
    the salvage path uses both 64 KiB and 1-byte chunks (lower
    chunk size salvages more bytes when corruption straddles
    the first 64 KiB), and a plain-JSON fallback covers older
    on-disk records that landed without the brotli wrapper.
    '''
    try:
        compressed: bytes = path.read_bytes()
    except OSError as exc:
        print(f'read error {path}: {exc}', file=sys.stderr)
        return None, False
    if not compressed:
        print(f'empty file {path}', file=sys.stderr)
        return None, False
    try:
        obj = json.loads(brotli.decompress(compressed))
        return obj if isinstance(obj, dict) else None, False
    except brotli.error:
        pass

    salvaged: bytes = _best_effort_decompress(
        compressed, chunk_size=65536,
    )
    if not salvaged:
        salvaged = _best_effort_decompress(
            compressed, chunk_size=1,
        )
    if not salvaged:
        salvaged = compressed
    try:
        obj = json.loads(salvaged)
    except Exception as exc:
        print(
            f'json parse failed {path}: {exc}',
            file=sys.stderr,
        )
        return None, False
    if not isinstance(obj, dict):
        print(
            f'not a JSON object (got {type(obj).__name__}) '
            f'{path}',
            file=sys.stderr,
        )
        return None, False
    return obj, True


async def _process_one_video(
    entry: os.DirEntry, mapping: MappingDB, dry_run: bool,
    client: ExchangeClient,
    channel_api_cache: _ChannelApiCache | None = None,
) -> VideoProcessResult:
    '''
    Process a single video file entry.
    '''

    path: Path = Path(entry.path)
    obj: dict | None
    needs_rewrite: bool
    obj, needs_rewrite = _read_json_br(path)
    if obj is None:
        return VideoProcessResult(error=True)
    video: YouTubeVideo | None = _video_from_obj(obj)
    if video is None:
        return VideoProcessResult(error=True)
    video_id = video.video_id
    if not isinstance(video_id, str):
        return VideoProcessResult()

    needed_augmentation: bool = _needs_augmentation(video)
    if not needed_augmentation:
        if needs_rewrite:
            saved: bool = _save_augmented(
                path, video.to_dict(), dry_run,
            )
            return VideoProcessResult(
                rewritten=saved,
                error=not saved,
            )
        return VideoProcessResult()

    info: dict | None = mapping.get(video_id)
    local_modified: bool
    still_needs: bool
    local_modified, still_needs = _apply_augmentation(video, info)

    api_modified: bool = False
    if still_needs:
        if channel_api_cache is None:
            channel_api_cache = {}
        api_info: dict | None = (
            await _fetch_channel_info_from_exchange(
                video, client, channel_api_cache,
            )
        )
        if api_info is not None:
            _cache_mapping(mapping, video_id, api_info)
            api_modified, still_needs = _apply_augmentation(
                video, api_info,
            )

    if local_modified or api_modified or needs_rewrite:
        if len(path.parts) >= 2 and path.parts[-2] == 'uploaded':
            original_path: Path = path
            path = Path(*path.parts[:-2], 'uploaded', *path.parts[-1:])
            print(
                f'Moving augmented video from {original_path} to {path}',
                file=sys.stderr
            )
        saved = _save_augmented(path, video.to_dict(), dry_run)

        return VideoProcessResult(
            needed_augmentation=needed_augmentation,
            augmented_from_local=local_modified and saved,
            augmented_from_api=api_modified and saved,
            unresolved=still_needs,
            rewritten=saved,
            error=not saved,
        )
    return VideoProcessResult(
        needed_augmentation=needed_augmentation,
        unresolved=still_needs,
    )


_VIDEO_PROGRESS_INTERVAL: int = 10000
_VIDEO_PROGRESS_SECONDS: float = 60.0
_VIDEO_STATUS_SECONDS: float = 1.0


def _format_video_status(
    opened: int, needs_augmentation: int,
    augmented_from_local: int, augmented_from_api: int,
    no_channel: int,
    api_cache: _ChannelApiCache,
) -> str:
    cached_hits: int = sum(1 for value in api_cache.values() if value)
    cached_misses: int = sum(
        1 for value in api_cache.values() if value is None
    )
    return (
        f'video progress: opened={opened} '
        f'needs_augmentation={needs_augmentation} '
        f'local_augmented={augmented_from_local} '
        f'api_augmented={augmented_from_api} '
        f'no_channel={no_channel} '
        f'api_cache_hits={cached_hits} '
        f'api_cache_misses={cached_misses}'
    )


def _print_video_status(
    status: str, *, final: bool = False,
) -> None:
    if not sys.stdout.isatty():
        return
    end: str = '\n' if final else ''
    print(f'\r\033[K  {status}', end=end, file=sys.stdout, flush=True)


async def augment_videos(
    video_dir: Path,
    mapping: MappingDB,
    dry_run: bool,
    limit: int | None,
    client: ExchangeClient,
) -> tuple[int, int, int, int, int]:
    '''
    Walk *video_dir* and augment each video JSON brotli file
    in place when channel fields are missing.

    Emits a progress line to stderr whichever comes first:
    every ``_VIDEO_PROGRESS_INTERVAL`` scanned files OR
    ``_VIDEO_PROGRESS_SECONDS`` of wall-clock time since the
    last progress line. The time-based trigger ensures
    visibility even when reads are slow (e.g. cold disk
    cache, NFS hops) and the count-based trigger keeps the
    log compact when reads are fast. Returns
    ``(opened, local_augmented, api_augmented, no_channel, errors)``.
    '''
    print(
        f'starting video augmentation pass over {video_dir} '
        f'(mapping has {len(mapping):,} entries)',
        flush=True,
    )
    opened: int = 0
    needs_augmentation: int = 0
    local_augmented: int = 0
    api_augmented: int = 0
    no_channel: int = 0
    errors: int = 0
    channel_api_cache: _ChannelApiCache = {}
    last_progress_at: float = time.monotonic()
    last_status_at: float = 0.0
    for entry in os.scandir(video_dir):
        if limit is not None and opened >= limit:
            break
        if not entry.is_file(follow_symlinks=False):
            continue
        if not _is_video_file(entry.name):
            continue
        opened += 1

        now: float = time.monotonic()
        if now - last_status_at >= _VIDEO_STATUS_SECONDS:
            _print_video_status(
                _format_video_status(
                    opened, needs_augmentation, local_augmented,
                    api_augmented, no_channel,
                    channel_api_cache,
                ),
            )
            last_status_at = now
        if (
            opened % _VIDEO_PROGRESS_INTERVAL == 0
            or now - last_progress_at >= _VIDEO_PROGRESS_SECONDS
        ):
            _print_video_status(
                _format_video_status(
                    opened, needs_augmentation, local_augmented,
                    api_augmented, no_channel,
                    channel_api_cache,
                ),
                final=True,
            )
            print(
                f'  video progress: opened={opened} '
                f'needs_augmentation={needs_augmentation} '
                f'local_augmented={local_augmented} '
                f'api_augmented={api_augmented} '
                f'no_channel={no_channel} '
                f'errors={errors}',
                file=sys.stderr, flush=True,
            )
            last_progress_at = now
        result: VideoProcessResult = await _process_one_video(
            entry, mapping, dry_run, client, channel_api_cache,
        )
        if result.needed_augmentation:
            needs_augmentation += 1
        if result.augmented_from_local:
            local_augmented += 1
        if result.augmented_from_api:
            api_augmented += 1
        if result.unresolved:
            no_channel += 1
        if result.error:
            errors += 1
    _print_video_status(
        _format_video_status(
            opened, needs_augmentation, local_augmented,
            api_augmented, no_channel,
            channel_api_cache,
        ),
        final=True,
    )
    return opened, local_augmented, api_augmented, no_channel, errors


async def _main_async() -> int:
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description=(
            'Augment local video files with channel-level fields '
            'fetched from the scrape.exchange Filter + Data APIs. '
            'Past API hits are cached in '
            'augment_videos_table.sqlite3 next to the video dir.'
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
    table_path: Path = (
        video_dir.parent / 'augment_videos_table.sqlite3'
    )

    if not video_dir.is_dir():
        print(
            f'video dir missing: {video_dir}',
            file=sys.stderr,
        )
        return 2
    print(
        f'starting augment_videos: video_data_dir={video_dir}, '
        f'exchange_url={settings.exchange_url}',
    )

    mapping: MappingDB
    last_at: datetime.datetime | None
    mapping, last_at = load_table(table_path)
    print(
        f'loaded {len(mapping):,} cached channel mappings; '
        f'last_processed_at='
        f'{last_at.isoformat() if last_at else "(never)"}'
    )

    started_at: datetime.datetime = datetime.datetime.now(
        datetime.timezone.utc,
    )
    # Anonymous ExchangeClient: the Data + Filter GET / POST
    # endpoints are public reads, so we skip the JWT setup that
    # ExchangeClient.setup() does. Resolving relative data_url
    # paths needs the base_url to be set, which the constructor
    # handles.
    client: ExchangeClient = ExchangeClient(
        exchange_url=settings.exchange_url,
    )
    try:
        v_scanned: int
        local_augmented: int
        api_augmented: int
        no_channel: int
        v_errors: int
        v_scanned, local_augmented, api_augmented, no_channel, v_errors = (
            await augment_videos(
                video_dir, mapping, args.dry_run, args.limit,
                client,
            )
        )
        save_table(mapping, started_at)
        print(f'persisted table to {table_path}')
        print(
            f'video files: scanned={v_scanned} '
            f'local_augmented={local_augmented} '
            f'api_augmented={api_augmented} '
            f'no_channel={no_channel} '
            f'errors={v_errors}'
        )
        return 0 if v_errors == 0 else 1
    finally:
        await client.aclose()
        mapping.close()


def main() -> int:
    return asyncio.run(_main_async())


if __name__ == '__main__':
    sys.exit(main())
