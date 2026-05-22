#!/usr/bin/env python3
'''
Report on persisted YouTube channel/video files.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import concurrent.futures
import csv
import datetime
import os
import sqlite3
import sys
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import brotli
import orjson
from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from scrape_exchange.file_management import MARKER_SUFFIXES
from scrape_exchange.youtube.youtube_channel import YouTubeChannel
from scrape_exchange.youtube.youtube_video import YouTubeVideo


_VIDEO_PREFIXES: tuple[str, ...] = ('video-min-', 'video-dlp-')
_CHANNEL_PREFIX: str = 'channel-'
_DATA_SUFFIX: str = '.json.br'
_REPORT_SCHEMA_VERSION: int = 1
_BATCH_SIZE: int = 500


class ReporterSettings(BaseSettings):
    '''Pydantic-settings reader for the reporter tool.'''

    model_config = SettingsConfigDict(
        env_file='.env', env_file_encoding='utf-8', extra='ignore',
        cli_kebab_case=True, cli_implicit_flags=True,
    )
    video_data_dir: str = Field(
        validation_alias=AliasChoices(
            'YOUTUBE_VIDEO_DATA_DIR',
            'video_data_dir',
        ),
    )
    channel_data_dir: str = Field(
        validation_alias=AliasChoices(
            'YOUTUBE_CHANNEL_DATA_DIR',
            'channel_data_dir',
        ),
    )
    video_priority_directory: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'YOUTUBE_VIDEO_PRIORITY_DIRECTORY',
            'youtube_video_priority_directory',
            'video_priority_directory',
        ),
    )
    channel_priority_directory: str = Field(
        default='priority',
        validation_alias=AliasChoices(
            'YOUTUBE_CHANNEL_PRIORITY_DIRECTORY',
            'youtube_channel_priority_directory',
            'channel_priority_directory',
        ),
    )
    db_path: Path | None = Field(
        default=None,
        validation_alias=AliasChoices('YT_REPORTER_DB_PATH', 'db_path'),
    )
    report_path: Path = Field(
        default=Path('yt_reporter.csv'),
        validation_alias=AliasChoices(
            'YT_REPORTER_REPORT_PATH',
            'report_path',
        ),
    )
    workers: int = Field(
        default_factory=lambda: os.cpu_count() or 1,
        validation_alias=AliasChoices('YT_REPORTER_WORKERS', 'workers'),
    )
    force: bool = Field(
        default=False,
        validation_alias=AliasChoices('YT_REPORTER_FORCE', 'force'),
    )
    limit: int | None = Field(
        default=None,
        validation_alias=AliasChoices('YT_REPORTER_LIMIT', 'limit'),
    )


@dataclass(frozen=True)
class ScanEntry:
    path: str
    kind: str
    dir_label: str
    prefix: str
    mtime: float
    size: int


@dataclass(frozen=True)
class FileResult:
    path: str
    kind: str
    mtime: float
    size: int
    decode_status: str
    issues: tuple[tuple[str, str], ...] = ()
    channel_id: str | None = None
    channel_handle: str | None = None
    channel_url: str | None = None


def _utc_now() -> str:
    return datetime.datetime.now(datetime.UTC).isoformat()


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == '':
        return True
    return False


def _resolve_child_dir(base: Path, configured: str | None) -> Path:
    raw: str = configured or 'priority'
    path: Path = Path(raw)
    return path if path.is_absolute() else base / path


def _scan_dirs(
    base: Path, priority_dir: Path,
) -> tuple[tuple[str, Path], ...]:
    return (
        ('root', base),
        ('uploaded', base / 'uploaded'),
        ('priority', priority_dir),
    )


def _video_prefix(name: str) -> str | None:
    for prefix in _VIDEO_PREFIXES:
        if name.startswith(prefix):
            return prefix
    return None


def _classify_entry(
    name: str, kind: str, dir_label: str,
) -> tuple[str, str] | None:
    marker_suffix: str | None = _marker_suffix(name)
    if kind == 'video':
        prefix: str | None = _video_prefix(name)
        if marker_suffix and name.startswith('video-'):
            return 'video_marker', marker_suffix
        if prefix and name.endswith(_DATA_SUFFIX):
            return 'video', prefix
        if dir_label == 'priority' and _is_bare_video_id(name):
            return 'video_priority_pending_id', 'bare-video-id'
        return None
    if marker_suffix and name.startswith(_CHANNEL_PREFIX):
        return 'channel_marker', marker_suffix
    if name.startswith(_CHANNEL_PREFIX) and name.endswith(_DATA_SUFFIX):
        return 'channel', _CHANNEL_PREFIX
    return None


def _marker_suffix(name: str) -> str | None:
    for suffix in MARKER_SUFFIXES:
        if name.endswith(suffix):
            return suffix
    return None


def _is_bare_video_id(name: str) -> bool:
    if len(name) != 11:
        return False
    return all(ch.isalnum() or ch in '_-' for ch in name)


def _iter_scan_entries(
    dirs: tuple[tuple[str, Path], ...], kind: str, limit: int | None = None,
) -> tuple[list[ScanEntry], Counter[str]]:
    entries: list[ScanEntry] = []
    counts: Counter[str] = Counter()
    for dir_label, directory in dirs:
        if not directory.is_dir():
            print(f'skipping missing {kind} dir: {directory}', file=sys.stderr)
            continue
        with os.scandir(directory) as it:
            for entry in it:
                if limit is not None and len(entries) >= limit:
                    return entries, counts
                if not entry.is_file(follow_symlinks=False):
                    continue
                classified: tuple[str, str] | None = _classify_entry(
                    entry.name, kind, dir_label
                )
                if classified is None:
                    continue
                entry_kind, prefix = classified
                try:
                    stat: os.stat_result = entry.stat(follow_symlinks=False)
                except OSError:
                    continue
                counts[f'files.{entry_kind}.{dir_label}.{prefix}'] += 1
                if entry_kind not in ('video', 'channel'):
                    continue
                entries.append(
                    ScanEntry(
                        path=entry.path,
                        kind=entry_kind,
                        dir_label=dir_label,
                        prefix=prefix,
                        mtime=stat.st_mtime,
                        size=stat.st_size,
                    )
                )
    return entries, counts


def _read_json_br(path: str) -> dict[str, Any]:
    raw: any = brotli.decompress(Path(path).read_bytes())
    data: any = orjson.loads(raw)
    if not isinstance(data, dict):
        raise ValueError('top-level JSON value is not an object')
    return data


def _process_channel_entry(entry: ScanEntry) -> FileResult:
    issues: list[tuple[str, str]] = []
    try:
        data: dict[str, any] = _read_json_br(entry.path)
    except brotli.error as exc:
        return _failed_result(entry, 'brotli_decompress_failed', str(exc))
    except orjson.JSONDecodeError as exc:
        return _failed_result(entry, 'json_parse_failed', str(exc))
    except Exception as exc:
        return _failed_result(entry, 'json_parse_failed', str(exc))
    try:
        channel = YouTubeChannel.from_dict(
            data, with_download_client=False,
        )
    except Exception as exc:
        return _failed_result(entry, 'model_deserialize_failed', str(exc))

    if _is_missing(channel.channel_id):
        issues.append(('channel_missing_channel_id', 'channel_id missing'))
    if _is_missing(channel.channel_handle):
        issues.append(
            ('channel_missing_channel_handle', 'channel_handle missing')
        )
    if _is_missing(channel.url):
        issues.append(('channel_missing_url', 'url missing'))
    if not channel.channel_thumbnails:
        issues.append(
            ('channel_missing_channel_thumbnails', 'no channel thumbnails')
        )
    if channel.view_count == 0:
        issues.append(('channel_view_count_zero', 'view_count is 0'))

    return FileResult(
        path=entry.path,
        kind='channel',
        mtime=entry.mtime,
        size=entry.size,
        decode_status='ok',
        issues=tuple(issues),
        channel_id=channel.channel_id,
        channel_handle=(
            channel.channel_handle.lstrip('@')
            if channel.channel_handle else None
        ),
        channel_url=channel.url,
    )


def _process_video_entry(
    entry: ScanEntry, db_path: str,
) -> FileResult:
    issues: list[tuple[str, str]] = []
    try:
        data: dict[str, any] = _read_json_br(entry.path)
    except brotli.error as exc:
        return _failed_result(entry, 'brotli_decompress_failed', str(exc))
    except orjson.JSONDecodeError as exc:
        return _failed_result(entry, 'json_parse_failed', str(exc))
    except Exception as exc:
        return _failed_result(entry, 'json_parse_failed', str(exc))
    try:
        video = YouTubeVideo.from_dict(data)
    except Exception as exc:
        return _failed_result(entry, 'model_deserialize_failed', str(exc))

    if _is_missing(video.channel_id):
        issues.append(('video_missing_channel_id', 'channel_id missing'))
    if _is_missing(video.channel_handle):
        issues.append(
            ('video_missing_channel_handle', 'channel_handle missing')
        )
    if _is_missing(video.channel_url):
        issues.append(('video_missing_channel_url', 'channel_url missing'))
    if _is_missing(video.embed_url):
        issues.append(('video_missing_embed_url', 'embed_url missing'))
    if not video.formats:
        issues.append(('video_missing_formats', 'formats empty'))
    if not video.thumbnails:
        issues.append(('video_missing_thumbnails', 'thumbnails empty'))
    elif any(_is_missing(t.url) for t in video.thumbnails.values()):
        issues.append(
            ('video_thumbnail_missing_url', 'one or more thumbnail URLs')
        )
    if (
        (
            not _is_missing(video.channel_id)
            or not _is_missing(video.channel_handle)
        ) and not _channel_exists(
            db_path, video.channel_id, video.channel_handle
        )
    ):
        issues.append(
            (
                'video_channel_not_in_channel_db',
                'no matching channel_id or channel_handle',
            )
        )

    return FileResult(
        path=entry.path,
        kind='video',
        mtime=entry.mtime,
        size=entry.size,
        decode_status='ok',
        issues=tuple(issues),
        channel_id=video.channel_id,
        channel_handle=(
            video.channel_handle.lstrip('@')
            if video.channel_handle else None
        ),
        channel_url=video.channel_url,
    )


def _failed_result(
    entry: ScanEntry, issue_code: str, detail: str,
) -> FileResult:
    return FileResult(
        path=entry.path,
        kind=entry.kind,
        mtime=entry.mtime,
        size=entry.size,
        decode_status=issue_code,
        issues=((issue_code, detail[:500]),),
    )


def _channel_exists(
    db_path: str, channel_id: str | None, channel_handle: str | None,
) -> bool:
    conn: sqlite3.Connection = sqlite3.connect(db_path)
    try:
        if not _is_missing(channel_id):
            row = conn.execute(
                'SELECT 1 FROM channel_index WHERE channel_id = ? LIMIT 1',
                (channel_id,),
            ).fetchone()
            if row is not None:
                return True
        if not _is_missing(channel_handle):
            handle: str = str(channel_handle).lstrip('@')
            row = conn.execute(
                'SELECT 1 FROM channel_index '
                'WHERE channel_handle = ? LIMIT 1',
                (handle,),
            ).fetchone()
            return row is not None
        return False
    finally:
        conn.close()


def _process_channel_batch(entries: list[ScanEntry]) -> list[FileResult]:
    return [_process_channel_entry(entry) for entry in entries]


def _process_video_batch(
    args: tuple[list[ScanEntry], str],
) -> list[FileResult]:
    entries, db_path = args
    return [_process_video_entry(entry, db_path) for entry in entries]


class ReporterDB:
    _SCHEMA: str = '''
    CREATE TABLE IF NOT EXISTS metadata (
        key TEXT PRIMARY KEY,
        value TEXT
    );
    CREATE TABLE IF NOT EXISTS runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        started_at TEXT NOT NULL,
        finished_at TEXT,
        status TEXT NOT NULL,
        workers INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS files (
        path TEXT PRIMARY KEY,
        kind TEXT NOT NULL,
        dir_label TEXT NOT NULL,
        prefix TEXT NOT NULL,
        mtime REAL NOT NULL,
        size INTEGER NOT NULL,
        last_seen_run_id INTEGER NOT NULL,
        last_processed_mtime REAL,
        decode_status TEXT
    );
    CREATE TABLE IF NOT EXISTS channel_index (
        path TEXT PRIMARY KEY,
        channel_id TEXT,
        channel_handle TEXT,
        url TEXT,
        mtime REAL NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_channel_index_id
        ON channel_index(channel_id);
    CREATE INDEX IF NOT EXISTS idx_channel_index_handle
        ON channel_index(channel_handle);
    CREATE TABLE IF NOT EXISTS video_index (
        path TEXT PRIMARY KEY,
        channel_id TEXT,
        channel_handle TEXT,
        mtime REAL NOT NULL
    );
    CREATE TABLE IF NOT EXISTS file_issues (
        path TEXT NOT NULL,
        issue_code TEXT NOT NULL,
        detail TEXT,
        PRIMARY KEY (path, issue_code, detail)
    );
    CREATE INDEX IF NOT EXISTS idx_file_issues_code
        ON file_issues(issue_code);
    CREATE TABLE IF NOT EXISTS run_stats (
        run_id INTEGER NOT NULL,
        stat_key TEXT NOT NULL,
        stat_value INTEGER NOT NULL,
        PRIMARY KEY (run_id, stat_key)
    );
    '''

    def __init__(self, path: Path) -> None:
        self.path: Path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn: sqlite3.Connection = sqlite3.connect(str(path))
        self.conn.executescript(self._SCHEMA)
        self.conn.execute('PRAGMA journal_mode = WAL')
        self.conn.execute('PRAGMA synchronous = NORMAL')
        self.conn.execute(
            'INSERT OR REPLACE INTO metadata(key, value) VALUES (?, ?)',
            ('schema_version', str(_REPORT_SCHEMA_VERSION)),
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def start_run(self, workers: int) -> int:
        cur: sqlite3.Cursor = self.conn.execute(
            'INSERT INTO runs(started_at, status, workers) VALUES (?, ?, ?)',
            (_utc_now(), 'running', workers),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def finish_run(self, run_id: int, status: str) -> None:
        self.conn.execute(
            'UPDATE runs SET finished_at = ?, status = ? WHERE id = ?',
            (_utc_now(), status, run_id),
        )
        if status == 'ok':
            self.conn.execute(
                'INSERT OR REPLACE INTO metadata(key, value) VALUES (?, ?)',
                ('last_successful_run_at', _utc_now()),
            )
        self.conn.commit()

    def upsert_seen(self, run_id: int, entry: ScanEntry) -> None:
        self.conn.execute(
            'INSERT INTO files('
            'path, kind, dir_label, prefix, mtime, size, '
            'last_seen_run_id, last_processed_mtime, decode_status'
            ') VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL) '
            'ON CONFLICT(path) DO UPDATE SET '
            'kind = excluded.kind, dir_label = excluded.dir_label, '
            'prefix = excluded.prefix, mtime = excluded.mtime, '
            'size = excluded.size, '
            'last_seen_run_id = excluded.last_seen_run_id',
            (
                entry.path, entry.kind, entry.dir_label, entry.prefix,
                entry.mtime, entry.size, run_id,
            ),
        )

    def needs_processing(self, entry: ScanEntry, force: bool) -> bool:
        if force:
            return True
        row = self.conn.execute(
            'SELECT last_processed_mtime FROM files WHERE path = ?',
            (entry.path,),
        ).fetchone()
        if row is None or row[0] is None:
            return True
        return float(entry.mtime) > float(row[0])

    def store_result(self, result: FileResult) -> None:
        self.conn.execute('DELETE FROM file_issues WHERE path = ?',
                          (result.path,))
        self.conn.executemany(
            'INSERT INTO file_issues(path, issue_code, detail) '
            'VALUES (?, ?, ?)',
            [
                (result.path, issue_code, detail)
                for issue_code, detail in result.issues
            ],
        )
        self.conn.execute(
            'UPDATE files SET last_processed_mtime = ?, '
            'decode_status = ? WHERE path = ?',
            (result.mtime, result.decode_status, result.path),
        )
        if result.kind == 'channel':
            if result.decode_status == 'ok':
                self.conn.execute(
                    'INSERT INTO channel_index('
                    'path, channel_id, channel_handle, url, mtime'
                    ') VALUES (?, ?, ?, ?, ?) '
                    'ON CONFLICT(path) DO UPDATE SET '
                    'channel_id = excluded.channel_id, '
                    'channel_handle = excluded.channel_handle, '
                    'url = excluded.url, mtime = excluded.mtime',
                    (
                        result.path, result.channel_id,
                        result.channel_handle, result.channel_url,
                        result.mtime,
                    ),
                )
            else:
                self.conn.execute(
                    'DELETE FROM channel_index WHERE path = ?',
                    (result.path,),
                )
        if result.kind == 'video':
            if result.decode_status == 'ok':
                self.conn.execute(
                    'INSERT INTO video_index('
                    'path, channel_id, channel_handle, mtime'
                    ') VALUES (?, ?, ?, ?) '
                    'ON CONFLICT(path) DO UPDATE SET '
                    'channel_id = excluded.channel_id, '
                    'channel_handle = excluded.channel_handle, '
                    'mtime = excluded.mtime',
                    (
                        result.path, result.channel_id,
                        result.channel_handle, result.mtime,
                    ),
                )
            else:
                self.conn.execute(
                    'DELETE FROM video_index WHERE path = ?',
                    (result.path,),
                )

    def store_stats(self, run_id: int, stats: dict[str, int]) -> None:
        self.conn.execute('DELETE FROM run_stats WHERE run_id = ?', (run_id,))
        self.conn.executemany(
            'INSERT INTO run_stats(run_id, stat_key, stat_value) '
            'VALUES (?, ?, ?)',
            [(run_id, key, value) for key, value in sorted(stats.items())],
        )
        self.conn.commit()

    def prune_stale_channel_index(self, run_id: int) -> None:
        self.conn.execute(
            'DELETE FROM channel_index WHERE path NOT IN ('
            'SELECT path FROM files '
            'WHERE kind = ? AND last_seen_run_id = ?'
            ')',
            ('channel', run_id),
        )
        self.conn.commit()

    def prune_stale_video_index(self, run_id: int) -> None:
        self.conn.execute(
            'DELETE FROM video_index WHERE path NOT IN ('
            'SELECT path FROM files '
            'WHERE kind = ? AND last_seen_run_id = ?'
            ')',
            ('video', run_id),
        )
        self.conn.commit()

    def recompute_video_channel_issues(self, run_id: int) -> None:
        issue_code = 'video_channel_not_in_channel_db'
        self.conn.execute(
            'DELETE FROM file_issues WHERE issue_code = ? '
            'AND path IN ('
            'SELECT path FROM files '
            'WHERE kind = ? AND last_seen_run_id = ?'
            ')',
            (issue_code, 'video', run_id),
        )
        rows: list[any] = self.conn.execute(
            'SELECT video_index.path FROM video_index '
            'JOIN files ON files.path = video_index.path '
            'WHERE files.kind = ? AND files.last_seen_run_id = ? '
            'AND ('
            '(video_index.channel_id IS NOT NULL '
            'AND video_index.channel_id != "") '
            'OR (video_index.channel_handle IS NOT NULL '
            'AND video_index.channel_handle != "")'
            ') '
            'AND NOT EXISTS ('
            'SELECT 1 FROM channel_index '
            'WHERE channel_index.channel_id = video_index.channel_id'
            ') '
            'AND NOT EXISTS ('
            'SELECT 1 FROM channel_index '
            'WHERE channel_index.channel_handle = video_index.channel_handle'
            ')',
            ('video', run_id),
        ).fetchall()
        self.conn.executemany(
            'INSERT OR IGNORE INTO file_issues(path, issue_code, detail) '
            'VALUES (?, ?, ?)',
            [
                (
                    path, issue_code,
                    'no matching channel_id or channel_handle',
                )
                for (path,) in rows
            ],
        )
        self.conn.commit()

    def current_issue_stats(self, run_id: int) -> dict[str, int]:
        rows: list[any] = self.conn.execute(
            'SELECT issue_code, COUNT(*) FROM file_issues '
            'JOIN files USING(path) '
            'WHERE files.last_seen_run_id = ? '
            'GROUP BY issue_code',
            (run_id,),
        ).fetchall()
        return {f'issues.{code}': int(count) for code, count in rows}


def _chunks(items: list[ScanEntry], size: int) -> list[list[ScanEntry]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


class StatusLine:
    def __init__(self) -> None:
        self.channels: int = 0
        self.videos: int = 0
        self.enabled: bool = sys.stdout.isatty()
        self.active: bool = False

    def update(
        self, *, channels: int | None = None,
        videos: int | None = None,
    ) -> None:
        if channels is not None:
            self.channels = channels
        if videos is not None:
            self.videos = videos
        if not self.enabled:
            return
        print(
            f'\r\033[Kprocessed: '
            f'channel_files={self.channels:,} '
            f'video_files={self.videos:,}',
            end='',
            flush=True,
        )
        self.active = True

    def clear(self) -> None:
        if self.enabled and self.active:
            print('\r\033[K', end='', flush=True)
            self.active = False

    def finish(self) -> None:
        if self.enabled and self.active:
            print('', flush=True)
            self.active = False


def _process_entries(
    entries: list[ScanEntry], kind: str, db: ReporterDB, run_id: int,
    workers: int, force: bool, status_line: StatusLine,
) -> int:
    to_process: list[ScanEntry] = []
    for entry in entries:
        db.upsert_seen(run_id, entry)
        if db.needs_processing(entry, force):
            to_process.append(entry)
    db.conn.commit()
    if not to_process:
        _update_status_line(status_line, kind, 0)
        return 0

    processed = 0
    batches: list[list[ScanEntry]] = _chunks(to_process, _BATCH_SIZE)
    if workers <= 1:
        if kind == 'channel':
            results_iter = (_process_channel_batch(batch) for batch in batches)
        else:
            results_iter = (
                _process_video_batch((batch, str(db.path)))
                for batch in batches
            )
        for results in results_iter:
            for result in results:
                db.store_result(result)
                processed += 1
            _update_status_line(status_line, kind, processed)
        db.conn.commit()
        return processed

    with concurrent.futures.ProcessPoolExecutor(
        max_workers=workers,
    ) as pool:
        if kind == 'channel':
            futures = [
                pool.submit(_process_channel_batch, batch)
                for batch in batches
            ]
        else:
            futures = [
                pool.submit(_process_video_batch, (batch, str(db.path)))
                for batch in batches
            ]
        for fut in concurrent.futures.as_completed(futures):
            for result in fut.result():
                db.store_result(result)
                processed += 1
            db.conn.commit()
            _update_status_line(status_line, kind, processed)
    return processed


def _update_status_line(
    status_line: StatusLine, kind: str, processed: int,
) -> None:
    if kind == 'channel':
        status_line.update(channels=processed)
    elif kind == 'video':
        status_line.update(videos=processed)


def _read_csv_header(path: Path) -> list[str] | None:
    try:
        with path.open('r', newline='', encoding='utf-8') as fh:
            return next(csv.reader(fh), None)
    except OSError:
        return None


def _csv_fieldnames(
    path: Path, stats: dict[str, int], existed_at_start: bool,
) -> list[str]:
    if existed_at_start and path.exists() and path.stat().st_size > 0:
        header = _read_csv_header(path)
        if header:
            return header
    return ['generated_at', *sorted(stats)]


def _csv_row(fieldnames: list[str], stats: dict[str, int]) -> dict[str, Any]:
    row: dict[str, Any] = {field: stats.get(field, '') for field in fieldnames}
    if 'generated_at' in fieldnames:
        row['generated_at'] = _utc_now()
    return row


def _write_csv_report(
    path: Path, stats: dict[str, int], existed_at_start: bool,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = _csv_fieldnames(path, stats, existed_at_start)
    append: bool = (
        existed_at_start and path.exists() and path.stat().st_size > 0
    )
    with path.open('a' if append else 'w', newline='', encoding='utf-8') as fh:
        writer: csv.DictWriter[str] = csv.DictWriter(fh, fieldnames=fieldnames)
        if not append:
            writer.writeheader()
        writer.writerow(_csv_row(fieldnames, stats))


def _build_stats(
    run_id: int, scan_counts: Counter[str], db: ReporterDB,
    processed_channels: int, processed_videos: int,
) -> dict[str, int]:
    stats: dict[str, int] = {
        key: int(value) for key, value in scan_counts.items()
    }
    stats['processed.channel'] = processed_channels
    stats['processed.video'] = processed_videos
    stats.update(db.current_issue_stats(run_id))
    row = db.conn.execute(
        'SELECT COUNT(*) FROM channel_index'
    ).fetchone()
    stats['channel_index.entries'] = int(row[0] if row else 0)
    return stats


def run_reporter(
    channel_dir: Path, video_dir: Path, db_path: Path, report_path: Path,
    workers: int, force: bool = False, limit: int | None = None,
    channel_priority_dir: Path | None = None,
    video_priority_dir: Path | None = None,
) -> dict[str, int]:
    started: float = time.monotonic()
    workers = max(1, workers)
    report_existed_at_start: bool = report_path.exists()
    db = ReporterDB(db_path)
    run_id = db.start_run(workers)
    status = 'error'
    status_line = StatusLine()
    try:
        channel_priority = (
            channel_priority_dir
            if channel_priority_dir is not None
            else _resolve_child_dir(channel_dir, 'priority')
        )
        video_priority: Path = (
            video_priority_dir
            if video_priority_dir is not None
            else _resolve_child_dir(video_dir, 'priority')
        )
        channel_entries: list[ScanEntry]
        channel_counts: Counter[str]
        video_entries: list[ScanEntry]
        video_counts: Counter[str]
        scan_counts: Counter[str]
        channel_entries, channel_counts = _iter_scan_entries(
            _scan_dirs(channel_dir, channel_priority), 'channel', limit,
        )
        video_entries, video_counts = _iter_scan_entries(
            _scan_dirs(video_dir, video_priority), 'video', limit,
        )
        scan_counts = channel_counts + video_counts

        print(f'found {len(channel_entries):,} channel data files')
        processed_channels: int = _process_entries(
            channel_entries, 'channel', db, run_id, workers, force,
            status_line,
        )
        db.prune_stale_channel_index(run_id)
        status_line.clear()
        print(f'found {len(video_entries):,} video data files')
        processed_videos: int = _process_entries(
            video_entries, 'video', db, run_id, workers, force,
            status_line,
        )
        status_line.finish()
        db.prune_stale_video_index(run_id)
        db.recompute_video_channel_issues(run_id)
        stats: dict[str, int] = _build_stats(
            run_id, scan_counts, db, processed_channels, processed_videos,
        )
        db.store_stats(run_id, stats)
        _write_csv_report(report_path, stats, report_existed_at_start)
        status = 'ok'
        print(
            f'reporter finished in {time.monotonic() - started:.1f}s',
            flush=True,
        )
        return stats
    finally:
        status_line.finish()
        db.finish_run(run_id, status)
        db.close()


def main() -> int:
    settings = ReporterSettings(_cli_parse_args=True)
    video_dir = Path(settings.video_data_dir)
    channel_dir = Path(settings.channel_data_dir)
    db_path: Path = settings.db_path or video_dir / 'yt_reporter.sqlite3'
    report_path: Path = settings.report_path
    channel_priority_dir = _resolve_child_dir(
        channel_dir, settings.channel_priority_directory,
    )
    video_priority_dir = (
        _resolve_child_dir(video_dir, settings.video_priority_directory)
        if settings.video_priority_directory is not None
        else video_dir / 'priority'
    )
    if not video_dir.is_dir():
        print(f'video dir missing: {video_dir}', file=sys.stderr)
        return 2
    if not channel_dir.is_dir():
        print(f'channel dir missing: {channel_dir}', file=sys.stderr)
        return 2
    run_reporter(
        channel_dir=channel_dir,
        video_dir=video_dir,
        db_path=db_path,
        report_path=report_path,
        workers=settings.workers,
        force=settings.force,
        limit=settings.limit,
        channel_priority_dir=channel_priority_dir,
        video_priority_dir=video_priority_dir,
    )
    print(f'wrote sqlite database: {db_path}')
    print(f'wrote csv report: {report_path}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
