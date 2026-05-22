#!/usr/bin/env python3

'''
Tool to import 3rd-party YouTube datasets by enqueueing every
discovered ``video_id`` to the Redis video scrape queue and
every newly-seen channel to the Redis channel scrape queue.
The production scrapers then fetch the actual video and channel
metadata directly from YouTube, so 3rd-party fields (title,
view_count, channel_subs, …) are not preserved — the dataset
serves only as a source of identifiers.

Datasets are downloaded via :mod:`kagglehub` (default cache:
``~/.cache/kaggle``). Auth comes from ``~/.kaggle/kaggle.json``
or the ``KAGGLE_USERNAME`` / ``KAGGLE_KEY`` env vars (env vars
override the file).

Each dataset's directory is walked recursively for ``.csv``,
``.jsonl`` (or ``.ndjson``) and ``.parquet`` files; for every
row a video_id and (optional) channel handle/id are extracted
via a column-alias mapping. Channel handles route to
``enqueue_unresolved``; valid ``UC…`` channel_ids route to
``enqueue_scheduled``. Re-running over the same dataset is a
near-no-op thanks to Redis ZADD-NX dedup in both queues.

The legacy ``--csv-file`` flag is preserved as a manual override
for offline work; when set, no Kaggle download happens and the
single file at that path is processed instead. The same format
dispatch applies — ``--csv-file foo.parquet`` works.

Examples:

  # Use the built-in dataset list:
  python tools/yt_import_3rdparty_data.py

  # Process one local file (manual mode):
  python tools/yt_import_3rdparty_data.py \\
      --csv-file /path/to/dataset.parquet

:author    : Boinko <boinko@scrape.exchange>
:copyright : 2026 Boinko
:license   : GPL-3.0
'''

import asyncio
import csv
import json
import logging
import os
import re

from pathlib import Path
from typing import Iterator, Literal
from dataclasses import dataclass, field

import kagglehub
import redis.asyncio as aioredis

# Private kagglehub helpers used by ``_dataset_is_cached`` to
# determine whether ``dataset_download`` will hit the local cache
# or trigger a fresh download.  These are private (leading-
# underscore in some cases, internal modules in others) and may
# be renamed across kagglehub minor versions, so the imports are
# guarded — failure here makes ``_dataset_is_cached`` return
# ``None`` and the caller falls back to the regular download
# path (the pre-existing behaviour).
try:
    from kagglehub.cache import Cache as _KhCache
    from kagglehub.clients import (
        build_kaggle_client as _kh_build_kaggle_client,
    )
    from kagglehub.handle import (
        parse_dataset_handle as _kh_parse_dataset_handle,
    )
    from kagglehub.http_resolver import (
        _get_current_version as _kh_get_current_version,
    )
    _KH_INTERNALS_AVAILABLE: bool = True
except ImportError:
    _KH_INTERNALS_AVAILABLE = False

from kagglehub.handle import DatasetHandle

from huggingface_hub import HfApi, snapshot_download
from huggingface_hub.errors import LocalEntryNotFoundError

import pyarrow.parquet as pq

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from scrape_exchange.logging import configure_logging
from scrape_exchange.settings import normalize_log_level
from scrape_exchange.video_scrape_queue import (
    RedisVideoScrapeQueue,
    VideoScrapeQueueSettings,
)
from scrape_exchange.channel_scrape_queue import (
    RedisChannelScrapeQueue,
    ChannelScrapeQueueSettings,
)

from scrape_exchange.youtube.youtube_channel import YouTubeChannel

_LOGGER: logging.Logger = logging.getLogger(__name__)


_VIDEO_ID_RE: re.Pattern[str] = re.compile(
    r'^[A-Za-z0-9_-]{11}$',
)

# YouTube handles: 3-30 characters, applied after stripping any
# leading "@". Allow letters / digits / combining marks from any
# script (CJK, Cyrillic, Arabic, Devanagari, Thai, …) by using a
# negative character class — reject only the characters that are
# clearly junk: whitespace, slashes, URL separators, quote
# marks, ``@``, backslash and ASCII control chars. The negative
# class lets through combining marks (Devanagari vowel signs,
# Arabic harakat, etc.) which ``\w`` excludes, so non-Latin
# handles round-trip. YouTube itself rejects handles that
# survive this filter but aren't real channels when the
# scraper later resolves them.
_CHANNEL_HANDLE_RE: re.Pattern[str] = re.compile(
    r'^[^\s/\\?#&=<>"\'@]{3,30}$',
)


# Dataset slugs (``owner/name``) processed by default. URLs and
# ``?select=...`` query strings copied straight from a Kaggle
# dataset page are also accepted; :func:`_parse_slug` strips
# them down to the bare slug and the loop below dedupes.
DEFAULT_KAGGLE_DATASETS: list[str] = [
    'canerkonuk/youtube-trending-videos-global',
    'sebastianbesinski/youtube-trending-videos-2025-updated-daily',
    'shebilmsp/youtube-trending-dataset-updated-daily',
    'senthil03/popular-youtube-videos-and-comments',
    'davidmarkawad/youtube-popularity-dataset',
    'asaniczka/trending-youtube-videos-113-countries'
]


# Logical field name -> ordered list of column-name candidates.
# Different uploads use different spellings; we try each candidate
# in order and pick the first non-empty value. Only the fields
# needed to identify videos and channels are kept — the rest of
# the dataset's metadata is discarded since the production
# scraper sources fresh data from YouTube.
COLUMN_ALIASES: dict[str, list[str]] = {
    'video_id': [
        'video_id', 'videoId', 'video_ID', 'id', 'youtube_id',
    ],
    'channel_id': ['channel_id', 'channelId', 'channel_ID'],
    'channel_handle': [
        'channel_handle', 'channelHandle', 'handle',
        'channel_username', 'channel_user',
    ],
}


_SUPPORTED_SUFFIXES: tuple[str, ...] = (
    '.csv', '.jsonl', '.ndjson', '.parquet',
)


# Match either a bare ``owner/name`` slug or the trailing
# ``owner/name`` portion of a full Kaggle URL. Stops at ``?`` and
# ``#`` so that ``?select=…`` query strings don't leak into the
# slug.
_KAGGLE_SLUG_RX: re.Pattern[str] = re.compile(
    r'(?:https?://(?:www\.)?kaggle\.com/datasets/)?'
    r'(?P<slug>[A-Za-z0-9][A-Za-z0-9_.-]*'
    r'/[A-Za-z0-9][A-Za-z0-9_.-]*)',
)


class ImportKaggleTrendingSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(Path(__file__).parent.parent / '.env'),
        env_file_encoding='utf-8',
        cli_parse_args=True,
        cli_kebab_case=True,
        populate_by_name=True,
        extra='ignore',
    )
    kaggle_datasets: list[str] = Field(
        default_factory=lambda: list(DEFAULT_KAGGLE_DATASETS),
        validation_alias=AliasChoices(
            'KAGGLE_DATASETS', 'kaggle_datasets',
        ),
        description=(
            'Kaggle dataset slugs (``owner/name``) or full '
            'dataset URLs to download and import. Repeat the '
            'flag or set the env var to a JSON list.'
        ),
    )
    kaggle_cache_dir: str = Field(
        default=str(Path.home() / '.cache' / 'kaggle'),
        validation_alias=AliasChoices(
            'KAGGLE_CACHE_DIR', 'kaggle_cache_dir',
        ),
        description=(
            'Local cache directory for kagglehub downloads. '
            'Exported as ``KAGGLEHUB_CACHE`` so kagglehub reads '
            'and writes there instead of its built-in default.'
        ),
    )
    hf_datasets: list[str] = Field(
        default_factory=list,
        validation_alias=AliasChoices(
            'HF_DATASETS', 'hf_datasets',
        ),
        description=(
            'Hugging Face dataset repo IDs (``org/name``) to '
            'download via ``huggingface_hub.snapshot_download`` '
            'and import. Repeat the flag or set the env var to '
            'a JSON list. Empty by default — opt-in.'
        ),
    )
    hf_cache_dir: str = Field(
        default=str(
            Path.home() / '.cache' / 'huggingface' / 'hub',
        ),
        validation_alias=AliasChoices(
            'HF_CACHE_DIR', 'hf_cache_dir',
        ),
        description=(
            'Local cache directory for huggingface_hub '
            'downloads. Exported as ``HF_HUB_CACHE`` so '
            'huggingface_hub reads and writes there instead of '
            'its built-in default.'
        ),
    )
    kaggle_dataset: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'KAGGLE_DATASET', 'kaggle_dataset',
        ),
        description=(
            'Ad-hoc single Kaggle dataset to process (slug or '
            'full dataset URL). When set, the configured '
            'defaults in ``kaggle_datasets`` AND ``hf_datasets`` '
            'are skipped — only the ad-hoc selection(s) are '
            'downloaded and imported. Useful for one-off '
            'backfills without editing the .env file. May be '
            'combined with ``--hf-dataset`` to back-fill one '
            'dataset from each provider in a single run.'
        ),
    )
    hf_dataset: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'HF_DATASET', 'hf_dataset',
        ),
        description=(
            'Ad-hoc single Hugging Face dataset (repo ID '
            '``org/name``) to process. Same precedence as '
            '``--kaggle-dataset``: when either ad-hoc flag is '
            'set, both default lists (``kaggle_datasets`` and '
            '``hf_datasets``) are skipped and only the ad-hoc '
            'selection(s) run.'
        ),
    )
    csv_file: str | None = Field(
        default=None,
        validation_alias=AliasChoices('CSV_FILE', 'csv_file'),
        description=(
            'Manual override: when set, skip the Kaggle '
            'download step and import this single file '
            'directly. Format is detected from the suffix '
            '(.csv, .jsonl/.ndjson, .parquet).'
        ),
    )
    redis_dsn: str = Field(
        default='redis://localhost:6379/0',
        validation_alias=AliasChoices(
            'REDIS_DSN', 'redis_dsn',
        ),
        description=(
            'Redis DSN for the video and channel scrape queues. '
            'Production: redis://localhost:6379/0.'
        ),
    )
    log_level: str = Field(
        default='INFO',
        validation_alias=AliasChoices('LOG_LEVEL', 'log_level'),
        description='Logging level (DEBUG, INFO, WARNING, ERROR).',
    )
    log_file: str = Field(
        default='/dev/stdout',
        validation_alias=AliasChoices('LOG_FILE', 'log_file'),
        description=(
            'Destination file for logs. ``/dev/stdout`` writes '
            'to standard output (the default).'
        ),
    )
    log_format: Literal['json', 'text'] = Field(
        default='text',
        validation_alias=AliasChoices('LOG_FORMAT', 'log_format'),
        description=(
            'Log format. ``json`` emits one structured JSON '
            'document per record (parsed into Elasticsearch); '
            '``text`` keeps the legacy colon-separated format.'
        ),
    )

    @field_validator('log_level', mode='before')
    @classmethod
    def _normalize_log_level(cls, v: str) -> str:
        return normalize_log_level(v)


@dataclass
class ImportStats:
    enqueued_videos: int = 0
    enqueued_channels: int = 0
    skipped: int = 0
    errors: int = 0

    def merge(self, other: 'ImportStats') -> None:
        self.enqueued_videos += other.enqueued_videos
        self.enqueued_channels += other.enqueued_channels
        self.skipped += other.skipped
        self.errors += other.errors


@dataclass
class DatasetReport:
    slug: str
    files: list[Path] = field(default_factory=list)
    stats: ImportStats = field(default_factory=ImportStats)
    download_error: str | None = None
    # ``True``  → ``dataset_download`` served from the local
    #            cache (no fresh data; the file walk is
    #            short-circuited).
    # ``False`` → kagglehub had to download a new version.
    # ``None``  → the cache pre-check was unavailable or
    #            failed; the caller fell back to the default
    #            download path and the cache state is unknown.
    cached: bool | None = None


def _parse_slug(value: str) -> str:
    '''
    Accept either a bare ``owner/name`` slug or a full Kaggle
    dataset URL (with optional ``?select=…`` query) and return
    the bare ``owner/name`` slug.
    '''
    match: re.Match[str] | None = _KAGGLE_SLUG_RX.search(
        value.strip(),
    )
    if not match:
        raise ValueError(
            f'cannot parse Kaggle dataset slug from {value!r}'
        )
    return match.group('slug')


def _pick(row: dict, field_name: str) -> str:
    '''
    Return the first non-empty trimmed value for *field_name*
    from *row*, trying each alias in :data:`COLUMN_ALIASES` in
    order. Returns ``''`` when no alias is present.
    '''
    for col in COLUMN_ALIASES.get(field_name, [field_name]):
        raw = row.get(col)
        if raw is None:
            continue
        text: str = str(raw).strip()
        if text:
            return text
    return ''


def _extract_channel(
    row: dict,
) -> tuple[str | None, str | None, str | None]:
    '''
    Return ``(dedup_key, channel_id, handle)`` for the channel
    represented by *row*, or ``(None, None, None)`` when neither
    a well-formed ``UC…`` ``channel_id`` nor a well-formed
    handle is present.

    The handle is left-stripped of any leading ``@`` and
    validated against :data:`_CHANNEL_HANDLE_RE`; the channel_id
    is validated against :data:`YouTubeChannel.CHANNEL_ID_REGEX_MATCH`. Junk
    values that fail validation are treated as absent so they never
    reach the Redis queue.

    The dedup key is the validated ``channel_id`` when present,
    otherwise the validated handle — channel_ids and handles
    live in disjoint namespaces in practice, so they share one
    ``seen`` set without collision risk.
    '''
    raw_channel_id: str = _pick(row, 'channel_id')
    raw_handle: str = _pick(row, 'channel_handle').lstrip('@')
    if (
        not raw_handle
        and row.get('channel')
        and str(row.get('channel'))[0] == '@'
    ):
        raw_handle = (
            str(row.get('channel')).lstrip('@').strip()
        )

    channel_id: str | None = (
        raw_channel_id
        if YouTubeChannel.CHANNEL_ID_REGEX_MATCH.fullmatch(raw_channel_id)
        else None
    )
    handle: str | None = (
        raw_handle
        if _CHANNEL_HANDLE_RE.fullmatch(raw_handle)
        else None
    )
    if channel_id is None and handle is None:
        return None, None, None

    key: str = channel_id or handle  # type: ignore[assignment]
    return key, channel_id, handle


def _find_data_files(root: Path) -> list[Path]:
    '''
    Return every file under *root* whose suffix is in
    :data:`_SUPPORTED_SUFFIXES`, sorted for determinism.
    '''
    matches: list[Path] = []
    for path in root.rglob('*'):
        if not path.is_file():
            continue
        if path.suffix.lower() in _SUPPORTED_SUFFIXES:
            matches.append(path)
    return sorted(matches)


def _iter_csv(path: Path) -> Iterator[dict]:
    with path.open(
        'r', newline='', encoding='utf-8', errors='replace',
    ) as f:
        reader: csv.DictReader = csv.DictReader(f)
        for row in reader:
            yield row


def _iter_jsonl(path: Path) -> Iterator[dict]:
    with path.open(
        'r', encoding='utf-8', errors='replace',
    ) as f:
        for raw in f:
            line: str = raw.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(obj, dict):
                yield obj


def _iter_parquet(path: Path) -> Iterator[dict]:
    # Stream by row groups so files larger than RAM stay safe.
    pf: pq.ParquetFile = pq.ParquetFile(str(path))
    for batch in pf.iter_batches(batch_size=4096):
        for row in batch.to_pylist():
            if isinstance(row, dict):
                yield row


def _iter_rows(path: Path) -> Iterator[dict]:
    '''
    Format-dispatching row iterator. Suffix-based: ``.csv``,
    ``.jsonl``/``.ndjson``, ``.parquet``. Unknown suffixes raise
    ``ValueError`` — callers filter via :func:`_find_data_files`.
    '''
    suffix: str = path.suffix.lower()
    if suffix == '.csv':
        return _iter_csv(path)
    if suffix in ('.jsonl', '.ndjson'):
        return _iter_jsonl(path)
    if suffix == '.parquet':
        return _iter_parquet(path)
    raise ValueError(f'unsupported file format: {path}')


async def _enqueue_channel(
    channel_queue: RedisChannelScrapeQueue,
    channel_id: str | None,
    handle: str | None,
    source: str,
) -> bool:
    '''
    Enqueue one channel. Returns ``True`` when something was
    enqueued, ``False`` when neither a validated channel_id nor
    a validated handle is present.

    Inputs are expected to be pre-validated by
    :func:`_extract_channel`: ``channel_id`` matches
    :data:`YouTubeChannel.CHANNEL_ID_REGEX_MATCH`, ``handle`` matches
    :data:`_CHANNEL_HANDLE_RE`. Validated ``channel_id``s route
    to ``enqueue_scheduled``; validated handles route to
    ``enqueue_unresolved``.
    '''
    if channel_id is not None:
        await channel_queue.enqueue_scheduled(
            channel_id, source=source,
        )
        return True
    if handle is not None:
        await channel_queue.enqueue_unresolved(
            handle, source=source,
        )
        return True
    return False


async def import_file(
    data_path: Path,
    video_queue: RedisVideoScrapeQueue,
    channel_queue: RedisChannelScrapeQueue,
    source: str,
    seen_channels: set[str],
) -> ImportStats:
    '''
    Iterate every row of *data_path* and enqueue each row's
    ``video_id`` to *video_queue* (skipping rows with no
    11-character video_id). For each newly-seen channel (deduped
    against *seen_channels*) enqueue the channel handle or
    channel_id to *channel_queue*.

    Returns counts of videos enqueued, channels enqueued, rows
    skipped (no video_id / malformed), and errors raised by the
    Redis layer.
    '''

    stats: ImportStats = ImportStats()
    try:
        rows: Iterator[dict] = _iter_rows(data_path)
    except Exception as exc:
        _LOGGER.error(
            'Cannot open data file', exc=exc,
            extra={'data_path': str(data_path)},
        )
        stats.errors += 1
        return stats

    row_num: int = 0
    try:
        for row_num, row in enumerate(rows, start=1):
            key: str | None
            channel_id: str | None
            handle: str | None
            key, channel_id, handle = _extract_channel(row)
            if key is not None and key not in seen_channels:
                seen_channels.add(key)
                try:
                    enqueued: bool = await _enqueue_channel(
                        channel_queue, channel_id, handle,
                        source,
                    )
                except Exception as exc:
                    _LOGGER.error(
                        'Failed to enqueue channel', exc=exc,
                        extra={
                            'data_file': data_path.name,
                            'row_num': row_num,
                            'channel_id': channel_id,
                            'handle': handle,
                        },
                    )
                    stats.errors += 1
                else:
                    if enqueued:
                        stats.enqueued_channels += 1

            video_id: str = _pick(row, 'video_id')
            if not video_id:
                stats.skipped += 1
                continue
            if not _VIDEO_ID_RE.fullmatch(video_id):
                stats.skipped += 1
                continue
            try:
                await video_queue.enqueue(
                    video_id, source=source,
                )
                stats.enqueued_videos += 1
            except Exception as exc:
                _LOGGER.error(
                    'Failed to enqueue video', exc=exc,
                    extra={
                        'data_file': data_path.name,
                        'row_num': row_num,
                        'video_id': video_id,
                    },
                )
                stats.errors += 1
    except Exception as exc:
        _LOGGER.error(
            'Row iteration failed', exc=exc,
            extra={
                'data_file': data_path.name,
                'row_num': row_num,
            },
        )
        stats.errors += 1
    return stats


def _dataset_is_cached(slug: str) -> bool | None:
    '''
    Return ``True`` if the resolved version of *slug* is already
    fully cached locally (so the next ``dataset_download`` call
    will be a cache hit), ``False`` if a download will happen,
    or ``None`` when the cache state cannot be determined (e.g.
    the underlying kagglehub helpers raised, or kagglehub was
    upgraded to a version that renamed them).

    Replicates the resolver check at
    ``kagglehub.http_resolver.DatasetHttpResolver._resolve``: if
    the slug is unversioned, fetch the current version via the
    Kaggle API, then ask :class:`Cache` whether that exact
    version is already on disk with a completion marker.

    A ``None`` return is non-fatal — the caller proceeds with the
    regular download path which is the pre-existing behaviour.
    '''
    if not _KH_INTERNALS_AVAILABLE:
        return None
    try:
        h: DatasetHandle = _kh_parse_dataset_handle(slug)
        if not h.is_versioned():
            with _kh_build_kaggle_client() as api_client:
                h = h.with_version(
                    _kh_get_current_version(api_client, h),
                )
        return _KhCache().load_from_cache(h) is not None
    except Exception as exc:
        _LOGGER.debug(
            'Cache pre-check failed; falling back to default '
            'download path',
            exc=exc, extra={'slug': slug},
        )
        return None


def _dataset_is_cached_hf(repo_id: str) -> bool | None:
    '''
    Hugging Face counterpart to :func:`_dataset_is_cached`.

    Asks the Hub API for *repo_id*'s current commit ``sha``, then
    asks ``huggingface_hub.snapshot_download`` whether the local
    cache already has that exact revision (via
    ``local_files_only=True``).

    * ``True``  → the next ``snapshot_download`` will be a cache
      hit (no new data).
    * ``False`` → a download will happen.
    * ``None``  → the pre-check failed (network error, missing
      auth, repo gone) and the cache state is unknown; the
      caller falls through to the regular download path.
    '''
    try:
        info = HfApi().dataset_info(repo_id)
    except Exception as exc:
        _LOGGER.debug(
            'HF dataset_info failed; cannot determine cache '
            'state',
            exc=exc, extra={'repo_id': repo_id},
        )
        return None
    if info.sha is None:
        return None
    try:
        snapshot_download(
            repo_id=repo_id,
            repo_type='dataset',
            revision=info.sha,
            local_files_only=True,
        )
        return True
    except LocalEntryNotFoundError:
        return False
    except Exception as exc:
        _LOGGER.debug(
            'HF cache pre-check failed; falling back to '
            'default download path',
            exc=exc, extra={'repo_id': repo_id},
        )
        return None


async def _walk_and_import(
    report: DatasetReport,
    local_path: Path,
    video_queue: RedisVideoScrapeQueue,
    channel_queue: RedisChannelScrapeQueue,
    source: str,
    seen_channels: set[str],
) -> None:
    '''
    Walk *local_path* for supported data files and run each
    through :func:`import_file`. Updates *report* in place.

    Shared by :func:`import_dataset` (Kaggle) and
    :func:`import_hf_dataset` (Hugging Face) — once the dataset's
    files are on disk, the downstream processing is identical.
    '''
    files: list[Path] = _find_data_files(local_path)
    if not files:
        _LOGGER.warning(
            'No supported data files found in dataset',
            extra={
                'slug': report.slug,
                'local_path': str(local_path),
                'supported_suffixes': list(_SUPPORTED_SUFFIXES),
            },
        )
        return
    report.files = files

    for data_path in files:
        _LOGGER.info(
            'Processing data file',
            extra={
                'slug': report.slug,
                'data_path': str(data_path),
            },
        )
        stats: ImportStats = await import_file(
            data_path, video_queue, channel_queue,
            source, seen_channels,
        )
        _LOGGER.info(
            'Data file complete',
            extra={
                'slug': report.slug,
                'data_file': data_path.name,
                'enqueued_videos': stats.enqueued_videos,
                'enqueued_channels': stats.enqueued_channels,
                'skipped': stats.skipped,
                'errors': stats.errors,
            },
        )
        report.stats.merge(stats)


async def import_dataset(
    slug: str,
    video_queue: RedisVideoScrapeQueue,
    channel_queue: RedisChannelScrapeQueue,
    seen_channels: set[str],
) -> DatasetReport:
    '''
    Download *slug* via :func:`kagglehub.dataset_download` and
    enqueue every recognised video_id / channel into the Redis
    queues.

    Before calling kagglehub the resolved version is checked
    against the local cache via :func:`_dataset_is_cached`. When
    the dataset is already cached (no new data) the file walk is
    skipped — Redis ZADD-NX would no-op anyway and skipping the
    walk saves CPU and avoids re-enqueueing the same channel
    keys.  ``report.cached`` records the outcome (True / False /
    None for unknown).
    '''

    report: DatasetReport = DatasetReport(slug=slug)
    report.cached = _dataset_is_cached(slug)
    _LOGGER.info(
        'Downloading dataset via kagglehub',
        extra={'slug': slug, 'cached': report.cached},
    )
    try:
        local_path: Path = Path(kagglehub.dataset_download(slug))
    except Exception as exc:
        report.download_error = (
            f'{type(exc).__name__}: {exc}'
        )
        report.stats.errors += 1
        _LOGGER.error(
            'Failed to download dataset', exc=exc,
            extra={
                'slug': slug,
                'download_error': report.download_error,
            },
        )
        return report

    if report.cached is True:
        _LOGGER.info(
            'Dataset already cached; skipping file walk',
            extra={'slug': slug, 'local_path': str(local_path)},
        )
        return report

    _LOGGER.info(
        'Dataset not cached, scanning for data files',
        extra={'slug': slug, 'local_path': str(local_path)},
    )
    source: str = f'3rdparty:kaggle:{slug}'
    await _walk_and_import(
        report, local_path, video_queue, channel_queue,
        source, seen_channels,
    )
    return report


async def import_hf_dataset(
    repo_id: str,
    video_queue: RedisVideoScrapeQueue,
    channel_queue: RedisChannelScrapeQueue,
    seen_channels: set[str],
) -> DatasetReport:
    '''
    Hugging Face counterpart to :func:`import_dataset`.

    Downloads *repo_id* via
    :func:`huggingface_hub.snapshot_download` (``repo_type=
    'dataset'``) and runs :func:`_walk_and_import` over the
    resulting snapshot directory.

    Before downloading, :func:`_dataset_is_cached_hf` checks
    whether the current revision is already on disk; on a hit
    the file walk is skipped (mirrors the Kaggle path).
    '''

    report: DatasetReport = DatasetReport(slug=repo_id)
    report.cached = _dataset_is_cached_hf(repo_id)
    _LOGGER.info(
        'Downloading dataset via huggingface_hub',
        extra={'repo_id': repo_id, 'cached': report.cached},
    )
    try:
        local_path: Path = Path(snapshot_download(
            repo_id=repo_id, repo_type='dataset',
        ))
    except Exception as exc:
        report.download_error = (
            f'{type(exc).__name__}: {exc}'
        )
        report.stats.errors += 1
        _LOGGER.error(
            'Failed to download HF dataset', exc=exc,
            extra={
                'repo_id': repo_id,
                'download_error': report.download_error,
            },
        )
        return report

    if report.cached is True:
        _LOGGER.info(
            'HF dataset already cached; skipping file walk',
            extra={
                'repo_id': repo_id,
                'local_path': str(local_path),
            },
        )
        return report

    _LOGGER.info(
        'HF dataset not cached, scanning for data files',
        extra={
            'repo_id': repo_id,
            'local_path': str(local_path),
        },
    )
    source: str = f'3rdparty:hf:{repo_id}'
    await _walk_and_import(
        report, local_path, video_queue, channel_queue,
        source, seen_channels,
    )
    return report


async def _run_manual_file(
    settings: ImportKaggleTrendingSettings,
    video_queue: RedisVideoScrapeQueue,
    channel_queue: RedisChannelScrapeQueue,
) -> int:
    '''
    Manual ``--csv-file`` mode: import a single local file
    straight into the Redis queues.
    '''

    data_path: Path = Path(settings.csv_file or '')
    if not data_path.is_file():
        _LOGGER.error(
            'Manual file not found',
            extra={'csv_file': str(data_path)},
        )
        return 2
    if data_path.suffix.lower() not in _SUPPORTED_SUFFIXES:
        _LOGGER.error(
            'Unsupported manual file suffix',
            extra={
                'csv_file': str(data_path),
                'suffix': data_path.suffix,
                'supported_suffixes': list(_SUPPORTED_SUFFIXES),
            },
        )
        return 2
    _LOGGER.info(
        'Manual file mode', extra={'csv_file': str(data_path)},
    )
    seen_channels: set[str] = set()
    source: str = f'3rdparty:manual:{data_path.name}'
    stats: ImportStats = await import_file(
        data_path, video_queue, channel_queue,
        source, seen_channels,
    )
    _LOGGER.info(
        'Manual file mode complete',
        extra={
            'enqueued_videos': stats.enqueued_videos,
            'enqueued_channels': stats.enqueued_channels,
            'skipped': stats.skipped,
            'errors': stats.errors,
        },
    )
    return 0 if stats.errors == 0 else 1


async def _import_kaggle_datasets(
    raw_slugs: list[str],
    video_queue: RedisVideoScrapeQueue,
    channel_queue: RedisChannelScrapeQueue,
    seen_channels: set[str],
    grand: ImportStats,
    reports: list[DatasetReport],
) -> None:
    '''
    Iterate *raw_slugs* and import each via
    :func:`import_dataset`. Caller owns *seen_channels*,
    *grand*, and *reports* and shares them across providers.
    '''
    seen_slugs: set[str] = set()
    for raw in raw_slugs:
        try:
            slug: str = _parse_slug(raw)
        except ValueError as exc:
            _LOGGER.error(
                'Cannot parse Kaggle dataset slug', exc=exc,
                extra={'raw_slug': raw},
            )
            grand.errors += 1
            continue
        if slug in seen_slugs:
            _LOGGER.info(
                'Skipping duplicate dataset',
                extra={'slug': slug},
            )
            continue
        seen_slugs.add(slug)
        report: DatasetReport = await import_dataset(
            slug, video_queue, channel_queue, seen_channels,
        )
        reports.append(report)
        grand.merge(report.stats)


async def _import_hf_datasets(
    raw_repo_ids: list[str],
    video_queue: RedisVideoScrapeQueue,
    channel_queue: RedisChannelScrapeQueue,
    seen_channels: set[str],
    grand: ImportStats,
    reports: list[DatasetReport],
) -> None:
    '''
    Iterate *raw_repo_ids* and import each via
    :func:`import_hf_dataset`. Symmetric with
    :func:`_import_kaggle_datasets`.
    '''
    seen_repo_ids: set[str] = set()
    for repo_id in raw_repo_ids:
        repo_id = repo_id.strip()
        if not repo_id:
            continue
        if repo_id in seen_repo_ids:
            _LOGGER.info(
                'Skipping duplicate dataset',
                extra={'repo_id': repo_id},
            )
            continue
        seen_repo_ids.add(repo_id)
        report: DatasetReport = await import_hf_dataset(
            repo_id, video_queue, channel_queue, seen_channels,
        )
        reports.append(report)
        grand.merge(report.stats)


async def _run_3rd_party(
    settings: ImportKaggleTrendingSettings,
    video_queue: RedisVideoScrapeQueue,
    channel_queue: RedisChannelScrapeQueue,
) -> int:
    '''
    Default mode: download every configured Kaggle and Hugging
    Face dataset and enqueue every discovered video_id / channel
    into the Redis queues. Both providers share a single
    ``seen_channels`` set so a channel discovered in (e.g.) a
    Kaggle slug isn't re-enqueued when it also appears in an HF
    repo.
    '''

    kaggle_cache_dir: Path = Path(
        settings.kaggle_cache_dir,
    ).expanduser()
    kaggle_cache_dir.mkdir(parents=True, exist_ok=True)
    # kagglehub reads this env var lazily on each call, so
    # exporting it before any download is sufficient. The
    # explicit ``KAGGLEHUB_CACHE`` name is what the library
    # documents; we don't reuse the legacy ``KAGGLE_CONFIG_DIR``
    # which controls credential location, not download cache.
    os.environ['KAGGLEHUB_CACHE'] = str(kaggle_cache_dir)

    hf_cache_dir: Path = Path(
        settings.hf_cache_dir,
    ).expanduser()
    hf_cache_dir.mkdir(parents=True, exist_ok=True)
    # ``HF_HUB_CACHE`` is the modern, narrowly-scoped env var
    # for the Hub download cache (``HF_HOME`` is the broader
    # config root). We point only the hub cache so other HF
    # state lives wherever the user has it configured.
    os.environ['HF_HUB_CACHE'] = str(hf_cache_dir)

    seen_channels: set[str] = set()
    grand: ImportStats = ImportStats()
    reports: list[DatasetReport] = []

    # ``--kaggle-dataset`` and ``--hf-dataset`` are ad-hoc,
    # single-shot overrides: when either is set, process only
    # the ad-hoc selection(s) and skip every default list. Both
    # may be set together to back-fill one dataset from each
    # provider in a single run.
    kaggle_slugs: list[str]
    hf_repo_ids: list[str]
    if settings.kaggle_dataset or settings.hf_dataset:
        _LOGGER.info(
            'Ad-hoc dataset specified; skipping the default '
            'Kaggle and HF dataset lists',
            extra={
                'kaggle_dataset': settings.kaggle_dataset,
                'hf_dataset': settings.hf_dataset,
                'skipped_kaggle_count': len(
                    settings.kaggle_datasets,
                ),
                'skipped_hf_count': len(settings.hf_datasets),
            },
        )
        kaggle_slugs = (
            [settings.kaggle_dataset]
            if settings.kaggle_dataset else []
        )
        hf_repo_ids = (
            [settings.hf_dataset]
            if settings.hf_dataset else []
        )
    else:
        kaggle_slugs = settings.kaggle_datasets
        hf_repo_ids = settings.hf_datasets

    await _import_kaggle_datasets(
        kaggle_slugs, video_queue, channel_queue,
        seen_channels, grand, reports,
    )
    await _import_hf_datasets(
        hf_repo_ids, video_queue, channel_queue,
        seen_channels, grand, reports,
    )

    _emit_summary(reports, grand)
    return 0 if grand.errors == 0 else 1


def _emit_summary(
    reports: list[DatasetReport], grand: ImportStats,
) -> None:
    for report in reports:
        if report.download_error:
            _LOGGER.warning(
                'Dataset download failed',
                extra={
                    'slug': report.slug,
                    'download_error': report.download_error,
                },
            )
            continue
        _LOGGER.info(
            'Dataset complete',
            extra={
                'slug': report.slug,
                'cached': report.cached,
                'files': len(report.files),
                'enqueued_videos': report.stats.enqueued_videos,
                'enqueued_channels': (
                    report.stats.enqueued_channels
                ),
                'skipped': report.stats.skipped,
                'errors': report.stats.errors,
            },
        )
    _LOGGER.info(
        'All datasets complete',
        extra={
            'enqueued_videos': grand.enqueued_videos,
            'enqueued_channels': grand.enqueued_channels,
            'skipped': grand.skipped,
            'errors': grand.errors,
        },
    )


async def main() -> int:
    settings: ImportKaggleTrendingSettings = (
        ImportKaggleTrendingSettings()
    )
    configure_logging(
        level=settings.log_level,
        filename=settings.log_file,
        log_format=settings.log_format,
    )

    redis: aioredis.Redis = aioredis.from_url(
        settings.redis_dsn, decode_responses=True,
    )
    video_queue: RedisVideoScrapeQueue = RedisVideoScrapeQueue(
        redis, VideoScrapeQueueSettings(),
    )
    channel_queue: RedisChannelScrapeQueue = (
        RedisChannelScrapeQueue(
            redis, ChannelScrapeQueueSettings(),
        )
    )
    try:
        if settings.csv_file:
            return await _run_manual_file(
                settings, video_queue, channel_queue,
            )
        return await _run_3rd_party(
            settings, video_queue, channel_queue,
        )
    finally:
        await redis.aclose()


if __name__ == '__main__':
    raise SystemExit(asyncio.run(main()))
