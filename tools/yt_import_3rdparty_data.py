#!/usr/bin/env python3

'''
Tool to import 3rd-party YouTube datasets into
``video-min-<video_id>.json.br`` files compatible with the
``YouTubeVideo`` model.

Datasets are downloaded via :mod:`kagglehub` (default cache:
``~/.cache/kaggle``). Auth comes from ``~/.kaggle/kaggle.json``
or the ``KAGGLE_USERNAME`` / ``KAGGLE_KEY`` env vars (env vars
override the file).

Each dataset's directory is walked recursively for ``.csv``,
``.jsonl`` (or ``.ndjson``) and ``.parquet`` files; rows are
converted to ``YouTubeVideo`` instances via a column-alias
mapping (different uploads use different column-name spellings).
Rows missing a ``video_id`` are skipped, as are rows whose output
``video-min-*.json.br`` file already exists in ``--save-dir``.

The legacy ``--csv-file`` flag is preserved as a manual override
for offline work; when set, no Kaggle download happens and the
single file at that path is processed instead. The same format
dispatch applies — ``--csv-file foo.parquet`` works.

Examples:

  # Use the built-in dataset list and default cache:
  python tools/import_kaggle_trending.py \\
      --save-dir /tmp/kaggle-videos

  # Process one local file (legacy mode):
  python tools/import_kaggle_trending.py \\
      --csv-file /path/to/dataset.parquet \\
      --save-dir /tmp/kaggle-videos

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

from dateutil import parser as dateutil_parser

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from scrape_exchange.logging import configure_logging
from scrape_exchange.settings import normalize_log_level
from scrape_exchange.youtube.youtube_video import (
    YouTubeMediaType,
    YouTubeVideo,
)


_LOGGER: logging.Logger = logging.getLogger(__name__)


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
# Different Kaggle uploads use different spellings; we try each
# candidate in order and pick the first non-empty value. New
# datasets may need additional aliases here, but most files use
# variants of the same handful of names.
COLUMN_ALIASES: dict[str, list[str]] = {
    'video_id': [
        'video_id', 'videoId', 'video_ID', 'id', 'youtube_id',
    ],
    'title': ['title', 'video_title', 'name'],
    'description': ['description', 'video_description', 'desc'],
    'channel_id': ['channel_id', 'channelId', 'channel_ID'],
    'channel_handle': [
        'channel_handle', 'channelHandle', 'handle',
        'channel_username', 'channel_user',
    ],
    'channel_subs': [
        'channel_subs', 'channel_subscribers', 'subscriber_count',
        'subscriberCount', 'subscribers', 'subs',
    ],
    'channel_name': [
        'channel_name', 'channel_title', 'channelTitle',
        'channel',
    ],
    'view_count': ['view_count', 'views', 'viewCount'],
    'like_count': ['like_count', 'likes', 'likeCount'],
    'comment_count': [
        'comment_count', 'comments_count', 'comments',
        'commentCount',
    ],
    'publish_date': [
        'publish_date', 'published_at', 'publishedAt',
        'publish_time', 'publishedAtSQL',
    ],
    'tags': ['video_tags', 'tags'],
    'thumbnail_url': [
        'thumbnail_url', 'thumbnail', 'thumbnail_link',
    ],
    'kind': ['kind', 'media_type', 'category'],
    'country': ['country', 'region', 'region_code'],
    'language': [
        'langauge', 'language', 'lang', 'default_audio_language',
    ],
}


MEDIA_TYPE_MAP: dict[str, YouTubeMediaType] = {
    'video': YouTubeMediaType.VIDEO,
    'short': YouTubeMediaType.SHORT,
    'shorts': YouTubeMediaType.SHORT,
    'playlist': YouTubeMediaType.PLAYLIST,
    'channel': YouTubeMediaType.CHANNEL,
    'live': YouTubeMediaType.LIVE,
    'movie': YouTubeMediaType.MOVIE,
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
    third_party_save_dir: str = Field(
        default='/var/tmp/3rdparty-videos',
        validation_alias=AliasChoices(
            'VIDEO_3RD_PARTY_SAVE_DIR', 'video_3rd_party_save_dir'
        ),
        description=(
            'Directory to write video-min-<video_id>.json.br '
            'files into.'
        ),
    )
    channels_file: str | None = Field(
        default='3rd-party-channels.jsonl',
        validation_alias=AliasChoices(
            'THIRD_PARTY_CHANNELS_FILE', 'channels_file',
        ),
        description=(
            'JSONL file to which newly-discovered 3rd-party '
            'channels are appended (one '
            '{"channel_id", "channel", "channel_subs"} object '
            'per line). Default: '
            '<save_dir>/3rdparty_channels.jsonl. Channels are '
            'deduped across all slugs in the same run; rows with '
            'only a channel_handle are accepted and the handle '
            'is used as the dedup key.'
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
    written: int = 0
    skipped: int = 0
    errors: int = 0

    def merge(self, other: 'ImportStats') -> None:
        self.written += other.written
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


def _safe_int(value: object | None) -> int | None:
    if value is None:
        return None
    text: str = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except (ValueError, TypeError):
        return None


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
) -> tuple[str | None, dict | None]:
    '''
    Return ``(dedup_key, channel_record)`` for the channel
    represented by *row*, or ``(None, None)`` when the row
    has neither a ``channel_id`` nor a ``channel_handle``.

    The handle is left-stripped of any leading ``@``. The
    dedup key is the ``channel_id`` when present, otherwise the
    bare handle — channel_ids and handles live in disjoint
    namespaces in practice, so they share one ``seen`` set
    without collision risk.
    '''
    channel_id: str = _pick(row, 'channel_id')
    handle: str = _pick(row, 'channel_handle').lstrip('@')
    if not handle and row.get('channel') and row.get('channel')[0] == '@':
        handle = str(row.get('channel')).lstrip('@').strip()

    if not channel_id and not handle:
        return None, None

    record: dict = {
        'channel_id': channel_id or None,
        'channel': handle or None,
        'channel_subs': _safe_int(_pick(row, 'channel_subs')),
    }
    key: str = channel_id or handle
    return key, record


def _append_channels_jsonl(
    path: Path, records: list[dict],
) -> None:
    '''Append one JSON object per line to *path*. Creates the
    parent directory and the file on first write.'''
    if not records:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a', encoding='utf-8') as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False))
            f.write('\n')


def row_to_video(row: dict) -> YouTubeVideo | None:
    '''
    Convert one parsed row into a populated :class:`YouTubeVideo`,
    or ``None`` when the row has no recognisable video_id.
    '''
    video_id: str = _pick(row, 'video_id')
    if not video_id:
        return None

    handle: str = _pick(row, 'channel_handle').lstrip('@')
    video: YouTubeVideo = YouTubeVideo(
        video_id=video_id,
        channel_handle=handle or None,
    )

    video.title = _pick(row, 'title') or None
    video.description = _pick(row, 'description') or None
    video.channel_id = _pick(row, 'channel_id') or None
    video.view_count = _safe_int(_pick(row, 'view_count'))
    video.like_count = _safe_int(_pick(row, 'like_count'))
    video.comment_count = _safe_int(_pick(row, 'comment_count'))
    video.locale = _pick(row, 'country') or None
    video.default_audio_language = _pick(row, 'language') or None

    video.url = YouTubeVideo.VIDEO_URL.format(
        video_id=video.video_id,
    )
    video.embed_url = YouTubeVideo.EMBED_URL.format(
        video_id=video.video_id,
    )

    raw_tags: str = _pick(row, 'tags')
    if raw_tags and raw_tags != '[None]':
        video.tags = {
            t.strip() for t in raw_tags.split('|') if t.strip()
        }

    kind: str = _pick(row, 'kind').lower()
    if kind:
        video.media_type = MEDIA_TYPE_MAP.get(kind)

    publish_date: str = _pick(row, 'publish_date')
    if publish_date:
        try:
            video.published_timestamp = dateutil_parser.parse(
                publish_date,
            )
        except (ValueError, TypeError):
            pass

    thumbnail: str = _pick(row, 'thumbnail_url')
    if thumbnail:
        video.channel_thumbnail_url = thumbnail

    return video


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


def _existing_artefact(
    third_party_save_dir: Path, video_id: str,
) -> Path | None:
    '''
    Return the existing artefact path for *video_id* in
    *third_party_save_dir*, or ``None`` when no artefact exists
    yet. Recognised artefacts:

    * ``video-min-<video_id>.json.br`` — full InnerTube record
    * ``video-dlp-<video_id>.json.br`` — InnerTube + yt-dlp record
    * ``<video_id>`` (bare, no extension) — sentinel touched by
      :func:`import_file` for rows that did not meet the
      ``video-min`` bar; presence means "already evaluated, do
      not re-process".
    '''
    for candidate in (
        third_party_save_dir / f'video-min-{video_id}.json.br',
        third_party_save_dir / f'video-dlp-{video_id}.json.br',
        third_party_save_dir / video_id,
    ):
        if candidate.exists():
            return candidate
    return None


async def import_file(
    data_path: Path, third_party_save_dir: Path,
    discovered: dict[str, dict], seen: set[str],
) -> ImportStats:
    '''
    Iterate every row of *data_path* and:

    * for each row, decide what to write based on
      :meth:`YouTubeVideo.classify`:

        - ``'video-dlp'`` → write
          ``video-dlp-<video_id>.json.br``
        - ``'video-min'`` → write
          ``video-min-<video_id>.json.br``
        - ``None`` → touch a bare ``<video_id>`` sentinel file in
          *third_party_save_dir* so the row is not re-evaluated
          on the next run.

      Rows whose video_id already has any of the three artefacts
      on disk are skipped without re-classifying.
    * record any newly-seen channel into *discovered* (a
      per-slug dict mutated in place) — keyed by channel_id, or
      handle when channel_id is absent. Channels whose key is
      already in *seen* (the run-wide dedup set) are not added.
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
            record: dict | None
            key, record = _extract_channel(row)
            if (
                key is not None and record is not None
                and key not in seen and key not in discovered
            ):
                discovered[key] = record

            video_id: str = _pick(row, 'video_id')
            if not video_id:
                stats.skipped += 1
                continue
            if _existing_artefact(
                third_party_save_dir, video_id,
            ) is not None:
                stats.skipped += 1
                continue
            try:
                video: YouTubeVideo | None = row_to_video(row)
                if video is None:
                    stats.skipped += 1
                    continue

                classification: str | None = video.classify()
                if classification is None:
                    # Below the video-min bar: touch a sentinel
                    # file (bare video_id, no extension) so the
                    # next run skips this row instead of
                    # re-evaluating it.
                    (third_party_save_dir / video_id).touch()
                    stats.skipped += 1
                    _LOGGER.debug(
                        'Row classified below video-min; '
                        'touched sentinel',
                        extra={
                            'video_id': video_id,
                            'data_file': data_path.name,
                            'row_num': row_num,
                        },
                    )
                    continue

                await video.to_file(
                    str(third_party_save_dir),
                    f'{classification}-',
                )
                stats.written += 1
            except Exception as exc:
                _LOGGER.error(
                    'Failed to process row', exc=exc,
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
    third_party_save_dir: Path,
    channels_file: Path,
    seen_channels: set[str],
) -> None:
    '''
    Walk *local_path* for supported data files, run each through
    :func:`import_file`, and flush newly-discovered channels.
    Updates *report* in place.

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

    discovered: dict[str, dict] = {}
    for data_path in files:
        _LOGGER.info(
            'Processing data file',
            extra={
                'slug': report.slug,
                'data_path': str(data_path),
            },
        )
        stats: ImportStats = await import_file(
            data_path, third_party_save_dir,
            discovered, seen_channels,
        )
        _LOGGER.info(
            'Data file complete',
            extra={
                'slug': report.slug,
                'data_file': data_path.name,
                'written': stats.written,
                'skipped': stats.skipped,
                'errors': stats.errors,
            },
        )
        report.stats.merge(stats)

    if discovered:
        _append_channels_jsonl(
            channels_file, list(discovered.values()),
        )
        seen_channels.update(discovered.keys())
        _LOGGER.info(
            'Appended new channels to channels file',
            extra={
                'slug': report.slug,
                'new_channels': len(discovered),
                'channels_file': str(channels_file),
            },
        )


async def import_dataset(
    slug: str, third_party_save_dir: Path,
    channels_file: Path, seen_channels: set[str],
) -> DatasetReport:
    '''
    Download *slug* via :func:`kagglehub.dataset_download` and
    process every supported data file in the resulting cache
    directory.

    Before calling kagglehub the resolved version is checked
    against the local cache via :func:`_dataset_is_cached`. When
    the dataset is already cached (no new data) the file walk is
    skipped entirely — every row would be no-op'd by the
    ``_existing_artefact`` row-level check anyway, so the walk
    would only burn CPU and risk re-emitting duplicate channel
    records to *channels_file*.  ``report.cached`` records the
    outcome (True / False / None for unknown).

    Newly-discovered channels (deduped against *seen_channels*
    and within this slug) are flushed as JSONL to
    *channels_file* once all files in the slug are processed,
    and their keys are merged into *seen_channels* so they are
    not re-emitted by later slugs in the same run.
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
    await _walk_and_import(
        report, local_path, third_party_save_dir,
        channels_file, seen_channels,
    )
    return report


async def import_hf_dataset(
    repo_id: str, third_party_save_dir: Path,
    channels_file: Path, seen_channels: set[str],
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
    await _walk_and_import(
        report, local_path, third_party_save_dir,
        channels_file, seen_channels,
    )
    return report


def _resolve_channels_file(
    settings: ImportKaggleTrendingSettings,
    third_party_save_dir: Path,
) -> Path:
    '''
    Return the JSONL path for newly-discovered channels —
    either the explicit ``--channels-file`` setting or
    ``<third_party_save_dir>/3rdparty_channels.jsonl``.
    '''

    if settings.channels_file:
        return Path(settings.channels_file).expanduser()
    return third_party_save_dir / '3rdparty_channels.jsonl'


async def _run_manual_file(
    settings: ImportKaggleTrendingSettings,
    third_party_save_dir: Path,
) -> int:
    '''
    Manual ``--csv-file`` mode: import a single local file.
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
    channels_file: Path = _resolve_channels_file(
        settings, third_party_save_dir,
    )
    discovered: dict[str, dict] = {}
    seen_channels: set[str] = set()
    stats: ImportStats = await import_file(
        data_path, third_party_save_dir, discovered, seen_channels,
    )
    if discovered:
        _append_channels_jsonl(
            channels_file, list(discovered.values()),
        )
        _LOGGER.info(
            'Appended new channels to channels file',
            extra={
                'new_channels': len(discovered),
                'channels_file': str(channels_file),
            },
        )
    _LOGGER.info(
        'Manual file mode complete',
        extra={
            'written': stats.written,
            'skipped': stats.skipped,
            'errors': stats.errors,
        },
    )
    return 0 if stats.errors == 0 else 1


async def _import_kaggle_datasets(
    raw_slugs: list[str],
    third_party_save_dir: Path,
    channels_file: Path,
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
            slug, third_party_save_dir,
            channels_file, seen_channels,
        )
        reports.append(report)
        grand.merge(report.stats)


async def _import_hf_datasets(
    raw_repo_ids: list[str],
    third_party_save_dir: Path,
    channels_file: Path,
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
            repo_id, third_party_save_dir,
            channels_file, seen_channels,
        )
        reports.append(report)
        grand.merge(report.stats)


async def _run_3rd_party(
    settings: ImportKaggleTrendingSettings,
    third_party_save_dir: Path,
) -> int:
    '''
    Default mode: download every configured Kaggle and Hugging
    Face dataset and import all supported data files within.
    Both providers share a single ``seen_channels`` set so a
    channel discovered in (e.g.) a Kaggle slug isn't re-flushed
    when it also appears in an HF repo.
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

    channels_file: Path = _resolve_channels_file(
        settings, third_party_save_dir,
    )
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
        kaggle_slugs, third_party_save_dir, channels_file,
        seen_channels, grand, reports,
    )
    await _import_hf_datasets(
        hf_repo_ids, third_party_save_dir, channels_file,
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
                'written': report.stats.written,
                'skipped': report.stats.skipped,
                'errors': report.stats.errors,
            },
        )
    _LOGGER.info(
        'All datasets complete',
        extra={
            'written': grand.written,
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
    third_party_save_dir: Path = Path(settings.third_party_save_dir)
    third_party_save_dir.mkdir(parents=True, exist_ok=True)

    if settings.csv_file:
        return await _run_manual_file(settings, third_party_save_dir)

    return await _run_3rd_party(settings, third_party_save_dir)


if __name__ == '__main__':
    raise SystemExit(asyncio.run(main()))
