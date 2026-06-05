#!/usr/bin/env python3
'''Operator CLI for the video scrape queue.

See docs/superpowers/specs/2026-05-18-video-scrape-queue-design.md
for the vocabulary.
'''

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import os
import re
import sys
from pathlib import Path
from typing import Awaitable, Callable

import httpx
import redis.asyncio as aioredis
from pydantic import AliasChoices, Field
from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
)

from scrape_exchange.exchange_client import ExchangeClient
from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.redis_client import redis_from_url
from scrape_exchange.scrape_api import get_data_by_param
from scrape_exchange.video_scrape_queue import (
    RedisVideoScrapeQueue,
    VideoScrapeQueueSettings,
    VideoState,
)
from scrape_exchange.youtube.uploaded_video_ids import (
    UploadedVideoIds,
)


_VIDEO_ID_RE: re.Pattern[str] = re.compile(
    r'^[A-Za-z0-9_-]{11}$',
)


_Handler = Callable[
    [argparse.Namespace, RedisVideoScrapeQueue],
    Awaitable[int],
]


class YtVideoQueueSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        populate_by_name=True,
        extra='ignore',
    )
    redis_dsn: str = Field(
        default='redis://localhost:6379/0',
    )
    # Mirror the field names used by ScraperSettings / VideoSettings
    # so the CLI reads the same .env entries the scraper does. Used
    # by the four-source 'scraped before' check (see the design at
    # docs/superpowers/specs/2026-06-05-video-queue-force-rescrape-design.md).
    exchange_url: str = Field(
        default='https://scrape.exchange',
        validation_alias=AliasChoices(
            'EXCHANGE_URL', 'exchange_url',
        ),
    )
    api_key_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'API_KEY_ID', 'api_key_id',
        ),
    )
    api_key_secret: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'API_KEY_SECRET', 'api_key_secret',
        ),
    )
    schema_version: str = Field(
        default='0.0.2',
        validation_alias=AliasChoices(
            'SCHEMA_VERSION', 'schema_version',
        ),
    )
    video_data_directory: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'YOUTUBE_VIDEO_DATA_DIR', 'video_data_directory',
        ),
    )


def _add_enqueue_policy_flags(
    parser: argparse.ArgumentParser,
) -> None:
    '''Register the shared skip/force flags on add and import.'''
    parser.add_argument(
        '--force', action='store_true',
        help=(
            'scrape regardless of whether the video was already '
            'scraped before (revives terminal records and tags '
            'them so the scraper bypasses the uploaded-set skip)'
        ),
    )
    parser.add_argument(
        '--no-remote-check', action='store_true',
        help=(
            'skip the scrape.exchange API existence check (the '
            'expensive per-id lookup); use only the uploaded set, '
            'terminal state and local disk to decide'
        ),
    )


def _build_parser() -> argparse.ArgumentParser:
    p: argparse.ArgumentParser = (
        argparse.ArgumentParser(
            prog='yt_video_queue',
        )
    )
    sub = p.add_subparsers(dest='command')
    sub.required = False
    c_count = sub.add_parser('count')
    c_count.add_argument('--state')
    sub.add_parser('stats')
    c_show = sub.add_parser('show')
    c_show.add_argument('video_id')

    c_add = sub.add_parser('add')
    c_add.add_argument('video_ids', nargs='*')
    c_add.add_argument(
        '--file', metavar='FILE',
        help='file with one video_id per line',
    )
    c_add.add_argument('--source', default='cli')
    _add_enqueue_policy_flags(c_add)

    c_remove = sub.add_parser('remove')
    c_remove.add_argument('video_ids', nargs='+')
    c_remove.add_argument('--note', default=None)

    c_rescrape = sub.add_parser('rescrape')
    c_rescrape.add_argument('video_ids', nargs='+')

    c_mark = sub.add_parser('mark')
    c_mark.add_argument('video_id')
    c_mark.add_argument(
        'state',
        help='one of: unavailable, failed, removed',
    )
    c_mark.add_argument('--note', default=None)

    c_unmark = sub.add_parser('unmark')
    c_unmark.add_argument('video_id')

    c_import = sub.add_parser('import')
    c_import.add_argument(
        'directory', nargs='?',
        default='/data/videos',
    )
    c_import.add_argument('--source', default='migration')
    _add_enqueue_policy_flags(c_import)

    c_ingest = sub.add_parser('ingest-sentinels')
    c_ingest.add_argument(
        'directory', nargs='?',
        default='/data/videos',
    )

    c_search = sub.add_parser('search')
    c_search.add_argument('pattern')
    c_search.add_argument(
        '--by',
        choices=['last_error', 'source'],
    )

    return p


async def cmd_count(
    ns: argparse.Namespace,
    queue: RedisVideoScrapeQueue,
) -> int:
    counts: dict[VideoState, int] = (
        await queue.count_by_state()
    )
    if ns.state:
        try:
            state: VideoState = VideoState(ns.state)
        except ValueError:
            sys.stderr.write(
                f'unknown state: {ns.state!r}\n',
            )
            return 2
        sys.stdout.write(
            f'{counts.get(state, 0)}\n',
        )
    else:
        total: int = sum(counts.values())
        sys.stdout.write(f'{total}\n')
    return 0


async def cmd_stats(
    ns: argparse.Namespace,
    queue: RedisVideoScrapeQueue,
) -> int:
    counts: dict[VideoState, int] = (
        await queue.count_by_state()
    )
    sys.stdout.write(
        f'{"state":<15} {"count":>12}\n',
    )
    for s in VideoState:
        n: int = counts.get(s, 0)
        sys.stdout.write(
            f'{s.value:<15} {n:>12}\n',
        )
    return 0


async def cmd_show(
    ns: argparse.Namespace,
    queue: RedisVideoScrapeQueue,
) -> int:
    meta: dict[str, str] = (
        await queue.get_meta(ns.video_id)
    )
    if not meta:
        sys.stderr.write(
            f'no video found for {ns.video_id!r}\n',
        )
        return 1
    for field, val in meta.items():
        sys.stdout.write(f'{field:<22}{val}\n')
    return 0


def _parse_video_ids(text: str) -> list[str]:
    '''Return valid video_ids from newline-delimited text.

    Blank lines and lines starting with '#' are skipped.
    Lines that don't match the video_id pattern are warned
    to stderr and skipped.
    '''
    ids: list[str] = []
    for raw in text.splitlines():
        vid: str = raw.strip()
        if not vid or vid.startswith('#'):
            continue
        if not _VIDEO_ID_RE.match(vid):
            sys.stderr.write(
                f'skipping invalid video_id: {vid!r}\n',
            )
            continue
        ids.append(vid)
    return ids


# Cheap -> expensive order for the four 'scraped before' sources.
_API_CHECK_CONCURRENCY: int = 8


@dataclasses.dataclass
class _ScrapedBeforeSources:
    '''Collaborators for the four-source 'scraped before' check.

    Any of ``file_mgmt`` / ``exchange_client`` may be ``None`` when
    that source is unavailable (fail-open). ``warnings`` lists the
    skipped sources for the operator.
    '''

    uploaded: UploadedVideoIds | None
    file_mgmt: AssetFileManagement | None
    exchange_client: ExchangeClient | None
    warnings: list[str]


@dataclasses.dataclass
class FilterResult:
    survivors: list[str]
    processed: int
    duplicates: int
    already_scraped: dict[str, int]


@dataclasses.dataclass
class EnqueueReport:
    processed: int = 0
    added: int = 0
    duplicates: int = 0
    forced: bool = False
    revived: int = 0
    forced_pending: int = 0
    already_scraped: dict[str, int] = dataclasses.field(
        default_factory=lambda: {
            'uploaded': 0, 'terminal': 0,
            'disk': 0, 'api': 0,
        },
    )


async def _build_sources(
    settings: 'YtVideoQueueSettings', *, remote_check: bool,
) -> _ScrapedBeforeSources:
    '''Construct the 'scraped before' collaborators from settings.

    Missing credentials / ``video_data_directory`` disable that
    source (fail-open) and append a warning rather than failing.
    '''
    warnings: list[str] = []
    uploaded: UploadedVideoIds = UploadedVideoIds(
        settings.redis_dsn,
    )
    file_mgmt: AssetFileManagement | None = None
    if settings.video_data_directory:
        file_mgmt = AssetFileManagement(
            settings.video_data_directory,
        )
    else:
        warnings.append(
            'disk check skipped: video_data_directory not set',
        )
    exchange_client: ExchangeClient | None = None
    if remote_check:
        if settings.api_key_id and settings.api_key_secret:
            try:
                exchange_client = await ExchangeClient.setup(
                    settings.api_key_id,
                    settings.api_key_secret,
                    settings.exchange_url,
                )
            except Exception as exc:
                warnings.append(
                    f'api check skipped: auth failed: {exc}',
                )
                exchange_client = None
            else:
                if not exchange_client.authenticated_username:
                    warnings.append(
                        'api check skipped: no uploader '
                        'username in JWT',
                    )
        else:
            warnings.append(
                'api check skipped: API_KEY_ID/API_KEY_SECRET '
                'not set',
            )
    return _ScrapedBeforeSources(
        uploaded=uploaded,
        file_mgmt=file_mgmt,
        exchange_client=exchange_client,
        warnings=warnings,
    )


async def _source_uploaded(
    video_ids: list[str], uploaded: UploadedVideoIds | None,
) -> tuple[list[str], int]:
    '''Source 1: batched uploaded-set membership.'''
    if uploaded is None or not video_ids:
        return list(video_ids), 0
    try:
        flags: dict[str, bool] = await uploaded.contains_many(
            video_ids,
        )
    except Exception:
        return list(video_ids), 0  # fail-open
    pending: list[str] = []
    hits: int = 0
    for vid in video_ids:
        if flags.get(vid):
            hits += 1
        else:
            pending.append(vid)
    return pending, hits


async def _source_terminal(
    video_ids: list[str], queue: RedisVideoScrapeQueue,
) -> tuple[list[str], int, int]:
    '''Source 2: terminal queue state (queued -> duplicate).'''
    pending: list[str] = []
    terminal_hits: int = 0
    duplicates: int = 0
    for vid in video_ids:
        try:
            state: VideoState | None = await queue.get_state(vid)
        except Exception:
            state = None
        if state is not None and (
            state in VideoState.terminal_states()
        ):
            terminal_hits += 1
        elif state == VideoState.QUEUED:
            duplicates += 1
        else:
            pending.append(vid)
    return pending, terminal_hits, duplicates


def _source_disk(
    video_ids: list[str], file_mgmt: AssetFileManagement | None,
) -> tuple[list[str], int]:
    '''Source 3: local disk (data dir + uploaded dir).'''
    if file_mgmt is None:
        return list(video_ids), 0
    pending: list[str] = []
    hits: int = 0
    for vid in video_ids:
        try:
            exists: bool = file_mgmt.video_scrape_output_exists(vid)
        except Exception:
            exists = False
        if exists:
            hits += 1
        else:
            pending.append(vid)
    return pending, hits


async def _source_api(
    video_ids: list[str],
    *,
    exchange_client: ExchangeClient | None,
    schema_version: str,
    remote_check: bool,
    concurrency: int,
) -> tuple[list[str], int]:
    '''Source 4: scrape.exchange API existence, scoped to our
    uploader. A 200 means we already uploaded it; 404 means not;
    any other error fails open (treated as not scraped).'''
    enabled: bool = (
        remote_check
        and exchange_client is not None
        and bool(exchange_client.authenticated_username)
    )
    if not enabled or not video_ids:
        return list(video_ids), 0
    assert exchange_client is not None
    username: str = exchange_client.authenticated_username or ''
    sem: asyncio.Semaphore = asyncio.Semaphore(concurrency)

    async def _exists(vid: str) -> bool | None:
        async with sem:
            try:
                await get_data_by_param(
                    exchange_client,
                    username=username,
                    platform='youtube',
                    entity='video',
                    version=schema_version,
                    platform_content_id=vid,
                )
                return True
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    return False
                return None
            except Exception:
                return None

    results: list[bool | None] = await asyncio.gather(
        *[_exists(vid) for vid in video_ids],
    )
    pending: list[str] = []
    hits: int = 0
    for vid, found in zip(video_ids, results):
        if found is True:
            hits += 1
        else:
            pending.append(vid)
    return pending, hits


async def _filter_already_scraped(
    video_ids: list[str],
    *,
    uploaded: UploadedVideoIds | None,
    queue: RedisVideoScrapeQueue,
    file_mgmt: AssetFileManagement | None,
    exchange_client: ExchangeClient | None,
    schema_version: str,
    remote_check: bool,
    concurrency: int = _API_CHECK_CONCURRENCY,
) -> FilterResult:
    '''Partition *video_ids* into survivors (to enqueue) and ids
    already scraped before, checking the four sources cheap ->
    expensive and short-circuiting per id. Unavailable sources are
    skipped (fail-open).
    '''
    already: dict[str, int] = {
        'uploaded': 0, 'terminal': 0, 'disk': 0, 'api': 0,
    }
    processed: int = len(video_ids)

    pending: list[str]
    pending, already['uploaded'] = await _source_uploaded(
        video_ids, uploaded,
    )
    pending, already['terminal'], duplicates = (
        await _source_terminal(pending, queue)
    )
    pending, already['disk'] = _source_disk(pending, file_mgmt)
    pending, already['api'] = await _source_api(
        pending,
        exchange_client=exchange_client,
        schema_version=schema_version,
        remote_check=remote_check,
        concurrency=concurrency,
    )

    return FilterResult(
        survivors=pending,
        processed=processed,
        duplicates=duplicates,
        already_scraped=already,
    )


async def _run_enqueue_policy(
    video_ids: list[str],
    *,
    ns: argparse.Namespace,
    queue: RedisVideoScrapeQueue,
    settings: 'YtVideoQueueSettings',
) -> EnqueueReport:
    '''Shared add/import logic: --force re-scrapes regardless;
    otherwise filter out already-scraped ids before enqueuing.'''
    force: bool = bool(getattr(ns, 'force', False))
    remote_check: bool = not bool(
        getattr(ns, 'no_remote_check', False),
    )
    source: str = getattr(ns, 'source', 'cli')

    if force:
        outcomes: dict[str, int] = {
            'added': 0, 'revived': 0, 'forced_pending': 0,
        }
        for vid in video_ids:
            result: str = await queue.force_enqueue(
                vid, source=source,
            )
            if result in outcomes:
                outcomes[result] += 1
        return EnqueueReport(
            processed=len(video_ids),
            forced=True,
            added=outcomes['added'],
            revived=outcomes['revived'],
            forced_pending=outcomes['forced_pending'],
        )

    sources: _ScrapedBeforeSources = await _build_sources(
        settings, remote_check=remote_check,
    )
    try:
        for warning in sources.warnings:
            sys.stderr.write(f'warning: {warning}\n')
        filtered: FilterResult = await _filter_already_scraped(
            video_ids,
            uploaded=sources.uploaded,
            queue=queue,
            file_mgmt=sources.file_mgmt,
            exchange_client=sources.exchange_client,
            schema_version=settings.schema_version,
            remote_check=remote_check,
        )
        added: int = 0
        duplicates: int = filtered.duplicates
        for vid in filtered.survivors:
            if await queue.enqueue(vid, source=source):
                added += 1
            else:
                duplicates += 1
        return EnqueueReport(
            processed=filtered.processed,
            added=added,
            duplicates=duplicates,
            already_scraped=filtered.already_scraped,
        )
    finally:
        if sources.exchange_client is not None:
            try:
                await sources.exchange_client.aclose()
            except Exception:
                pass


def _write_enqueue_status(
    label: str, report: EnqueueReport,
) -> None:
    if report.forced:
        sys.stdout.write(
            f'{label}: processed={report.processed} '
            f'added={report.added} revived={report.revived} '
            f'forced_pending={report.forced_pending} (force)\n',
        )
        return
    a: dict[str, int] = report.already_scraped
    total: int = a['uploaded'] + a['terminal'] + a['disk'] + a['api']
    sys.stdout.write(
        f'{label}: processed={report.processed} '
        f'already_scraped={total} '
        f'(uploaded={a["uploaded"]} terminal={a["terminal"]} '
        f'disk={a["disk"]} api={a["api"]}) '
        f'duplicates={report.duplicates} added={report.added}\n',
    )


async def cmd_add(
    ns: argparse.Namespace,
    queue: RedisVideoScrapeQueue,
) -> int:
    video_ids: list[str] = list(ns.video_ids)
    if getattr(ns, 'file', None):
        text: str = await asyncio.to_thread(
            Path(ns.file).read_text,
        )
        video_ids.extend(_parse_video_ids(text))
    if not sys.stdin.isatty():
        text = await asyncio.to_thread(sys.stdin.read)
        video_ids.extend(_parse_video_ids(text))
    if not video_ids:
        sys.stderr.write('no video_ids provided\n')
        return 2
    report: EnqueueReport = await _run_enqueue_policy(
        video_ids, ns=ns, queue=queue,
        settings=YtVideoQueueSettings(),
    )
    _write_enqueue_status('add', report)
    return 0


async def cmd_remove(
    ns: argparse.Namespace,
    queue: RedisVideoScrapeQueue,
) -> int:
    for vid in ns.video_ids:
        await queue.mark(
            vid, state=VideoState.REMOVED,
            note=ns.note,
        )
    return 0


async def cmd_rescrape(
    ns: argparse.Namespace,
    queue: RedisVideoScrapeQueue,
) -> int:
    for vid in ns.video_ids:
        state: VideoState | None = (
            await queue.get_state(vid)
        )
        if state is None:
            sys.stderr.write(
                f'no video found for {vid!r}\n',
            )
            continue
        if state in VideoState.terminal_states():
            await queue.unmark(vid)
    return 0


async def cmd_mark(
    ns: argparse.Namespace,
    queue: RedisVideoScrapeQueue,
) -> int:
    try:
        target: VideoState = VideoState(ns.state)
    except ValueError:
        sys.stderr.write(
            f'unknown state: {ns.state!r}\n',
        )
        return 2
    await queue.mark(
        ns.video_id, state=target, note=ns.note,
    )
    return 0


async def cmd_unmark(
    ns: argparse.Namespace,
    queue: RedisVideoScrapeQueue,
) -> int:
    await queue.unmark(ns.video_id)
    return 0


async def cmd_ingest_sentinels(
    ns: argparse.Namespace,
    queue: RedisVideoScrapeQueue,
) -> int:
    # Legacy filename pattern:
    # video-(min|dlp|yt)-<video_id>.json.br.<suffix>
    prefix_re: re.Pattern[str] = re.compile(
        r'^video-(?:min|dlp|yt)-'
        r'(?P<vid>[A-Za-z0-9_-]{11})'
        r'\.json\.br\.'
        r'(?P<suffix>invalid|not_found|unresolved)$',
    )
    moved: int = 0
    for entry in os.listdir(ns.directory):
        path: str = os.path.join(
            ns.directory, entry,
        )
        if not os.path.isfile(path):
            continue
        m: re.Match[str] | None = prefix_re.match(
            entry,
        )
        if m is None:
            continue
        video_id: str = m.group('vid')
        await queue.mark(
            video_id,
            state=VideoState.FAILED,
            last_error=(
                f'migrated from {m.group("suffix")}'
            ),
            note='migrated from filesystem',
        )
        os.unlink(path)
        moved += 1
    sys.stdout.write(f'ingested: {moved}\n')
    return 0


async def cmd_search(
    ns: argparse.Namespace,
    queue: RedisVideoScrapeQueue,
) -> int:
    fields: tuple[str, ...] = (
        (ns.by,) if ns.by
        else ('last_error', 'source')
    )
    matches: list[str] = await queue.search_meta(
        ns.pattern, fields=fields,
    )
    for vid in matches:
        sys.stdout.write(f'{vid}\n')
    return 0


async def cmd_import(
    ns: argparse.Namespace,
    queue: RedisVideoScrapeQueue,
) -> int:
    # Collect valid sentinels first so the skip/force policy can
    # run as a batch (the API source short-circuits cheaper checks).
    video_ids: list[str] = []
    paths: list[str] = []
    for entry in os.listdir(ns.directory):
        path: str = os.path.join(ns.directory, entry)
        if not os.path.isfile(path):
            continue
        if not _VIDEO_ID_RE.match(entry):
            continue
        video_ids.append(entry)
        paths.append(path)
    if not video_ids:
        _write_enqueue_status('import', EnqueueReport(processed=0))
        return 0
    report: EnqueueReport = await _run_enqueue_policy(
        video_ids, ns=ns, queue=queue,
        settings=YtVideoQueueSettings(),
    )
    # Every valid id is dispositioned (added / duplicate /
    # already_scraped, or force-enqueued), so consume all their
    # sentinels — leaving them would re-report the same files on
    # every run. Invalid filenames were never collected here.
    for path in paths:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
    _write_enqueue_status('import', report)
    return 0


_HANDLERS: dict[str, _Handler] = {}

_HANDLERS['count'] = cmd_count
_HANDLERS['stats'] = cmd_stats
_HANDLERS['show'] = cmd_show
_HANDLERS['add'] = cmd_add
_HANDLERS['remove'] = cmd_remove
_HANDLERS['rescrape'] = cmd_rescrape
_HANDLERS['mark'] = cmd_mark
_HANDLERS['unmark'] = cmd_unmark
_HANDLERS['import'] = cmd_import
_HANDLERS['ingest-sentinels'] = cmd_ingest_sentinels
_HANDLERS['search'] = cmd_search


async def main_async(argv: list[str]) -> int:
    parser: argparse.ArgumentParser = _build_parser()
    ns: argparse.Namespace = parser.parse_args(argv)
    if not ns.command or ns.command not in _HANDLERS:
        sys.stderr.write(
            f'unknown or missing command: '
            f'{ns.command!r}\n'
        )
        raise SystemExit(2)
    settings: YtVideoQueueSettings = (
        YtVideoQueueSettings()
    )
    redis: aioredis.Redis = redis_from_url(
        settings.redis_dsn,
        component='yt-video-queue-cli',
        decode_responses=True,
        max_connections=1,
    )
    queue: RedisVideoScrapeQueue = (
        RedisVideoScrapeQueue(
            redis, VideoScrapeQueueSettings(),
        )
    )
    try:
        return await _HANDLERS[ns.command](
            ns, queue,
        )
    finally:
        await redis.aclose()


def main() -> None:
    raise SystemExit(
        asyncio.run(main_async(sys.argv[1:])),
    )


if __name__ == '__main__':
    main()
