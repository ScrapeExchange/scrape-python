#!/usr/bin/env python3
'''Operator CLI for the video scrape queue.

See docs/superpowers/specs/2026-05-18-video-scrape-queue-design.md
for the vocabulary.
'''

from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
from pathlib import Path
from typing import Awaitable, Callable

import redis.asyncio as aioredis
from pydantic import Field
from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
)

from scrape_exchange.redis_client import redis_from_url
from scrape_exchange.video_scrape_queue import (
    RedisVideoScrapeQueue,
    VideoScrapeQueueSettings,
    VideoState,
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
        extra='ignore',
    )
    redis_dsn: str = Field(
        default='redis://localhost:6379/0',
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


async def _enqueue_video_ids(
    queue: RedisVideoScrapeQueue,
    video_ids: list[str],
    *,
    source: str,
) -> tuple[int, int, int]:
    processed: int = 0
    added: int = 0
    duplicates: int = 0
    for vid in video_ids:
        processed += 1
        if await queue.enqueue(vid, source=source):
            added += 1
        else:
            duplicates += 1
    return processed, duplicates, added


def _write_enqueue_status(
    *,
    label: str,
    processed: int,
    duplicates: int,
    added: int,
) -> None:
    sys.stdout.write(
        f'{label}: processed={processed} '
        f'duplicates={duplicates} added={added}\n',
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
    processed: int
    duplicates: int
    added: int
    processed, duplicates, added = await _enqueue_video_ids(
        queue, video_ids, source=ns.source,
    )
    _write_enqueue_status(
        label='add',
        processed=processed,
        duplicates=duplicates,
        added=added,
    )
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
    processed: int = 0
    duplicates: int = 0
    added: int = 0
    for entry in os.listdir(ns.directory):
        path: str = os.path.join(
            ns.directory, entry,
        )
        if not os.path.isfile(path):
            continue
        if not _VIDEO_ID_RE.match(entry):
            continue
        processed += 1
        if await queue.enqueue(entry, source='migration'):
            added += 1
        else:
            duplicates += 1
        os.unlink(path)
    _write_enqueue_status(
        label='import',
        processed=processed,
        duplicates=duplicates,
        added=added,
    )
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
