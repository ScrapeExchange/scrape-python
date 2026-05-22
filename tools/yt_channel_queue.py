#!/usr/bin/env python3
'''Operator CLI for the channel scrape queue.

Backs every subcommand with the ChannelScrapeQueue
interface. See ADR 0001 and
docs/superpowers/specs/2026-05-18-channel-scrape-queue-design.md
for the full vocabulary.
'''

from __future__ import annotations

import asyncio
import json
import os
import sys
from typing import Awaitable, Callable
from argparse import (
    ArgumentParser, Namespace,
    _SubParsersAction, _MutuallyExclusiveGroup
)

import redis.asyncio as aioredis
from pydantic import Field
from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
)

from scrape_exchange.channel_scrape_queue import (
    ChannelScrapeQueueSettings,
    ChannelState,
    RedisChannelScrapeQueue,
)
from scrape_exchange.youtube.channel_identity import (
    ChannelIdentityStore,
    is_valid_channel_handle,
)

from scrape_exchange.youtube.youtube_channel import YouTubeChannel


_Handler = Callable[
    [Namespace, RedisChannelScrapeQueue],
    Awaitable[int],
]


class YtChannelQueueSettings(BaseSettings):
    # Standalone settings — the CLI tool is
    # importable on its own and does not pull in
    # scraper-level config (proxies, etc.).
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore',
    )

    redis_dsn: str = Field(
        default='redis://localhost:6379/0',
    )


def _normalise_key(key: str) -> str:
    '''Convert operator input into a prefixed
    queue member. Channel ids match
    ``^UC[A-Za-z0-9_-]{22}$``; anything else is
    treated as a handle and gets ``h:`` plus a
    canonical strip.'''
    stripped: str = (
        key.strip().removeprefix('@').strip()
    )
    if YouTubeChannel.CHANNEL_ID_REGEX_MATCH.fullmatch(stripped):
        return f'i:{stripped}'
    return f'h:{stripped}'


def _build_parser() -> ArgumentParser:
    p: ArgumentParser = ArgumentParser(prog='yt_channel_queue')

    sub: _SubParsersAction[ArgumentParser] = \
        p.add_subparsers(dest='command')
    sub.required = False
    c_count = sub.add_parser('count')
    c_count.add_argument('--tier', type=int)
    c_count.add_argument('--state')
    sub.add_parser('stats')
    c_show: ArgumentParser = sub.add_parser('show')
    c_show.add_argument('key')
    c_search = sub.add_parser('search')
    c_search.add_argument('pattern')
    c_search.add_argument(
        '--by',
        choices=['handle', 'channel_id', 'name'],
    )
    c_add: ArgumentParser = sub.add_parser(
        'add',
        help=(
            'Add one or more channels. KEY can be a UC… '
            'channel_id, a handle (with or without leading '
            '@), or "-" to read newline-separated entries '
            'from stdin. With no KEY arguments, stdin is '
            'read. Stdin lines may also be JSON with '
            '"channel_id" and/or "channel_handle" fields.'
        ),
    )
    c_add.add_argument('keys', nargs='*')
    c_add.add_argument(
        '--priority', action='store_true',
    )
    c_add.add_argument('--source', default='cli')
    c_remove: ArgumentParser = sub.add_parser('remove')
    c_remove.add_argument('keys', nargs='+')
    c_remove.add_argument('--note', default=None)
    c_rescrape = sub.add_parser('rescrape')
    c_rescrape.add_argument('keys', nargs='+')
    c_mark: ArgumentParser = sub.add_parser('mark')
    c_mark.add_argument('key')
    c_mark.add_argument(
        'state',
        help=(
            'one of: not_found, invalid_handle, '
            'inconsistent_identity, terminated, '
            'unresolved, removed, soft_unavailable'
        ),
    )
    c_mark.add_argument('--note', default=None)
    c_mark.add_argument(
        '--hard',
        action='store_true',
        help='for soft_unavailable, jump to hard',
    )
    c_unmark: ArgumentParser = sub.add_parser('unmark')
    c_unmark.add_argument('key')
    c_unmark.add_argument('--to', default=None)
    c_import = sub.add_parser('import')
    c_import.add_argument('file')
    mx: _MutuallyExclusiveGroup = c_import.add_mutually_exclusive_group()
    mx.add_argument(
        '--replace', action='store_true',
    )
    mx.add_argument(
        '--merge', action='store_true',
    )
    c_ingest: ArgumentParser = sub.add_parser('ingest-sentinels')
    c_ingest.add_argument('directory')
    return p


# Handler registry. Subcommands register themselves
# here via module-level mutation.
_HANDLERS: dict[str, _Handler] = {}


async def cmd_count(
    ns: Namespace,
    queue: RedisChannelScrapeQueue,
) -> int:
    counts: dict[ChannelState, int] = (
        await queue.count_by_state()
    )
    if ns.state:
        try:
            state: ChannelState = (
                ChannelState(ns.state)
            )
        except ValueError:
            sys.stderr.write(
                f'unknown state: {ns.state!r}\n'
            )
            return 2
        sys.stdout.write(
            f'{counts.get(state, 0)}\n',
        )
    elif ns.tier is not None:
        per_tier: dict[int, int] = (
            await queue.count_by_tier()
        )
        sys.stdout.write(
            f'{per_tier.get(ns.tier, 0)}\n',
        )
    else:
        total: int = sum(counts.values())
        sys.stdout.write(f'{total}\n')
    return 0


async def cmd_stats(
    ns: Namespace,
    queue: RedisChannelScrapeQueue,
) -> int:
    counts: dict[ChannelState, int] = (
        await queue.count_by_state()
    )
    sys.stdout.write(
        f'{"state":<25} {"count":>12}\n',
    )
    for s in ChannelState:
        n: int = counts.get(s, 0)
        sys.stdout.write(
            f'{s.value:<25} {n:>12}\n',
        )
    per_tier: dict[int, int] = (
        await queue.count_by_tier()
    )
    sys.stdout.write('\nscheduled tiers:\n')
    for tier, n in sorted(per_tier.items()):
        sys.stdout.write(
            f'  tier {tier}: {n}\n',
        )
    return 0


async def cmd_show(
    ns: Namespace,
    queue: RedisChannelScrapeQueue,
) -> int:
    member: str = _normalise_key(ns.key)
    meta: dict[str, str] = (
        await queue.get_meta(member)
    )
    if not meta:
        sys.stderr.write(
            f'no channel found for {ns.key!r}\n',
        )
        return 1
    for field, val in meta.items():
        sys.stdout.write(f'{field:<22}{val}\n')
    return 0


async def cmd_search(
    ns: Namespace,
    queue: RedisChannelScrapeQueue,
) -> int:
    fields: tuple[str, ...] = (
        (ns.by,) if ns.by else (
            'handle', 'channel_id', 'name',
        )
    )
    matches: list[str] = await queue.search_meta(
        ns.pattern, fields=fields,
    )
    for member in matches:
        meta: dict[str, str] = (
            await queue.get_meta(member)
        )
        handle: str = meta.get('handle', '')
        cid: str = meta.get('channel_id', '')
        state: str = meta.get('state', '')
        sys.stdout.write(
            f'{handle:<24} {cid:<24} {state}\n',
        )
    return 0


def _parse_add_entry(
    raw: str,
) -> tuple[str | None, str | None] | None:
    '''Parse one ``add`` input line. Returns
    ``(channel_id, channel_handle)`` (either may be ``None``)
    or ``None`` if the line should be skipped (blank, comment,
    unparseable JSON, or JSON with no identifying fields).

    Recognised formats:

    * Blank line / line beginning with ``#`` — skipped silently.
    * JSON object (starts with ``{``) — read ``channel_id``
      and ``channel_handle`` fields; warn and skip if neither
      is present.
    * Anything else — treated as a single bare token. UC ids
      matching :data:`YouTubeChannel.CHANNEL_ID_REGEX_MATCH` return as
      ``channel_id``; anything else returns as
      ``channel_handle`` (after stripping any leading ``@``).
    '''
    stripped: str = raw.strip()
    if not stripped or stripped.startswith('#'):
        return None
    if stripped.startswith('{'):
        try:
            obj = json.loads(stripped)
        except json.JSONDecodeError:
            sys.stderr.write(
                f'warning: skipping unparseable JSON line: '
                f'{stripped!r}\n',
            )
            return None
        if not isinstance(obj, dict):
            sys.stderr.write(
                f'warning: JSON line is not an object: '
                f'{stripped!r}\n',
            )
            return None
        cid_raw = obj.get('channel_id')
        handle_raw = obj.get('channel_handle')
        cid: str | None = (
            str(cid_raw).strip() if cid_raw else ''
        ) or None
        handle: str | None = (
            str(handle_raw).strip().removeprefix('@').strip()
            if handle_raw else ''
        ) or None
        if cid is None and handle is None:
            sys.stderr.write(
                f'warning: JSON line has neither channel_id '
                f'nor channel_handle; skipping: '
                f'{stripped!r}\n',
            )
            return None
        if (
            handle is not None
            and not is_valid_channel_handle(handle)
        ):
            sys.stderr.write(
                f'warning: channel_handle {handle!r} fails '
                f'YouTubeChannel.CHANNEL_HANDLE_REGEX; '
                f'skipping: {stripped!r}\n',
            )
            return None
        return cid, handle
    token: str = stripped.removeprefix('@').strip()
    if not token:
        return None
    if YouTubeChannel.CHANNEL_ID_REGEX_MATCH.fullmatch(token):
        return token, None
    if not is_valid_channel_handle(token):
        sys.stderr.write(
            f'warning: channel_handle {token!r} fails '
            f'YouTubeChannel.CHANNEL_HANDLE_REGEX; '
            f'skipping: {stripped!r}\n',
        )
        return None
    return None, token


def _iter_add_inputs(keys: list[str]) -> "list[str]":
    '''Expand the positional ``keys`` argument into a flat
    list of input lines. ``-`` is replaced inline with the
    contents of stdin (one entry per line); an empty
    ``keys`` list reads stdin entirely.
    '''
    if not keys:
        return list(sys.stdin)
    out: list[str] = []
    for k in keys:
        if k == '-':
            out.extend(sys.stdin)
        else:
            out.append(k)
    return out


async def _enqueue_resolved_channel(
    queue: RedisChannelScrapeQueue,
    *,
    channel_id: str | None,
    channel_handle: str | None,
    source: str,
    priority: bool,
    identity: ChannelIdentityStore | None,
) -> None:
    '''Route one ``(channel_id, channel_handle)`` pair to the
    appropriate queue method. A well-formed ``channel_id``
    always wins; otherwise the handle is resolved via the
    identity store (when supplied) before falling back to
    ``enqueue_unresolved``.
    '''
    if channel_id and YouTubeChannel.CHANNEL_ID_REGEX_MATCH.fullmatch(
        channel_id,
    ):
        await queue.enqueue_scheduled(
            channel_id, source=source, priority=priority,
        )
        return
    if not channel_handle:
        return
    cid: str | None = None
    if identity is not None:
        cid = await identity.lookup_id_for_handle(
            channel_handle,
        )
    if cid:
        await queue.enqueue_scheduled(
            cid, source=source, priority=priority,
        )
    else:
        await queue.enqueue_unresolved(
            channel_handle,
            source=source, priority=priority,
        )


async def cmd_add(
    ns: Namespace,
    queue: RedisChannelScrapeQueue,
    *,
    identity: ChannelIdentityStore | None = None,
) -> int:
    for raw in _iter_add_inputs(list(ns.keys or [])):
        parsed: tuple[str | None, str | None] | None = (
            _parse_add_entry(raw)
        )
        if parsed is None:
            continue
        channel_id, channel_handle = parsed
        await _enqueue_resolved_channel(
            queue,
            channel_id=channel_id,
            channel_handle=channel_handle,
            source=ns.source,
            priority=ns.priority,
            identity=identity,
        )
    return 0


async def cmd_remove(
    ns: Namespace,
    queue: RedisChannelScrapeQueue,
) -> int:
    for raw in ns.keys:
        await queue.mark(
            _normalise_key(raw),
            state=ChannelState.REMOVED,
            note=ns.note,
        )
    return 0


async def cmd_rescrape(
    ns: Namespace,
    queue: RedisChannelScrapeQueue,
) -> int:
    for raw in ns.keys:
        member: str = _normalise_key(raw)
        state: ChannelState | None = (
            await queue.get_state(member)
        )
        if state is None:
            sys.stderr.write(
                f'no channel found for {raw!r}\n',
            )
            continue
        if state in ChannelState.terminal_states():
            await queue.unmark(member)
        elif (
            state == ChannelState.SCHEDULED
            and member.startswith('i:')
        ):
            # Pull score to 0 via a large negative
            # backoff. The XX guard inside
            # requeue_with_backoff means this is a
            # no-op if the member isn't actually in
            # the ZSET.
            await queue.requeue_with_backoff(
                member[2:],
                seconds=-1_000_000_000,
                now=0.0,
            )
        elif (
            state == ChannelState.PENDING_RESOLUTION
            and member.startswith('h:')
        ):
            await queue.requeue_with_backoff(
                member[2:],
                seconds=-1_000_000_000,
                now=0.0,
                unresolved=True,
            )
    return 0


async def cmd_mark(
    ns: Namespace,
    queue: RedisChannelScrapeQueue,
) -> int:
    try:
        target: ChannelState = ChannelState(ns.state)
    except ValueError:
        sys.stderr.write(
            f'unknown state: {ns.state!r}\n',
        )
        return 2
    if (
        ns.hard
        and target == ChannelState.SOFT_UNAVAILABLE
    ):
        target = ChannelState.HARD_UNAVAILABLE
    member: str = _normalise_key(ns.key)
    await queue.mark(
        member, state=target, note=ns.note,
    )
    return 0


async def cmd_unmark(
    ns: Namespace,
    queue: RedisChannelScrapeQueue,
) -> int:
    member: str = _normalise_key(ns.key)
    await queue.unmark(member)
    return 0


def _read_lines(path: str) -> list[str]:
    '''Read all lines from *path* synchronously.'''
    with open(path, 'r') as fh:
        return fh.readlines()


def _parse_import_line(
    line: str,
) -> tuple[str, str] | None:
    '''Parse one channels.lst JSON line.

    Returns ``(cid, handle)`` where at most one of
    the two is non-empty, or ``None`` to skip.
    '''
    line = line.strip()
    if not line:
        return None
    try:
        rec: dict[str, str | None] = json.loads(line)
    except ValueError:
        return None
    cid_raw: str | None = rec.get('channel_id')
    handle_raw: str | None = rec.get('channel_handle')
    cid: str = (
        str(cid_raw).strip() if cid_raw else ''
    )
    handle: str = (
        str(handle_raw).lstrip('@').strip()
        if handle_raw else ''
    )
    return (cid, handle)


async def _enqueue_import_record(
    cid: str,
    handle: str,
    queue: RedisChannelScrapeQueue,
    identity: ChannelIdentityStore | None,
) -> tuple[int, int]:
    '''Enqueue one record; return (resolved, unresolved)
    increment tuple.

    A ``cid`` is only honoured when it matches the
    modern ``UC[A-Za-z0-9_-]{22}`` format. If the
    ``channel_id`` field holds a string that does not
    match (likely a legacy ``/user/<name>`` username
    that got written to the wrong field by an older
    importer), promote it to the handle slot when
    no handle is otherwise set. The scraper's resolve
    phase will then attempt to look it up via the
    InnerTube path and either bind a real channel_id
    or mark the entry as ``not_found``.
    '''
    if cid and not YouTubeChannel.CHANNEL_ID_REGEX_MATCH.fullmatch(cid):
        if not handle:
            handle = cid
        cid = ''
    if cid:
        await queue.enqueue_scheduled(
            cid, source='migration',
        )
        return (1, 0)
    if not handle:
        return (0, 0)
    hit: str | None = None
    if identity is not None:
        hit = await identity.lookup_id_for_handle(
            handle,
        )
    if hit:
        await queue.enqueue_scheduled(
            hit, source='migration',
        )
        return (1, 0)
    await queue.enqueue_unresolved(
        handle, source='migration',
    )
    return (0, 1)


async def _clear_channel_keys(
    queue: RedisChannelScrapeQueue,
) -> None:
    sys.stderr.write(
        'replace mode: clearing '
        'youtube:channel:* keys first\n',
    )
    async for key in queue._redis.scan_iter(
        match='youtube:channel:*',
    ):
        await queue._redis.delete(key)


async def cmd_import(
    ns: Namespace,
    queue: RedisChannelScrapeQueue,
    *,
    identity: ChannelIdentityStore | None = None,
) -> int:
    if ns.replace:
        await _clear_channel_keys(queue)
    added_resolved: int = 0
    added_unresolved: int = 0
    for raw_line in _read_lines(ns.file):
        parsed = _parse_import_line(raw_line)
        if parsed is None:
            continue
        cid, handle = parsed
        r, u = await _enqueue_import_record(
            cid, handle, queue, identity,
        )
        added_resolved += r
        added_unresolved += u
    sys.stdout.write(
        f'imported: {added_resolved} resolved, '
        f'{added_unresolved} unresolved\n',
    )
    return 0


async def cmd_ingest_sentinels(
    ns: Namespace,
    queue: RedisChannelScrapeQueue,
) -> int:
    prefix: str = 'channel-'
    suffix_state: list[tuple[str, ChannelState]] = [
        (
            '.json.br.not_found',
            ChannelState.NOT_FOUND,
        ),
        (
            '.json.br.unresolved',
            ChannelState.UNRESOLVED,
        ),
    ]
    moved: dict[ChannelState, int] = {}
    for entry in os.listdir(ns.directory):
        path: str = os.path.join(
            ns.directory, entry,
        )
        if not os.path.isfile(path):
            continue
        for suffix, state in suffix_state:
            if not entry.endswith(suffix):
                continue
            key: str = entry[
                len(prefix):-len(suffix)
            ]
            member: str = (
                f'i:{key}'
                if YouTubeChannel.CHANNEL_ID_REGEX_MATCH.fullmatch(key)
                else f'h:{key}'
            )
            await queue.mark(
                member,
                state=state,
                note='migrated from filesystem',
            )
            os.unlink(path)
            moved[state] = moved.get(state, 0) + 1
            break
    for state, n in moved.items():
        sys.stdout.write(
            f'{state.value}: {n}\n',
        )
    return 0


_HANDLERS['count'] = cmd_count
_HANDLERS['stats'] = cmd_stats
_HANDLERS['show'] = cmd_show
_HANDLERS['search'] = cmd_search
_HANDLERS['add'] = cmd_add
_HANDLERS['remove'] = cmd_remove
_HANDLERS['rescrape'] = cmd_rescrape
_HANDLERS['mark'] = cmd_mark
_HANDLERS['unmark'] = cmd_unmark
_HANDLERS['import'] = cmd_import
_HANDLERS['ingest-sentinels'] = cmd_ingest_sentinels


async def main_async(argv: list[str]) -> int:
    parser: ArgumentParser = _build_parser()
    ns: Namespace = parser.parse_args(argv)
    if not ns.command or ns.command not in _HANDLERS:
        sys.stderr.write(
            f'unknown or missing command: '
            f'{ns.command!r}\n'
        )
        raise SystemExit(2)
    settings: YtChannelQueueSettings = (
        YtChannelQueueSettings()
    )
    redis: aioredis.Redis = aioredis.from_url(
        settings.redis_dsn,
        decode_responses=True,
    )
    queue: RedisChannelScrapeQueue = (
        RedisChannelScrapeQueue(
            redis, ChannelScrapeQueueSettings(),
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
