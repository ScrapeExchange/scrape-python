#!/usr/bin/env python3
'''Operator CLI for the channel scrape queue.

Backs every subcommand with the ChannelScrapeQueue
interface. See ADR 0001 and
docs/superpowers/specs/2026-05-18-channel-scrape-queue-design.md
for the full vocabulary.
'''

import asyncio
import json
import json as json_mod
import os
import sys
import time
from datetime import datetime, timezone
from typing import Awaitable, Callable
from argparse import (
    ArgumentParser, Namespace,
    _SubParsersAction, _MutuallyExclusiveGroup
)

import redis.asyncio as aioredis
from redis.exceptions import ConnectionError as RedisConnectionError
from pydantic import AliasChoices, Field
from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
)

from scrape_exchange.channel_queue_reconcile import (
    ChannelQueueAuditor,
    ChannelQueueRepairer,
    DriftClassReport,
    REPAIR_MODES,
    RepairOptions,
    ReconcileReport,
    publish_reconcile_metrics,
    record_reconcile_error,
)
from scrape_exchange.channel_scrape_queue import (
    ChannelScrapeQueueSettings,
    ChannelState,
    RedisChannelScrapeQueue,
)
from scrape_exchange.creator_queue import parse_priority_queues
from scrape_exchange.metrics_server import start_metrics_server
from scrape_exchange.redis_client import redis_from_url
from scrape_exchange.youtube.channel_identity import (
    ChannelIdentityStore,
    is_valid_channel_handle,
)

from scrape_exchange.youtube.youtube_channel import (
    YouTubeChannel,
)


_Handler = Callable[
    [Namespace, RedisChannelScrapeQueue],
    Awaitable[int],
]


_LUA_BACKFILL_RSS_CREATORS = '''
-- KEYS[1] = youtube:channel:tiers
-- KEYS[2..S+1] = youtube:channel:queue:scheduled:<tier>
-- KEYS[S+2..S+T+1] = terminal-state hashes
-- ARGV[1] = scheduled key count (S)
-- ARGV[2] = terminal hash count (T)
-- ARGV[3] = youtube:channel:meta: prefix
-- ARGV[4] = source
-- ARGV[5] = created_at unix seconds
-- ARGV[6..N] = channel IDs
local added = 0
local skipped = 0
local scheduled_count = tonumber(ARGV[1])
local terminal_count = tonumber(ARGV[2])
local scheduled_start = 2
local terminal_start = scheduled_start + scheduled_count
for i = 6, #ARGV do
    local cid = ARGV[i]
    local member = 'i:' .. cid
    local exists = false
    for k = scheduled_start, terminal_start - 1 do
        if redis.call('ZSCORE', KEYS[k], member) then
            exists = true
            break
        end
    end
    if not exists then
        for k = terminal_start, terminal_start + terminal_count - 1 do
            if redis.call('HEXISTS', KEYS[k], member) == 1 then
                exists = true
                break
            end
        end
    end
    if exists then
        skipped = skipped + 1
    else
        local tier = tonumber(redis.call('HGET', KEYS[1], cid) or '0')
        if tier == nil or tier < 0 or tier >= scheduled_count then
            tier = 0
        end
        local scheduled_key = KEYS[scheduled_start + tier]
        local zadded = redis.call(
            'ZADD', scheduled_key, 'NX', 0, member
        )
        if zadded == 1 then
            redis.call('HSETNX', KEYS[1], cid, '0')
            local meta_key = ARGV[3] .. member
            redis.call(
                'HSET', meta_key,
                'channel_id', cid,
                'source', ARGV[4],
                'created_at', ARGV[5],
                'state', 'scheduled'
            )
            added = added + 1
        else
            skipped = skipped + 1
        end
    end
end
return {added, skipped}
'''


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
    rss_priority_queues: str = Field(
        default='1:10000000,4:1000000,12:100000,24:10000,48:0',
        validation_alias=AliasChoices(
            'RSS_PRIORITY_QUEUES',
            'rss_priority_queues',
        ),
        description=(
            'RSS queue tier spec used only to discover queue '
            'keys and the second-to-last default RSS repair tier.'
        ),
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
    c_rescrape.add_argument('keys', nargs='*')
    c_rescrape.add_argument(
        '--mode',
        choices=['default', 'full', 'metadata'],
        default='default',
        help=(
            'default: make due now and let the scraper choose; '
            'full: force video_ids; metadata: force without video_ids'
        ),
    )
    c_mark: ArgumentParser = sub.add_parser(
        'mark',
        help=(
            'Mark one channel, or pass --batch to mark '
            'multiple channel IDs/handles from args or stdin.'
        ),
    )
    c_mark.add_argument(
        'args',
        nargs='*',
        metavar='key state',
        help=(
            'default: KEY STATE. With --batch: STATE [KEY ...]. '
            'Batch KEY may be "-" for stdin; with no KEY, stdin '
            'is read.'
        ),
    )
    c_mark.add_argument(
        '--batch',
        action='store_true',
        help=(
            'read multiple channel_ids/channel_handles from '
            'command line or stdin'
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
    c_backfill = sub.add_parser(
        'backfill-rss',
        help=(
            'One-shot backfill from rss:youtube:creators into '
            'youtube:channel:queue:scheduled:0. Only channel IDs '
            'with no existing channel queue meta are inserted; '
            'new entries are due immediately.'
        ),
    )
    c_backfill.add_argument(
        '--source', default='rss_backfill',
    )
    c_backfill.add_argument(
        '--batch-size', type=int, default=1000,
    )
    c_backfill.add_argument(
        '--limit', type=int, default=None,
        help='maximum number of RSS creator IDs to inspect',
    )
    c_backfill.add_argument(
        '--dry-run', action='store_true',
    )
    c_reconcile: ArgumentParser = sub.add_parser(
        'reconcile',
        help=(
            'Scan the channel scrape queue, channel '
            'metadata, and the RSS creator inventory for '
            'cross-system drift. By default this is read-only; '
            'pass --repair for Phase 2 safe repairs.'
        ),
    )
    c_reconcile.add_argument(
        '--dry-run',
        action='store_true',
        default=True,
    )
    c_reconcile.add_argument(
        '--json',
        action='store_true',
        default=False,
    )
    c_reconcile.add_argument(
        '--sample', type=int, default=5,
        help='Up to N example members per drift class',
    )
    c_reconcile.add_argument(
        '--batch-size', type=int, default=1000,
        help='Redis SCAN/pipeline batch size',
    )
    c_reconcile.add_argument(
        '--limit', type=int, default=None,
        help=(
            'Inspect at most N candidates per scan'
        ),
    )
    c_reconcile.add_argument(
        '--repair',
        choices=sorted(REPAIR_MODES),
        default=None,
        help=(
            'Apply one Phase 2 safe repair mode before '
            'emitting the reconciliation report.'
        ),
    )
    c_reconcile.add_argument(
        '--source',
        default='reconcile',
        help='Source label written to repaired channel/RSS meta',
    )
    c_reconcile.add_argument(
        '--due-now',
        action='store_true',
        default=False,
        help='Insert repaired queue entries with score 0',
    )
    c_reconcile.add_argument(
        '--max-repairs',
        type=int,
        default=None,
        help='Stop after applying at most N repairs',
    )
    c_reconcile.add_argument(
        '--default-channel-tier',
        type=int,
        default=None,
        help=(
            'Tier for missing channel queue entries. '
            'Defaults to the second-to-last channel tier.'
        ),
    )
    c_reconcile.add_argument(
        '--default-rss-tier',
        type=int,
        default=None,
        help=(
            'Tier for RSS auto-seed entries. Defaults to '
            'the second-to-last configured RSS tier.'
        ),
    )
    c_reconcile.add_argument(
        '--spread-window-seconds',
        type=int,
        default=86400,
        help=(
            'Deterministic scheduling spread for repaired '
            'entries when --due-now is not set.'
        ),
    )
    c_reconcile.add_argument(
        '--metrics-port',
        type=int,
        default=None,
        help=(
            'Expose reconcile Prometheus metrics on this port. '
            'Use with --watch for a long-running metrics process.'
        ),
    )
    c_reconcile.add_argument(
        '--watch',
        action='store_true',
        default=False,
        help=(
            'Run reconcile repeatedly, publishing metrics after '
            'each pass. Repairs are not allowed in watch mode.'
        ),
    )
    c_reconcile.add_argument(
        '--interval-seconds',
        type=float,
        default=1800.0,
        help='Delay between --watch reconcile passes.',
    )
    c_export: ArgumentParser = sub.add_parser(
        'export',
        help=(
            'Stream channel-queue members for a given state as '
            'JSON Lines (one record per line). Each record '
            'contains the member id, all available meta fields, '
            'and — for scheduled / pending_resolution — '
            'the score and tier.'
        ),
    )
    c_export.add_argument(
        'state',
        help=(
            'Which queue to export. Accepts any ChannelState '
            "value (e.g. 'scheduled', 'pending_resolution', "
            "'not_found', 'unresolved', 'terminated', "
            "'removed', 'invalid_handle', "
            "'inconsistent_identity', 'soft_unavailable', "
            "'hard_unavailable', 'topic', 'no_videos', "
            "'low_subs')."
        ),
    )
    c_export.add_argument(
        '--tier',
        type=int,
        default=None,
        help=(
            'Only meaningful for state=scheduled. Restrict to '
            'one tier index. Omit to export every scheduled '
            'tier merged.'
        ),
    )
    c_export.add_argument(
        '--output',
        default=None,
        help=(
            'Output file path. Default: stdout. Use "-" or '
            'omit for stdout.'
        ),
    )
    c_export.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Redis SCAN/pipeline batch size. Default 1000.',
    )
    c_export.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Stop after emitting N records.',
    )
    c_export_rss: ArgumentParser = sub.add_parser(
        'export-rss',
        help=(
            'Stream RSS-side inventory (rss:youtube:* keys) '
            'as JSON Lines. Complementary to "export": these '
            'are not part of the channel scrape queue; they '
            'are the RSS scraper\'s own state.'
        ),
    )
    c_export_rss.add_argument(
        'kind',
        choices=['creators', 'suppressed', 'tiers', 'queue'],
        help=(
            'Which RSS-side structure to export. '
            "'creators' = active inventory hash. "
            "'suppressed' = suppression hash with JSON "
            "envelopes. 'tiers' = RSS polling tier per "
            "channel. 'queue' = per-tier polling zset "
            '(score = due_time).'
        ),
    )
    c_export_rss.add_argument(
        '--tier',
        type=int,
        default=None,
        help=(
            'Only meaningful for kind=queue. Restrict to '
            'one tier index. Omit to export every '
            'rss:youtube:queue:* key merged.'
        ),
    )
    c_export_rss.add_argument(
        '--output',
        default=None,
        help=(
            'Output file path. Default: stdout. Use "-" or '
            'omit for stdout.'
        ),
    )
    c_export_rss.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Redis SCAN/pipeline batch size. Default 1000.',
    )
    c_export_rss.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Stop after emitting N records.',
    )
    return p


# Handler registry. Subcommands register themselves
# here via module-level mutation.
_HANDLERS: dict[str, _Handler] = {}


_EPOCH_DISPLAY_FIELDS: frozenset[str] = frozenset({
    'created_at',
    'last_attempt_at',
})


def _format_epoch_datetime(raw: str) -> str:
    '''Format a Redis epoch timestamp for operator output.

    Invalid values are returned unchanged so partially-written or
    legacy meta records remain inspectable.
    '''
    try:
        value: float = float(raw)
    except (TypeError, ValueError):
        return raw
    return datetime.fromtimestamp(
        value, tz=timezone.utc,
    ).strftime('%Y-%m-%d %H:%M:%S UTC')


def _format_meta_for_output(
    meta: dict[str, str],
) -> dict[str, str]:
    formatted: dict[str, str] = dict(meta)
    for field in _EPOCH_DISPLAY_FIELDS:
        if field in formatted:
            formatted[field] = _format_epoch_datetime(
                formatted[field],
            )
    return formatted


# Redis hash mapping channel_id -> channel_handle, mirroring
# ``RedisCreatorMap``'s ``f'{platform}:creator_map'`` key. This tool is
# YouTube-only, so the platform is fixed.
_CREATOR_MAP_KEY: str = 'youtube:creator_map'


def _record_has_handle(record: dict[str, object]) -> bool:
    '''True when the record already carries a handle from its meta
    hash (``channel_handle`` or ``handle``). The meta value wins over
    the creator-map lookup, so such records are left untouched.'''
    return bool(record.get('channel_handle')) or bool(
        record.get('handle'),
    )


def _channel_id_for_handle_lookup(
    record: dict[str, object], member: str,
) -> str | None:
    '''Channel id used to look up the handle in the creator map: the
    meta ``channel_id`` when present, otherwise the id encoded in an
    ``i:`` member. Returns ``None`` for handle-keyed members that have
    no resolved channel_id.'''
    cid: object = record.get('channel_id')
    if isinstance(cid, str) and cid:
        return cid
    if member.startswith('i:') and len(member) > 2:
        return member[2:]
    return None


async def _attach_channel_handles(
    queue: RedisChannelScrapeQueue,
    batch_records: list[tuple[str, dict[str, object]]],
) -> None:
    '''Enrich a batch of export records in place with a
    ``channel_handle`` resolved from the creator map.

    Only records lacking a handle are looked up (meta wins), and the
    lookup is a single batched ``HMGET`` per scan batch so large
    exports stay bounded. Records whose channel_id is absent from the
    creator map are left without a ``channel_handle`` field.
    '''
    ids: list[str] = []
    seen: set[str] = set()
    for member, record in batch_records:
        if _record_has_handle(record):
            continue
        cid: str | None = _channel_id_for_handle_lookup(record, member)
        if cid is not None and cid not in seen:
            seen.add(cid)
            ids.append(cid)
    if not ids:
        return
    values: list[str | None] = await queue._redis.hmget(
        _CREATOR_MAP_KEY, ids,
    )
    handle_by_id: dict[str, str | None] = dict(zip(ids, values))
    for member, record in batch_records:
        if _record_has_handle(record):
            continue
        cid = _channel_id_for_handle_lookup(record, member)
        if cid is None:
            continue
        handle: str | None = handle_by_id.get(cid)
        if handle:
            record['channel_handle'] = handle


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
    meta = _format_meta_for_output(meta)
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


def _iter_key_inputs(keys: list[str]) -> "list[str]":
    '''Expand the positional ``keys`` argument into a flat
    list of input lines. ``-`` is replaced inline with the
    contents of stdin (one entry per line); an empty
    ``keys`` list reads stdin entirely.
    '''
    if not keys:
        return [line.strip() for line in sys.stdin]
    out: list[str] = []
    for k in keys:
        if k == '-':
            out.extend(line.strip() for line in sys.stdin)
        else:
            out.append(k)
    return out


def _iter_add_inputs(keys: list[str]) -> "list[str]":
    return _iter_key_inputs(keys)


async def _resolve_member_for_add(
    identity: ChannelIdentityStore | None,
    *,
    channel_id: str | None,
    channel_handle: str | None,
) -> str | None:
    '''Compute the Redis member id that an ``add`` / ``import``
    call would target, replicating
    :func:`_enqueue_resolved_channel`'s routing.

    Returns ``i:<channel_id>`` when a valid channel_id is in
    hand (directly or via identity-store lookup of the handle),
    ``h:<handle>`` when only an unresolvable handle remains,
    or ``None`` when neither path applies.
    '''
    if (
        channel_id
        and YouTubeChannel.CHANNEL_ID_REGEX_MATCH.fullmatch(
            channel_id,
        )
    ):
        return f'i:{channel_id}'
    if not channel_handle:
        return None
    if identity is not None:
        cid: str | None = await identity.lookup_id_for_handle(
            channel_handle,
        )
        if cid:
            return f'i:{cid}'
    return f'h:{channel_handle}'


async def _classify_pre_existing(
    queue: RedisChannelScrapeQueue,
    identity: ChannelIdentityStore | None,
    *,
    channel_id: str | None,
    channel_handle: str | None,
) -> str:
    '''Pre-add classification.

    Looks up the meta hash for the member that the add/import
    flow would target and returns:

    * a :class:`ChannelState` value (e.g. ``'scheduled'``,
      ``'not_found'``) if meta has a ``state`` field
    * ``'no_state'`` — meta hash exists but no ``state`` field
    * ``'new'`` — no meta hash at all
    * ``'invalid_input'`` — neither channel_id nor a
      resolvable handle was supplied
    '''
    member: str | None = await _resolve_member_for_add(
        identity,
        channel_id=channel_id,
        channel_handle=channel_handle,
    )
    if member is None:
        return 'invalid_input'
    meta: dict[str, str] = await queue.get_meta(member)
    if not meta:
        return 'new'
    state: str | None = meta.get('state')
    if state is None:
        return 'no_state'
    return state


def _ordered_classification_keys(
    counts: dict[str, int],
) -> list[str]:
    '''Sort keys for the add/import classification report.

    ChannelState values first in enum order, then synthetic
    classifications (``new``, ``no_state``, ``invalid_input``).
    Only keys with non-zero counts are returned.
    '''
    state_order: list[str] = [s.value for s in ChannelState]
    synthetic: list[str] = [
        'new', 'no_state', 'invalid_input',
    ]
    ordered: list[str] = []
    for key in state_order + synthetic:
        if counts.get(key, 0) > 0:
            ordered.append(key)
    # Anything unexpected (future state values) — append.
    for key in sorted(counts.keys()):
        if (
            key not in state_order
            and key not in synthetic
            and counts[key] > 0
            and key not in ordered
        ):
            ordered.append(key)
    return ordered


def _write_classification_report(
    counts: dict[str, int],
    *,
    total: int,
    header: str,
) -> None:
    '''Emit the classification breakdown to stdout.'''
    sys.stdout.write(f'{header} {total} input(s):\n')
    sys.stdout.write('  pre-existing states:\n')
    keys: list[str] = _ordered_classification_keys(counts)
    if not keys:
        sys.stdout.write('    (none)\n')
        return
    width: int = max(len(k) for k in keys)
    width = max(width, 24)
    for key in keys:
        sys.stdout.write(
            f'    {key:<{width}} {counts[key]:>10}\n',
        )


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
    counts: dict[str, int] = {}
    total: int = 0
    for raw in _iter_add_inputs(list(ns.keys or [])):
        parsed: tuple[str | None, str | None] | None = (
            _parse_add_entry(raw)
        )
        if parsed is None:
            continue
        total += 1
        channel_id, channel_handle = parsed
        classification: str = await _classify_pre_existing(
            queue, identity,
            channel_id=channel_id,
            channel_handle=channel_handle,
        )
        counts[classification] = (
            counts.get(classification, 0) + 1
        )
        await _enqueue_resolved_channel(
            queue,
            channel_id=channel_id,
            channel_handle=channel_handle,
            source=ns.source,
            priority=ns.priority,
            identity=identity,
        )
    _write_classification_report(
        counts, total=total, header='added',
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
    for raw in _iter_key_inputs(ns.keys):
        if not raw.strip():
            continue
        member: str = _normalise_key(raw)
        state: ChannelState | None = (
            await queue.get_state(member)
        )
        if state is None:
            sys.stderr.write(
                f'no channel found for {raw!r}\n',
            )
            continue
        await queue.force_rescrape(
            member,
            mode=ns.mode,
            source='cli',
        )
    return 0


async def cmd_mark(
    ns: Namespace,
    queue: RedisChannelScrapeQueue,
) -> int:
    batch: bool = getattr(ns, 'batch', False)
    args: list[str] | None = getattr(ns, 'args', None)
    state_value: str | None = getattr(ns, 'state', None)
    raw_keys: list[str]
    if args is not None:
        if batch:
            if len(args) < 1:
                sys.stderr.write(
                    'usage: mark --batch STATE [KEY ...]\n',
                )
                return 2
            state_value = args[0]
            raw_keys = args[1:]
        else:
            if len(args) != 2:
                sys.stderr.write(
                    'usage: mark KEY STATE\n',
                )
                return 2
            raw_keys = [args[0]]
            state_value = args[1]
    elif batch:
        raw_keys = getattr(ns, 'keys', [])
    else:
        key_value: str | None = getattr(ns, 'key', None)
        if key_value is None or state_value is None:
            sys.stderr.write(
                'usage: mark KEY STATE\n',
            )
            return 2
        raw_keys = [key_value]

    if state_value is None:
        sys.stderr.write(
            'usage: mark KEY STATE\n',
        )
        return 2
    try:
        target: ChannelState = ChannelState(state_value)
    except ValueError:
        sys.stderr.write(
            f'unknown state: {state_value!r}\n',
        )
        return 2
    hard: bool = getattr(ns, 'hard', False)
    note: str | None = getattr(ns, 'note', None)
    if (
        hard
        and target == ChannelState.SOFT_UNAVAILABLE
    ):
        target = ChannelState.HARD_UNAVAILABLE
    for raw in _iter_key_inputs(raw_keys):
        if not raw.strip():
            continue
        member: str = _normalise_key(raw)
        await queue.mark(
            member, state=target, note=note,
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


async def _revive_if_terminal(
    queue: RedisChannelScrapeQueue,
    member: str,
) -> ChannelState | None:
    '''Re-enqueue *member* if it sits in a terminal state.

    When an imported member is currently parked in any
    terminal-state hash (``not_found``, ``removed``,
    ``terminated``, ...), ``enqueue_scheduled`` /
    ``enqueue_unresolved`` would leave a split record: the
    member back on an active queue but still listed in the
    terminal hash with a stale meta ``state``. ``unmark``
    removes it from every terminal hash, resets meta
    ``state``, and re-enqueues to the queue matching the
    member prefix. We pass an explicit normal-position
    score so revived channels do not jump to the front.

    Returns the prior terminal state when a revival
    happened, otherwise ``None``.
    '''
    state: ChannelState | None = await queue.get_state(member)
    if state is None or state not in ChannelState.terminal_states():
        return None
    await queue.unmark(member, score=time.time())
    return state


async def _enqueue_import_record(
    cid: str,
    handle: str,
    queue: RedisChannelScrapeQueue,
    identity: ChannelIdentityStore | None,
) -> tuple[int, int, ChannelState | None]:
    '''Enqueue one record; return
    ``(resolved, unresolved, revived_from)``.

    ``revived_from`` is the prior :class:`ChannelState` when
    the member was rescued from a terminal state, else
    ``None``.

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
        revived: ChannelState | None = await _revive_if_terminal(
            queue, f'i:{cid}',
        )
        if revived is None:
            await queue.enqueue_scheduled(
                cid, source='migration',
            )
        return (1, 0, revived)
    if not handle:
        return (0, 0, None)
    hit: str | None = None
    if identity is not None:
        hit = await identity.lookup_id_for_handle(
            handle,
        )
    if hit:
        revived = await _revive_if_terminal(
            queue, f'i:{hit}',
        )
        if revived is None:
            await queue.enqueue_scheduled(
                hit, source='migration',
            )
        return (1, 0, revived)
    canonical: str = queue._normalise_handle(handle)
    revived = await _revive_if_terminal(
        queue, f'h:{canonical}',
    )
    if revived is None:
        await queue.enqueue_unresolved(
            handle, source='migration',
        )
    return (0, 1, revived)


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
    revived: dict[ChannelState, int] = {}
    counts: dict[str, int] = {}
    total: int = 0
    for raw_line in _read_lines(ns.file):
        parsed = _parse_import_line(raw_line)
        if parsed is None:
            continue
        total += 1
        cid, handle = parsed
        # Reuse the same routing logic the enqueue uses for
        # classification so the report reflects what would be
        # checked against the queue: an empty / malformed cid
        # is promoted to the handle slot.
        cid_for_lookup: str = cid
        handle_for_lookup: str = handle
        if (
            cid_for_lookup
            and not YouTubeChannel.CHANNEL_ID_REGEX_MATCH.fullmatch(
                cid_for_lookup,
            )
        ):
            if not handle_for_lookup:
                handle_for_lookup = cid_for_lookup
            cid_for_lookup = ''
        classification: str = await _classify_pre_existing(
            queue, identity,
            channel_id=cid_for_lookup or None,
            channel_handle=handle_for_lookup or None,
        )
        counts[classification] = (
            counts.get(classification, 0) + 1
        )
        r, u, revived_from = await _enqueue_import_record(
            cid, handle, queue, identity,
        )
        added_resolved += r
        added_unresolved += u
        if revived_from is not None:
            revived[revived_from] = (
                revived.get(revived_from, 0) + 1
            )
    sys.stdout.write(
        f'imported: {added_resolved} resolved, '
        f'{added_unresolved} unresolved\n',
    )
    if revived:
        breakdown: str = ', '.join(
            f'{state.value}={n}'
            for state, n in sorted(
                revived.items(),
                key=lambda kv: kv[0].value,
            )
        )
        sys.stdout.write(
            f'revived from terminal: '
            f'{sum(revived.values())} ({breakdown})\n',
        )
    _write_classification_report(
        counts, total=total, header='processed',
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


async def cmd_backfill_rss(
    ns: Namespace,
    queue: RedisChannelScrapeQueue,
) -> int:
    if ns.batch_size <= 0:
        sys.stderr.write('--batch-size must be > 0\n')
        return 2
    inspected: int = 0
    malformed: int = 0
    already_known: int = 0
    added: int = 0
    batch: list[str] = []
    scheduled_keys: list[str] = [
        queue._k_scheduled(t)
        for t in range(len(queue._tiers))
    ]
    terminal_keys: list[str] = [
        queue._k_state(s)
        for s in sorted(
            ChannelState.terminal_states(),
            key=lambda state: state.value,
        )
    ]

    async def flush() -> None:
        nonlocal added, already_known, batch
        if not batch:
            return
        if ns.dry_run:
            pipe = queue._redis.pipeline(
                transaction=False,
            )
            for cid in batch:
                member: str = f'i:{cid}'
                for key in scheduled_keys:
                    pipe.zscore(key, member)
                for key in terminal_keys:
                    pipe.hexists(key, member)
            results: list[int] = await pipe.execute()
            group_size: int = (
                len(scheduled_keys) + len(terminal_keys)
            )
            for i in range(0, len(results), group_size):
                group = results[i:i + group_size]
                exists = any(group)
                if exists:
                    already_known += 1
                else:
                    added += 1
            batch = []
            return
        raw: list[int] = await queue._redis.eval(
            _LUA_BACKFILL_RSS_CREATORS,
            1 + len(scheduled_keys) + len(terminal_keys),
            queue._k_tiers(),
            *scheduled_keys,
            *terminal_keys,
            str(len(scheduled_keys)),
            str(len(terminal_keys)),
            'youtube:channel:meta:',
            ns.source,
            str(int(time.time())),
            *batch,
        )
        added += int(raw[0])
        already_known += int(raw[1])
        batch = []

    cursor: int | str = 0
    while True:
        cursor, data = await queue._redis.hscan(
            'rss:youtube:creators',
            cursor,
            count=ns.batch_size,
        )
        for cid in data.keys():
            if ns.limit is not None and inspected >= ns.limit:
                await flush()
                sys.stdout.write(
                    f'backfill-rss: inspected={inspected} '
                    f'added={added} already_known={already_known} '
                    f'malformed={malformed}'
                    f' dry_run={str(ns.dry_run).lower()}\n',
                )
                return 0
            inspected += 1
            if not YouTubeChannel.CHANNEL_ID_REGEX_MATCH.fullmatch(cid):
                malformed += 1
                continue
            batch.append(cid)
            if len(batch) >= ns.batch_size:
                await flush()
        if cursor == 0:
            break
    await flush()
    sys.stdout.write(
        f'backfill-rss: inspected={inspected} '
        f'added={added} already_known={already_known} '
        f'malformed={malformed}'
        f' dry_run={str(ns.dry_run).lower()}\n',
    )
    return 0


async def cmd_reconcile(
    ns: Namespace,
    queue: RedisChannelScrapeQueue,
) -> int:
    repair_mode: str | None = getattr(ns, 'repair', None)
    metrics_port: int | None = getattr(ns, 'metrics_port', None)
    watch: bool = bool(getattr(ns, 'watch', False))
    if watch and repair_mode:
        sys.stderr.write(
            '--watch cannot be combined with --repair\n',
        )
        return 2
    if metrics_port is not None:
        start_metrics_server(metrics_port)

    async def run_once() -> ReconcileReport:
        repaired: dict[str, int] = {}
        started: float = time.monotonic()
        if repair_mode:
            settings: YtChannelQueueSettings = (
                YtChannelQueueSettings()
            )
            rss_tiers: tuple[int, ...] = tuple(
                t.tier for t in parse_priority_queues(
                    settings.rss_priority_queues,
                )
            )
            repairer: ChannelQueueRepairer = (
                ChannelQueueRepairer(
                    queue,
                    queue._redis,
                    batch_size=ns.batch_size,
                )
            )
            repaired = await repairer.repair(
                RepairOptions(
                    mode=repair_mode,
                    source=getattr(ns, 'source', 'reconcile'),
                    due_now=getattr(ns, 'due_now', False),
                    max_repairs=getattr(ns, 'max_repairs', None),
                    default_channel_tier=getattr(
                        ns, 'default_channel_tier', None,
                    ),
                    default_rss_tier=getattr(
                        ns, 'default_rss_tier', None,
                    ),
                    rss_queue_tiers=rss_tiers,
                    spread_window_seconds=getattr(
                        ns,
                        'spread_window_seconds',
                        86400,
                    ),
                )
            )
        auditor: ChannelQueueAuditor = ChannelQueueAuditor(
            queue,
            queue._redis,
            batch_size=ns.batch_size,
            sample_size=ns.sample,
            limit=ns.limit,
        )
        report: ReconcileReport = await auditor.scan()
        report.dry_run = repair_mode is None
        report.repaired = repaired
        if metrics_port is not None:
            publish_reconcile_metrics(
                report,
                mode=repair_mode or 'dry-run',
                duration_seconds=time.monotonic() - started,
            )
        return report

    if watch:
        while True:
            try:
                await run_once()
            except Exception:
                record_reconcile_error('watch')
                raise
            await asyncio.sleep(
                float(getattr(ns, 'interval_seconds', 1800.0)),
            )

    report: ReconcileReport = await run_once()
    if ns.json:
        sys.stdout.write(
            json_mod.dumps(report.to_dict(), indent=2),
        )
        sys.stdout.write('\n')
        return 0
    sys.stdout.write(
        f'{"inspected":<32} {"count":>12}\n',
    )
    for k, v in sorted(report.inspected.items()):
        sys.stdout.write(
            f'  {k:<30} {v:>12}\n',
        )
    sys.stdout.write('\ndrift:\n')
    for kind in sorted(report.drift.keys()):
        r: DriftClassReport = report.drift[kind]
        sys.stdout.write(
            f'  {kind:<48} {r.count:>12}\n',
        )
        for s in r.samples:
            sys.stdout.write(
                f'      sample: {s.member}\n',
            )
    if report.repaired:
        sys.stdout.write('\nrepaired:\n')
        for mode, count in sorted(report.repaired.items()):
            sys.stdout.write(f'  {mode:<48} {count:>12}\n')
    return 0


async def cmd_export(
    ns: Namespace,
    queue: RedisChannelScrapeQueue,
) -> int:
    '''Stream queue members for a given state as JSON Lines.

    Each emitted line is a JSON object with these keys:

    * ``member`` — Redis member id (``i:UC...`` or ``h:handle``)
    * ``state`` — the requested state, lowercase
    * ``tier`` — only for ``state=scheduled``; the zset index
    * ``score`` — only for zset-backed queues (scheduled,
      pending_resolution); the member's score
    * all fields from ``HGETALL <meta_key>`` (handle,
      channel_id, name, source, created_at, last_attempt_at,
      note, etc.) — flat-merged into the top-level object
    * ``channel_handle`` — resolved from the
      ``youtube:creator_map`` (channel_id -> handle) Redis hash
      when the record has a channel_id but no handle of its own.
      Omitted when the creator map has no entry. A handle already
      present in the meta hash is left untouched.

    Use ``--output FILE`` to write to a file instead of stdout.
    '''
    state_label: str = ns.state.lower()
    try:
        state: ChannelState = ChannelState(state_label)
    except ValueError:
        sys.stderr.write(
            f'unknown state: {ns.state!r}. Valid values: '
            f'{sorted(s.value for s in ChannelState)}\n',
        )
        return 2

    # Decide which Redis keys to walk and how:
    # SCHEDULED + PENDING_RESOLUTION live in zsets (score is
    # meaningful); every other state lives in a hash.
    scan_plan: list[tuple[str, str, int | None]] = []
    if state is ChannelState.SCHEDULED:
        num_tiers: int = len(queue._tiers)
        if ns.tier is not None:
            if not (0 <= ns.tier < num_tiers):
                sys.stderr.write(
                    f'--tier must be in [0, {num_tiers}); '
                    f'got {ns.tier}\n',
                )
                return 2
            scan_plan.append(
                (queue._k_scheduled(ns.tier), 'zset', ns.tier),
            )
        else:
            for t in range(num_tiers):
                scan_plan.append(
                    (queue._k_scheduled(t), 'zset', t),
                )
    elif state is ChannelState.PENDING_RESOLUTION:
        if ns.tier is not None:
            sys.stderr.write(
                '--tier is only meaningful for '
                'state=scheduled\n',
            )
            return 2
        scan_plan.append(
            (queue._k_unresolved(), 'zset', None),
        )
    else:
        if ns.tier is not None:
            sys.stderr.write(
                '--tier is only meaningful for '
                'state=scheduled\n',
            )
            return 2
        scan_plan.append(
            (queue._k_state(state), 'hash', None),
        )

    out_path: str | None = ns.output
    if out_path is None or out_path == '-':
        out = sys.stdout
        close_out: bool = False
    else:
        out = open(out_path, 'w', encoding='utf-8')
        close_out = True

    emitted: int = 0
    batch_size: int = max(ns.batch_size, 1)
    try:
        for key, kind, tier in scan_plan:
            if ns.limit is not None and emitted >= ns.limit:
                break
            async for record in _iter_state_members(
                queue, key, kind, tier, state_label,
                batch_size,
            ):
                out.write(json.dumps(record) + '\n')
                emitted += 1
                if (
                    ns.limit is not None
                    and emitted >= ns.limit
                ):
                    break
    finally:
        if close_out:
            out.close()

    sys.stderr.write(
        f'exported {emitted} {state_label} record(s)\n',
    )
    return 0


async def _iter_state_members(
    queue: RedisChannelScrapeQueue,
    key: str,
    kind: str,
    tier: int | None,
    state_label: str,
    batch_size: int,
):
    '''Yield one merged dict per member of ``key``.

    ``kind`` is ``'zset'`` (scheduled / unresolved) or
    ``'hash'`` (terminal states). For ``'zset'`` the member's
    score is included; for ``'hash'`` the hash value is not
    parsed (terminal hashes typically store empty strings or
    JSON notes; the per-member metadata of interest lives in
    the ``meta`` hash).
    '''
    cursor: int = 0
    while True:
        if kind == 'zset':
            cursor, items = await queue._redis.zscan(
                key, cursor=cursor, count=batch_size,
            )
            pairs: list[tuple[str, float | None]] = [
                (member, float(score))
                for member, score in items
            ]
        else:
            cursor, items_map = await queue._redis.hscan(
                key, cursor=cursor, count=batch_size,
            )
            pairs = [
                (member, None)
                for member in items_map.keys()
            ]

        batch_records: list[tuple[str, dict[str, object]]] = (
            await _build_batch_records(
                queue, pairs, state_label, tier,
            )
        )
        for _member, record in batch_records:
            yield record

        if cursor == 0:
            return


async def _build_batch_records(
    queue: RedisChannelScrapeQueue,
    pairs: list[tuple[str, float | None]],
    state_label: str,
    tier: int | None,
) -> list[tuple[str, dict[str, object]]]:
    '''Build one merged export record per ``(member, score)`` pair and
    enrich the batch with creator-map handles via a single HMGET.'''
    batch_records: list[tuple[str, dict[str, object]]] = []
    for member, score in pairs:
        meta: dict[str, str] = await queue.get_meta(member)
        record: dict[str, object] = dict(
            _format_meta_for_output(meta),
        )
        record['member'] = member
        record['state'] = state_label
        if score is not None:
            record['score'] = score
        if tier is not None:
            record['tier'] = tier
        batch_records.append((member, record))
    await _attach_channel_handles(queue, batch_records)
    return batch_records


_RSS_TIER_FLAG_HINT: str = (
    '--tier is only meaningful for kind=queue\n'
)


async def _build_rss_scan_plan(
    ns: Namespace,
    queue: RedisChannelScrapeQueue,
    batch_size: int,
) -> tuple[list[tuple[str, str, int | None]], int | None]:
    '''Return ``(scan_plan, error_rc)``.

    On argument-validation failure writes to stderr and
    returns ``([], 2)``. Otherwise returns the plan and
    ``None``.
    '''
    kind: str = ns.kind
    if kind in ('creators', 'suppressed', 'tiers'):
        if ns.tier is not None:
            sys.stderr.write(_RSS_TIER_FLAG_HINT)
            return [], 2
        return [(f'rss:youtube:{kind}', kind, None)], None
    if kind == 'queue':
        if ns.tier is not None:
            return [(
                f'rss:youtube:queue:{ns.tier}',
                kind, ns.tier,
            )], None
        return await _discover_rss_queue_tiers(
            queue, batch_size,
        ), None
    sys.stderr.write(f'unknown kind: {kind!r}\n')
    return [], 2


async def _discover_rss_queue_tiers(
    queue: RedisChannelScrapeQueue,
    batch_size: int,
) -> list[tuple[str, str, int | None]]:
    '''SCAN ``rss:youtube:queue:*`` and return a scan plan
    sorted by numeric tier. Keys whose suffix isn't a valid
    int are skipped.
    '''
    tier_keys: list[tuple[int, str]] = []
    async for key in queue._redis.scan_iter(
        match='rss:youtube:queue:*',
        count=batch_size,
    ):
        suffix: str = key.rsplit(':', 1)[-1]
        try:
            tier_keys.append((int(suffix), key))
        except ValueError:
            continue
    return [
        (key, 'queue', tier)
        for tier, key in sorted(tier_keys)
    ]


async def cmd_export_rss(
    ns: Namespace,
    queue: RedisChannelScrapeQueue,
) -> int:
    '''Stream RSS-side inventory as JSON Lines.

    Walks one of the ``rss:youtube:*`` keys depending on
    ``ns.kind`` and emits one record per channel:

    * ``creators`` — every ``channel_id -> label`` mapping
      from ``rss:youtube:creators`` (active RSS inventory).
    * ``suppressed`` — every ``channel_id -> JSON envelope``
      from ``rss:youtube:suppressed``. The envelope's fields
      (``reason``, ``source``, ``ts``) are merged into the
      record when parseable; on parse failure the raw value
      is preserved under ``raw_value``.
    * ``tiers`` — every ``channel_id -> tier_str`` mapping
      from ``rss:youtube:tiers`` (RSS polling tier per
      channel). Tier value is also parsed to ``int`` when
      possible.
    * ``queue`` — every ``channel_id -> score`` member of
      ``rss:youtube:queue:<tier>``. Score is the next-poll
      due time. ``--tier`` pins one tier; otherwise every
      discoverable tier key is iterated.
    '''
    batch_size: int = max(ns.batch_size, 1)
    scan_plan: list[tuple[str, str, int | None]]
    err_rc: int | None
    scan_plan, err_rc = await _build_rss_scan_plan(
        ns, queue, batch_size,
    )
    if err_rc is not None:
        return err_rc

    out_path: str | None = ns.output
    if out_path is None or out_path == '-':
        out = sys.stdout
        close_out: bool = False
    else:
        out = open(out_path, 'w', encoding='utf-8')
        close_out = True

    emitted: int = 0
    try:
        for key, kind, tier in scan_plan:
            if ns.limit is not None and emitted >= ns.limit:
                break
            async for record in _iter_rss_members(
                queue, key, kind, tier, batch_size,
            ):
                out.write(json.dumps(record) + '\n')
                emitted += 1
                if (
                    ns.limit is not None
                    and emitted >= ns.limit
                ):
                    break
    finally:
        if close_out:
            out.close()

    sys.stderr.write(
        f'exported {emitted} rss-{ns.kind} record(s)\n',
    )
    return 0


def _rss_creators_record(
    channel_id: str, value: str,
) -> dict[str, object]:
    return {
        'channel_id': channel_id,
        'kind': 'creators',
        'label': value,
    }


def _rss_tiers_record(
    channel_id: str, value: str,
) -> dict[str, object]:
    parsed: object
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = value
    return {
        'channel_id': channel_id,
        'kind': 'tiers',
        'tier': parsed,
    }


def _rss_suppressed_record(
    channel_id: str, value: str,
) -> dict[str, object]:
    record: dict[str, object] = {
        'channel_id': channel_id,
        'kind': 'suppressed',
    }
    try:
        envelope: object = json.loads(value)
    except (TypeError, ValueError):
        record['raw_value'] = value
        return record
    if not isinstance(envelope, dict):
        record['raw_value'] = value
        return record
    for k_field, v_field in envelope.items():
        if k_field in record:
            continue
        record[k_field] = v_field
    return record


_RSS_HASH_BUILDERS: dict[
    str, Callable[[str, str], dict[str, object]],
] = {
    'creators': _rss_creators_record,
    'tiers': _rss_tiers_record,
    'suppressed': _rss_suppressed_record,
}


async def _iter_rss_members(
    queue: RedisChannelScrapeQueue,
    key: str,
    kind: str,
    tier: int | None,
    batch_size: int,
):
    '''Yield one dict per channel in the RSS key.

    Shape depends on ``kind``; per-kind builders live in
    ``_RSS_HASH_BUILDERS`` (for the three hash-backed
    inventories) and inline for ``queue`` (which is a zset).
    '''
    if kind == 'queue':
        async for record in _iter_rss_queue(
            queue, key, tier, batch_size,
        ):
            yield record
        return
    builder: Callable[
        [str, str], dict[str, object],
    ] = _RSS_HASH_BUILDERS[kind]
    async for record in _iter_rss_hash(
        queue, key, builder, batch_size,
    ):
        yield record


async def _iter_rss_queue(
    queue: RedisChannelScrapeQueue,
    key: str,
    tier: int | None,
    batch_size: int,
):
    cursor: int = 0
    while True:
        cursor, items = await queue._redis.zscan(
            key, cursor=cursor, count=batch_size,
        )
        for channel_id, score in items:
            yield {
                'channel_id': channel_id,
                'kind': 'queue',
                'tier': tier,
                'score': float(score),
            }
        if cursor == 0:
            return


async def _iter_rss_hash(
    queue: RedisChannelScrapeQueue,
    key: str,
    builder: Callable[[str, str], dict[str, object]],
    batch_size: int,
):
    cursor: int = 0
    while True:
        cursor, items_map = await queue._redis.hscan(
            key, cursor=cursor, count=batch_size,
        )
        for channel_id, value in items_map.items():
            yield builder(channel_id, value)
        if cursor == 0:
            return


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
_HANDLERS['backfill-rss'] = cmd_backfill_rss
_HANDLERS['reconcile'] = cmd_reconcile
_HANDLERS['export'] = cmd_export
_HANDLERS['export-rss'] = cmd_export_rss


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
    redis: aioredis.Redis = redis_from_url(
        settings.redis_dsn,
        component='yt-channel-queue-cli',
        decode_responses=True,
        max_connections=1,
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
    except RedisConnectionError as exc:
        sys.stderr.write(
            f'redis connection failed: {exc}\n'
        )
        return 1
    finally:
        await redis.aclose()


def main() -> None:
    raise SystemExit(
        asyncio.run(main_async(sys.argv[1:])),
    )


if __name__ == '__main__':
    main()
