#!/usr/bin/env python3
'''Platform/entity-agnostic operator CLI for scrape queues.

    scrape-queue [--platform P] [--entity E] <subcommand> [args]

Defaults to ``--platform tiktok --entity creator``. The tool holds no
platform logic: it resolves a ``(platform, entity)`` adapter from
``scrape_exchange.queue_admin`` and dispatches subcommands to it. See
docs/superpowers/specs/2026-06-10-agnostic-queue-tool-design.md.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import csv
import json
import sys
from argparse import ArgumentParser, Namespace
from typing import Awaitable, Callable

from pydantic import AliasChoices, Field
from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
)

from scrape_exchange.queue_admin import (
    ImportReport,
    normalize_tiktok_creator_submission,
    OperatorQueue,
    get_adapter,
)


class QueueToolSettings(BaseSettings):
    '''Minimal env-driven config for the operator CLI.'''

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore',
    )

    redis_dsn: str | None = Field(
        default=None,
        validation_alias=AliasChoices('REDIS_DSN', 'redis_dsn'),
    )
    worker_id: str = Field(
        default='queue-cli',
        validation_alias=AliasChoices('WORKER_ID', 'worker_id'),
    )
    creator_priority_queues: str = Field(
        default='6:1000000,24:100000,72:10000,168:0',
        validation_alias=AliasChoices(
            'TIKTOK_CREATOR_PRIORITY_QUEUES',
            'creator_priority_queues',
        ),
    )


def _build_parser() -> ArgumentParser:
    p: ArgumentParser = ArgumentParser(prog='queue')
    p.add_argument('--platform', default='tiktok')
    p.add_argument('--entity', default='creator')
    sub = p.add_subparsers(dest='command')

    sub.add_parser('count')
    sub.add_parser('stats')

    c_show: ArgumentParser = sub.add_parser('show')
    c_show.add_argument('member_id')

    c_search: ArgumentParser = sub.add_parser('search')
    c_search.add_argument('term')
    c_search.add_argument('--limit', type=int, default=20)

    c_add: ArgumentParser = sub.add_parser('add')
    c_add.add_argument(
        'members',
        nargs='*',
        help='Members to add. Use "-" or omit members to read stdin.',
    )
    c_add.add_argument('--weight', type=int, default=0)

    c_remove: ArgumentParser = sub.add_parser('remove')
    c_remove.add_argument('members', nargs='+')

    c_rescrape: ArgumentParser = sub.add_parser('rescrape')
    c_rescrape.add_argument('members', nargs='+')

    c_import: ArgumentParser = sub.add_parser('import')
    c_import.add_argument('path')

    c_export: ArgumentParser = sub.add_parser('export')
    c_export.add_argument('--jsonl', action='store_true')
    c_export.add_argument('--output', default=None)
    c_export.add_argument('--state', default=None)

    return p


def _normalizes_tiktok_creator_handles(
    adapter: OperatorQueue,
) -> bool:
    return adapter.platform == 'tiktok' and adapter.entity == 'creator'


def _member_key(adapter: OperatorQueue) -> str:
    '''The record-dict key holding the member id for *adapter*.'''
    return (
        'creator_id'
        if adapter.member_label == 'username'
        else adapter.member_label
    )


def _stdin_members() -> list[str]:
    members: list[str] = []
    for line in sys.stdin:
        member: str = line.strip()
        if not member or member.startswith('#'):
            continue
        members.append(member)
    return members


def _add_members_from_args(ns: Namespace) -> list[str]:
    members: list[str] = []
    stdin_members: list[str] | None = None
    for member in ns.members:
        if member == '-':
            if stdin_members is None:
                stdin_members = _stdin_members()
            members.extend(stdin_members)
        else:
            members.append(member)
    if not members:
        members.extend(_stdin_members())
    return members


async def cmd_count(
    adapter: OperatorQueue, ns: Namespace,
) -> int:
    counts: dict[str, int] = await adapter.count_by_state()
    print(sum(counts.values()))
    return 0


async def cmd_stats(
    adapter: OperatorQueue, ns: Namespace,
) -> int:
    counts: dict[str, int] = await adapter.count_by_state()
    print(f'{"state":<12} {"count":>10}')
    for state in adapter.states():
        print(f'{state:<12} {counts.get(state, 0):>10}')
    return 0


async def cmd_show(
    adapter: OperatorQueue, ns: Namespace,
) -> int:
    rec: dict | None = await adapter.show(ns.member_id)
    if rec is None:
        print(f'{ns.member_id}: not found', file=sys.stderr)
        return 1
    for key, value in rec.items():
        print(f'{key:<12} {value}')
    return 0


async def cmd_search(
    adapter: OperatorQueue, ns: Namespace,
) -> int:
    hits: list[dict] = await adapter.search(ns.term, ns.limit)
    member_key: str = _member_key(adapter)
    for rec in hits:
        print(
            f'{rec.get(member_key, ""):<24} '
            f'{rec.get("state", ""):<10} '
            f'tier={rec.get("tier")}',
        )
    print(f'({len(hits)} match(es))', file=sys.stderr)
    return 0


async def cmd_add(
    adapter: OperatorQueue, ns: Namespace,
) -> int:
    pairs: list[tuple[str, int]] = []
    for member in _add_members_from_args(ns):
        resolved: str | None = member
        if _normalizes_tiktok_creator_handles(adapter):
            resolved = normalize_tiktok_creator_submission(member)
        if resolved is not None:
            pairs.append((resolved, ns.weight))
    added: int = await adapter.add(pairs)
    print(f'added {added}')
    return 0


async def cmd_remove(
    adapter: OperatorQueue, ns: Namespace,
) -> int:
    removed: int = 0
    for member in ns.members:
        if await adapter.remove(member):
            removed += 1
    print(f'removed {removed}')
    return 0


async def cmd_rescrape(
    adapter: OperatorQueue, ns: Namespace,
) -> int:
    count: int = await adapter.rescrape(ns.members)
    print(f'rescheduled {count}')
    return 0


async def cmd_import(
    adapter: OperatorQueue, ns: Namespace,
) -> int:
    report: ImportReport = await adapter.import_members(ns.path)
    print(
        f'lines={report.total_lines} added={report.added} '
        f'duplicates={report.duplicates} invalid={report.invalid} '
        f'blank={report.blank} comments={report.comments}',
    )
    return 0


async def cmd_export(
    adapter: OperatorQueue, ns: Namespace,
) -> int:
    state_filter: str | None = None
    if ns.state is not None:
        state_filter = ns.state.lower()
        valid: list[str] = adapter.states()
        if state_filter not in valid:
            print(
                f'unknown state {ns.state!r}; valid: '
                f'{", ".join(valid)}',
                file=sys.stderr,
            )
            return 2
    member_key: str = _member_key(adapter)
    count: int = 0
    stream = sys.stdout
    should_close: bool = False
    if ns.output is not None:
        stream = open(ns.output, 'a', encoding='utf-8', newline='')
        should_close = True
    try:
        writer = csv.writer(stream, lineterminator='\n')
        async for rec in adapter.export():
            if (
                state_filter is not None
                and rec.get('state') != state_filter
            ):
                continue
            if ns.jsonl:
                print(json.dumps(rec, default=str), file=stream)
            else:
                writer.writerow([
                    rec.get(member_key, ''),
                    rec.get('state', ''),
                    rec.get('tier', ''),
                ])
            count += 1
    finally:
        if should_close:
            stream.close()
    print(f'({count} exported)', file=sys.stderr)
    return 0


_HANDLERS: dict[
    str, Callable[[OperatorQueue, Namespace], Awaitable[int]]
] = {
    'count': cmd_count,
    'stats': cmd_stats,
    'show': cmd_show,
    'search': cmd_search,
    'add': cmd_add,
    'remove': cmd_remove,
    'rescrape': cmd_rescrape,
    'import': cmd_import,
    'export': cmd_export,
}


async def main_async(argv: list[str]) -> int:
    parser: ArgumentParser = _build_parser()
    ns: Namespace = parser.parse_args(argv)
    if ns.command is None:
        parser.print_help()
        return 2

    settings: QueueToolSettings = QueueToolSettings()
    try:
        adapter: OperatorQueue = get_adapter(
            ns.platform, ns.entity, settings,
        )
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    try:
        return await _HANDLERS[ns.command](adapter, ns)
    finally:
        await adapter.close()


def main() -> None:
    sys.exit(asyncio.run(main_async(sys.argv[1:])))


if __name__ == '__main__':
    main()
