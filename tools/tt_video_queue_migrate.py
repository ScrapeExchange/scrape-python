#!/usr/bin/env python3
'''Move the TikTok video scrape queue from Redis to backlog-server.

One-shot migration. The Redis queue (``tiktok:video:*``) held full
video URLs; backlog-server stores bare numeric ids in its
``tiktok:video`` queue (codec ``tiktok_video``). Three commands, each
writing to ``--output-dir`` the flat files that ``backlog-server
import`` reads:

``export-scraped``
    Every TikTok video id already on scrape.exchange, whoever uploaded
    it, read from the scrape-api MongoDB into ``scraped.txt``. The
    ``platform, entity, _id`` index selects the documents; each one is
    fetched for its ``platform_content_id`` (about a minute for two
    million). Imported as ``scraped``, so they are never queued again.
    Needs the ``migration`` extra (pymongo).

``export-queue``
    The Redis queue: waiting ids with their enqueue time, ids popped
    but never completed (queued again, at the front), and terminal
    ids with state and reason, converted from URLs to ids, into
    ``queued.tsv`` and ``terminal.tsv``. Run it with every TikTok
    video producer and ``tt_video_scrape.py`` stopped.

``delete-redis``
    Deletes the Redis keys of the old queue to free their memory.
    Without ``--apply`` it only counts them. With ``--apply`` it first
    checks that backlog-server serves the ``tiktok:video`` queue.

Always start with ``--dry-run`` for the export commands: it prints
the query plan or key counts and a sample, and writes nothing. See
``TIKTOK.md`` (TikTok video queue on backlog-server) for the full
procedure.

Examples::

    uv run --extra migration python tools/tt_video_queue_migrate.py \\
        export-scraped --dry-run
    uv run python tools/tt_video_queue_migrate.py export-queue \\
        --output-dir /var/tmp/tt-backlog
    uv run python tools/tt_video_queue_migrate.py delete-redis --apply

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import json
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

from pydantic import AliasChoices, Field, SecretStr
from pydantic_settings import (
    BaseSettings,
    CliPositionalArg,
    SettingsConfigDict,
)

from scrape_exchange.backlog_server_queue import normalize_reason
from scrape_exchange.redis_client import (
    backlog_redis_from_url,
    redis_from_url,
)

SCRAPED_INDEX: str = '_data_platform_entity_id_idx'
EXCHANGE_DATABASE: str = 'scrape'
EXCHANGE_COLLECTION: str = 'data'

REDIS_PREFIX: str = 'tiktok:video'
REDIS_QUEUE_KEY: str = f'{REDIS_PREFIX}:queue'
TERMINAL_STATES: tuple[str, ...] = ('unavailable', 'failed', 'removed')
REDIS_TERMINAL_KEYS: dict[str, str] = {
    state: f'{REDIS_PREFIX}:{state}' for state in TERMINAL_STATES
}
# Key families of RedisVideoScrapeQueue(platform='tiktok'). Nothing
# else under the prefix is touched.
REDIS_KEY_PATTERNS: tuple[str, ...] = (
    f'{REDIS_PREFIX}:qmeta:*',
    f'{REDIS_PREFIX}:meta:*',
)
BACKLOG_QUEUE: str = 'tiktok:video'

BATCH_SIZE: int = 10_000
DELETE_BATCH: int = 1000
SAMPLE_SIZE: int = 10

# Canonical id, as the backlog-server tiktok_video codec accepts it.
_VIDEO_ID_RE: re.Pattern[str] = re.compile(r'[1-9]\d{0,19}')
_VIDEO_URL_RE: re.Pattern[str] = re.compile(r'/video/([1-9]\d{0,19})\b')
_U64_MAX: int = 2**64 - 1


class MigrateSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file='.env', extra='ignore', cli_parse_args=True,
        cli_kebab_case=True, cli_implicit_flags=True,
        hide_input_in_errors=True, populate_by_name=True,
    )

    command: CliPositionalArg[
        Literal['export-scraped', 'export-queue', 'delete-redis']
    ] = Field(description='What to do.')
    exchange_mongo_dsn: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'exchange_mongo_dsn', 'SCRAPE_API_MONGO_DSN',
        ),
        description='scrape-api MongoDB URL (export-scraped).',
    )
    redis_dsn: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices('redis_dsn', 'REDIS_DSN'),
        description='Scraper Redis URL (export-queue, delete-redis).',
    )
    backlog_server_dsn: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'backlog_server_dsn', 'BACKLOG_SERVER_DSN',
        ),
        description='backlog-server URL (delete-redis --apply).',
    )
    output_dir: Path = Field(
        default=Path('.'), description='Directory for the output files.',
    )
    dry_run: bool = Field(
        default=False,
        description='Print the plan or counts and a sample; write nothing.',
    )
    apply: bool = Field(
        default=False,
        description='delete-redis: really delete the keys.',
    )
    limit: int = Field(
        default=0, ge=0,
        description='export-scraped: maximum documents; 0 means all.',
    )


@dataclass
class QueueExport:
    '''The Redis queue, converted to ids.'''

    # id -> enqueue time (epoch seconds); 0 sorts first.
    queued: dict[str, int]
    # id -> (state, reason, ts)
    terminal: dict[str, tuple[str, str, int]]


@dataclass
class QueueCounts:
    waiting: int = 0
    in_progress: int = 0
    terminal: int = 0
    duplicates: int = 0
    invalid: int = 0


def video_id_from_member(member: str) -> str | None:
    '''Bare id of a Redis queue member: a bare id or a video URL.'''
    candidate: str = member.strip()
    value: str
    if _VIDEO_ID_RE.fullmatch(candidate):
        value = candidate
    else:
        match: re.Match[str] | None = _VIDEO_URL_RE.search(candidate)
        if match is None:
            return None
        value = match.group(1)
    if int(value) > _U64_MAX:
        return None
    return value


def format_queued_line(video_id: str, enqueued_at: int) -> str:
    '''``video_id<TAB>enqueued_at`` for queued.tsv.'''
    return f'{video_id}\t{max(int(enqueued_at), 0)}'


def terminal_entry(
    state: str, raw_record: str | None,
) -> tuple[str, str, int]:
    '''(state, reason code, ts) of a Redis terminal hash value.'''
    record: Any = None
    if raw_record:
        try:
            record = json.loads(raw_record)
        except ValueError:
            record = None
    if not isinstance(record, dict):
        record = {}
    ts: int = 0
    try:
        ts = max(int(float(record.get('ts') or 0)), 0)
    except (TypeError, ValueError):
        ts = 0
    return state, normalize_reason(record.get('last_error')), ts


def format_terminal_line(
    video_id: str, entry: tuple[str, str, int],
) -> str:
    '''``video_id<TAB>state<TAB>reason<TAB>ts`` for terminal.tsv.'''
    state, reason, ts = entry
    return f'{video_id}\t{state}\t{reason}\t{ts}'


async def collect_redis_queue(
    redis: Any, counts: QueueCounts,
) -> QueueExport:
    '''Read the Redis TikTok video queue and convert members to ids.

    A terminal id wins over a queued one; an id popped but never
    completed (only in a qmeta bucket) is queued at the front.
    '''
    export: QueueExport = QueueExport(queued={}, terminal={})
    member: str
    video_id: str | None
    for state, key in REDIS_TERMINAL_KEYS.items():
        async for member, raw in redis.hscan_iter(key, count=1000):
            video_id = video_id_from_member(member)
            if video_id is None:
                counts.invalid += 1
                continue
            if video_id in export.terminal:
                counts.duplicates += 1
                continue
            export.terminal[video_id] = terminal_entry(state, raw)
            counts.terminal += 1
    score: float
    async for member, score in redis.zscan_iter(
        REDIS_QUEUE_KEY, count=1000,
    ):
        video_id = video_id_from_member(member)
        if video_id is None:
            counts.invalid += 1
            continue
        if video_id in export.queued or video_id in export.terminal:
            counts.duplicates += 1
            continue
        export.queued[video_id] = int(score)
        counts.waiting += 1
    bucket: str
    async for bucket in redis.scan_iter(
        match=f'{REDIS_PREFIX}:qmeta:*', count=1000,
    ):
        async for member, _packed in redis.hscan_iter(bucket, count=1000):
            video_id = video_id_from_member(member)
            if video_id is None:
                counts.invalid += 1
                continue
            if video_id in export.queued or video_id in export.terminal:
                continue
            # Popped by a scraper that never completed it.
            export.queued[video_id] = 0
            counts.in_progress += 1
    return export


def write_queue_files(export: QueueExport, output_dir: Path) -> None:
    '''Write queued.tsv and terminal.tsv for backlog-server import.'''
    output_dir.mkdir(parents=True, exist_ok=True)
    with (output_dir / 'queued.tsv').open('w', encoding='utf-8') as fh:
        for video_id, enqueued_at in export.queued.items():
            fh.write(format_queued_line(video_id, enqueued_at) + '\n')
    with (output_dir / 'terminal.tsv').open('w', encoding='utf-8') as fh:
        for video_id, entry in export.terminal.items():
            fh.write(format_terminal_line(video_id, entry) + '\n')


def _require(value: SecretStr | None, name: str) -> str:
    if value is None:
        sys.stderr.write(f'{name} is required for this command\n')
        raise SystemExit(2)
    return value.get_secret_value()


async def export_queue(settings: MigrateSettings) -> dict[str, Any]:
    redis: Any = redis_from_url(
        _require(settings.redis_dsn, 'REDIS_DSN'),
        component='tt-video-queue-migrate',
        decode_responses=True,
    )
    counts: QueueCounts = QueueCounts()
    try:
        if settings.dry_run:
            sample: list[tuple[str, float]] = await redis.zrange(
                REDIS_QUEUE_KEY, 0, SAMPLE_SIZE - 1, withscores=True,
            )
            return {
                'queue_size': await redis.zcard(REDIS_QUEUE_KEY),
                'terminal_sizes': {
                    state: await redis.hlen(key)
                    for state, key in REDIS_TERMINAL_KEYS.items()
                },
                'sample': [
                    {
                        'member': member,
                        'video_id': video_id_from_member(member),
                        'enqueued_at': int(score),
                    }
                    for member, score in sample
                ],
            }
        export: QueueExport = await collect_redis_queue(redis, counts)
    finally:
        await redis.aclose()
    write_queue_files(export, settings.output_dir)
    return {
        **asdict(counts),
        'queued_lines': len(export.queued),
        'terminal_lines': len(export.terminal),
        'output_dir': str(settings.output_dir),
    }


async def export_scraped(settings: MigrateSettings) -> dict[str, Any]:
    try:
        from pymongo import AsyncMongoClient
    except ImportError:
        sys.stderr.write(
            'export-scraped needs pymongo: run it with '
            '`uv run --extra migration python ...`\n',
        )
        raise SystemExit(2) from None
    client: Any = AsyncMongoClient(
        _require(settings.exchange_mongo_dsn, 'SCRAPE_API_MONGO_DSN'),
        appname='tt-video-queue-migrate',
    )
    written: int = 0
    invalid: int = 0
    try:
        coll: Any = client[EXCHANGE_DATABASE][EXCHANGE_COLLECTION]
        query: dict[str, Any] = {'platform': 'tiktok', 'entity': 'video'}
        projection: dict[str, int] = {'platform_content_id': 1, '_id': 0}
        if settings.dry_run:
            plan: dict[str, Any] = await coll.find(
                query, projection,
            ).hint(SCRAPED_INDEX).limit(SAMPLE_SIZE).explain()
            sample: list[dict[str, Any]] = await coll.find(
                query, projection,
            ).hint(SCRAPED_INDEX).limit(SAMPLE_SIZE).to_list(None)
            return {
                'winning_plan': plan['queryPlanner']['winningPlan'],
                'sample': sample,
            }
        settings.output_dir.mkdir(parents=True, exist_ok=True)
        out: Path = settings.output_dir / 'scraped.txt'
        cursor: Any = coll.find(query, projection).hint(
            SCRAPED_INDEX,
        ).batch_size(BATCH_SIZE)
        if settings.limit:
            cursor = cursor.limit(settings.limit)
        with out.open('w', encoding='utf-8') as fh:
            async for doc in cursor:
                video_id: str | None = video_id_from_member(
                    str(doc.get('platform_content_id') or ''),
                )
                if video_id is None:
                    invalid += 1
                    continue
                fh.write(f'{video_id}\n')
                written += 1
    finally:
        await client.close()
    return {'scraped': written, 'invalid': invalid, 'file': str(out)}


async def _count_redis_keys(redis: Any) -> dict[str, int]:
    counts: dict[str, int] = {
        REDIS_QUEUE_KEY: int(await redis.exists(REDIS_QUEUE_KEY)),
    }
    for key in REDIS_TERMINAL_KEYS.values():
        counts[key] = int(await redis.exists(key))
    for pattern in REDIS_KEY_PATTERNS:
        total: int = 0
        async for _key in redis.scan_iter(match=pattern, count=1000):
            total += 1
        counts[pattern] = total
    return counts


async def _backlog_queue_info(dsn: str) -> dict[str, int]:
    '''BACKLOG.INFO of the tiktok:video queue; raises when the server
    does not serve it.'''
    server: Any = backlog_redis_from_url(
        dsn, component='tt-video-queue-migrate', decode_responses=True,
    )
    try:
        reply: list[Any] = await server.execute_command(
            'BACKLOG.INFO', BACKLOG_QUEUE,
        )
    finally:
        await server.aclose()
    return {
        str(reply[i]): int(reply[i + 1])
        for i in range(0, len(reply) - 1, 2)
    }


async def delete_redis(settings: MigrateSettings) -> dict[str, Any]:
    redis: Any = redis_from_url(
        _require(settings.redis_dsn, 'REDIS_DSN'),
        component='tt-video-queue-migrate',
        decode_responses=True,
    )
    try:
        before: dict[str, int] = await _count_redis_keys(redis)
        if not settings.apply:
            return {'would_delete': before, 'applied': False}
        backlog: dict[str, int] = await _backlog_queue_info(
            _require(settings.backlog_server_dsn, 'BACKLOG_SERVER_DSN'),
        )
        deleted: int = 0
        fixed: list[str] = [
            REDIS_QUEUE_KEY, *REDIS_TERMINAL_KEYS.values(),
        ]
        deleted += int(await redis.unlink(*fixed))
        for pattern in REDIS_KEY_PATTERNS:
            batch: list[str] = []
            async for key in redis.scan_iter(match=pattern, count=1000):
                batch.append(key)
                if len(batch) >= DELETE_BATCH:
                    deleted += int(await redis.unlink(*batch))
                    batch = []
            if batch:
                deleted += int(await redis.unlink(*batch))
    finally:
        await redis.aclose()
    return {
        'deleted_keys': deleted,
        'before': before,
        'backlog_server': backlog,
        'applied': True,
    }


async def main_async(settings: MigrateSettings) -> dict[str, Any]:
    if settings.command == 'export-scraped':
        return await export_scraped(settings)
    if settings.command == 'export-queue':
        return await export_queue(settings)
    return await delete_redis(settings)


def main() -> int:
    settings: MigrateSettings = MigrateSettings()  # type: ignore[call-arg]
    summary: dict[str, Any] = asyncio.run(main_async(settings))
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
