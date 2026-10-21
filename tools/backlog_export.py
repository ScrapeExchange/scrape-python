'''Export the YouTube video backlog for ``backlog-server import``.

Two commands, each writing tab-separated files to --output-dir:

``--command scraped``
    Every YouTube video_id already on scrape.exchange, read from the
    scrape-api MongoDB through its covering index, into
    ``scraped.txt``. Safe to run while the scrapers run.

``--command backlog``
    The scraper queue: every ``youtube_videos`` document from the
    queue MongoDB (natural-order scan, no sort) plus the Redis hot
    window, into ``queued.tsv`` and ``terminal.tsv``. Run it with the
    YouTube producers, video scrapers and refill job stopped.

Always start with --dry-run: it prints the query plan and a sample
and writes nothing. See docs/operator/2026-10-20-backlog-server-
cutover.md for the full procedure.
'''

import asyncio
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal, TextIO

import redis.asyncio as aioredis
from pydantic import AliasChoices, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from pymongo import AsyncMongoClient

from scrape_exchange.backlog_server_queue import normalize_reason
from scrape_exchange.video_backlog import (
    STATE_HOT,
    STATE_QUEUED,
    TERMINAL_STATES,
)

SCRAPED_INDEX: str = (
    '_data_username_platform_entity_version_platform_content_id_idx'
)
EXCHANGE_DATABASE: str = 'scrape'
EXCHANGE_COLLECTION: str = 'data'
QUEUE_DATABASE: str = 'scraper'
QUEUE_COLLECTION: str = 'youtube_videos'
HOT_WINDOW_KEY: str = 'youtube:video:queue'
REDIS_TERMINAL_KEYS: tuple[str, ...] = tuple(
    f'youtube:video:{state}' for state in TERMINAL_STATES
)
BATCH_SIZE: int = 10_000
SAMPLE_SIZE: int = 10


class ExportSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file='.env', extra='ignore', cli_parse_args=True,
        cli_kebab_case=True, cli_implicit_flags=True,
        hide_input_in_errors=True, populate_by_name=True,
    )

    command: Literal['scraped', 'backlog'] = Field(
        description='What to export.',
    )
    exchange_mongo_dsn: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'exchange_mongo_dsn', 'SCRAPE_API_MONGO_DSN',
        ),
        description='scrape-api MongoDB URL (scraped command).',
    )
    mongo_dsn: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices('mongo_dsn', 'MONGO_DSN'),
        description='Scraper queue MongoDB URL (backlog command).',
    )
    redis_dsn: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices('redis_dsn', 'REDIS_DSN'),
        description='Scraper Redis URL (backlog command).',
    )
    exchange_username: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'exchange_username', 'EXCHANGE_USERNAME',
        ),
        description=(
            'scrape.exchange account that uploaded the videos; the '
            'index used by the scraped command starts with it.'
        ),
    )
    output_dir: Path = Field(
        default=Path('.'), description='Directory for the output files.',
    )
    dry_run: bool = Field(
        default=False,
        description='Print the query plan and a sample; write nothing.',
    )
    limit: int = Field(
        default=0, ge=0, description='Maximum documents; 0 means all.',
    )


@dataclass
class ExportCounts:
    queued: int = 0
    terminal: int = 0
    scraped: int = 0
    hot_window: int = 0
    skipped: int = 0


def _int(value: Any) -> int:
    try:
        return max(int(float(value)), 0)
    except (TypeError, ValueError):
        return 0


def format_queued_line(doc: dict[str, Any]) -> str:
    '''``video_id<TAB>enqueued_at`` for a queued or hot document.'''
    return f'{doc["_id"]}\t{_int(doc.get("enqueued_at"))}'


def format_terminal_line(doc: dict[str, Any]) -> str | None:
    '''``video_id<TAB>state<TAB>reason<TAB>ts`` for a terminal
    document, or None for any other state.'''
    state: Any = doc.get('state')
    if state not in TERMINAL_STATES:
        return None
    record: dict[str, Any] = doc.get('record') or {}
    reason: str = normalize_reason(record.get('last_error'))
    ts: int = _int(record.get('ts', doc.get('enqueued_at')))
    return f'{doc["_id"]}\t{state}\t{reason}\t{ts}'


def route_document(
    doc: dict[str, Any], counts: ExportCounts,
) -> tuple[str, str] | None:
    '''Which output a queue document goes to, and its line.'''
    if not isinstance(doc.get('_id'), str) or not doc['_id']:
        counts.skipped += 1
        return None
    state: Any = doc.get('state')
    if state in (STATE_QUEUED, STATE_HOT):
        counts.queued += 1
        return 'queued', format_queued_line(doc)
    line: str | None = format_terminal_line(doc)
    if line is None:
        counts.skipped += 1
        return None
    counts.terminal += 1
    return 'terminal', line


def _require(value: SecretStr | None, name: str) -> str:
    if value is None:
        sys.stderr.write(f'{name} is required for this command\n')
        raise SystemExit(2)
    return value.get_secret_value()


async def export_scraped(settings: ExportSettings) -> ExportCounts:
    if not settings.exchange_username:
        sys.stderr.write('EXCHANGE_USERNAME is required\n')
        raise SystemExit(2)
    client: AsyncMongoClient = AsyncMongoClient(
        _require(settings.exchange_mongo_dsn, 'SCRAPE_API_MONGO_DSN'),
        appname='backlog-export',
    )
    counts: ExportCounts = ExportCounts()
    try:
        coll: Any = client[EXCHANGE_DATABASE][EXCHANGE_COLLECTION]
        query: dict[str, Any] = {
            'username': settings.exchange_username,
            'platform': 'youtube',
            'entity': 'video',
        }
        projection: dict[str, int] = {'platform_content_id': 1, '_id': 0}
        if settings.dry_run:
            plan: dict[str, Any] = await coll.find(
                query, projection,
            ).hint(SCRAPED_INDEX).limit(SAMPLE_SIZE).explain()
            winning: Any = plan['queryPlanner']['winningPlan']
            print(json.dumps(winning, indent=2, default=str))
            sample: list[dict[str, Any]] = await coll.find(
                query, projection,
            ).hint(SCRAPED_INDEX).limit(SAMPLE_SIZE).to_list(None)
            print(json.dumps(sample, default=str))
            return counts
        out: Path = settings.output_dir / 'scraped.txt'
        with out.open('w', encoding='utf-8') as fh:
            cursor: Any = coll.find(query, projection).hint(
                SCRAPED_INDEX,
            ).batch_size(BATCH_SIZE)
            if settings.limit:
                cursor = cursor.limit(settings.limit)
            doc: dict[str, Any]
            async for doc in cursor:
                vid: Any = doc.get('platform_content_id')
                if isinstance(vid, str) and vid:
                    fh.write(f'{vid}\n')
                    counts.scraped += 1
                else:
                    counts.skipped += 1
    finally:
        await client.close()
    return counts


async def _export_hot_window(
    redis: aioredis.Redis, fh: TextIO, counts: ExportCounts,
) -> None:
    member: str
    score: float
    async for member, score in redis.zscan_iter(
        HOT_WINDOW_KEY, count=BATCH_SIZE,
    ):
        fh.write(f'{member}\t{_int(score)}\n')
        counts.hot_window += 1


async def export_backlog(settings: ExportSettings) -> ExportCounts:
    client: AsyncMongoClient = AsyncMongoClient(
        _require(settings.mongo_dsn, 'MONGO_DSN'), appname='backlog-export',
    )
    redis: aioredis.Redis = aioredis.Redis.from_url(
        _require(settings.redis_dsn, 'REDIS_DSN'), decode_responses=True,
    )
    counts: ExportCounts = ExportCounts()
    projection: dict[str, int] = {'state': 1, 'enqueued_at': 1, 'record': 1}
    try:
        db: Any = client.get_default_database(default=QUEUE_DATABASE)
        coll: Any = db[QUEUE_COLLECTION]
        if settings.dry_run:
            sample: list[dict[str, Any]] = await coll.find(
                {}, projection,
            ).limit(SAMPLE_SIZE).to_list(None)
            print(json.dumps(sample, default=str))
            sizes: dict[str, int] = {
                key: int(await redis.hlen(key))
                for key in REDIS_TERMINAL_KEYS
            }
            sizes[HOT_WINDOW_KEY] = int(await redis.zcard(HOT_WINDOW_KEY))
            print(json.dumps(sizes))
            return counts
        queued_path: Path = settings.output_dir / 'queued.tsv'
        terminal_path: Path = settings.output_dir / 'terminal.tsv'
        with queued_path.open('w', encoding='utf-8') as qf, \
                terminal_path.open('w', encoding='utf-8') as tf:
            outputs: dict[str, TextIO] = {'queued': qf, 'terminal': tf}
            cursor: Any = coll.find({}, projection).batch_size(BATCH_SIZE)
            if settings.limit:
                cursor = cursor.limit(settings.limit)
            doc: dict[str, Any]
            async for doc in cursor:
                routed: tuple[str, str] | None = route_document(doc, counts)
                if routed is not None:
                    outputs[routed[0]].write(f'{routed[1]}\n')
            await _export_hot_window(redis, qf, counts)
    finally:
        await client.close()
        await redis.aclose()
    return counts


async def run(settings: ExportSettings) -> ExportCounts:
    if not settings.dry_run:
        settings.output_dir.mkdir(parents=True, exist_ok=True)
    if settings.command == 'scraped':
        return await export_scraped(settings)
    return await export_backlog(settings)


def main() -> None:
    settings: ExportSettings = ExportSettings()
    counts: ExportCounts = asyncio.run(run(settings))
    if not settings.dry_run:
        print(json.dumps(asdict(counts)))


if __name__ == '__main__':
    main()
