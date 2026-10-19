'''
Copy every key of one Redis database into another with MIGRATE.

Used to move the scraper Redis into a database of another server; see
docs/superpowers/specs/2026-10-19-scraper-redis-to-homedata-design.md.
MIGRATE always runs with COPY (the source keeps its data) and never
with REPLACE (a key already in the target is an error).

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import redis
from redis.connection import parse_url


@dataclass(frozen=True)
class Endpoint:
    '''Address of one Redis database.'''

    host: str
    port: int
    db: int
    username: str | None
    password: str | None


def endpoint_from_dsn(dsn: str) -> Endpoint:
    '''Parse a ``redis://`` URL into an :class:`Endpoint`.'''
    parts: dict[str, Any] = parse_url(dsn)
    return Endpoint(
        host=parts.get('host', 'localhost'),
        port=int(parts.get('port', 6379)),
        db=int(parts.get('db', 0)),
        username=parts.get('username'),
        password=parts.get('password'),
    )


def check_distinct(source: Endpoint, target: Endpoint) -> None:
    '''Refuse to copy a database onto itself.'''
    if (source.host, source.port, source.db) == (
        target.host, target.port, target.db,
    ):
        raise ValueError('source and target are the same database')


def split_batches(
    keys: list[str],
    sizes: dict[str, int],
    batch_size: int,
    big_key_bytes: int,
) -> tuple[list[list[str]], list[str]]:
    '''Group *keys* into MIGRATE batches; keys whose size exceeds
    *big_key_bytes* are returned separately to be copied one by one.'''
    small: list[str] = []
    big: list[str] = []
    key: str
    for key in keys:
        if sizes.get(key, 0) > big_key_bytes:
            big.append(key)
        else:
            small.append(key)
    batches: list[list[str]] = [
        small[start:start + batch_size]
        for start in range(0, len(small), batch_size)
    ]
    return batches, big


def migrate_args(
    target: Endpoint, keys: list[str], timeout_ms: int,
) -> list[str | int]:
    '''Arguments of a ``MIGRATE ... COPY ... KEYS`` command.'''
    args: list[str | int] = [
        'MIGRATE', target.host, target.port, '', target.db,
        timeout_ms, 'COPY',
    ]
    if target.username and target.password:
        args += ['AUTH2', target.username, target.password]
    elif target.password:
        args += ['AUTH', target.password]
    return args + ['KEYS', *keys]


class MigrateError(Exception):
    '''MIGRATE failed; the copy must stop.'''


@dataclass
class CopyStats:
    '''Progress of a copy.'''

    copied: int = 0
    skipped: int = 0
    failed: list[str] = field(default_factory=list)
    seconds: float = 0.0


@dataclass
class SampleReport:
    '''Result of :func:`sample_copy`.'''

    keys: int
    seconds: float
    source_dbsize: int
    estimated_seconds: float


def _migrate(
    source: redis.Redis, target: Endpoint, keys: list[str],
    timeout_ms: int, stats: CopyStats,
) -> None:
    try:
        reply: Any = source.execute_command(
            *migrate_args(target, keys, timeout_ms),
        )
    except redis.ResponseError as exc:
        stats.failed.extend(keys)
        raise MigrateError(f'MIGRATE failed: {exc}') from exc
    if reply in (b'NOKEY', 'NOKEY'):
        stats.skipped += len(keys)
    else:
        stats.copied += len(keys)


def _copy_keys(
    source: redis.Redis,
    target: Endpoint,
    keys: list[str],
    stats: CopyStats,
    *,
    batch_size: int,
    timeout_ms: int,
    big_key_bytes: int,
    big_key_timeout_ms: int,
) -> None:
    '''Copy *keys*: small ones in MIGRATE batches, big ones one at a
    time with the longer timeout.'''
    # One round trip for the sizes of all keys: a call per key would
    # add a network round trip per key to the downtime.
    pipe: Any = source.pipeline(transaction=False)
    key: str
    for key in keys:
        pipe.memory_usage(key)
    sizes: dict[str, int] = {
        key: int(size or 0) for key, size in zip(keys, pipe.execute())
    }
    batches, big = split_batches(keys, sizes, batch_size, big_key_bytes)
    batch: list[str]
    for batch in batches:
        _migrate(source, target, batch, timeout_ms, stats)
    for key in big:
        _migrate(source, target, [key], big_key_timeout_ms, stats)


def copy_all(
    source: redis.Redis,
    target: Endpoint,
    target_client: redis.Redis,
    *,
    batch_size: int,
    timeout_ms: int,
    big_key_bytes: int,
    big_key_timeout_ms: int,
    allow_nonempty: bool = False,
    limit: int | None = None,
    progress: Callable[[CopyStats], None] | None = None,
) -> tuple[CopyStats, list[str]]:
    '''Copy every key of *source* (or the first *limit* keys) into
    *target*. Keys that expire before they are copied are counted as
    skipped.

    :returns: the statistics and the keys sent to MIGRATE.
    :raises ValueError: the target database is not empty and
        *allow_nonempty* is false.
    :raises MigrateError: a MIGRATE call failed.
    '''
    if not allow_nonempty and target_client.dbsize():
        raise ValueError('target database is not empty')
    stats: CopyStats = CopyStats()
    sent: list[str] = []
    started: float = time.monotonic()
    pending: list[str] = []

    def _flush() -> None:
        _copy_keys(
            source, target, pending, stats, batch_size=batch_size,
            timeout_ms=timeout_ms, big_key_bytes=big_key_bytes,
            big_key_timeout_ms=big_key_timeout_ms,
        )
        sent.extend(pending)
        pending.clear()
        stats.seconds = time.monotonic() - started
        if progress is not None:
            progress(stats)

    key: Any
    for key in source.scan_iter(count=batch_size):
        if isinstance(key, bytes):
            key = key.decode()
        pending.append(key)
        if limit is not None and len(sent) + len(pending) >= limit:
            break
        if len(pending) >= batch_size:
            _flush()
    if pending:
        _flush()
    stats.seconds = time.monotonic() - started
    return stats, sent


def sample_copy(
    source: redis.Redis,
    target: Endpoint,
    target_client: redis.Redis,
    *,
    size: int,
    batch_size: int,
    timeout_ms: int,
    big_key_bytes: int,
    big_key_timeout_ms: int,
    allow_nonempty: bool = False,
) -> SampleReport:
    '''Copy *size* keys, measure the speed, then delete exactly those
    keys from the target.'''
    stats, sent = copy_all(
        source, target, target_client, batch_size=batch_size,
        timeout_ms=timeout_ms, big_key_bytes=big_key_bytes,
        big_key_timeout_ms=big_key_timeout_ms,
        allow_nonempty=allow_nonempty, limit=size,
    )
    start: int
    for start in range(0, len(sent), batch_size):
        target_client.delete(*sent[start:start + batch_size])
    total: int = int(source.dbsize())
    rate: float = len(sent) / stats.seconds if stats.seconds else 0.0
    return SampleReport(
        keys=len(sent), seconds=stats.seconds, source_dbsize=total,
        estimated_seconds=total / rate if rate else 0.0,
    )


@dataclass
class VerifyReport:
    '''Result of :func:`verify`.'''

    source_dbsize: int
    target_dbsize: int
    checked: int
    mismatches: list[str]


_SIZE_COMMANDS: dict[str, str] = {
    'hash': 'HLEN', 'zset': 'ZCARD', 'set': 'SCARD', 'list': 'LLEN',
    'string': 'STRLEN', 'stream': 'XLEN',
}


def _decode(value: Any) -> Any:
    return value.decode() if isinstance(value, bytes) else value


def key_shape(
    client: redis.Redis, key: str,
) -> tuple[str, int, bool] | None:
    '''``(type, size, has_ttl)`` of *key*, or None if it is missing.'''
    kind: str = _decode(client.type(key))
    if kind == 'none':
        return None
    size: int = 0
    if kind in _SIZE_COMMANDS:
        size = int(client.execute_command(_SIZE_COMMANDS[kind], key))
    elif kind == 'MBbloom--':
        info: list[Any] = client.execute_command('BF.INFO', key)
        fields: dict[str, Any] = {
            _decode(info[i]): info[i + 1] for i in range(0, len(info), 2)
        }
        size = int(fields.get('Number of items inserted', 0))
    return kind, size, int(client.ttl(key)) > 0


def verify(
    source: redis.Redis, target_client: redis.Redis, *, sample_size: int,
) -> VerifyReport:
    '''Compare up to *sample_size* source keys with the target by type,
    size and whether an expiry is set. A key that no longer exists in
    the source (it expired after the copy) is skipped.'''
    mismatches: list[str] = []
    checked: int = 0
    key: Any
    for key in source.scan_iter(count=1000):
        if checked >= sample_size:
            break
        key = _decode(key)
        expected: tuple[str, int, bool] | None = key_shape(source, key)
        if expected is None:
            continue
        checked += 1
        if key_shape(target_client, key) != expected:
            mismatches.append(key)
    return VerifyReport(
        source_dbsize=int(source.dbsize()),
        target_dbsize=int(target_client.dbsize()),
        checked=checked,
        mismatches=mismatches,
    )
