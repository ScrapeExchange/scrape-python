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

from dataclasses import dataclass
from typing import Any

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
