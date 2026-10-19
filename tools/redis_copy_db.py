'''
Copy every key of one Redis database into another with MIGRATE COPY.

Run it on the host of the source Redis, so MIGRATE (which the source
server executes) can reach the target. Example:

    SOURCE_REDIS_DSN=redis://:pw@localhost:6380/0 \\
    TARGET_REDIS_DSN=redis://:pw@<target-host>:6379/1 \\
        python tools/redis_copy_db.py --mode sample --sample-size 50000

Modes: copy (default), sample (copy N keys, report the speed, delete
them again), verify (compare key counts and a sample of keys).

Exit codes: 0 success; 1 a MIGRATE failed or verify found mismatches;
2 invalid settings or a refused target.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import sys
from collections.abc import Callable
from typing import Literal

import redis
from pydantic import AliasChoices, Field, SecretStr, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

from scrape_exchange.redis_copy import (
    CopyStats,
    Endpoint,
    MigrateError,
    SampleReport,
    VerifyReport,
    check_distinct,
    copy_all,
    endpoint_from_dsn,
    sample_copy,
    verify,
)


class CopySettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file='.env', extra='ignore', cli_parse_args=True,
        cli_kebab_case=True, cli_implicit_flags=True,
        hide_input_in_errors=True, populate_by_name=True,
    )

    source_dsn: SecretStr = Field(
        validation_alias=AliasChoices('source_dsn', 'SOURCE_REDIS_DSN'),
        description='Redis URL of the database to copy from.',
    )
    target_dsn: SecretStr = Field(
        validation_alias=AliasChoices('target_dsn', 'TARGET_REDIS_DSN'),
        description='Redis URL of the database to copy into.',
    )
    mode: Literal['copy', 'sample', 'verify'] = Field(
        default='copy', description='What to do.',
    )
    sample_size: int = Field(
        default=50_000, ge=1,
        description='Keys to copy in sample mode, or check in verify.',
    )
    batch_size: int = Field(
        default=1_000, ge=1, le=10_000,
        description='Keys per MIGRATE call.',
    )
    timeout_ms: int = Field(
        default=60_000, ge=1, description='Timeout per MIGRATE call.',
    )
    big_key_bytes: int = Field(
        default=10_000_000, ge=1,
        description='Keys larger than this are copied one at a time.',
    )
    big_key_timeout_ms: int = Field(
        default=600_000, ge=1, description='Timeout for a big key.',
    )
    allow_nonempty: bool = Field(
        default=False,
        description='Allow copying into a database that has keys.',
    )


def _progress_printer(total: int) -> Callable[[CopyStats], None]:
    '''Progress callback printing keys done, keys/s and the ETA
    against the source's key count *total*.'''

    def _progress(stats: CopyStats) -> None:
        done: int = stats.copied + stats.skipped
        rate: float = done / stats.seconds if stats.seconds else 0.0
        eta: float = (total - done) / rate if rate else 0.0
        sys.stderr.write(
            f'{done}/{total} keys ({rate:.0f} keys/s, '
            f'ETA {eta / 60:.1f} min)\n',
        )

    return _progress


def _verify(
    source: redis.Redis, target_client: redis.Redis, sample_size: int,
) -> int:
    report: VerifyReport = verify(
        source, target_client, sample_size=sample_size,
    )
    print(
        f'source keys {report.source_dbsize} target keys '
        f'{report.target_dbsize} checked {report.checked} '
        f'mismatches {len(report.mismatches)}',
    )
    key: str
    for key in report.mismatches[:20]:
        print(f'  mismatch: {key}')
    return 1 if report.mismatches else 0


def run(settings: CopySettings) -> int:
    source_url: str = settings.source_dsn.get_secret_value()
    target_url: str = settings.target_dsn.get_secret_value()
    target: Endpoint = endpoint_from_dsn(target_url)
    try:
        check_distinct(endpoint_from_dsn(source_url), target)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    source: redis.Redis = redis.Redis.from_url(source_url)
    target_client: redis.Redis = redis.Redis.from_url(target_url)
    if settings.mode == 'verify':
        return _verify(source, target_client, settings.sample_size)
    common: dict[str, int] = dict(
        batch_size=settings.batch_size, timeout_ms=settings.timeout_ms,
        big_key_bytes=settings.big_key_bytes,
        big_key_timeout_ms=settings.big_key_timeout_ms,
    )
    try:
        if settings.mode == 'sample':
            sample: SampleReport = sample_copy(
                source, target, target_client,
                size=settings.sample_size,
                allow_nonempty=settings.allow_nonempty, **common,
            )
            print(
                f'sampled {sample.keys} keys in {sample.seconds:.1f} s; '
                f'source has {sample.source_dbsize} keys; estimated '
                f'full copy {sample.estimated_seconds / 60:.1f} min',
            )
            return 0
        stats, _ = copy_all(
            source, target, target_client,
            allow_nonempty=settings.allow_nonempty,
            progress=_progress_printer(int(source.dbsize())), **common,
        )
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except MigrateError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(
        f'copied {stats.copied} skipped {stats.skipped} failed '
        f'{len(stats.failed)} in {stats.seconds:.1f} s',
    )
    return 1 if stats.failed else 0


def main() -> int:
    try:
        return run(CopySettings())
    except ValidationError as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
