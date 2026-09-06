'''Shared Redis client construction helpers.'''

import asyncio
import os
from typing import Any, Awaitable, Callable, TypeVar

from scrape_exchange.worker_id import get_worker_id


DEFAULT_MAX_CONNECTIONS: int = 4
DEFAULT_BUSY_RETRIES: int = 8
DEFAULT_BUSY_RETRY_BASE_SECONDS: float = 0.25
DEFAULT_BUSY_RETRY_MAX_SECONDS: float = 5.0

_T = TypeVar('_T')


def _redis_max_connections() -> int:
    raw: str | None = os.environ.get('REDIS_MAX_CONNECTIONS')
    if not raw:
        return DEFAULT_MAX_CONNECTIONS
    try:
        value: int = int(raw)
    except ValueError:
        return DEFAULT_MAX_CONNECTIONS
    return max(value, 1)


def _env_int(name: str, default: int) -> int:
    raw: str | None = os.environ.get(name)
    if not raw:
        return default
    try:
        value: int = int(raw)
    except ValueError:
        return default
    return max(value, 0)


def _env_float(name: str, default: float) -> float:
    raw: str | None = os.environ.get(name)
    if not raw:
        return default
    try:
        value: float = float(raw)
    except ValueError:
        return default
    return max(value, 0.0)


def redis_busy_retry_settings() -> tuple[int, float, float]:
    return (
        _env_int('REDIS_BUSY_RETRIES', DEFAULT_BUSY_RETRIES),
        _env_float(
            'REDIS_BUSY_RETRY_BASE_SECONDS',
            DEFAULT_BUSY_RETRY_BASE_SECONDS,
        ),
        _env_float(
            'REDIS_BUSY_RETRY_MAX_SECONDS',
            DEFAULT_BUSY_RETRY_MAX_SECONDS,
        ),
    )


def is_redis_busy_script_error(error: BaseException) -> bool:
    try:
        from redis.exceptions import ResponseError
    except ImportError:
        return False
    if not isinstance(error, ResponseError):
        return False
    message: str = str(error)
    return (
        'BUSY Redis is busy running a script' in message
        or 'You can only call SCRIPT KILL' in message
    )


def _is_redis_loading_error(error: BaseException) -> bool:
    try:
        from redis.exceptions import BusyLoadingError
    except ImportError:
        return False
    return isinstance(error, BusyLoadingError)


async def call_with_redis_busy_retry(
    operation: Callable[[], Awaitable[_T]],
    *,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> _T:
    '''Retry commands rejected while Redis runs a script or loads data.'''
    retries, base_delay, max_delay = redis_busy_retry_settings()
    attempt: int = 0
    while True:
        try:
            return await operation()
        except Exception as exc:
            if (
                not (
                    is_redis_busy_script_error(exc)
                    or _is_redis_loading_error(exc)
                )
                or attempt >= retries
            ):
                raise
            delay: float = min(
                max_delay,
                base_delay * (2 ** attempt),
            )
            attempt += 1
            if delay > 0:
                await sleep(delay)


def redis_client_name(component: str) -> str:
    safe_component: str = (
        component.replace(' ', '-').replace(':', '-')
    )
    return (
        f'scrape-python:{safe_component}:'
        f'w{get_worker_id()}:pid{os.getpid()}'
    )


def _retrying_pipeline_class(aioredis: Any):
    class BusyRetryPipeline(aioredis.client.Pipeline):
        async def execute(
            self, raise_on_error: bool = True,
        ) -> list[Any]:
            retries, base_delay, max_delay = (
                redis_busy_retry_settings()
            )
            attempt: int = 0
            while True:
                command_stack: list[Any] = list(
                    self.command_stack,
                )
                scripts: set[Any] = set(self.scripts)
                try:
                    return await super(
                        BusyRetryPipeline, self,
                    ).execute(raise_on_error=raise_on_error)
                except Exception as exc:
                    if (
                        not is_redis_busy_script_error(exc)
                        or attempt >= retries
                    ):
                        raise
                    delay: float = min(
                        max_delay,
                        base_delay * (2 ** attempt),
                    )
                    attempt += 1
                    # ``redis-py`` resets pipelines after execute(),
                    # including failures. Restore only for the retry.
                    self.command_stack = command_stack
                    self.scripts = scripts
                    if delay > 0:
                        await asyncio.sleep(delay)

    return BusyRetryPipeline


def _retrying_redis_class(aioredis: Any):
    pipeline_cls = _retrying_pipeline_class(aioredis)

    class BusyRetryRedis(aioredis.Redis):
        async def execute_command(
            self, *args: Any, **options: Any,
        ) -> Any:
            async def _execute_once() -> Any:
                return await super(
                    BusyRetryRedis, self,
                ).execute_command(*args, **options)

            return await call_with_redis_busy_retry(
                _execute_once,
            )

        def pipeline(
            self, transaction: bool = True,
            shard_hint: str | None = None,
        ) -> Any:
            return pipeline_cls(
                self.connection_pool,
                self.response_callbacks,
                transaction,
                shard_hint,
            )

    return BusyRetryRedis


def redis_from_url(
    redis_dsn: str,
    *,
    component: str,
    max_connections: int | None = None,
    **kwargs: Any,
):
    import redis.asyncio as aioredis

    kwargs.setdefault(
        'max_connections',
        (
            _redis_max_connections()
            if max_connections is None
            else max(max_connections, 1)
        ),
    )
    kwargs.setdefault('client_name', redis_client_name(component))
    pool = aioredis.BlockingConnectionPool.from_url(
        redis_dsn, **kwargs,
    )
    redis_cls = _retrying_redis_class(aioredis)
    return redis_cls(connection_pool=pool)
