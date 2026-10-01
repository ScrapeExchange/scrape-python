'''Shared Redis client construction helpers.'''

import asyncio
import os
from collections.abc import Awaitable, Callable
from typing import Any

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from scrape_exchange.worker_id import get_worker_id

DEFAULT_MAX_CONNECTIONS: int = 4
DEFAULT_BUSY_RETRIES: int = 8
DEFAULT_BUSY_RETRY_BASE_SECONDS: float = 0.25
DEFAULT_BUSY_RETRY_MAX_SECONDS: float = 5.0
# Connection failures (timeout/drop) retry with exponential backoff
# starting at 1s and doubling per failure; once the delay would
# exceed 256s the error is raised. Only connection establishment
# is retried; commands may have executed before a response is lost.
DEFAULT_CONNECT_RETRY_BASE_SECONDS: float = 1.0
DEFAULT_CONNECT_RETRY_MAX_SECONDS: float = 256.0


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


class RedisConnectRetrySettings(BaseSettings):
    model_config: SettingsConfigDict = SettingsConfigDict(
        env_prefix='REDIS_CONNECT_RETRY_',
    )

    base_seconds: float = Field(
        default=DEFAULT_CONNECT_RETRY_BASE_SECONDS,
        gt=0, allow_inf_nan=False,
    )
    max_seconds: float = Field(
        default=DEFAULT_CONNECT_RETRY_MAX_SECONDS,
        ge=0, allow_inf_nan=False,
    )


async def call_with_redis_connect_retry[T](
    operation: Callable[[], Awaitable[T]],
    *,
    settings: RedisConnectRetrySettings | None = None,
    sleep: Callable[[float], Awaitable[None]] | None = None,
    touch_work: Callable[[], None] | None = None,
) -> T:
    '''Retry only connection setup, before submitting application commands.'''
    config: RedisConnectRetrySettings = (
        settings if settings is not None else RedisConnectRetrySettings()
    )
    sleeper: Callable[[float], Awaitable[None]] = sleep or asyncio.sleep
    touch: Callable[[], None] = (
        touch_work if touch_work is not None else _default_touch_work()
    )
    delay: float = config.base_seconds
    while True:
        try:
            return await operation()
        except Exception as exc:
            if (
                not is_redis_connection_error(exc)
                or _is_redis_loading_error(exc)
                or delay > config.max_seconds
            ):
                raise
            await _paced_sleep(delay, sleeper, touch)
            delay *= 2


# Backoff sleeps are paced in slices no longer than this, pulsing the
# liveness watchdog's work signal between slices: a scraper parked in
# Redis-reconnect backoff is alive by intent, not wedged, and must not
# be killed by the 180s work-signal timeout mid-retry.
_WATCHDOG_TOUCH_SLICE_SECONDS: float = 30.0


def _default_touch_work() -> Callable[[], None]:
    '''Return the watchdog's work-signal touch, or a no-op when no
    watchdog is installed.'''
    try:
        from scrape_exchange.watchdog import Watchdog
    except Exception:
        return lambda: None
    return Watchdog.get().touch_work


async def _paced_sleep(
    delay: float,
    sleeper: Callable[[float], Awaitable[None]],
    touch_work: Callable[[], None],
) -> None:
    '''Sleep *delay* in slices of at most 30s, pulsing the watchdog
    work signal before and between slices.'''
    touch_work()
    remaining: float = delay
    while remaining > 0:
        slice_seconds: float = min(
            remaining, _WATCHDOG_TOUCH_SLICE_SECONDS,
        )
        await sleeper(slice_seconds)
        remaining -= slice_seconds
        touch_work()


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


def is_redis_connection_error(error: BaseException) -> bool:
    '''True for connection timeouts/drops: ``redis.exceptions``
    connection/timeout errors, socket-level ``OSError``s, and asyncio
    timeouts.

    ``AuthenticationError`` subclasses ``ConnectionError`` but is
    permanent — retrying with growing backoff would only stall the
    scraper, so it is excluded.
    '''
    try:
        from redis.exceptions import (
            AuthenticationError,
        )
        from redis.exceptions import (
            ConnectionError as RedisConnectionError,
        )
        from redis.exceptions import (
            TimeoutError as RedisTimeoutError,
        )
    except ImportError:
        return False
    if isinstance(error, AuthenticationError):
        return False
    return isinstance(
        error, (RedisConnectionError, RedisTimeoutError, OSError),
    )


async def call_with_redis_busy_retry[T](
    operation: Callable[[], Awaitable[T]],
    *,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> T:
    '''Retry commands rejected while Redis runs a script or loads data.'''
    retries, base_delay, max_delay = redis_busy_retry_settings()
    busy_attempt: int = 0
    while True:
        try:
            return await operation()
        except Exception as exc:
            if (
                is_redis_busy_script_error(exc)
                or _is_redis_loading_error(exc)
            ):
                if busy_attempt >= retries:
                    raise
                delay: float = min(
                    max_delay,
                    base_delay * (2 ** busy_attempt),
                )
                busy_attempt += 1
            else:
                raise
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


def _retrying_pool_class(aioredis: Any) -> Any:
    class ConnectRetryPool(aioredis.BlockingConnectionPool):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self._connect_retry_settings: RedisConnectRetrySettings = (
                RedisConnectRetrySettings()
            )

        async def ensure_connection(self, connection: Any) -> None:
            async def connect_once() -> None:
                await super(ConnectRetryPool, self).ensure_connection(
                    connection,
                )

            # Pool validation happens before commands or pipeline writes.
            # Retrying execute_command()/execute() could repeat mutations.
            await call_with_redis_connect_retry(
                connect_once, settings=self._connect_retry_settings,
            )

    return ConnectRetryPool


def _retrying_pipeline_class(aioredis: Any):
    class BusyRetryPipeline(aioredis.client.Pipeline):
        async def execute(
            self, raise_on_error: bool = True,
        ) -> list[Any]:
            retries, base_delay, max_delay = (
                redis_busy_retry_settings()
            )
            busy_attempt: int = 0
            while True:
                command_stack: list[Any] = list(
                    self.command_stack,
                )
                scripts: set[Any] = set(self.scripts)
                try:
                    return await super().execute(raise_on_error=raise_on_error)
                except Exception as exc:
                    if (
                        is_redis_busy_script_error(exc)
                        or _is_redis_loading_error(exc)
                    ):
                        if busy_attempt >= retries:
                            raise
                        delay: float = min(
                            max_delay,
                            base_delay * (2 ** busy_attempt),
                        )
                        busy_attempt += 1
                    else:
                        raise
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
    pool_cls: Any = _retrying_pool_class(aioredis)
    pool: Any = pool_cls.from_url(
        redis_dsn, **kwargs,
    )
    redis_cls = _retrying_redis_class(aioredis)
    return redis_cls(connection_pool=pool)
