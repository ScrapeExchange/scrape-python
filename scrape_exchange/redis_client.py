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

# Backlog-server client tuning. backlog-server is one process on one
# host speaking RESP, reached over the LAN, so a restart, a dropped
# socket or a wedged compaction is an ordinary event for its clients
# rather than an outage they should die on.
DEFAULT_BACKLOG_CONNECT_TIMEOUT_SECONDS: float = 5.0
DEFAULT_BACKLOG_SOCKET_TIMEOUT_SECONDS: float = 30.0
DEFAULT_BACKLOG_HEALTH_CHECK_INTERVAL_SECONDS: float = 30.0
DEFAULT_BACKLOG_COMMAND_RETRIES: int = 5
DEFAULT_BACKLOG_RETRY_BASE_SECONDS: float = 1.0
DEFAULT_BACKLOG_RETRY_CAP_SECONDS: float = 30.0


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


class BacklogRedisSettings(BaseSettings):
    '''Connection tuning for the backlog-server client.

    ``BACKLOG_REDIS_*`` rather than ``BACKLOG_*``: the server's own
    configuration (data directory, listen address, password) already
    owns that namespace.
    '''

    model_config: SettingsConfigDict = SettingsConfigDict(
        env_prefix='BACKLOG_REDIS_',
    )

    connect_timeout_seconds: float = Field(
        default=DEFAULT_BACKLOG_CONNECT_TIMEOUT_SECONDS,
        gt=0, allow_inf_nan=False,
    )
    socket_timeout_seconds: float = Field(
        default=DEFAULT_BACKLOG_SOCKET_TIMEOUT_SECONDS,
        gt=0, allow_inf_nan=False,
    )
    health_check_interval_seconds: float = Field(
        default=DEFAULT_BACKLOG_HEALTH_CHECK_INTERVAL_SECONDS,
        ge=0, allow_inf_nan=False,
    )
    command_retries: int = Field(
        default=DEFAULT_BACKLOG_COMMAND_RETRIES,
        ge=0,
    )
    retry_base_seconds: float = Field(
        default=DEFAULT_BACKLOG_RETRY_BASE_SECONDS,
        gt=0, allow_inf_nan=False,
    )
    retry_cap_seconds: float = Field(
        default=DEFAULT_BACKLOG_RETRY_CAP_SECONDS,
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


async def paced_sleep(
    delay: float,
    *,
    sleep: Callable[[float], Awaitable[None]] | None = None,
) -> None:
    '''Watchdog-aware sleep for callers that park on a long delay
    (retry backoff, reconnect wait) and must not be mistaken for a
    wedged process by the 180s work-signal timeout.'''
    await _paced_sleep(delay, sleep or asyncio.sleep, _default_touch_work())


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
    connect_retry: bool = True,
    **kwargs: Any,
):
    '''Build the shared Redis client.

    :param connect_retry: wrap connection establishment in the pool's
        watchdog-aware connect retry (1s to 256s). Turn it off only for
        a client whose own ``retry`` policy already reconnects: stacked
        on top of it, every pool connect attempt would run that policy
        in full (see :func:`backlog_redis_from_url`).
    '''
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
    # redis-py 8 defaults socket_timeout to 5 s (7.x: no timeout).
    # Reading a large reply, such as HGETALL of the 8.5M-entry creator
    # map, takes longer than that, so keep 7.x behaviour unless the
    # caller or the DSN (?socket_timeout=) sets a timeout.
    kwargs.setdefault('socket_timeout', None)
    pool_cls: Any = (
        _retrying_pool_class(aioredis) if connect_retry
        else aioredis.BlockingConnectionPool
    )
    pool: Any = pool_cls.from_url(
        redis_dsn, **kwargs,
    )
    redis_cls = _retrying_redis_class(aioredis)
    return redis_cls(connection_pool=pool)


def backlog_command_retry(
    settings: BacklogRedisSettings | None = None,
) -> Any:
    '''Retry policy for a command sent to backlog-server.

    Every ``BACKLOG.*`` command is idempotent, so a command that hit a
    connection error may be sent again: ``ADD`` answers 0 for an
    already-known ID, ``MARK``/``COMPLETE``/``REQUEUE``/``TOUCH`` set a
    state or a lease to a value, ``STATE``/``INFO`` are reads, and a
    replayed ``POP`` only leaves the first batch leased until its lease
    expires (the IDs come back to the queue; nothing is lost).

    That is what makes this the deliberate exception to the rule the
    pool follows: for the Redis queue's Lua mutations a lost response
    means the command *may* have run, so retries stay at connection
    establishment and never replay a command (see
    :func:`scrape_exchange.redis_client.redis_from_url`). Do not carry
    this policy over to the shared Redis client.
    '''
    from redis.asyncio.retry import Retry
    from redis.backoff import ExponentialWithJitterBackoff
    from redis.exceptions import ConnectionError as RedisConnectionError
    from redis.exceptions import TimeoutError as RedisTimeoutError

    config: BacklogRedisSettings = (
        settings if settings is not None else BacklogRedisSettings()
    )
    return Retry(
        backoff=ExponentialWithJitterBackoff(
            base=config.retry_base_seconds,
            cap=config.retry_cap_seconds,
        ),
        retries=config.command_retries,
        supported_errors=(
            RedisConnectionError, RedisTimeoutError,
        ),
    )


def backlog_redis_from_url(
    redis_dsn: str,
    *,
    component: str,
    **kwargs: Any,
) -> Any:
    '''Build a client for backlog-server that survives it going away.

    :func:`redis_from_url` on its own leaves ``redis-py`` with no
    command retries, no health check and no read timeout, which is fine
    for a Redis that is up but not for a single-process server on
    another host. This client adds:

    * ``socket_connect_timeout``: bounds one connect attempt, so an
      unreachable host fails fast and is retried by the policy below
      instead of blocking on the OS TCP timeout.
    * ``socket_timeout``: bounds a reply that never arrives. The
      shared client deliberately leaves this unset because reading the
      8.5M-entry creator map takes longer than any sane timeout;
      backlog-server replies are small and prompt.
    * ``health_check_interval``: a PING is sent on a connection that
      has been idle past the interval, so a socket the server closed
      while nothing was happening is caught before the command is
      written to it. The reconnect after a failed PING runs under the
      retry policy below; with ``command_retries=0`` the PING failure
      is raised to the caller.
    * the retry policy from :func:`backlog_command_retry`: a command
      that hit a connection error reconnects and is sent again, with
      bounded jittered exponential backoff. At the defaults the sleeps
      total 60s in the worst case (2+4+8+16+30), plus up to six
      connect timeouts (30s) when the host does not answer: inside
      the watchdog's 180s work-signal window.

    The pool's own connect retry (:func:`redis_from_url`'s
    ``connect_retry``) is turned off: redis-py runs this policy on
    every connect, so stacking the pool's 1s-to-256s loop on top made
    one checkout from the pool retry for 15 minutes or more before
    the caller heard about it. Waiting out a longer outage is the
    caller's job, with its own paced backoff and metrics (see
    ``_pop_entries_resilient`` in ``tools/yt_video_scrape.py``).

    :param redis_dsn: the backlog-server DSN
    :param component: client name for the connection, as in
        :func:`redis_from_url`

    Values in the DSN's query string (``?socket_timeout=``,
    ``?health_check_interval=``) win over these defaults, as in
    :func:`redis_from_url`.
    '''
    config: BacklogRedisSettings = BacklogRedisSettings()
    kwargs.setdefault(
        'socket_connect_timeout', config.connect_timeout_seconds,
    )
    kwargs.setdefault('socket_timeout', config.socket_timeout_seconds)
    kwargs.setdefault(
        'health_check_interval', config.health_check_interval_seconds,
    )
    kwargs.setdefault('retry', backlog_command_retry(config))
    return redis_from_url(
        redis_dsn, component=component, connect_retry=False, **kwargs,
    )
