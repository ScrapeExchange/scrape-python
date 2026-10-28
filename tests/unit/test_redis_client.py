'''Unit tests for shared Redis client construction.'''

import asyncio
import os
import unittest
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.redis_client import (
    DEFAULT_BACKLOG_COMMAND_RETRIES,
    DEFAULT_BACKLOG_CONNECT_TIMEOUT_SECONDS,
    DEFAULT_BACKLOG_HEALTH_CHECK_INTERVAL_SECONDS,
    DEFAULT_BACKLOG_MAX_CONNECTIONS,
    DEFAULT_BACKLOG_SOCKET_TIMEOUT_SECONDS,
    DEFAULT_MAX_CONNECTIONS,
    BacklogRedisSettings,
    RedisConnectRetrySettings,
    backlog_command_retry,
    backlog_redis_from_url,
    call_with_redis_busy_retry,
    call_with_redis_connect_retry,
    is_redis_busy_script_error,
    redis_client_name,
    redis_from_url,
)


class TestRedisClientName(unittest.TestCase):

    def test_includes_component_worker_and_pid(self) -> None:
        with (
            patch(
                'scrape_exchange.redis_client.get_worker_id',
                return_value='7',
            ),
            patch('scrape_exchange.redis_client.os.getpid',
                  return_value=1234),
        ):
            name: str = redis_client_name('youtube rss queue')

        self.assertEqual(
            name,
            'scrape-python:youtube-rss-queue:w7:pid1234',
        )


class TestRedisFromUrl(unittest.TestCase):

    def test_sets_default_pool_cap_and_client_name(self) -> None:
        with (
            patch(
                'redis.asyncio.BlockingConnectionPool.from_url',
            ) as from_url,
            patch.dict(os.environ, {}, clear=True),
        ):
            pool = MagicMock()
            from_url.return_value = pool
            client = redis_from_url(
                'redis://localhost:6379/0',
                component='test-component',
                decode_responses=True,
            )

        kwargs: dict[str, object] = from_url.call_args.kwargs
        self.assertEqual(
            kwargs['max_connections'],
            DEFAULT_MAX_CONNECTIONS,
    RedisConnectRetrySettings,
        )
        self.assertEqual(
            kwargs['client_name'],
            redis_client_name('test-component'),
        )
        self.assertTrue(kwargs['decode_responses'])
        self.assertIs(client.connection_pool, pool)

    def test_env_overrides_default_pool_cap(self) -> None:
        with (
            patch(
                'redis.asyncio.BlockingConnectionPool.from_url',
            ) as from_url,
            patch.dict(
                os.environ, {'REDIS_MAX_CONNECTIONS': '9'},
                clear=True,
            ),
        ):
            from_url.return_value = MagicMock()
            redis_from_url(
                'redis://localhost:6379/0',
                component='test-component',
            )

        self.assertEqual(
            from_url.call_args.kwargs['max_connections'],
            9,
        )

    def test_explicit_pool_cap_wins(self) -> None:
        with (
            patch(
                'redis.asyncio.BlockingConnectionPool.from_url',
            ) as from_url,
            patch.dict(
                os.environ, {'REDIS_MAX_CONNECTIONS': '9'},
                clear=True,
            ),
        ):
            from_url.return_value = MagicMock()
            redis_from_url(
                'redis://localhost:6379/0',
                component='test-component',
                max_connections=1,
            )

        self.assertEqual(
            from_url.call_args.kwargs['max_connections'],
            1,
        )


class TestRedisFromUrlSocketTimeout(unittest.TestCase):
    '''redis-py 8 defaults socket_timeout to 5 s; large replies such as
    HGETALL of the creator map take longer to read, so the shared
    client keeps redis-py 7's "no timeout" unless one is asked for.'''

    @staticmethod
    def _timeout(url: str, **kwargs: object) -> object:
        client = redis_from_url(url, component='test', **kwargs)
        return client.connection_pool.connection_kwargs.get(
            'socket_timeout', 'missing',
        )

    def test_no_socket_timeout_by_default(self) -> None:
        self.assertIsNone(self._timeout('redis://localhost:6379/0'))

    def test_explicit_socket_timeout_wins(self) -> None:
        self.assertEqual(
            self._timeout('redis://localhost:6379/0', socket_timeout=3.0),
            3.0,
        )

    def test_dsn_socket_timeout_is_honoured(self) -> None:
        self.assertEqual(
            self._timeout('redis://localhost:6379/0?socket_timeout=7'),
            7.0,
        )


class TestRedisBusyRetry(unittest.IsolatedAsyncioTestCase):

    async def test_command_recovers_after_dataset_loading(self) -> None:
        from redis.asyncio import Redis
        from redis.exceptions import BusyLoadingError

        command: AsyncMock = AsyncMock(side_effect=[
            BusyLoadingError('Redis is loading the dataset in memory'),
            [1, 0],
        ])
        with (
            patch.object(Redis, 'execute_command', command),
            patch.dict(os.environ, {
                'REDIS_BUSY_RETRIES': '2',
                'REDIS_BUSY_RETRY_BASE_SECONDS': '0',
            }, clear=True),
        ):
            client: Redis = redis_from_url(
                'redis://localhost:6379/0', component='test',
            )
            try:
                result: list[int] = await client.evalsha('sha', 0)
            finally:
                await client.aclose()

        self.assertEqual(result, [1, 0])
        self.assertEqual(command.await_count, 2)
        self.assertEqual(
            command.await_args_list[0], command.await_args_list[1],
        )

    async def test_dataset_loading_exhausts_bounded_backoff(self) -> None:
        from redis.exceptions import BusyLoadingError

        attempts: int = 0
        sleeps: list[float] = []

        async def operation() -> str:
            nonlocal attempts
            attempts += 1
            raise BusyLoadingError('Redis is loading the dataset in memory')

        async def sleep(delay: float) -> None:
            sleeps.append(delay)

        with (
            patch.dict(os.environ, {
                'REDIS_BUSY_RETRIES': '3',
                'REDIS_BUSY_RETRY_BASE_SECONDS': '0.25',
                'REDIS_BUSY_RETRY_MAX_SECONDS': '0.5',
            }, clear=True),
            self.assertRaises(BusyLoadingError),
        ):
            await call_with_redis_busy_retry(operation, sleep=sleep)

        self.assertEqual(attempts, 4)
        self.assertEqual(sleeps, [0.25, 0.5, 0.5])

    async def test_retries_busy_script_response(
        self,
    ) -> None:
        from redis.exceptions import ResponseError

        attempts: int = 0
        sleeps: list[float] = []

        async def operation() -> str:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise ResponseError(
                    'BUSY Redis is busy running a script. '
                    'You can only call SCRIPT KILL or '
                    'SHUTDOWN NOSAVE.',
                )
            return 'ok'

        async def sleep(delay: float) -> None:
            sleeps.append(delay)

        with patch.dict(
            os.environ,
            {
                'REDIS_BUSY_RETRIES': '2',
                'REDIS_BUSY_RETRY_BASE_SECONDS': '0.01',
                'REDIS_BUSY_RETRY_MAX_SECONDS': '0.01',
            },
            clear=True,
        ):
            result: str = await call_with_redis_busy_retry(
                operation, sleep=sleep,
            )

        self.assertEqual(result, 'ok')
        self.assertEqual(attempts, 2)
        self.assertEqual(sleeps, [0.01])

    async def test_does_not_retry_other_response_error(
        self,
    ) -> None:
        from redis.exceptions import ResponseError

        attempts: int = 0

        async def operation() -> str:
            nonlocal attempts
            attempts += 1
            raise ResponseError('WRONGTYPE Operation failed')

        with self.assertRaises(ResponseError):
            await call_with_redis_busy_retry(operation)

        self.assertEqual(attempts, 1)

    def test_identifies_busy_script_error(self) -> None:
        from redis.exceptions import ResponseError

        self.assertTrue(is_redis_busy_script_error(
            ResponseError(
                'BUSY Redis is busy running a script. '
                'You can only call SCRIPT KILL or '
                'SHUTDOWN NOSAVE.',
            ),
        ))
        self.assertFalse(is_redis_busy_script_error(
            ResponseError('WRONGTYPE Operation failed'),
        ))

    async def test_pipeline_retries_busy_script_response(
        self,
    ) -> None:
        from redis.asyncio.client import Pipeline
        from redis.exceptions import ResponseError

        attempts: int = 0

        async def execute(
            pipeline: Pipeline,
            raise_on_error: bool = True,
        ) -> list[str]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                pipeline.command_stack = []
                raise ResponseError(
                    'BUSY Redis is busy running a script. '
                    'You can only call SCRIPT KILL or '
                    'SHUTDOWN NOSAVE.',
                )
            self.assertTrue(pipeline.command_stack)
            return ['ok']

        with (
            patch(
                'redis.asyncio.BlockingConnectionPool.from_url',
            ) as from_url,
            patch.object(Pipeline, 'execute', execute),
            patch.dict(
                os.environ,
                {
                    'REDIS_BUSY_RETRIES': '2',
                    'REDIS_BUSY_RETRY_BASE_SECONDS': '0',
                },
                clear=True,
            ),
        ):
            from_url.return_value = MagicMock()
            client = redis_from_url(
                'redis://localhost:6379/0',
                component='test-component',
            )
            pipe = client.pipeline(transaction=False)
            pipe.set('key', 'value')
            result: list[str] = await pipe.execute()

        self.assertEqual(result, ['ok'])
        self.assertEqual(attempts, 2)


class TestRedisConnectionRetry(unittest.IsolatedAsyncioTestCase):

    async def test_timeout_retries_with_doubling_backoff(self) -> None:
        from redis.exceptions import TimeoutError as RedisTimeoutError

        attempts: int = 0
        sleeps: list[float] = []

        async def operation() -> str:
            nonlocal attempts
            attempts += 1
            if attempts <= 3:
                raise RedisTimeoutError('Timeout reading from socket')
            return 'ok'

        async def sleep(delay: float) -> None:
            sleeps.append(delay)

        result: str = await call_with_redis_connect_retry(
            operation, sleep=sleep,
        )

        self.assertEqual(result, 'ok')
        self.assertEqual(attempts, 4)
        self.assertEqual(sleeps, [1.0, 2.0, 4.0])

    async def test_stops_when_backoff_exceeds_256s(self) -> None:
        from redis.exceptions import ConnectionError as RedisConnectionError

        attempts: int = 0
        sleeps: list[float] = []

        async def operation() -> str:
            nonlocal attempts
            attempts += 1
            raise RedisConnectionError('Connection refused')

        async def sleep(delay: float) -> None:
            sleeps.append(delay)

        with self.assertRaises(RedisConnectionError):
            await call_with_redis_connect_retry(operation, sleep=sleep)

        # Delays 1..256 — 512 would exceed the 256s cap. Each delay
        # is slept in watchdog-paced slices of at most 30s.
        self.assertEqual(sleeps[:5], [1.0, 2.0, 4.0, 8.0, 16.0])
        self.assertTrue(all(s <= 30.0 for s in sleeps))
        self.assertEqual(sum(sleeps), 511.0)
        self.assertEqual(attempts, 10)

    async def test_recovers_mid_sequence_after_drop(self) -> None:
        from redis.exceptions import ConnectionError as RedisConnectionError
        from redis.exceptions import TimeoutError as RedisTimeoutError

        attempts: int = 0
        sleeps: list[float] = []

        async def operation() -> int:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RedisTimeoutError('timed out')
            if attempts == 2:
                raise OSError('Network is unreachable')
            if attempts == 3:
                raise RedisConnectionError('Connection closed by server')
            return attempts

        async def sleep(delay: float) -> None:
            sleeps.append(delay)

        result: int = await call_with_redis_connect_retry(
            operation, sleep=sleep,
        )

        self.assertEqual(result, 4)
        self.assertEqual(sleeps, [1.0, 2.0, 4.0])

    async def test_auth_error_is_not_retried(self) -> None:
        from redis.exceptions import AuthenticationError

        attempts: int = 0

        async def operation() -> str:
            nonlocal attempts
            attempts += 1
            raise AuthenticationError('WRONGPASS')

        async def sleep(delay: float) -> None:
            sleeps.append(delay)

        sleeps: list[float] = []

        with self.assertRaises(AuthenticationError):
            await call_with_redis_connect_retry(operation, sleep=sleep)

        self.assertEqual(attempts, 1)

    async def test_backoff_caps_at_env_max(self) -> None:
        from redis.exceptions import ConnectionError as RedisConnectionError

        attempts: int = 0
        sleeps: list[float] = []

        async def operation() -> str:
            nonlocal attempts
            attempts += 1
            raise RedisConnectionError('Connection refused')

        async def sleep(delay: float) -> None:
            sleeps.append(delay)

        with (
            patch.dict(os.environ, {
                'REDIS_CONNECT_RETRY_BASE_SECONDS': '0.5',
                'REDIS_CONNECT_RETRY_MAX_SECONDS': '3',
            }, clear=True),
            self.assertRaises(RedisConnectionError),
        ):
            await call_with_redis_connect_retry(operation, sleep=sleep)

        # 0.5, 1, 2 — the next delay of 4 would exceed the 3s cap.
        self.assertEqual(sleeps, [0.5, 1.0, 2.0])

    async def test_lost_responses_do_not_replay_commands(self) -> None:
        from fakeredis import FakeServer
        from fakeredis.aioredis import FakeAsyncRedisConnection
        from redis.exceptions import TimeoutError as RedisTimeoutError

        # Exercise ordinary commands and both pipeline modes. The fake
        # server executes each mutation before we discard its response.
        read_response: Any = FakeAsyncRedisConnection.read_response
        mode: str
        for mode in ('command', 'pipeline', 'transaction'):
            with self.subTest(mode=mode):
                client: Any = redis_from_url(
                    'redis://localhost:6379/0', component='test',
                    connection_class=FakeAsyncRedisConnection,
                    decode_responses=True, server=FakeServer(),
                )
                await client.zadd('queue', {'first': 1, 'second': 2})
                failed: bool = False

                async def lose_response(
                    connection: FakeAsyncRedisConnection,
                    *args: Any, **kwargs: Any,
                ) -> Any:
                    nonlocal failed
                    result: Any = await read_response(
                        connection, *args, **kwargs,
                    )
                    if not failed:
                        failed = True
                        raise RedisTimeoutError('Response lost')
                    return result

                try:
                    with (
                        patch.object(
                            FakeAsyncRedisConnection, 'read_response',
                            lose_response,
                        ),
                        patch(
                            'scrape_exchange.redis_client.asyncio.sleep',
                            new_callable=AsyncMock,
                        ),
                        patch.dict(os.environ, {
                            'REDIS_CONNECT_RETRY_BASE_SECONDS': '0.001',
                        }, clear=True),self.assertRaises(RedisTimeoutError)
                    ):
                        if mode == 'command':
                            await client.zpopmin('queue', 1)
                        else:
                            pipe: Any = client.pipeline(
                                transaction=mode == 'transaction',
                            )
                            pipe.zpopmin('queue', 1)
                            await pipe.execute()
                    self.assertEqual(
                        await client.zrange('queue', 0, -1), ['second'],
                    )
                finally:
                    await client.aclose()
                    await client.connection_pool.disconnect()

    async def test_connect_failure_recovers_before_submission(self) -> None:
        from fakeredis import FakeServer
        from fakeredis.aioredis import FakeAsyncRedisConnection
        from redis.exceptions import ConnectionError as RedisConnectionError

        connect: Any = FakeAsyncRedisConnection.connect
        pipeline: bool
        for pipeline in (False, True):
            with self.subTest(pipeline=pipeline):
                client: Any = redis_from_url(
                    'redis://localhost:6379/0', component='test',
                    connection_class=FakeAsyncRedisConnection,
                    decode_responses=True, server=FakeServer(),
                )
                failed: bool = False

                async def fail_connect_once(
                    connection: FakeAsyncRedisConnection,
                ) -> None:
                    nonlocal failed
                    if not failed:
                        failed = True
                        raise RedisConnectionError('Connection refused')
                    await connect(connection)

                try:
                    with (
                        patch.object(
                            FakeAsyncRedisConnection, 'connect',
                            fail_connect_once,
                        ),
                        patch(
                            'scrape_exchange.redis_client.asyncio.sleep',
                            new_callable=AsyncMock,
                        ),
                    ):
                        if pipeline:
                            pipe: Any = client.pipeline()
                            pipe.incr('counter')
                            self.assertEqual(await pipe.execute(), [1])
                        else:
                            self.assertEqual(await client.incr('counter'), 1)
                    self.assertEqual(await client.get('counter'), '1')
                finally:
                    await client.aclose()
                    await client.connection_pool.disconnect()

    async def test_cancelled_connect_releases_pool_slot(self) -> None:
        from fakeredis import FakeServer
        from fakeredis.aioredis import FakeAsyncRedisConnection
        from redis.exceptions import ConnectionError as RedisConnectionError

        client: Any = redis_from_url(
            'redis://localhost:6379/0', component='test',
            connection_class=FakeAsyncRedisConnection,
            server=FakeServer(), max_connections=1,
        )
        try:
            with (
                patch.object(
                    FakeAsyncRedisConnection, 'connect',
                    side_effect=RedisConnectionError('Connection refused'),
                ),
                patch(
                    'scrape_exchange.redis_client.asyncio.sleep',
                    side_effect=asyncio.CancelledError,
                ),self.assertRaises(asyncio.CancelledError)
            ):
                await client.ping()
            self.assertTrue(await asyncio.wait_for(client.ping(), timeout=1))
        finally:
            await client.aclose()
            await client.connection_pool.disconnect()


class TestRedisConnectRetrySettings(unittest.TestCase):
    def test_environment_overrides(self) -> None:
        with patch.dict(os.environ, {
            'REDIS_CONNECT_RETRY_BASE_SECONDS': '0.5',
            'REDIS_CONNECT_RETRY_MAX_SECONDS': '3',
        }, clear=True):
            settings: RedisConnectRetrySettings = RedisConnectRetrySettings()
        self.assertEqual(settings.base_seconds, 0.5)
        self.assertEqual(settings.max_seconds, 3)

    def test_rejects_unbounded_or_invalid_backoff(self) -> None:
        from pydantic import ValidationError

        name: str
        values: tuple[str, ...]
        value: str
        for name, values in (
            ('BASE', ('0', '-1', 'nan', 'inf', 'invalid')),
            ('MAX', ('-1', 'nan', 'inf', 'invalid')),
        ):
            for value in values:
                with (
                    self.subTest(name=name, value=value),
                    patch.dict(os.environ, {
                        f'REDIS_CONNECT_RETRY_{name}_SECONDS': value,
                    }, clear=True),
                    self.assertRaises(ValidationError),
                ):
                    RedisConnectRetrySettings()


class TestWatchdogPacedBackoff(unittest.IsolatedAsyncioTestCase):
    '''Reconnect backoff must pulse the watchdog work signal, or the
    180s work-signal timeout kills a healthy-but-reconnecting scraper
    (2026-09-26 watchdog terminations).'''

    async def test_long_backoff_touches_work_per_slice(self) -> None:
        from redis.exceptions import ConnectionError as RedisConnectionError
        from scrape_exchange.redis_client import _WATCHDOG_TOUCH_SLICE_SECONDS

        attempts: int = 0
        sleeps: list[float] = []
        touches: int = 0

        async def operation() -> str:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RedisConnectionError('Connection refused')
            return 'ok'

        async def sleep(delay: float) -> None:
            sleeps.append(delay)
            # Simulate the watchdog firing mid-slice if the signal
            # goes stale: a 256s single sleep would breach the 180s
            # timeout, slices of <=30s must not.
            self.assertLessEqual(
                delay, _WATCHDOG_TOUCH_SLICE_SECONDS,
            )

        def touch_work() -> None:
            nonlocal touches
            touches += 1

        from scrape_exchange.redis_client import RedisConnectRetrySettings
        result: str = await call_with_redis_connect_retry(
            operation, sleep=sleep, touch_work=touch_work,
            settings=RedisConnectRetrySettings(
                base_seconds=256.0, max_seconds=256.0,
            ),
        )

        self.assertEqual(result, 'ok')
        # 256s delay paced as 30s slices: the signal never goes
        # stale, and every slice boundary pulses it.
        self.assertEqual(sleeps, [30.0] * 8 + [16.0])
        self.assertEqual(touches, len(sleeps) + 1)

    async def test_backoff_walk_slices_long_delays(self) -> None:
        from redis.exceptions import TimeoutError as RedisTimeoutError

        sleeps: list[float] = []
        touches: int = 0

        async def operation() -> str:
            raise RedisTimeoutError('timed out')

        async def sleep(delay: float) -> None:
            sleeps.append(delay)

        def touch_work() -> None:
            nonlocal touches
            touches += 1

        with self.assertRaises(RedisTimeoutError):
            await call_with_redis_connect_retry(
                operation, sleep=sleep, touch_work=touch_work,
            )

        # The full 1..256 walk, every slice <= 30s.
        self.assertEqual(sum(sleeps), 511.0)
        self.assertTrue(all(s <= 30.0 for s in sleeps))
        self.assertGreaterEqual(touches, len(sleeps))

    def test_default_touch_resolves_installed_watchdog(self) -> None:
        from scrape_exchange.redis_client import _default_touch_work
        from scrape_exchange.watchdog import Watchdog

        watchdog: Watchdog = Watchdog(
            loop_timeout=0.0, work_timeout=0.0,
        )
        try:
            Watchdog.set_instance(watchdog)
            self.assertEqual(_default_touch_work(), watchdog.touch_work)
        finally:
            Watchdog.reset()


class TestBacklogRedisFromUrl(unittest.TestCase):
    '''The backlog-server client is built for a server that restarts:
    bounded connect and read timeouts, a health check on idle
    connections, and a command retry that reconnects.'''

    @staticmethod
    def _kwargs(url: str = 'redis://localhost:6390/0', **extra: object):
        client = backlog_redis_from_url(
            url, component='backlog-test', **extra,
        )
        return client.connection_pool.connection_kwargs

    def test_defaults_bound_connect_and_read(self) -> None:
        kwargs = self._kwargs()
        self.assertEqual(
            kwargs['socket_connect_timeout'],
            DEFAULT_BACKLOG_CONNECT_TIMEOUT_SECONDS,
        )
        self.assertEqual(
            kwargs['socket_timeout'],
            DEFAULT_BACKLOG_SOCKET_TIMEOUT_SECONDS,
        )
        self.assertEqual(
            kwargs['health_check_interval'],
            DEFAULT_BACKLOG_HEALTH_CHECK_INTERVAL_SECONDS,
        )

    def test_command_retry_covers_connection_and_timeout(self) -> None:
        from redis.exceptions import ConnectionError as RedisConnectionError
        from redis.exceptions import TimeoutError as RedisTimeoutError

        retry = self._kwargs()['retry']
        self.assertEqual(
            retry.get_retries(), DEFAULT_BACKLOG_COMMAND_RETRIES,
        )
        supported = set(retry._supported_errors)
        self.assertIn(RedisConnectionError, supported)
        self.assertIn(RedisTimeoutError, supported)
        # The shared Redis client must keep its connect-only rule.
        self.assertIsNone(
            redis_from_url(
                'redis://localhost:6379/0', component='shared',
            ).connection_pool.connection_kwargs.get('retry'),
        )

    def test_slow_reply_is_bounded_on_the_backlog_client_only(
        self,
    ) -> None:
        '''socket_timeout=None is deliberate on the shared client (the
        creator map's HGETALL outlives any timeout) and must not leak
        to the backlog client.'''
        self.assertIsNone(
            redis_from_url(
                'redis://localhost:6379/0', component='shared',
            ).connection_pool.connection_kwargs['socket_timeout'],
        )
        self.assertIsNotNone(self._kwargs()['socket_timeout'])

    def test_dsn_query_string_wins(self) -> None:
        kwargs = self._kwargs(
            'redis://localhost:6390/0?socket_timeout=7'
            '&health_check_interval=5',
        )
        self.assertEqual(kwargs['socket_timeout'], 7.0)
        self.assertEqual(kwargs['health_check_interval'], 5)

    def test_environment_overrides(self) -> None:
        with patch.dict(
            os.environ,
            {
                'BACKLOG_REDIS_COMMAND_RETRIES': '2',
                'BACKLOG_REDIS_CONNECT_TIMEOUT_SECONDS': '1.5',
                'BACKLOG_REDIS_SOCKET_TIMEOUT_SECONDS': '9',
                'BACKLOG_REDIS_HEALTH_CHECK_INTERVAL_SECONDS': '0',
                'BACKLOG_REDIS_RETRY_CAP_SECONDS': '4',
            },
            clear=True,
        ):
            kwargs = self._kwargs()
        self.assertEqual(kwargs['retry'].get_retries(), 2)
        self.assertEqual(kwargs['socket_connect_timeout'], 1.5)
        self.assertEqual(kwargs['socket_timeout'], 9.0)
        self.assertEqual(kwargs['health_check_interval'], 0)
        self.assertEqual(
            kwargs['retry']._backoff._cap, 4.0,
        )

    def test_backoff_worst_case_stays_inside_watchdog_window(
        self,
    ) -> None:
        '''A command retry sleeps without pulsing the watchdog, so the
        whole retry budget has to fit well inside the 180s work-signal
        timeout.'''
        retry = backlog_command_retry()
        backoff = retry._backoff
        # Un-jittered upper bound of every sleep the retry can make.
        worst: float = 0.0
        for attempt in range(1, retry.get_retries() + 1):
            worst += min(
                backoff._cap, backoff._base * 2 ** attempt,
            )
        self.assertEqual(worst, 60.0)
        self.assertLess(worst, 120.0)
        self.assertGreater(backoff.compute(1), 0.0)

    def test_backlog_pool_has_no_stacked_connect_retry(self) -> None:
        '''The command retry already reconnects; the shared pool's
        connect retry on top of it multiplied the attempts.'''
        import redis.asyncio as aioredis

        shared_pool: Any = redis_from_url(
            'redis://localhost:6379/0', component='shared',
        ).connection_pool
        backlog_pool: Any = backlog_redis_from_url(
            'redis://localhost:6390/0', component='backlog-test',
        ).connection_pool
        self.assertEqual(
            type(shared_pool).__name__, 'ConnectRetryPool',
        )
        self.assertIs(
            type(backlog_pool), aioredis.BlockingConnectionPool,
        )

    def test_pool_cap_defaults_to_the_shared_client_cap(self) -> None:
        '''The backlog client stays at the shared client's 4 until a
        process is known to hold one pool: one pool per event loop
        multiplies whatever this number is.'''
        client = backlog_redis_from_url(
            'redis://localhost:6390/0', component='backlog-test',
        )
        self.assertEqual(
            client.connection_pool.max_connections,
            DEFAULT_BACKLOG_MAX_CONNECTIONS,
        )
        self.assertEqual(
            DEFAULT_BACKLOG_MAX_CONNECTIONS, DEFAULT_MAX_CONNECTIONS,
        )
        shared = redis_from_url(
            'redis://localhost:6379/0', component='shared-test',
        )
        self.assertEqual(
            shared.connection_pool.max_connections,
            DEFAULT_MAX_CONNECTIONS,
        )

    def test_pool_cap_from_environment(self) -> None:
        with patch.dict(
            os.environ,
            {'BACKLOG_REDIS_MAX_CONNECTIONS': '48'},
            clear=True,
        ):
            client = backlog_redis_from_url(
                'redis://localhost:6390/0', component='backlog-test',
            )
        self.assertEqual(client.connection_pool.max_connections, 48)

    def test_rejects_invalid_tuning(self) -> None:
        from pydantic import ValidationError

        with self.assertRaises(ValidationError):
            BacklogRedisSettings(connect_timeout_seconds=0)
        with self.assertRaises(ValidationError):
            BacklogRedisSettings(command_retries=-1)
        with self.assertRaises(ValidationError):
            BacklogRedisSettings(retry_base_seconds=0)
        with self.assertRaises(ValidationError):
            BacklogRedisSettings(max_connections=0)


class TestBacklogConnectAttemptsAreBounded(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_unreachable_server_fails_after_one_retry_budget(
        self,
    ) -> None:
        '''With the server refusing connections, a command must fail
        after command_retries + 1 connect attempts, not that many per
        step of the pool's 1s-to-256s connect retry (60 attempts and
        15+ minutes before the fix).'''
        import redis.asyncio.connection as connection_module
        from redis.exceptions import ConnectionError as RedisConnectionError

        attempts: list[int] = []

        async def refuse(self: Any) -> None:
            attempts.append(1)
            raise ConnectionRefusedError('refused')

        client: Any = backlog_redis_from_url(
            'redis://localhost:6390/0', component='backlog-test',
        )
        with (
            patch.object(connection_module.Connection, '_connect', refuse),
            patch('redis.asyncio.retry.sleep', new_callable=AsyncMock),
            patch(
                'scrape_exchange.redis_client._paced_sleep',
                new_callable=AsyncMock,
            ) as paced,
            self.assertRaises(RedisConnectionError),
        ):
            await client.execute_command('BACKLOG.INFO', 'q')
        self.assertEqual(
            len(attempts), DEFAULT_BACKLOG_COMMAND_RETRIES + 1,
        )
        paced.assert_not_awaited()
