'''Unit tests for shared Redis client construction.'''

import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.redis_client import (
    DEFAULT_MAX_CONNECTIONS,
    call_with_redis_busy_retry,
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
