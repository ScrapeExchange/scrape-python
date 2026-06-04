'''Unit tests for shared Redis client construction.'''

import os
import unittest
from unittest.mock import MagicMock, patch

from scrape_exchange.redis_client import (
    DEFAULT_MAX_CONNECTIONS,
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
            patch('redis.asyncio.Redis') as redis_cls,
            patch.dict(os.environ, {}, clear=True),
        ):
            pool = MagicMock()
            from_url.return_value = pool
            redis_cls.return_value = MagicMock()
            redis_from_url(
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
        redis_cls.assert_called_once_with(connection_pool=pool)

    def test_env_overrides_default_pool_cap(self) -> None:
        with (
            patch(
                'redis.asyncio.BlockingConnectionPool.from_url',
            ) as from_url,
            patch('redis.asyncio.Redis') as redis_cls,
            patch.dict(
                os.environ, {'REDIS_MAX_CONNECTIONS': '9'},
                clear=True,
            ),
        ):
            from_url.return_value = MagicMock()
            redis_cls.return_value = MagicMock()
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
            patch('redis.asyncio.Redis') as redis_cls,
            patch.dict(
                os.environ, {'REDIS_MAX_CONNECTIONS': '9'},
                clear=True,
            ),
        ):
            from_url.return_value = MagicMock()
            redis_cls.return_value = MagicMock()
            redis_from_url(
                'redis://localhost:6379/0',
                component='test-component',
                max_connections=1,
            )

        self.assertEqual(
            from_url.call_args.kwargs['max_connections'],
            1,
        )
