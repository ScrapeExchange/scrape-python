'''
Unit tests for ``tools.tt_video_scrape``.
'''

import asyncio
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from redis import exceptions as redis_exceptions

from scrape_exchange.file_management import AssetFileManagement
from scrape_exchange.video_scrape_queue import VideoState
from tools import tt_video_scrape as tool


_COLLATERAL: Path = Path(__file__).parent.parent / 'collateral'


def _payload() -> dict:
    return json.loads(
        (_COLLATERAL / 'tiktok' / 'video_info_sample.json')
        .read_text(),
    )


class _FakeSessionCtx:
    def __init__(self, api: object) -> None:
        self._api: object = api

    async def __aenter__(self) -> object:
        return self._api

    async def __aexit__(self, *exc: object) -> bool:
        return False


def _settings(**overrides: object) -> MagicMock:
    settings: MagicMock = MagicMock()
    settings.video_data_directory = overrides.get(
        'video_data_directory', '/tmp/videos',
    )
    settings.redis_dsn = overrides.get(
        'redis_dsn', 'redis://h:6379/0',
    )
    settings.video_transient_max_attempts = overrides.get(
        'video_transient_max_attempts', 3,
    )
    settings.video_transient_backoff_seconds = overrides.get(
        'video_transient_backoff_seconds', 0,
    )
    settings.video_concurrency = overrides.get('video_concurrency', 0)
    settings.video_queue_batch = overrides.get('video_queue_batch', 50)
    settings.video_queue_idle_poll_seconds = overrides.get(
        'video_queue_idle_poll_seconds', 2.0,
    )
    return settings


class TestVideoSettings(unittest.TestCase):

    def test_generic_log_flags_feed_video_logging(self) -> None:
        settings = tool.VideoSettings(
            _env_file=None,
            _cli_parse_args=[
                '--log-file', '/dev/stdout',
                '--log-level', 'debug',
            ],
        )
        self.assertEqual(settings.video_log_file, '/dev/stdout')
        self.assertEqual(settings.video_log_level, 'DEBUG')

    def test_typo_concurrency_env_alias_is_accepted(self) -> None:
        with patch.dict(
            os.environ,
            {'TIKTOP_VIDEO_CONCURRENCY': '4'},
            clear=True,
        ):
            settings = tool.VideoSettings(
                _env_file=None,
                _cli_parse_args=[],
            )
        self.assertEqual(settings.video_concurrency, 4)


class TestMain(unittest.TestCase):

    def test_explicit_concurrency_is_fleet_wide_cap(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            proxy_file: str = os.path.join(d, 'proxies.txt')
            with open(proxy_file, 'w') as f:
                f.write('\n'.join(
                    f'http://p{i}:8080' for i in range(1, 11)
                ))
            argv: list[str] = [
                'tt_video_scrape.py',
                '--redis-dsn', 'redis://h:6379/0',
                '--tiktok-video-data-dir', d,
            ]
            env: dict[str, str] = {
                'PROXY_FILES': proxy_file,
                'TIKTOK_VIDEO_NUM_PROCESSES': '4',
                'TIKTOK_VIDEO_CONCURRENCY': '5',
            }
            sampled: list[str] = [
                'http://p9:8080', 'http://p4:8080',
                'http://p7:8080', 'http://p2:8080',
                'http://p10:8080',
            ]
            with patch.dict(os.environ, env, clear=True), patch.object(
                tool.sys, 'argv', argv,
            ), patch.object(
                tool, 'random_proxy_subset', return_value=sampled,
            ) as proxy_subset, patch.object(
                tool, 'ScraperRunner',
            ) as runner_cls:
                runner_cls.return_value.run_sync.return_value = 0
                with self.assertRaises(SystemExit):
                    tool.main()

            proxy_subset.assert_called_once()
            self.assertEqual(
                runner_cls.call_args.kwargs['settings'].proxies,
                sampled,
            )
            self.assertEqual(
                runner_cls.call_args.kwargs['num_processes'],
                4,
            )
            self.assertEqual(
                runner_cls.call_args.kwargs['concurrency'],
                5,
            )
            self.assertEqual(
                runner_cls.call_args.kwargs['child_concurrencies'],
                [2, 1, 1, 1],
            )
            self.assertEqual(
                runner_cls.call_args.kwargs['concurrency_env_var'],
                'TIKTOK_VIDEO_CONCURRENCY',
            )
            self.assertTrue(
                runner_cls.call_args.kwargs['split_proxy_pool'],
            )


class TestValidateSettings(unittest.TestCase):

    def test_missing_redis_exits(self) -> None:
        with self.assertRaises(SystemExit):
            tool._validate_settings(_settings(redis_dsn=''))

    def test_missing_data_dir_exits(self) -> None:
        with self.assertRaises(SystemExit):
            tool._validate_settings(
                _settings(video_data_directory=''),
            )

    def test_valid_creates_missing_dir(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            data_dir: str = os.path.join(d, 'data')
            tool._validate_settings(
                _settings(video_data_directory=data_dir),
            )
            self.assertTrue(os.path.isdir(data_dir))


class TestRunWorkerTopology(unittest.IsolatedAsyncioTestCase):

    async def test_video_concurrency_caps_bootstrapped_proxies(
        self,
    ) -> None:
        proxies: list[str] = [
            'http://p1:8080', 'http://p2:8080', 'http://p3:8080',
        ]
        pool: MagicMock = MagicMock()
        pool.bootstrap = AsyncMock()
        pool.shutdown = AsyncMock()
        pool.run_refresh_loop = AsyncMock()
        pool.ready_proxies = MagicMock(return_value=proxies[:2])

        queue: MagicMock = MagicMock()
        queue._redis.aclose = AsyncMock()

        ctx: MagicMock = MagicMock()
        ctx.settings = _settings(video_concurrency=2)
        ctx.settings.ms_token_ttl_seconds = 14400
        ctx.settings.ms_token_refresh_fraction = 0.75
        ctx.settings.ms_token_refresh_interval_seconds = 300
        ctx.settings.session_bootstrap_timeout_ms = 120000
        ctx.proxies = proxies
        ctx.rate_limiter = MagicMock()

        async def _noop_loop(*a: object) -> None:
            await asyncio.sleep(0)

        with patch.object(
            tool, 'TikTokSessionPool', return_value=pool,
        ) as pool_cls, patch.object(
            tool, '_build_queue', return_value=queue,
        ), patch.object(
            tool, '_queue_driven_loop', _noop_loop,
        ), patch.object(
            tool, 'AssetFileManagement',
        ):
            await tool._run_worker(ctx)

        self.assertEqual(
            pool_cls.call_args.kwargs['proxies'],
            proxies[:2],
        )
        pool.bootstrap.assert_awaited_once()
        pool.shutdown.assert_awaited_once()
        queue._redis.aclose.assert_awaited_once()

    async def test_no_configured_proxies_uses_direct_session(
        self,
    ) -> None:
        pool: MagicMock = MagicMock()
        pool.bootstrap = AsyncMock()
        pool.shutdown = AsyncMock()
        pool.run_refresh_loop = AsyncMock()
        pool.ready_proxies = MagicMock(
            return_value=[tool.DIRECT_SESSION_PROXY],
        )

        queue: MagicMock = MagicMock()
        queue._redis.aclose = AsyncMock()

        ctx: MagicMock = MagicMock()
        ctx.settings = _settings(video_concurrency=0)
        ctx.settings.ms_token_ttl_seconds = 14400
        ctx.settings.ms_token_refresh_fraction = 0.75
        ctx.settings.ms_token_refresh_interval_seconds = 300
        ctx.settings.session_bootstrap_timeout_ms = 120000
        ctx.proxies = []
        ctx.rate_limiter = MagicMock()

        loop_calls: list[tuple[list[str], int]] = []

        async def _capture_loop(
            _queue: object,
            _settings: object,
            _pool: object,
            proxies: list[str],
            _fm: object,
            concurrency: int,
        ) -> None:
            loop_calls.append((proxies, concurrency))
            await asyncio.sleep(0)

        with patch.object(
            tool, 'TikTokSessionPool', return_value=pool,
        ) as pool_cls, patch.object(
            tool, '_build_queue', return_value=queue,
        ), patch.object(
            tool, '_queue_driven_loop', _capture_loop,
        ), patch.object(
            tool, 'AssetFileManagement',
        ):
            await tool._run_worker(ctx)

        self.assertEqual(pool_cls.call_args.kwargs['proxies'], [])
        self.assertEqual(
            loop_calls, [([tool.DIRECT_SESSION_PROXY], 1)],
        )
        pool.bootstrap.assert_awaited_once()
        pool.shutdown.assert_awaited_once()
        queue._redis.aclose.assert_awaited_once()


class TestRedisRetry(unittest.IsolatedAsyncioTestCase):

    async def test_backoff_retries_connection_errors_and_caps_sleep(
        self,
    ) -> None:
        calls: int = 0
        sleeps: list[float] = []

        async def _operation() -> str:
            nonlocal calls
            calls += 1
            if calls <= 8:
                raise redis_exceptions.ConnectionError('redis down')
            return 'ok'

        async def _sleep(seconds: float) -> None:
            sleeps.append(seconds)

        with patch.object(tool.asyncio, 'sleep', _sleep):
            result: str = await tool._redis_operation_with_backoff(
                'test operation',
                _operation,
            )

        self.assertEqual(result, 'ok')
        self.assertEqual(calls, 9)
        self.assertEqual(
            sleeps,
            [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 60.0, 60.0],
        )

    async def test_queue_loop_retries_redis_pop_connection_error(
        self,
    ) -> None:
        queue: MagicMock = MagicMock()
        queue.pop = AsyncMock(side_effect=[
            redis_exceptions.ConnectionError('redis down'),
            asyncio.CancelledError(),
        ])
        settings: MagicMock = _settings(video_queue_batch=5)
        settings.video_queue_idle_poll_seconds = 0
        pool: MagicMock = MagicMock()
        fm: MagicMock = MagicMock()
        sleeps: list[float] = []

        async def _sleep(seconds: float) -> None:
            sleeps.append(seconds)

        with patch.object(tool.asyncio, 'sleep', _sleep):
            with self.assertRaises(asyncio.CancelledError):
                await tool._queue_driven_loop(
                    queue,
                    settings,
                    pool,
                    ['http://p:8080'],
                    fm,
                    1,
                )

        self.assertEqual(queue.pop.await_count, 2)
        self.assertEqual(sleeps, [1.0])


class TestPayloadFetch(unittest.IsolatedAsyncioTestCase):

    def test_video_filename(self) -> None:
        self.assertEqual(
            tool._video_filename('123'),
            'tiktok-video-123.json.br',
        )

    def test_video_id_from_ref_requires_url(self) -> None:
        self.assertEqual(
            tool._video_id_from_ref(
                'https://www.tiktok.com/@charli/video/'
                '7000000000000000001?lang=en',
            ),
            '7000000000000000001',
        )
        with self.assertRaises(ValueError):
            tool._video_id_from_ref('7000000000000000001')

    def test_unwraps_item_info_shape(self) -> None:
        payload: dict = _payload()
        self.assertIs(
            tool._unwrap_video_payload({
                'itemInfo': {'itemStruct': payload},
            }),
            payload,
        )

    async def test_fetch_video_payload_unwraps_response(self) -> None:
        api: MagicMock = MagicMock()
        video_obj: MagicMock = MagicMock()
        video_obj.info = AsyncMock(
            return_value={
                'itemInfo': {'itemStruct': _payload()},
            },
        )
        api.video = MagicMock(return_value=video_obj)
        payload: dict = await tool._fetch_video_payload(
            api,
            'https://www.tiktok.com/@charlidamelio/video/'
            '7000000000000000001',
        )
        self.assertEqual(payload['id'], '7000000000000000001')
        api.video.assert_called_once()

    async def test_fetch_video_payload_prefers_url_fetch(self) -> None:
        api: MagicMock = MagicMock()
        video_obj: MagicMock = MagicMock()
        video_obj.info = AsyncMock(
            return_value={
                'itemInfo': {'itemStruct': _payload()},
            },
        )
        api.video = MagicMock(return_value=video_obj)

        payload: dict = await tool._fetch_video_payload(
            api,
            'https://www.tiktok.com/@charlidamelio/video/'
            '7000000000000000001',
        )

        self.assertEqual(payload['id'], '7000000000000000001')
        api.video.assert_called_once_with(
            url=(
                'https://www.tiktok.com/@charlidamelio/video/'
                '7000000000000000001'
            ),
        )
        video_obj.info.assert_awaited_once()


class TestScrapeOneQueued(unittest.IsolatedAsyncioTestCase):

    async def test_success_writes_record_and_completes_queue(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as d:
            fm: AssetFileManagement = AssetFileManagement(
                d,
                prefix_rankings={'video': [tool.VIDEO_FILE_PREFIX]},
            )
            api: MagicMock = MagicMock()
            video_obj: MagicMock = MagicMock()
            video_obj.info = AsyncMock(
                return_value={
                    'itemInfo': {'itemStruct': _payload()},
                },
            )
            api.video = MagicMock(return_value=video_obj)
            pool: MagicMock = MagicMock()
            pool.session_for = MagicMock(
                return_value=_FakeSessionCtx(api),
            )
            queue: MagicMock = MagicMock()
            queue.complete = AsyncMock()
            queue.mark = AsyncMock()
            queue.bump_attempts = AsyncMock(return_value=1)
            await tool._scrape_one_queued(
                'https://www.tiktok.com/@charlidamelio/video/'
                '7000000000000000001',
                queue=queue,
                settings=_settings(),
                pool=pool,
                proxy='http://p:8080',
                fm=fm,
            )
            record: dict = await fm.read_file(
                tool._video_filename('7000000000000000001'),
            )
        self.assertEqual(record['video_id'], '7000000000000000001')
        self.assertEqual(record['username'], 'charlidamelio')
        queue.complete.assert_awaited_once_with(
            'https://www.tiktok.com/@charlidamelio/video/'
            '7000000000000000001',
        )
        queue.mark.assert_not_awaited()

    async def test_unavailable_marks_terminal_state(self) -> None:
        fm: MagicMock = MagicMock()
        pool: MagicMock = MagicMock()
        pool.session_for = MagicMock(
            return_value=_FakeSessionCtx(MagicMock()),
        )
        queue: MagicMock = MagicMock()
        queue.complete = AsyncMock()
        queue.mark = AsyncMock()
        queue.bump_attempts = AsyncMock()
        with patch.object(
            tool,
            '_fetch_video_payload',
            side_effect=RuntimeError('not found'),
        ), patch.object(
            tool, 'classify_tiktok_error', return_value='unavailable',
        ):
            await tool._scrape_one_queued(
                '7000000000000000001',
                queue=queue,
                settings=_settings(),
                pool=pool,
                proxy='http://p:8080',
                fm=fm,
            )
        queue.mark.assert_awaited_once_with(
            '7000000000000000001',
            state=VideoState.UNAVAILABLE,
            last_error='unavailable',
        )
        queue.complete.assert_not_awaited()

    async def test_transient_exhaustion_marks_failed(self) -> None:
        fm: MagicMock = MagicMock()
        pool: MagicMock = MagicMock()
        pool.session_for = MagicMock(
            return_value=_FakeSessionCtx(MagicMock()),
        )
        queue: MagicMock = MagicMock()
        queue.complete = AsyncMock()
        queue.mark = AsyncMock()
        queue.bump_attempts = AsyncMock(return_value=1)
        with patch.object(
            tool,
            '_fetch_video_payload',
            side_effect=RuntimeError('timeout'),
        ), patch.object(
            tool, 'classify_tiktok_error', return_value='transient',
        ):
            await tool._scrape_one_queued(
                '7000000000000000001',
                queue=queue,
                settings=_settings(video_transient_max_attempts=2),
                pool=pool,
                proxy='http://p:8080',
                fm=fm,
            )
        self.assertEqual(queue.bump_attempts.await_count, 2)
        queue.mark.assert_awaited_once()
        self.assertEqual(
            queue.mark.call_args.kwargs['state'],
            VideoState.FAILED,
        )


if __name__ == '__main__':
    unittest.main()
