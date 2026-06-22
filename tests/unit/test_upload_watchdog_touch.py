'''The upload tools run under ScraperRunner, so the liveness watchdog
terminates them after WATCHDOG_WORK_TIMEOUT_SECONDS unless their work
loops touch the watchdog work signal. These tests guard that the upload
loops call Watchdog.get().touch_work() while operating.
'''

import asyncio
import contextlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.watchdog import Watchdog


class _WatchdogProbe(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.wd = Watchdog(loop_timeout=0.0, work_timeout=0.0)
        self.wd.touch_work = MagicMock()
        Watchdog.set_instance(self.wd)

    def tearDown(self) -> None:
        Watchdog.reset()


class TestVideoUploadWorkerTouches(_WatchdogProbe):
    async def test_upload_worker_touches_work_per_file(self) -> None:
        from tools.yt_video_upload import _upload_worker
        queue: asyncio.Queue = asyncio.Queue()
        await queue.put('video-min-x.json.br')
        with patch(
            'tools.yt_video_upload._process_upload_file',
            new=AsyncMock(),
        ) as proc:
            task = asyncio.create_task(
                _upload_worker(
                    None, queue, MagicMock(), MagicMock(),
                    MagicMock(), MagicMock(), MagicMock(),
                    MagicMock(),
                )
            )
            await queue.join()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        proc.assert_awaited_once()
        self.wd.touch_work.assert_called()

    async def test_idle_video_watcher_yields_and_touches_work(
        self,
    ) -> None:
        from tools.yt_video_upload import _watch_and_upload

        captured_kwargs = {}

        async def idle_watcher(*_args, **kwargs):
            captured_kwargs.update(kwargs)
            yield set()
            await asyncio.sleep(3600)

        queue: asyncio.Queue = asyncio.Queue()
        settings = MagicMock()
        settings.video_data_directory = '/tmp'
        with patch(
            'tools.yt_video_upload.awatch', idle_watcher,
        ):
            task = asyncio.create_task(
                _watch_and_upload(queue, MagicMock(), settings),
            )
            await asyncio.sleep(0.01)
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        self.assertFalse(captured_kwargs['recursive'])
        self.assertTrue(captured_kwargs['yield_on_timeout'])
        self.wd.touch_work.assert_called()


class TestChannelBulkLoopTouches(_WatchdogProbe):
    async def test_bulk_loop_touches_work_each_iteration(
        self,
    ) -> None:
        from tools.yt_channel_upload import _unified_bulk_upload_loop
        settings = MagicMock()
        settings.bulk_batch_size = 10
        settings.max_active_bulk_jobs = 1
        fm = MagicMock()
        fm.list_base.return_value = []
        with patch(
            'tools.yt_channel_upload._wait_for_channel_changes',
            new=AsyncMock(side_effect=asyncio.CancelledError),
        ):
            with self.assertRaises(asyncio.CancelledError):
                await _unified_bulk_upload_loop(
                    settings, MagicMock(), fm, MagicMock(),
                )
        self.wd.touch_work.assert_called()

    async def test_idle_channel_watcher_keeps_touching_work(
        self,
    ) -> None:
        from tools.yt_channel_upload import _wait_for_channel_changes

        async def never_changes(*_args, **_kwargs):
            while True:
                await asyncio.sleep(3600)
                yield set()

        with patch(
            'tools.yt_channel_upload.awatch',
            never_changes,
        ), patch(
            'tools.yt_channel_upload._WATCHDOG_TOUCH_INTERVAL',
            0.01,
        ):
            task = asyncio.create_task(
                _wait_for_channel_changes(Path('/tmp')),
            )
            await asyncio.sleep(0.03)
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        self.wd.touch_work.assert_called()

    async def test_bulk_drain_touches_work_per_batch(self) -> None:
        from tools.yt_channel_upload import upload_channels
        settings = MagicMock()
        settings.bulk_batch_size = 10
        settings.bulk_max_batch_bytes = 10 ** 9
        fm = MagicMock()
        fm.list_base.return_value = ['channel-x.json.br']
        creator_map = MagicMock()
        creator_map.redis_client = None
        with patch(
            'tools.yt_channel_upload._upload_concurrency',
            return_value=1,
        ), patch(
            'tools.yt_channel_upload._prepare_channel_line',
            new=AsyncMock(return_value=None),
        ):
            await upload_channels(
                settings, MagicMock(), fm, creator_map,
                MagicMock(), MagicMock(),
            )
        self.wd.touch_work.assert_called()

    async def test_channel_batch_scan_and_decode_run_off_loop(
        self,
    ) -> None:
        import brotli
        import orjson
        from tools.yt_channel_upload import _unified_bulk_upload_loop

        with tempfile.TemporaryDirectory() as tmp:
            base_dir = Path(tmp)
            path = base_dir / 'channel-UCtest.json.br'
            path.write_bytes(brotli.compress(orjson.dumps({
                'channel_id': 'UCtest',
            })))

            fm = MagicMock()
            fm.base_dir = base_dir
            fm.list_base = MagicMock(side_effect=[[path.name], []])
            validator = MagicMock()
            validator.validate.return_value = 'invalid'
            fm.mark_invalid = AsyncMock()

            settings = MagicMock()
            settings.bulk_batch_size = 100
            settings.max_active_bulk_jobs = 1

            class _StopLoop(Exception):
                pass

            async def stop_loop(*_args, **_kwargs):
                raise _StopLoop()

            original_to_thread = asyncio.to_thread
            calls = []

            async def recording_to_thread(func, *args, **kwargs):
                calls.append(func)
                return await original_to_thread(func, *args, **kwargs)

            with patch(
                'tools.yt_channel_upload.asyncio.to_thread',
                recording_to_thread,
            ), patch(
                'tools.yt_channel_upload._wait_for_channel_changes',
                stop_loop,
            ):
                with self.assertRaises(_StopLoop):
                    await _unified_bulk_upload_loop(
                        settings, MagicMock(), fm, validator,
                    )

            self.assertGreaterEqual(len(calls), 2)
            self.wd.touch_work.assert_called()


if __name__ == '__main__':
    unittest.main()
