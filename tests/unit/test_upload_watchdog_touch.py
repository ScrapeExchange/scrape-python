'''The upload tools run under ScraperRunner, so the liveness watchdog
terminates them after WATCHDOG_WORK_TIMEOUT_SECONDS unless their work
loops touch the watchdog work signal. These tests guard that the upload
loops call Watchdog.get().touch_work() while operating.
'''

import asyncio
import contextlib
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


if __name__ == '__main__':
    unittest.main()
