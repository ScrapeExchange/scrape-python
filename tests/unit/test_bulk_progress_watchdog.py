'''The bulk-progress WebSocket wait must keep ticking the liveness
watchdog even when the server sends no progress frame for a long time,
so the upload tool is not killed mid-job. The per-recv wait is capped to
a small interval and the loop touches between waits, while the overall
job deadline still bounds the total wait.
'''

import asyncio
import unittest
from unittest.mock import MagicMock, patch

from scrape_exchange.watchdog import Watchdog


class _WSContextManager:
    def __init__(self, ws: object) -> None:
        self._ws = ws

    async def __aenter__(self) -> object:
        return self._ws

    async def __aexit__(self, *_args: object) -> bool:
        return False


class TestBulkProgressChunkTouch(
    unittest.IsolatedAsyncioTestCase,
):
    def setUp(self) -> None:
        self.wd = Watchdog(loop_timeout=0.0, work_timeout=0.0)
        self.wd.touch_work = MagicMock()
        Watchdog.set_instance(self.wd)

    def tearDown(self) -> None:
        Watchdog.reset()

    async def test_recv_gap_keeps_touching_until_deadline(
        self,
    ) -> None:
        from scrape_exchange import bulk_upload

        async def _hang() -> str:
            await asyncio.sleep(3600)
            return ''

        fake_ws = MagicMock()
        fake_ws.recv = _hang
        client = MagicMock()
        client.headers = {'Authorization': 'Bearer x'}

        with patch.object(
            bulk_upload.websockets, 'connect',
            return_value=_WSContextManager(fake_ws),
        ), patch.object(
            bulk_upload, '_WS_RECV_TOUCH_INTERVAL', 0.01,
            create=True,
        ):
            result = await bulk_upload.stream_bulk_job_progress(
                'job1',
                'https://api.scrape.exchange',
                client,
                timeout_seconds=0.08,
            )

        # No terminal status arrived; the overall deadline bounds it.
        self.assertFalse(result)
        # Touched many times (≈deadline/interval), not just once.
        self.assertGreaterEqual(self.wd.touch_work.call_count, 3)


if __name__ == '__main__':
    unittest.main()
