'''Unit tests for scrape_exchange.async_scope.'''

import asyncio
import unittest
from typing import Any
from unittest import mock

from scrape_exchange import video_queue_factory as vqf
from scrape_exchange.async_scope import run_async


class TestRunAsync(unittest.TestCase):

    def setUp(self) -> None:
        # The client cache is process-wide; every test starts empty so
        # a client left by another test cannot satisfy this one.
        patched: Any = mock.patch.object(vqf, '_BY_LOOP', {})
        patched.start()
        self.addCleanup(patched.stop)

    @staticmethod
    def _register(client: Any) -> None:
        loop: Any = asyncio.get_running_loop()
        vqf._BY_LOOP[loop] = {'redis://backlog': client}

    def test_returns_the_body_value(self) -> None:
        async def body() -> int:
            return 7

        self.assertEqual(run_async(body()), 7)

    def test_closes_the_running_loops_clients(self) -> None:
        closed: list[str] = []

        async def body() -> None:
            client: Any = mock.MagicMock()

            async def aclose() -> None:
                closed.append('closed')

            client.aclose = aclose
            TestRunAsync._register(client)

        run_async(body())
        self.assertEqual(closed, ['closed'])
        self.assertEqual(vqf._BY_LOOP, {})

    def test_closes_clients_when_the_body_raises(self) -> None:
        closed: list[str] = []

        async def body() -> None:
            client: Any = mock.MagicMock()

            async def aclose() -> None:
                closed.append('closed')

            client.aclose = aclose
            TestRunAsync._register(client)
            raise RuntimeError('boom')

        with self.assertRaises(RuntimeError):
            run_async(body())
        self.assertEqual(closed, ['closed'])

    def test_a_failing_close_does_not_mask_the_result(self) -> None:
        async def body() -> str:
            client: Any = mock.MagicMock()

            async def aclose() -> None:
                raise OSError('already gone')

            client.aclose = aclose
            TestRunAsync._register(client)
            return 'ok'

        with self.assertLogs(level='WARNING'):
            self.assertEqual(run_async(body()), 'ok')

    def test_close_without_a_loop_drops_the_sync_cache(self) -> None:
        '''Called with no running loop the clients cannot be awaited,
        so the reference is dropped instead of kept forever.'''
        vqf._NO_LOOP['redis://x'] = mock.MagicMock()
        coro: Any = vqf.close_backlog_clients()
        with self.assertRaises(StopIteration):
            # Drives the coroutine with no loop: the no-loop branch
            # returns before its first await.
            coro.send(None)
        self.assertEqual(vqf._NO_LOOP, {})
