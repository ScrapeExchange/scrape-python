'''Unit tests for tools.yt_video_queue.'''

import unittest

from tools.yt_video_queue import main_async


class TestDispatcherStub(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_unknown_command_exits_nonzero(
        self,
    ) -> None:
        with self.assertRaises(SystemExit) as ctx:
            await main_async(['no-such-command'])
        self.assertNotEqual(ctx.exception.code, 0)

    async def test_empty_argv_exits_nonzero(
        self,
    ) -> None:
        with self.assertRaises(SystemExit) as ctx:
            await main_async([])
        self.assertNotEqual(ctx.exception.code, 0)


import argparse
from io import StringIO
from unittest.mock import AsyncMock, patch

from scrape_exchange.video_scrape_queue import (
    VideoState,
)


class TestCount(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_total(self) -> None:
        queue = AsyncMock()
        queue.count_by_state.return_value = {
            VideoState.QUEUED: 10,
            VideoState.FAILED: 2,
            VideoState.UNAVAILABLE: 1,
            VideoState.REMOVED: 0,
        }
        from tools.yt_video_queue import cmd_count
        ns = argparse.Namespace(state=None)
        out = StringIO()
        with patch('sys.stdout', out):
            rc: int = await cmd_count(ns, queue)
        self.assertEqual(rc, 0)
        self.assertIn('13', out.getvalue())

    async def test_by_state(self) -> None:
        queue = AsyncMock()
        queue.count_by_state.return_value = {
            VideoState.FAILED: 7,
        }
        from tools.yt_video_queue import cmd_count
        ns = argparse.Namespace(state='failed')
        out = StringIO()
        with patch('sys.stdout', out):
            rc: int = await cmd_count(ns, queue)
        self.assertEqual(rc, 0)
        self.assertIn('7', out.getvalue())


class TestStats(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_lists_all_states(self) -> None:
        queue = AsyncMock()
        queue.count_by_state.return_value = {
            s: i for i, s in enumerate(VideoState)
        }
        from tools.yt_video_queue import cmd_stats
        out = StringIO()
        with patch('sys.stdout', out):
            rc: int = await cmd_stats(
                argparse.Namespace(), queue,
            )
        self.assertEqual(rc, 0)
        for s in VideoState:
            self.assertIn(s.value, out.getvalue())


class TestShow(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_prints_meta(self) -> None:
        queue = AsyncMock()
        queue.get_meta.return_value = {
            'source': 'rss', 'state': 'queued',
        }
        from tools.yt_video_queue import cmd_show
        ns = argparse.Namespace(video_id='aaa')
        out = StringIO()
        with patch('sys.stdout', out):
            rc: int = await cmd_show(ns, queue)
        self.assertEqual(rc, 0)
        self.assertIn('rss', out.getvalue())

    async def test_missing_returns_1(self) -> None:
        queue = AsyncMock()
        queue.get_meta.return_value = {}
        from tools.yt_video_queue import cmd_show
        ns = argparse.Namespace(video_id='nope')
        rc: int = await cmd_show(ns, queue)
        self.assertEqual(rc, 1)


class TestAdd(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_enqueues_each_id(self) -> None:
        queue = AsyncMock()
        queue.enqueue.side_effect = [True, False]
        from tools.yt_video_queue import cmd_add
        ns = argparse.Namespace(
            video_ids=['aaaaaaaaaaa', 'bbbbbbbbbbb'],
            source='cli',
            file=None,
        )
        out = StringIO()
        stdin = StringIO('')
        stdin.isatty = lambda: True
        with patch('sys.stdout', out), patch('sys.stdin', stdin):
            rc: int = await cmd_add(ns, queue)
        self.assertEqual(rc, 0)
        self.assertEqual(
            queue.enqueue.await_count, 2,
        )
        self.assertIn(
            'add: processed=2 duplicates=1 added=1',
            out.getvalue(),
        )


class TestRemove(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_marks_removed(self) -> None:
        queue = AsyncMock()
        from tools.yt_video_queue import cmd_remove
        ns = argparse.Namespace(
            video_ids=['aaa'], note='op',
        )
        rc: int = await cmd_remove(ns, queue)
        self.assertEqual(rc, 0)
        queue.mark.assert_awaited_once()
        kwargs = queue.mark.await_args.kwargs
        self.assertEqual(
            kwargs['state'], VideoState.REMOVED,
        )


class TestRescrape(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_terminal_unmarks(self) -> None:
        queue = AsyncMock()
        queue.get_state.return_value = (
            VideoState.FAILED
        )
        from tools.yt_video_queue import cmd_rescrape
        ns = argparse.Namespace(video_ids=['aaa'])
        rc: int = await cmd_rescrape(ns, queue)
        self.assertEqual(rc, 0)
        queue.unmark.assert_awaited_once_with('aaa')

    async def test_queued_returns_ok(self) -> None:
        queue = AsyncMock()
        queue.get_state.return_value = (
            VideoState.QUEUED
        )
        from tools.yt_video_queue import cmd_rescrape
        ns = argparse.Namespace(video_ids=['aaa'])
        rc: int = await cmd_rescrape(ns, queue)
        self.assertEqual(rc, 0)
        queue.unmark.assert_not_called()


class TestMarkUnmark(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_mark_failed(self) -> None:
        queue = AsyncMock()
        from tools.yt_video_queue import cmd_mark
        ns = argparse.Namespace(
            video_id='aaa', state='failed',
            note='op',
        )
        rc: int = await cmd_mark(ns, queue)
        self.assertEqual(rc, 0)
        queue.mark.assert_awaited_once()
        kwargs = queue.mark.await_args.kwargs
        self.assertEqual(
            kwargs['state'], VideoState.FAILED,
        )

    async def test_unknown_state_returns_2(
        self,
    ) -> None:
        queue = AsyncMock()
        from tools.yt_video_queue import cmd_mark
        ns = argparse.Namespace(
            video_id='aaa', state='bogus',
            note=None,
        )
        rc: int = await cmd_mark(ns, queue)
        self.assertEqual(rc, 2)
        queue.mark.assert_not_called()

    async def test_unmark(self) -> None:
        queue = AsyncMock()
        from tools.yt_video_queue import cmd_unmark
        ns = argparse.Namespace(video_id='aaa')
        rc: int = await cmd_unmark(ns, queue)
        self.assertEqual(rc, 0)
        queue.unmark.assert_awaited_once_with('aaa')


import os
import shutil
import tempfile


class TestImport(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_enqueues_valid_ids_only(
        self,
    ) -> None:
        queue = AsyncMock()
        td = tempfile.mkdtemp()
        try:
            for name in [
                'dQw4w9WgXcQ', 'aBcDeFgHiJk',
            ]:
                with open(
                    os.path.join(td, name), 'w',
                ) as f:
                    f.write('')
            for name in [
                'video-min-dQw4w9WgXcQ.json.br',
                'short',
                'priority',
                '12345678901234567890',
            ]:
                with open(
                    os.path.join(td, name), 'w',
                ) as f:
                    f.write('')
            from tools.yt_video_queue import cmd_import
            ns = argparse.Namespace(directory=td)
            queue.enqueue.side_effect = [True, False]
            out = StringIO()
            with patch('sys.stdout', out):
                rc: int = await cmd_import(ns, queue)
            self.assertEqual(rc, 0)
            self.assertEqual(
                queue.enqueue.await_count, 2,
            )
            self.assertIn(
                'import: processed=2 duplicates=1 added=1',
                out.getvalue(),
            )
            remaining = set(os.listdir(td))
            self.assertNotIn(
                'dQw4w9WgXcQ', remaining,
            )
            self.assertNotIn(
                'aBcDeFgHiJk', remaining,
            )
            self.assertIn('short', remaining)
            self.assertIn(
                'video-min-dQw4w9WgXcQ.json.br',
                remaining,
            )
        finally:
            shutil.rmtree(td)


class TestIngestSentinels(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_sweeps_invalid_and_not_found(
        self,
    ) -> None:
        queue = AsyncMock()
        td = tempfile.mkdtemp()
        try:
            for name in [
                'video-min-aaaaaaaaaaa.json.br.invalid',
                'video-min-bbbbbbbbbbb.json.br.not_found',
                'video-min-ccccccccccc.json.br.unresolved',
                # Normal output file is left alone:
                'video-min-ddddddddddd.json.br',
            ]:
                with open(
                    os.path.join(td, name), 'w',
                ) as f:
                    f.write('')
            from tools.yt_video_queue import (
                cmd_ingest_sentinels,
            )
            ns = argparse.Namespace(directory=td)
            rc: int = await cmd_ingest_sentinels(
                ns, queue,
            )
            self.assertEqual(rc, 0)
            self.assertEqual(
                queue.mark.await_count, 3,
            )
            for call in queue.mark.await_args_list:
                self.assertEqual(
                    call.kwargs['state'],
                    VideoState.FAILED,
                )
            remaining = set(os.listdir(td))
            self.assertIn(
                'video-min-ddddddddddd.json.br',
                remaining,
            )
            self.assertEqual(len(remaining), 1)
        finally:
            shutil.rmtree(td)


class TestSearch(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_default_fields(self) -> None:
        queue = AsyncMock()
        queue.search_meta.return_value = [
            'aaa', 'bbb',
        ]
        from tools.yt_video_queue import cmd_search
        ns = argparse.Namespace(
            pattern='*timeout*', by=None,
        )
        out = StringIO()
        with patch('sys.stdout', out):
            rc: int = await cmd_search(ns, queue)
        self.assertEqual(rc, 0)
        # search_meta called with default fields tuple
        queue.search_meta.assert_awaited_once()
        kwargs = queue.search_meta.await_args.kwargs
        self.assertEqual(
            kwargs['fields'],
            ('last_error', 'source'),
        )
        self.assertIn('aaa', out.getvalue())
        self.assertIn('bbb', out.getvalue())

    async def test_by_filters_field(self) -> None:
        queue = AsyncMock()
        queue.search_meta.return_value = []
        from tools.yt_video_queue import cmd_search
        ns = argparse.Namespace(
            pattern='rss', by='source',
        )
        out = StringIO()
        with patch('sys.stdout', out):
            rc: int = await cmd_search(ns, queue)
        self.assertEqual(rc, 0)
        queue.search_meta.assert_awaited_once()
        kwargs = queue.search_meta.await_args.kwargs
        self.assertEqual(
            kwargs['fields'], ('source',),
        )
