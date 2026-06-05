'''Unit tests for tools.yt_video_queue.'''

import unittest

import httpx

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
from unittest.mock import AsyncMock, MagicMock, patch

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


def _stub_sources() -> object:
    '''A _ScrapedBeforeSources with an empty uploaded set and no
    disk / API source (so the filter passes every id through).'''
    from tools.yt_video_queue import _ScrapedBeforeSources
    uploaded = AsyncMock()
    uploaded.contains_many.return_value = {}
    return _ScrapedBeforeSources(
        uploaded=uploaded,
        file_mgmt=None,
        exchange_client=None,
        warnings=[],
    )


class TestAdd(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_enqueues_each_id(self) -> None:
        queue = AsyncMock()
        queue.enqueue.side_effect = [True, False]
        queue.get_state.return_value = None
        from tools.yt_video_queue import cmd_add
        ns = argparse.Namespace(
            video_ids=['aaaaaaaaaaa', 'bbbbbbbbbbb'],
            source='cli',
            file=None,
            force=False,
            no_remote_check=True,
        )
        out = StringIO()
        stdin = StringIO('')
        stdin.isatty = lambda: True
        with patch('sys.stdout', out), patch(
            'sys.stdin', stdin,
        ), patch(
            'tools.yt_video_queue._build_sources',
            new=AsyncMock(return_value=_stub_sources()),
        ):
            rc: int = await cmd_add(ns, queue)
        self.assertEqual(rc, 0)
        self.assertEqual(
            queue.enqueue.await_count, 2,
        )
        self.assertIn(
            'add: processed=2 already_scraped=0 '
            '(uploaded=0 terminal=0 disk=0 api=0) '
            'duplicates=1 added=1',
            out.getvalue(),
        )

    async def test_force_uses_force_enqueue(self) -> None:
        queue = AsyncMock()
        queue.force_enqueue.side_effect = [
            'added', 'revived', 'forced_pending',
        ]
        from tools.yt_video_queue import cmd_add
        ns = argparse.Namespace(
            video_ids=[
                'aaaaaaaaaaa', 'bbbbbbbbbbb', 'ccccccccccc',
            ],
            source='cli',
            file=None,
            force=True,
            no_remote_check=False,
        )
        out = StringIO()
        stdin = StringIO('')
        stdin.isatty = lambda: True
        with patch('sys.stdout', out), patch(
            'sys.stdin', stdin,
        ):
            rc: int = await cmd_add(ns, queue)
        self.assertEqual(rc, 0)
        self.assertEqual(queue.force_enqueue.await_count, 3)
        queue.enqueue.assert_not_called()
        value = out.getvalue()
        self.assertIn('processed=3', value)
        self.assertIn('added=1', value)
        self.assertIn('revived=1', value)
        self.assertIn('forced_pending=1', value)
        self.assertIn('(force)', value)


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
            ns = argparse.Namespace(
                directory=td,
                source='migration',
                force=False,
                no_remote_check=True,
            )
            queue.enqueue.side_effect = [True, False]
            queue.get_state.return_value = None
            out = StringIO()
            with patch('sys.stdout', out), patch(
                'tools.yt_video_queue._build_sources',
                new=AsyncMock(return_value=_stub_sources()),
            ):
                rc: int = await cmd_import(ns, queue)
            self.assertEqual(rc, 0)
            self.assertEqual(
                queue.enqueue.await_count, 2,
            )
            self.assertIn(
                'import: processed=2 ',
                out.getvalue(),
            )
            self.assertIn('added=1', out.getvalue())
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

    async def test_already_scraped_sentinel_consumed(
        self,
    ) -> None:
        '''An id filtered as already-scraped must still have its
        sentinel unlinked, else it re-reports forever.'''
        from tools.yt_video_queue import (
            _ScrapedBeforeSources, cmd_import,
        )
        queue = AsyncMock()
        queue.get_state.return_value = None
        td = tempfile.mkdtemp()
        try:
            with open(
                os.path.join(td, 'dQw4w9WgXcQ'), 'w',
            ) as f:
                f.write('')
            uploaded = AsyncMock()
            uploaded.contains_many.return_value = {
                'dQw4w9WgXcQ': True,
            }
            sources = _ScrapedBeforeSources(
                uploaded=uploaded,
                file_mgmt=None,
                exchange_client=None,
                warnings=[],
            )
            ns = argparse.Namespace(
                directory=td,
                source='migration',
                force=False,
                no_remote_check=True,
            )
            out = StringIO()
            with patch('sys.stdout', out), patch(
                'tools.yt_video_queue._build_sources',
                new=AsyncMock(return_value=sources),
            ):
                rc: int = await cmd_import(ns, queue)
            self.assertEqual(rc, 0)
            queue.enqueue.assert_not_called()
            self.assertIn('already_scraped=1', out.getvalue())
            # Sentinel consumed despite never being enqueued.
            self.assertEqual(os.listdir(td), [])
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


def _http_404() -> httpx.HTTPStatusError:
    request = httpx.Request('GET', 'http://x/data')
    response = httpx.Response(404, request=request)
    return httpx.HTTPStatusError(
        '404', request=request, response=response,
    )


def _api_client(username: str | None = 'me') -> MagicMock:
    client = MagicMock()
    client.authenticated_username = username
    return client


class TestFilterAlreadyScraped(
    unittest.IsolatedAsyncioTestCase,
):

    def _queue(self, state: object = None) -> AsyncMock:
        queue = AsyncMock()
        queue.get_state.return_value = state
        return queue

    def _uploaded(
        self, mapping: dict | None = None,
    ) -> AsyncMock:
        uploaded = AsyncMock()
        uploaded.contains_many.return_value = mapping or {}
        return uploaded

    async def test_uploaded_hit_short_circuits(
        self,
    ) -> None:
        from tools.yt_video_queue import (
            _filter_already_scraped,
        )
        queue = self._queue()
        res = await _filter_already_scraped(
            ['aaa'],
            uploaded=self._uploaded({'aaa': True}),
            queue=queue,
            file_mgmt=None,
            exchange_client=None,
            schema_version='0.0.2',
            remote_check=True,
        )
        self.assertEqual(res.survivors, [])
        self.assertEqual(res.already_scraped['uploaded'], 1)
        # Short-circuit: later sources not consulted.
        queue.get_state.assert_not_called()

    async def test_terminal_state_counts_as_scraped(
        self,
    ) -> None:
        from tools.yt_video_queue import (
            _filter_already_scraped,
        )
        res = await _filter_already_scraped(
            ['aaa'],
            uploaded=self._uploaded({}),
            queue=self._queue(VideoState.FAILED),
            file_mgmt=None,
            exchange_client=None,
            schema_version='0.0.2',
            remote_check=True,
        )
        self.assertEqual(res.survivors, [])
        self.assertEqual(res.already_scraped['terminal'], 1)

    async def test_queued_is_duplicate_not_scraped(
        self,
    ) -> None:
        from tools.yt_video_queue import (
            _filter_already_scraped,
        )
        res = await _filter_already_scraped(
            ['aaa'],
            uploaded=self._uploaded({}),
            queue=self._queue(VideoState.QUEUED),
            file_mgmt=None,
            exchange_client=None,
            schema_version='0.0.2',
            remote_check=True,
        )
        self.assertEqual(res.survivors, [])
        self.assertEqual(res.duplicates, 1)
        self.assertEqual(res.already_scraped['terminal'], 0)

    async def test_disk_hit(self) -> None:
        from tools.yt_video_queue import (
            _filter_already_scraped,
        )
        fm = MagicMock()
        fm.video_scrape_output_exists.return_value = True
        res = await _filter_already_scraped(
            ['aaa'],
            uploaded=self._uploaded({}),
            queue=self._queue(None),
            file_mgmt=fm,
            exchange_client=None,
            schema_version='0.0.2',
            remote_check=True,
        )
        self.assertEqual(res.survivors, [])
        self.assertEqual(res.already_scraped['disk'], 1)

    async def test_api_200_scoped_to_uploader(self) -> None:
        from tools.yt_video_queue import (
            _filter_already_scraped,
        )
        g = AsyncMock()
        with patch(
            'tools.yt_video_queue.get_data_by_param', new=g,
        ):
            res = await _filter_already_scraped(
                ['aaa'],
                uploaded=self._uploaded({}),
                queue=self._queue(None),
                file_mgmt=None,
                exchange_client=_api_client('me'),
                schema_version='0.0.2',
                remote_check=True,
            )
        self.assertEqual(res.survivors, [])
        self.assertEqual(res.already_scraped['api'], 1)
        # Scoped to our uploader, not schema_owner.
        kwargs = g.await_args.kwargs
        self.assertEqual(kwargs['username'], 'me')
        self.assertEqual(kwargs['platform'], 'youtube')
        self.assertEqual(kwargs['entity'], 'video')

    async def test_api_404_is_survivor(self) -> None:
        from tools.yt_video_queue import (
            _filter_already_scraped,
        )
        g = AsyncMock(side_effect=_http_404())
        with patch(
            'tools.yt_video_queue.get_data_by_param', new=g,
        ):
            res = await _filter_already_scraped(
                ['aaa'],
                uploaded=self._uploaded({}),
                queue=self._queue(None),
                file_mgmt=None,
                exchange_client=_api_client('me'),
                schema_version='0.0.2',
                remote_check=True,
            )
        self.assertEqual(res.survivors, ['aaa'])
        self.assertEqual(res.already_scraped['api'], 0)

    async def test_api_error_fails_open(self) -> None:
        from tools.yt_video_queue import (
            _filter_already_scraped,
        )
        g = AsyncMock(side_effect=RuntimeError('boom'))
        with patch(
            'tools.yt_video_queue.get_data_by_param', new=g,
        ):
            res = await _filter_already_scraped(
                ['aaa'],
                uploaded=self._uploaded({}),
                queue=self._queue(None),
                file_mgmt=None,
                exchange_client=_api_client('me'),
                schema_version='0.0.2',
                remote_check=True,
            )
        self.assertEqual(res.survivors, ['aaa'])

    async def test_no_remote_check_skips_api(self) -> None:
        from tools.yt_video_queue import (
            _filter_already_scraped,
        )
        g = AsyncMock()
        with patch(
            'tools.yt_video_queue.get_data_by_param', new=g,
        ):
            res = await _filter_already_scraped(
                ['aaa'],
                uploaded=self._uploaded({}),
                queue=self._queue(None),
                file_mgmt=None,
                exchange_client=_api_client('me'),
                schema_version='0.0.2',
                remote_check=False,
            )
        self.assertEqual(res.survivors, ['aaa'])
        g.assert_not_awaited()

    async def test_uploaded_unavailable_fails_open(
        self,
    ) -> None:
        from tools.yt_video_queue import (
            _filter_already_scraped,
        )
        uploaded = AsyncMock()
        uploaded.contains_many.side_effect = RuntimeError(
            'redis down',
        )
        res = await _filter_already_scraped(
            ['aaa'],
            uploaded=uploaded,
            queue=self._queue(None),
            file_mgmt=None,
            exchange_client=None,
            schema_version='0.0.2',
            remote_check=True,
        )
        self.assertEqual(res.survivors, ['aaa'])
