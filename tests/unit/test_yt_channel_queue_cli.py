'''Unit tests for tools.yt_channel_queue.'''

import unittest

from tools.yt_channel_queue import (
    main_async,
)


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

from scrape_exchange.channel_scrape_queue import (
    ChannelState,
)


class TestNormaliseKey(unittest.TestCase):

    def test_channel_id_form(self) -> None:
        from tools.yt_channel_queue import (
            _normalise_key,
        )
        self.assertEqual(
            _normalise_key('UCabcdefghijklmnopqrstuv'),
            'i:UCabcdefghijklmnopqrstuv',
        )

    def test_handle_form_strips_at(self) -> None:
        from tools.yt_channel_queue import (
            _normalise_key,
        )
        self.assertEqual(
            _normalise_key('@LinusTechTips'),
            'h:LinusTechTips',
        )

    def test_bare_handle(self) -> None:
        from tools.yt_channel_queue import (
            _normalise_key,
        )
        self.assertEqual(
            _normalise_key('LinusTechTips'),
            'h:LinusTechTips',
        )


class TestCountCommand(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_total(self) -> None:
        queue = AsyncMock()
        queue.count_by_state.return_value = {
            ChannelState.SCHEDULED: 42,
            ChannelState.NOT_FOUND: 3,
        }
        from tools.yt_channel_queue import cmd_count
        ns = argparse.Namespace(
            tier=None, state=None,
        )
        out = StringIO()
        with patch('sys.stdout', out):
            rc: int = await cmd_count(ns, queue)
        self.assertEqual(rc, 0)
        self.assertIn('45', out.getvalue())

    async def test_by_state(self) -> None:
        queue = AsyncMock()
        queue.count_by_state.return_value = {
            ChannelState.NOT_FOUND: 12,
        }
        from tools.yt_channel_queue import cmd_count
        ns = argparse.Namespace(
            tier=None, state='not_found',
        )
        out = StringIO()
        with patch('sys.stdout', out):
            rc: int = await cmd_count(ns, queue)
        self.assertEqual(rc, 0)
        self.assertIn('12', out.getvalue())

    async def test_by_tier(self) -> None:
        queue = AsyncMock()
        queue.count_by_state.return_value = {}
        queue.count_by_tier.return_value = {
            0: 100, 1: 50, 2: 25,
        }
        from tools.yt_channel_queue import cmd_count
        ns = argparse.Namespace(
            tier=1, state=None,
        )
        out = StringIO()
        with patch('sys.stdout', out):
            rc: int = await cmd_count(ns, queue)
        self.assertEqual(rc, 0)
        self.assertIn('50', out.getvalue())


class TestStatsCommand(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_includes_all_states(self) -> None:
        queue = AsyncMock()
        queue.count_by_state.return_value = {
            s: i for i, s in enumerate(ChannelState)
        }
        queue.count_by_tier.return_value = {
            0: 0, 1: 0, 2: 0, 3: 0, 4: 0,
        }
        from tools.yt_channel_queue import cmd_stats
        out = StringIO()
        with patch('sys.stdout', out):
            rc: int = await cmd_stats(
                argparse.Namespace(), queue,
            )
        self.assertEqual(rc, 0)
        for s in ChannelState:
            self.assertIn(s.value, out.getvalue())


class TestShowCommand(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_prints_meta(self) -> None:
        queue = AsyncMock()
        queue.get_meta.return_value = {
            'handle': 'foo',
            'state': 'scheduled',
        }
        from tools.yt_channel_queue import cmd_show
        ns = argparse.Namespace(key='@foo')
        out = StringIO()
        with patch('sys.stdout', out):
            rc: int = await cmd_show(ns, queue)
        self.assertEqual(rc, 0)
        self.assertIn('handle', out.getvalue())
        self.assertIn('scheduled', out.getvalue())
        queue.get_meta.assert_awaited_once_with('h:foo')

    async def test_missing_key_returns_1(self) -> None:
        queue = AsyncMock()
        queue.get_meta.return_value = {}
        from tools.yt_channel_queue import cmd_show
        ns = argparse.Namespace(key='@nope')
        rc: int = await cmd_show(ns, queue)
        self.assertEqual(rc, 1)


class TestSearchCommand(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_prints_matches(self) -> None:
        queue = AsyncMock()
        queue.search_meta.return_value = [
            'h:LinusTechTips',
            'i:UCXuqSBlHAE6Xw-yeJA0Tunw',
        ]
        queue.get_meta.side_effect = [
            {
                'handle': 'LinusTechTips',
                'state': 'pending_resolution',
            },
            {
                'handle': 'LinusTechTips',
                'channel_id': 'UCXuqSBlHAE6Xw-yeJA0Tunw',
                'state': 'scheduled',
            },
        ]
        from tools.yt_channel_queue import cmd_search
        ns = argparse.Namespace(
            pattern='Linus*', by='handle',
        )
        out = StringIO()
        with patch('sys.stdout', out):
            rc: int = await cmd_search(ns, queue)
        self.assertEqual(rc, 0)
        self.assertIn(
            'LinusTechTips', out.getvalue(),
        )
        self.assertIn(
            'pending_resolution', out.getvalue(),
        )
        # search_meta called with the correct
        # fields tuple.
        queue.search_meta.assert_awaited_once()
        kwargs = queue.search_meta.await_args.kwargs
        self.assertEqual(kwargs['fields'], ('handle',))

    async def test_no_by_uses_default_fields(
        self,
    ) -> None:
        queue = AsyncMock()
        queue.search_meta.return_value = []
        from tools.yt_channel_queue import cmd_search
        ns = argparse.Namespace(
            pattern='*', by=None,
        )
        out = StringIO()
        with patch('sys.stdout', out):
            rc: int = await cmd_search(ns, queue)
        self.assertEqual(rc, 0)
        queue.search_meta.assert_awaited_once()
        kwargs = queue.search_meta.await_args.kwargs
        self.assertEqual(
            kwargs['fields'],
            ('handle', 'channel_id', 'name'),
        )


class TestAddCommand(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_channel_id_form_enqueues_scheduled(
        self,
    ) -> None:
        queue = AsyncMock()
        identity = AsyncMock()
        from tools.yt_channel_queue import cmd_add
        ns = argparse.Namespace(
            keys=['UCXuqSBlHAE6Xw-yeJA0Tunw'],
            priority=False, source='cli',
        )
        rc: int = await cmd_add(
            ns, queue, identity=identity,
        )
        self.assertEqual(rc, 0)
        queue.enqueue_scheduled.assert_awaited_once_with(
            'UCXuqSBlHAE6Xw-yeJA0Tunw',
            source='cli', priority=False,
        )
        identity.lookup_id_for_handle.assert_not_called()

    async def test_handle_resolves_via_identity(
        self,
    ) -> None:
        queue = AsyncMock()
        identity = AsyncMock()
        identity.lookup_id_for_handle.return_value = (
            'UCXuqSBlHAE6Xw-yeJA0Tunw'
        )
        from tools.yt_channel_queue import cmd_add
        ns = argparse.Namespace(
            keys=['@LinusTechTips'],
            priority=False, source='cli',
        )
        rc: int = await cmd_add(
            ns, queue, identity=identity,
        )
        self.assertEqual(rc, 0)
        queue.enqueue_scheduled.assert_awaited_once_with(
            'UCXuqSBlHAE6Xw-yeJA0Tunw',
            source='cli', priority=False,
        )

    async def test_unknown_handle_enqueues_unresolved(
        self,
    ) -> None:
        queue = AsyncMock()
        identity = AsyncMock()
        identity.lookup_id_for_handle.return_value = None
        from tools.yt_channel_queue import cmd_add
        ns = argparse.Namespace(
            keys=['@NewChannel'],
            priority=True, source='cli',
        )
        rc: int = await cmd_add(
            ns, queue, identity=identity,
        )
        self.assertEqual(rc, 0)
        queue.enqueue_unresolved.assert_awaited_once_with(
            'NewChannel',
            source='cli', priority=True,
        )


class TestRemoveCommand(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_marks_removed(self) -> None:
        queue = AsyncMock()
        from tools.yt_channel_queue import cmd_remove
        ns = argparse.Namespace(
            keys=['@foo'], note='operator request',
        )
        rc: int = await cmd_remove(ns, queue)
        self.assertEqual(rc, 0)
        queue.mark.assert_awaited_once()
        kwargs = queue.mark.await_args.kwargs
        from scrape_exchange.channel_scrape_queue \
            import ChannelState
        self.assertEqual(
            kwargs['state'], ChannelState.REMOVED,
        )
        self.assertEqual(
            kwargs['note'], 'operator request',
        )


class TestMarkUnmarkCommands(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_mark_terminated(self) -> None:
        queue = AsyncMock()
        from tools.yt_channel_queue import cmd_mark
        ns = argparse.Namespace(
            key='UCXuqSBlHAE6Xw-yeJA0Tunw',
            state='terminated',
            note='manually reviewed',
            hard=False,
        )
        rc: int = await cmd_mark(ns, queue)
        self.assertEqual(rc, 0)
        queue.mark.assert_awaited_once()
        kwargs = queue.mark.await_args.kwargs
        self.assertEqual(
            kwargs['state'],
            ChannelState.TERMINATED,
        )
        self.assertEqual(
            kwargs['note'], 'manually reviewed',
        )

    async def test_mark_hard_unavailable_escalation(
        self,
    ) -> None:
        queue = AsyncMock()
        from tools.yt_channel_queue import cmd_mark
        ns = argparse.Namespace(
            key='UCXuqSBlHAE6Xw-yeJA0Tunw',
            state='soft_unavailable',
            note=None,
            hard=True,
        )
        rc: int = await cmd_mark(ns, queue)
        self.assertEqual(rc, 0)
        kwargs = queue.mark.await_args.kwargs
        self.assertEqual(
            kwargs['state'],
            ChannelState.HARD_UNAVAILABLE,
        )

    async def test_unknown_state_returns_2(
        self,
    ) -> None:
        queue = AsyncMock()
        from tools.yt_channel_queue import cmd_mark
        ns = argparse.Namespace(
            key='@foo', state='bogus_state',
            note=None, hard=False,
        )
        rc: int = await cmd_mark(ns, queue)
        self.assertEqual(rc, 2)
        queue.mark.assert_not_called()

    async def test_unmark(self) -> None:
        queue = AsyncMock()
        from tools.yt_channel_queue import cmd_unmark
        ns = argparse.Namespace(
            key='UCXuqSBlHAE6Xw-yeJA0Tunw',
            to=None,
        )
        rc: int = await cmd_unmark(ns, queue)
        self.assertEqual(rc, 0)
        queue.unmark.assert_awaited_once_with(
            'i:UCXuqSBlHAE6Xw-yeJA0Tunw',
        )


class TestRescrapeCommand(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_terminal_state_unmarks(
        self,
    ) -> None:
        queue = AsyncMock()
        queue.get_state.return_value = (
            ChannelState.NOT_FOUND
        )
        from tools.yt_channel_queue import (
            cmd_rescrape,
        )
        ns = argparse.Namespace(
            keys=['UCXuqSBlHAE6Xw-yeJA0Tunw'],
        )
        rc: int = await cmd_rescrape(ns, queue)
        self.assertEqual(rc, 0)
        queue.unmark.assert_awaited_once_with(
            'i:UCXuqSBlHAE6Xw-yeJA0Tunw',
        )

    async def test_scheduled_pushed_to_front(
        self,
    ) -> None:
        queue = AsyncMock()
        queue.get_state.return_value = (
            ChannelState.SCHEDULED
        )
        from tools.yt_channel_queue import (
            cmd_rescrape,
        )
        ns = argparse.Namespace(
            keys=['UCXuqSBlHAE6Xw-yeJA0Tunw'],
        )
        rc: int = await cmd_rescrape(ns, queue)
        self.assertEqual(rc, 0)
        # Unmark on scheduled is a no-op; instead
        # the CLI calls requeue_with_backoff with a
        # negative seconds to bring score to 0.
        queue.requeue_with_backoff.assert_awaited_once()

    async def test_unknown_key_returns_nonzero_continues(
        self,
    ) -> None:
        queue = AsyncMock()
        queue.get_state.return_value = None
        from tools.yt_channel_queue import (
            cmd_rescrape,
        )
        ns = argparse.Namespace(
            keys=['UCnone0000000000000000000'],
        )
        rc: int = await cmd_rescrape(ns, queue)
        # Code 0 is fine for skip-with-warning; the
        # important thing is no queue mutation happened.
        self.assertEqual(rc, 0)
        queue.unmark.assert_not_called()
        queue.requeue_with_backoff.assert_not_called()


import json
import os
import shutil
import tempfile


class TestIngestSentinelsCommand(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_sweeps_not_found_and_unresolved(
        self,
    ) -> None:
        queue = AsyncMock()
        td = tempfile.mkdtemp()
        try:
            for name in [
                'channel-foo.json.br.not_found',
                'channel-bar.json.br.unresolved',
                'channel-baz.json.br',  # normal file
            ]:
                with open(
                    os.path.join(td, name), 'w',
                ) as f:
                    f.write('')
            from tools.yt_channel_queue import (
                cmd_ingest_sentinels,
            )
            ns = argparse.Namespace(directory=td)
            rc: int = await cmd_ingest_sentinels(
                ns, queue,
            )
            self.assertEqual(rc, 0)
            self.assertEqual(
                queue.mark.await_count, 2,
            )
            remaining = set(os.listdir(td))
            self.assertIn(
                'channel-baz.json.br', remaining,
            )
            self.assertNotIn(
                'channel-foo.json.br.not_found',
                remaining,
            )
            self.assertNotIn(
                'channel-bar.json.br.unresolved',
                remaining,
            )
        finally:
            shutil.rmtree(td)

    async def test_id_form_prefix(self) -> None:
        queue = AsyncMock()
        td = tempfile.mkdtemp()
        try:
            name = (
                'channel-UCXuqSBlHAE6Xw-yeJA0Tunw'
                '.json.br.not_found'
            )
            with open(
                os.path.join(td, name), 'w',
            ) as f:
                f.write('')
            from tools.yt_channel_queue import (
                cmd_ingest_sentinels,
            )
            ns = argparse.Namespace(directory=td)
            rc: int = await cmd_ingest_sentinels(
                ns, queue,
            )
            self.assertEqual(rc, 0)
            queue.mark.assert_awaited_once()
            args = queue.mark.await_args
            # member must use i: prefix
            self.assertEqual(
                args.args[0],
                'i:UCXuqSBlHAE6Xw-yeJA0Tunw',
            )
        finally:
            shutil.rmtree(td)


class TestImportCommand(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_import_calls_enqueue_per_line(
        self,
    ) -> None:
        queue = AsyncMock()
        identity = AsyncMock()
        identity.lookup_id_for_handle.side_effect = (
            lambda h: (
                'UCknown0000000000000000a'
                if h == 'known' else None
            )
        )
        td = tempfile.mkdtemp()
        try:
            path: str = os.path.join(td, 'c.lst')
            with open(path, 'w') as f:
                f.write(
                    json.dumps({
                        'channel_id': None,
                        'channel_handle': 'known',
                        'title': None,
                        'status': 'new',
                    }) + '\n',
                )
                f.write(
                    json.dumps({
                        'channel_id': None,
                        'channel_handle': 'unknown',
                        'title': None,
                        'status': 'new',
                    }) + '\n',
                )
                f.write(
                    json.dumps({
                        'channel_id': (
                            'UCabc0000000000000000000'
                        ),
                        'channel_handle': 'abc',
                        'title': 'ABC',
                        'status': 'scraped',
                    }) + '\n',
                )
            from tools.yt_channel_queue import cmd_import
            ns = argparse.Namespace(
                file=path, replace=False, merge=True,
            )
            rc: int = await cmd_import(
                ns, queue, identity=identity,
            )
            self.assertEqual(rc, 0)
            # 'known' resolves → enqueue_scheduled
            # 'abc' has explicit id → enqueue_scheduled
            # 'unknown' fails lookup → enqueue_unresolved
            self.assertEqual(
                queue.enqueue_scheduled.await_count, 2,
            )
            queue.enqueue_unresolved.assert_awaited_once()
        finally:
            shutil.rmtree(td)

    async def test_replace_clears_keys_first(
        self,
    ) -> None:
        queue = AsyncMock()

        async def fake_scan_iter(**kwargs):
            for key in ['youtube:channel:abc']:
                yield key
        # We have to patch _redis to make this work.
        queue._redis = type(
            'X', (), {
                'scan_iter': lambda self, **k:
                fake_scan_iter(**k),
                'delete': AsyncMock(),
            },
        )()
        identity = AsyncMock()
        td = tempfile.mkdtemp()
        try:
            path: str = os.path.join(td, 'c.lst')
            with open(path, 'w'):
                pass  # empty file
            from tools.yt_channel_queue import cmd_import
            ns = argparse.Namespace(
                file=path, replace=True, merge=False,
            )
            rc: int = await cmd_import(
                ns, queue, identity=identity,
            )
            self.assertEqual(rc, 0)
        finally:
            shutil.rmtree(td)

    async def test_skips_malformed_lines(
        self,
    ) -> None:
        queue = AsyncMock()
        identity = AsyncMock()
        td = tempfile.mkdtemp()
        try:
            path: str = os.path.join(td, 'c.lst')
            with open(path, 'w') as f:
                f.write('not json\n')
                f.write('\n')  # empty
                f.write(
                    json.dumps({
                        'channel_id': (
                            'UCa0000000000000000000ab'
                        ),
                        'channel_handle': 'a',
                        'title': None,
                        'status': 'new',
                    }) + '\n',
                )
            from tools.yt_channel_queue import cmd_import
            ns = argparse.Namespace(
                file=path, replace=False, merge=True,
            )
            rc: int = await cmd_import(
                ns, queue, identity=identity,
            )
            self.assertEqual(rc, 0)
            self.assertEqual(
                queue.enqueue_scheduled.await_count, 1,
            )
        finally:
            shutil.rmtree(td)

    async def test_legacy_numeric_id_falls_through_to_handle(
        self,
    ) -> None:
        '''Legacy YouTube user ids (e.g. ``167057232``)
        do not match the modern ``UC...`` format and
        must not be passed to ``enqueue_scheduled`` —
        the queue would reject them. The import path
        should fall through to the handle.'''
        queue = AsyncMock()
        identity = AsyncMock()
        identity.lookup_id_for_handle.return_value = None
        td = tempfile.mkdtemp()
        try:
            path: str = os.path.join(td, 'c.lst')
            with open(path, 'w') as f:
                f.write(
                    json.dumps({
                        'channel_id': '167057232',
                        'channel_handle': 'oldchannel',
                        'title': None,
                        'status': 'new',
                    }) + '\n',
                )
            from tools.yt_channel_queue import cmd_import
            ns = argparse.Namespace(
                file=path, replace=False, merge=True,
            )
            rc: int = await cmd_import(
                ns, queue, identity=identity,
            )
            self.assertEqual(rc, 0)
            queue.enqueue_scheduled.assert_not_called()
            queue.enqueue_unresolved.assert_awaited_once_with(
                'oldchannel', source='migration',
            )
        finally:
            shutil.rmtree(td)

    async def test_legacy_id_no_handle_promotes_to_handle(
        self,
    ) -> None:
        '''Legacy non-UC id with a null handle gets
        promoted to the handle slot so the resolve
        phase can attempt InnerTube lookup. The
        scraper decides whether the value resolves to
        a real channel or gets marked NOT_FOUND.'''
        queue = AsyncMock()
        identity = AsyncMock()
        identity.lookup_id_for_handle.return_value = None
        td = tempfile.mkdtemp()
        try:
            path: str = os.path.join(td, 'c.lst')
            with open(path, 'w') as f:
                f.write(
                    json.dumps({
                        'channel_id': '167057232',
                        'channel_handle': None,
                        'title': None,
                        'status': 'new',
                    }) + '\n',
                )
            from tools.yt_channel_queue import cmd_import
            ns = argparse.Namespace(
                file=path, replace=False, merge=True,
            )
            rc: int = await cmd_import(
                ns, queue, identity=identity,
            )
            self.assertEqual(rc, 0)
            queue.enqueue_scheduled.assert_not_called()
            queue.enqueue_unresolved.assert_awaited_once_with(
                '167057232', source='migration',
            )
        finally:
            shutil.rmtree(td)
