'''Unit tests for tools.yt_channel_queue.'''

import argparse
import io
import json
import json as json_mod
import os
import shutil
import tempfile
import unittest
from io import StringIO
from unittest.mock import AsyncMock, MagicMock, patch

import fakeredis.aioredis
from redis.exceptions import ConnectionError as RedisConnectionError

from scrape_exchange.channel_scrape_queue import (
    ChannelScrapeQueueSettings,
    ChannelState,
    RedisChannelScrapeQueue,
)
from tools.yt_channel_queue import (
    cmd_export,
    cmd_export_rss,
    cmd_reconcile,
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

    async def test_redis_connection_error_returns_clean_error(
        self,
    ) -> None:
        import tools.yt_channel_queue as cli

        redis = AsyncMock()
        handler = AsyncMock(
            side_effect=RedisConnectionError(
                'max number of clients reached',
            ),
        )
        err = StringIO()

        with (
            patch.object(cli, 'redis_from_url') as from_url,
            patch.dict(cli._HANDLERS, {'count': handler}),
            patch('sys.stderr', err),
        ):
            from_url.return_value = redis
            rc: int = await cli.main_async(['count'])

        self.assertEqual(rc, 1)
        self.assertIn(
            'redis connection failed: max number of clients reached',
            err.getvalue(),
        )
        redis.aclose.assert_awaited_once()

    async def test_main_async_limits_cli_redis_pool_to_one(
        self,
    ) -> None:
        import tools.yt_channel_queue as cli

        redis = AsyncMock()
        handler = AsyncMock(return_value=0)

        with (
            patch.object(cli, 'redis_from_url') as from_url,
            patch.dict(cli._HANDLERS, {'count': handler}),
        ):
            from_url.return_value = redis
            rc: int = await cli.main_async(['count'])

        self.assertEqual(rc, 0)
        self.assertEqual(from_url.call_args.kwargs['max_connections'], 1)
        redis.aclose.assert_awaited_once()


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

    async def test_formats_epoch_meta_fields(self) -> None:
        queue = AsyncMock()
        queue.get_meta.return_value = {
            'handle': 'foo',
            'state': 'scheduled',
            'created_at': '0',
            'last_attempt_at': '60',
        }
        from tools.yt_channel_queue import cmd_show
        ns = argparse.Namespace(key='@foo')
        out = StringIO()
        with patch('sys.stdout', out):
            rc: int = await cmd_show(ns, queue)
        self.assertEqual(rc, 0)
        self.assertIn(
            'created_at            1970-01-01 00:00:00 UTC',
            out.getvalue(),
        )
        self.assertIn(
            'last_attempt_at       1970-01-01 00:01:00 UTC',
            out.getvalue(),
        )

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
        queue.get_meta = AsyncMock(return_value={})
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
        queue.get_meta = AsyncMock(return_value={})
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
        queue.get_meta = AsyncMock(return_value={})
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

    async def test_terminal_state_forces_rescrape(
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
            mode='default',
        )
        rc: int = await cmd_rescrape(ns, queue)
        self.assertEqual(rc, 0)
        queue.force_rescrape.assert_awaited_once_with(
            'i:UCXuqSBlHAE6Xw-yeJA0Tunw',
            mode='default',
            source='cli',
        )

    async def test_scheduled_forces_full_rescrape(
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
            mode='full',
        )
        rc: int = await cmd_rescrape(ns, queue)
        self.assertEqual(rc, 0)
        queue.force_rescrape.assert_awaited_once_with(
            'i:UCXuqSBlHAE6Xw-yeJA0Tunw',
            mode='full',
            source='cli',
        )

    async def test_pending_resolution_forces_metadata_rescrape(
        self,
    ) -> None:
        queue = AsyncMock()
        queue.get_state.return_value = (
            ChannelState.PENDING_RESOLUTION
        )
        from tools.yt_channel_queue import (
            cmd_rescrape,
        )
        ns = argparse.Namespace(
            keys=['@veritasium'],
            mode='metadata',
        )
        rc: int = await cmd_rescrape(ns, queue)
        self.assertEqual(rc, 0)
        queue.force_rescrape.assert_awaited_once_with(
            'h:veritasium',
            mode='metadata',
            source='cli',
        )

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
            mode='default',
        )
        rc: int = await cmd_rescrape(ns, queue)
        # Code 0 is fine for skip-with-warning; the
        # important thing is no queue mutation happened.
        self.assertEqual(rc, 0)
        queue.force_rescrape.assert_not_called()

    async def test_reads_keys_from_stdin_when_no_args(
        self,
    ) -> None:
        queue = AsyncMock()
        queue.get_state.return_value = ChannelState.SCHEDULED
        from tools.yt_channel_queue import cmd_rescrape
        ns = argparse.Namespace(keys=[], mode='full')

        with patch(
            'sys.stdin',
            StringIO(
                'UCXuqSBlHAE6Xw-yeJA0Tunw\n'
                '@veritasium\n'
                '\n'
            ),
        ):
            rc: int = await cmd_rescrape(ns, queue)

        self.assertEqual(rc, 0)
        self.assertEqual(queue.force_rescrape.await_count, 2)
        queue.force_rescrape.assert_any_await(
            'i:UCXuqSBlHAE6Xw-yeJA0Tunw',
            mode='full',
            source='cli',
        )
        queue.force_rescrape.assert_any_await(
            'h:veritasium',
            mode='full',
            source='cli',
        )

    async def test_dash_reads_keys_from_stdin_inline(
        self,
    ) -> None:
        queue = AsyncMock()
        queue.get_state.return_value = ChannelState.SCHEDULED
        from tools.yt_channel_queue import cmd_rescrape
        ns = argparse.Namespace(
            keys=[
                'UCXuqSBlHAE6Xw-yeJA0Tunw',
                '-',
                '@minutephysics',
            ],
            mode='metadata',
        )

        with patch('sys.stdin', StringIO('@veritasium\n')):
            rc: int = await cmd_rescrape(ns, queue)

        self.assertEqual(rc, 0)
        self.assertEqual(queue.force_rescrape.await_count, 3)
        calls = [
            call.args[0]
            for call in queue.force_rescrape.await_args_list
        ]
        self.assertEqual(
            calls,
            [
                'i:UCXuqSBlHAE6Xw-yeJA0Tunw',
                'h:veritasium',
                'h:minutephysics',
            ],
        )


class TestRescrapeParser(unittest.TestCase):

    def test_rescrape_default_mode(self) -> None:
        from tools.yt_channel_queue import _build_parser
        ns = _build_parser().parse_args([
            'rescrape', '@veritasium',
        ])
        self.assertEqual(ns.mode, 'default')

    def test_rescrape_full_mode(self) -> None:
        from tools.yt_channel_queue import _build_parser
        ns = _build_parser().parse_args([
            'rescrape', '--mode', 'full', '@veritasium',
        ])
        self.assertEqual(ns.mode, 'full')

    def test_rescrape_metadata_mode(self) -> None:
        from tools.yt_channel_queue import _build_parser
        ns = _build_parser().parse_args([
            'rescrape', '--mode', 'metadata', '@veritasium',
        ])
        self.assertEqual(ns.mode, 'metadata')

    def test_rescrape_allows_no_positional_args(self) -> None:
        from tools.yt_channel_queue import _build_parser
        ns = _build_parser().parse_args([
            'rescrape', '--mode', 'full',
        ])
        self.assertEqual(ns.keys, [])
        self.assertEqual(ns.mode, 'full')


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
        queue.get_meta = AsyncMock(return_value={})
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


class TestImportRevivesTerminal(
    unittest.IsolatedAsyncioTestCase,
):
    '''Importing a member already in a terminal state must
    remove it from that state and re-enqueue it.'''

    async def asyncSetUp(self) -> None:
        self.redis = fakeredis.aioredis.FakeRedis(
            decode_responses=True,
        )
        self.queue: RedisChannelScrapeQueue = (
            RedisChannelScrapeQueue(
                self.redis,
                ChannelScrapeQueueSettings(),
            )
        )
        self.td: str = tempfile.mkdtemp()

    async def asyncTearDown(self) -> None:
        shutil.rmtree(self.td)
        await self.redis.aclose()

    async def _import(
        self,
        records: list[dict[str, object]],
        *,
        identity: object | None = None,
    ) -> str:
        from tools.yt_channel_queue import cmd_import

        path: str = os.path.join(self.td, 'c.lst')
        with open(path, 'w') as f:
            for rec in records:
                f.write(json.dumps(rec) + '\n')
        ns = argparse.Namespace(
            file=path, replace=False, merge=True,
        )
        out = StringIO()
        with patch('sys.stdout', out):
            rc: int = await cmd_import(
                ns, self.queue, identity=identity,
            )
        self.assertEqual(rc, 0)
        return out.getvalue()

    async def test_revives_not_found_channel_id(
        self,
    ) -> None:
        cid: str = 'UCXuqSBlHAE6Xw-yeJA0Tunw'
        member: str = f'i:{cid}'
        await self.queue.mark(
            member, state=ChannelState.NOT_FOUND,
        )

        report: str = await self._import([
            {'channel_id': cid, 'channel_handle': None},
        ])

        self.assertFalse(
            await self.redis.hexists(
                'youtube:channel:not_found', member,
            ),
        )
        self.assertEqual(
            await self.queue.get_state(member),
            ChannelState.SCHEDULED,
        )
        self.assertIn('revived from terminal: 1', report)
        self.assertIn('not_found=1', report)

    async def test_revives_removed_channel_id(
        self,
    ) -> None:
        cid: str = 'UCabc0000000000000000000'
        member: str = f'i:{cid}'
        await self.queue.mark(
            member, state=ChannelState.REMOVED,
        )

        report: str = await self._import([
            {'channel_id': cid, 'channel_handle': None},
        ])

        self.assertFalse(
            await self.redis.hexists(
                'youtube:channel:removed', member,
            ),
        )
        self.assertEqual(
            await self.queue.get_state(member),
            ChannelState.SCHEDULED,
        )
        self.assertIn('revived from terminal: 1', report)
        self.assertIn('removed=1', report)

    async def test_revives_not_found_handle(
        self,
    ) -> None:
        handle: str = 'LegacyHandle'
        member: str = f'h:{handle}'
        await self.queue.mark(
            member, state=ChannelState.NOT_FOUND,
        )

        report: str = await self._import(
            [{'channel_id': None,
              'channel_handle': handle}],
            identity=None,
        )

        self.assertFalse(
            await self.redis.hexists(
                'youtube:channel:not_found', member,
            ),
        )
        self.assertEqual(
            await self.queue.get_state(member),
            ChannelState.PENDING_RESOLUTION,
        )
        self.assertIn('revived from terminal: 1', report)

    async def test_new_channel_id_not_revived(
        self,
    ) -> None:
        cid: str = 'UCabc0000000000000000000'
        member: str = f'i:{cid}'

        report: str = await self._import([
            {'channel_id': cid, 'channel_handle': None},
        ])

        self.assertEqual(
            await self.queue.get_state(member),
            ChannelState.SCHEDULED,
        )
        self.assertNotIn('revived from terminal', report)


class TestBackfillRssCommand(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_backfill_rss_dry_run_counts_candidates(
        self,
    ) -> None:
        from tools.yt_channel_queue import cmd_backfill_rss

        queue = AsyncMock()
        queue._tiers = [object()]
        queue._k_scheduled = (
            lambda tier: f'youtube:channel:queue:scheduled:{tier}'
        )
        queue._k_state = (
            lambda state: f'youtube:channel:{state.value}'
        )
        queue._redis.hscan = AsyncMock(return_value=(
            0,
            {
                'UCa0000000000000000000ab': 'A',
                'not-a-channel-id': 'bad',
                'UCb0000000000000000000cd': 'B',
            },
        ))
        pipe = MagicMock()
        group_size = 1 + len(ChannelState.terminal_states())
        pipe.execute = AsyncMock(return_value=(
            [None] * group_size
            + ['0'] + [0] * (group_size - 1)
        ))
        queue._redis.pipeline = lambda transaction=False: pipe

        ns = argparse.Namespace(
            batch_size=2,
            limit=None,
            dry_run=True,
            source='rss_backfill',
        )
        rc: int = await cmd_backfill_rss(ns, queue)

        self.assertEqual(rc, 0)
        self.assertEqual(pipe.zscore.call_count, 2)
        self.assertEqual(
            pipe.hexists.call_count,
            2 * len(ChannelState.terminal_states()),
        )
        pipe.execute.assert_awaited_once()
        queue._redis.eval.assert_not_called()

    async def test_backfill_rss_executes_lua_batch(
        self,
    ) -> None:
        from tools.yt_channel_queue import cmd_backfill_rss

        queue = AsyncMock()
        queue._tiers = [object(), object()]
        queue._k_tiers = lambda: 'youtube:channel:tiers'
        queue._k_scheduled = (
            lambda tier: f'youtube:channel:queue:scheduled:{tier}'
        )
        queue._k_state = (
            lambda state: f'youtube:channel:{state.value}'
        )
        queue._redis.hscan = AsyncMock(return_value=(
            0,
            {
                'UCa0000000000000000000ab': 'A',
                'UCb0000000000000000000cd': 'B',
            },
        ))
        queue._redis.eval = AsyncMock(return_value=[2, 0])

        ns = argparse.Namespace(
            batch_size=1000,
            limit=None,
            dry_run=False,
            source='rss_backfill',
        )
        rc: int = await cmd_backfill_rss(ns, queue)

        self.assertEqual(rc, 0)
        queue._redis.eval.assert_awaited_once()
        args = queue._redis.eval.await_args.args
        self.assertEqual(args[2], 'youtube:channel:tiers')
        self.assertEqual(
            args[3],
            'youtube:channel:queue:scheduled:0',
        )
        self.assertEqual(
            args[4],
            'youtube:channel:queue:scheduled:1',
        )
        self.assertIn('UCa0000000000000000000ab', args)
        self.assertIn('UCb0000000000000000000cd', args)

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
        queue.get_meta = AsyncMock(return_value={})
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
        queue.get_meta = AsyncMock(return_value={})
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
        queue.get_meta = AsyncMock(return_value={})
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


class TestReconcileCommand(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self.redis = fakeredis.aioredis.FakeRedis(
            decode_responses=True,
        )
        self.queue: RedisChannelScrapeQueue = (
            RedisChannelScrapeQueue(
                self.redis,
                ChannelScrapeQueueSettings(),
            )
        )

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    def _ns(
        self, **kwargs: object,
    ) -> argparse.Namespace:
        defaults: dict[str, object] = {
            'dry_run': True,
            'json': False,
            'sample': 5,
            'batch_size': 1000,
            'limit': None,
            'repair': None,
            'source': 'reconcile',
            'due_now': False,
            'max_repairs': None,
            'default_channel_tier': None,
            'default_rss_tier': None,
            'spread_window_seconds': 86400,
        }
        defaults.update(kwargs)
        return argparse.Namespace(**defaults)

    async def test_dry_run_returns_zero(
        self,
    ) -> None:
        buf: io.StringIO = io.StringIO()
        with patch('sys.stdout', buf):
            rc: int = await cmd_reconcile(
                self._ns(), self.queue,
            )
        self.assertEqual(rc, 0)
        # Plain-text mode mentions the drift class names
        self.assertIn(
            'rss_creator_missing_channel_state',
            buf.getvalue(),
        )

    async def test_json_mode_emits_valid_json(
        self,
    ) -> None:
        buf: io.StringIO = io.StringIO()
        with patch('sys.stdout', buf):
            rc: int = await cmd_reconcile(
                self._ns(json=True), self.queue,
            )
        self.assertEqual(rc, 0)
        payload: dict = json_mod.loads(buf.getvalue())
        self.assertIn('inspected', payload)
        self.assertIn('drift', payload)
        self.assertTrue(payload['dry_run'])

    async def test_drift_count_reflects_seeded_state(
        self,
    ) -> None:
        await self.redis.hset(
            'rss:youtube:creators',
            'UCxxxxxxxxxxxxxxxxxxxxxx', 'x',
        )
        buf: io.StringIO = io.StringIO()
        with patch('sys.stdout', buf):
            await cmd_reconcile(
                self._ns(json=True), self.queue,
            )
        payload: dict = json_mod.loads(buf.getvalue())
        self.assertEqual(
            payload['drift']
                   ['rss_creator_missing_channel_state']
                   ['count'],
            1,
        )

    async def test_repair_rss_missing_schedules_due_now(
        self,
    ) -> None:
        await self.redis.hset(
            'rss:youtube:creators',
            'UCxxxxxxxxxxxxxxxxxxxxxx', 'x',
        )
        buf: io.StringIO = io.StringIO()
        with patch('sys.stdout', buf):
            await cmd_reconcile(
                self._ns(
                    json=True,
                    repair='rss-missing',
                    due_now=True,
                ),
                self.queue,
            )
        payload: dict = json_mod.loads(buf.getvalue())
        self.assertFalse(payload['dry_run'])
        self.assertEqual(
            payload['repaired']['rss-missing'], 1,
        )
        score: float | None = await self.redis.zscore(
            'youtube:channel:queue:scheduled:3',
            'i:UCxxxxxxxxxxxxxxxxxxxxxx',
        )
        self.assertEqual(score, 0.0)

    async def test_repair_rss_seed_uses_meta_handle(
        self,
    ) -> None:
        await self.queue.enqueue_scheduled(
            'UCxxxxxxxxxxxxxxxxxxxxxx',
            source='test',
            priority=True,
        )
        await self.redis.hset(
            'youtube:channel:meta:i:UCxxxxxxxxxxxxxxxxxxxxxx',
            'handle',
            'ExampleHandle',
        )
        with patch('sys.stdout', io.StringIO()):
            await cmd_reconcile(
                self._ns(repair='rss-seed'),
                self.queue,
            )
        label: str | None = await self.redis.hget(
            'rss:youtube:creators',
            'UCxxxxxxxxxxxxxxxxxxxxxx',
        )
        self.assertEqual(label, 'ExampleHandle')
        tier: str | None = await self.redis.hget(
            'rss:youtube:tiers',
            'UCxxxxxxxxxxxxxxxxxxxxxx',
        )
        self.assertEqual(tier, '4')
        self.assertTrue(
            await self.redis.sismember(
                'rss:youtube:names',
                'examplehandle',
            )
        )

    async def test_repair_rss_seed_heals_missing_name_index(
        self,
    ) -> None:
        await self.queue.enqueue_scheduled(
            'UCxxxxxxxxxxxxxxxxxxxxxx',
            source='test',
            priority=True,
        )
        await self.redis.hset(
            'rss:youtube:creators',
            'UCxxxxxxxxxxxxxxxxxxxxxx',
            'ExampleHandle',
        )
        with patch('sys.stdout', io.StringIO()):
            await cmd_reconcile(
                self._ns(repair='rss-seed'),
                self.queue,
            )
        self.assertTrue(
            await self.redis.sismember(
                'rss:youtube:names',
                'examplehandle',
            )
        )

    async def test_repair_rss_suppress_removes_active_rss(
        self,
    ) -> None:
        await self.redis.hset(
            'rss:youtube:creators',
            'UCxxxxxxxxxxxxxxxxxxxxxx',
            'ExampleHandle',
        )
        await self.redis.hset(
            'rss:youtube:tiers',
            'UCxxxxxxxxxxxxxxxxxxxxxx',
            '4',
        )
        await self.redis.sadd(
            'rss:youtube:names',
            'examplehandle',
        )
        await self.redis.zadd(
            'rss:youtube:queue:4',
            {'UCxxxxxxxxxxxxxxxxxxxxxx': 0},
        )
        await self.queue.mark(
            'i:UCxxxxxxxxxxxxxxxxxxxxxx',
            state=ChannelState.NOT_FOUND,
        )
        with patch('sys.stdout', io.StringIO()):
            await cmd_reconcile(
                self._ns(repair='rss-suppress'),
                self.queue,
            )
        self.assertFalse(
            await self.redis.hexists(
                'rss:youtube:creators',
                'UCxxxxxxxxxxxxxxxxxxxxxx',
            )
        )
        self.assertTrue(
            await self.redis.hexists(
                'rss:youtube:suppressed',
                'UCxxxxxxxxxxxxxxxxxxxxxx',
            )
        )
        self.assertIsNone(
            await self.redis.zscore(
                'rss:youtube:queue:4',
                'UCxxxxxxxxxxxxxxxxxxxxxx',
            )
        )
        self.assertFalse(
            await self.redis.sismember(
                'rss:youtube:names',
                'examplehandle',
            )
        )

    async def test_repair_tier_placement_preserves_score(
        self,
    ) -> None:
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:0',
            {'i:UCxxxxxxxxxxxxxxxxxxxxxx': 123.0},
        )
        await self.redis.hset(
            'youtube:channel:tiers',
            'UCxxxxxxxxxxxxxxxxxxxxxx',
            '2',
        )
        with patch('sys.stdout', io.StringIO()):
            await cmd_reconcile(
                self._ns(repair='tier-placement'),
                self.queue,
            )
        self.assertIsNone(
            await self.redis.zscore(
                'youtube:channel:queue:scheduled:0',
                'i:UCxxxxxxxxxxxxxxxxxxxxxx',
            )
        )
        self.assertEqual(
            await self.redis.zscore(
                'youtube:channel:queue:scheduled:2',
                'i:UCxxxxxxxxxxxxxxxxxxxxxx',
            ),
            123.0,
        )

    async def test_reconcile_watch_rejects_repair_mode(
        self,
    ) -> None:
        buf: io.StringIO = io.StringIO()
        with patch('sys.stderr', buf):
            rc: int = await cmd_reconcile(
                self._ns(repair='rss-missing', watch=True),
                self.queue,
            )
        self.assertEqual(rc, 2)
        self.assertIn('--watch cannot be combined', buf.getvalue())


class TestExportCommand(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self.redis = fakeredis.aioredis.FakeRedis(
            decode_responses=True,
        )
        self.queue = RedisChannelScrapeQueue(
            self.redis, ChannelScrapeQueueSettings(),
        )

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    def _ns(
        self,
        state: str,
        *,
        tier: int | None = None,
        output: str | None = None,
        batch_size: int = 1000,
        limit: int | None = None,
    ) -> argparse.Namespace:
        return argparse.Namespace(
            state=state,
            tier=tier,
            output=output,
            batch_size=batch_size,
            limit=limit,
        )

    def _parse_jsonl(self, payload: str) -> list[dict]:
        return [
            json.loads(line)
            for line in payload.splitlines()
            if line.strip()
        ]

    async def test_export_unknown_state_returns_error(
        self,
    ) -> None:
        buf: StringIO = StringIO()
        err: StringIO = StringIO()
        with patch('sys.stdout', buf), patch(
            'sys.stderr', err,
        ):
            rc: int = await cmd_export(
                self._ns('nonsense_state'), self.queue,
            )
        self.assertEqual(rc, 2)
        self.assertIn('unknown state', err.getvalue())

    async def test_export_terminal_hash_state(self) -> None:
        await self.redis.hset(
            'youtube:channel:not_found',
            'i:UCaaaaaaaaaaaaaaaaaaaaaa', '',
        )
        await self.redis.hset(
            'youtube:channel:meta:i:UCaaaaaaaaaaaaaaaaaaaaaa',
            mapping={
                'channel_id': 'UCaaaaaaaaaaaaaaaaaaaaaa',
                'handle': 'alpha',
                'state': 'not_found',
                'source': 'import',
                'created_at': '0',
                'last_attempt_at': '60',
            },
        )
        buf: StringIO = StringIO()
        with patch('sys.stdout', buf), patch(
            'sys.stderr', StringIO(),
        ):
            rc: int = await cmd_export(
                self._ns('not_found'), self.queue,
            )
        self.assertEqual(rc, 0)
        rows: list[dict] = self._parse_jsonl(buf.getvalue())
        self.assertEqual(len(rows), 1)
        row: dict = rows[0]
        self.assertEqual(
            row['member'], 'i:UCaaaaaaaaaaaaaaaaaaaaaa',
        )
        self.assertEqual(row['state'], 'not_found')
        self.assertEqual(row['handle'], 'alpha')
        self.assertEqual(
            row['channel_id'], 'UCaaaaaaaaaaaaaaaaaaaaaa',
        )
        self.assertEqual(row['source'], 'import')
        self.assertEqual(
            row['created_at'], '1970-01-01 00:00:00 UTC',
        )
        self.assertEqual(
            row['last_attempt_at'],
            '1970-01-01 00:01:00 UTC',
        )
        # No score / tier for a terminal hash.
        self.assertNotIn('score', row)
        self.assertNotIn('tier', row)

    async def test_export_scheduled_all_tiers_includes_tier_and_score(
        self,
    ) -> None:
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:0',
            {'i:UCaaaaaaaaaaaaaaaaaaaaaa': 100.0},
        )
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:1',
            {'i:UCbbbbbbbbbbbbbbbbbbbbbb': 200.0},
        )
        await self.redis.hset(
            'youtube:channel:meta:i:UCaaaaaaaaaaaaaaaaaaaaaa',
            mapping={
                'channel_id': 'UCaaaaaaaaaaaaaaaaaaaaaa',
                'state': 'scheduled',
            },
        )
        await self.redis.hset(
            'youtube:channel:meta:i:UCbbbbbbbbbbbbbbbbbbbbbb',
            mapping={
                'channel_id': 'UCbbbbbbbbbbbbbbbbbbbbbb',
                'state': 'scheduled',
            },
        )
        buf: StringIO = StringIO()
        with patch('sys.stdout', buf), patch(
            'sys.stderr', StringIO(),
        ):
            rc: int = await cmd_export(
                self._ns('scheduled'), self.queue,
            )
        self.assertEqual(rc, 0)
        rows: list[dict] = self._parse_jsonl(buf.getvalue())
        self.assertEqual(len(rows), 2)
        by_tier: dict[int, dict] = {
            r['tier']: r for r in rows
        }
        self.assertEqual(set(by_tier.keys()), {0, 1})
        self.assertEqual(by_tier[0]['score'], 100.0)
        self.assertEqual(by_tier[1]['score'], 200.0)
        self.assertEqual(by_tier[0]['state'], 'scheduled')

    async def test_export_scheduled_with_tier_filter(
        self,
    ) -> None:
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:0',
            {'i:UCaaaaaaaaaaaaaaaaaaaaaa': 100.0},
        )
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:1',
            {'i:UCbbbbbbbbbbbbbbbbbbbbbb': 200.0},
        )
        buf: StringIO = StringIO()
        with patch('sys.stdout', buf), patch(
            'sys.stderr', StringIO(),
        ):
            rc: int = await cmd_export(
                self._ns('scheduled', tier=1), self.queue,
            )
        self.assertEqual(rc, 0)
        rows: list[dict] = self._parse_jsonl(buf.getvalue())
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            rows[0]['member'],
            'i:UCbbbbbbbbbbbbbbbbbbbbbb',
        )
        self.assertEqual(rows[0]['tier'], 1)

    async def test_export_pending_resolution_uses_unresolved_zset(
        self,
    ) -> None:
        await self.redis.zadd(
            'youtube:channel:queue:unresolved',
            {'h:somehandle': 42.0},
        )
        await self.redis.hset(
            'youtube:channel:meta:h:somehandle',
            mapping={
                'handle': 'somehandle',
                'state': 'pending_resolution',
            },
        )
        buf: StringIO = StringIO()
        with patch('sys.stdout', buf), patch(
            'sys.stderr', StringIO(),
        ):
            rc: int = await cmd_export(
                self._ns('pending_resolution'), self.queue,
            )
        self.assertEqual(rc, 0)
        rows: list[dict] = self._parse_jsonl(buf.getvalue())
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['member'], 'h:somehandle')
        self.assertEqual(rows[0]['score'], 42.0)
        self.assertEqual(
            rows[0]['state'], 'pending_resolution',
        )

    async def test_export_limit_caps_output(self) -> None:
        for i in range(5):
            cid: str = f'UC{chr(ord("a") + i) * 22}'
            await self.redis.hset(
                'youtube:channel:not_found', f'i:{cid}', '',
            )
        buf: StringIO = StringIO()
        with patch('sys.stdout', buf), patch(
            'sys.stderr', StringIO(),
        ):
            rc: int = await cmd_export(
                self._ns('not_found', limit=2),
                self.queue,
            )
        self.assertEqual(rc, 0)
        rows: list[dict] = self._parse_jsonl(buf.getvalue())
        self.assertEqual(len(rows), 2)

    async def test_export_tier_flag_rejected_for_non_scheduled(
        self,
    ) -> None:
        err: StringIO = StringIO()
        with patch('sys.stdout', StringIO()), patch(
            'sys.stderr', err,
        ):
            rc: int = await cmd_export(
                self._ns('not_found', tier=0), self.queue,
            )
        self.assertEqual(rc, 2)
        self.assertIn(
            'only meaningful for state=scheduled',
            err.getvalue(),
        )

    async def test_export_tier_out_of_range_rejected(
        self,
    ) -> None:
        err: StringIO = StringIO()
        with patch('sys.stdout', StringIO()), patch(
            'sys.stderr', err,
        ):
            rc: int = await cmd_export(
                self._ns('scheduled', tier=999),
                self.queue,
            )
        self.assertEqual(rc, 2)
        self.assertIn('--tier must be in', err.getvalue())

    async def test_export_to_file(self) -> None:
        import tempfile
        await self.redis.hset(
            'youtube:channel:not_found',
            'i:UCaaaaaaaaaaaaaaaaaaaaaa', '',
        )
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.jsonl', delete=False,
        ) as fh:
            path: str = fh.name
        try:
            with patch('sys.stderr', StringIO()):
                rc: int = await cmd_export(
                    self._ns(
                        'not_found', output=path,
                    ),
                    self.queue,
                )
            self.assertEqual(rc, 0)
            with open(path, encoding='utf-8') as fh:
                contents: str = fh.read()
            rows: list[dict] = self._parse_jsonl(contents)
            self.assertEqual(len(rows), 1)
            self.assertEqual(
                rows[0]['member'],
                'i:UCaaaaaaaaaaaaaaaaaaaaaa',
            )
        finally:
            os.unlink(path)


class TestExportRssCommand(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self.redis = fakeredis.aioredis.FakeRedis(
            decode_responses=True,
        )
        self.queue = RedisChannelScrapeQueue(
            self.redis, ChannelScrapeQueueSettings(),
        )

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    def _ns(
        self,
        kind: str,
        *,
        tier: int | None = None,
        output: str | None = None,
        batch_size: int = 1000,
        limit: int | None = None,
    ) -> argparse.Namespace:
        return argparse.Namespace(
            kind=kind,
            tier=tier,
            output=output,
            batch_size=batch_size,
            limit=limit,
        )

    def _parse_jsonl(self, payload: str) -> list[dict]:
        return [
            json.loads(line)
            for line in payload.splitlines()
            if line.strip()
        ]

    async def test_export_creators(self) -> None:
        await self.redis.hset(
            'rss:youtube:creators',
            mapping={
                'UCaaaaaaaaaaaaaaaaaaaaaa': 'alpha',
                'UCbbbbbbbbbbbbbbbbbbbbbb': 'beta',
            },
        )
        buf: StringIO = StringIO()
        with patch('sys.stdout', buf), patch(
            'sys.stderr', StringIO(),
        ):
            rc: int = await cmd_export_rss(
                self._ns('creators'), self.queue,
            )
        self.assertEqual(rc, 0)
        rows: list[dict] = self._parse_jsonl(buf.getvalue())
        self.assertEqual(len(rows), 2)
        by_cid: dict[str, dict] = {
            r['channel_id']: r for r in rows
        }
        self.assertEqual(
            by_cid['UCaaaaaaaaaaaaaaaaaaaaaa']['label'],
            'alpha',
        )
        self.assertEqual(
            by_cid['UCaaaaaaaaaaaaaaaaaaaaaa']['kind'],
            'creators',
        )

    async def test_export_suppressed_parses_envelope(
        self,
    ) -> None:
        envelope: str = json.dumps({
            'reason': 'not_found_confirmed',
            'source': 'reconcile_rss_suppress',
            'ts': '2026-05-24T10:00:00Z',
        })
        await self.redis.hset(
            'rss:youtube:suppressed',
            'UCaaaaaaaaaaaaaaaaaaaaaa', envelope,
        )
        buf: StringIO = StringIO()
        with patch('sys.stdout', buf), patch(
            'sys.stderr', StringIO(),
        ):
            rc: int = await cmd_export_rss(
                self._ns('suppressed'), self.queue,
            )
        self.assertEqual(rc, 0)
        rows: list[dict] = self._parse_jsonl(buf.getvalue())
        self.assertEqual(len(rows), 1)
        row: dict = rows[0]
        self.assertEqual(
            row['channel_id'], 'UCaaaaaaaaaaaaaaaaaaaaaa',
        )
        self.assertEqual(row['kind'], 'suppressed')
        self.assertEqual(row['reason'], 'not_found_confirmed')
        self.assertEqual(
            row['source'], 'reconcile_rss_suppress',
        )
        self.assertEqual(row['ts'], '2026-05-24T10:00:00Z')

    async def test_export_suppressed_falls_back_to_raw_value(
        self,
    ) -> None:
        await self.redis.hset(
            'rss:youtube:suppressed',
            'UCaaaaaaaaaaaaaaaaaaaaaa',
            'not-json-at-all',
        )
        buf: StringIO = StringIO()
        with patch('sys.stdout', buf), patch(
            'sys.stderr', StringIO(),
        ):
            rc: int = await cmd_export_rss(
                self._ns('suppressed'), self.queue,
            )
        self.assertEqual(rc, 0)
        rows: list[dict] = self._parse_jsonl(buf.getvalue())
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            rows[0]['raw_value'], 'not-json-at-all',
        )
        self.assertNotIn('reason', rows[0])

    async def test_export_tiers_coerces_int(self) -> None:
        await self.redis.hset(
            'rss:youtube:tiers',
            mapping={
                'UCaaaaaaaaaaaaaaaaaaaaaa': '2',
                'UCbbbbbbbbbbbbbbbbbbbbbb': 'not-an-int',
            },
        )
        buf: StringIO = StringIO()
        with patch('sys.stdout', buf), patch(
            'sys.stderr', StringIO(),
        ):
            rc: int = await cmd_export_rss(
                self._ns('tiers'), self.queue,
            )
        self.assertEqual(rc, 0)
        rows: list[dict] = self._parse_jsonl(buf.getvalue())
        by_cid: dict[str, dict] = {
            r['channel_id']: r for r in rows
        }
        self.assertEqual(
            by_cid['UCaaaaaaaaaaaaaaaaaaaaaa']['tier'], 2,
        )
        self.assertEqual(
            by_cid['UCbbbbbbbbbbbbbbbbbbbbbb']['tier'],
            'not-an-int',
        )

    async def test_export_queue_all_tiers_includes_score_and_tier(
        self,
    ) -> None:
        await self.redis.zadd(
            'rss:youtube:queue:1',
            {'UCaaaaaaaaaaaaaaaaaaaaaa': 100.0},
        )
        await self.redis.zadd(
            'rss:youtube:queue:3',
            {'UCbbbbbbbbbbbbbbbbbbbbbb': 300.0},
        )
        buf: StringIO = StringIO()
        with patch('sys.stdout', buf), patch(
            'sys.stderr', StringIO(),
        ):
            rc: int = await cmd_export_rss(
                self._ns('queue'), self.queue,
            )
        self.assertEqual(rc, 0)
        rows: list[dict] = self._parse_jsonl(buf.getvalue())
        self.assertEqual(len(rows), 2)
        by_tier: dict[int, dict] = {
            r['tier']: r for r in rows
        }
        self.assertEqual(set(by_tier.keys()), {1, 3})
        self.assertEqual(by_tier[1]['score'], 100.0)
        self.assertEqual(by_tier[3]['score'], 300.0)
        self.assertEqual(by_tier[1]['kind'], 'queue')

    async def test_export_queue_tier_filter(self) -> None:
        await self.redis.zadd(
            'rss:youtube:queue:1',
            {'UCaaaaaaaaaaaaaaaaaaaaaa': 100.0},
        )
        await self.redis.zadd(
            'rss:youtube:queue:3',
            {'UCbbbbbbbbbbbbbbbbbbbbbb': 300.0},
        )
        buf: StringIO = StringIO()
        with patch('sys.stdout', buf), patch(
            'sys.stderr', StringIO(),
        ):
            rc: int = await cmd_export_rss(
                self._ns('queue', tier=3), self.queue,
            )
        self.assertEqual(rc, 0)
        rows: list[dict] = self._parse_jsonl(buf.getvalue())
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['tier'], 3)
        self.assertEqual(
            rows[0]['channel_id'],
            'UCbbbbbbbbbbbbbbbbbbbbbb',
        )

    async def test_export_rss_tier_rejected_for_non_queue(
        self,
    ) -> None:
        err: StringIO = StringIO()
        with patch('sys.stdout', StringIO()), patch(
            'sys.stderr', err,
        ):
            rc: int = await cmd_export_rss(
                self._ns('creators', tier=0),
                self.queue,
            )
        self.assertEqual(rc, 2)
        self.assertIn(
            'only meaningful for kind=queue',
            err.getvalue(),
        )

    async def test_export_rss_limit_caps_output(self) -> None:
        for i in range(5):
            cid: str = f'UC{chr(ord("a") + i) * 22}'
            await self.redis.hset(
                'rss:youtube:creators', cid, f'label-{i}',
            )
        buf: StringIO = StringIO()
        with patch('sys.stdout', buf), patch(
            'sys.stderr', StringIO(),
        ):
            rc: int = await cmd_export_rss(
                self._ns('creators', limit=2),
                self.queue,
            )
        self.assertEqual(rc, 0)
        rows: list[dict] = self._parse_jsonl(buf.getvalue())
        self.assertEqual(len(rows), 2)


class TestAddClassificationReport(
    unittest.IsolatedAsyncioTestCase,
):
    '''The 'add' subcommand prints a state-by-state report
    of how many input channels were already present in the
    queue under each ChannelState before the enqueue ran.
    '''

    async def asyncSetUp(self) -> None:
        self.redis = fakeredis.aioredis.FakeRedis(
            decode_responses=True,
        )
        self.queue = RedisChannelScrapeQueue(
            self.redis, ChannelScrapeQueueSettings(),
        )

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    async def test_report_counts_each_existing_state(
        self,
    ) -> None:
        await self.redis.hset(
            'youtube:channel:meta:i:UCaaaaaaaaaaaaaaaaaaaaaa',
            mapping={
                'channel_id': 'UCaaaaaaaaaaaaaaaaaaaaaa',
                'state': 'scheduled',
            },
        )
        await self.redis.hset(
            'youtube:channel:meta:i:UCbbbbbbbbbbbbbbbbbbbbbb',
            mapping={
                'channel_id': 'UCbbbbbbbbbbbbbbbbbbbbbb',
                'state': 'not_found',
            },
        )
        await self.redis.hset(
            'youtube:channel:meta:i:UCcccccccccccccccccccccc',
            mapping={
                'channel_id': 'UCcccccccccccccccccccccc',
                'state': 'terminated',
            },
        )
        ns: argparse.Namespace = argparse.Namespace(
            keys=[
                'UCaaaaaaaaaaaaaaaaaaaaaa',
                'UCbbbbbbbbbbbbbbbbbbbbbb',
                'UCcccccccccccccccccccccc',
                'UCdddddddddddddddddddddd',
            ],
            priority=False,
            source='cli',
        )
        from tools.yt_channel_queue import cmd_add
        buf: StringIO = StringIO()
        with patch('sys.stdout', buf):
            rc: int = await cmd_add(ns, self.queue)
        self.assertEqual(rc, 0)
        out: str = buf.getvalue()
        self.assertIn('added 4 input(s):', out)
        self.assertIn('pre-existing states:', out)
        self.assertRegex(out, r'scheduled\s+1')
        self.assertRegex(out, r'not_found\s+1')
        self.assertRegex(out, r'terminated\s+1')
        self.assertRegex(out, r'new\s+1')

    async def test_report_orders_states_by_enum(
        self,
    ) -> None:
        await self.redis.hset(
            'youtube:channel:meta:i:UCaaaaaaaaaaaaaaaaaaaaaa',
            mapping={
                'channel_id': 'UCaaaaaaaaaaaaaaaaaaaaaa',
                'state': 'not_found',
            },
        )
        await self.redis.hset(
            'youtube:channel:meta:i:UCbbbbbbbbbbbbbbbbbbbbbb',
            mapping={
                'channel_id': 'UCbbbbbbbbbbbbbbbbbbbbbb',
                'state': 'scheduled',
            },
        )
        ns: argparse.Namespace = argparse.Namespace(
            keys=[
                'UCaaaaaaaaaaaaaaaaaaaaaa',
                'UCbbbbbbbbbbbbbbbbbbbbbb',
            ],
            priority=False,
            source='cli',
        )
        from tools.yt_channel_queue import cmd_add
        buf: StringIO = StringIO()
        with patch('sys.stdout', buf):
            await cmd_add(ns, self.queue)
        out: str = buf.getvalue()
        sched_idx: int = out.find('scheduled')
        nf_idx: int = out.find('not_found')
        self.assertLess(
            sched_idx, nf_idx,
            msg='scheduled must appear before not_found '
                '(enum order)',
        )

    async def test_report_omits_zero_count_rows(
        self,
    ) -> None:
        ns: argparse.Namespace = argparse.Namespace(
            keys=['UCaaaaaaaaaaaaaaaaaaaaaa'],
            priority=False,
            source='cli',
        )
        from tools.yt_channel_queue import cmd_add
        buf: StringIO = StringIO()
        with patch('sys.stdout', buf):
            await cmd_add(ns, self.queue)
        out: str = buf.getvalue()
        self.assertIn('new', out)
        self.assertNotIn('not_found', out)
        self.assertNotIn('terminated', out)
        self.assertNotIn('scheduled', out)


class TestImportClassificationReport(
    unittest.IsolatedAsyncioTestCase,
):
    '''The 'import' subcommand prints both the legacy
    'imported: X resolved, Y unresolved' line AND a
    state-by-state report.'''

    async def asyncSetUp(self) -> None:
        self.redis = fakeredis.aioredis.FakeRedis(
            decode_responses=True,
        )
        self.queue = RedisChannelScrapeQueue(
            self.redis, ChannelScrapeQueueSettings(),
        )

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    async def test_import_emits_report_alongside_legacy_line(
        self,
    ) -> None:
        await self.redis.hset(
            'youtube:channel:meta:i:UCaaaaaaaaaaaaaaaaaaaaaa',
            mapping={
                'channel_id': 'UCaaaaaaaaaaaaaaaaaaaaaa',
                'state': 'not_found',
            },
        )
        td: str = tempfile.mkdtemp()
        try:
            path: str = os.path.join(td, 'c.lst')
            with open(path, 'w') as fh:
                fh.write(
                    json.dumps({
                        'channel_id': (
                            'UCaaaaaaaaaaaaaaaaaaaaaa'
                        ),
                        'channel_handle': None,
                    }) + '\n',
                )
                fh.write(
                    json.dumps({
                        'channel_id': (
                            'UCbbbbbbbbbbbbbbbbbbbbbb'
                        ),
                        'channel_handle': None,
                    }) + '\n',
                )
            ns: argparse.Namespace = argparse.Namespace(
                file=path, replace=False, merge=True,
            )
            from tools.yt_channel_queue import cmd_import
            buf: StringIO = StringIO()
            with patch('sys.stdout', buf):
                rc: int = await cmd_import(ns, self.queue)
            self.assertEqual(rc, 0)
            out: str = buf.getvalue()
            self.assertIn('imported:', out)
            self.assertIn('processed 2 input(s):', out)
            self.assertIn('pre-existing states:', out)
            self.assertRegex(out, r'not_found\s+1')
            self.assertRegex(out, r'new\s+1')
        finally:
            shutil.rmtree(td)


class TestExportChannelHandleEnrichment(
    unittest.IsolatedAsyncioTestCase,
):
    '''cmd_export enriches records with the channel_handle looked up
    from the ``youtube:creator_map`` (id->handle) Redis hash.'''

    async def asyncSetUp(self) -> None:
        self.redis = fakeredis.aioredis.FakeRedis(
            decode_responses=True,
        )
        self.queue = RedisChannelScrapeQueue(
            self.redis, ChannelScrapeQueueSettings(),
        )

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    def _ns(self, state: str) -> argparse.Namespace:
        return argparse.Namespace(
            state=state, tier=None, output=None,
            batch_size=1000, limit=None,
        )

    async def _export_rows(self, state: str) -> list[dict]:
        buf: StringIO = StringIO()
        with patch('sys.stdout', buf), patch(
            'sys.stderr', StringIO(),
        ):
            rc: int = await cmd_export(self._ns(state), self.queue)
        self.assertEqual(rc, 0)
        return [
            json.loads(line)
            for line in buf.getvalue().splitlines()
            if line.strip()
        ]

    async def test_handle_added_from_creator_map(self) -> None:
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:0',
            {'i:UCaaaaaaaaaaaaaaaaaaaaaa': 100.0},
        )
        await self.redis.hset(
            'youtube:channel:meta:i:UCaaaaaaaaaaaaaaaaaaaaaa',
            mapping={
                'channel_id': 'UCaaaaaaaaaaaaaaaaaaaaaa',
                'state': 'scheduled',
            },
        )
        await self.redis.hset(
            'youtube:creator_map',
            'UCaaaaaaaaaaaaaaaaaaaaaa', '@alpha',
        )
        rows: list[dict] = await self._export_rows('scheduled')
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['channel_handle'], '@alpha')

    async def test_channel_id_derived_from_member_id(self) -> None:
        # Meta lacks channel_id; it is recovered from the i: member.
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:0',
            {'i:UCcccccccccccccccccccccc': 1.0},
        )
        await self.redis.hset(
            'youtube:channel:meta:i:UCcccccccccccccccccccccc',
            mapping={'state': 'scheduled'},
        )
        await self.redis.hset(
            'youtube:creator_map',
            'UCcccccccccccccccccccccc', '@gamma',
        )
        rows: list[dict] = await self._export_rows('scheduled')
        self.assertEqual(rows[0]['channel_handle'], '@gamma')

    async def test_meta_handle_wins_no_override(self) -> None:
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:0',
            {'i:UCbbbbbbbbbbbbbbbbbbbbbb': 1.0},
        )
        await self.redis.hset(
            'youtube:channel:meta:i:UCbbbbbbbbbbbbbbbbbbbbbb',
            mapping={
                'channel_id': 'UCbbbbbbbbbbbbbbbbbbbbbb',
                'handle': 'metahandle',
                'state': 'scheduled',
            },
        )
        await self.redis.hset(
            'youtube:creator_map',
            'UCbbbbbbbbbbbbbbbbbbbbbb', '@different',
        )
        rows: list[dict] = await self._export_rows('scheduled')
        self.assertEqual(rows[0]['handle'], 'metahandle')
        self.assertNotIn('channel_handle', rows[0])

    async def test_no_creator_map_entry_omits_field(self) -> None:
        await self.redis.zadd(
            'youtube:channel:queue:scheduled:0',
            {'i:UCdddddddddddddddddddddd': 1.0},
        )
        await self.redis.hset(
            'youtube:channel:meta:i:UCdddddddddddddddddddddd',
            mapping={
                'channel_id': 'UCdddddddddddddddddddddd',
                'state': 'scheduled',
            },
        )
        rows: list[dict] = await self._export_rows('scheduled')
        self.assertNotIn('channel_handle', rows[0])
