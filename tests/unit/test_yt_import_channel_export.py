'''
Tests for tools.yt_import_channel_export.

Importer behaviours:
- Silent fill when Redis is empty.
- Casing-only diffs auto-resolve to the mixed-case variant.
- True handle conflicts are deferred to the channel scraper via
  queue.enqueue_scheduled(channel_id, source='importer',
  priority=True).
- InconsistentIdentityError during bind() enqueues BOTH
  channel_ids involved.
- Empty / unparseable JSONL lines don't crash the run.
'''

import io
import unittest
from typing import Iterable
from unittest.mock import AsyncMock, MagicMock

from scrape_exchange.creator_map import InMemoryCreatorMap
from scrape_exchange.handle_map import NullHandleMap
from scrape_exchange.youtube.channel_identity import (
    ChannelIdentityStore,
)


def _make_jsonl(lines: Iterable[str]) -> io.StringIO:
    return io.StringIO('\n'.join(lines) + '\n')


def _make_queue() -> MagicMock:
    '''Return an AsyncMock-based queue stub.'''
    q: MagicMock = MagicMock()
    q.enqueue_scheduled = AsyncMock()
    q.enqueue_unresolved = AsyncMock()
    return q


class _ImporterTestCase(unittest.IsolatedAsyncioTestCase):
    '''Common scaffolding: a ChannelIdentityStore and a mock
    queue for every test.'''

    async def asyncSetUp(self) -> None:
        self.queue: MagicMock = _make_queue()
        self.store: ChannelIdentityStore = ChannelIdentityStore(
            creator_map=InMemoryCreatorMap(),
            handle_map=NullHandleMap(),
        )


class TestImporterNonDestructive(_ImporterTestCase):
    async def test_fill_silently_when_redis_empty(self) -> None:
        from tools.yt_import_channel_export import run_import
        jsonl: io.StringIO = _make_jsonl([
            '{"channel_id":"UC1","channel_handle":"Foo",'
            '"title":"FooTitle"}',
        ])
        summary = await run_import(
            jsonl, store=self.store,
            queue=self.queue,
        )
        self.assertEqual(summary.filled, 1)
        self.assertEqual(summary.deferred_to_scraper, 0)
        self.assertEqual(
            await self.store.creator_map.get('UC1'), 'Foo',
        )
        self.assertEqual(
            await self.store.handle_map.get('Foo'), 'UC1',
        )
        self.queue.enqueue_scheduled.assert_not_called()

    async def test_existing_match_is_no_op(self) -> None:
        from tools.yt_import_channel_export import run_import
        await self.store.bind('UC1', 'Foo')
        jsonl: io.StringIO = _make_jsonl([
            '{"channel_id":"UC1","channel_handle":"Foo"}',
        ])
        summary = await run_import(
            jsonl, store=self.store,
            queue=self.queue,
        )
        self.assertEqual(summary.no_change, 1)
        self.assertEqual(summary.deferred_to_scraper, 0)
        self.queue.enqueue_scheduled.assert_not_called()


class TestImporterCasingAutoResolve(_ImporterTestCase):
    async def test_redis_lowercase_takes_jsonl_mixed(
        self,
    ) -> None:
        from tools.yt_import_channel_export import run_import
        await self.store.bind('UC1', 'linustechtips')
        jsonl: io.StringIO = _make_jsonl([
            '{"channel_id":"UC1","channel_handle":'
            '"LinusTechTips"}',
        ])
        summary = await run_import(
            jsonl, store=self.store,
            queue=self.queue,
        )
        self.assertEqual(summary.casing_auto_resolved, 1)
        self.assertEqual(summary.deferred_to_scraper, 0)
        self.assertEqual(
            await self.store.creator_map.get('UC1'),
            'LinusTechTips',
        )
        self.queue.enqueue_scheduled.assert_not_called()

    async def test_jsonl_lowercase_takes_redis_mixed(
        self,
    ) -> None:
        from tools.yt_import_channel_export import run_import
        await self.store.bind('UC1', 'LinusTechTips')
        jsonl: io.StringIO = _make_jsonl([
            '{"channel_id":"UC1","channel_handle":'
            '"linustechtips"}',
        ])
        summary = await run_import(
            jsonl, store=self.store,
            queue=self.queue,
        )
        self.assertEqual(summary.casing_auto_resolved, 1)
        self.assertEqual(summary.deferred_to_scraper, 0)
        self.assertEqual(
            await self.store.creator_map.get('UC1'),
            'LinusTechTips',
        )
        self.queue.enqueue_scheduled.assert_not_called()

    async def test_both_mixed_case_defers_to_scraper(
        self,
    ) -> None:
        '''Two non-equal mixed-case variants is ambiguous —
        deferred to the scraper via a queue enqueue.'''
        from tools.yt_import_channel_export import run_import
        await self.store.bind('UC1', 'LinusTechTips')
        jsonl: io.StringIO = _make_jsonl([
            '{"channel_id":"UC1","channel_handle":'
            '"LinusTechtips"}',
        ])
        summary = await run_import(
            jsonl, store=self.store,
            queue=self.queue,
        )
        self.assertEqual(summary.casing_auto_resolved, 0)
        self.assertEqual(summary.deferred_to_scraper, 1)
        self.queue.enqueue_scheduled.assert_called_once_with(
            'UC1', source='importer', priority=True,
        )


class TestImporterHandleConflictDefersToScraper(_ImporterTestCase):
    async def test_handle_disagreement_enqueues_channel_id(
        self,
    ) -> None:
        from tools.yt_import_channel_export import run_import
        await self.store.bind('UC1', 'OldName')
        jsonl: io.StringIO = _make_jsonl([
            '{"channel_id":"UC1","channel_handle":"NewName"}',
        ])
        summary = await run_import(
            jsonl, store=self.store,
            queue=self.queue,
        )
        self.assertEqual(summary.deferred_to_scraper, 1)
        # Redis remains untouched — the scraper will overwrite
        # via its normal pipeline.
        self.assertEqual(
            await self.store.creator_map.get('UC1'), 'OldName',
        )
        self.queue.enqueue_scheduled.assert_called_once_with(
            'UC1', source='importer', priority=True,
        )

    async def test_multiple_conflicts_enqueue_each_channel_id(
        self,
    ) -> None:
        from tools.yt_import_channel_export import run_import
        await self.store.bind('UC1', 'A_redis')
        await self.store.bind('UC2', 'B_redis')
        jsonl: io.StringIO = _make_jsonl([
            '{"channel_id":"UC1","channel_handle":"A_jsonl"}',
            '{"channel_id":"UC2","channel_handle":"B_jsonl"}',
        ])
        summary = await run_import(
            jsonl, store=self.store,
            queue=self.queue,
        )
        self.assertEqual(summary.deferred_to_scraper, 2)
        calls = {
            call.args[0]
            for call in
            self.queue.enqueue_scheduled.call_args_list
        }
        self.assertEqual(calls, {'UC1', 'UC2'})


class TestImporterInconsistentBindEnqueuesBoth(_ImporterTestCase):
    '''When ``bind()`` raises InconsistentIdentityError (the
    handle is already bound to a *different* channel_id), the
    importer enqueues BOTH channel_ids at priority so the
    scraper resolves which is authoritative.'''

    async def test_both_channel_ids_enqueued(self) -> None:
        from tools.yt_import_channel_export import run_import
        # Seed handle_map: 'GCN' currently bound to UC_a.
        await self.store.bind('UC_a', 'GCN')
        # JSONL says UC_b owns 'GCN' too — collision.
        jsonl: io.StringIO = _make_jsonl([
            '{"channel_id":"UC_b","channel_handle":"GCN"}',
            # Unrelated fresh record still lands.
            '{"channel_id":"UC_c","channel_handle":"Fresh"}',
        ])
        summary = await run_import(
            jsonl, store=self.store,
            queue=self.queue,
        )
        self.assertEqual(summary.skipped_inconsistent, 1)
        self.assertEqual(summary.filled, 1)
        # Both channel_ids of the collision get enqueued.
        enqueued_ids = {
            call.args[0]
            for call in
            self.queue.enqueue_scheduled.call_args_list
        }
        self.assertIn('UC_a', enqueued_ids)
        self.assertIn('UC_b', enqueued_ids)
        # The fresh record landed without being enqueued.
        fresh_ids = {
            call.args[0]
            for call in
            self.queue.enqueue_scheduled.call_args_list
        }
        self.assertNotIn('UC_c', fresh_ids)
        self.assertEqual(
            await self.store.creator_map.get('UC_c'), 'Fresh',
        )
        # UC_b never gained a creator_map entry.
        self.assertIsNone(
            await self.store.creator_map.get('UC_b'),
        )


class TestImporterParseError(_ImporterTestCase):
    async def test_unparseable_line_counted_and_run_continues(
        self,
    ) -> None:
        from tools.yt_import_channel_export import run_import
        jsonl: io.StringIO = _make_jsonl([
            'this is not json',
            '{"channel_id":"UC1","channel_handle":"Foo"}',
        ])
        summary = await run_import(
            jsonl, store=self.store,
            queue=self.queue,
        )
        self.assertEqual(summary.parse_errors, 1)
        self.assertEqual(summary.filled, 1)


if __name__ == '__main__':
    unittest.main()
