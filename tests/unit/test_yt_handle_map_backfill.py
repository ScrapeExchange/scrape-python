import asyncio
import unittest

from scrape_exchange.creator_map import InMemoryCreatorMap
from scrape_exchange.handle_map import NullHandleMap
from scrape_exchange.youtube.channel_identity import (
    ChannelIdentityStore,
)


class TestBackfill(unittest.IsolatedAsyncioTestCase):
    async def test_mirrors_creator_map_into_handle_map(
        self,
    ) -> None:
        from tools.yt_handle_map_backfill import run_backfill
        creator: InMemoryCreatorMap = InMemoryCreatorMap()
        await creator.put('UC1', 'Foo')
        await creator.put('UC2', 'Bar')
        handle: NullHandleMap = NullHandleMap()
        store: ChannelIdentityStore = ChannelIdentityStore(
            creator_map=creator, handle_map=handle,
        )
        summary = await run_backfill(store)
        self.assertEqual(summary.processed, 2)
        self.assertEqual(summary.skipped_invalid, 0)
        self.assertEqual(await handle.get('Foo'), 'UC1')
        self.assertEqual(await handle.get('Bar'), 'UC2')

    async def test_skips_invalid_handles(self) -> None:
        from tools.yt_handle_map_backfill import run_backfill
        creator: InMemoryCreatorMap = InMemoryCreatorMap()
        await creator.put('UC1', 'Foo')
        await creator.put('UC2', 'with space')
        handle: NullHandleMap = NullHandleMap()
        store: ChannelIdentityStore = ChannelIdentityStore(
            creator_map=creator, handle_map=handle,
        )
        summary = await run_backfill(store)
        self.assertEqual(summary.processed, 1)
        self.assertEqual(summary.skipped_invalid, 1)
        self.assertEqual(summary.skipped_inconsistent, 0)

    async def test_skips_inconsistent_handle_bindings(
        self,
    ) -> None:
        '''When two channel_ids share a handle in
        creator_map, the second bind raises
        InconsistentIdentityError; backfill swallows it and
        counts the skip.'''
        from tools.yt_handle_map_backfill import run_backfill
        creator: InMemoryCreatorMap = InMemoryCreatorMap()
        await creator.put('UC1', 'KmiX')
        await creator.put('UC2', 'KmiX')  # same handle, conflict
        handle: NullHandleMap = NullHandleMap()
        store: ChannelIdentityStore = ChannelIdentityStore(
            creator_map=creator, handle_map=handle,
        )
        summary = await run_backfill(store)
        self.assertEqual(summary.processed, 1)
        self.assertEqual(summary.skipped_invalid, 0)
        self.assertEqual(summary.skipped_inconsistent, 1)
        # First binding wins.
        self.assertEqual(await handle.get('KmiX'), 'UC1')
