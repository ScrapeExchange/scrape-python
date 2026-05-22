import unittest
from unittest.mock import AsyncMock, patch

from scrape_exchange.creator_map import InMemoryCreatorMap
from scrape_exchange.handle_map import NullHandleMap
from scrape_exchange.youtube.channel_identity import (
    ChannelIdentityStore,
)


class TestInlineResolve(unittest.IsolatedAsyncioTestCase):
    async def test_id_only_entry_resolves_and_persists(
        self,
    ) -> None:
        '''channel_id resolves via channel_identity and binds
        both directions.'''
        store: ChannelIdentityStore = ChannelIdentityStore(
            creator_map=InMemoryCreatorMap(),
            handle_map=NullHandleMap(),
        )
        with patch(
            'tools.yt_channel_scrape.resolve_channel_id',
            new=AsyncMock(return_value='ResolvedHandle'),
        ):
            from tools.yt_channel_scrape import (
                resolve_and_bind_for_entry,
            )
            handle: str | None = (
                await resolve_and_bind_for_entry(
                    channel_id='UC1', store=store,
                )
            )
        self.assertEqual(handle, 'ResolvedHandle')
        self.assertEqual(
            await store.creator_map.get('UC1'),
            'ResolvedHandle',
        )
        self.assertEqual(
            await store.handle_map.get('ResolvedHandle'),
            'UC1',
        )

    async def test_unresolvable_returns_none_no_bind(
        self,
    ) -> None:
        '''resolve_channel_id returns None -> no bind happens.'''
        store: ChannelIdentityStore = ChannelIdentityStore(
            creator_map=InMemoryCreatorMap(),
            handle_map=NullHandleMap(),
        )
        with patch(
            'tools.yt_channel_scrape.resolve_channel_id',
            new=AsyncMock(return_value=None),
        ):
            from tools.yt_channel_scrape import (
                resolve_and_bind_for_entry,
            )
            handle: str | None = (
                await resolve_and_bind_for_entry(
                    channel_id='UC1', store=store,
                )
            )
        self.assertIsNone(handle)
        self.assertIsNone(await store.creator_map.get('UC1'))
