'''
Unit tests for ChannelIdentityStore.bind().

Uses InMemoryCreatorMap paired with NullHandleMap (handle_map.py,
also stateful) so both directions of the lookup triangle can be
verified without touching Redis.
'''

import asyncio
import unittest

from scrape_exchange.creator_map import InMemoryCreatorMap
from scrape_exchange.handle_map import NullHandleMap
from scrape_exchange.youtube.channel_identity import (
    ChannelIdentityStore,
)


class TestChannelIdentityBind(unittest.TestCase):
    def _store(self) -> ChannelIdentityStore:
        return ChannelIdentityStore(
            creator_map=InMemoryCreatorMap(),
            handle_map=NullHandleMap(),
        )

    def test_bind_writes_both_directions(self) -> None:
        store = self._store()
        asyncio.run(store.bind('UC1', 'LinusTechTips'))
        self.assertEqual(
            asyncio.run(store.creator_map.get('UC1')),
            'LinusTechTips',
        )
        self.assertEqual(
            asyncio.run(store.handle_map.get('LinusTechTips')),
            'UC1',
        )

    def test_bind_idempotent(self) -> None:
        store = self._store()
        asyncio.run(store.bind('UC1', 'Foo'))
        asyncio.run(store.bind('UC1', 'Foo'))
        self.assertEqual(
            asyncio.run(store.creator_map.get('UC1')), 'Foo',
        )
        self.assertEqual(
            asyncio.run(store.handle_map.get('Foo')), 'UC1',
        )

    def test_bind_overwrites_handle_for_same_id(self) -> None:
        store = self._store()
        asyncio.run(store.bind('UC1', 'OldName'))
        asyncio.run(store.bind('UC1', 'NewName'))
        self.assertEqual(
            asyncio.run(store.creator_map.get('UC1')),
            'NewName',
        )
        self.assertEqual(
            asyncio.run(store.handle_map.get('NewName')),
            'UC1',
        )

    def test_bind_rejects_empty_id(self) -> None:
        store = self._store()
        with self.assertRaises(ValueError):
            asyncio.run(store.bind('', 'Foo'))

    def test_bind_rejects_empty_handle(self) -> None:
        store = self._store()
        with self.assertRaises(ValueError):
            asyncio.run(store.bind('UC1', ''))

    def test_bind_strips_leading_at(self) -> None:
        store = self._store()
        asyncio.run(store.bind('UC1', '@Foo'))
        self.assertEqual(
            asyncio.run(store.creator_map.get('UC1')),
            'Foo',
        )
        self.assertEqual(
            asyncio.run(store.handle_map.get('Foo')), 'UC1',
        )

    def test_bind_rejects_handle_with_whitespace(self) -> None:
        store = self._store()
        with self.assertRaises(ValueError):
            asyncio.run(store.bind('UC1', 'with space'))

    def test_bind_rejects_handle_with_slash(self) -> None:
        store = self._store()
        with self.assertRaises(ValueError):
            asyncio.run(store.bind('UC1', 'with/slash'))

    def test_bind_rejects_lowercase_uc_prefix(self) -> None:
        # Lowercase 'uc' prefix is never a valid YouTube
        # channel_id. ``bind()`` must reject so the maps cannot
        # be corrupted from any code path that bypasses
        # ``normalise_channel_id``.
        store = self._store()
        with self.assertRaises(ValueError):
            asyncio.run(
                store.bind(
                    'uc1toeoupkjz1qclgx-u8yfa', 'SomeHandle',
                ),
            )

    def test_bind_rejects_mixed_case_uc_prefix(self) -> None:
        store = self._store()
        with self.assertRaises(ValueError):
            asyncio.run(
                store.bind(
                    'Uc1toeoupkjz1qclgx-u8YFA', 'SomeHandle',
                ),
            )

    def test_bind_accepts_canonical_uppercase_prefix(self) -> None:
        # Mixed-case body is fine; the YouTube channel_id alphabet
        # is case-sensitive base64url. Only the prefix is
        # constrained.
        store = self._store()
        asyncio.run(
            store.bind('UC1ToEoUPkjz1qcLGX-u8YFA', 'SomeHandle'),
        )
        self.assertEqual(
            asyncio.run(
                store.creator_map.get(
                    'UC1ToEoUPkjz1qcLGX-u8YFA',
                ),
            ),
            'SomeHandle',
        )


class TestBindInconsistencyDetection(
    unittest.IsolatedAsyncioTestCase,
):
    async def test_raises_when_handle_bound_to_different_id(
        self,
    ) -> None:
        '''bind() raises InconsistentIdentityError when the
        handle is already bound to a different channel_id.'''
        from scrape_exchange.youtube.channel_identity import (
            InconsistentIdentityError,
        )
        store: ChannelIdentityStore = ChannelIdentityStore(
            creator_map=InMemoryCreatorMap(),
            handle_map=NullHandleMap(),
        )
        await store.bind('UC1', 'SharedHandle')
        with self.assertRaises(InconsistentIdentityError):
            await store.bind('UC2', 'SharedHandle')
        # Maps remain in their pre-failed-bind state.
        self.assertEqual(
            await store.creator_map.get('UC1'), 'SharedHandle',
        )
        self.assertIsNone(
            await store.creator_map.get('UC2'),
        )
        self.assertEqual(
            await store.handle_map.get('SharedHandle'), 'UC1',
        )

    async def test_rebind_same_id_is_idempotent(self) -> None:
        '''bind() does NOT raise when the same (id, handle) pair
        is bound twice.'''
        store: ChannelIdentityStore = ChannelIdentityStore(
            creator_map=InMemoryCreatorMap(),
            handle_map=NullHandleMap(),
        )
        await store.bind('UC1', 'Foo')
        await store.bind('UC1', 'Foo')  # idempotent — no error


class TestResolveChannelId(unittest.IsolatedAsyncioTestCase):
    async def test_returns_handle_for_known_id(self) -> None:
        '''Sanity-only: ``resolve_channel_id`` returns a non-empty
        string for a known live channel id, normalised to the rules
        accepted by ``bind()``. Skipped if INNERTUBE_LIVE=0 in env.
        '''
        import os
        if os.getenv('INNERTUBE_LIVE') != '1':
            self.skipTest('set INNERTUBE_LIVE=1 to run live test')
        from scrape_exchange.youtube.channel_identity import (
            resolve_channel_id,
        )
        handle: str | None = await resolve_channel_id(
            'UCXuqSBlHAE6Xw-yeJA0Tunw',
        )
        self.assertIsInstance(handle, str)
        self.assertTrue(handle)
        self.assertNotIn(' ', handle or '')
        self.assertNotIn('/', handle or '')


if __name__ == '__main__':
    unittest.main()
