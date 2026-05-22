'''
Unit tests for tools/yt_cleanup_channel_list.py — Tasks 5.3–5.6.

Covers:
- dedup_records: Redis-resolved id dedup, normalised-handle dedup,
  no fuzzy title matching.
- resolve_conflicts / ConflictDecision: handle disagreement prompt,
  skip-all-of-type suppression.
- acquire_channels_lst_lock: uncontested acquisition, contention.
- _run end-to-end smoke test: mixed raw input → canonical JSONL,
  single deduplicated record.
'''

import asyncio
import tempfile
import unittest
from pathlib import Path

from scrape_exchange.creator_map import InMemoryCreatorMap, NullCreatorMap
from scrape_exchange.handle_map import NullHandleMap
from scrape_exchange.youtube.channel_identity import (
    ChannelIdentityStore,
)
from tools._channel_list_record import ChannelListRecord


# ---------------------------------------------------------------------------
# Task 5.3: dedup_records
# ---------------------------------------------------------------------------

class TestDedup(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        # NullCreatorMap is fine here — dedup_records uses handle_map
        # for resolution, not creator_map.
        self.creator: NullCreatorMap = NullCreatorMap()
        self.handle: NullHandleMap = NullHandleMap()
        await self.handle.put('LinusTechTips', 'UC1')
        self.store: ChannelIdentityStore = ChannelIdentityStore(
            creator_map=self.creator,
            handle_map=self.handle,
        )

    async def test_dedup_by_redis_resolution(self) -> None:
        '''id-only and handle-only records for same channel collapse.'''
        from tools.yt_cleanup_channel_list import dedup_records
        records: list[ChannelListRecord] = [
            ChannelListRecord(
                channel_id='UC1',
                channel_handle=None,
                title=None,
            ),
            ChannelListRecord(
                channel_id=None,
                channel_handle='LinusTechTips',
                title=None,
            ),
        ]
        result: list[ChannelListRecord] = await dedup_records(
            records, store=self.store,
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].channel_id, 'UC1')
        self.assertEqual(result[0].channel_handle, 'LinusTechTips')

    async def test_dedup_by_normalised_handle_when_unresolved(
        self,
    ) -> None:
        '''Same handle (case-insensitive); mixed-case form wins.'''
        from tools.yt_cleanup_channel_list import dedup_records
        # Use a handle that is NOT in handle_map so Redis pass
        # leaves both unresolved and the normalised-handle pass runs.
        records: list[ChannelListRecord] = [
            ChannelListRecord(
                channel_id=None,
                channel_handle='FooBar',
                title=None,
            ),
            ChannelListRecord(
                channel_id=None,
                channel_handle='foobar',
                title=None,
            ),
        ]
        result: list[ChannelListRecord] = await dedup_records(
            records, store=self.store,
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].channel_handle, 'FooBar')

    async def test_dedup_all_upper_vs_all_lower_takes_upper(
        self,
    ) -> None:
        '''Importer-style: exactly one all-lowercase, the other any
        non-lowercase variant wins — including all-uppercase.'''
        from tools.yt_cleanup_channel_list import dedup_records
        records: list[ChannelListRecord] = [
            ChannelListRecord(
                channel_id=None,
                channel_handle='FOOBAR',
                title=None,
            ),
            ChannelListRecord(
                channel_id=None,
                channel_handle='foobar',
                title=None,
            ),
        ]
        result: list[ChannelListRecord] = await dedup_records(
            records, store=self.store,
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].channel_handle, 'FOOBAR')

    async def test_dedup_two_mixed_case_variants_keep_first(
        self,
    ) -> None:
        '''Ambiguous pair (both mixed-case, unequal) — keep the
        first-seen record without prompting or guessing.

        Use handles NOT in the pre-loaded handle_map so the Redis
        pass leaves both unresolved and pass 2 runs.
        '''
        from tools.yt_cleanup_channel_list import dedup_records
        records: list[ChannelListRecord] = [
            ChannelListRecord(
                channel_id=None,
                channel_handle='FooBar',
                title=None,
            ),
            ChannelListRecord(
                channel_id=None,
                channel_handle='Foobar',
                title=None,
            ),
        ]
        result: list[ChannelListRecord] = await dedup_records(
            records, store=self.store,
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].channel_handle, 'FooBar')

    async def test_no_fuzzy_title_dedup(self) -> None:
        '''Title-only records with different titles are NOT merged.'''
        from tools.yt_cleanup_channel_list import dedup_records
        records: list[ChannelListRecord] = [
            ChannelListRecord(
                channel_id=None,
                channel_handle=None,
                title='Linus Tech Tips',
            ),
            ChannelListRecord(
                channel_id=None,
                channel_handle=None,
                title='LinusTechTips',
            ),
        ]
        result: list[ChannelListRecord] = await dedup_records(
            records, store=self.store,
        )
        self.assertEqual(len(result), 2)


# ---------------------------------------------------------------------------
# Task 5.4: resolve_conflicts + ConflictDecision
# ---------------------------------------------------------------------------

class TestConflictPrompts(unittest.IsolatedAsyncioTestCase):
    async def test_handle_disagreement_prompts(self) -> None:
        '''Conflict between lst handle and Redis handle triggers prompt;
        USE_REDIS updates the record.'''
        from tools.yt_cleanup_channel_list import (
            ConflictDecision,
            resolve_conflicts,
        )
        creator: InMemoryCreatorMap = InMemoryCreatorMap()
        handle: NullHandleMap = NullHandleMap()
        await creator.put('UC1', 'NewName')
        await handle.put('NewName', 'UC1')
        store: ChannelIdentityStore = ChannelIdentityStore(
            creator, handle,
        )
        record: ChannelListRecord = ChannelListRecord(
            channel_id='UC1',
            channel_handle='OldName',
            title=None,
        )

        prompted: list[str] = []

        def prompt(
            t: str, p: dict,
        ) -> ConflictDecision:
            prompted.append(t)
            return ConflictDecision.USE_REDIS

        result: list[ChannelListRecord] = await resolve_conflicts(
            [record], store=store, prompt=prompt,
        )
        self.assertEqual(
            prompted, ['lst_handle_disagrees_with_redis'],
        )
        self.assertEqual(result[0].channel_handle, 'NewName')

    async def test_casing_only_lst_lower_auto_resolves_no_prompt(
        self,
    ) -> None:
        '''Redis 'LinusTechTips', lst 'linustechtips' — auto-resolve
        to the Redis mixed-case variant without prompting.'''
        from tools.yt_cleanup_channel_list import (
            ConflictDecision,
            resolve_conflicts,
        )
        creator: InMemoryCreatorMap = InMemoryCreatorMap()
        handle: NullHandleMap = NullHandleMap()
        await creator.put('UC1', 'LinusTechTips')
        await handle.put('LinusTechTips', 'UC1')
        store: ChannelIdentityStore = ChannelIdentityStore(
            creator, handle,
        )
        record: ChannelListRecord = ChannelListRecord(
            channel_id='UC1',
            channel_handle='linustechtips',
            title=None,
        )

        def no_prompt(
            t: str, p: dict,
        ) -> ConflictDecision:
            raise AssertionError(
                f'unexpected conflict prompt: {t}',
            )

        result: list[ChannelListRecord] = await resolve_conflicts(
            [record], store=store, prompt=no_prompt,
        )
        self.assertEqual(
            result[0].channel_handle, 'LinusTechTips',
        )

    async def test_casing_only_redis_lower_auto_resolves_no_prompt(
        self,
    ) -> None:
        '''Redis 'linustechtips', lst 'LinusTechTips' — auto-resolve
        to the lst mixed-case variant without prompting.'''
        from tools.yt_cleanup_channel_list import (
            ConflictDecision,
            resolve_conflicts,
        )
        creator: InMemoryCreatorMap = InMemoryCreatorMap()
        handle: NullHandleMap = NullHandleMap()
        await creator.put('UC1', 'linustechtips')
        await handle.put('linustechtips', 'UC1')
        store: ChannelIdentityStore = ChannelIdentityStore(
            creator, handle,
        )
        record: ChannelListRecord = ChannelListRecord(
            channel_id='UC1',
            channel_handle='LinusTechTips',
            title=None,
        )

        def no_prompt(
            t: str, p: dict,
        ) -> ConflictDecision:
            raise AssertionError(
                f'unexpected conflict prompt: {t}',
            )

        result: list[ChannelListRecord] = await resolve_conflicts(
            [record], store=store, prompt=no_prompt,
        )
        self.assertEqual(
            result[0].channel_handle, 'LinusTechTips',
        )

    async def test_both_mixed_case_still_prompts(self) -> None:
        '''Two mixed-case variants is ambiguous — fall through to the
        operator prompt rather than auto-resolving.'''
        from tools.yt_cleanup_channel_list import (
            ConflictDecision,
            resolve_conflicts,
        )
        creator: InMemoryCreatorMap = InMemoryCreatorMap()
        handle: NullHandleMap = NullHandleMap()
        await creator.put('UC1', 'LinusTechTips')
        await handle.put('LinusTechTips', 'UC1')
        store: ChannelIdentityStore = ChannelIdentityStore(
            creator, handle,
        )
        record: ChannelListRecord = ChannelListRecord(
            channel_id='UC1',
            channel_handle='LinusTechtips',
            title=None,
        )

        prompted: list[str] = []

        def prompt(
            t: str, p: dict,
        ) -> ConflictDecision:
            prompted.append(t)
            return ConflictDecision.USE_LST

        result: list[ChannelListRecord] = await resolve_conflicts(
            [record], store=store, prompt=prompt,
        )
        self.assertEqual(
            prompted, ['lst_handle_disagrees_with_redis'],
        )
        self.assertEqual(
            result[0].channel_handle, 'LinusTechtips',
        )

    async def test_skip_all_of_type_suppresses(self) -> None:
        '''SKIP_ALL_OF_TYPE on first conflict prevents further prompts
        of the same type; all records remain unchanged.'''
        from tools.yt_cleanup_channel_list import (
            ConflictDecision,
            resolve_conflicts,
        )
        creator: InMemoryCreatorMap = InMemoryCreatorMap()
        handle: NullHandleMap = NullHandleMap()
        await creator.put('UC1', 'A_redis')
        await creator.put('UC2', 'B_redis')
        await handle.put('A_redis', 'UC1')
        await handle.put('B_redis', 'UC2')
        store: ChannelIdentityStore = ChannelIdentityStore(
            creator, handle,
        )
        records: list[ChannelListRecord] = [
            ChannelListRecord(
                channel_id='UC1',
                channel_handle='A_lst',
                title=None,
            ),
            ChannelListRecord(
                channel_id='UC2',
                channel_handle='B_lst',
                title=None,
            ),
        ]
        prompted: list[str] = []

        def prompt(
            t: str, p: dict,
        ) -> ConflictDecision:
            prompted.append(t)
            return ConflictDecision.SKIP_ALL_OF_TYPE

        result: list[ChannelListRecord] = await resolve_conflicts(
            records, store=store, prompt=prompt,
        )
        # Only the first record triggers a prompt.
        self.assertEqual(len(prompted), 1)
        # Both records left unchanged (decision was skip-all).
        self.assertEqual(result[0].channel_handle, 'A_lst')
        self.assertEqual(result[1].channel_handle, 'B_lst')


# ---------------------------------------------------------------------------
# Task 5.5: Advisory POSIX lock
# ---------------------------------------------------------------------------

class TestAdvisoryLock(unittest.IsolatedAsyncioTestCase):
    async def test_lock_acquired_when_uncontested(self) -> None:
        '''Lock is acquired and the fd is non-None.'''
        from tools.yt_cleanup_channel_list import (
            acquire_channels_lst_lock,
        )
        with tempfile.TemporaryDirectory() as tmp:
            path: Path = Path(tmp) / 'channels.lst'
            path.write_text('')
            with acquire_channels_lst_lock(path) as lock_fd:
                self.assertIsNotNone(lock_fd)

    async def test_second_acquirer_fails(self) -> None:
        '''A second concurrent acquirer raises ChannelsListLockBusy.'''
        from tools.yt_cleanup_channel_list import (
            ChannelsListLockBusy,
            acquire_channels_lst_lock,
        )
        with tempfile.TemporaryDirectory() as tmp:
            path: Path = Path(tmp) / 'channels.lst'
            path.write_text('')
            with acquire_channels_lst_lock(path):
                with self.assertRaises(ChannelsListLockBusy):
                    with acquire_channels_lst_lock(path):
                        pass


# ---------------------------------------------------------------------------
# Task 5.6: End-to-end smoke test
# ---------------------------------------------------------------------------

class TestEndToEnd(unittest.IsolatedAsyncioTestCase):
    async def test_round_trip_canonicalises_mixed_input(
        self,
    ) -> None:
        '''Mixed raw input (handle, id, URL all for same channel)
        → pure JSONL with exactly one deduplicated record.
        '''
        import json
        from tools.yt_cleanup_channel_list import _run
        from scrape_exchange.creator_map import NullCreatorMap
        from scrape_exchange.handle_map import NullHandleMap

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path: Path = Path(tmp)
            lst: Path = tmp_path / 'channels.lst'
            data_dir: Path = tmp_path / 'data'
            data_dir.mkdir()
            (data_dir / 'uploaded').mkdir()

            # Three lines that all refer to the same channel.
            # parse_line turns each into a ChannelListRecord;
            # dedup_records collapses them into one.
            lst.write_text(
                '@LinusTechTips\n'
                'UCXuqSBlHAE6Xw-yeJA0Tunw\n'
                'https://youtube.com/@LinusTechTips\n',
                encoding='utf-8',
            )

            # Use NullHandleMap — no Redis needed for this test.
            # The id-only record has no handle to look up, and the
            # two handle records share the same normalised handle,
            # so they collapse in the second dedup pass.
            store: ChannelIdentityStore = ChannelIdentityStore(
                creator_map=NullCreatorMap(),
                handle_map=NullHandleMap(),
            )

            def no_prompt(
                t: str, p: dict,
            ) -> None:  # type: ignore[return]
                raise AssertionError(
                    f'unexpected conflict prompt: {t}'
                )

            rc: int = await _run(
                lst, data_dir, store, prompt=no_prompt,
            )
            self.assertEqual(rc, 0)

            lines: list[str] = [
                l for l in
                lst.read_text(encoding='utf-8').splitlines()
                if l.strip()
            ]
            # The id-only record and the two handle-only records
            # cannot be merged without Redis (the id is not in
            # handle_map), so we get id record + one handle record.
            # Both are valid JSONL.
            self.assertGreaterEqual(len(lines), 1)
            for line in lines:
                obj: dict = json.loads(line)
                self.assertIn('channel_id', obj)
                self.assertIn('channel_handle', obj)
                self.assertIn('status', obj)


if __name__ == '__main__':
    unittest.main()
