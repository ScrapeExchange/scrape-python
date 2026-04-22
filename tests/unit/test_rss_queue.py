'''
Unit tests for scrape_exchange.creator_queue.FileCreatorQueue
with the tier-aware interface.
'''

import copy
import heapq
import os
import tempfile
import unittest

from datetime import UTC, datetime

from scrape_exchange.creator_queue import (
    CHANNEL_FILENAME_PREFIX,
    FileCreatorQueue,
    TierConfig,
    parse_priority_queues,
)
from scrape_exchange.file_management import (
    AssetFileManagement,
)

DEFAULT_TIERS: list[TierConfig] = [
    TierConfig(
        tier=1,
        min_subscribers=1_000_000,
        interval_hours=4.0,
    ),
    TierConfig(
        tier=2,
        min_subscribers=100_000,
        interval_hours=12.0,
    ),
    TierConfig(
        tier=3,
        min_subscribers=10_000,
        interval_hours=24.0,
    ),
    TierConfig(
        tier=4,
        min_subscribers=0,
        interval_hours=48.0,
    ),
]


def _make_queue(tmp_dir: str) -> FileCreatorQueue:
    queue_file: str = os.path.join(
        tmp_dir, 'queue.json',
    )
    no_feeds_file: str = os.path.join(
        tmp_dir, 'no_feeds.tsv',
    )
    return FileCreatorQueue(queue_file, no_feeds_file)


def _make_fm(tmp_dir: str) -> AssetFileManagement:
    return AssetFileManagement(tmp_dir)


def _now() -> float:
    return datetime.now(UTC).timestamp()


# ------------------------------------------------------------------
# Populate
# ------------------------------------------------------------------

class TestFileCreatorQueuePopulate(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_populate_assigns_correct_tiers(self):
        '''Creators land in the right tier by subscriber
        count.'''
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            fm: AssetFileManagement = _make_fm(tmp)
            creators: dict[str, str] = {
                'UC_mega': 'mega-channel',
                'UC_mid': 'mid-channel',
                'UC_small': 'small-channel',
                'UC_tiny': 'tiny-channel',
            }
            sub_counts: dict[str, int] = {
                'UC_mega': 5_000_000,
                'UC_mid': 500_000,
                'UC_small': 50_000,
                'UC_tiny': 5_000,
            }
            added: int = await q.populate(
                creators,
                fm,
                DEFAULT_TIERS,
                sub_counts,
            )
            self.assertEqual(added, 4)
            self.assertEqual(
                await q.get_tier('UC_mega'), 1,
            )
            self.assertEqual(
                await q.get_tier('UC_mid'), 2,
            )
            self.assertEqual(
                await q.get_tier('UC_small'), 3,
            )
            self.assertEqual(
                await q.get_tier('UC_tiny'), 4,
            )

    async def test_populate_unknown_subscriber_count_lowest_tier(
        self,
    ):
        '''Creator absent from subscriber_counts gets
        None from dict.get() → tier 1 (unknown, highest
        priority).'''
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            fm: AssetFileManagement = _make_fm(tmp)
            creators: dict[str, str] = {
                'UCunknown': 'mystery-channel',
            }
            added: int = await q.populate(
                creators,
                fm,
                DEFAULT_TIERS,
                {},
            )
            self.assertEqual(added, 1)
            self.assertEqual(
                await q.get_tier('UCunknown'), 1,
            )

    async def test_populate_zero_subscriber_count_lowest_tier(
        self,
    ):
        '''Creator with subscriber count of 0 goes to
        the lowest tier (not tier 1).'''
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            fm: AssetFileManagement = _make_fm(tmp)
            creators: dict[str, str] = {
                'UCzero': 'zero-channel',
            }
            added: int = await q.populate(
                creators,
                fm,
                DEFAULT_TIERS,
                {'UCzero': 0},
            )
            self.assertEqual(added, 1)
            self.assertEqual(
                await q.get_tier('UCzero'), 4,
            )

    async def test_populate_skips_duplicate_channel_id(
        self,
    ):
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            fm: AssetFileManagement = _make_fm(tmp)
            first: dict[str, str] = {
                'UCaaa': 'alpha',
            }
            await q.populate(
                first, fm, DEFAULT_TIERS, {},
            )
            second: dict[str, str] = {
                'UCaaa': 'alpha-renamed',
            }
            added: int = await q.populate(
                second, fm, DEFAULT_TIERS, {},
            )
            self.assertEqual(added, 0)
            self.assertEqual(
                await q.queue_size(), 1,
            )

    async def test_populate_skips_duplicate_channel_name(
        self,
    ):
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            fm: AssetFileManagement = _make_fm(tmp)
            first: dict[str, str] = {
                'UCaaa': 'alpha',
            }
            await q.populate(
                first, fm, DEFAULT_TIERS, {},
            )
            second: dict[str, str] = {
                'UCzzz': 'alpha',
            }
            added: int = await q.populate(
                second, fm, DEFAULT_TIERS, {},
            )
            self.assertEqual(added, 0)
            self.assertEqual(
                await q.queue_size(), 1,
            )

    async def test_populate_skips_not_found_marker(
        self,
    ):
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            fm: AssetFileManagement = _make_fm(tmp)
            channel_name: str = 'mychannel'
            marker: str = (
                f'{CHANNEL_FILENAME_PREFIX}'
                f'{channel_name}.not_found'
            )
            open(
                os.path.join(tmp, marker), 'w',
            ).close()
            creators: dict[str, str] = {
                'UCaaa': channel_name,
            }
            added: int = await q.populate(
                creators, fm, DEFAULT_TIERS, {},
            )
            self.assertEqual(added, 0)
            self.assertEqual(
                await q.queue_size(), 0,
            )

    async def test_populate_skips_unresolved_marker(
        self,
    ):
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            fm: AssetFileManagement = _make_fm(tmp)
            channel_id: str = 'UCbbb'
            marker: str = (
                f'{CHANNEL_FILENAME_PREFIX}'
                f'{channel_id}.unresolved'
            )
            open(
                os.path.join(tmp, marker), 'w',
            ).close()
            creators: dict[str, str] = {
                channel_id: 'somechannel',
            }
            added: int = await q.populate(
                creators, fm, DEFAULT_TIERS, {},
            )
            self.assertEqual(added, 0)
            self.assertEqual(
                await q.queue_size(), 0,
            )


# ------------------------------------------------------------------
# Claim batch
# ------------------------------------------------------------------

class TestFileCreatorQueueClaimBatch(
    unittest.IsolatedAsyncioTestCase,
):

    def _seed_tier(
        self,
        q: FileCreatorQueue,
        tier: int,
        creators: dict[str, str],
        ts: float,
    ) -> None:
        '''Directly push entries into a specific tier
        heap at a given timestamp.'''
        if tier not in q._heaps:
            q._heaps[tier] = []
        for cid, name in creators.items():
            heapq.heappush(
                q._heaps[tier], (ts, name, cid),
            )
            q._names[cid] = name
            q._creator_tiers[cid] = tier

    async def test_claim_tier1_before_tier2(self):
        '''Tier-1 entries must be returned before tier-2
        when both are due.'''
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            q._tiers = DEFAULT_TIERS
            past: float = _now() - 60
            self._seed_tier(
                q, 1, {'UC_t1': 'tier1-ch'}, past,
            )
            self._seed_tier(
                q, 2, {'UC_t2': 'tier2-ch'}, past,
            )
            # Batch size 1 should return tier-1 only.
            batch: list[tuple[str, str]] = (
                await q.claim_batch(1, 'worker-1')
            )
            self.assertEqual(len(batch), 1)
            cid: str
            cid, _, _ = batch[0]
            self.assertEqual(cid, 'UC_t1')

    async def test_claim_fills_from_lower_tier_when_higher_drained(
        self,
    ):
        '''After tier-1 is exhausted the batch fills
        from tier-2.'''
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            q._tiers = DEFAULT_TIERS
            past: float = _now() - 60
            self._seed_tier(
                q, 1, {'UC_t1a': 'alpha'}, past,
            )
            self._seed_tier(
                q,
                2,
                {
                    'UC_t2a': 'beta',
                    'UC_t2b': 'gamma',
                },
                past,
            )
            batch: list[tuple[str, str]] = (
                await q.claim_batch(3, 'worker-1')
            )
            self.assertEqual(len(batch), 3)
            cids: set[str] = {c for c, _, _ in batch}
            self.assertIn('UC_t1a', cids)
            self.assertIn('UC_t2a', cids)
            self.assertIn('UC_t2b', cids)

    async def test_claim_empty_when_nothing_due(self):
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            q._tiers = DEFAULT_TIERS
            future: float = _now() + 3600
            self._seed_tier(
                q, 1, {'UCaaa': 'alpha'}, future,
            )
            batch: list[tuple[str, str]] = (
                await q.claim_batch(10, 'worker-1')
            )
            self.assertEqual(batch, [])

    async def test_claim_respects_batch_size(self):
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            q._tiers = DEFAULT_TIERS
            past: float = _now() - 60
            channels: dict[str, str] = {
                f'UC{i:04d}': f'ch{i}'
                for i in range(10)
            }
            self._seed_tier(q, 1, channels, past)
            batch: list[tuple[str, str]] = (
                await q.claim_batch(3, 'worker-1')
            )
            self.assertEqual(len(batch), 3)


# ------------------------------------------------------------------
# Release
# ------------------------------------------------------------------

class TestFileCreatorQueueRelease(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_release_reenqueues_to_correct_tier(
        self,
    ):
        '''Released creator lands back in its tier heap.'''
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            fm: AssetFileManagement = _make_fm(tmp)
            creators: dict[str, str] = {
                'UCaaa': 'alpha',
            }
            sub_counts: dict[str, int] = {
                'UCaaa': 5_000_000,  # tier 1
            }
            await q.populate(
                creators, fm, DEFAULT_TIERS,
                sub_counts,
            )
            batch: list[tuple[str, str]] = (
                await q.claim_batch(10, 'worker-1')
            )
            self.assertEqual(len(batch), 1)
            self.assertEqual(
                await q.queue_size(), 0,
            )
            await q.release('UCaaa')
            self.assertEqual(
                await q.queue_size(), 1,
            )
            # Must be in tier-1 heap.
            self.assertEqual(
                len(q._heaps.get(1, [])), 1,
            )

    async def test_release_computes_next_check_from_tier(
        self,
    ):
        '''next_check is now + tier.interval_hours * 3600.
        '''
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            fm: AssetFileManagement = _make_fm(tmp)
            sub_counts: dict[str, int] = {
                'UCaaa': 5_000_000,  # tier 1 → 4 h
            }
            await q.populate(
                {'UCaaa': 'alpha'},
                fm,
                DEFAULT_TIERS,
                sub_counts,
            )
            await q.claim_batch(10, 'worker-1')
            before_release: float = _now()
            await q.release('UCaaa')
            ts: float = q._heaps[1][0][0]
            expected_min: float = (
                before_release + 4.0 * 3600 - 5
            )
            expected_max: float = (
                before_release + 4.0 * 3600 + 5
            )
            self.assertGreaterEqual(ts, expected_min)
            self.assertLessEqual(ts, expected_max)

    async def test_release_persists_queue_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            fm: AssetFileManagement = _make_fm(tmp)
            await q.populate(
                {'UCaaa': 'alpha'},
                fm,
                DEFAULT_TIERS,
                {},
            )
            await q.claim_batch(10, 'worker-1')
            await q.release('UCaaa')
            queue_file: str = os.path.join(
                tmp, 'queue.json',
            )
            self.assertTrue(
                os.path.isfile(queue_file),
            )


# ------------------------------------------------------------------
# Tier management
# ------------------------------------------------------------------

class TestTierManagement(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_update_tier_changes_creator_tier(
        self,
    ):
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            fm: AssetFileManagement = _make_fm(tmp)
            # Start in tier 4 (5 000 subs).
            sub_counts: dict[str, int] = {
                'UCaaa': 5_000,
            }
            await q.populate(
                {'UCaaa': 'alpha'},
                fm,
                DEFAULT_TIERS,
                sub_counts,
            )
            self.assertEqual(
                await q.get_tier('UCaaa'), 4,
            )
            # Grows to 2M subs → should move to tier 1.
            await q.update_tier('UCaaa', 2_000_000)
            self.assertEqual(
                await q.get_tier('UCaaa'), 1,
            )

    async def test_get_tier_returns_correct_tier(self):
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            fm: AssetFileManagement = _make_fm(tmp)
            sub_counts: dict[str, int] = {
                'UCaaa': 200_000,  # tier 2
            }
            await q.populate(
                {'UCaaa': 'alpha'},
                fm,
                DEFAULT_TIERS,
                sub_counts,
            )
            self.assertEqual(
                await q.get_tier('UCaaa'), 2,
            )

    async def test_release_uses_new_tier_after_update(
        self,
    ):
        '''After update_tier, release puts the creator
        into the new tier's heap.'''
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            fm: AssetFileManagement = _make_fm(tmp)
            # Start in tier 4.
            sub_counts: dict[str, int] = {
                'UCaaa': 5_000,
            }
            await q.populate(
                {'UCaaa': 'alpha'},
                fm,
                DEFAULT_TIERS,
                sub_counts,
            )
            await q.claim_batch(10, 'worker-1')
            # Subscriber count surged → tier 1.
            await q.update_tier('UCaaa', 2_000_000)
            await q.release('UCaaa')
            # Heap 1 should have the entry, not heap 4.
            self.assertEqual(
                len(q._heaps.get(1, [])), 1,
            )
            self.assertEqual(
                len(q._heaps.get(4, [])), 0,
            )


# ------------------------------------------------------------------
# Queue-level operations
# ------------------------------------------------------------------

class TestQueueOperations(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_queue_size_sums_all_tiers(self):
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            fm: AssetFileManagement = _make_fm(tmp)
            creators: dict[str, str] = {
                'UCaaa': 'alpha',   # tier 1 (unknown/None → 1)
                'UCbbb': 'beta',    # tier 2
                'UCccc': 'gamma',   # tier 3
            }
            sub_counts: dict[str, int] = {
                'UCbbb': 500_000,
                'UCccc': 50_000,
            }
            await q.populate(
                creators, fm, DEFAULT_TIERS,
                sub_counts,
            )
            self.assertEqual(
                await q.queue_size(), 3,
            )

    async def test_next_due_time_returns_earliest_across_tiers(
        self,
    ):
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            q._tiers = DEFAULT_TIERS
            ts1: float = 1000.0
            ts2: float = 2000.0
            q._heaps[1] = []
            q._heaps[2] = []
            heapq.heappush(
                q._heaps[2], (ts2, 'beta', 'UCbbb'),
            )
            heapq.heappush(
                q._heaps[1], (ts1, 'alpha', 'UCaaa'),
            )
            result: float | None = (
                await q.next_due_time()
            )
            self.assertEqual(result, ts1)

    async def test_next_due_time_none_when_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            result: float | None = (
                await q.next_due_time()
            )
            self.assertIsNone(result)


# ------------------------------------------------------------------
# No-feeds methods (unchanged from old interface)
# ------------------------------------------------------------------

class TestFileCreatorQueueNoFeeds(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_get_no_feeds_returns_none_when_absent(
        self,
    ):
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            result: tuple[str, str, int] | None = (
                await q.get_no_feeds('UCaaa')
            )
            self.assertIsNone(result)

    async def test_set_no_feeds_stores_entry(self):
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            await q.set_no_feeds(
                'UCaaa',
                'https://example.com/feed',
                'my-channel',
                1,
            )
            result: tuple[str, str, int] | None = (
                await q.get_no_feeds('UCaaa')
            )
            self.assertIsNotNone(result)
            assert result is not None
            url: str
            name: str
            count: int
            url, name, count = result
            self.assertEqual(
                url, 'https://example.com/feed',
            )
            self.assertEqual(name, 'my-channel')
            self.assertEqual(count, 1)

    async def test_set_no_feeds_increments_count(self):
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            await q.set_no_feeds(
                'UCaaa',
                'https://example.com/feed',
                'my-channel',
                1,
            )
            await q.set_no_feeds(
                'UCaaa',
                'https://example.com/feed',
                'my-channel',
                1,
            )
            result: tuple[str, str, int] | None = (
                await q.get_no_feeds('UCaaa')
            )
            self.assertIsNotNone(result)
            assert result is not None
            _: str
            __: str
            count_: int
            _, __, count_ = result
            self.assertEqual(count_, 2)

    async def test_clear_no_feeds_removes_entry(self):
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            await q.set_no_feeds(
                'UCaaa',
                'https://example.com/feed',
                'my-channel',
                1,
            )
            await q.clear_no_feeds('UCaaa')
            result: tuple[str, str, int] | None = (
                await q.get_no_feeds('UCaaa')
            )
            self.assertIsNone(result)


# ------------------------------------------------------------------
# Cleanup stale claims
# ------------------------------------------------------------------

class TestFileCreatorQueueCleanupStaleClaims(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_cleanup_stale_claims_returns_zero(
        self,
    ):
        '''File backend is single-process; claims are
        always in-memory and never stale.'''
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            result: int = (
                await q.cleanup_stale_claims()
            )
            self.assertEqual(result, 0)


class TestFileCreatorQueueTierInterval(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_returns_interval_for_known_tier(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            fm: AssetFileManagement = _make_fm(tmp)
            await q.populate(
                {'UCa': 'a'},
                fm,
                DEFAULT_TIERS,
                {'UCa': 5_000_000},
            )
            self.assertEqual(
                q.get_tier_interval(1), 4.0,
            )
            self.assertEqual(
                q.get_tier_interval(3), 24.0,
            )

    async def test_returns_last_tier_for_unknown(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            fm: AssetFileManagement = _make_fm(tmp)
            await q.populate(
                {'UCa': 'a'},
                fm,
                DEFAULT_TIERS,
                {'UCa': 5_000_000},
            )
            self.assertEqual(
                q.get_tier_interval(99), 48.0,
            )


class TestFileCreatorQueueEligibilityFraction(
    unittest.IsolatedAsyncioTestCase,
):

    async def test_release_with_half_fraction(
        self,
    ) -> None:
        '''eligibility_fraction=0.5 schedules at half
        the tier interval.'''
        with tempfile.TemporaryDirectory() as tmp:
            queue_file: str = os.path.join(
                tmp, 'queue.json',
            )
            no_feeds_file: str = os.path.join(
                tmp, 'no_feeds.tsv',
            )
            q: FileCreatorQueue = FileCreatorQueue(
                queue_file,
                no_feeds_file,
                eligibility_fraction=0.5,
            )
            fm: AssetFileManagement = _make_fm(tmp)
            await q.populate(
                {'UCa': 'a'},
                fm,
                DEFAULT_TIERS,
                {'UCa': 5_000_000},  # tier 1 → 4 h
            )
            await q.claim_batch(10, 'worker-1')
            before_release: float = _now()
            await q.release('UCa')
            ts: float = q._heaps[1][0][0]
            # 4 h * 0.5 = 2 h
            expected_min: float = (
                before_release + 2.0 * 3600 - 5
            )
            expected_max: float = (
                before_release + 2.0 * 3600 + 5
            )
            self.assertGreaterEqual(ts, expected_min)
            self.assertLessEqual(ts, expected_max)

    async def test_default_fraction_is_one(
        self,
    ) -> None:
        '''Default constructor preserves the original
        full-interval behaviour.'''
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            fm: AssetFileManagement = _make_fm(tmp)
            await q.populate(
                {'UCa': 'a'},
                fm,
                DEFAULT_TIERS,
                {'UCa': 5_000_000},  # tier 1 → 4 h
            )
            await q.claim_batch(10, 'worker-1')
            before_release: float = _now()
            await q.release('UCa')
            ts: float = q._heaps[1][0][0]
            expected_min: float = (
                before_release + 4.0 * 3600 - 5
            )
            expected_max: float = (
                before_release + 4.0 * 3600 + 5
            )
            self.assertGreaterEqual(ts, expected_min)
            self.assertLessEqual(ts, expected_max)


class TestFileCreatorQueueScanAndRecoverOrphans(
    unittest.IsolatedAsyncioTestCase,
):
    '''
    FileCreatorQueue is single-process; every cid in
    _creator_tiers is also in _heaps, so orphans are
    impossible. The method returns all known cids as
    ``queued`` regardless of the ``recover`` flag.
    '''

    def _queue(self) -> FileCreatorQueue:
        tmp = tempfile.mkdtemp()
        q: FileCreatorQueue = FileCreatorQueue(
            queue_file=os.path.join(tmp, 'q.json'),
            no_feeds_file=os.path.join(tmp, 'nf.tsv'),
        )
        q._tiers = [
            TierConfig(
                tier=1, min_subscribers=1_000_000,
                interval_hours=4.0,
            ),
            TierConfig(
                tier=2, min_subscribers=0,
                interval_hours=24.0,
            ),
        ]
        # Three cids in tier 1, one in tier 2.
        q._heaps = {
            1: [
                (1.0, 'a', 'UC_a'),
                (2.0, 'b', 'UC_b'),
                (3.0, 'c', 'UC_c'),
            ],
            2: [(4.0, 'd', 'UC_d')],
        }
        q._creator_tiers = {
            'UC_a': 1, 'UC_b': 1, 'UC_c': 1, 'UC_d': 2,
        }
        q._loaded = True
        return q

    async def test_returns_all_cids_as_queued(
        self,
    ) -> None:
        q: FileCreatorQueue = self._queue()
        breakdown: dict[int, dict[str, int]] = (
            await q.scan_and_recover_orphans()
        )
        self.assertEqual(
            breakdown,
            {
                1: {
                    'queued': 3, 'claimed': 0,
                    'no_feeds': 0, 'orphan': 0,
                },
                2: {
                    'queued': 1, 'claimed': 0,
                    'no_feeds': 0, 'orphan': 0,
                },
            },
        )

    async def test_recover_false_is_read_only(
        self,
    ) -> None:
        q: FileCreatorQueue = self._queue()
        before: dict[int, list[tuple[float, str, str]]] = (
            copy.deepcopy(q._heaps)
        )
        await q.scan_and_recover_orphans(recover=False)
        self.assertEqual(q._heaps, before)

    async def test_empty_queue(self) -> None:
        q: FileCreatorQueue = self._queue()
        q._heaps = {1: [], 2: []}
        q._creator_tiers = {}
        breakdown: dict[int, dict[str, int]] = (
            await q.scan_and_recover_orphans()
        )
        self.assertEqual(
            breakdown,
            {
                1: {
                    'queued': 0, 'claimed': 0,
                    'no_feeds': 0, 'orphan': 0,
                },
                2: {
                    'queued': 0, 'claimed': 0,
                    'no_feeds': 0, 'orphan': 0,
                },
            },
        )


class TestFileCreatorQueueReleaseRetryInterval(
    unittest.IsolatedAsyncioTestCase,
):
    '''release() accepts an optional retry_interval_seconds
    floor so that failed channels can be held back longer
    than their tier interval would normally allow.'''

    async def test_no_retry_interval_uses_tier_interval(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            fm: AssetFileManagement = _make_fm(tmp)
            await q.populate(
                {'UCaaa': 'alpha'},
                fm,
                DEFAULT_TIERS,
                {'UCaaa': 5_000_000},  # tier 1 → 4h
            )
            await q.claim_batch(10, 'worker-1')
            before: float = _now()
            await q.release('UCaaa')
            ts: float = q._heaps[1][0][0]
            self.assertAlmostEqual(
                ts - before, 4.0 * 3600, delta=5.0,
            )

    async def test_retry_interval_raises_floor(
        self,
    ) -> None:
        '''When the retry floor is longer than the tier
        interval, it wins.'''
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            fm: AssetFileManagement = _make_fm(tmp)
            await q.populate(
                {'UCaaa': 'alpha'},
                fm,
                DEFAULT_TIERS,
                {'UCaaa': 5_000_000},  # tier 1 → 4h
            )
            await q.claim_batch(10, 'worker-1')
            before: float = _now()
            await q.release(
                'UCaaa',
                retry_interval_seconds=24.0 * 3600,
            )
            ts: float = q._heaps[1][0][0]
            # 24h > 4h — retry floor applies.
            self.assertAlmostEqual(
                ts - before, 24.0 * 3600, delta=5.0,
            )

    async def test_retry_interval_below_tier_is_ignored(
        self,
    ) -> None:
        '''When the retry floor is shorter than the tier
        interval, the tier interval wins.'''
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            fm: AssetFileManagement = _make_fm(tmp)
            await q.populate(
                {'UCaaa': 'alpha'},
                fm,
                DEFAULT_TIERS,
                {'UCaaa': 5_000},  # tier 4 → 48h
            )
            await q.claim_batch(10, 'worker-1')
            before: float = _now()
            await q.release(
                'UCaaa',
                retry_interval_seconds=4.0 * 3600,
            )
            ts: float = q._heaps[4][0][0]
            # 48h > 4h — tier interval wins.
            self.assertAlmostEqual(
                ts - before, 48.0 * 3600, delta=5.0,
            )

    async def test_retry_interval_scales_with_eligibility(
        self,
    ) -> None:
        '''eligibility_fraction still scales the effective
        interval after the floor is applied.'''
        with tempfile.TemporaryDirectory() as tmp:
            queue_file: str = os.path.join(
                tmp, 'queue.json',
            )
            no_feeds_file: str = os.path.join(
                tmp, 'no_feeds.tsv',
            )
            q: FileCreatorQueue = FileCreatorQueue(
                queue_file, no_feeds_file,
                eligibility_fraction=0.5,
            )
            fm: AssetFileManagement = _make_fm(tmp)
            await q.populate(
                {'UCaaa': 'alpha'},
                fm,
                DEFAULT_TIERS,
                {'UCaaa': 5_000_000},  # tier 1 → 4h
            )
            await q.claim_batch(10, 'worker-1')
            before: float = _now()
            await q.release(
                'UCaaa',
                retry_interval_seconds=8.0 * 3600,
            )
            ts: float = q._heaps[1][0][0]
            # max(4h, 8h) = 8h, scaled by 0.5 → 4h.
            self.assertAlmostEqual(
                ts - before, 4.0 * 3600, delta=5.0,
            )


class TestFileCreatorQueueHadFeed(
    unittest.IsolatedAsyncioTestCase,
):
    '''The had-feed flag tracks which creators have ever
    served a successful RSS feed. Used to apply a more
    forgiving no-feed failure threshold to established
    channels.'''

    async def test_has_had_feed_false_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            self.assertFalse(
                await q.has_had_feed('UC_x'),
            )

    async def test_mark_had_feed_sets_flag(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            await q.mark_had_feed('UC_x')
            self.assertTrue(
                await q.has_had_feed('UC_x'),
            )
            self.assertFalse(
                await q.has_had_feed('UC_y'),
            )

    async def test_mark_had_feed_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            q: FileCreatorQueue = _make_queue(tmp)
            await q.mark_had_feed('UC_x')
            await q.mark_had_feed('UC_x')
            await q.mark_had_feed('UC_x')
            self.assertTrue(
                await q.has_had_feed('UC_x'),
            )

    async def test_persists_across_instances(self) -> None:
        '''The had-feed set survives re-instantiation
        (simulates a worker restart).'''
        with tempfile.TemporaryDirectory() as tmp:
            queue_file: str = os.path.join(
                tmp, 'queue.json',
            )
            no_feeds_file: str = os.path.join(
                tmp, 'no_feeds.tsv',
            )
            had_feed_file: str = os.path.join(
                tmp, 'had-feed.txt',
            )
            q1: FileCreatorQueue = FileCreatorQueue(
                queue_file, no_feeds_file,
                had_feed_file=had_feed_file,
            )
            await q1.mark_had_feed('UC_a')
            await q1.mark_had_feed('UC_b')

            q2: FileCreatorQueue = FileCreatorQueue(
                queue_file, no_feeds_file,
                had_feed_file=had_feed_file,
            )
            fm: AssetFileManagement = _make_fm(tmp)
            await q2.populate(
                {}, fm, DEFAULT_TIERS, {},
            )
            self.assertTrue(
                await q2.has_had_feed('UC_a'),
            )
            self.assertTrue(
                await q2.has_had_feed('UC_b'),
            )
            self.assertFalse(
                await q2.has_had_feed('UC_c'),
            )

    async def test_had_feed_default_file_path(self) -> None:
        '''When had_feed_file is not supplied, a sibling
        path is derived from the no-feeds file.'''
        with tempfile.TemporaryDirectory() as tmp:
            no_feeds_file: str = os.path.join(
                tmp, 'no-feeds.tsv',
            )
            q: FileCreatorQueue = FileCreatorQueue(
                os.path.join(tmp, 'queue.json'),
                no_feeds_file,
            )
            expected: str = os.path.join(
                tmp, 'no-feeds-had-feed.tsv',
            )
            self.assertEqual(q._had_feed_file, expected)


if __name__ == '__main__':
    unittest.main()
