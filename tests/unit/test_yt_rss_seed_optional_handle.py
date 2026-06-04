'''RSS queue seeding must not require a channel_handle: a channel with
a channel_id but no handle is still seeded, keyed by a unique label so
the populate name-dedup does not collapse the cohort.'''

import unittest
from unittest import mock


def _tier(tier: int, min_subscribers: int) -> mock.MagicMock:
    t = mock.MagicMock()
    t.tier = tier
    t.min_subscribers = min_subscribers
    return t


class TestSeedOptionalHandle(unittest.IsolatedAsyncioTestCase):

    async def _run(self, records: list[dict]) -> dict:
        from tools import yt_rss_scrape as mod
        creator_queue = mock.AsyncMock()
        creator_queue.known_creator_ids.return_value = set()
        creator_queue.populate = mock.AsyncMock(
            return_value=len(records),
        )
        channel_fm = mock.MagicMock()
        channel_fm.list_uploaded.return_value = [
            f'f{i}' for i in range(len(records))
        ]
        channel_fm.read_uploaded = mock.AsyncMock(
            side_effect=records,
        )
        tiers = [_tier(1, 1000), _tier(2, 0)]
        await mod._seed_queue_from_uploaded_channels(
            creator_queue, channel_fm, tiers,
        )
        creator_queue.populate.assert_awaited_once()
        return creator_queue.populate.await_args.args[0]

    async def test_two_handleless_channels_both_seeded(self) -> None:
        cid_a = 'UCaaaaaaaaaaaaaaaaaaaaaa'
        cid_b = 'UCbbbbbbbbbbbbbbbbbbbbbb'
        creators = await self._run([
            {'channel_id': cid_a},  # no channel_handle
            {'channel_id': cid_b},  # no channel_handle
        ])
        # Both seeded, keyed by their unique channel_id (not collapsed).
        self.assertEqual(set(creators), {cid_a, cid_b})
        # Label falls back to the channel_id, which is unique.
        self.assertEqual(creators[cid_a], cid_a)
        self.assertEqual(creators[cid_b], cid_b)

    async def test_handle_used_as_label_when_present(self) -> None:
        cid = 'UCcccccccccccccccccccccc'
        creators = await self._run([
            {'channel_id': cid, 'channel_handle': 'realhandle'},
        ])
        self.assertEqual(creators[cid], 'realhandle')


if __name__ == '__main__':
    unittest.main()
