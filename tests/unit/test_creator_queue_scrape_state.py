'''
Unit tests for RedisCreatorQueue scrape-result state.
'''

import unittest

try:
    import fakeredis.aioredis
    _HAVE_FAKEREDIS: bool = True
except ImportError:
    _HAVE_FAKEREDIS = False

from scrape_exchange.creator_queue import (
    RedisCreatorQueue,
    TierConfig,
)


_TIERS: list[TierConfig] = [
    TierConfig(tier=1, min_subscribers=1_000_000, interval_hours=6),
    TierConfig(tier=2, min_subscribers=0, interval_hours=168),
]


@unittest.skipUnless(_HAVE_FAKEREDIS, 'fakeredis not installed')
class TestCreatorQueueScrapeState(unittest.IsolatedAsyncioTestCase):

    async def _queue(self) -> RedisCreatorQueue:
        redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
        q: RedisCreatorQueue = RedisCreatorQueue(
            redis_dsn='redis://fake',
            worker_id='w1',
            platform='instagram',
            key_namespace='scrape',
        )
        q._redis = redis
        q._tiers = _TIERS
        q._key_queues = q._build_queue_keys(_TIERS)
        return q

    async def test_show_member_includes_separate_scrape_status(
        self,
    ) -> None:
        q: RedisCreatorQueue = await self._queue()
        await q.add_member('natgeo', 'natgeo', 10_000_000)
        await q.record_scrape_failure(
            'natgeo',
            status='unknown_followers',
            error='missing follower count',
            worker_id='w1',
            proxy_ip='proxy1',
            evidence={'detected_markers': ['profile_identity']},
        )

        rec: dict | None = await q.show_member('natgeo')

        self.assertIsNotNone(rec)
        assert rec is not None
        self.assertEqual(rec['state'], 'queued')
        self.assertEqual(rec['queue_state'], 'queued')
        self.assertEqual(rec['scrape_status'], 'unknown_followers')
        self.assertEqual(
            rec['scrape_state']['failure_count'], 1,
        )

    async def test_success_resets_failure_count_and_counts_status(
        self,
    ) -> None:
        q: RedisCreatorQueue = await self._queue()
        await q.add_member('natgeo', 'natgeo', 10_000_000)
        await q.record_scrape_failure(
            'natgeo',
            status='unknown_followers',
            error='missing follower count',
        )
        await q.record_scrape_success(
            'natgeo',
            follower_count=282_000_000,
        )

        state: dict = await q.get_scrape_state('natgeo')
        counts: dict[str, int] = await q.count_by_scrape_status()

        self.assertEqual(state['scrape_status'], 'scraped')
        self.assertEqual(state['failure_count'], 0)
        self.assertEqual(state['last_follower_count'], 282_000_000)
        self.assertEqual(counts, {'scraped': 1})


if __name__ == '__main__':
    unittest.main()
