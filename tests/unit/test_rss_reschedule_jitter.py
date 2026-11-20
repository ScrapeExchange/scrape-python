'''
Unit tests for RSS reschedule jitter: after a successful poll the next
one is scheduled up to ``reschedule_jitter`` earlier than the usual
delay, never later, so channels polled together drift apart. Failed
polls keep their exact retry delay.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import os
import random
import tempfile
import unittest
from datetime import UTC, datetime
from unittest import mock

import fakeredis.aioredis

from scrape_exchange.creator_queue import (
    FileCreatorQueue,
    RedisCreatorQueue,
    TierConfig,
    jittered_delay,
)
from scrape_exchange.file_management import AssetFileManagement
from tools import yt_rss_scrape

TIERS: list[TierConfig] = [
    TierConfig(tier=1, min_subscribers=0, interval_hours=24),
]
DELAY: float = 24 * 3600 * 0.5  # tier interval x eligibility fraction
JITTER: float = 0.1


class TestJitteredDelay(unittest.TestCase):

    def test_within_window_and_never_later(self) -> None:
        rng: random.Random = random.Random(1)
        delays: list[float] = [
            jittered_delay(DELAY, JITTER, rng) for _ in range(500)
        ]
        self.assertTrue(
            all(DELAY * (1 - JITTER) <= d <= DELAY for d in delays),
        )
        # Spread out, not a constant.
        self.assertGreater(max(delays) - min(delays), DELAY * 0.05)

    def test_zero_jitter_is_exact(self) -> None:
        self.assertEqual(
            jittered_delay(DELAY, 0.0, random.Random(1)), DELAY,
        )


class TestRedisQueueReleaseJitter(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(decode_responses=True)
        )

    async def asyncTearDown(self) -> None:
        await self.redis.aclose()

    def _queue(self, jitter: float) -> RedisCreatorQueue:
        queue: RedisCreatorQueue = RedisCreatorQueue(
            redis_dsn='redis://fake', worker_id='w1',
            platform='youtube', eligibility_fraction=0.5,
            reschedule_jitter=jitter, rng=random.Random(3),
        )
        queue._redis = self.redis
        queue._tiers = TIERS
        queue._key_queues = queue._build_queue_keys(TIERS)
        return queue

    async def _delays(
        self, queue: RedisCreatorQueue, *,
        retry_interval_seconds: float | None = None,
    ) -> list[float]:
        delays: list[float] = []
        index: int
        for index in range(50):
            cid: str = f'UC{index:022d}'
            before: float = datetime.now(UTC).timestamp()
            await queue.release(
                cid, retry_interval_seconds=retry_interval_seconds,
            )
            score: float | None = await self.redis.zscore(
                queue._queue_key(1), cid,
            )
            delays.append(score - before)
        return delays

    async def test_success_release_is_jittered_earlier(self) -> None:
        delays: list[float] = await self._delays(self._queue(JITTER))
        self.assertTrue(
            all(
                DELAY * (1 - JITTER) - 1 <= d <= DELAY + 1
                for d in delays
            ),
        )
        # Channels released together no longer share a due time.
        self.assertGreater(max(delays) - min(delays), DELAY * 0.05)

    async def test_failed_release_is_not_jittered(self) -> None:
        retry: float = 48 * 3600
        delays: list[float] = await self._delays(
            self._queue(JITTER), retry_interval_seconds=retry,
        )
        expected: float = retry * 0.5
        self.assertTrue(all(abs(d - expected) <= 1 for d in delays))

    async def test_zero_jitter_keeps_exact_delay(self) -> None:
        delays: list[float] = await self._delays(self._queue(0.0))
        self.assertTrue(all(abs(d - DELAY) <= 1 for d in delays))


class TestFileQueueReleaseJitter(unittest.IsolatedAsyncioTestCase):

    async def test_success_release_is_jittered_earlier(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            queue: FileCreatorQueue = FileCreatorQueue(
                os.path.join(tmp, 'queue.json'),
                os.path.join(tmp, 'no_feeds.tsv'),
                eligibility_fraction=0.5,
                reschedule_jitter=JITTER,
                rng=random.Random(5),
            )
            creators: dict[str, str] = {
                f'UC{index:022d}': f'name{index}' for index in range(30)
            }
            await queue.populate(
                creators, AssetFileManagement(tmp), TIERS, {},
            )
            before: float = datetime.now(UTC).timestamp()
            cid: str
            for cid in creators:
                await queue.release(cid)
            scores: list[float] = [
                score for score, _name, _cid in queue._heaps[1]
                if score > before
            ]
        delays: list[float] = [score - before for score in scores]
        self.assertEqual(len(delays), 30)
        self.assertTrue(
            all(
                DELAY * (1 - JITTER) - 1 <= d <= DELAY + 1
                for d in delays
            ),
        )
        self.assertGreater(max(delays) - min(delays), DELAY * 0.05)


class TestRssSettingsJitter(unittest.TestCase):

    def test_default_and_env_override(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop('RSS_RESCHEDULE_JITTER', None)
            default: yt_rss_scrape.RssSettings = (
                yt_rss_scrape.RssSettings(
                    _env_file=None, _cli_parse_args=False,
                )
            )
        self.assertEqual(default.reschedule_jitter, 0.1)
        with mock.patch.dict(
            os.environ, {'RSS_RESCHEDULE_JITTER': '0.2'},
        ):
            override: yt_rss_scrape.RssSettings = (
                yt_rss_scrape.RssSettings(
                    _env_file=None, _cli_parse_args=False,
                )
            )
        self.assertEqual(override.reschedule_jitter, 0.2)


if __name__ == '__main__':
    unittest.main()
