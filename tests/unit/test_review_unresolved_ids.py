'''Unit tests for review_unresolved_ids deduplication via
RedisClaim and creator_map recheck.'''

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

import fakeredis.aioredis

from scrape_exchange.creator_map import RedisCreatorMap
from scrape_exchange.redis_claim import RedisClaim
from scrape_exchange.youtube.exchange_channels_set import (
    RedisExchangeChannelsSet,
)
from tools.yt_channel_scrape import (
    _select_new_channels_via_set,
    review_unresolved_ids,
)


class _StubFM:
    '''Stand-in for AssetFileManagement that no-ops on
    mark_unresolved and reports no markers.'''

    def marker_path(self, *args: str) -> object:
        class _P:
            def exists(self) -> bool:
                return False
        return _P()

    async def mark_unresolved(
        self, *args: str, content: str = '',
    ) -> None:
        return None


class TestReviewUnresolvedIdsDedup(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(
                decode_responses=True,
            )
        )
        self.cm: RedisCreatorMap = (
            RedisCreatorMap.__new__(RedisCreatorMap)
        )
        self.cm._redis = self.redis
        self.cm._key = 'youtube:creator_map'
        self.claim_a: RedisClaim = RedisClaim(
            self.redis,
            key_prefix='youtube:resolving:',
            ttl_seconds=60,
            owner='worker-A',
        )
        self.claim_b: RedisClaim = RedisClaim(
            self.redis,
            key_prefix='youtube:resolving:',
            ttl_seconds=60,
            owner='worker-B',
        )

    async def asyncTearDown(self) -> None:
        await self.redis.flushall()
        await self.redis.aclose()

    async def test_concurrent_resolution_calls_browse_once(
        self,
    ) -> None:
        '''When workers A and B both see UC123 as unresolved
        and run review_unresolved_ids concurrently, only one
        InnerTube call should fire.'''

        call_count: int = 0

        async def fake_resolve(cid: str) -> str:
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.05)
            return 'resolvedhandle'

        with patch.object(
            __import__(
                'scrape_exchange.youtube.youtube_channel',
                fromlist=['YouTubeChannel'],
            ).YouTubeChannel,
            'resolve_channel_id',
            new=AsyncMock(side_effect=fake_resolve),
        ):
            results: list[set[str]] = await asyncio.gather(
                review_unresolved_ids(
                    {'UC123'}, self.cm, _StubFM(),
                    concurrency=4,
                    max_resolved_channels=10,
                    claim=self.claim_a,
                ),
                review_unresolved_ids(
                    {'UC123'}, self.cm, _StubFM(),
                    concurrency=4,
                    max_resolved_channels=10,
                    claim=self.claim_b,
                ),
            )

        self.assertEqual(call_count, 1)
        winners: set[str] = results[0] | results[1]
        self.assertEqual(winners, {'resolvedhandle'})

    async def test_no_claim_falls_back_cleanly(
        self,
    ) -> None:
        '''When claim=None (e.g. FileCreatorMap path), the
        function still resolves and writes to creator_map.'''

        async def fake_resolve(cid: str) -> str:
            return 'soloresolved'

        with patch.object(
            __import__(
                'scrape_exchange.youtube.youtube_channel',
                fromlist=['YouTubeChannel'],
            ).YouTubeChannel,
            'resolve_channel_id',
            new=AsyncMock(side_effect=fake_resolve),
        ):
            result: set[str] = await review_unresolved_ids(
                {'UC777'}, self.cm, _StubFM(),
                concurrency=2,
                max_resolved_channels=10,
                claim=None,
            )

        self.assertEqual(result, {'soloresolved'})
        self.assertEqual(
            await self.cm.get('UC777'),
            'soloresolved',
        )

    async def test_recheck_hit_skips_innertube(
        self,
    ) -> None:
        '''When creator_map already has the id, the function
        returns the existing handle and does NOT call
        YouTubeChannel.resolve_channel_id.'''

        await self.cm.put('UC555', 'preresolved')

        with patch.object(
            __import__(
                'scrape_exchange.youtube.youtube_channel',
                fromlist=['YouTubeChannel'],
            ).YouTubeChannel,
            'resolve_channel_id',
            new=AsyncMock(),
        ) as mock_resolve:
            result: set[str] = await review_unresolved_ids(
                {'UC555'}, self.cm, _StubFM(),
                concurrency=2,
                max_resolved_channels=10,
                claim=self.claim_a,
            )

        self.assertEqual(result, {'preresolved'})
        mock_resolve.assert_not_called()


class TestSelectNewChannelsViaSet(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(
                decode_responses=True,
            )
        )
        self.s: RedisExchangeChannelsSet = (
            RedisExchangeChannelsSet(self.redis)
        )

    async def asyncTearDown(self) -> None:
        await self.redis.flushall()
        await self.redis.aclose()

    async def test_filters_known_handles(self) -> None:
        await self.s.add_many({'alpha', 'bravo'})
        candidates: list[str] = [
            'alpha', 'bravo', 'charlie', 'delta',
        ]
        selected: set[str] = (
            await _select_new_channels_via_set(
                candidates, self.s,
                max_new_channels=10,
                already_resolved_count=0,
            )
        )
        self.assertEqual(selected, {'charlie', 'delta'})

    async def test_respects_max_budget(self) -> None:
        candidates: list[str] = [
            f'h{i}' for i in range(20)
        ]
        selected: set[str] = (
            await _select_new_channels_via_set(
                candidates, self.s,
                max_new_channels=5,
                already_resolved_count=0,
            )
        )
        # First five candidates in input order, since the
        # function caps before the SISMEMBER call.
        self.assertEqual(
            selected,
            {'h0', 'h1', 'h2', 'h3', 'h4'},
        )

    async def test_respects_already_resolved_count(
        self,
    ) -> None:
        '''When some channels were resolved earlier in the
        same run, the budget remaining for new selections is
        ``max_new_channels - already_resolved_count``. Verify
        the loop guard breaks at that boundary.'''
        candidates: list[str] = [
            f'h{i}' for i in range(20)
        ]
        selected: set[str] = (
            await _select_new_channels_via_set(
                candidates, self.s,
                max_new_channels=5,
                already_resolved_count=3,
            )
        )
        # Loop must stop once selected + already_resolved >=
        # max_new_channels, i.e. once selected reaches 2.
        self.assertEqual(len(selected), 2)
        # And the two selected must be the first two
        # candidates in input order.
        self.assertEqual(selected, {'h0', 'h1'})
