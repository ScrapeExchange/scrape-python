'''Unit tests for scrape_exchange.redis_claim.RedisClaim.'''

import asyncio
import unittest

import fakeredis.aioredis

from scrape_exchange.redis_claim import RedisClaim


class TestRedisClaim(
    unittest.IsolatedAsyncioTestCase,
):

    async def asyncSetUp(self) -> None:
        self.redis: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(
                decode_responses=True,
            )
        )
        self.claim: RedisClaim = RedisClaim(
            self.redis,
            key_prefix='youtube:resolving:',
            ttl_seconds=60,
            owner='worker-1',
        )

    async def asyncTearDown(self) -> None:
        await self.redis.flushall()
        await self.redis.aclose()

    async def test_try_claim_first_caller_wins(
        self,
    ) -> None:
        won: bool = await self.claim.try_claim('UC123')
        self.assertTrue(won)

    async def test_second_caller_loses(
        self,
    ) -> None:
        first: bool = await self.claim.try_claim('UC123')
        second: bool = await self.claim.try_claim('UC123')
        self.assertTrue(first)
        self.assertFalse(second)

    async def test_release_frees_claim(
        self,
    ) -> None:
        await self.claim.try_claim('UC123')
        await self.claim.release('UC123')
        won: bool = await self.claim.try_claim('UC123')
        self.assertTrue(won)

    async def test_context_manager_yields_won(
        self,
    ) -> None:
        async with self.claim.acquire('UC999') as won:
            self.assertTrue(won)
        # After release, can claim again
        won_again: bool = await self.claim.try_claim(
            'UC999',
        )
        self.assertTrue(won_again)

    async def test_ttl_recovers_from_crash(
        self,
    ) -> None:
        '''Simulate a crashed owner: the claim key expires via the
        TTL set by try_claim (ex=ttl_seconds). fakeredis 2.x checks
        key expiration using real wall-clock time (db.time is set to
        time.time() on every command), so asyncio.sleep advances the
        effective clock. Pattern: ttl_seconds=1, sleep 1.05 s.'''
        r: fakeredis.aioredis.FakeRedis = (
            fakeredis.aioredis.FakeRedis(
                decode_responses=True,
            )
        )
        claim: RedisClaim = RedisClaim(
            r,
            key_prefix='youtube:resolving:',
            ttl_seconds=1,
            owner='worker-1',
        )
        first: bool = await claim.try_claim('UC123')
        self.assertTrue(first)
        # Let the TTL expire (fakeredis uses real wall-clock time).
        # Use 2.0 s margin so cold Python startup does not shorten
        # the effective wait below 1 s.
        await asyncio.sleep(2.0)
        won: bool = await claim.try_claim('UC123')
        self.assertTrue(won)
        await r.aclose()

    async def test_context_manager_does_not_release_on_loss(
        self,
    ) -> None:
        '''A losing acquire must not delete the winner's key.'''
        other: RedisClaim = RedisClaim(
            self.redis,
            key_prefix='youtube:resolving:',
            ttl_seconds=60,
            owner='worker-2',
        )
        await self.claim.try_claim('UC123')
        async with other.acquire('UC123') as won:
            self.assertFalse(won)
        # Winner's claim must still be in Redis: another caller
        # cannot acquire while the original key is held.
        still_held: bool = await self.claim.try_claim('UC123')
        self.assertFalse(still_held)
