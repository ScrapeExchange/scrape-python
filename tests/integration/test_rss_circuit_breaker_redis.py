'''Redis-backend integration tests for the RSS circuit breaker.

Skipped when ``REDIS_DSN_TEST`` env var is not set. The expected
DSN is a real Redis on a database number the test can flush
between cases — DO NOT point this at production.'''

import asyncio
import os
import unittest

import redis.asyncio as redis_async

from scrape_exchange.youtube._rss_circuit_state import (
    CircuitParams,
)
from scrape_exchange.youtube.rss_circuit_breaker import (
    _RedisCircuitBackend,
)


_DSN: str | None = os.environ.get('REDIS_DSN_TEST')


def _run(coro):
    loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


@unittest.skipIf(
    _DSN is None, 'set REDIS_DSN_TEST to run',
)
class TestRedisCircuitBackend(unittest.TestCase):
    '''Drives the Lua script via the production backend.'''

    def setUp(self) -> None:
        assert _DSN is not None
        self._client = redis_async.from_url(
            _DSN, decode_responses=True,
        )
        _run(self._client.flushdb())

    def tearDown(self) -> None:
        _run(self._client.aclose())

    def test_trip_returns_rollback_channels(self) -> None:
        params: CircuitParams = CircuitParams(
            fail_threshold=3,
            window_size=5,
            initial_open_seconds=10,
            max_open_seconds=7200,
            impaired_reopen_threshold=2,
            recovery_threshold=3,
        )
        backend = _RedisCircuitBackend(params, _DSN)

        async def run() -> None:
            r1 = await backend.record_outcome(
                channel_id='UC1', was_not_found=True,
            )
            r2 = await backend.record_outcome(
                channel_id='UC2', was_not_found=True,
            )
            r3 = await backend.record_outcome(
                channel_id='UC3', was_not_found=True,
            )
            self.assertIsNone(r1.transition)
            self.assertIsNone(r2.transition)
            self.assertEqual(
                r3.transition.to_state, 'open-regular',
            )
            self.assertEqual(
                r3.rollback_channel_ids, ['UC1', 'UC2'],
            )
            # After the trip, the breaker is open.
            state = await backend.read_state()
            self.assertTrue(state.is_open)

        _run(run())

    def test_redis_time_drives_open_to_closed(self) -> None:
        params: CircuitParams = CircuitParams(
            fail_threshold=2,
            window_size=5,
            initial_open_seconds=1,   # 1-second cooldown for the test
            max_open_seconds=7200,
            impaired_reopen_threshold=10,
            recovery_threshold=10,
        )
        backend = _RedisCircuitBackend(params, _DSN)

        async def run() -> None:
            # Trip with 2 consecutive 404s (F=2).
            await backend.record_outcome(
                channel_id='UC1', was_not_found=True,
            )
            r2 = await backend.record_outcome(
                channel_id='UC2', was_not_found=True,
            )
            self.assertEqual(
                r2.transition.to_state, 'open-regular',
            )
            self.assertTrue(r2.state_after.is_open)
            # Wait past the cooldown.
            await asyncio.sleep(1.5)
            # Next report should observe Redis-time expiry and
            # transition to closed-impaired.
            r3 = await backend.record_outcome(
                channel_id='UC3', was_not_found=False,
            )
            self.assertIsNotNone(r3.transition)
            self.assertEqual(
                r3.transition.from_state, 'open-regular',
            )
            self.assertEqual(
                r3.transition.to_state, 'closed-impaired',
            )

        _run(run())


if __name__ == '__main__':
    unittest.main()
