'''Tests for RssCircuitBreaker.wait_until_closed chunk-and-touch.

A worker parked on an open circuit may legitimately wait far longer than
the watchdog's work timeout (rss_circuit_max_open_seconds defaults to
7200s). The park must therefore sleep in bounded chunks and touch the
watchdog each chunk, so an intentional wait is not mistaken for a hang.
'''

import asyncio
import unittest

from unittest.mock import MagicMock, patch

from scrape_exchange.watchdog import Watchdog
from scrape_exchange.youtube import rss_circuit_breaker as rcb
from scrape_exchange.youtube.rss_circuit_breaker import _CircuitBackend
from scrape_exchange.youtube._rss_circuit_state import (
    CircuitParams,
    CircuitReport,
    CircuitState,
)


_PARAMS: CircuitParams = CircuitParams(
    fail_threshold=3,
    window_size=5,
    initial_open_seconds=100,
    max_open_seconds=7200,
    impaired_reopen_threshold=2,
    recovery_threshold=3,
)


class _FakeClock:
    def __init__(self) -> None:
        self.now: float = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class _OpenUntilBackend(_CircuitBackend):
    '''Reports open until ``open_until_ts`` per a fake clock.'''

    def __init__(self, clock: _FakeClock, open_seconds: float) -> None:
        super().__init__(params=_PARAMS, wait_jitter_seconds=0.0)
        self._clock: _FakeClock = clock
        self._open_until: float = clock() + open_seconds

    async def read_state(self) -> CircuitState:
        is_open: bool = self._clock() < self._open_until
        return CircuitState(
            mode='regular',
            is_open=is_open,
            open_until_ts=self._open_until if is_open else 0.0,
            current_cooldown_s=int(self._open_until - 1000.0),
            consecutive_404s=0,
            consecutive_successes=0,
        )

    async def record_outcome(
        self, *, channel_id: str, was_not_found: bool,
    ) -> CircuitReport:  # pragma: no cover - unused
        raise NotImplementedError


class TestWaitUntilClosedChunkAndTouch(unittest.TestCase):
    def setUp(self) -> None:
        self.wd: Watchdog = Watchdog(loop_timeout=0.0, work_timeout=0.0)
        self.wd.touch_work = MagicMock()
        Watchdog.set_instance(self.wd)

    def tearDown(self) -> None:
        Watchdog.reset()

    def test_long_park_sleeps_in_bounded_chunks_and_touches(
        self,
    ) -> None:
        clock = _FakeClock()
        backend = _OpenUntilBackend(clock, open_seconds=100.0)
        sleeps: list[float] = []

        async def _fake_sleep(seconds: float) -> None:
            sleeps.append(seconds)
            clock.advance(seconds)

        with patch.object(rcb.time, 'time', clock), \
                patch.object(rcb.asyncio, 'sleep', _fake_sleep):
            slept: float = asyncio.run(backend.wait_until_closed())

        # Every chunk is bounded well under the open window.
        self.assertTrue(sleeps)
        self.assertLessEqual(max(sleeps), 30.0)
        # The watchdog was touched once per chunk, not once total.
        self.assertEqual(self.wd.touch_work.call_count, len(sleeps))
        self.assertGreater(self.wd.touch_work.call_count, 1)
        # Total time parked still covers the open window.
        self.assertAlmostEqual(slept, 100.0, places=5)

    def test_closed_circuit_returns_without_touching(self) -> None:
        clock = _FakeClock()
        backend = _OpenUntilBackend(clock, open_seconds=0.0)
        with patch.object(rcb.time, 'time', clock):
            slept: float = asyncio.run(backend.wait_until_closed())
        self.assertEqual(slept, 0.0)
        self.wd.touch_work.assert_not_called()


if __name__ == '__main__':
    unittest.main()
