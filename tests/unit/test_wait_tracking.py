'''Unit tests for scrape_exchange.wait_tracking.'''

import asyncio
import unittest
from unittest.mock import patch

from scrape_exchange import wait_tracking
from scrape_exchange.wait_tracking import (
    ScrapeTimer,
    record_wait,
    tracked_sleep,
)


class _Clock:
    '''Settable stand-in for time.monotonic.'''

    def __init__(self, now: float = 1000.0) -> None:
        self.now: float = now

    def __call__(self) -> float:
        return self.now


class TestScrapeTimer(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.clock: _Clock = _Clock()
        patcher = patch.object(wait_tracking, '_now', self.clock)
        patcher.start()
        self.addCleanup(patcher.stop)

    async def test_no_waits_active_is_elapsed(self) -> None:
        timer: ScrapeTimer = ScrapeTimer.start()
        self.clock.now += 5.0
        self.assertAlmostEqual(timer.waited(), 0.0)
        self.assertAlmostEqual(timer.active(), 5.0)

    async def test_wait_is_subtracted(self) -> None:
        timer: ScrapeTimer = ScrapeTimer.start()
        record_wait(1001.0, 1003.0)
        self.clock.now += 5.0
        self.assertAlmostEqual(timer.waited(), 2.0)
        self.assertAlmostEqual(timer.active(), 3.0)

    async def test_overlapping_waits_counted_once(self) -> None:
        timer: ScrapeTimer = ScrapeTimer.start()
        record_wait(1001.0, 1003.0)
        record_wait(1002.0, 1004.0)
        record_wait(1006.0, 1007.0)
        self.clock.now += 10.0
        self.assertAlmostEqual(timer.waited(), 4.0)
        self.assertAlmostEqual(timer.active(), 6.0)

    async def test_wait_before_start_is_clipped(self) -> None:
        record_wait(998.0, 1002.0)
        timer: ScrapeTimer = ScrapeTimer.start()
        self.clock.now += 5.0
        self.assertAlmostEqual(timer.waited(), 0.0)
        # A wait that began before the timer counts from its start.
        timer2: ScrapeTimer = ScrapeTimer.start()
        record_wait(self.clock.now - 1.0, self.clock.now + 2.0)
        self.clock.now += 4.0
        self.assertAlmostEqual(timer2.waited(), 2.0)

    async def test_child_task_waits_are_recorded(self) -> None:
        timer: ScrapeTimer = ScrapeTimer.start()

        async def child() -> None:
            record_wait(1001.0, 1002.5)

        await asyncio.create_task(child())
        self.clock.now += 5.0
        self.assertAlmostEqual(timer.waited(), 1.5)

    async def test_sibling_tasks_are_isolated(self) -> None:
        timers: dict[str, ScrapeTimer] = {}
        step: asyncio.Event = asyncio.Event()

        async def worker(name: str, wait_start: float) -> None:
            timers[name] = ScrapeTimer.start()
            await step.wait()
            record_wait(wait_start, wait_start + 1.0)

        # The parent task already has a tracker; workers must not
        # share it.
        ScrapeTimer.start()
        tasks: list[asyncio.Task] = [
            asyncio.create_task(worker('a', 1001.0)),
            asyncio.create_task(worker('b', 1003.0)),
        ]
        await asyncio.sleep(0)
        step.set()
        await asyncio.gather(*tasks)
        self.clock.now += 10.0
        self.assertAlmostEqual(timers['a'].waited(), 1.0)
        self.assertAlmostEqual(timers['b'].waited(), 1.0)

    async def test_record_without_timer_is_noop(self) -> None:
        async def lone() -> None:
            record_wait(1.0, 2.0)

        await asyncio.create_task(lone())

    async def test_old_intervals_are_pruned(self) -> None:
        ScrapeTimer.start()
        tracker = wait_tracking._TRACKER.get()
        record_wait(1000.0, 1001.0)
        self.clock.now += wait_tracking._RETENTION_SECONDS + 10.0
        record_wait(self.clock.now - 1.0, self.clock.now)
        self.assertEqual(len(tracker.intervals), 1)

    async def test_tracked_sleep_records_wait(self) -> None:
        timer: ScrapeTimer = ScrapeTimer.start()

        async def fake_sleep(seconds: float) -> None:
            self.clock.now += seconds

        with patch.object(wait_tracking.asyncio, 'sleep', fake_sleep):
            await tracked_sleep(3.0)
        self.clock.now += 1.0
        self.assertAlmostEqual(timer.waited(), 3.0)
        self.assertAlmostEqual(timer.active(), 1.0)


if __name__ == '__main__':
    unittest.main()
