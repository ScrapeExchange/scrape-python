'''Unit tests for scrape_exchange.wait_tracking.'''

import asyncio
import unittest
from unittest.mock import patch

from scrape_exchange import wait_tracking
from scrape_exchange.wait_tracking import (
    ScrapeTimer,
    tracked_sleep,
    waiting,
)


class _Clock:
    '''Settable stand-in for wait_tracking._now.'''

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

    def _wait(self, seconds: float) -> None:
        '''One wait of *seconds* in the current task.'''
        with waiting():
            self.clock.now += seconds

    async def test_no_waits_active_is_elapsed(self) -> None:
        timer: ScrapeTimer = ScrapeTimer.start()
        self.clock.now += 5.0
        self.assertAlmostEqual(timer.waited(), 0.0)
        self.assertAlmostEqual(timer.active(), 5.0)
        self.assertAlmostEqual(timer.elapsed(), 5.0)

    async def test_wait_is_subtracted(self) -> None:
        timer: ScrapeTimer = ScrapeTimer.start()
        self.clock.now += 1.0
        self._wait(2.0)
        self.clock.now += 2.0
        self.assertAlmostEqual(timer.waited(), 2.0)
        self.assertAlmostEqual(timer.active(), 3.0)

    async def test_overlapping_waits_counted_once(self) -> None:
        timer: ScrapeTimer = ScrapeTimer.start()
        # 1001-1003 and 1002-1004 overlap: 3s of waiting, not 4s.
        self.clock.now = 1001.0
        first = waiting()
        first.__enter__()
        self.clock.now = 1002.0
        second = waiting()
        second.__enter__()
        self.clock.now = 1003.0
        first.__exit__(None, None, None)
        self.clock.now = 1004.0
        second.__exit__(None, None, None)
        self.clock.now = 1006.0
        self._wait(1.0)
        self.clock.now = 1010.0
        self.assertAlmostEqual(timer.waited(), 4.0)
        self.assertAlmostEqual(timer.active(), 6.0)

    async def test_wait_in_progress_counts(self) -> None:
        timer: ScrapeTimer = ScrapeTimer.start()
        with waiting():
            self.clock.now += 3.0
            self.assertAlmostEqual(timer.waited(), 3.0)
            self.assertAlmostEqual(timer.active(), 0.0)

    async def test_wait_before_start_is_not_counted(self) -> None:
        ScrapeTimer.start()
        self._wait(4.0)
        timer: ScrapeTimer = ScrapeTimer.start()
        self.clock.now += 5.0
        self.assertAlmostEqual(timer.waited(), 0.0)

    async def test_timer_started_mid_wait(self) -> None:
        ScrapeTimer.start()
        with waiting():
            self.clock.now += 1.0
            timer: ScrapeTimer = ScrapeTimer.start()
            self.clock.now += 2.0
        self.clock.now += 4.0
        self.assertAlmostEqual(timer.waited(), 2.0)
        self.assertAlmostEqual(timer.active(), 4.0)

    async def test_exception_ends_wait(self) -> None:
        timer: ScrapeTimer = ScrapeTimer.start()
        with self.assertRaises(RuntimeError), waiting():
            self.clock.now += 2.0
            raise RuntimeError('boom')
        self.clock.now += 3.0
        self.assertAlmostEqual(timer.waited(), 2.0)

    async def test_child_task_waits_are_recorded(self) -> None:
        timer: ScrapeTimer = ScrapeTimer.start()

        async def child() -> None:
            self._wait(1.5)

        await asyncio.create_task(child())
        self.clock.now += 5.0
        self.assertAlmostEqual(timer.waited(), 1.5)

    async def test_child_timer_waits_reach_parent_timer(self) -> None:
        parent: ScrapeTimer = ScrapeTimer.start()
        children: list[ScrapeTimer] = []

        async def child() -> None:
            timer: ScrapeTimer = ScrapeTimer.start()
            children.append(timer)
            self._wait(2.0)
            self.clock.now += 1.0

        await asyncio.create_task(child())
        self.assertAlmostEqual(children[0].waited(), 2.0)
        self.assertAlmostEqual(children[0].active(), 1.0)
        self.assertAlmostEqual(parent.waited(), 2.0)
        self.assertAlmostEqual(parent.active(), 1.0)

    async def test_sibling_tasks_are_isolated(self) -> None:
        timers: dict[str, ScrapeTimer] = {}
        started: asyncio.Event = asyncio.Event()

        async def worker(name: str, seconds: float) -> None:
            timers[name] = ScrapeTimer.start()
            await started.wait()
            self._wait(seconds)

        tasks: list[asyncio.Task] = [
            asyncio.create_task(worker('a', 1.0)),
            asyncio.create_task(worker('b', 2.0)),
        ]
        await asyncio.sleep(0)
        started.set()
        await asyncio.gather(*tasks)
        self.assertAlmostEqual(timers['a'].waited(), 1.0)
        self.assertAlmostEqual(timers['b'].waited(), 2.0)

    async def test_waiting_without_timer_is_noop(self) -> None:
        async def lone() -> None:
            self._wait(1.0)

        await asyncio.create_task(lone())

    async def test_tracker_keeps_no_history(self) -> None:
        ScrapeTimer.start()
        tracker = wait_tracking._TRACKER.get()
        for _ in range(1000):
            self._wait(0.5)
        self.assertEqual(tracker.in_progress, 0)
        self.assertAlmostEqual(tracker.total, 500.0)

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
