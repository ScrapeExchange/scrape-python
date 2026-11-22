'''Measure scrape durations without the time spent waiting.

Scrape duration metrics should show how long YouTube (or another
platform) takes to answer, not how long a worker waited for a
rate-limiter token or slept between retries. Waits are recorded as
wall-clock intervals:

* :meth:`RateLimiter.acquire` records its whole duration (per-proxy
  lock, token sleeps, jitter).
* :func:`tracked_sleep` replaces ``asyncio.sleep`` for retry and
  penalty backoff inside a timed scrape.

A :class:`ScrapeTimer` subtracts the waits recorded since it started.
Each asyncio task gets its own tracker the first time it starts a
timer; tasks it creates afterwards (for example the transcript fetch
that runs alongside NEXT) inherit that tracker, so their waits count
too. Waits that overlap are counted once.
'''

import asyncio
import time
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Self

# Intervals that ended longer ago than this are dropped. No timed
# scrape runs this long, so pruning never loses a wait that matters.
_RETENTION_SECONDS: float = 7200.0


@dataclass
class _WaitTracker:
    '''Wait intervals recorded by one asyncio task and its children.'''

    owner: asyncio.Task | None
    intervals: list[tuple[float, float]] = field(default_factory=list)

    def add(self, start: float, end: float) -> None:
        if end <= start:
            return
        cutoff: float = end - _RETENTION_SECONDS
        if self.intervals and self.intervals[0][1] < cutoff:
            self.intervals = [
                iv for iv in self.intervals if iv[1] >= cutoff
            ]
        self.intervals.append((start, end))

    def waited_between(self, start: float, end: float) -> float:
        '''Seconds within [start, end] covered by any wait interval.'''
        clipped: list[tuple[float, float]] = sorted(
            (max(s, start), min(e, end))
            for s, e in self.intervals
            if e > start and s < end
        )
        total: float = 0.0
        cur_start: float | None = None
        cur_end: float = 0.0
        for s, e in clipped:
            if cur_start is None or s > cur_end:
                if cur_start is not None:
                    total += cur_end - cur_start
                cur_start, cur_end = s, e
            elif e > cur_end:
                cur_end = e
        if cur_start is not None:
            total += cur_end - cur_start
        return total


def _now() -> float:
    return time.monotonic()


_TRACKER: ContextVar[_WaitTracker | None] = ContextVar(
    'wait_tracker', default=None,
)


def _current_task() -> asyncio.Task | None:
    try:
        return asyncio.current_task()
    except RuntimeError:
        return None


def record_wait(start: float, end: float) -> None:
    '''Record that the current task waited from *start* to *end*
    (``time.monotonic`` values). A no-op when no timer is running in
    this task or a parent task.'''
    tracker: _WaitTracker | None = _TRACKER.get()
    if tracker is not None:
        tracker.add(start, end)


async def tracked_sleep(seconds: float) -> None:
    '''``asyncio.sleep`` that counts as waiting, for retry and
    penalty backoff inside a timed scrape.'''
    start: float = _now()
    try:
        await asyncio.sleep(seconds)
    finally:
        record_wait(start, _now())


@dataclass
class ScrapeTimer:
    '''Times one scrape, separating active time from waiting.'''

    started: float
    tracker: _WaitTracker

    @classmethod
    def start(cls) -> Self:
        '''Start timing in the current task, giving the task its own
        wait tracker if it does not have one yet.'''
        task: asyncio.Task | None = _current_task()
        tracker: _WaitTracker | None = _TRACKER.get()
        if tracker is None or tracker.owner is not task:
            tracker = _WaitTracker(owner=task)
            _TRACKER.set(tracker)
        return cls(started=_now(), tracker=tracker)

    def elapsed(self) -> float:
        '''Wall-clock seconds since the timer started.'''
        return _now() - self.started

    def waited(self) -> float:
        '''Seconds since the timer started spent waiting for
        rate-limiter tokens or in backoff sleeps.'''
        return self.tracker.waited_between(
            self.started, _now(),
        )

    def active(self) -> float:
        '''Seconds since the timer started, not counting waits.'''
        now: float = _now()
        return max(
            0.0,
            now - self.started
            - self.tracker.waited_between(self.started, now),
        )
