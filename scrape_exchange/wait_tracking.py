'''Measure scrape durations without the time spent waiting.

Scrape duration metrics should show how long YouTube (or another
platform) takes to answer, not how long a worker waited for a
rate-limiter token or slept between retries. Code that waits wraps the
wait in :func:`waiting`:

* :meth:`RateLimiter.acquire` counts as one wait (per-proxy lock,
  token sleeps, jitter).
* :func:`tracked_sleep` replaces ``asyncio.sleep`` for retry and
  penalty backoff inside a timed scrape.

A :class:`ScrapeTimer` subtracts the waiting done since it started.
Each asyncio task gets its own tracker the first time it starts a
timer. Tasks it creates afterwards inherit that tracker, so their
waits count too (for example the transcript fetch that runs alongside
NEXT). A task that starts its own timer gets a tracker linked to the
one it inherited, and passes its waits up, so a channel scrape still
sees the waits of the tab scrapes it runs in child tasks.

A tracker counts the waits in progress and adds to its total when the
count drops back to zero, so overlapping waits are counted once and a
tracker keeps no history: memory stays constant however long a worker
task lives.
'''

import asyncio
import time
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Self


def _now() -> float:
    return time.monotonic()


@dataclass
class _WaitTracker:
    '''Running total of the time one asyncio task (and the tasks it
    created) spent waiting.'''

    owner: asyncio.Task | None
    parent: _WaitTracker | None = None
    # Number of waits in progress, and when the current run of
    # overlapping waits began.
    in_progress: int = 0
    run_started: float = 0.0
    # Seconds of waiting in finished runs.
    total: float = 0.0

    def begin(self, now: float) -> None:
        if self.in_progress == 0:
            self.run_started = now
        self.in_progress += 1

    def end(self, now: float) -> None:
        self.in_progress -= 1
        if self.in_progress == 0:
            self.total += now - self.run_started

    def waited_until(self, now: float) -> float:
        '''Seconds of waiting so far, including a run in progress.'''
        if self.in_progress:
            return self.total + (now - self.run_started)
        return self.total

    def chain(self) -> Iterator[_WaitTracker]:
        tracker: _WaitTracker | None = self
        while tracker is not None:
            yield tracker
            tracker = tracker.parent


_TRACKER: ContextVar[_WaitTracker | None] = ContextVar(
    'wait_tracker', default=None,
)


def _current_task() -> asyncio.Task | None:
    try:
        return asyncio.current_task()
    except RuntimeError:
        return None


@contextmanager
def waiting() -> Iterator[None]:
    '''Count the time spent in the block as waiting, for the timers
    of the current task and the tasks above it. A no-op when no timer
    has been started in this task or a parent task.'''
    tracker: _WaitTracker | None = _TRACKER.get()
    if tracker is None:
        yield
        return
    trackers: list[_WaitTracker] = list(tracker.chain())
    started: float = _now()
    for t in trackers:
        t.begin(started)
    try:
        yield
    finally:
        ended: float = _now()
        for t in trackers:
            t.end(ended)


async def tracked_sleep(seconds: float) -> None:
    '''``asyncio.sleep`` that counts as waiting, for retry and
    penalty backoff inside a timed scrape.'''
    with waiting():
        await asyncio.sleep(seconds)


@dataclass
class ScrapeTimer:
    '''Times one scrape, separating active time from waiting.'''

    started: float
    tracker: _WaitTracker
    # The tracker's waiting total when the timer started.
    waited_at_start: float

    @classmethod
    def start(cls) -> Self:
        '''Start timing in the current task, giving the task its own
        wait tracker if it does not have one yet.'''
        task: asyncio.Task | None = _current_task()
        tracker: _WaitTracker | None = _TRACKER.get()
        if tracker is None or tracker.owner is not task:
            tracker = _WaitTracker(owner=task, parent=tracker)
            _TRACKER.set(tracker)
        now: float = _now()
        return cls(
            started=now, tracker=tracker,
            waited_at_start=tracker.waited_until(now),
        )

    def elapsed(self) -> float:
        '''Wall-clock seconds since the timer started.'''
        return _now() - self.started

    def waited(self) -> float:
        '''Seconds since the timer started spent waiting for
        rate-limiter tokens or in backoff sleeps.'''
        return (
            self.tracker.waited_until(_now()) - self.waited_at_start
        )

    def active(self) -> float:
        '''Seconds since the timer started, not counting waits.'''
        now: float = _now()
        waited: float = (
            self.tracker.waited_until(now) - self.waited_at_start
        )
        return max(0.0, now - self.started - waited)
