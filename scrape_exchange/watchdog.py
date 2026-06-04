'''
Process liveness watchdog for the scraper fleet.

A scraper worker process can wedge silently: every async task ends up
awaiting a primitive that never completes (an unreleased lock, a
``to_thread`` future starved by a saturated executor) or the event loop
itself freezes on a blocking syscall. The process stays alive — the
Prometheus metrics server runs in a separate daemon thread — so it keeps
reading ``up=1`` while completing zero work and logging nothing. This
module turns that silent hang into an auto-recovering restart.

Two monotonic timestamps are watched by one daemon thread:

* ``touch_loop()`` is called by an async heartbeat task every few
  seconds; if it goes stale the event loop is frozen.
* ``touch_work()`` is called on each consumer-loop iteration (and from
  any intentional long sleep, e.g. a circuit-open park); if it goes
  stale every worker is wedged while the loop still spins.

On breach the daemon thread terminates the process with ``os._exit(1)``
so the supervisor respawns it. Termination must not block: a wedged
worker thread may hold a ``logging`` handler lock, so the watchdog never
calls ``logging`` — it writes the reason straight to the stderr fd and
dumps all thread stacks via :mod:`faulthandler`.

:author     : boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import faulthandler
import os
import sys
import threading
import time

from collections.abc import Callable
from typing import ClassVar


# Extra grace added to ``work_timeout`` for the independent faulthandler
# backstop timer, so it only fires after the daemon thread has had a
# clear chance to act first.
_BACKSTOP_GRACE_SECONDS: float = 30.0


def _default_write(message: str) -> None:
    '''Write *message* straight to the stderr fd. No logging lock.'''
    try:
        os.write(2, message.encode('utf-8', 'replace'))
    except OSError:
        pass


def _default_dump() -> None:
    '''Dump every thread's stack to stderr. No logging lock.'''
    try:
        faulthandler.dump_traceback(file=sys.stderr, all_threads=True)
    except Exception:
        pass


def _default_metric_inc(signal: str) -> None:
    '''Increment the durable termination counter, labelled by signal.

    Imported lazily so importing the watchdog does not pull in the
    Prometheus metric registry (and so tests can inject their own).
    '''
    try:
        from scrape_exchange.scraper_metrics import (
            METRIC_WATCHDOG_TERMINATIONS,
        )
        METRIC_WATCHDOG_TERMINATIONS.labels(signal=signal).inc()
    except Exception:
        pass


def _default_arm_backstop(timeout: float) -> None:
    '''Arm (or re-arm) the faulthandler kernel-level exit timer.'''
    try:
        faulthandler.dump_traceback_later(timeout, exit=True)
    except Exception:
        pass


def _default_cancel_backstop() -> None:
    '''Cancel the faulthandler exit timer (on clean shutdown).'''
    try:
        faulthandler.cancel_dump_traceback_later()
    except Exception:
        pass


class Watchdog:
    '''Dual-signal liveness watchdog.

    Construct with the two timeouts and optionally injected side-effect
    callables (used by tests). In production the process-wide singleton
    is installed by :meth:`set_instance` and driven by :meth:`start`.
    '''

    _instance: ClassVar['Watchdog | None'] = None

    def __init__(
        self,
        *,
        loop_timeout: float,
        work_timeout: float,
        clock: Callable[[], float] = time.monotonic,
        exit_fn: Callable[[int], None] = os._exit,
        write_fn: Callable[[str], None] = _default_write,
        dump_fn: Callable[[], None] = _default_dump,
        metric_inc: Callable[[str], None] = _default_metric_inc,
        arm_backstop: Callable[[float], None] = _default_arm_backstop,
        cancel_backstop: Callable[[], None] = _default_cancel_backstop,
    ) -> None:
        self._loop_timeout: float = loop_timeout
        self._work_timeout: float = work_timeout
        self._clock: Callable[[], float] = clock
        self._exit_fn: Callable[[int], None] = exit_fn
        self._write_fn: Callable[[str], None] = write_fn
        self._dump_fn: Callable[[], None] = dump_fn
        self._metric_inc: Callable[[str], None] = metric_inc
        self._arm_backstop: Callable[[float], None] = arm_backstop
        self._cancel_backstop: Callable[[], None] = cancel_backstop
        now: float = clock()
        self._loop_ts: float = now
        self._work_ts: float = now
        self._thread: threading.Thread | None = None
        self._stopped: bool = False

    @property
    def stopped(self) -> bool:
        '''True once :meth:`stop` has been called.'''
        return self._stopped

    # -- progress signals (called from async code) ------------------

    def touch_loop(self) -> None:
        '''Mark the event loop alive. Atomic float store in CPython.'''
        self._loop_ts = self._clock()

    def touch_work(self) -> None:
        '''Mark forward worker progress. Atomic float store.'''
        self._work_ts = self._clock()

    # -- decision logic (pure, unit-tested) -------------------------

    def check(self) -> str | None:
        '''Return the stale signal name, or ``None`` if both fresh.

        The loop signal is checked first: a frozen event loop is the
        more urgent failure and uses the tighter timeout.
        '''
        now: float = self._clock()
        if (
            self._loop_timeout > 0
            and now - self._loop_ts > self._loop_timeout
        ):
            return 'loop'
        if (
            self._work_timeout > 0
            and now - self._work_ts > self._work_timeout
        ):
            return 'work'
        return None

    def run_once(self) -> bool:
        '''One watchdog iteration. Terminate if stale; else re-arm the
        backstop. Returns ``True`` if it terminated (tests inject a
        non-exiting ``exit_fn``, so control may return).'''
        signal: str | None = self.check()
        if signal is not None:
            self._terminate(signal)
            return True
        if self._work_timeout > 0:
            self._arm_backstop(
                self._work_timeout + _BACKSTOP_GRACE_SECONDS,
            )
        return False

    def _terminate(self, signal: str) -> None:
        '''Best-effort, lock-free diagnostics then ``os._exit(1)``.'''
        stale: float = self._clock() - (
            self._loop_ts if signal == 'loop' else self._work_ts
        )
        self._write_fn(
            f'watchdog: {signal} signal stale for {stale:.0f}s; '
            f'terminating pid {os.getpid()}\n'
        )
        self._dump_fn()
        self._metric_inc(signal)
        self._exit_fn(1)

    # -- production thread ------------------------------------------

    def start(self, check_interval: float = 5.0) -> None:
        '''Spawn the daemon watchdog thread. Idempotent.'''
        if self._thread is not None:
            return
        if self._work_timeout > 0:
            self._arm_backstop(
                self._work_timeout + _BACKSTOP_GRACE_SECONDS,
            )
        thread: threading.Thread = threading.Thread(
            target=self._loop,
            args=(check_interval,),
            name='scraper-watchdog',
            daemon=True,
        )
        self._thread = thread
        thread.start()

    def stop(self) -> None:
        '''Stop the watchdog and cancel the faulthandler backstop.

        Called on clean shutdown so the daemon thread does not fire and
        the kernel exit timer is disarmed. Idempotent.
        '''
        self._stopped = True
        self._cancel_backstop()

    def _loop(self, check_interval: float) -> None:
        while not self._stopped:
            time.sleep(check_interval)
            if self._stopped:
                return
            if self.run_once():
                return

    # -- singleton --------------------------------------------------

    @classmethod
    def get(cls) -> 'Watchdog':
        '''Return the installed singleton, or a default disabled one.

        The default (timeouts 0) never fires and is never started, so
        ``touch_*`` from worker code is always safe even when no
        watchdog has been installed (e.g. in tests).
        '''
        if cls._instance is None:
            cls._instance = cls(loop_timeout=0.0, work_timeout=0.0)
        return cls._instance

    @classmethod
    def set_instance(cls, watchdog: 'Watchdog') -> None:
        '''Install *watchdog* as the process-wide singleton.'''
        cls._instance = watchdog

    @classmethod
    def reset(cls) -> None:
        '''Drop the singleton (tests).'''
        cls._instance = None
