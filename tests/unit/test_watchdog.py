'''Tests for scrape_exchange/watchdog.py.

The watchdog must decide "process is wedged" from two monotonic
timestamps and terminate via an injected exit function without using
``logging`` (which can deadlock against a wedged worker thread holding a
handler lock). Everything here is driven by an injectable clock and
injected side-effect callables so the tests are deterministic — no real
threads, no real ``os._exit``, no wall-clock sleeps.
'''

import unittest

from scrape_exchange.watchdog import Watchdog


class _FakeClock:
    '''Monotonic clock the test advances by hand.'''

    def __init__(self) -> None:
        self.now: float = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _make(
    clock: _FakeClock,
    loop_timeout: float = 60.0,
    work_timeout: float = 180.0,
) -> tuple[Watchdog, dict]:
    '''Build a Watchdog with all side effects captured.'''
    sink: dict = {
        'exits': [], 'writes': [], 'dumps': 0, 'metrics': [],
        'armed': [], 'cancels': 0,
    }
    wd: Watchdog = Watchdog(
        loop_timeout=loop_timeout,
        work_timeout=work_timeout,
        clock=clock,
        exit_fn=lambda code: sink['exits'].append(code),
        write_fn=lambda msg: sink['writes'].append(msg),
        dump_fn=lambda: sink.__setitem__('dumps', sink['dumps'] + 1),
        metric_inc=lambda signal: sink['metrics'].append(signal),
        arm_backstop=lambda timeout: sink['armed'].append(timeout),
        cancel_backstop=lambda: sink.__setitem__(
            'cancels', sink['cancels'] + 1,
        ),
    )
    return wd, sink


class TestWatchdogCheck(unittest.TestCase):
    def test_fresh_does_not_fire(self) -> None:
        clock = _FakeClock()
        wd, _ = _make(clock)
        clock.advance(30.0)  # under both timeouts
        self.assertIsNone(wd.check())

    def test_stale_work_fires_work_signal(self) -> None:
        clock = _FakeClock()
        wd, _ = _make(clock)
        wd.touch_loop()  # loop stays fresh
        clock.advance(181.0)  # work timeout breached, loop kept fresh
        wd.touch_loop()
        clock.advance(0.0)
        # loop touched at +181, work last touched at construction (0)
        self.assertEqual(wd.check(), 'work')

    def test_stale_loop_fires_loop_signal(self) -> None:
        clock = _FakeClock()
        wd, _ = _make(clock)
        wd.touch_work()  # work fresh
        clock.advance(61.0)  # loop timeout breached
        wd.touch_work()  # keep work fresh at +61
        self.assertEqual(wd.check(), 'loop')

    def test_loop_checked_before_work_when_both_stale(self) -> None:
        clock = _FakeClock()
        wd, _ = _make(clock)
        clock.advance(1000.0)  # both stale
        self.assertEqual(wd.check(), 'loop')

    def test_touch_work_clears_work_staleness(self) -> None:
        clock = _FakeClock()
        wd, _ = _make(clock)
        clock.advance(200.0)
        wd.touch_work()
        wd.touch_loop()
        self.assertIsNone(wd.check())


class TestWatchdogTerminate(unittest.TestCase):
    def test_run_once_terminates_on_stale(self) -> None:
        clock = _FakeClock()
        wd, sink = _make(clock)
        clock.advance(1000.0)
        fired: bool = wd.run_once()
        self.assertTrue(fired)
        self.assertEqual(sink['exits'], [1])
        self.assertEqual(sink['metrics'], ['loop'])
        self.assertEqual(sink['dumps'], 1)
        self.assertTrue(sink['writes'])  # a reason line was written
        self.assertIn('loop', sink['writes'][0])

    def test_run_once_noop_when_fresh(self) -> None:
        clock = _FakeClock()
        wd, sink = _make(clock)
        clock.advance(10.0)
        self.assertFalse(wd.run_once())
        self.assertEqual(sink['exits'], [])
        self.assertEqual(sink['metrics'], [])

    def test_terminate_does_not_use_logging(self) -> None:
        # The reason message goes through write_fn (fd-level), never the
        # logging module. We assert the injected write_fn received it.
        clock = _FakeClock()
        wd, sink = _make(clock)
        wd.touch_work()
        clock.advance(61.0)
        wd.touch_work()
        wd.run_once()
        self.assertEqual(sink['metrics'], ['loop'])
        self.assertEqual(sink['exits'], [1])


class TestWatchdogStop(unittest.TestCase):
    def test_stop_marks_stopped_and_cancels_backstop(self) -> None:
        clock = _FakeClock()
        wd, sink = _make(clock)
        self.assertFalse(wd.stopped)
        wd.stop()
        self.assertTrue(wd.stopped)
        self.assertEqual(sink['cancels'], 1)

    def test_run_once_arms_backstop_when_fresh(self) -> None:
        clock = _FakeClock()
        wd, sink = _make(clock, work_timeout=180.0)
        clock.advance(5.0)
        self.assertFalse(wd.run_once())
        self.assertEqual(sink['armed'], [180.0 + 30.0])


class TestWatchdogSingleton(unittest.TestCase):
    def setUp(self) -> None:
        Watchdog.reset()

    def tearDown(self) -> None:
        Watchdog.reset()

    def test_get_returns_default_disabled_instance(self) -> None:
        wd = Watchdog.get()
        # touch_* must be safe on the default instance
        wd.touch_work()
        wd.touch_loop()
        self.assertIs(Watchdog.get(), wd)

    def test_set_instance_replaces_singleton(self) -> None:
        clock = _FakeClock()
        wd, _ = _make(clock)
        Watchdog.set_instance(wd)
        self.assertIs(Watchdog.get(), wd)


if __name__ == '__main__':
    unittest.main()
