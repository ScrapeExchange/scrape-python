'''Tests for the RSS circuit breaker façade with the in-process
backend (no Redis required).'''

import asyncio
import unittest

from scrape_exchange.youtube._rss_circuit_state import (
    CircuitParams,
)
from scrape_exchange.youtube.rss_circuit_breaker import (
    RssCircuitBreaker,
    _InProcessCircuitBackend,
)


def _run(coro):
    loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _params() -> CircuitParams:
    return CircuitParams(
        fail_threshold=3,
        window_size=5,
        initial_open_seconds=10,
        max_open_seconds=7200,
        impaired_reopen_threshold=2,
        recovery_threshold=3,
    )


class TestInProcessBackend(unittest.TestCase):

    def test_report_until_trip(self) -> None:
        backend: _InProcessCircuitBackend = (
            _InProcessCircuitBackend(_params())
        )

        async def run() -> None:
            r1 = await backend.record_outcome(
                channel_id='UC1', was_not_found=True,
            )
            self.assertIsNone(r1.transition)
            self.assertFalse(r1.suppress_channel_failure)

            r2 = await backend.record_outcome(
                channel_id='UC2', was_not_found=True,
            )
            self.assertIsNone(r2.transition)

            r3 = await backend.record_outcome(
                channel_id='UC3', was_not_found=True,
            )
            # Trip!
            self.assertIsNotNone(r3.transition)
            self.assertEqual(
                r3.transition.to_state, 'open-regular',
            )
            self.assertTrue(r3.suppress_channel_failure)
            self.assertEqual(
                r3.rollback_channel_ids, ['UC1', 'UC2'],
            )

        _run(run())

    def test_acquire_no_op_when_closed(self) -> None:
        backend: _InProcessCircuitBackend = (
            _InProcessCircuitBackend(
                _params(), wait_jitter_seconds=999.0,
            )
        )

        async def run() -> None:
            # Closed at construction; acquire returns immediately.
            await asyncio.wait_for(
                backend.wait_until_closed(), timeout=0.1,
            )

        _run(run())

    def test_acquire_zero_jitter_returns_immediately_when_stale_open(
        self,
    ) -> None:
        '''Regression guard: with wait_jitter_seconds=0 the previous
        fast-path behavior is preserved.'''
        import time as _time
        backend: _InProcessCircuitBackend = (
            _InProcessCircuitBackend(
                _params(), wait_jitter_seconds=0.0,
            )
        )
        backend._state.is_open = True
        backend._state.mode = 'regular'
        backend._state.open_until_ts = _time.time() - 1.0

        async def run() -> None:
            start: float = _time.monotonic()
            await asyncio.wait_for(
                backend.wait_until_closed(), timeout=0.5,
            )
            self.assertLess(
                _time.monotonic() - start, 0.1,
            )

        _run(run())

    def test_acquire_applies_jitter_when_open_with_future_until(
        self,
    ) -> None:
        '''When the cooldown is still in the future, acquire sleeps
        approximately wait + jitter seconds.'''
        import time as _time
        backend: _InProcessCircuitBackend = (
            _InProcessCircuitBackend(
                _params(), wait_jitter_seconds=0.5,
            )
        )
        backend._state.is_open = True
        backend._state.mode = 'regular'
        backend._state.open_until_ts = _time.time() + 0.2

        async def run() -> None:
            start: float = _time.monotonic()
            await asyncio.wait_for(
                backend.wait_until_closed(), timeout=2.0,
            )
            elapsed: float = _time.monotonic() - start
            self.assertGreaterEqual(elapsed, 0.2)
            self.assertLessEqual(elapsed, 0.9)

        _run(run())

    def test_acquire_applies_jitter_when_stale_open(self) -> None:
        '''Stale-open acquirers spread over the jitter window.'''
        import time as _time
        backend: _InProcessCircuitBackend = (
            _InProcessCircuitBackend(
                _params(), wait_jitter_seconds=0.5,
            )
        )
        backend._state.is_open = True
        backend._state.mode = 'regular'
        backend._state.open_until_ts = _time.time() - 1.0

        async def run() -> None:
            start: float = _time.monotonic()
            slept_values: list[float] = await asyncio.gather(
                backend.wait_until_closed(),
                backend.wait_until_closed(),
                backend.wait_until_closed(),
            )
            elapsed: float = _time.monotonic() - start
            self.assertLessEqual(elapsed, 0.9)
            self.assertTrue(
                any(s > 0.0 for s in slept_values),
                f'Expected at least one non-zero sleep; got '
                f'{slept_values}',
            )

        _run(run())

    def test_acquire_wave_spread_across_jitter_window(self) -> None:
        '''Concurrent acquirers do not all return together.'''
        import time as _time
        backend: _InProcessCircuitBackend = (
            _InProcessCircuitBackend(
                _params(), wait_jitter_seconds=1.0,
            )
        )
        backend._state.is_open = True
        backend._state.mode = 'regular'
        backend._state.open_until_ts = _time.time() + 0.05

        async def one() -> float:
            await backend.wait_until_closed()
            return _time.monotonic()

        async def run() -> None:
            return_times: list[float] = await asyncio.gather(
                *[one() for _ in range(20)],
            )
            spread: float = max(return_times) - min(return_times)
            self.assertGreaterEqual(spread, 0.2)

        _run(run())

    def test_wait_jitter_seconds_stored_on_backend(self) -> None:
        backend: _InProcessCircuitBackend = (
            _InProcessCircuitBackend(
                _params(), wait_jitter_seconds=42.0,
            )
        )
        self.assertEqual(backend._wait_jitter_seconds, 42.0)


class TestFacadeGet(unittest.TestCase):

    def test_in_process_when_no_redis_no_state_dir(self) -> None:
        breaker: RssCircuitBreaker = RssCircuitBreaker.get(
            redis_dsn=None,
            state_dir=None,
            fail_threshold=3,
            window_size=5,
            initial_open_seconds=10,
            max_open_seconds=7200,
            impaired_reopen_threshold=2,
            recovery_threshold=3,
        )
        self.assertIsInstance(
            breaker._backend, _InProcessCircuitBackend,
        )

    def test_singleton_per_dsn(self) -> None:
        # Two calls with the same (None, None) → same instance.
        # Process-wide singleton is keyed on the storage target.
        a: RssCircuitBreaker = RssCircuitBreaker.get(
            redis_dsn=None, state_dir=None,
            fail_threshold=3, window_size=5,
            initial_open_seconds=10, max_open_seconds=7200,
            impaired_reopen_threshold=2, recovery_threshold=3,
        )
        b: RssCircuitBreaker = RssCircuitBreaker.get(
            redis_dsn=None, state_dir=None,
            fail_threshold=3, window_size=5,
            initial_open_seconds=10, max_open_seconds=7200,
            impaired_reopen_threshold=2, recovery_threshold=3,
        )
        self.assertIs(a, b)

    def test_redis_backend_accepts_wait_jitter_seconds(self) -> None:
        from scrape_exchange.youtube.rss_circuit_breaker import (
            _RedisCircuitBackend,
        )
        backend = _RedisCircuitBackend(
            _params(),
            redis_dsn='redis://localhost:6379/0',
            wait_jitter_seconds=15.0,
        )
        self.assertEqual(backend._wait_jitter_seconds, 15.0)

    def test_file_backend_accepts_wait_jitter_seconds(self) -> None:
        from scrape_exchange.youtube.rss_circuit_breaker import (
            _FileCircuitBackend,
        )
        backend = _FileCircuitBackend(
            _params(),
            state_dir='/tmp/nonexistent',
            wait_jitter_seconds=15.0,
        )
        self.assertEqual(backend._wait_jitter_seconds, 15.0)

    def test_get_passes_jitter_to_in_process_backend(self) -> None:
        breaker: RssCircuitBreaker = RssCircuitBreaker.get(
            redis_dsn=None, state_dir=None,
            fail_threshold=7, window_size=9,
            initial_open_seconds=11, max_open_seconds=7201,
            impaired_reopen_threshold=2, recovery_threshold=4,
            wait_jitter_seconds=7.5,
        )
        self.assertEqual(
            breaker._backend._wait_jitter_seconds, 7.5,
        )

    def test_get_singleton_distinguishes_by_jitter(self) -> None:
        a: RssCircuitBreaker = RssCircuitBreaker.get(
            redis_dsn=None, state_dir=None,
            fail_threshold=3, window_size=5,
            initial_open_seconds=10, max_open_seconds=7200,
            impaired_reopen_threshold=2, recovery_threshold=3,
            wait_jitter_seconds=10.0,
        )
        b: RssCircuitBreaker = RssCircuitBreaker.get(
            redis_dsn=None, state_dir=None,
            fail_threshold=3, window_size=5,
            initial_open_seconds=10, max_open_seconds=7200,
            impaired_reopen_threshold=2, recovery_threshold=3,
            wait_jitter_seconds=20.0,
        )
        self.assertIsNot(a, b)

    def test_get_default_jitter_is_30_seconds(self) -> None:
        breaker: RssCircuitBreaker = RssCircuitBreaker.get(
            redis_dsn=None, state_dir=None,
            fail_threshold=2, window_size=4,
            initial_open_seconds=8, max_open_seconds=7202,
            impaired_reopen_threshold=1, recovery_threshold=2,
        )
        self.assertEqual(
            breaker._backend._wait_jitter_seconds, 30.0,
        )


class TestScraperWiring(unittest.TestCase):

    def test_get_rss_circuit_breaker_passes_jitter(self) -> None:
        from scrape_exchange.youtube.settings import (
            YouTubeScraperSettings,
        )
        from tools.yt_rss_scrape import _get_rss_circuit_breaker

        s = YouTubeScraperSettings(
            _env_file=None, _cli_parse_args=[],
        )
        object.__setattr__(
            s, 'rss_circuit_wait_jitter_seconds', 12.5,
        )
        breaker = _get_rss_circuit_breaker(s)
        self.assertEqual(
            breaker._backend._wait_jitter_seconds, 12.5,
        )


class TestMigration(unittest.TestCase):
    '''The old per-proxy RSS circuit must be gone.'''

    def test_old_rss_circuit_state_removed(self) -> None:
        from scrape_exchange.youtube import youtube_rate_limiter
        self.assertFalse(
            hasattr(youtube_rate_limiter, '_RssCircuitState'),
            'Old _RssCircuitState should be removed by the '
            'migration step',
        )

    def test_old_circuit_settings_removed(self) -> None:
        from scrape_exchange.youtube.settings import (
            YouTubeScraperSettings,
        )
        s = YouTubeScraperSettings(
            _env_file=None, _cli_parse_args=[],
        )
        for old in (
            'youtube_rss_circuit_threshold',
            'youtube_rss_timeout_threshold',
            'youtube_rss_circuit_min_cooldown_seconds',
            'youtube_rss_circuit_max_cooldown_seconds',
        ):
            self.assertFalse(
                hasattr(s, old),
                f'{old} should be removed by the migration step',
            )


if __name__ == '__main__':
    unittest.main()
