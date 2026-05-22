'''Pure unit tests for the RSS circuit-breaker state machine.

Exercises ``apply_outcome`` without any Redis / file / asyncio
dependency. ``now`` is injected so transitions can be driven
without sleeping.'''

import unittest

from scrape_exchange.youtube._rss_circuit_state import (
    CircuitParams,
    CircuitState,
    apply_outcome,
)


def _default_params() -> CircuitParams:
    return CircuitParams(
        fail_threshold=8,
        window_size=10,
        initial_open_seconds=60,
        max_open_seconds=7200,
        impaired_reopen_threshold=3,
        recovery_threshold=50,
    )


def _initial_state() -> CircuitState:
    return CircuitState(
        mode='regular',
        is_open=False,
        open_until_ts=0.0,
        current_cooldown_s=60,
        consecutive_404s=0,
        consecutive_successes=0,
    )


class TestApplyOutcome(unittest.TestCase):

    def test_404_in_closed_regular_under_threshold(self) -> None:
        state: CircuitState = _initial_state()
        new_state, new_window, report = apply_outcome(
            state, window=[],
            channel_id='UC1', was_not_found=True,
            now=1000.0, params=_default_params(),
        )
        self.assertEqual(new_state.mode, 'regular')
        self.assertFalse(new_state.is_open)
        self.assertIsNone(report.transition)
        self.assertFalse(report.suppress_channel_failure)
        self.assertEqual(report.rollback_channel_ids, [])
        self.assertEqual(new_window, [('UC1', True)])

    def test_404_in_closed_regular_trips_at_threshold(self) -> None:
        params: CircuitParams = _default_params()
        # window already has 7 404s; the 8th trips.
        window: list[tuple[str, bool]] = [
            (f'UC{i}', True) for i in range(7)
        ]
        state: CircuitState = _initial_state()
        new_state, new_window, report = apply_outcome(
            state, window=window,
            channel_id='UC8', was_not_found=True,
            now=1000.0, params=params,
        )
        self.assertTrue(new_state.is_open)
        self.assertEqual(new_state.mode, 'regular')
        self.assertEqual(new_state.open_until_ts, 1060.0)
        self.assertIsNotNone(report.transition)
        self.assertEqual(
            report.transition.from_state, 'closed-regular',
        )
        self.assertEqual(
            report.transition.to_state, 'open-regular',
        )
        self.assertTrue(report.suppress_channel_failure)
        # Rollback: every 404 in the pre-trip window except the
        # trigger (UC8). UC0..UC6 → 7 channels.
        self.assertEqual(
            report.rollback_channel_ids,
            [f'UC{i}' for i in range(7)],
        )
        # Window is cleared on transition.
        self.assertEqual(new_window, [])

    def test_open_regular_expires_to_closed_impaired(self) -> None:
        params: CircuitParams = _default_params()
        state: CircuitState = CircuitState(
            mode='regular',
            is_open=True,
            open_until_ts=1000.0,
            current_cooldown_s=60,
            consecutive_404s=0,
            consecutive_successes=0,
        )
        # A report arrives after open_until expired. Outcome
        # contents are immaterial — the time check triggers the
        # transition first.
        new_state, _new_window, report = apply_outcome(
            state, window=[],
            channel_id='UCX', was_not_found=False,
            now=1100.0, params=params,
        )
        self.assertFalse(new_state.is_open)
        self.assertEqual(new_state.mode, 'impaired')
        # Cooldown doubled on the open→close transition.
        self.assertEqual(new_state.current_cooldown_s, 120)
        self.assertIsNotNone(report.transition)
        self.assertEqual(
            report.transition.from_state, 'open-regular',
        )
        self.assertEqual(
            report.transition.to_state, 'closed-impaired',
        )

    def test_closed_impaired_consec_404s_reopen(self) -> None:
        params: CircuitParams = _default_params()
        state: CircuitState = CircuitState(
            mode='impaired',
            is_open=False,
            open_until_ts=0.0,
            current_cooldown_s=120,
            consecutive_404s=2,  # 2 already; 3rd will trip C=3.
            consecutive_successes=0,
        )
        new_state, _new_window, report = apply_outcome(
            state, window=[],
            channel_id='UCY', was_not_found=True,
            now=2000.0, params=params,
        )
        self.assertTrue(new_state.is_open)
        self.assertEqual(new_state.mode, 'impaired')
        self.assertEqual(new_state.open_until_ts, 2120.0)
        self.assertEqual(
            report.transition.from_state, 'closed-impaired',
        )
        self.assertEqual(
            report.transition.to_state, 'open-impaired',
        )
        self.assertTrue(report.suppress_channel_failure)
        # No rollback list on impaired re-trips.
        self.assertEqual(report.rollback_channel_ids, [])

    def test_closed_impaired_n_successes_recovers(self) -> None:
        params: CircuitParams = _default_params()
        state: CircuitState = CircuitState(
            mode='impaired',
            is_open=False,
            open_until_ts=0.0,
            current_cooldown_s=480,
            consecutive_404s=0,
            consecutive_successes=49,
        )
        new_state, _new_window, report = apply_outcome(
            state, window=[],
            channel_id='UCZ', was_not_found=False,
            now=3000.0, params=params,
        )
        self.assertEqual(new_state.mode, 'regular')
        self.assertFalse(new_state.is_open)
        # S resets to the initial value on recovery.
        self.assertEqual(new_state.current_cooldown_s, 60)
        self.assertEqual(new_state.consecutive_404s, 0)
        self.assertEqual(new_state.consecutive_successes, 0)
        self.assertEqual(
            report.transition.to_state, 'closed-regular',
        )
        self.assertFalse(report.suppress_channel_failure)

    def test_open_impaired_expires_doubles_cooldown(self) -> None:
        params: CircuitParams = _default_params()
        state: CircuitState = CircuitState(
            mode='impaired',
            is_open=True,
            open_until_ts=1000.0,
            current_cooldown_s=120,
            consecutive_404s=0,
            consecutive_successes=0,
        )
        new_state, _new_window, report = apply_outcome(
            state, window=[],
            channel_id='UCW', was_not_found=False,
            now=1200.0, params=params,
        )
        self.assertFalse(new_state.is_open)
        self.assertEqual(new_state.mode, 'impaired')
        self.assertEqual(new_state.current_cooldown_s, 240)
        self.assertEqual(
            report.transition.from_state, 'open-impaired',
        )
        self.assertEqual(
            report.transition.to_state, 'closed-impaired',
        )

    def test_cooldown_doubling_caps_at_max(self) -> None:
        params: CircuitParams = _default_params()
        state: CircuitState = CircuitState(
            mode='impaired',
            is_open=True,
            open_until_ts=1000.0,
            current_cooldown_s=3840,
            consecutive_404s=0,
            consecutive_successes=0,
        )
        new_state, _new_window, _report = apply_outcome(
            state, window=[],
            channel_id='UCV', was_not_found=False,
            now=2000.0, params=params,
        )
        # 3840 * 2 = 7680 > 7200 cap; clamps at 7200.
        self.assertEqual(new_state.current_cooldown_s, 7200)

    def test_never_scraped_404_does_not_enter_window(
        self,
    ) -> None:
        # The breaker is told never-scraped channels via the
        # façade NOT calling apply_outcome at all. Verify here
        # that a success on a previously-scraped channel does
        # NOT increment consecutive_404s and resets it instead.
        params: CircuitParams = _default_params()
        state: CircuitState = CircuitState(
            mode='impaired',
            is_open=False,
            open_until_ts=0.0,
            current_cooldown_s=120,
            consecutive_404s=2,
            consecutive_successes=0,
        )
        new_state, _w, _r = apply_outcome(
            state, window=[],
            channel_id='UCQ', was_not_found=False,
            now=4000.0, params=params,
        )
        # A success resets consecutive_404s.
        self.assertEqual(new_state.consecutive_404s, 0)
        self.assertEqual(new_state.consecutive_successes, 1)

    def test_window_keeps_last_T_outcomes(self) -> None:
        params: CircuitParams = _default_params()
        # Pre-fill window with T=10 success entries.
        window: list[tuple[str, bool]] = [
            (f'UC{i}', False) for i in range(10)
        ]
        state: CircuitState = _initial_state()
        new_state, new_window, _r = apply_outcome(
            state, window=window,
            channel_id='UC10', was_not_found=False,
            now=5000.0, params=params,
        )
        # New entry appended, oldest dropped — still T entries.
        self.assertEqual(len(new_window), 10)
        self.assertEqual(new_window[-1], ('UC10', False))
        self.assertEqual(new_window[0], ('UC1', False))


if __name__ == '__main__':
    unittest.main()
