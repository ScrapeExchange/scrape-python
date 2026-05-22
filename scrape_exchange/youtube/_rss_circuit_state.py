'''Pure state machine for the RSS circuit breaker.

This module defines the dataclasses and the single pure
function ``apply_outcome`` that every backend (Redis, file,
in-process) calls under its own atomicity primitive. Keeping
the rules in one place avoids drift between implementations.

The Lua version embedded in the Redis backend is a hand-port
of this same function; integration tests against a real Redis
exercise the Lua and keep it honest.
'''

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class CircuitParams:
    '''Tunables, supplied by pydantic-settings at construction.'''
    fail_threshold: int             # F
    window_size: int                # T
    initial_open_seconds: int       # S
    max_open_seconds: int           # cap on doubled S
    impaired_reopen_threshold: int  # C
    recovery_threshold: int         # N


@dataclass
class CircuitState:
    mode: str               # 'regular' | 'impaired'
    is_open: bool
    open_until_ts: float    # 0.0 when closed
    current_cooldown_s: int
    consecutive_404s: int
    consecutive_successes: int


@dataclass
class CircuitTransition:
    from_state: str
    to_state: str
    cooldown_seconds: int


@dataclass
class CircuitReport:
    transition: CircuitTransition | None
    suppress_channel_failure: bool
    rollback_channel_ids: list[str] = field(default_factory=list)
    state_after: CircuitState | None = None


def _state_label(state: CircuitState) -> str:
    open_str: str = 'open' if state.is_open else 'closed'
    return f'{open_str}-{state.mode}'


def _double_capped(current: int, cap: int) -> int:
    doubled: int = current * 2
    return cap if doubled > cap else doubled


def apply_outcome(
    state: CircuitState,
    window: list[tuple[str, bool]],
    *,
    channel_id: str,
    was_not_found: bool,
    now: float,
    params: CircuitParams,
) -> tuple[CircuitState, list[tuple[str, bool]], CircuitReport]:
    '''Pure: given current state, window, outcome and params,
    return (new_state, new_window, CircuitReport).

    The window argument is the snapshot *before* the current
    outcome is appended. The returned window reflects the
    append + the state-transition-driven clear.
    '''
    before_label: str = _state_label(state)

    # First: time-driven open→closed transitions, regardless of
    # the current outcome. (The outcome will still be applied
    # to the resulting closed state below.)
    if state.is_open and now >= state.open_until_ts:
        new_cooldown: int = _double_capped(
            state.current_cooldown_s, params.max_open_seconds,
        )
        new_state: CircuitState = CircuitState(
            mode='impaired',
            is_open=False,
            open_until_ts=0.0,
            current_cooldown_s=new_cooldown,
            consecutive_404s=0,
            consecutive_successes=0,
        )
        transition: CircuitTransition = CircuitTransition(
            from_state=before_label,
            to_state=_state_label(new_state),
            cooldown_seconds=0,
        )
        # Window cleared on transition.
        return (
            new_state,
            [],
            CircuitReport(
                transition=transition,
                suppress_channel_failure=True,
                rollback_channel_ids=[],
                state_after=new_state,
            ),
        )

    # Still open and not yet expired — no state change, no
    # window update, no rollback. Caller acquire()s elsewhere;
    # this branch is defensive in case a report() races with
    # an open state.
    if state.is_open:
        return (
            state,
            window,
            CircuitReport(
                transition=None,
                suppress_channel_failure=True,
                rollback_channel_ids=[],
                state_after=state,
            ),
        )

    # Closed. Append outcome and trim to window_size.
    new_window: list[tuple[str, bool]] = (
        window + [(channel_id, was_not_found)]
    )
    if len(new_window) > params.window_size:
        new_window = new_window[-params.window_size:]

    if state.mode == 'regular':
        # In closed-regular we evaluate F-of-T on the window.
        count_404: int = sum(1 for _, f in new_window if f)
        if count_404 >= params.fail_threshold:
            # TRIP.
            new_state = CircuitState(
                mode='regular',
                is_open=True,
                open_until_ts=now + state.current_cooldown_s,
                current_cooldown_s=state.current_cooldown_s,
                consecutive_404s=0,
                consecutive_successes=0,
            )
            # Rollback list: every 404 in the pre-trip window
            # except the trigger (the entry we just appended).
            rollback: list[str] = [
                cid for cid, f in new_window[:-1] if f
            ]
            transition = CircuitTransition(
                from_state=before_label,
                to_state=_state_label(new_state),
                cooldown_seconds=state.current_cooldown_s,
            )
            return (
                new_state,
                [],   # window cleared on transition
                CircuitReport(
                    transition=transition,
                    suppress_channel_failure=True,
                    rollback_channel_ids=rollback,
                    state_after=new_state,
                ),
            )
        # No trip — closed-regular stays closed-regular.
        return (
            state,
            new_window,
            CircuitReport(
                transition=None,
                suppress_channel_failure=False,
                rollback_channel_ids=[],
                state_after=state,
            ),
        )

    # state.mode == 'impaired', is_open == False.
    if was_not_found:
        new_404s: int = state.consecutive_404s + 1
        new_successes: int = 0
    else:
        new_404s = 0
        new_successes = state.consecutive_successes + 1

    if new_404s >= params.impaired_reopen_threshold:
        # RE-TRIP.
        new_state = CircuitState(
            mode='impaired',
            is_open=True,
            open_until_ts=now + state.current_cooldown_s,
            current_cooldown_s=state.current_cooldown_s,
            consecutive_404s=0,
            consecutive_successes=0,
        )
        transition = CircuitTransition(
            from_state=before_label,
            to_state=_state_label(new_state),
            cooldown_seconds=state.current_cooldown_s,
        )
        return (
            new_state,
            [],
            CircuitReport(
                transition=transition,
                suppress_channel_failure=True,
                rollback_channel_ids=[],
                state_after=new_state,
            ),
        )

    if new_successes >= params.recovery_threshold:
        # RECOVERY.
        new_state = CircuitState(
            mode='regular',
            is_open=False,
            open_until_ts=0.0,
            current_cooldown_s=params.initial_open_seconds,
            consecutive_404s=0,
            consecutive_successes=0,
        )
        transition = CircuitTransition(
            from_state=before_label,
            to_state=_state_label(new_state),
            cooldown_seconds=0,
        )
        return (
            new_state,
            [],
            CircuitReport(
                transition=transition,
                suppress_channel_failure=False,
                rollback_channel_ids=[],
                state_after=new_state,
            ),
        )

    # No transition; stay closed-impaired, advance counters.
    new_state = CircuitState(
        mode='impaired',
        is_open=False,
        open_until_ts=0.0,
        current_cooldown_s=state.current_cooldown_s,
        consecutive_404s=new_404s,
        consecutive_successes=new_successes,
    )
    return (
        new_state,
        new_window,
        CircuitReport(
            transition=None,
            suppress_channel_failure=True,
            rollback_channel_ids=[],
            state_after=new_state,
        ),
    )
