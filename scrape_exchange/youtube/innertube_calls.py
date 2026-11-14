'''Track which YouTube calls one video scrape attempt made.

The video scraper labels its ``scrape_duration_seconds`` observations
with the calls that were sent, e.g. ``player_next_transcript``, so the
cost of optional calls (transcript, channel handle lookup) is visible.
'''

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar, Token

# Order in which calls appear in the label value.
CALL_ORDER: tuple[str, ...] = (
    'player', 'next', 'transcript', 'ytdlp', 'channel_browse',
)

_CALLS: ContextVar[set[str] | None] = ContextVar(
    'innertube_calls', default=None,
)


def record_call(call: str) -> None:
    '''Note that *call* was sent; a no-op outside :func:`track_calls`.'''
    calls: set[str] | None = _CALLS.get()
    if calls is not None:
        calls.add(call)


@contextmanager
def track_calls() -> Iterator[set[str]]:
    '''Collect the calls sent within the block (same asyncio task).'''
    calls: set[str] = set()
    token: Token = _CALLS.set(calls)
    try:
        yield calls
    finally:
        _CALLS.reset(token)


def calls_label(calls: set[str]) -> str:
    '''Render *calls* as a label value, 'none' when empty.'''
    return '_'.join(c for c in CALL_ORDER if c in calls) or 'none'
