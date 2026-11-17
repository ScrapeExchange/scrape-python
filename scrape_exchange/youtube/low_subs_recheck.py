'''
Re-check of YouTube channels parked in the ``low_subs`` end state.

A channel below ``CHANNEL_MIN_SUBSCRIBERS`` is parked in the terminal
``low_subs`` state and its files are kept on disk with the ``.low_subs``
suffix. Channels grow, so the re-check fetches their current counts and
moves the ones that are no longer tiny back into their regular tier.
Channels it moves are added to :data:`LOW_SUBS_RELEASED_KEY`, where the
video scraper's re-check mode picks them up to release their videos.
'''

import enum
import json
from dataclasses import dataclass
from typing import Any

from ..channel_scrape_queue import KEY_PREFIX as CHANNEL_KEY_PREFIX
from .privacy import ChannelSize, classify_for_settings, parse_count
from .settings import YouTubeScraperSettings

# Redis set of channel IDs the channel re-check moved out of
# ``low_subs``; the video re-check releases their ``.low_subs`` videos.
LOW_SUBS_RELEASED_KEY: str = f'{CHANNEL_KEY_PREFIX}:low_subs_released'

SECONDS_PER_DAY: float = 86400.0


@dataclass
class LowSubsEntry:
    '''A channel parked in ``low_subs``, from its state-hash record.'''

    member: str
    channel_id: str
    subscriber_count: int | None
    view_count: int | None
    checked_at: float | None
    record: dict[str, Any]


class RecheckAction(str, enum.Enum):
    REQUEUE = 'requeue'
    TOPIC = 'topic'
    STILL_TINY = 'still_tiny'


def _load_record(raw: str | None) -> dict[str, Any]:
    '''The JSON record of a parked member; ``{}`` when it is missing,
    not JSON or not a JSON object.'''
    if not raw:
        return {}
    try:
        record: Any = json.loads(raw)
    except ValueError:
        return {}
    return record if isinstance(record, dict) else {}


def _as_timestamp(value: Any) -> float | None:
    if value is None or value == '':
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_low_subs_entries(
    entries: list[tuple[str, str | None]],
) -> list[LowSubsEntry]:
    '''Turn the ``(member, record)`` pairs of the ``low_subs`` state
    hash into entries. Only channel-ID (``i:``) members are kept; a bad
    record counts as never checked with unknown counts.'''
    result: list[LowSubsEntry] = []
    member: str
    raw: str | None
    for member, raw in entries:
        if not member.startswith('i:'):
            continue
        record: dict[str, Any] = _load_record(raw)
        result.append(LowSubsEntry(
            member=member,
            channel_id=member[2:],
            subscriber_count=parse_count(record.get('subscriber_count')),
            view_count=parse_count(record.get('view_count')),
            checked_at=_as_timestamp(record.get('checked_at')),
            record=record,
        ))
    return result


def select_due(
    entries: list[LowSubsEntry],
    *,
    now: float,
    recheck_after_days: float,
    limit: int | None,
) -> tuple[list[LowSubsEntry], int]:
    '''The entries due for a re-check, in the order to check them, and
    the number skipped because they were checked recently.

    Never-checked channels come first; within each group the largest
    stored subscriber count comes first and unknown counts last.'''
    cutoff: float = now - recheck_after_days * SECONDS_PER_DAY
    due: list[LowSubsEntry] = [
        entry for entry in entries
        if entry.checked_at is None or entry.checked_at <= cutoff
    ]
    skipped: int = len(entries) - len(due)
    due.sort(key=lambda entry: (
        entry.checked_at is not None,
        entry.subscriber_count is None,
        -(entry.subscriber_count or 0),
    ))
    if limit is not None:
        due = due[:limit]
    return due, skipped


def effective_count(fresh: int | None, stored: int | None) -> int | None:
    '''The fresh count, else the stored one: a single parse miss must
    not make a known channel tiny.'''
    return fresh if fresh is not None else stored


def decide(
    subscriber_count: int | None,
    view_count: int | None,
    is_topic: bool,
    settings: YouTubeScraperSettings,
) -> RecheckAction:
    '''What to do with a re-checked channel, from its effective
    counts.'''
    if is_topic:
        return RecheckAction.TOPIC
    size: ChannelSize = classify_for_settings(
        subscriber_count, view_count, settings,
    )
    if size is ChannelSize.TINY:
        return RecheckAction.STILL_TINY
    return RecheckAction.REQUEUE
