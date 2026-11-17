'''
Re-check of YouTube channels parked in the ``low_subs`` end state.

A channel below ``CHANNEL_MIN_SUBSCRIBERS`` is parked in the terminal
``low_subs`` state and its files are kept on disk with the ``.low_subs``
suffix. Channels grow, so the re-check fetches their current counts and
moves the ones that are no longer tiny back into their regular tier.
Channels it moves are added to :data:`LOW_SUBS_RELEASED_KEY`, where the
video scraper's re-check mode picks them up to release their videos.
'''

import asyncio
import enum
import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from ..channel_scrape_queue import KEY_PREFIX as CHANNEL_KEY_PREFIX
from ..channel_scrape_queue import ChannelState, RedisChannelScrapeQueue
from .privacy import ChannelSize, classify_for_settings, parse_count
from .settings import YouTubeScraperSettings
from .youtube_channel import YouTubeChannel

_LOGGER: logging.Logger = logging.getLogger(__name__)

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


@dataclass
class BrowseResult:
    '''An InnerTube browse of a channel: the response and whether its
    title makes it a YouTube topic channel.'''

    data: dict
    is_topic: bool


BrowseFn = Callable[[str], Awaitable[BrowseResult]]


@dataclass
class RecheckSummary:
    '''Outcome counts of one channel pass. ``checked`` counts the due
    entries attempted, failed browses included.'''

    checked: int = 0
    requeued: int = 0
    topic: int = 0
    still_tiny: int = 0
    failed: int = 0
    skipped_recent: int = 0

    def line(self, apply: bool) -> str:
        return (
            f'recheck-low-subs: checked={self.checked} '
            f'requeued={self.requeued} topic={self.topic} '
            f'still_tiny={self.still_tiny} failed={self.failed} '
            f'skipped_recent={self.skipped_recent} '
            f'dry_run={str(not apply).lower()}'
        )


def _count_fields(
    subscriber_count: int | None, view_count: int | None,
) -> dict[str, str]:
    '''The counts that are known, as the strings the queue stores.'''
    fields: dict[str, str] = {}
    if subscriber_count is not None:
        fields['subscriber_count'] = str(subscriber_count)
    if view_count is not None:
        fields['view_count'] = str(view_count)
    return fields


async def _stamp_checked(
    queue: RedisChannelScrapeQueue,
    entry: LowSubsEntry,
    now: float,
    subscriber_count: int | None,
    view_count: int | None,
) -> None:
    fields: dict[str, Any] = {'checked_at': int(now)}
    fields.update(_count_fields(subscriber_count, view_count))
    await queue.update_parked_record(
        entry.member, ChannelState.LOW_SUBS, fields,
    )


async def _recheck_one(
    queue: RedisChannelScrapeQueue,
    settings: YouTubeScraperSettings,
    browse: BrowseFn,
    entry: LowSubsEntry,
    summary: RecheckSummary,
    *,
    apply: bool,
    now: float,
) -> None:
    summary.checked += 1
    meta: dict[str, str] = await queue.get_meta(entry.member)
    stored_subs: int | None = effective_count(
        parse_count(meta.get('subscriber_count')), entry.subscriber_count,
    )
    stored_views: int | None = effective_count(
        parse_count(meta.get('view_count')), entry.view_count,
    )
    try:
        result: BrowseResult = await browse(entry.channel_id)
    except Exception as exc:
        summary.failed += 1
        _LOGGER.info(
            'low_subs recheck: browse failed',
            extra={'channel_id': entry.channel_id, 'error': str(exc)},
        )
        if apply:
            await _stamp_checked(
                queue, entry, now, stored_subs, stored_views,
            )
        return

    fresh_subs: int | None
    fresh_subs, _ = YouTubeChannel.parse_subscriber_count_with_outcome(
        result.data,
    )
    fresh_views: int | None = YouTubeChannel.parse_view_count(result.data)
    subs: int | None = effective_count(fresh_subs, stored_subs)
    views: int | None = effective_count(fresh_views, stored_views)
    action: RecheckAction = decide(subs, views, result.is_topic, settings)
    if action is RecheckAction.TOPIC:
        summary.topic += 1
    elif action is RecheckAction.REQUEUE:
        summary.requeued += 1
    else:
        summary.still_tiny += 1
    if not apply:
        return

    fresh: dict[str, str] = _count_fields(fresh_subs, fresh_views)
    if fresh:
        await queue.set_meta(entry.member, **fresh)
    if action is RecheckAction.STILL_TINY:
        await _stamp_checked(queue, entry, now, subs, views)
        return
    tier: int
    if action is RecheckAction.TOPIC:
        await queue.set_meta(entry.member, topic='1')
        tier = queue.topic_tier(subs)
    else:
        tier = queue.tier_for_sub_count(subs or 0)
    await queue.unmark(entry.member, score=now, tier=tier)
    await queue._redis.sadd(LOW_SUBS_RELEASED_KEY, entry.channel_id)


async def recheck_low_subs_channels(
    queue: RedisChannelScrapeQueue,
    settings: YouTubeScraperSettings,
    browse: BrowseFn,
    *,
    apply: bool,
    now: float,
    recheck_after_days: float,
    limit: int | None,
    concurrency: int,
) -> RecheckSummary:
    '''Re-check the channels parked in ``low_subs`` that are due, and
    in *apply* mode move those that are no longer tiny back into their
    tier. In dry-run nothing is written.'''
    entries: list[LowSubsEntry] = parse_low_subs_entries(
        await queue.iter_parked(ChannelState.LOW_SUBS),
    )
    due: list[LowSubsEntry]
    summary: RecheckSummary = RecheckSummary()
    due, summary.skipped_recent = select_due(
        entries, now=now, recheck_after_days=recheck_after_days,
        limit=limit,
    )
    semaphore: asyncio.Semaphore = asyncio.Semaphore(max(concurrency, 1))

    async def run(entry: LowSubsEntry) -> None:
        async with semaphore:
            await _recheck_one(
                queue, settings, browse, entry, summary,
                apply=apply, now=now,
            )

    await asyncio.gather(*(run(entry) for entry in due))
    return summary
