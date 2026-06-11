'''Auditor and safe repairer for channel/RSS queue drift.

This module implements Phase 1 of the design spec at
docs/superpowers/specs/2026-05-23-channel-rss-queue-drift-management-design.md.
It scans the channel scrape queue, channel terminal state
hashes, channel metadata, and the RSS creator inventory
and Phase 2's explicit safe repair modes. The auditor never
mutates Redis; the repairer only runs when the operator
passes a concrete ``--repair`` mode.

:maintainer: Boinko <boinko@scrape.exchange>
:copyright: Copyright 2026
:license: GPLv3
'''

from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from logging import Logger

import redis.asyncio as aioredis
from prometheus_client import Counter, Gauge, Histogram

from scrape_exchange.channel_scrape_queue import (
    ChannelState,
    RedisChannelScrapeQueue,
)


KEY_RSS_CREATORS: str = 'rss:youtube:creators'
KEY_RSS_SUPPRESSED: str = 'rss:youtube:suppressed'

RSS_SUPPRESSING_STATES: frozenset[ChannelState] = (
    frozenset({
        ChannelState.NOT_FOUND,
        ChannelState.INVALID_HANDLE,
        ChannelState.TERMINATED,
        ChannelState.REMOVED,
        ChannelState.HARD_UNAVAILABLE,
        ChannelState.TOPIC,
        ChannelState.NO_VIDEOS,
        ChannelState.LOW_SUBS,
    })
)

# 24-character YouTube channel IDs (case-sensitive; the
# canonical form is always upper-UC plus 22 base64url-ish
# chars).
_CHANNEL_ID_RE: re.Pattern[str] = re.compile(
    r'^UC[A-Za-z0-9_-]{22}$',
)

DEFAULT_RSS_QUEUE_TIERS: tuple[int, ...] = (1, 2, 3, 4, 5)
DEFAULT_REPAIR_SPREAD_SECONDS: int = 24 * 60 * 60
DEFAULT_META_ORPHAN_SPREAD_SECONDS: int = 6 * 60 * 60

REPAIR_MODES: frozenset[str] = frozenset({
    'rss-missing',
    'rss-seed',
    'rss-suppress',
    'meta-orphans',
    'meta-cache',
    'state-membership',
    'tier-placement',
    'unresolved-revive',
    'all-safe',
})

# A channel_id-keyed member lands in the terminal ``unresolved`` hash with
# this ``last_error`` when the channel scraper could not find a handle in
# creator_map. Those channels are scrapable by channel_id and should be
# revived (see the 2026-05-31 channel-id-scrape design).
_NO_HANDLE_LAST_ERROR_PREFIX: str = 'no handle in creator_map'


def _record_is_no_handle(record: str | None) -> bool:
    '''True when an ``unresolved`` state-hash record's ``last_error``
    marks the creator-map-miss cause that revival targets.'''
    if not record:
        return False
    try:
        data: object = json.loads(record)
    except (ValueError, TypeError):
        return False
    if not isinstance(data, dict):
        return False
    last_error: str = data.get('last_error') or ''
    return last_error.startswith(_NO_HANDLE_LAST_ERROR_PREFIX)


def _extract_channel_ids(
    members: Iterable[str],
) -> list[str]:
    '''Filter an iterable of queue member keys to those
    that carry a valid channel ID.

    Keeps only ``i:``-prefixed members whose suffix matches
    ``_CHANNEL_ID_RE``. Handle-keyed members (``h:`` prefix)
    have no RSS analogue and are discarded.
    '''
    result: list[str] = []
    for m in members:
        if not m.startswith('i:'):
            continue
        cid: str = m[2:]
        if _CHANNEL_ID_RE.match(cid):
            result.append(cid)
    return result


def _is_present(value: object) -> bool:
    '''Return True for Redis membership check results that
    represent presence.

    ``ZSCORE`` can validly return ``0.0`` for score-zero
    members, while ``HEXISTS`` returns ``False`` for absence.
    '''
    return value is not None and value is not False


def _stable_jitter_seconds(
    key: str,
    spread_seconds: int,
) -> int:
    '''Deterministic jitter in ``[0, spread_seconds)``.

    Used by repairs so repeated dry-ish repair runs do not
    reshuffle the whole queue. ``spread_seconds <= 0`` means
    "no jitter".
    '''
    if spread_seconds <= 0:
        return 0
    digest: bytes = hashlib.sha256(
        key.encode('utf-8'),
    ).digest()
    return int.from_bytes(digest[:8], 'big') % spread_seconds


@dataclass
class DriftSample:
    '''A single example of a drift class occurrence.

    Phase 1 only stores the member ID and optional
    context for human review; later phases may attach
    suggested repair actions.
    '''

    member: str
    context: dict[str, str] = field(default_factory=dict)


@dataclass
class DriftClassReport:
    '''Per-drift-class aggregate from a single scan.'''

    kind: str
    count: int = 0
    samples: list[DriftSample] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return {
            'kind': self.kind,
            'count': self.count,
            'samples': [s.member for s in self.samples],
            'sample_context': [
                s.context for s in self.samples
            ],
        }


@dataclass
class ReconcileReport:
    '''Top-level report returned by
    ``ChannelQueueAuditor.scan()``.

    ``inspected`` records cardinalities of the structures
    that were walked; ``drift`` maps each drift class name
    to its per-class report.
    '''

    inspected: dict[str, int] = field(default_factory=dict)
    drift: dict[str, DriftClassReport] = field(
        default_factory=dict,
    )
    dry_run: bool = True
    repaired: dict[str, int] = field(default_factory=dict)
    # Count of terminal ``unresolved`` i: members whose ``last_error`` is
    # the creator-map-miss cause that ``unresolved-revive`` would revive.
    # Read-only preview surfaced by a no-``--repair`` reconcile run.
    revivable_unresolved: int = 0

    def to_dict(self) -> dict[str, object]:
        return {
            'inspected': self.inspected,
            'drift': {
                kind: r.to_dict()
                for kind, r in self.drift.items()
            },
            'dry_run': self.dry_run,
            'repaired': self.repaired,
            'revivable_unresolved': self.revivable_unresolved,
        }


_LOGGER: Logger = logging.getLogger(__name__)

CHANNEL_QUEUE_DRIFT_SIZE: Gauge = Gauge(
    'channel_queue_drift_size',
    'Current channel/RSS queue drift count by class.',
    ['kind'],
)
CHANNEL_QUEUE_DRIFT_REPAIRED: Counter = Counter(
    'channel_queue_drift_repaired_total',
    'Channel/RSS queue drift repairs performed by mode.',
    ['kind', 'mode'],
)
RSS_CREATOR_SUPPRESSED: Counter = Counter(
    'rss_creator_suppressed_total',
    'RSS suppressions created by reconciliation or confirmation.',
    ['reason', 'source'],
)
CHANNEL_QUEUE_RECONCILE_LAST_SUCCESS: Gauge = Gauge(
    'channel_queue_reconcile_last_success_timestamp',
    'Unix timestamp of the last successful channel queue reconcile.',
)
CHANNEL_QUEUE_RECONCILE_DURATION: Histogram = Histogram(
    'channel_queue_reconcile_duration_seconds',
    'Runtime of a channel queue reconcile pass.',
    ['mode'],
)
CHANNEL_QUEUE_RECONCILE_SCAN_ERRORS: Counter = Counter(
    'channel_queue_reconcile_scan_errors_total',
    'Channel queue reconcile scan or repair failures.',
    ['phase'],
)


def publish_reconcile_metrics(
    report: ReconcileReport,
    *,
    mode: str,
    duration_seconds: float,
) -> None:
    '''Publish Prometheus metrics for a completed reconcile pass.

    The report carries aggregate repair counts by repair mode, not
    per drift class, so the repair counter uses the mode value for
    both labels. Drift gauges retain the exact stable class names
    used by JSON output.
    '''
    for kind, drift in report.drift.items():
        CHANNEL_QUEUE_DRIFT_SIZE.labels(kind=kind).set(
            drift.count,
        )
    for repair_mode, count in report.repaired.items():
        if count <= 0:
            continue
        CHANNEL_QUEUE_DRIFT_REPAIRED.labels(
            kind=repair_mode,
            mode=repair_mode,
        ).inc(count)
        if repair_mode == 'rss-suppress':
            RSS_CREATOR_SUPPRESSED.labels(
                reason='channel_terminal',
                source='reconcile',
            ).inc(count)
    CHANNEL_QUEUE_RECONCILE_LAST_SUCCESS.set(time.time())
    CHANNEL_QUEUE_RECONCILE_DURATION.labels(mode=mode).observe(
        duration_seconds,
    )


def record_reconcile_error(phase: str) -> None:
    '''Increment the reconcile error counter for *phase*.'''
    CHANNEL_QUEUE_RECONCILE_SCAN_ERRORS.labels(
        phase=phase,
    ).inc()


class ChannelQueueAuditor:
    '''Read-only auditor for channel/RSS queue drift.

    Consumes:

    * ``RedisChannelScrapeQueue`` — encapsulates the
      channel-side keys + helpers (``_k_scheduled``,
      ``_k_state``, ``_k_meta``, ``_k_unresolved``).
    * ``redis.asyncio.Redis`` — used directly for
      RSS-side keys that the queue object does not own
      (``rss:youtube:creators``,
      ``rss:youtube:suppressed``).

    Never mutates Redis. All repair modes belong in a
    later phase.
    '''

    def __init__(
        self,
        queue: RedisChannelScrapeQueue,
        redis_client: aioredis.Redis,
        *,
        batch_size: int = 1000,
        sample_size: int = 5,
        limit: int | None = None,
    ) -> None:
        self._queue: RedisChannelScrapeQueue = queue
        self._redis: aioredis.Redis = redis_client
        self._batch_size: int = batch_size
        self._sample_size: int = sample_size
        self._limit: int | None = limit

    async def scan(self) -> ReconcileReport:
        '''Run all Phase 1 scans and return a single
        consolidated report.'''
        report: ReconcileReport = ReconcileReport(
            dry_run=True,
        )
        await self._populate_inspected(report)
        rss_missing: DriftClassReport = (
            await self._scan_rss_missing()
        )
        report.drift[rss_missing.kind] = rss_missing
        state_missing: DriftClassReport
        unseedable: DriftClassReport
        state_missing, unseedable = (
            await self._scan_state_missing_rss()
        )
        report.drift[state_missing.kind] = state_missing
        report.drift[unseedable.kind] = unseedable
        rss_suppress: DriftClassReport = (
            await self._scan_rss_suppression()
        )
        report.drift[rss_suppress.kind] = rss_suppress
        rss_suppressed_active: DriftClassReport = (
            await self._scan_rss_suppressed_active()
        )
        report.drift[rss_suppressed_active.kind] = (
            rss_suppressed_active
        )
        sched_orphans: DriftClassReport
        pending_orphans: DriftClassReport
        terminal_orphans: DriftClassReport
        sched_orphans, pending_orphans, terminal_orphans = (
            await self._scan_meta_orphans()
        )
        report.drift[sched_orphans.kind] = sched_orphans
        report.drift[pending_orphans.kind] = (
            pending_orphans
        )
        report.drift[terminal_orphans.kind] = (
            terminal_orphans
        )
        for drift in await self._scan_meta_cache():
            report.drift[drift.kind] = drift
        tier_mismatch: DriftClassReport = (
            await self._scan_tier_placement()
        )
        report.drift[tier_mismatch.kind] = tier_mismatch
        identity_missing: DriftClassReport = (
            await self._scan_identity_map_missing()
        )
        report.drift[identity_missing.kind] = (
            identity_missing
        )
        multi_state: DriftClassReport = (
            await self._scan_multiple_states()
        )
        report.drift[multi_state.kind] = multi_state
        report.revivable_unresolved = (
            await self._count_revivable_unresolved()
        )
        return report

    async def _count_revivable_unresolved(self) -> int:
        '''Count terminal ``unresolved`` i: members that the
        ``unresolved-revive`` repair would re-schedule (read-only).'''
        state_key: str = self._queue._k_state(ChannelState.UNRESOLVED)
        cursor: int = 0
        count: int = 0
        while True:
            cursor, items = await self._redis.hscan(
                state_key, cursor=cursor, count=self._batch_size,
            )
            for member, record in items.items():
                if member.startswith('i:') and _record_is_no_handle(
                    record,
                ):
                    count += 1
            if cursor == 0:
                break
        return count

    async def _scan_state_missing_rss(
        self,
    ) -> tuple[DriftClassReport, DriftClassReport]:
        '''Walk scheduled tiers + terminal hashes. For
        each channel-ID member, check RSS accounting.

        Returns two reports — the primary drift class and
        the "unseedable" sub-class for members missing a
        label.
        '''
        main: DriftClassReport = DriftClassReport(
            kind='channel_state_missing_rss_accounting',
        )
        sub: DriftClassReport = DriftClassReport(
            kind='channel_state_missing_rss_unseedable',
        )
        scheduled_keys: list[str] = [
            self._queue._k_scheduled(t)
            for t in range(len(self._queue._tiers))
        ]
        # Channel-ID members live in scheduled and
        # channel-ID terminal hashes. Handle-keyed
        # structures (unresolved + identity-failure
        # terminals) are out of scope for the RSS
        # accounting scan because
        # ``rss:youtube:creators`` is keyed by
        # channel_id.
        terminal_keys: list[str] = [
            self._queue._k_state(s)
            for s in sorted(
                ChannelState.terminal_states(),
                key=lambda s: s.value,
            )
        ]
        seen: set[str] = set()
        inspected: int = 0
        for key in scheduled_keys:
            inspected = await self._zscan_rss_members(
                key, seen, main, sub, inspected,
            )
            if (
                self._limit is not None
                and inspected >= self._limit
            ):
                break
        for key in terminal_keys:
            inspected = await self._hscan_rss_members(
                key, seen, main, sub, inspected,
            )
            if (
                self._limit is not None
                and inspected >= self._limit
            ):
                break
        return main, sub

    async def _zscan_rss_members(
        self,
        key: str,
        seen: set[str],
        main: DriftClassReport,
        sub: DriftClassReport,
        inspected: int,
    ) -> int:
        '''ZSCAN one sorted-set key and emit each member
        into the RSS-accounting drift reports.

        Returns the updated ``inspected`` count.
        '''
        cursor: int = 0
        while True:
            cursor, items = await self._redis.zscan(
                key,
                cursor=cursor,
                count=self._batch_size,
            )
            for member, _score in items:
                await self._emit_rss_accounting(
                    member, seen, main, sub,
                )
                inspected += 1
                if (
                    self._limit is not None
                    and inspected >= self._limit
                ):
                    return inspected
            if cursor == 0:
                break
        return inspected

    async def _hscan_rss_members(
        self,
        key: str,
        seen: set[str],
        main: DriftClassReport,
        sub: DriftClassReport,
        inspected: int,
    ) -> int:
        '''HSCAN one hash key and emit each member into
        the RSS-accounting drift reports.

        Returns the updated ``inspected`` count.
        '''
        cursor: int = 0
        while True:
            cursor, items_map = (
                await self._redis.hscan(
                    key,
                    cursor=cursor,
                    count=self._batch_size,
                )
            )
            for member in items_map.keys():
                await self._emit_rss_accounting(
                    member, seen, main, sub,
                )
                inspected += 1
                if (
                    self._limit is not None
                    and inspected >= self._limit
                ):
                    return inspected
            if cursor == 0:
                break
        return inspected

    async def _emit_rss_accounting(
        self,
        member: str,
        seen: set[str],
        main: DriftClassReport,
        sub: DriftClassReport,
    ) -> None:
        '''Check a single channel-ID-keyed queue member
        against RSS accounting and record any drift.

        Skips members already in ``seen``, handle-keyed
        members, and malformed channel IDs. Mutates
        ``main`` and ``sub`` in place.
        '''
        if member in seen:
            return
        seen.add(member)
        if not member.startswith('i:'):
            return
        channel_id: str = member[2:]
        if not _CHANNEL_ID_RE.match(channel_id):
            return
        pipe: aioredis.client.Pipeline = (
            self._redis.pipeline(transaction=False)
        )
        pipe.hexists(KEY_RSS_CREATORS, channel_id)
        pipe.hexists(KEY_RSS_SUPPRESSED, channel_id)
        pipe.hget('youtube:creator_map', channel_id)
        pipe.hget(
            self._queue._k_meta(member), 'handle',
        )
        pipe.hget(
            self._queue._k_meta(member), 'name',
        )
        (
            in_creators,
            in_suppressed,
            map_handle,
            meta_handle,
            meta_name,
        ) = await pipe.execute()
        if in_creators or in_suppressed:
            return
        main.count += 1
        if len(main.samples) < self._sample_size:
            main.samples.append(DriftSample(member=member))
        label: str | None = (
            map_handle or meta_handle or meta_name
        )
        if not label:
            sub.count += 1
            if len(sub.samples) < self._sample_size:
                sub.samples.append(
                    DriftSample(member=member),
                )

    async def _scan_rss_missing(
        self,
    ) -> DriftClassReport:
        '''Scan ``rss:youtube:creators``. Report any
        channel_id that is absent from every channel-side
        active or terminal structure.
        '''
        report: DriftClassReport = DriftClassReport(
            kind='rss_creator_missing_channel_state',
        )
        scheduled_keys: list[str] = [
            self._queue._k_scheduled(t)
            for t in range(len(self._queue._tiers))
        ]
        terminal_keys: list[str] = [
            self._queue._k_state(s)
            for s in sorted(
                ChannelState.terminal_states(),
                key=lambda s: s.value,
            )
        ]
        unresolved_key: str = (
            self._queue._k_unresolved()
        )
        batch: list[str] = []
        inspected: int = 0

        async def flush() -> None:
            nonlocal inspected
            if not batch:
                return
            pipe: aioredis.client.Pipeline = (
                self._redis.pipeline(transaction=False)
            )
            for cid in batch:
                member: str = f'i:{cid}'
                for key in scheduled_keys:
                    pipe.zscore(key, member)
                for key in terminal_keys:
                    pipe.hexists(key, member)
                pipe.zscore(unresolved_key, member)
            results: list[object] = await pipe.execute()
            per_member: int = (
                len(scheduled_keys)
                + len(terminal_keys)
                + 1  # +1 for unresolved queue zscore
            )
            for i, cid in enumerate(batch):
                group: list[object] = results[
                    i * per_member:(i + 1) * per_member
                ]
                # Use 'is not' identity checks: ZSCORE
                # returns 0.0 (not None) for score-zero
                # members, and 0.0 == False under ==, so
                # any equality-based form ('v not in
                # (None, False)' or plain 'if v') would
                # wrongly treat score-0 as absent.
                present: bool = any(
                    v is not None and v is not False
                    for v in group
                )
                inspected += 1
                if not present:
                    report.count += 1
                    if len(report.samples) < (
                        self._sample_size
                    ):
                        report.samples.append(
                            DriftSample(
                                member=f'i:{cid}',
                            ),
                        )
            batch.clear()

        cursor: int = 0
        while True:
            cursor, items = await self._redis.hscan(
                KEY_RSS_CREATORS,
                cursor=cursor,
                count=self._batch_size,
            )
            for cid, _label in items.items():
                if not _CHANNEL_ID_RE.match(cid):
                    continue
                batch.append(cid)
                if self._limit is not None and (
                    inspected + len(batch)
                    >= self._limit
                ):
                    break
                if len(batch) >= self._batch_size:
                    await flush()
            await flush()
            if cursor == 0 or (
                self._limit is not None
                and inspected >= self._limit
            ):
                break
        return report

    async def _scan_rss_suppression(
        self,
    ) -> DriftClassReport:
        '''Walk channel terminal states that should suppress
        RSS. Report any member that is missing from
        ``rss:youtube:suppressed``.

        Only ``i:``-prefixed (channel-ID-keyed) members are
        checked; handle-keyed members have no RSS analogue
        and are skipped.
        '''
        report: DriftClassReport = DriftClassReport(
            kind=(
                'channel_terminal_missing_rss_suppression'
            ),
        )
        inspected: int = 0
        for state in sorted(
            RSS_SUPPRESSING_STATES,
            key=lambda s: s.value,
        ):
            inspected = (
                await self._hscan_suppression_state(
                    state, report, inspected,
                )
            )
            if (
                self._limit is not None
                and inspected >= self._limit
            ):
                break
        return report

    async def _hscan_suppression_state(
        self,
        state: ChannelState,
        report: DriftClassReport,
        inspected: int,
    ) -> int:
        '''HSCAN one terminal-state hash and check each
        channel-ID member against ``rss:youtube:suppressed``.

        Returns the updated ``inspected`` count.
        '''
        key: str = self._queue._k_state(state)
        cursor: int = 0
        while True:
            cursor, items = await self._redis.hscan(
                key,
                cursor=cursor,
                count=self._batch_size,
            )
            channel_ids: list[str] = (
                _extract_channel_ids(items.keys())
            )
            inspected = (
                await self._check_suppression_batch(
                    channel_ids, state, report, inspected,
                )
            )
            if cursor == 0 or (
                self._limit is not None
                and inspected >= self._limit
            ):
                break
        return inspected

    async def _check_suppression_batch(
        self,
        channel_ids: list[str],
        state: ChannelState,
        report: DriftClassReport,
        inspected: int,
    ) -> int:
        '''Pipeline-check a batch of channel IDs against
        ``rss:youtube:suppressed`` and record any drift.

        Returns the updated ``inspected`` count.
        '''
        if not channel_ids:
            return inspected
        pipe: aioredis.client.Pipeline = (
            self._redis.pipeline(transaction=False)
        )
        for cid in channel_ids:
            pipe.hexists(KEY_RSS_SUPPRESSED, cid)
        results: list[bool] = await pipe.execute()
        for cid, exists in zip(channel_ids, results):
            inspected += 1
            if not exists:
                report.count += 1
                if (
                    len(report.samples)
                    < self._sample_size
                ):
                    report.samples.append(
                        DriftSample(
                            member=f'i:{cid}',
                            context={
                                'state': state.value,
                            },
                        ),
                    )
            if (
                self._limit is not None
                and inspected >= self._limit
            ):
                break
        return inspected

    async def _scan_meta_orphans(
        self,
    ) -> tuple[
        DriftClassReport,
        DriftClassReport,
        DriftClassReport,
    ]:
        '''Walk ``youtube:channel:meta:*``. Report meta
        records whose declared ``state`` is ``scheduled``
        or ``pending_resolution`` but whose member is
        absent from the corresponding active queue.

        Other meta/state mismatches (terminal, etc.) are
        Phase 2 scope.
        '''
        scheduled_missing: DriftClassReport = (
            DriftClassReport(
                kind='meta_scheduled_missing_zset',
            )
        )
        pending_missing: DriftClassReport = (
            DriftClassReport(
                kind='meta_pending_missing_unresolved',
            )
        )
        terminal_missing: DriftClassReport = (
            DriftClassReport(
                kind='meta_terminal_missing_hash',
            )
        )
        scheduled_keys: list[str] = [
            self._queue._k_scheduled(t)
            for t in range(len(self._queue._tiers))
        ]
        terminal_states: set[str] = {
            state.value
            for state in ChannelState.terminal_states()
        }
        unresolved_key: str = (
            self._queue._k_unresolved()
        )
        prefix: str = self._queue._k_meta('')
        inspected: int = 0
        async for key in self._redis.scan_iter(
            match=f'{prefix}*',
            count=self._batch_size,
        ):
            member: str = key[len(prefix):]
            state_value: str | None = (
                await self._redis.hget(key, 'state')
            )
            if state_value == (
                ChannelState.SCHEDULED.value
            ) and member.startswith('i:'):
                await self._check_scheduled_member(
                    member,
                    scheduled_keys,
                    scheduled_missing,
                )
            elif state_value == (
                ChannelState.PENDING_RESOLUTION.value
            ) and member.startswith('h:'):
                await self._check_pending_member(
                    member,
                    unresolved_key,
                    pending_missing,
                )
            elif state_value in terminal_states:
                state = ChannelState(state_value)
                exists: bool = await self._redis.hexists(
                    self._queue._k_state(state), member,
                )
                if not exists:
                    terminal_missing.count += 1
                    if (
                        len(terminal_missing.samples)
                        < self._sample_size
                    ):
                        terminal_missing.samples.append(
                            DriftSample(
                                member=member,
                                context={'state': state.value},
                            ),
                        )
            inspected += 1
            if (
                self._limit is not None
                and inspected >= self._limit
            ):
                break
        return scheduled_missing, pending_missing, terminal_missing

    async def _scan_rss_suppressed_active(
        self,
    ) -> DriftClassReport:
        '''Report RSS-suppressed channels that still have
        active channel scrape work.

        This is informational: RSS feed suppression does not
        necessarily mean channel scraping should be terminalized.
        '''
        report: DriftClassReport = DriftClassReport(
            kind='rss_suppressed_channel_active',
        )
        scheduled_keys: list[str] = [
            self._queue._k_scheduled(t)
            for t in range(len(self._queue._tiers))
        ]
        unresolved_key: str = self._queue._k_unresolved()
        inspected: int = 0
        cursor: int = 0
        while True:
            cursor, items = await self._redis.hscan(
                KEY_RSS_SUPPRESSED,
                cursor=cursor,
                count=self._batch_size,
            )
            pipe: aioredis.client.Pipeline = (
                self._redis.pipeline(transaction=False)
            )
            channel_ids: list[str] = []
            for channel_id in items.keys():
                if not _CHANNEL_ID_RE.match(channel_id):
                    continue
                channel_ids.append(channel_id)
                member: str = f'i:{channel_id}'
                for key in scheduled_keys:
                    pipe.zscore(key, member)
                pipe.zscore(unresolved_key, member)
            if channel_ids:
                results: list[object] = await pipe.execute()
                per_member: int = len(scheduled_keys) + 1
                for i, channel_id in enumerate(channel_ids):
                    group: list[object] = results[
                        i * per_member:(i + 1) * per_member
                    ]
                    inspected += 1
                    if any(_is_present(v) for v in group):
                        report.count += 1
                        if (
                            len(report.samples)
                            < self._sample_size
                        ):
                            report.samples.append(
                                DriftSample(
                                    member=f'i:{channel_id}',
                                ),
                            )
                    if (
                        self._limit is not None
                        and inspected >= self._limit
                    ):
                        return report
            if cursor == 0:
                break
        return report

    async def _scan_meta_cache(
        self,
    ) -> tuple[
        DriftClassReport,
        DriftClassReport,
        DriftClassReport,
    ]:
        '''Report queue/terminal members whose cached meta is
        missing or disagrees with terminal authority.
        '''
        zset_missing: DriftClassReport = DriftClassReport(
            kind='zset_missing_meta',
        )
        terminal_missing: DriftClassReport = DriftClassReport(
            kind='terminal_hash_missing_meta',
        )
        terminal_disagrees: DriftClassReport = DriftClassReport(
            kind='terminal_hash_meta_disagrees',
        )
        inspected: int = 0
        for tier in range(len(self._queue._tiers)):
            inspected = await self._scan_zset_missing_meta_key(
                self._queue._k_scheduled(tier),
                ChannelState.SCHEDULED,
                zset_missing,
                inspected,
            )
            if self._limit is not None and inspected >= self._limit:
                return (
                    zset_missing,
                    terminal_missing,
                    terminal_disagrees,
                )
        inspected = await self._scan_zset_missing_meta_key(
            self._queue._k_unresolved(),
            ChannelState.PENDING_RESOLUTION,
            zset_missing,
            inspected,
        )
        if self._limit is not None and inspected >= self._limit:
            return zset_missing, terminal_missing, terminal_disagrees
        for state in sorted(
            ChannelState.terminal_states(),
            key=lambda item: item.value,
        ):
            inspected = await self._scan_terminal_meta_key(
                state,
                terminal_missing,
                terminal_disagrees,
                inspected,
            )
            if self._limit is not None and inspected >= self._limit:
                break
        return zset_missing, terminal_missing, terminal_disagrees

    async def _scan_zset_missing_meta_key(
        self,
        key: str,
        state: ChannelState,
        report: DriftClassReport,
        inspected: int,
    ) -> int:
        cursor: int = 0
        while True:
            cursor, items = await self._redis.zscan(
                key,
                cursor=cursor,
                count=self._batch_size,
            )
            for member, _score in items:
                inspected += 1
                exists: bool = await self._redis.exists(
                    self._queue._k_meta(member),
                )
                if not exists:
                    report.count += 1
                    if len(report.samples) < self._sample_size:
                        report.samples.append(
                            DriftSample(
                                member=member,
                                context={'state': state.value},
                            ),
                        )
                if (
                    self._limit is not None
                    and inspected >= self._limit
                ):
                    return inspected
            if cursor == 0:
                break
        return inspected

    async def _scan_terminal_meta_key(
        self,
        state: ChannelState,
        missing: DriftClassReport,
        disagrees: DriftClassReport,
        inspected: int,
    ) -> int:
        cursor: int = 0
        key: str = self._queue._k_state(state)
        while True:
            cursor, items = await self._redis.hscan(
                key,
                cursor=cursor,
                count=self._batch_size,
            )
            for member in items.keys():
                inspected += 1
                meta_key: str = self._queue._k_meta(member)
                meta: dict[str, str] = await self._redis.hgetall(
                    meta_key,
                )
                if not meta:
                    missing.count += 1
                    if len(missing.samples) < self._sample_size:
                        missing.samples.append(
                            DriftSample(
                                member=member,
                                context={'state': state.value},
                            ),
                        )
                elif meta.get('state') != state.value:
                    disagrees.count += 1
                    if len(disagrees.samples) < self._sample_size:
                        disagrees.samples.append(
                            DriftSample(
                                member=member,
                                context={
                                    'terminal_state': state.value,
                                    'meta_state': (
                                        meta.get('state') or ''
                                    ),
                                },
                            ),
                        )
                if (
                    self._limit is not None
                    and inspected >= self._limit
                ):
                    return inspected
            if cursor == 0:
                break
        return inspected

    async def _scan_tier_placement(
        self,
    ) -> DriftClassReport:
        report: DriftClassReport = DriftClassReport(
            kind='scheduled_tier_mismatch',
        )
        inspected: int = 0
        for current_tier in range(len(self._queue._tiers)):
            cursor: int = 0
            key: str = self._queue._k_scheduled(current_tier)
            while True:
                cursor, items = await self._redis.zscan(
                    key,
                    cursor=cursor,
                    count=self._batch_size,
                )
                for member, _score in items:
                    inspected += 1
                    if not member.startswith('i:'):
                        continue
                    raw: str | None = await self._redis.hget(
                        self._queue._k_tiers(), member[2:],
                    )
                    if raw is None:
                        continue
                    try:
                        desired_tier: int = int(raw)
                    except ValueError:
                        continue
                    if (
                        0 <= desired_tier < len(self._queue._tiers)
                        and desired_tier != current_tier
                    ):
                        report.count += 1
                        if len(report.samples) < self._sample_size:
                            report.samples.append(
                                DriftSample(
                                    member=member,
                                    context={
                                        'current_tier': str(
                                            current_tier,
                                        ),
                                        'desired_tier': str(
                                            desired_tier,
                                        ),
                                    },
                                ),
                            )
                    if (
                        self._limit is not None
                        and inspected >= self._limit
                    ):
                        return report
                if cursor == 0:
                    break
        return report

    async def _scan_identity_map_missing(
        self,
    ) -> DriftClassReport:
        report: DriftClassReport = DriftClassReport(
            kind='identity_map_missing_for_channel_state',
        )
        seen: set[str] = set()
        inspected: int = 0
        async for member in self._iter_channel_id_authority_members():
            if member in seen:
                continue
            seen.add(member)
            inspected += 1
            exists: bool = await self._redis.hexists(
                'youtube:creator_map', member[2:],
            )
            if not exists:
                report.count += 1
                if len(report.samples) < self._sample_size:
                    report.samples.append(
                        DriftSample(member=member),
                    )
            if (
                self._limit is not None
                and inspected >= self._limit
            ):
                break
        return report

    async def _scan_multiple_states(
        self,
    ) -> DriftClassReport:
        report: DriftClassReport = DriftClassReport(
            kind='member_in_multiple_states',
        )
        states_by_member: dict[str, list[str]] = {}
        inspected: int = 0

        async def add(member: str, state: str) -> None:
            states_by_member.setdefault(member, []).append(state)

        for tier in range(len(self._queue._tiers)):
            cursor: int = 0
            key: str = self._queue._k_scheduled(tier)
            while True:
                cursor, items = await self._redis.zscan(
                    key,
                    cursor=cursor,
                    count=self._batch_size,
                )
                for member, _score in items:
                    inspected += 1
                    await add(member, f'scheduled:{tier}')
                    if (
                        self._limit is not None
                        and inspected >= self._limit
                    ):
                        break
                if cursor == 0 or (
                    self._limit is not None
                    and inspected >= self._limit
                ):
                    break
        cursor = 0
        while True:
            cursor, items = await self._redis.zscan(
                self._queue._k_unresolved(),
                cursor=cursor,
                count=self._batch_size,
            )
            for member, _score in items:
                inspected += 1
                await add(member, 'pending_resolution')
                if (
                    self._limit is not None
                    and inspected >= self._limit
                ):
                    break
            if cursor == 0 or (
                self._limit is not None
                and inspected >= self._limit
            ):
                break
        for state in ChannelState.terminal_states():
            cursor = 0
            while True:
                cursor, items = await self._redis.hscan(
                    self._queue._k_state(state),
                    cursor=cursor,
                    count=self._batch_size,
                )
                for member in items.keys():
                    inspected += 1
                    await add(member, state.value)
                    if (
                        self._limit is not None
                        and inspected >= self._limit
                    ):
                        break
                if cursor == 0 or (
                    self._limit is not None
                    and inspected >= self._limit
                ):
                    break
        for member, states in states_by_member.items():
            if len(states) <= 1:
                continue
            report.count += 1
            if len(report.samples) < self._sample_size:
                report.samples.append(
                    DriftSample(
                        member=member,
                        context={'states': ','.join(states)},
                    ),
                )
        return report

    async def _iter_channel_id_authority_members(self):
        for tier in range(len(self._queue._tiers)):
            cursor: int = 0
            while True:
                cursor, items = await self._redis.zscan(
                    self._queue._k_scheduled(tier),
                    cursor=cursor,
                    count=self._batch_size,
                )
                for member, _score in items:
                    if member.startswith('i:') and _CHANNEL_ID_RE.match(
                        member[2:],
                    ):
                        yield member
                if cursor == 0:
                    break
        for state in ChannelState.terminal_states():
            cursor = 0
            while True:
                cursor, items = await self._redis.hscan(
                    self._queue._k_state(state),
                    cursor=cursor,
                    count=self._batch_size,
                )
                for member in items.keys():
                    if member.startswith('i:') and _CHANNEL_ID_RE.match(
                        member[2:],
                    ):
                        yield member
                if cursor == 0:
                    break

    async def _check_scheduled_member(
        self,
        member: str,
        scheduled_keys: list[str],
        report: DriftClassReport,
    ) -> None:
        '''Pipeline ZSCORE over all scheduled tiers for
        ``member``. Record drift if absent from all tiers.

        Note: ZSCORE returns 0.0 (not None) for a
        score-zero member — use ``v is not None`` to
        distinguish presence from absence.
        '''
        pipe: aioredis.client.Pipeline = (
            self._redis.pipeline(transaction=False)
        )
        for sk in scheduled_keys:
            pipe.zscore(sk, member)
        scores: list[object] = await pipe.execute()
        # 0.0 is a valid score; None means absent.
        present: bool = any(
            v is not None for v in scores
        )
        if not present:
            report.count += 1
            if len(report.samples) < self._sample_size:
                report.samples.append(
                    DriftSample(member=member),
                )

    async def _check_pending_member(
        self,
        member: str,
        unresolved_key: str,
        report: DriftClassReport,
    ) -> None:
        '''ZSCORE the unresolved queue for ``member``.
        Record drift if absent.

        Note: ZSCORE returns 0.0 (not None) for a
        score-zero member — use ``v is not None`` to
        distinguish presence from absence.
        '''
        score: object = await self._redis.zscore(
            unresolved_key, member,
        )
        # 0.0 is a valid score; None means absent.
        if score is None:
            report.count += 1
            if len(report.samples) < self._sample_size:
                report.samples.append(
                    DriftSample(member=member),
                )

    async def _populate_inspected(
        self, report: ReconcileReport,
    ) -> None:
        '''Populate ``report.inspected`` with key
        cardinalities from both channel and RSS sides.'''
        pipe: aioredis.client.Pipeline = (
            self._redis.pipeline(transaction=False)
        )
        pipe.hlen(KEY_RSS_CREATORS)
        pipe.hlen(KEY_RSS_SUPPRESSED)
        pipe.zcard(self._queue._k_unresolved())
        for t in range(len(self._queue._tiers)):
            pipe.zcard(self._queue._k_scheduled(t))
        results: list[int] = await pipe.execute()
        report.inspected['rss_creators'] = results[0]
        report.inspected['rss_suppressed'] = results[1]
        report.inspected['unresolved_members'] = (
            results[2]
        )
        report.inspected['scheduled_members'] = sum(
            results[3:],
        )
        # Approximate channel_meta count via SCAN with
        # MATCH; HLEN is not available on a key glob.
        meta_count: int = 0
        async for _ in self._redis.scan_iter(
            match=f'{self._queue._k_meta("")}*',
            count=self._batch_size,
        ):
            meta_count += 1
        report.inspected['channel_meta'] = meta_count


@dataclass
class RepairOptions:
    '''Operator-selected options for Phase 2 repair runs.'''

    mode: str
    source: str = 'reconcile'
    due_now: bool = False
    max_repairs: int | None = None
    default_channel_tier: int | None = None
    default_rss_tier: int | None = None
    rss_queue_tiers: tuple[int, ...] = DEFAULT_RSS_QUEUE_TIERS
    spread_window_seconds: int = DEFAULT_REPAIR_SPREAD_SECONDS


class ChannelQueueRepairer:
    '''Safe Phase 2 repair actions for channel/RSS drift.

    Repairs are intentionally conservative: terminal channel
    state wins, existing channel tiers are preserved, RSS queue
    priorities are never copied into channel queue priorities,
    and unseedable RSS accounting is left for the report.
    '''

    def __init__(
        self,
        queue: RedisChannelScrapeQueue,
        redis_client: aioredis.Redis,
        *,
        batch_size: int = 1000,
    ) -> None:
        self._queue: RedisChannelScrapeQueue = queue
        self._redis: aioredis.Redis = redis_client
        self._batch_size: int = batch_size

    async def repair(
        self,
        options: RepairOptions,
    ) -> dict[str, int]:
        if options.mode not in REPAIR_MODES:
            raise ValueError(
                f'unknown repair mode: {options.mode!r}'
            )
        counts: dict[str, int] = {}
        modes: tuple[str, ...]
        if options.mode == 'all-safe':
            modes = (
                'rss-missing',
                'rss-seed',
                'rss-suppress',
                'meta-orphans',
                'meta-cache',
                'state-membership',
                'tier-placement',
                'unresolved-revive',
            )
        else:
            modes = (options.mode,)
        for mode in modes:
            if self._limit_reached(counts, options):
                break
            if mode == 'rss-missing':
                counts[mode] = await self._repair_rss_missing(
                    options, counts,
                )
            elif mode == 'rss-seed':
                counts[mode] = await self._repair_rss_seed(
                    options, counts,
                )
            elif mode == 'rss-suppress':
                counts[mode] = await self._repair_rss_suppress(
                    options, counts,
                )
            elif mode == 'meta-orphans':
                counts[mode] = await self._repair_meta_orphans(
                    options, counts,
                )
            elif mode == 'meta-cache':
                counts[mode] = await self._repair_meta_cache(
                    options, counts,
                )
            elif mode == 'state-membership':
                counts[mode] = await self._repair_state_membership(
                    options, counts,
                )
            elif mode == 'tier-placement':
                counts[mode] = await self._repair_tier_placement(
                    options, counts,
                )
            elif mode == 'unresolved-revive':
                counts[mode] = await self._repair_unresolved_revive(
                    options, counts,
                )
        return counts

    async def _repair_unresolved_revive(
        self,
        options: RepairOptions,
        counts: dict[str, int],
    ) -> int:
        '''Re-schedule channel_id members stuck in terminal
        ``unresolved`` solely because creator_map lacked their handle.

        These channels are scrapable by channel_id, so they are
        ``unmark``-ed back to ``scheduled`` at a spread due-time (via
        ``_score_for_member``) to avoid a thundering herd.
        '''
        repaired: int = 0
        state_key: str = self._queue._k_state(ChannelState.UNRESOLVED)
        cursor: int = 0
        while True:
            cursor, items = await self._redis.hscan(
                state_key, cursor=cursor, count=self._batch_size,
            )
            for member, record in items.items():
                if not member.startswith('i:'):
                    continue
                if not _record_is_no_handle(record):
                    continue
                score: float = self._score_for_member(
                    member, options=options,
                )
                await self._queue.unmark(member, score=score)
                repaired += 1
                if self._budget_done(repaired, counts, options):
                    return repaired
            if cursor == 0:
                break
        return repaired

    @staticmethod
    def _limit_reached(
        counts: dict[str, int],
        options: RepairOptions,
    ) -> bool:
        if options.max_repairs is None:
            return False
        return sum(counts.values()) >= options.max_repairs

    def _remaining_budget(
        self,
        counts: dict[str, int],
        options: RepairOptions,
    ) -> int | None:
        if options.max_repairs is None:
            return None
        return max(0, options.max_repairs - sum(counts.values()))

    def _default_channel_tier(
        self, options: RepairOptions,
    ) -> int:
        if options.default_channel_tier is not None:
            return max(
                0,
                min(
                    options.default_channel_tier,
                    len(self._queue._tiers) - 1,
                ),
            )
        # Second-to-last tier by default. The catch-all
        # lowest tier is deliberately reserved for channels
        # we care about least.
        return max(0, len(self._queue._tiers) - 2)

    def _default_rss_tier(
        self, options: RepairOptions,
    ) -> int:
        if options.default_rss_tier is not None:
            return options.default_rss_tier
        if len(options.rss_queue_tiers) >= 2:
            return options.rss_queue_tiers[-2]
        return options.rss_queue_tiers[-1]

    async def _channel_present(self, member: str) -> bool:
        pipe: aioredis.client.Pipeline = (
            self._redis.pipeline(transaction=False)
        )
        pipe.zscore(self._queue._k_unresolved(), member)
        for tier in range(len(self._queue._tiers)):
            pipe.zscore(self._queue._k_scheduled(tier), member)
        for state in ChannelState.terminal_states():
            pipe.hexists(self._queue._k_state(state), member)
        results: list[object] = await pipe.execute()
        return any(_is_present(v) for v in results)

    async def _terminal_state(
        self, member: str,
    ) -> ChannelState | None:
        pipe: aioredis.client.Pipeline = (
            self._redis.pipeline(transaction=False)
        )
        states: list[ChannelState] = sorted(
            ChannelState.terminal_states(),
            key=lambda state: state.value,
        )
        for state in states:
            pipe.hexists(self._queue._k_state(state), member)
        results: list[bool] = await pipe.execute()
        for state, present in zip(states, results):
            if present:
                return state
        return None

    def _score_for_member(
        self,
        member: str,
        *,
        options: RepairOptions,
        spread_seconds: int | None = None,
    ) -> float:
        if options.due_now:
            return 0.0
        spread: int = (
            options.spread_window_seconds
            if spread_seconds is None
            else spread_seconds
        )
        return float(
            int(time.time())
            + _stable_jitter_seconds(member, spread)
        )

    async def _tier_for_channel(
        self,
        channel_id: str,
        options: RepairOptions,
    ) -> int:
        raw: str | None = await self._redis.hget(
            self._queue._k_tiers(), channel_id,
        )
        if raw is not None:
            try:
                tier = int(raw)
            except ValueError:
                tier = self._default_channel_tier(options)
            return max(0, min(tier, len(self._queue._tiers) - 1))
        tier = self._default_channel_tier(options)
        await self._redis.hsetnx(
            self._queue._k_tiers(),
            channel_id,
            str(tier),
        )
        return tier

    async def _schedule_channel(
        self,
        channel_id: str,
        *,
        reason: str,
        options: RepairOptions,
    ) -> bool:
        member: str = f'i:{channel_id}'
        if await self._channel_present(member):
            return False
        tier: int = await self._tier_for_channel(
            channel_id, options,
        )
        score: float = self._score_for_member(
            member, options=options,
        )
        now: str = str(int(time.time()))
        pipe: aioredis.client.Pipeline = (
            self._redis.pipeline(transaction=True)
        )
        pipe.zadd(
            self._queue._k_scheduled(tier),
            {member: score},
            nx=True,
        )
        pipe.hset(
            self._queue._k_meta(member),
            mapping={
                'channel_id': channel_id,
                'state': ChannelState.SCHEDULED.value,
                'source': options.source,
                'reconciled_at': now,
                'reconcile_reason': reason,
            },
        )
        pipe.hsetnx(
            self._queue._k_meta(member),
            'created_at',
            now,
        )
        results: list[object] = await pipe.execute()
        return int(results[0] or 0) == 1

    async def _repair_rss_missing(
        self,
        options: RepairOptions,
        counts: dict[str, int],
    ) -> int:
        repaired: int = 0
        cursor: int = 0
        while True:
            cursor, items = await self._redis.hscan(
                KEY_RSS_CREATORS,
                cursor=cursor,
                count=self._batch_size,
            )
            for channel_id in items.keys():
                if not _CHANNEL_ID_RE.match(channel_id):
                    continue
                if await self._schedule_channel(
                    channel_id,
                    reason='rss_creator_missing_channel_state',
                    options=options,
                ):
                    repaired += 1
                if self._budget_done(
                    repaired, counts, options,
                ):
                    return repaired
            if cursor == 0:
                break
        return repaired

    async def _repair_rss_seed(
        self,
        options: RepairOptions,
        counts: dict[str, int],
    ) -> int:
        repaired: int = 0
        seen: set[str] = set()
        async for member in self._iter_channel_id_members():
            if member in seen:
                continue
            seen.add(member)
            terminal: ChannelState | None = (
                await self._terminal_state(member)
            )
            if terminal in RSS_SUPPRESSING_STATES:
                continue
            channel_id: str = member[2:]
            existing_label: str | None
            in_creators: bool
            in_suppressed: bool
            in_creators, in_suppressed, existing_label = (
                await self._rss_accounting_state(channel_id)
            )
            if in_suppressed:
                continue
            if in_creators:
                if existing_label:
                    added: int = await self._redis.sadd(
                        'rss:youtube:names',
                        existing_label.lower(),
                    )
                    if added:
                        repaired += 1
                if self._budget_done(repaired, counts, options):
                    return repaired
                continue
            label: str | None = await self._rss_label(
                member, channel_id,
            )
            if not label:
                continue
            await self._write_rss_seed(
                channel_id, label, options,
            )
            repaired += 1
            if self._budget_done(repaired, counts, options):
                return repaired
        return repaired

    async def _repair_rss_suppress(
        self,
        options: RepairOptions,
        counts: dict[str, int],
    ) -> int:
        repaired: int = 0
        for state in sorted(
            RSS_SUPPRESSING_STATES,
            key=lambda item: item.value,
        ):
            cursor: int = 0
            state_key: str = self._queue._k_state(state)
            while True:
                cursor, items = await self._redis.hscan(
                    state_key,
                    cursor=cursor,
                    count=self._batch_size,
                )
                for member in items.keys():
                    if not member.startswith('i:'):
                        continue
                    channel_id: str = member[2:]
                    if not _CHANNEL_ID_RE.match(channel_id):
                        continue
                    changed: bool = await self._write_rss_suppressed(
                        channel_id, state, options,
                    )
                    if changed:
                        repaired += 1
                    if self._budget_done(
                        repaired, counts, options,
                    ):
                        return repaired
                if cursor == 0:
                    break
        return repaired

    async def _repair_meta_orphans(
        self,
        options: RepairOptions,
        counts: dict[str, int],
    ) -> int:
        repaired: int = 0
        async for member, meta in self._iter_meta_records():
            state: str | None = meta.get('state')
            if await self._channel_present(member):
                continue
            if (
                state == ChannelState.SCHEDULED.value
                and member.startswith('i:')
            ):
                channel_id: str = member[2:]
                if not _CHANNEL_ID_RE.match(channel_id):
                    continue
                if await self._schedule_channel(
                    channel_id,
                    reason='meta_scheduled_missing_zset',
                    options=options,
                ):
                    repaired += 1
            elif (
                state
                == ChannelState.PENDING_RESOLUTION.value
                and member.startswith('h:')
            ):
                score: float = self._score_for_member(
                    member,
                    options=options,
                    spread_seconds=(
                        DEFAULT_META_ORPHAN_SPREAD_SECONDS
                    ),
                )
                added: int = await self._redis.zadd(
                    self._queue._k_unresolved(),
                    {member: score},
                    nx=True,
                )
                if added:
                    repaired += 1
            if self._budget_done(repaired, counts, options):
                return repaired
        return repaired

    async def _repair_meta_cache(
        self,
        options: RepairOptions,
        counts: dict[str, int],
    ) -> int:
        repaired: int = 0
        for tier in range(len(self._queue._tiers)):
            cursor: int = 0
            key: str = self._queue._k_scheduled(tier)
            while True:
                cursor, items = await self._redis.zscan(
                    key,
                    cursor=cursor,
                    count=self._batch_size,
                )
                for member, _score in items:
                    if await self._ensure_meta_state(
                        member,
                        ChannelState.SCHEDULED,
                        options,
                    ):
                        repaired += 1
                    if self._budget_done(
                        repaired, counts, options,
                    ):
                        return repaired
                if cursor == 0:
                    break
        cursor = 0
        while True:
            cursor, items = await self._redis.zscan(
                self._queue._k_unresolved(),
                cursor=cursor,
                count=self._batch_size,
            )
            for member, _score in items:
                if await self._ensure_meta_state(
                    member,
                    ChannelState.PENDING_RESOLUTION,
                    options,
                ):
                    repaired += 1
                if self._budget_done(repaired, counts, options):
                    return repaired
            if cursor == 0:
                break
        for state in ChannelState.terminal_states():
            cursor = 0
            while True:
                cursor, items = await self._redis.hscan(
                    self._queue._k_state(state),
                    cursor=cursor,
                    count=self._batch_size,
                )
                for member in items.keys():
                    if await self._ensure_meta_state(
                        member, state, options,
                    ):
                        repaired += 1
                    if self._budget_done(
                        repaired, counts, options,
                    ):
                        return repaired
                if cursor == 0:
                    break
        return repaired

    async def _repair_tier_placement(
        self,
        options: RepairOptions,
        counts: dict[str, int],
    ) -> int:
        repaired: int = 0
        for current_tier in range(len(self._queue._tiers)):
            cursor: int = 0
            current_key: str = self._queue._k_scheduled(
                current_tier,
            )
            while True:
                cursor, items = await self._redis.zscan(
                    current_key,
                    cursor=cursor,
                    count=self._batch_size,
                )
                for member, score in items:
                    if not member.startswith('i:'):
                        continue
                    channel_id: str = member[2:]
                    raw: str | None = await self._redis.hget(
                        self._queue._k_tiers(), channel_id,
                    )
                    if raw is None:
                        continue
                    try:
                        desired_tier = int(raw)
                    except ValueError:
                        continue
                    if (
                        desired_tier == current_tier
                        or desired_tier < 0
                        or desired_tier >= len(self._queue._tiers)
                    ):
                        continue
                    pipe: aioredis.client.Pipeline = (
                        self._redis.pipeline(transaction=True)
                    )
                    pipe.zrem(current_key, member)
                    pipe.zadd(
                        self._queue._k_scheduled(desired_tier),
                        {member: float(score)},
                    )
                    await pipe.execute()
                    repaired += 1
                    if self._budget_done(
                        repaired, counts, options,
                    ):
                        return repaired
                if cursor == 0:
                    break
        return repaired

    async def _repair_state_membership(
        self,
        options: RepairOptions,
        counts: dict[str, int],
    ) -> int:
        '''Remove active queue memberships for terminal members.

        Terminal hashes are the authority for stopped channels. A member
        found in both a terminal hash and a scheduled/unresolved zset is
        unsafe to scrape again, and also causes RSS/channel drift reports
        to balloon. This repair leaves the terminal hash intact, removes
        active zset memberships, and refreshes the meta cache to match
        the terminal state.
        '''
        repaired: int = 0
        scheduled_keys: list[str] = [
            self._queue._k_scheduled(t)
            for t in range(len(self._queue._tiers))
        ]
        active_keys: list[str] = [
            self._queue._k_unresolved(),
            *scheduled_keys,
        ]
        for state in ChannelState.terminal_states():
            cursor: int = 0
            state_key: str = self._queue._k_state(state)
            while True:
                cursor, items = await self._redis.hscan(
                    state_key,
                    cursor=cursor,
                    count=self._batch_size,
                )
                for member in items.keys():
                    pipe: aioredis.client.Pipeline = (
                        self._redis.pipeline(transaction=True)
                    )
                    for key in active_keys:
                        pipe.zrem(key, member)
                    results: list[object] = await pipe.execute()
                    removed: int = sum(int(r or 0) for r in results)
                    changed_meta: bool = await self._ensure_meta_state(
                        member, state, options,
                    )
                    if removed or changed_meta:
                        repaired += 1
                    if self._budget_done(
                        repaired, counts, options,
                    ):
                        return repaired
                if cursor == 0:
                    break
        return repaired

    def _budget_done(
        self,
        repaired_in_mode: int,
        prior_counts: dict[str, int],
        options: RepairOptions,
    ) -> bool:
        remaining = self._remaining_budget(
            prior_counts, options,
        )
        return (
            remaining is not None
            and repaired_in_mode >= remaining
        )

    async def _rss_accounting_state(
        self, channel_id: str,
    ) -> tuple[bool, bool, str | None]:
        pipe: aioredis.client.Pipeline = (
            self._redis.pipeline(transaction=False)
        )
        pipe.hexists(KEY_RSS_CREATORS, channel_id)
        pipe.hexists(KEY_RSS_SUPPRESSED, channel_id)
        pipe.hget(KEY_RSS_CREATORS, channel_id)
        in_creators, in_suppressed, label = await pipe.execute()
        return bool(in_creators), bool(in_suppressed), label

    async def _rss_label(
        self,
        member: str,
        channel_id: str,
    ) -> str | None:
        pipe: aioredis.client.Pipeline = (
            self._redis.pipeline(transaction=False)
        )
        pipe.hget('youtube:creator_map', channel_id)
        pipe.hget(self._queue._k_meta(member), 'handle')
        pipe.hget(self._queue._k_meta(member), 'name')
        map_handle, meta_handle, meta_name = (
            await pipe.execute()
        )
        label: str | None = (
            map_handle or meta_handle or meta_name
        )
        return label or None

    async def _write_rss_seed(
        self,
        channel_id: str,
        label: str,
        options: RepairOptions,
    ) -> None:
        tier: int = self._default_rss_tier(options)
        score: float = self._score_for_member(
            f'rss:{channel_id}',
            options=options,
        )
        pipe: aioredis.client.Pipeline = (
            self._redis.pipeline(transaction=True)
        )
        pipe.hset(KEY_RSS_CREATORS, channel_id, label)
        pipe.hset('rss:youtube:tiers', channel_id, str(tier))
        pipe.sadd('rss:youtube:names', label.lower())
        pipe.zadd(f'rss:youtube:queue:{tier}', {channel_id: score})
        pipe.hdel(KEY_RSS_SUPPRESSED, channel_id)
        await pipe.execute()

    async def _write_rss_suppressed(
        self,
        channel_id: str,
        state: ChannelState,
        options: RepairOptions,
    ) -> bool:
        pipe: aioredis.client.Pipeline = (
            self._redis.pipeline(transaction=False)
        )
        pipe.hexists(KEY_RSS_SUPPRESSED, channel_id)
        pipe.hexists(KEY_RSS_CREATORS, channel_id)
        pipe.hget(KEY_RSS_CREATORS, channel_id)
        exists_suppressed, exists_creator, label = (
            await pipe.execute()
        )
        changed: bool = bool(
            not exists_suppressed or exists_creator
        )
        record: str = json.dumps({
            'state': state.value,
            'source': options.source,
            'ts': int(time.time()),
        })
        pipe = self._redis.pipeline(transaction=True)
        pipe.hset(KEY_RSS_SUPPRESSED, channel_id, record)
        pipe.hdel(KEY_RSS_CREATORS, channel_id)
        pipe.hdel('rss:youtube:tiers', channel_id)
        if label:
            pipe.srem('rss:youtube:names', label.lower())
        for tier in options.rss_queue_tiers:
            pipe.zrem(f'rss:youtube:queue:{tier}', channel_id)
        await pipe.execute()
        return changed

    async def _ensure_meta_state(
        self,
        member: str,
        state: ChannelState,
        options: RepairOptions,
    ) -> bool:
        meta_key: str = self._queue._k_meta(member)
        current: str | None = await self._redis.hget(
            meta_key, 'state',
        )
        mapping: dict[str, str] = {
            'state': state.value,
            'source': options.source,
            'reconciled_at': str(int(time.time())),
        }
        if member.startswith('i:'):
            mapping['channel_id'] = member[2:]
        elif member.startswith('h:'):
            mapping['handle'] = member[2:]
        if current == state.value:
            return False
        await self._redis.hset(meta_key, mapping=mapping)
        return True

    async def _iter_channel_id_members(self):
        for tier in range(len(self._queue._tiers)):
            cursor: int = 0
            while True:
                cursor, items = await self._redis.zscan(
                    self._queue._k_scheduled(tier),
                    cursor=cursor,
                    count=self._batch_size,
                )
                for member, _score in items:
                    if member.startswith('i:') and _CHANNEL_ID_RE.match(
                        member[2:],
                    ):
                        yield member
                if cursor == 0:
                    break
        for state in ChannelState.terminal_states():
            cursor = 0
            while True:
                cursor, items = await self._redis.hscan(
                    self._queue._k_state(state),
                    cursor=cursor,
                    count=self._batch_size,
                )
                for member in items.keys():
                    if member.startswith('i:') and _CHANNEL_ID_RE.match(
                        member[2:],
                    ):
                        yield member
                if cursor == 0:
                    break

    async def _iter_meta_records(self):
        async for key in self._redis.scan_iter(
            match=f'{self._queue._k_meta("")}*',
            count=self._batch_size,
        ):
            member: str = key.removeprefix(
                self._queue._k_meta(''),
            )
            meta: dict[str, str] = await self._redis.hgetall(
                key,
            )
            yield member, meta
