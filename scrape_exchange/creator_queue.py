'''
Abstract queue interface for scraping platform creators
with two interchangeable backends.

* :class:`FileCreatorQueue` — wraps the existing in-process
  min-heap + JSON/TSV file persistence.  Single-host only.
* :class:`RedisCreatorQueue` — sorted set in Redis with
  per-creator claim locks.  Works across hosts.

Both expose the same async interface via :class:`CreatorQueue`
so ``worker_loop`` is backend-agnostic.  The interface
uses platform-neutral terminology (``creator_id`` /
``creator_name``) so it can serve platforms beyond
YouTube.

Priority tiers route high-subscriber creators to faster
polling intervals.  Tier 1 is highest priority (shortest
interval); the last tier is lowest priority.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import heapq
import logging
import os
import shutil

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiofiles
import orjson

from scrape_exchange.file_management import (
    AssetFileManagement,
)

_LOGGER: logging.Logger = logging.getLogger(__name__)

# Claim TTL in seconds.  If a worker crashes while
# processing a creator, the claim expires and
# ``populate()`` re-enqueues the orphan.
DEFAULT_CLAIM_TTL: int = 600

# No-feeds entries expire after 24 hours in Redis so
# creators that were temporarily broken get retried.
_NO_FEEDS_TTL: int = 86400

BACKUP_SUFFIX: str = 'bak'

CHANNEL_FILENAME_PREFIX: str = 'channel-'


# -----------------------------------------------------------
# Tier configuration
# -----------------------------------------------------------

@dataclass(frozen=True)
class TierConfig:
    '''
    Configuration for a single priority tier.

    :param tier: 1-based tier number; 1 = highest
        priority (shortest interval).
    :param min_subscribers: Minimum subscriber count
        for creators to qualify for this tier.
        Use 0 for the catch-all lowest-priority tier.
    :param interval_hours: Polling interval in hours.
    '''

    tier: int
    min_subscribers: int
    interval_hours: float


def parse_priority_queues(spec: str) -> list[TierConfig]:
    '''
    Parse a tier specification string into a list of
    :class:`TierConfig` objects.

    Format: ``interval_hours:min_subscribers`` pairs
    separated by commas, ordered from highest to lowest
    priority.  Tier numbers are assigned 1..N in order.

    Example::

        parse_priority_queues('4:1000000,12:100000,24:10000,48:0')

    :param spec: Comma-separated
        ``interval_hours:min_subscribers`` pairs.
    :returns: List of :class:`TierConfig`, tier 1 first.
    :raises ValueError: On malformed input.
    '''

    tiers: list[TierConfig] = []
    for tier_num, pair in enumerate(
        spec.split(','), start=1,
    ):
        parts: list[str] = pair.strip().split(':')
        if len(parts) != 2:
            raise ValueError(
                f'Invalid tier pair {pair!r}; '
                f'expected interval_hours:min_subscribers'
            )
        interval_hours: float = float(parts[0])
        min_subscribers: int = int(parts[1])
        tiers.append(TierConfig(
            tier=tier_num,
            min_subscribers=min_subscribers,
            interval_hours=interval_hours,
        ))
    return tiers


def tier_for_subscriber_count(
    tiers: list[TierConfig],
    count: int | None,
) -> int:
    '''
    Return the tier number for a given subscriber count.

    Tiers must be ordered tier-1 first (highest priority).
    A creator is placed in the first tier where
    ``count >= tier.min_subscribers``.

    ``None`` means the subscriber count is genuinely
    unknown (never scraped); these go to tier 1 so new
    creators are scraped quickly.  ``0`` is treated as a
    known value and falls through to the normal threshold
    comparison (landing in the lowest tier).

    :param tiers: Ordered list of :class:`TierConfig`.
    :param count: Subscriber count, or ``None`` for
        unknown.
    :returns: 1-based tier number.
    '''

    if count is None:
        return 1
    for tc in tiers:
        if count >= tc.min_subscribers:
            return tc.tier
    # Fallback: last tier (lowest priority).
    return tiers[-1].tier


def _should_skip_creator(
    creator_name: str,
    creator_id: str,
    channel_fm: AssetFileManagement,
) -> bool:
    '''
    Check whether a creator has a ``.not_found`` or
    ``.unresolved`` marker file on the local filesystem.
    '''

    not_found: Path = (
        channel_fm.base_dir
        / f'{CHANNEL_FILENAME_PREFIX}'
        f'{creator_name}.not_found'
    )
    unresolved: Path = (
        channel_fm.base_dir
        / f'{CHANNEL_FILENAME_PREFIX}'
        f'{creator_id}.unresolved'
    )
    return not_found.exists() or unresolved.exists()


# -----------------------------------------------------------
# Abstract base
# -----------------------------------------------------------

class CreatorQueue(ABC):
    '''
    Abstract base for the RSS creator priority queue.

    Uses platform-neutral terminology so the same
    interface can serve YouTube channels, Twitch
    streamers, or any other platform's creators.

    Usage::

        q: CreatorQueue = RedisCreatorQueue(
            dsn, wid, 'youtube',
        )
        await q.populate(
            creator_map, fm, tiers, sub_counts,
        )
        batch = await q.claim_batch(12, wid)
        for cid, name in batch:
            ...
            await q.release(cid)
    '''

    @abstractmethod
    async def known_creator_ids(self) -> set[str]:
        '''
        Return the set of creator IDs already present
        in any tier queue.  Used by callers to skip
        expensive subscriber-count lookups for creators
        that are already enqueued.
        '''

    @abstractmethod
    async def populate(
        self,
        creators: dict[str, str],
        channel_fm: AssetFileManagement,
        tiers: list[TierConfig],
        subscriber_counts: dict[str, int],
    ) -> int:
        '''
        Add creators not already present in the queue.

        :param creators: Mapping of ``creator_id`` to
            ``creator_name``.
        :param channel_fm: File management instance used
            to check for skip-marker files.
        :param tiers: Ordered list of
            :class:`TierConfig` (tier 1 first).
        :param subscriber_counts: Mapping of
            ``creator_id`` to subscriber count.
        :returns: Count of newly added creators.
        '''

    @abstractmethod
    async def claim_batch(
        self,
        batch_size: int,
        worker_id: str,
        claim_ttl: int = DEFAULT_CLAIM_TTL,
        cutoff: float | None = None,
    ) -> list[tuple[str, str, float]]:
        '''
        Atomically pop up to *batch_size* creators that
        are due (score <= *cutoff*) and claim them.

        :param cutoff: Unix timestamp ceiling.
            Defaults to ``now`` when ``None``.

        Fills from tier 1 first, then tier 2, etc.

        :returns:
            ``[(creator_id, creator_name,
            scheduled_time), ...]`` where
            *scheduled_time* is the queue score
            (the time the channel was due).
        '''

    @abstractmethod
    async def release(
        self,
        creator_id: str,
    ) -> None:
        '''
        Release a claim and re-enqueue the creator.

        The next check time is computed from the
        creator's current tier and the queue's
        ``eligibility_fraction`` (set at construction
        time, default ``1.0``):
        ``now + tier.interval_hours * 3600
        * eligibility_fraction``.
        '''

    @abstractmethod
    async def update_tier(
        self,
        creator_id: str,
        subscriber_count: int,
    ) -> None:
        '''
        Recompute the creator's tier from the given
        subscriber count and update internal state.

        If the creator is currently claimed (between
        ``claim_batch`` and ``release``), the new tier
        takes effect on the next ``release`` call.
        '''

    @abstractmethod
    async def get_tier(self, creator_id: str) -> int:
        '''Return the creator's current tier number.'''

    @abstractmethod
    def get_tier_interval(self, tier: int) -> float:
        '''
        Return the polling interval (in hours) for the
        given tier. Falls back to the last (lowest)
        tier's interval when the tier is unknown.
        '''

    @abstractmethod
    async def next_due_time(self) -> float | None:
        '''UTC timestamp of the earliest due creator.'''

    @abstractmethod
    async def get_no_feeds(
        self,
        creator_id: str,
    ) -> tuple[str, str, int] | None:
        '''Return ``(url, name, count)`` or ``None``.'''

    @abstractmethod
    async def set_no_feeds(
        self,
        creator_id: str,
        url: str,
        name: str,
        count: int,
    ) -> None:
        '''Create or update a no-feeds entry.'''

    @abstractmethod
    async def clear_no_feeds(
        self,
        creator_id: str,
    ) -> None:
        '''Remove a no-feeds entry.'''

    @abstractmethod
    async def remove(
        self,
        creator_id: str,
    ) -> None:
        '''
        Permanently remove a creator from the queue.

        Removes the creator from all tier sorted sets
        and any active claim, but keeps the entry in
        the creators hash, tiers hash, and names index
        so ``populate()`` continues to skip the channel
        and does not re-add it.
        '''

    @abstractmethod
    async def queue_size(self) -> int:
        '''Current number of creators across all tiers.'''

    @abstractmethod
    async def queue_sizes_by_tier(
        self,
    ) -> dict[int, int]:
        '''Per-tier creator counts: ``{tier: count}``.'''

    @abstractmethod
    async def cleanup_stale_claims(self) -> int:
        '''
        Re-enqueue creators whose claims expired
        (crash recovery).  Returns count recovered.
        '''


# -----------------------------------------------------------
# File-backed implementation (single host)
# -----------------------------------------------------------

class FileCreatorQueue(CreatorQueue):
    '''
    In-process per-tier min-heaps persisted to a JSON
    file.  No-feeds state is persisted to a TSV file.

    This wraps the existing queue logic so that
    ``worker_loop`` can use the :class:`CreatorQueue`
    interface without behavioural changes when
    ``REDIS_DSN`` is not set.

    Queue file format::

        {
          "tiers": [{"tier": 1, "min_subscribers": ...,
                     "interval_hours": ...}, ...],
          "queues": {"1": [[ts, name, cid], ...], ...},
          "creator_tiers": {"cid": tier, ...}
        }

    Legacy format (plain list) is accepted for migration:
    all entries are assigned to the last (lowest) tier.
    '''

    def __init__(
        self,
        queue_file: str,
        no_feeds_file: str,
        eligibility_fraction: float = 1.0,
    ) -> None:
        self._queue_file: str = queue_file
        self._no_feeds_file: str = no_feeds_file
        self._eligibility_fraction: float = eligibility_fraction

        # Per-tier heaps: tier number → heap of
        # (timestamp, name, creator_id).
        self._heaps: dict[
            int, list[tuple[float, str, str]]
        ] = {}

        # creator_id → tier number
        self._creator_tiers: dict[str, int] = {}

        # creator_id → creator_name
        self._names: dict[str, str] = {}

        self._no_feeds: dict[
            str, tuple[str, str, int]
        ] = {}
        self._claimed: set[str] = set()
        self._tiers: list[TierConfig] = []
        self._loaded: bool = False

    def _ensure_heap(self, tier: int) -> None:
        if tier not in self._heaps:
            self._heaps[tier] = []

    async def _read_queue_file(self) -> Any:
        '''
        Read and return parsed JSON from the queue
        file, falling back to the backup on failure.
        Returns ``None`` if neither file is readable.
        '''

        bak: str = (
            f'{self._queue_file}.{BACKUP_SUFFIX}'
        )
        if os.path.isfile(self._queue_file):
            try:
                async with aiofiles.open(
                    self._queue_file, 'rb',
                ) as fd:
                    data: bytes = await fd.read()
                raw: Any = orjson.loads(data)
                try:
                    shutil.copyfile(
                        self._queue_file, bak,
                    )
                except OSError:
                    pass
                return raw
            except Exception as exc:
                _LOGGER.warning(
                    'Failed to load queue file',
                    extra={
                        'exc': str(exc),
                        'queue_file': (
                            self._queue_file
                        ),
                    },
                )

        if not os.path.isfile(bak):
            return None
        try:
            shutil.copyfile(bak, self._queue_file)
            async with aiofiles.open(
                self._queue_file, 'rb',
            ) as fd:
                bak_data: bytes = await fd.read()
            _LOGGER.info('Using backup queue file')
            return orjson.loads(bak_data)
        except Exception:
            return None

    def _ingest_legacy(
        self,
        entries: list[Any],
        last_tier: int,
    ) -> None:
        '''Load a legacy (flat-list) queue file.'''

        _LOGGER.info(
            'Migrating legacy queue file to '
            'tiered format',
        )
        seen_names: set[str] = set()
        seen_ids: set[str] = set()
        self._ensure_heap(last_tier)
        for entry in entries:
            ts: float = entry[0]
            name: str = entry[1]
            cid: str = entry[2]
            if (name.lower() in seen_names
                    or cid.lower() in seen_ids):
                continue
            seen_names.add(name.lower())
            seen_ids.add(cid.lower())
            self._heaps[last_tier].append(
                (ts, name, cid),
            )
            self._creator_tiers[cid] = last_tier
            self._names[cid] = name
        heapq.heapify(self._heaps[last_tier])

    def _ingest_tiered(
        self, raw: dict[str, Any],
    ) -> None:
        '''Load a new tiered-format queue file.'''

        queues: dict[str, list[Any]] = (
            raw.get('queues', {})
        )
        creator_tiers: dict[str, int] = (
            raw.get('creator_tiers', {})
        )
        seen_names: set[str] = set()
        seen_ids: set[str] = set()
        for tier_str, entries in queues.items():
            tier_num: int = int(tier_str)
            self._ensure_heap(tier_num)
            for entry in entries:
                ts: float = entry[0]
                name: str = entry[1]
                cid: str = entry[2]
                if (name.lower() in seen_names
                        or cid.lower() in seen_ids):
                    continue
                seen_names.add(name.lower())
                seen_ids.add(cid.lower())
                self._heaps[tier_num].append(
                    (ts, name, cid),
                )
                self._names[cid] = name
            heapq.heapify(self._heaps[tier_num])
        for cid, t in creator_tiers.items():
            self._creator_tiers[cid] = t

    async def _load_queue(self) -> None:
        '''Load the queue from the JSON file on disk.'''

        raw: Any = await self._read_queue_file()
        if not raw:
            return

        num_tiers: int = len(self._tiers)
        last_tier: int = num_tiers if num_tiers else 1

        if isinstance(raw, list):
            self._ingest_legacy(raw, last_tier)
        else:
            self._ingest_tiered(raw)

    async def _load_no_feeds(self) -> None:
        '''Load no-feeds state from the TSV file.'''

        if not os.path.isfile(self._no_feeds_file):
            return
        async with aiofiles.open(
            self._no_feeds_file, 'r',
        ) as f:
            async for line in f:
                parts: list[str] = (
                    line.strip().split()
                )
                if len(parts) < 3:
                    continue
                cid: str = parts[0]
                url: str = parts[1]
                name: str = parts[2]
                count: int = 1
                if len(parts) >= 4:
                    try:
                        count = int(parts[3])
                    except ValueError:
                        pass
                if cid in self._no_feeds:
                    prev: int = (
                        self._no_feeds[cid][2]
                    )
                    self._no_feeds[cid] = (
                        url, name, prev + count,
                    )
                else:
                    self._no_feeds[cid] = (
                        url, name, count,
                    )

    async def _persist_queue(self) -> None:
        '''Write the current heaps to the JSON file.'''

        queues: dict[str, list[Any]] = {
            str(tier): list(heap)
            for tier, heap in self._heaps.items()
        }
        tiers_data: list[dict[str, Any]] = [
            {
                'tier': tc.tier,
                'min_subscribers': tc.min_subscribers,
                'interval_hours': tc.interval_hours,
            }
            for tc in self._tiers
        ]
        payload: dict[str, Any] = {
            'tiers': tiers_data,
            'queues': queues,
            'creator_tiers': self._creator_tiers,
        }
        try:
            async with aiofiles.open(
                self._queue_file, 'wb',
            ) as fd:
                await fd.write(orjson.dumps(
                    payload,
                    option=orjson.OPT_INDENT_2,
                ))
        except OSError as exc:
            _LOGGER.warning(
                'Failed to write queue file',
                extra={'exc': str(exc)},
            )

    async def _persist_no_feeds(self) -> None:
        '''Write the no-feeds dict to the TSV file.'''

        try:
            async with aiofiles.open(
                self._no_feeds_file, 'w',
            ) as f:
                for cid, (
                    url, name, count,
                ) in sorted(
                    self._no_feeds.items(),
                    key=lambda x: x[1][2],
                    reverse=True,
                ):
                    await f.write(
                        f'{cid}\t{url}\t{name}'
                        f'\t{count}\n'
                    )
        except OSError as exc:
            _LOGGER.warning(
                'Failed to write no-feeds file',
                extra={'exc': str(exc)},
            )

    def _tier_config(self, tier: int) -> TierConfig:
        '''Look up TierConfig by tier number.'''

        for tc in self._tiers:
            if tc.tier == tier:
                return tc
        return self._tiers[-1]

    def get_tier_interval(self, tier: int) -> float:
        return self._tier_config(tier).interval_hours

    def _retier_existing(
        self,
        tiers: list[TierConfig],
        subscriber_counts: dict[str, int],
    ) -> None:
        '''
        Move loaded creators to the correct tier
        based on *subscriber_counts*.  Called on first
        load so legacy entries (all in the lowest tier)
        get reassigned immediately.
        '''

        for cid in list(self._creator_tiers):
            if cid not in subscriber_counts:
                continue
            count: int = subscriber_counts[cid]
            new_tier: int = (
                tier_for_subscriber_count(
                    tiers, count,
                )
            )
            old_tier: int = self._creator_tiers[cid]
            if new_tier == old_tier:
                continue
            # Capture timestamp before removing.
            old_heap: list = self._heaps.get(
                old_tier, [],
            )
            ts: float = next(
                (
                    t for t, _, c in old_heap
                    if c == cid
                ),
                datetime.now(UTC).timestamp(),
            )
            name: str = self._names.get(cid, cid)
            # Remove from old heap and rebuild.
            self._heaps[old_tier] = [
                e for e in old_heap if e[2] != cid
            ]
            heapq.heapify(self._heaps[old_tier])
            # Insert into new heap.
            self._ensure_heap(new_tier)
            heapq.heappush(
                self._heaps[new_tier],
                (ts, name, cid),
            )
            self._creator_tiers[cid] = new_tier

    # -- CreatorQueue interface --------------------------------

    async def known_creator_ids(self) -> set[str]:
        return set(self._creator_tiers.keys())

    async def populate(
        self,
        creators: dict[str, str],
        channel_fm: AssetFileManagement,
        tiers: list[TierConfig],
        subscriber_counts: dict[str, int],
    ) -> int:
        self._tiers = tiers
        for tc in tiers:
            self._ensure_heap(tc.tier)

        if not self._loaded:
            await self._load_queue()
            await self._load_no_feeds()
            self._loaded = True

            # Re-tier existing entries whose subscriber
            # count is now known (migration from legacy
            # flat queue).
            self._retier_existing(
                tiers, subscriber_counts,
            )

        existing_ids: set[str] = set()
        existing_names: set[str] = set()
        for heap in self._heaps.values():
            for _, name, cid in heap:
                existing_ids.add(cid.lower())
                existing_names.add(name.lower())

        now: float = datetime.now(UTC).timestamp()
        added: int = 0
        for cid, name in creators.items():
            if (cid.lower() in existing_ids
                    or name.lower()
                    in existing_names):
                continue
            if _should_skip_creator(
                name, cid, channel_fm,
            ):
                continue
            count: int | None = subscriber_counts.get(cid)
            tier: int = tier_for_subscriber_count(
                tiers, count,
            )
            self._ensure_heap(tier)
            heapq.heappush(
                self._heaps[tier],
                (now, name, cid),
            )
            self._creator_tiers[cid] = tier
            existing_ids.add(cid.lower())
            existing_names.add(name.lower())
            self._names[cid] = name
            added += 1

        return added

    async def claim_batch(
        self,
        batch_size: int,
        worker_id: str,
        claim_ttl: int = DEFAULT_CLAIM_TTL,
        cutoff: float | None = None,
    ) -> list[tuple[str, str, float]]:
        ts: float = (
            cutoff
            if cutoff is not None
            else datetime.now(UTC).timestamp()
        )
        batch: list[tuple[str, str, float]] = []
        for tc in sorted(
            self._tiers, key=lambda t: t.tier,
        ):
            heap: list[tuple[float, str, str]] = (
                self._heaps.get(tc.tier, [])
            )
            while (
                heap
                and heap[0][0] <= ts
                and len(batch) < batch_size
            ):
                score: float
                name: str
                cid: str
                score, name, cid = heapq.heappop(heap)
                self._claimed.add(cid)
                self._names[cid] = name
                batch.append((cid, name, score))
            if len(batch) >= batch_size:
                break
        return batch

    async def release(
        self,
        creator_id: str,
    ) -> None:
        self._claimed.discard(creator_id)
        tier: int = self._creator_tiers.get(
            creator_id,
            self._tiers[-1].tier if self._tiers else 1,
        )
        tc: TierConfig = self._tier_config(tier)
        now: float = datetime.now(UTC).timestamp()
        next_check: float = (
            now
            + tc.interval_hours * 3600 * self._eligibility_fraction
        )
        name: str = self._names.get(
            creator_id, creator_id,
        )
        self._ensure_heap(tier)
        heapq.heappush(
            self._heaps[tier],
            (next_check, name, creator_id),
        )
        await self._persist_queue()

    async def update_tier(
        self,
        creator_id: str,
        subscriber_count: int,
    ) -> None:
        new_tier: int = tier_for_subscriber_count(
            self._tiers, subscriber_count,
        )
        self._creator_tiers[creator_id] = new_tier
        # If not currently claimed, move in-heap entry.
        # We use lazy deletion: just update the tier
        # map; the stale heap entry will be skipped
        # when popped (creator_id check vs
        # _creator_tiers).  On release(), it will be
        # pushed to the correct heap.
        # No-op for in-flight claimed creators;
        # release() reads _creator_tiers on completion.

    async def get_tier(self, creator_id: str) -> int:
        return self._creator_tiers.get(
            creator_id,
            self._tiers[-1].tier if self._tiers else 1,
        )

    async def next_due_time(self) -> float | None:
        best: float | None = None
        for heap in self._heaps.values():
            if heap:
                ts: float = heap[0][0]
                if best is None or ts < best:
                    best = ts
        return best

    async def get_no_feeds(
        self, creator_id: str,
    ) -> tuple[str, str, int] | None:
        return self._no_feeds.get(creator_id)

    async def set_no_feeds(
        self,
        creator_id: str,
        url: str,
        name: str,
        count: int,
    ) -> None:
        if creator_id in self._no_feeds:
            prev_url: str
            prev_name: str
            prev_count: int
            prev_url, prev_name, prev_count = (
                self._no_feeds[creator_id]
            )
            self._no_feeds[creator_id] = (
                url or prev_url,
                name or prev_name,
                prev_count + count,
            )
        else:
            self._no_feeds[creator_id] = (
                url, name, count,
            )
        await self._persist_no_feeds()

    async def clear_no_feeds(
        self, creator_id: str,
    ) -> None:
        self._no_feeds.pop(creator_id, None)
        await self._persist_no_feeds()

    async def remove(
        self, creator_id: str,
    ) -> None:
        self._claimed.discard(creator_id)
        tier: int | None = self._creator_tiers.get(
            creator_id,
        )
        # Keep creator_id in _creator_tiers and _names
        # so populate() still sees it in existing_ids /
        # existing_names and won't re-add the channel.
        if tier is not None and tier in self._heaps:
            self._heaps[tier] = [
                entry for entry in self._heaps[tier]
                if entry[2] != creator_id
            ]
            heapq.heapify(self._heaps[tier])
        await self._persist_queue()

    async def queue_size(self) -> int:
        return sum(
            len(h) for h in self._heaps.values()
        )

    async def queue_sizes_by_tier(
        self,
    ) -> dict[int, int]:
        return {
            t: len(h)
            for t, h in self._heaps.items()
        }

    async def cleanup_stale_claims(self) -> int:
        # File backend is single-process; claims
        # are in-memory and never stale.
        return 0


# -----------------------------------------------------------
# Redis-backed implementation (cross-host)
# -----------------------------------------------------------

# The Lua claim script iterates all tier queues in
# priority order, collecting due entries until the
# batch is full.
#
# KEYS[1..N]   = queue:1..queue:N (tier sorted sets)
# KEYS[N+1]    = creators hash
# ARGV[1]      = now (unix timestamp as string)
# ARGV[2]      = batch_size
# ARGV[3]      = worker_id
# ARGV[4]      = claim_ttl (seconds)
# ARGV[5]      = claim_key prefix

_LUA_CLAIM_BATCH: str = '''\
local num_tiers = #KEYS - 1
local remaining = tonumber(ARGV[2])
local out = {}
for q = 1, num_tiers do
    if remaining <= 0 then break end
    local due = redis.call(
        'ZRANGEBYSCORE', KEYS[q],
        '-inf', ARGV[1],
        'WITHSCORES',
        'LIMIT', 0, remaining)
    for i = 1, #due, 2 do
        local cid = due[i]
        local score = due[i + 1]
        local key = ARGV[5] .. cid
        local ok = redis.call(
            'SET', key, ARGV[3],
            'NX', 'EX', tonumber(ARGV[4]))
        if ok then
            redis.call('ZREM', KEYS[q], cid)
            local name = redis.call(
                'HGET', KEYS[num_tiers + 1],
                cid) or ''
            out[#out + 1] = cid
            out[#out + 1] = name
            out[#out + 1] = score
            remaining = remaining - 1
        end
    end
end
return out
'''

# Orphan recovery: scan creators hash; for each
# creator check whether it is in ANY tier queue or
# currently claimed.  If neither, re-add to the
# tier recorded in the tiers hash (or last tier as
# fallback).
#
# KEYS[1]      = creators hash
# KEYS[2..N+1] = queue:1..queue:N
# KEYS[N+2]    = tiers hash
# ARGV[1]      = now (unix timestamp)
# ARGV[2]      = claim_key prefix
# ARGV[3]      = last tier number (string)

_LUA_RECOVER_ORPHANS: str = '''\
local recovered = 0
local num_queues = #KEYS - 2
local cursor = '0'
repeat
    local result = redis.call(
        'HSCAN', KEYS[1], cursor, 'COUNT', 200)
    cursor = result[1]
    local data = result[2]
    for i = 1, #data, 2 do
        local cid = data[i]
        local in_any = false
        for q = 1, num_queues do
            local sc = redis.call(
                'ZSCORE', KEYS[q + 1], cid)
            if sc then
                in_any = true
                break
            end
        end
        if not in_any then
            local claimed = redis.call(
                'EXISTS', ARGV[2] .. cid)
            if claimed == 0 then
                local tier_str = redis.call(
                    'HGET', KEYS[#KEYS], cid)
                local tier = tonumber(tier_str)
                    or tonumber(ARGV[3])
                local qkey = KEYS[tier + 1]
                redis.call(
                    'ZADD', qkey,
                    ARGV[1], cid)
                recovered = recovered + 1
            end
        end
    end
until cursor == '0'
return recovered
'''


class RedisCreatorQueue(CreatorQueue):
    '''
    Redis-backed queue using per-tier sorted sets.

    Multiple workers across hosts share the sorted sets.
    Per-creator claims (``SET NX EX``) prevent duplicate
    processing.

    All Redis keys are prefixed with
    ``rss:{platform}:`` so multiple platforms can
    coexist on the same Redis instance.

    Tier queues: ``rss:{platform}:queue:1``, ``:queue:2``
    etc.  Tier membership: ``rss:{platform}:tiers``
    hash (``creator_id`` → tier number as string).
    Case-insensitive name index: ``rss:{platform}:names``
    set (lowercased channel names for dedup).

    Uses ``redis.asyncio`` following the pattern
    established by :class:`_RedisBackend` in
    ``scrape_exchange/rate_limiter.py``.
    '''

    def __init__(
        self,
        redis_dsn: str,
        worker_id: str,
        platform: str = 'youtube',
        eligibility_fraction: float = 1.0,
    ) -> None:
        import redis.asyncio as aioredis
        self._redis: aioredis.Redis = (
            aioredis.from_url(
                redis_dsn, decode_responses=True,
            )
        )
        self._worker_id: str = worker_id
        self._platform: str = platform
        self._eligibility_fraction: float = eligibility_fraction

        p: str = self._platform
        self._key_creators: str = (
            f'rss:{p}:creators'
        )
        self._key_tiers: str = f'rss:{p}:tiers'
        self._claim_prefix: str = (
            f'rss:{p}:claim:'
        )
        self._no_feeds_prefix: str = (
            f'rss:{p}:no_feeds:'
        )
        self._key_names: str = f'rss:{p}:names'
        self._names_indexed: bool = False

        # Populated by populate(); tier queues keys.
        self._key_queues: list[str] = []
        self._tiers: list[TierConfig] = []

        self._claim_script: Any = None
        self._recover_script: Any = None

    async def _ensure_names_index(self) -> None:
        '''
        Populate ``rss:{p}:names`` from the creators
        hash if the set does not yet exist.  Uses
        ``HSCAN`` to avoid loading the entire hash
        into Python memory at once.  Idempotent:
        ``SADD`` is a no-op for existing members,
        so concurrent workers running this are safe.
        '''

        if self._names_indexed:
            return
        self._names_indexed = True

        if await self._redis.exists(self._key_names):
            return
        if not await self._redis.exists(
            self._key_creators,
        ):
            return

        _LOGGER.info(
            'Building names index from creators hash',
        )
        cursor: str | int = 0
        while True:
            cursor, data = await self._redis.hscan(
                self._key_creators,
                cursor,
                count=1000,
            )
            if data:
                lowered: list[str] = [
                    v.lower() for v in data.values()
                ]
                await self._redis.sadd(
                    self._key_names, *lowered,
                )
            if cursor == 0:
                break

    def _build_queue_keys(
        self, tiers: list[TierConfig],
    ) -> list[str]:
        p: str = self._platform
        return [
            f'rss:{p}:queue:{tc.tier}'
            for tc in sorted(
                tiers, key=lambda t: t.tier,
            )
        ]

    def _ensure_scripts(self) -> None:
        '''Register Lua scripts on first use.'''

        if self._claim_script is not None:
            return
        self._claim_script = (
            self._redis.register_script(
                _LUA_CLAIM_BATCH,
            )
        )
        self._recover_script = (
            self._redis.register_script(
                _LUA_RECOVER_ORPHANS,
            )
        )

    def get_tier_interval(self, tier: int) -> float:
        '''Return interval_hours for the given tier.'''

        for tc in self._tiers:
            if tc.tier == tier:
                return tc.interval_hours
        return self._tiers[-1].interval_hours

    @staticmethod
    def _count_added(results: list[Any]) -> int:
        '''
        Count how many ZADD NX operations succeeded.

        Pipeline results are grouped as (zadd, hset,
        hset) per creator; only the zadd at index 0
        of each group indicates a new member.
        '''

        return sum(
            1
            for i in range(0, len(results), 3)
            if results[i]
        )

    async def _recover_orphans(
        self, last_tier: int,
    ) -> int:
        '''Re-enqueue creators not in any queue.'''

        now: float = datetime.now(UTC).timestamp()
        recovered: int = (
            await self._recover_script(
                keys=[
                    self._key_creators,
                    *self._key_queues,
                    self._key_tiers,
                ],
                args=[
                    str(now),
                    self._claim_prefix,
                    str(last_tier),
                ],
            )
        )
        if recovered:
            _LOGGER.info(
                'Recovered orphaned creators',
                extra={'recovered': recovered},
            )
        return recovered

    # -- CreatorQueue interface --------------------------------

    async def known_creator_ids(self) -> set[str]:
        '''Return all creator IDs from the creators
        hash (superset of all tier queues).'''

        keys: list[str] = await self._redis.hkeys(
            self._key_creators,
        )
        return set(keys)

    async def _migrate_legacy_key(
        self,
        tiers: list[TierConfig],
        subscriber_counts: dict[str, int],
    ) -> int:
        '''
        Migrate from the old single ``rss:{platform}:queue``
        sorted set to per-tier queues.  Returns the count
        of members migrated.

        Uses ``RENAME`` to atomically claim the old key so
        that concurrent scrapers starting at the same time
        don't both attempt the migration.  The renamed
        temporary key is deleted after all members have
        been distributed to tier queues.
        '''

        p: str = self._platform
        old_key: str = f'rss:{p}:queue'
        tmp_key: str = (
            f'rss:{p}:queue:_migrating'
            f':{self._worker_id}'
        )

        # Atomically grab the old key.  If another
        # worker already renamed it, this fails and
        # we skip the migration.
        try:
            await self._redis.rename(
                old_key, tmp_key,
            )
        except Exception:
            # Key doesn't exist or was already
            # renamed by another worker.
            return 0

        members: list[tuple[str, float]] = (
            await self._redis.zrange(
                tmp_key, 0, -1, withscores=True,
            )
        )
        if not members:
            await self._redis.delete(tmp_key)
            return 0

        _LOGGER.info(
            'Migrating legacy Redis queue to '
            'tiered format',
            extra={'members': len(members)},
        )
        pipe = self._redis.pipeline()
        for cid, score in members:
            count: int = subscriber_counts.get(
                cid, 0,
            )
            tier: int = tier_for_subscriber_count(
                tiers, count,
            )
            queue_key: str = (
                f'rss:{p}:queue:{tier}'
            )
            pipe.zadd(
                queue_key, {cid: score}, nx=True,
            )
            pipe.hset(
                self._key_tiers,
                cid, str(tier),
            )
        pipe.delete(tmp_key)
        await pipe.execute()

        _LOGGER.info(
            'Legacy queue migration complete',
            extra={
                'migrated': len(members),
            },
        )
        return len(members)

    async def populate(
        self,
        creators: dict[str, str],
        channel_fm: AssetFileManagement,
        tiers: list[TierConfig],
        subscriber_counts: dict[str, int],
    ) -> int:
        self._tiers = tiers
        self._key_queues = self._build_queue_keys(
            tiers,
        )
        self._ensure_scripts()

        # Migrate old single-queue key if present.
        await self._migrate_legacy_key(
            tiers, subscriber_counts,
        )

        # One-time backfill of the names index from
        # the creators hash (uses HSCAN, not HGETALL).
        await self._ensure_names_index()

        # Pre-filter candidates that should be skipped
        # regardless of name dedup.
        candidates: list[tuple[str, str]] = []
        for cid, name in creators.items():
            if _should_skip_creator(
                name, cid, channel_fm,
            ):
                continue
            candidates.append((cid, name))

        now: float = datetime.now(UTC).timestamp()
        added: int = 0
        last_tier: int = tiers[-1].tier
        chunk_size: int = 500

        for i in range(
            0, len(candidates), chunk_size,
        ):
            chunk: list[tuple[str, str]] = (
                candidates[i:i + chunk_size]
            )
            lowered: list[str] = [
                name.lower() for _, name in chunk
            ]

            # Batch membership check — one round-trip
            # per chunk, O(1) per name in Redis.
            exists_flags: list[int] = (
                await self._redis.smismember(
                    self._key_names, lowered,
                )
            )

            pipe = self._redis.pipeline()
            new_names: list[str] = []
            batch_count: int = 0
            for (cid, name), lname, exists in zip(
                chunk, lowered, exists_flags,
            ):
                if exists:
                    continue
                count: int | None = (
                    subscriber_counts.get(cid)
                )
                tier: int = tier_for_subscriber_count(
                    tiers, count,
                )
                p: str = self._platform
                queue_key: str = (
                    f'rss:{p}:queue:{tier}'
                )
                pipe.zadd(
                    queue_key, {cid: now}, nx=True,
                )
                pipe.hset(
                    self._key_creators, cid, name,
                )
                pipe.hset(
                    self._key_tiers, cid, str(tier),
                )
                new_names.append(lname)
                batch_count += 1

            if batch_count:
                results: list[Any] = (
                    await pipe.execute()
                )
                added += self._count_added(results)
                await self._redis.sadd(
                    self._key_names, *new_names,
                )

        added += await self._recover_orphans(
            last_tier,
        )
        return added

    async def claim_batch(
        self,
        batch_size: int,
        worker_id: str,
        claim_ttl: int = DEFAULT_CLAIM_TTL,
        cutoff: float | None = None,
    ) -> list[tuple[str, str, float]]:
        self._ensure_scripts()
        ts: float = (
            cutoff
            if cutoff is not None
            else datetime.now(UTC).timestamp()
        )
        raw: list = (
            await self._claim_script(
                keys=[
                    *self._key_queues,
                    self._key_creators,
                ],
                args=[
                    str(ts),
                    str(batch_size),
                    worker_id,
                    str(claim_ttl),
                    self._claim_prefix,
                ],
            )
        )
        result: list[tuple[str, str, float]] = []
        for i in range(0, len(raw), 3):
            result.append((
                raw[i],
                raw[i + 1],
                float(raw[i + 2]),
            ))
        return result

    async def release(
        self,
        creator_id: str,
    ) -> None:
        tier_str: str | None = (
            await self._redis.hget(
                self._key_tiers, creator_id,
            )
        )
        fallback_tier: int = (
            self._tiers[-1].tier if self._tiers else 1
        )
        tier: int = (
            int(tier_str) if tier_str else fallback_tier
        )
        interval: float = self.get_tier_interval(tier)
        now: float = datetime.now(UTC).timestamp()
        next_check: float = (
            now
            + interval * 3600 * self._eligibility_fraction
        )
        p: str = self._platform
        queue_key: str = f'rss:{p}:queue:{tier}'
        pipe = self._redis.pipeline()
        pipe.delete(
            f'{self._claim_prefix}{creator_id}',
        )
        pipe.zadd(
            queue_key,
            {creator_id: next_check},
        )
        # Self-heal: if the tier hash had no entry for this
        # creator, write the fallback so the cid never ends
        # up as a zset orphan (in queue:N but missing from
        # _key_tiers). Historical schema drift and partial
        # failures produce such orphans; this keeps the two
        # data structures in sync going forward.
        if tier_str is None:
            pipe.hset(
                self._key_tiers,
                creator_id,
                str(tier),
            )
        await pipe.execute()

    async def update_tier(
        self,
        creator_id: str,
        subscriber_count: int,
    ) -> None:
        new_tier: int = tier_for_subscriber_count(
            self._tiers, subscriber_count,
        )
        await self._redis.hset(
            self._key_tiers,
            creator_id,
            str(new_tier),
        )
        # Creator is currently claimed; no sorted-set
        # manipulation needed.  release() will zadd to
        # the correct queue using the updated tier.

    async def get_tier(self, creator_id: str) -> int:
        tier_str: str | None = (
            await self._redis.hget(
                self._key_tiers, creator_id,
            )
        )
        if tier_str:
            return int(tier_str)
        return (
            self._tiers[-1].tier
            if self._tiers
            else 1
        )

    async def next_due_time(self) -> float | None:
        best: float | None = None
        for key in self._key_queues:
            items: list[tuple[str, float]] = (
                await self._redis.zrange(
                    key, 0, 0, withscores=True,
                )
            )
            if items:
                ts: float = items[0][1]
                if best is None or ts < best:
                    best = ts
        return best

    async def get_no_feeds(
        self, creator_id: str,
    ) -> tuple[str, str, int] | None:
        key: str = (
            f'{self._no_feeds_prefix}{creator_id}'
        )
        data: dict[str, str] = (
            await self._redis.hgetall(key)
        )
        if not data:
            return None
        return (
            data.get('url', ''),
            data.get('name', ''),
            int(data.get('count', '0')),
        )

    async def set_no_feeds(
        self,
        creator_id: str,
        url: str,
        name: str,
        count: int,
    ) -> None:
        key: str = (
            f'{self._no_feeds_prefix}{creator_id}'
        )
        existing: dict[str, str] = (
            await self._redis.hgetall(key)
        )
        prev_count: int = int(
            existing.get('count', '0')
        )
        pipe = self._redis.pipeline()
        pipe.hset(key, mapping={
            'url': url or existing.get('url', ''),
            'name': (
                name
                or existing.get('name', '')
            ),
            'count': str(prev_count + count),
        })
        pipe.expire(key, _NO_FEEDS_TTL)
        await pipe.execute()

    async def clear_no_feeds(
        self, creator_id: str,
    ) -> None:
        await self._redis.delete(
            f'{self._no_feeds_prefix}{creator_id}',
        )

    async def remove(
        self, creator_id: str,
    ) -> None:
        # Keep creator in _key_creators, _key_tiers,
        # and _key_names so populate() still sees it
        # and won't re-add the channel.
        pipe = self._redis.pipeline()
        for key in self._key_queues:
            pipe.zrem(key, creator_id)
        pipe.delete(
            f'{self._claim_prefix}{creator_id}',
        )
        await pipe.execute()

    async def queue_size(self) -> int:
        total: int = 0
        for key in self._key_queues:
            total += await self._redis.zcard(key)
        return total

    async def queue_sizes_by_tier(
        self,
    ) -> dict[int, int]:
        sizes: dict[int, int] = {}
        for tc in self._tiers:
            key: str = (
                f'rss:{self._platform}'
                f':queue:{tc.tier}'
            )
            sizes[tc.tier] = (
                await self._redis.zcard(key)
            )
        return sizes

    async def cleanup_stale_claims(self) -> int:
        self._ensure_scripts()
        now: float = datetime.now(UTC).timestamp()
        last_tier: int = (
            self._tiers[-1].tier if self._tiers else 1
        )
        return await self._recover_script(
            keys=[
                self._key_creators,
                *self._key_queues,
                self._key_tiers,
            ],
            args=[
                str(now),
                self._claim_prefix,
                str(last_tier),
            ],
        )
