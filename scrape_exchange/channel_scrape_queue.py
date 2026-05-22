'''Redis-backed work queue for the channel scraper.

Implements the ChannelScrapeQueue(ABC) interface described in
docs/superpowers/specs/2026-05-18-channel-scrape-queue-design.md
and ADR 0001. v1 ships RedisChannelScrapeQueue only; a
FileChannelScrapeQueue is planned for v2.
'''

import enum
import json
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Self

import redis.asyncio as aioredis
from pydantic import Field
from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
)


KEY_PREFIX: str = 'youtube:channel'


@dataclass(frozen=True)
class ChannelTierConfig:
    '''Configuration for one channel-queue priority tier.

    :param tier: 0-based tier index; 0 = highest priority
        (shortest re-scrape interval).
    :param min_subscribers: Minimum subscriber count for
        channels to qualify for this tier. 0 marks the
        catch-all lowest-priority tier.
    :param interval_seconds: Seconds between re-scrapes;
        ``-1`` means drop the channel from the queue after
        one successful scrape (no respawn).
    '''

    tier: int
    min_subscribers: int
    interval_seconds: int


def parse_channel_priority_queues(
    spec: str,
) -> list[ChannelTierConfig]:
    '''Parse a tier spec string into ``ChannelTierConfig``s.

    Format mirrors the RSS scraper's ``RSS_PRIORITY_QUEUES``:
    ``interval_seconds:min_subscribers`` pairs separated by
    commas, ordered from highest to lowest priority. Channel
    intervals are in *days* (not hours, unlike RSS) so the
    ``-1`` "no-respawn" sentinel survives as an integer.

    Example::

        parse_channel_priority_queues(
            '7:1000000,30:100000,'
            '90:10000,180:1000,365:0'
        )

    The last tier must have ``min_subscribers=0`` so every
    channel maps to some tier.

    :raises ValueError: On malformed input or a missing
        ``min_subscribers=0`` catch-all.
    '''

    tiers: list[ChannelTierConfig] = []
    for tier_num, pair in enumerate(spec.split(',')):
        parts: list[str] = pair.strip().split(':')
        if len(parts) != 2:
            raise ValueError(
                f'Invalid tier pair {pair!r}; expected '
                f'interval_days:min_subscribers',
            )
        if int(parts[0]) > 0:
            interval_seconds: int = int(parts[0]) * 24 * 60 * 60
        else:
            interval_seconds = -1
        min_subscribers: int = int(parts[1])
        tiers.append(ChannelTierConfig(
            tier=tier_num,
            min_subscribers=min_subscribers,
            interval_seconds=interval_seconds,
        ))
    if not tiers:
        raise ValueError(
            'channel_priority_queues must define at least '
            'one tier',
        )
    if tiers[-1].min_subscribers != 0:
        raise ValueError(
            'last tier must have min_subscribers=0 '
            '(catch-all); got '
            f'{tiers[-1].min_subscribers}',
        )
    return tiers


_POP_SCHEDULED_LUA: str = '''
-- KEYS: tier_queue_key
-- ARGV: now, batch
local members = redis.call(
    'ZRANGEBYSCORE', KEYS[1], 0, ARGV[1],
    'LIMIT', 0, ARGV[2]
)
if #members > 0 then
    redis.call('ZREM', KEYS[1], unpack(members))
end
return members
'''

_REAP_LUA: str = '''
-- KEYS[1] = soft_unavailable hash
-- KEYS[2] = queue:scheduled:0 ZSET
-- ARGV[1] = member
if redis.call('HDEL', KEYS[1], ARGV[1]) == 1 then
    redis.call('ZADD', KEYS[2], 0, ARGV[1])
    return 1
end
return 0
'''

_UNMARK_LUA: str = '''
-- KEYS[1] = meta hash
-- KEYS[2] = target queue (unresolved or scheduled:<tier>)
-- ARGV[1] = member
-- ARGV[2] = target state value
-- ARGV[3] = target score (as string, will be converted)
local member = ARGV[1]
local target_state = ARGV[2]
local target_score = tonumber(ARGV[3])

local states = {
    'not_found', 'invalid_handle',
    'inconsistent_identity', 'terminated',
    'unresolved', 'removed',
    'soft_unavailable', 'hard_unavailable',
}
for _, s in ipairs(states) do
    redis.call(
        'HDEL', 'youtube:channel:' .. s, member
    )
end
redis.call('ZADD', KEYS[2], target_score, member)
redis.call('HSET', KEYS[1], 'state', target_state)
redis.call(
    'HDEL', KEYS[1],
    'unavailable_attempts', 'resolve_attempts'
)
return 1
'''

_MARK_LUA: str = '''
-- KEYS[1] = meta hash
-- KEYS[2] = target state hash
-- ARGV[1] = member
-- ARGV[2] = state value
-- ARGV[3] = record JSON
-- ARGV[4] = tier count
local member = ARGV[1]
local state = ARGV[2]
local record = ARGV[3]
local tier_count = tonumber(ARGV[4])

redis.call(
    'ZREM',
    'youtube:channel:queue:unresolved',
    member
)
for t = 0, tier_count - 1 do
    redis.call(
        'ZREM',
        'youtube:channel:queue:scheduled:' .. t,
        member
    )
end
local states = {
    'not_found', 'invalid_handle',
    'inconsistent_identity', 'terminated',
    'unresolved', 'removed',
    'soft_unavailable', 'hard_unavailable',
}
for _, s in ipairs(states) do
    redis.call(
        'HDEL', 'youtube:channel:' .. s, member
    )
end
redis.call('HSET', KEYS[2], member, record)
redis.call('HSET', KEYS[1], 'state', state)
return 1
'''


class ChannelState(str, enum.Enum):
    PENDING_RESOLUTION = 'pending_resolution'
    SCHEDULED = 'scheduled'
    NOT_FOUND = 'not_found'
    INVALID_HANDLE = 'invalid_handle'
    INCONSISTENT_IDENTITY = 'inconsistent_identity'
    TERMINATED = 'terminated'
    UNRESOLVED = 'unresolved'
    REMOVED = 'removed'
    SOFT_UNAVAILABLE = 'soft_unavailable'
    HARD_UNAVAILABLE = 'hard_unavailable'

    @classmethod
    def terminal_states(
        cls,
    ) -> frozenset[Self]:
        return frozenset({
            cls.NOT_FOUND,
            cls.INVALID_HANDLE,
            cls.INCONSISTENT_IDENTITY,
            cls.TERMINATED,
            cls.UNRESOLVED,
            cls.REMOVED,
            cls.SOFT_UNAVAILABLE,
            cls.HARD_UNAVAILABLE,
        })


class ChannelScrapeQueueSettings(BaseSettings):
    # Standalone settings — does not inherit from
    # ``ScraperSettings`` because the queue is also
    # consumed by ``yt_channel_queue.py`` (CLI) which has
    # no need for scraper-level config.
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore',
    )

    channel_priority_queues: str = Field(
        default='7:1000000,30:100000,90:10000,180:1000,365:0',
        description=(
            'Comma-separated tier spec '
            '"interval_days:min_subscribers", highest '
            'priority first. -1 in the interval means drop '
            'the channel from the queue after one '
            'successful scrape. The last tier must have '
            'min_subscribers=0 (catch-all). Mirrors the '
            'RSS scraper\'s RSS_PRIORITY_QUEUES, but in days.'
        ),
    )
    channel_queue_resolve_batch: int = Field(default=25)
    channel_queue_scrape_batch: int = Field(default=50)
    channel_queue_idle_poll_seconds: float = Field(
        default=2.0,
    )
    channel_resolve_max_attempts: int = Field(default=5)
    channel_resolve_backoff_seconds: int = Field(
        default=300,
    )
    channel_unavailable_hard_threshold: int = Field(
        default=3,
    )
    channel_unavailable_soft_retry_seconds: int = Field(
        default=86400,
    )
    channel_soft_reap_interval_seconds: int = Field(
        default=60,
    )


class ChannelScrapeQueue(ABC):
    '''Abstract work queue for the channel scraper.

    v1 scaffold: only ``enqueue_unresolved`` is declared.
    Additional abstract methods are introduced in later
    tasks (see plan: docs/superpowers/plans/
    2026-05-18-channel-scrape-queue.md).
    '''

    @abstractmethod
    async def enqueue_unresolved(
        self,
        handle: str,
        *,
        source: str,
        priority: bool = False,
    ) -> None: ...

    @abstractmethod
    async def enqueue_scheduled(
        self,
        channel_id: str,
        *,
        source: str,
        priority: bool = False,
    ) -> None: ...

    @abstractmethod
    async def pop_unresolved(
        self, batch: int,
    ) -> list[str]: ...

    @abstractmethod
    async def pop_scheduled(
        self, batch: int, *, now: float,
    ) -> list[str]: ...

    @abstractmethod
    async def promote_to_scheduled(
        self, handle: str, channel_id: str,
    ) -> None: ...

    @abstractmethod
    async def update_tier(
        self,
        channel_id: str,
        *,
        sub_count: int,
        now: float,
    ) -> None: ...

    @abstractmethod
    async def requeue_with_backoff(
        self,
        key: str,
        *,
        seconds: int,
        now: float,
        unresolved: bool = False,
    ) -> None: ...

    @abstractmethod
    async def mark(
        self,
        member: str,
        *,
        state: ChannelState,
        note: str | None = None,
        last_error: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None: ...

    @abstractmethod
    async def mark_soft_unavailable(
        self,
        channel_id: str,
        *,
        last_error: str | None,
    ) -> None: ...

    @abstractmethod
    async def unmark(self, member: str) -> None: ...

    @abstractmethod
    async def reap_soft_unavailable(
        self, *, now: float,
    ) -> int: ...

    @abstractmethod
    async def get_state(
        self, member: str,
    ) -> ChannelState | None: ...

    @abstractmethod
    async def in_state(
        self, member: str, state: ChannelState,
    ) -> bool: ...

    @abstractmethod
    async def get_meta(
        self, member: str,
    ) -> dict[str, str]: ...

    @abstractmethod
    async def set_meta(
        self, member: str, **fields: str,
    ) -> None: ...

    @abstractmethod
    async def count_by_state(
        self,
    ) -> dict[ChannelState, int]: ...

    @abstractmethod
    async def count_by_tier(
        self,
    ) -> dict[int, int]: ...

    @abstractmethod
    async def search_meta(
        self,
        pattern: str,
        fields: tuple[str, ...] = (
            'handle', 'channel_id', 'name',
        ),
    ) -> list[str]: ...


class RedisChannelScrapeQueue(ChannelScrapeQueue):
    '''Redis-backed implementation of ChannelScrapeQueue.

    All keys live under the ``youtube:channel:*`` namespace
    (see the project Redis-key convention). Methods provide
    atomic semantics via Lua scripts where state transitions
    cross multiple keys.
    '''

    def __init__(
        self,
        redis: aioredis.Redis,
        settings: ChannelScrapeQueueSettings,
    ) -> None:
        self._redis: aioredis.Redis = redis
        self._settings: ChannelScrapeQueueSettings = (
            settings
        )
        self._tiers: list[ChannelTierConfig] = (
            parse_channel_priority_queues(
                settings.channel_priority_queues,
            )
        )

    def _tier_for_sub_count(self, sub_count: int) -> int:
        '''Map a subscriber count to a tier index.

        Walks the parsed tier list in order; the first tier
        whose ``min_subscribers`` threshold is reached wins.
        Falls back to the last tier (the catch-all) if no
        threshold matches.
        '''
        for cfg in self._tiers:
            if sub_count >= cfg.min_subscribers:
                return cfg.tier
        return self._tiers[-1].tier

    def _k_unresolved(self) -> str:
        return f'{KEY_PREFIX}:queue:unresolved'

    def _k_scheduled(self, tier: int) -> str:
        return f'{KEY_PREFIX}:queue:scheduled:{tier}'

    def _k_meta(self, member: str) -> str:
        return f'{KEY_PREFIX}:meta:{member}'

    def _k_state(self, state: ChannelState) -> str:
        return f'{KEY_PREFIX}:{state.value}'

    def _k_tiers(self) -> str:
        return f'{KEY_PREFIX}:tiers'

    @staticmethod
    def _normalise_handle(handle: str) -> str:
        '''Strip whitespace then a leading ``@``, then
        whitespace again. Raises ``ValueError`` if the
        result is empty.'''
        canonical: str = (
            handle.strip().removeprefix('@').strip()
        )
        if not canonical:
            raise ValueError('empty handle')
        return canonical

    async def enqueue_unresolved(
        self,
        handle: str,
        *,
        source: str,
        priority: bool = False,
    ) -> None:
        canonical: str = self._normalise_handle(handle)
        member: str = f'h:{canonical}'
        now: float = time.time()
        score: float = 0.0 if priority else now
        meta_key: str = self._k_meta(member)
        queue_key: str = self._k_unresolved()
        pipe: aioredis.client.Pipeline = (
            self._redis.pipeline(transaction=True)
        )
        pipe.zadd(queue_key, {member: score}, nx=True)
        pipe.hsetnx(meta_key, 'handle', canonical)
        pipe.hsetnx(meta_key, 'source', source)
        pipe.hsetnx(
            meta_key, 'created_at', str(int(now)),
        )
        pipe.hsetnx(
            meta_key,
            'state',
            ChannelState.PENDING_RESOLUTION.value,
        )
        await pipe.execute()

    async def enqueue_scheduled(
        self,
        channel_id: str,
        *,
        source: str,
        priority: bool = False,
    ) -> None:
        if not channel_id.startswith('UC'):
            raise ValueError(
                f'channel_id must start with UC: '
                f'{channel_id!r}'
            )
        member: str = f'i:{channel_id}'
        now: float = time.time()
        tier_str: str | None = await self._redis.hget(
            self._k_tiers(), channel_id,
        )
        tier: int = int(tier_str) if tier_str else 0
        score: float = 0.0 if priority else now
        queue_key: str = self._k_scheduled(tier)
        meta_key: str = self._k_meta(member)
        pipe: aioredis.client.Pipeline = (
            self._redis.pipeline(transaction=True)
        )
        pipe.zadd(queue_key, {member: score}, nx=True)
        pipe.hsetnx(meta_key, 'channel_id', channel_id)
        pipe.hsetnx(meta_key, 'source', source)
        pipe.hsetnx(
            meta_key, 'created_at', str(int(now)),
        )
        pipe.hsetnx(
            meta_key,
            'state',
            ChannelState.SCHEDULED.value,
        )
        await pipe.execute()

    async def pop_unresolved(
        self, batch: int,
    ) -> list[str]:
        queue_key: str = self._k_unresolved()
        raw: list[tuple[str, float]] = (
            await self._redis.zpopmin(queue_key, batch)
        )
        return [
            member.removeprefix('h:')
            for member, _score in raw
        ]

    async def promote_to_scheduled(
        self, handle: str, channel_id: str,
    ) -> None:
        if not channel_id.startswith('UC'):
            raise ValueError(
                f'channel_id must start with UC: '
                f'{channel_id!r}'
            )
        canonical: str = self._normalise_handle(handle)
        old_member: str = f'h:{canonical}'
        new_member: str = f'i:{channel_id}'
        old_meta: str = self._k_meta(old_member)
        new_meta: str = self._k_meta(new_member)
        tier_str: str | None = await self._redis.hget(
            self._k_tiers(), channel_id,
        )
        tier: int = int(tier_str) if tier_str else 0
        # Read old meta to migrate fields. This isn't
        # in the transaction (no MULTI guarantees on
        # reads), but the destination side IS atomic.
        fields: dict[str, str] = (
            await self._redis.hgetall(old_meta)
        )
        fields['channel_id'] = channel_id
        fields['handle'] = canonical
        fields['state'] = ChannelState.SCHEDULED.value
        pipe: aioredis.client.Pipeline = (
            self._redis.pipeline(transaction=True)
        )
        # Fields that always reflect the new identity.
        forced: dict[str, str] = {
            'channel_id': channel_id,
            'handle': canonical,
            'state': ChannelState.SCHEDULED.value,
        }
        # Old-meta fields we want to carry forward only
        # if the destination doesn't already have them.
        carried: dict[str, str] = {
            k: v for k, v in fields.items()
            if k not in forced
        }
        pipe.zrem(self._k_unresolved(), old_member)
        pipe.zadd(
            self._k_scheduled(tier),
            {new_member: 0.0},
            nx=True,
        )
        pipe.hset(new_meta, mapping=forced)
        for field, value in carried.items():
            pipe.hsetnx(new_meta, field, value)
        pipe.delete(old_meta)
        await pipe.execute()

    async def pop_scheduled(
        self, batch: int, *, now: float,
    ) -> list[str]:
        out: list[str] = []
        remaining: int = batch
        num_tiers: int = len(self._tiers)
        for tier in range(num_tiers):
            if remaining <= 0:
                break
            members: list[str] = await self._redis.eval(
                _POP_SCHEDULED_LUA,
                1,
                self._k_scheduled(tier),
                str(now),
                str(remaining),
            )
            if not members:
                continue
            out.extend(
                m.removeprefix('i:') for m in members
            )
            remaining -= len(members)
        return out

    async def update_tier(
        self,
        channel_id: str,
        *,
        sub_count: int,
        now: float,
    ) -> None:
        member: str = f'i:{channel_id}'
        # If an operator (or scrape-phase auto-mark) put
        # this channel into a terminal state while the
        # scrape that produced this call was in flight,
        # do not override that decision. Caller's fresh
        # scrape data is dropped on the floor — that's
        # the correct policy: operator intent wins.
        current: ChannelState | None = (
            await self.get_state(member)
        )
        if (
            current is not None
            and current in ChannelState.terminal_states()
        ):
            return
        new_tier: int = self._tier_for_sub_count(sub_count)
        interval: int = self._tiers[new_tier].interval_seconds
        old_tier_str: str | None = (
            await self._redis.hget(
                self._k_tiers(), channel_id,
            )
        )
        pipe: aioredis.client.Pipeline = (
            self._redis.pipeline(transaction=True)
        )
        if (
            old_tier_str is not None
            and int(old_tier_str) != new_tier
        ):
            pipe.zrem(
                self._k_scheduled(int(old_tier_str)),
                member,
            )
        pipe.hset(
            self._k_tiers(), channel_id, str(new_tier),
        )
        if interval < 0:
            pipe.zrem(
                self._k_scheduled(new_tier), member,
            )
        else:
            pipe.zadd(
                self._k_scheduled(new_tier),
                {member: now + float(interval)},
            )
        pipe.hset(
            self._k_meta(member),
            mapping={
                'state': ChannelState.SCHEDULED.value,
                'last_attempt_at': str(int(now)),
            },
        )
        pipe.hdel(
            self._k_meta(member),
            'unavailable_attempts',
        )
        await pipe.execute()

    async def requeue_with_backoff(
        self,
        key: str,
        *,
        seconds: int,
        now: float,
        unresolved: bool = False,
    ) -> None:
        new_score: float = now + float(seconds)
        if unresolved:
            canonical: str = self._normalise_handle(key)
            await self._redis.zadd(
                self._k_unresolved(),
                {f'h:{canonical}': new_score},
                xx=True,
            )
            return
        member: str = f'i:{key}'
        tier_str: str | None = await self._redis.hget(
            self._k_tiers(), key,
        )
        tier: int = int(tier_str) if tier_str else 0
        await self._redis.zadd(
            self._k_scheduled(tier),
            {member: new_score},
            xx=True,
        )

    async def mark_soft_unavailable(
        self,
        channel_id: str,
        *,
        last_error: str | None,
    ) -> None:
        member: str = f'i:{channel_id}'
        meta_key: str = self._k_meta(member)
        new_count: int = await self._redis.hincrby(
            meta_key, 'unavailable_attempts', 1,
        )
        threshold: int = (
            self._settings
            .channel_unavailable_hard_threshold
        )
        now: float = time.time()
        if new_count >= threshold:
            await self.mark(
                member,
                state=ChannelState.HARD_UNAVAILABLE,
                last_error=last_error,
                extra={'retries_done': new_count - 1},
            )
            return
        retry_seconds: int = (
            self._settings
            .channel_unavailable_soft_retry_seconds
        )
        await self.mark(
            member,
            state=ChannelState.SOFT_UNAVAILABLE,
            last_error=last_error,
            extra={
                'next_retry_at': int(
                    now + retry_seconds,
                ),
            },
        )

    async def mark(
        self,
        member: str,
        *,
        state: ChannelState,
        note: str | None = None,
        last_error: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        if state not in ChannelState.terminal_states():
            raise ValueError(
                f'mark target must be terminal, got '
                f'{state.value!r}'
            )
        now: float = time.time()
        record: dict[str, Any] = {
            'ts': int(now),
            'last_error': last_error,
            'note': note,
        }
        if extra:
            record.update(extra)
        meta_key: str = self._k_meta(member)
        state_key: str = self._k_state(state)
        tier_count: int = len(self._tiers)
        await self._redis.eval(
            _MARK_LUA,
            2,
            meta_key,
            state_key,
            member,
            state.value,
            json.dumps(record),
            str(tier_count),
        )

    async def unmark(self, member: str) -> None:
        if member.startswith('h:'):
            target_queue: str = self._k_unresolved()
            target_state: ChannelState = (
                ChannelState.PENDING_RESOLUTION
            )
            target_score: float = time.time()
        elif member.startswith('i:'):
            channel_id: str = member[2:]
            tier_str: str | None = (
                await self._redis.hget(
                    self._k_tiers(), channel_id,
                )
            )
            tier: int = (
                int(tier_str) if tier_str else 0
            )
            target_queue = self._k_scheduled(tier)
            target_state = ChannelState.SCHEDULED
            target_score = 0.0
        else:
            raise ValueError(
                f'member must have h: or i: prefix: '
                f'{member!r}'
            )
        await self._redis.eval(
            _UNMARK_LUA,
            2,
            self._k_meta(member),
            target_queue,
            member,
            target_state.value,
            str(target_score),
        )

    async def reap_soft_unavailable(
        self, *, now: float,
    ) -> int:
        soft_key: str = self._k_state(
            ChannelState.SOFT_UNAVAILABLE,
        )
        items: dict[str, str] = (
            await self._redis.hgetall(soft_key)
        )
        reaped: int = 0
        for member, raw in items.items():
            try:
                record: dict[str, Any] = (
                    json.loads(raw)
                )
            except (TypeError, ValueError):
                continue
            next_retry: float = float(
                record.get('next_retry_at', 0),
            )
            if next_retry > now:
                continue
            # Look up the tier so we re-queue at the
            # right priority (not always tier 0).
            channel_id: str = member.removeprefix('i:')
            tier_str: str | None = (
                await self._redis.hget(
                    self._k_tiers(), channel_id,
                )
            )
            tier: int = (
                int(tier_str) if tier_str else 0
            )
            moved: int = await self._redis.eval(
                _REAP_LUA,
                2,
                soft_key,
                self._k_scheduled(tier),
                member,
            )
            if moved == 1:
                reaped += 1
                await self._redis.hset(
                    self._k_meta(member),
                    'state',
                    ChannelState.SCHEDULED.value,
                )
        return reaped

    async def get_state(
        self, member: str,
    ) -> ChannelState | None:
        raw: str | None = await self._redis.hget(
            self._k_meta(member), 'state',
        )
        if raw is None:
            return None
        try:
            return ChannelState(raw)
        except ValueError:
            return None

    async def in_state(
        self, member: str, state: ChannelState,
    ) -> bool:
        current: ChannelState | None = (
            await self.get_state(member)
        )
        return current == state

    async def get_meta(
        self, member: str,
    ) -> dict[str, str]:
        return await self._redis.hgetall(
            self._k_meta(member),
        )

    async def set_meta(
        self, member: str, **fields: str,
    ) -> None:
        if fields:
            await self._redis.hset(
                self._k_meta(member),
                mapping=fields,
            )

    async def count_by_state(
        self,
    ) -> dict[ChannelState, int]:
        # Stable sorted list of terminal states so
        # pipeline result positions line up.
        terminal_ordered: list[ChannelState] = sorted(
            ChannelState.terminal_states(),
            key=lambda s: s.value,
        )
        num_tiers: int = len(self._tiers)
        pipe: aioredis.client.Pipeline = (
            self._redis.pipeline(transaction=False)
        )
        pipe.zcard(self._k_unresolved())
        for t in range(num_tiers):
            pipe.zcard(self._k_scheduled(t))
        for s in terminal_ordered:
            pipe.hlen(self._k_state(s))
        results: list[int] = await pipe.execute()
        out: dict[ChannelState, int] = {}
        out[ChannelState.PENDING_RESOLUTION] = (
            results[0]
        )
        out[ChannelState.SCHEDULED] = sum(
            results[1:1 + num_tiers],
        )
        for s, n in zip(
            terminal_ordered,
            results[1 + num_tiers:],
        ):
            out[s] = n
        return out

    async def count_by_tier(
        self,
    ) -> dict[int, int]:
        num_tiers: int = len(self._tiers)
        pipe: aioredis.client.Pipeline = (
            self._redis.pipeline(transaction=False)
        )
        for t in range(num_tiers):
            pipe.zcard(self._k_scheduled(t))
        results: list[int] = await pipe.execute()
        return {
            t: results[t] for t in range(num_tiers)
        }

    @staticmethod
    def _any_field_matches(
        vals: list[str | None],
        pattern: str,
    ) -> bool:
        import fnmatch
        return any(
            v is not None
            and fnmatch.fnmatchcase(v, pattern)
            for v in vals
        )

    async def search_meta(
        self,
        pattern: str,
        fields: tuple[str, ...] = (
            'handle', 'channel_id', 'name',
        ),
    ) -> list[str]:
        out: list[str] = []
        cursor: int = 0
        while True:
            cursor, keys = await self._redis.scan(
                cursor=cursor,
                match=f'{KEY_PREFIX}:meta:*',
                count=500,
            )
            if keys:
                pipe: aioredis.client.Pipeline = (
                    self._redis.pipeline(
                        transaction=False,
                    )
                )
                for k in keys:
                    pipe.hmget(k, *fields)
                values: list[list[str | None]] = (
                    await pipe.execute()
                )
                for key, vals in zip(keys, values):
                    if self._any_field_matches(
                        vals, pattern,
                    ):
                        member: str = key.split(
                            ':meta:', 1,
                        )[1]
                        out.append(member)
            if cursor == 0:
                break
        return out
