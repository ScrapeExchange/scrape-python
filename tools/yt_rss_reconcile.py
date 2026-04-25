#!/usr/bin/env python3

'''
Reconcile the RSS queue Redis state.

Repairs two kinds of desync between the tier hash
(``rss:{platform}:tiers``) and the per-tier zsets
(``rss:{platform}:queue:N``):

1. **Hash orphans** — creators classified in the tier
   hash but not present in any tier zset (nor currently
   claimed). These cids will never be processed until
   they are re-enqueued. Caused by worker crashes
   between ``claim_batch()`` and ``release()`` and by
   ``remove()`` calls that never got a follow-up
   repopulate. Handled via
   :meth:`RedisCreatorQueue.cleanup_stale_claims`
   (existing Lua script).

2. **Zset orphans** — cids present in a tier zset but
   with no entry in the tier hash. Caused by
   historical schema drift: early versions of
   ``populate()`` only wrote the zset, not the tier
   hash. ``release()`` now self-heals on future
   rotation, but a one-shot repair removes the
   backlog immediately. For each zset orphan, this
   tool writes the tier hash entry matching the zset
   the cid is currently in.

Runs dry-run by default. Pass ``--apply`` to write.

Requires REDIS_DSN to be set.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import asyncio
import sys

from datetime import UTC, datetime

from pydantic import Field

import redis.asyncio as aioredis

from scrape_exchange.creator_queue import (
    RedisCreatorQueue,
    TierConfig,
    parse_priority_queues,
)
from scrape_exchange.settings import ScraperSettings


DEFAULT_PRIORITY_QUEUES: str = (
    '1:10000000,4:1000000,12:100000,24:10000,48:0'
)


class ReconcileSettings(ScraperSettings):
    '''Settings for the RSS queue reconcile tool.'''

    platform: str = Field(
        default='youtube',
        description='Platform prefix for the Redis keys',
    )
    priority_queues: str = Field(
        default=DEFAULT_PRIORITY_QUEUES,
        description=(
            'Tier spec matching the RSS scraper '
            '(interval_hours:min_subscribers pairs)'
        ),
    )
    apply: bool = Field(
        default=False,
        description=(
            'If true, write fixes to Redis. Otherwise '
            'run in dry-run mode.'
        ),
    )


async def _count_per_tier(
    redis: aioredis.Redis, platform: str,
    tiers: list[TierConfig],
) -> dict[int, int]:
    sizes: dict[int, int] = {}
    for tc in tiers:
        key: str = f'rss:{platform}:queue:{tc.tier}'
        sizes[tc.tier] = await redis.zcard(key)
    return sizes


async def _hash_tier_counts(
    redis: aioredis.Redis, platform: str,
) -> dict[int, int]:
    '''Count cids per tier in the tier classification hash.'''
    counts: dict[int, int] = {}
    cursor: int = 0
    while True:
        cursor, chunk = await redis.hscan(
            f'rss:{platform}:tiers',
            cursor, count=5000,
        )
        for _cid, tier_str in chunk.items():
            t: int = int(tier_str)
            counts[t] = counts.get(t, 0) + 1
        if cursor == 0:
            break
    return counts


async def _fix_tier_mismatches(
    redis: aioredis.Redis, platform: str,
    tiers: list[TierConfig], apply: bool,
) -> dict[int, int]:
    '''
    For each queue:N zset, find cids whose tier hash entry
    is either absent or says tier M ≠ N. Cids currently
    claimed by a worker are skipped (an in-flight
    update_tier between claim and release is a legitimate
    transient mismatch). Otherwise:

    * **Absent hash entry** — write one matching the zset
      (self-heal the rare "true zset orphan" case).
    * **Hash says different tier** — move the cid to the
      zset matching the hash (the hash is the source of
      truth; ``release()`` uses it to pick the queue).

    :returns: Mapping of zset tier number → count of cids
        moved or hash-written (or would-be, for dry-run).
    '''
    tiers_key: str = f'rss:{platform}:tiers'
    claim_prefix: str = f'rss:{platform}:claim:'
    now: float = datetime.now(UTC).timestamp()
    fixed: dict[int, int] = {}
    for tc in tiers:
        zset_tier: int = tc.tier
        queue_key: str = (
            f'rss:{platform}:queue:{zset_tier}'
        )
        # Cids where hash entry is missing → to be HSET.
        orphan_cids: list[str] = []
        # Cids where hash tier differs → to be moved.
        move_ops: list[tuple[str, int]] = []
        cursor: int = 0
        while True:
            cursor, batch = await redis.zscan(
                queue_key, cursor, count=1000,
            )
            if batch:
                batch_cids: list[str] = [
                    member for member, _score in batch
                ]
                existing: list[str | None] = (
                    await redis.hmget(
                        tiers_key, batch_cids,
                    )
                )
                # Skip cids currently claimed: any apparent
                # mismatch is an in-flight update_tier
                # between claim and release.
                pipe = redis.pipeline()
                for cid in batch_cids:
                    pipe.exists(f'{claim_prefix}{cid}')
                claim_flags: list[int] = (
                    await pipe.execute()
                )
                for cid, tier_str, claimed in zip(
                    batch_cids, existing, claim_flags,
                ):
                    if claimed:
                        continue
                    if tier_str is None:
                        orphan_cids.append(cid)
                        continue
                    hash_tier: int = int(tier_str)
                    if hash_tier != zset_tier:
                        move_ops.append((cid, hash_tier))
            if cursor == 0:
                break

        fixed[zset_tier] = (
            len(orphan_cids) + len(move_ops)
        )

        if not apply:
            continue

        if orphan_cids:
            for i in range(0, len(orphan_cids), 1000):
                chunk: list[str] = orphan_cids[i:i + 1000]
                mapping: dict[str, str] = {
                    cid: str(zset_tier) for cid in chunk
                }
                await redis.hset(
                    tiers_key, mapping=mapping,
                )
        if move_ops:
            pipe = redis.pipeline()
            for cid, hash_tier in move_ops:
                dest_key: str = (
                    f'rss:{platform}:queue:{hash_tier}'
                )
                pipe.zrem(queue_key, cid)
                pipe.zadd(dest_key, {cid: now})
            await pipe.execute()
    return fixed


async def _count_hash_orphans(
    redis: aioredis.Redis, platform: str,
    tiers: list[TierConfig],
) -> int:
    '''
    Count tier-hash entries whose cid is in no tier zset
    and is not currently claimed. Mirrors the filter in
    ``_LUA_RECOVER_ORPHANS`` but as a read-only Python
    pass, for dry-run accuracy. EXISTS checks are
    pipelined per chunk to keep runtime linear in the
    number of chunks rather than the number of orphans.
    '''
    tiers_key: str = f'rss:{platform}:tiers'
    claim_prefix: str = f'rss:{platform}:claim:'
    orphans: int = 0
    cursor: int = 0
    while True:
        cursor, chunk = await redis.hscan(
            tiers_key, cursor, count=5000,
        )
        if chunk:
            cids: list[str] = list(chunk.keys())
            in_any: set[str] = set()
            for tc in tiers:
                qk: str = (
                    f'rss:{platform}:queue:{tc.tier}'
                )
                scores: list[float | None] = (
                    await redis.zmscore(qk, cids)
                )
                for cid, sc in zip(cids, scores):
                    if sc is not None:
                        in_any.add(cid)
            candidates: list[str] = [
                cid for cid in cids if cid not in in_any
            ]
            if candidates:
                pipe = redis.pipeline()
                for cid in candidates:
                    pipe.exists(f'{claim_prefix}{cid}')
                exist_results: list[int] = (
                    await pipe.execute()
                )
                orphans += sum(
                    1 for r in exist_results if not r
                )
        if cursor == 0:
            break
    return orphans


async def main() -> None:
    settings: ReconcileSettings = ReconcileSettings()
    if not settings.redis_dsn:
        print(
            'Error: REDIS_DSN must be set',
            file=sys.stderr,
        )
        sys.exit(1)

    tiers: list[TierConfig] = parse_priority_queues(
        settings.priority_queues,
    )
    queue: RedisCreatorQueue = RedisCreatorQueue(
        settings.redis_dsn,
        worker_id='reconcile',
        platform=settings.platform,
    )

    # Direct Redis handle for the diagnostic counts and
    # the zset-orphan fix (uses lower-level commands than
    # the queue abstraction exposes).
    redis: aioredis.Redis = aioredis.from_url(
        settings.redis_dsn, decode_responses=True,
    )

    hash_before: dict[int, int] = await _hash_tier_counts(
        redis, settings.platform,
    )
    zset_before: dict[int, int] = await _count_per_tier(
        redis, settings.platform, tiers,
    )

    print(
        f'mode: {"APPLY" if settings.apply else "dry-run"}'
    )
    print()
    print('before:')
    print(
        f'{"tier":>4} {"hash":>10} {"zset":>10} {"gap":>10}'
    )
    for tc in tiers:
        h: int = hash_before.get(tc.tier, 0)
        z: int = zset_before.get(tc.tier, 0)
        print(f'{tc.tier:>4} {h:>10} {z:>10} {h - z:>10}')

    # Hash orphans: cid in hash, in no zset. Lua script.
    queue._tiers = tiers
    queue._key_queues = queue._build_queue_keys(tiers)
    if settings.apply:
        recovered: int = (
            await queue.cleanup_stale_claims()
        )
    else:
        recovered = await _count_hash_orphans(
            redis, settings.platform, tiers,
        )

    # Tier-mismatch fixes (queue:N ↔ hash tier M) and true
    # zset orphans (queue:N, no hash entry).
    tier_mismatches_fixed: dict[int, int] = (
        await _fix_tier_mismatches(
            redis, settings.platform,
            tiers, settings.apply,
        )
    )

    print()
    action: str = 'recovered' if settings.apply else 'would recover'
    print(f'hash orphans {action}: {recovered}')
    action_zset: str = (
        'fixed' if settings.apply else 'would fix'
    )
    print(
        f'tier mismatches {action_zset} per zset tier:'
    )
    for tc in tiers:
        n: int = tier_mismatches_fixed.get(tc.tier, 0)
        print(f'{tc.tier:>4} {n:>10}')

    if settings.apply:
        hash_after: dict[int, int] = (
            await _hash_tier_counts(
                redis, settings.platform,
            )
        )
        zset_after: dict[int, int] = (
            await _count_per_tier(
                redis, settings.platform, tiers,
            )
        )
        print()
        print('after:')
        print(
            f'{"tier":>4} {"hash":>10} '
            f'{"zset":>10} {"gap":>10}'
        )
        for tc in tiers:
            h: int = hash_after.get(tc.tier, 0)
            z: int = zset_after.get(tc.tier, 0)
            print(
                f'{tc.tier:>4} {h:>10} {z:>10} '
                f'{h - z:>10}'
            )

    await redis.aclose()


if __name__ == '__main__':
    asyncio.run(main())
