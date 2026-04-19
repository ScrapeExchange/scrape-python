# RSS eligibility fraction & SLA threshold

Date: 2026-04-19

## Problem

Two related issues with the YouTube RSS scraper:

1. **Stale RSS feeds.** A channel only re-enters the per-tier
   queue exactly `interval_hours` after its last release. There is no
   headroom: if any worker is briefly slow, the next fetch slips past
   the target. Channels could often be refreshed sooner without
   exceeding the per-IP rate limits.

2. **`RSS tier SLA (on-time vs overdue)` Grafana panel reads 0%.**
   The recorder in `tools/yt_rss_scrape.py:_record_tier_sla`
   classifies a fetch as "on-time" only when
   `now <= scheduled_time`. But `scheduled_time` is the queue's
   z-set score, and the Lua claim script (Redis) and the
   in-process heap claimer (file backend) only return entries where
   `score <= now`. `process_now` is captured *after* `claim_batch`
   returns, so by construction `process_now >= scheduled_time`. The
   on-time branch is dead code; every fetch increments the overdue
   counter, and `on_time / (on_time + overdue) = 0 / N = 0%`.

Both issues are solved by the same change: introduce headroom
between when a channel becomes *eligible* and when its SLA window
*expires*.

## Design

### Behaviour

- A channel becomes eligible for the next RSS fetch once
  `eligibility_fraction × interval` has elapsed since its last
  release. Default: `0.5` — a channel re-enters the queue once half
  its tier interval has elapsed.
- "On-time" means processed within the tier's full target interval.
  "Overdue" means processed after the full interval has elapsed.

With `eligibility_fraction = 0.5` the system aims to fetch each
channel halfway through its target interval, leaving the second
half as headroom. As long as the actual fetch lands anywhere in
the second half (i.e. before `last_run + interval`), it counts as
on-time.

### New setting

In `RssSettings` (`tools/yt_rss_scrape.py`):

```python
eligibility_fraction: float = Field(
    default=0.5,
    validation_alias=AliasChoices(
        'RSS_ELIGIBILITY_FRACTION', 'eligibility_fraction'
    ),
    description=(
        'Fraction of the tier interval after which a channel '
        'becomes eligible to be RSS-fetched again. 0.5 means a '
        'channel re-enters the queue once half its tier interval '
        'has elapsed since the last run.'
    ),
)
```

### Queue constructor

Both `FileCreatorQueue` and `RedisCreatorQueue` accept:

```python
eligibility_fraction: float = 1.0
```

Default is `1.0` to preserve existing behaviour for any other
caller (tests, ad-hoc scripts) that constructs a queue directly.
Stored as `self._eligibility_fraction`.

`_run_worker` in `tools/yt_rss_scrape.py` passes
`settings.eligibility_fraction` when constructing the queue.

### `release()`

In both backends, change:

```python
next_check: float = now + tc.interval_hours * 3600
```

to:

```python
next_check: float = (
    now
    + tc.interval_hours * 3600 * self._eligibility_fraction
)
```

### Expose tier interval

Add a public `get_tier_interval(tier) -> float` (returns hours) on
both `FileCreatorQueue` and `RedisCreatorQueue` (and as an
abstract method on the `CreatorQueue` base) so the SLA recorder
can compute the deadline.

Underlying implementations already exist:
- `FileCreatorQueue` has `_tier_config(tier)` returning
  `TierConfig` (whose `.interval_hours` is what we need).
- `RedisCreatorQueue` has `_tier_interval(tier)` returning hours
  directly.

The new public method just wraps these.

### SLA recorder

In `tools/yt_rss_scrape.py:_record_tier_sla`:

```python
def _record_tier_sla(
    tier: int,
    scheduled_time: float,
    now: float,
    interval_seconds: float,
    eligibility_fraction: float,
) -> None:
    deadline: float = (
        scheduled_time
        + (1.0 - eligibility_fraction) * interval_seconds
    )
    wid: str = get_worker_id()
    if now <= deadline:
        METRIC_TIER_ON_TIME.labels(
            tier=str(tier), worker_id=wid,
        ).inc()
    else:
        METRIC_TIER_OVERDUE.labels(
            tier=str(tier), worker_id=wid,
        ).inc()
```

With `eligibility_fraction = 0.5`, `deadline` simplifies to
`last_run + interval` — channels processed within their target
interval count as on-time.

At `eligibility_fraction = 1.0`, `deadline` collapses to the score
itself, so the SLA stays at 0%. This is the current behaviour and
is mathematically correct: with no scheduling headroom, no fetch
can be on-time. The setting introduces the headroom that makes
the SLA achievable.

Call site at `tools/yt_rss_scrape.py:1356`:

```python
interval_seconds: float = (
    creator_queue.get_tier_interval(tier) * 3600
)
_record_tier_sla(
    tier, sched, process_now,
    interval_seconds,
    settings.eligibility_fraction,
)
```

### Grafana

No JSON change required. The panel formula
`on_time / (on_time + overdue)` is correct; only the inputs change.

### Migration

None. Existing Redis scores drain naturally as channels are
released. The highest-priority (shortest-interval) tiers reach
steady state within a couple of hours; the longest tier within
roughly one full interval.

## Testing

Unit tests:

- `tests/unit/test_rss_queue.py`: `release()` with
  `eligibility_fraction=0.5` schedules `next_check` at half the
  tier interval. Default constructor (no fraction passed) keeps
  `1.0` behaviour.
- `tests/integration/test_rss_queue_redis.py`: same coverage for
  the Redis backend; verify the z-set score after a release.
- New unit test for `_record_tier_sla` covering:
  - on-time at `deadline - epsilon`,
  - overdue at `deadline + epsilon`,
  - `eligibility_fraction = 1.0` always overdue (matches today),
  - `eligibility_fraction = 0.5` on-time at `last_run + interval - epsilon`.

Existing tests should pass unchanged because the queue default
remains `1.0`.

## Rollout

1. Merge with code default `eligibility_fraction = 0.5` on
   `RssSettings`.
2. Deploy to homeserver / homedata. No `.env` change required —
   the new default takes effect immediately.
3. Watch the `RSS tier SLA (on-time vs overdue)` panel. Tier 1
   should show non-zero on-time within minutes. Longest tier
   reaches steady state within roughly one full interval.
4. If 0.5 turns out to be too aggressive (rate-limit failures
   climb) or too conservative (RSS feeds still go stale), tune
   `RSS_ELIGIBILITY_FRACTION` in `.env` without redeploying.
