'''
Rate limiter for Instagram scrapers.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import os

from enum import Enum

from scrape_exchange.rate_limiter import (
    RateLimiter,
    _BucketConfig,
)


class InstagramCallType(str, Enum):
    '''Discriminator for Instagram rate-limit buckets.'''

    CREATOR = 'creator'
    BOOTSTRAP = 'bootstrap'


def _env_rpm(name: str, default: float, *fallbacks: str) -> float:
    for candidate in (name, *fallbacks):
        raw: str | None = os.environ.get(candidate)
        if raw is not None and raw != '':
            return float(raw)
    return default


_DEFAULT_CREATOR_RPM: float = 1.0
_DEFAULT_BOOTSTRAP_RPM: float = 2.0


class InstagramRateLimiter(RateLimiter[InstagramCallType]):
    '''Async-safe per-proxy limiter for Instagram browser fetches.'''

    def __init__(
        self,
        state_dir: str | None = None,
        redis_dsn: str | None = None,
    ) -> None:
        super().__init__(
            'instagram',
            state_dir=state_dir,
            redis_dsn=redis_dsn,
        )

    @property
    def default_configs(
        self,
    ) -> dict[InstagramCallType, _BucketConfig]:
        creator_rpm: float = _env_rpm(
            'IG_CREATOR_RPM', _DEFAULT_CREATOR_RPM,
            'IG_GLOBAL_RPM',
        )
        bootstrap_rpm: float = _env_rpm(
            'IG_BOOTSTRAP_RPM', _DEFAULT_BOOTSTRAP_RPM,
            'IG_GLOBAL_RPM',
        )
        return {
            InstagramCallType.CREATOR: _BucketConfig(
                burst=1,
                refill_rate=creator_rpm / 60,
                jitter_min=1.0,
                jitter_max=5.0,
            ),
            InstagramCallType.BOOTSTRAP: _BucketConfig(
                burst=2,
                refill_rate=bootstrap_rpm / 60,
                jitter_min=1.0,
                jitter_max=4.0,
            ),
        }

    @property
    def global_config(self) -> _BucketConfig:
        return _BucketConfig(
            burst=10_000,
            refill_rate=10_000 / 60,
            jitter_min=0.0,
            jitter_max=0.0,
        )
