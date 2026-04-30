'''
Centralised rate-limit arbiter for TikTok scrapers.

Two per-proxy buckets:

  - ``API`` — every TikTokApi call (User.info, User.videos,
    Video.info, Hashtag.info, Hashtag.videos). Default 30 rpm,
    overridable via ``TIKTOK_GLOBAL_RPM``.
  - ``BOOTSTRAP`` — session creation and ms_token refresh.
    Default 6 rpm, overridable via ``TIKTOK_BOOTSTRAP_RPM``.

Backend selection (Redis > shared-file > in-process) inherited
from :class:`scrape_exchange.rate_limiter.RateLimiter`. Redis key
prefix is ``tiktok`` so per-proxy buckets are disjoint from
YouTube's.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import os

from scrape_exchange.rate_limiter import (
    RateLimiter,
    _BucketConfig,
)
from scrape_exchange.tiktok.tiktok_types import TikTokCallType


def _env_rpm(name: str, default: float) -> float:
    raw: str | None = os.environ.get(name)
    if raw is None or raw == '':
        return default
    return float(raw)


# Conservative defaults for first ramp; tune from Grafana
# once live.
_DEFAULT_API_RPM: float = 30.0
_DEFAULT_BOOTSTRAP_RPM: float = 6.0


class TikTokRateLimiter(RateLimiter[TikTokCallType]):
    '''
    Async-safe rate limiter for TikTok scrapers.

    Usage::

        limiter = TikTokRateLimiter()
        limiter.set_proxies(proxies)
        await limiter.acquire(
            TikTokCallType.API, proxy='http://p:8080',
        )
    '''

    def __init__(
        self, state_dir: str | None = None,
        redis_dsn: str | None = None,
    ) -> None:
        super().__init__(
            'tiktok',
            state_dir=state_dir,
            redis_dsn=redis_dsn,
        )

    @property
    def default_configs(
        self,
    ) -> dict[TikTokCallType, _BucketConfig]:
        api_rpm: float = _env_rpm(
            'TIKTOK_GLOBAL_RPM', _DEFAULT_API_RPM,
        )
        bootstrap_rpm: float = _env_rpm(
            'TIKTOK_BOOTSTRAP_RPM', _DEFAULT_BOOTSTRAP_RPM,
        )
        return {
            TikTokCallType.API: _BucketConfig(
                burst=10,
                refill_rate=api_rpm / 60,
                jitter_min=0.2,
                jitter_max=0.8,
            ),
            TikTokCallType.BOOTSTRAP: _BucketConfig(
                burst=2,
                refill_rate=bootstrap_rpm / 60,
                jitter_min=1.0,
                jitter_max=3.0,
            ),
        }

    @property
    def global_config(self) -> _BucketConfig:
        # No cross-type aggregate gate for TikTok — return a
        # no-op bucket. The per-type buckets do all the gating.
        return _BucketConfig(
            burst=10_000,
            refill_rate=10_000 / 60,
            jitter_min=0.0,
            jitter_max=0.0,
        )
