'''
Centralised rate-limit arbiter for TikTok scrapers.

Per-proxy buckets:

  - ``CREATOR_API`` — TikTokApi calls made by the creator scraper
    (User.info, User.videos, reposts, liked videos). Default 6 rpm,
    overridable via ``TIKTOK_CREATOR_RPM``; falls back to
    ``TIKTOK_GLOBAL_RPM`` for backwards compatibility.
  - ``VIDEO_API`` — TikTokApi calls made by the video scraper
    (Video.info). Default 6 rpm, overridable via
    ``TIKTOK_VIDEO_RPM``; falls back to ``TIKTOK_GLOBAL_RPM``.
  - ``API`` — legacy shared TikTokApi bucket, retained for callers
    that have not yet selected a scraper-specific call type.
    Default 6 rpm, overridable via ``TIKTOK_GLOBAL_RPM``.
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


def _env_rpm(name: str, default: float, *fallbacks: str) -> float:
    for candidate in (name, *fallbacks):
        raw: str | None = os.environ.get(candidate)
        if raw is not None and raw != '':
            return float(raw)
    return default


# Conservative defaults for first ramp; tune from Grafana
# once live.
_DEFAULT_API_RPM: float = 6.0
_DEFAULT_BOOTSTRAP_RPM: float = 6.0


class TikTokRateLimiter(RateLimiter[TikTokCallType]):
    '''
    Async-safe rate limiter for TikTok scrapers.

    Usage::

        limiter = TikTokRateLimiter()
        limiter.set_proxies(proxies)
        await limiter.acquire(
            TikTokCallType.CREATOR_API, proxy='http://p:8080',
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
        creator_rpm: float = _env_rpm(
            'TIKTOK_CREATOR_RPM', _DEFAULT_API_RPM,
            'TIKTOK_GLOBAL_RPM',
        )
        video_rpm: float = _env_rpm(
            'TIKTOK_VIDEO_RPM', _DEFAULT_API_RPM,
            'TIKTOK_GLOBAL_RPM',
        )
        bootstrap_rpm: float = _env_rpm(
            'TIKTOK_BOOTSTRAP_RPM', _DEFAULT_BOOTSTRAP_RPM,
        )
        return {
            TikTokCallType.API: _BucketConfig(
                burst=2,
                refill_rate=api_rpm / 60,
                jitter_min=1.0,
                jitter_max=4.0,
            ),
            TikTokCallType.CREATOR_API: _BucketConfig(
                burst=2,
                refill_rate=creator_rpm / 60,
                jitter_min=1.0,
                jitter_max=4.0,
            ),
            TikTokCallType.VIDEO_API: _BucketConfig(
                burst=2,
                refill_rate=video_rpm / 60,
                jitter_min=1.0,
                jitter_max=4.0,
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
