'''
Rate limiter for all Scrape.Exchange API traffic.

Provides :class:`ScrapeExchangeRateLimiter`, a concrete subclass of
:class:`RateLimiter` that manages token-bucket rate limiting for
GET and POST calls to the Scrape.Exchange API.  Unlike the YouTube
rate limiter (which is per-proxy), this limiter runs a single
bucket set (proxy=``None``) because all API traffic goes to one
server regardless of which proxy was used for the upstream scrape.

When ``RATE_LIMITER_STATE_DIR`` is set, the shared-file backend
coordinates across every scraper process on the host so the
aggregate API rate stays within the configured ceiling.

On HTTP 429, callers should invoke :meth:`penalise` to drain
tokens proportionally to the ``Retry-After`` header (or a default
penalty), forcing all concurrent tasks to back off.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import logging

from enum import Enum
from typing import ClassVar

from scrape_exchange.rate_limiter import RateLimiter, _BucketConfig

_LOGGER: logging.Logger = logging.getLogger(__name__)

# Default penalty applied when the API returns 429 and no
# Retry-After header is present, in seconds.
DEFAULT_429_PENALTY_SECONDS: float = 10.0


class ScrapeExchangeCallType(str, Enum):
    '''Discriminator for Scrape.Exchange API call families.'''
    GET = 'get'
    POST = 'post'


# Fleet-wide rate ceilings (shared across all workers via
# the Redis or shared-file backend).
#
# POST burst/refill_rate can be overridden at init time
# via the ``post_rate`` parameter.
_DEFAULT_CONFIGS: dict[ScrapeExchangeCallType, _BucketConfig] = {
    ScrapeExchangeCallType.GET: _BucketConfig(
        burst=40,
        refill_rate=400.0,
        jitter_min=0.0,
        jitter_max=0.0,
    ),
    ScrapeExchangeCallType.POST: _BucketConfig(
        burst=16,
        refill_rate=80.0,
        jitter_min=0.0,
        jitter_max=0.0,
    ),
}

_GLOBAL_CONFIG: _BucketConfig = _BucketConfig(
    burst=50,
    refill_rate=500.0,
    jitter_min=0.0,
    jitter_max=0.0,
)


class ScrapeExchangeRateLimiter(RateLimiter[ScrapeExchangeCallType]):
    '''
    Async-safe, singleton rate limiter for Scrape.Exchange API
    traffic.

    The POST bucket's ``burst`` and ``refill_rate`` are configured
    at instantiation time so each scraper tool can set them to
    ``num_processes * concurrency``.  GET limits are generous by
    default (100/s) and rarely need tuning.

    Usage::

        limiter = ScrapeExchangeRateLimiter.get(
            state_dir='/tmp/scrape_exchange',
            post_rate=3,
        )
        await limiter.acquire(ScrapeExchangeCallType.POST)
    '''

    _instance: ClassVar[
        'ScrapeExchangeRateLimiter | None'
    ] = None

    def __init__(
        self,
        state_dir: str | None = None,
        post_rate: float | None = None,
        redis_dsn: str | None = None,
    ) -> None:
        # Build the configs before calling super().__init__,
        # which reads self.default_configs / self.global_config
        # during backend construction.
        self._configs: dict[
            ScrapeExchangeCallType, _BucketConfig
        ] = dict(_DEFAULT_CONFIGS)
        self._global: _BucketConfig = _GLOBAL_CONFIG

        if post_rate is not None and post_rate > 0:
            # Cap at fleet-wide POST ceiling.
            capped_rate: float = min(
                post_rate,
                _DEFAULT_CONFIGS[
                    ScrapeExchangeCallType.POST
                ].refill_rate,
            )
            capped_burst: int = min(
                max(1, int(post_rate * 2)),
                _DEFAULT_CONFIGS[
                    ScrapeExchangeCallType.POST
                ].burst,
            )
            self._configs[ScrapeExchangeCallType.POST] = (
                _BucketConfig(
                    burst=capped_burst,
                    refill_rate=capped_rate,
                    jitter_min=0.0,
                    jitter_max=0.0,
                )
            )
            _LOGGER.info(
                'ScrapeExchangeRateLimiter POST rate',
                extra={
                    'requested_rate': post_rate,
                    'capped_rate': capped_rate,
                    'capped_burst': capped_burst,
                },
            )

        super().__init__(
            'scrape_exchange',
            state_dir=state_dir,
            redis_dsn=redis_dsn,
        )

    @property
    def default_configs(
        self,
    ) -> dict[ScrapeExchangeCallType, _BucketConfig]:
        return self._configs

    @property
    def global_config(self) -> _BucketConfig:
        return self._global

    @classmethod
    def get(
        cls,
        state_dir: str | None = None,
        post_rate: float | None = None,
        redis_dsn: str | None = None,
    ) -> 'ScrapeExchangeRateLimiter':
        '''
        Return the process-wide singleton, creating it on
        first call.  ``state_dir``, ``post_rate``, and
        ``redis_dsn`` are only honoured on the very first
        call; subsequent calls return the existing instance.
        '''
        if cls._instance is None:
            cls._instance = cls(
                state_dir=state_dir,
                post_rate=post_rate,
                redis_dsn=redis_dsn,
            )
        return cls._instance
