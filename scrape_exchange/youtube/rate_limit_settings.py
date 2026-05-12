'''Per-call-type rate-limit refill rates for the YouTube
rate limiter.

Each :class:`YouTubeCallType` has its own token-bucket
refill rate, expressed here in requests per minute for
readability (the rate limiter consumes req/s). Burst caps
are intentionally NOT tunable from settings — they were
deliberately lowered to 2 for PLAYER and RSS after the
2026-05-09 SYN-flood diagnosis (commit 21aa54a) and
raising them again would re-introduce the simultaneous-
CONNECT pile-up that caused the original timeout_connect
storm.

Defaults match the values that ship in production after
the 2026-05-12 rate-limit review:

* BROWSE  — 150/min (slack; reuse unchanged)
* PLAYER  — 30/min  (raised from 10/min — was saturated,
  9.5s p95 sleep, draining the keep-alive pool between
  ``/player`` calls)
* NEXT    — 150/min (slack)
* HTML    — 9/min   (intentional anti-bot ceiling)
* RSS     — 30/min  (raised from 9.6/min — was saturated,
  3.6s p95 sleep)

Override any of them in production via the matching env var
(``PLAYER_REFILL_PER_MIN`` etc.) without a code change. The
limiter reads :data:`YT_RATE_LIMITS` once at module import
so a config change requires a restart.
'''

from pathlib import Path

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class YouTubeRateLimitSettings(BaseSettings):
    '''Per-call-type token-bucket refill rates in
    requests per minute. Loaded once at module import.'''

    model_config = SettingsConfigDict(
        env_file=(
            str(Path(__file__).parent.parent.parent / '.env'),
        ),
        env_file_encoding='utf-8',
        extra='ignore',
    )

    browse_refill_per_min: float = Field(
        default=150.0,
        validation_alias=AliasChoices(
            'BROWSE_REFILL_PER_MIN',
            'browse_refill_per_min',
        ),
        description=(
            'Token-bucket refill rate for InnerTube '
            'browse calls, in requests per minute per '
            'proxy. Default 150/min.'
        ),
    )
    player_refill_per_min: float = Field(
        default=30.0,
        validation_alias=AliasChoices(
            'PLAYER_REFILL_PER_MIN',
            'player_refill_per_min',
        ),
        description=(
            'Token-bucket refill rate for InnerTube '
            'player calls, in requests per minute per '
            'proxy. Default 30/min (raised from 10/min on '
            '2026-05-12 after diagnosing that the previous '
            'limit was saturating workers with 9.5s p95 '
            'sleep and draining the keep-alive pool).'
        ),
    )
    next_refill_per_min: float = Field(
        default=150.0,
        validation_alias=AliasChoices(
            'NEXT_REFILL_PER_MIN', 'next_refill_per_min',
        ),
        description=(
            'Token-bucket refill rate for InnerTube next '
            'calls, in requests per minute per proxy. '
            'Default 150/min.'
        ),
    )
    html_refill_per_min: float = Field(
        default=9.0,
        validation_alias=AliasChoices(
            'HTML_REFILL_PER_MIN', 'html_refill_per_min',
        ),
        description=(
            'Token-bucket refill rate for HTML (channel '
            'about-page) calls, in requests per minute '
            'per proxy. Default 9/min — intentionally '
            'kept very low because the HTML path triggers '
            'YouTube WAF anti-bot at higher rates.'
        ),
    )
    rss_refill_per_min: float = Field(
        default=30.0,
        validation_alias=AliasChoices(
            'RSS_REFILL_PER_MIN', 'rss_refill_per_min',
        ),
        description=(
            'Token-bucket refill rate for RSS XML fetches, '
            'in requests per minute per proxy. Default '
            '30/min (raised from 9.6/min on 2026-05-12 '
            'after diagnosing 3.6s p95 sleep was draining '
            'the keep-alive pool).'
        ),
    )


YT_RATE_LIMITS: YouTubeRateLimitSettings = (
    YouTubeRateLimitSettings()
)
