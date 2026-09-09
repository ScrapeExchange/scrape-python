'''OnlyFans CLI/environment settings with the shared proxy loader.'''

import math
from pathlib import Path
from typing import Self

from pydantic import AliasChoices, Field, model_validator
from pydantic_settings import SettingsConfigDict

from scrape_exchange.creator_queue import TierConfig, parse_priority_queues
from scrape_exchange.settings import ScraperSettings

DEFAULT_PRIORITY_QUEUES: str = '24:1000000,72:100000,168:10000,336:0'


def parse_like_priority_queues(spec: str) -> list[TierConfig]:
    '''Parse hours:minimum_likes with a final zero-like catch-all tier.'''
    tiers: list[TierConfig] = parse_priority_queues(spec)
    if not tiers or tiers[-1].min_subscribers != 0:
        raise ValueError('Like tiers must end with an hours:0 catch-all')
    previous_count: int | None = None
    previous_hours: float = 0
    tier: TierConfig
    for tier in tiers:
        if (
            not math.isfinite(tier.interval_hours)
            or tier.interval_hours <= 0
            or tier.interval_hours < previous_hours
            or tier.min_subscribers < 0
            or (previous_count is not None
                and tier.min_subscribers >= previous_count)
        ):
            raise ValueError(
                'Use positive, increasing hours and descending like counts',
            )
        previous_count = tier.min_subscribers
        previous_hours = tier.interval_hours
    return tiers


class OnlyFansScraperSettings(ScraperSettings):
    model_config = SettingsConfigDict(env_prefix='ONLYFANS_')

    log_level: str = Field(
        default='INFO',
        validation_alias=AliasChoices(
            'ONLYFANS_CREATOR_LOG_LEVEL', 'ONLYFANS_LOG_LEVEL',
            'LOG_LEVEL', 'log_level',
        ),
        description='Logging level for the OnlyFans creator scraper.',
    )
    log_file: str = Field(
        default='/dev/stdout',
        validation_alias=AliasChoices(
            'ONLYFANS_CREATOR_LOG_FILE', 'ONLYFANS_LOG_FILE',
            'LOG_FILE', 'log_file',
        ),
        description='Log destination; defaults to container stdout.',
    )

    username: str | None = Field(
        default=None, description='One public creator username or @handle.',
    )
    creator_file: Path | None = Field(
        default=None,
        description='File with one username or profile URL per line.',
    )
    creator_data_directory: Path = Field(
        default=Path('data/onlyfans/creators'),
        validation_alias=AliasChoices(
            'ONLYFANS_CREATOR_DATA_DIR', 'creator_data_directory',
        ),
        description='Directory for compressed creator metadata.',
    )
    concurrency: int = Field(
        default=1, ge=1,
        description='Maximum async tasks; at most one task per proxy.',
    )
    creator_rpm: float = Field(
        default=2, gt=0, allow_inf_nan=False,
        description='Profile navigations per minute per proxy.',
    )
    data_rpm: float = Field(
        default=30, gt=0, allow_inf_nan=False,
        description='Website API requests per minute per proxy.',
    )
    profile_timeout_seconds: float = Field(
        default=90, gt=0, allow_inf_nan=False,
        description='Deadline including rate-limit waits for one profile.',
    )
    browser_timeout_seconds: float = Field(
        default=90, gt=0, allow_inf_nan=False,
    )
    blocked_cooldown_seconds: float = Field(
        default=300, gt=0, allow_inf_nan=False,
        description='Shared per-proxy penalty after a block or challenge.',
    )
    metrics_port: int | None = Field(default=None, ge=1, le=65535)
    creator_priority_queues: str = Field(
        default=DEFAULT_PRIORITY_QUEUES,
        description='Refresh hours:minimum_likes, ending with hours:0.',
    )
    creator_claim_ttl_seconds: int = Field(
        default=300, ge=1,
        description='Redis claim lifetime, including browser startup.',
    )
    creator_retry_interval_seconds: float = Field(
        default=300, gt=0, allow_inf_nan=False,
        description='Delay before retrying a failed queued creator.',
    )

    @model_validator(mode='after')
    def validate_browser_proxies(self) -> Self:
        if any(proxy.startswith('local://') for proxy in self.proxies):
            raise ValueError(
                'Native source-address entries are unsupported by the '
                'browser; configure HTTP(S) proxies in PROXY_FILES',
            )
        return self

    @model_validator(mode='after')
    def validate_queue(self) -> Self:
        parse_like_priority_queues(self.creator_priority_queues)
        if self.creator_claim_ttl_seconds <= (
            self.browser_timeout_seconds + self.profile_timeout_seconds + 60
        ):
            raise ValueError(
                'Claim lifetime must exceed browser and profile deadlines '
                'by more than 60 seconds',
            )
        return self
