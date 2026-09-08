'''Twitch navigation and profile-request buckets shared across hosts.'''

from enum import Enum

from scrape_exchange.rate_limiter import RateLimiter, _BucketConfig
from scrape_exchange.twitch.settings import TwitchScraperSettings


class TwitchCallType(str, Enum):
    CREATOR = 'creator'
    DATA = 'data'
    BOOTSTRAP = 'bootstrap'


class TwitchRateLimiter(RateLimiter[TwitchCallType]):
    def __init__(self, settings: TwitchScraperSettings) -> None:
        self.settings: TwitchScraperSettings = settings
        super().__init__(
            'twitch', redis_dsn=settings.redis_dsn,
            state_dir=settings.rate_limiter_state_dir,
        )

    @property
    def default_configs(self) -> dict[TwitchCallType, _BucketConfig]:
        return {
            TwitchCallType.CREATOR: _BucketConfig(
                burst=1, refill_rate=self.settings.creator_rpm / 60,
                jitter_min=0, jitter_max=1,
            ),
            TwitchCallType.DATA: _BucketConfig(
                burst=4, refill_rate=self.settings.data_rpm / 60,
                jitter_min=0, jitter_max=0,
            ),
            TwitchCallType.BOOTSTRAP: _BucketConfig(
                burst=1, refill_rate=self.settings.bootstrap_rpm / 60,
                jitter_min=0, jitter_max=1,
            ),
        }

    @property
    def global_config(self) -> _BucketConfig:
        return _BucketConfig(
            burst=6,
            refill_rate=(self.settings.data_rpm + self.settings.creator_rpm
                         + self.settings.bootstrap_rpm) / 60,
            jitter_min=0, jitter_max=0,
        )
