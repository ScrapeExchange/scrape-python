'''Per-proxy website limits using the shared Redis/file token buckets.'''

from enum import Enum
from pathlib import Path
from urllib.parse import SplitResult, urlsplit

from scrape_exchange.onlyfans.settings import OnlyFansScraperSettings
from scrape_exchange.rate_limiter import (
    RateLimiter,
    _BucketConfig,
    _RedisBackend,
)


class OnlyFansCallType(str, Enum):
    CREATOR = 'creator'
    DATA = 'data'


class OnlyFansRateLimiter(RateLimiter[OnlyFansCallType]):
    def __init__(self, settings: OnlyFansScraperSettings) -> None:
        self.settings: OnlyFansScraperSettings = settings
        state_dir: str = (
            str(Path(settings.rate_limiter_state_dir) / 'onlyfans')
            if settings.rate_limiter_state_dir else ''
        )
        super().__init__(
            'onlyfans', redis_dsn=settings.redis_dsn,
            state_dir=state_dir,
        )

    def _global_labels(self, proxy: str | None) -> dict[str, str]:
        labels: dict[str, str] = super()._global_labels(proxy)
        if proxy:
            parsed: SplitResult = urlsplit(proxy)
            labels['proxy'] = f'{parsed.hostname}:{parsed.port}'
        return labels

    def _labels(
        self, call_type: OnlyFansCallType, proxy: str | None,
    ) -> dict[str, str]:
        return {**self._global_labels(proxy), 'api': call_type.value}

    async def aclose(self) -> None:
        if isinstance(self._backend, _RedisBackend):
            await self._backend._redis.aclose()

    @property
    def default_configs(self) -> dict[OnlyFansCallType, _BucketConfig]:
        return {
            OnlyFansCallType.CREATOR: _BucketConfig(
                burst=1, refill_rate=self.settings.creator_rpm / 60,
                jitter_min=0, jitter_max=1,
            ),
            OnlyFansCallType.DATA: _BucketConfig(
                burst=4, refill_rate=self.settings.data_rpm / 60,
                jitter_min=0, jitter_max=0,
            ),
        }

    @property
    def global_config(self) -> _BucketConfig:
        return _BucketConfig(
            burst=5,
            refill_rate=(self.settings.creator_rpm + self.settings.data_rpm)
            / 60,
            jitter_min=0, jitter_max=0,
        )
