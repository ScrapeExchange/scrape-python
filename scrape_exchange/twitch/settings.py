'''Credential-free scraper configuration via environment and CLI.'''

from typing import Any, Self

from pydantic import AliasChoices, Field, field_validator, model_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

from scrape_exchange.creator_queue import parse_priority_queues
from scrape_exchange.proxy_loader import ProxyCatalog, set_active_catalog
from scrape_exchange.settings import ScraperSettings, normalize_log_level


class TwitchScraperSettings(ScraperSettings):
    model_config = SettingsConfigDict(env_prefix='TWITCH_')

    username: str | None = Field(
        default=None,
        description='CLI only: scrape one profile and exit; no queue.',
    )
    creator_data_directory: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'TWITCH_CREATOR_DATA_DIR', 'creator_data_directory',
        ),
        description='Directory for compressed creator records.',
    )
    creator_num_processes: int = Field(default=1, ge=1)
    creator_concurrency: int = Field(
        default=1, ge=1, description='Total async tasks across processes.',
    )
    creator_disable_proxies: bool = False
    creator_priority_queues: str = '24:1000000,72:100000,168:10000,336:0'
    creator_claim_ttl_seconds: int = Field(default=300, ge=1)
    creator_queue_idle_poll_seconds: float = Field(default=30, gt=0)
    creator_orphan_recovery_interval_seconds: float = Field(
        default=300, gt=0,
    )
    creator_retry_interval_seconds: float = Field(default=300, gt=0)
    creator_bot_cooldown_seconds: float = Field(default=1800, gt=0)
    creator_bot_cooldown_max_seconds: float = Field(default=7200, gt=0)
    creator_profile_timeout_seconds: float = Field(default=60, gt=0)
    creator_data_wait_seconds: float = Field(default=15, gt=0)
    session_bootstrap_timeout_seconds: float = Field(default=90, gt=0)
    creator_rpm: float = Field(default=2, gt=0)
    data_rpm: float = Field(default=60, gt=0)
    bootstrap_rpm: float = Field(default=2, gt=0)
    metrics_port: int = Field(
        default=9910, ge=0, le=65535,
        validation_alias=AliasChoices(
            'TWITCH_CREATOR_METRICS_PORT', 'metrics_port',
        ),
    )
    creator_log_file: str = '/dev/stdout'
    creator_log_level: str = 'INFO'

    @field_validator('creator_log_level', mode='before')
    @classmethod
    def validate_log_level(cls, value: str) -> str:
        return normalize_log_level(value)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple:
        # CLI arguments are a separate source inserted by pydantic-settings.
        def environment() -> dict[str, Any]:
            values: dict[str, Any] = env_settings()
            values.pop('username', None)
            return values

        def dotenv() -> dict[str, Any]:
            values: dict[str, Any] = dotenv_settings()
            values.pop('username', None)
            return values

        def secrets() -> dict[str, Any]:
            values: dict[str, Any] = file_secret_settings()
            values.pop('username', None)
            return values

        return init_settings, environment, dotenv, secrets

    @model_validator(mode='after')
    def validate_limits(self) -> Self:
        if self.creator_claim_ttl_seconds <= (
            self.creator_profile_timeout_seconds + 60
        ):
            raise ValueError('Claim TTL must exceed scrape timeout by 60s')
        if self.creator_bot_cooldown_max_seconds < (
            self.creator_bot_cooldown_seconds
        ):
            raise ValueError('Maximum cooldown must exceed initial cooldown')
        parse_priority_queues(self.creator_priority_queues)
        return self

    @model_validator(mode='after')
    def _load_proxy_catalog(self) -> Self:
        if self.creator_disable_proxies:
            object.__setattr__(self, 'proxies', [])
            set_active_catalog(ProxyCatalog())
            return self
        # Pydantic binds this descriptor to a method at runtime.
        super()._load_proxy_catalog()  # ty: ignore[call-non-callable]
        return self
