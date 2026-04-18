'''
Class leveraging pydantic-settings to manage configuration for the
scrape_exchange tools.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

from pathlib import Path
from typing import Literal
from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


_VALID_LOG_LEVELS: set[str] = {
    'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL',
}


def normalize_log_level(v: str) -> str:
    '''
    Validator helper for any ``*_log_level`` pydantic field. Normalises
    case and asserts the value is one of the accepted Python logging
    levels. Raises :class:`ValueError` with a helpful message on an
    unknown level.
    '''
    upper: str = v.upper() if isinstance(v, str) else v
    if upper not in _VALID_LOG_LEVELS:
        raise ValueError(
            f'log_level must be one of {sorted(_VALID_LOG_LEVELS)}, '
            f'got {v!r}'
        )
    return upper


class ScraperSettings(BaseSettings):
    '''
    Tool configuration loaded in priority order:
    CLI flags > environment variables > .env file > built-in defaults.
    '''

    model_config = SettingsConfigDict(
        env_file=(
            str(Path(__file__).parent.parent / '.env'),
            '.env',
        ),
        env_file_encoding='utf-8',
        cli_parse_args=True,
        cli_kebab_case=True,
        populate_by_name=True,
        extra='ignore',
    )

    exchange_url: str = Field(
        default='https://scrape.exchange',
        validation_alias=AliasChoices(
            'EXCHANGE_URL', 'exchange_url'
        ),
        description='Base URL for the Scrape.Exchange API',
    )
    api_key_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices('API_KEY_ID', 'api_key_id'),
        description='API key ID for authenticating with the Scrape.Exchange API',       # noqa: E501
    )
    api_key_secret: str | None = Field(
        default=None,
        validation_alias=AliasChoices('API_KEY_SECRET', 'api_key_secret'),
        description='API key secret for authenticating with the Scrape.Exchange API',   # noqa: E501
    )
    log_level: str = Field(
        default='INFO',
        validation_alias=AliasChoices('LOG_LEVEL', 'log_level'),
        description='Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)',
    )
    log_file: str = Field(
        default='/dev/stdout',
        validation_alias=AliasChoices('LOG_FILE', 'log_file'),
        description='Log file path',
    )
    log_format: Literal['json', 'text'] = Field(
        default='json',
        validation_alias=AliasChoices('LOG_FORMAT', 'log_format'),
        description='Log record format: "json" (structured) or "text"',
    )
    proxies: str | None = Field(
        default=None,
        validation_alias=AliasChoices('PROXIES', 'proxies'),
        description=(
            'Comma-separated list of proxy URLs to use for scraping (e.g. '
            '"http://proxy1:port,http://proxy2:port"). If not set, no '
            'proxy will be used.'
        )
    )
    rate_limiter_state_dir: str = Field(
        default='/tmp/scrape_exchange',
        validation_alias=AliasChoices(
            'RATE_LIMITER_STATE_DIR', 'rate_limiter_state_dir'
        ),
        description=(
            'Directory for cross-process rate-limiter state. When set '
            'to a non-empty path, token buckets are persisted per '
            'proxy in that directory and every process on this host '
            'that shares the proxy pool converges on one view of per-'
            'proxy rate limits, regardless of which scrape tool they '
            'belong to. Must be on a local filesystem (not NFS/CIFS/'
            'SSHFS) — the rate limiter will fail-fast on init '
            'otherwise. Set to an empty string to disable shared '
            'state and fall back to per-process buckets (useful in '
            'tests).'
        ),
    )

    worker_id: str = Field(
        default='0',
        validation_alias=AliasChoices(
            'WORKER_ID', 'worker_id'
        ),
        description=(
            'Unique identifier for this worker process, '
            'used as a Prometheus metric label to '
            'distinguish workers whose instance labels '
            'collide. Set automatically by the supervisor.'
        ),
    )

    redis_dsn: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'REDIS_DSN', 'redis_dsn'
        ),
        description=(
            'Redis connection string for cross-host '
            'rate-limiter coordination. When set, the '
            'Redis backend is preferred over the shared-'
            'file backend. Example: '
            'redis://localhost:6379/0'
        ),
    )

    @field_validator('log_level', mode='before')
    @classmethod
    def uppercase_log_level(cls, v: str) -> str:
        return normalize_log_level(v)
