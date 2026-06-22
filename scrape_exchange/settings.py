'''
Class leveraging pydantic-settings to manage configuration for the
scrape_exchange tools.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

from pathlib import Path
from typing import ClassVar, Literal
from pydantic import (
    AliasChoices, Field, field_validator, model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict
from scrape_exchange.proxy_loader import (
    load_proxy_catalog, load_proxy_entries, set_active_catalog,
)


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
    proxy_files: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'PROXY_FILES', 'proxy_files',
        ),
        description=(
            'Comma-separated list of files; each line is a proxy '
            'URL (http://host:port[:user:pass] or '
            'http://user:pass@host:port) or a local egress IP '
            '(local://x.x.x.x). Replaces the legacy PROXIES env '
            'var.'
        ),
    )
    proxies_env: str | None = Field(
        default=None,
        exclude=True,
        validation_alias=AliasChoices(
            'PROXIES', 'proxies_env',
        ),
        description=(
            'Legacy comma-separated proxy entries. Used by scraper '
            'supervisor child processes after splitting PROXY_FILES.'
        ),
    )
    # ClassVar: not a pydantic/env field. Instance value is
    # set by _load_proxy_catalog, shadowing the class default.
    proxies: ClassVar[tuple[str, ...]] = ()
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

    watchdog_enabled: bool = Field(
        default=True,
        validation_alias=AliasChoices(
            'WATCHDOG_ENABLED', 'watchdog_enabled',
        ),
        description=(
            'Enable the liveness watchdog. When on, a daemon thread '
            'terminates the worker (os._exit(1), so the supervisor '
            'respawns it) if the event loop or all workers stop making '
            'progress. Set false to disable in environments where the '
            'supervisor/container will not restart the process.'
        ),
    )
    watchdog_loop_timeout_seconds: float = Field(
        default=60.0,
        validation_alias=AliasChoices(
            'WATCHDOG_LOOP_TIMEOUT_SECONDS',
            'watchdog_loop_timeout_seconds',
        ),
        description=(
            'Seconds the async heartbeat may go un-touched before the '
            'watchdog treats the event loop as frozen and terminates '
            'the process. Tighter than the work timeout because a '
            'frozen loop is the more urgent failure.'
        ),
    )
    watchdog_work_timeout_seconds: float = Field(
        default=180.0,
        validation_alias=AliasChoices(
            'WATCHDOG_WORK_TIMEOUT_SECONDS',
            'watchdog_work_timeout_seconds',
        ),
        description=(
            'Seconds with no forward worker progress before the '
            'watchdog terminates the process. Must exceed any '
            'legitimate global quiet period; intentional long sleeps '
            '(e.g. an open RSS circuit) chunk-and-touch the watchdog so '
            'they do not look like a hang.'
        ),
    )
    innertube_executor_threads: int = Field(
        default=16,
        validation_alias=AliasChoices(
            'INNERTUBE_EXECUTOR_THREADS', 'innertube_executor_threads',
        ),
        description=(
            'Thread-pool size for the dedicated executor that runs the '
            'synchronous InnerTube player/next/browse calls. Isolated '
            'from the default executor so a wedged InnerTube call cannot '
            'starve cookie-file/brotli to_thread work. Also caps '
            'per-process InnerTube concurrency (throughput is roughly '
            'this / p50 scrape seconds). Each thread costs ~8 MB of '
            'stack; keep well within the 1 GB/tool budget.'
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

    @field_validator('proxy_files', mode='before')
    @classmethod
    def _coerce_proxy_files(cls, v: object) -> object:
        if v is None or isinstance(v, str):
            return v
        if isinstance(v, Path):
            return str(v)
        if isinstance(v, (list, tuple)):
            return ','.join(str(p) for p in v)
        return v

    @model_validator(mode='after')
    def _load_proxy_catalog(self) -> 'ScraperSettings':
        paths: list[Path] = []
        if self.proxy_files:
            paths = [
                Path(p.strip())
                for p in self.proxy_files.split(',')
                if p.strip()
            ]
        if paths:
            catalog = load_proxy_catalog(paths)
        elif self.proxies_env:
            entries: list[str] = [
                entry.strip()
                for entry in self.proxies_env.split(',')
                if entry.strip()
            ]
            catalog = load_proxy_entries(entries)
        else:
            catalog = load_proxy_catalog([])
        # ClassVar attrs aren't in model_fields; write to instance
        # __dict__ directly to avoid pydantic's __setattr__ guard
        # rejecting an unknown field.
        object.__setattr__(self, 'proxies', catalog.entries)
        set_active_catalog(catalog)
        return self
