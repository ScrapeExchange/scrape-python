'''
Configuration for the TikTok scraper package.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

from pydantic import AliasChoices, Field

from ..settings import ScraperSettings


class TikTokScraperSettings(ScraperSettings):
    '''
    Tool configuration for TikTok scrapers. Inherits proxies,
    redis_dsn, rate_limiter_state_dir, worker_id, log_*, api_key_*,
    and exchange_url from ScraperSettings.
    '''

    creator_data_directory: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'TIKTOK_CREATOR_DATA_DIR', 'creator_data_directory',
        ),
        description='Directory for scraped TikTok creator data',
    )
    video_data_directory: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'TIKTOK_VIDEO_DATA_DIR', 'video_data_directory',
        ),
        description='Directory for scraped TikTok video data',
    )
    hashtag_data_directory: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'TIKTOK_HASHTAG_DATA_DIR', 'hashtag_data_directory',
        ),
        description='Directory for scraped TikTok hashtag data',
    )
    creator_list: str = Field(
        default='tiktok_creators.lst',
        validation_alias=AliasChoices(
            'TIKTOK_CREATOR_LIST', 'creator_list',
        ),
        description='File listing creator usernames to scrape',
    )
    creator_map_file: str = Field(
        default='tiktok_creator_map.csv',
        validation_alias=AliasChoices(
            'TIKTOK_CREATOR_MAP_FILE', 'creator_map_file',
        ),
        description=(
            'CSV mapping username,sec_uid for creators '
            'scraped during this run.'
        ),
    )
    session_state_dir: str = Field(
        default='/tmp/scrape_exchange/tiktok',
        validation_alias=AliasChoices(
            'TIKTOK_SESSION_STATE_DIR', 'session_state_dir',
        ),
        description=(
            'Persists ms_tokens and Chromium profile dirs '
            'per proxy.'
        ),
    )
    ms_token_ttl_seconds: int = Field(
        default=14400,
        validation_alias=AliasChoices(
            'TIKTOK_MS_TOKEN_TTL', 'ms_token_ttl_seconds',
        ),
        description='Refresh ms_token after this many seconds',
    )
    bulk_batch_size: int = Field(
        default=1000,
        validation_alias=AliasChoices(
            'BULK_BATCH_SIZE', 'bulk_batch_size',
        ),
        description='Maximum records per bulk-upload batch',
    )
    bulk_max_batch_bytes: int = Field(
        default=7 * 1024 ** 3,
        validation_alias=AliasChoices(
            'BULK_MAX_BATCH_BYTES', 'bulk_max_batch_bytes',
        ),
        description='Soft byte cap for one bulk-upload batch',
    )
    bulk_progress_timeout_seconds: float = Field(
        default=1800.0,
        validation_alias=AliasChoices(
            'BULK_PROGRESS_TIMEOUT', 'bulk_progress_timeout',
        ),
        description=(
            'Max seconds to wait on the bulk-upload progress '
            'WebSocket for a terminal status.'
        ),
    )
