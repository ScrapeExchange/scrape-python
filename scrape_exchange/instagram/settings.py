'''
Configuration for Instagram scraper tools.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

from pydantic import AliasChoices, Field

from scrape_exchange.settings import ScraperSettings


class InstagramScraperSettings(ScraperSettings):
    '''Shared Instagram scraper settings.'''

    creator_data_directory: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'IG_CREATOR_DATA_DIR', 'creator_data_directory',
        ),
        description='Directory for scraped Instagram creator data',
    )
    session_state_dir: str = Field(
        default='/tmp/scrape_exchange/instagram',
        validation_alias=AliasChoices(
            'IG_SESSION_STATE_DIR', 'session_state_dir',
        ),
        description='Directory for Instagram browser session state',
    )
    session_bootstrap_timeout_ms: int = Field(
        default=90000,
        validation_alias=AliasChoices(
            'IG_SESSION_BOOTSTRAP_TIMEOUT',
            'session_bootstrap_timeout_ms',
        ),
        description='Milliseconds to wait for browser bootstrap',
    )
