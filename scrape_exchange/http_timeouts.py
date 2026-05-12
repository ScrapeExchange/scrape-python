'''Per-scraper HTTP timeout settings.

Each scraper type — RSS, channel, video — gets its own pair
of connect / request timeout values. The values come from
:class:`HttpTimeoutSettings`, a pydantic-settings model
that picks them up from env vars or the project ``.env``
file. Defaults are 5s connect and 10s request, matching the
RSS playbook recommendation.

* ``RSS_CONNECT_TIMEOUT`` / ``RSS_REQUEST_TIMEOUT``
* ``CHANNEL_CONNECT_TIMEOUT`` / ``CHANNEL_REQUEST_TIMEOUT``
* ``VIDEO_CONNECT_TIMEOUT`` / ``VIDEO_REQUEST_TIMEOUT``

Two import surfaces:

* :data:`HTTP_TIMEOUTS` — the full settings object. Use when
  the caller needs to look up a *different* scraper's
  timeout (e.g. an RSS process calling the InnerTube API
  needs the RSS values; a future cross-scraper helper might
  want all three).
* :data:`HTTP_CONNECT_TIMEOUT` / :data:`HTTP_REQUEST_TIMEOUT`
  — convenience aliases pre-resolved at module import time
  to the values for *this* process, identified by
  ``sys.argv[0]``. Existing call sites that imported the
  old shared constants keep working unchanged.

Resolution map for the convenience aliases:

* ``yt_rss_scrape.py``     → RSS values
* ``yt_channel_scrape.py`` → channel values
* ``yt_video_scrape.py``   → video values
* anything else            → RSS values (safe fallback)
'''

import os
import sys

from pathlib import Path

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class HttpTimeoutSettings(BaseSettings):
    '''Per-scraper HTTP timeout configuration. Loaded once
    at module import.'''

    model_config = SettingsConfigDict(
        env_file=(
            str(Path(__file__).parent.parent / '.env'),
        ),
        env_file_encoding='utf-8',
        extra='ignore',
    )

    rss_connect_timeout: float = Field(
        default=5.0,
        validation_alias=AliasChoices(
            'RSS_CONNECT_TIMEOUT', 'rss_connect_timeout',
        ),
        description=(
            'TCP + CONNECT + TLS handshake budget for an '
            'RSS scraper HTTPS request, in seconds. '
            'Default 5s. Applies to RSS XML fetches and to '
            'the InnerTube API calls the RSS scraper makes '
            'to enrich videos.'
        ),
    )
    rss_request_timeout: float = Field(
        default=30.0,
        validation_alias=AliasChoices(
            'RSS_REQUEST_TIMEOUT', 'rss_request_timeout',
        ),
        description=(
            'Read + write + pool budget for an RSS scraper '
            'HTTPS request, in seconds. Default 10s.'
        ),
    )
    channel_connect_timeout: float = Field(
        default=5.0,
        validation_alias=AliasChoices(
            'CHANNEL_CONNECT_TIMEOUT',
            'channel_connect_timeout',
        ),
        description=(
            'TCP + CONNECT + TLS handshake budget for a '
            'channel scraper HTTPS request, in seconds. '
            'Default 5s. Applies to InnerTube browse_channel '
            'and HTML about-page fetches.'
        ),
    )
    channel_request_timeout: float = Field(
        default=10.0,
        validation_alias=AliasChoices(
            'CHANNEL_REQUEST_TIMEOUT',
            'channel_request_timeout',
        ),
        description=(
            'Read + write + pool budget for a channel '
            'scraper HTTPS request, in seconds. Default 10s.'
        ),
    )
    video_connect_timeout: float = Field(
        default=5.0,
        validation_alias=AliasChoices(
            'VIDEO_CONNECT_TIMEOUT', 'video_connect_timeout',
        ),
        description=(
            'TCP + CONNECT + TLS handshake budget for a '
            'video scraper HTTPS request, in seconds. '
            'Default 5s.'
        ),
    )
    video_request_timeout: float = Field(
        default=10.0,
        validation_alias=AliasChoices(
            'VIDEO_REQUEST_TIMEOUT', 'video_request_timeout',
        ),
        description=(
            'Read + write + pool budget for a video '
            'scraper HTTPS request, in seconds. Default 10s.'
        ),
    )


# Singleton instance — loaded once at module import, mirrors
# how every other settings object in the codebase works.
HTTP_TIMEOUTS: HttpTimeoutSettings = HttpTimeoutSettings()


# Resolve the convenience aliases for *this* process based
# on the script name. Keeps existing import sites
# (``from scrape_exchange.http_timeouts import
# HTTP_CONNECT_TIMEOUT``) working without each having to
# know which scraper it belongs to — the shared
# AsyncYouTubeClient gets the right values automatically
# depending on which scraper imported it first.
_SCRAPER_TO_FIELDS: dict[str, tuple[str, str]] = {
    'yt_rss_scrape.py': (
        'rss_connect_timeout', 'rss_request_timeout',
    ),
    'yt_channel_scrape.py': (
        'channel_connect_timeout',
        'channel_request_timeout',
    ),
    'yt_video_scrape.py': (
        'video_connect_timeout', 'video_request_timeout',
    ),
    'rebuild_creator_map.py': (
        'channel_connect_timeout',
        'channel_request_timeout',
    ),
}

_SCRIPT_NAME: str = (
    os.path.basename(sys.argv[0]) if sys.argv else ''
)
_connect_field: str
_request_field: str
_connect_field, _request_field = _SCRAPER_TO_FIELDS.get(
    _SCRIPT_NAME,
    ('rss_connect_timeout', 'rss_request_timeout'),
)

HTTP_CONNECT_TIMEOUT: float = getattr(
    HTTP_TIMEOUTS, _connect_field,
)
HTTP_REQUEST_TIMEOUT: float = getattr(
    HTTP_TIMEOUTS, _request_field,
)
