'''
Class leveraging pydantic-settings to manage configuration for the
scrape_exchange YouTube tools.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import os

from urllib.parse import urlsplit

from pydantic import AliasChoices, Field

from ..settings import ScraperSettings


# Channels below this many subscribers are not worth scraping videos
# from; see YouTubeScraperSettings.channel_min_subscribers.
DEFAULT_CHANNEL_MIN_SUBSCRIBERS: int = 10
# Below this many subscribers a channel is treated as an individual:
# personal data in its descriptions is masked. Above it YouTube
# channels can be monetized.
DEFAULT_CHANNEL_PRIVACY_SUBSCRIBERS: int = 4000
# Views per subscriber assumed when a channel has no subscriber count.
DEFAULT_CHANNEL_VIEWS_PER_SUBSCRIBER: int = 100


class YouTubeScraperSettings(ScraperSettings):
    channel_data_directory: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'YOUTUBE_CHANNEL_DATA_DIR',
            'YOUTUBE_CHANNELS_DATA_DIR',
            'channel_data_directory',
        ),
        description='Directory to save the scraped data',
    )
    channel_priority_directory: str = Field(
        default='priority',
        validation_alias=AliasChoices(
            'YOUTUBE_CHANNEL_PRIORITY_DIRECTORY',
            'youtube_channel_priority_directory',
        ),
        description=(
            'Directory where the RSS scraper writes lite '
            'channel-stat records (channel_id, '
            'channel_handle, url, title, '
            'subscriber_count, video_count, view_count, '
            'description) for tools/scrape_upload.py '
            'to pick up and POST to '
            'scrape.exchange. Defaults to "priority" '
            'which is resolved relative to '
            'YOUTUBE_CHANNEL_DATA_DIR; an absolute path '
            'is taken as-is.'
        ),
    )

    @property
    def channel_priority_directory_path(self) -> str:
        '''Return the fully-resolved channel priority
        directory. Relative values (including the default
        ``'priority'``) are joined to
        ``channel_data_directory``; absolute values are used
        verbatim.

        :raises ValueError: if
            ``channel_priority_directory`` is relative and
            ``channel_data_directory`` is not set. The
            producer and consumer both call this at startup,
            so misconfiguration surfaces before any
            per-channel work runs.
        '''
        raw: str = self.channel_priority_directory
        if os.path.isabs(raw):
            return raw
        base: str | None = self.channel_data_directory
        if not base:
            raise ValueError(
                'channel_priority_directory is relative '
                f'({raw!r}) but channel_data_directory is '
                'not set; either set '
                'YOUTUBE_CHANNEL_DATA_DIR or pass an '
                'absolute '
                'YOUTUBE_CHANNEL_PRIORITY_DIRECTORY.'
            )
        return os.path.join(base, raw)

    channel_map_file: str = Field(
        default='channel_map.csv',
        validation_alias=AliasChoices(
            'YOUTUBE_CHANNEL_MAP_FILE', 'channel_map_file'
        ),
        description=(
            'CSV file to save mapping of channel IDs to names for channels '
            'scraped during this run (format: channel_id,channel_handle).'
        )
    )
    channel_list: str = Field(
        default='channels.lst',
        validation_alias=AliasChoices(
            'YOUTUBE_CHANNEL_LIST', 'channel_list'
        )
    )
    video_data_directory: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'YOUTUBE_VIDEO_DATA_DIR', 'video_data_directory'
        ),
        description='Directory to save the scraped video data',
    )
    channel_min_subscribers: int = Field(
        default=DEFAULT_CHANNEL_MIN_SUBSCRIBERS,
        ge=0,
        validation_alias=AliasChoices(
            'CHANNEL_MIN_SUBSCRIBERS', 'channel_min_subscribers',
        ),
        description=(
            'Channels with fewer subscribers are scraped once, saved '
            'with the .low_subs suffix (never uploaded) and marked '
            'low_subs; their videos are saved as .low_subs instead of '
            'being uploaded. A channel without a subscriber count is '
            'compared on views (this value times '
            'YOUTUBE_CHANNEL_VIEWS_PER_SUBSCRIBER); without either '
            'count it counts as below the minimum. Featured-channel '
            'links with a known count below it are not queued. 0 '
            'disables the check.'
        ),
    )
    channel_privacy_subscribers: int = Field(
        default=DEFAULT_CHANNEL_PRIVACY_SUBSCRIBERS,
        ge=0,
        validation_alias=AliasChoices(
            'YOUTUBE_CHANNEL_PRIVACY_SUBSCRIBERS',
            'channel_privacy_subscribers',
        ),
        description=(
            'Channels with fewer subscribers get email addresses, '
            'phone numbers, street addresses and links to personal '
            'websites masked in their channel and video '
            'descriptions. 0 disables masking.'
        ),
    )
    channel_views_per_subscriber: int = Field(
        default=DEFAULT_CHANNEL_VIEWS_PER_SUBSCRIBER,
        ge=1,
        validation_alias=AliasChoices(
            'YOUTUBE_CHANNEL_VIEWS_PER_SUBSCRIBER',
            'channel_views_per_subscriber',
        ),
        description=(
            'When a channel has no subscriber count, its total view '
            'count is compared to the subscriber thresholds times '
            'this factor.'
        ),
    )
    privacy_allowed_domains: str = Field(
        default='',
        validation_alias=AliasChoices(
            'YOUTUBE_PRIVACY_ALLOWED_DOMAINS',
            'privacy_allowed_domains',
        ),
        description=(
            'Comma-separated domains whose links are never masked, '
            'in addition to the built-in list of social networks '
            'and well-known sites.'
        ),
    )

    @property
    def extra_allowed_domains(self) -> frozenset[str]:
        '''Return :attr:`privacy_allowed_domains` as a set of
        lowercased domains. A scheme, path and leading ``www.`` are
        dropped, so ``https://www.mysite.example/`` becomes
        ``mysite.example``.'''
        domains: set[str] = set()
        item: str
        for item in self.privacy_allowed_domains.split(','):
            candidate: str = item.strip()
            if not candidate:
                continue
            if '://' not in candidate:
                candidate = f'https://{candidate}'
            try:
                host: str = urlsplit(candidate).hostname or ''
            except ValueError:
                continue
            domain: str = host.lower().rstrip('.').removeprefix('www.')
            if domain:
                domains.add(domain)
        return frozenset(domains)
    bulk_batch_size: int = Field(
        default=1000,
        validation_alias=AliasChoices(
            'BULK_BATCH_SIZE', 'bulk_batch_size',
        ),
        description=(
            'Maximum number of records sent per bulk-upload POST. '
            'The byte cap ``bulk_max_batch_bytes`` is applied '
            'alongside and whichever cap is hit first finalises '
            'the batch.'
        ),
    )
    bulk_max_batch_bytes: int = Field(
        default=7 * 1024 * 1024 * 1024,
        validation_alias=AliasChoices(
            'BULK_MAX_BATCH_BYTES', 'bulk_max_batch_bytes',
        ),
        description=(
            'Soft byte cap for one bulk-upload batch. Stays under '
            'the bulk endpoint server-side 8 GB limit with '
            'headroom for multipart framing.'
        ),
    )
    bulk_progress_timeout_seconds: float = Field(
        default=1800.0,
        validation_alias=AliasChoices(
            'BULK_PROGRESS_TIMEOUT', 'bulk_progress_timeout',
        ),
        description=(
            'Maximum seconds to wait on the bulk-upload progress '
            'WebSocket for a terminal status before giving up. '
            'Source files are left in base_dir for the next run.'
        ),
    )
    rss_circuit_fail_threshold: int = Field(
        default=8,
        validation_alias=AliasChoices(
            'RSS_CIRCUIT_FAIL_THRESHOLD',
            'rss_circuit_fail_threshold',
        ),
        description=(
            'F: number of 404s in the last T attempts on '
            'previously-scraped channels that trips the RSS '
            'circuit.'
        ),
    )
    rss_circuit_window_size: int = Field(
        default=10,
        validation_alias=AliasChoices(
            'RSS_CIRCUIT_WINDOW_SIZE',
            'rss_circuit_window_size',
        ),
        description=(
            'T: size of the rolling attempt window evaluated '
            'against rss_circuit_fail_threshold.'
        ),
    )
    rss_circuit_initial_open_seconds: int = Field(
        default=60,
        validation_alias=AliasChoices(
            'RSS_CIRCUIT_INITIAL_OPEN_SECONDS',
            'rss_circuit_initial_open_seconds',
        ),
        description=(
            'S: initial cooldown in seconds when the RSS '
            'circuit first trips.'
        ),
    )
    rss_circuit_max_open_seconds: int = Field(
        default=7200,
        validation_alias=AliasChoices(
            'RSS_CIRCUIT_MAX_OPEN_SECONDS',
            'rss_circuit_max_open_seconds',
        ),
        description=(
            'Ceiling for the doubled S in impaired mode '
            '(2 h by default).'
        ),
    )
    rss_circuit_impaired_reopen_threshold: int = Field(
        default=3,
        validation_alias=AliasChoices(
            'RSS_CIRCUIT_IMPAIRED_REOPEN_THRESHOLD',
            'rss_circuit_impaired_reopen_threshold',
        ),
        description=(
            'C: consecutive 404s while in impaired-closed that '
            're-open the circuit at the doubled cooldown.'
        ),
    )
    rss_circuit_recovery_threshold: int = Field(
        default=50,
        validation_alias=AliasChoices(
            'RSS_CIRCUIT_RECOVERY_THRESHOLD',
            'rss_circuit_recovery_threshold',
        ),
        description=(
            'N: consecutive successes while in impaired-closed '
            'that return the circuit to regular mode.'
        ),
    )
    rss_circuit_wait_jitter_seconds: float = Field(
        default=30.0,
        validation_alias=AliasChoices(
            'RSS_CIRCUIT_WAIT_JITTER_SECONDS',
            'rss_circuit_wait_jitter_seconds',
        ),
        description=(
            'Maximum random jitter (seconds) added to every '
            'circuit-breaker wait. Spreads worker wake-ups so '
            'only one probe leaks per recovery window.'
        ),
    )
