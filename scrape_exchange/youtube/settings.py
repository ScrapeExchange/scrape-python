'''
Class leveraging pydantic-settings to manage configuration for the
scrape_exchange YouTube tools.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

from pydantic import AliasChoices, Field

from ..settings import ScraperSettings


class YouTubeScraperSettings(ScraperSettings):
    channel_data_directory: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'YOUTUBE_CHANNEL_DATA_DIR', 'channel_data_directory'
        ),
        description='Directory to save the scraped data',
    )
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
