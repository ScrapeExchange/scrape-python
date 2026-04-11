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
    upload_only: bool = Field(
        default=False,
        validation_alias=AliasChoices('UPLOAD_ONLY', 'upload_only'),
        description='Only perform the upload step, skipping data scraping',
    )
    no_upload: bool = Field(
        default=False,
        validation_alias=AliasChoices('#NO_UPLOAD', '#no_upload'),
        description='Only perform the scraping step, skipping data upload',
    )
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
            'scraped during this run (format: channel_id,channel_name).'
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
