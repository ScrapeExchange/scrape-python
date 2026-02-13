'''
Model a thumbnail of a Youtube video

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

from enum import Enum
from uuid import uuid4
from uuid import UUID
from typing import Self


MAX_SPOOLED_FILE: int = 1024 * 1024
CHUNK_SIZE: int = 64 * 1024

class YouTubeThumbnailSize(Enum):
    # flake8: noqa=E221
    DEFAULT         = 'default'
    MEDIUM          = 'medium'
    HIGH            = 'high'


class YouTubeThumbnail:
    def __init__(self, data: dict, display_hint: str | None = None
                 ) -> None:
        self.url: str = data.get('url')
        self.width: int | None = data.get('width')
        self.height: int | None = data.get('height')
        self.id: str | None = data.get('id')
        self.preference: str = str(data.get('preference', ''))
        self.youtube_url: str = self.url

        # What type of display the thumbnail was created for.
        # Only used for channel banners
        # For banners, YouTube uses: 'banner', 'tvBanner', 'mobileBanner'
        self.display_hint: str | None = display_hint

    def __str__(self) -> str:
        # YouTube has thumbnails with '-mo' appended to the end of the URL
        # that is the same as the thumbnail without it
        size: int
        if isinstance(self.size, YouTubeThumbnailSize):
            size = self.size.value
        else:
            size = self.size

        return f'{size}_{self.width}_{self.height}_{self.url.rstrip("-mo")}'

    def __hash__(self) -> int:
        value: int = hash(
            f'{self.width}:{self.height}:{self.url}'
        )
        return value

    def __eq__(self, thumbnail: Self) -> bool:
        if not isinstance(thumbnail, YouTubeThumbnail):
            return False

        same: bool = (
            self.url == thumbnail.url
            or (
                self.width == thumbnail.width
                and self.height == thumbnail.height
                and self.id == thumbnail.id
                and self.preference == thumbnail.preference
            )
        )

        return same

    def __lt__(self, thumbnail: Self) -> bool:
        return (self.width * self.height) < (thumbnail.width * thumbnail.height)

    def to_dict(self) -> dict[str, str | int | UUID]:
        '''
        Returns a dict representation of the thumbnail
        '''

        data: dict[str, str | int | UUID] = {
            'id': self.id,
            'url': self.url,
            'width': self.width,
            'height': self.height,
            'preference': self.preference,
            'display_hint': self.display_hint
        }

        if self.youtube_url:
            data['youtube_url'] = self.youtube_url

        if self.display_hint:
            data['display_hint'] = self.display_hint

        return data

    @staticmethod
    def from_dict(data: dict[str, str | int | UUID]) -> Self:
        '''
        Factory for YouTubeThumbnail, parses data as provided by yt-dlp
        '''

        thumbnail = YouTubeThumbnail(data=data)

        return thumbnail