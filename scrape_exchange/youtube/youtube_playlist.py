'''
Model a YouTube playlist as scraped from a channel's playlists tab

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import re

from typing import Self
from logging import Logger
from logging import getLogger
from datetime import UTC, datetime

_LOGGER: Logger = getLogger(__name__)

_VIDEO_COUNT_RE: re.Pattern[str] = re.compile(r'(\d+)\s+video')


class YouTubePlaylist:
    '''
    Represents a YouTube playlist as returned by the InnerTube browse API
    on a channel's "Playlists" tab.

    The data comes from a ``lockupViewModel`` item inside the
    ``gridRenderer`` of the playlists tab.
    '''

    PLAYLIST_URL: str = 'https://www.youtube.com/playlist?list={playlist_id}'

    def __init__(self,
                 playlist_id: str | None = None,
                 title: str | None = None,
                 video_count: int = 0,
                 thumbnail_url: str | None = None,
                 channel_id: str | None = None) -> None:

        self.playlist_id: str | None = playlist_id
        self.title: str | None = title
        self.video_count: int = video_count
        self.thumbnail_url: str | None = thumbnail_url
        self.channel_id: str | None = channel_id
        self.created_timestamp: datetime = datetime.now(tz=UTC)

    def __eq__(self, other: Self) -> bool:
        if not isinstance(other, YouTubePlaylist):
            return False

        return (
            self.playlist_id == other.playlist_id
            and self.title == other.title
            and self.video_count == other.video_count
            and self.thumbnail_url == other.thumbnail_url
            and self.channel_id == other.channel_id
        )

    def __hash__(self) -> int:
        return hash(self.playlist_id)

    def __repr__(self) -> str:
        return (
            f'YouTubePlaylist(id={self.playlist_id}, '
            f'title={self.title!r}, '
            f'video_count={self.video_count})'
        )

    @property
    def url(self) -> str | None:
        if self.playlist_id:
            return self.PLAYLIST_URL.format(
                playlist_id=self.playlist_id
            )
        return None

    def to_dict(self) -> dict[str, any]:
        '''
        Returns a dict representation of the playlist
        '''

        return {
            'playlist_id': self.playlist_id,
            'title': self.title,
            'video_count': self.video_count,
            'thumbnail_url': self.thumbnail_url,
            'channel_id': self.channel_id,
            'url': self.url,
            'created_timestamp': self.created_timestamp.isoformat(),
        }

    @staticmethod
    def from_dict(data: dict[str, any]) -> Self:
        '''
        Factory method that creates a YouTubePlaylist from a dict
        previously produced by ``to_dict``.
        '''

        playlist = YouTubePlaylist(
            playlist_id=data.get('playlist_id'),
            title=data.get('title'),
            video_count=int(data.get('video_count', 0)),
            thumbnail_url=data.get('thumbnail_url'),
            channel_id=data.get('channel_id'),
        )

        ts: str | None = data.get('created_timestamp')
        if ts:
            playlist.created_timestamp = datetime.fromisoformat(ts)

        return playlist

    @staticmethod
    def from_innertube(item: dict[str, any],
                       channel_id: str | None = None) -> Self | None:
        '''
        Factory method that creates a YouTubePlaylist from the raw
        ``lockupViewModel`` dict returned by the InnerTube browse API.

        :param item: a single item from the gridRenderer contents list
        :param channel_id: optional channel ID to associate
        :returns: a YouTubePlaylist or None if the item is not a playlist
        '''

        lvm: dict = item.get('lockupViewModel', {})
        if not lvm:
            return None

        content_type: str = lvm.get('contentType', '')
        if content_type != 'LOCKUP_CONTENT_TYPE_PLAYLIST':
            _LOGGER.debug(
                f'Skipping non-playlist lockup: {content_type}'
            )
            return None

        playlist_id: str | None = lvm.get('contentId')
        if not playlist_id:
            return None

        # Title
        meta: dict = lvm.get(
            'metadata', {}
        ).get('lockupMetadataViewModel', {})
        title: str | None = meta.get('title', {}).get('content')

        # Thumbnail URL
        thumb_vm: dict = lvm.get(
            'contentImage', {}
        ).get(
            'collectionThumbnailViewModel', {}
        ).get(
            'primaryThumbnail', {}
        ).get('thumbnailViewModel', {})

        thumbnail_url: str | None = None
        sources: list = thumb_vm.get('image', {}).get('sources', [])
        if sources:
            thumbnail_url = sources[0].get('url')

        # Video count from overlay badge (e.g. "22 videos")
        video_count: int = 0
        for overlay in thumb_vm.get('overlays', []):
            badges: list = overlay.get(
                'thumbnailOverlayBadgeViewModel', {}
            ).get('thumbnailBadges', [])
            for badge in badges:
                badge_text: str = badge.get(
                    'thumbnailBadgeViewModel', {}
                ).get('text', '')
                match: re.Match | None = _VIDEO_COUNT_RE.search(
                    badge_text
                )
                if match:
                    video_count = int(match.group(1))

        playlist = YouTubePlaylist(
            playlist_id=playlist_id,
            title=title,
            video_count=video_count,
            thumbnail_url=thumbnail_url,
            channel_id=channel_id,
        )

        return playlist
