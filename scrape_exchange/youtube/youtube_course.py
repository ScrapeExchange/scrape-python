'''
Model a YouTube course as scraped from a channel's courses tab

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

from typing import Self
from logging import Logger
from logging import getLogger
from datetime import UTC, datetime

_LOGGER: Logger = getLogger(__name__)


class YouTubeCourseVideo:
    '''
    A single child video inside a YouTube course (playlist).
    '''

    def __init__(self,
                 video_id: str | None = None,
                 title: str | None = None,
                 duration_label: str | None = None) -> None:
        self.video_id: str | None = video_id
        self.title: str | None = title
        self.duration_label: str | None = duration_label

    def __eq__(self, other: Self) -> bool:
        if not isinstance(other, YouTubeCourseVideo):
            return False

        return (
            self.video_id == other.video_id
            and self.title == other.title
            and self.duration_label == other.duration_label
        )

    def __hash__(self) -> int:
        return hash(self.video_id)

    def __repr__(self) -> str:
        return (
            f'YouTubeCourseVideo(id={self.video_id}, '
            f'title={self.title!r})'
        )

    def to_dict(self) -> dict[str, any]:
        return {
            'video_id': self.video_id,
            'title': self.title,
            'duration_label': self.duration_label,
        }

    @staticmethod
    def from_dict(data: dict[str, any]) -> Self:
        return YouTubeCourseVideo(
            video_id=data.get('video_id'),
            title=data.get('title'),
            duration_label=data.get('duration_label'),
        )

    @staticmethod
    def from_innertube(child: dict[str, any]) -> Self | None:
        '''
        Parse a ``childVideoRenderer`` dict from the InnerTube API.
        '''

        cvr: dict = child.get('childVideoRenderer', {})
        if not cvr:
            return None

        video_id: str | None = cvr.get(
            'navigationEndpoint', {}
        ).get(
            'watchEndpoint', {}
        ).get('videoId')

        title: str | None = cvr.get(
            'title', {}
        ).get('simpleText')

        duration_label: str | None = cvr.get(
            'lengthText', {}
        ).get(
            'accessibility', {}
        ).get(
            'accessibilityData', {}
        ).get('label')

        return YouTubeCourseVideo(
            video_id=video_id,
            title=title,
            duration_label=duration_label,
        )


class YouTubeCourse:
    '''
    Represents a YouTube course as returned by the InnerTube browse API
    on a channel's "Courses" tab.

    The data comes from a ``playlistRenderer`` inside a
    ``richItemRenderer`` of the courses tab.
    '''

    PLAYLIST_URL: str = (
        'https://www.youtube.com/playlist?list={playlist_id}'
    )

    def __init__(self,
                 playlist_id: str | None = None,
                 title: str | None = None,
                 video_count: int = 0,
                 thumbnail_url: str | None = None,
                 channel_id: str | None = None,
                 videos: list[YouTubeCourseVideo] | None = None
                 ) -> None:

        self.playlist_id: str | None = playlist_id
        self.title: str | None = title
        self.video_count: int = video_count
        self.thumbnail_url: str | None = thumbnail_url
        self.channel_id: str | None = channel_id
        self.videos: list[YouTubeCourseVideo] = videos or []
        self.created_timestamp: datetime = datetime.now(tz=UTC)

    def __eq__(self, other: Self) -> bool:
        if not isinstance(other, YouTubeCourse):
            return False

        return (
            self.playlist_id == other.playlist_id
            and self.title == other.title
            and self.video_count == other.video_count
            and self.thumbnail_url == other.thumbnail_url
            and self.channel_id == other.channel_id
            and self.videos == other.videos
        )

    def __hash__(self) -> int:
        return hash(self.playlist_id)

    def __repr__(self) -> str:
        return (
            f'YouTubeCourse(id={self.playlist_id}, '
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
        Returns a dict representation of the course
        '''

        return {
            'playlist_id': self.playlist_id,
            'title': self.title,
            'video_count': self.video_count,
            'thumbnail_url': self.thumbnail_url,
            'channel_id': self.channel_id,
            'url': self.url,
            'videos': [v.to_dict() for v in self.videos],
            'created_timestamp': self.created_timestamp.isoformat(),
        }

    @staticmethod
    def from_dict(data: dict[str, any]) -> Self:
        '''
        Factory method that creates a YouTubeCourse from a dict
        previously produced by ``to_dict``.
        '''

        course = YouTubeCourse(
            playlist_id=data.get('playlist_id'),
            title=data.get('title'),
            video_count=int(data.get('video_count', 0)),
            thumbnail_url=data.get('thumbnail_url'),
            channel_id=data.get('channel_id'),
            videos=[
                YouTubeCourseVideo.from_dict(v)
                for v in data.get('videos', [])
            ],
        )

        ts: str | None = data.get('created_timestamp')
        if ts:
            course.created_timestamp = datetime.fromisoformat(ts)

        return course

    @staticmethod
    def from_innertube(item: dict[str, any],
                       channel_id: str | None = None
                       ) -> Self | None:
        '''
        Factory method that creates a YouTubeCourse from a raw
        ``richItemRenderer`` dict returned by the InnerTube browse API
        on the courses tab.

        :param item: a single richItemRenderer from the contents list
        :param channel_id: optional channel ID to associate
        :returns: a YouTubeCourse or None if parsing fails
        '''

        pr: dict = item.get(
            'richItemRenderer', {}
        ).get(
            'content', {}
        ).get('playlistRenderer', {})

        if not pr:
            return None

        playlist_id: str | None = pr.get('playlistId')
        if not playlist_id:
            return None

        title: str | None = pr.get('title', {}).get('simpleText')

        video_count: int = int(pr.get('videoCount', 0))

        # First thumbnail set is the main course thumbnail
        thumbnail_url: str | None = None
        thumb_sets: list = pr.get('thumbnails', [])
        if thumb_sets:
            sources: list = thumb_sets[0].get('thumbnails', [])
            if sources:
                thumbnail_url = sources[0].get('url')

        # Child videos previewed in the listing
        videos: list[YouTubeCourseVideo] = []
        for child in pr.get('videos', []):
            video: YouTubeCourseVideo | None = (
                YouTubeCourseVideo.from_innertube(child)
            )
            if video:
                videos.append(video)

        return YouTubeCourse(
            playlist_id=playlist_id,
            title=title,
            video_count=video_count,
            thumbnail_url=thumbnail_url,
            channel_id=channel_id,
            videos=videos,
        )
