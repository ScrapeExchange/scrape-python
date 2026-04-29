'''
Data types and enums for YouTube scraping.

:maintainer: Boinko <boinko@scrape.exchange>
:copyright: Copyright 2026
:license: GPLv3
'''

from enum import Enum
from typing import Self
from dataclasses import dataclass


class YouTubeChannelPageType(Enum):
    VIDEOS = 'Videos'
    SHORTS = 'Shorts'
    LIVE = 'Live'
    COURSES = 'Courses'
    PODCASTS = 'Podcasts'
    PLAYLISTS = 'Playlists'
    POSTS = 'Posts'


@dataclass
class YouTubeChannelLink:
    channel_handle: str
    subscriber_count: int

    def __hash__(self) -> int:
        return hash(self.channel_handle)

    def to_dict(self) -> dict[str, str | int]:
        return {
            'channel_handle': self.channel_handle,
            'subscriber_count': self.subscriber_count,
        }

    @staticmethod
    def from_dict(data: dict[str, str | int]) -> Self:
        return YouTubeChannelLink(
            channel_handle=data['channel_handle'],
            subscriber_count=data['subscriber_count'],
        )

