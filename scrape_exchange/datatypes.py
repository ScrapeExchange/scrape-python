'''
Datatypes and Enums used across the scrape exchange package.
'''

from enum import Enum
from typing import Self

class IngestStatus(str, Enum):
    # flake8: noqa: E201
    NONE =          'none'
    PENDING =       'pending'
    SCRAPED =       'scraped'
    UPLOADED =      'uploaded'
    FAILED =        'failed'
    UNAVAILABLE =   'unavailable'

class Platform(str, Enum):
    YOUTUBE = 'youtube'
    TIKTOK = 'tiktok'
    TWITCH = 'twitch'
    KICK = 'kick'
    RUMBLE = 'rumble'
    FACEBOOK = 'facebook'
    INSTAGRAM = 'instagram'
    X = 'x'
    TELEGRAM = 'telegram'
    THREADS = 'threads'
    REDDIT = 'reddit'

    @classmethod
    def _missing_(cls, value: str) -> Self | None:
        value = value.lower()
        for member in cls:
            if member.lower() == value:
                return member
        return None

class PlatformEntityType(str, Enum):
    CHANNEL = 'channel'
    VIDEO = 'video'
    MESSAGE = 'message'
    COMMENT = 'comment'
    POST = 'post'
    TOPIC = 'topic'
    THREAD = 'thread'
    USER = 'user'
    STREAM = 'stream'
    IMAGE = 'image'
    STORY = 'story'
    REEL = 'reel'
    GROUP = 'group'
    PAGE = 'page'
    SUBREDDIT = 'subreddit'
    SPACE = 'space'
