'''
Datatypes and Enums used across the scrape exchange package.
'''

from enum import Enum
from typing import Self


MAX_KEEPALIVE_REQUESTS: int = 80


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

SocialNetworks: dict[str, str] = {
    'facebook': 'Facebook',
    'twitter': 'Twitter',
    'x': 'X',
    'instagram': 'Instagram',
    'youtube': 'YouTube',
    'linkedin': 'LinkedIn',
    'pinterest': 'Pinterest',
    'tumblr': 'tumblr',
    'reddit': 'reddit',
    'snapchat': 'Snapchat',
    'flickr': 'flickr',
    'tiktok': 'TikTok',
    'whatsapp': 'WhatsApp',
    'telegram': 'Telegram',
    'signal': 'Signal',
    'messenger': 'messenger',
    'parler': 'Parler',
    'gab': 'Gab',
    'rumble': 'Rumble',
    'patreon': 'Patreon',
    'twitch': 'twitch',
    'spotify': 'Spotify',
    'discord': 'Discord',
    'slack': 'Slack',
    'nebula': 'Nebula',
    'wechat': 'WeChat',
    'douyin': 'Douyin',
    'kuaishou': 'Kuaishou',
    'weibo': 'Weibo',
    'qq': 'QQ',
    'qzone': 'Qzone',
    'myjosh': 'Josh',
    'microsoft': 'Teams',
    'quora': 'Quora',
    'skype': 'Skype',
    'tieba': 'Tieba',
    'baidu': 'Baidu',
    'viber': 'Viber',
    'line': 'Line',
    'imo': 'Imo',
    'xiaohongshu': 'Xiaohongshu',
    'likee': 'Likee',
    'picsart': 'Picsart',
    'soundcloud': 'SoundCloud',
    'onlyfans': 'OnlyFans',
    'vevo': 'Vevo',
    'vk': 'VK',
    'threads': 'Threads',
    'zoom': 'Zoom',
    'meet': 'Meet',
    'clubhouse': 'Clubhouse',
    'imessage': 'iMessage',
    'facetime': 'FaceTime',
    'byo': 'BYO.Tube',
    'steampowered': 'Steam',
    'linktr.ee': 'Linktree',
    'amzn': 'Amazon',
    'amazon': 'Amazon',
}