'''
TikTokVideo model — single video or photo-carousel post.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import re
from datetime import datetime, timezone
from typing import Self

from pydantic import BaseModel, ConfigDict

from scrape_exchange.tiktok.tiktok_caption import Caption
from scrape_exchange.tiktok.tiktok_sound import Sound
from scrape_exchange.tiktok.tiktok_thumbnail import Thumbnail


_HASHTAG_RE: re.Pattern[str] = re.compile(
    r'(?<!\w)#(\w+)',
)
_MENTION_RE: re.Pattern[str] = re.compile(
    r'(?<!\w)@(\w+)',
)


def extract_hashtags(desc: str | None) -> list[str]:
    if not desc:
        return []
    return _HASHTAG_RE.findall(desc)


def extract_mentions(desc: str | None) -> list[str]:
    if not desc:
        return []
    return _MENTION_RE.findall(desc)


class TikTokVideo(BaseModel):
    model_config = ConfigDict(extra='forbid')

    video_id: str
    username: str
    sec_uid: str
    description: str | None = None
    created_timestamp: datetime
    duration: int | None = None
    aspect_ratio: float | None = None
    is_photo_post: bool = False
    image_count: int | None = None
    view_count: int = 0
    like_count: int = 0
    comment_count: int = 0
    share_count: int = 0
    collect_count: int = 0
    thumbnails: dict[str, Thumbnail] = {}
    sound: Sound | None = None
    hashtags: list[str] = []
    mentions: list[str] = []
    subtitles: dict[str, Caption] = {}
    url: str
    availability: str | None = None
    scraped_timestamp: datetime

    @classmethod
    def from_api(
        cls,
        payload: dict,
        scraped_timestamp: datetime | None = None,
    ) -> Self:
        author: dict = payload.get('author', {})
        video_block: dict = payload.get('video', {})
        stats: dict = payload.get('stats', {})
        music: dict | None = payload.get('music')
        image_post: dict | None = payload.get('imagePost')

        username: str | None = author.get('uniqueId')
        video_id: str | None = payload.get('id')
        desc: str | None = payload.get('desc')

        thumbnails: dict[str, Thumbnail] = {}
        for key in ('cover', 'originCover', 'dynamicCover'):
            thumb_url: str | None = video_block.get(key)
            if thumb_url:
                thumbnails[key] = Thumbnail(url=thumb_url)

        subtitle_infos: list[dict] = (
            video_block.get('subtitleInfos') or []
        )
        subtitles: dict[str, Caption] = {}
        for info in subtitle_infos:
            lang: str | None = info.get('LanguageCodeName')
            if not lang:
                continue
            subtitles[lang] = Caption(
                language_code=lang,
                url=info.get('Url'),
                extension=info.get('Format'),
            )

        sound_obj: Sound | None = None
        if music:
            sound_obj = Sound(
                id=str(music['id']),
                title=music.get('title'),
                author_name=music.get('authorName'),
                original=music.get('original'),
                duration=music.get('duration'),
                play_url=music.get('playUrl'),
            )

        when: datetime = (
            scraped_timestamp
            or datetime.now(timezone.utc)
        )

        is_photo: bool = image_post is not None
        image_count: int | None = None
        if is_photo:
            image_count = len(image_post.get('images', []))

        create_time: int | None = payload.get('createTime')
        created_ts: datetime | None = None
        if create_time is not None:
            created_ts = datetime.fromtimestamp(
                int(create_time), tz=timezone.utc,
            )

        url: str | None = None
        if username and video_id:
            url = (
                f'https://www.tiktok.com/@{username}'
                f'/video/{video_id}'
            )

        return cls(
            video_id=video_id,
            username=username,
            sec_uid=author.get('secUid'),
            description=desc,
            created_timestamp=created_ts,
            duration=video_block.get('duration'),
            aspect_ratio=None,
            is_photo_post=is_photo,
            image_count=image_count,
            view_count=int(stats.get('playCount', 0)),
            like_count=int(stats.get('diggCount', 0)),
            comment_count=int(stats.get('commentCount', 0)),
            share_count=int(stats.get('shareCount', 0)),
            collect_count=int(stats.get('collectCount', 0)),
            thumbnails=thumbnails,
            sound=sound_obj,
            hashtags=extract_hashtags(desc),
            mentions=extract_mentions(desc),
            subtitles=subtitles,
            url=url,
            availability=None,
            scraped_timestamp=when,
        )

    def to_dict(self) -> dict:
        '''
        Return a dict for JSON-Schema validation and bulk
        upload. Drops None-valued fields so $ref-typed optional
        properties (e.g. ``sound``) are absent rather than
        ``null``.
        '''
        return self.model_dump(mode='json', exclude_none=True)
