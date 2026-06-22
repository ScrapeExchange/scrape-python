'''
TikTokVideo model — single video or photo-carousel post.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import re
from datetime import datetime, timezone
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field

from scrape_exchange.tiktok.tiktok_avatar import (
    AvatarRef,
    avatar_refs_from_payload,
)
from scrape_exchange.tiktok.tiktok_caption import Caption
from scrape_exchange.tiktok.tiktok_format import (
    TikTokFormat,
    formats_from_payload,
)
from scrape_exchange.tiktok.tiktok_sound import Sound
from scrape_exchange.tiktok.tiktok_thumbnail import Thumbnail


_HASHTAG_RE: re.Pattern[str] = re.compile(
    r'(?<!\w)#(\w+)',
)
_MENTION_RE: re.Pattern[str] = re.compile(
    r'(?<!\w)@(\w+)',
)
_AUTHOR_AVATAR_PREFERENCE: tuple[str, ...] = (
    'medium', 'thumb', 'large',
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
    author_id: str | None = None
    author_nickname: str | None = None
    author_signature: str | None = None
    author_avatar_urls: list[AvatarRef] = Field(default_factory=list)
    author_avatar_url: str | None = None
    author_verified: bool | None = None
    author_private: bool | None = None
    author_secret: bool | None = None
    author_relation: int | None = None
    author_comment_setting: int | None = None
    author_download_setting: int | None = None
    author_duet_setting: int | None = None
    author_stitch_setting: int | None = None
    author_embed_banned: bool | None = None
    author_open_favorite: bool | None = None
    author_ftc: bool | None = None
    author_is_ad_virtual: bool | None = None
    author_user_story_status: int | None = None
    author_stats: dict[str, int] = Field(default_factory=dict)
    description: str | None = None
    aigc_description: str | None = None
    created_timestamp: datetime
    duration: int | None = None
    video_api_id: str | None = None
    is_hd_bitrate: bool | None = None
    formats: dict[str, TikTokFormat] = Field(
        default_factory=dict,
    )
    play_url: str | None = None
    download_url: str | None = None
    definition: str | None = None
    video_quality: str | None = None
    container_format: str | None = None
    codec: str | None = None
    bitrate: int | None = None
    encoded_type: str | None = None
    is_photo_post: bool = False
    image_count: int | None = None
    image_urls: list[str] = Field(default_factory=list)
    image_cover_urls: dict[str, str] = Field(default_factory=dict)
    view_count: int = 0
    like_count: int = 0
    comment_count: int = 0
    share_count: int = 0
    collect_count: int = 0
    repost_count: int | None = None
    category_type: int | None = None
    text_language: str | None = None
    text_translatable: bool | None = None
    title_language: str | None = None
    title_translatable: bool | None = None
    is_ad: bool | None = None
    is_reviewing: bool | None = None
    official_item: bool | None = None
    original_item: bool | None = None
    private_item: bool | None = None
    secret: bool | None = None
    share_enabled: bool | None = None
    for_friend: bool | None = None
    collected: bool | None = None
    digged: bool | None = None
    duet_display: int | None = None
    stitch_display: int | None = None
    item_comment_status: int | None = None
    can_repost: bool | None = None
    creator_ai_comment: dict[str, Any] = Field(default_factory=dict)
    backend_source_event_tracking: str | None = None
    thumbnails: dict[str, Thumbnail] = Field(default_factory=dict)
    sound: Sound | None = None
    hashtags: list[str] = Field(default_factory=list)
    mentions: list[str] = Field(default_factory=list)
    subtitles: dict[str, Caption] = Field(default_factory=dict)
    url: str
    availability: str | None = None
    scraped_timestamp: datetime
    raw_item: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_api(
        cls,
        payload: dict,
        scraped_timestamp: datetime | None = None,
    ) -> Self:
        author: dict = payload.get('author', {})
        video_block: dict = payload.get('video', {})
        stats: dict = payload.get('stats', {})
        stats_v2: dict = payload.get('statsV2', {})
        author_stats_raw: dict = (
            payload.get('authorStatsV2')
            or payload.get('authorStats', {})
        )
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
            music_id_raw: object = music.get('id')
            music_id: str | None = (
                str(music_id_raw) if music_id_raw is not None else None
            )
            sound_obj = Sound(
                id=music_id,
                title=music.get('title'),
                author_name=music.get('authorName'),
                original=music.get('original'),
                duration=music.get('duration'),
                play_url=music.get('playUrl'),
                cover_large_url=music.get('coverLarge'),
                cover_medium_url=music.get('coverMedium'),
                cover_thumb_url=music.get('coverThumb'),
                copyrighted=music.get('isCopyrighted'),
                commerce_music=music.get('is_commerce_music'),
                unlimited_music=music.get('is_unlimited_music'),
                private=music.get('private'),
                shoot_duration=music.get('shoot_duration'),
            )

        when: datetime = (
            scraped_timestamp
            or datetime.now(timezone.utc)
        )

        is_photo: bool = image_post is not None
        image_count: int | None = None
        image_urls: list[str] = []
        image_cover_urls: dict[str, str] = {}
        if is_photo:
            images: list = image_post.get('images', [])
            image_count = len(images)
            image_urls = _extract_image_urls(images)
            image_cover_urls = _extract_image_covers(image_post)

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
            author_id=_to_optional_str(author.get('id')),
            author_nickname=author.get('nickname'),
            author_signature=author.get('signature'),
            author_avatar_urls=avatar_refs_from_payload(author),
            author_verified=author.get('verified'),
            author_private=author.get('privateAccount'),
            author_secret=author.get('secret'),
            author_relation=_to_optional_int(author.get('relation')),
            author_comment_setting=_to_optional_int(
                author.get('commentSetting'),
            ),
            author_download_setting=_to_optional_int(
                author.get('downloadSetting'),
            ),
            author_duet_setting=_to_optional_int(
                author.get('duetSetting'),
            ),
            author_stitch_setting=_to_optional_int(
                author.get('stitchSetting'),
            ),
            author_embed_banned=author.get('isEmbedBanned'),
            author_open_favorite=author.get('openFavorite'),
            author_ftc=author.get('ftc'),
            author_is_ad_virtual=author.get('isADVirtual'),
            author_user_story_status=_to_optional_int(
                author.get('UserStoryStatus'),
            ),
            author_stats=_int_dict(author_stats_raw),
            description=desc,
            aigc_description=payload.get('AIGCDescription'),
            created_timestamp=created_ts,
            duration=video_block.get('duration'),
            video_api_id=_to_optional_str(video_block.get('id')),
            is_hd_bitrate=payload.get('IsHDBitrate'),
            formats=formats_from_payload(video_block),
            play_url=_url_or_none(video_block.get('playAddr')),
            download_url=_url_or_none(
                video_block.get('downloadAddr'),
            ),
            definition=_str_or_none(
                video_block.get('definition'),
            ),
            video_quality=_str_or_none(
                video_block.get('videoQuality'),
            ),
            container_format=_str_or_none(
                video_block.get('format'),
            ),
            codec=_str_or_none(video_block.get('codecType')),
            bitrate=_to_optional_int(
                video_block.get('bitrate'),
            ),
            encoded_type=_str_or_none(
                video_block.get('encodedType'),
            ),
            is_photo_post=is_photo,
            image_count=image_count,
            image_urls=image_urls,
            image_cover_urls=image_cover_urls,
            view_count=int(stats.get('playCount', 0)),
            like_count=int(stats.get('diggCount', 0)),
            comment_count=int(stats.get('commentCount', 0)),
            share_count=int(stats.get('shareCount', 0)),
            collect_count=int(stats.get('collectCount', 0)),
            repost_count=_to_optional_int(stats_v2.get('repostCount')),
            category_type=_to_optional_int(payload.get('CategoryType')),
            text_language=payload.get('textLanguage'),
            text_translatable=payload.get('textTranslatable'),
            title_language=payload.get('titleLanguage'),
            title_translatable=payload.get('titleTranslatable'),
            is_ad=payload.get('isAd'),
            is_reviewing=payload.get('isReviewing'),
            official_item=payload.get('officalItem'),
            original_item=payload.get('originalItem'),
            private_item=payload.get('privateItem'),
            secret=payload.get('secret'),
            share_enabled=payload.get('shareEnabled'),
            for_friend=payload.get('forFriend'),
            collected=payload.get('collected'),
            digged=payload.get('digged'),
            duet_display=_to_optional_int(payload.get('duetDisplay')),
            stitch_display=_to_optional_int(payload.get('stitchDisplay')),
            item_comment_status=_to_optional_int(
                payload.get('itemCommentStatus'),
            ),
            can_repost=_extract_can_repost(payload),
            creator_ai_comment=_dict_or_empty(
                payload.get('creatorAIComment'),
            ),
            backend_source_event_tracking=payload.get(
                'backendSourceEventTracking',
            ),
            thumbnails=thumbnails,
            sound=sound_obj,
            hashtags=extract_hashtags(desc),
            mentions=extract_mentions(desc),
            subtitles=subtitles,
            url=url,
            availability=None,
            scraped_timestamp=when,
            raw_item=payload,
        )

    def to_dict(self) -> dict:
        '''
        Return a dict for JSON-Schema validation and bulk
        upload. Drops None-valued fields so $ref-typed optional
        properties (e.g. ``sound``) are absent rather than
        ``null``. Keeps top-level stream codec/bitrate details off
        disk because the public schema stores them under ``formats``.
        '''
        record: dict[str, Any] = self.model_dump(
            mode='json',
            exclude_none=True,
            exclude={'codec', 'bitrate'},
        )
        author_avatar_url: str | None = _preferred_author_avatar_url(
            self.author_avatar_urls,
        )
        if author_avatar_url is not None:
            record['author_avatar_url'] = author_avatar_url
        return record


def _to_optional_str(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _preferred_author_avatar_url(
    refs: list[AvatarRef],
) -> str | None:
    by_name: dict[str, str] = {
        ref.name: ref.url for ref in refs if ref.url
    }
    for name in _AUTHOR_AVATAR_PREFERENCE:
        url: str | None = by_name.get(name)
        if url:
            return url
    return None


def _to_optional_int(value: object) -> int | None:
    if value is None or value == '':
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _str_or_none(value: object) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    return value


def _url_or_none(value: object) -> str | None:
    return _str_or_none(value)


def _to_optional_float(value: object) -> float | None:
    if value is None or value == '':
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_dict(raw: dict) -> dict[str, int]:
    out: dict[str, int] = {}
    for key, value in raw.items():
        parsed: int | None = _to_optional_int(value)
        if parsed is not None:
            out[key] = parsed
    return out


def _dict_or_empty(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def _extract_can_repost(payload: dict) -> bool | None:
    item_control: object = payload.get('item_control')
    if not isinstance(item_control, dict):
        return None
    value: object = item_control.get('can_repost')
    if isinstance(value, bool):
        return value
    return None


def _first_url(value: object) -> str | None:
    if isinstance(value, str) and value:
        return value
    if not isinstance(value, dict):
        return None
    url_list: object = value.get('urlList')
    if isinstance(url_list, list):
        for item in url_list:
            if isinstance(item, str) and item:
                return item
    return None


def _extract_image_urls(images: list) -> list[str]:
    urls: list[str] = []
    for image in images:
        if not isinstance(image, dict):
            continue
        image_url: str | None = _first_url(
            image.get('imageURL') or image.get('imageUrl'),
        )
        if image_url:
            urls.append(image_url)
    return urls


def _extract_image_covers(image_post: dict) -> dict[str, str]:
    covers: dict[str, str] = {}
    for key in ('cover', 'shareCover'):
        block: object = image_post.get(key)
        if not isinstance(block, dict):
            continue
        url: str | None = _first_url(block.get('imageURL'))
        if url:
            covers[key] = url
    return covers
