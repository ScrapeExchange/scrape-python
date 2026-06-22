'''
TikTokCreator model — top-level user/profile data.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

from datetime import datetime, timezone
from typing import Self

from pydantic import BaseModel, ConfigDict, Field

from scrape_exchange.tiktok.tiktok_avatar import (
    AvatarRef,
    avatar_refs_from_payload,
)


class TikTokVideoRef(BaseModel):
    '''Compact reference to a TikTok video discovered from a profile.'''

    model_config = ConfigDict(extra='forbid')

    video_id: str
    username: str
    url: str


class TikTokPlaylistRef(BaseModel):
    '''Compact reference to a TikTok playlist discovered from a profile.'''

    model_config = ConfigDict(extra='forbid')

    playlist_id: str
    name: str | None = None
    video_count: int | None = None
    cover_url: str | None = None


class TikTokCreator(BaseModel):
    '''A scraped TikTok creator/user profile.'''

    model_config = ConfigDict(extra='forbid')

    username: str
    sec_uid: str
    user_id: str
    nickname: str | None = None
    signature: str | None = None
    avatar_thumbnail: str | None = None
    avatar_urls: list[AvatarRef] = Field(default_factory=list)
    verified: bool = False
    private_account: bool = False
    region: str | None = None
    language: str | None = None
    follower_count: int = 0
    following_count: int = 0
    heart_count: int = 0
    video_count: int = 0
    friend_count: int = 0
    videos: list[TikTokVideoRef] = Field(default_factory=list)
    reposts: list[TikTokVideoRef] = Field(default_factory=list)
    liked: list[TikTokVideoRef] = Field(default_factory=list)
    playlists: list[TikTokPlaylistRef] = Field(default_factory=list)
    url: str
    scraped_timestamp: datetime

    @classmethod
    def from_user_info(
        cls,
        resp: dict,
        scraped_timestamp: datetime | None = None,
    ) -> Self:
        '''
        Build a TikTokCreator from the *raw* response that
        ``TikTokApi.User.info`` returns, namely
        ``{"userInfo": {"user": {...}, "stats": {...},
        "statsV2": {...}}}``.

        ``stats`` carries the counters as 32-bit ints, which
        overflow into negatives for huge accounts (>2.1B). TikTok
        ships the same counters as strings under ``statsV2`` to
        dodge that overflow, so ``statsV2`` wins where both are
        present (``from_api`` coerces the string values via
        ``int()``).
        '''
        user_info: dict = resp['userInfo']
        user: dict = user_info['user']
        stats: dict = {
            **user_info.get('stats', {}),
            **user_info.get('statsV2', {}),
        }
        return cls.from_api(
            {**user, 'stats': stats},
            scraped_timestamp=scraped_timestamp,
        )

    @classmethod
    def from_api(
        cls,
        payload: dict,
        scraped_timestamp: datetime | None = None,
    ) -> Self:
        '''
        Build a TikTokCreator from a *flattened* user dict: the
        ``User.info`` ``userInfo.user`` object plus a merged
        ``stats`` key. Use :meth:`from_user_info` to flatten the
        raw response (it handles the ``stats``/``statsV2``
        overflow); this method is the lower-level mapper.

        Required keys: ``uniqueId``, ``secUid``, ``id``.
        Missing required keys raise pydantic ValidationError.
        '''
        username: str | None = payload.get('uniqueId')
        stats: dict = payload.get('stats', {})
        avatar_url: str | None = payload.get('avatarLarger') or None
        when: datetime = (
            scraped_timestamp
            or datetime.now(timezone.utc)
        )
        return cls(
            username=username,
            sec_uid=payload.get('secUid'),
            user_id=payload.get('id'),
            nickname=payload.get('nickname'),
            signature=payload.get('signature'),
            avatar_thumbnail=avatar_url,
            avatar_urls=avatar_refs_from_payload(payload),
            verified=bool(payload.get('verified', False)),
            private_account=bool(
                payload.get('privateAccount', False),
            ),
            region=payload.get('region'),
            language=payload.get('language'),
            follower_count=int(stats.get('followerCount', 0)),
            following_count=int(
                stats.get('followingCount', 0),
            ),
            heart_count=int(stats.get('heartCount', 0)),
            video_count=int(stats.get('videoCount', 0)),
            friend_count=int(stats.get('friendCount', 0)),
            url=(
                f'https://www.tiktok.com/@{username}'
                if username else None
            ),
            scraped_timestamp=when,
        )

    def to_dict(self) -> dict:
        '''
        Return a dict suitable for JSON-Schema validation and
        bulk upload. Drops fields that are ``None`` so the
        record matches the schema (whose nullable fields use
        ``["string", "null"]``); optional fields like
        ``avatar_thumbnail`` are simply absent when unset.
        '''
        return self.model_dump(mode='json', exclude_none=True)
