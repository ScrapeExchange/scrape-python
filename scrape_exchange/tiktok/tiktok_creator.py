'''
TikTokCreator model — top-level user/profile data.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

from datetime import datetime, timezone
from typing import Self

from pydantic import BaseModel, ConfigDict

from scrape_exchange.tiktok.tiktok_thumbnail import Thumbnail


class TikTokCreator(BaseModel):
    '''A scraped TikTok creator/user profile.'''

    model_config = ConfigDict(extra='forbid')

    username: str
    sec_uid: str
    user_id: str
    nickname: str | None = None
    signature: str | None = None
    avatar_thumbnail: Thumbnail | None = None
    verified: bool = False
    private_account: bool = False
    region: str | None = None
    language: str | None = None
    follower_count: int = 0
    following_count: int = 0
    heart_count: int = 0
    video_count: int = 0
    friend_count: int = 0
    url: str
    scraped_timestamp: datetime

    @classmethod
    def from_api(
        cls,
        payload: dict,
        scraped_timestamp: datetime | None = None,
    ) -> Self:
        '''
        Build a TikTokCreator from the dict that
        ``TikTokApi.User.info`` returns.

        Required keys: ``uniqueId``, ``secUid``, ``id``.
        Missing required keys raise pydantic ValidationError.
        '''
        username: str = payload['uniqueId']
        stats: dict = payload.get('stats', {})
        avatar_url: str | None = payload.get('avatarLarger')
        avatar: Thumbnail | None = None
        if avatar_url:
            avatar = Thumbnail(url=avatar_url)
        when: datetime = (
            scraped_timestamp
            or datetime.now(timezone.utc)
        )
        return cls(
            username=username,
            sec_uid=payload['secUid'],
            user_id=payload['id'],
            nickname=payload.get('nickname'),
            signature=payload.get('signature'),
            avatar_thumbnail=avatar,
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
            url=f'https://www.tiktok.com/@{username}',
            scraped_timestamp=when,
        )

    def to_dict(self) -> dict:
        '''
        Return a dict suitable for JSON-Schema validation and
        bulk upload. Drops fields that are ``None`` so the
        record matches the schema (whose nullable fields use
        ``["string", "null"]`` while ``$ref``-typed optionals
        like ``avatar_thumbnail`` reject explicit nulls).
        '''
        return self.model_dump(mode='json', exclude_none=True)
