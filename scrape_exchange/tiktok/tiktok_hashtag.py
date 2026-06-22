'''
TikTokHashtag model — hashtag profile + summary stats.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

from datetime import datetime, timezone
from typing import Self

from pydantic import BaseModel, ConfigDict


class TikTokHashtag(BaseModel):
    model_config = ConfigDict(extra='forbid')

    name: str
    hashtag_id: str
    description: str | None = None
    view_count: int = 0
    video_count: int = 0
    is_commerce: bool = False
    url: str
    scraped_timestamp: datetime

    @classmethod
    def from_api(
        cls,
        payload: dict,
        scraped_timestamp: datetime | None = None,
    ) -> Self:
        name: str | None = payload.get('title')
        hashtag_id_raw: object = payload.get('id')
        hashtag_id: str | None = (
            str(hashtag_id_raw)
            if hashtag_id_raw is not None
            else None
        )
        stats: dict = payload.get('stats', {})
        when: datetime = (
            scraped_timestamp
            or datetime.now(timezone.utc)
        )
        url: str | None = None
        if name:
            url = f'https://www.tiktok.com/tag/{name}'
        return cls(
            name=name,
            hashtag_id=hashtag_id,
            description=payload.get('desc'),
            view_count=int(stats.get('viewCount', 0)),
            video_count=int(stats.get('videoCount', 0)),
            is_commerce=bool(payload.get('isCommerce', False)),
            url=url,
            scraped_timestamp=when,
        )

    def to_dict(self) -> dict:
        return self.model_dump(
            mode='json', exclude_none=True,
        )
