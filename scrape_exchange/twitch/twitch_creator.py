'''Public profile record, independent of the browser and queue.'''

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class TwitchLink(BaseModel):
    model_config = ConfigDict(extra='forbid')

    url: str
    title: str | None = None


class TwitchPanel(BaseModel):
    model_config = ConfigDict(extra='forbid')

    title: str | None = None
    description: str | None = None
    image_url: str | None = None
    link_url: str | None = None


class TwitchCreator(BaseModel):
    model_config = ConfigDict(extra='forbid')

    username: str
    url: str
    scraped_timestamp: datetime
    user_id: str | None = None
    display_name: str | None = None
    biography: str | None = None
    avatar_url: str | None = None
    banner_url: str | None = None
    follower_count: int | None = Field(default=None, ge=0)
    follower_count_is_approximate: bool = False
    partner: bool | None = None
    affiliate: bool | None = None
    social_links: list[TwitchLink] = Field(default_factory=list)
    panels: list[TwitchPanel] = Field(default_factory=list)
    title: str | None = None
    category_id: str | None = Field(default=None, pattern=r'^[0-9]+$')
    category_name: str | None = None
    language: str | None = Field(default=None, min_length=1)
    primary_color_hex: str | None = Field(
        default=None, pattern=r'^[0-9A-F]{6}$',
    )
    sources: list[str] = Field(default_factory=list)
    completeness: Literal['partial', 'complete'] = 'partial'

    def to_dict(self) -> dict:
        return self.model_dump(mode='json', exclude_none=True)
